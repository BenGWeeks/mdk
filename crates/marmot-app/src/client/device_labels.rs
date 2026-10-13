//! Optional label discovery. Projections only enqueue local demand; workers own I/O.
use super::AppClient;
use crate::{AppError, MarmotApp};
use cgka_traits::{GroupId, MaintenanceStorage, TransportEndpoint};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::time::Instant;

/// Maximum remote accounts queried in one worker pass.
pub(crate) const DEVICE_LABEL_MAX_ACCOUNTS_PER_PASS: usize = 8;
/// Maximum kind-30443 events admitted per remote account, including sibling slots.
pub(crate) const DEVICE_LABEL_EVENT_LIMIT: usize = 12;
/// Minimum cooldown after pass completion, including misses and failures.
pub(crate) const DEVICE_LABEL_REFETCH_INTERVAL: Duration = Duration::from_secs(3600);
/// Bound pending demand and the cooldown ledger without evicting unexpired entries.
const DEVICE_LABEL_MAX_TRACKED_ACCOUNTS: usize = 1024;
/// Bound group notification demand associated with a single remote account.
const DEVICE_LABEL_MAX_GROUPS_PER_ACCOUNT: usize = 128;
/// Bound demanded devices within each group.
const DEVICE_LABEL_MAX_DEVICES_PER_GROUP: usize = 128;

type GroupDemand = HashMap<GroupId, HashSet<String>>;
/// Bound the complete discovery + package fetch wait for each optional account.
const DEVICE_LABEL_FETCH_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Default)]
struct Demand {
    groups: BTreeMap<String, GroupDemand>,
    attempted: HashMap<String, Instant>,
    active: HashSet<String>,
}

#[derive(Clone, Default)]
pub(crate) struct DeviceLabelEnrichment {
    demand: Arc<Mutex<Demand>>,
    pub(crate) wake: Arc<tokio::sync::Notify>,
}

impl DeviceLabelEnrichment {
    pub(crate) fn schedule(&self, account: &str, group: &GroupId, device_id: &str) {
        let now = Instant::now();
        let mut demand = self.demand.lock().unwrap_or_else(|e| e.into_inner());
        if !demand.active.contains(account)
            && demand
                .attempted
                .get(account)
                .is_some_and(|at| now.duration_since(*at) < DEVICE_LABEL_REFETCH_INTERVAL)
        {
            return;
        }
        demand
            .attempted
            .retain(|_, at| now.duration_since(*at) < DEVICE_LABEL_REFETCH_INTERVAL);
        if !demand.groups.contains_key(account)
            && demand.groups.len() + demand.attempted.len() >= DEVICE_LABEL_MAX_TRACKED_ACCOUNTS
        {
            // Keep the first admitted accounts deterministically. Rejected
            // accounts acquire no cooldown, so subsequent reads retry them.
            tracing::debug!(target: "marmot_app::device_labels", method = "schedule",
                pending_accounts = demand.groups.len(), "label demand saturated; retry on next read");
            return;
        }
        let groups = demand.groups.entry(account.to_owned()).or_default();
        if groups.contains_key(group) || groups.len() < DEVICE_LABEL_MAX_GROUPS_PER_ACCOUNT {
            let devices = groups.entry(group.clone()).or_default();
            if devices.len() < DEVICE_LABEL_MAX_DEVICES_PER_GROUP {
                devices.insert(device_id.to_owned());
            }
        }
        self.wake.notify_one();
    }

    pub(crate) fn begin(&self) -> Vec<String> {
        let mut demand = self.demand.lock().unwrap_or_else(|e| e.into_inner());
        if !demand.active.is_empty() {
            return Vec::new();
        }
        let accounts = demand
            .groups
            .keys()
            .take(DEVICE_LABEL_MAX_ACCOUNTS_PER_PASS)
            .cloned()
            .collect::<Vec<_>>();
        for account in &accounts {
            demand.attempted.insert(account.clone(), Instant::now());
            demand.active.insert(account.clone());
        }
        accounts
    }

    fn finish(&self) -> BTreeMap<String, GroupDemand> {
        let mut demand = self.demand.lock().unwrap_or_else(|e| e.into_inner());
        let accounts = std::mem::take(&mut demand.active);
        accounts
            .into_iter()
            .filter_map(|account| {
                // Charge the completed attempt: earlier accounts in a slow pass
                // must also wait the full interval before another fetch.
                demand.attempted.insert(account.clone(), Instant::now());
                demand
                    .groups
                    .remove(&account)
                    .map(|groups| (account, groups))
            })
            .collect()
    }
}

pub(crate) struct FetchedLabel {
    pub(crate) account: String,
    pub(crate) key: Vec<u8>,
    pub(crate) label: String,
    pub(crate) created_at: u64,
}

#[cfg(test)]
thread_local! { pub(crate) static LABEL_QUERY_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

/// A fixed burst window; later admissions never postpone publication.
pub(crate) const DEVICE_LABEL_CHANGE_COALESCE_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Default)]
struct LabelEventState {
    events: Option<tokio::sync::broadcast::Sender<crate::MarmotAppEvent>>,
    executor: Option<tokio::runtime::Handle>,
    task: Option<tokio::task::AbortHandle>,
    generation: u64,
    // One pending scope regardless of how many accounts change in a burst.
    pending: Option<Option<String>>,
}

struct WorkerDemand {
    demand: Weak<Mutex<Demand>>,
    wake: Weak<tokio::sync::Notify>,
}

/// The registry retains no worker state after exit, including on cancellation.
pub(crate) struct DeviceLabelDemandRegistration {
    workers: Arc<Mutex<HashMap<String, WorkerDemand>>>,
    owner: String,
    demand: Weak<Mutex<Demand>>,
}
impl Drop for DeviceLabelDemandRegistration {
    fn drop(&mut self) {
        let mut workers = self.workers.lock().unwrap_or_else(|e| e.into_inner());
        if workers
            .get(&self.owner)
            .is_some_and(|worker| worker.demand.ptr_eq(&self.demand))
        {
            workers.remove(&self.owner);
        }
    }
}

/// Created before spawn so even an unpolled, cancelled task retries its scope.
struct LabelPublicationGuard {
    state: Weak<Mutex<LabelEventState>>,
    generation: u64,
    deadline: Instant,
    // Retry a cancelled executor once on the admission executor, then on a
    // fenced OS thread if that executor has also been destroyed.
    retry: bool,
    executor_id: Option<tokio::runtime::Id>,
    fallback: Option<tokio::runtime::Handle>,
}
impl Drop for LabelPublicationGuard {
    fn drop(&mut self) {
        let Some(shared) = self.state.upgrade() else {
            return;
        };
        let retry = {
            let mut state = shared.lock().unwrap_or_else(|e| e.into_inner());
            if state.generation != self.generation {
                return;
            }
            state.task = None;
            // Do not overwrite a managed executor installed since admission.
            if state.executor.as_ref().map(tokio::runtime::Handle::id) == self.executor_id {
                state.executor = self.fallback.clone();
            }
            if self.retry && state.pending.is_some() {
                // Fence the rejected spawn's late abort-handle installation.
                state.generation = state.generation.wrapping_add(1);
                Some(state.generation)
            } else {
                state.pending = None;
                None
            }
        };
        if let Some(generation) = retry {
            spawn_label_publication(
                shared,
                generation,
                self.fallback.take(),
                None,
                self.deadline,
            );
        }
    }
}

// Spawning can synchronously drop the future (and lock state through its
// guard). Aborting can also start cancellation. Neither runs under state.
fn spawn_label_publication(
    shared: Arc<Mutex<LabelEventState>>,
    generation: u64,
    executor: Option<tokio::runtime::Handle>,
    fallback: Option<tokio::runtime::Handle>,
    deadline: Instant,
) {
    let guard = LabelPublicationGuard {
        state: Arc::downgrade(&shared),
        generation,
        deadline,
        retry: executor.is_some(),
        executor_id: executor.as_ref().map(tokio::runtime::Handle::id),
        fallback,
    };
    let publication_state = shared.clone();
    let publish = move || {
        let mut state = publication_state.lock().unwrap_or_else(|e| e.into_inner());
        if state.generation != generation {
            return;
        }
        if let Some(account_id_hex) = state.pending.take()
            && let Some(events) = &state.events
        {
            let _ = events.send(crate::MarmotAppEvent::DeviceLabelsChanged { account_id_hex });
        }
        state.task = None;
        // Disarm cancellation without discarding the managed executor.
        state.generation = state.generation.wrapping_add(1);
    };
    if let Some(executor) = executor {
        let task = executor
            .spawn(async move {
                let _guard = guard;
                tokio::time::sleep_until(deadline).await;
                publish();
            })
            .abort_handle();
        let installed = {
            let mut state = shared.lock().unwrap_or_else(|e| e.into_inner());
            if state.generation == generation && state.pending.is_some() {
                state.task = Some(task.clone());
                true
            } else {
                false
            }
        };
        if !installed {
            task.abort();
        }
    } else {
        // Synchronous hosts and destroyed executors still get a fixed-window
        // publisher. Generation fencing suppresses it after runtime teardown.
        std::thread::spawn(move || {
            let _guard = guard;
            std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
            publish();
        });
    }
}

#[derive(Clone, Default)]
pub(crate) struct DeviceLabelEvents {
    state: Arc<Mutex<LabelEventState>>,
    workers: Arc<Mutex<HashMap<String, WorkerDemand>>>,
    admission: Arc<Mutex<()>>,
}
impl DeviceLabelEvents {
    pub(crate) fn attach(&self, events: tokio::sync::broadcast::Sender<crate::MarmotAppEvent>) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.events = Some(events);
        state.executor = tokio::runtime::Handle::try_current().ok();
    }
    pub(crate) fn set_executor(&self, executor: tokio::runtime::Handle) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .executor = Some(executor);
    }
    pub(crate) fn stop(&self) {
        let task = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.generation = state.generation.wrapping_add(1);
            state.pending = None;
            state.events = None;
            state.executor = None;
            state.task.take()
        };
        if let Some(task) = task {
            task.abort();
        }
    }
    #[cfg(test)]
    pub(crate) fn demand_receiver_count(&self) -> usize {
        self.workers.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
    pub(crate) fn register_demand(
        &self,
        owner: &str,
        enrichment: &DeviceLabelEnrichment,
    ) -> DeviceLabelDemandRegistration {
        let demand = Arc::downgrade(&enrichment.demand);
        self.workers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                owner.to_owned(),
                WorkerDemand {
                    demand: demand.clone(),
                    wake: Arc::downgrade(&enrichment.wake),
                },
            );
        DeviceLabelDemandRegistration {
            workers: self.workers.clone(),
            owner: owner.to_owned(),
            demand,
        }
    }
    pub(crate) fn schedule(&self, owner: &str, messages: &[crate::TimelineMessageRecord]) {
        self.schedule_senders(
            owner,
            messages.iter().map(|message| {
                (
                    message.group_id_hex.as_str(),
                    message.sender.as_str(),
                    &message.sender_device,
                )
            }),
        );
    }
    pub(crate) fn schedule_senders<'a>(
        &self,
        owner: &str,
        messages: impl IntoIterator<Item = (&'a str, &'a str, &'a crate::SenderDevice)>,
    ) {
        // The bounded set belongs to the worker. Notify is only a wake-up:
        // coalescing or losing a wake never removes pending work.
        let enrichment = {
            let workers = self.workers.lock().unwrap_or_else(|e| e.into_inner());
            let Some(worker) = workers.get(owner) else {
                return;
            };
            let (Some(demand), Some(wake)) = (worker.demand.upgrade(), worker.wake.upgrade())
            else {
                return;
            };
            DeviceLabelEnrichment { demand, wake }
        };
        let mut batch = HashSet::new();
        for (group, account, device) in messages {
            if let crate::SenderDevice::Exact {
                device_id,
                client_label: None,
                ..
            } = device
                && batch.insert((group, account, device_id.as_str()))
                && let Ok(group) = hex::decode(group)
            {
                enrichment.schedule(account, &GroupId::new(group), device_id);
            }
        }
    }
    /// Prefer the managed executor over the admission caller's executor. If
    /// its stored handle is stale, retain the pending scope and deadline and
    /// retry on the caller's executor, then a teardown-fenced OS thread. A
    /// destroyed executor must never silently lose an admitted label change.
    fn publish_change(&self, account: &str) {
        let (generation, executor) = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.events.is_none() {
                return;
            }
            if let Some(scope) = &mut state.pending {
                if scope.as_deref() != Some(account) {
                    *scope = None;
                }
                return;
            }
            state.pending = Some(Some(account.to_owned()));
            state.generation = state.generation.wrapping_add(1);
            (state.generation, state.executor.clone())
        };
        let current = tokio::runtime::Handle::try_current().ok();
        let fallback = current.filter(|current| {
            executor
                .as_ref()
                .is_none_or(|executor| executor.id() != current.id())
        });
        spawn_label_publication(
            self.state.clone(),
            generation,
            executor,
            fallback,
            Instant::now() + DEVICE_LABEL_CHANGE_COALESCE_INTERVAL,
        );
    }
}

/// One batch-local resolver, shared by message records and projection deltas.
pub(crate) struct DeviceLabelResolver<'a> {
    app: &'a MarmotApp,
    candidates: HashMap<String, Vec<(Vec<u8>, String)>>,
    devices: HashMap<(String, String), HashMap<String, String>>,
}
impl<'a> DeviceLabelResolver<'a> {
    pub(crate) fn new(app: &'a MarmotApp) -> Self {
        Self {
            app,
            candidates: HashMap::new(),
            devices: HashMap::new(),
        }
    }
    pub(crate) fn label(
        &mut self,
        group_hex: &str,
        sender: &str,
        device_id: &str,
    ) -> Option<String> {
        let key = (group_hex.to_owned(), sender.to_owned());
        if !self.devices.contains_key(&key) {
            let group = GroupId::new(hex::decode(group_hex).ok()?);
            let candidates = self.candidates.entry(sender.to_owned()).or_insert_with(|| {
                self.app
                    .resolve_device_labels([sender.to_owned()], |_, _, package| {
                        cgka_engine::key_package::key_package_metadata(package).map_err(|_| {
                            AppError::InvalidKeyPackageEvent("invalid advisory package".into())
                        })
                    })
                    .into_iter()
                    .map(|((_, key), label)| (key, label))
                    .collect()
            });
            self.devices.insert(
                key.clone(),
                candidates
                    .iter()
                    .map(|(key, label)| {
                        (
                            cgka_traits::message::member_device_id(&group, key),
                            label.clone(),
                        )
                    })
                    .collect(),
            );
        }
        self.devices
            .get(&key)
            .and_then(|labels| labels.get(device_id))
            .cloned()
    }
    pub(crate) fn enrich(&mut self, group: &str, sender: &str, device: &mut crate::SenderDevice) {
        if let crate::SenderDevice::Exact {
            device_id,
            client_label,
            ..
        } = device
        {
            *client_label = self.label(group, sender, device_id);
        }
    }
    pub(crate) fn timeline(&mut self, message: &mut crate::TimelineMessageRecord) {
        self.enrich(
            &message.group_id_hex,
            &message.sender,
            &mut message.sender_device,
        );
    }
}

impl MarmotApp {
    /// Advisory reads are local, account-bounded, and never affect attribution.
    pub(crate) fn enrich_sender_device_label(
        &self,
        group: &GroupId,
        sender: &str,
        device: &mut crate::SenderDevice,
    ) {
        if let crate::SenderDevice::Exact {
            device_id,
            client_label,
            ..
        } = device
        {
            *client_label = self.cached_sender_device_label(group, sender, device_id);
        }
    }

    pub(crate) fn cached_sender_device_label(
        &self,
        group: &GroupId,
        sender: &str,
        device_id: &str,
    ) -> Option<String> {
        DeviceLabelResolver::new(self).label(&hex::encode(group.as_slice()), sender, device_id)
    }

    /// Precedence: durable own publication, validated directory package, bounded cache.
    pub(crate) fn resolve_device_labels(
        &self,
        account_ids: impl IntoIterator<Item = String>,
        mut validate: impl FnMut(
            &str,
            bool,
            &cgka_traits::engine::KeyPackage,
        )
            -> Result<cgka_engine::key_package::KeyPackageMetadata, AppError>,
    ) -> HashMap<(String, Vec<u8>), String> {
        let account_ids = account_ids.into_iter().collect::<Vec<_>>();
        if account_ids.is_empty() {
            return HashMap::new();
        }
        let mut labels = HashMap::new();
        let mut failed_advisory_reads = 0usize;
        let mut unavailable_accounts = HashSet::new();
        #[cfg(test)]
        LABEL_QUERY_COUNT.with(|count| count.set(count.get() + 1));
        match self.shared_storage().and_then(|storage| {
            storage
                .device_client_labels(&account_ids)
                .map_err(AppError::from)
        }) {
            Ok(rows) => {
                for row in rows {
                    if let Some(label) =
                        crate::key_package_records::sanitize_client_label(&row.client_label)
                    {
                        labels.insert((row.account_id_hex, row.signature_key), label);
                    }
                }
            }
            Err(_) => failed_advisory_reads += 1,
        }
        match self.member_device_key_packages(&account_ids) {
            Ok(packages) => {
                for (account_id, package) in packages {
                    let result = (|| -> Result<(), AppError> {
                        let Some(package) = package? else {
                            return Ok(());
                        };
                        let Some(label) = package
                            .client_label
                            .as_deref()
                            .and_then(crate::key_package_records::sanitize_client_label)
                        else {
                            return Ok(());
                        };
                        let bytes = hex::decode(&package.key_package_hex)?;
                        let metadata = validate(
                            &account_id,
                            false,
                            &cgka_traits::engine::KeyPackage::new(bytes).with_protocol_profile(
                                cgka_traits::group::ProtocolProfile::Current,
                            ),
                        )?;
                        if metadata.credential_identity_hex == account_id {
                            let key = (account_id.clone(), metadata.signature_key);
                            labels.insert(key, label);
                        } else {
                            return Err(AppError::InvalidKeyPackageEvent(
                                "advisory identity mismatch".into(),
                            ));
                        }
                        Ok(())
                    })();
                    if result.is_err() {
                        unavailable_accounts.insert(account_id);
                        failed_advisory_reads += 1;
                    }
                }
            }
            Err(_) => {
                unavailable_accounts.extend(account_ids.iter().cloned());
                failed_advisory_reads += 1;
            }
        }
        // Preserve the corrupt-advisory failure contract for both surfaces.
        labels.retain(|(account, _), _| !unavailable_accounts.contains(account));
        // Own durable publications are never evicted and need no directory/network.
        if let Ok(accounts) = self.account_home().accounts() {
            for account in accounts
                .into_iter()
                .filter(|a| account_ids.contains(&a.account_id_hex))
            {
                let publication = (|| -> Result<(), AppError> {
                    let Some(artifact) = self
                        .account_storage(&account.label)?
                        .key_package_lifecycle()?
                        .and_then(|state| state.authored_signed_event)
                    else {
                        return Ok(());
                    };
                    let event: transport_nostr_peeler::NostrTransportEvent =
                        serde_json::from_slice(&artifact.bytes)?;
                    if event.pubkey != account.account_id_hex
                        || event.to_verified_nostr_event().is_err()
                    {
                        return Ok(());
                    }
                    let (fetched, metadata) =
                        crate::key_package_records::key_package_from_record_with_metadata(
                            &crate::RelayEventRecord {
                                event,
                                endpoints: Vec::new(),
                            },
                            |package| validate(&account.account_id_hex, true, package),
                        )?;
                    if metadata.credential_identity_hex == account.account_id_hex
                        && let Some(label) = fetched.client_label
                    {
                        labels.insert((account.account_id_hex, metadata.signature_key), label);
                    }
                    Ok(())
                })();
                failed_advisory_reads += usize::from(publication.is_err());
            }
        }
        if failed_advisory_reads > 0 {
            tracing::warn!(target: "marmot_app::device_labels", method = "resolve_device_labels",
                failed_advisory_reads, "advisory device label enrichment unavailable");
        }
        labels
    }

    pub(crate) fn enrich_message_device_label(&self, message: &mut crate::AppMessageRecord) {
        if let Ok(bytes) = hex::decode(&message.group_id_hex) {
            self.enrich_sender_device_label(
                &GroupId::new(bytes),
                &message.sender,
                &mut message.sender_device,
            );
        } else if let crate::SenderDevice::Exact { client_label, .. } = &mut message.sender_device {
            *client_label = None;
        }
    }

    pub(crate) fn enrich_timeline_device_label(&self, message: &mut crate::TimelineMessageRecord) {
        if let Ok(bytes) = hex::decode(&message.group_id_hex) {
            self.enrich_sender_device_label(
                &GroupId::new(bytes),
                &message.sender,
                &mut message.sender_device,
            );
        } else if let crate::SenderDevice::Exact { client_label, .. } = &mut message.sender_device {
            *client_label = None;
        }
    }

    /// Cache a validated invitation selection or this installation's publication.
    pub(crate) fn remember_device_client_label(
        &self,
        fetched: &crate::FetchedKeyPackage,
    ) -> Result<bool, AppError> {
        let Some(label) = fetched
            .client_label
            .as_deref()
            .and_then(crate::key_package_records::sanitize_client_label)
        else {
            return Ok(false);
        };
        let metadata = cgka_engine::key_package::key_package_metadata(&fetched.key_package)
            .map_err(|_| AppError::InvalidKeyPackageEvent("invalid advisory package".into()))?;
        if metadata.credential_identity_hex != fetched.account_id_hex {
            return Ok(false);
        }
        self.admit_device_client_label(
            &fetched.account_id_hex,
            &metadata.signature_key,
            &label,
            fetched.created_at,
        )
    }

    pub(crate) fn admit_device_client_label(
        &self,
        account: &str,
        key: &[u8],
        label: &str,
        created_at: u64,
    ) -> Result<bool, AppError> {
        // Storage reports label changes atomically with its bounded eviction.
        // No resolver, roster or window reads run under this admission guard.
        let changed = {
            let _guard = self
                .device_label_events
                .admission
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            self.shared_storage()?
                .put_device_client_label(account, key, label, created_at)?
        };
        if changed {
            self.device_label_events.publish_change(account);
        }
        Ok(changed)
    }

    async fn fetch_device_client_labels(
        &self,
        account: &str,
    ) -> Result<Vec<FetchedLabel>, AppError> {
        let mut routes = self.account_relay_list_status_for_account_id(account)?;
        if routes.nip65.relays.is_empty() {
            routes = self
                .fetch_account_relay_list_status_for_account_id(
                    account,
                    self.directory_source_relays(&[]),
                )
                .await?;
        }
        let mut endpoints = self.retain_safe_discovered_endpoints(
            routes
                .nip65
                .relays
                .into_iter()
                .map(TransportEndpoint)
                .collect(),
            "device label enrichment",
        );
        if endpoints.is_empty() {
            endpoints = self.directory_source_relays(&[]);
        }
        if endpoints.is_empty() {
            return Ok(Vec::new());
        }
        let mut records = self
            .fetch_key_package_events_for_account_id_with_limit(
                account,
                &endpoints,
                DEVICE_LABEL_EVENT_LIMIT,
            )
            .await?;
        crate::sort_directory_records(&mut records);
        records.reverse();
        let mut labels = Vec::new();
        // Also cap locally: an injected/misbehaving fetcher cannot enlarge admission.
        for record in records.into_iter().take(DEVICE_LABEL_EVENT_LIMIT) {
            if record.event.kind != crate::KIND_MARMOT_KEY_PACKAGE
                || !self.directory_freshness().accepts(&record)
                || record.event.to_verified_nostr_event().is_err()
            {
                continue;
            }
            let Ok((fetched, metadata)) =
                crate::key_package_records::key_package_from_record_with_metadata(
                    &record,
                    |package| {
                        cgka_engine::key_package::key_package_metadata(package).map_err(|_| {
                            AppError::InvalidKeyPackageEvent("invalid advisory package".into())
                        })
                    },
                )
            else {
                continue;
            };
            if fetched.account_id_hex != account || metadata.credential_identity_hex != account {
                continue;
            }
            if let Some(label) = fetched.client_label {
                labels.push(FetchedLabel {
                    account: account.to_owned(),
                    key: metadata.signature_key,
                    label,
                    created_at: fetched.created_at,
                });
            }
        }
        Ok(labels)
    }

    pub(crate) async fn run_device_label_pass(self, accounts: Vec<String>) -> Vec<FetchedLabel> {
        let mut labels = Vec::new();
        for account in accounts {
            if let Ok(Ok(fetched)) = tokio::time::timeout(
                DEVICE_LABEL_FETCH_TIMEOUT,
                self.fetch_device_client_labels(&account),
            )
            .await
            {
                labels.extend(fetched);
            }
        }
        labels
    }
}

impl AppClient {
    /// Admit only demanded leaves and publish bounded, read-time display changes.
    pub(crate) fn finish_device_label_pass(&mut self, labels: Vec<FetchedLabel>) {
        let demand = self.device_label_enrichment.finish();
        let mut failed_admissions = 0usize;
        for label in labels
            .into_iter()
            .take(DEVICE_LABEL_MAX_ACCOUNTS_PER_PASS * DEVICE_LABEL_EVENT_LIMIT)
        {
            let Some(groups) = demand.get(&label.account) else {
                continue;
            };
            let relevant = groups.iter().any(|(group, ids)| {
                ids.contains(&cgka_traits::message::member_device_id(group, &label.key))
            });
            if !relevant {
                continue;
            }
            let Some(sanitized) = crate::key_package_records::sanitize_client_label(&label.label)
            else {
                continue;
            };
            if self
                .app
                .admit_device_client_label(&label.account, &label.key, &sanitized, label.created_at)
                .is_err()
            {
                failed_admissions += 1;
            }
        }

        if failed_admissions > 0 {
            tracing::debug!(target: "marmot_app::device_labels", method = "finish_device_label_pass", failed_admissions, "optional label admission unavailable");
        }
    }

    /// Re-read bounded open viewports, retaining failed invalidation for retry.
    pub(crate) fn refresh_device_label_windows(&mut self, account: Option<&str>) -> bool {
        let views = crate::runtime::conversation_label_viewports(self);
        let mut complete = true;
        for (observer, rows) in views {
            if account.is_some_and(|account| !rows.iter().any(|row| row.sender == account)) {
                continue;
            }
            let Some(view) = observer.upgrade() else {
                continue;
            };
            complete &= crate::runtime::publish_conversation_captures_from_session(
                &[observer],
                self.runtime.session(),
                &self.app,
                &self.state.label,
                &view.group,
            );
        }
        complete
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn device_labels_worker_teardown_releases_demand() {
        let events = DeviceLabelEvents::default();
        let worker = DeviceLabelEnrichment::default();
        let demand = Arc::downgrade(&worker.demand);
        let receiver = events.register_demand("owner", &worker);
        drop(worker);
        drop(receiver);
        assert!(
            demand.upgrade().is_none(),
            "logged out worker remains retained"
        );
        assert_eq!(events.demand_receiver_count(), 0);
        for _ in 0..1000 {
            events.publish_change("other");
        }
        assert_eq!(events.demand_receiver_count(), 0);
    }
    // Admission is synchronous: a Tokio timeout on the calling task cannot
    // interrupt a mutex deadlock. Keep it on a detached OS thread and bound
    // both admission and delivery from the test thread.
    fn admission_after_executor_destruction(earlier_publication: bool, async_admission: bool) {
        let labels = DeviceLabelEvents::default();
        let (events, mut receiver) = tokio::sync::broadcast::channel(2);
        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        executor.block_on(async {
            labels.attach(events);
            if earlier_publication {
                labels.publish_change("a");
                tokio::time::timeout(Duration::from_secs(2), receiver.recv())
                    .await
                    .expect("earlier publication must complete")
                    .unwrap();
            }
        });
        drop(executor);
        let (done, completion) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let survivor = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            if !async_admission {
                labels.publish_change("b");
                done.send(()).unwrap();
            }
            survivor.block_on(async {
                if async_admission {
                    labels.publish_change("b");
                    done.send(()).unwrap();
                }
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(2), receiver.recv())
                        .await
                        .expect("stale executor must not lose the admitted change")
                        .unwrap(),
                    crate::MarmotAppEvent::DeviceLabelsChanged {
                        account_id_hex: Some("b".into())
                    }
                );
                done.send(()).unwrap();
            });
        });
        completion
            .recv_timeout(Duration::from_secs(3))
            .expect("admission deadlocked on a destroyed executor");
        completion
            .recv_timeout(Duration::from_secs(3))
            .expect("publication did not finish on the surviving executor");
    }

    #[test]
    fn device_labels_round7_admission_after_unused_executor_destruction() {
        admission_after_executor_destruction(false, true);
    }

    #[test]
    fn device_labels_round7_admission_after_completed_executor_destruction() {
        admission_after_executor_destruction(true, true);
    }

    #[test]
    fn device_labels_round7_synchronous_admission_after_executor_destruction() {
        admission_after_executor_destruction(false, false);
    }

    #[tokio::test(start_paused = true)]
    async fn device_labels_round7_stop_fences_cancelled_publisher() {
        let labels = DeviceLabelEvents::default();
        let (events, mut receiver) = tokio::sync::broadcast::channel(2);
        labels.attach(events);
        labels.publish_change("a");
        labels.stop();
        tokio::task::yield_now().await;
        tokio::time::advance(DEVICE_LABEL_CHANGE_COALESCE_INTERVAL).await;
        assert!(receiver.try_recv().is_err());
        assert!(labels.state.lock().unwrap().pending.is_none());
        assert!(labels.state.lock().unwrap().task.is_none());
    }

    #[test]
    fn device_labels_round6_cancelled_executor_allows_later_publication() {
        let labels = DeviceLabelEvents::default();
        let (events, mut receiver) = tokio::sync::broadcast::channel(2);
        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        executor.block_on(async {
            labels.attach(events);
            labels.publish_change("a");
            tokio::task::yield_now().await;
        });
        drop(executor);
        let survivor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        survivor.block_on(async {
            tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                .await
                .expect("cancelled publisher must deliver its pending change")
                .unwrap();
            labels.publish_change("b");
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                    .await
                    .expect("cancelled publisher must not strand later admissions")
                    .unwrap(),
                crate::MarmotAppEvent::DeviceLabelsChanged {
                    account_id_hex: Some("b".into())
                }
            );
        });
    }
    #[test]
    fn device_labels_managed_executor_survives_foreign_admission_executor() {
        let labels = DeviceLabelEvents::default();
        let (events, mut receiver) = tokio::sync::broadcast::channel(2);
        let managed = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        managed.block_on(async {
            labels.attach(events);
        });
        let foreign = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        foreign.block_on(async {
            labels.publish_change("a");
        });
        drop(foreign);
        managed.block_on(async {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                    .await
                    .unwrap()
                    .unwrap(),
                crate::MarmotAppEvent::DeviceLabelsChanged {
                    account_id_hex: Some("a".into())
                }
            );
        });
    }

    #[test]
    fn device_labels_unpolled_cancelled_publisher_clears_pending() {
        let labels = DeviceLabelEvents::default();
        let (events, mut receiver) = tokio::sync::broadcast::channel(2);
        let managed = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        managed.block_on(async {
            labels.attach(events);
            labels.publish_change("a");
        });
        drop(managed);
        let survivor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        survivor.block_on(async {
            tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                .await
                .expect("unpolled cancelled publisher must deliver its pending change")
                .unwrap();
            assert!(labels.state.lock().unwrap().pending.is_none());
            labels.publish_change("b");
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                    .await
                    .unwrap()
                    .unwrap(),
                crate::MarmotAppEvent::DeviceLabelsChanged {
                    account_id_hex: Some("b".into())
                }
            );
        });
        labels.stop();
        assert!(labels.state.lock().unwrap().pending.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn device_labels_events_coalesce_and_bound_unknown_scope() {
        let labels = DeviceLabelEvents::default();
        let (events, mut receiver) = tokio::sync::broadcast::channel(2);
        labels.attach(events);
        for _ in 0..1000 {
            labels.publish_change("a");
        }
        tokio::task::yield_now().await;
        tokio::time::advance(DEVICE_LABEL_CHANGE_COALESCE_INTERVAL).await;
        assert_eq!(
            receiver.recv().await.unwrap(),
            crate::MarmotAppEvent::DeviceLabelsChanged {
                account_id_hex: Some("a".into())
            }
        );
        assert!(receiver.try_recv().is_err());
        labels.publish_change("a");
        labels.publish_change("b");
        tokio::task::yield_now().await;
        tokio::time::advance(DEVICE_LABEL_CHANGE_COALESCE_INTERVAL).await;
        assert_eq!(
            receiver.recv().await.unwrap(),
            crate::MarmotAppEvent::DeviceLabelsChanged {
                account_id_hex: None
            }
        );
        assert!(receiver.try_recv().is_err());
    }
    #[tokio::test(start_paused = true)]
    async fn device_label_demand_coalesces_bounds_and_rate_limits_misses() {
        let state = DeviceLabelEnrichment::default();
        let group = GroupId::new([1]);
        for _ in 0..50 {
            state.schedule("account", &group, "device");
        }
        assert_eq!(state.begin(), vec!["account"]);
        state.schedule("account", &GroupId::new([2]), "device");
        assert!(state.begin().is_empty());
        // A slow pass must not consume part of the minimum re-fetch cooldown.
        tokio::time::advance(Duration::from_secs(30)).await;
        assert_eq!(state.finish()["account"].len(), 2);
        state.schedule("account", &group, "device");
        assert!(state.begin().is_empty(), "misses have cooldown too");
        tokio::time::advance(DEVICE_LABEL_REFETCH_INTERVAL - Duration::from_secs(1)).await;
        state.schedule("account", &group, "device");
        assert!(state.begin().is_empty());
        tokio::time::advance(Duration::from_secs(1)).await;
        state.schedule("account", &group, "device");
        assert_eq!(state.begin(), vec!["account"]);
        state.finish();
        for i in 0..DEVICE_LABEL_MAX_TRACKED_ACCOUNTS * 2 {
            state.schedule(&format!("remote-{i}"), &group, "device");
        }
        assert!(state.demand.lock().unwrap().groups.len() <= DEVICE_LABEL_MAX_TRACKED_ACCOUNTS);
        assert_eq!(state.begin().len(), DEVICE_LABEL_MAX_ACCOUNTS_PER_PASS);
        state.finish();
        assert_eq!(state.begin().len(), DEVICE_LABEL_MAX_ACCOUNTS_PER_PASS);
    }
}
