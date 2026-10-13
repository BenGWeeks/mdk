//! Optional label discovery. Projections only enqueue local demand; workers own I/O.
use super::AppClient;
use crate::{AppError, MarmotApp};
use cgka_traits::{GroupId, TransportEndpoint};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
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
/// Completion only compares the recent viewport, never retained history.
const DEVICE_LABEL_RECENT_SENDERS: usize = 64;

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
        self.shared_storage()
            .ok()
            .and_then(|storage| storage.device_client_labels(&[sender.to_owned()]).ok())
            .and_then(|labels| {
                labels.into_iter().find_map(|row| {
                    (cgka_traits::message::member_device_id(group, &row.signature_key) == device_id)
                        .then(|| {
                            crate::key_package_records::sanitize_client_label(&row.client_label)
                        })
                        .flatten()
                })
            })
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
        Ok(self.shared_storage()?.put_device_client_label(
            &fetched.account_id_hex,
            &metadata.signature_key,
            &label,
            fetched.created_at,
        )?)
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
    pub(crate) fn finish_device_label_pass(&mut self, labels: Vec<FetchedLabel>) -> Vec<GroupId> {
        let demand = self.device_label_enrichment.finish();
        let groups = demand
            .values()
            .flat_map(|groups| groups.keys().cloned())
            .collect::<HashSet<_>>();
        let mut before = HashMap::new();
        for group in groups {
            let roster = self.members(&group).unwrap_or_default();
            let messages = self
                .app
                .timeline_messages_with_query(
                    &self.state.label,
                    crate::TimelineMessageQuery {
                        group_id_hex: Some(hex::encode(group.as_slice())),
                        pagination: crate::TimelinePagination {
                            limit: Some(DEVICE_LABEL_RECENT_SENDERS),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                )
                .map(|page| page.messages)
                .unwrap_or_default();
            before.insert(group, (roster, messages));
        }
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
                .shared_storage()
                .and_then(|storage| {
                    storage
                        .put_device_client_label(
                            &label.account,
                            &label.key,
                            &sanitized,
                            label.created_at,
                        )
                        .map_err(AppError::from)
                })
                .is_err()
            {
                failed_admissions += 1;
            }
        }
        let mut changed_groups = Vec::new();
        for (group, (roster, messages)) in before {
            let roster_changed = self.members(&group).is_ok_and(|after| after != roster);
            let mut changed_messages = Vec::new();
            for mut message in messages {
                let before = message.sender_device.clone();
                self.app.enrich_timeline_device_label(&mut message);
                if before != message.sender_device {
                    changed_messages.push(message);
                }
            }
            if roster_changed || !changed_messages.is_empty() {
                changed_groups.push(group.clone());
                crate::runtime::publish_conversation_captures_from_session(
                    &self.conversation_captures,
                    self.runtime.session(),
                    &self.app,
                    &self.state.label,
                    &group,
                );
            }
            if !changed_messages.is_empty() {
                let changes = changed_messages
                    .iter()
                    .cloned()
                    .map(|message| crate::TimelineMessageChange::Upsert {
                        trigger: crate::TimelineUpdateTrigger::MessageEditedOrReprojected,
                        message: Box::new(message),
                    })
                    .collect();
                self.pending_projection_updates
                    .push(crate::AppProjectionUpdate {
                        group_id_hex: hex::encode(group.as_slice()),
                        timeline_messages: changed_messages,
                        timeline_changes: changes,
                        chat_list_row: None,
                        chat_list_trigger: crate::ChatListUpdateTrigger::SnapshotRefresh,
                    });
            }
        }
        if failed_admissions > 0 {
            tracing::debug!(target: "marmot_app::device_labels", method = "finish_device_label_pass", failed_admissions, "optional label admission unavailable");
        }
        changed_groups.sort_by(|a, b| a.as_slice().cmp(b.as_slice()));
        changed_groups
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
