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
/// Bound the complete discovery + package fetch wait for each optional account.
const DEVICE_LABEL_FETCH_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Default)]
struct Demand {
    groups: BTreeMap<String, HashSet<GroupId>>,
    attempted: HashMap<String, Instant>,
    active: HashSet<String>,
}

#[derive(Clone, Default)]
pub(crate) struct DeviceLabelEnrichment {
    demand: Arc<Mutex<Demand>>,
    pub(crate) wake: Arc<tokio::sync::Notify>,
}

impl DeviceLabelEnrichment {
    pub(crate) fn schedule(&self, account: &str, group: &GroupId) {
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
        if groups.len() < DEVICE_LABEL_MAX_GROUPS_PER_ACCOUNT {
            groups.insert(group.clone());
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

    fn finish(&self) -> BTreeMap<String, HashSet<GroupId>> {
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
    account: String,
    key: Vec<u8>,
    label: String,
    created_at: u64,
}

impl MarmotApp {
    /// Cache every validated selected package, independent of its replaceable directory slot.
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
    /// Admit advisory cache facts and refresh existing Exact sender rows, then notify hosts.
    pub(crate) fn finish_device_label_pass(&mut self, labels: Vec<FetchedLabel>) -> Vec<GroupId> {
        let groups = self.device_label_enrichment.finish();
        let mut changed_groups = HashSet::new();
        let mut failed_admissions = 0usize;
        for label in labels {
            let result = (|| -> Result<(), AppError> {
                self.app.shared_storage()?.put_device_client_label(
                    &label.account,
                    &label.key,
                    &label.label,
                    label.created_at,
                )?;
                let Some(accepted) = self
                    .app
                    .shared_storage()?
                    .device_client_labels(std::slice::from_ref(&label.account))?
                    .into_iter()
                    .find(|row| row.signature_key == label.key)
                else {
                    return Ok(());
                };
                if let Some(groups) = groups.get(&label.account) {
                    for group in groups {
                        changed_groups.insert(group.clone());
                        let id = cgka_traits::message::member_device_id(group, &label.key);
                        let update = self
                            .app
                            .account_storage(&self.state.label)?
                            .enrich_sender_device_label(
                                &hex::encode(group.as_slice()),
                                &label.account,
                                &id,
                                &accepted.client_label,
                            )?;
                        if let Some(update) = update {
                            self.pending_projection_updates
                                .push(self.app.app_projection_update(&self.state.label, update)?);
                        }
                    }
                }
                Ok(())
            })();
            if result.is_err() {
                failed_admissions += 1;
            }
        }
        if failed_admissions > 0 {
            tracing::debug!(target: "marmot_app::device_labels", method = "finish_device_label_pass", failed_admissions, "optional label admission unavailable");
        }
        changed_groups.into_iter().collect()
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
            state.schedule("account", &group);
        }
        assert_eq!(state.begin(), vec!["account"]);
        state.schedule("account", &GroupId::new([2]));
        assert!(state.begin().is_empty());
        // A slow pass must not consume part of the minimum re-fetch cooldown.
        tokio::time::advance(Duration::from_secs(30)).await;
        assert_eq!(state.finish()["account"].len(), 2);
        state.schedule("account", &group);
        assert!(state.begin().is_empty(), "misses have cooldown too");
        tokio::time::advance(DEVICE_LABEL_REFETCH_INTERVAL - Duration::from_secs(1)).await;
        state.schedule("account", &group);
        assert!(state.begin().is_empty());
        tokio::time::advance(Duration::from_secs(1)).await;
        state.schedule("account", &group);
        assert_eq!(state.begin(), vec!["account"]);
        state.finish();
        for i in 0..DEVICE_LABEL_MAX_TRACKED_ACCOUNTS * 2 {
            state.schedule(&format!("remote-{i}"), &group);
        }
        assert!(state.demand.lock().unwrap().groups.len() <= DEVICE_LABEL_MAX_TRACKED_ACCOUNTS);
        assert_eq!(state.begin().len(), DEVICE_LABEL_MAX_ACCOUNTS_PER_PASS);
        state.finish();
        assert_eq!(state.begin().len(), DEVICE_LABEL_MAX_ACCOUNTS_PER_PASS);
    }
}
