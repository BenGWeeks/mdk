//! Validated public KeyPackage display labels, retained across slot replacement.
use super::{SharedSqliteResultExt, SqliteSharedStorage};
use cgka_traits::storage::StorageResult;
use rusqlite::{params, params_from_iter};

pub struct DeviceClientLabel {
    pub account_id_hex: String,
    pub signature_key: Vec<u8>,
    pub client_label: String,
    pub created_at: u64,
}

impl SqliteSharedStorage {
    /// Only the app's validated KeyPackage parser may supply these advisory facts.
    pub fn put_device_client_label(
        &self,
        account: &str,
        key: &[u8],
        label: &str,
        created_at: u64,
    ) -> StorageResult<bool> {
        Ok(self
            .lock()?
            .execute(
                "INSERT INTO device_client_labels VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(account_id_hex, signature_key) DO UPDATE SET
             client_label=excluded.client_label, created_at=excluded.created_at
             WHERE excluded.created_at >= device_client_labels.created_at
               AND (excluded.created_at > device_client_labels.created_at
                    OR excluded.client_label != device_client_labels.client_label)",
                params![account, key, label, crate::u64_to_i64(created_at)?],
            )
            .storage()?
            != 0)
    }

    /// Account-scoped batches avoid a full installation cache scan on roster reads.
    pub fn device_client_labels(
        &self,
        accounts: &[String],
    ) -> StorageResult<Vec<DeviceClientLabel>> {
        let conn = self.lock()?;
        let mut labels = Vec::new();
        for batch in accounts.chunks(crate::SQLITE_BIND_PARAMETER_CHUNK) {
            let placeholders = vec!["?"; batch.len()].join(",");
            let mut stmt = conn.prepare_cached(&format!(
                "SELECT account_id_hex, signature_key, client_label, created_at FROM device_client_labels WHERE account_id_hex IN ({placeholders})"
            )).storage()?;
            let rows = stmt
                .query_map(params_from_iter(batch), |row| {
                    let created_at: i64 = row.get(3)?;
                    Ok(DeviceClientLabel {
                        account_id_hex: row.get(0)?,
                        signature_key: row.get(1)?,
                        client_label: row.get(2)?,
                        created_at: u64::try_from(created_at)
                            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(3, created_at))?,
                    })
                })
                .storage()?;
            for row in rows {
                labels.push(row.storage()?);
            }
        }
        Ok(labels)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn device_labels_retain_siblings_and_reject_older_labels_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shared.sqlite3");
        let storage = SqliteSharedStorage::open(&path).unwrap();
        assert!(
            storage
                .put_device_client_label("account", b"first", "Piggy", 10)
                .unwrap()
        );
        assert!(
            storage
                .put_device_client_label("account", b"second", "Noise", 11)
                .unwrap()
        );
        assert!(
            !storage
                .put_device_client_label("account", b"first", "Old", 9)
                .unwrap()
        );
        assert!(
            !storage
                .put_device_client_label("account", b"first", "Piggy", 10)
                .unwrap()
        );
        assert!(
            storage
                .put_device_client_label("account", b"first", "Piggy", 20)
                .unwrap()
        );
        assert!(
            !storage
                .put_device_client_label("account", b"first", "Stale", 15)
                .unwrap()
        );
        storage.close().unwrap();
        let storage = SqliteSharedStorage::open(path).unwrap();
        let labels = storage.device_client_labels(&["account".into()]).unwrap();
        assert_eq!(labels.len(), 2);
        assert!(
            labels
                .iter()
                .any(|row| row.signature_key == b"first" && row.client_label == "Piggy")
        );
        assert!(
            storage
                .device_client_labels(&["other".into()])
                .unwrap()
                .is_empty()
        );
    }
}
