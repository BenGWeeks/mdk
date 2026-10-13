//! Validated public KeyPackage display labels, retained across slot replacement.
use super::{SharedSqliteResultExt, SqliteSharedStorage};
use cgka_traits::storage::StorageResult;
use rusqlite::{OptionalExtension, params, params_from_iter};

/// Maximum retained leaf labels per remote account, including invitation selections.
pub const DEVICE_CLIENT_LABELS_PER_ACCOUNT: usize = 64;

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
        let mut conn = self.lock()?;
        let tx = conn.transaction().storage()?;
        let before: Option<(String, i64)> = tx.query_row(
            "SELECT client_label, created_at FROM device_client_labels WHERE account_id_hex=?1 AND signature_key=?2",
            params![account, key], |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional().storage()?;
        let created_at = crate::u64_to_i64(created_at)?;
        if before.as_ref().is_some_and(|(_, at)| *at > created_at) {
            return Ok(false);
        }
        let changed = before
            .as_ref()
            .is_none_or(|(previous, _)| previous != label);
        tx.execute(
            "INSERT INTO device_client_labels (account_id_hex, signature_key, client_label, created_at, confirmed_order)
             VALUES (?1, ?2, ?3, ?4, (SELECT COALESCE(MAX(confirmed_order), 0)+1 FROM device_client_labels WHERE account_id_hex=?1))
             ON CONFLICT(account_id_hex, signature_key) DO UPDATE SET
             client_label=excluded.client_label, created_at=excluded.created_at, confirmed_order=excluded.confirmed_order",
            params![account, key, label, created_at],
        ).storage()?;
        tx.execute(
            "DELETE FROM device_client_labels WHERE account_id_hex=?1 AND signature_key IN
             (SELECT signature_key FROM device_client_labels WHERE account_id_hex=?1
              ORDER BY confirmed_order DESC, signature_key DESC LIMIT -1 OFFSET ?2)",
            params![account, DEVICE_CLIENT_LABELS_PER_ACCOUNT as i64],
        )
        .storage()?;
        tx.commit().storage()?;
        Ok(changed)
    }

    /// Exact leaf lookup avoids loading sibling labels when the signature key is known.
    pub fn device_client_label(
        &self,
        account: &str,
        key: &[u8],
    ) -> StorageResult<Option<DeviceClientLabel>> {
        self.lock()?.query_row(
            "SELECT account_id_hex, signature_key, client_label, created_at FROM device_client_labels WHERE account_id_hex=?1 AND signature_key=?2",
            params![account, key], read_label,
        ).optional().storage()
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
                .query_map(params_from_iter(batch), read_label)
                .storage()?;
            for row in rows {
                labels.push(row.storage()?);
            }
        }
        Ok(labels)
    }
}

fn read_label(row: &rusqlite::Row<'_>) -> rusqlite::Result<DeviceClientLabel> {
    let created_at: i64 = row.get(3)?;
    Ok(DeviceClientLabel {
        account_id_hex: row.get(0)?,
        signature_key: row.get(1)?,
        client_label: row.get(2)?,
        created_at: u64::try_from(created_at)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(3, created_at))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn device_labels_cache_is_bounded_and_confirmation_evicts_deterministically() {
        let storage = SqliteSharedStorage::in_memory().unwrap();
        for i in 0u8..64 {
            storage
                .put_device_client_label("a", &[i], "label", 10)
                .unwrap();
        }
        storage
            .put_device_client_label("a", &[0], "label", 10)
            .unwrap();
        storage
            .put_device_client_label("a", &[64], "label", 11)
            .unwrap();
        let labels = storage.device_client_labels(&["a".into()]).unwrap();
        assert_eq!(labels.len(), 64);
        assert!(labels.iter().any(|row| row.signature_key == [0]));
        assert!(!labels.iter().any(|row| row.signature_key == [1]));
        assert_eq!(
            storage
                .device_client_label("a", &[0])
                .unwrap()
                .unwrap()
                .client_label,
            "label"
        );
        assert!(storage.device_client_label("a", &[1]).unwrap().is_none());
    }

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
            !storage
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
