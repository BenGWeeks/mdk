//! Label-only updates preserve authenticated attribution and source/retention facts.
use super::*;

impl SqliteAccountStorage {
    /// Refresh only Exact devices already attributed to this account and leaf.
    /// Both raw app rows and their timeline projections change in one transaction.
    pub fn enrich_sender_device_label(
        &self,
        group: &str,
        account: &str,
        device_id: &str,
        label: &str,
    ) -> StorageResult<Option<TimelineProjectionUpdate>> {
        self.connection.with_transaction(|| {
            let conn = self.lock()?;
            let mut stmt = conn.prepare_cached(
                "SELECT message_id_hex, sender_device FROM app_events
                 WHERE group_id_hex=?1 AND sender=?2 AND sender_device IS NOT NULL"
            ).storage()?;
            let rows = stmt.query_map(params![group, account], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))).storage()?;
            let mut changed = Vec::new();
            for row in rows {
                let (id, json) = row.storage()?;
                let mut device: cgka_traits::message::SenderDevice = serde_json::from_str(&json).map_err(|_| StorageError::Serialization("invalid sender device".into()))?;
                if let cgka_traits::message::SenderDevice::Exact { device_id: existing, client_label, .. } = &mut device
                    && existing == device_id && client_label.as_deref() != Some(label) {
                    *client_label = Some(label.to_owned());
                    changed.push((id, sender_device_json(&device)?));
                }
            }
            drop(stmt);
            for (id, json) in &changed {
                conn.execute_cached("UPDATE app_events SET sender_device=?3 WHERE group_id_hex=?1 AND message_id_hex=?2", params![group, id, json]).storage()?;
                // Label-only refresh: do not rebuild content, reactions or delivery state.
                conn.execute_cached("UPDATE message_timeline SET sender_device=?3 WHERE group_id_hex=?1 AND message_id_hex=?2", params![group, id, json]).storage()?;
            }
            if changed.is_empty() { return Ok(None); }
            let messages = timeline_records_by_ids_tx(&conn, group, changed.into_iter().map(|(id, _)| id).collect())?;
            let changes = messages.iter().cloned().map(|message| TimelineMessageChange::Upsert {
                trigger: TimelineUpdateTrigger::MessageEditedOrReprojected, message: Box::new(message),
            }).collect();
            Ok(Some(TimelineProjectionUpdate { group_id_hex: group.to_owned(), messages, changes }))
        })
    }
}
