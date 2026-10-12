//! Monotonic intent membership and indexed, bounded management reads.
use crate::SqliteResultExt;
use cgka_traits::storage::StorageResult;
use rusqlite::Transaction;
pub(crate) fn apply(tx: &Transaction<'_>) -> StorageResult<()> {
    tx.execute_batch("ALTER TABLE attachment_acquisition ADD COLUMN management_sequence INTEGER NOT NULL DEFAULT 0 CHECK(management_sequence>=0);
        UPDATE attachment_acquisition SET management_sequence=rowid;
        ALTER TABLE attachment_acquisition ADD COLUMN management_ready INTEGER NOT NULL DEFAULT 0 CHECK(management_ready IN(0,1));
        UPDATE attachment_acquisition SET management_ready=(state=3 AND EXISTS(SELECT 1 FROM retained_attachment_bytes b WHERE b.token=attachment_acquisition.token));
        CREATE TABLE attachment_management_sequence(id INTEGER PRIMARY KEY CHECK(id=1),value INTEGER NOT NULL CHECK(typeof(value)='integer' AND value>=0));
        INSERT INTO attachment_management_sequence SELECT 1,coalesce(max(management_sequence),0) FROM attachment_acquisition;
        CREATE UNIQUE INDEX attachment_management_sequence_index ON attachment_acquisition(management_sequence);
        CREATE INDEX attachment_management_group_sequence ON attachment_acquisition(group_id_hex,management_sequence);
        CREATE INDEX attachment_management_work ON attachment_acquisition(management_sequence) WHERE management_ready=0;
        CREATE INDEX attachment_management_group_work ON attachment_acquisition(group_id_hex,management_sequence) WHERE management_ready=0;
        CREATE TRIGGER attachment_management_state_changed AFTER UPDATE OF state ON attachment_acquisition WHEN OLD.state<>NEW.state BEGIN
            UPDATE attachment_acquisition SET management_ready=(NEW.state=3 AND EXISTS(SELECT 1 FROM retained_attachment_bytes b WHERE b.token=NEW.token)) WHERE token=NEW.token;
        END;
        CREATE TRIGGER attachment_management_bytes_added AFTER INSERT ON retained_attachment_bytes BEGIN
            UPDATE attachment_acquisition SET management_ready=(state=3) WHERE token=NEW.token;
        END;
        CREATE TRIGGER attachment_management_bytes_removed AFTER DELETE ON retained_attachment_bytes BEGIN
            UPDATE attachment_acquisition SET management_ready=0 WHERE token=OLD.token;
        END;
        CREATE INDEX attachment_management_group_notices ON account_recovery_obligations(group_id,parked_at_ms,id) WHERE state=0 AND eligibility=4;
        CREATE TRIGGER attachment_management_inserted AFTER INSERT ON attachment_acquisition BEGIN
            UPDATE attachment_management_sequence SET value=value+1 WHERE id=1;
            UPDATE attachment_acquisition SET management_sequence=(SELECT value FROM attachment_management_sequence WHERE id=1),management_ready=(state=3 AND EXISTS(SELECT 1 FROM retained_attachment_bytes b WHERE b.token=NEW.token)) WHERE token=NEW.token;
        END;

") .storage()
}
#[cfg(test)]
mod tests {
    #[test]
    fn populated_v108_upgrade_seeds_unique_intents_and_indexes_work() {
        let mut c = rusqlite::Connection::open_in_memory().unwrap();
        crate::migrations::run(&mut c, &crate::migrations::MIGRATIONS[..108]).unwrap();
        c.execute_batch("PRAGMA foreign_keys=OFF; INSERT INTO attachment_acquisition(token,group_id_hex,message_id_hex,attachment_index,source_message_id_hex,source_epoch,slot_json,plaintext_digest,state,attempts,due,expires_at) VALUES(randomblob(16),'aa','one',0,'one',0,'[]',randomblob(32),0,0,0,NULL);").unwrap();
        assert_eq!(crate::migrations::run_all(&mut c).unwrap(), 1);
        assert_eq!(crate::migrations::run_all(&mut c).unwrap(), 0);
        let a: i64 = c
            .query_row(
                "SELECT management_sequence FROM attachment_acquisition",
                [],
                |r| r.get(0),
            )
            .unwrap();
        c.execute_batch("UPDATE attachment_acquisition SET explicit_request=1;")
            .unwrap();
        let b: i64 = c
            .query_row(
                "SELECT management_sequence FROM attachment_acquisition",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(b, a, "intent advancement belongs to the storage commands");
    }
}
