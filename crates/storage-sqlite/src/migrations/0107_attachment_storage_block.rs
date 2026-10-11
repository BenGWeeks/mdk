//! Durable "not enough free disk to publish" outcome for attachment acquisition.
use cgka_traits::storage::StorageResult;
use rusqlite::Transaction;

use crate::SqliteResultExt;

pub(crate) fn apply(tx: &Transaction<'_>) -> StorageResult<()> {
    // Plaintext bytes whose publication reserve the device could not cover when
    // a verified or size-known transfer stopped. NULL means the job is not
    // parked for disk space. Existing jobs never start parked.
    //
    // A storage-parked job keeps its ciphertext checkpoint: the blocked state
    // is the same terminal-looking row as a failure, but the next admitted
    // attempt is meant to resume (ideally without any request), so the
    // terminal cleanup trigger must not discard the prefix for it.
    tx.execute_batch(
        "ALTER TABLE attachment_acquisition ADD COLUMN storage_blocked_bytes INTEGER
             CHECK(storage_blocked_bytes IS NULL OR storage_blocked_bytes>=0);
        DROP TRIGGER attachment_partial_terminal;
        CREATE TRIGGER attachment_partial_terminal AFTER UPDATE OF state ON attachment_acquisition
        WHEN NEW.state=3 OR (NEW.state=5 AND NEW.permission_paused=0)
            OR (NEW.state=4 AND NEW.cancelled=0 AND NEW.storage_blocked_bytes IS NULL) BEGIN
            DELETE FROM attachment_partial WHERE token=NEW.token;
        END;",
    )
    .storage()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::{MIGRATIONS, run};

    fn partials(conn: &rusqlite::Connection) -> i64 {
        conn.query_row("SELECT count(*) FROM attachment_partial", [], |r| r.get(0))
            .unwrap()
    }

    #[test]
    fn storage_block_migration_preserves_jobs_and_is_transactional() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        run(&mut conn, &MIGRATIONS[..106]).unwrap();
        conn.execute_batch("PRAGMA foreign_keys=OFF;
            INSERT INTO attachment_acquisition(token,group_id_hex,message_id_hex,attachment_index,source_message_id_hex,source_epoch,slot_json,plaintext_digest,state,attempts,due)
            VALUES(x'01010101010101010101010101010101','g','m',0,'s',1,'[\"imeta\"]',randomblob(32),2,7,1234);
            INSERT INTO attachment_partial(token,ciphertext_digest,locator_digest,etag,total,received,expires_at)
            VALUES(x'01010101010101010101010101010101',randomblob(32),randomblob(32),'\"v1\"',10,3,99999);").unwrap();
        let tx = conn.transaction().unwrap();
        apply(&tx).unwrap();
        tx.rollback().unwrap();
        assert!(
            conn.prepare("SELECT storage_blocked_bytes FROM attachment_acquisition")
                .is_err()
        );
        run(&mut conn, MIGRATIONS).unwrap();
        run(&mut conn, MIGRATIONS).unwrap();
        let row = conn
            .query_row(
                "SELECT state,attempts,due,storage_blocked_bytes FROM attachment_acquisition",
                [],
                |r| {
                    Ok((
                        r.get::<_, u8>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, Option<i64>>(3)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(row, (2, 7, 1234, None));
        assert!(
            conn.execute(
                "UPDATE attachment_acquisition SET storage_blocked_bytes=-1",
                []
            )
            .is_err(),
            "negative reserve requirements are rejected"
        );
        // Parking for disk space keeps the checkpoint; any other blocked
        // transition still discards it, as before this migration.
        assert_eq!(partials(&conn), 1);
        conn.execute(
            "UPDATE attachment_acquisition SET state=4,due=NULL,storage_blocked_bytes=4096",
            [],
        )
        .unwrap();
        assert_eq!(
            partials(&conn),
            1,
            "a storage-parked job resumes its prefix"
        );
        conn.execute(
            "UPDATE attachment_acquisition SET state=0,due=1,storage_blocked_bytes=NULL",
            [],
        )
        .unwrap();
        assert_eq!(partials(&conn), 1, "readmission keeps the prefix");
        conn.execute("UPDATE attachment_acquisition SET state=4,due=NULL", [])
            .unwrap();
        assert_eq!(partials(&conn), 0, "an ordinary block still cleans up");
    }
}
