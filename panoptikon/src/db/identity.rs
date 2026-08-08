//! Per-database identity UUIDs (`database_identity`, one row per database).
//!
//! Every index and user_data database is stamped with a UUID by a migration
//! (`…_database_identity.sql`). It is a server-side matching key only: it
//! never appears in API responses, URLs or the UI — names remain the
//! human-facing handle. The pinboard↔database association reads the index
//! database's UUID as its primary match key.
//!
//! Both helpers here are deliberately *tolerant*: a database that predates
//! the migration has no identity to offer and simply claims nothing. The
//! probe additionally distinguishes that from "could not look" — see
//! [`DbIdentityProbe`], whose whole point is that the two must not be
//! conflated by the caller.

use std::path::Path;

use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};

/// Schema-qualified on purpose: a pinboard connection has user_data
/// ATTACHed, and user_data carries a `database_identity` table of its own.
/// Unqualified, this would still resolve to `main` — but only by SQLite's
/// search order, which is not a thing to rely on for an identity read.
const READ_INDEX_UUID_SQL: &str = "SELECT uuid FROM main.database_identity WHERE id = 1";

/// Whether the identity table exists in the probed database itself. Asked
/// separately from the identity read so that "this database predates the
/// migration" (a plain answer) is never confused with "the query failed"
/// (no answer at all).
const HAS_IDENTITY_TABLE_SQL: &str =
    "SELECT 1 FROM main.sqlite_master WHERE type = 'table' AND name = 'database_identity'";

/// Whether `value` has the shape every identity UUID in this system is
/// written in: 32 lowercase hex characters, unhyphenated — what
/// `lower(hex(randomblob(16)))` produces in the migrations and what
/// [`instance_id`](super::instance_id) writes to its file.
pub(crate) fn is_identity_uuid(value: &str) -> bool {
    value.len() == 32 && value.chars().all(|ch| matches!(ch, '0'..='9' | 'a'..='f'))
}

/// The identity UUID of the index database on an already-open connection
/// (its `main` schema). `None` for a database migrated before the identity
/// table existed, whose row somehow went missing, or whose row holds a value
/// that cannot be an identity — a matching key that matches nothing is not
/// worth carrying.
pub(crate) async fn current_index_db_uuid(conn: &mut SqliteConnection) -> Option<String> {
    match sqlx::query_scalar::<_, String>(READ_INDEX_UUID_SQL)
        .fetch_optional(&mut *conn)
        .await
    {
        Ok(Some(uuid)) if is_identity_uuid(&uuid) => Some(uuid),
        Ok(Some(uuid)) => {
            tracing::warn!(uuid, "index database identity row is not a usable UUID");
            None
        }
        Ok(None) => None,
        // A database that predates the identity migration answers here, as
        // "no such table" — a plain fact about an old database, and the only
        // expected failure. Anything else is a real error, and it silently
        // switches off the identity clause of the association rule for this
        // request: the library then comes back nearly empty under the
        // association filter, which must not be an unexplained mystery.
        Err(err) if is_missing_identity_table(&err) => {
            tracing::debug!(error = %err, "index database has no identity table");
            None
        }
        Err(err) => {
            tracing::warn!(
                error = %err,
                "could not read the index database's identity; \
                 pinboard associations cannot match this database by identity"
            );
            None
        }
    }
}

/// Whether a failed identity read is just "this database predates the
/// identity migration": SQLite reports a missing table as an error rather
/// than as an empty result.
fn is_missing_identity_table(err: &sqlx::Error) -> bool {
    matches!(err, sqlx::Error::Database(db) if db.message().contains("no such table"))
}

/// What an identity probe of a database file could establish.
///
/// The distinction is load-bearing for the not-claimed-elsewhere gate of the
/// association rule's clause (b), which fires only when a stamped UUID
/// belongs to **no existing local index database**. A database that could
/// not be read might be holding exactly that UUID, so consumers must treat
/// [`Unknown`](DbIdentityProbe::Unknown) as *possibly claimed* and refuse the
/// name fallback — fail closed. Silently folding it into
/// [`ClaimsNothing`](DbIdentityProbe::ClaimsNothing) would let a momentarily
/// locked database hand its boards to a same-named one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DbIdentityProbe {
    /// The database was read and holds this identity.
    Claimed(String),
    /// The database was read and claims no identity: no identity table (it
    /// predates the migration), no row, or a row that cannot be a UUID. Also
    /// the answer for a path with no file at all — there is no database
    /// there to claim anything.
    ClaimsNothing,
    /// The file exists but could not be interrogated: locked, corrupt,
    /// unreadable, a directory in its place, a read-only filesystem that
    /// refuses the `-shm` file. Nothing may be concluded from it.
    Unknown,
}

/// The identity UUID of an arbitrary index database *file*, read through a
/// throwaway read-only connection.
///
/// This must never go through the normal open path: `migrate_path` runs
/// pending migrations plus a post-migration ANALYZE, and the association
/// filter probes every local index database it has not seen yet — turning a
/// pinboard listing into a migrate-and-ANALYZE of the whole data folder.
/// Read-only, one row, close.
///
/// Read-only here means "changes no data": opening a WAL database still
/// creates (and leaves behind) its `-shm` sidecar, and may leave a `-wal`.
/// That is accepted and benign — the probe touches the filesystem, it just
/// never migrates, writes rows, or creates a database.
pub(crate) async fn probe_index_db_uuid(index_db_file: &Path) -> DbIdentityProbe {
    match std::fs::metadata(index_db_file) {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return DbIdentityProbe::ClaimsNothing;
        }
        Err(err) => {
            tracing::debug!(
                path = %index_db_file.display(),
                error = %err,
                "could not stat a database file to read its identity"
            );
            return DbIdentityProbe::Unknown;
        }
    }
    // `create_if_missing` defaults to false, so a file that vanished between
    // the stat and here fails to open rather than being created as an empty
    // database.
    let options = SqliteConnectOptions::new()
        .filename(index_db_file)
        .read_only(true);
    let mut conn = match SqliteConnection::connect_with(&options).await {
        Ok(conn) => conn,
        Err(err) => {
            tracing::debug!(
                path = %index_db_file.display(),
                error = %err,
                "could not open index database to read its identity"
            );
            return DbIdentityProbe::Unknown;
        }
    };
    let probe = probe_open_connection(&mut conn).await;
    if let Err(err) = conn.close().await {
        tracing::debug!(error = %err, "failed to close identity probe connection");
    }
    probe
}

/// The probe's verdict for an already-open connection: table first, then the
/// row, so a missing table reads as "claims nothing" while every genuine
/// query failure reads as "unknown".
async fn probe_open_connection(conn: &mut SqliteConnection) -> DbIdentityProbe {
    match sqlx::query_scalar::<_, i64>(HAS_IDENTITY_TABLE_SQL)
        .fetch_optional(&mut *conn)
        .await
    {
        Ok(Some(_)) => {}
        Ok(None) => return DbIdentityProbe::ClaimsNothing,
        Err(err) => {
            tracing::debug!(error = %err, "could not inspect a database for its identity table");
            return DbIdentityProbe::Unknown;
        }
    }
    match sqlx::query_scalar::<_, String>(READ_INDEX_UUID_SQL)
        .fetch_optional(&mut *conn)
        .await
    {
        Ok(Some(uuid)) if is_identity_uuid(&uuid) => DbIdentityProbe::Claimed(uuid),
        Ok(Some(uuid)) => {
            tracing::warn!(uuid, "probed database identity row is not a usable UUID");
            DbIdentityProbe::ClaimsNothing
        }
        Ok(None) => DbIdentityProbe::ClaimsNothing,
        Err(err) => {
            tracing::debug!(error = %err, "could not read a probed database's identity row");
            DbIdentityProbe::Unknown
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::{migrate_index_db_file, setup_test_databases};

    /// Creates and migrates an index database at `<dir>/<name>/index.db`,
    /// the same layout the data folder uses.
    async fn migrated_index_db(dir: &Path, name: &str) -> std::path::PathBuf {
        let db_dir = dir.join(name);
        std::fs::create_dir_all(&db_dir).unwrap();
        let path = db_dir.join("index.db");
        migrate_index_db_file(&path).await.unwrap();
        path
    }

    fn claimed(probe: DbIdentityProbe) -> String {
        match probe {
            DbIdentityProbe::Claimed(uuid) => uuid,
            other => panic!("expected a claimed identity, got {other:?}"),
        }
    }

    // The UUID is minted once by the migration and never rewritten, so it is
    // stable across opens — and two databases never share one.
    #[tokio::test]
    async fn identity_is_stable_across_opens_and_unique_per_database() {
        let dir = tempfile::tempdir().unwrap();
        let a = migrated_index_db(dir.path(), "alpha").await;
        let b = migrated_index_db(dir.path(), "beta").await;

        let first = claimed(probe_index_db_uuid(&a).await);
        assert!(
            is_identity_uuid(&first),
            "unexpected identity shape: {first}"
        );

        // A second gateway start re-runs the migrator with nothing pending.
        migrate_index_db_file(&a).await.unwrap();
        let second = claimed(probe_index_db_uuid(&a).await);
        assert_eq!(first, second, "the identity must survive a reopen");

        let other = claimed(probe_index_db_uuid(&b).await);
        assert_ne!(first, other, "two databases must not share an identity");
    }

    // Everything that answers "nothing" must be told apart from everything
    // that cannot answer. Here: a pre-migration database and a path that does
    // not exist claim nothing — while a real migrated database read through
    // the very same connect options does produce its identity (the positive
    // control: "claims nothing" must not be an artefact of the read path).
    #[tokio::test]
    async fn probe_claims_nothing_without_an_identity_table() {
        let dir = tempfile::tempdir().unwrap();

        let bare = dir.path().join("bare.db");
        {
            let options = SqliteConnectOptions::new()
                .filename(&bare)
                .create_if_missing(true);
            let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
            sqlx::query("CREATE TABLE something (id INTEGER PRIMARY KEY)")
                .execute(&mut conn)
                .await
                .unwrap();
            conn.close().await.unwrap();
        }
        assert_eq!(
            probe_index_db_uuid(&bare).await,
            DbIdentityProbe::ClaimsNothing
        );

        assert_eq!(
            probe_index_db_uuid(&dir.path().join("missing.db")).await,
            DbIdentityProbe::ClaimsNothing
        );

        let migrated = migrated_index_db(dir.path(), "real").await;
        let uuid = claimed(probe_index_db_uuid(&migrated).await);
        assert!(is_identity_uuid(&uuid), "unexpected identity shape: {uuid}");
    }

    // A file that exists but cannot be interrogated is `Unknown`, never
    // `ClaimsNothing`: clause (b)'s gate must fail closed on it.
    #[tokio::test]
    async fn probe_is_unknown_when_the_file_cannot_be_read() {
        let dir = tempfile::tempdir().unwrap();

        // A directory standing where the database file should be: portable,
        // and open fails on every platform.
        let as_directory = dir.path().join("directory.db");
        std::fs::create_dir(&as_directory).unwrap();
        assert_eq!(
            probe_index_db_uuid(&as_directory).await,
            DbIdentityProbe::Unknown
        );

        // A file that is not a database: SQLite accepts the open and rejects
        // the header on the first read, so the failure surfaces on the query.
        let garbage = dir.path().join("garbage.db");
        std::fs::write(&garbage, b"this is not a SQLite database, not even close").unwrap();
        assert_eq!(
            probe_index_db_uuid(&garbage).await,
            DbIdentityProbe::Unknown
        );
    }

    // The read is schema-qualified: user_data carries an identity table of
    // its own, and an association must key on the INDEX database's UUID.
    #[tokio::test]
    async fn current_uuid_reads_the_index_schema_not_the_attached_user_data() {
        assert!(
            READ_INDEX_UUID_SQL.contains("main."),
            "the identity read must be schema-qualified"
        );

        let mut dbs = setup_test_databases().await;
        let index_uuid = current_index_db_uuid(&mut dbs.index_conn)
            .await
            .expect("the migrated index db has an identity");

        let user_data_uuid: String =
            sqlx::query_scalar("SELECT uuid FROM user_data.database_identity WHERE id = 1")
                .fetch_one(&mut dbs.index_conn)
                .await
                .expect("the migrated user_data db has an identity");

        assert!(
            is_identity_uuid(&index_uuid),
            "unexpected shape: {index_uuid}"
        );
        assert_ne!(
            index_uuid, user_data_uuid,
            "the index identity must not be read out of the attached user_data schema"
        );

        // A row that cannot be a UUID is no identity at all.
        sqlx::query("UPDATE main.database_identity SET uuid = '' WHERE id = 1")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        assert_eq!(current_index_db_uuid(&mut dbs.index_conn).await, None);

        // ...and with main's table gone the read must report no identity
        // rather than falling through to user_data's table, which is exactly
        // what an unqualified read would do.
        sqlx::query("DROP TABLE main.database_identity")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        assert_eq!(
            current_index_db_uuid(&mut dbs.index_conn).await,
            None,
            "the index identity read must not resolve to the attached user_data table"
        );
    }
}
