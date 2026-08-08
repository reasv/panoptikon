//! Per-database identity UUIDs (`database_identity`, one row per database).
//!
//! Every index and user_data database is stamped with a UUID by a migration
//! (`…_database_identity.sql`). It is a server-side matching key only: it
//! never appears in API responses, URLs or the UI — names remain the
//! human-facing handle. The pinboard↔database association reads the index
//! database's UUID as its primary match key.
//!
//! Both helpers here are deliberately *tolerant*: a database that predates
//! the migration, or one that cannot be opened at all, has no identity to
//! offer and simply claims nothing. Neither case is an error worth failing a
//! request over, so both read as `None`.

use std::path::Path;

use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};

/// Schema-qualified on purpose: a pinboard connection has user_data
/// ATTACHed, and user_data carries a `database_identity` table of its own.
/// Unqualified, this would still resolve to `main` — but only by SQLite's
/// search order, which is not a thing to rely on for an identity read.
const READ_INDEX_UUID_SQL: &str = "SELECT uuid FROM main.database_identity WHERE id = 1";

/// Whether `value` has the shape every identity UUID in this system is
/// written in: 32 lowercase hex characters, unhyphenated — what
/// `lower(hex(randomblob(16)))` produces in the migrations and what
/// [`instance_id`](super::instance_id) writes to its file.
pub(crate) fn is_identity_uuid(value: &str) -> bool {
    value.len() == 32 && value.chars().all(|ch| matches!(ch, '0'..='9' | 'a'..='f'))
}

/// The identity UUID of the index database on an already-open connection
/// (its `main` schema). `None` for a database migrated before the identity
/// table existed, or whose row somehow went missing.
#[allow(dead_code)] // Consumed by the pinboard association match rule (step 2).
pub(crate) async fn current_index_db_uuid(conn: &mut SqliteConnection) -> Option<String> {
    match sqlx::query_scalar::<_, String>(READ_INDEX_UUID_SQL)
        .fetch_optional(&mut *conn)
        .await
    {
        Ok(uuid) => uuid,
        Err(err) => {
            tracing::debug!(error = %err, "index database has no readable identity row");
            None
        }
    }
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
/// `None` for a missing file, a file that is not a database, one that cannot
/// be opened, and one without the identity table: all of them legitimately
/// claim nothing.
#[allow(dead_code)] // Consumed by the local-UUID cache (step 2).
pub(crate) async fn probe_index_db_uuid(index_db_file: &Path) -> Option<String> {
    // `create_if_missing` defaults to false, so a missing file fails to open
    // rather than being created as an empty database.
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
            return None;
        }
    };
    let uuid = current_index_db_uuid(&mut conn).await;
    if let Err(err) = conn.close().await {
        tracing::debug!(error = %err, "failed to close identity probe connection");
    }
    uuid
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

    // The UUID is minted once by the migration and never rewritten, so it is
    // stable across opens — and two databases never share one.
    #[tokio::test]
    async fn identity_is_stable_across_opens_and_unique_per_database() {
        let dir = tempfile::tempdir().unwrap();
        let a = migrated_index_db(dir.path(), "alpha").await;
        let b = migrated_index_db(dir.path(), "beta").await;

        let first = probe_index_db_uuid(&a).await.expect("alpha has an identity");
        assert!(is_identity_uuid(&first), "unexpected identity shape: {first}");

        // A second gateway start re-runs the migrator with nothing pending.
        migrate_index_db_file(&a).await.unwrap();
        let second = probe_index_db_uuid(&a).await.expect("alpha keeps it");
        assert_eq!(first, second, "the identity must survive a reopen");

        let other = probe_index_db_uuid(&b).await.expect("beta has an identity");
        assert_ne!(first, other, "two databases must not share an identity");
    }

    // Everything that cannot answer reads as "claims nothing" rather than as
    // an error: a pre-migration database, a file that is not one of ours, and
    // a path that does not exist.
    #[tokio::test]
    async fn probe_is_none_without_an_identity_table() {
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
        assert_eq!(probe_index_db_uuid(&bare).await, None);

        assert_eq!(probe_index_db_uuid(&dir.path().join("missing.db")).await, None);
    }

    // The read is schema-qualified: user_data carries an identity table of
    // its own, and an association must key on the INDEX database's UUID.
    #[tokio::test]
    async fn current_uuid_reads_the_index_schema_not_the_attached_user_data() {
        let mut dbs = setup_test_databases().await;
        let index_uuid = current_index_db_uuid(&mut dbs.index_conn)
            .await
            .expect("the migrated index db has an identity");

        let user_data_uuid: String =
            sqlx::query_scalar("SELECT uuid FROM user_data.database_identity WHERE id = 1")
                .fetch_one(&mut dbs.index_conn)
                .await
                .expect("the migrated user_data db has an identity");

        assert!(is_identity_uuid(&index_uuid), "unexpected shape: {index_uuid}");
        assert_ne!(
            index_uuid, user_data_uuid,
            "the index identity must not be read out of the attached user_data schema"
        );
    }
}
