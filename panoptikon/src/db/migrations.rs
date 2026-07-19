use anyhow::{Context, Result};
use sqlx::{
    AssertSqlSafe, Connection, SqlSafeStr, SqliteConnection,
    migrate::{Migrate, Migration, Migrator},
    sqlite::SqliteConnectOptions,
};
use std::{
    borrow::Cow,
    fs,
    path::{Path, PathBuf},
    sync::LazyLock,
};

// sqlx checksums migration files byte-for-byte, but git autocrlf renders the
// same commit with LF or CRLF depending on platform and config — so two
// builds of identical sources embed different checksums and refuse each
// other's databases ("previously applied but has been modified"). Normalize
// to LF before the Migrator ever sees the SQL: checksums are then a function
// of content, not of checkout flavor. Databases that already recorded CRLF
// checksums are repaired at startup by repair_line_ending_checksums.
static INDEX_MIGRATOR: LazyLock<Migrator> =
    LazyLock::new(|| normalize_line_endings(sqlx::migrate!("migrations/index")));
static STORAGE_MIGRATOR: LazyLock<Migrator> =
    LazyLock::new(|| normalize_line_endings(sqlx::migrate!("migrations/storage")));
static USER_DATA_MIGRATOR: LazyLock<Migrator> =
    LazyLock::new(|| normalize_line_endings(sqlx::migrate!("migrations/user_data")));

fn normalize_line_endings(raw: Migrator) -> Migrator {
    let migrations: Vec<Migration> = raw
        .migrations
        .iter()
        .map(|migration| {
            if migration.sql.as_str().contains('\r') {
                // Migration::new recomputes the checksum from the given SQL.
                Migration::new(
                    migration.version,
                    migration.description.clone(),
                    migration.migration_type,
                    AssertSqlSafe(migration.sql.as_str().replace("\r\n", "\n")).into_sql_str(),
                    migration.no_tx,
                )
            } else {
                migration.clone()
            }
        })
        .collect();
    Migrator {
        migrations: Cow::Owned(migrations),
        ..raw
    }
}

// Alembic head revisions of the Python-managed schemas (see
// the python-legacy branch's alembic versions). Each init.sql is a snapshot of
// the schema at exactly this revision, so a Python-created database may be
// baselined (init recorded as applied without executing it) only when its
// alembic_version matches: baselining an out-of-date schema would leave the
// gateway assuming columns that don't exist. Update these if Python ever
// gains another alembic migration.
const INDEX_ALEMBIC_HEAD: &str = "b2c3d4e5f6a7";
const STORAGE_ALEMBIC_HEAD: &str = "31adcda83d69";
const USER_DATA_ALEMBIC_HEAD: &str = "31adcda83d69";

#[derive(Debug)]
pub(crate) struct DbPaths {
    pub index_db: String,
    pub user_data_db: String,
    pub index_db_file: PathBuf,
    pub storage_db_file: PathBuf,
    pub user_db_file: PathBuf,
}

#[cfg(test)]
pub(crate) struct InMemoryDatabases {
    pub index_conn: SqliteConnection,
    // Held open so the shared-cache in-memory databases attached to
    // index_conn survive for the lifetime of the test.
    #[allow(dead_code)]
    pub storage_conn: SqliteConnection,
    #[allow(dead_code)]
    pub user_data_conn: SqliteConnection,
}

pub(crate) async fn migrate_databases_on_disk(
    index_db: Option<&str>,
    user_data_db: Option<&str>,
) -> Result<DbPaths> {
    let (default_index, default_user) = db_default_names();
    let index_db = index_db.unwrap_or(&default_index).to_string();
    let user_data_db = user_data_db.unwrap_or(&default_user).to_string();
    let paths = db_paths(&index_db, &user_data_db)?;
    migrate_path(&paths.index_db_file, &INDEX_MIGRATOR, INDEX_ALEMBIC_HEAD).await?;
    migrate_path(
        &paths.storage_db_file,
        &STORAGE_MIGRATOR,
        STORAGE_ALEMBIC_HEAD,
    )
    .await?;
    migrate_path(
        &paths.user_db_file,
        &USER_DATA_MIGRATOR,
        USER_DATA_ALEMBIC_HEAD,
    )
    .await?;
    Ok(paths)
}

pub(crate) async fn migrate_all_databases_on_disk() -> Result<()> {
    let data_dir = crate::config::runtime().data_folder.clone();
    let index_db_dir = data_dir.join("index");
    let user_data_db_dir = data_dir.join("user_data");

    if index_db_dir.is_dir() {
        for entry in fs::read_dir(&index_db_dir)
            .with_context(|| format!("failed to read index db dir {}", index_db_dir.display()))?
        {
            let entry = entry.context("failed to read index db dir entry")?;
            let file_type = entry
                .file_type()
                .context("failed to read index db dir entry file type")?;
            if !file_type.is_dir() {
                continue;
            }
            let db_dir = entry.path();
            let index_db_file = db_dir.join("index.db");
            if index_db_file.is_file() {
                migrate_path(&index_db_file, &INDEX_MIGRATOR, INDEX_ALEMBIC_HEAD).await?;
            }
            let storage_db_file = db_dir.join("storage.db");
            if storage_db_file.is_file() {
                migrate_path(&storage_db_file, &STORAGE_MIGRATOR, STORAGE_ALEMBIC_HEAD).await?;
            }
        }
    }

    if user_data_db_dir.is_dir() {
        for entry in fs::read_dir(&user_data_db_dir).with_context(|| {
            format!(
                "failed to read user data db dir {}",
                user_data_db_dir.display()
            )
        })? {
            let entry = entry.context("failed to read user data db dir entry")?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let is_db = path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("db"));
            if !is_db {
                continue;
            }
            migrate_path(&path, &USER_DATA_MIGRATOR, USER_DATA_ALEMBIC_HEAD).await?;
        }
    }

    Ok(())
}

fn db_default_names() -> (String, String) {
    let runtime = crate::config::runtime();
    (runtime.index_db.clone(), runtime.user_data_db.clone())
}

fn db_paths(index_db: &str, user_data_db: &str) -> Result<DbPaths> {
    let data_dir = crate::config::runtime().data_folder.clone();
    let index_db_dir = data_dir.join("index");
    let user_data_db_dir = data_dir.join("user_data");

    fs::create_dir_all(&index_db_dir)
        .with_context(|| format!("failed to create index dir {}", index_db_dir.display()))?;
    fs::create_dir_all(&user_data_db_dir).with_context(|| {
        format!(
            "failed to create user data dir {}",
            user_data_db_dir.display()
        )
    })?;

    let index_dir = index_db_dir.join(index_db);
    fs::create_dir_all(&index_dir)
        .with_context(|| format!("failed to create index db dir {}", index_dir.display()))?;

    Ok(DbPaths {
        index_db: index_db.to_string(),
        user_data_db: user_data_db.to_string(),
        index_db_file: index_dir.join("index.db"),
        storage_db_file: index_dir.join("storage.db"),
        user_db_file: user_data_db_dir.join(format!("{user_data_db}.db")),
    })
}

async fn migrate_path(path: &Path, migrator: &Migrator, expected_alembic_head: &str) -> Result<()> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    let mut conn = SqliteConnection::connect_with(&options)
        .await
        .with_context(|| format!("failed to open database {}", path.display()))?;
    let fresh = !has_user_tables(&mut conn).await?;
    ensure_baseline_if_needed(&mut conn, migrator, expected_alembic_head)
        .await
        .with_context(|| format!("refusing to migrate database {}", path.display()))?;
    reconcile_recorded_checksums(&mut conn, migrator)
        .await
        .with_context(|| format!("failed to reconcile checksums in {}", path.display()))?;
    migrator
        .run(&mut conn)
        .await
        .with_context(|| format!("failed to migrate database {}", path.display()))?;
    if fresh {
        stamp_alembic_head(&mut conn, expected_alembic_head)
            .await
            .with_context(|| format!("failed to stamp database {}", path.display()))?;
    }
    // All three databases are read while other connections write them (index
    // and storage by jobs, user_data directly by API handlers). WAL is a
    // persistent property of the file, so setting it once here covers every
    // later connection; without this a fresh database would sit in rollback
    // journal until its first write connection happened to set WAL, giving
    // readers and writers classic shared/exclusive lock contention.
    sqlx::query("PRAGMA journal_mode=WAL")
        .execute(&mut conn)
        .await
        .with_context(|| format!("failed to enable WAL on {}", path.display()))?;
    Ok(())
}

/// Marks a freshly created database as being at the alembic head. init.sql
/// creates `alembic_version` empty; the Python server's alembic decides what
/// to run from that table, so left empty it would attempt the initial
/// migration on top of the existing tables and fail at startup. Stamping
/// keeps gateway-created databases manageable by Python during the
/// transition. (Done in code rather than in init.sql: editing a shipped
/// migration changes its sqlx checksum and breaks every already-migrated or
/// baselined database.)
async fn stamp_alembic_head(conn: &mut SqliteConnection, head: &str) -> Result<()> {
    if !table_exists(conn, "alembic_version").await? {
        return Ok(());
    }
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM alembic_version")
        .fetch_one(&mut *conn)
        .await
        .context("failed to read alembic_version")?;
    if count.0 == 0 {
        sqlx::query("INSERT INTO alembic_version (version_num) VALUES (?1)")
            .bind(head)
            .execute(&mut *conn)
            .await
            .context("failed to insert alembic head revision")?;
    }
    Ok(())
}

async fn ensure_baseline_if_needed(
    conn: &mut SqliteConnection,
    migrator: &Migrator,
    expected_alembic_head: &str,
) -> Result<()> {
    let has_user_tables = has_user_tables(conn).await?;
    if !has_user_tables {
        return Ok(());
    }

    let has_migrations_table = table_exists(conn, "_sqlx_migrations").await?;
    let applied_count = if has_migrations_table {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(&mut *conn)
            .await
            .context("failed to read applied migrations count")?;
        row.0
    } else {
        0
    };

    if applied_count > 0 {
        return Ok(());
    }

    // About to baseline a database the gateway has never migrated: it must be
    // a Python-created database at exactly the alembic head the init snapshot
    // was taken from. Anything else (an older Python schema, or a database
    // from some other source) must be migrated by Python first — recording
    // the snapshot as applied over the wrong schema would fail at runtime
    // instead, far from the cause.
    let alembic_version: Option<(String,)> = if table_exists(conn, "alembic_version").await? {
        sqlx::query_as("SELECT version_num FROM alembic_version LIMIT 1")
            .fetch_optional(&mut *conn)
            .await
            .context("failed to read alembic_version")?
    } else {
        None
    };
    match alembic_version.as_ref().map(|(v,)| v.as_str()) {
        Some(version) if version == expected_alembic_head => {}
        Some(version) => anyhow::bail!(
            "existing database is at alembic revision {version}, expected head \
             {expected_alembic_head}; run the Python server once to migrate it \
             before starting the gateway"
        ),
        None => anyhow::bail!(
            "existing database has tables but no alembic_version; refusing to \
             baseline a schema of unknown provenance"
        ),
    }

    conn.ensure_migrations_table("_sqlx_migrations")
        .await
        .context("failed to ensure migrations table")?;

    let baseline = migrator
        .iter()
        .find(|migration| !migration.migration_type.is_down_migration());
    if let Some(migration) = baseline {
        sqlx::query(
            r#"
INSERT OR IGNORE INTO _sqlx_migrations (
    version,
    description,
    success,
    checksum,
    execution_time
) VALUES (?1, ?2, TRUE, ?3, 0)
            "#,
        )
        .bind(migration.version)
        .bind(migration.description.as_ref())
        .bind(migration.checksum.as_ref())
        .execute(&mut *conn)
        .await
        .context("failed to record baseline migration")?;
    }

    Ok(())
}

/// Reconciles recorded checksums with the embedded (LF-normalized)
/// migrations so a checksum mismatch on an already-applied migration can
/// never fail startup.
///
/// sqlx checksums the migration file bytes, which vary by checkout: git
/// autocrlf renders the same blob with LF or CRLF per platform/config, and
/// a working tree can even hold a MIXED rendering git considers unmodified
/// (it cleans to the same blob — observed in the wild: 227 CRLF + 2 LF
/// lines). Two correct builds of identical sources can therefore embed
/// different checksums and refuse each other's databases with "previously
/// applied but has been modified" — which would hit every user whose
/// binary's build environment ever changes, including everyone upgrading
/// across releases built on different platforms.
///
/// Policy, per successfully-applied migration whose recorded checksum
/// differs from the embedded one:
/// - matches the CRLF rendering of the embedded SQL → provably the same
///   content; re-record silently.
/// - anything else (e.g. a hash of a mixed rendering, which cannot be
///   reconstructed) → re-record with a WARNING. Migrations in this project
///   are append-only and never edited after shipping, so a mismatch here
///   is a rendering artifact, not divergent SQL; and failing startup would
///   not repair a genuinely divergent history anyway — it would only lock
///   the user out of their data. This mirrors alembic (which this schema
///   migrated from): applied migrations are identified by version, with
///   checksums as a diagnostic rather than a gate.
///
/// Nothing is ever executed here; pending migrations still run normally.
async fn reconcile_recorded_checksums(
    conn: &mut SqliteConnection,
    migrator: &Migrator,
) -> Result<()> {
    if !table_exists(conn, "_sqlx_migrations").await? {
        return Ok(());
    }
    let applied: Vec<(i64, Vec<u8>, bool)> =
        sqlx::query_as("SELECT version, checksum, success FROM _sqlx_migrations")
            .fetch_all(&mut *conn)
            .await
            .context("failed to read applied migration checksums")?;
    for migration in migrator.iter() {
        if migration.migration_type.is_down_migration() {
            continue;
        }
        let Some((_, recorded, success)) = applied
            .iter()
            .find(|(version, _, _)| *version == migration.version)
        else {
            continue;
        };
        if !success || recorded.as_slice() == migration.checksum.as_ref() {
            continue;
        }
        let crlf_variant = Migration::new(
            migration.version,
            migration.description.clone(),
            migration.migration_type,
            AssertSqlSafe(migration.sql.as_str().replace('\n', "\r\n")).into_sql_str(),
            migration.no_tx,
        );
        if recorded.as_slice() == crlf_variant.checksum.as_ref() {
            tracing::info!(
                version = migration.version,
                "re-recording checksum of applied migration (line-ending rendering difference)"
            );
        } else {
            tracing::warn!(
                version = migration.version,
                "recorded checksum of applied migration does not match the shipped SQL in any \
                 known line-ending rendering; re-recording it (migrations are append-only, so \
                 this is expected to be a checkout-rendering artifact of an older build — if \
                 you have actually edited a shipped migration file, this database may not \
                 match the schema the gateway expects)"
            );
        }
        sqlx::query("UPDATE _sqlx_migrations SET checksum = ?1 WHERE version = ?2")
            .bind(migration.checksum.as_ref())
            .bind(migration.version)
            .execute(&mut *conn)
            .await
            .context("failed to re-record migration checksum")?;
    }
    Ok(())
}

async fn table_exists(conn: &mut SqliteConnection, table_name: &str) -> Result<bool> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?1 LIMIT 1")
            .bind(table_name)
            .fetch_optional(conn)
            .await
            .context("failed to check for migrations table")?;
    Ok(row.is_some())
}

async fn has_user_tables(conn: &mut SqliteConnection) -> Result<bool> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name != '_sqlx_migrations' LIMIT 1",
    )
    .fetch_optional(conn)
    .await
    .context("failed to inspect sqlite_master tables")?;
    Ok(row.is_some())
}

#[cfg(test)]
pub(crate) async fn migrate_in_memory(
    index_db: String,
    user_data_db: String,
) -> Result<InMemoryDatabases> {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static MEMORY_KEY_COUNTER: AtomicU64 = AtomicU64::new(0);

    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("failed to read system clock")?
        .as_nanos();
    let unique = MEMORY_KEY_COUNTER.fetch_add(1, Ordering::Relaxed);
    let key_suffix = format!("{suffix}-{unique}");
    let index_key = memory_key(&format!("index-{index_db}"), &key_suffix);
    let storage_key = memory_key(&format!("storage-{index_db}"), &key_suffix);
    let user_data_key = memory_key(&format!("user-data-{user_data_db}"), &key_suffix);

    // Use shared-cache in-memory database names so separate connections can attach to them.
    let index_uri = format!("{index_key}?mode=memory&cache=shared");
    let storage_uri = format!("{storage_key}?mode=memory&cache=shared");
    let user_data_uri = format!("{user_data_key}?mode=memory&cache=shared");
    let index_url = format!("sqlite://{index_uri}");
    let storage_url = format!("sqlite://{storage_uri}");
    let user_data_url = format!("sqlite://{user_data_uri}");

    let mut index_conn = SqliteConnection::connect(&index_url)
        .await
        .with_context(|| format!("failed to open in-memory index db {index_key}"))?;
    let mut storage_conn = SqliteConnection::connect(&storage_url)
        .await
        .with_context(|| format!("failed to open in-memory storage db {storage_key}"))?;
    let mut user_data_conn = SqliteConnection::connect(&user_data_url)
        .await
        .with_context(|| format!("failed to open in-memory user data db {user_data_key}"))?;

    INDEX_MIGRATOR
        .run(&mut index_conn)
        .await
        .context("failed to migrate in-memory index db")?;
    STORAGE_MIGRATOR
        .run(&mut storage_conn)
        .await
        .context("failed to migrate in-memory storage db")?;
    USER_DATA_MIGRATOR
        .run(&mut user_data_conn)
        .await
        .context("failed to migrate in-memory user data db")?;

    sqlx::query("ATTACH DATABASE ? AS storage")
        .bind(&storage_uri)
        .execute(&mut index_conn)
        .await
        .context("failed to attach in-memory storage db")?;
    sqlx::query("ATTACH DATABASE ? AS user_data")
        .bind(&user_data_uri)
        .execute(&mut index_conn)
        .await
        .context("failed to attach in-memory user data db")?;

    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut index_conn)
        .await
        .context("failed to enable foreign_keys for in-memory index db")?;
    sqlx::query("PRAGMA case_sensitive_like = ON")
        .execute(&mut index_conn)
        .await
        .context("failed to enable case_sensitive_like for in-memory index db")?;

    Ok(InMemoryDatabases {
        index_conn,
        storage_conn,
        user_data_conn,
    })
}

#[cfg(test)]
fn memory_key(label: &str, suffix: &str) -> String {
    let mut normalized = String::with_capacity(label.len());
    for ch in label.chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
        } else {
            normalized.push('_');
        }
    }
    format!("file:panoptikon-{normalized}-{suffix}")
}

#[cfg(test)]
pub(crate) async fn setup_test_databases() -> InMemoryDatabases {
    let (index_db, user_data_db) = db_default_names();
    migrate_in_memory(index_db, user_data_db)
        .await
        .expect("failed to create in-memory test databases")
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn connect(path: &Path) -> SqliteConnection {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);
        SqliteConnection::connect_with(&options)
            .await
            .expect("failed to open test database")
    }

    /// Creates a fake Python-created user_data DB: alembic_version at the
    /// given revision plus a `bookmarks` table whose shape differs from the
    /// init snapshot, so any accidental execution of init.sql fails loudly
    /// ("table already exists") instead of passing silently.
    async fn fake_python_db(path: &Path, alembic_revision: Option<&str>) {
        let mut conn = connect(path).await;
        if let Some(revision) = alembic_revision {
            sqlx::query("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)")
                .execute(&mut conn)
                .await
                .unwrap();
            sqlx::query("INSERT INTO alembic_version VALUES (?1)")
                .bind(revision)
                .execute(&mut conn)
                .await
                .unwrap();
        }
        sqlx::query("CREATE TABLE bookmarks (fake_marker INTEGER PRIMARY KEY)")
            .execute(&mut conn)
            .await
            .unwrap();
        conn.close().await.unwrap();
    }

    async fn sqlx_migration_count(path: &Path) -> i64 {
        let mut conn = connect(path).await;
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        conn.close().await.unwrap();
        count.0
    }

    /// Number of up migrations shipped for user_data; baselining records the
    /// init snapshot and executes the rest, so a migrated DB records all.
    fn user_data_migration_total() -> i64 {
        USER_DATA_MIGRATOR
            .iter()
            .filter(|migration| !migration.migration_type.is_down_migration())
            .count() as i64
    }

    // A Python DB at alembic head is baselined: the init migration is
    // recorded as applied but never executed (the fake bookmarks shape
    // survives untouched).
    #[tokio::test]
    async fn baseline_records_without_executing_at_head() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("default.db");
        fake_python_db(&path, Some(USER_DATA_ALEMBIC_HEAD)).await;

        migrate_path(&path, &USER_DATA_MIGRATOR, USER_DATA_ALEMBIC_HEAD)
            .await
            .expect("baseline at head should succeed");

        assert_eq!(
            sqlx_migration_count(&path).await,
            user_data_migration_total()
        );
        let mut conn = connect(&path).await;
        let cols: Vec<(i64, String, String, i64, Option<String>, i64)> =
            sqlx::query_as("SELECT * FROM pragma_table_info('bookmarks')")
                .fetch_all(&mut conn)
                .await
                .unwrap();
        assert_eq!(cols.len(), 1, "init.sql must not have been executed");
        assert_eq!(cols[0].1, "fake_marker");
        conn.close().await.unwrap();
    }

    // A Python DB behind head must not be baselined — the init snapshot
    // assumes columns an older schema doesn't have.
    #[tokio::test]
    async fn baseline_refuses_outdated_alembic_revision() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("default.db");
        fake_python_db(&path, Some("31adcda83d68")).await;

        let err = migrate_path(&path, &USER_DATA_MIGRATOR, USER_DATA_ALEMBIC_HEAD)
            .await
            .expect_err("outdated revision must be refused");
        assert!(format!("{err:#}").contains("alembic revision"), "{err:#}");
    }

    // A non-empty DB without alembic_version is of unknown provenance and
    // must be refused rather than baselined.
    #[tokio::test]
    async fn baseline_refuses_unknown_provenance() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("default.db");
        fake_python_db(&path, None).await;

        let err = migrate_path(&path, &USER_DATA_MIGRATOR, USER_DATA_ALEMBIC_HEAD)
            .await
            .expect_err("missing alembic_version must be refused");
        assert!(format!("{err:#}").contains("no alembic_version"), "{err:#}");
    }

    // Embedded migration SQL is LF-normalized no matter how the checkout
    // rendered the files (git autocrlf), so checksums are content-addressed
    // rather than platform-addressed.
    #[test]
    fn embedded_migrations_are_lf_normalized() {
        for migrator in [&*INDEX_MIGRATOR, &*STORAGE_MIGRATOR, &*USER_DATA_MIGRATOR] {
            for migration in migrator.iter() {
                assert!(
                    !migration.sql.as_str().contains('\r'),
                    "migration {} carries CR bytes into its checksum",
                    migration.version
                );
            }
        }
    }

    /// The CRLF rendering's checksum — what a binary built from a CRLF
    /// checkout would have recorded for the same migration.
    fn crlf_checksum(migration: &Migration) -> Vec<u8> {
        Migration::new(
            migration.version,
            migration.description.clone(),
            migration.migration_type,
            AssertSqlSafe(migration.sql.as_str().replace('\n', "\r\n")).into_sql_str(),
            migration.no_tx,
        )
        .checksum
        .to_vec()
    }

    // A DB whose checksums were recorded by a CRLF-checkout build is
    // accepted: the line-ending-only mismatch is repaired in place instead
    // of failing "previously applied but has been modified".
    #[tokio::test]
    async fn crlf_recorded_checksums_are_repaired() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("default.db");
        migrate_path(&path, &USER_DATA_MIGRATOR, USER_DATA_ALEMBIC_HEAD)
            .await
            .unwrap();

        let mut conn = connect(&path).await;
        for migration in USER_DATA_MIGRATOR.iter() {
            if migration.migration_type.is_down_migration() {
                continue;
            }
            sqlx::query("UPDATE _sqlx_migrations SET checksum = ?1 WHERE version = ?2")
                .bind(crlf_checksum(migration))
                .bind(migration.version)
                .execute(&mut conn)
                .await
                .unwrap();
        }
        conn.close().await.unwrap();

        migrate_path(&path, &USER_DATA_MIGRATOR, USER_DATA_ALEMBIC_HEAD)
            .await
            .expect("CRLF-recorded checksums must be repaired, not refused");

        let mut conn = connect(&path).await;
        let rows: Vec<(i64, Vec<u8>)> =
            sqlx::query_as("SELECT version, checksum FROM _sqlx_migrations")
                .fetch_all(&mut conn)
                .await
                .unwrap();
        for migration in USER_DATA_MIGRATOR.iter() {
            if migration.migration_type.is_down_migration() {
                continue;
            }
            let recorded = &rows
                .iter()
                .find(|(version, _)| *version == migration.version)
                .expect("migration row present")
                .1;
            assert_eq!(
                recorded.as_slice(),
                migration.checksum.as_ref(),
                "repair rewrote version {} to the embedded checksum",
                migration.version
            );
        }
        conn.close().await.unwrap();
    }

    // A recorded checksum matching NO known rendering (e.g. the hash of a
    // mixed-line-ending checkout that cannot be reconstructed) must not
    // fail startup either: an applied migration is identified by its
    // version, the checksum is re-recorded (with a warning), and
    // subsequent runs are clean.
    #[tokio::test]
    async fn unexplained_checksum_mismatch_is_rerecorded_not_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("default.db");
        migrate_path(&path, &USER_DATA_MIGRATOR, USER_DATA_ALEMBIC_HEAD)
            .await
            .unwrap();

        let mut conn = connect(&path).await;
        sqlx::query("UPDATE _sqlx_migrations SET checksum = ?1 WHERE version = (SELECT MIN(version) FROM _sqlx_migrations)")
            .bind(vec![0u8; 48])
            .execute(&mut conn)
            .await
            .unwrap();
        conn.close().await.unwrap();

        migrate_path(&path, &USER_DATA_MIGRATOR, USER_DATA_ALEMBIC_HEAD)
            .await
            .expect("an applied migration must never fail startup on a checksum");

        let mut conn = connect(&path).await;
        let (recorded,): (Vec<u8>,) = sqlx::query_as(
            "SELECT checksum FROM _sqlx_migrations WHERE version = (SELECT MIN(version) FROM _sqlx_migrations)",
        )
        .fetch_one(&mut conn)
        .await
        .unwrap();
        let first = USER_DATA_MIGRATOR
            .iter()
            .find(|migration| !migration.migration_type.is_down_migration())
            .unwrap();
        assert_eq!(recorded.as_slice(), first.checksum.as_ref());
        conn.close().await.unwrap();
    }

    // A fresh file gets the real schema from init.sql, including the
    // alembic_version row that keeps the DB readable by the Python server.
    #[tokio::test]
    async fn fresh_database_is_created_from_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("default.db");

        migrate_path(&path, &USER_DATA_MIGRATOR, USER_DATA_ALEMBIC_HEAD)
            .await
            .expect("fresh database creation should succeed");

        assert_eq!(
            sqlx_migration_count(&path).await,
            user_data_migration_total()
        );
        let mut conn = connect(&path).await;
        let version: (String,) = sqlx::query_as("SELECT version_num FROM alembic_version")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(version.0, USER_DATA_ALEMBIC_HEAD);
        conn.close().await.unwrap();
    }
}
