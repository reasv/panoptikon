use anyhow::{Context, Result};
use sqlx::{
    Connection, SqliteConnection,
    migrate::{Migrate, Migrator},
    sqlite::SqliteConnectOptions,
};
use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static INDEX_MIGRATOR: Migrator = sqlx::migrate!("migrations/index");
static STORAGE_MIGRATOR: Migrator = sqlx::migrate!("migrations/storage");
static USER_DATA_MIGRATOR: Migrator = sqlx::migrate!("migrations/user_data");

// Alembic head revisions of the Python-managed schemas (see
// src/panoptikon/db/migrations/*/versions). Each init.sql is a snapshot of
// the schema at exactly this revision, so a Python-created database may be
// baselined (init recorded as applied without executing it) only when its
// alembic_version matches: baselining an out-of-date schema would leave the
// gateway assuming columns that don't exist. Update these if Python ever
// gains another alembic migration.
const INDEX_ALEMBIC_HEAD: &str = "b2c3d4e5f6a7";
const STORAGE_ALEMBIC_HEAD: &str = "31adcda83d69";
const USER_DATA_ALEMBIC_HEAD: &str = "31adcda83d69";

#[derive(Clone, Copy, Debug)]
pub(crate) enum MigrationTarget {
    Disk,
    Memory,
}

#[derive(Debug)]
pub(crate) struct DbPaths {
    pub index_db: String,
    pub user_data_db: String,
    pub index_db_file: PathBuf,
    pub storage_db_file: PathBuf,
    pub user_db_file: PathBuf,
}

pub(crate) struct InMemoryDatabases {
    pub index_db: String,
    pub user_data_db: String,
    pub index_key: String,
    pub storage_key: String,
    pub user_data_key: String,
    pub index_conn: SqliteConnection,
    pub storage_conn: SqliteConnection,
    pub user_data_conn: SqliteConnection,
}

pub(crate) enum MigratedDatabases {
    Disk(DbPaths),
    Memory(InMemoryDatabases),
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
    migrate_path(&paths.storage_db_file, &STORAGE_MIGRATOR, STORAGE_ALEMBIC_HEAD).await?;
    migrate_path(&paths.user_db_file, &USER_DATA_MIGRATOR, USER_DATA_ALEMBIC_HEAD).await?;
    Ok(paths)
}

pub(crate) async fn migrate_all_databases_on_disk() -> Result<()> {
    let data_dir = PathBuf::from(env::var("DATA_FOLDER").unwrap_or_else(|_| "data".to_string()));
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

pub(crate) async fn migrate_databases(
    index_db: Option<&str>,
    user_data_db: Option<&str>,
    target: MigrationTarget,
) -> Result<MigratedDatabases> {
    let (default_index, default_user) = db_default_names();
    let index_db = index_db.unwrap_or(&default_index).to_string();
    let user_data_db = user_data_db.unwrap_or(&default_user).to_string();

    match target {
        MigrationTarget::Disk => {
            let paths = migrate_databases_on_disk(Some(&index_db), Some(&user_data_db)).await?;
            Ok(MigratedDatabases::Disk(paths))
        }
        MigrationTarget::Memory => {
            let dbs = migrate_in_memory(index_db, user_data_db).await?;
            Ok(MigratedDatabases::Memory(dbs))
        }
    }
}

fn db_default_names() -> (String, String) {
    let index_default = env::var("INDEX_DB").unwrap_or_else(|_| "default".to_string());
    let user_default = env::var("USER_DATA_DB").unwrap_or_else(|_| "default".to_string());
    (index_default, user_default)
}

fn db_paths(index_db: &str, user_data_db: &str) -> Result<DbPaths> {
    let data_dir = PathBuf::from(env::var("DATA_FOLDER").unwrap_or_else(|_| "data".to_string()));
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
    migrator
        .run(&mut conn)
        .await
        .with_context(|| format!("failed to migrate database {}", path.display()))?;
    if fresh {
        stamp_alembic_head(&mut conn, expected_alembic_head)
            .await
            .with_context(|| format!("failed to stamp database {}", path.display()))?;
    }
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

    conn.ensure_migrations_table()
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

async fn migrate_in_memory(index_db: String, user_data_db: String) -> Result<InMemoryDatabases> {
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
        index_db,
        user_data_db,
        index_key,
        storage_key,
        user_data_key,
        index_conn,
        storage_conn,
        user_data_conn,
    })
}

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
    match migrate_databases(None, None, MigrationTarget::Memory)
        .await
        .expect("failed to create in-memory test databases")
    {
        MigratedDatabases::Memory(databases) => databases,
        MigratedDatabases::Disk(_) => unreachable!("expected in-memory databases for tests"),
    }
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

        assert_eq!(sqlx_migration_count(&path).await, 1);
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

    // A fresh file gets the real schema from init.sql, including the
    // alembic_version row that keeps the DB readable by the Python server.
    #[tokio::test]
    async fn fresh_database_is_created_from_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("default.db");

        migrate_path(&path, &USER_DATA_MIGRATOR, USER_DATA_ALEMBIC_HEAD)
            .await
            .expect("fresh database creation should succeed");

        assert_eq!(sqlx_migration_count(&path).await, 1);
        let mut conn = connect(&path).await;
        let version: (String,) = sqlx::query_as("SELECT version_num FROM alembic_version")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(version.0, USER_DATA_ALEMBIC_HEAD);
        conn.close().await.unwrap();
    }
}
