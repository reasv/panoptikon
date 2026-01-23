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
    migrate_path(&paths.index_db_file, &INDEX_MIGRATOR).await?;
    migrate_path(&paths.storage_db_file, &STORAGE_MIGRATOR).await?;
    migrate_path(&paths.user_db_file, &USER_DATA_MIGRATOR).await?;
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
                migrate_path(&index_db_file, &INDEX_MIGRATOR).await?;
            }
            let storage_db_file = db_dir.join("storage.db");
            if storage_db_file.is_file() {
                migrate_path(&storage_db_file, &STORAGE_MIGRATOR).await?;
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
            migrate_path(&path, &USER_DATA_MIGRATOR).await?;
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

async fn migrate_path(path: &Path, migrator: &Migrator) -> Result<()> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    let mut conn = SqliteConnection::connect_with(&options)
        .await
        .with_context(|| format!("failed to open database {}", path.display()))?;
    ensure_baseline_if_needed(&mut conn, migrator).await?;
    migrator
        .run(&mut conn)
        .await
        .with_context(|| format!("failed to migrate database {}", path.display()))?;
    Ok(())
}

async fn ensure_baseline_if_needed(conn: &mut SqliteConnection, migrator: &Migrator) -> Result<()> {
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
