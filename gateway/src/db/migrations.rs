use anyhow::{Context, Result};
use sqlx::{Connection, SqliteConnection, migrate::Migrator, sqlite::SqliteConnectOptions};
use std::{
    env,
    fs,
    path::{Path, PathBuf},
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
            let paths = db_paths(&index_db, &user_data_db)?;
            migrate_path(&paths.index_db_file, &INDEX_MIGRATOR).await?;
            migrate_path(&paths.storage_db_file, &STORAGE_MIGRATOR).await?;
            migrate_path(&paths.user_db_file, &USER_DATA_MIGRATOR).await?;
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
    fs::create_dir_all(&user_data_db_dir)
        .with_context(|| format!("failed to create user data dir {}", user_data_db_dir.display()))?;

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
    migrator
        .run(&mut conn)
        .await
        .with_context(|| format!("failed to migrate database {}", path.display()))?;
    Ok(())
}

async fn migrate_in_memory(index_db: String, user_data_db: String) -> Result<InMemoryDatabases> {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("failed to read system clock")?
        .as_nanos();
    let index_key = memory_key(&format!("index-{index_db}"), suffix);
    let storage_key = memory_key(&format!("storage-{index_db}"), suffix);
    let user_data_key = memory_key(&format!("user-data-{user_data_db}"), suffix);

    // Use shared-cache in-memory database names so separate connections can attach to them.
    let index_url = format!("sqlite://{index_key}?mode=memory&cache=shared");
    let storage_url = format!("sqlite://{storage_key}?mode=memory&cache=shared");
    let user_data_url = format!("sqlite://{user_data_key}?mode=memory&cache=shared");

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
        .bind(&storage_key)
        .execute(&mut index_conn)
        .await
        .context("failed to attach in-memory storage db")?;
    sqlx::query("ATTACH DATABASE ? AS user_data")
        .bind(&user_data_key)
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

fn memory_key(label: &str, suffix: u128) -> String {
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
