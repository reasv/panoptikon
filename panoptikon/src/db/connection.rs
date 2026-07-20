use axum::{
    extract::{FromRequestParts, Query},
    http::request::Parts,
};
use serde::Deserialize;
use sqlx::{
    Connection, SqliteConnection, SqlitePool,
    pool::PoolConnection,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use std::{
    collections::HashMap,
    fs,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::Duration,
};
use url::Url;

use crate::api_error::ApiError;
use crate::db::sql_functions::ensure_sqlite_extensions;

pub struct ReadOnly;
/// Read-only mode that skips the user_data attach for endpoints that never
/// touch bookmarks or pinboards (e.g. file/thumbnail serving).
pub struct ReadOnlyNoUserData;
pub struct UserDataWrite;

pub(crate) trait DbMode {
    const WRITE_LOCK: bool;
    const USER_DATA_WL: bool;
    const ATTACH_USER_DATA: bool;
}

impl DbMode for ReadOnly {
    const WRITE_LOCK: bool = false;
    const USER_DATA_WL: bool = false;
    const ATTACH_USER_DATA: bool = true;
}

impl DbMode for ReadOnlyNoUserData {
    const WRITE_LOCK: bool = false;
    const USER_DATA_WL: bool = false;
    const ATTACH_USER_DATA: bool = false;
}

impl DbMode for UserDataWrite {
    const WRITE_LOCK: bool = false;
    const USER_DATA_WL: bool = true;
    const ATTACH_USER_DATA: bool = true;
}

/// A request-scoped database handle: read-only requests check a connection
/// out of a shared per-(index_db, user_data_db) pool, write requests open a
/// dedicated connection. Derefs to `SqliteConnection` so handlers can pass
/// `&mut db.conn` to query helpers either way.
pub enum DbConn {
    Pooled(PoolConnection<sqlx::Sqlite>),
    Direct(SqliteConnection),
}

impl Deref for DbConn {
    type Target = SqliteConnection;

    fn deref(&self) -> &SqliteConnection {
        match self {
            DbConn::Pooled(conn) => conn,
            DbConn::Direct(conn) => conn,
        }
    }
}

impl DerefMut for DbConn {
    fn deref_mut(&mut self) -> &mut SqliteConnection {
        match self {
            DbConn::Pooled(conn) => conn,
            DbConn::Direct(conn) => conn,
        }
    }
}

pub struct DbConnection<M: DbMode> {
    pub conn: DbConn,
    pub index_db: String,
    pub user_data_db: String,
    _mode: PhantomData<M>,
}

impl<M: DbMode> Drop for DbConnection<M> {
    fn drop(&mut self) {
        // Unconditional bump on release of a user-data write connection:
        // over-invalidating the search cache (e.g. on a failed write) is
        // safe; only under-invalidation would be a bug. `USER_DATA_WL` is a
        // const, so this compiles out entirely for read connections.
        if M::USER_DATA_WL {
            crate::db::epochs::bump_user_data_epoch(&self.user_data_db);
        }
    }
}

pub(crate) async fn open_index_db_read(
    index_db: &str,
    user_data_db: &str,
) -> Result<SqliteConnection, ApiError> {
    let paths = db_paths(index_db, user_data_db)?;
    connect_db(&paths, false, false, true).await
}

pub(crate) async fn open_index_db_read_no_user_data(
    index_db: &str,
) -> Result<SqliteConnection, ApiError> {
    let paths = db_paths_index_only(index_db)?;
    connect_db(&paths, false, false, false).await
}

pub(crate) async fn open_index_db_write_no_user_data(
    index_db: &str,
) -> Result<SqliteConnection, ApiError> {
    let paths = db_paths_index_only(index_db)?;
    connect_db(&paths, true, false, false).await
}

#[derive(Deserialize)]
struct DbQuery {
    index_db: Option<String>,
    user_data_db: Option<String>,
}

struct DbNames {
    index_db: String,
    user_data_db: String,
}

pub(crate) struct DbPaths {
    pub(crate) index_db_file: PathBuf,
    pub(crate) user_db_file: PathBuf,
    pub(crate) storage_db_file: PathBuf,
}

pub(crate) struct IndexStoragePaths {
    pub(crate) index_db_file: PathBuf,
    pub(crate) storage_db_file: PathBuf,
}

impl<S, M> FromRequestParts<S> for DbConnection<M>
where
    S: Send + Sync,
    M: DbMode,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let query = Query::<DbQuery>::try_from_uri(&parts.uri)
            .map_err(|_| ApiError::bad_request("Invalid query parameters"))?;

        // Read-only requests share pooled connections; only writes still open
        // (and tear down) a dedicated connection per request.
        if !M::WRITE_LOCK && !M::USER_DATA_WL {
            let names = resolve_db_names_unchecked(&query.0);
            let conn = acquire_read_conn(&query.0, &names, M::ATTACH_USER_DATA).await?;
            return Ok(Self {
                conn: DbConn::Pooled(conn),
                index_db: names.index_db,
                user_data_db: names.user_data_db,
                _mode: PhantomData,
            });
        }

        let names = resolve_db_names(query.0)?;
        let paths = db_paths(&names.index_db, &names.user_data_db)?;
        let conn = connect_db(&paths, M::WRITE_LOCK, M::USER_DATA_WL, M::ATTACH_USER_DATA).await?;

        Ok(Self {
            conn: DbConn::Direct(conn),
            index_db: names.index_db,
            user_data_db: names.user_data_db,
            _mode: PhantomData,
        })
    }
}

/// Max pooled read connections per (index_db, user_data_db) pair. Sized for a
/// browser's worth of concurrent image/search requests plus SSR; SQLite read
/// connections are cheap but each holds its own page cache.
const READ_POOL_MAX_CONNECTIONS: u32 = 16;
const READ_POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// Key: (index_db, user_data_db-or-empty, attach_user_data).
type ReadPoolKey = (String, String, bool);

fn read_pools() -> &'static Mutex<HashMap<ReadPoolKey, SqlitePool>> {
    static POOLS: OnceLock<Mutex<HashMap<ReadPoolKey, SqlitePool>>> = OnceLock::new();
    POOLS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Closes and forgets every pooled read connection that touches `db_name`
/// (as either the index or the user-data database). MUST be called before
/// deleting, renaming, or replacing a database file: idle pooled connections
/// hold open OS file handles for up to `READ_POOL_IDLE_TIMEOUT` (which block
/// deletion/renaming outright on Windows), and connections opened against a
/// since-replaced file would keep serving its old contents until then.
/// In-flight requests finish normally — `close()` waits for checked-out
/// connections to be returned; the next request transparently builds a fresh
/// pool against whatever file is at the path.
#[allow(dead_code)] // No in-process DB delete/rename/restore flow exists yet.
pub(crate) async fn invalidate_read_pools(db_name: &str) {
    let removed: Vec<SqlitePool> = {
        let mut pools = read_pools().lock().expect("read pool registry poisoned");
        let keys: Vec<ReadPoolKey> = pools
            .keys()
            .filter(|(index_db, user_data_db, _)| index_db == db_name || user_data_db == db_name)
            .cloned()
            .collect();
        keys.into_iter()
            .filter_map(|key| pools.remove(&key))
            .collect()
    };
    for pool in removed {
        pool.close().await;
    }
}

async fn acquire_read_conn(
    query: &DbQuery,
    names: &DbNames,
    attach_user_data: bool,
) -> Result<PoolConnection<sqlx::Sqlite>, ApiError> {
    let key = (
        names.index_db.clone(),
        if attach_user_data {
            names.user_data_db.clone()
        } else {
            String::new()
        },
        attach_user_data,
    );

    let existing = {
        let pools = read_pools().lock().expect("read pool registry poisoned");
        pools.get(&key).cloned()
    };
    let pool = match existing {
        Some(pool) => pool,
        None => {
            // First use of this DB pair: validate the client-supplied names
            // (defaults are trusted, as before) and prepare directories once
            // here instead of on every request.
            check_dbs(
                query.index_db.as_deref(),
                if attach_user_data {
                    query.user_data_db.as_deref()
                } else {
                    None
                },
            )?;
            let paths = db_paths(&names.index_db, &names.user_data_db)?;
            let pool = build_read_pool(&paths, attach_user_data)?;
            let mut pools = read_pools().lock().expect("read pool registry poisoned");
            // A concurrent request may have raced us here; keep the first pool.
            pools.entry(key).or_insert(pool).clone()
        }
    };

    pool.acquire().await.map_err(|err| {
        tracing::error!(error = %err, "failed to acquire read connection");
        ApiError::internal("Failed to open database")
    })
}

fn build_read_pool(paths: &DbPaths, attach_user_data: bool) -> Result<SqlitePool, ApiError> {
    ensure_sqlite_extensions()?;
    let options = SqliteConnectOptions::new()
        .filename(&paths.index_db_file)
        .read_only(true);
    let storage_path = paths.storage_db_file.to_string_lossy().to_string();
    let user_data_path = attach_user_data.then(|| user_data_attach_path(&paths.user_db_file, true));

    let pool = SqlitePoolOptions::new()
        .max_connections(READ_POOL_MAX_CONNECTIONS)
        .min_connections(0)
        .idle_timeout(Some(READ_POOL_IDLE_TIMEOUT))
        .test_before_acquire(false)
        .after_connect(move |conn, _meta| {
            let storage_path = storage_path.clone();
            let user_data_path = user_data_path.clone();
            Box::pin(async move {
                sqlx::query("ATTACH DATABASE ? AS storage")
                    .bind(storage_path)
                    .execute(&mut *conn)
                    .await?;
                if let Some(user_data_path) = user_data_path {
                    sqlx::query("ATTACH DATABASE ? AS user_data")
                        .bind(user_data_path)
                        .execute(&mut *conn)
                        .await?;
                }
                sqlx::query("PRAGMA foreign_keys = ON")
                    .execute(&mut *conn)
                    .await?;
                sqlx::query("PRAGMA case_sensitive_like = ON")
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect_lazy_with(options);
    Ok(pool)
}

fn resolve_db_names(query: DbQuery) -> Result<DbNames, ApiError> {
    check_dbs(query.index_db.as_deref(), query.user_data_db.as_deref())?;
    Ok(resolve_db_names_unchecked(&query))
}

fn resolve_db_names_unchecked(query: &DbQuery) -> DbNames {
    let (default_index, default_user) = db_default_names();
    DbNames {
        index_db: query.index_db.clone().unwrap_or(default_index),
        user_data_db: query.user_data_db.clone().unwrap_or(default_user),
    }
}

fn db_default_names() -> (String, String) {
    let runtime = crate::config::runtime();
    (runtime.index_db.clone(), runtime.user_data_db.clone())
}

pub(crate) fn index_storage_paths_unchecked(index_db: &str) -> IndexStoragePaths {
    let data_dir = crate::config::runtime().data_folder.clone();
    let index_db_dir = data_dir.join("index");
    let index_dir = index_db_dir.join(index_db);
    IndexStoragePaths {
        index_db_file: index_dir.join("index.db"),
        storage_db_file: index_dir.join("storage.db"),
    }
}

fn index_storage_paths(index_db: &str) -> Result<IndexStoragePaths, ApiError> {
    let index_paths = index_storage_paths_unchecked(index_db);
    let index_db_dir = index_paths
        .index_db_file
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));

    fs::create_dir_all(&index_db_dir).map_err(|err| {
        tracing::error!(error = %err, "failed to create index dir");
        ApiError::internal("Failed to prepare database directories")
    })?;

    Ok(index_paths)
}

fn db_paths_index_only(index_db: &str) -> Result<DbPaths, ApiError> {
    let index_paths = index_storage_paths(index_db)?;
    Ok(DbPaths {
        index_db_file: index_paths.index_db_file,
        storage_db_file: index_paths.storage_db_file,
        user_db_file: PathBuf::new(),
    })
}

fn db_paths(index_db: &str, user_data_db: &str) -> Result<DbPaths, ApiError> {
    let index_paths = index_storage_paths(index_db)?;
    let user_data_db_dir = crate::config::runtime().data_folder.join("user_data");
    fs::create_dir_all(&user_data_db_dir).map_err(|err| {
        tracing::error!(error = %err, "failed to create user data dir");
        ApiError::internal("Failed to prepare database directories")
    })?;

    Ok(DbPaths {
        index_db_file: index_paths.index_db_file,
        storage_db_file: index_paths.storage_db_file,
        user_db_file: user_data_db_dir.join(format!("{user_data_db}.db")),
    })
}

fn db_lists() -> Result<(Vec<String>, Vec<String>), ApiError> {
    let data_dir = crate::config::runtime().data_folder.clone();
    let index_dir = data_dir.join("index");
    let user_data_dir = data_dir.join("user_data");

    fs::create_dir_all(&index_dir).map_err(|err| {
        tracing::error!(error = %err, "failed to create index dir");
        ApiError::internal("Failed to read database list")
    })?;
    fs::create_dir_all(&user_data_dir).map_err(|err| {
        tracing::error!(error = %err, "failed to create user data dir");
        ApiError::internal("Failed to read database list")
    })?;

    let mut index_dbs = Vec::new();
    let entries = fs::read_dir(&index_dir).map_err(|err| {
        tracing::error!(error = %err, "failed to read index dir");
        ApiError::internal("Failed to read database list")
    })?;
    for entry in entries {
        let entry = entry.map_err(|err| {
            tracing::error!(error = %err, "failed to read index dir entry");
            ApiError::internal("Failed to read database list")
        })?;
        let path = entry.path();
        if path.is_dir() && path.join("index.db").exists() {
            if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
                index_dbs.push(name.to_string());
            }
        }
    }

    let mut user_data_dbs = Vec::new();
    let entries = fs::read_dir(&user_data_dir).map_err(|err| {
        tracing::error!(error = %err, "failed to read user data dir");
        ApiError::internal("Failed to read database list")
    })?;
    for entry in entries {
        let entry = entry.map_err(|err| {
            tracing::error!(error = %err, "failed to read user data dir entry");
            ApiError::internal("Failed to read database list")
        })?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("db") {
            if let Some(stem) = path.file_stem().and_then(|name| name.to_str()) {
                user_data_dbs.push(stem.to_string());
            }
        }
    }

    Ok((index_dbs, user_data_dbs))
}

fn check_dbs(index_db: Option<&str>, user_data_db: Option<&str>) -> Result<(), ApiError> {
    if index_db.is_none() && user_data_db.is_none() {
        return Ok(());
    }

    let (index_dbs, user_data_dbs) = db_lists()?;
    if let Some(index_db) = index_db {
        if !index_dbs.iter().any(|entry| entry == index_db) {
            return Err(ApiError::not_found(format!(
                "Index database {index_db} not found"
            )));
        }
    }

    // Python reports this as "Index database ... not found" (copy-paste bug
    // in api/routers/utils.py); deliberately diverge — the wrong label sends
    // people hunting for a missing index DB when it is the user-data DB.
    if let Some(user_data_db) = user_data_db {
        if !user_data_dbs.iter().any(|entry| entry == user_data_db) {
            return Err(ApiError::not_found(format!(
                "User data database {user_data_db} not found"
            )));
        }
    }

    Ok(())
}

/// True when the `readonly` config key requests read-only mode. Python
/// parity (READONLY env var there): strips write locks and skips startup
/// migrations.
pub(crate) fn readonly_mode() -> bool {
    crate::config::runtime().readonly
}

async fn connect_db(
    paths: &DbPaths,
    write_lock: bool,
    user_data_wl: bool,
    attach_user_data: bool,
) -> Result<SqliteConnection, ApiError> {
    ensure_sqlite_extensions()?;
    let readonly_mode = readonly_mode();
    let write_lock = write_lock && !readonly_mode;
    let user_data_wl = user_data_wl && attach_user_data && !readonly_mode;
    let open_readonly = !write_lock && !user_data_wl;

    let mut conn = if open_readonly {
        let options = SqliteConnectOptions::new()
            .filename(&paths.index_db_file)
            .read_only(true);
        let mut conn = SqliteConnection::connect_with(&options)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "failed to open index database");
                ApiError::internal("Failed to open database")
            })?;

        sqlx::query("ATTACH DATABASE ? AS storage")
            .bind(paths.storage_db_file.to_string_lossy().to_string())
            .execute(&mut conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "failed to attach storage database");
                ApiError::internal("Failed to open database")
            })?;
        conn
    } else {
        let options = SqliteConnectOptions::new()
            .filename(&paths.index_db_file)
            .create_if_missing(write_lock);
        let mut conn = SqliteConnection::connect_with(&options)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "failed to open index database");
                ApiError::internal("Failed to open database")
            })?;

        sqlx::query("ATTACH DATABASE ? AS storage")
            .bind(paths.storage_db_file.to_string_lossy().to_string())
            .execute(&mut conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "failed to attach storage database");
                ApiError::internal("Failed to open database")
            })?;
        if write_lock {
            sqlx::query("PRAGMA journal_mode=WAL")
                .execute(&mut conn)
                .await
                .map_err(|err| {
                    tracing::error!(error = %err, "failed to enable WAL mode");
                    ApiError::internal("Failed to open database")
                })?;
        }
        conn
    };

    if attach_user_data {
        if !write_lock || user_data_wl {
            let user_data_path = user_data_attach_path(&paths.user_db_file, !user_data_wl);
            sqlx::query("ATTACH DATABASE ? AS user_data")
                .bind(user_data_path)
                .execute(&mut conn)
                .await
                .map_err(|err| {
                    tracing::error!(error = %err, "failed to attach user data database");
                    ApiError::internal("Failed to open database")
                })?;
            if user_data_wl {
                sqlx::query("PRAGMA user_data.journal_mode=WAL")
                    .execute(&mut conn)
                    .await
                    .map_err(|err| {
                        tracing::error!(error = %err, "failed to enable WAL for user data");
                        ApiError::internal("Failed to open database")
                    })?;
            }
        }
    }

    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to enable foreign keys");
            ApiError::internal("Failed to open database")
        })?;
    sqlx::query("PRAGMA case_sensitive_like = ON")
        .execute(&mut conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to enable case-sensitive LIKE");
            ApiError::internal("Failed to open database")
        })?;

    Ok(conn)
}

fn user_data_attach_path(path: &Path, read_only: bool) -> String {
    if !read_only {
        return path.to_string_lossy().to_string();
    }
    if let Ok(mut url) = Url::from_file_path(path) {
        url.set_query(Some("mode=ro"));
        return url.to_string();
    }
    let path = path.to_string_lossy().replace('\\', "/");
    format!("file:{path}?mode=ro")
}
