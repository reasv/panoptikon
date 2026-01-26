use axum::{
    extract::{FromRequestParts, Query},
    http::request::Parts,
};
use serde::Deserialize;
use url::Url;
use libsqlite3_sys::{SQLITE_OK, sqlite3_auto_extension};
use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};
use sqlite_vec::sqlite3_vec_init;
use std::{env, fs, marker::PhantomData, path::{Path, PathBuf}, sync::OnceLock};

use crate::api_error::ApiError;

pub struct ReadOnly;
pub struct UserDataWrite;
pub struct SystemWrite;

pub(crate) trait DbMode {
    const WRITE_LOCK: bool;
    const USER_DATA_WL: bool;
}

impl DbMode for ReadOnly {
    const WRITE_LOCK: bool = false;
    const USER_DATA_WL: bool = false;
}

impl DbMode for UserDataWrite {
    const WRITE_LOCK: bool = false;
    const USER_DATA_WL: bool = true;
}

impl DbMode for SystemWrite {
    const WRITE_LOCK: bool = true;
    const USER_DATA_WL: bool = false;
}

pub struct DbConnection<M: DbMode> {
    pub conn: SqliteConnection,
    pub index_db: String,
    pub user_data_db: String,
    _mode: PhantomData<M>,
}

pub(crate) async fn open_index_db_read(
    index_db: &str,
    user_data_db: &str,
) -> Result<SqliteConnection, ApiError> {
    let paths = db_paths(index_db, user_data_db)?;
    connect_db(&paths, false, false, true).await
}

pub(crate) async fn open_index_db_write(
    index_db: &str,
    user_data_db: &str,
) -> Result<SqliteConnection, ApiError> {
    let paths = db_paths(index_db, user_data_db)?;
    connect_db(&paths, true, false, true).await
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
        let names = resolve_db_names(query.0)?;
        let paths = db_paths(&names.index_db, &names.user_data_db)?;
        let conn = connect_db(&paths, M::WRITE_LOCK, M::USER_DATA_WL, true).await?;

        Ok(Self {
            conn,
            index_db: names.index_db,
            user_data_db: names.user_data_db,
            _mode: PhantomData,
        })
    }
}

fn resolve_db_names(query: DbQuery) -> Result<DbNames, ApiError> {
    check_dbs(query.index_db.as_deref(), query.user_data_db.as_deref())?;
    let (default_index, default_user) = db_default_names();
    Ok(DbNames {
        index_db: query.index_db.unwrap_or(default_index),
        user_data_db: query.user_data_db.unwrap_or(default_user),
    })
}

fn db_default_names() -> (String, String) {
    let index_default = env::var("INDEX_DB").unwrap_or_else(|_| "default".to_string());
    let user_default = env::var("USER_DATA_DB").unwrap_or_else(|_| "default".to_string());
    (index_default, user_default)
}

pub(crate) fn db_paths_unchecked(index_db: &str, user_data_db: &str) -> DbPaths {
    let index_paths = index_storage_paths_unchecked(index_db);
    let data_dir = PathBuf::from(env::var("DATA_FOLDER").unwrap_or_else(|_| "data".to_string()));
    let user_data_db_dir = data_dir.join("user_data");

    DbPaths {
        index_db_file: index_paths.index_db_file,
        storage_db_file: index_paths.storage_db_file,
        user_db_file: user_data_db_dir.join(format!("{user_data_db}.db")),
    }
}

pub(crate) fn index_storage_paths_unchecked(index_db: &str) -> IndexStoragePaths {
    let data_dir = PathBuf::from(env::var("DATA_FOLDER").unwrap_or_else(|_| "data".to_string()));
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
    let user_data_db_dir = {
        let data_dir =
            PathBuf::from(env::var("DATA_FOLDER").unwrap_or_else(|_| "data".to_string()));
        data_dir.join("user_data")
    };
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
    let data_dir = PathBuf::from(env::var("DATA_FOLDER").unwrap_or_else(|_| "data".to_string()));
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

    if let Some(user_data_db) = user_data_db {
        if !user_data_dbs.iter().any(|entry| entry == user_data_db) {
            return Err(ApiError::not_found(format!(
                "Index database {user_data_db} not found"
            )));
        }
    }

    Ok(())
}

async fn connect_db(
    paths: &DbPaths,
    write_lock: bool,
    user_data_wl: bool,
    attach_user_data: bool,
) -> Result<SqliteConnection, ApiError> {
    ensure_sqlite_vec_loaded()?;
    let readonly_mode = env::var("READONLY")
        .ok()
        .map(|value| {
            let value = value.to_lowercase();
            matches!(value.as_str(), "true" | "1")
        })
        .unwrap_or(false);
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

fn ensure_sqlite_vec_loaded() -> Result<(), ApiError> {
    static EXT_LOADED: OnceLock<()> = OnceLock::new();
    if EXT_LOADED.get().is_some() {
        return Ok(());
    }

    let status = unsafe {
        sqlite3_auto_extension(Some(std::mem::transmute(
            sqlite3_vec_init as *const (),
        )))
    };
    if status != SQLITE_OK {
        tracing::error!(status, "failed to register sqlite-vec extension");
        return Err(ApiError::internal("Failed to load sqlite-vec extension"));
    }
    let _ = EXT_LOADED.set(());
    Ok(())
}
