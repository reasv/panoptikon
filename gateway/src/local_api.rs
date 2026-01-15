use anyhow::{Context, Result};
use axum::{
    Json,
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    response::IntoResponse,
};
use std::{env, fs, path::PathBuf, sync::Arc};

use crate::proxy::{
    DbInfo, ProxyState, SingleDbInfo, extract_username, filter_db_info_payload,
    resolve_effective_host, ruleset_allows, select_policy,
};

pub async fn db_info(
    State(state): State<Arc<ProxyState>>,
    req: Request<Body>,
) -> impl IntoResponse {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let effective_host =
        resolve_effective_host(&req, state.settings.server.trust_forwarded_headers);
    let policy = match select_policy(&state.settings, effective_host.as_deref()) {
        Some(policy) => policy,
        None => {
            tracing::warn!(
                method = %method,
                path = %path,
                host = effective_host.as_deref().unwrap_or("<missing>"),
                "request denied: no policy matched"
            );
            return StatusCode::FORBIDDEN.into_response();
        }
    };

    if !ruleset_allows(&state.settings, policy, &method, &path) {
        tracing::warn!(
            method = %method,
            path = %path,
            policy = %policy.name,
            "request denied: ruleset"
        );
        return StatusCode::FORBIDDEN.into_response();
    }

    let username = match extract_username(policy, &req) {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(
                method = %method,
                path = %path,
                policy = %policy.name,
                reason = error.reason,
                "request denied: invalid username"
            );
            return error.status.into_response();
        }
    };

    let info = match load_db_info() {
        Ok(info) => info,
        Err(err) => {
            tracing::error!(error = %err, "failed to load db info");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let info = match filter_db_info_payload(info, policy, username.as_deref()) {
        Ok(info) => info,
        Err(error) => {
            tracing::warn!(
                method = %method,
                path = %path,
                policy = %policy.name,
                reason = error.reason,
                "request denied: db info filtering"
            );
            return error.status.into_response();
        }
    };

    tracing::info!(
        method = %method,
        path = %path,
        policy = %policy.name,
        "served local /api/db"
    );

    Json(info).into_response()
}

fn load_db_info() -> Result<DbInfo> {
    let (index_default, user_default) = db_defaults();
    let (index_dbs, user_data_dbs) = db_lists()?;
    Ok(DbInfo {
        index: SingleDbInfo {
            current: index_default,
            all: index_dbs,
        },
        user_data: SingleDbInfo {
            current: user_default,
            all: user_data_dbs,
        },
    })
}

fn db_defaults() -> (String, String) {
    let index_default = env::var("INDEX_DB").unwrap_or_else(|_| "default".to_string());
    let user_default = env::var("USER_DATA_DB").unwrap_or_else(|_| "default".to_string());
    (index_default, user_default)
}

fn db_lists() -> Result<(Vec<String>, Vec<String>)> {
    let data_dir = PathBuf::from(env::var("DATA_FOLDER").unwrap_or_else(|_| "data".to_string()));
    let index_dir = data_dir.join("index");
    let user_data_dir = data_dir.join("user_data");

    fs::create_dir_all(&index_dir)
        .with_context(|| format!("failed to create index dir {}", index_dir.display()))?;
    fs::create_dir_all(&user_data_dir)
        .with_context(|| format!("failed to create user data dir {}", user_data_dir.display()))?;

    let mut index_dbs = Vec::new();
    for entry in fs::read_dir(&index_dir)
        .with_context(|| format!("failed to read index dir {}", index_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() && path.join("index.db").exists() {
            if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
                index_dbs.push(name.to_string());
            }
        }
    }

    let mut user_data_dbs = Vec::new();
    for entry in fs::read_dir(&user_data_dir)
        .with_context(|| format!("failed to read user data dir {}", user_data_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("db") {
            if let Some(stem) = path.file_stem().and_then(|name| name.to_str()) {
                user_data_dbs.push(stem.to_string());
            }
        }
    }

    Ok((index_dbs, user_data_dbs))
}
