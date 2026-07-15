//! Policy-scoped Relay pairing registry.
//!
//! Actions never pass through Panoptikon: this registry lets any browser
//! profile using the same endpoint recover the credential for the Relay
//! discovered on its own loopback interface.

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::{
    path::PathBuf,
    sync::{Arc, OnceLock},
};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{api_error::ApiError, policy::PolicyContext, proxy::ProxyState};

static STORE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PairingRecord {
    policy: String,
    relay_id: Uuid,
    instance_id: Uuid,
    credential: String,
}

#[derive(Debug, Deserialize)]
pub struct SavePairing {
    instance_id: Uuid,
    credential: String,
}

#[derive(Debug, Serialize)]
pub struct PairingResponse {
    relay_id: Uuid,
    instance_id: Uuid,
    credential: String,
}

fn relay_enabled(state: &ProxyState, context: &PolicyContext) -> Result<(), ApiError> {
    let policy = state
        .settings
        .policies
        .iter()
        .find(|policy| policy.name == context.policy_name)
        .ok_or_else(|| ApiError::internal("matched policy missing from configuration"))?;
    if policy
        .client
        .get("relay_enabled")
        .and_then(serde_json::Value::as_bool)
        == Some(false)
    {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "Relay is disabled for this endpoint",
        ));
    }
    Ok(())
}

fn store_path(state: &ProxyState) -> PathBuf {
    state.settings.data_folder.join("relay-pairings.json")
}

async fn load(path: &std::path::Path) -> Result<Vec<PairingRecord>, ApiError> {
    match tokio::fs::read(path).await {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|error| ApiError::internal(format!("invalid Relay pairing store: {error}"))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(ApiError::internal(format!(
            "failed to read Relay pairing store: {error}"
        ))),
    }
}

async fn save(path: &std::path::Path, records: &[PairingRecord]) -> Result<(), ApiError> {
    let parent = path
        .parent()
        .ok_or_else(|| ApiError::internal("invalid Relay pairing store path"))?;
    tokio::fs::create_dir_all(parent).await.map_err(|error| {
        ApiError::internal(format!("failed to create Relay pairing directory: {error}"))
    })?;
    let tmp = path.with_extension(format!("json.{}.tmp", std::process::id()));
    let bytes = serde_json::to_vec_pretty(records)
        .map_err(|error| ApiError::internal(format!("failed to encode Relay pairings: {error}")))?;
    tokio::fs::write(&tmp, bytes).await.map_err(|error| {
        ApiError::internal(format!("failed to write Relay pairing store: {error}"))
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        tokio::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))
            .await
            .map_err(|error| {
                ApiError::internal(format!("failed to protect Relay pairing store: {error}"))
            })?;
    }
    if let Err(error) = tokio::fs::rename(&tmp, path).await {
        if path.exists() {
            tokio::fs::remove_file(path).await.ok();
            tokio::fs::rename(&tmp, path).await.map_err(|retry| {
                ApiError::internal(format!(
                    "failed to commit Relay pairing store: {error}; {retry}"
                ))
            })?;
        } else {
            return Err(ApiError::internal(format!(
                "failed to commit Relay pairing store: {error}"
            )));
        }
    }
    Ok(())
}

fn no_store<T: IntoResponse>(body: T) -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-store")], body)
}

pub async fn get_pairing(
    State(state): State<Arc<ProxyState>>,
    Extension(context): Extension<PolicyContext>,
    Path(relay_id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    relay_enabled(&state, &context)?;
    let _guard = STORE_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    let records = load(&store_path(&state)).await?;
    let Some(record) = records
        .into_iter()
        .find(|item| item.policy == context.policy_name && item.relay_id == relay_id)
    else {
        return Ok(no_store(StatusCode::NOT_FOUND.into_response()));
    };
    Ok(no_store(
        Json(PairingResponse {
            relay_id,
            instance_id: record.instance_id,
            credential: record.credential,
        })
        .into_response(),
    ))
}

pub async fn put_pairing(
    State(state): State<Arc<ProxyState>>,
    Extension(context): Extension<PolicyContext>,
    Path(relay_id): Path<Uuid>,
    _headers: HeaderMap,
    Json(input): Json<SavePairing>,
) -> Result<impl IntoResponse, ApiError> {
    relay_enabled(&state, &context)?;
    if input.credential.len() < 32 || input.credential.len() > 512 {
        return Err(ApiError::bad_request("invalid Relay credential"));
    }
    let _guard = STORE_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    let path = store_path(&state);
    let mut records = load(&path).await?;
    records.retain(|item| !(item.policy == context.policy_name && item.relay_id == relay_id));
    records.push(PairingRecord {
        policy: context.policy_name,
        relay_id,
        instance_id: input.instance_id,
        credential: input.credential,
    });
    save(&path, &records).await?;
    Ok(no_store(StatusCode::NO_CONTENT))
}

pub async fn delete_pairing(
    State(state): State<Arc<ProxyState>>,
    Extension(context): Extension<PolicyContext>,
    Path(relay_id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    relay_enabled(&state, &context)?;
    let _guard = STORE_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    let path = store_path(&state);
    let mut records = load(&path).await?;
    records.retain(|item| !(item.policy == context.policy_name && item.relay_id == relay_id));
    save(&path, &records).await?;
    Ok(no_store(StatusCode::NO_CONTENT))
}
