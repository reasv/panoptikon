//! Policy-scoped Relay pairing registry and durable pairing operations.
//!
//! File actions never pass through Panoptikon. The registry lets browsers
//! recover a Relay credential and resume an interrupted pairing operation.

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::{StatusCode, header},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::{
    path::PathBuf,
    sync::{Arc, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{api_error::ApiError, policy::PolicyContext, proxy::ProxyState};

static STORE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
const OPERATION_TTL_SECS: i64 = 10 * 60;
const MAX_PENDING_OPERATIONS: usize = 256;
const MAX_PENDING_OPERATIONS_PER_POLICY: usize = 64;
const MAX_PAIRINGS: usize = 4096;
const MAX_PAIRINGS_PER_POLICY: usize = 2048;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PairingRecord {
    policy: String,
    relay_id: Uuid,
    instance_id: Uuid,
    credential: String,
    #[serde(default)]
    operation_id: Option<Uuid>,
    created_unix: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PairingOperation {
    policy: String,
    relay_id: Uuid,
    operation_id: Uuid,
    created_unix: i64,
    expires_unix: i64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct PairingStore {
    #[serde(default)]
    pairings: Vec<PairingRecord>,
    #[serde(default)]
    operations: Vec<PairingOperation>,
}

#[derive(Debug, Deserialize)]
pub struct CommitPairing {
    relay_id: Uuid,
    instance_id: Uuid,
    credential: String,
}

#[derive(Debug, Serialize)]
pub struct PairingResponse {
    relay_id: Uuid,
    instance_id: Uuid,
    credential: String,
    operation_id: Option<Uuid>,
}

#[derive(Debug, Serialize)]
pub struct PairingOperationResponse {
    relay_id: Uuid,
    operation_id: Uuid,
    expires_unix: i64,
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
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

fn prune_operations(store: &mut PairingStore, now: i64) -> bool {
    let old_len = store.operations.len();
    store.operations.retain(|item| item.expires_unix > now);
    old_len != store.operations.len()
}

fn operation_capacity_available(store: &PairingStore, policy: &str) -> bool {
    store.operations.len() < MAX_PENDING_OPERATIONS
        && store
            .operations
            .iter()
            .filter(|item| item.policy == policy)
            .count()
            < MAX_PENDING_OPERATIONS_PER_POLICY
}

fn pairing_capacity_available(store: &PairingStore, policy: &str) -> bool {
    store.pairings.len() < MAX_PAIRINGS
        && store
            .pairings
            .iter()
            .filter(|item| item.policy == policy)
            .count()
            < MAX_PAIRINGS_PER_POLICY
}

async fn load(path: &std::path::Path) -> Result<PairingStore, ApiError> {
    match tokio::fs::read(path).await {
        Ok(bytes) => match serde_json::from_slice(&bytes) {
            Ok(store) => Ok(store),
            Err(primary) => {
                // Upgrade the initial Relay implementation's top-level array.
                let legacy =
                    serde_json::from_slice::<Vec<serde_json::Value>>(&bytes).map_err(|_| {
                        ApiError::internal(format!("invalid Relay pairing store: {primary}"))
                    })?;
                let now = unix_now();
                let mut pairings = Vec::with_capacity(legacy.len());
                for item in legacy {
                    pairings.push(PairingRecord {
                        policy: item
                            .get("policy")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_owned(),
                        relay_id: serde_json::from_value(
                            item.get("relay_id").cloned().unwrap_or_default(),
                        )
                        .map_err(|_| ApiError::internal("invalid legacy Relay ID"))?,
                        instance_id: serde_json::from_value(
                            item.get("instance_id").cloned().unwrap_or_default(),
                        )
                        .map_err(|_| ApiError::internal("invalid legacy Relay instance ID"))?,
                        credential: item
                            .get("credential")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_owned(),
                        operation_id: None,
                        created_unix: now,
                    });
                }
                Ok(PairingStore {
                    pairings,
                    operations: Vec::new(),
                })
            }
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(PairingStore::default()),
        Err(error) => Err(ApiError::internal(format!(
            "failed to read Relay pairing store: {error}"
        ))),
    }
}

async fn save(path: &std::path::Path, store: &PairingStore) -> Result<(), ApiError> {
    let parent = path
        .parent()
        .ok_or_else(|| ApiError::internal("invalid Relay pairing store path"))?;
    tokio::fs::create_dir_all(parent).await.map_err(|error| {
        ApiError::internal(format!("failed to create Relay pairing directory: {error}"))
    })?;
    let tmp = path.with_extension(format!("json.{}.tmp", std::process::id()));
    let bytes = serde_json::to_vec_pretty(store)
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
    let path = store_path(&state);
    let mut store = load(&path).await?;
    if prune_operations(&mut store, unix_now()) {
        save(&path, &store).await?;
    }
    let Some(record) = store
        .pairings
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
            operation_id: record.operation_id,
        })
        .into_response(),
    ))
}

pub async fn get_pairing_operation(
    State(state): State<Arc<ProxyState>>,
    Extension(context): Extension<PolicyContext>,
    Path(relay_id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    relay_enabled(&state, &context)?;
    let _guard = STORE_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    let path = store_path(&state);
    let mut store = load(&path).await?;
    let changed = prune_operations(&mut store, unix_now());
    let operation = store
        .operations
        .iter()
        .find(|item| item.policy == context.policy_name && item.relay_id == relay_id)
        .cloned();
    if changed {
        save(&path, &store).await?;
    }
    let Some(operation) = operation else {
        return Ok(no_store(StatusCode::NOT_FOUND.into_response()));
    };
    Ok(no_store(
        Json(PairingOperationResponse {
            relay_id,
            operation_id: operation.operation_id,
            expires_unix: operation.expires_unix,
        })
        .into_response(),
    ))
}

pub async fn begin_pairing_operation(
    State(state): State<Arc<ProxyState>>,
    Extension(context): Extension<PolicyContext>,
    Path(relay_id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    relay_enabled(&state, &context)?;
    let _guard = STORE_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    let path = store_path(&state);
    let mut store = load(&path).await?;
    let now = unix_now();
    if prune_operations(&mut store, now) {
        // Persist garbage collection even when this request is an idempotent
        // read or is rejected by one of the bounds below.
        save(&path, &store).await?;
    }
    if let Some(operation) = store
        .operations
        .iter()
        .find(|item| item.policy == context.policy_name && item.relay_id == relay_id)
    {
        return Ok(no_store(
            Json(PairingOperationResponse {
                relay_id,
                operation_id: operation.operation_id,
                expires_unix: operation.expires_unix,
            })
            .into_response(),
        ));
    }
    if !operation_capacity_available(&store, &context.policy_name) {
        return Err(ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "too many pending Relay pairing operations",
        ));
    }
    let operation = PairingOperation {
        policy: context.policy_name,
        relay_id,
        operation_id: Uuid::new_v4(),
        created_unix: now,
        expires_unix: now + OPERATION_TTL_SECS,
    };
    let response = PairingOperationResponse {
        relay_id,
        operation_id: operation.operation_id,
        expires_unix: operation.expires_unix,
    };
    store.operations.push(operation);
    save(&path, &store).await?;
    Ok(no_store(
        (StatusCode::ACCEPTED, Json(response)).into_response(),
    ))
}

pub async fn commit_pairing_operation(
    State(state): State<Arc<ProxyState>>,
    Extension(context): Extension<PolicyContext>,
    Path(operation_id): Path<Uuid>,
    Json(input): Json<CommitPairing>,
) -> Result<impl IntoResponse, ApiError> {
    relay_enabled(&state, &context)?;
    if input.credential.len() < 32 || input.credential.len() > 512 {
        return Err(ApiError::bad_request("invalid Relay credential"));
    }
    let _guard = STORE_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    let path = store_path(&state);
    let mut store = load(&path).await?;
    prune_operations(&mut store, unix_now());

    if let Some(existing) = store
        .pairings
        .iter()
        .find(|item| item.policy == context.policy_name && item.relay_id == input.relay_id)
    {
        if existing.operation_id == Some(operation_id)
            && existing.instance_id == input.instance_id
            && existing.credential == input.credential
        {
            store
                .operations
                .retain(|item| item.operation_id != operation_id);
            save(&path, &store).await?;
            return Ok(no_store(StatusCode::NO_CONTENT));
        }
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "Relay pairing already changed",
        ));
    }

    let Some(operation) = store.operations.iter().find(|item| {
        item.operation_id == operation_id
            && item.policy == context.policy_name
            && item.relay_id == input.relay_id
    }) else {
        return Err(ApiError::new(
            StatusCode::GONE,
            "Relay pairing operation expired",
        ));
    };
    let created_unix = operation.created_unix;
    if !pairing_capacity_available(&store, &context.policy_name) {
        return Err(ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "too many stored Relay pairings",
        ));
    }
    store.pairings.push(PairingRecord {
        policy: context.policy_name,
        relay_id: input.relay_id,
        instance_id: input.instance_id,
        credential: input.credential,
        operation_id: Some(operation_id),
        created_unix,
    });
    store
        .operations
        .retain(|item| item.operation_id != operation_id);
    save(&path, &store).await?;
    Ok(no_store(StatusCode::NO_CONTENT))
}

pub async fn cancel_pairing_operation(
    State(state): State<Arc<ProxyState>>,
    Extension(context): Extension<PolicyContext>,
    Path(operation_id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    relay_enabled(&state, &context)?;
    let _guard = STORE_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    let path = store_path(&state);
    let mut store = load(&path).await?;
    store
        .operations
        .retain(|item| !(item.operation_id == operation_id && item.policy == context.policy_name));
    prune_operations(&mut store, unix_now());
    save(&path, &store).await?;
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
    let mut store = load(&path).await?;
    store
        .pairings
        .retain(|item| !(item.policy == context.policy_name && item.relay_id == relay_id));
    store
        .operations
        .retain(|item| !(item.policy == context.policy_name && item.relay_id == relay_id));
    save(&path, &store).await?;
    Ok(no_store(StatusCode::NO_CONTENT))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Expired operations are removed while durable pairings remain intact.
    #[test]
    fn operation_gc_is_bounded_and_does_not_remove_pairings() {
        let relay_id = Uuid::new_v4();
        let mut store = PairingStore {
            pairings: vec![PairingRecord {
                policy: "public".into(),
                relay_id,
                instance_id: Uuid::new_v4(),
                credential: "credential".into(),
                operation_id: None,
                created_unix: 1,
            }],
            operations: vec![
                PairingOperation {
                    policy: "public".into(),
                    relay_id,
                    operation_id: Uuid::new_v4(),
                    created_unix: 1,
                    expires_unix: 9,
                },
                PairingOperation {
                    policy: "public".into(),
                    relay_id: Uuid::new_v4(),
                    operation_id: Uuid::new_v4(),
                    created_unix: 8,
                    expires_unix: 20,
                },
            ],
        };
        assert!(prune_operations(&mut store, 10));
        assert_eq!(store.operations.len(), 1);
        assert_eq!(store.pairings.len(), 1);
    }

    #[test]
    fn registry_capacity_is_bounded_per_policy_and_globally() {
        let operation = |policy: &str| PairingOperation {
            policy: policy.into(),
            relay_id: Uuid::new_v4(),
            operation_id: Uuid::new_v4(),
            created_unix: 1,
            expires_unix: 2,
        };
        let pairing = |policy: &str| PairingRecord {
            policy: policy.into(),
            relay_id: Uuid::new_v4(),
            instance_id: Uuid::new_v4(),
            credential: "credential".into(),
            operation_id: Some(Uuid::new_v4()),
            created_unix: 1,
        };

        let mut store = PairingStore::default();
        store.operations = (0..MAX_PENDING_OPERATIONS_PER_POLICY)
            .map(|_| operation("public"))
            .collect();
        assert!(!operation_capacity_available(&store, "public"));
        assert!(operation_capacity_available(&store, "private"));
        store.operations = (0..MAX_PENDING_OPERATIONS)
            .map(|_| operation("private"))
            .collect();
        assert!(!operation_capacity_available(&store, "another"));

        store.pairings = (0..MAX_PAIRINGS_PER_POLICY)
            .map(|_| pairing("public"))
            .collect();
        assert!(!pairing_capacity_available(&store, "public"));
        assert!(pairing_capacity_available(&store, "private"));
        store.pairings = (0..MAX_PAIRINGS).map(|_| pairing("private")).collect();
        assert!(!pairing_capacity_available(&store, "another"));
    }
}
