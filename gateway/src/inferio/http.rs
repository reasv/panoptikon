//! HTTP surface of the local inferio orchestrator: a wire-compatible port
//! of `src/inferio/router.py` + `src/inferio/utils.py` (design doc §7).
//!
//! Mounted (via `nest_service`) under `/api/inference`, exactly where the
//! proxy used to forward these paths, and *behind the same policy layer*
//! (which strips `index_db`/`user_data_db` for inference paths). The
//! gateway's own `InferenceApiClient` (`inferio_client.rs`) is the parity
//! oracle: everything encoded here must round-trip through that client
//! unchanged.
//!
//! Wire formats replicated exactly from Python:
//! - predict request: multipart form with a `data` field holding a JSON
//!   string `{"inputs": [...]}` (entries: object | string | null) and
//!   `files` parts whose *filenames* are integer batch indices.
//! - predict response: single binary output -> `application/octet-stream`;
//!   all-binary -> `multipart/mixed; boundary=multipart-boundary` with
//!   Python's exact part headers (`Content-Type: application/octet-stream`,
//!   `Content-Disposition: attachment; filename="output{i}.bin"`); anything
//!   else -> JSON `{"outputs": [...]}` where bytes entries become
//!   `{"__type__": "base64", "content": ...}`.
//! - `GET /cache/{key}` renders a never-expiring entry as Python's
//!   `datetime.max.isoformat()` literal `9999-12-31T23:59:59.999999`.
//! - errors use FastAPI's `{"detail": ...}` shape (`ApiError`), with
//!   router.py's exact detail strings for the 500s.
//!
//! Additive (design §7/§8): optional `max_batch` query param on predict
//! (forwarded to the dispatcher's merge cap), optional `prewarm` query
//! param on load AND predict (the lazy-warm hint, absent = true — see
//! `prewarm.rs`), and `GET /health`
//! (orchestrator + per-model liveness, queue depths, batch caps — see
//! [`ModelManager::health`]). `/health` lives on the nested router, so it
//! is `/api/inference/health` in gateway mode and subcommand mode alike;
//! standalone mode additionally keeps the original bare `/health` path
//! (same handler) for anything already probing the subcommand there.
//! When `inference_local` is disabled, `/api/inference/health` proxies
//! upstream like every other inference path (a Python upstream 404s it —
//! fine, the endpoint has no Python counterpart).

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use anyhow::Result;
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Multipart, Path, Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};

use super::manager::{HealthReport, ManagerConfig, ModelManager};
use super::prewarm::PrewarmConfig;
use super::registry::{RegistryCache, RegistryConfig};
use super::worker::{WorkerDeadlines, WorkerInput, WorkerOutput, WorkerSpawnConfig};
use crate::api_error::ApiError;
use crate::config::Settings;

/// Python renders "never expires" as `datetime.max.isoformat()`.
const NEVER_EXPIRES: &str = "9999-12-31T23:59:59.999999";

/// Shared state of the local inference service: the model manager plus the
/// mtime-cached registry used by `/metadata`.
pub struct InferioState {
    pub manager: Arc<ModelManager>,
    pub registry: Arc<StdMutex<RegistryCache>>,
}

impl InferioState {
    /// Build the manager + registry from `[inference_local]` config.
    /// Requires a running tokio runtime (the manager spawns its TTL
    /// sweeper). Workers spawn lazily, so a missing interpreter or impl dir
    /// only surfaces on the first model load.
    pub fn from_settings(settings: &Settings) -> Result<Arc<Self>> {
        let local = &settings.inference_local;
        let registry_config = if local.config_dirs.is_empty() {
            RegistryConfig::default_dirs().unwrap_or_else(|err| {
                // A missing built-in config folder must not hard-fail
                // gateway boot: Python only surfaces it when the registry
                // is actually read, and broken registry TOML already
                // degrades lazily here too (/metadata and loads error per
                // call). Warn and continue with the user dir only — a
                // missing dir is skipped with a warning at load time, so
                // the worst case is an empty registry.
                tracing::warn!(
                    error = %format!("{err:#}"),
                    "built-in inference config folder not found; serving with \
                     the user config dir only (registry may be empty)"
                );
                RegistryConfig {
                    config_dirs: vec![
                        std::env::var_os("INFERIO_CONFIG_DIR")
                            .map(std::path::PathBuf::from)
                            .unwrap_or_else(|| std::path::PathBuf::from("config/inference")),
                    ],
                }
            })
        } else {
            RegistryConfig {
                config_dirs: local.config_dirs.clone(),
            }
        };
        let registry = Arc::new(StdMutex::new(RegistryCache::new(registry_config)));

        let mut deadlines = WorkerDeadlines::default();
        if let Some(secs) = local.handshake_secs {
            deadlines.handshake = Duration::from_secs(secs);
        }
        if let Some(secs) = local.load_secs {
            deadlines.load = Duration::from_secs(secs);
        }
        if let Some(secs) = local.unload_grace_secs {
            deadlines.unload_grace = Duration::from_secs(secs);
        }
        if let Some(secs) = local.terminate_grace_secs {
            deadlines.terminate_grace = Duration::from_secs(secs);
        }

        let spawn = WorkerSpawnConfig {
            python: local.resolved_python(),
            impl_dirs: local.resolved_impl_dirs(),
            pythonpath: local.resolved_pythonpath(),
            env: Vec::new(),
            cwd: None,
            deadlines,
        };
        let manager = ModelManager::new(
            ManagerConfig {
                spawn,
                default_max_batch: local.default_max_batch,
                sweep_interval: Duration::from_secs(local.sweep_interval_secs.max(1)),
                prewarm: PrewarmConfig {
                    enabled: local.prewarm.enabled,
                    lazy: local.prewarm.lazy,
                    always_warm: local.prewarm.always_warm.clone(),
                },
            },
            Arc::clone(&registry),
        );
        Ok(Arc::new(Self { manager, registry }))
    }
}

/// The inference routes, path-relative so they can be nested under
/// `/api/inference` (gateway and standalone mode mount the same router).
/// The body limit is disabled to match the proxy path, which streamed
/// request bodies without any size cap (predict batches carry raw images).
pub fn router(state: Arc<InferioState>) -> Router {
    Router::new()
        .route("/predict/{group}/{inference_id}", post(predict))
        .route("/load/{group}/{inference_id}", put(load_model))
        .route(
            "/cache/{cache_key}/{group}/{inference_id}",
            delete(unload_model),
        )
        .route(
            "/cache/{cache_key}",
            get(get_cache_expiration).delete(clear_cache),
        )
        .route("/cache", get(get_cached_models))
        .route("/metadata", get(get_metadata))
        .route("/health", get(health))
        .layer(DefaultBodyLimit::disable())
        .with_state(state)
}

/// Router for the `inferio` subcommand (design §3 "GPU lender" mode): the
/// inference surface (which includes `/api/inference/health`) plus the
/// original bare `/health` path — same handler, kept so existing probes of
/// the subcommand keep working.
pub fn standalone_router(state: Arc<InferioState>) -> Router {
    Router::new()
        .nest_service("/api/inference", router(Arc::clone(&state)))
        .route("/health", get(health))
        .with_state(state)
}

#[derive(Debug, Deserialize)]
struct LoadParams {
    cache_key: String,
    lru_size: i64,
    ttl_seconds: i64,
    /// Additive over Python: lazy prewarm hint (design §8). Absent = true;
    /// `prewarm=false` suppresses keeping a warm worker of this model's
    /// impl class after the load.
    prewarm: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct PredictParams {
    cache_key: String,
    lru_size: i64,
    ttl_seconds: i64,
    /// Additive over Python: per-request cap on dispatch-time batch merging.
    max_batch: Option<u32>,
    /// Additive over Python: lazy prewarm hint, as on load (absent = true).
    prewarm: Option<bool>,
}

/// `POST /predict/{group}/{inference_id}` — router.py `predict`.
/// Parses the multipart request, auto-loads the model (pinned for the
/// duration, TTL restored afterwards — the manager owns those semantics),
/// runs the batch, and encodes the response exactly like
/// `utils.encode_output_response`.
async fn predict(
    State(state): State<Arc<InferioState>>,
    Path((group, inference_id)): Path<(String, String)>,
    Query(params): Query<PredictParams>,
    mut multipart: Multipart,
) -> Result<Response, ApiError> {
    let mut data: Option<String> = None;
    let mut files: Vec<(Option<i64>, Vec<u8>)> = Vec::new();
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|err| ApiError::bad_request(format!("invalid multipart body: {err}")))?
    {
        match field.name() {
            Some("data") => {
                data =
                    Some(field.text().await.map_err(|err| {
                        ApiError::bad_request(format!("invalid data field: {err}"))
                    })?);
            }
            Some("files") => {
                // Python maps each file to its batch slot via the filename,
                // which must be an integer index (utils.py:19-31).
                let index = field
                    .file_name()
                    .and_then(|name| name.trim().trim_matches('"').parse::<i64>().ok());
                let bytes = field
                    .bytes()
                    .await
                    .map_err(|err| ApiError::bad_request(format!("invalid file field: {err}")))?;
                files.push((index, bytes.to_vec()));
            }
            // FastAPI ignores unknown form fields; so do we.
            _ => {}
        }
    }
    let data = data.ok_or_else(|| {
        // FastAPI answers a missing required Form field with 422.
        ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, "Field required: data")
    })?;
    let inputs = parse_input_request(&data, files)?;

    let full_id = format!("{group}/{inference_id}");
    tracing::debug!(
        model = %full_id,
        inputs = inputs.len(),
        "processing local inference predict"
    );
    let outputs = state
        .manager
        .predict(
            &full_id,
            &params.cache_key,
            params.lru_size,
            params.ttl_seconds,
            params.max_batch,
            params.prewarm,
            inputs,
        )
        .await
        .map_err(|err| {
            let chain = format!("{err:#}");
            tracing::error!(model = %full_id, error = %chain, "prediction failed");
            // router.py detail strings: load failures vs. predict failures.
            if chain.contains("failed to load model") {
                ApiError::internal("Failed to load model")
            } else {
                ApiError::internal("Prediction failed")
            }
        })?;
    Ok(encode_output_response(outputs))
}

/// `PUT /load/{group}/{inference_id}` — router.py `load_model`:
/// `{"status": "loaded"}` on success, 500 `"Failed to load model"` on any
/// error (details go to the log, like Python's `logger.error`).
async fn load_model(
    State(state): State<Arc<InferioState>>,
    Path((group, inference_id)): Path<(String, String)>,
    Query(params): Query<LoadParams>,
) -> Result<Json<JsonValue>, ApiError> {
    let full_id = format!("{group}/{inference_id}");
    state
        .manager
        .load_model(
            &full_id,
            &params.cache_key,
            params.lru_size,
            params.ttl_seconds,
            params.prewarm,
        )
        .await
        .map_err(|err| {
            tracing::error!(model = %full_id, error = %format!("{err:#}"), "failed to load model");
            ApiError::internal("Failed to load model")
        })?;
    Ok(Json(json!({"status": "loaded"})))
}

/// `DELETE /cache/{cache_key}/{group}/{inference_id}` — router.py
/// `unload_model`: always `{"status": "unloaded"}` (Python doesn't report
/// whether the entry existed).
async fn unload_model(
    State(state): State<Arc<InferioState>>,
    Path((cache_key, group, inference_id)): Path<(String, String, String)>,
) -> Result<Json<JsonValue>, ApiError> {
    let full_id = format!("{group}/{inference_id}");
    state
        .manager
        .unload_model(&cache_key, &full_id)
        .await
        .map_err(|err| ApiError::internal(format!("failed to unload model: {err:#}")))?;
    Ok(Json(json!({"status": "unloaded"})))
}

/// `DELETE /cache/{cache_key}` — router.py `clear_cache`:
/// `{"status": "cleared"}`.
async fn clear_cache(
    State(state): State<Arc<InferioState>>,
    Path(cache_key): Path<String>,
) -> Result<Json<JsonValue>, ApiError> {
    state
        .manager
        .clear_cache(&cache_key)
        .await
        .map_err(|err| ApiError::internal(format!("failed to clear cache: {err:#}")))?;
    Ok(Json(json!({"status": "cleared"})))
}

/// `GET /cache/{cache_key}` — router.py `get_cache_expiration`:
/// `{"expirations": {id: isoformat}}`, with `datetime.max` for ttl -1.
async fn get_cache_expiration(
    State(state): State<Arc<InferioState>>,
    Path(cache_key): Path<String>,
) -> Json<JsonValue> {
    let expirations: serde_json::Map<String, JsonValue> = state
        .manager
        .cache_expirations(&cache_key)
        .into_iter()
        .map(|(inference_id, expiration)| {
            (
                inference_id,
                JsonValue::String(expiration.unwrap_or_else(|| NEVER_EXPIRES.to_string())),
            )
        })
        .collect();
    Json(json!({"expirations": expirations}))
}

/// `GET /cache` — router.py `get_cached_models`:
/// `{"cache": {inference_id: [cache_keys]}}`.
async fn get_cached_models(State(state): State<Arc<InferioState>>) -> Json<JsonValue> {
    Json(json!({"cache": state.manager.cached_models()}))
}

/// `GET /metadata` — router.py `get_metadata`: mtime-gated registry reload
/// (RegistryCache mirrors `load_config(config, mtime)`), then the
/// `list_inference_ids` shape.
async fn get_metadata(State(state): State<Arc<InferioState>>) -> Result<Json<JsonValue>, ApiError> {
    let snapshot = state.registry.lock().unwrap().get();
    match snapshot {
        Ok(registry) => Ok(Json(registry.metadata_json())),
        Err(err) => {
            tracing::error!(error = %format!("{err:#}"), "failed to load inference registry");
            Err(ApiError::internal("Failed to load inference metadata"))
        }
    }
}

/// `GET /health` (additive, design §7; no Python counterpart): orchestrator
/// + per-model liveness, loaded models, queue depths, and batch caps — the
/// serde shape is [`HealthReport`], assembled by [`ModelManager::health`].
/// Supersedes the earlier standalone-only `{"status": "ok", "loaded": ...}`
/// body: the loaded-model map is now the richer `models` array.
async fn health(State(state): State<Arc<InferioState>>) -> Json<HealthReport> {
    Json(state.manager.health())
}

/// Port of `utils.parse_input_request`: the `data` form field is a JSON
/// string whose `inputs` array defines the batch (missing key -> empty ->
/// 400 "No inputs provided"); each uploaded file is attached to the batch
/// slot named by its integer filename, anything unmappable is Python's
/// exact 400 `Invalid index {index} in Content-Disposition header` (with
/// `None` for a missing/non-integer filename).
fn parse_input_request(
    data: &str,
    files: Vec<(Option<i64>, Vec<u8>)>,
) -> Result<Vec<WorkerInput>, ApiError> {
    let parsed: JsonValue = serde_json::from_str(data)
        .map_err(|err| ApiError::bad_request(format!("invalid JSON in data field: {err}")))?;
    let raw_inputs = match parsed.get("inputs") {
        None => Vec::new(),
        Some(JsonValue::Array(items)) => items.clone(),
        Some(_) => return Err(ApiError::bad_request("inputs must be an array")),
    };
    let mut inputs: Vec<WorkerInput> = raw_inputs
        .into_iter()
        .map(|item| WorkerInput {
            // JSON null means "file-only input" (PredictionInput.data=None).
            data: if item.is_null() { None } else { Some(item) },
            file: None,
        })
        .collect();
    if inputs.is_empty() {
        return Err(ApiError::bad_request("No inputs provided"));
    }
    for (index, bytes) in files {
        let slot = index
            .and_then(|idx| usize::try_from(idx).ok())
            .filter(|idx| *idx < inputs.len());
        match slot {
            Some(idx) => inputs[idx].file = Some(bytes),
            None => {
                let rendered = index.map_or_else(|| "None".to_string(), |value| value.to_string());
                return Err(ApiError::bad_request(format!(
                    "Invalid index {rendered} in Content-Disposition header"
                )));
            }
        }
    }
    Ok(inputs)
}

/// Port of `utils.encode_output_response`, byte-for-byte:
/// - exactly one binary output -> raw `application/octet-stream` body;
/// - all outputs binary -> `multipart/mixed; boundary=multipart-boundary`
///   with Python's literal part framing (see module docs);
/// - otherwise JSON `{"outputs": [...]}` with bytes entries wrapped as
///   `{"__type__": "base64", "content": ...}`.
fn encode_output_response(outputs: Vec<WorkerOutput>) -> Response {
    if outputs.len() == 1 && matches!(outputs[0], WorkerOutput::Bytes(_)) {
        let WorkerOutput::Bytes(bytes) = outputs.into_iter().next().expect("len checked") else {
            unreachable!("variant checked above");
        };
        return ([(header::CONTENT_TYPE, "application/octet-stream")], bytes).into_response();
    }

    if outputs
        .iter()
        .all(|output| matches!(output, WorkerOutput::Bytes(_)))
    {
        // Python uses this fixed boundary (utils.py:44); the client's
        // parser reads it back out of the Content-Type header either way.
        const BOUNDARY: &str = "multipart-boundary";
        let mut body: Vec<u8> = Vec::new();
        for (idx, output) in outputs.iter().enumerate() {
            let WorkerOutput::Bytes(bytes) = output else {
                unreachable!("all-bytes checked above");
            };
            body.extend_from_slice(
                format!(
                    "--{BOUNDARY}\r\nContent-Type: application/octet-stream\r\n\
                     Content-Disposition: attachment; filename=\"output{idx}.bin\"\r\n\r\n"
                )
                .as_bytes(),
            );
            body.extend_from_slice(bytes);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(format!("--{BOUNDARY}--\r\n").as_bytes());
        return (
            [(
                header::CONTENT_TYPE,
                format!("multipart/mixed; boundary={BOUNDARY}"),
            )],
            body,
        )
            .into_response();
    }

    let encoded: Vec<JsonValue> = outputs
        .into_iter()
        .map(|output| match output {
            WorkerOutput::Json(value) => value,
            WorkerOutput::Bytes(bytes) => json!({
                "__type__": "base64",
                "content": BASE64_STANDARD.encode(&bytes),
            }),
        })
        .collect();
    Json(json!({"outputs": encoded})).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inferio_client::{
        InferenceApiClient, InferenceFile, InferenceInput, PredictOutput, parse_predict_response,
    };
    use axum::body::to_bytes;
    use serde_json::json;
    use std::fs;
    use std::path::{Path, PathBuf};

    // ------------------------------------------------------------------
    // Response-encoding parity (pure): everything encode_output_response
    // produces must be parseable by the gateway client's own parser
    // (parse_predict_response), which was written against the Python
    // server — that makes it the wire-parity oracle.
    // ------------------------------------------------------------------

    async fn split_response(response: Response) -> (String, Vec<u8>) {
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (content_type, body.to_vec())
    }

    /// A single binary output is returned as a raw octet-stream body (the
    /// npy embedding fast path), and the client parses it back as a
    /// one-element Binary batch.
    #[tokio::test]
    async fn single_binary_output_is_octet_stream() {
        let payload = b"\x93NUMPY-not-really".to_vec();
        let response = encode_output_response(vec![WorkerOutput::Bytes(payload.clone())]);
        let (content_type, body) = split_response(response).await;
        assert_eq!(content_type, "application/octet-stream");
        assert_eq!(body, payload);

        match parse_predict_response(&content_type, &body).unwrap() {
            PredictOutput::Binary(outputs) => assert_eq!(outputs, vec![payload]),
            other => panic!("client parsed {other:?}"),
        }
    }

    /// Multiple all-binary outputs use multipart/mixed with Python's exact
    /// framing: fixed boundary, per-part Content-Type + attachment
    /// Content-Disposition with `output{i}.bin` filenames, `\r\n` part
    /// terminators, and a trailing `--boundary--` line. Verified two ways:
    /// byte-for-byte against the literal Python construction, and by
    /// round-tripping through the client's multipart parser.
    #[tokio::test]
    async fn multiple_binary_outputs_match_python_multipart_bytes() {
        let response = encode_output_response(vec![
            WorkerOutput::Bytes(b"AAA".to_vec()),
            WorkerOutput::Bytes(b"BB".to_vec()),
        ]);
        let (content_type, body) = split_response(response).await;
        assert_eq!(content_type, "multipart/mixed; boundary=multipart-boundary");

        let expected: Vec<u8> = [
            &b"--multipart-boundary\r\nContent-Type: application/octet-stream\r\nContent-Disposition: attachment; filename=\"output0.bin\"\r\n\r\nAAA\r\n"[..],
            &b"--multipart-boundary\r\nContent-Type: application/octet-stream\r\nContent-Disposition: attachment; filename=\"output1.bin\"\r\n\r\nBB\r\n"[..],
            &b"--multipart-boundary--\r\n"[..],
        ]
        .concat();
        assert_eq!(body, expected, "byte-for-byte Python framing");

        match parse_predict_response(&content_type, &body).unwrap() {
            PredictOutput::Binary(outputs) => {
                assert_eq!(outputs, vec![b"AAA".to_vec(), b"BB".to_vec()]);
            }
            other => panic!("client parsed {other:?}"),
        }
    }

    /// Mixed JSON + binary outputs fall back to the JSON envelope: bytes
    /// entries become `{"__type__": "base64", "content": ...}` and JSON
    /// entries pass through; the client sees a Json batch.
    #[tokio::test]
    async fn mixed_outputs_encode_binary_as_base64_json() {
        let response = encode_output_response(vec![
            WorkerOutput::Json(json!({"tags": ["a"]})),
            WorkerOutput::Bytes(b"\x01\x02".to_vec()),
        ]);
        let (content_type, body) = split_response(response).await;
        assert!(content_type.contains("application/json"));

        let value: JsonValue = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["outputs"][0], json!({"tags": ["a"]}));
        assert_eq!(value["outputs"][1]["__type__"], "base64");
        assert_eq!(
            BASE64_STANDARD
                .decode(value["outputs"][1]["content"].as_str().unwrap())
                .unwrap(),
            b"\x01\x02"
        );

        match parse_predict_response(&content_type, &body).unwrap() {
            PredictOutput::Json(outputs) => assert_eq!(outputs.len(), 2),
            other => panic!("client parsed {other:?}"),
        }
    }

    /// All-JSON outputs produce the plain `{"outputs": [...]}` envelope
    /// with values untouched.
    #[tokio::test]
    async fn json_outputs_use_outputs_envelope() {
        let response =
            encode_output_response(vec![WorkerOutput::Json(json!({"echo": {"text": "x"}}))]);
        let (content_type, body) = split_response(response).await;
        assert!(content_type.contains("application/json"));
        let value: JsonValue = serde_json::from_slice(&body).unwrap();
        assert_eq!(value, json!({"outputs": [{"echo": {"text": "x"}}]}));
    }

    // ------------------------------------------------------------------
    // Request-parsing parity (pure): the `data` JSON + indexed files ->
    // WorkerInput mapping of utils.parse_input_request.
    // ------------------------------------------------------------------

    /// Files attach to the batch slot named by their integer filename;
    /// data-only entries keep file=None; JSON null entries become
    /// data=None (file-only inputs); string entries stay JSON strings.
    #[test]
    fn multipart_inputs_map_files_by_index() {
        let data = r#"{"inputs": [{"a": 1}, null, "text"]}"#;
        let files = vec![(Some(0), b"f0".to_vec()), (Some(2), b"f2".to_vec())];
        let inputs = parse_input_request(data, files).unwrap();
        assert_eq!(inputs.len(), 3);
        assert_eq!(inputs[0].data, Some(json!({"a": 1})));
        assert_eq!(inputs[0].file, Some(b"f0".to_vec()));
        assert_eq!(inputs[1].data, None, "JSON null -> data None");
        assert_eq!(inputs[1].file, None);
        assert_eq!(inputs[2].data, Some(json!("text")));
        assert_eq!(inputs[2].file, Some(b"f2".to_vec()));
    }

    /// Python's exact 400s: an empty (or missing) inputs array is "No
    /// inputs provided"; an out-of-range index and a non-integer filename
    /// render as `Invalid index {i}` / `Invalid index None`.
    #[test]
    fn multipart_input_errors_match_python_details() {
        let err = parse_input_request(r#"{"inputs": []}"#, vec![]).unwrap_err();
        assert!(format!("{err:?}").contains("No inputs provided"));
        let err = parse_input_request(r#"{}"#, vec![]).unwrap_err();
        assert!(format!("{err:?}").contains("No inputs provided"));

        let err = parse_input_request(r#"{"inputs": [null]}"#, vec![(Some(5), b"x".to_vec())])
            .unwrap_err();
        assert!(
            format!("{err:?}").contains("Invalid index 5 in Content-Disposition header"),
            "unexpected error: {err:?}"
        );
        let err =
            parse_input_request(r#"{"inputs": [null]}"#, vec![(None, b"x".to_vec())]).unwrap_err();
        assert!(
            format!("{err:?}").contains("Invalid index None in Content-Disposition header"),
            "unexpected error: {err:?}"
        );
    }

    // ------------------------------------------------------------------
    // Round-trip integration: real axum server, real worker subprocess,
    // driven end-to-end by the gateway's real InferenceApiClient — proving
    // the existing extraction/PQL/preload/UI consumers work unchanged when
    // the inference upstream is this local implementation.
    // ------------------------------------------------------------------

    fn workspace_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
    }

    /// Same spawn setup as the manager.rs tests: repo venv python, cwd =
    /// repo root, PYTHONPATH=src, NO_CUDNN, fixture impl dir.
    fn test_spawn_config() -> WorkerSpawnConfig {
        let root = workspace_root();
        let python = if cfg!(windows) {
            root.join(".venv/Scripts/python.exe")
        } else {
            root.join(".venv/bin/python")
        };
        if !python.is_file() {
            panic!(
                "inferio http tests need the repo venv interpreter at {} — create the dev venv first",
                python.display()
            );
        }
        WorkerSpawnConfig {
            python,
            impl_dirs: vec![root.join("tests/inferio_worker/fixture_impls")],
            pythonpath: vec![root.join("src")],
            env: vec![("NO_CUDNN".to_owned(), "true".to_owned())],
            cwd: Some(root),
            deadlines: WorkerDeadlines::default(),
        }
    }

    /// In-process server over an ephemeral port, echo fixture registry.
    async fn spawn_test_server() -> (Arc<InferioState>, String, tempfile::TempDir) {
        spawn_test_server_with_registry(
            r#"
[group.echo]
config.impl_class = "echo_test"
[group.echo.inference_ids.test]
metadata.description = "echo fixture"
"#,
        )
        .await
    }

    /// In-process server over an ephemeral port with a caller-supplied
    /// registry TOML (server default_max_batch stays high, 32, so batching
    /// tests can prove caps come from the request, not the server config).
    /// Prewarm pool disabled — the hint-threading test uses
    /// [`spawn_test_server_with_prewarm`].
    async fn spawn_test_server_with_registry(
        registry_toml: &str,
    ) -> (Arc<InferioState>, String, tempfile::TempDir) {
        spawn_test_server_with_prewarm(
            registry_toml,
            PrewarmConfig {
                enabled: false,
                lazy: false,
                always_warm: Vec::new(),
            },
        )
        .await
    }

    async fn spawn_test_server_with_prewarm(
        registry_toml: &str,
        prewarm: PrewarmConfig,
    ) -> (Arc<InferioState>, String, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("registry.toml"), registry_toml).unwrap();
        let registry = Arc::new(StdMutex::new(RegistryCache::new(RegistryConfig {
            config_dirs: vec![dir.path().to_path_buf()],
        })));
        let manager = ModelManager::new(
            ManagerConfig {
                spawn: test_spawn_config(),
                default_max_batch: 32,
                sweep_interval: Duration::from_secs(60),
                prewarm,
            },
            Arc::clone(&registry),
        );
        let state = Arc::new(InferioState { manager, registry });
        let app = Router::new().nest_service("/api/inference", router(Arc::clone(&state)));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (state, format!("http://{addr}"), dir)
    }

    /// The money test: the REAL gateway client (inferio_client.rs) drives
    /// the local HTTP surface end-to-end against a real worker process —
    /// metadata shows the registry group, load answers {"status":"loaded"},
    /// a data-only predict comes back as JSON outputs, a file predict
    /// exercises the binary octet-stream path through the client's own
    /// parser, two file inputs exercise multipart/mixed, /cache reflects
    /// the load, GET /cache/{key} renders ttl=-1 as datetime.max, and
    /// unload empties the cache. Wire compatibility, proven by the
    /// consumer.
    #[tokio::test]
    async fn real_client_roundtrip_against_local_http_service() {
        let (state, base_url, _registry_dir) = spawn_test_server().await;
        let client = InferenceApiClient::new_with_metadata_cache(base_url.clone(), false)
            .expect("client builds");

        // /metadata: the echo group with the Python list_inference_ids shape.
        let metadata = client.get_metadata().await.expect("metadata");
        assert_eq!(
            metadata["echo"]["inference_ids"]["test"]["description"],
            json!("echo fixture")
        );

        // PUT /load: Python's exact status body.
        let loaded = client
            .load_model("echo/test", "key", 10, -1, None)
            .await
            .expect("load");
        assert_eq!(loaded, json!({"status": "loaded"}));

        // Data-only predict -> JSON outputs through the client parser.
        let output = client
            .predict(
                "echo/test",
                "key",
                10,
                -1,
                None,
                None,
                &[InferenceInput::new(json!({"text": "hi"}), None)],
            )
            .await
            .expect("json predict");
        match output {
            PredictOutput::Json(values) => {
                assert_eq!(values, vec![json!({"echo": {"text": "hi"}})]);
            }
            other => panic!("expected Json output, got {other:?}"),
        }

        // Single file input -> echo returns bytes -> octet-stream path.
        let output = client
            .predict(
                "echo/test",
                "key",
                10,
                -1,
                None,
                None,
                &[InferenceInput::new(
                    JsonValue::Null,
                    Some(InferenceFile::Bytes(b"abc".to_vec())),
                )],
            )
            .await
            .expect("binary predict");
        match output {
            PredictOutput::Binary(outputs) => {
                assert_eq!(outputs, vec![b"echo:abc".to_vec()]);
            }
            other => panic!("expected Binary output, got {other:?}"),
        }

        // Two file inputs -> all-bytes -> multipart/mixed path, order kept.
        let output = client
            .predict(
                "echo/test",
                "key",
                10,
                -1,
                None,
                None,
                &[
                    InferenceInput::new(
                        JsonValue::Null,
                        Some(InferenceFile::Bytes(b"one".to_vec())),
                    ),
                    InferenceInput::new(
                        JsonValue::Null,
                        Some(InferenceFile::Bytes(b"two".to_vec())),
                    ),
                ],
            )
            .await
            .expect("multipart predict");
        match output {
            PredictOutput::Binary(outputs) => {
                assert_eq!(outputs, vec![b"echo:one".to_vec(), b"echo:two".to_vec()]);
            }
            other => panic!("expected Binary output, got {other:?}"),
        }

        // GET /cache: the model is referenced by our cache key.
        let cached = client.get_cached_models().await.expect("cache list");
        assert_eq!(cached, json!({"cache": {"echo/test": ["key"]}}));

        // GET /cache/{key} (no client helper): ttl -1 renders as Python's
        // datetime.max isoformat literal.
        let expirations: JsonValue = reqwest::get(format!("{base_url}/api/inference/cache/key"))
            .await
            .expect("cache expiration request")
            .json()
            .await
            .expect("cache expiration json");
        assert_eq!(
            expirations,
            json!({"expirations": {"echo/test": "9999-12-31T23:59:59.999999"}})
        );

        // DELETE /cache/{key}/{group}/{id} then the cache is empty.
        let unloaded = client
            .unload_model("echo/test", "key")
            .await
            .expect("unload");
        assert_eq!(unloaded, json!({"status": "unloaded"}));
        let cached = client.get_cached_models().await.expect("cache list");
        assert_eq!(cached, json!({"cache": {}}));

        state.manager.shutdown().await;
    }

    /// Extracts the `{"batch": n}` sizes the batchsize_test fixture reports
    /// from a client-side PredictOutput.
    fn reported_batches(output: &PredictOutput) -> Vec<u64> {
        match output {
            PredictOutput::Json(values) => values
                .iter()
                .map(|value| value["batch"].as_u64().expect("fixture reports batch"))
                .collect(),
            other => panic!("batchsize fixture returns JSON outputs, got {other:?}"),
        }
    }

    /// Phase 2/3 cap propagation, proven end-to-end through the job stack:
    /// predicts driven through the real InferencePool (which wraps the real
    /// InferenceApiClient over HTTP) carry the extraction job's batch size
    /// to GPU batch formation as `max_batch`.
    ///
    /// Capped phase: a primer request keeps the worker busy (the
    /// batchsize_test fixture sleeps 300ms per batch) while six concurrent
    /// single-input requests, all with max_batch=Some(2), queue up behind
    /// it — every reported GPU batch must be <= 2 even though the server's
    /// own default cap is 32.
    ///
    /// Uncapped contrast phase: the same shape with max_batch=None — the
    /// six queued singles merge freely under the server default, so at
    /// least one reported batch exceeds 2. That proves the capped phase's
    /// ceiling came from the request param, not from timing or server
    /// config.
    #[tokio::test]
    async fn pool_max_batch_caps_gpu_batches_end_to_end() {
        use crate::config::InferenceEndpointConfig;
        use crate::jobs::inference_pool::InferencePool;

        let (state, base_url, _registry_dir) = spawn_test_server_with_registry(
            r#"
[group.batch]
config.impl_class = "batchsize_test"
[group.batch.inference_ids.test]
metadata.description = "batch size reporter"
"#,
        )
        .await;
        let pool = InferencePool::new(vec![InferenceEndpointConfig {
            base_url,
            weight: 1.0,
            use_for_jobs: true,
        }])
        .expect("pool builds");

        // Preload so the primer isn't skewed by worker spawn latency.
        pool.load_model_all("batch/test", "key", 10, -1, None)
            .await
            .expect("load");

        // One primer + six queued single-input predicts, all sharing the
        // given max_batch — returns every reported batch size.
        async fn run_phase(pool: &InferencePool, max_batch: Option<u32>) -> Vec<u64> {
            let primer = {
                let pool = pool.clone();
                tokio::spawn(async move {
                    pool.predict(
                        "batch/test",
                        "key",
                        10,
                        -1,
                        max_batch,
                        None,
                        &[InferenceInput::new(json!(0), None)],
                    )
                    .await
                })
            };
            // Let the primer dispatch alone (worker sleeps 300ms), so the
            // rest are guaranteed to queue and become mergeable.
            tokio::time::sleep(Duration::from_millis(100)).await;
            let mut rest = Vec::new();
            for i in 1..=6 {
                let pool = pool.clone();
                rest.push(tokio::spawn(async move {
                    pool.predict(
                        "batch/test",
                        "key",
                        10,
                        -1,
                        max_batch,
                        None,
                        &[InferenceInput::new(json!(i), None)],
                    )
                    .await
                }));
            }
            let mut batches = reported_batches(&primer.await.unwrap().expect("primer predict"));
            for task in rest {
                batches.extend(reported_batches(
                    &task.await.unwrap().expect("queued predict"),
                ));
            }
            batches
        }

        let capped = run_phase(&pool, Some(2)).await;
        assert!(
            capped.iter().all(|&batch| batch <= 2),
            "max_batch=2 through pool+client caps every GPU batch: {capped:?}"
        );

        let uncapped = run_phase(&pool, None).await;
        assert!(
            uncapped.iter().any(|&batch| batch > 2),
            "without max_batch the queued singles merge past 2: {uncapped:?}"
        );

        state.manager.shutdown().await;
    }

    /// `GET /api/inference/health` through the real HTTP server (gateway-
    /// mode mounting: the nested inference router, no standalone wrapper)
    /// returns 200 with the [`HealthReport`] JSON shape — asserted by serde
    /// round-trip into the same structs the handler serialized from. Empty
    /// manager: status "ok", registry_ok, zero models. After a real load
    /// via the gateway client, the model appears with its cache key and
    /// replica counts. Finally the standalone router's bare `/health`
    /// (subcommand mode) serves the identical shape — the path existing
    /// probes rely on keeps working.
    #[tokio::test]
    async fn health_endpoint_serves_json_shape_over_http() {
        let (state, base_url, _registry_dir) = spawn_test_server().await;

        // Empty state over the wire.
        let response = reqwest::get(format!("{base_url}/api/inference/health"))
            .await
            .expect("health request");
        assert_eq!(response.status(), 200);
        let health: HealthReport = response
            .json()
            .await
            .expect("health body parses into the HealthReport serde shape");
        assert_eq!(health.status, "ok");
        assert!(!health.shutting_down);
        assert!(health.registry_ok, "the echo fixture registry parses");
        assert_eq!(health.model_count, 0);
        assert!(health.models.is_empty());
        // The prewarm section serde round-trips too (this server runs with
        // the pool disabled; the enabled shape is covered by the prewarm
        // param test).
        assert!(!health.prewarm.enabled);
        assert!(!health.prewarm.lazy);
        assert!(health.prewarm.warm.is_empty());

        // Load a model through the real client, then health reports it.
        let client = InferenceApiClient::new_with_metadata_cache(base_url.clone(), false)
            .expect("client builds");
        client
            .load_model("echo/test", "key", 10, -1, None)
            .await
            .expect("load");
        let health: HealthReport = reqwest::get(format!("{base_url}/api/inference/health"))
            .await
            .expect("health request")
            .json()
            .await
            .expect("health json");
        assert_eq!(health.model_count, 1);
        assert_eq!(health.models.len(), 1);
        let model = &health.models[0];
        assert_eq!(model.inference_id, "echo/test");
        assert_eq!(model.cache_keys, vec!["key".to_string()]);
        assert_eq!(model.replicas.total, 1);
        assert_eq!(model.replicas.free, 1, "idle model: replica in the pool");
        assert_eq!(model.queue_depth, 0);
        assert_eq!(
            model.last_effective_cap, None,
            "no window dispatched yet -> null on the wire"
        );

        // Standalone (subcommand) mounting: bare /health, same handler.
        let standalone = standalone_router(Arc::clone(&state));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let standalone_url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            axum::serve(listener, standalone).await.unwrap();
        });
        let health: HealthReport = reqwest::get(format!("{standalone_url}/health"))
            .await
            .expect("standalone health request")
            .json()
            .await
            .expect("standalone health json");
        assert_eq!(health.status, "ok");
        assert_eq!(health.model_count, 1, "same manager, same report");

        state.manager.shutdown().await;
    }

    /// The additive `prewarm` query param end to end over real HTTP
    /// (design §8): `prewarm=false` on PUT /load parses (200) and
    /// suppresses the lazy warm (pool empty right after — the lazy slot
    /// insertion is synchronous when it fires, so this is deterministic);
    /// an absent param means true (the lazy slot exists immediately after
    /// the load); an explicit `prewarm=true` parses; a non-boolean value is
    /// a client error rather than a silent default. POST /predict accepts
    /// the param through the real gateway client (which serializes it only
    /// when the caller has an opinion). The health report over HTTP shows
    /// the enabled pool's prewarm section with the warm entry.
    #[tokio::test]
    async fn prewarm_param_parses_and_gates_lazy_warm_over_http() {
        let (state, base_url, _registry_dir) = spawn_test_server_with_prewarm(
            r#"
[group.echo]
config.impl_class = "echo_test"
[group.echo.inference_ids.test]
"#,
            PrewarmConfig {
                enabled: true,
                lazy: true,
                always_warm: Vec::new(),
            },
        )
        .await;
        let http = reqwest::Client::new();
        let load_url = |extra: &str| {
            format!(
                "{base_url}/api/inference/load/echo/test?cache_key=key&lru_size=10&ttl_seconds=-1{extra}"
            )
        };

        // prewarm=false: parses, loads, and leaves no warm worker behind.
        let response = http.put(load_url("&prewarm=false")).send().await.unwrap();
        assert_eq!(response.status(), 200);
        assert!(
            state.manager.prewarm_pool().health().warm.is_empty(),
            "prewarm=false suppressed the lazy warm"
        );

        // Absent = true: after an unload, a plain load leaves a lazy slot.
        http.delete(format!("{base_url}/api/inference/cache/key/echo/test"))
            .send()
            .await
            .unwrap();
        let response = http.put(load_url("")).send().await.unwrap();
        assert_eq!(response.status(), 200);
        assert!(
            !state.manager.prewarm_pool().health().warm.is_empty(),
            "absent hint means true: the lazy slot exists after the load"
        );

        // Explicit true parses; banana does not.
        let response = http.put(load_url("&prewarm=true")).send().await.unwrap();
        assert_eq!(response.status(), 200);
        let response = http.put(load_url("&prewarm=banana")).send().await.unwrap();
        assert!(
            response.status().is_client_error(),
            "a non-boolean prewarm value is rejected, got {}",
            response.status()
        );

        // predict accepts the param via the real client (prewarm=false on
        // the wire) and still returns normal outputs.
        let client = InferenceApiClient::new_with_metadata_cache(base_url.clone(), false)
            .expect("client builds");
        let output = client
            .predict(
                "echo/test",
                "key",
                10,
                -1,
                None,
                Some(false),
                &[InferenceInput::new(json!(1), None)],
            )
            .await
            .expect("predict with prewarm=false");
        match output {
            PredictOutput::Json(values) => assert_eq!(values, vec![json!({"echo": 1})]),
            other => panic!("expected Json output, got {other:?}"),
        }

        // Health over the wire reports the enabled pool with its entry.
        let health: HealthReport = reqwest::get(format!("{base_url}/api/inference/health"))
            .await
            .expect("health request")
            .json()
            .await
            .expect("health json");
        assert!(health.prewarm.enabled);
        assert!(health.prewarm.lazy);
        assert!(
            health
                .prewarm
                .warm
                .iter()
                .any(|entry| entry.impl_class == "echo_test"
                    && (entry.state == "warm" || entry.state == "spawning")),
            "the lazy slot shows in the health prewarm section: {:?}",
            health.prewarm.warm
        );

        state.manager.shutdown().await;
    }

    /// A missing built-in registry config dir must not hard-fail gateway
    /// boot: from_settings degrades to a working state (warn + user dir
    /// only, here also missing -> empty registry), matching Python's
    /// warn-not-fail posture and the lazy degradation already used for
    /// broken registry TOML. Cargo runs this with CWD = the gateway crate,
    /// where `src/inferio/config` does not exist.
    #[tokio::test]
    async fn from_settings_degrades_when_builtin_config_dir_is_missing() {
        use crate::config::{
            InferenceLocalConfig, Settings, UpstreamConfig, UpstreamsConfig,
        };

        // Force the default-dirs error path deterministically: no env
        // override, and no src/inferio/config relative to the test CWD.
        unsafe { std::env::remove_var("BASE_INFERENCE_CONFIG_FOLDER") };
        assert!(
            !std::path::Path::new("src/inferio/config").is_dir(),
            "test premise: the built-in config dir is absent from the crate CWD"
        );

        let settings = Settings {
            server: crate::config::ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 0,
                trust_forwarded_headers: false,
            },
            upstreams: UpstreamsConfig {
                ui: crate::config::UiUpstreamConfig {
                    base_url: "http://127.0.0.1:6339".to_string(),
                    local: false,
                    dir: None,
                    node: None,
                    build: Default::default(),
                },
                api: UpstreamConfig {
                    base_url: "http://127.0.0.1:6342".to_string(),
                    local: false,
                },
                inference: Vec::new(),
            },
            search: Default::default(),
            jobs: Default::default(),
            rulesets: Default::default(),
            policies: Vec::new(),
            inference_local: InferenceLocalConfig {
                enabled: true,
                ..Default::default()
            },
        };

        let state = InferioState::from_settings(&settings)
            .expect("missing built-in config dir degrades instead of failing boot");
        // The degraded registry is empty but serviceable: /metadata-style
        // reads succeed with no groups.
        let registry = state.registry.lock().unwrap().get().expect("empty registry loads");
        assert!(registry.groups.is_empty());
        state.manager.shutdown().await;
    }
}
