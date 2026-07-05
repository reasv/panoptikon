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
//! Additive (design §7): optional `max_batch` query param on predict
//! (forwarded to the dispatcher's merge cap) and, in standalone `inferio`
//! mode, `GET /health`.

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use anyhow::{Context, Result};
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

use super::manager::{ManagerConfig, ModelManager};
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
            RegistryConfig::default_dirs()
                .context("resolving inference registry config directories")?
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
        .layer(DefaultBodyLimit::disable())
        .with_state(state)
}

/// Router for the `inferio` subcommand (design §3 "GPU lender" mode): only
/// the inference surface plus a `/health` endpoint (design §7 addition).
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
}

#[derive(Debug, Deserialize)]
struct PredictParams {
    cache_key: String,
    lru_size: i64,
    ttl_seconds: i64,
    /// Additive over Python: per-request cap on dispatch-time batch merging.
    max_batch: Option<u32>,
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

/// `GET /health` (standalone mode; additive, design §7): orchestrator
/// liveness plus the loaded-model map.
async fn health(State(state): State<Arc<InferioState>>) -> Json<JsonValue> {
    Json(json!({"status": "ok", "loaded": state.manager.cached_models()}))
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
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("registry.toml"),
            r#"
[group.echo]
config.impl_class = "echo_test"
[group.echo.inference_ids.test]
metadata.description = "echo fixture"
"#,
        )
        .unwrap();
        let registry = Arc::new(StdMutex::new(RegistryCache::new(RegistryConfig {
            config_dirs: vec![dir.path().to_path_buf()],
        })));
        let manager = ModelManager::new(
            ManagerConfig {
                spawn: test_spawn_config(),
                default_max_batch: 32,
                sweep_interval: Duration::from_secs(60),
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
            .load_model("echo/test", "key", 10, -1)
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
}
