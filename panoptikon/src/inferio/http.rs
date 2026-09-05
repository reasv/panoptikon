//! HTTP surface of the local inferio orchestrator: a wire-compatible port of
//! the legacy Python `inferio/router.py` + `inferio/utils.py`.
//!
//! Mounted (via `nest_service`) under `/api/inference`, behind the same policy
//! layer as the proxy path it replaced. The gateway's own `InferenceApiClient`
//! (`inferio_client.rs`) is the parity oracle: everything encoded here must
//! round-trip through it unchanged.
//!
//! The wire formats, the additive query params and `/health`, the transport
//! constants, the buffered body extractor and the failure table are all in
//! docs/inferio-transport.md, "Inference server (http.rs)".

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use anyhow::Result;
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, FromRequest, Multipart, Path, Query, Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use http_body_util::BodyExt as _;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use utoipa::{IntoParams, OpenApi, ToSchema};

use super::manager::{HealthReport, ManagerConfig, ModelManager};
use super::prewarm::PrewarmConfig;
use super::registry::{RegistryCache, RegistryConfig};
use super::slot_error::Unattempted;
use super::worker::{
    MAX_FRAME_BYTES, WorkerDeadlines, WorkerInput, WorkerOutput, WorkerSpawnConfig,
};
use crate::api_error::ApiError;
use crate::config::Settings;
use crate::db::ledger::truncate_error;

/// Python renders "never expires" as `datetime.max.isoformat()`.
const NEVER_EXPIRES: &str = "9999-12-31T23:59:59.999999";

/// Response header carrying the orchestrator's desired in-flight figure, in
/// **items**, for the model that just answered. A *response* header, so the
/// policy layer's inbound `x-panoptikon-*` strip does not touch it. See
/// docs/inferio-transport.md, "Inference server (http.rs)".
pub(crate) const DESIRED_IN_FLIGHT_HEADER: &str = "x-panoptikon-desired-in-flight-items";

/// `detail.kind` of a predict that failed because the inference **worker
/// process died** with the request in flight. The blast radius is a whole
/// window, so the caller has to be able to tell in order to re-queue those
/// items rather than record them as errors.
pub(crate) const WORKER_DIED_KIND: &str = "worker_died";

/// `detail.kind` of a predict whose **request body never arrived in full**, so
/// the batch was never parsed. Same assertion as [`WORKER_DIED_KIND`], a
/// separate token because the causes are. The 400 stays; the kind tells "your
/// bytes were wrong" from "they did not all get here".
pub(crate) const REQUEST_INCOMPLETE_KIND: &str = "request_incomplete";

/// `detail.kind` of a predict this server **declined to read**, being already
/// at [`PREDICT_INFLIGHT_BODY_BYTES`] of other bodies. Nothing is wrong with
/// the request; there was no room to buffer it. `503`.
pub(crate) const BODY_BUDGET_KIND: &str = "body_budget_exhausted";

/// Every rendering that means **this predict never reached a model**, so the
/// request's items are untouched and re-submitting them is correct. **The
/// fallback, not the primary signal**: these sites also attach a typed
/// [`Unattempted`](crate::inferio::slot_error::Unattempted) marker, which
/// [`classify_predict_failure`] downcasts *first*, so an untyped path still
/// classifies as before. One worker death produces five *different* strings
/// depending on where each affected request stood, and only the first says
/// "failed fatally". Each entry is cited to the place that formats it, and the
/// unit tests assert on the exact literals.
const UNATTEMPTED_REQUEST_MARKERS: [&str; 5] = [
    // `Worker::fatal`: the window on the replica that died and, re-raised by
    // `dispatch::fail_requests`, everything queued behind it.
    "failed fatally",
    // `dispatch::reap_idle_replicas`: an idle replica found gone by the
    // liveness sweep. Never contains "failed fatally".
    "exited while idle",
    // `Worker::roundtrip` refusing to write to an already-poisoned worker.
    "is dead after a previous fatal error",
    // `ModelManager::predict` when its reply oneshot is dropped: how a window
    // on a *surviving* replica learns a sibling died.
    "dropped the request",
    // Two sites: `ModelManager::predict` when `tx.send` fails (a death, for
    // the tail of a window reaching it after the fatal arm closed the channel)
    // and `dispatch`'s `End::Graceful` arm (a real unload).
    "was unloaded",
];

/// The context `ensure_loaded` puts on a load failure, minus the model id.
const LOAD_FAILURE_MARKER: &str = "failed to load model";

/// What a failed `ModelManager::predict` was, as far as the wire cares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PredictFailure {
    /// Could not be brought up: router.py's `Failed to load model`.
    LoadFailed,
    /// Never reached a model — [`UNATTEMPTED_REQUEST_MARKERS`].
    Unattempted,
    /// Anything else: the model ran and the attempt failed.
    Other,
}

/// Classify a failed predict: the typed marker first, then its rendered
/// `anyhow` chain. Pure, so the coupling to four other modules' message
/// formats is pinned by unit tests rather than by a live worker death; the
/// downcast walks the whole chain, so added `.context` cannot hide it.
/// **The load check keeps its precedence** over both unattempted signals — a
/// model that will not come up must not cost each item a second attempt — but
/// anchored on this model's id, because the chain includes a fatal error's
/// stderr tail and an unanchored `contains("failed to load model")` would let
/// an old line reclassify a real mid-window death. The unanchored form is
/// honoured last, for router.py parity.
fn classify_predict_failure(err: &anyhow::Error, chain: &str, full_id: &str) -> PredictFailure {
    if chain.contains(&format!("{LOAD_FAILURE_MARKER} {full_id}")) {
        return PredictFailure::LoadFailed;
    }
    if err.downcast_ref::<Unattempted>().is_some() {
        return PredictFailure::Unattempted;
    }
    if UNATTEMPTED_REQUEST_MARKERS
        .iter()
        .any(|marker| chain.contains(marker))
    {
        return PredictFailure::Unattempted;
    }
    if chain.contains(LOAD_FAILURE_MARKER) {
        return PredictFailure::LoadFailed;
    }
    PredictFailure::Other
}

/// The `{"detail": …}` body of an inference error, in two shapes: the string
/// form byte-identical for router.py parity, the object form additive.
#[derive(serde::Serialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum InferenceErrorDetail {
    /// router.py's plain detail strings. Never constructed here (those go
    /// through [`crate::api_error::ErrorBody`]) but half of the wire contract.
    #[allow(dead_code)]
    Message(String),
    /// Machine-readable: `kind` names the failure, the rest is its context.
    Structured(InferenceErrorFields),
}

/// The fields a structured [`InferenceErrorDetail`] can carry — one flat
/// struct, since every consumer dispatches on `kind` first.
#[derive(serde::Serialize, ToSchema, Default)]
pub(crate) struct InferenceErrorFields {
    /// A stable token: [`WORKER_DIED_KIND`], [`REQUEST_INCOMPLETE_KIND`],
    /// [`BODY_BUDGET_KIND`], `load_cooldown`.
    pub kind: String,
    /// Human-readable summary: the string the plain form would carry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// The model the failure is about, `group/name`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// The last error that put the model in this state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// RFC 3339 instant the model may be retried at.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_at: Option<String>,
    /// Consecutive failures counted so far.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failures: Option<u32>,
}

/// The body every inference error path serializes: `{"detail": …}` as in
/// [`crate::api_error::ErrorBody`], with an object detail permitted.
#[derive(serde::Serialize, ToSchema)]
pub(crate) struct InferenceErrorBody {
    pub detail: InferenceErrorDetail,
}

/// Build an error response with a structured detail, in one place.
pub(crate) fn structured_error(status: StatusCode, fields: InferenceErrorFields) -> Response {
    (
        status,
        Json(InferenceErrorBody {
            detail: InferenceErrorDetail::Structured(fields),
        }),
    )
        .into_response()
}

/// Shared state: the model manager plus the registry `/metadata` reads.
pub struct InferioState {
    pub manager: Arc<ModelManager>,
    pub registry: Arc<StdMutex<RegistryCache>>,
    /// Probed at startup; drives the `/metadata` availability overlay.
    pub compute_caps: super::capability::HostComputeCaps,
    /// Calibration profiles for the `/metadata` overlay; also the ledger's.
    pub calibration: Option<Arc<super::calibration::CalibrationStore>>,
    /// Model name of the GPU a model loads on by default — the one the
    /// calibration overlay can answer for unambiguously. `None`, and no
    /// overlay, on a host with no inventory.
    pub default_gpu_name: Option<String>,
}

impl InferioState {
    /// Build the manager + registry from `[inference_local]` config. Needs a
    /// running tokio runtime; workers spawn lazily, so a missing interpreter
    /// surfaces on the first load.
    pub fn from_settings(settings: &Settings) -> Result<Arc<Self>> {
        let local = &settings.inference_local;
        let registry_config = if local.config_dirs.is_empty() {
            RegistryConfig::default_dirs().unwrap_or_else(|err| {
                // A missing built-in config folder must not hard-fail boot.
                tracing::warn!(
                    error = %format!("{err:#}"),
                    "built-in inference config folder not found; serving with \
                     the user config dir only (registry may be empty)"
                );
                RegistryConfig {
                    config_dirs: vec![std::path::PathBuf::from("config/inference")],
                }
            })
        } else {
            RegistryConfig {
                config_dirs: local.config_dirs.clone(),
            }
        };
        // Shipped baselines live in a `calibration/` subdirectory of each
        // registry dir; the loader itself never recurses.
        let registry_dirs = registry_config.config_dirs.clone();
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

        // Worker env follows the wheels actually installed (the setup
        // sentinel), not a re-probe of the hardware: `auto` on a host with
        // /opt/rocm must not inject HIP paths into a venv synced as cpu/cuda.
        // Config resolution is the fallback for user-managed interpreters, and
        // the answer is also every profile key's `backend` component.
        let accelerator = if local.python.is_some() {
            crate::setup::effective_accelerator(local.python_env.accelerator)
        } else {
            crate::setup::installed_accelerator().unwrap_or_else(|| {
                crate::setup::effective_accelerator(local.python_env.accelerator)
            })
        };
        let spawn = WorkerSpawnConfig {
            python: local.resolved_python(),
            impl_dirs: local.resolved_impl_dirs(),
            pythonpath: local.resolved_pythonpath(),
            env: crate::accelerator_env::worker_env(accelerator),
            env_remove: Vec::new(),
            cwd: None,
            deadlines,
            // The pin *variable* follows the resolved accelerator, not the
            // inventory: a ROCm host with an unknown inventory still writes
            // the registry pin into HIP's own variable, where only an index
            // means anything (docs/rocm-batch-calibration-parity.md, D2).
            pin_env_var: super::gpu::pin_env_var(accelerator),
        };
        // One probe answers both hardware questions: which GPUs exist (for
        // pinning and the ledger) and what they can do (the /metadata
        // overlay), against the same resolved accelerator.
        let host = super::gpu::probe(accelerator);
        // The calibration store: shipped baselines beside the registry, the
        // generated file in the data folder. Every profile key's environment
        // half resolves once, here.
        let calibration = super::calibration::CalibrationStore::new(
            super::calibration::StorePaths::beside_registry(
                &registry_dirs,
                &crate::config::runtime().data_folder,
            ),
            super::calibration::StoreEnv {
                platform: super::calibration::StoreEnv::platform_name(),
                backend: accelerator_backend(accelerator).to_owned(),
                generator: format!("panoptikon {}", crate::resources::VERSION),
            },
        );
        let default_gpu_name = host.inventory.default_gpu_name();
        let manager = ModelManager::new(
            ManagerConfig {
                spawn,
                default_max_batch: local.default_max_batch,
                sweep_interval: Duration::from_secs(local.sweep_interval_secs.max(1)),
                loads: local.into(),
                prewarm: PrewarmConfig {
                    enabled: local.prewarm.enabled,
                    lazy: local.prewarm.lazy,
                    always_warm: local.prewarm.always_warm.clone(),
                },
                gpus: host.inventory,
                vram: vram_budgets(&local.vram),
                calibration: Some(Arc::clone(&calibration) as Arc<_>),
            },
            Arc::clone(&registry),
        );
        Ok(Arc::new(Self {
            manager,
            registry,
            compute_caps: host.caps,
            calibration: Some(calibration),
            default_gpu_name,
        }))
    }

    /// Resolve external-input declarations from this local Inferio registry.
    /// Desktop management uses it directly, so a configured remote upstream
    /// cannot be mistaken for the local instance.
    pub fn external_inputs_json(&self) -> Result<JsonValue> {
        self.registry
            .lock()
            .unwrap()
            .get()
            .and_then(|registry| registry.external_inputs_json())
    }
}

/// `[inference_local.vram]` → the ledger's budget table. The config expresses
/// a per-GPU override as absent-means-inherit and the ledger wants a resolved
/// [`VramBudget`] per GPU; resolving here keeps the inheritance rule in
/// `VramConfig::for_gpu` alone, and the ledger's hot path a map lookup.
fn vram_budgets(config: &crate::config::VramConfig) -> super::ledger::VramBudgets {
    let (margin, cap_fraction) = (config.margin, config.cap_fraction);
    let mut budgets = super::ledger::VramBudgets::uniform(super::ledger::VramBudget {
        margin,
        cap_fraction,
    });
    for uuid in config.gpu.keys() {
        let (margin, cap_fraction) = config.for_gpu(uuid);
        budgets = budgets.with_gpu(
            uuid.clone(),
            super::ledger::VramBudget {
                margin,
                cap_fraction,
            },
        );
    }
    budgets
}

/// The `backend` component of a calibration profile key: which torch build the
/// measurements were taken against. `Auto` here means resolution failed, and
/// `cpu` promises the least. Apple Silicon keys as `mps` — same default-PyPI
/// wheels as a macOS `cpu` host, but the measurements are of a Metal device
/// against a unified-memory budget (docs/unified-memory-admission.md).
fn accelerator_backend(accelerator: crate::config::Accelerator) -> &'static str {
    match accelerator {
        crate::config::Accelerator::Cuda => "cuda",
        crate::config::Accelerator::Rocm => "rocm",
        crate::config::Accelerator::Mps => "mps",
        crate::config::Accelerator::Cpu | crate::config::Accelerator::Auto => "cpu",
    }
}

/// Bytes one predict request body may carry: [`MAX_FRAME_BYTES`], the
/// orchestrator's wall on one worker-protocol frame, which already bounds the
/// inputs on the way in. Over it is a `413`. It bounds one request, not this
/// process's memory — that is [`PREDICT_INFLIGHT_BODY_BYTES`]. Derivation:
/// docs/inferio-transport.md.
pub(crate) const PREDICT_BODY_LIMIT: usize = MAX_FRAME_BYTES;

/// **Predict request bytes this process holds in memory at once**, across
/// every connection, stream and peer — the bound [`PREDICT_BODY_LIMIT`] cannot
/// be. At the wall a request is refused `503` with a `Retry-After`, typed
/// [`crate::inferio_client::BODY_BUDGET_KIND`], never queued. Derivation:
/// docs/inferio-transport.md.
pub(crate) const PREDICT_INFLIGHT_BODY_BYTES: usize = 4 * 1024 * 1024 * 1024;

const _: () = assert!(
    PREDICT_INFLIGHT_BODY_BYTES >= 2 * PREDICT_BODY_LIMIT,
    "the in-flight budget must admit two maximal bodies, or a maximal request \
     can be refused for as long as another one is in flight"
);

/// The budget. Process-wide because the resource is: both listener modes
/// mount this router, and a per-router bound bounds no memory.
static PREDICT_BODY_BYTES: tokio::sync::Semaphore =
    tokio::sync::Semaphore::const_new(PREDICT_INFLIGHT_BODY_BYTES);

/// Predict bodies ever refused for want of budget. Reported on `/health`: a
/// bound nobody can see is indistinguishable from a bug.
static PREDICT_BODY_REFUSALS: AtomicU64 = AtomicU64::new(0);

/// What this process's predict-body budget is doing, for `/health`. A caller
/// refused while `in_flight_bytes` is far below `budget_bytes` is hitting a
/// *burst*, and the answer is its own request sizing.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct PredictBodyBudgetHealth {
    /// [`PREDICT_BODY_LIMIT`]: the largest body read, then `413`.
    pub request_limit_bytes: u64,
    /// [`PREDICT_INFLIGHT_BODY_BYTES`]: predict body bytes held at once.
    pub budget_bytes: u64,
    /// Of those, how many are reserved now: arriving plus being parsed.
    pub in_flight_bytes: u64,
    /// Predict requests refused for want of budget since startup; `0` is
    /// what an operator should expect.
    pub refused_requests: u64,
}

/// The budget's state, read off the semaphore rather than a counter beside
/// it: there is only one truth about how much is reserved.
pub(crate) fn predict_body_budget_health() -> PredictBodyBudgetHealth {
    budget_health(
        PREDICT_BODY_BYTES.available_permits(),
        PREDICT_BODY_REFUSALS.load(Relaxed),
    )
}

/// "What the semaphore says" mapped to what `/health` reports, pure so it can
/// be asserted without racing the process-wide budget.
fn budget_health(available: usize, refusals: u64) -> PredictBodyBudgetHealth {
    PredictBodyBudgetHealth {
        request_limit_bytes: PREDICT_BODY_LIMIT as u64,
        budget_bytes: PREDICT_INFLIGHT_BODY_BYTES as u64,
        in_flight_bytes: PREDICT_INFLIGHT_BODY_BYTES.saturating_sub(available) as u64,
        refused_requests: refusals,
    }
}

/// Bytes the budget hands out at a time when the body declares no length; one
/// with a `Content-Length` reserves once, exactly. Charging a chunked body per
/// frame would take the semaphore thousands of times.
const PREDICT_BODY_RESERVE_GRANULE: usize = 1024 * 1024;

/// The inference routes, path-relative so they can be nested under
/// `/api/inference` (gateway and standalone mode mount the same router).
///
/// axum's own body limit stays disabled: it is enforced by
/// `Bytes::from_request`, while [`predict`] collects its body itself so a
/// truncated one can be told apart from a malformed one, applying
/// [`PREDICT_BODY_LIMIT`] in its own extractor.
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
        .route("/external-inputs", get(get_external_inputs))
        .route("/health", get(health))
        .layer(DefaultBodyLimit::disable())
        .with_state(state)
}

/// Router for the `inferio` subcommand: the inference surface plus the bare
/// `/health` path, same handler, kept for existing probes.
pub fn standalone_router(state: Arc<InferioState>) -> Router {
    Router::new()
        .nest_service("/api/inference", router(Arc::clone(&state)))
        .route("/health", get(health))
        .with_state(state)
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct LoadParams {
    cache_key: String,
    lru_size: i64,
    ttl_seconds: i64,
    /// Additive: lazy prewarm hint (absent = true); false keeps no warm worker.
    prewarm: Option<bool>,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct PredictParams {
    cache_key: String,
    lru_size: i64,
    ttl_seconds: i64,
    /// Additive: per-request cap on dispatch-time batch merging.
    max_batch: Option<u32>,
    /// Additive: lazy prewarm hint, as on load (absent = true).
    prewarm: Option<bool>,
}

// Doc-only OpenAPI shapes: the predict wire formats are hand-rolled above the
// serde layer, so none of these is (de)serialized by the handlers.

/// A raw binary payload (schema: string, format binary).
#[derive(ToSchema)]
#[schema(value_type = String, format = Binary)]
struct BinaryBlob(#[allow(dead_code)] String);

/// Multipart form body of `POST /predict/{group}/{inference_id}`.
#[derive(ToSchema)]
#[allow(dead_code)]
struct InferencePredictRequest {
    /// JSON string of the batch: `{"inputs": [...]}`, each entry an object,
    /// a string, or null (file-only).
    data: String,
    /// Binary batch inputs; each part's *filename* is its `inputs` index.
    files: Option<Vec<BinaryBlob>>,
}

/// JSON envelope of a predict response, unless every output is binary.
#[derive(ToSchema)]
#[allow(dead_code)]
struct PredictJsonResponse {
    /// One output per input; binary ones wrapped as
    /// `{"__type__": "base64", "content": ...}`, a rejected input as
    /// `{"__error__": {"class": ..., "message": ...}}`.
    outputs: Vec<JsonValue>,
}

/// `{"status": "loaded" | "unloaded" | "cleared"}` (Python parity).
#[derive(ToSchema)]
#[allow(dead_code)]
struct StatusResponse {
    status: String,
}

/// Response of `GET /cache/{cache_key}`.
#[derive(ToSchema)]
#[allow(dead_code)]
struct CacheKeyResponse {
    /// inference_id -> ISO-8601 expiry; ttl -1 is `9999-12-31T23:59:59.999999`.
    expirations: std::collections::BTreeMap<String, String>,
}

/// Response of `GET /cache`.
#[derive(ToSchema)]
#[allow(dead_code)]
struct CacheListResponse {
    /// inference_id -> cache keys currently referencing it.
    cache: std::collections::BTreeMap<String, Vec<String>>,
}

/// The predict body, read to its end *before* it is parsed, so the request
/// stream ends normally (a streamed parse leaves an h2 stream reset behind on
/// every predict, and 1 024 of them GOAWAY the connection) and a transport
/// failure stops looking like a malformed body. It costs one extra resident
/// copy, which [`PREDICT_INFLIGHT_BODY_BYTES`] bounds. See
/// docs/inferio-transport.md.
struct BufferedMultipart {
    multipart: Multipart,
    /// The collected body, so a failed parse can say *why*; multer holds
    /// slices of it anyway.
    body: axum::body::Bytes,
    /// The boundary the request's `Content-Type` declared, if usable.
    boundary: Option<String>,
    /// This body's claim on the process-wide budget, returned on drop.
    _reservation: BodyReservation,
}

/// One request's claim on [`PREDICT_INFLIGHT_BODY_BYTES`], returned by `Drop`
/// so every exit path accounts for itself. It grows: a body declaring its
/// length reserves once, one that does not in
/// [`PREDICT_BODY_RESERVE_GRANULE`] steps — always **try**, never a wait, so
/// two half-reserved bodies can never wait on each other.
struct BodyReservation {
    /// The budget drawn on. A parameter rather than the static, so a test
    /// can exercise exhaustion without starving the whole process.
    budget: &'static tokio::sync::Semaphore,
    permit: Option<tokio::sync::SemaphorePermit<'static>>,
}

impl BodyReservation {
    fn new(budget: &'static tokio::sync::Semaphore) -> Self {
        Self {
            budget,
            permit: None,
        }
    }

    fn held(&self) -> usize {
        self.permit
            .as_ref()
            .map_or(0, |permit| permit.num_permits())
    }

    /// Reserve up to `wanted` bytes, or say the budget is out; idempotent.
    fn reserve(&mut self, wanted: usize) -> Result<(), PredictBodyError> {
        let held = self.held();
        let Some(extra) = wanted.checked_sub(held).filter(|extra| *extra > 0) else {
            return Ok(());
        };
        let extra = u32::try_from(extra).unwrap_or(u32::MAX);
        match self.budget.try_acquire_many(extra) {
            Ok(permit) => {
                match &mut self.permit {
                    Some(existing) => existing.merge(permit),
                    slot @ None => *slot = Some(permit),
                }
                Ok(())
            }
            Err(_) => {
                let refusals = PREDICT_BODY_REFUSALS.fetch_add(1, Relaxed) + 1;
                tracing::warn!(
                    wanted,
                    held,
                    budget_bytes = PREDICT_INFLIGHT_BODY_BYTES,
                    available_bytes = self.budget.available_permits(),
                    refusals,
                    "refusing a predict body: this process is already holding its \
                     whole predict-body budget in memory. The batch was never \
                     parsed, so the caller may re-submit it"
                );
                Err(PredictBodyError::Overloaded)
            }
        }
    }
}

/// Why a predict body could not be read as a batch — typed, so the decision
/// and the rendering stay separate.
#[derive(Debug)]
enum PredictBodyError {
    /// Every byte arrived and they are not a valid batch. An ordinary 400.
    Malformed(String),
    /// The bytes did not all arrive; see [`REQUEST_INCOMPLETE_KIND`].
    Incomplete(String),
    /// No `data` field. FastAPI answers a missing required Form field 422.
    MissingData,
    /// Larger than [`PREDICT_BODY_LIMIT`]. A `413`: send a smaller batch.
    TooLarge,
    /// Already at [`PREDICT_INFLIGHT_BODY_BYTES`]. A `503` with a
    /// `Retry-After`; re-sending is the answer.
    Overloaded,
}

impl IntoResponse for PredictBodyError {
    fn into_response(self) -> Response {
        match self {
            Self::Malformed(detail) => ApiError::bad_request(detail).into_response(),
            Self::Incomplete(detail) => {
                tracing::warn!(
                    detail = %detail,
                    "a predict request body did not arrive as a whole multipart body; \
                     the items in it were never attempted"
                );
                structured_error(
                    StatusCode::BAD_REQUEST,
                    InferenceErrorFields {
                        kind: REQUEST_INCOMPLETE_KIND.to_owned(),
                        message: Some("Request body did not arrive in full".to_owned()),
                        last_error: Some(truncate_error(&detail).into_owned()),
                        ..Default::default()
                    },
                )
            }
            Self::MissingData => {
                ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, "Field required: data")
                    .into_response()
            }
            Self::TooLarge => ApiError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                format!(
                    "predict request body exceeds the {} MiB limit; send fewer inputs \
                     per request",
                    PREDICT_BODY_LIMIT / (1024 * 1024)
                ),
            )
            .into_response(),
            Self::Overloaded => {
                let mut response = structured_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    InferenceErrorFields {
                        kind: BODY_BUDGET_KIND.to_owned(),
                        message: Some(format!(
                            "the server is already holding its whole {} MiB predict-body \
                             budget; this batch was not read and was not attempted",
                            PREDICT_INFLIGHT_BODY_BYTES / (1024 * 1024)
                        )),
                        ..Default::default()
                    },
                );
                // A figure the caller can act on: the budget is released by
                // requests already parsing.
                response
                    .headers_mut()
                    .insert(header::RETRY_AFTER, header::HeaderValue::from_static("1"));
                response
            }
        }
    }
}

/// Collect a request body under a per-request ceiling **and** the process-wide
/// byte budget, keeping the four outcomes distinct: truncated, malformed, too
/// large, and no room right now. The budget is charged **before the bytes are
/// read** — from `Content-Length`, or in [`PREDICT_BODY_RESERVE_GRANULE`]
/// steps where there is none — so a body is never admitted into memory the
/// process has not accounted for; a declared length over `limit` is refused
/// unread.
async fn collect_within(
    body: Body,
    limit: usize,
    budget: &'static tokio::sync::Semaphore,
) -> Result<(axum::body::Bytes, BodyReservation), PredictBodyError> {
    use axum::body::HttpBody as _;

    let mut reservation = BodyReservation::new(budget);
    // Every request the shipped client builds declares a length.
    let declared = body.size_hint().exact();
    let granule = if declared.is_some() {
        1
    } else {
        PREDICT_BODY_RESERVE_GRANULE
    };
    if let Some(declared) = declared {
        let declared = usize::try_from(declared).unwrap_or(usize::MAX);
        if declared > limit {
            return Err(PredictBodyError::TooLarge);
        }
        reservation.reserve(declared)?;
    }
    let mut body = std::pin::pin!(body);
    let mut collected = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = match frame {
            Ok(frame) => frame,
            // The body stream itself failed: nothing was attempted.
            Err(err) => {
                return Err(PredictBodyError::Incomplete(format!(
                    "the request body stream failed: {}",
                    error_chain(&err)
                )));
            }
        };
        // Trailers carry no payload; a body that ends in them is complete.
        let Ok(data) = frame.into_data() else {
            continue;
        };
        let wanted = collected.len().saturating_add(data.len());
        if wanted > limit {
            return Err(PredictBodyError::TooLarge);
        }
        // A no-op when the declared length covered this; the real charge
        // otherwise, or when a peer oversent.
        reservation.reserve(wanted.next_multiple_of(granule).min(limit))?;
        collected.extend_from_slice(&data);
    }
    Ok((axum::body::Bytes::from(collected), reservation))
}

impl<S> FromRequest<S> for BufferedMultipart
where
    S: Send + Sync,
{
    type Rejection = PredictBodyError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let (parts, body) = req.into_parts();
        let boundary = parts
            .headers
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(multipart_boundary);
        let (bytes, reservation) =
            collect_within(body, PREDICT_BODY_LIMIT, &PREDICT_BODY_BYTES).await?;
        let request = Request::from_parts(parts, Body::from(bytes.clone()));
        Multipart::from_request(request, state)
            .await
            .map(|multipart| Self {
                multipart,
                body: bytes,
                boundary,
                _reservation: reservation,
            })
            .map_err(|rejection| {
                // No usable boundary: the header is what is wrong.
                PredictBodyError::Malformed(format!(
                    "invalid multipart body: {}",
                    rejection.body_text()
                ))
            })
    }
}

impl BufferedMultipart {
    /// The `data` field and the file parts, by index — or why the body could
    /// not be read as a batch. All three places a multipart parse can fail are
    /// here, because they all need the cause underneath axum's fixed sentence
    /// and [`Self::classify`]'s verdict.
    async fn into_fields(
        mut self,
    ) -> Result<(String, Vec<(Option<i64>, Vec<u8>)>), PredictBodyError> {
        let mut data: Option<String> = None;
        let mut files: Vec<(Option<i64>, Vec<u8>)> = Vec::new();
        loop {
            let field = match self.multipart.next_field().await {
                Ok(Some(field)) => field,
                Ok(None) => break,
                Err(err) => return Err(self.classify("invalid multipart body", &err)),
            };
            match field.name() {
                Some("data") => {
                    data = Some(match field.text().await {
                        Ok(text) => text,
                        Err(err) => return Err(self.classify("invalid data field", &err)),
                    });
                }
                Some("files") => {
                    // Python maps each file to its batch slot via the
                    // filename, which must be an integer index.
                    let index = field
                        .file_name()
                        .and_then(|name| name.trim().trim_matches('"').parse::<i64>().ok());
                    let bytes = match field.bytes().await {
                        Ok(bytes) => bytes,
                        Err(err) => return Err(self.classify("invalid file field", &err)),
                    };
                    files.push((index, bytes.to_vec()));
                }
                // FastAPI ignores unknown form fields; so do we.
                _ => {}
            }
        }
        data.map(|data| (data, files))
            .ok_or(PredictBodyError::MissingData)
    }

    /// A failed parse, split by the only distinction that changes what the
    /// caller should do. The whole body is in hand, so the question is asked
    /// of the bytes rather than inferred from a parser's error variant: does
    /// what arrived carry the closing delimiter of the declared boundary? If
    /// not, nothing was parsed and re-submitting is right
    /// ([`REQUEST_INCOMPLETE_KIND`]); if so, something *inside* them is wrong.
    /// Asked only after the parse has already rejected the body.
    fn classify(
        &self,
        prose: &str,
        err: &axum::extract::multipart::MultipartError,
    ) -> PredictBodyError {
        let cause = error_chain(err);
        let whole = self
            .boundary
            .as_deref()
            .is_none_or(|boundary| body_carries_closing_delimiter(&self.body, boundary));
        if whole {
            return PredictBodyError::Malformed(format!("{prose}: {cause}"));
        }
        PredictBodyError::Incomplete(format!(
            "{prose}: {cause}; {} bytes arrived without the closing delimiter of boundary {}",
            self.body.len(),
            self.boundary.as_deref().unwrap_or("?"),
        ))
    }
}

/// The `boundary` parameter of the request's `Content-Type`, read through
/// `mime` because that is how multer reads it: the verdict above turns on the
/// body carrying *this* boundary's closing delimiter, so it must be the string
/// multer parsed with. `None` when the header carries none.
fn multipart_boundary(content_type: &str) -> Option<String> {
    let content_type = content_type.parse::<mime_guess::mime::Mime>().ok()?;
    let boundary = content_type
        .get_param(mime_guess::mime::BOUNDARY)?
        .as_str()
        .to_owned();
    (!boundary.is_empty()).then_some(boundary)
}

/// Whether `body` contains `--<boundary>--`, the delimiter ending a
/// `multipart/form-data` body (RFC 2046 §5.1.1). Searched rather than required
/// at the end, because an epilogue after it is legal.
fn body_carries_closing_delimiter(body: &[u8], boundary: &str) -> bool {
    let needle = format!("--{boundary}--").into_bytes();
    if body.len() < needle.len() {
        return false;
    }
    body.windows(needle.len()).any(|window| window == needle)
}

/// An error rendered with everything under it: a `Display` names the layer.
fn error_chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut rendered = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        let text = cause.to_string();
        if !text.is_empty() && !rendered.ends_with(&text) {
            rendered.push_str(": ");
            rendered.push_str(&text);
        }
        source = cause.source();
    }
    rendered
}

/// `POST /predict/{group}/{inference_id}` — router.py `predict`. Parses the
/// multipart request, auto-loads the model (pinned for the duration), runs
/// the batch, and encodes the response like `utils.encode_output_response`.
#[utoipa::path(
    post,
    operation_id = "predict",
    path = "/predict/{group}/{inference_id}",
    tag = "inference",
    summary = "Run a batch prediction on a model",
    description = "Runs a batch of inputs through a model, auto-loading it into the \
        given cache slot first if needed. The response encoding depends on the \
        outputs: exactly one binary output is returned raw as \
        `application/octet-stream`; all-binary outputs use `multipart/mixed`; \
        anything else is the JSON `{\"outputs\": [...]}` envelope. An input the \
        model rejected on its own occupies its output slot as \
        `{\"__error__\": {\"class\": \"input\"|\"transient\", \"message\": ...}}`, \
        which always selects the JSON envelope.",
    params(
        ("group" = String, Path, description = "Model group (first segment of the inference ID)"),
        ("inference_id" = String, Path, description = "Model ID within the group"),
        PredictParams
    ),
    request_body(content = InferencePredictRequest, content_type = "multipart/form-data"),
    responses(
        (status = 200, description = "Model outputs", content(
            (PredictJsonResponse = "application/json"),
            (BinaryBlob = "application/octet-stream"),
            (BinaryBlob = "multipart/mixed")
        ), headers(
            ("x-panoptikon-desired-in-flight-items" = String, description =
                "Items the orchestrator would like the caller to keep inside \
                 in-flight predict requests for this model. Additive and \
                 optional: absent means the orchestrator has no opinion (an \
                 older server, or a model that has not dispatched a window \
                 yet) and the caller keeps its own floor.")
        )),
        (status = 400, description = "Malformed multipart body or inputs. `detail` is a \
            plain string when the bytes themselves are wrong, and an object carrying \
            `kind = \"request_incomplete\"` when the request body did not arrive in full — \
            that one says the batch was never parsed and never reached a model, so its \
            items are untouched and re-submitting them is correct.", body = InferenceErrorBody),
        (status = 422, description = "Missing required `data` form field", body = crate::api_error::ErrorBody),
        (status = 500, description = "Model load or prediction failure. `detail` is a \
            plain string for an ordinary failure and an object carrying a machine-readable \
            `kind` for the ones a caller must act on differently — `worker_died` says the \
            inference worker process died with the request in flight, so the request's items \
            were never attempted and re-submitting them is correct.", body = InferenceErrorBody),
        (status = 413, description = "The request body is larger than this server will \
            read. Send fewer inputs per request; re-sending the same body will get the \
            same answer.", body = crate::api_error::ErrorBody),
        (status = 503, description = "Temporarily refused, with a `Retry-After`. \
            `kind = \"body_budget_exhausted\"` means the server is already holding its \
            whole predict-body budget in memory, so this body was never read and its \
            items were never attempted — re-submit it. `kind = \"load_cooldown\"` is the \
            opposite case and must not be retried before `retry_at`.", body = InferenceErrorBody)
    )
)]
async fn predict(
    State(state): State<Arc<InferioState>>,
    Path((group, inference_id)): Path<(String, String)>,
    Query(params): Query<PredictParams>,
    body: BufferedMultipart,
) -> Result<Response, ApiError> {
    let (data, files) = match body.into_fields().await {
        Ok(fields) => fields,
        Err(failure) => return Ok(failure.into_response()),
    };
    let inputs = parse_input_request(&data, files)?;

    let full_id = format!("{group}/{inference_id}");
    tracing::debug!(
        model = %full_id,
        inputs = inputs.len(),
        "processing local inference predict"
    );
    let outputs = match state
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
    {
        Ok(outputs) => outputs,
        Err(err) => return predict_failure_response(err, &full_id),
    };
    // Deliberately not this request's own window but whatever this model
    // formed most recently: a running opinion, not a receipt. A model gone in
    // the gap answers `None` and omits the header.
    let desired = state.manager.desired_in_flight_items(&full_id);
    Ok(with_desired_in_flight(
        encode_output_response(outputs),
        desired,
    ))
}

/// The answer to a failed `ModelManager::predict`, in one place so
/// [`classify_predict_failure`] alone decides the shape a caller sees.
fn predict_failure_response(err: anyhow::Error, full_id: &str) -> Result<Response, ApiError> {
    // The cooldown first: a *refusal to try*, with its own status.
    if let Some(response) = load_cooldown_response(&err) {
        return Ok(response);
    }
    let chain = format!("{err:#}");
    tracing::error!(model = %full_id, error = %chain, "prediction failed");
    match classify_predict_failure(&err, &chain, full_id) {
        PredictFailure::LoadFailed => Err(ApiError::internal("Failed to load model")),
        PredictFailure::Unattempted => Ok(structured_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            InferenceErrorFields {
                kind: WORKER_DIED_KIND.to_owned(),
                // The string the plain form carries, so a prose-only client
                // is unaffected.
                message: Some("Prediction failed".to_owned()),
                model: Some(full_id.to_owned()),
                last_error: Some(truncate_error(&chain).into_owned()),
                ..Default::default()
            },
        )),
        PredictFailure::Other => Err(ApiError::internal("Prediction failed")),
    }
}

/// The pinned 503 of the per-model load-failure cooldown, when this error is
/// one: `Retry-After: <seconds>` plus a `load_cooldown` detail carrying
/// `model`, `last_error`, `retry_at` and `failures`. The whole chain is
/// searched, so added context still gets the right answer. 503 so a job's
/// client can tell "do not retry now" from "this attempt failed".
fn load_cooldown_response(err: &anyhow::Error) -> Option<Response> {
    let cooldown = err
        .chain()
        .find_map(|source| source.downcast_ref::<super::manager::LoadCooldownError>())?;
    tracing::warn!(
        model = %cooldown.model,
        failures = cooldown.failures,
        retry_after_secs = cooldown.retry_after_secs,
        "refusing a predict: the model is in its load-failure cooldown"
    );
    let mut response = structured_error(
        StatusCode::SERVICE_UNAVAILABLE,
        InferenceErrorFields {
            kind: super::manager::LOAD_COOLDOWN_KIND.to_owned(),
            message: Some(cooldown.to_string()),
            model: Some(cooldown.model.clone()),
            last_error: Some(truncate_error(&cooldown.last_error).into_owned()),
            retry_at: Some(cooldown.retry_at.to_rfc3339()),
            failures: Some(cooldown.failures),
        },
    );
    if let Ok(value) = header::HeaderValue::from_str(&cooldown.retry_after_secs.to_string()) {
        response.headers_mut().insert(header::RETRY_AFTER, value);
    }
    Some(response)
}

/// Attach [`DESIRED_IN_FLIGHT_HEADER`] to an encoded predict response, body
/// and other headers byte-identical. `None` omits it.
fn with_desired_in_flight(mut response: Response, desired: Option<u64>) -> Response {
    if let Some(value) = desired
        && let Ok(value) = header::HeaderValue::from_str(&value.to_string())
    {
        response.headers_mut().insert(
            header::HeaderName::from_static(DESIRED_IN_FLIGHT_HEADER),
            value,
        );
    }
    response
}

/// `PUT /load/{group}/{inference_id}` — router.py `load_model`:
/// `{"status": "loaded"}`, or 500 `"Failed to load model"` with the details
/// logged as Python's `logger.error`.
#[utoipa::path(
    put,
    operation_id = "load_model",
    path = "/load/{group}/{inference_id}",
    tag = "inference",
    summary = "Load a model into a cache slot",
    params(
        ("group" = String, Path, description = "Model group (first segment of the inference ID)"),
        ("inference_id" = String, Path, description = "Model ID within the group"),
        LoadParams
    ),
    responses(
        (status = 200, description = "Model loaded", body = StatusResponse),
        (status = 500, description = "Load failure", body = crate::api_error::ErrorBody)
    )
)]
async fn load_model(
    State(state): State<Arc<InferioState>>,
    Path((group, inference_id)): Path<(String, String)>,
    Query(params): Query<LoadParams>,
) -> Result<Response, ApiError> {
    let full_id = format!("{group}/{inference_id}");
    if let Err(err) = state
        .manager
        .load_model(
            &full_id,
            &params.cache_key,
            params.lru_size,
            params.ttl_seconds,
            params.prewarm,
        )
        .await
    {
        // The predict path's cooldown answer: an explicit load is the one
        // path that would otherwise keep asking.
        if let Some(response) = load_cooldown_response(&err) {
            return Ok(response);
        }
        tracing::error!(model = %full_id, error = %format!("{err:#}"), "failed to load model");
        return Err(ApiError::internal("Failed to load model"));
    }
    Ok(Json(json!({"status": "loaded"})).into_response())
}

/// `DELETE /cache/{key}/{group}/{id}` — router.py `unload_model`: always
/// `{"status": "unloaded"}`.
#[utoipa::path(
    delete,
    operation_id = "unload_model",
    path = "/cache/{cache_key}/{group}/{inference_id}",
    tag = "inference",
    summary = "Unload a model from a cache slot",
    params(
        ("cache_key" = String, Path, description = "Cache slot the model was loaded under"),
        ("group" = String, Path, description = "Model group (first segment of the inference ID)"),
        ("inference_id" = String, Path, description = "Model ID within the group")
    ),
    responses(
        (status = 200, description = "Unloaded (whether or not the entry existed)", body = StatusResponse),
        (status = 500, description = "Unload failure", body = crate::api_error::ErrorBody)
    )
)]
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

/// `DELETE /cache/{cache_key}` — router.py `clear_cache`, `"cleared"`.
#[utoipa::path(
    delete,
    operation_id = "clear_cache",
    path = "/cache/{cache_key}",
    tag = "inference",
    summary = "Unload every model in a cache slot",
    params(("cache_key" = String, Path, description = "Cache slot to clear")),
    responses(
        (status = 200, description = "Cache cleared", body = StatusResponse),
        (status = 500, description = "Clear failure", body = crate::api_error::ErrorBody)
    )
)]
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
/// `{"expirations": {id: isoformat}}`, `datetime.max` for ttl -1.
#[utoipa::path(
    get,
    operation_id = "get_cache_expiration",
    path = "/cache/{cache_key}",
    tag = "inference",
    summary = "Get model expiration times for a cache slot",
    params(("cache_key" = String, Path, description = "Cache slot to inspect")),
    responses(
        (status = 200, description = "Expiration times per loaded model", body = CacheKeyResponse)
    )
)]
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

/// `GET /cache` — router.py `get_cached_models`, `{inference_id: [keys]}`.
#[utoipa::path(
    get,
    operation_id = "get_cached_models",
    path = "/cache",
    tag = "inference",
    summary = "List loaded models and the cache slots referencing them",
    responses(
        (status = 200, description = "Loaded models per cache slot", body = CacheListResponse)
    )
)]
async fn get_cached_models(State(state): State<Arc<InferioState>>) -> Json<JsonValue> {
    Json(json!({"cache": state.manager.cached_models()}))
}

/// `GET /metadata` — router.py `get_metadata`: mtime-gated reload, then
/// `list_inference_ids`.
#[utoipa::path(
    get,
    operation_id = "get_metadata",
    path = "/metadata",
    tag = "inference",
    summary = "Get the inference model registry",
    description = "Free-form registry metadata: `{group: {\"inference_ids\": {id: \
        {\"description\", ...}}, <group metadata>...}}`. The shape of the \
        per-model and per-group metadata is registry-defined.",
    responses(
        (status = 200, description = "Registry metadata by group", body = JsonValue),
        (status = 500, description = "Registry failed to load", body = crate::api_error::ErrorBody)
    )
)]
async fn get_metadata(State(state): State<Arc<InferioState>>) -> Result<Json<JsonValue>, ApiError> {
    let snapshot = state.registry.lock().unwrap().get();
    match snapshot {
        Ok(registry) => {
            let mut body = registry.metadata_json();
            super::capability::overlay_metadata(&mut body, &state.compute_caps);
            if let Some(store) = state.calibration.as_ref() {
                super::calibration::overlay_metadata(
                    &mut body,
                    store,
                    &registry,
                    state.default_gpu_name.as_deref(),
                );
            }
            Ok(Json(body))
        }
        Err(err) => {
            tracing::error!(error = %format!("{err:#}"), "failed to load inference registry");
            Err(ApiError::internal("Failed to load inference metadata"))
        }
    }
}

/// Declared model external inputs and presence. Values are never exposed.
#[utoipa::path(
    get,
    operation_id = "get_external_inputs",
    path = "/external-inputs",
    tag = "inference",
    summary = "Get model external-input requirements",
    responses(
        (status = 200, description = "Reusable definitions, model usages, and configured presence", body = JsonValue),
        (status = 500, description = "Registry or environment resolution failed", body = crate::api_error::ErrorBody)
    )
)]
async fn get_external_inputs(
    State(state): State<Arc<InferioState>>,
) -> Result<Json<JsonValue>, ApiError> {
    match state.external_inputs_json() {
        Ok(status) => Ok(Json(status)),
        Err(err) => {
            tracing::error!(error = %format!("{err:#}"), "failed to resolve external inputs");
            Err(ApiError::internal(
                "Failed to resolve inference external inputs",
            ))
        }
    }
}

/// `GET /health` (additive; no Python counterpart): orchestrator and per-model
/// liveness, loaded models, queue depths and batch caps. Shape:
/// [`HealthReport`], from [`ModelManager::health`].
#[utoipa::path(
    get,
    operation_id = "health",
    path = "/health",
    tag = "inference",
    summary = "Inference service health",
    description = "Orchestrator + per-model liveness, queue depths, and batch \
        caps. Gateway addition — the Python inference server has no such \
        endpoint (a proxied upstream 404s it).",
    responses(
        (status = 200, description = "Health report", body = HealthReport)
    )
)]
async fn health(State(state): State<Arc<InferioState>>) -> Json<HealthReport> {
    Json(state.manager.health())
}

/// Port of `utils.parse_input_request`. Wire details and the exact 400s: see
/// docs/inferio-transport.md, "Wire formats (Python parity)".
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

/// Port of `utils.encode_output_response`, byte-for-byte: one binary output
/// renders raw, all-binary as `multipart/mixed`, anything else as the JSON
/// `{"outputs": [...]}` envelope — which a batch carrying a typed per-item
/// error slot always takes, since the binary encodings have nowhere to put
/// one. See docs/inferio-transport.md, "Wire formats (Python parity)".
fn encode_output_response(outputs: Vec<WorkerOutput>) -> Response {
    let has_error_slot = outputs
        .iter()
        .any(|output| matches!(output, WorkerOutput::Error(_)));
    if !has_error_slot && outputs.len() == 1 && matches!(outputs[0], WorkerOutput::Bytes(_)) {
        let WorkerOutput::Bytes(bytes) = outputs.into_iter().next().expect("len checked") else {
            unreachable!("variant checked above");
        };
        return ([(header::CONTENT_TYPE, "application/octet-stream")], bytes).into_response();
    }

    if !has_error_slot
        && outputs
            .iter()
            .all(|output| matches!(output, WorkerOutput::Bytes(_)))
    {
        // Python uses this fixed boundary; the client's parser reads it back
        // out of the Content-Type header either way.
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
            WorkerOutput::Error(error) => error.to_json(),
        })
        .collect();
    Json(json!({"outputs": encoded})).into_response()
}

/// OpenAPI subdocument for the inference surface, nested under
/// `/api/inference` by the gateway's main doc; paths are router-relative.
/// Documented regardless of `inference_local`, since when it is disabled the
/// same paths proxy to an upstream serving the same contract.
#[derive(OpenApi)]
#[openapi(
    paths(
        predict,
        load_model,
        unload_model,
        get_cache_expiration,
        clear_cache,
        get_cached_models,
        get_metadata,
        get_external_inputs,
        health
    ),
    components(schemas(
        InferencePredictRequest,
        BinaryBlob,
        PredictJsonResponse,
        StatusResponse,
        CacheKeyResponse,
        CacheListResponse,
        crate::api_error::ErrorBody,
        InferenceErrorBody,
        InferenceErrorDetail,
        InferenceErrorFields,
        HealthReport,
        super::manager::ModelHealth,
        super::manager::ReplicaHealth,
        super::manager::CostHealth,
        super::manager::ReplicaTelemetryHealth,
        super::manager::BatchHealth,
        super::gpu::GpuInfo,
        super::prewarm::PrewarmHealth,
        super::prewarm::PrewarmWorkerHealth
    ))
)]
pub struct InferioApiDoc;

#[cfg(test)]
mod tests {
    use super::super::slot_error::{SlotError, SlotErrorClass};
    use super::*;
    use crate::db::ledger::MAX_ERROR_BYTES;
    use crate::inferio_client::{
        InferenceApiClient, InferenceFile, InferenceInput, PredictOutput, parse_predict_response,
    };
    use anyhow::anyhow;
    use axum::body::to_bytes;
    use serde_json::json;
    use std::fs;

    /// The GPU the fixture calibration overlay answers for: tests must never
    /// depend on the host's.
    const TEST_GPU: &str = "TEST 9000";

    // Response-encoding parity: everything encode_output_response produces
    // must parse in the gateway client's own parser, which was written
    // against the Python server and is therefore the wire-parity oracle.

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

    /// The client's own reading of an all-binary response.
    fn binary_outputs(content_type: &str, body: &[u8]) -> Vec<Vec<u8>> {
        match parse_predict_response(content_type, body).unwrap().outputs {
            PredictOutput::Binary(outputs) => outputs,
            other => panic!("client parsed {other:?}"),
        }
    }

    /// The client's own reading of a JSON response.
    fn json_outputs(content_type: &str, body: &[u8]) -> Vec<JsonValue> {
        match parse_predict_response(content_type, body).unwrap().outputs {
            PredictOutput::Json(outputs) => outputs,
            other => panic!("client parsed {other:?}"),
        }
    }

    fn slot_error(class: SlotErrorClass, message: &str) -> WorkerOutput {
        let message = message.to_owned();
        WorkerOutput::Error(SlotError { class, message })
    }

    /// The three encodings and the client's reading of each: one binary output
    /// is a raw octet-stream (the npy embedding fast path), all-binary is
    /// `multipart/mixed` with Python's literal framing, anything else the JSON
    /// envelope with bytes base64-wrapped.
    #[tokio::test]
    async fn outputs_take_the_encoding_python_used() {
        let multipart: Vec<u8> = [
            &b"--multipart-boundary\r\nContent-Type: application/octet-stream\r\nContent-Disposition: attachment; filename=\"output0.bin\"\r\n\r\nAAA\r\n"[..],
            &b"--multipart-boundary\r\nContent-Type: application/octet-stream\r\nContent-Disposition: attachment; filename=\"output1.bin\"\r\n\r\nBB\r\n"[..],
            &b"--multipart-boundary--\r\n"[..],
        ]
        .concat();
        for (outputs, want_type, want_body, want_client) in [
            (
                vec![WorkerOutput::Bytes(b"\x93NUMPY".to_vec())],
                "application/octet-stream",
                b"\x93NUMPY".to_vec(),
                vec![b"\x93NUMPY".to_vec()],
            ),
            (
                vec![
                    WorkerOutput::Bytes(b"AAA".to_vec()),
                    WorkerOutput::Bytes(b"BB".to_vec()),
                ],
                "multipart/mixed; boundary=multipart-boundary",
                multipart,
                vec![b"AAA".to_vec(), b"BB".to_vec()],
            ),
        ] {
            let (content_type, body) = split_response(encode_output_response(outputs)).await;
            assert!(content_type.starts_with(want_type), "{content_type}");
            assert_eq!(body, want_body, "byte-for-byte Python framing");
            assert_eq!(binary_outputs(&content_type, &body), want_client);
        }

        // All-JSON: the plain envelope, values untouched.
        let (content_type, body) =
            split_response(encode_output_response(vec![WorkerOutput::Json(
                json!({"echo": {"text": "x"}}),
            )]))
            .await;
        assert!(content_type.contains("application/json"));
        let value: JsonValue = serde_json::from_slice(&body).unwrap();
        assert_eq!(value, json!({"outputs": [{"echo": {"text": "x"}}]}));

        // Mixed JSON and binary: bytes entries base64-wrapped, JSON untouched.
        let (content_type, body) = split_response(encode_output_response(vec![
            WorkerOutput::Json(json!({"tags": ["a"]})),
            WorkerOutput::Bytes(b"\x01\x02".to_vec()),
        ]))
        .await;
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
        match parse_predict_response(&content_type, &body)
            .unwrap()
            .outputs
        {
            PredictOutput::Json(outputs) => assert_eq!(outputs.len(), 2),
            other => panic!("client parsed {other:?}"),
        }
    }

    /// A typed per-item error slot forces the JSON envelope even for an
    /// otherwise all-binary batch (the binary encodings have nowhere to put a
    /// typed failure), and the client parses it back into the surviving
    /// payloads plus the slot error at its input's index.
    #[tokio::test]
    async fn an_error_slot_forces_the_json_envelope_and_round_trips() {
        let batch = vec![
            WorkerOutput::Bytes(b"AAA".to_vec()),
            slot_error(SlotErrorClass::Input, "Unreadable image: truncated"),
            WorkerOutput::Bytes(b"BB".to_vec()),
        ];
        let (content_type, body) = split_response(encode_output_response(batch)).await;
        assert!(content_type.contains("application/json"));
        let value: JsonValue = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            value["outputs"][1],
            json!({"__error__": {"class": "input", "message": "Unreadable image: truncated"}})
        );
        let parsed = parse_predict_response(&content_type, &body).unwrap();
        assert_eq!(parsed.errors.len(), 1);
        assert_eq!(parsed.errors[0].index, 1, "the index is the input's");
        assert_eq!(parsed.errors[0].class, SlotErrorClass::Input);
        assert_eq!(
            binary_outputs(&content_type, &body),
            vec![b"AAA".to_vec(), b"BB".to_vec()]
        );

        // A JSON-output model (tags/text): survivors stay JSON values and the
        // failed slot is reported separately, never as an output.
        let survivors = vec![
            slot_error(SlotErrorClass::Transient, "try again"),
            WorkerOutput::Json(json!({"tags": ["a"]})),
        ];
        let (content_type, body) = split_response(encode_output_response(survivors)).await;
        let parsed = parse_predict_response(&content_type, &body).unwrap();
        assert_eq!(parsed.errors.len(), 1);
        assert_eq!(parsed.errors[0].class, SlotErrorClass::Transient);
        assert_eq!(parsed.errors[0].index, 0);
        let json = json_outputs(&content_type, &body);
        assert_eq!(json, vec![json!({"tags": ["a"]})]);

        // A batch where every slot errored yields no outputs at all.
        let only_error = vec![slot_error(SlotErrorClass::Input, "Unreadable image")];
        let (content_type, body) = split_response(encode_output_response(only_error)).await;
        let parsed = parse_predict_response(&content_type, &body).unwrap();
        assert_eq!(parsed.errors.len(), 1);
        assert!(parsed.outputs.is_empty(), "nothing succeeded");
    }

    /// Port of `utils.parse_input_request`: files attach to the slot named by
    /// their integer filename, JSON null entries are file-only inputs, and
    /// Python's exact 400s come out of an empty batch and a bad filename.
    #[test]
    fn multipart_inputs_map_files_by_index() {
        let inputs = parse_input_request(
            r#"{"inputs": [{"a": 1}, null, "text"]}"#,
            vec![(Some(0), b"f0".to_vec()), (Some(2), b"f2".to_vec())],
        )
        .unwrap();
        assert_eq!(inputs.len(), 3);
        assert_eq!(inputs[0].data, Some(json!({"a": 1})));
        assert_eq!(inputs[0].file, Some(b"f0".to_vec()));
        assert_eq!(inputs[1].data, None, "JSON null -> data None");
        assert_eq!(inputs[1].file, None);
        assert_eq!(inputs[2].data, Some(json!("text")));
        assert_eq!(inputs[2].file, Some(b"f2".to_vec()));

        let none = "Invalid index None in Content-Disposition header";
        for (data, files, want) in [
            (r#"{"inputs": []}"#, vec![], "No inputs provided"),
            (r#"{}"#, vec![], "No inputs provided"),
            (
                r#"{"inputs": [null]}"#,
                vec![(Some(5), b"x".to_vec())],
                "Invalid index 5 in Content-Disposition header",
            ),
            (r#"{"inputs": [null]}"#, vec![(None, b"x".to_vec())], none),
        ] {
            let err = parse_input_request(data, files).unwrap_err();
            assert!(
                format!("{err:?}").contains(want),
                "unexpected error: {err:?}"
            );
        }
    }

    // Round-trip integration: a real axum server and a real worker
    // subprocess, driven by the gateway's real InferenceApiClient.
    use super::super::worker::testing::test_spawn_config;

    /// In-process server over an ephemeral port, echo fixture registry.
    async fn spawn_test_server() -> (Arc<InferioState>, String, tempfile::TempDir) {
        spawn_test_server_with_registry(
            r#"
[external_inputs.test_token]
label = "Test token"
description = "HTTP fixture input"
secret = true
required = false
[external_inputs.test_token.source]
type = "environment"
variable = "INFERIO_HTTP_TEST_TOKEN_XYZ"

[group.echo]
config.impl_class = "echo_test"
[group.echo.inference_ids.test]
metadata.description = "echo fixture"
[group.echo.inference_ids.test.external_inputs.test_token]
"#,
        )
        .await
    }

    /// In-process server with a caller-supplied registry TOML. The server
    /// `default_max_batch` stays high (32) so batching tests prove caps come
    /// from the request; the prewarm pool is disabled.
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
        use super::super::calibration::{CalibrationStore, StoreEnv, StorePaths};

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        fs::write(root.join("registry.toml"), registry_toml).unwrap();
        let registry = Arc::new(StdMutex::new(RegistryCache::new(RegistryConfig {
            config_dirs: vec![root.clone()],
        })));
        // Rooted in the test's own temp dir: no shipped baselines, an empty
        // local file, no debounce.
        let calibration = CalibrationStore::with_debounce(
            StorePaths::beside_registry(std::slice::from_ref(&root), &root.join("data")),
            StoreEnv {
                platform: "windows".to_owned(),
                backend: "cuda".to_owned(),
                generator: "panoptikon test".to_owned(),
            },
            Duration::ZERO,
        );
        let manager = ModelManager::new(
            ManagerConfig {
                spawn: test_spawn_config(),
                default_max_batch: 32,
                sweep_interval: Duration::from_secs(60),
                loads: super::super::manager::LoadPolicy::default(),
                prewarm,
                // Tests must not depend on the host's GPUs.
                gpus: super::super::gpu::GpuInventory::unknown(),
                vram: super::super::ledger::VramBudgets::default(),
                calibration: Some(Arc::clone(&calibration) as Arc<_>),
            },
            Arc::clone(&registry),
        );
        let state = Arc::new(InferioState {
            manager,
            registry,
            compute_caps: super::super::capability::HostComputeCaps::unknown(),
            calibration: Some(calibration),
            // The overlay still needs *a* GPU to answer for, so name one.
            default_gpu_name: Some(TEST_GPU.to_owned()),
        });
        let app = Router::new().nest_service("/api/inference", router(Arc::clone(&state)));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (state, format!("http://{addr}"), dir)
    }

    /// The money test: the REAL gateway client drives the local HTTP surface
    /// end to end against a real worker — metadata, load, three predicts (one
    /// per encoding), `/cache`, `GET /cache/{key}` rendering ttl -1 as
    /// `datetime.max`, and unload. Wire compatibility, proven by the consumer.
    #[tokio::test]
    async fn real_client_roundtrip_against_local_http_service() {
        let (state, base_url, _registry_dir) = spawn_test_server().await;
        let client = InferenceApiClient::new_with_metadata_cache(base_url.clone(), false)
            .expect("client builds");
        let predict = async |inputs: &[InferenceInput]| {
            client
                .predict("echo/test", "key", 10, -1, None, None, inputs)
                .await
                .expect("predict")
        };
        let file = |bytes: &[u8]| {
            InferenceInput::new(JsonValue::Null, Some(InferenceFile::Bytes(bytes.to_vec())))
        };

        let metadata = client.get_metadata().await.expect("metadata");
        let echo = &metadata["echo"]["inference_ids"]["test"];
        assert_eq!(echo["description"], json!("echo fixture"));
        // The whole exchange runs over HTTP/2 cleartext with prior knowledge,
        // against `axum::serve` exactly as the gateway serves it — so this
        // also pins that hyper-util's auto builder accepts the h2 preface.
        assert_eq!(
            client.known_transport(),
            Some(crate::inferio_client::Transport::H2c),
            "the real client and the real service must agree on h2c"
        );

        let inputs = client.get_external_inputs().await.expect("external inputs");
        assert_eq!(
            inputs["definitions"]["test_token"]["configured"],
            json!(false)
        );
        assert_eq!(inputs["models"]["echo/test"][0]["required"], json!(false));
        let loaded = client
            .load_model("echo/test", "key", 10, -1, None)
            .await
            .expect("load");
        assert_eq!(loaded, json!({"status": "loaded"}));

        // Data-only -> JSON outputs. The desired in-flight figure rides on a
        // response header in all three encodings; only the JSON envelope could
        // have carried it in a body.
        let output = predict(&[InferenceInput::new(json!({"text": "hi"}), None)]).await;
        let desired = output
            .desired_in_flight_items
            .expect("the orchestrator published a figure");
        assert!(desired > 0);
        match output.outputs {
            PredictOutput::Json(values) => {
                assert_eq!(values, vec![json!({"echo": {"text": "hi"}})])
            }
            other => panic!("expected Json output, got {other:?}"),
        }

        let one = vec![file(b"abc")];
        let two = vec![file(b"one"), file(b"two")];
        for (inputs, want) in [
            (one, vec![b"echo:abc".to_vec()]),
            (two, vec![b"echo:one".to_vec(), b"echo:two".to_vec()]),
        ] {
            let output = predict(&inputs).await;
            assert_eq!(
                output.desired_in_flight_items,
                Some(desired),
                "octet-stream and multipart/mixed responses carry it too"
            );
            match output.outputs {
                PredictOutput::Binary(outputs) => assert_eq!(outputs, want),
                other => panic!("expected Binary output, got {other:?}"),
            }
        }

        let cached = async || client.get_cached_models().await.expect("cache list");
        assert_eq!(cached().await, json!({"cache": {"echo/test": ["key"]}}));
        // GET /cache/{key} has no client helper.
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
        let unloaded = client
            .unload_model("echo/test", "key")
            .await
            .expect("unload");
        assert_eq!(unloaded, json!({"status": "unloaded"}));
        assert_eq!(cached().await, json!({"cache": {}}));

        state.manager.shutdown().await;
    }

    /// The six renderings that mean the request never reached a model, in the
    /// order [`UNATTEMPTED_REQUEST_MARKERS`] documents them: `Worker::fatal`,
    /// `dispatch::reap_idle_replicas` (which does *not* say "failed fatally"),
    /// `Worker::roundtrip` refusing a poisoned worker, `ModelManager::predict`
    /// with its reply oneshot dropped, the same losing the race with the fatal
    /// arm, and `dispatch`'s `End::Graceful` arm.
    ///
    /// These literals are the coupling: if one of the cited `format!`s
    /// changes, the tests below are what notice.
    fn death_renderings(model: &str) -> Vec<String> {
        vec![
            format!(
                "inferio worker {model}#0 failed fatally: early eof; process status: \
                 signal 9; stderr tail:\nTraceback…"
            ),
            format!("inferio worker for model {model} exited while idle: pid 41 signal 9"),
            format!("inferio worker {model}#1 is dead after a previous fatal error"),
            format!("the dispatcher for model {model} dropped the request"),
            format!("model {model} was unloaded before the request could be queued"),
            format!("model {model} was unloaded"),
        ]
    }

    /// All six classify as "never attempted", so a death's whole blast radius
    /// is re-queued. Untyped on purpose: this is the *fallback* path, what an
    /// error predating the typed marker looks like here.
    #[test]
    fn every_shape_of_a_worker_death_classifies_as_unattempted() {
        let model = "clip/model-a";
        for chain in death_renderings(model) {
            assert_eq!(
                classify_predict_failure(&anyhow!("{chain}"), &chain, model),
                PredictFailure::Unattempted,
                "{chain}"
            );
        }
    }

    /// The typed marker is the primary signal: an `Unattempted` error
    /// classifies as one whatever it says, so a reworded death cannot cost a
    /// window its re-queue.
    #[test]
    fn the_typed_marker_classifies_a_death_whatever_it_renders() {
        let model = "clip/model-a";
        let novel = "the replica evaporated in a way nobody has written a marker for";
        // Untyped, the same text is an ordinary failure, so the typed case is
        // about the type and not the words; the marker survives added
        // `.context` because the downcast walks the whole chain; and the load
        // check still outranks it, so a model that will not come up does not
        // cost every item a second full attempt.
        for (err, want) in [
            (Unattempted::error(novel), PredictFailure::Unattempted),
            (anyhow!("{novel}"), PredictFailure::Other),
            (
                Unattempted::error(novel)
                    .context("inference request failed")
                    .context("endpoint http://localhost:1/api/inference"),
                PredictFailure::Unattempted,
            ),
            (
                Unattempted::error("inferio worker died")
                    .context(format!("failed to load model {model}")),
                PredictFailure::LoadFailed,
            ),
        ] {
            let chain = format!("{err:#}");
            assert_eq!(
                classify_predict_failure(&err, &chain, model),
                want,
                "{chain}"
            );
        }
        // The message passes through byte for byte.
        assert_eq!(format!("{}", Unattempted::error(novel)), novel);
    }

    /// A death whose stderr tail carries the words a *load* failure uses must
    /// still be a death: the tail is a ring of whatever the worker logged
    /// recently, and an unanchored match would cost the window its re-queue.
    #[test]
    fn a_stale_load_line_in_the_stderr_tail_cannot_forge_a_load_failure() {
        let model = "clip/model-a";
        let stale = format!(
            "inferio worker {model}#0 failed fatally: early eof; stderr tail:\n\
             [worker] failed to load model weights from cache, retrying\n[worker] ok"
        );
        // `ensure_loaded`'s context always names the model, so the real thing
        // still wins, including a worker that died *while* loading.
        let real = format!(
            "failed to load model {model}: inferio worker {model}#0 failed fatally: early eof"
        );
        let parity = "failed to load model something-else: nope".to_owned();
        let ordinary = "the worker returned an error".to_owned();
        for (chain, want) in [
            (stale, PredictFailure::Unattempted),
            (real, PredictFailure::LoadFailed),
            (parity, PredictFailure::LoadFailed),
            (ordinary, PredictFailure::Other),
        ] {
            assert_eq!(
                classify_predict_failure(&anyhow!("{chain}"), &chain, model),
                want,
                "{chain}"
            );
        }
    }

    /// The same six renderings carried to the JSON the extraction job reads,
    /// because that is where the re-queue decision is made: a shape that
    /// classified right but answered the wrong body would re-queue nothing.
    #[tokio::test]
    async fn every_shape_of_a_worker_death_reaches_the_job_as_worker_died() {
        let model = "clip/model-a";
        let status = StatusCode::INTERNAL_SERVER_ERROR;
        for rendering in death_renderings(model) {
            let err = Unattempted::error(rendering.clone());
            let response = predict_failure_response(err, model)
                .unwrap_or_else(|api| panic!("{rendering} answered a plain error: {api:?}"));
            assert_eq!(response.status(), status);
            let body = to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body");
            let failure = crate::inferio_client::InferenceFailure::parse(
                reqwest::StatusCode::INTERNAL_SERVER_ERROR,
                None,
                &String::from_utf8_lossy(&body),
            );
            assert!(failure.is_worker_death(), "{rendering} -> {failure:?}");
            assert_eq!(failure.model.as_deref(), Some(model));
            assert_eq!(
                failure.last_error.as_deref(),
                Some(rendering.as_str()),
                "the job records what actually happened"
            );
        }

        // The counterexample: an ordinary predict failure must stay a plain
        // error, or every failed item would be re-submitted for nothing.
        let ordinary = predict_failure_response(anyhow!("the model returned no outputs"), model);
        assert!(ordinary.is_err(), "an ordinary failure is not structured");
    }

    /// A predict whose worker dies mid-request answers the machine-readable
    /// `worker_died` kind, and the gateway's client parses it back into the
    /// typed failure the extraction job keys its re-queue on — both halves in
    /// one round trip.
    #[tokio::test]
    async fn a_worker_death_predict_answers_a_machine_readable_kind() {
        let (_state, base_url, _registry_dir) = spawn_test_server_with_registry(
            r#"
[group.dying]
config.impl_class = "dying_test"
[group.dying.inference_ids.test]
metadata.description = "kills its worker on predict"
"#,
        )
        .await;
        let client =
            InferenceApiClient::new_with_metadata_cache(base_url, false).expect("client builds");
        let err = client
            .predict(
                "dying/test",
                "key",
                1,
                60,
                None,
                Some(false),
                &[InferenceInput::new(json!({"text": "hi"}), None)],
            )
            .await
            .expect_err("the worker exits mid-predict");

        let failure = crate::inferio_client::inference_failure(&err)
            .unwrap_or_else(|| panic!("the client must type a refused predict; got {err:#}"));
        assert_eq!(failure.status, 500, "{failure}");
        assert!(
            failure.is_worker_death() && !failure.is_load_cooldown(),
            "{failure}"
        );
        assert_eq!(failure.model.as_deref(), Some("dying/test"), "{failure}");
        assert_eq!(failure.message, "Prediction failed", "{failure}");
        // The chain the operator greps for is carried through, clamped.
        let last_error = failure.last_error.as_deref().unwrap_or_default();
        assert!(last_error.contains("failed fatally"), "{failure}");
        assert!(
            last_error.len() <= MAX_ERROR_BYTES + 4,
            "{}",
            last_error.len()
        );
    }

    /// The `{"batch": n}` sizes the batchsize_test fixture reports.
    fn reported_batches(output: &PredictOutput) -> Vec<u64> {
        match output {
            PredictOutput::Json(values) => values
                .iter()
                .map(|value| value["batch"].as_u64().expect("fixture reports batch"))
                .collect(),
            other => panic!("batchsize fixture returns JSON outputs, got {other:?}"),
        }
    }

    /// Cap propagation end to end through the job stack: predicts driven
    /// through the real InferencePool carry the extraction job's batch size to
    /// GPU batch formation. A primer keeps the worker busy (the fixture sleeps
    /// 300 ms per batch) while six singles queue behind it; capped at 2 every
    /// reported batch is <= 2 despite the server's own default of 32, and the
    /// uncapped contrast merges past 2 — which is what proves the ceiling came
    /// from the request rather than from timing or config.
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
        // Preload, so the primer is not skewed by worker spawn latency.
        pool.load_model_all("batch/test", "key", 10, -1, None)
            .await
            .expect("load");

        // One primer plus six queued singles, all sharing `max_batch`.
        async fn run_phase(pool: &InferencePool, max_batch: Option<u32>) -> Vec<u64> {
            let one = |pool: InferencePool, index: u64| {
                tokio::spawn(async move {
                    pool.predict(
                        "batch/test",
                        "key",
                        10,
                        -1,
                        max_batch,
                        None,
                        &[InferenceInput::new(json!(index), None)],
                    )
                    .await
                })
            };
            let primer = one(pool.clone(), 0);
            // Let the primer dispatch alone (the worker sleeps 300 ms), so the
            // rest are guaranteed to queue and become mergeable.
            tokio::time::sleep(Duration::from_millis(100)).await;
            let rest: Vec<_> = (1..=6).map(|index| one(pool.clone(), index)).collect();
            let mut batches =
                reported_batches(&primer.await.unwrap().expect("primer predict").outputs);
            for task in rest {
                batches.extend(reported_batches(
                    &task.await.unwrap().expect("queued predict").outputs,
                ));
            }
            batches
        }

        let capped = run_phase(&pool, Some(2)).await;
        assert!(
            capped.iter().all(|&batch| batch <= 2),
            "max_batch=2 caps every GPU batch: {capped:?}"
        );
        let uncapped = run_phase(&pool, None).await;
        assert!(
            uncapped.iter().any(|&batch| batch > 2),
            "without it the queued singles merge past 2: {uncapped:?}"
        );
        state.manager.shutdown().await;
    }

    /// `GET /api/inference/health` returns 200 with the [`HealthReport`]
    /// shape, asserted by serde round-trip into the structs the handler
    /// serialized from: empty manager, then after a real load, then the
    /// standalone router's bare `/health`, which existing probes rely on.
    #[tokio::test]
    async fn health_endpoint_serves_json_shape_over_http() {
        let (state, base_url, _registry_dir) = spawn_test_server().await;
        let health_at = async |url: String| -> HealthReport {
            reqwest::get(url)
                .await
                .expect("health request")
                .json()
                .await
                .expect("health body parses into the HealthReport serde shape")
        };

        let health = health_at(format!("{base_url}/api/inference/health")).await;
        assert_eq!(health.status, "ok");
        assert!(!health.shutting_down);
        assert!(health.registry_ok, "the echo fixture registry parses");
        assert_eq!(health.model_count, 0);
        assert!(health.models.is_empty());
        // The prewarm section round-trips too; the enabled shape is covered by
        // the prewarm test.
        assert!(!health.prewarm.enabled);
        assert!(!health.prewarm.lazy);
        assert!(health.prewarm.warm.is_empty());

        let client = InferenceApiClient::new_with_metadata_cache(base_url.clone(), false)
            .expect("client builds");
        client
            .load_model("echo/test", "key", 10, -1, None)
            .await
            .expect("load");
        let health = health_at(format!("{base_url}/api/inference/health")).await;
        assert_eq!(health.model_count, 1);
        assert_eq!(health.models.len(), 1);
        let model = &health.models[0];
        assert_eq!(model.inference_id, "echo/test");
        assert_eq!(model.cache_keys, vec!["key".to_string()]);
        assert_eq!(model.replicas.total, 1);
        assert_eq!(model.replicas.free, 1, "idle model: replica in the pool");
        assert_eq!(model.queue_depth, 0);
        // Nothing dispatched yet -> null on the wire.
        assert_eq!(model.last_grant_units, None);
        assert_eq!(model.last_window_items, None);
        assert_eq!(model.desired_in_flight_items, None);
        assert_eq!(model.queue_bound_windows, 0);
        assert!(
            health.vram.is_empty(),
            "an unknown GPU inventory means an empty ledger and no admission"
        );

        // The client side is reported too: the endpoint the real client just
        // used, with its transport, connections and gate concurrency.
        let endpoint = health
            .inference_clients
            .iter()
            .find(|entry| entry.base_url.starts_with(&base_url))
            .expect("the endpoint the test client used is reported");
        // No figure published yet, so the gate sits at its floor.
        assert_eq!(endpoint.transport, "h2c");
        let lanes = crate::inferio_client::INFERENCE_CONNECTION_LANES;
        assert_eq!(endpoint.pool_connections, Some(lanes));
        assert_eq!(
            endpoint.max_concurrent_requests,
            crate::inferio_client::INFERENCE_MAX_CONCURRENT_REQUESTS
        );
        assert_eq!(endpoint.in_flight_requests, 0);
        assert_eq!(endpoint.connections_in_use, Some(0));

        // The server side's one peer-movable memory bound: the constants an
        // operator needs to read a 503 against.
        let budget = &health.predict_body_budget;
        assert_eq!(budget.budget_bytes, PREDICT_INFLIGHT_BODY_BYTES as u64);
        assert_eq!(budget.request_limit_bytes, PREDICT_BODY_LIMIT as u64);
        assert!(
            budget.in_flight_bytes <= PREDICT_INFLIGHT_BODY_BYTES as u64,
            "a reservation is returned when its request is answered"
        );

        // Standalone (subcommand) mounting: bare /health, same handler.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let standalone_url = format!("http://{}", listener.local_addr().unwrap());
        let standalone = standalone_router(Arc::clone(&state));
        tokio::spawn(async move {
            axum::serve(listener, standalone).await.unwrap();
        });
        let health = health_at(format!("{standalone_url}/health")).await;
        assert_eq!(health.status, "ok");
        assert_eq!(health.model_count, 1, "same manager, same report");
        state.manager.shutdown().await;
    }

    /// The additive `prewarm` query param end to end: `prewarm=false` on PUT
    /// /load parses and suppresses the lazy warm (the slot insertion is
    /// synchronous, so an empty pool right after is deterministic), an absent
    /// param means true, a non-boolean is a client error rather than a silent
    /// default, POST /predict accepts it through the real client, and the
    /// health report shows the enabled pool's warm entry.
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
        let load = async |extra: &str| {
            http.put(format!(
                "{base_url}/api/inference/load/echo/test\
                 ?cache_key=key&lru_size=10&ttl_seconds=-1{extra}"
            ))
            .send()
            .await
            .unwrap()
        };

        let warm = || !state.manager.prewarm_pool().health().warm.is_empty();
        assert_eq!(load("&prewarm=false").await.status(), 200);
        assert!(!warm(), "prewarm=false suppressed the lazy warm");
        // Absent = true: after an unload, a plain load leaves a lazy slot.
        http.delete(format!("{base_url}/api/inference/cache/key/echo/test"))
            .send()
            .await
            .unwrap();
        assert_eq!(load("").await.status(), 200);
        assert!(warm(), "absent hint means true: the lazy slot exists");
        assert_eq!(load("&prewarm=true").await.status(), 200);
        let rejected = load("&prewarm=banana").await;
        assert!(
            rejected.status().is_client_error(),
            "a non-boolean prewarm value is rejected, got {}",
            rejected.status()
        );

        // predict accepts the param via the real client, and still answers.
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
        match output.outputs {
            PredictOutput::Json(values) => assert_eq!(values, vec![json!({"echo": 1})]),
            other => panic!("expected Json output, got {other:?}"),
        }

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

    /// Settings whose registry resolves to nothing: `python/inferio/config`
    /// does not exist relative to the crate CWD, so `from_settings` takes its
    /// degraded path and serves an empty registry. Cheap, and it never spawns
    /// a worker, so tests that stop before the manager can use it.
    fn registryless_settings() -> crate::config::Settings {
        assert!(
            !std::path::Path::new("python/inferio/config").is_dir(),
            "test premise: the built-in config dir is absent from the crate CWD"
        );
        // Only `server` and `upstreams` have no serde default; everything
        // else, `inference_local` included, takes its shipped one.
        let mut settings: crate::config::Settings = toml::from_str(
            r#"
[server]
host = "127.0.0.1"
port = 0
[upstreams.ui]
base_url = "http://127.0.0.1:6339"
[upstreams.api]
base_url = "http://127.0.0.1:6342"
"#,
        )
        .expect("the minimal settings parse");
        settings.inference_local.enabled = true;
        settings
    }

    /// A missing built-in registry config dir must not hard-fail gateway boot:
    /// `from_settings` degrades to a working, empty registry, matching
    /// Python's warn-not-fail posture.
    #[tokio::test]
    async fn from_settings_degrades_when_builtin_config_dir_is_missing() {
        let state = InferioState::from_settings(&registryless_settings())
            .expect("missing built-in config dir degrades instead of failing boot");
        let registry = state
            .registry
            .lock()
            .unwrap()
            .get()
            .expect("empty registry loads");
        assert!(registry.groups.is_empty());
        state.manager.shutdown().await;
    }

    /// `/metadata` carries the calibration overlay: what the store knows about
    /// each priced model on the GPU it would load on. Additive and read-only,
    /// and absent for a `none`-class model, which is never priced.
    #[tokio::test]
    async fn metadata_carries_the_calibration_overlay() {
        use super::super::calibration::{CalibrationProfiles, ProfileUpdate};

        let (state, base_url, _registry_dir) = spawn_test_server_with_registry(
            r#"
[group.echo]
config.impl_class = "echo_test"
[group.echo.metadata.cost]
unit = "item"
aggregation = "count"
epoch = 1
seed_units = 8

[group.echo.inference_ids.test]
metadata.description = "echo fixture"

[group.echo.inference_ids.remote]
metadata.cost.unit = "none"
"#,
        )
        .await;
        state.calibration.as_ref().unwrap().record(ProfileUpdate {
            inference_id: "echo/test".to_owned(),
            epoch: 1,
            gpu_name: TEST_GPU.to_owned(),
            torch: "2.7.1+cu128".to_owned(),
            dtype: "fp16".to_owned(),
            unit: "item",
            aggregation: "count",
            base_mb: 4321,
            base_method: Some("nvml".to_owned()),
            dtype_method: Some("selected".to_owned()),
            slope_mb_per_unit: 0.79,
            residual_mb: 96.0,
            samples: 38,
            knee_units: Some(512),
            knee_withdrawn: false,
            max_units_measured: 1024,
            local_samples: 12,
            knee_clean_windows: 0,
            ring: Vec::new(),
        });

        let metadata: JsonValue = reqwest::get(format!("{base_url}/api/inference/metadata"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let ids = &metadata["echo"]["inference_ids"];
        let calibrated = &ids["test"]["calibration"];
        for (field, want) in [
            ("status", json!("local")),
            ("gpu", json!(TEST_GPU)),
            ("dtype", json!("fp16")),
            ("base_mb", json!(4321)),
            ("slope_mb_per_unit", json!(0.79)),
            ("samples", json!(38)),
            ("local_samples", json!(12)),
            ("max_units_measured", json!(1024)),
            ("knee_units", json!(512)),
        ] {
            assert_eq!(calibrated[field], want, "{field}");
        }
        assert_eq!(
            ids["test"]["description"],
            json!("echo fixture"),
            "the registry metadata itself is untouched"
        );
        assert!(
            ids["remote"].get("calibration").is_none(),
            "a none-class model is never priced, so it is never calibrated"
        );
        state.manager.shutdown().await;
    }

    // The predict handler must read the request body to its end: see
    // docs/inferio-transport.md, "The buffered multipart extractor".

    const PROBE_BOUNDARY: &str = "0123456789abcdef-0123456789abcdef";

    /// A well-formed predict body chunked the way reqwest chunks one (the
    /// closing boundary is its own chunk, the end of the stream a further
    /// poll), reporting whether it was read to its end.
    fn probe_body_after(tail_delay: Duration) -> (Body, Arc<std::sync::atomic::AtomicBool>) {
        use std::sync::atomic::Ordering;

        const BOUNDARY: &str = PROBE_BOUNDARY;
        let head = format!(
            "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"data\"\r\n\r\n\
             {{\"inputs\":[null]}}\r\n\
             --{BOUNDARY}\r\nContent-Disposition: form-data; name=\"files\"; \
             filename=\"0\"\r\nContent-Type: application/octet-stream\r\n\r\n{}\r\n",
            "x".repeat(4096)
        );
        let drained = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let chunks = vec![
            axum::body::Bytes::from(head),
            axum::body::Bytes::from(format!("--{BOUNDARY}--\r\n")),
        ];
        let stream = futures_util::stream::unfold(
            (chunks.into_iter(), Arc::clone(&drained)),
            move |(mut chunks, drained)| async move {
                match chunks.next() {
                    Some(chunk) => Some((
                        Ok::<axum::body::Bytes, std::io::Error>(chunk),
                        (chunks, drained),
                    )),
                    None => {
                        // Polled past the last chunk: only reachable by
                        // reading the body to its end. The delay models a real
                        // h2 client's terminal DATA frame.
                        tokio::time::sleep(tail_delay).await;
                        drained.store(true, Ordering::SeqCst);
                        None
                    }
                }
            },
        );
        (Body::from_stream(stream), drained)
    }

    /// The handler must poll the request body past its last chunk, or hyper
    /// resets the h2 stream on every predict and 1 024 of those GOAWAY the
    /// connection. Asserted at the body, so it needs no socket, but through
    /// the real router and handler.
    #[tokio::test]
    async fn a_predict_reads_the_request_body_to_its_end() {
        use std::sync::atomic::Ordering;
        use tower::ServiceExt as _;

        // The "before": the streamed parse this handler used to do. multer
        // answers `None` at the closing boundary and never polls past it.
        let (body, streamed_drain) = probe_body_after(Duration::from_millis(200));
        let request = axum::http::Request::builder()
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={PROBE_BOUNDARY}"),
            )
            .body(body)
            .unwrap();
        let mut streamed = Multipart::from_request(request, &())
            .await
            .expect("the boundary parses");
        let streamed_parse = async {
            while let Some(field) = streamed.next_field().await.expect("the body parses") {
                let _ = field.bytes().await.expect("the field reads");
            }
        };
        let finished_early = tokio::time::timeout(Duration::from_millis(20), streamed_parse)
            .await
            .is_ok();
        assert!(
            finished_early && !streamed_drain.load(Ordering::SeqCst),
            "test premise: a streamed multipart parse answers at the closing \
             boundary and never waits for the end of the request stream"
        );

        // The "after": the shipped handler on an identical body. The missing
        // model makes it fail after the body, and nothing spawns.
        let (body, drained) = probe_body_after(Duration::from_millis(200));
        let state = InferioState::from_settings(&registryless_settings()).expect("state builds");
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/predict/nope/model?cache_key=k&lru_size=1&ttl_seconds=-1")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={PROBE_BOUNDARY}"),
            )
            .body(body)
            .unwrap();
        let response = router(Arc::clone(&state))
            .oneshot(request)
            .await
            .expect("the router answers");
        assert!(
            drained.load(Ordering::SeqCst),
            "the handler answered without reading the request body to its end"
        );
        // And the body it read parsed: what is left is the missing model.
        let (content_type, body) = split_response(response).await;
        assert!(content_type.contains("application/json"), "{content_type}");
        let detail = String::from_utf8_lossy(&body);
        assert!(
            !detail.contains("invalid multipart body"),
            "a well-formed body must not be reported as a malformed one: {detail}"
        );
        state.manager.shutdown().await;
    }

    /// `/health` names the **effective** pixel canvas a model's grants are
    /// priced under: the same unit budget describes a very different batch
    /// depending on whether one is in force, and the one in force may be a
    /// canvas the registry never stated.
    #[test]
    fn the_health_cost_section_names_the_effective_canvas() {
        use super::super::cost::{CostAggregation, CostDimension, CostUnit};
        use super::super::manager::CostHealth;

        let uncapped = CostDimension {
            unit: CostUnit::Item,
            aggregation: Some(CostAggregation::Count),
            epoch: 1,
            seed_units: Some(8),
            degraded: false,
            canvas_pixels: None,
        };
        assert_eq!(CostHealth::from(uncapped).canvas_pixels, None);
        let canvassed = CostDimension {
            unit: CostUnit::Pixel,
            aggregation: Some(CostAggregation::Sum),
            canvas_pixels: Some(1_835_008),
            ..uncapped
        };
        assert_eq!(
            CostHealth::from(canvassed).canvas_pixels,
            Some(1_835_008),
            "the canvas the grants are actually priced under"
        );
    }

    /// The two bounds on a predict body, and the three answers they produce.
    ///
    /// **Per request**: a whole body that is too big is neither malformed nor
    /// never-arrived but its own answer — `413` — so a caller sends a smaller
    /// batch instead of re-submitting forever. **Process-wide**: the aggregate
    /// budget answers a typed `503` rather than waiting, because a wait would
    /// hold the request stream open. Both are driven through
    /// [`collect_within`] with small limits and a budget of the test's own —
    /// exhausting the process-wide one would refuse every other test's
    /// predicts — with the semaphore the only substitution; the shipped
    /// constants are checked at the end.
    #[tokio::test]
    async fn the_predict_body_budget_is_a_process_wide_ceiling_that_answers_503() {
        let chunked = |sizes: [usize; 2]| {
            let chunks: Vec<_> = sizes
                .map(|size| Ok::<_, std::io::Error>(axum::body::Bytes::from(vec![b'x'; size])))
                .into_iter()
                .collect();
            Body::from_stream(futures_util::stream::iter(chunks))
        };
        let whole = |size| Body::from(vec![b'x'; size]);
        let over = collect_within(whole(64), 32, &PREDICT_BODY_BYTES).await;
        assert!(matches!(over, Err(PredictBodyError::TooLarge)));
        assert_eq!(
            PredictBodyError::TooLarge.into_response().status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
        let exact = collect_within(whole(32), 32, &PREDICT_BODY_BYTES).await;
        assert!(
            matches!(&exact, Ok((bytes, _)) if bytes.len() == 32),
            "a body exactly at the limit is not over it"
        );
        // A body that declares no length is charged and bounded as it arrives,
        // so the limit depends on no peer being honest about a length.
        let undeclared = collect_within(chunked([24, 24]), 32, &PREDICT_BODY_BYTES).await;
        assert!(
            matches!(undeclared, Err(PredictBodyError::TooLarge)),
            "an undeclared body is bounded by what arrives, not by what it claims"
        );
        let in_pieces = collect_within(chunked([16, 16]), 32, &PREDICT_BODY_BYTES).await;
        assert!(
            matches!(in_pieces, Ok((bytes, _)) if bytes.len() == 32),
            "and it is not refused for arriving in pieces"
        );

        // Leaked for the `'static` a reservation outlives its request with.
        let budget: &'static tokio::sync::Semaphore =
            Box::leak(Box::new(tokio::sync::Semaphore::new(64)));
        let mut hog = BodyReservation::new(budget);
        hog.reserve(56)
            .expect("an empty budget admits a whole body");
        assert_eq!(budget.available_permits(), 8);

        // 8 bytes still fit; the ninth does not, and is refused rather than
        // waited on — this test would hang instead of failing if it waited.
        let refusals_before = predict_body_budget_health().refused_requests;
        let (fits, _) = collect_within(whole(8), PREDICT_BODY_LIMIT, budget)
            .await
            .expect("a body inside the remaining budget is read");
        assert_eq!(fits.len(), 8);
        let refused = collect_within(whole(9), PREDICT_BODY_LIMIT, budget).await;
        assert!(matches!(refused, Err(PredictBodyError::Overloaded)));
        assert!(
            predict_body_budget_health().refused_requests > refusals_before,
            "a refusal is counted where an operator can see it"
        );

        // A 503 with a retry delay and the kind `is_unattempted()` reads.
        let response = PredictBodyError::Overloaded.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get(header::RETRY_AFTER).unwrap(),
            "1",
            "a temporarily-full server owes the caller a delay"
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let failure = crate::inferio_client::InferenceFailure::parse(
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            Some(1),
            &String::from_utf8_lossy(&body),
        );
        assert_eq!(failure.kind.as_deref(), Some(BODY_BUDGET_KIND));
        assert!(failure.is_unattempted());
        assert!(
            !failure.is_load_cooldown(),
            "and it is not the one 503 that must never be retried"
        );
        drop(hog);
        assert_eq!(
            budget.available_permits(),
            64,
            "every reservation is returned by Drop, on every path"
        );

        // The report is the semaphore's own state, asserted on the mapping
        // because the real budget is shared with every other test here.
        let health = budget_health(PREDICT_INFLIGHT_BODY_BYTES - 4096, 7);
        let (budget_bytes, limit) = (
            PREDICT_INFLIGHT_BODY_BYTES as u64,
            PREDICT_BODY_LIMIT as u64,
        );
        assert_eq!(health.budget_bytes, budget_bytes);
        assert_eq!(health.request_limit_bytes, limit);
        assert_eq!(health.in_flight_bytes, 4096);
        assert_eq!(health.refused_requests, 7);
        assert!(predict_body_budget_health().in_flight_bytes <= budget_bytes);

        // The shipped ceiling is the orchestrator's own frame wall, so a body
        // it refuses could never have become a worker frame; and two maximal
        // bodies fit the budget, so a maximal request is never refused for as
        // long as another maximal one is in flight.
        assert_eq!(PREDICT_BODY_LIMIT, MAX_FRAME_BYTES);
        const { assert!(crate::inferio::worker::FRAME_INPUT_BYTES_BUDGET < PREDICT_BODY_LIMIT) };
        const { assert!(PREDICT_INFLIGHT_BODY_BYTES >= 2 * PREDICT_BODY_LIMIT) };
    }

    /// The split that decides whether the caller re-submits, through the real
    /// router: a body that stops before its closing delimiter is *incomplete*
    /// (nothing was parsed, and the detail carries the cause out from under
    /// axum's fixed sentence), while a whole body that is simply not a
    /// multipart is an ordinary bad request.
    #[tokio::test]
    async fn a_malformed_predict_body_names_its_cause() {
        use tower::ServiceExt as _;

        let state = InferioState::from_settings(&registryless_settings()).expect("state builds");
        let post = async |body: &'static str| {
            let request = axum::http::Request::builder()
                .method("POST")
                .uri("/predict/nope/model?cache_key=k&lru_size=1&ttl_seconds=-1")
                .header(
                    header::CONTENT_TYPE,
                    "multipart/form-data; boundary=BOUNDARY",
                )
                .body(Body::from(body))
                .unwrap();
            let response = router(Arc::clone(&state))
                .oneshot(request)
                .await
                .expect("the router answers");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let (_content_type, body) = split_response(response).await;
            String::from_utf8_lossy(&body).to_string()
        };
        use crate::inferio_client::InferenceFailure;
        let parse =
            |detail: &str| InferenceFailure::parse(reqwest::StatusCode::BAD_REQUEST, None, detail);

        // The `data` field never terminates, so the failure is multer's
        // IncompleteFieldData through `Field::text`.
        let truncated =
            post("--BOUNDARY\r\nContent-Disposition: form-data; name=\"data\"\r\n\r\n{").await;
        assert!(
            truncated.contains("invalid data field"),
            "the prose the operator already knows: {truncated}"
        );
        assert!(
            truncated.contains("incomplete data"),
            "and the cause underneath it: {truncated}"
        );
        let failure = parse(&truncated);
        assert!(
            failure.is_request_incomplete() && failure.is_unattempted(),
            "a body that stops before its closing delimiter is an unattempted \
             request, not a verdict on the media: {truncated}"
        );

        // Whole body, closing delimiter and all, but the part's headers are
        // not headers.
        let complete = post("--BOUNDARY\r\n\u{1}not a header\r\n\r\nx\r\n--BOUNDARY--\r\n").await;
        assert!(
            complete.contains("invalid multipart body"),
            "the prose the operator already knows: {complete}"
        );
        assert!(
            !parse(&complete).is_unattempted(),
            "a whole body that is not a multipart is a bad request, and \
             re-submitting it would only fail again: {complete}"
        );
        state.manager.shutdown().await;
    }

    /// The split above is only as good as the boundary it reads out of the
    /// header, and it has to be the one *multer* read or the verdict is about
    /// a body nobody sent — so each shape is asserted against
    /// `multer::parse_boundary`'s own answer too.
    #[test]
    fn the_declared_boundary_is_the_one_the_parser_used() {
        fn multer_boundary(content_type: &str) -> Option<String> {
            // multer 3.1 `parse_boundary`, inlined: not re-exported by axum,
            // and the point is to reproduce it exactly.
            let mime = content_type.parse::<mime_guess::mime::Mime>().ok()?;
            if mime.type_() != mime_guess::mime::MULTIPART
                || mime.subtype() != mime_guess::mime::FORM_DATA
            {
                return None;
            }
            Some(
                mime.get_param(mime_guess::mime::BOUNDARY)?
                    .as_str()
                    .to_owned(),
            )
        }

        // The parameter name is case-insensitive; `=` is not a token
        // character, so an unquoted value carrying one is not a parameter at
        // all; a quoted boundary (the only way to send one with a space in it)
        // arrives unquoted, which is the form multer looks for in the body;
        // and `mime` rejects whitespace around the `=`.
        for (content_type, expected) in [
            ("multipart/form-data; boundary=abc-123", Some("abc-123")),
            ("multipart/form-data; BOUNDARY=abc-123", Some("abc-123")),
            ("multipart/form-data; boundary=a=b", None),
            ("multipart/form-data; boundary=\"a b\"", Some("a b")),
            ("multipart/form-data ; boundary = abc", None),
            ("multipart/form-data", None),
            ("multipart/form-data; boundary=", None),
        ] {
            assert_eq!(
                multipart_boundary(content_type),
                expected.map(str::to_owned),
                "boundary of {content_type:?}"
            );
            assert_eq!(
                multipart_boundary(content_type),
                multer_boundary(content_type).filter(|value| !value.is_empty()),
                "this file and the parser must read {content_type:?} the same way"
            );
        }
        // And what it finds is what the closing delimiter uses.
        assert!(body_carries_closing_delimiter(
            b"--abc\r\nx\r\n--abc--\r\nepilogue",
            "abc"
        ));
        assert!(!body_carries_closing_delimiter(
            b"--abc\r\nx\r\n--abc\r\n",
            "abc"
        ));
        assert!(!body_carries_closing_delimiter(b"", "abc"));
    }
}
