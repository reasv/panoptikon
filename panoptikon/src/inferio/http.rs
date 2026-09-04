//! HTTP surface of the local inferio orchestrator: a wire-compatible port
//! of the legacy Python `inferio/router.py` + `inferio/utils.py`
//! (python-legacy branch; design doc §7).
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
//! - predict response, additive: a batch containing a typed per-item error
//!   slot always uses the JSON envelope, with those slots rendered as
//!   `{"__error__": {"class": ..., "message": ...}}` (see
//!   `super::slot_error` and the worker-protocol doc). Batches without error
//!   slots keep the byte-identical Python encoding above.
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
use utoipa::{IntoParams, OpenApi, ToSchema};

use super::manager::{HealthReport, ManagerConfig, ModelManager};
use super::prewarm::PrewarmConfig;
use super::registry::{RegistryCache, RegistryConfig};
use super::worker::{WorkerDeadlines, WorkerInput, WorkerOutput, WorkerSpawnConfig};
use crate::api_error::ApiError;
use crate::config::Settings;

/// Python renders "never expires" as `datetime.max.isoformat()`.
const NEVER_EXPIRES: &str = "9999-12-31T23:59:59.999999";

/// Response header carrying the orchestrator's desired in-flight figure, in
/// **items**, for the model that just answered (test protocol §8 G7, brief
/// (b); `docs/inferio-worker-protocol.md`, "Desired in-flight items").
///
/// A header rather than a body field because a predict answers in three
/// encodings — raw `application/octet-stream`, `multipart/mixed` and the JSON
/// `{"outputs": ...}` envelope — and only one of them has anywhere to put a
/// scalar. A header is additive in all three, is ignored by every existing
/// client, and is absent from a Python-era inference server, which is exactly
/// the "no opinion" case the caller must already handle.
///
/// It is a *response* header, so the policy layer's inbound
/// `x-panoptikon-*` strip (`policy.rs`) does not touch it.
pub(crate) const DESIRED_IN_FLIGHT_HEADER: &str = "x-panoptikon-desired-in-flight-items";

/// Machine-readable `kind` of a predict that failed because the inference
/// **worker process died** while the request was in flight, as opposed to
/// the model failing to load or the worker answering with an error.
///
/// It exists because the blast radius of one death is a whole in-flight
/// window (run1 finding F7: 1 542 items lost to a single death, on a job that
/// still reported *completed*), and the only sane response — re-queue the
/// window's items once rather than record them as errors — needs the caller
/// to be able to *tell*. Matching on the human message would put a job's
/// retry policy at the mercy of a log string, so the kind rides in the body.
pub(crate) const WORKER_DIED_KIND: &str = "worker_died";

/// Every rendering that means **this predict never reached a model**, so the
/// request's items are untouched and re-submitting them is the correct answer.
///
/// This is the condition [`WORKER_DIED_KIND`] actually names — the kind is
/// named for the case that produces it in practice, but what it *asserts* is
/// only that the request never reached a model, which is the thing a caller
/// can act on. One worker death does not produce one error: it produces five
/// *different* strings depending on where each affected request was standing
/// when the model went down, and only the first of them says "failed fatally".
/// Matching that one alone — which is what this handler did before — left most
/// of a death's blast radius classified as an ordinary prediction failure,
/// which is run1 finding F7 in miniature: the items were recorded as errors
/// even though nothing had been attempted.
///
/// Each entry is cited to the single place that formats it. **If one of those
/// renderings changes, the unit tests below fail** — they assert on the exact
/// literals, so the coupling is checked rather than hoped for.
///
/// None of this is how it should work. `ModelManager::predict` answers an
/// `anyhow` chain with no typed marker on it — `WorkerError` is the *non*-fatal
/// per-request error, and a fatal teardown is a bare `anyhow!` — so text is the
/// only signal that survives to this layer on every path. The principled fix is
/// an `Unattempted` marker attached with `anyhow::Error::new` at each of the
/// sites below and downcast here, exactly as R9's cooldown already does
/// ([`load_cooldown_response`] downcasts `LoadCooldownError` two lines above
/// this check). That fix has to be made in `worker.rs`, `dispatch.rs` and
/// `manager.rs`; until it is, this list is the bridge, and the downcast should
/// be added *before* it rather than replacing it, so an older rendering still
/// classifies.
const UNATTEMPTED_REQUEST_MARKERS: [&str; 5] = [
    // `Worker::fatal` (`worker.rs`): the window that was executing on the
    // replica that died, and — re-raised verbatim by `dispatch::fail_requests`
    // — every request still queued behind it.
    "failed fatally",
    // `dispatch::reap_idle_replicas`: the liveness sweep found an idle
    // replica's process gone, so the whole model is taken down and the queue
    // is failed with this instead. Never contains "failed fatally".
    "exited while idle",
    // `Worker::roundtrip` refusing to write to an already-poisoned worker.
    "is dead after a previous fatal error",
    // `ModelManager::predict`, when its reply oneshot is dropped without an
    // answer. This is how a window running on a *surviving* replica learns
    // that a sibling died: `dispatch`'s `End::Fatal` arm calls
    // `in_flight.shutdown()`, which aborts those window tasks and drops their
    // senders, so the fatal message never reaches them at all.
    "dropped the request",
    // Two sites, one substring. `ModelManager::predict` when `tx.send` fails
    // ("model {id} was unloaded before the request could be queued"), and
    // `dispatch`'s `End::Graceful` arm failing the queue ("model {id} was
    // unloaded").
    //
    // The first of those *is* a death: the fatal arm calls `rx.close()` and
    // then the dispatch task ends, so every request that reaches `tx.send` a
    // moment late — the tail of the same window — gets this instead of the
    // fatal message. The second is a real unload (an explicit one, or the
    // manager shutting down), which is not a death but is the same fact about
    // the request: it never reached a model, its items are untouched, and one
    // re-submission is the correct answer (the next predict reloads the
    // model). Both are bounded by the caller's one-retry budget either way.
    "was unloaded",
];

/// The context `ModelManager::ensure_loaded` puts on a load failure, minus the
/// model id it interpolates.
const LOAD_FAILURE_MARKER: &str = "failed to load model";

/// What a failed `ModelManager::predict` was, as far as the wire is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PredictFailure {
    /// The model could not be brought up. router.py's `Failed to load model`.
    LoadFailed,
    /// The request never reached a model — [`UNATTEMPTED_REQUEST_MARKERS`].
    Unattempted,
    /// Anything else: the model ran and the attempt failed.
    Other,
}

/// Classify a failed predict from its rendered `anyhow` chain.
///
/// Pure, so the coupling to four other modules' message formats is pinned by
/// unit tests rather than by a live worker death per case.
///
/// **The load check keeps its precedence**, deliberately: a worker that dies
/// *while loading* renders both, and re-queueing is the answer to an
/// established worker dying mid-window, not to a model that will not come up
/// (which R9's cooldown answers). Getting that order wrong would make every
/// dies-on-load model cost each item a second full attempt.
///
/// But it is checked **anchored on this model's id** first, because the chain
/// this reads includes a fatal error's *stderr tail* — a ring buffer of
/// whatever the worker logged over its recent life. An unanchored
/// `contains("failed to load model")` therefore lets a line a model's own code
/// printed minutes ago silently reclassify a real mid-window death as a load
/// failure, costing the window its re-queue. `ensure_loaded` always renders
/// `failed to load model {inference_id}`, so the anchored form is exact, and a
/// worker would have to print this gateway's own context string *with this
/// model's id* to forge it. The unanchored form is still honoured last, so
/// every chain that answered `Failed to load model` before still does
/// (router.py parity) unless a death marker outranks it.
fn classify_predict_failure(chain: &str, full_id: &str) -> PredictFailure {
    if chain.contains(&format!("{LOAD_FAILURE_MARKER} {full_id}")) {
        return PredictFailure::LoadFailed;
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

/// The `{"detail": …}` body of an inference error, in the two shapes this
/// surface answers with.
///
/// The string form is the original one and stays byte-identical for every
/// failure that already had a detail string (router.py parity, see the module
/// docs). The object form is additive and exists for the failures a *caller*
/// has to act on differently — today a worker death (a job re-queues) and,
/// for the load-failure cooldown, an "unavailable until" answer — so the
/// machine-readable half never has to be recovered by pattern-matching prose.
#[derive(serde::Serialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum InferenceErrorDetail {
    /// router.py's plain detail strings. Never constructed here — the plain
    /// failures keep going through [`crate::api_error::ErrorBody`], byte for
    /// byte — but it is half of the wire contract and half of the generated
    /// client's type, so it is documented rather than inferred from absence.
    #[allow(dead_code)]
    Message(String),
    /// A failure with a machine-readable [`InferenceErrorDetail::Message`]
    /// replacement: `kind` names it, the rest is per-kind context.
    Structured(InferenceErrorFields),
}

/// The fields a structured [`InferenceErrorDetail`] can carry. One flat,
/// wholly-optional-but-`kind` struct rather than a variant per kind: every
/// consumer dispatches on `kind` first, and a single shape keeps the wire
/// contract (and the generated client type) from growing a case per failure
/// mode.
#[derive(serde::Serialize, ToSchema, Default)]
pub(crate) struct InferenceErrorFields {
    /// What went wrong, as a stable token: [`WORKER_DIED_KIND`], or
    /// `load_cooldown` for the per-model load-failure backoff.
    pub kind: String,
    /// Human-readable summary; the string the plain form would have carried.
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

/// The body every inference error path serializes. Same `{"detail": …}`
/// envelope as [`crate::api_error::ErrorBody`]; only the detail is allowed to
/// be an object.
#[derive(serde::Serialize, ToSchema)]
pub(crate) struct InferenceErrorBody {
    pub detail: InferenceErrorDetail,
}

/// Build an error response whose detail is the structured form. Kept beside
/// the types so every structured failure — this file's worker death, and the
/// load-failure cooldown — answers in exactly one shape.
pub(crate) fn structured_error(status: StatusCode, fields: InferenceErrorFields) -> Response {
    (
        status,
        Json(InferenceErrorBody {
            detail: InferenceErrorDetail::Structured(fields),
        }),
    )
        .into_response()
}

/// Shared state of the local inference service: the model manager plus the
/// mtime-cached registry used by `/metadata`.
pub struct InferioState {
    pub manager: Arc<ModelManager>,
    pub registry: Arc<StdMutex<RegistryCache>>,
    /// Probed once at startup; drives the `/metadata` availability overlay.
    pub compute_caps: super::capability::HostComputeCaps,
    /// Calibration profiles (shipped baselines + the local store), for the
    /// `/metadata` calibration overlay. The ledger holds the same store.
    pub calibration: Option<Arc<super::calibration::CalibrationStore>>,
    /// Model name of the board a model would load on by default — the one
    /// board the calibration overlay can answer for unambiguously. `None` on
    /// a host with no GPU inventory, where the overlay is omitted entirely.
    pub default_gpu_name: Option<String>,
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
                    config_dirs: vec![std::path::PathBuf::from("config/inference")],
                }
            })
        } else {
            RegistryConfig {
                config_dirs: local.config_dirs.clone(),
            }
        };
        // Shipped calibration baselines live in a `calibration/` subdirectory
        // of each registry dir (the registry loader itself never recurses).
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
        // sentinel), not a re-probe of the hardware: config `auto` on a
        // host with /opt/rocm must not inject HIP paths into a venv that
        // was deliberately synced as cpu/cuda. Config resolution remains
        // the fallback for user-managed interpreters and legacy venvs,
        // which have no sentinel. The same answer is the `backend`
        // component of every calibration profile key — a profile measured
        // against ROCm wheels says nothing about a CUDA build.
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
            // The pin *variable* follows the same resolved accelerator as the
            // worker env and the profile keys — not the inventory: a ROCm
            // host whose inventory came back unknown (ambient restriction,
            // probe failure) still writes the operator's registry pin into
            // HIP's own variable, where an index is the only thing that means
            // anything — so `resolve_pin` canonicalises numeric pins there,
            // drops anything HIP could not read as an index, and writes no
            // pin at all under a HIP-layer ambient restriction
            // (docs/rocm-batch-calibration-parity.md, D2).
            pin_env_var: super::gpu::pin_env_var(accelerator),
        };
        // One probe answers both hardware questions: which boards exist
        // (worker→GPU pinning, the per-GPU ledger) and what they can do (the
        // /metadata availability overlay). Probed once at startup, against
        // the interface the installed wheels actually talk to — the same
        // resolved accelerator the worker env and profile keys use, so a
        // ROCm host is never asked about NVIDIA boards or vice versa.
        let host = super::gpu::probe(accelerator);
        // The calibration store: shipped baselines beside the registry, the
        // generated file in the data folder. The environment half of every
        // profile key is resolved once, here — it cannot change while the
        // process runs, and a caller that got it wrong would mis-key every
        // profile it wrote.
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
        let default_gpu_name = host.inventory.default_board_name();
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
    /// Desktop management uses this directly so an explicitly configured
    /// remote primary upstream cannot be mistaken for the local instance.
    pub fn external_inputs_json(&self) -> Result<JsonValue> {
        self.registry
            .lock()
            .unwrap()
            .get()
            .and_then(|registry| registry.external_inputs_json())
    }
}

/// `[inference_local.vram]` → the ledger's budget table.
///
/// One shape change across the seam: the config expresses a per-board override
/// as "fields that may be absent, meaning inherit", while the ledger wants a
/// resolved [`VramBudget`] per board. Resolving here rather than in the ledger
/// keeps the inheritance rule in one place — `VramConfig::for_board` — and
/// keeps the ledger's hot path a plain map lookup.
fn vram_budgets(config: &crate::config::VramConfig) -> super::ledger::VramBudgets {
    let (margin, cap_fraction) = (config.margin, config.cap_fraction);
    let mut budgets = super::ledger::VramBudgets::uniform(super::ledger::VramBudget {
        margin,
        cap_fraction,
    });
    for uuid in config.gpu.keys() {
        let (margin, cap_fraction) = config.for_board(uuid);
        budgets = budgets.with_board(
            uuid.clone(),
            super::ledger::VramBudget {
                margin,
                cap_fraction,
            },
        );
    }
    budgets
}

/// The `backend` component of a calibration profile key: which torch build
/// the measurements were taken against. `Auto` reaching this point means
/// resolution failed outright (a validation error), and `cpu` is the label
/// that promises the least.
///
/// Apple Silicon keys as `mps`, which is the split this comment used to
/// reserve: the wheels are the same default-PyPI ones a macOS `cpu` host
/// installs, but the measurements are of a Metal device against a
/// unified-memory budget and describe nothing a CPU-wheel box would measure
/// (docs/unified-memory-admission.md, "Calibration keying summary"). Nothing
/// on a Mac ever registered with the ledger before that design, so no
/// existing profile changes meaning.
fn accelerator_backend(accelerator: crate::config::Accelerator) -> &'static str {
    match accelerator {
        crate::config::Accelerator::Cuda => "cuda",
        crate::config::Accelerator::Rocm => "rocm",
        crate::config::Accelerator::Mps => "mps",
        crate::config::Accelerator::Cpu | crate::config::Accelerator::Auto => "cpu",
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
        .route("/external-inputs", get(get_external_inputs))
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

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct LoadParams {
    cache_key: String,
    lru_size: i64,
    ttl_seconds: i64,
    /// Additive over Python: lazy prewarm hint (design §8). Absent = true;
    /// `prewarm=false` suppresses keeping a warm worker of this model's
    /// impl class after the load.
    prewarm: Option<bool>,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct PredictParams {
    cache_key: String,
    lru_size: i64,
    ttl_seconds: i64,
    /// Additive over Python: per-request cap on dispatch-time batch merging.
    max_batch: Option<u32>,
    /// Additive over Python: lazy prewarm hint, as on load (absent = true).
    prewarm: Option<bool>,
}

// ----------------------------------------------------------------------
// Doc-only OpenAPI shapes. The predict request/response wire formats are
// hand-rolled above the serde layer (multipart parsing, three response
// encodings), so these structs exist purely to document them — none of
// them are ever (de)serialized by the handlers.
// ----------------------------------------------------------------------

/// A raw binary payload (schema: string, format binary).
#[derive(ToSchema)]
#[schema(value_type = String, format = Binary)]
struct BinaryBlob(#[allow(dead_code)] String);

/// Multipart form body of `POST /predict/{group}/{inference_id}`.
#[derive(ToSchema)]
#[allow(dead_code)]
struct InferencePredictRequest {
    /// JSON string of the batch: `{"inputs": [...]}` where each entry is an
    /// object, a string, or null (null = file-only input).
    data: String,
    /// Binary batch inputs. Each part's *filename* must be the integer
    /// index of the `inputs` entry it attaches to.
    files: Option<Vec<BinaryBlob>>,
}

/// JSON envelope of a predict response (used whenever the outputs are not
/// all binary).
#[derive(ToSchema)]
#[allow(dead_code)]
struct PredictJsonResponse {
    /// One output per input; binary outputs are wrapped as
    /// `{"__type__": "base64", "content": "<base64>"}`, and an input the
    /// model rejected on its own is
    /// `{"__error__": {"class": "input" | "transient", "message": "..."}}`.
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
    /// inference_id -> ISO-8601 expiration; never-expiring entries
    /// (ttl -1) render as `9999-12-31T23:59:59.999999`.
    expirations: std::collections::BTreeMap<String, String>,
}

/// Response of `GET /cache`.
#[derive(ToSchema)]
#[allow(dead_code)]
struct CacheListResponse {
    /// inference_id -> cache keys currently referencing it.
    cache: std::collections::BTreeMap<String, Vec<String>>,
}

/// `POST /predict/{group}/{inference_id}` — router.py `predict`.
/// Parses the multipart request, auto-loads the model (pinned for the
/// duration, TTL restored afterwards — the manager owns those semantics),
/// runs the batch, and encodes the response exactly like
/// `utils.encode_output_response`.
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
        (status = 400, description = "Malformed multipart body or inputs", body = crate::api_error::ErrorBody),
        (status = 422, description = "Missing required `data` form field", body = crate::api_error::ErrorBody),
        (status = 500, description = "Model load or prediction failure. `detail` is a \
            plain string for an ordinary failure and an object carrying a machine-readable \
            `kind` for the ones a caller must act on differently — `worker_died` says the \
            inference worker process died with the request in flight, so the request's items \
            were never attempted and re-submitting them is correct.", body = InferenceErrorBody)
    )
)]
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
        Err(err) => {
            // R9's cooldown first: it is a *refusal to try*, so it is
            // neither of the two failures below and carries its own status.
            if let Some(response) = load_cooldown_response(&err) {
                return Ok(response);
            }
            let chain = format!("{err:#}");
            tracing::error!(model = %full_id, error = %chain, "prediction failed");
            // router.py detail strings: load failures vs. predict failures.
            match classify_predict_failure(&chain, &full_id) {
                PredictFailure::LoadFailed => {
                    return Err(ApiError::internal("Failed to load model"));
                }
                PredictFailure::Unattempted => {
                    return Ok(structured_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        InferenceErrorFields {
                            kind: WORKER_DIED_KIND.to_owned(),
                            // Same string the plain form carries, so a client
                            // that only renders prose is unaffected by the
                            // shape change.
                            message: Some("Prediction failed".to_owned()),
                            model: Some(full_id.clone()),
                            last_error: Some(clamp_detail(&chain)),
                            ..Default::default()
                        },
                    ));
                }
                PredictFailure::Other => {
                    return Err(ApiError::internal("Prediction failed"));
                }
            }
        }
    };
    // Read straight after the predict. Deliberately not this request's own
    // window: it is whatever window this model formed most recently, which
    // under concurrent predicts may be a later one. That is the point — the
    // figure is a running opinion about the model, not a receipt for one
    // request, so stale by a window is exactly as useful. A model unloaded in
    // the gap answers `None`, which omits the header for that one response.
    let desired = state.manager.desired_in_flight_items(&full_id);
    Ok(with_desired_in_flight(
        encode_output_response(outputs),
        desired,
    ))
}

/// Bound on the error text a structured detail carries. A fatal worker error
/// renders its stderr tail, which is a ring buffer of whatever the worker
/// logged over its recent life and can be tens of kilobytes; the full text is
/// already in the log line above, and the caller persists this one per failed
/// item. 2000 bytes matches the extraction ledger's own audit clamp.
const MAX_DETAIL_BYTES: usize = 2000;

/// Clamp an error chain to [`MAX_DETAIL_BYTES`], on a char boundary.
fn clamp_detail(text: &str) -> String {
    if text.len() <= MAX_DETAIL_BYTES {
        return text.to_owned();
    }
    let mut end = MAX_DETAIL_BYTES;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

/// The pinned 503 of the per-model load-failure cooldown (R9), when this
/// error is one: `Retry-After: <seconds>` plus
/// `{"detail": {"kind": "load_cooldown", "model", "last_error", "retry_at",
/// "failures"}}`. The whole chain is searched rather than the outermost
/// error, so a caller that adds context to it still gets the right answer.
///
/// 503 rather than 500 because it is exactly what 503 means — the model is
/// temporarily unavailable and the response says for how long — and because
/// a job's client must be able to tell "do not bother retrying this now" from
/// "this attempt failed".
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
            last_error: Some(clamp_detail(&cooldown.last_error)),
            retry_at: Some(cooldown.retry_at.to_rfc3339()),
            failures: Some(cooldown.failures),
        },
    );
    if let Ok(value) = header::HeaderValue::from_str(&cooldown.retry_after_secs.to_string()) {
        response.headers_mut().insert(header::RETRY_AFTER, value);
    }
    Some(response)
}

/// Attach [`DESIRED_IN_FLIGHT_HEADER`] to an already-encoded predict
/// response, leaving the body and every other header byte-identical. `None`
/// (or a value that will not fit a header) omits it.
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
/// `{"status": "loaded"}` on success, 500 `"Failed to load model"` on any
/// error (details go to the log, like Python's `logger.error`).
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
        // The same cooldown answer the predict path gives (R9): an explicit
        // load request is the one path that would otherwise keep asking.
        if let Some(response) = load_cooldown_response(&err) {
            return Ok(response);
        }
        tracing::error!(model = %full_id, error = %format!("{err:#}"), "failed to load model");
        return Err(ApiError::internal("Failed to load model"));
    }
    Ok(Json(json!({"status": "loaded"})).into_response())
}

/// `DELETE /cache/{cache_key}/{group}/{inference_id}` — router.py
/// `unload_model`: always `{"status": "unloaded"}` (Python doesn't report
/// whether the entry existed).
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

/// `DELETE /cache/{cache_key}` — router.py `clear_cache`:
/// `{"status": "cleared"}`.
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
/// `{"expirations": {id: isoformat}}`, with `datetime.max` for ttl -1.
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

/// `GET /cache` — router.py `get_cached_models`:
/// `{"cache": {inference_id: [cache_keys]}}`.
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

/// `GET /metadata` — router.py `get_metadata`: mtime-gated registry reload
/// (RegistryCache mirrors `load_config(config, mtime)`), then the
/// `list_inference_ids` shape.
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
            // Additive and read-only, exactly like the availability overlay
            // above: what the calibration store knows about each priced model
            // on the board it would load on.
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

/// `GET /health` (additive, design §7; no Python counterpart): orchestrator
/// + per-model liveness, loaded models, queue depths, and batch caps — the
/// serde shape is [`HealthReport`], assembled by [`ModelManager::health`].
/// Supersedes the earlier standalone-only `{"status": "ok", "loaded": ...}`
/// body: the loaded-model map is now the richer `models` array.
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
///
/// Additive: a batch containing a typed per-item error slot always takes the
/// JSON envelope (the binary encodings have nowhere to put a typed failure),
/// with the erroring slots rendered as `{"__error__": {class, message}}` and
/// the surviving binary payloads keeping the existing base64 wrapper. Absent
/// error slots the encoding is bit-for-bit what it always was, so existing
/// consumers see no change.
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
            WorkerOutput::Error(error) => error.to_json(),
        })
        .collect();
    Json(json!({"outputs": encoded})).into_response()
}

/// OpenAPI subdocument for the inference surface, nested under
/// `/api/inference` by the gateway's main doc (`openapi.rs`). Paths here
/// are router-relative, matching how [`router`] is mounted. Documented
/// regardless of `inference_local`: when disabled, the same paths proxy to
/// an upstream serving the same contract (minus `/health` on Python).
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
    use crate::inferio_client::{
        InferenceApiClient, InferenceFile, InferenceInput, PredictOutput, parse_predict_response,
    };
    use axum::body::to_bytes;
    use serde_json::json;
    use std::fs;
    use std::path::{Path, PathBuf};

    /// The board the test fixture's calibration overlay answers for. Tests
    /// must never depend on the host's GPUs, so the fixture names one.
    const TEST_GPU: &str = "TEST 9000";

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

        match parse_predict_response(&content_type, &body)
            .unwrap()
            .outputs
        {
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

        match parse_predict_response(&content_type, &body)
            .unwrap()
            .outputs
        {
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

        match parse_predict_response(&content_type, &body)
            .unwrap()
            .outputs
        {
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

    /// A typed per-item error slot forces the JSON envelope even for an
    /// otherwise all-binary batch (the raw and multipart encodings have
    /// nowhere to put a typed failure), and the client parses it back into
    /// the surviving *binary* payloads plus the slot error at its input's
    /// index — an embedding model must not suddenly hand the extraction job
    /// base64 JSON just because one frame of the item was undecodable.
    #[tokio::test]
    async fn an_error_slot_forces_the_json_envelope_and_round_trips() {
        let response = encode_output_response(vec![
            WorkerOutput::Bytes(b"AAA".to_vec()),
            WorkerOutput::Error(SlotError {
                class: SlotErrorClass::Input,
                message: "Unreadable image: truncated".to_owned(),
            }),
            WorkerOutput::Bytes(b"BB".to_vec()),
        ]);
        let (content_type, body) = split_response(response).await;
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
        match parsed.outputs {
            PredictOutput::Binary(outputs) => {
                assert_eq!(outputs, vec![b"AAA".to_vec(), b"BB".to_vec()]);
            }
            other => panic!("client parsed {other:?}"),
        }
    }

    /// The same for a JSON-output model (tags/text): survivors stay JSON
    /// values and the failed slot is reported separately, never as an output.
    /// A batch where every slot errored yields no outputs at all.
    #[tokio::test]
    async fn error_slots_are_separated_from_json_survivors() {
        let response = encode_output_response(vec![
            WorkerOutput::Error(SlotError {
                class: SlotErrorClass::Transient,
                message: "try again".to_owned(),
            }),
            WorkerOutput::Json(json!({"tags": ["a"]})),
        ]);
        let (content_type, body) = split_response(response).await;
        let parsed = parse_predict_response(&content_type, &body).unwrap();
        assert_eq!(parsed.errors.len(), 1);
        assert_eq!(parsed.errors[0].class, SlotErrorClass::Transient);
        assert_eq!(parsed.errors[0].index, 0);
        match parsed.outputs {
            PredictOutput::Json(values) => assert_eq!(values, vec![json!({"tags": ["a"]})]),
            other => panic!("client parsed {other:?}"),
        }

        let response = encode_output_response(vec![WorkerOutput::Error(SlotError {
            class: SlotErrorClass::Input,
            message: "Unreadable image".to_owned(),
        })]);
        let (content_type, body) = split_response(response).await;
        let parsed = parse_predict_response(&content_type, &body).unwrap();
        assert_eq!(parsed.errors.len(), 1);
        assert!(parsed.outputs.is_empty(), "nothing succeeded");
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

    /// Test interpreter default: the managed venv (`python/.venv`) if
    /// present, else the legacy root `.venv` (pre-restructure installs).
    fn test_venv_python(root: &Path, rel: &str) -> PathBuf {
        let managed = root.join("python/.venv").join(rel);
        if managed.is_file() {
            managed
        } else {
            root.join(".venv").join(rel)
        }
    }

    /// Same spawn setup as the manager.rs tests: repo venv python, cwd =
    /// repo root, PYTHONPATH=python, NO_CUDNN, fixture impl dir.
    fn test_spawn_config() -> WorkerSpawnConfig {
        let root = workspace_root();
        // PANOPTIKON_TEST_PYTHON overrides the repo-venv interpreter (any
        // python with msgpack works), e.g. running the suite under WSL
        // against a Windows checkout, whose .venv is a Windows venv.
        let python = match std::env::var_os("PANOPTIKON_TEST_PYTHON") {
            Some(explicit) => PathBuf::from(explicit),
            None if cfg!(windows) => test_venv_python(&root, "Scripts/python.exe"),
            None => test_venv_python(&root, "bin/python"),
        };
        if !python.is_file() {
            panic!(
                "inferio http tests need the repo venv interpreter at {} — create the dev venv first",
                python.display()
            );
        }
        WorkerSpawnConfig {
            python,
            impl_dirs: vec![root.join("python/tests/inferio_worker/fixture_impls")],
            pythonpath: vec![root.join("python")],
            env: vec![("NO_CUDNN".to_owned(), "true".to_owned())],
            env_remove: Vec::new(),
            cwd: Some(root),
            deadlines: WorkerDeadlines::default(),
            // The fixture impls echo `CUDA_VISIBLE_DEVICES`, which is also
            // what every non-ROCm host writes.
            pin_env_var: crate::inferio::gpu::CUDA_PIN_ENV_VAR,
        }
    }

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
        // A calibration store rooted in the test's own temp dir: no shipped
        // baselines, an empty local file, and no debounce so a recorded
        // profile is visible to `/metadata` immediately.
        let calibration = super::super::calibration::CalibrationStore::with_debounce(
            super::super::calibration::StorePaths::beside_registry(
                &[dir.path().to_path_buf()],
                &dir.path().join("data"),
            ),
            super::super::calibration::StoreEnv {
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
            // Tests must not depend on the host's GPUs.
            compute_caps: super::super::capability::HostComputeCaps::unknown(),
            calibration: Some(calibration),
            // ...but the calibration overlay needs *a* board to answer for,
            // so the fixture names one.
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
        // R10': the whole exchange below runs over HTTP/2 cleartext with
        // prior knowledge — one multiplexed connection to the local service,
        // not one socket pair per in-flight predict (run1 blocker F6). The
        // service is `axum::serve`, exactly as the gateway serves it, so this
        // also pins that hyper-util's auto builder accepts the h2 preface.
        assert_eq!(
            client.known_transport(),
            Some(crate::inferio_client::Transport::H2c),
            "the real client and the real service must agree on h2c"
        );

        let external_inputs = client.get_external_inputs().await.expect("external inputs");
        assert_eq!(
            external_inputs["definitions"]["test_token"]["configured"],
            json!(false)
        );
        assert_eq!(
            external_inputs["models"]["echo/test"][0]["required"],
            json!(false)
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
        // The desired in-flight figure rides on the response header, in
        // every one of the three encodings — which is why it is a header and
        // not a body field: only the JSON envelope could have carried it.
        let desired = output
            .desired_in_flight_items
            .expect("the orchestrator published a figure");
        assert!(desired > 0);
        match output.outputs {
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
        assert_eq!(
            output.desired_in_flight_items,
            Some(desired),
            "octet-stream responses carry it too"
        );
        match output.outputs {
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
        assert_eq!(
            output.desired_in_flight_items,
            Some(desired),
            "and so do multipart/mixed responses"
        );
        match output.outputs {
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

    /// Every rendering that means the request never reached a model, quoted
    /// from the five places that format them, classifies as "never attempted"
    /// — so all of a death's blast radius is re-queued, not just the fraction
    /// of it standing on the replica that died.
    ///
    /// These literals are the coupling: if one of the cited `format!`s
    /// changes, this test is what notices, and the fix is to update
    /// [`UNATTEMPTED_REQUEST_MARKERS`] with it.
    #[test]
    fn every_shape_of_a_worker_death_classifies_as_unattempted() {
        let model = "clip/model-a";
        // `Worker::fatal` (worker.rs): the window executing on the dead
        // replica, and every request `dispatch::fail_requests` re-raises it to.
        let fatal = format!(
            "inferio worker {model}#0 failed fatally: early eof; process status: \
             signal 9; stderr tail:\nTraceback…"
        );
        // `dispatch::reap_idle_replicas`: an idle replica found dead by the
        // liveness sweep. Note it does *not* say "failed fatally".
        let idle = format!("inferio worker for model {model} exited while idle: pid 41 signal 9");
        // `Worker::roundtrip`: a request written to an already-poisoned worker.
        let poisoned = format!("inferio worker {model}#1 is dead after a previous fatal error");
        // `ModelManager::predict`: the reply oneshot was dropped. This is what
        // a window on a *surviving* replica sees when a sibling dies and
        // `in_flight.shutdown()` aborts it.
        let dropped = format!("the dispatcher for model {model} dropped the request");
        // `ModelManager::predict` when the send fails: the dispatch task has
        // already closed its receiver on the way out of the fatal arm, so the
        // tail of the same window lands here instead of on the fatal message.
        let too_late = format!("model {model} was unloaded before the request could be queued");
        // `dispatch`'s `End::Graceful` arm. Not a death, but the same fact
        // about the request: it never reached a model.
        let unloaded = format!("model {model} was unloaded");

        for chain in [&fatal, &idle, &poisoned, &dropped, &too_late, &unloaded] {
            assert_eq!(
                classify_predict_failure(chain, model),
                PredictFailure::Unattempted,
                "{chain}"
            );
        }
    }

    /// A death whose stderr tail happens to carry the words a *load* failure
    /// uses must still be a death.
    ///
    /// The chain a fatal error renders includes the worker's stderr tail — a
    /// ring of whatever it logged over its recent life, including a previous
    /// respawn's own complaints. An unanchored `contains("failed to load
    /// model")` would let that stale line reclassify a real mid-window death
    /// as a load failure and cost the whole window its re-queue.
    #[test]
    fn a_stale_load_line_in_the_stderr_tail_cannot_forge_a_load_failure() {
        let model = "clip/model-a";
        let death_with_a_stale_tail = format!(
            "inferio worker {model}#0 failed fatally: early eof; stderr tail:\n\
             [worker] failed to load model weights from cache, retrying\n\
             [worker] ok"
        );
        assert_eq!(
            classify_predict_failure(&death_with_a_stale_tail, model),
            PredictFailure::Unattempted,
            "a stale tail line must not outrank a death marker"
        );

        // The real thing — `ensure_loaded`'s context, which always names the
        // model — still wins, including when the worker died *while* loading.
        let real = format!(
            "failed to load model {model}: inferio worker {model}#0 failed fatally: \
             early eof"
        );
        assert_eq!(
            classify_predict_failure(&real, model),
            PredictFailure::LoadFailed,
            "a load failure of this model keeps its precedence over the death"
        );

        // Parity: a chain that said `Failed to load model` before, with no
        // death marker on it, still does.
        assert_eq!(
            classify_predict_failure("failed to load model something-else: nope", model),
            PredictFailure::LoadFailed
        );
        assert_eq!(
            classify_predict_failure("the worker returned an error", model),
            PredictFailure::Other
        );
    }

    /// A predict whose worker dies mid-request answers with the
    /// machine-readable `worker_died` kind, and the gateway's own client
    /// parses it back into the typed failure the extraction job keys its
    /// re-queue on. Both halves of run1 finding F7's fix in one round trip:
    /// without the kind on the wire, or without the client typing it, the job
    /// is back to recording a whole in-flight window as item errors.
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
        assert!(failure.is_worker_death(), "{failure}");
        assert!(!failure.is_load_cooldown(), "{failure}");
        assert_eq!(failure.model.as_deref(), Some("dying/test"), "{failure}");
        assert_eq!(failure.message, "Prediction failed", "{failure}");
        // The chain the operator greps for is carried through, clamped, so a
        // job can record *why* an item was lost without re-reading the log.
        let last_error = failure.last_error.as_deref().unwrap_or_default();
        assert!(last_error.contains("failed fatally"), "{failure}");
        assert!(
            last_error.len() <= MAX_DETAIL_BYTES + 4,
            "{}",
            last_error.len()
        );
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
            model.last_grant_units, None,
            "no window dispatched yet -> null on the wire"
        );
        assert_eq!(model.last_window_items, None);
        assert!(
            health.vram.is_empty(),
            "an unknown GPU inventory means an empty ledger and no admission"
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
        match output.outputs {
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
    /// broken registry TOML. Cargo runs this with CWD = the panoptikon crate,
    /// where `python/inferio/config` does not exist.
    #[tokio::test]
    async fn from_settings_degrades_when_builtin_config_dir_is_missing() {
        use crate::config::{InferenceLocalConfig, Settings, UpstreamConfig, UpstreamsConfig};

        // Force the default-dirs error path deterministically: no
        // python/inferio/config relative to the test CWD.
        assert!(
            !std::path::Path::new("python/inferio/config").is_dir(),
            "test premise: the built-in config dir is absent from the crate CWD"
        );

        let settings = Settings {
            server: crate::config::ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 0,
                trust_forwarded_headers: false,
                policy_token_key: None,
                endpoints: Vec::new(),
                check_for_updates: false,
            },
            upstreams: UpstreamsConfig {
                ui: crate::config::UiUpstreamConfig {
                    base_url: "http://127.0.0.1:6339".to_string(),
                    local: false,
                    dir: None,
                    node: None,
                    build: Default::default(),
                    api_endpoint: None,
                },
                api: UpstreamConfig {
                    base_url: "http://127.0.0.1:6342".to_string(),
                    local: false,
                },
                inference: Vec::new(),
            },
            data_folder: std::path::PathBuf::from("data"),
            index_db: "default".to_string(),
            user_data_db: "default".to_string(),
            readonly: false,
            temp_dir: std::path::PathBuf::from("data/tmp"),
            logging: Default::default(),
            open: Default::default(),
            search: Default::default(),
            jobs: Default::default(),
            transcode: Default::default(),
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
        let registry = state
            .registry
            .lock()
            .unwrap()
            .get()
            .expect("empty registry loads");
        assert!(registry.groups.is_empty());
        state.manager.shutdown().await;
    }

    /// `/metadata` carries the calibration overlay: what the store knows
    /// about each priced model on the board it would load on. Additive and
    /// read-only, exactly like the Package-1 availability overlay — and
    /// absent for a `none`-class model, which is never priced at all.
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
        let calibrated = &metadata["echo"]["inference_ids"]["test"]["calibration"];
        assert_eq!(calibrated["status"], json!("local"));
        assert_eq!(calibrated["gpu"], json!(TEST_GPU));
        assert_eq!(calibrated["dtype"], json!("fp16"));
        assert_eq!(calibrated["base_mb"], json!(4321));
        assert_eq!(calibrated["slope_mb_per_unit"], json!(0.79));
        assert_eq!(calibrated["samples"], json!(38));
        assert_eq!(calibrated["local_samples"], json!(12));
        assert_eq!(calibrated["max_units_measured"], json!(1024));
        assert_eq!(calibrated["knee_units"], json!(512));
        // The registry metadata itself is untouched.
        assert_eq!(
            metadata["echo"]["inference_ids"]["test"]["description"],
            json!("echo fixture")
        );
        assert!(
            metadata["echo"]["inference_ids"]["remote"]
                .get("calibration")
                .is_none(),
            "a none-class model is never priced, so it is never calibrated"
        );
        state.manager.shutdown().await;
    }
}
