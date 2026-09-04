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

/// `detail.kind` of a predict whose **request body never arrived in full**,
/// so the batch was never parsed and never reached a model.
///
/// It is the same assertion [`WORKER_DIED_KIND`] makes — the request's items
/// are untouched, so re-submitting them is correct — for the one other way a
/// predict can end without being attempted: the body stream failing under
/// the handler. It is a *separate* token because the two are separate facts,
/// and a log line that blames a worker for a broken request body sends the
/// next reader to the wrong place.
///
/// The status stays 400: a request body that stops early is a malformed
/// request, and that is what every intermediary in the path will call it.
/// The kind is what lets the *caller* tell "your bytes were wrong" (retrying
/// changes nothing) from "your bytes did not all get here" (retrying is the
/// whole answer) — a distinction the status alone cannot carry.
pub(crate) const REQUEST_INCOMPLETE_KIND: &str = "request_incomplete";

/// `detail.kind` of a predict this server **declined to read**, because it is
/// already holding [`PREDICT_INFLIGHT_BODY_BYTES`] of other predict bodies in
/// memory.
///
/// The third way a predict can end without being attempted, and the only one
/// of the three that is a statement about *this server* rather than about a
/// worker or a connection: nothing is wrong with the request, there was
/// simply no room to buffer it. It rides on a `503` with a `Retry-After`,
/// which is what a temporarily-full server owes a caller, and it is a
/// separate token from the other two for the same reason they are separate
/// from each other — a log line that blames a broken body for an overloaded
/// server sends the next reader to the wrong place.
pub(crate) const BODY_BUDGET_KIND: &str = "body_budget_exhausted";

/// Every rendering that means **this predict never reached a model**, so the
/// request's items are untouched and re-submitting them is the correct answer.
///
/// **The documented fallback, not the primary signal.** Since run2 the six
/// sites that produce these strings attach a typed
/// [`Unattempted`](crate::inferio::slot_error::Unattempted) marker, which
/// [`classify_predict_failure`] downcasts *first*; this list still runs after
/// that downcast so an error raised by code that predates the marker — or by
/// a path nobody has typed yet — classifies exactly as it did before. It is
/// kept, and kept tested, for that reason alone: deleting it would make the
/// classification silently narrower for any such path.
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
/// The typed marker each of these renderings now carries lives at three
/// places, covering all six shapes: `Worker::fatal` and the poisoned
/// `Worker::roundtrip` (`worker.rs`), `dispatch::fail_requests` — every
/// queue-failing path funnels through it, so the fatal arm, the graceful
/// unload, the tail of a died-on window and an isolation pass's remainder are
/// one change — and `ModelManager::predict`'s two arms. The marker carries
/// the message rather than wrapping it, so each of the literals below is
/// still produced byte for byte and these tests still mean something.
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

/// Classify a failed predict: the typed marker first, then its rendered
/// `anyhow` chain.
///
/// Pure, so the coupling to four other modules' message formats is pinned by
/// unit tests rather than by a live worker death per case.
///
/// **The downcast is the primary signal** and runs before the substring list
/// (`UNATTEMPTED_REQUEST_MARKERS`, whose docs say why the list survives it).
/// It walks the whole context chain, so the `.context` the inference pool and
/// the job runner add on the way out cannot hide it.
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
fn classify_predict_failure(err: &anyhow::Error, chain: &str, full_id: &str) -> PredictFailure {
    // The load check keeps its precedence over *both* unattempted signals,
    // for the reason above: a model that will not come up must not cost each
    // item a second full attempt. A worker that dies while loading renders
    // the anchored load context, and `ensure_loaded` wraps whatever the
    // spawn produced — typed marker included.
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

/// Bytes one predict request body may carry.
///
/// The route used to run under `DefaultBodyLimit::disable()` — no limit at
/// all — inherited from the proxy path it replaced. Since `7e96de62` the
/// handler buffers the whole body before parsing it (a streamed parse
/// answered with the request stream still open, which cost the connection an
/// `ENHANCE_YOUR_CALM` GOAWAY every ~65 000 streams), so "no limit" now means
/// "a peer decides how much of this server's memory one stream holds". That
/// has to be a number, and the number has to be one nothing legitimate can
/// hit.
///
/// [`MAX_FRAME_BYTES`] is that number: it is the orchestrator's own wall on
/// one worker-protocol frame, and it already bounds the *inputs* on the way
/// in — `jobs::extraction`'s `check_frame_budget` refuses a single input
/// above `FRAME_INPUT_BYTES_BUDGET` (this figure minus the envelope) before
/// any predict is attempted, as a persisted `resource` verdict. So a body
/// above it carries either one input this machine has already decided it
/// cannot infer, or a batch larger than the largest object either side of
/// the worker protocol ever holds. Refusing it at the door with a `413` is
/// strictly better than buffering it and failing later.
///
/// **It bounds one request, and it is sized for the largest legitimate one:
/// a request carrying a single input.** `check_frame_budget` admits an input
/// up to `FRAME_INPUT_BYTES_BUDGET` (this figure minus 8 MiB), and multipart
/// adds only a couple of hundred bytes of envelope per part, so a one-input
/// predict can legitimately come within that margin of this limit. Anything
/// lower would refuse a request the job builds and has already committed to;
/// anything higher would admit a body the worker frame could not carry. It is
/// deliberately **not** derived from "64 inputs per request", which is the
/// other legitimate shape: 64 x the largest input is 128 GiB, a number that
/// bounds nothing.
///
/// **It is not, on its own, a memory bound**, and it never was. There is no
/// limit on how many connections a peer opens, so
/// `MAX_CONCURRENT_STREAMS x PREDICT_BODY_LIMIT` is not a ceiling either —
/// it is one connection's worth of an unbounded number. The ceiling is
/// [`PREDICT_INFLIGHT_BODY_BYTES`], which bounds the sum across every stream
/// and every peer; this limit's job is to decide *per request* what is
/// plausible, and to answer `413` rather than `503` when it is not.
pub(crate) const PREDICT_BODY_LIMIT: usize = MAX_FRAME_BYTES;

/// **Predict request bytes this process holds in memory at once**, summed
/// across every connection, every stream and every peer.
///
/// This is the bound [`PREDICT_BODY_LIMIT`] cannot be. A per-request limit
/// multiplied by a stream limit is not a memory budget when nothing bounds
/// the number of connections — and even against one connection,
/// `512 x 2 GiB` is a terabyte, which is a statement about arithmetic rather
/// than about this machine. Since `7e96de62` the handler buffers each body
/// before parsing it, so "how much can arrive at once" is a question with a
/// real answer, and it has one.
///
/// **The number, derived from what the shipped client can legitimately
/// offer.** A gateway job holds at most `[jobs] intermediate_data_budget_mb`
/// (1 GiB by default) of loaded item data at a time — `jobs::extraction`
/// takes that byte budget *before* a unit permit, so it bounds the item bytes
/// in flight, which are exactly the bytes its predict bodies carry. Four
/// times it covers four gateways at the shipped default running against one
/// inference server, which is more than the deployment this exists for (a NAS
/// and a GPU box) ever has. It is also `2 x PREDICT_BODY_LIMIT`, which is the
/// property that keeps the budget from being a trap: the largest request this
/// server will accept can always be admitted beside another one of the same
/// size, so no legitimate request is ever permanently unadmittable.
///
/// **The worst case, honestly.** The budget counts bytes as they arrive. A
/// body being *parsed* is briefly resident twice — the collected buffer, plus
/// the per-field copies `parse_input_request` takes out of it — so the
/// resident peak this admits is up to twice the budget, ~8 GiB, and only if
/// every admitted byte is mid-parse at the same instant. Steady state for the
/// job this serves is a few hundred KiB per request over a few hundred
/// concurrent requests: two orders of magnitude below it.
///
/// **What happens at the wall.** The request is refused with `503` and a
/// `Retry-After`, typed so the caller knows the batch was never parsed
/// ([`crate::inferio_client::BODY_BUDGET_KIND`]) — the same assertion
/// `worker_died` and `request_incomplete` make, and it earns the same
/// recovery. It is never a wait: waiting would hold the stream open, which is
/// the very thing `7e96de62` is about, and it would convert an overload into
/// an unbounded latency instead of an answer.
pub(crate) const PREDICT_INFLIGHT_BODY_BYTES: usize = 4 * 1024 * 1024 * 1024;

const _: () = assert!(
    PREDICT_INFLIGHT_BODY_BYTES >= 2 * PREDICT_BODY_LIMIT,
    "the in-flight budget must admit two maximal bodies, or a maximal request \
     can be refused for as long as another one is in flight"
);

/// The budget itself. Process-wide because the resource is: both listener
/// modes (gateway and `panoptikon inferio`) mount this router, and a bound
/// that is per-router is not a bound on the machine's memory.
static PREDICT_BODY_BYTES: tokio::sync::Semaphore =
    tokio::sync::Semaphore::const_new(PREDICT_INFLIGHT_BODY_BYTES);

/// Predict bodies refused for want of budget, ever. Reported on `/health`,
/// because a bound nobody can see is indistinguishable from a bug — which is
/// the whole lesson of run2 S1.
static PREDICT_BODY_REFUSALS: AtomicU64 = AtomicU64::new(0);

/// What this process's predict-body budget is doing right now, for
/// `/health`.
///
/// A refusal is a `503` an operator will see in a job's logs, so the state
/// that produced it has to be readable somewhere that is not a guess. The
/// pair to watch is `in_flight_bytes` against `budget_bytes`: a job that is
/// being refused while the first is far below the second is being refused by
/// a *burst* rather than by a level, and the answer is the caller's request
/// sizing, not this number.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct PredictBodyBudgetHealth {
    /// [`PREDICT_BODY_LIMIT`]: the largest single predict body this server
    /// will read, past which it answers `413`.
    pub request_limit_bytes: u64,
    /// [`PREDICT_INFLIGHT_BODY_BYTES`]: predict body bytes this process will
    /// hold at once, across every connection and peer.
    pub budget_bytes: u64,
    /// Of those, how many are reserved right now — bodies arriving, plus
    /// bodies being parsed.
    pub in_flight_bytes: u64,
    /// Predict requests refused for want of budget since this process
    /// started. `0` is the number an operator should expect to see.
    pub refused_requests: u64,
}

/// The budget's current state, read off the semaphore itself rather than off
/// a counter kept beside it — there is only ever one truth about how much is
/// reserved.
pub(crate) fn predict_body_budget_health() -> PredictBodyBudgetHealth {
    budget_health(
        PREDICT_BODY_BYTES.available_permits(),
        PREDICT_BODY_REFUSALS.load(Relaxed),
    )
}

/// The mapping from "what the semaphore says" to what `/health` reports, as
/// a pure function so it can be asserted without racing the process-wide
/// budget every other test in this binary is also using.
fn budget_health(available: usize, refusals: u64) -> PredictBodyBudgetHealth {
    PredictBodyBudgetHealth {
        request_limit_bytes: PREDICT_BODY_LIMIT as u64,
        budget_bytes: PREDICT_INFLIGHT_BODY_BYTES as u64,
        in_flight_bytes: PREDICT_INFLIGHT_BODY_BYTES.saturating_sub(available) as u64,
        refused_requests: refusals,
    }
}

/// Bytes the budget hands out at a time when the body declares no length.
///
/// A body with a `Content-Length` reserves once, exactly. A chunked one has
/// to be charged as it arrives, and charging per frame would take the
/// semaphore thousands of times for one body; a 1 MiB granularity makes it a
/// handful, and over-reserves by less than one granule.
const PREDICT_BODY_RESERVE_GRANULE: usize = 1024 * 1024;

/// The inference routes, path-relative so they can be nested under
/// `/api/inference` (gateway and standalone mode mount the same router).
///
/// axum's own body limit stays disabled, as it has been since this replaced
/// the streaming proxy path: it is enforced by `Bytes::from_request`, and the
/// one route with a large body ([`predict`]) collects its body itself so that
/// a truncated one can be told apart from a malformed one. That route applies
/// [`PREDICT_BODY_LIMIT`] in its own extractor instead, which is where the
/// limit can also produce the right status.
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

/// The predict body, read to its end *before* it is parsed.
///
/// Two separate things depend on the whole body being collected first, and
/// both of them are run2 defect P2:
///
/// * **The request stream has to reach its end.** A server that answers
///   while the request body is still open must reset the stream
///   (RFC 9113 §8.1), and hyper does. The client's terminal DATA frame then
///   lands on a stream this end has already closed, which h2 reports as a
///   STREAM_CLOSED *stream error* and counts against
///   `max_local_error_resets` — a counter that only ever rises, for the
///   whole life of the connection. At 1 024 of them h2 stops the connection
///   with `GOAWAY(ENHANCE_YOUR_CALM, "too_many_internal_resets")`, and
///   **every** request body still being read on it fails at once. multer
///   stops at the closing boundary and never polls the frame after it, so
///   the streamed parse this handler used left that reset behind on every
///   predict, on the one connection the gateway's h2c self-call keeps for a
///   whole job. Measured against this router over h2c with the real client:
///   381 of 300 032 predicts failed their parse, every one of them with
///   `hyper::Error(Body, GoAway(b"too_many_internal_resets",
///   ENHANCE_YOUR_CALM, Library))` under axum's fixed sentence — the exact
///   `400 invalid multipart body` run2 P2 reported. Collecting the body
///   first is what makes the stream end normally; the same 300 032 then
///   fail none.
///
/// * **A transport failure stops looking like a malformed body.** Streamed,
///   "the connection broke under me" and "these bytes are not multipart"
///   both arrive as `axum::extract::multipart::MultipartError`, whose
///   `Display` is one fixed sentence with no cause attached — which is why
///   P2 reached the operator as `400 invalid multipart body` and nothing
///   else. Collected, the two are different code paths: a failed `collect`
///   is the body not arriving ([`REQUEST_INCOMPLETE_KIND`], the caller
///   re-submits), and anything multer says afterwards is genuinely about the
///   bytes.
///
/// What it costs is one extra resident copy of the body: `predict` already
/// copies every field into a `Vec` before the batch runs, and the collected
/// bytes now stay alive beside those copies until the parse is done. That
/// cost is what [`PREDICT_INFLIGHT_BODY_BYTES`] bounds, per request through
/// [`PREDICT_BODY_LIMIT`] and in aggregate through the reservation this
/// holds.
struct BufferedMultipart {
    multipart: Multipart,
    /// The collected body, kept so a failed parse can say *why* it failed.
    /// Free to keep: multer holds slices of this same buffer.
    body: axum::body::Bytes,
    /// The boundary the request's `Content-Type` declared, when it declared
    /// a usable one.
    boundary: Option<String>,
    /// The bytes this body holds out of the process-wide budget, returned
    /// when the parse is done and this is dropped. Not read; owned.
    _reservation: BodyReservation,
}

/// One request's claim on [`PREDICT_INFLIGHT_BODY_BYTES`], returned by
/// `Drop` so every exit path — a refusal, a stream failure, a parse failure,
/// a cancelled request — accounts for itself without a single explicit
/// release.
///
/// It grows: a body that declares its length reserves once, and one that
/// does not is charged in [`PREDICT_BODY_RESERVE_GRANULE`] steps as it
/// arrives. Growth is always **try**, never a wait, so two half-reserved
/// bodies can never wait on each other — an exhausted budget is answered,
/// not queued.
struct BodyReservation {
    /// The budget this draws on. A parameter rather than a reference to the
    /// static, so a test can exercise exhaustion without starving every other
    /// request in the process (which is the same class of cross-test
    /// interference the `/health` client section's `try_lock` caused).
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

    /// Reserve up to `wanted` bytes in total for this body, or say the budget
    /// is out. Idempotent below what is already held.
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

/// Why a predict body could not be read as a batch. Typed and small so the
/// *decision* (below) and the *rendering* (in `IntoResponse`) stay separate
/// — and so the one distinction a caller acts on is made in one place.
#[derive(Debug)]
enum PredictBodyError {
    /// Every byte arrived and they are not a valid batch. An ordinary 400:
    /// asking again produces the same answer.
    Malformed(String),
    /// The bytes did not all arrive, so nothing was parsed and nothing was
    /// attempted. See [`REQUEST_INCOMPLETE_KIND`].
    Incomplete(String),
    /// No `data` field. FastAPI answers a missing required Form field 422,
    /// and so does this.
    MissingData,
    /// The body is larger than [`PREDICT_BODY_LIMIT`]. A `413`, and a verdict
    /// about the request rather than about the media: the caller has to send
    /// a smaller batch, and re-sending the same one will not help.
    TooLarge,
    /// This process is already holding [`PREDICT_INFLIGHT_BODY_BYTES`] of
    /// predict bodies. A `503` with a `Retry-After`: nothing about this
    /// request is wrong, there is simply no room to read it right now, and
    /// re-sending it *is* the answer.
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
                        last_error: Some(clamp_detail(&detail)),
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
                // A figure the caller can act on rather than a bare 503: the
                // budget is released by requests that are already being
                // parsed, so the wait is short and bounded by them.
                response
                    .headers_mut()
                    .insert(header::RETRY_AFTER, header::HeaderValue::from_static("1"));
                response
            }
        }
    }
}

/// Collect a request body under a per-request ceiling **and** the
/// process-wide byte budget, keeping the four outcomes distinct.
///
/// The `DefaultBodyLimit` layer cannot do this job for [`predict`]: it is
/// enforced by `Bytes::from_request`, and this route deliberately collects the
/// raw body itself so a *truncated* body can be told apart from a *malformed*
/// one (`7e96de62`). "Whole, and larger than we will hold" and "whole, and
/// there is no room to hold it right now" are two further things, and each
/// earns its own status rather than being reported as one of the others: the
/// first says shrink the batch, the second says send the same batch again in
/// a moment.
///
/// The budget is charged **before the bytes are read**, from
/// `Content-Length` where there is one and in
/// [`PREDICT_BODY_RESERVE_GRANULE`] steps where there is not, so a body is
/// never admitted into memory the process has not already accounted for. A
/// declared length over `limit` is refused without reading a byte of it.
async fn collect_within(
    body: Body,
    limit: usize,
    budget: &'static tokio::sync::Semaphore,
) -> Result<(axum::body::Bytes, BodyReservation), PredictBodyError> {
    use axum::body::HttpBody as _;

    let mut reservation = BodyReservation::new(budget);
    // The declared length, when the body declares one — every request the
    // shipped client builds does, because `reqwest`'s multipart form is
    // assembled from in-memory parts and sizes itself. A declared length is
    // charged once and exactly; only an undeclared body pays the granule.
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
            // The body stream itself failed: nothing was parsed, so nothing
            // was attempted.
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
        // A no-op when the declared length already covered this, and the
        // real charge when there was none — or when a peer sent more than it
        // said it would.
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
                // No usable boundary in the content type. Nothing about the
                // body can be judged without one, so it is the header that
                // is wrong, and that is an ordinary bad request.
                PredictBodyError::Malformed(format!(
                    "invalid multipart body: {}",
                    rejection.body_text()
                ))
            })
    }
}

impl BufferedMultipart {
    /// The `data` field and the file parts, by index — or why the body could
    /// not be read as a batch.
    ///
    /// All three places a multipart parse can fail are here, in one piece,
    /// because they all need the same two things: the cause underneath
    /// axum's fixed sentence, and [`Self::classify`]'s verdict on whether
    /// the body was wrong or merely incomplete.
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
                    // filename, which must be an integer index
                    // (utils.py:19-31).
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
    /// caller should do.
    ///
    /// The whole body is in hand, so the question is asked of the bytes
    /// rather than inferred from a parser's error variant: **does what
    /// arrived carry the closing delimiter of the boundary this request
    /// declared?**
    ///
    /// * It does not → what arrived is not a whole `multipart/form-data`
    ///   body for these headers. Either it stopped early or it is not the
    ///   body they describe; either way the batch was never parsed and never
    ///   reached a model, so its items are untouched and re-submitting them
    ///   is the right answer ([`REQUEST_INCOMPLETE_KIND`]).
    /// * It does → the delimiters are all here and something *inside* them
    ///   is wrong. That is a bad request in the ordinary sense.
    ///
    /// Asked only after the parse has already rejected the body, so a valid
    /// request is never held against it and the scan costs nothing in the
    /// case that matters.
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

/// The `boundary` parameter of the request's `Content-Type`.
///
/// Read through `mime`, because that is how the parser underneath reads it
/// (multer 3.1 `parse_boundary`: parse the header as a `Mime`, take
/// `get_param(BOUNDARY).as_str()`). The verdict below turns on whether the
/// body carries *this* boundary's closing delimiter, so it has to be the
/// same string multer parsed with; a second, hand-rolled reading of the
/// header would eventually disagree with it about some header neither of us
/// thought about, and disagreeing here means answering the wrong thing.
///
/// `None` when the header carries none — a different failure, and one
/// axum's own extractor rejects before this file sees it.
fn multipart_boundary(content_type: &str) -> Option<String> {
    let content_type = content_type.parse::<mime_guess::mime::Mime>().ok()?;
    let boundary = content_type
        .get_param(mime_guess::mime::BOUNDARY)?
        .as_str()
        .to_owned();
    (!boundary.is_empty()).then_some(boundary)
}

/// Whether `body` contains `--<boundary>--`, the delimiter that ends a
/// `multipart/form-data` body (RFC 2046 §5.1.1). Searched rather than
/// required at the end, because an epilogue after it is legal.
fn body_carries_closing_delimiter(body: &[u8], boundary: &str) -> bool {
    let needle = format!("--{boundary}--").into_bytes();
    if body.len() < needle.len() {
        return false;
    }
    body.windows(needle.len()).any(|window| window == needle)
}

/// An error rendered with everything under it. The one-line `Display` of
/// `MultipartError` (and of a body error) names the layer, never the cause,
/// so the cause has to be walked out of the source chain by hand — without
/// this, a parse failure is a sentence that fits every parse failure.
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

/// The answer to a failed `ModelManager::predict`, in one place so the shape
/// a caller sees is decided by [`classify_predict_failure`] and nothing else
/// — and so a test can drive each rendering of a worker death through the
/// exact code the handler runs, rather than through a copy of it.
fn predict_failure_response(err: anyhow::Error, full_id: &str) -> Result<Response, ApiError> {
    // R9's cooldown first: it is a *refusal to try*, so it is neither of the
    // two failures below and carries its own status.
    if let Some(response) = load_cooldown_response(&err) {
        return Ok(response);
    }
    let chain = format!("{err:#}");
    tracing::error!(model = %full_id, error = %chain, "prediction failed");
    // router.py detail strings: load failures vs. predict failures.
    match classify_predict_failure(&err, &chain, full_id) {
        PredictFailure::LoadFailed => Err(ApiError::internal("Failed to load model")),
        PredictFailure::Unattempted => Ok(structured_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            InferenceErrorFields {
                kind: WORKER_DIED_KIND.to_owned(),
                // Same string the plain form carries, so a client that only
                // renders prose is unaffected by the shape change.
                message: Some("Prediction failed".to_owned()),
                model: Some(full_id.to_owned()),
                last_error: Some(clamp_detail(&chain)),
                ..Default::default()
            },
        )),
        PredictFailure::Other => Err(ApiError::internal("Prediction failed")),
    }
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
    use anyhow::anyhow;
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

        // Untyped on purpose: these are the *fallback* path, i.e. exactly
        // what an error raised by code predating the typed marker looks like
        // here. The typed path is the test below.
        for chain in [&fatal, &idle, &poisoned, &dropped, &too_late, &unloaded] {
            assert_eq!(
                classify_predict_failure(&anyhow!("{chain}"), chain, model),
                PredictFailure::Unattempted,
                "{chain}"
            );
        }
    }

    /// The typed marker is the primary signal: an `Unattempted` error
    /// classifies as one whatever it says, so a reworded death cannot
    /// silently cost a window its re-queue — which is the whole weakness the
    /// substring list above was a bridge for.
    #[test]
    fn the_typed_marker_classifies_a_death_whatever_it_renders() {
        let model = "clip/model-a";
        let novel = "the replica evaporated in a way nobody has written a marker for";
        assert_eq!(
            classify_predict_failure(&Unattempted::error(novel), novel, model),
            PredictFailure::Unattempted
        );
        // The same text, untyped, is an ordinary failure — so the assertion
        // above is about the type and not about the words.
        assert_eq!(
            classify_predict_failure(&anyhow!("{novel}"), novel, model),
            PredictFailure::Other
        );

        // The marker survives the `.context` the pool and the job runner add
        // on the way out: `downcast_ref` walks the whole chain.
        let wrapped = Unattempted::error(novel)
            .context("inference request failed")
            .context("endpoint http://localhost:1/api/inference");
        assert_eq!(
            classify_predict_failure(&wrapped, &format!("{wrapped:#}"), model),
            PredictFailure::Unattempted
        );

        // And the load check still outranks it, for the reason it outranks
        // the substrings: a model that will not come up must not cost every
        // item a second full attempt.
        let while_loading = Unattempted::error("inferio worker died")
            .context(format!("failed to load model {model}"));
        assert_eq!(
            classify_predict_failure(&while_loading, &format!("{while_loading:#}"), model),
            PredictFailure::LoadFailed
        );

        // The message is passed through byte for byte, so the fallback list
        // and its tests still describe what a caller actually sees.
        assert_eq!(format!("{}", Unattempted::error(novel)), novel);
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
            classify_predict_failure(
                &anyhow!("{death_with_a_stale_tail}"),
                &death_with_a_stale_tail,
                model
            ),
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
            classify_predict_failure(&anyhow!("{real}"), &real, model),
            PredictFailure::LoadFailed,
            "a load failure of this model keeps its precedence over the death"
        );

        // Parity: a chain that said `Failed to load model` before, with no
        // death marker on it, still does.
        let parity = "failed to load model something-else: nope";
        assert_eq!(
            classify_predict_failure(&anyhow!("{parity}"), parity, model),
            PredictFailure::LoadFailed
        );
        let ordinary = "the worker returned an error";
        assert_eq!(
            classify_predict_failure(&anyhow!("{ordinary}"), ordinary, model),
            PredictFailure::Other
        );
    }

    /// The six renderings, through the code the handler actually runs and out
    /// the other side **as the extraction job's own client reads them**.
    ///
    /// The classification test above stops at the verdict; this one carries
    /// each shape to the JSON the job sees, because that is where the
    /// re-queue decision is really made: `classify_item_failure` asks
    /// `InferenceFailure::is_worker_death()`, which reads `detail.kind` out
    /// of exactly this body. A shape that classified right but answered the
    /// wrong body would re-queue nothing.
    #[tokio::test]
    async fn every_shape_of_a_worker_death_reaches_the_job_as_worker_died() {
        let model = "clip/model-a";
        let shapes: Vec<anyhow::Error> = vec![
            // `Worker::fatal`, and every request `dispatch::fail_requests`
            // hands the same rendering to.
            Unattempted::error(format!(
                "inferio worker {model}#0 failed fatally: early eof; process status: \
                 signal 9; stderr tail:\nTraceback…"
            )),
            // `dispatch::reap_idle_replicas`, through `fail_requests`.
            Unattempted::error(format!(
                "inferio worker for model {model} exited while idle: pid 41 signal 9"
            )),
            // `Worker::roundtrip` refusing a poisoned worker.
            Unattempted::error(format!(
                "inferio worker {model}#1 is dead after a previous fatal error"
            )),
            // `ModelManager::predict`, reply sender dropped.
            Unattempted::error(format!(
                "the dispatcher for model {model} dropped the request"
            )),
            // `ModelManager::predict`, send after the dispatcher ended.
            Unattempted::error(format!(
                "model {model} was unloaded before the request could be queued"
            )),
            // `dispatch`'s `End::Graceful` arm, through `fail_requests`.
            Unattempted::error(format!("model {model} was unloaded")),
        ];
        for err in shapes {
            let rendered = format!("{err:#}");
            let response = predict_failure_response(err, model)
                .unwrap_or_else(|api| panic!("{rendered} answered a plain error: {api:?}"));
            assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
            let body = to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body");
            let failure = crate::inferio_client::InferenceFailure::parse(
                reqwest::StatusCode::INTERNAL_SERVER_ERROR,
                None,
                &String::from_utf8_lossy(&body),
            );
            assert!(
                failure.is_worker_death(),
                "{rendered} reached the job as {failure:?}"
            );
            assert_eq!(failure.model.as_deref(), Some(model));
            assert_eq!(
                failure.last_error.as_deref(),
                Some(rendered.as_str()),
                "the job records what actually happened"
            );
        }

        // The counterexample, through the same path: an ordinary predict
        // failure must stay a plain error, or every failed item would be
        // re-submitted once for nothing.
        let ordinary = predict_failure_response(anyhow!("the model returned no outputs"), model);
        assert!(ordinary.is_err(), "an ordinary failure is not structured");
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
        assert_eq!(
            model.desired_in_flight_items, None,
            "no window dispatched yet -> no figure published yet"
        );
        assert_eq!(model.queue_bound_windows, 0);
        assert!(
            health.vram.is_empty(),
            "an unknown GPU inventory means an empty ledger and no admission"
        );

        // **S1: the client side is reported too.** The endpoint the real
        // client above just used is in the registry, with the transport it
        // resolved, the connections it may hold and the concurrency its gate
        // currently admits. Every one of these was invisible during run2's S1,
        // and the one log line that named any of them named the wrong number.
        let endpoint = health
            .inference_clients
            .iter()
            .find(|entry| entry.base_url.starts_with(&base_url))
            .expect("the endpoint the test client used is reported");
        assert_eq!(
            endpoint.transport, "h2c",
            "the serve loop speaks HTTP/2 cleartext with prior knowledge"
        );
        assert_eq!(
            endpoint.pool_connections,
            Some(crate::inferio_client::INFERENCE_CONNECTION_LANES)
        );
        assert_eq!(
            endpoint.max_concurrent_requests,
            crate::inferio_client::INFERENCE_MAX_CONCURRENT_REQUESTS,
            "no figure published yet, so the gate sits at its floor"
        );
        assert_eq!(
            endpoint.in_flight_requests, 0,
            "nothing is in flight while the health probe is being answered"
        );
        assert_eq!(endpoint.connections_in_use, Some(0));

        // The server side's one peer-movable memory bound. The predict above
        // has been answered, so its body's reservation is back; what stays is
        // the pair of constants an operator needs to read a 503 against.
        assert_eq!(
            health.predict_body_budget.budget_bytes,
            PREDICT_INFLIGHT_BODY_BYTES as u64
        );
        assert_eq!(
            health.predict_body_budget.request_limit_bytes,
            PREDICT_BODY_LIMIT as u64
        );
        assert!(
            health.predict_body_budget.in_flight_bytes <= PREDICT_INFLIGHT_BODY_BYTES as u64,
            "a reservation is returned when its request is answered"
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
    /// Settings whose registry resolves to nothing: `python/inferio/config`
    /// does not exist relative to the crate CWD cargo runs tests in, so
    /// `InferioState::from_settings` takes its degraded path and the state
    /// it builds serves an empty registry. Cheap, and it never spawns a
    /// worker — which is what makes it usable by tests that only care about
    /// what the HTTP layer does before the manager is reached.
    fn registryless_settings() -> crate::config::Settings {
        use crate::config::{InferenceLocalConfig, Settings, UpstreamConfig, UpstreamsConfig};

        // Force the default-dirs error path deterministically: no
        // python/inferio/config relative to the test CWD.
        assert!(
            !std::path::Path::new("python/inferio/config").is_dir(),
            "test premise: the built-in config dir is absent from the crate CWD"
        );

        Settings {
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
        }
    }

    #[tokio::test]
    async fn from_settings_degrades_when_builtin_config_dir_is_missing() {
        let settings = registryless_settings();
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

    // ------------------------------------------------------------------
    // P2 (run2 Phase A): the predict handler must read the request body to
    // its end.
    // ------------------------------------------------------------------

    /// The root cause of P2, pinned on the one property that fixes it.
    ///
    /// A server that answers while the request body is still open has to
    /// reset the stream (RFC 9113 §8.1), and hyper does. The client's
    /// terminal DATA frame then arrives on a stream this end has closed,
    /// which h2 counts as a local error reset; 1 024 of those on one
    /// connection and it answers `GOAWAY(ENHANCE_YOUR_CALM,
    /// "too_many_internal_resets")`, failing every request in flight. multer
    /// stops at the closing boundary and never polls what follows it, so the
    /// streamed parse this handler used left exactly that reset behind on
    /// every predict.
    ///
    /// Asserted at the body rather than at the transport, so it is
    /// deterministic and needs no socket: the body reports when it is polled
    /// past its last chunk, and the request goes through the real router and
    /// the real handler. The bytes and the chunking are reqwest's — the
    /// closing boundary is its own chunk, and the end of the stream is a
    /// separate poll — because that is the client this surface is called by.
    /// The boundary the probe body below uses.
    const PROBE_BOUNDARY: &str = "0123456789abcdef-0123456789abcdef";

    /// A well-formed predict body, chunked the way reqwest chunks one — the
    /// closing boundary is its own chunk and the end of the stream is a
    /// further poll — that reports whether it was read to its end.
    fn probe_body_after(tail_delay: Duration) -> (Body, Arc<std::sync::atomic::AtomicBool>) {
        use std::sync::atomic::Ordering;

        const BOUNDARY: &str = PROBE_BOUNDARY;
        let mut head = Vec::new();
        head.extend_from_slice(format!("--{BOUNDARY}\r\n").as_bytes());
        head.extend_from_slice(b"Content-Disposition: form-data; name=\"data\"\r\n\r\n");
        head.extend_from_slice(br#"{"inputs":[null]}"#);
        head.extend_from_slice(b"\r\n");
        head.extend_from_slice(format!("--{BOUNDARY}\r\n").as_bytes());
        head.extend_from_slice(
            b"Content-Disposition: form-data; name=\"files\"; filename=\"0\"\r\n\
              Content-Type: application/octet-stream\r\n\r\n",
        );
        head.extend_from_slice(&vec![b'x'; 4096]);
        head.extend_from_slice(b"\r\n");
        let closing = format!("--{BOUNDARY}--\r\n").into_bytes();

        let drained = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let chunks = vec![
            axum::body::Bytes::from(head),
            axum::body::Bytes::from(closing),
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
                        // Polled past the last chunk: the only way to see
                        // this is to read the body to its end. The delay
                        // models the terminal DATA frame arriving after the
                        // bytes — which is how a real h2 client sends one.
                        tokio::time::sleep(tail_delay).await;
                        drained.store(true, Ordering::SeqCst);
                        None
                    }
                }
            },
        );

        (Body::from_stream(stream), drained)
    }

    #[tokio::test]
    async fn a_predict_reads_the_request_body_to_its_end() {
        use std::sync::atomic::Ordering;
        use tower::ServiceExt as _;

        // The "before": the streamed parse this handler used to do, on the
        // same body. multer answers `None` at the closing boundary and never
        // polls what follows it, so the request stream never ends — which is
        // the reset hyper then has to send.
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

        // The "after": the shipped handler, on an identical body.
        let (body, drained) = probe_body_after(Duration::from_millis(200));
        let settings = registryless_settings();
        let state = InferioState::from_settings(&settings).expect("state builds");
        let request = axum::http::Request::builder()
            .method("POST")
            // A model the empty registry does not have: the handler fails
            // after the body, which is all this test is about, and nothing
            // spawns.
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
            "the handler answered without reading the request body to its end, \
             which makes hyper reset the h2 stream on every predict (run2 P2)"
        );
        // And the body it read parsed: the failure that is left is the
        // missing model, not the multipart.
        let (content_type, body) = split_response(response).await;
        assert!(content_type.contains("application/json"), "{content_type}");
        let detail = String::from_utf8_lossy(&body);
        assert!(
            !detail.contains("invalid multipart body"),
            "a well-formed body must not be reported as a malformed one: {detail}"
        );
        state.manager.shutdown().await;
    }

    /// The other half of P2: what the handler *says* when a multipart parse
    /// fails. axum renders every cause of a `MultipartError` as the same
    /// sentence, so the run2 log line ("invalid multipart body: Error parsing
    /// `multipart/form-data` request") named the layer and nothing else. The
    /// detail must carry the cause out of the source chain, or the next one
    /// is as undiagnosable as this one was.
    ///
    /// `/health` names the **effective** pixel canvas a model's grants are
    /// priced under.
    ///
    /// Without it `last_grant_units` is ambiguous: under a canvas the worker
    /// prices every input at `min(raw_pixels, canvas_pixels)`, so the same
    /// unit budget describes a very different batch depending on whether one
    /// is in force — and the canvas that *is* in force may be one the registry
    /// never stated (a model whose canvas is knowable only from its own load
    /// report has it filled in at spawn).
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

    /// The predict route's body ceiling: a whole body that is too big is
    /// neither "malformed" nor "never arrived" but its own answer — `413`,
    /// naming the limit — so a caller learns to send a smaller batch instead
    /// of re-submitting the same one forever.
    ///
    /// Asserted on [`collect_within`] with a small limit rather than by
    /// sending [`PREDICT_BODY_LIMIT`] bytes: the mapping from an over-long
    /// body to the status is the whole of the behaviour, and the constant is
    /// checked separately below.
    #[tokio::test]
    async fn a_predict_body_over_the_limit_is_too_large_not_malformed() {
        let over = collect_within(Body::from(vec![b'x'; 64]), 32, &PREDICT_BODY_BYTES).await;
        assert!(matches!(over, Err(PredictBodyError::TooLarge)));
        let response = PredictBodyError::TooLarge.into_response();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

        let exact = collect_within(Body::from(vec![b'x'; 32]), 32, &PREDICT_BODY_BYTES).await;
        assert!(
            matches!(&exact, Ok((bytes, _)) if bytes.len() == 32),
            "a body exactly at the limit is not over it"
        );

        // A body that declares no length at all is charged and bounded as it
        // arrives, so the limit does not depend on a peer being honest about
        // `Content-Length` — nor on it sending one.
        let chunked = Body::from_stream(futures_util::stream::iter(vec![
            Ok::<_, std::io::Error>(axum::body::Bytes::from_static(&[b'x'; 24])),
            Ok(axum::body::Bytes::from_static(&[b'x'; 24])),
        ]));
        assert!(
            matches!(
                collect_within(chunked, 32, &PREDICT_BODY_BYTES).await,
                Err(PredictBodyError::TooLarge)
            ),
            "an undeclared body is bounded by what arrives, not by what it claims"
        );
        let chunked_ok = Body::from_stream(futures_util::stream::iter(vec![
            Ok::<_, std::io::Error>(axum::body::Bytes::from_static(&[b'x'; 16])),
            Ok(axum::body::Bytes::from_static(&[b'x'; 16])),
        ]));
        assert!(
            matches!(
                collect_within(chunked_ok, 32, &PREDICT_BODY_BYTES).await,
                Ok((bytes, _)) if bytes.len() == 32
            ),
            "and it is not refused for arriving in pieces"
        );

        // The shipped ceiling is the orchestrator's own frame wall, so a body
        // it refuses could never have become a worker frame anyway — and it
        // is sized for the largest legitimate *single-input* request, which
        // `check_frame_budget` admits up to 8 MiB below it.
        assert_eq!(PREDICT_BODY_LIMIT, MAX_FRAME_BYTES);
        assert!(crate::inferio::worker::FRAME_INPUT_BYTES_BUDGET < PREDICT_BODY_LIMIT);
    }

    /// **The bound `PREDICT_BODY_LIMIT` cannot be: what this process holds in
    /// predict bodies at once, across every connection and every peer.**
    ///
    /// A per-request limit times a per-connection stream limit is not a
    /// memory bound — nothing limits how many connections a peer opens, and
    /// even for one connection `512 x 2 GiB` is a statement about arithmetic.
    /// So the aggregate is a budget, charged before the bytes are read; and
    /// because a body that cannot be admitted must be *answered* rather than
    /// queued (a wait would hold the request stream open, which is the whole
    /// of `7e96de62`), the refusal is a typed `503` the caller already knows
    /// how to act on.
    ///
    /// Driven against a budget of the test's own, for the reason the whole
    /// budget exists: exhausting the process-wide one would refuse every
    /// other test's predicts in this binary. The semaphore is the only thing
    /// substituted; the code under test, the counter and the rendering are
    /// the production ones.
    #[tokio::test]
    async fn the_predict_body_budget_is_a_process_wide_ceiling_that_answers_503() {
        // Two maximal bodies fit, so a maximal request is never refused for
        // as long as another maximal one is in flight.
        assert!(PREDICT_INFLIGHT_BODY_BYTES >= 2 * PREDICT_BODY_LIMIT);

        // A tiny stand-in for the real budget, leaked so it has the `'static`
        // lifetime a reservation outlives its request with.
        let budget: &'static tokio::sync::Semaphore =
            Box::leak(Box::new(tokio::sync::Semaphore::new(64)));

        // Hold everything but 8 bytes of it.
        let mut hog = BodyReservation::new(budget);
        hog.reserve(56)
            .expect("an empty budget admits a whole body");
        assert_eq!(budget.available_permits(), 8);

        // 8 bytes still fit; the ninth does not, and is refused rather than
        // waited on — this test would hang instead of failing if it waited.
        let refusals_before = predict_body_budget_health().refused_requests;
        let (fits, _) = collect_within(Body::from(vec![b'x'; 8]), PREDICT_BODY_LIMIT, budget)
            .await
            .expect("a body inside the remaining budget is read");
        assert_eq!(fits.len(), 8);
        let refused = collect_within(Body::from(vec![b'x'; 9]), PREDICT_BODY_LIMIT, budget).await;
        assert!(matches!(refused, Err(PredictBodyError::Overloaded)));
        assert!(
            predict_body_budget_health().refused_requests > refusals_before,
            "a refusal is counted where an operator can see it"
        );

        // And the report is the semaphore's own state, not a counter kept
        // beside it. Asserted on the mapping, because the real budget is
        // shared with every other test in this binary.
        let health = budget_health(PREDICT_INFLIGHT_BODY_BYTES - 4096, 7);
        assert_eq!(health.budget_bytes, PREDICT_INFLIGHT_BODY_BYTES as u64);
        assert_eq!(health.request_limit_bytes, PREDICT_BODY_LIMIT as u64);
        assert_eq!(health.in_flight_bytes, 4096);
        assert_eq!(health.refused_requests, 7);
        assert!(predict_body_budget_health().in_flight_bytes <= PREDICT_INFLIGHT_BODY_BYTES as u64);

        // The refusal is a 503 with a retry delay and the kind the caller's
        // `is_unattempted()` reads — the batch was never parsed, so its items
        // are untouched.
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
        assert!(
            failure.is_unattempted(),
            "the items were never handed to a model, so the caller re-submits"
        );
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
    }

    /// This body also stops before its closing delimiter, so it is the
    /// *incomplete* half of the split as well: the caller is told the batch
    /// was never parsed, and re-submits it.
    #[tokio::test]
    async fn a_malformed_predict_body_names_its_cause() {
        use tower::ServiceExt as _;

        let settings = registryless_settings();
        let state = InferioState::from_settings(&settings).expect("state builds");
        // A body that stops in the middle of a part: well-formed prefix,
        // no closing boundary.
        let truncated = "--BOUNDARY\r\nContent-Disposition: form-data; name=\"data\"\r\n\r\n{";
        // (the `data` field never terminates, so the failure is multer's
        // IncompleteFieldData — reached through `Field::text`, which is one
        // of the three places the handler renders a parse failure)
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/predict/nope/model?cache_key=k&lru_size=1&ttl_seconds=-1")
            .header(
                header::CONTENT_TYPE,
                "multipart/form-data; boundary=BOUNDARY",
            )
            .body(Body::from(truncated))
            .unwrap();

        let response = router(Arc::clone(&state))
            .oneshot(request)
            .await
            .expect("the router answers");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let (_content_type, body) = split_response(response).await;
        let detail = String::from_utf8_lossy(&body).to_string();
        assert!(
            detail.contains("invalid data field"),
            "the prose the operator already knows: {detail}"
        );
        assert!(
            detail.contains("incomplete data"),
            "and the cause underneath it, which is the half P2 was missing: {detail}"
        );
        // Nothing was parsed and nothing was attempted, and the caller is
        // told so in the one field it can act on.
        let failure = crate::inferio_client::InferenceFailure::parse(
            reqwest::StatusCode::BAD_REQUEST,
            None,
            &detail,
        );
        assert!(
            failure.is_request_incomplete() && failure.is_unattempted(),
            "a body that stops before its closing delimiter is an unattempted \
             request, not a verdict on the media: {detail}"
        );
        state.manager.shutdown().await;
    }

    /// The split above is only as good as the boundary it reads out of the
    /// header — and, more than that, it has to be the boundary *multer*
    /// read, or the verdict is about a body nobody sent. So the shapes here
    /// are asserted against `multer::parse_boundary`'s own answer as well as
    /// against the expected string.
    #[test]
    fn the_declared_boundary_is_the_one_the_parser_used() {
        fn multer_boundary(content_type: &str) -> Option<String> {
            // multer 3.1 `parse_boundary`, inlined: it is not re-exported by
            // axum, and the point is to reproduce it exactly.
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

        for (content_type, expected) in [
            ("multipart/form-data; boundary=abc-123", Some("abc-123")),
            // The parameter name is case-insensitive; the value is not.
            ("multipart/form-data; BOUNDARY=abc-123", Some("abc-123")),
            // `=` is not a token character, so an unquoted value carrying
            // one is not a parameter at all — to `mime`, to multer, and so
            // to this.
            ("multipart/form-data; boundary=a=b", None),
            // A quoted boundary — the only way to send one containing a
            // space — arrives unquoted, which is the form multer then looks
            // for in the body.
            ("multipart/form-data; boundary=\"a b\"", Some("a b")),
            // `mime` rejects whitespace around the `=`, so multer sees no
            // multipart at all and neither does this.
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
        // And what it finds is what the closing delimiter is looked for
        // with, epilogue or no epilogue.
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

    /// The other side of the same split, and the reason it is asked of the
    /// bytes rather than of the parser's error variant: a body that carries
    /// the closing delimiter of the boundary it declared *did* all arrive,
    /// so whatever is wrong is wrong inside it. Asking again would produce
    /// the same answer, and the caller must not be told to re-submit.
    #[tokio::test]
    async fn a_complete_but_invalid_predict_body_is_an_ordinary_bad_request() {
        use tower::ServiceExt as _;

        let settings = registryless_settings();
        let state = InferioState::from_settings(&settings).expect("state builds");
        // Whole body, closing delimiter and all — but the part's headers are
        // not headers, so multer rejects what is unambiguously all here.
        let complete = "--BOUNDARY\r\n\u{1}not a header\r\n\r\nx\r\n--BOUNDARY--\r\n";
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/predict/nope/model?cache_key=k&lru_size=1&ttl_seconds=-1")
            .header(
                header::CONTENT_TYPE,
                "multipart/form-data; boundary=BOUNDARY",
            )
            .body(Body::from(complete))
            .unwrap();

        let response = router(Arc::clone(&state))
            .oneshot(request)
            .await
            .expect("the router answers");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let (_content_type, body) = split_response(response).await;
        let detail = String::from_utf8_lossy(&body).to_string();
        assert!(
            detail.contains("invalid multipart body"),
            "the prose the operator already knows: {detail}"
        );
        let failure = crate::inferio_client::InferenceFailure::parse(
            reqwest::StatusCode::BAD_REQUEST,
            None,
            &detail,
        );
        assert!(
            !failure.is_unattempted(),
            "a whole body that is not a multipart is a bad request, and \
             re-submitting it would only fail again: {detail}"
        );
        state.manager.shutdown().await;
    }
}
