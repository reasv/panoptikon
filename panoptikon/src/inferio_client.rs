//! The gateway's HTTP client for an inference endpoint: transport selection
//! (h2c with prior knowledge, HTTP/1.1 fallback), the per-endpoint connection
//! lanes and in-flight gate, and the typed failures a predict can end in.
//!
//! See docs/inferio-transport.md "Client (inferio_client.rs)" for the
//! constants and their derivation, the gate arithmetic and the failure-kind
//! and transport-phase tables.

use anyhow::{Context, Result, bail};
use reqwest::header::CONTENT_TYPE;
use reqwest::multipart::{Form, Part};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry::policies::ExponentialBackoff;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::warn;

use crate::config::Settings;
use crate::inferio::slot_error::{ProtocolViolation, SlotErrorClass, slot_error_from_json};

#[derive(Debug, Clone)]
pub(crate) enum InferenceFile {
    Path(PathBuf),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone)]
pub(crate) struct InferenceInput {
    pub data: Value,
    pub file: Option<InferenceFile>,
}

impl InferenceInput {
    pub fn new(data: Value, file: Option<InferenceFile>) -> Self {
        Self { data, file }
    }
}

#[derive(Debug)]
pub(crate) enum PredictOutput {
    Json(Vec<Value>),
    Binary(Vec<Vec<u8>>),
}

impl PredictOutput {
    /// How many successful outputs this carries. Only ever zero when every
    /// slot of the response was a typed error.
    pub fn len(&self) -> usize {
        match self {
            PredictOutput::Json(values) => values.len(),
            PredictOutput::Binary(values) => values.len(),
        }
    }

    /// True when nothing succeeded, which is the one case callers must not
    /// merge (an empty `Json` would clash with a `Binary` sibling chunk).
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// One input's typed failure, carried alongside the surviving outputs
/// (`docs/inferio-worker-protocol.md`, "Per-item error slots"). `index` is the
/// *input*'s position: erroring slots are removed from the outputs.
#[derive(Debug, Clone)]
pub(crate) struct PredictSlotError {
    pub index: usize,
    pub class: SlotErrorClass,
    pub message: String,
}

/// A predict response: the outputs of the inputs that succeeded, plus the
/// typed per-slot failures of the ones that did not. `errors` is empty for
/// every response a server without per-item error slots can produce.
#[derive(Debug)]
pub(crate) struct PredictResponse {
    pub outputs: PredictOutput,
    pub errors: Vec<PredictSlotError>,
    /// The orchestrator's desired in-flight figure for this model, in items
    /// ([`DESIRED_IN_FLIGHT_HEADER`]). `None` when the server did not say, or
    /// said something unparsable or zero; callers then keep their own floor.
    pub desired_in_flight_items: Option<u64>,
}

/// Response header the local orchestrator publishes the figure on
/// (`inferio::http::DESIRED_IN_FLIGHT_HEADER`; documented in
/// `docs/inferio-worker-protocol.md`).
pub(crate) const DESIRED_IN_FLIGHT_HEADER: &str = "x-panoptikon-desired-in-flight-items";

/// `detail.kind` of a predict that failed because the inference worker
/// process died with the request in flight
/// (`inferio::http::WORKER_DIED_KIND`). The items were never attempted.
pub(crate) const WORKER_DIED_KIND: &str = "worker_died";

/// `detail.kind` of a request refused because the model is inside its
/// per-model load-failure cooldown. Unlike every other 503 this must **not**
/// be retried.
pub(crate) const LOAD_COOLDOWN_KIND: &str = "load_cooldown";

/// `detail.kind` of a predict the server never parsed because its **request
/// body did not arrive in full** (`inferio::http::REQUEST_INCOMPLETE_KIND`).
/// It rides on a 400 and is the one 400 that must not be read as a verdict.
pub(crate) const REQUEST_INCOMPLETE_KIND: &str = "request_incomplete";

/// `detail.kind` of a predict the server refused to **read** because it was
/// already holding its whole predict-body budget
/// (`inferio::http::BODY_BUDGET_KIND`). A 503 with a `Retry-After`; nothing
/// was parsed.
pub(crate) const BODY_BUDGET_KIND: &str = "body_budget_exhausted";

/// `detail.kind` this client writes on a failure of **its own transport**: a
/// predict that ended before an answer was read, or read to its end. The one
/// kind that never travels on the wire and cannot.
pub(crate) const TRANSPORT_KIND: &str = "transport";

/// How far a predict got before its transport failed, which is the whole of
/// what such a failure says about the item. In request order; the load-bearing
/// boundary is between [`Self::Headers`] and [`Self::Body`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportPhase {
    /// **No connection was established** — refused, unreachable, a DNS or TLS
    /// failure, or a connect timeout. Not one byte left this process.
    Connect,
    /// **The connection was up and no response head came of it** — a reset, a
    /// refused stream, or a body that stopped being writable. Claims only
    /// that no answer had been produced, not that nothing was parsed.
    Send,
    /// **The request was delivered and no response head ever arrived** — the
    /// connection went away, or the read deadline passed first. Not proof the
    /// server did nothing, only that no verdict reached this caller.
    Headers,
    /// **The response head arrived and the body did not survive the trip** —
    /// a `GOAWAY` mid-body, a reset, a truncation, a read timeout. Not
    /// "unattempted" ([`InferenceFailure::warrants_resubmission`]).
    Body,
}

impl TransportPhase {
    /// Stable and lowercase, for the log line and the job's audit text.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::Send => "send",
            Self::Headers => "headers",
            Self::Body => "body",
        }
    }

    /// Whether the failure was observed **before any answer existed**, i.e.
    /// every phase short of a response head. The one fact
    /// [`InferenceFailure::is_unattempted`] needs from a transport failure.
    pub fn is_before_any_answer(self) -> bool {
        !matches!(self, Self::Body)
    }
}

/// This client's classification of a transport failure: how far the request
/// got, and what `reqwest` called the error — the phase is a judgement and
/// the class is the evidence for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TransportFailure {
    /// How far the request got. This is what callers act on.
    pub phase: TransportPhase,
    /// `reqwest`'s own name for the error ([`reqwest_error_class`]), refined
    /// with `refused_stream` where the chain names one.
    pub class: &'static str,
}

/// A request the inference server refused, with the machine-readable half of
/// its `{"detail": …}` body parsed out. Typed rather than prose so a caller's
/// decision survives the error being wrapped in context. `kind` is `None`
/// when the body carried a plain string detail.
#[derive(Debug, Clone)]
pub(crate) struct InferenceFailure {
    /// HTTP status of the refusal, or **0 when there was no response at
    /// all** — a client-side [`TRANSPORT_KIND`] failure.
    pub status: u16,
    /// `detail.kind`, when the body carried a structured detail.
    pub kind: Option<String>,
    /// Human-readable summary: `detail.message` for a structured detail, the
    /// whole `detail` for a plain string one, and the raw body when it is
    /// neither.
    pub message: String,
    /// The model the failure is about, `group/name`.
    pub model: Option<String>,
    /// The last error that put the model here, or the fatal chain.
    pub last_error: Option<String>,
    /// RFC 3339 instant the model may be retried at.
    pub retry_at: Option<String>,
    /// Consecutive load failures counted so far.
    pub failures: Option<u32>,
    /// `Retry-After`, in seconds, when the server sent one.
    pub retry_after_secs: Option<u64>,
    /// Set only by [`InferenceFailure::from_transport`], i.e. only when
    /// *this* process observed its own request fail, so it is unforgeable:
    /// [`InferenceFailure::parse`] leaves it `None` whatever the body says.
    pub transport: Option<TransportFailure>,
}

impl InferenceFailure {
    /// Parse one refused response. Never fails: a body this cannot read is
    /// still a failure with no machine-readable half. Crate-visible so the
    /// local service's tests can read their own answers as the job does.
    pub(crate) fn parse(status: reqwest::StatusCode, retry_after: Option<u64>, body: &str) -> Self {
        let mut failure = Self {
            status: status.as_u16(),
            kind: None,
            message: body.trim().to_owned(),
            model: None,
            last_error: None,
            retry_at: None,
            failures: None,
            retry_after_secs: retry_after,
            // A peer cannot classify this client's transport: a body that
            // claims `kind = "transport"` arrives with no phase.
            transport: None,
        };
        let Ok(parsed) = serde_json::from_str::<Value>(body) else {
            return failure;
        };
        let Some(detail) = parsed.get("detail") else {
            return failure;
        };
        if let Some(text) = detail.as_str() {
            failure.message = text.to_owned();
            return failure;
        }
        let Some(object) = detail.as_object() else {
            return failure;
        };
        let text = |key: &str| object.get(key).and_then(Value::as_str).map(str::to_owned);
        failure.kind = text("kind");
        if let Some(message) = text("message") {
            failure.message = message;
        }
        failure.model = text("model");
        failure.last_error = text("last_error");
        failure.retry_at = text("retry_at");
        failure.failures = object
            .get("failures")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok());
        failure
    }

    /// This client's own account of a predict whose transport failed.
    /// `status` is 0 because there is no status, and `last_error` is the
    /// whole source chain — `reqwest`'s `Display` names only the layer.
    pub(crate) fn from_transport(phase: TransportPhase, err: &reqwest::Error) -> Self {
        Self {
            status: 0,
            kind: Some(TRANSPORT_KIND.to_owned()),
            message: err.to_string(),
            model: None,
            last_error: Some(error_chain(err)),
            retry_at: None,
            failures: None,
            retry_after_secs: None,
            transport: Some(TransportFailure {
                phase,
                class: reqwest_error_class(err),
            }),
        }
    }

    /// The worker process died with the request in flight.
    pub fn is_worker_death(&self) -> bool {
        self.kind.as_deref() == Some(WORKER_DIED_KIND)
    }

    /// The request body never arrived in full, so the server never parsed
    /// the batch.
    pub fn is_request_incomplete(&self) -> bool {
        self.kind.as_deref() == Some(REQUEST_INCOMPLETE_KIND)
    }

    /// The server refused to read the body: it was already holding its whole
    /// predict-body budget.
    pub fn is_body_budget_exhausted(&self) -> bool {
        self.kind.as_deref() == Some(BODY_BUDGET_KIND)
    }

    /// This client's classification of its own transport failure. Keyed on
    /// the phase field rather than the kind string, so a server answering
    /// `{"kind": "transport"}` still gets `None` here.
    pub fn transport_phase(&self) -> Option<TransportPhase> {
        self.transport.map(|failure| failure.phase)
    }

    /// **No answer about this request's items had been produced when it
    /// failed** — the three server kinds, plus every transport phase short of
    /// a response head. Keyed on the typed kind, never on the status;
    /// [`TransportPhase::Body`] is deliberately not here.
    pub fn is_unattempted(&self) -> bool {
        self.is_worker_death()
            || self.is_request_incomplete()
            || self.is_body_budget_exhausted()
            || self
                .transport_phase()
                .is_some_and(TransportPhase::is_before_any_answer)
    }

    /// **Re-submitting this request's items is correct.**
    /// [`Self::is_unattempted`] plus [`TransportPhase::Body`], which rests on
    /// a predict being idempotent. This is what a re-queue policy asks.
    pub fn warrants_resubmission(&self) -> bool {
        self.is_unattempted() || self.transport_phase().is_some()
    }

    /// The model is inside its per-model load-failure cooldown.
    pub fn is_load_cooldown(&self) -> bool {
        self.kind.as_deref() == Some(LOAD_COOLDOWN_KIND)
    }
}

impl std::fmt::Display for InferenceFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.status == 0 {
            // There is no status when there was no response; printing a 0
            // would read as one.
            write!(f, "inference request failed (no response)")?;
        } else {
            write!(f, "inference request failed ({})", self.status)?;
        }
        if let Some(kind) = &self.kind {
            match self.transport {
                // The phase is the load-bearing half of a transport failure,
                // so it is printed with the kind rather than after it.
                Some(transport) => write!(f, " [{kind}/{}]", transport.phase.as_str())?,
                None => write!(f, " [{kind}]")?,
            }
        }
        if !self.message.is_empty() {
            write!(f, ": {}", self.message)?;
        }
        if let Some(retry_at) = &self.retry_at {
            write!(f, "; retry at {retry_at}")?;
        }
        if let Some(last_error) = &self.last_error {
            write!(f, "; last error: {last_error}")?;
        }
        Ok(())
    }
}

impl std::error::Error for InferenceFailure {}

/// The typed failure inside an error chain, if there is one. The chain is
/// what callers hold: `InferencePool` wraps, and the job path adds context.
pub(crate) fn inference_failure(err: &anyhow::Error) -> Option<&InferenceFailure> {
    err.downcast_ref::<InferenceFailure>()
}

/// `Retry-After` in seconds, when the header is present and is a plain
/// delta-seconds value (the only form this surface sends).
fn retry_after_secs(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
}

impl PredictResponse {
    fn plain(outputs: PredictOutput) -> Self {
        Self {
            outputs,
            errors: Vec::new(),
            desired_in_flight_items: None,
        }
    }
}

/// How this client talks to one inference endpoint. Under HTTP/1.1 a
/// concurrent request costs a socket; under HTTP/2 it is a stream on a
/// pooled connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Transport {
    /// HTTP/2 cleartext with prior knowledge.
    H2c,
    /// HTTP/1.1, one connection per concurrent request.
    Http11,
}

impl Transport {
    /// Whether requests share connections; the job's descriptor clamp is a
    /// different quantity in the two modes.
    pub fn is_multiplexed(self) -> bool {
        matches!(self, Transport::H2c)
    }
}

/// Independent HTTP/2 connections ("lanes") this client may hold to one
/// inference endpoint. Each lane is its own `reqwest::Client` with its own
/// pool, because hyper-util shares one connection across a pool however wide
/// the window gets. Recruited by load ([`EndpointRuntime::pick_lane`]).
pub(crate) const INFERENCE_CONNECTION_LANES: usize = 64;

/// Streams this client offers **one** h2 connection before recruiting the
/// next lane. Below every common server default, so a peer runs them rather
/// than queueing them invisibly inside `h2`.
const H2_STREAMS_PER_CONNECTION: usize = 64;

/// The **floor** of the h2c concurrency gate, and the fixed HTTP/1.1 gate.
/// Under HTTP/1.1 it never moves and must never follow a model's batching
/// advice, because there an admitted request *is* a socket.
pub(crate) const INFERENCE_MAX_CONCURRENT_REQUESTS: usize = 4 * H2_STREAMS_PER_CONNECTION;

/// The **ceiling** of the h2c gate: past it, some lane would be offered more
/// than [`H2_STREAMS_PER_CONNECTION`] streams.
pub(crate) const INFERENCE_MAX_CONCURRENT_STREAMS: usize =
    INFERENCE_CONNECTION_LANES * H2_STREAMS_PER_CONNECTION;

/// One independent HTTP/2 connection to an endpoint, and how much work is on
/// it right now. Its client is built when the lane is first recruited: a
/// `reqwest::Client` costs hundreds of KiB of RSS.
#[derive(Debug)]
struct Lane {
    clients: OnceLock<EndpointClients>,
    in_flight: AtomicUsize,
}

/// The permits the h2c gate wants to exist, and the shrink it has not been
/// able to apply yet. Same rule as `jobs::extraction::UnitBudget`: a shrink
/// withholds permits as they come back, never taking one back in flight.
#[derive(Debug)]
struct GateState {
    target: usize,
    pending_shrink: usize,
}

/// The clients, lanes and shared state of one inference endpoint, shared per
/// base URL across every [`InferenceApiClient`] for it: an unshared
/// connection pool is not a bound.
#[derive(Debug)]
struct EndpointRuntime {
    /// [`INFERENCE_CONNECTION_LANES`] independent h2 clients, each its own
    /// pool and therefore its own connection — built as recruited ([`Lane`]).
    h2: Vec<Lane>,
    /// Lane 0's client, built eagerly, so reachability is settled when the
    /// endpoint is registered rather than mid-predict; also the fallback if a
    /// later lane's build fails.
    h2_seed: EndpointClients,
    /// One client: under HTTP/1.1 a request is a socket regardless, so there
    /// is nothing for a lane to buy.
    h1: EndpointClients,
    /// The resolved transport, `None` until the first probe and again after a
    /// connection error (a server can be restarted into a different one).
    transport: RwLock<Option<Transport>>,
    /// The h2c concurrency gate. Resizable — see
    /// [`Self::set_in_flight_target`].
    h2_gate: Arc<tokio::sync::Semaphore>,
    h2_gate_state: std::sync::Mutex<GateState>,
    /// The HTTP/1.1 gate, fixed at [`INFERENCE_MAX_CONCURRENT_REQUESTS`]
    /// forever: there, an admitted request *is* a socket.
    h1_gate: Arc<tokio::sync::Semaphore>,
}

impl EndpointRuntime {
    /// The clients for one lane, building that lane's own on first use;
    /// `get_or_init` runs the builder at most once.
    fn lane_clients(&self, lane: usize) -> EndpointClients {
        self.h2[lane]
            .clients
            .get_or_init(|| {
                EndpointClients::build(reqwest::ClientBuilder::http2_prior_knowledge, 1)
                    .unwrap_or_else(|err| {
                        warn!(
                            lane,
                            error = %err,
                            "failed to build an additional inference connection lane; \
                             sharing the first lane's connection instead"
                        );
                        self.h2_seed.clone()
                    })
            })
            .clone()
    }

    /// The lane a new request goes on: the least loaded of the lanes the
    /// current load actually *requires*, not of all lanes — spreading over
    /// every lane would cost a socket per request. Racy by design.
    fn pick_lane(&self) -> usize {
        let loads: Vec<usize> = self
            .h2
            .iter()
            .map(|lane| lane.in_flight.load(Relaxed))
            .collect();
        let total: usize = loads.iter().sum();
        let needed = total
            .saturating_add(1)
            .div_ceil(H2_STREAMS_PER_CONNECTION.max(1));
        let recruited = needed.clamp(1, loads.len());
        let mut best = 0usize;
        for idx in 1..recruited {
            if loads[idx] < loads[best] {
                best = idx;
            }
        }
        best
    }

    /// Follow the desired-in-flight figure the endpoint published, clamped
    /// between [`INFERENCE_MAX_CONCURRENT_REQUESTS`] and
    /// [`INFERENCE_MAX_CONCURRENT_STREAMS`]. The figure is in *items* and the
    /// gate counts *requests*; using it directly over-provisions permits,
    /// never sockets. HTTP/1.1's gate is a different semaphore, never moved.
    fn set_in_flight_target(&self, requests: u64) {
        let wanted = usize::try_from(requests).unwrap_or(usize::MAX).clamp(
            INFERENCE_MAX_CONCURRENT_REQUESTS,
            INFERENCE_MAX_CONCURRENT_STREAMS,
        );
        let mut state = self
            .h2_gate_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match wanted.cmp(&state.target) {
            std::cmp::Ordering::Greater => {
                let grow = wanted - state.target;
                // Growth first cancels a shrink that never landed: those
                // permits are still in existence.
                let cancelled = state.pending_shrink.min(grow);
                state.pending_shrink -= cancelled;
                if grow > cancelled {
                    self.h2_gate.add_permits(grow - cancelled);
                }
                state.target = wanted;
            }
            std::cmp::Ordering::Less => {
                state.pending_shrink += state.target - wanted;
                state.target = wanted;
            }
            std::cmp::Ordering::Equal => {}
        }
        // Whatever is free right now can go immediately; the rest is
        // withheld on release, below.
        let removed = self.h2_gate.forget_permits(state.pending_shrink);
        state.pending_shrink -= removed;
    }

    /// Hand back one gate permit, retiring it instead of re-issuing it while
    /// a shrink is outstanding. `Semaphore` hands a released permit straight
    /// to a waiter, so `forget_permits` alone can never land a shrink.
    fn release_h2_permit(&self, permit: tokio::sync::OwnedSemaphorePermit) {
        let mut state = self
            .h2_gate_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.pending_shrink > 0 {
            state.pending_shrink -= 1;
            permit.forget();
        } else {
            drop(permit);
        }
    }

    /// The gate's target and how many of it are in use, for `/health`.
    fn gate_snapshot(&self, transport: Option<Transport>) -> (usize, usize) {
        match transport {
            Some(Transport::Http11) => (
                INFERENCE_MAX_CONCURRENT_REQUESTS,
                INFERENCE_MAX_CONCURRENT_REQUESTS.saturating_sub(self.h1_gate.available_permits()),
            ),
            _ => {
                // Permits in existence are `target + pending_shrink`, so in
                // flight is that minus what is free; from `target` alone, a
                // saturated shrinking endpoint would read as idle.
                let (target, pending) = {
                    let state = self
                        .h2_gate_state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    (state.target, state.pending_shrink)
                };
                (
                    target,
                    target
                        .saturating_add(pending)
                        .saturating_sub(self.h2_gate.available_permits()),
                )
            }
        }
    }

    /// What this endpoint is doing right now, for `/health`.
    fn health(&self, base_url: &str) -> InferenceTransportHealth {
        let transport = self.transport.try_read().ok().and_then(|guard| *guard);
        let (target, in_flight) = self.gate_snapshot(transport);
        let multiplexed = !matches!(transport, Some(Transport::Http11));
        InferenceTransportHealth {
            base_url: base_url.to_owned(),
            transport: match transport {
                Some(Transport::H2c) => "h2c",
                Some(Transport::Http11) => "http/1.1",
                None => "unknown",
            }
            .to_owned(),
            pool_connections: multiplexed.then_some(INFERENCE_CONNECTION_LANES),
            connections_in_use: multiplexed.then(|| self.lanes_in_use()),
            max_concurrent_requests: target,
            in_flight_requests: in_flight,
        }
    }

    /// Lanes carrying at least one request — the sockets actually in use.
    fn lanes_in_use(&self) -> usize {
        self.h2
            .iter()
            .filter(|lane| lane.in_flight.load(Relaxed) > 0)
            .count()
    }
}

/// What one inference endpoint's client is doing right now, for `/health`.
/// Every field is a measured quantity, not a constant restated.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct InferenceTransportHealth {
    /// The endpoint this describes.
    pub base_url: String,
    /// `h2c` | `http/1.1` | `unknown` (nothing has talked to it yet, which
    /// the job-side descriptor budget reads as the HTTP/1.1 case).
    pub transport: String,
    /// Independent connections this client may hold to the endpoint; `null`
    /// under HTTP/1.1, where a connection is a request, not a pool slot.
    pub pool_connections: Option<usize>,
    /// Of those, how many are carrying at least one request right now — the
    /// sockets actually in use. `null` under HTTP/1.1.
    pub connections_in_use: Option<usize>,
    /// Requests the gate currently admits: under h2c the endpoint's own
    /// published figure, clamped; under HTTP/1.1 a constant.
    pub max_concurrent_requests: usize,
    /// Of those, how many are in flight right now.
    pub in_flight_requests: usize,
}

/// One admitted request's claim on an endpoint: its gate permit and its lane.
/// Both are returned by `Drop`, so every exit path accounts for itself.
struct EndpointLease {
    endpoint: Arc<EndpointRuntime>,
    lane: Option<usize>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    multiplexed: bool,
}

impl Drop for EndpointLease {
    fn drop(&mut self) {
        if let Some(lane) = self.lane.take() {
            self.endpoint.h2[lane].in_flight.fetch_sub(1, Relaxed);
        }
        if let Some(permit) = self.permit.take() {
            if self.multiplexed {
                self.endpoint.release_h2_permit(permit);
            } else {
                drop(permit);
            }
        }
    }
}

#[derive(Debug, Clone)]
struct EndpointClients {
    raw: reqwest::Client,
    middleware: ClientWithMiddleware,
}

impl EndpointClients {
    fn build(
        configure: impl FnOnce(reqwest::ClientBuilder) -> reqwest::ClientBuilder,
        pool_max_idle_per_host: usize,
    ) -> Result<Self> {
        let raw =
            configure(reqwest::Client::builder().pool_max_idle_per_host(pool_max_idle_per_host))
                .build()
                .context("failed to build inference API client")?;
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
        let middleware = ClientBuilder::new(raw.clone())
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();
        Ok(Self { raw, middleware })
    }
}

static ENDPOINTS: OnceLock<std::sync::Mutex<HashMap<String, Arc<EndpointRuntime>>>> =
    OnceLock::new();

/// The shared runtime for `base_url`, building it on first use.
fn endpoint_runtime(base_url: &str) -> Result<Arc<EndpointRuntime>> {
    let registry = ENDPOINTS.get_or_init(|| std::sync::Mutex::new(HashMap::new()));
    let mut guard = registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(existing) = guard.get(base_url) {
        return Ok(Arc::clone(existing));
    }
    // A lane *is* a connection, so one idle connection each. Only the first
    // is built here; `pick_lane` recruits the rest.
    let seed = EndpointClients::build(reqwest::ClientBuilder::http2_prior_knowledge, 1)?;
    let mut lanes = Vec::with_capacity(INFERENCE_CONNECTION_LANES);
    for index in 0..INFERENCE_CONNECTION_LANES {
        let clients = OnceLock::new();
        if index == 0 {
            let _ = clients.set(seed.clone());
        }
        lanes.push(Lane {
            clients,
            in_flight: AtomicUsize::new(0),
        });
    }
    let runtime = Arc::new(EndpointRuntime {
        h2: lanes,
        h2_seed: seed,
        h1: EndpointClients::build(
            |builder| builder.http1_only(),
            INFERENCE_MAX_CONCURRENT_REQUESTS,
        )?,
        transport: RwLock::new(None),
        h2_gate: Arc::new(tokio::sync::Semaphore::new(
            INFERENCE_MAX_CONCURRENT_REQUESTS,
        )),
        h2_gate_state: std::sync::Mutex::new(GateState {
            target: INFERENCE_MAX_CONCURRENT_REQUESTS,
            pending_shrink: 0,
        }),
        h1_gate: Arc::new(tokio::sync::Semaphore::new(
            INFERENCE_MAX_CONCURRENT_REQUESTS,
        )),
    });
    guard.insert(base_url.to_string(), Arc::clone(&runtime));
    Ok(runtime)
}

/// Every inference endpoint this process holds a client for, for `/health`.
/// Read off the shared registry, so it covers the job pool, the PQL path and
/// the preload loop alike; empty on a node that only *serves* inference. Each
/// endpoint's transport is read with `try_read`, so a health probe never
/// waits on an in-flight transport probe.
pub(crate) fn endpoint_health() -> Vec<InferenceTransportHealth> {
    let Some(registry) = ENDPOINTS.get() else {
        return Vec::new();
    };
    let guard = registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut endpoints: Vec<InferenceTransportHealth> = guard
        .iter()
        .map(|(base_url, runtime)| runtime.health(base_url))
        .collect();
    endpoints.sort_by(|a, b| a.base_url.cmp(&b.base_url));
    endpoints
}

#[derive(Debug, Clone)]
pub(crate) struct InferenceApiClient {
    base_url: String,
    endpoint: Arc<EndpointRuntime>,
    cache_metadata: bool,
}

#[derive(Debug, Clone)]
struct CachedMetadata {
    value: Value,
    fetched_at: Instant,
}

static METADATA_CACHE: OnceLock<RwLock<HashMap<String, CachedMetadata>>> = OnceLock::new();
const METADATA_CACHE_TTL: Duration = Duration::from_secs(300);
const PREDICT_MAX_RETRIES: u32 = 3;
const PREDICT_MIN_DELAY: Duration = Duration::from_secs(1);
const PREDICT_MAX_DELAY: Duration = Duration::from_secs(5);

impl InferenceApiClient {
    pub fn new_with_metadata_cache(
        base_url: impl Into<String>,
        cache_metadata: bool,
    ) -> Result<Self> {
        let base_url = normalize_base_url(base_url.into());
        let endpoint = endpoint_runtime(&base_url)?;
        Ok(Self {
            base_url,
            endpoint,
            cache_metadata,
        })
    }

    /// The transport in use, probing once if it is not known yet.
    ///
    /// **A downgrade is only ever recorded on positive evidence**: a wrong
    /// memo costs the endpoint its multiplexing for the life of the process.
    /// A failed probe alone is not evidence — the ambiguous class
    /// ([`Self::could_be_an_http2_refusal`]) is resolved by repeating the h2
    /// probe and then requiring the peer to answer over HTTP/1.1.
    async fn transport(&self) -> Transport {
        if let Some(transport) = *self.endpoint.transport.read().await {
            return transport;
        }
        let transport = match self.probe_h2c().await {
            Ok(()) => Transport::H2c,
            Err(err) if !Self::could_be_an_http2_refusal(&err) => {
                // Unreachable, not un-multiplexed: nothing is remembered, so
                // the next call probes again. This attempt uses HTTP/1.1,
                // which an h2c server also serves.
                warn!(
                    endpoint = %self.base_url,
                    error = %err,
                    "could not reach the inference endpoint to establish which \
                     HTTP version it speaks; not recording a fallback"
                );
                return Transport::Http11;
            }
            Err(first) => match self.probe_h2c().await {
                // The first failure was the blip, not the peer.
                Ok(()) => Transport::H2c,
                Err(second) if self.peer_answers_http11().await => {
                    warn!(
                        endpoint = %self.base_url,
                        error = %second,
                        first_error = %first,
                        "the inference endpoint answers HTTP/1.1 but not HTTP/2 \
                         cleartext; falling back to HTTP/1.1 for this endpoint"
                    );
                    Transport::Http11
                }
                Err(second) => {
                    warn!(
                        endpoint = %self.base_url,
                        error = %second,
                        first_error = %first,
                        "the inference endpoint answered neither HTTP/2 cleartext \
                         nor HTTP/1.1; not recording a fallback"
                    );
                    return Transport::Http11;
                }
            },
        };
        // Last writer wins, and both writers agree: two concurrent probes
        // reach the same peer.
        *self.endpoint.transport.write().await = Some(transport);
        if transport == Transport::H2c {
            // Every figure here is one this client can actually deliver.
            tracing::debug!(
                endpoint = %self.base_url,
                connection_lanes = INFERENCE_CONNECTION_LANES,
                streams_per_lane = H2_STREAMS_PER_CONNECTION,
                max_concurrent = INFERENCE_MAX_CONCURRENT_REQUESTS,
                max_concurrent_ceiling = INFERENCE_MAX_CONCURRENT_STREAMS,
                "multiplexing inference requests over HTTP/2 cleartext"
            );
        }
        transport
    }

    /// One h2c probe: `GET /cache`, sent with prior knowledge. The body is
    /// never read — any status is already proof that the frames parsed.
    async fn probe_h2c(&self) -> reqwest::Result<()> {
        // Lane 0: the lane the first real requests will land on anyway.
        self.endpoint
            .h2_seed
            .raw
            .get(format!("{}/cache", self.base_url))
            .send()
            .await
            .map(|_| ())
    }

    /// Whether the peer answers the same request over HTTP/1.1 — the proof
    /// that it is alive, so its refusal of the h2 preface was about the
    /// protocol rather than the network. Any status counts.
    async fn peer_answers_http11(&self) -> bool {
        self.endpoint
            .h1
            .raw
            .get(format!("{}/cache", self.base_url))
            .send()
            .await
            .is_ok()
    }

    /// Whether a failed probe *could* be the peer refusing HTTP/2 rather than
    /// the peer being unreachable. An HTTP/1.1-only server rejects the h2
    /// preface after connecting, so the failure is neither `is_connect` nor
    /// `is_timeout`; those are network facts, never protocol facts. Only
    /// "could" — a true answer starts [`Self::transport`]'s decision.
    fn could_be_an_http2_refusal(err: &reqwest::Error) -> bool {
        !err.is_connect() && !err.is_timeout()
    }

    /// The transport already resolved for this endpoint, without probing.
    /// `None` means nothing has talked to it yet, which callers sizing
    /// resource budgets must read as the HTTP/1.1 case.
    pub fn known_transport(&self) -> Option<Transport> {
        self.endpoint
            .transport
            .try_read()
            .ok()
            .and_then(|guard| *guard)
    }

    /// Clears the remembered transport so the next request re-probes. Called
    /// on a connection error: a server can be restarted into another build.
    async fn forget_transport(&self) {
        *self.endpoint.transport.write().await = None;
    }

    /// Every non-predict call's send result, funnelled through one place so
    /// that a transport-level failure invalidates the memo here too
    /// (`predict` does the same inline). Without it a memo can go stale
    /// *upward* forever: a job that fails at `load_model` never reaches the
    /// predict that would clear it.
    async fn checked_send(
        &self,
        result: std::result::Result<reqwest::Response, reqwest_middleware::Error>,
        context: &'static str,
    ) -> Result<reqwest::Response> {
        if let Err(reqwest_middleware::Error::Reqwest(err)) = &result
            && (err.is_connect() || err.is_request())
            // Same exception as in `predict`: only an HTTP/2 peer can refuse
            // a stream, so it is evidence *for* the memo, not against it.
            && !is_refused_stream(err)
        {
            self.forget_transport().await;
        }
        result.context(context)
    }

    /// The clients for the transport in use, plus the concurrency permit that
    /// keeps requests queueing on the pool instead of opening sockets. Taken
    /// on **both** transports: `in_flight_unit_ceiling` is evaluated once
    /// before the item loop, so HTTP/1.1 is reachable under a window already
    /// sized for multiplexing.
    async fn active(&self) -> (Transport, EndpointClients, EndpointLease) {
        let transport = self.transport().await;
        match transport {
            Transport::H2c => {
                let permit = Arc::clone(&self.endpoint.h2_gate)
                    .acquire_owned()
                    .await
                    .ok();
                // The lane is chosen *after* the permit, so the load the
                // choice is made on is the load that will actually run.
                let lane = self.endpoint.pick_lane();
                self.endpoint.h2[lane].in_flight.fetch_add(1, Relaxed);
                let clients = self.endpoint.lane_clients(lane);
                (
                    transport,
                    clients,
                    EndpointLease {
                        endpoint: Arc::clone(&self.endpoint),
                        lane: Some(lane),
                        permit,
                        multiplexed: true,
                    },
                )
            }
            Transport::Http11 => {
                let permit = Arc::clone(&self.endpoint.h1_gate)
                    .acquire_owned()
                    .await
                    .ok();
                (
                    transport,
                    self.endpoint.h1.clone(),
                    EndpointLease {
                        endpoint: Arc::clone(&self.endpoint),
                        lane: None,
                        permit,
                        multiplexed: false,
                    },
                )
            }
        }
    }

    /// Apply a desired-in-flight figure this endpoint published. h2c only —
    /// see [`EndpointRuntime::set_in_flight_target`].
    pub fn observe_desired_in_flight(&self, items: u64) {
        self.endpoint.set_in_flight_target(items);
    }

    pub fn from_settings_with_metadata_cache(
        settings: &Settings,
        cache_metadata: bool,
    ) -> Result<Self> {
        let inference = settings
            .upstreams
            .inference
            .first()
            .context("inference upstream missing from settings")?;
        Self::new_with_metadata_cache(inference.base_url.clone(), cache_metadata)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn predict(
        &self,
        inference_id: &str,
        cache_key: &str,
        lru_size: i64,
        ttl_seconds: i64,
        max_batch: Option<u32>,
        prewarm: Option<bool>,
        inputs: &[InferenceInput],
    ) -> Result<PredictResponse> {
        let url = format!("{}/predict/{}", self.base_url, inference_id);
        let mut query: Vec<(&str, String)> = vec![
            ("cache_key", cache_key.to_string()),
            ("lru_size", lru_size.to_string()),
            ("ttl_seconds", ttl_seconds.to_string()),
        ];
        // Per-request cap on server-side batch merging (design doc §6), sent
        // only when the caller has an opinion; older servers ignore it.
        if let Some(max_batch) = max_batch {
            query.push(("max_batch", max_batch.to_string()));
        }
        // Lazy prewarm hint (design doc §8): absent = true on the server, so
        // only callers with an opinion (extraction jobs: false) send it.
        if let Some(prewarm) = prewarm {
            query.push(("prewarm", prewarm.to_string()));
        }
        let mut attempts: u32 = 0;
        loop {
            let form = build_predict_form(inputs).await?;
            // Resolved per attempt and held for exactly the request: every
            // `continue` below drops the lease *before* waiting out its
            // backoff, so a retry never holds a concurrency slot while idle.
            let (_transport, clients, lease) = self.active().await;
            let response = clients
                .raw
                .post(&url)
                .query(&query)
                .multipart(form)
                .send()
                .await;

            match response {
                Ok(response) => {
                    if response.status().is_success() {
                        let content_type = response
                            .headers()
                            .get(CONTENT_TYPE)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or("")
                            .to_string();
                        // Absent or unparsable leaves the caller on its own
                        // floor. Read before the body consumes the response.
                        let desired = response
                            .headers()
                            .get(DESIRED_IN_FLIGHT_HEADER)
                            .and_then(|value| value.to_str().ok())
                            .and_then(|value| value.trim().parse::<u64>().ok())
                            .filter(|value| *value > 0);
                        let body = match response.bytes().await {
                            Ok(body) => body.to_vec(),
                            // The head arrived, so the server answered and
                            // this end lost the answer. Typed for the job to
                            // re-submit; not retried here, because recovering
                            // a lost answer is the job's per-item budget to
                            // spend, not this loop's per-request one.
                            Err(err) => {
                                let failure =
                                    InferenceFailure::from_transport(TransportPhase::Body, &err);
                                warn!(
                                    %url,
                                    phase = TransportPhase::Body.as_str(),
                                    class = reqwest_error_class(&err),
                                    error = %error_chain(&err),
                                    "inference predict answered and the answer was lost in \
                                     transit; its items have no verdict"
                                );
                                return Err(anyhow::Error::new(err))
                                    .context(failure)
                                    .context("inference predict response body failed");
                            }
                        };
                        let mut parsed = parse_predict_response(&content_type, &body)?;
                        parsed.desired_in_flight_items = desired;
                        return Ok(parsed);
                    }

                    let status = response.status();
                    let retry_after = retry_after_secs(response.headers());
                    // Read before deciding: the body says whether a 503 is
                    // transient or the cooldown the caller must see.
                    let body = response.text().await.unwrap_or_default();
                    let failure = InferenceFailure::parse(status, retry_after, &body);
                    if failure.is_load_cooldown() {
                        warn!(
                            %url,
                            %status,
                            model = failure.model.as_deref().unwrap_or("?"),
                            retry_at = failure.retry_at.as_deref().unwrap_or("?"),
                            "inference predict refused: the model is in its load-failure cooldown"
                        );
                        return Err(anyhow::Error::new(failure));
                    }
                    if should_retry_status(status)
                        && let Some(delay) = next_retry_delay(attempts)
                    {
                        attempts += 1;
                        drop(lease);
                        tokio::time::sleep(delay).await;
                        continue;
                    }

                    warn!(%url, %status, %body, "inference predict failed");
                    return Err(anyhow::Error::new(failure));
                }
                Err(err) => {
                    // A transport failure invalidates what the probe learned.
                    // A refused stream is the exception and the memo is
                    // *kept*: only an h2 peer can send RST_STREAM, so it is
                    // positive proof, and forgetting it would make a peer
                    // with a small stream limit re-probe on every burst.
                    if !is_refused_stream(&err) && (err.is_connect() || err.is_request()) {
                        self.forget_transport().await;
                    }
                    if should_retry_error(&err)
                        && let Some(delay) = next_retry_delay(attempts)
                    {
                        attempts += 1;
                        drop(lease);
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    // Out of retries. Typed by *where* the request stopped:
                    // untyped, the job could only record the items as failed.
                    let phase = send_phase(&err);
                    let failure = InferenceFailure::from_transport(phase, &err);
                    warn!(
                        %url,
                        phase = phase.as_str(),
                        class = reqwest_error_class(&err),
                        attempts,
                        error = %error_chain(&err),
                        "inference predict transport failure; its items have no verdict"
                    );
                    // Context over the `reqwest` error rather than replacing
                    // it, so `downcast_ref` finds it and the chain survives.
                    return Err(anyhow::Error::new(err))
                        .context(failure)
                        .context("inference predict request failed");
                }
            }
        }
    }

    pub async fn load_model(
        &self,
        inference_id: &str,
        cache_key: &str,
        lru_size: i64,
        ttl_seconds: i64,
        prewarm: Option<bool>,
    ) -> Result<Value> {
        let url = format!("{}/load/{}", self.base_url, inference_id);
        let mut query: Vec<(&str, String)> = vec![
            ("cache_key", cache_key.to_string()),
            ("lru_size", lru_size.to_string()),
            ("ttl_seconds", ttl_seconds.to_string()),
        ];
        // Lazy prewarm hint, as on predict.
        if let Some(prewarm) = prewarm {
            query.push(("prewarm", prewarm.to_string()));
        }
        let (_transport, clients, _slot) = self.active().await;
        let response = self
            .checked_send(
                clients.middleware.put(url).query(&query).send().await,
                "inference load request failed",
            )
            .await?;
        parse_json_response(response).await
    }

    pub async fn unload_model(&self, inference_id: &str, cache_key: &str) -> Result<Value> {
        let url = format!("{}/cache/{}/{}", self.base_url, cache_key, inference_id);
        let (_transport, clients, _slot) = self.active().await;
        let response = self
            .checked_send(
                clients.middleware.delete(url).send().await,
                "inference unload request failed",
            )
            .await?;
        parse_json_response(response).await
    }

    pub async fn clear_cache(&self, cache_key: &str) -> Result<Value> {
        let url = format!("{}/cache/{}", self.base_url, cache_key);
        let (_transport, clients, _slot) = self.active().await;
        let response = self
            .checked_send(
                clients.middleware.delete(url).send().await,
                "inference clear cache request failed",
            )
            .await?;
        parse_json_response(response).await
    }

    // Only exercised by the inferio HTTP tests; mirrors the Python client API.
    #[allow(dead_code)]
    pub async fn get_cached_models(&self) -> Result<Value> {
        let url = format!("{}/cache", self.base_url);
        let (_transport, clients, _slot) = self.active().await;
        let response = self
            .checked_send(
                clients.middleware.get(url).send().await,
                "inference cache list request failed",
            )
            .await?;
        parse_json_response(response).await
    }

    pub async fn get_metadata(&self) -> Result<Value> {
        if !self.cache_metadata {
            return self.fetch_metadata().await;
        }
        let cache = METADATA_CACHE.get_or_init(|| RwLock::new(HashMap::new()));
        {
            let guard = cache.read().await;
            if let Some(entry) = guard.get(&self.base_url)
                && entry.fetched_at.elapsed() < METADATA_CACHE_TTL
            {
                return Ok(entry.value.clone());
            }
        }

        let value = self.fetch_metadata().await?;
        let mut guard = cache.write().await;
        guard.insert(
            self.base_url.clone(),
            CachedMetadata {
                value: value.clone(),
                fetched_at: Instant::now(),
            },
        );
        Ok(value)
    }

    async fn fetch_metadata(&self) -> Result<Value> {
        let url = format!("{}/metadata", self.base_url);
        let (_transport, clients, _slot) = self.active().await;
        let response = self
            .checked_send(
                clients.middleware.get(url).send().await,
                "inference metadata request failed",
            )
            .await?;
        parse_json_response(response).await
    }

    pub async fn get_external_inputs(&self) -> Result<Value> {
        let url = format!("{}/external-inputs", self.base_url);
        let (_transport, clients, _slot) = self.active().await;
        let response = self
            .checked_send(
                clients.middleware.get(url).send().await,
                "inference external-input request failed",
            )
            .await?;
        parse_json_response(response).await
    }

    /// Fetch external inputs when the upstream implements the additive
    /// endpoint. Only a genuine 404 means an older server; availability,
    /// authorization and decoding failures remain errors.
    pub async fn get_external_inputs_optional(&self) -> Result<Option<Value>> {
        let url = format!("{}/external-inputs", self.base_url);
        let (_transport, clients, _slot) = self.active().await;
        let response = self
            .checked_send(
                clients.middleware.get(url).send().await,
                "inference external-input request failed",
            )
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        parse_json_response(response).await.map(Some)
    }
}

async fn file_to_part(idx: usize, file: &InferenceFile) -> Result<Part> {
    let name = idx.to_string();
    let part = match file {
        InferenceFile::Path(path) => {
            let bytes = tokio::fs::read(path)
                .await
                .with_context(|| format!("failed to read file {}", path.display()))?;
            Part::bytes(bytes)
        }
        InferenceFile::Bytes(bytes) => Part::bytes(bytes.clone()),
    };
    Ok(part.file_name(name).mime_str("application/octet-stream")?)
}

async fn build_predict_form(inputs: &[InferenceInput]) -> Result<Form> {
    let payload = json!({
        "inputs": inputs.iter().map(|item| item.data.clone()).collect::<Vec<_>>(),
    });
    let mut form = Form::new().text("data", serde_json::to_string(&payload)?);
    for (idx, input) in inputs.iter().enumerate() {
        if let Some(file) = &input.file {
            let part = file_to_part(idx, file).await?;
            form = form.part("files", part);
        }
    }
    Ok(form)
}

fn should_retry_status(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 429 | 502 | 503 | 504)
}

fn should_retry_error(err: &reqwest::Error) -> bool {
    err.is_connect() || err.is_timeout() || is_refused_stream(err)
}

/// The phase a failed `send()` reached. `send()` resolves when the response
/// head arrives, so every error it reports happened before that. `reqwest`'s
/// predicates are not disjoint, so the order below is the claim.
fn send_phase(err: &reqwest::Error) -> TransportPhase {
    if err.is_connect() {
        TransportPhase::Connect
    } else if err.is_timeout() {
        // The request went out and nothing came back inside the deadline.
        TransportPhase::Headers
    } else {
        // `Kind::Request` and the rest: a reset, a refused stream, a
        // `GOAWAY`, an unwritable body. A connection, and no answer.
        TransportPhase::Send
    }
}

/// `reqwest`'s own name for an error, for the log line and the audit.
/// Ordered so the most specific true claim wins.
fn reqwest_error_class(err: &reqwest::Error) -> &'static str {
    if is_refused_stream(err) {
        "refused_stream"
    } else if err.is_connect() {
        "connect"
    } else if err.is_timeout() {
        "timeout"
    } else if err.is_decode() {
        "decode"
    } else if err.is_body() {
        "body"
    } else if err.is_request() {
        "request"
    } else if err.is_redirect() {
        "redirect"
    } else if err.is_builder() {
        "builder"
    } else if err.is_status() {
        "status"
    } else {
        "unknown"
    }
}

/// The whole source chain, joined. See [`InferenceFailure::from_transport`]
/// for why the top-level `Display` alone is not enough to act on.
fn error_chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut rendered = err.to_string();
    let mut source = err.source();
    while let Some(current) = source {
        rendered.push_str(" <- ");
        rendered.push_str(&current.to_string());
        source = current.source();
    }
    rendered
}

/// Whether the peer refused to *open* the stream — HTTP/2 `REFUSED_STREAM`,
/// which RFC 9113 §8.7 defines as "not processed", so it is safe to retry.
/// Reachable in ordinary operation, not only under abuse.
fn is_refused_stream(err: &reqwest::Error) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(current) = source {
        if let Some(h2) = current.downcast_ref::<h2::Error>()
            && h2.reason() == Some(h2::Reason::REFUSED_STREAM)
        {
            return true;
        }
        source = current.source();
    }
    false
}

fn next_retry_delay(attempts: u32) -> Option<std::time::Duration> {
    if attempts >= PREDICT_MAX_RETRIES {
        return None;
    }
    let multiplier = 1u64 << attempts;
    let min_ms = PREDICT_MIN_DELAY.as_millis() as u64;
    let max_ms = PREDICT_MAX_DELAY.as_millis() as u64;
    let delay_ms = min_ms.saturating_mul(multiplier).min(max_ms);
    Some(Duration::from_millis(delay_ms))
}

fn normalize_base_url(raw: String) -> String {
    let trimmed = raw.trim_end_matches('/');
    if trimmed.ends_with("/api/inference") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/api/inference")
    }
}

/// Parse a predict response body. Also the parity oracle for the local
/// orchestrator's HTTP tests: whatever `inferio::http` encodes must parse
/// here. Only the JSON envelope can carry a typed error slot.
pub(crate) fn parse_predict_response(content_type: &str, body: &[u8]) -> Result<PredictResponse> {
    if content_type.contains("application/json") {
        let value: Value = serde_json::from_slice(body)?;
        let outputs = value
            .get("outputs")
            .and_then(|item| item.as_array())
            .context("predict response missing outputs array")?;
        return parse_json_outputs(outputs);
    }

    if content_type.contains("multipart/mixed") {
        let boundary =
            extract_boundary(content_type).context("multipart response missing boundary")?;
        let outputs = parse_multipart_outputs(body, &boundary)?;
        return Ok(PredictResponse::plain(PredictOutput::Binary(outputs)));
    }

    if content_type.contains("application/octet-stream") {
        return Ok(PredictResponse::plain(PredictOutput::Binary(vec![
            body.to_vec(),
        ])));
    }

    bail!("unexpected inference response content type: {content_type}");
}

/// Splits a JSON `outputs` array into surviving payloads and typed slot
/// errors. Base64 unwrapping only fires when the batch carried an error slot,
/// so every response an older server can produce passes through unchanged.
/// `PredictOutput` is one type for the whole response, so a batch mixing
/// binary and JSON survivors is reported rather than silently dropped.
fn parse_json_outputs(outputs: &[Value]) -> Result<PredictResponse> {
    let mut errors = Vec::new();
    let mut survivors: Vec<&Value> = Vec::with_capacity(outputs.len());
    for (index, value) in outputs.iter().enumerate() {
        match slot_error_from_json(value) {
            Some(Ok(error)) => errors.push(PredictSlotError {
                index,
                class: error.class,
                message: error.message,
            }),
            // Typed, because it is deterministic: callers must not spend an
            // isolation pass re-asking a server that will answer identically.
            Some(Err(reason)) => {
                return Err(anyhow::Error::new(ProtocolViolation::new(format!(
                    "predict output {index} is a malformed error slot: {reason}"
                ))));
            }
            None => survivors.push(value),
        }
    }
    if errors.is_empty() {
        return Ok(PredictResponse::plain(PredictOutput::Json(
            survivors.into_iter().cloned().collect(),
        )));
    }
    let wrapped = survivors.iter().filter(|v| is_base64_wrapper(v)).count();
    if wrapped == 0 {
        return Ok(PredictResponse {
            outputs: PredictOutput::Json(survivors.into_iter().cloned().collect()),
            errors,
            desired_in_flight_items: None,
        });
    }
    if wrapped != survivors.len() {
        return Err(anyhow::Error::new(ProtocolViolation::new(format!(
            "predict response mixes {wrapped} binary and {} JSON outputs, \
             which have no common representation",
            survivors.len() - wrapped
        ))));
    }
    let mut decoded = Vec::with_capacity(survivors.len());
    for value in survivors {
        decoded.push(decode_base64_wrapper(value)?);
    }
    Ok(PredictResponse {
        outputs: PredictOutput::Binary(decoded),
        errors,
        desired_in_flight_items: None,
    })
}

fn is_base64_wrapper(value: &Value) -> bool {
    value
        .get("__type__")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "base64")
}

fn decode_base64_wrapper(value: &Value) -> Result<Vec<u8>> {
    use base64::Engine as _;
    let content = value
        .get("content")
        .and_then(Value::as_str)
        .context("base64 output missing content")?;
    base64::engine::general_purpose::STANDARD
        .decode(content.as_bytes())
        .context("invalid base64 output")
}

async fn parse_json_response(response: reqwest::Response) -> Result<Value> {
    if response.status().is_success() {
        return response
            .json::<Value>()
            .await
            .context("decode inference response");
    }
    let status = response.status();
    let retry_after = retry_after_secs(response.headers());
    let body = response.text().await.unwrap_or_default();
    // Typed here too: a load that hits the per-model cooldown must reach the
    // job with its kind intact, exactly like a predict does.
    Err(anyhow::Error::new(InferenceFailure::parse(
        status,
        retry_after,
        &body,
    )))
}

fn extract_boundary(content_type: &str) -> Option<String> {
    content_type.split(';').find_map(|segment| {
        let segment = segment.trim();
        segment
            .strip_prefix("boundary=")
            .map(|value| value.trim_matches('"').to_string())
    })
}

fn parse_multipart_outputs(body: &[u8], boundary: &str) -> Result<Vec<Vec<u8>>> {
    let marker = format!("--{boundary}");
    let mut outputs = Vec::new();

    for part in split_by_boundary(body, marker.as_bytes()) {
        if part.is_empty() || part == b"--\r\n" || part == b"--" {
            continue;
        }
        let Some((headers, content)) = split_headers(part) else {
            continue;
        };
        let Some(filename) = extract_filename(headers) else {
            continue;
        };
        let index = filename
            .trim_start_matches("output")
            .trim_end_matches(".bin")
            .parse::<usize>()
            .ok();
        let mut data = content.to_vec();
        while data.ends_with(b"\r\n") {
            data.truncate(data.len().saturating_sub(2));
        }
        match index {
            Some(idx) => {
                if outputs.len() <= idx {
                    outputs.resize(idx + 1, Vec::new());
                }
                outputs[idx] = data;
            }
            None => outputs.push(data),
        }
    }

    Ok(outputs)
}

fn split_by_boundary<'a>(body: &'a [u8], marker: &[u8]) -> Vec<&'a [u8]> {
    if marker.is_empty() {
        return vec![body];
    }
    let mut parts = Vec::new();
    let mut cursor = 0;
    while let Some(pos) = find_subslice(&body[cursor..], marker) {
        let end = cursor + pos;
        parts.push(&body[cursor..end]);
        cursor = end + marker.len();
    }
    parts.push(&body[cursor..]);
    parts
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn split_headers(part: &[u8]) -> Option<(&[u8], &[u8])> {
    let separator = b"\r\n\r\n";
    part.windows(separator.len())
        .position(|window| window == separator)
        .map(|idx| (&part[..idx], &part[idx + separator.len()..]))
}

fn extract_filename(headers: &[u8]) -> Option<String> {
    let header_str = std::str::from_utf8(headers).ok()?;
    for line in header_str.lines() {
        let line = line.trim();
        if !line.to_ascii_lowercase().starts_with("content-disposition") {
            continue;
        }
        for segment in line.split(';') {
            let segment = segment.trim();
            if let Some(value) = segment.strip_prefix("filename=") {
                return Some(value.trim_matches('"').to_string());
            }
        }
    }
    None
}

#[allow(dead_code)]
fn file_input_from_path(path: impl AsRef<Path>) -> InferenceFile {
    InferenceFile::Path(path.as_ref().to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::RawQuery;
    use axum::http::StatusCode;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex as StdMutex};

    fn text_input(text: &str) -> InferenceInput {
        InferenceInput::new(serde_json::json!({"text": text}), None)
    }

    /// Descriptors this process holds, the accepted server ends included:
    /// the stub runs in this process exactly as local inference does.
    #[cfg(target_os = "linux")]
    fn open_fds() -> usize {
        std::fs::read_dir("/proc/self/fd")
            .expect("/proc/self/fd is readable on Linux")
            .count()
    }

    /// Concurrency measured at the server's own handler: how many predicts
    /// are inside it at once, over how many TCP connections.
    struct ConcurrencyProbe {
        in_flight: std::sync::atomic::AtomicUsize,
        peak: std::sync::atomic::AtomicUsize,
        peers: StdMutex<std::collections::HashSet<SocketAddr>>,
        gate: tokio::sync::watch::Sender<bool>,
    }

    impl ConcurrencyProbe {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                in_flight: std::sync::atomic::AtomicUsize::new(0),
                peak: std::sync::atomic::AtomicUsize::new(0),
                peers: StdMutex::new(std::collections::HashSet::new()),
                gate: tokio::sync::watch::channel(false).0,
            })
        }

        fn release(&self, open: bool) {
            self.gate.send_replace(open);
        }

        fn peak(&self) -> usize {
            self.peak.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn sockets(&self) -> usize {
            self.peers.lock().expect("probe mutex").len()
        }
    }

    /// A stub inference endpoint whose predict handler *blocks* until the
    /// test releases it, served through the gateway's own serve loop and
    /// advertising `max_streams`. Every predict is counted and its peer
    /// recorded, so concurrency and sockets are measured.
    async fn spawn_blocking_stub(probe: Arc<ConcurrencyProbe>, max_streams: u32) -> String {
        use std::sync::atomic::Ordering::SeqCst;

        let handler_probe = Arc::clone(&probe);
        let app = Router::new()
            .route(
                "/api/inference/cache",
                get(|| async { Json(serde_json::json!({"cache": {}})) }),
            )
            .route(
                "/api/inference/predict/{group}/{id}",
                post(
                    move |axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<
                        SocketAddr,
                    >| {
                        let probe = Arc::clone(&handler_probe);
                        async move {
                            probe.peers.lock().expect("probe mutex").insert(peer);
                            let now = probe.in_flight.fetch_add(1, SeqCst) + 1;
                            probe.peak.fetch_max(now, SeqCst);
                            let mut released = probe.gate.subscribe();
                            let _ = released.wait_for(|open| *open).await;
                            probe.in_flight.fetch_sub(1, SeqCst);
                            Json(serde_json::json!({"outputs": [{"ok": true}]}))
                        }
                    },
                ),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            // The product's own serve loop, so the stream limit under test is
            // advertised exactly the way the gateway advertises its own.
            crate::serve_with_streams(listener, app, std::future::pending(), max_streams)
                .await
                .unwrap();
        });
        format!("http://{addr}")
    }

    /// What actually bounds concurrent predicts, measured at both ends and in
    /// the descriptor table. Two peers: one at this binary's own stream limit,
    /// and one advertising far less than a lane is offered — the client cannot
    /// read a peer's limit, so the requirement there is not "match it" but
    /// "survive it".
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_server_and_the_pool_bound_concurrent_predicts() {
        // Above every bound in play (the client gate, the server's stream
        // limit, the pool), so the smallest is what the handler sees.
        const OFFERED: usize = 400;
        /// Far below `H2_STREAMS_PER_CONNECTION`: every lane is over-offered.
        const STINGY_PEER_STREAMS: u32 = 16;

        // The server's stream limit must not be the tightest bound on our
        // own client's concurrency.
        assert!(crate::MAX_CONCURRENT_STREAMS as usize > INFERENCE_MAX_CONCURRENT_REQUESTS);

        for (max_streams, label) in [
            (crate::MAX_CONCURRENT_STREAMS, "a peer at our own limit"),
            (STINGY_PEER_STREAMS, "a peer with a small stream limit"),
        ] {
            let probe = ConcurrencyProbe::new();
            let base_url = spawn_blocking_stub(Arc::clone(&probe), max_streams).await;
            let client = InferenceApiClient::new_with_metadata_cache(base_url, false).unwrap();
            // One round trip first, so first-contact allocations are paid.
            probe.release(true);
            client
                .predict("g/model", "k", 1, 60, None, None, &[text_input("warm")])
                .await
                .expect("the stub answers");
            probe.release(false);
            assert_eq!(
                client.known_transport(),
                Some(Transport::H2c),
                "{label}: h2c with prior knowledge"
            );
            #[cfg(target_os = "linux")]
            let baseline = open_fds();

            let mut inflight = tokio::task::JoinSet::new();
            for _ in 0..OFFERED {
                let client = client.clone();
                // For the stingy peer this is the whole point: a stream limit
                // below what we offer is a *wait*, not an error.
                inflight.spawn(async move {
                    client
                        .predict("g/model", "k", 1, 60, None, None, &[text_input("x")])
                        .await
                        .expect("the stub answers");
                });
            }
            // Sample until the peak stops moving for a second, with a hard
            // deadline so a regression cannot hang the suite.
            #[cfg(target_os = "linux")]
            let mut peak_fds = baseline;
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            let (mut last, mut stable) = (usize::MAX, 0);
            while stable < 10 && std::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(100)).await;
                #[cfg(target_os = "linux")]
                {
                    peak_fds = peak_fds.max(open_fds());
                }
                let peak = probe.peak();
                stable = if peak == last && peak > 0 {
                    stable + 1
                } else {
                    0
                };
                last = peak;
            }

            let (peak, sockets) = (probe.peak(), probe.sockets());
            probe.release(true);
            let mut answered = 0usize;
            while let Some(result) = inflight.join_next().await {
                result.expect("no panic");
                answered += 1;
            }
            assert_eq!(answered, OFFERED, "{label}: every predict must complete");
            assert!(
                sockets <= INFERENCE_CONNECTION_LANES,
                "{label}: {sockets} connections exceeds the lane count"
            );
            if max_streams == STINGY_PEER_STREAMS {
                assert!(
                    peak <= STINGY_PEER_STREAMS as usize * INFERENCE_CONNECTION_LANES,
                    "{label}: its own limit bounds its handler, not {peak}"
                );
            } else {
                // The client's own gate, not the transport's silent default
                // and not what was offered; and because lanes are recruited by
                // load rather than spread across, the connection count is the
                // concurrency over the per-lane stream budget.
                assert_eq!(peak, INFERENCE_MAX_CONCURRENT_REQUESTS, "{label}");
                assert_eq!(
                    sockets,
                    peak.div_ceil(H2_STREAMS_PER_CONNECTION),
                    "{label}: {peak} requests over {sockets} lanes"
                );
            }
            // The descriptor bound `in_flight_unit_ceiling` relies on: both
            // ends of every lane plus slack, whatever the window's width.
            #[cfg(target_os = "linux")]
            {
                let growth = peak_fds.saturating_sub(baseline);
                let bound = 2 * INFERENCE_CONNECTION_LANES + 8;
                assert!(growth <= bound, "{label}: {growth} fds past {bound}");
                assert!(growth < OFFERED, "{label}: {growth} fds for {OFFERED}");
            }
        }
    }

    /// A raw TCP peer that is not an HTTP server. `Http11` answers a fixed
    /// HTTP/1.1 response, which is what an HTTP/1.1-only peer does to an
    /// HTTP/2 preface. `Drop` accepts and drops — the ambiguous class neither
    /// `is_connect` nor `is_timeout` catches. `Silent` holds the connection
    /// open and says nothing; its accepted halves are kept alive on purpose,
    /// since dropping them would make it `Drop`.
    enum RawPeer {
        Http11,
        Drop,
        Silent,
    }

    async fn spawn_raw_peer(kind: RawPeer) -> SocketAddr {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((mut socket, _)) = listener.accept().await {
                match kind {
                    RawPeer::Drop => drop(socket),
                    RawPeer::Silent => held.push(socket),
                    RawPeer::Http11 => {
                        let mut scratch = [0u8; 4096];
                        let _ = socket.read(&mut scratch).await;
                        let body = br#"{"cache":{}}"#;
                        let head = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                             content-length: {}\r\nconnection: close\r\n\r\n",
                            body.len()
                        );
                        let _ = socket.write_all(head.as_bytes()).await;
                        let _ = socket.write_all(body).await;
                        let _ = socket.shutdown().await;
                    }
                }
            }
        });
        addr
    }

    /// A port nothing in this process can be listening on. Binding and
    /// dropping an *ephemeral* port is not sound: it goes straight back to
    /// the pool this binary's other tests bind from, so the "closed" port is
    /// occasionally a neighbour's stub. Port 1 is below `ip_local_port_range`
    /// and cannot be bound without privileges.
    async fn closed_port() -> SocketAddr {
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        assert!(
            tokio::net::TcpStream::connect(addr).await.is_err(),
            "premise: {addr} refuses connections; something here is listening"
        );
        addr
    }

    /// The fallback: a server that does not speak h2c is detected once,
    /// remembered, and served over HTTP/1.1 — with the request that
    /// discovered it still succeeding.
    #[tokio::test]
    async fn an_http1_only_endpoint_falls_back_and_stays_usable() {
        let addr = spawn_raw_peer(RawPeer::Http11).await;
        let client =
            InferenceApiClient::new_with_metadata_cache(format!("http://{addr}"), false).unwrap();
        let none = client.known_transport();
        assert_eq!(none, None, "nothing is assumed before the first request");
        let cached = client.get_cached_models().await.expect("HTTP/1.1 answers");
        assert_eq!(cached, serde_json::json!({"cache": {}}));
        // The fallback is remembered, not re-probed per request: a second
        // call answers without another probe.
        assert_eq!(client.known_transport(), Some(Transport::Http11));
        assert!(client.get_cached_models().await.is_ok());
        assert_eq!(client.known_transport(), Some(Transport::Http11));
    }

    /// The gate admits at most [`INFERENCE_MAX_CONCURRENT_REQUESTS`] requests
    /// in **both** transports, asserted on the permit itself.
    #[tokio::test]
    async fn both_transports_take_a_concurrency_permit() {
        for transport in [Transport::H2c, Transport::Http11] {
            let client = InferenceApiClient::new_with_metadata_cache(
                format!("http://gate-test-{transport:?}"),
                false,
            )
            .unwrap();
            // Pinned rather than probed: nothing listens on that name.
            let runtime = Arc::clone(&client.endpoint);
            *runtime.transport.write().await = Some(transport);

            let gate = match transport {
                Transport::H2c => Arc::clone(&runtime.h2_gate),
                Transport::Http11 => Arc::clone(&runtime.h1_gate),
            };
            let before = gate.available_permits();
            assert_eq!(before, INFERENCE_MAX_CONCURRENT_REQUESTS);
            let (resolved, _clients, lease) = client.active().await;
            assert_eq!(resolved, transport);
            assert!(
                lease.permit.is_some(),
                "{transport:?} goes through the gate"
            );
            assert_eq!(
                gate.available_permits(),
                before - 1,
                "{transport:?} holds a permit while in flight"
            );
            // The lane is claimed on the multiplexed path only, and both are
            // returned with the lease.
            assert_eq!(lease.lane.is_some(), transport.is_multiplexed());
            drop(lease);
            assert_eq!(gate.available_permits(), before);
            assert!(runtime.h2.iter().all(|l| l.in_flight.load(Relaxed) == 0));
        }
    }

    /// The h2c gate follows the endpoint's published figure between its floor
    /// and its ceiling, the HTTP/1.1 gate never moves, and a shrink lands even
    /// while every permit is out: `forget_permits` can only take what is
    /// available, so the deficit is repaid on the release path.
    #[tokio::test]
    async fn a_gate_shrink_lands_through_releases_not_only_through_free_permits() {
        let client =
            InferenceApiClient::new_with_metadata_cache("http://gate-shrink-test", false).unwrap();
        let runtime = Arc::clone(&client.endpoint);
        *runtime.transport.write().await = Some(Transport::H2c);
        let permits = || runtime.h2_gate.available_permits();
        assert_eq!(permits(), INFERENCE_MAX_CONCURRENT_REQUESTS);

        // Growth up to the ceiling and no further, then back to the floor —
        // the constant every deployment already runs at, so a small published
        // figure can never throttle one.
        client.observe_desired_in_flight(1_632);
        assert_eq!(permits(), 1_632);
        client.observe_desired_in_flight(u64::MAX);
        assert_eq!(permits(), INFERENCE_MAX_CONCURRENT_STREAMS);
        client.observe_desired_in_flight(1);
        assert_eq!(permits(), INFERENCE_MAX_CONCURRENT_REQUESTS);
        assert_eq!(
            runtime.h1_gate.available_permits(),
            INFERENCE_MAX_CONCURRENT_REQUESTS,
            "HTTP/1.1 is a different semaphore and never moves"
        );

        // Saturate: every permit held by an in-flight request.
        client.observe_desired_in_flight(512);
        let mut held = Vec::new();
        for _ in 0..512 {
            let (_transport, _clients, lease) = client.active().await;
            held.push(lease);
        }
        assert_eq!(permits(), 0);

        // Shrink to the floor. Nothing is free, so the deficit is 512 - 256,
        // and `/health` still reports what is really in flight: reporting
        // `target - available` renders a saturated, shrinking endpoint idle.
        client.observe_desired_in_flight(u64::from(INFERENCE_MAX_CONCURRENT_REQUESTS as u32));
        assert_eq!(permits(), 0);
        assert_eq!(
            runtime.gate_snapshot(Some(Transport::H2c)),
            (INFERENCE_MAX_CONCURRENT_REQUESTS, 512),
            "in flight is permits in existence minus what is free"
        );

        // Every release repays the deficit before it re-issues anything, and
        // the gate then settles at exactly the new target.
        for _ in 0..256 {
            held.pop();
        }
        assert_eq!(permits(), 0, "the first 256 releases are retired");
        while held.pop().is_some() {}
        assert_eq!(permits(), INFERENCE_MAX_CONCURRENT_REQUESTS);
        assert!(runtime.h2.iter().all(|l| l.in_flight.load(Relaxed) == 0));
        assert_eq!(
            runtime.gate_snapshot(Some(Transport::H2c)),
            (INFERENCE_MAX_CONCURRENT_REQUESTS, 0),
            "the two expressions agree again once the deficit is repaid"
        );
    }

    /// A request waiting out a backoff holds neither a gate permit nor a lane
    /// claim, read off the health snapshot while it is provably mid-backoff.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_backoff_holds_no_gate_permit_and_no_lane() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::SeqCst;

        // 503 once, then answer: the first attempt ends in the retry path and
        // the client sleeps `PREDICT_MIN_DELAY` (1s).
        let attempts = Arc::new(AtomicUsize::new(0));
        let handler_attempts = Arc::clone(&attempts);
        let app = Router::new().route(
            "/api/inference/predict/{group}/{model}",
            post(move || {
                let attempts = Arc::clone(&handler_attempts);
                async move {
                    let json = [(axum::http::header::CONTENT_TYPE, "application/json")];
                    if attempts.fetch_add(1, SeqCst) == 0 {
                        return (
                            StatusCode::SERVICE_UNAVAILABLE,
                            json,
                            "{\"detail\":{\"kind\":\"body_budget_exhausted\"}}",
                        );
                    }
                    (StatusCode::OK, json, "{\"outputs\":[]}")
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            crate::serve_with_stream_limit(listener, app, std::future::pending()).await
        });

        let client = InferenceApiClient::new_with_metadata_cache(base_url, false).unwrap();
        let runtime = Arc::clone(&client.endpoint);
        let predicting = tokio::spawn(async move {
            client
                .predict("g/model", "k", 1, 60, None, None, &[text_input("x")])
                .await
        });

        // Sample inside the backoff: the first attempt has been answered and
        // the second has not been sent. The window is a whole second.
        while attempts.load(SeqCst) < 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(attempts.load(SeqCst), 1, "sampled between the two attempts");
        let (_target, in_flight) = runtime.gate_snapshot(Some(Transport::H2c));
        assert_eq!(in_flight, 0, "a waiting retry holds no gate permit");
        assert_eq!(runtime.lanes_in_use(), 0, "and no lane claim either");

        predicting
            .await
            .expect("no panic")
            .expect("the second attempt is answered");
        assert_eq!(attempts.load(SeqCst), 2);
    }

    /// Lanes are recruited by load, not spread across, and a lane's client is
    /// built when the lane is recruited rather than when the endpoint is.
    #[tokio::test]
    async fn lanes_are_recruited_by_load() {
        let client =
            InferenceApiClient::new_with_metadata_cache("http://lane-pick-test", false).unwrap();
        let runtime = Arc::clone(&client.endpoint);
        let built = || {
            runtime
                .h2
                .iter()
                .filter(|l| l.clients.get().is_some())
                .count()
        };
        let load = |lane: usize, n: usize| runtime.h2[lane].in_flight.store(n, Relaxed);

        // Registering an endpoint builds lane 0 only, and it is the probe's
        // lane too. Recruiting a lane builds exactly that lane; asking again
        // re-uses it, because a lane is one connection.
        assert_eq!(built(), 1, "not {INFERENCE_CONNECTION_LANES} clients");
        assert!(runtime.h2[0].clients.get().is_some());
        let recruited = runtime.lane_clients(7);
        assert_eq!(built(), 2);
        assert!(runtime.h2[7].clients.get().is_some());
        drop((recruited, runtime.lane_clients(7)));
        assert_eq!(built(), 2);

        let full = H2_STREAMS_PER_CONNECTION;
        for (loads, expected, label) in [
            ([0, 0], 0, "empty: one lane"),
            ([full - 1, 0], 0, "under one lane's budget: still that lane"),
            ([full, 0], 1, "full: the next lane is recruited"),
            (
                [50, 20],
                1,
                "least-loaded in the prefix, not first-with-room",
            ),
            ([full, full], 2, "a third lane only once two are full"),
        ] {
            for lane in 0..runtime.h2.len() {
                load(lane, loads.get(lane).copied().unwrap_or(0));
            }
            assert_eq!(runtime.pick_lane(), expected, "{label}");
        }
        // And it never leaves the array.
        for lane in 0..runtime.h2.len() {
            load(lane, H2_STREAMS_PER_CONNECTION);
        }
        assert!(runtime.pick_lane() < INFERENCE_CONNECTION_LANES);
    }

    /// A peer that could not be reached must not be recorded as HTTP/1.1:
    /// the memo is written once and only a *predict* failure clears it, so a
    /// blip at first contact would cost the endpoint its multiplexing for the
    /// life of the process. Both shapes: a closed port, and an accept-and-drop
    /// peer, which `reqwest` reports as it reports an h2-preface refusal.
    #[tokio::test]
    async fn an_unreachable_endpoint_is_not_remembered_as_http11() {
        let closed = closed_port().await;
        let dropping = spawn_raw_peer(RawPeer::Drop).await;
        for (addr, label) in [(closed, "a closed port"), (dropping, "a peer that drops")] {
            let client =
                InferenceApiClient::new_with_metadata_cache(format!("http://{addr}"), false)
                    .unwrap();
            let call = client.get_cached_models().await;
            assert!(call.is_err(), "{label}: nothing answers");
            let memo = client.known_transport();
            assert_eq!(memo, None, "{label}: says nothing about the protocol");
        }

        // The classifier that decides it, on the connect error directly: a
        // connect failure is a network fact, not a protocol one.
        let err = reqwest::Client::new()
            .get(format!("http://{closed}/cache"))
            .send()
            .await
            .expect_err("the port is closed");
        assert!(err.is_connect());
        assert!(!InferenceApiClient::could_be_an_http2_refusal(&err));
    }

    /// Every phase of a transport failure is classified by where the request
    /// stopped. On real `reqwest` errors from real sockets, because the
    /// classification *is* a reading of `reqwest`'s error.
    #[tokio::test]
    async fn each_phase_of_a_transport_failure_is_classified_by_where_it_stopped() {
        let closed = closed_port().await;
        for (addr, timeout, phase, class, label) in [
            (
                closed,
                None,
                TransportPhase::Connect,
                "connect",
                "nothing is listening",
            ),
            (
                spawn_raw_peer(RawPeer::Drop).await,
                None,
                TransportPhase::Send,
                "request",
                "the connection is accepted and dropped",
            ),
            (
                spawn_raw_peer(RawPeer::Silent).await,
                Some(Duration::from_millis(250)),
                TransportPhase::Headers,
                "timeout",
                "the peer holds the connection open and says nothing",
            ),
        ] {
            let mut builder = reqwest::Client::builder();
            if let Some(timeout) = timeout {
                builder = builder.timeout(timeout);
            }
            let err = builder
                .build()
                .unwrap()
                .get(format!("http://{addr}/predict"))
                .send()
                .await
                .expect_err(label);
            assert_eq!(send_phase(&err), phase, "{label}: {err}");
            assert_eq!(reqwest_error_class(&err), class, "{label}: {err}");
            let failure = InferenceFailure::from_transport(send_phase(&err), &err);
            assert_eq!(failure.status, 0, "no status without a response");
            assert_eq!(failure.kind.as_deref(), Some(TRANSPORT_KIND));
            // No response head means no verdict had been produced.
            assert!(failure.is_unattempted(), "{label}");
            assert!(failure.warrants_resubmission(), "{label}");
        }
    }

    /// A predict that never reaches its peer comes back typed, past the whole
    /// retry budget, without disturbing the transport memo.
    #[tokio::test]
    async fn a_predict_that_never_reaches_its_peer_is_typed_and_requeueable() {
        let closed = closed_port().await;
        let client =
            InferenceApiClient::new_with_metadata_cache(format!("http://{closed}"), false).unwrap();

        let started = std::time::Instant::now();
        let err = client
            .predict("g/model", "k", 1, 60, None, None, &[text_input("x")])
            .await
            .expect_err("nothing is listening");
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_secs(7),
            "the whole retry budget must be spent first (1s + 2s + 4s), took {elapsed:?}"
        );

        let failure = inference_failure(&err).expect("typed through the context chain");
        assert_eq!(failure.kind.as_deref(), Some(TRANSPORT_KIND));
        assert_eq!(failure.transport_phase(), Some(TransportPhase::Connect));
        assert!(failure.is_unattempted() && failure.warrants_resubmission());
        // The whole cause chain is kept, not just reqwest's own sentence, and
        // the human context is still the outermost layer.
        let chain = failure.last_error.as_deref().unwrap_or_default();
        assert!(chain.contains(" <- "), "{chain}");
        assert!(format!("{err:#}").contains("inference predict request failed"));
        assert_eq!(
            client.known_transport(),
            None,
            "classifying a transport failure must not disturb the memo"
        );
    }

    /// An answer lost in transit is typed too but not called unattempted: the
    /// server ran the batch, so the re-submission rests on idempotence.
    #[tokio::test]
    async fn an_answer_lost_mid_body_is_typed_by_the_phase_it_died_in() {
        let app = Router::new()
            .route(
                "/api/inference/cache",
                get(|| async { Json(serde_json::json!({"cache": {}})) }),
            )
            .route(
                "/api/inference/predict/{group}/{model}",
                post(|| async {
                    use futures_util::StreamExt;
                    // Head and a first chunk, flushed; *then* the body dies.
                    // The pause is what makes this the case under test: hyper
                    // resets the stream on a body error, and a reset that
                    // overtakes the head is a `Send`-phase failure.
                    let head = futures_util::stream::once(async {
                        Ok::<Vec<u8>, std::io::Error>(b"{\"outputs\":".to_vec())
                    });
                    let lost = futures_util::stream::once(async {
                        tokio::time::sleep(Duration::from_millis(150)).await;
                        Err(std::io::Error::other("the answer was lost in transit"))
                    });
                    axum::body::Body::from_stream(head.chain(lost))
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            crate::serve_with_stream_limit(listener, app, std::future::pending()).await
        });

        let client = InferenceApiClient::new_with_metadata_cache(base_url, false).unwrap();
        let err = client
            .predict("g/model", "k", 1, 60, None, None, &[text_input("x")])
            .await
            .expect_err("the answer never arrives whole");

        let failure = inference_failure(&err).expect("a lost answer is typed too");
        assert_eq!(failure.transport_phase(), Some(TransportPhase::Body));
        assert!(!failure.is_unattempted(), "the server did the work");
        assert!(failure.warrants_resubmission(), "a predict is idempotent");
        // The phase is the load-bearing half, so it is printed with the kind.
        assert!(
            failure.to_string().contains("[transport/body]"),
            "{failure}"
        );
        assert_eq!(
            client.known_transport(),
            Some(Transport::H2c),
            "a body that died after the head says nothing about the protocol"
        );
    }

    /// A peer cannot claim this client's classification: the phase is written
    /// only by `from_transport`, so a body saying `kind = "transport"` buys
    /// nothing with it.
    #[test]
    fn a_peer_cannot_claim_the_clients_own_transport_classification() {
        let failure = InferenceFailure::parse(
            StatusCode::BAD_REQUEST,
            None,
            r#"{"detail":{"kind":"transport","message":"nice try"}}"#,
        );
        assert_eq!(failure.kind.as_deref(), Some(TRANSPORT_KIND));
        assert_eq!(
            failure.transport_phase(),
            None,
            "no phase came off the wire"
        );
        // An untyped-in-fact 400 behaves exactly as it did before.
        assert!(!failure.is_unattempted());
        assert!(!failure.warrants_resubmission());
    }

    /// Two clients for the same endpoint share one connection pool and one
    /// transport decision. The gateway builds several per endpoint (the job
    /// pool, the PQL path, the preload loop), and an unshared pool is not a
    /// bound.
    #[tokio::test]
    async fn clients_for_one_endpoint_share_their_pool() {
        let base_url =
            spawn_blocking_stub(ConcurrencyProbe::new(), crate::MAX_CONCURRENT_STREAMS).await;
        let first = InferenceApiClient::new_with_metadata_cache(base_url.clone(), false).unwrap();
        let second = InferenceApiClient::new_with_metadata_cache(base_url, false).unwrap();
        assert!(first.get_cached_models().await.is_ok());
        assert_eq!(first.known_transport(), Some(Transport::H2c));
        assert_eq!(
            second.known_transport(),
            Some(Transport::H2c),
            "the second client must inherit the first one's probe"
        );
    }

    /// Optional external-input discovery treats only a 404 as an older
    /// unsupported server; other failures stay visible to callers.
    #[tokio::test]
    async fn optional_external_inputs_only_ignores_not_found() {
        let app = Router::new()
            .route(
                "/missing/api/inference/external-inputs",
                get(|| async { StatusCode::NOT_FOUND }),
            )
            .route(
                "/broken/api/inference/external-inputs",
                get(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let client = |path| {
            InferenceApiClient::new_with_metadata_cache(format!("http://{addr}/{path}"), false)
                .unwrap()
        };
        let missing = client("missing").get_external_inputs_optional().await;
        assert_eq!(missing.unwrap(), None);
        assert!(
            client("broken")
                .get_external_inputs_optional()
                .await
                .is_err()
        );
    }

    /// `max_batch` and `prewarm` appear as query params exactly when the
    /// caller passes `Some`, on both predict and load, never as empty values.
    /// Captured off a stub because the client builds the URLs internally.
    #[tokio::test]
    async fn urls_carry_max_batch_and_prewarm_only_when_some() {
        let captured: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let sink = |captured: &Arc<StdMutex<Vec<String>>>, body: Value| {
            let captured = Arc::clone(captured);
            move |RawQuery(query): RawQuery| {
                let captured = Arc::clone(&captured);
                let body = body.clone();
                async move {
                    captured.lock().unwrap().push(query.unwrap_or_default());
                    Json(body)
                }
            }
        };
        let app = Router::new()
            .route(
                "/api/inference/predict/{group}/{id}",
                post(sink(&captured, json!({"outputs": [{"ok": true}]}))),
            )
            .route(
                "/api/inference/load/{group}/{id}",
                axum::routing::put(sink(&captured, json!({"status": "loaded"}))),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let client = InferenceApiClient::new_with_metadata_cache(format!("http://{addr}"), false)
            .expect("client builds");
        let inputs = [InferenceInput::new(json!({"text": "x"}), None)];
        for (max_batch, prewarm) in [(Some(7), Some(false)), (None, None)] {
            client
                .predict("group/model", "key", 10, -1, max_batch, prewarm, &inputs)
                .await
                .expect("predict");
        }
        for prewarm in [Some(false), None] {
            client
                .load_model("group/model", "key", 10, -1, prewarm)
                .await
                .expect("load");
        }

        // Request index, the fragment, and whether it must be there: the
        // additive params only for the `Some` calls, the pre-existing ones
        // always, on predict (0, 1) and on load (2, 3).
        let queries = captured.lock().unwrap().clone();
        assert_eq!(queries.len(), 4, "all four requests reached the stub");
        for (index, fragment, present) in [
            (0usize, "max_batch=7", true),
            (0, "prewarm=false", true),
            (0, "cache_key=key", true),
            (0, "lru_size=10", true),
            (0, "ttl_seconds=-1", true),
            (1, "max_batch", false),
            (1, "prewarm", false),
            (2, "prewarm=false", true),
            (3, "prewarm", false),
        ] {
            let query = &queries[index];
            assert_eq!(query.contains(fragment), present, "{fragment} in {query}");
        }
    }

    fn envelope(outputs: Vec<Value>) -> (String, Vec<u8>) {
        (
            "application/json".to_string(),
            serde_json::to_vec(&json!({ "outputs": outputs })).unwrap(),
        )
    }

    /// A response the client cannot represent is a typed protocol violation
    /// rather than a payload or a guessed class — deterministic, so the
    /// extraction job skips the isolation pass. Wrappedness is per slot while
    /// `PredictOutput` is one type for the whole response, so a mixed batch
    /// has no common representation and would otherwise reach an output
    /// handler that finds no `transcription` and drops it silently.
    #[test]
    fn a_response_with_no_common_representation_is_a_typed_protocol_violation() {
        for (outputs, expected, label) in [
            (
                vec![
                    json!({"__error__": {"class": "input", "message": "Unreadable image"}}),
                    json!({"__type__": "base64", "content": "QUFB"}),
                    json!({"transcription": "hello"}),
                ],
                "mixes 1 binary and 1 JSON",
                "a batch mixing binary and JSON survivors",
            ),
            (
                vec![
                    json!({"transcription": "hello"}),
                    json!({"__error__": {"class": "blocked", "message": "not ours"}}),
                ],
                "predict output 1",
                "a malformed error slot",
            ),
        ] {
            let (content_type, body) = envelope(outputs);
            let err = parse_predict_response(&content_type, &body).expect_err(label);
            assert!(
                err.downcast_ref::<ProtocolViolation>().is_some(),
                "{label}: {err:#}"
            );
            assert!(format!("{err:#}").contains(expected), "{label}: {err:#}");
        }
    }

    /// The unmixed shapes round-trip: all survivors wrapped is a binary
    /// batch, none wrapped is a JSON batch, and the slot error keeps its
    /// *input's* index while the survivors close ranks. The legacy no-slot
    /// envelope passes through verbatim, pinning every older server's shape.
    #[test]
    fn unmixed_survivors_round_trip_beside_an_error_slot() {
        let (content_type, body) = envelope(vec![
            json!({"__type__": "base64", "content": "QUFB"}),
            json!({"__error__": {"class": "input", "message": "Unreadable image"}}),
            json!({"__type__": "base64", "content": "QkI="}),
        ]);
        let parsed = parse_predict_response(&content_type, &body).unwrap();
        assert_eq!(parsed.errors.len(), 1);
        assert_eq!(parsed.errors[0].index, 1);
        match parsed.outputs {
            PredictOutput::Binary(outputs) => {
                assert_eq!(outputs, vec![b"AAA".to_vec(), b"BB".to_vec()]);
            }
            other => panic!("client parsed {other:?}"),
        }

        let (content_type, body) = envelope(vec![
            json!({"__error__": {"class": "transient", "message": "try again"}}),
            json!({"transcription": "hello"}),
        ]);
        let parsed = parse_predict_response(&content_type, &body).unwrap();
        assert_eq!(parsed.errors[0].class, SlotErrorClass::Transient);
        match parsed.outputs {
            PredictOutput::Json(values) => {
                assert_eq!(values, vec![json!({"transcription": "hello"})]);
            }
            other => panic!("client parsed {other:?}"),
        }

        let legacy = vec![
            json!({"__type__": "base64", "content": "QUFB"}),
            json!({"transcription": "hello"}),
        ];
        let (content_type, body) = envelope(legacy.clone());
        let parsed = parse_predict_response(&content_type, &body).unwrap();
        assert!(parsed.errors.is_empty());
        match parsed.outputs {
            PredictOutput::Json(parsed) => assert_eq!(parsed, legacy),
            other => panic!("client parsed {other:?}"),
        }
    }
}
