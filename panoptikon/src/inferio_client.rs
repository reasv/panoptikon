//! The gateway's HTTP client for an inference endpoint: transport selection
//! (h2c with prior knowledge, HTTP/1.1 fallback), the per-endpoint connection
//! lanes and in-flight gate, and the typed failures a predict can end in.
//!
//! See docs/inferio-transport.md "Client (inferio_client.rs)".

use anyhow::{Context, Result, bail};
use reqwest::header::CONTENT_TYPE;
use reqwest::multipart::{Form, Part};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::{RetryTransientMiddleware, Retryable, RetryableStrategy};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::config::Settings;
use crate::inferio::slot_error::{ProtocolViolation, SlotErrorClass, slot_error_from_json};
use crate::log_throttle::LogThrottle;

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
    /// Items the server wants kept in flight ([`DESIRED_IN_FLIGHT_HEADER`]);
    /// `None` when absent, unparsable or zero.
    pub desired_in_flight_items: Option<u64>,
}

/// Mirrors `inferio::http::DESIRED_IN_FLIGHT_HEADER`.
pub(crate) const DESIRED_IN_FLIGHT_HEADER: &str = "x-panoptikon-desired-in-flight-items";

/// `detail.kind`: the worker died with the request in flight (unattempted).
pub(crate) const WORKER_DIED_KIND: &str = "worker_died";

/// `detail.kind`: the model is in its load-failure cooldown. A 503 that
/// must not be retried.
pub(crate) const LOAD_COOLDOWN_KIND: &str = "load_cooldown";

/// `detail.kind`: the request body did not arrive in full. A 400 that is
/// not a verdict on the items.
pub(crate) const REQUEST_INCOMPLETE_KIND: &str = "request_incomplete";

/// `detail.kind`: refused unread, the server's predict-body budget is full
/// (503 with `Retry-After`).
pub(crate) const BODY_BUDGET_KIND: &str = "body_budget_exhausted";

/// `detail.kind`: refused unread, the body is over the per-request limit
/// (413). Split the batch; re-sending gets the same answer.
pub(crate) const REQUEST_TOO_LARGE_KIND: &str = "request_too_large";

/// `detail.kind` this client writes for its own transport failures; never on
/// the wire.
pub(crate) const TRANSPORT_KIND: &str = "transport";

/// How far a predict got before its transport failed, in request order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportPhase {
    /// No connection was established.
    Connect,
    /// Connected, and no response head came of it (reset, refused stream,
    /// `GOAWAY`, unwritable body), whether or not the request was fully sent.
    Send,
    /// Sent, and no response head arrived.
    Headers,
    /// The response head arrived and the body did not. Not "unattempted".
    Body,
}

impl TransportPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::Send => "send",
            Self::Headers => "headers",
            Self::Body => "body",
        }
    }

    /// Every phase short of a response head.
    pub fn is_before_any_answer(self) -> bool {
        !matches!(self, Self::Body)
    }
}

/// This client's classification of a transport failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TransportFailure {
    pub phase: TransportPhase,
    /// `reqwest`'s name for the error ([`reqwest_error_class`]).
    pub class: &'static str,
}

/// A refused inference request with its `{"detail": …}` body parsed, typed so
/// the caller's decision survives added context.
#[derive(Debug, Clone)]
pub(crate) struct InferenceFailure {
    /// 0 when there was no response ([`TRANSPORT_KIND`]).
    pub status: u16,
    pub kind: Option<String>,
    /// `detail.message`, a string `detail`, or the raw body.
    pub message: String,
    pub model: Option<String>,
    pub last_error: Option<String>,
    pub retry_at: Option<String>,
    pub failures: Option<u32>,
    pub retry_after_secs: Option<u64>,
    /// Set only by [`InferenceFailure::from_transport`], never from a body.
    pub transport: Option<TransportFailure>,
}

impl InferenceFailure {
    /// Parse one refused response; never fails.
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

    /// A predict whose transport failed. `last_error` is the whole source
    /// chain: `reqwest`'s `Display` names only the layer.
    pub(crate) fn from_transport(phase: TransportPhase, err: &reqwest::Error) -> Self {
        Self {
            message: err.to_string(),
            ..Self::transport(phase, reqwest_error_class(err), error_chain(err))
        }
    }

    /// A predict whose transport failed without a `reqwest` error, such as
    /// one cut off because its peer was declared frozen.
    pub(crate) fn transport(phase: TransportPhase, class: &'static str, cause: String) -> Self {
        Self {
            status: 0,
            kind: Some(TRANSPORT_KIND.to_owned()),
            message: cause.clone(),
            model: None,
            last_error: Some(cause),
            retry_at: None,
            failures: None,
            retry_after_secs: None,
            transport: Some(TransportFailure { phase, class }),
        }
    }

    pub fn is_worker_death(&self) -> bool {
        self.kind.as_deref() == Some(WORKER_DIED_KIND)
    }

    pub fn is_request_incomplete(&self) -> bool {
        self.kind.as_deref() == Some(REQUEST_INCOMPLETE_KIND)
    }

    pub fn is_body_budget_exhausted(&self) -> bool {
        self.kind.as_deref() == Some(BODY_BUDGET_KIND)
    }

    /// Not part of [`Self::is_unattempted`]: re-sending the same bytes gets
    /// the same answer. The sender splits the batch
    /// (`jobs::extraction::run_chunked_inference`).
    pub fn is_request_too_large(&self) -> bool {
        self.kind.as_deref() == Some(REQUEST_TOO_LARGE_KIND)
    }

    pub fn transport_phase(&self) -> Option<TransportPhase> {
        self.transport.map(|failure| failure.phase)
    }

    /// No answer about the items had been produced: the three server kinds
    /// plus transport phases short of a response head.
    pub fn is_unattempted(&self) -> bool {
        self.is_worker_death()
            || self.is_request_incomplete()
            || self.is_body_budget_exhausted()
            || self
                .transport_phase()
                .is_some_and(TransportPhase::is_before_any_answer)
    }

    /// Re-submitting the items is correct: [`Self::is_unattempted`] plus
    /// [`TransportPhase::Body`] (predict is idempotent).
    pub fn warrants_resubmission(&self) -> bool {
        self.is_unattempted() || self.transport_phase().is_some()
    }

    pub fn is_load_cooldown(&self) -> bool {
        self.kind.as_deref() == Some(LOAD_COOLDOWN_KIND)
    }
}

impl std::fmt::Display for InferenceFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.status == 0 {
            write!(f, "inference request failed (no response)")?;
        } else {
            write!(f, "inference request failed ({})", self.status)?;
        }
        if let Some(kind) = &self.kind {
            match self.transport {
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

/// The typed failure inside an error chain, if there is one.
pub(crate) fn inference_failure(err: &anyhow::Error) -> Option<&InferenceFailure> {
    err.downcast_ref::<InferenceFailure>()
}

/// `Retry-After` as delta-seconds.
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

/// How this client talks to one inference endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Transport {
    /// HTTP/2: cleartext with prior knowledge, or ALPN-negotiated over TLS.
    H2c,
    /// HTTP/1.1: one socket per concurrent request.
    Http11,
}

/// A remembered transport; `expires` is `None` for protocol evidence.
#[derive(Clone, Copy, Debug)]
struct Remembered {
    transport: Transport,
    expires: Option<Instant>,
}

impl Remembered {
    fn in_force(self) -> Option<Transport> {
        match self.expires {
            Some(at) if at <= Instant::now() => None,
            _ => Some(self.transport),
        }
    }
}

/// What a probe's answer may be remembered as.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Memo {
    /// Protocol evidence: remembered for the life of the process.
    Settled,
    /// The probe timed out: HTTP/1.1 for [`PROVISIONAL_MEMO_TTL`].
    Provisional,
    /// No evidence at all; the next call probes again.
    Unrecorded,
}

impl Transport {
    /// Whether requests share connections.
    pub fn is_multiplexed(self) -> bool {
        matches!(self, Transport::H2c)
    }
}

/// HTTP/2 connections ("lanes") per endpoint, each its own `reqwest::Client`:
/// hyper-util puts a whole pool on one connection. Recruited by load.
pub(crate) const INFERENCE_CONNECTION_LANES: usize = 64;

/// Streams per lane before the next is recruited; below common server limits.
const H2_STREAMS_PER_CONNECTION: usize = 64;

/// The floor of the concurrency gate on both transports.
pub(crate) const INFERENCE_MAX_CONCURRENT_REQUESTS: usize = 4 * H2_STREAMS_PER_CONNECTION;

/// The ceiling of the concurrency gate on both transports; the HTTP/1.1 one
/// is lowered to the descriptor budget.
pub(crate) const INFERENCE_MAX_CONCURRENT_STREAMS: usize =
    INFERENCE_CONNECTION_LANES * H2_STREAMS_PER_CONNECTION;

/// The HTTP/1.1 gate's ceiling: under HTTP/1.1 an admitted request is a
/// socket, so at most what `soft_nofile` holds, never below the floor.
fn http1_gate_ceiling(soft_nofile: u64) -> usize {
    crate::rlimit::http1_requests_within(soft_nofile).clamp(
        INFERENCE_MAX_CONCURRENT_REQUESTS,
        INFERENCE_MAX_CONCURRENT_STREAMS,
    )
}

/// One HTTP/2 connection and its current load; the client is built lazily.
#[derive(Debug)]
struct Lane {
    clients: OnceLock<EndpointClients>,
    in_flight: AtomicUsize,
}

/// A concurrency gate that follows a target between
/// [`INFERENCE_MAX_CONCURRENT_REQUESTS`] and its ceiling. A shrink withholds
/// permits as they come back, so permits in existence are
/// `target + pending_shrink`.
#[derive(Debug)]
struct Gate {
    permits: Arc<tokio::sync::Semaphore>,
    ceiling: usize,
    state: std::sync::Mutex<GateState>,
}

#[derive(Debug)]
struct GateState {
    target: usize,
    pending_shrink: usize,
}

impl Gate {
    fn new(ceiling: usize) -> Self {
        Self {
            permits: Arc::new(tokio::sync::Semaphore::new(
                INFERENCE_MAX_CONCURRENT_REQUESTS,
            )),
            ceiling,
            state: std::sync::Mutex::new(GateState {
                target: INFERENCE_MAX_CONCURRENT_REQUESTS,
                pending_shrink: 0,
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, GateState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Follow a desired-in-flight figure, clamped to the floor and ceiling.
    fn set_target(&self, requests: u64) {
        let wanted = usize::try_from(requests)
            .unwrap_or(usize::MAX)
            .clamp(INFERENCE_MAX_CONCURRENT_REQUESTS, self.ceiling);
        let mut state = self.lock();
        match wanted.cmp(&state.target) {
            std::cmp::Ordering::Greater => {
                let grow = wanted - state.target;
                // Growth first cancels a pending shrink.
                let cancelled = state.pending_shrink.min(grow);
                state.pending_shrink -= cancelled;
                if grow > cancelled {
                    self.permits.add_permits(grow - cancelled);
                }
                state.target = wanted;
            }
            std::cmp::Ordering::Less => {
                state.pending_shrink += state.target - wanted;
                state.target = wanted;
            }
            std::cmp::Ordering::Equal => {}
        }
        let removed = self.permits.forget_permits(state.pending_shrink);
        state.pending_shrink -= removed;
    }

    /// Return a permit, retiring it while a shrink is pending (`Semaphore`
    /// hands a released permit straight to a waiter).
    fn release(&self, permit: tokio::sync::OwnedSemaphorePermit) {
        let mut state = self.lock();
        if state.pending_shrink > 0 {
            state.pending_shrink -= 1;
            permit.forget();
        } else {
            drop(permit);
        }
    }

    /// The target and the permits held.
    fn snapshot(&self) -> (usize, usize) {
        let (target, pending) = {
            let state = self.lock();
            (state.target, state.pending_shrink)
        };
        (
            target,
            target
                .saturating_add(pending)
                .saturating_sub(self.permits.available_permits()),
        )
    }
}

/// When health checks start and how long one may take. One starts at most
/// every `timeout`.
#[derive(Debug, Clone, Copy)]
struct HealthCheckTiming {
    /// A request with no response head for this long starts the checks.
    after: Duration,
    /// The deadline of one check.
    timeout: Duration,
}

/// Since when the server is declared frozen, and how many checks have
/// finished, so a caller can wait for the next one.
#[derive(Debug, Clone, Copy, Default)]
struct Verdict {
    /// `None` while the server answers.
    frozen_since: Option<chrono::DateTime<chrono::Local>>,
    checks: u64,
}

#[derive(Debug, Default)]
struct HealthCheckState {
    /// Requests that have waited longer than `HealthCheckTiming::after`.
    stalled: usize,
    /// Whether the check task runs; at most one per endpoint.
    running: bool,
    /// Checks in a row without an answer, kept across check tasks.
    misses: u32,
}

/// Health checks of the server behind a base URL, sent through that URL so
/// they cross any proxy the requests cross: a proxy answers HTTP/2 pings
/// itself, and HTTP/1.1 has none.
#[derive(Debug)]
struct HealthChecks {
    timing: HealthCheckTiming,
    /// Built by the first check.
    clients: OnceLock<CheckClients>,
    /// Written only by the check task.
    verdict: tokio::sync::watch::Sender<Verdict>,
    state: std::sync::Mutex<HealthCheckState>,
}

/// The health checks' own clients, sharing no connection or stream limit with
/// requests. They pick the version as requests do: over TLS both are one
/// client that offers h2 and HTTP/1.1 in ALPN; in the clear the transport in
/// force picks h2 with prior knowledge or HTTP/1.1. They keep no idle
/// connection, so each check dials a new one and a wedged connection is never
/// asked again.
#[derive(Debug)]
struct CheckClients {
    h2: reqwest::Client,
    http1: reqwest::Client,
}

impl CheckClients {
    fn build(tls: bool) -> Result<Self> {
        let build = |builder: reqwest::ClientBuilder| {
            builder
                .build()
                .context("failed to build the health check client")
        };
        if tls {
            let negotiating = build(client_builder(0))?;
            return Ok(Self {
                h2: negotiating.clone(),
                http1: negotiating,
            });
        }
        Ok(Self {
            h2: build(client_builder(0).http2_prior_knowledge())?,
            http1: build(client_builder(0).http1_only())?,
        })
    }
}

impl HealthChecks {
    fn lock(&self) -> std::sync::MutexGuard<'_, HealthCheckState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// A request failed because its server is declared frozen.
#[derive(Debug)]
pub(crate) struct PeerFrozen;

impl std::fmt::Display for PeerFrozen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "it did not answer {HEALTH_CHECK_MISSES} health checks in a row \
             (GET /api/inference/health)"
        )
    }
}

impl std::error::Error for PeerFrozen {}

/// One endpoint's clients, lanes and state, shared per base URL.
#[derive(Debug)]
struct EndpointRuntime {
    h2: Vec<Lane>,
    /// Lane 0's client, built eagerly; the fallback if a lane's build fails.
    h2_seed: EndpointClients,
    h1: EndpointClients,
    tls: bool,
    /// TLS only: offers h2 and HTTP/1.1 in ALPN, for the probe. The lanes
    /// offer only h2.
    negotiating: Option<reqwest::Client>,
    /// `None` until the first probe and again after a connection error.
    transport: RwLock<Option<Remembered>>,
    /// The transport last logged as chosen.
    announced: std::sync::Mutex<Option<Transport>>,
    probe_lock: tokio::sync::Mutex<()>,
    /// Probes finished and the last verdict, memoized or not, so a caller that
    /// waited out a probe takes its answer.
    last_probe: std::sync::Mutex<(u64, Transport)>,
    /// Both resized by [`Self::set_in_flight_target`].
    h2_gate: Gate,
    h1_gate: Gate,
    probe_log: LogThrottle,
    predict_log: LogThrottle,
    stall_log: LogThrottle,
    health_checks: HealthChecks,
}

impl EndpointRuntime {
    fn check_clients(&self) -> &CheckClients {
        self.health_checks.clients.get_or_init(|| {
            CheckClients::build(self.tls).unwrap_or_else(|err| {
                warn!(
                    error = %err,
                    "failed to build the inference health check client; \
                     checking on the HTTP/1.1 client instead"
                );
                CheckClients {
                    h2: self.h1.raw.clone(),
                    http1: self.h1.raw.clone(),
                }
            })
        })
    }

    fn lane_clients(&self, lane: usize) -> EndpointClients {
        self.h2[lane]
            .clients
            .get_or_init(|| {
                EndpointClients::build(h2_client_builder, 1).unwrap_or_else(|err| {
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

    /// The least loaded of the lanes the current load requires (not of all
    /// lanes, which would cost a socket per request). Racy by design.
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

    /// Follow the endpoint's desired-in-flight figure on both gates. Items
    /// used as requests only over-provisions permits.
    fn set_in_flight_target(&self, requests: u64) {
        self.h2_gate.set_target(requests);
        self.h1_gate.set_target(requests);
    }

    /// The gate of `transport`; an unknown one reads as h2c.
    fn gate(&self, transport: Option<Transport>) -> &Gate {
        match transport {
            Some(Transport::Http11) => &self.h1_gate,
            _ => &self.h2_gate,
        }
    }

    fn health(&self, base_url: &str) -> InferenceTransportHealth {
        let transport = self
            .transport
            .try_read()
            .ok()
            .and_then(|guard| *guard)
            .and_then(Remembered::in_force);
        let (target, in_flight) = self.gate(transport).snapshot();
        let multiplexed = !matches!(transport, Some(Transport::Http11));
        InferenceTransportHealth {
            base_url: base_url.to_owned(),
            transport: self.label(transport).to_owned(),
            pool_connections: multiplexed.then_some(INFERENCE_CONNECTION_LANES),
            connections_in_use: multiplexed.then(|| self.lanes_in_use()),
            max_concurrent_requests: target,
            in_flight_requests: in_flight,
            frozen_since: self
                .health_checks
                .verdict
                .borrow()
                .frozen_since
                .map(|since| since.to_rfc3339()),
        }
    }

    fn label(&self, transport: Option<Transport>) -> &'static str {
        match transport {
            Some(Transport::H2c) if self.tls => "h2",
            Some(Transport::H2c) => "h2c",
            Some(Transport::Http11) => "http/1.1",
            None => "unknown",
        }
    }

    fn lanes_in_use(&self) -> usize {
        self.h2
            .iter()
            .filter(|lane| lane.in_flight.load(Relaxed) > 0)
            .count()
    }
}

/// What one inference endpoint's client is doing right now.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct InferenceTransportHealth {
    /// The endpoint this describes.
    pub base_url: String,
    /// `h2c` | `h2` (over TLS) | `http/1.1` | `unknown` (not contacted yet).
    pub transport: String,
    /// Connections this client may hold; `null` under HTTP/1.1.
    pub pool_connections: Option<usize>,
    /// Of those, how many carry a request now; `null` under HTTP/1.1.
    pub connections_in_use: Option<usize>,
    /// Requests the gate currently admits.
    pub max_concurrent_requests: usize,
    /// Of those, how many are in flight right now.
    pub in_flight_requests: usize,
    /// RFC 3339 instant the server was declared frozen: it missed its health
    /// checks, and its requests fail until it answers one. `null` while it
    /// answers.
    pub frozen_since: Option<String>,
}

/// One admitted request's gate permit and lane, both returned on `Drop`.
struct EndpointLease {
    endpoint: Arc<EndpointRuntime>,
    lane: Option<usize>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    transport: Transport,
}

impl Drop for EndpointLease {
    fn drop(&mut self) {
        if let Some(lane) = self.lane.take() {
            self.endpoint.h2[lane].in_flight.fetch_sub(1, Relaxed);
        }
        if let Some(permit) = self.permit.take() {
            self.endpoint.gate(Some(self.transport)).release(permit);
        }
    }
}

/// A request counted as stalled for as long as it lives.
struct Stalled<'a>(&'a HealthChecks);

impl Drop for Stalled<'_> {
    fn drop(&mut self) {
        self.0.lock().stalled -= 1;
    }
}

/// Retry rule for the non-predict endpoints. Unlike the middleware default, a
/// 503 (possibly the load cooldown) and a 500 (a failed load, possibly after
/// the full load deadline) are not retried; `predict` has its own loop.
struct InferenceRetryStrategy;

impl RetryableStrategy for InferenceRetryStrategy {
    fn handle(
        &self,
        result: &std::result::Result<reqwest::Response, reqwest_middleware::Error>,
    ) -> Option<Retryable> {
        match result {
            Ok(response) => {
                should_retry_status_unread(response.status()).then_some(Retryable::Transient)
            }
            Err(reqwest_middleware::Error::Reqwest(err)) => {
                should_retry_error(err).then_some(Retryable::Transient)
            }
            Err(_) => Some(Retryable::Fatal),
        }
    }
}

/// Whether an endpoint is reached over TLS, where ALPN picks the version.
fn is_tls_endpoint(base_url: &str) -> bool {
    base_url.len() >= 8 && base_url[..8].eq_ignore_ascii_case("https://")
}

/// How an endpoint's h2 lanes are built: prior knowledge in the clear and over
/// TLS alike, the windows in [`crate::H2_STREAM_WINDOW`], and keep-alive
/// pings. Over TLS prior knowledge offers only `h2` in ALPN, so a lane is used
/// only once the negotiating probe has seen the peer choose h2; negotiating on
/// every connection would open one per request of a cold burst.
fn h2_client_builder(builder: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
    h2_lane_builder(builder, H2_KEEP_ALIVE_INTERVAL, H2_KEEP_ALIVE_TIMEOUT)
}

/// [`h2_client_builder`] with the keep-alive as parameters, for tests.
fn h2_lane_builder(
    builder: reqwest::ClientBuilder,
    keep_alive_interval: Duration,
    keep_alive_timeout: Duration,
) -> reqwest::ClientBuilder {
    builder
        .http2_prior_knowledge()
        .http2_initial_stream_window_size(crate::H2_STREAM_WINDOW)
        .http2_initial_connection_window_size(crate::H2_CONNECTION_WINDOW)
        .http2_keep_alive_interval(keep_alive_interval)
        .http2_keep_alive_timeout(keep_alive_timeout)
}

fn client_builder(pool_max_idle_per_host: usize) -> reqwest::ClientBuilder {
    let builder = reqwest::Client::builder().pool_max_idle_per_host(pool_max_idle_per_host);
    #[cfg(all(test, target_os = "linux"))]
    let builder = tests::trust_test_certificate(builder);
    builder
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
        let raw = configure(client_builder(pool_max_idle_per_host))
            .build()
            .context("failed to build inference API client")?;
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
        let middleware = ClientBuilder::new(raw.clone())
            .with(RetryTransientMiddleware::new_with_policy_and_strategy(
                retry_policy,
                InferenceRetryStrategy,
            ))
            .build();
        Ok(Self { raw, middleware })
    }
}

static ENDPOINTS: OnceLock<std::sync::Mutex<HashMap<String, Arc<EndpointRuntime>>>> =
    OnceLock::new();

fn endpoint_runtime(base_url: &str, checks: HealthCheckTiming) -> Result<Arc<EndpointRuntime>> {
    let registry = ENDPOINTS.get_or_init(|| std::sync::Mutex::new(HashMap::new()));
    let mut guard = registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(existing) = guard.get(base_url) {
        return Ok(Arc::clone(existing));
    }
    // Only lane 0 is built here; `pick_lane` recruits the rest.
    let tls = is_tls_endpoint(base_url);
    let seed = EndpointClients::build(h2_client_builder, 1)?;
    let negotiating = if tls {
        Some(
            client_builder(1)
                .build()
                .context("failed to build inference API client")?,
        )
    } else {
        None
    };
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
        tls,
        negotiating,
        transport: RwLock::new(None),
        announced: std::sync::Mutex::new(None),
        probe_lock: tokio::sync::Mutex::new(()),
        last_probe: std::sync::Mutex::new((0, Transport::Http11)),
        h2_gate: Gate::new(INFERENCE_MAX_CONCURRENT_STREAMS),
        h1_gate: Gate::new(http1_gate_ceiling(crate::rlimit::soft_nofile_limit())),
        probe_log: LogThrottle::new(
            format!("could not reach the inference endpoint {base_url}"),
            tracing::Level::WARN,
        ),
        predict_log: LogThrottle::new(
            format!("inference predict failures against {base_url}"),
            tracing::Level::WARN,
        ),
        stall_log: LogThrottle::new(
            format!("inference predicts still waiting on {base_url}"),
            tracing::Level::WARN,
        ),
        health_checks: HealthChecks {
            timing: checks,
            clients: OnceLock::new(),
            verdict: tokio::sync::watch::Sender::new(Verdict::default()),
            state: std::sync::Mutex::new(HealthCheckState::default()),
        },
    });
    guard.insert(base_url.to_string(), Arc::clone(&runtime));
    Ok(runtime)
}

/// Every inference endpoint this process holds a client for, for `/health`.
/// Uses `try_read`, so it never waits on a transport probe.
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
    /// The URL as configured, which error messages name.
    base_url: String,
    /// `base_url` normalized to end in `/api/inference`.
    api_url: String,
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
/// Transport probe deadline: the clients have no request timeout and every
/// caller waits behind the prober.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// How long HTTP/1.1 stands after a probe timed out, before re-probing.
const PROVISIONAL_MEMO_TTL: Duration = Duration::from_secs(60);
/// An h2 lane with a request open pings its peer after this long without a
/// frame from it.
const H2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30);
/// How long a ping may go unanswered before the connection is closed and its
/// requests fail. Pings bound how long a peer may be silent, not how long a
/// batch may take: a working peer answers them while it infers.
const H2_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(20);
/// A predict with no response head is logged after this long, and again each
/// time the wait doubles; it is cut off only if the peer is declared frozen.
const STALL_WARN_AFTER: Duration = Duration::from_secs(120);
/// A frozen server is declared after 30 + 2 × 10 = 50 s, the keep-alive's
/// own bound (30 + 20 s). `/health` reads in-memory state and touches no
/// model, so a server that is alive answers it in well under 10 s however
/// busy; only the path to it (this process, a proxy) can be slower.
const HEALTH_CHECKS: HealthCheckTiming = HealthCheckTiming {
    after: Duration::from_secs(30),
    timeout: Duration::from_secs(10),
};
/// Checks in a row without an answer that declare the server frozen.
const HEALTH_CHECK_MISSES: u32 = 2;

impl InferenceApiClient {
    pub fn new_with_metadata_cache(
        base_url: impl Into<String>,
        cache_metadata: bool,
    ) -> Result<Self> {
        let base_url = base_url.into();
        let api_url = normalize_base_url(base_url.clone());
        let endpoint = endpoint_runtime(&api_url, HEALTH_CHECKS)?;
        Ok(Self {
            base_url,
            api_url,
            endpoint,
            cache_metadata,
        })
    }

    /// The URL as configured.
    pub(crate) fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The transport in use, probing once if it is not known yet. A downgrade
    /// is settled only on protocol evidence (ALPN over TLS; in the clear, two
    /// failed h2 probes plus an HTTP/1.1 answer); a probe timeout records
    /// HTTP/1.1 provisionally.
    async fn transport(&self) -> Transport {
        if let Some(transport) = self.remembered_transport().await {
            return transport;
        }
        let probes_before = self.last_probe().0;
        let _probing = self.endpoint.probe_lock.lock().await;
        if let Some(transport) = self.remembered_transport().await {
            return transport;
        }
        let last = self.last_probe();
        if last.0 != probes_before {
            // Someone probed while we waited; take its answer.
            return last.1;
        }
        let (transport, memo, reason) = self.probe_transport().await;
        {
            let mut last = self
                .endpoint
                .last_probe
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            last.0 += 1;
            last.1 = transport;
        }
        let expires = match memo {
            Memo::Settled => None,
            Memo::Provisional => Some(Instant::now() + PROVISIONAL_MEMO_TTL),
            Memo::Unrecorded => return transport,
        };
        *self.endpoint.transport.write().await = Some(Remembered { transport, expires });
        self.announce(transport, reason);
        transport
    }

    /// One INFO line when the recorded transport is the first for this
    /// endpoint or differs from the last one logged.
    fn announce(&self, transport: Transport, reason: &str) {
        let previous = {
            let mut announced = self
                .endpoint
                .announced
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if *announced == Some(transport) {
                return;
            }
            announced.replace(transport)
        };
        let label = self.endpoint.label(Some(transport));
        let previous = previous.map_or("none", |previous| self.endpoint.label(Some(previous)));
        info!(
            endpoint = %self.api_url,
            transport = label,
            previous,
            reason,
            connection_lanes = transport
                .is_multiplexed()
                .then_some(INFERENCE_CONNECTION_LANES),
            max_concurrent = INFERENCE_MAX_CONCURRENT_REQUESTS,
            "inference transport chosen"
        );
    }

    async fn remembered_transport(&self) -> Option<Transport> {
        self.endpoint
            .transport
            .read()
            .await
            .and_then(Remembered::in_force)
    }

    fn last_probe(&self) -> (u64, Transport) {
        *self
            .endpoint
            .last_probe
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The verdict, how far it may be remembered, and why.
    async fn probe_transport(&self) -> (Transport, Memo, &'static str) {
        let first = match &self.endpoint.negotiating {
            Some(client) => self.probe_cache(client).await,
            None => self.probe_h2c().await,
        };
        match first {
            Ok(version) if self.endpoint.tls => {
                if version == reqwest::Version::HTTP_2 {
                    (Transport::H2c, Memo::Settled, "ALPN selected h2")
                } else {
                    (Transport::Http11, Memo::Settled, "ALPN selected HTTP/1.1")
                }
            }
            Ok(_) => (
                Transport::H2c,
                Memo::Settled,
                "the peer answered HTTP/2 with prior knowledge",
            ),
            // A failed TLS probe is never protocol evidence (ALPN negotiates).
            Err(err) if self.endpoint.tls || !Self::could_be_an_http2_refusal(&err) => {
                if err.is_timeout() {
                    warn!(
                        endpoint = %self.api_url,
                        error = %err,
                        ttl_secs = PROVISIONAL_MEMO_TTL.as_secs(),
                        "the transport probe timed out against the inference \
                         endpoint; using HTTP/1.1 until the next probe"
                    );
                    return (
                        Transport::Http11,
                        Memo::Provisional,
                        "the probe timed out; provisional",
                    );
                }
                // Unreachable: nothing is remembered; this attempt uses HTTP/1.1.
                if self.endpoint.probe_log.admit() {
                    warn!(
                        endpoint = %self.api_url,
                        error = %error_chain(&err),
                        "could not reach the inference endpoint to establish which \
                         HTTP version it speaks; not recording a fallback"
                    );
                }
                (Transport::Http11, Memo::Unrecorded, "unreachable")
            }
            Err(first) => match self.probe_h2c().await {
                Ok(_) => (
                    Transport::H2c,
                    Memo::Settled,
                    "the peer answered HTTP/2 with prior knowledge",
                ),
                Err(second) if self.peer_answers_http11().await => {
                    warn!(
                        endpoint = %self.api_url,
                        error = %second,
                        first_error = %first,
                        "the inference endpoint answers HTTP/1.1 but not HTTP/2 \
                         cleartext; falling back to HTTP/1.1 for this endpoint"
                    );
                    (
                        Transport::Http11,
                        Memo::Settled,
                        "the peer refused HTTP/2 twice and answered HTTP/1.1",
                    )
                }
                Err(second) => {
                    if self.endpoint.probe_log.admit() {
                        warn!(
                            endpoint = %self.api_url,
                            error = %second,
                            first_error = %first,
                            "the inference endpoint answered neither HTTP/2 cleartext \
                             nor HTTP/1.1; not recording a fallback"
                        );
                    }
                    (Transport::Http11, Memo::Unrecorded, "unreachable")
                }
            },
        }
    }

    /// One `GET /cache` probe; any status proves the frames parsed.
    async fn probe_cache(&self, client: &reqwest::Client) -> reqwest::Result<reqwest::Version> {
        client
            .get(format!("{}/cache", self.api_url))
            .timeout(PROBE_TIMEOUT)
            .send()
            .await
            .map(|response| response.version())
    }

    async fn probe_h2c(&self) -> reqwest::Result<reqwest::Version> {
        self.probe_cache(&self.endpoint.h2_seed.raw).await
    }

    /// Whether the peer answers over HTTP/1.1, proving an h2 failure was about
    /// the protocol.
    async fn peer_answers_http11(&self) -> bool {
        self.probe_cache(&self.endpoint.h1.raw).await.is_ok()
    }

    /// Whether a failed probe could be the peer refusing HTTP/2: neither a
    /// connect error nor a timeout.
    fn could_be_an_http2_refusal(err: &reqwest::Error) -> bool {
        !err.is_connect() && !err.is_timeout()
    }

    /// The resolved transport, without probing. Budget callers must read
    /// `None` as HTTP/1.1.
    pub fn known_transport(&self) -> Option<Transport> {
        self.endpoint
            .transport
            .try_read()
            .ok()
            .and_then(|guard| *guard)
            .and_then(Remembered::in_force)
    }

    /// Clears the remembered transport so the next request re-probes.
    async fn forget_transport(&self) {
        *self.endpoint.transport.write().await = None;
    }

    /// Non-predict sends: failed while the server is frozen as a predict is,
    /// and a transport failure invalidates the memo by the same rule.
    async fn checked_send(
        &self,
        send: impl Future<Output = std::result::Result<reqwest::Response, reqwest_middleware::Error>>,
        context: &'static str,
    ) -> Result<reqwest::Response> {
        let result = match self.until_answered(send).await {
            Ok(result) => result,
            Err(frozen) => return Err(anyhow::Error::new(frozen)).context(context),
        };
        // The middleware's retries are already spent.
        if let Err(reqwest_middleware::Error::Reqwest(err)) = &result
            && invalidates_transport_memo(err, false)
        {
            self.forget_transport().await;
        }
        result.context(context)
    }

    /// Awaits `send` unless the server is declared frozen, before or while it
    /// waits. A request that has waited `timing.after` keeps the health checks
    /// running until it ends. While the server is frozen, a request that starts
    /// a check awaits its verdict and the others fail at once.
    async fn until_answered<T>(&self, send: impl Future<Output = T>) -> Result<T, PeerFrozen> {
        let checks = &self.endpoint.health_checks;
        let mut verdict = checks.verdict.subscribe();
        let seen = *verdict.borrow_and_update();
        if seen.frozen_since.is_some() {
            if !self.start_health_checks() {
                return Err(PeerFrozen);
            }
            // A check records its verdict within its deadline.
            let next = verdict.wait_for(|now| now.checks > seen.checks).await;
            if !next.is_ok_and(|now| now.frozen_since.is_none()) {
                return Err(PeerFrozen);
            }
        }
        let stall = async {
            tokio::time::sleep(checks.timing.after).await;
            checks.lock().stalled += 1;
            let _stalled = Stalled(checks);
            self.start_health_checks();
            std::future::pending::<()>().await;
        };
        tokio::select! {
            output = send => Ok(output),
            Ok(_) = verdict.wait_for(|now| now.frozen_since.is_some()) => Err(PeerFrozen),
            () = stall => unreachable!("never completes"),
        }
    }

    /// When the server was declared frozen, `None` while it answers. While it
    /// is frozen, also starts a health check unless one runs, so asking again
    /// finds the server once it answers.
    pub(crate) fn recheck_if_frozen(&self) -> Option<chrono::DateTime<chrono::Local>> {
        let since = self.endpoint.health_checks.verdict.borrow().frozen_since;
        if since.is_some() {
            self.start_health_checks();
        }
        since
    }

    /// The deadline of one health check.
    pub(crate) fn health_check_timeout(&self) -> Duration {
        self.endpoint.health_checks.timing.timeout
    }

    /// Starts the check task unless it runs; whether this call started it.
    fn start_health_checks(&self) -> bool {
        {
            let mut state = self.endpoint.health_checks.lock();
            if state.running {
                return false;
            }
            state.running = true;
        }
        let client = self.clone();
        tokio::spawn(async move { client.run_health_checks().await });
        true
    }

    /// Checks the server, then again every `timing.timeout` while a request is
    /// stalled or the last check missed short of a verdict. The
    /// [`HEALTH_CHECK_MISSES`]th miss in a row declares it frozen and an answer
    /// clears that, each change logged once. A task of its own, so a verdict
    /// outlives the requests that asked for it.
    async fn run_health_checks(&self) {
        let checks = &self.endpoint.health_checks;
        loop {
            let started = tokio::time::Instant::now();
            let answer = self.health_check().await;
            let misses = {
                let mut state = checks.lock();
                state.misses = if answer.is_ok() {
                    0
                } else {
                    state.misses.saturating_add(1)
                };
                state.misses
            };
            let mut was_frozen = false;
            checks.verdict.send_modify(|verdict| {
                was_frozen = verdict.frozen_since.is_some();
                let frozen = answer.is_err() && (was_frozen || misses >= HEALTH_CHECK_MISSES);
                verdict.frozen_since =
                    frozen.then(|| verdict.frozen_since.unwrap_or_else(chrono::Local::now));
                verdict.checks += 1;
            });
            match answer {
                Err(error) if !was_frozen && misses >= HEALTH_CHECK_MISSES => warn!(
                    endpoint = %self.api_url,
                    %error,
                    "the inference server did not answer {misses} health checks in a row; \
                     failing its requests until it answers again"
                ),
                Ok(()) if was_frozen => info!(
                    endpoint = %self.api_url,
                    "the inference server answers its health check again"
                ),
                _ => {}
            }
            tokio::time::sleep_until(started + checks.timing.timeout).await;
            let mut state = checks.lock();
            let verdict_pending = (1..HEALTH_CHECK_MISSES).contains(&state.misses);
            if state.stalled == 0 && !verdict_pending {
                state.running = false;
                return;
            }
        }
    }

    /// `GET /health` on the checks' own clients, so it never waits for a
    /// connection or a stream behind the requests. A miss is a timeout, or a
    /// 502, 503 or 504: a proxy saying the server behind it did not answer.
    /// Anything else, a refused connection or a failed TLS handshake included,
    /// is no evidence of a freeze.
    async fn health_check(&self) -> std::result::Result<(), String> {
        let transport = match self.remembered_transport().await {
            Some(transport) => transport,
            None => self.last_probe().1,
        };
        let clients = self.endpoint.check_clients();
        let client = if transport.is_multiplexed() {
            &clients.h2
        } else {
            &clients.http1
        };
        let sent = client
            .get(format!("{}/health", self.api_url))
            .timeout(self.endpoint.health_checks.timing.timeout)
            .send()
            .await;
        match sent {
            Ok(response) if matches!(response.status().as_u16(), 502..=504) => {
                Err(format!("answered {}", response.status()))
            }
            Err(err) if err.is_timeout() => Err(error_chain(&err)),
            _ => Ok(()),
        }
    }

    /// The clients for the transport in use plus a concurrency permit, taken on
    /// both transports: the job sizes its window once, so HTTP/1.1 can arrive
    /// under an h2-sized window.
    async fn active(&self) -> (Transport, EndpointClients, EndpointLease) {
        let transport = self.transport().await;
        match transport {
            Transport::H2c => {
                let permit = Arc::clone(&self.endpoint.h2_gate.permits)
                    .acquire_owned()
                    .await
                    .ok();
                // After the permit, so the choice sees the load that will run.
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
                        transport,
                    },
                )
            }
            Transport::Http11 => {
                let permit = Arc::clone(&self.endpoint.h1_gate.permits)
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
                        transport,
                    },
                )
            }
        }
    }

    /// Apply a desired-in-flight figure this endpoint published.
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
        let url = format!("{}/predict/{}", self.api_url, inference_id);
        let mut query: Vec<(&str, String)> = vec![
            ("cache_key", cache_key.to_string()),
            ("lru_size", lru_size.to_string()),
            ("ttl_seconds", ttl_seconds.to_string()),
        ];
        // Per-request cap on server-side batch merging (design doc §6).
        if let Some(max_batch) = max_batch {
            query.push(("max_batch", max_batch.to_string()));
        }
        // Lazy prewarm hint (design doc §8); absent means true.
        if let Some(prewarm) = prewarm {
            query.push(("prewarm", prewarm.to_string()));
        }
        let mut attempts: u32 = 0;
        loop {
            let form = build_predict_form(inputs).await?;
            // Per attempt; every `continue` drops the lease before backing off.
            let (transport, clients, lease) = self.active().await;
            let send = clients.raw.post(&url).query(&query).multipart(form).send();
            let send = self.until_answered(send);
            let response = await_warning_while_stalled(send, STALL_WARN_AFTER, |waited| {
                if self.endpoint.stall_log.admit() {
                    warn!(
                        %url,
                        waited_secs = waited.as_secs(),
                        transport = self.endpoint.label(Some(transport)),
                        "inference predict has no response yet; still waiting"
                    );
                }
            })
            .await;
            let response = match response {
                Ok(response) => response,
                // As a keep-alive timeout fails it: no answer, not retried here.
                Err(frozen) => {
                    let failure = InferenceFailure::transport(
                        TransportPhase::Headers,
                        "timeout",
                        frozen.to_string(),
                    );
                    return Err(anyhow::Error::new(frozen))
                        .context(failure)
                        .context("inference predict request failed");
                }
            };

            match response {
                Ok(response) => {
                    if response.status().is_success() {
                        let content_type = response
                            .headers()
                            .get(CONTENT_TYPE)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or("")
                            .to_string();
                        // Read before the body consumes the response.
                        let desired = response
                            .headers()
                            .get(DESIRED_IN_FLIGHT_HEADER)
                            .and_then(|value| value.to_str().ok())
                            .and_then(|value| value.trim().parse::<u64>().ok())
                            .filter(|value| *value > 0);
                        let body = match response.bytes().await {
                            Ok(body) => body.to_vec(),
                            // The answer was lost: typed for the job to
                            // re-submit, not retried here.
                            Err(err) => {
                                let failure =
                                    InferenceFailure::from_transport(TransportPhase::Body, &err);
                                if self
                                    .endpoint
                                    .predict_log
                                    .admit_for(&format!("{inference_id} body"))
                                {
                                    warn!(
                                        %url,
                                        phase = TransportPhase::Body.as_str(),
                                        class = reqwest_error_class(&err),
                                        error = %error_chain(&err),
                                        "inference predict answered and the answer was lost \
                                         in transit; its items have no verdict"
                                    );
                                }
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
                    // The body says whether a 503 is the cooldown.
                    let body = response.text().await.unwrap_or_default();
                    let failure = InferenceFailure::parse(status, retry_after, &body);
                    if failure.is_load_cooldown() {
                        if self
                            .endpoint
                            .predict_log
                            .admit_for(&format!("{inference_id} cooldown"))
                        {
                            warn!(
                                %url,
                                %status,
                                model = failure.model.as_deref().unwrap_or("?"),
                                retry_at = failure.retry_at.as_deref().unwrap_or("?"),
                                "inference predict refused: the model is in its load-failure \
                                 cooldown"
                            );
                        }
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

                    if self
                        .endpoint
                        .predict_log
                        .admit_for(&format!("{inference_id} status"))
                    {
                        warn!(%url, %status, %body, "inference predict failed");
                    }
                    return Err(anyhow::Error::new(failure));
                }
                Err(err) => {
                    let backoff = should_retry_error(&err)
                        .then(|| next_retry_delay(attempts))
                        .flatten();
                    if invalidates_transport_memo(&err, backoff.is_some()) {
                        self.forget_transport().await;
                    }
                    if let Some(delay) = backoff {
                        attempts += 1;
                        drop(lease);
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    // Out of retries: typed by where the request stopped.
                    let phase = send_phase(&err);
                    let failure = InferenceFailure::from_transport(phase, &err);
                    if self
                        .endpoint
                        .predict_log
                        .admit_for(&format!("{inference_id} transport"))
                    {
                        warn!(
                            %url,
                            phase = phase.as_str(),
                            class = reqwest_error_class(&err),
                            attempts,
                            error = %error_chain(&err),
                            "inference predict transport failure; its items have no verdict"
                        );
                    }
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
        let url = format!("{}/load/{}", self.api_url, inference_id);
        let mut query: Vec<(&str, String)> = vec![
            ("cache_key", cache_key.to_string()),
            ("lru_size", lru_size.to_string()),
            ("ttl_seconds", ttl_seconds.to_string()),
        ];
        if let Some(prewarm) = prewarm {
            query.push(("prewarm", prewarm.to_string()));
        }
        let (_transport, clients, _slot) = self.active().await;
        let response = self
            .checked_send(
                clients.middleware.put(url).query(&query).send(),
                "inference load request failed",
            )
            .await?;
        parse_json_response(response).await
    }

    pub async fn unload_model(&self, inference_id: &str, cache_key: &str) -> Result<Value> {
        let url = format!("{}/cache/{}/{}", self.api_url, cache_key, inference_id);
        let (_transport, clients, _slot) = self.active().await;
        let response = self
            .checked_send(
                clients.middleware.delete(url).send(),
                "inference unload request failed",
            )
            .await?;
        parse_json_response(response).await
    }

    pub async fn clear_cache(&self, cache_key: &str) -> Result<Value> {
        let url = format!("{}/cache/{}", self.api_url, cache_key);
        let (_transport, clients, _slot) = self.active().await;
        let response = self
            .checked_send(
                clients.middleware.delete(url).send(),
                "inference clear cache request failed",
            )
            .await?;
        parse_json_response(response).await
    }

    // Only exercised by the inferio HTTP tests; mirrors the Python client API.
    #[allow(dead_code)]
    pub async fn get_cached_models(&self) -> Result<Value> {
        let url = format!("{}/cache", self.api_url);
        let (_transport, clients, _slot) = self.active().await;
        let response = self
            .checked_send(
                clients.middleware.get(url).send(),
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
            if let Some(entry) = guard.get(&self.api_url)
                && entry.fetched_at.elapsed() < METADATA_CACHE_TTL
            {
                return Ok(entry.value.clone());
            }
        }

        let value = self.fetch_metadata().await?;
        let mut guard = cache.write().await;
        guard.insert(
            self.api_url.clone(),
            CachedMetadata {
                value: value.clone(),
                fetched_at: Instant::now(),
            },
        );
        Ok(value)
    }

    async fn fetch_metadata(&self) -> Result<Value> {
        let url = format!("{}/metadata", self.api_url);
        let (_transport, clients, _slot) = self.active().await;
        let response = self
            .checked_send(
                clients.middleware.get(url).send(),
                "inference metadata request failed",
            )
            .await?;
        parse_json_response(response).await
    }

    pub async fn get_external_inputs(&self) -> Result<Value> {
        let url = format!("{}/external-inputs", self.api_url);
        let (_transport, clients, _slot) = self.active().await;
        let response = self
            .checked_send(
                clients.middleware.get(url).send(),
                "inference external-input request failed",
            )
            .await?;
        parse_json_response(response).await
    }

    /// Fetch external inputs when the upstream implements the additive
    /// endpoint. Only a genuine 404 means an older server; availability,
    /// authorization and decoding failures remain errors.
    pub async fn get_external_inputs_optional(&self) -> Result<Option<Value>> {
        let url = format!("{}/external-inputs", self.api_url);
        let (_transport, clients, _slot) = self.active().await;
        let response = self
            .checked_send(
                clients.middleware.get(url).send(),
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

/// [`should_retry_status`] without the body: 500 and 503 are not retried
/// ([`InferenceRetryStrategy`]).
fn should_retry_status_unread(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 429 | 502 | 504)
}

/// A keep-alive timeout is not retried in place: the peer has already been
/// silent for a ping interval plus its timeout, and the job's re-queue is the
/// retry.
fn should_retry_error(err: &reqwest::Error) -> bool {
    !is_keep_alive_timeout(err)
        && (err.is_connect()
            || err.is_timeout()
            || is_refused_stream(err)
            || is_connection_closed(err)
            || is_connection_lost(err))
}

/// An h2 connection closed because its peer stopped answering pings. The
/// clients set no other hyper timeout; the probe's deadline is `reqwest`'s own.
fn is_keep_alive_timeout(err: &reqwest::Error) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(current) = source {
        if let Some(hyper) = current.downcast_ref::<hyper::Error>()
            && hyper.is_timeout()
        {
            return true;
        }
        source = current.source();
    }
    false
}

/// Awaits `request` without a deadline, calling `on_stall` with the time
/// waited after `first` and each time the wait doubles from there.
async fn await_warning_while_stalled<F: std::future::Future>(
    request: F,
    first: Duration,
    mut on_stall: impl FnMut(Duration),
) -> F::Output {
    let started = tokio::time::Instant::now();
    let mut request = std::pin::pin!(request);
    let mut waited = first;
    loop {
        match tokio::time::timeout_at(started + waited, request.as_mut()).await {
            Ok(output) => return output,
            Err(_) => on_stall(started.elapsed()),
        }
        waited = waited.saturating_mul(2);
    }
}

/// The phase a failed `send()` reached (always before the response head).
/// `reqwest`'s predicates overlap, so order matters.
fn send_phase(err: &reqwest::Error) -> TransportPhase {
    if err.is_connect() {
        TransportPhase::Connect
    } else if err.is_timeout() {
        TransportPhase::Headers
    } else {
        TransportPhase::Send
    }
}

/// `reqwest`'s name for an error; the most specific true claim wins.
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

/// The whole source chain, joined.
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

/// Whether a failed send invalidates the transport memo: a connect or request
/// error does, except a timeout (a silent peer, not a protocol), a refused
/// stream (proof of HTTP/2) and a first [`is_connection_closed`] (a keep-alive
/// race behind a proxy).
fn invalidates_transport_memo(err: &reqwest::Error, retrying: bool) -> bool {
    (err.is_connect() || err.is_request())
        && !err.is_timeout()
        && !is_refused_stream(err)
        && !(retrying && is_connection_closed(err))
}

/// hyper's "connection closed before message completed": routine behind a
/// proxy with a shorter keep-alive, and safe to resend when idempotent.
fn is_connection_closed(err: &reqwest::Error) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(current) = source {
        if let Some(hyper) = current.downcast_ref::<hyper::Error>()
            && hyper.is_incomplete_message()
        {
            return true;
        }
        source = current.source();
    }
    false
}

/// The other shapes of a connection dying under a sent request: a canceled
/// hyper error, or a `ConnectionReset`/`ConnectionAborted` I/O error. No answer
/// was begun, so an idempotent request can be resent.
fn is_connection_lost(err: &reqwest::Error) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(current) = source {
        if let Some(hyper) = current.downcast_ref::<hyper::Error>()
            && hyper.is_canceled()
        {
            return true;
        }
        if let Some(io) = current.downcast_ref::<std::io::Error>()
            && matches!(
                io.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
            )
        {
            return true;
        }
        source = current.source();
    }
    false
}

/// HTTP/2 `REFUSED_STREAM`: "not processed" (RFC 9113 §8.7), safe to retry.
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

/// Parse a predict response body. Only the JSON envelope can carry a typed
/// error slot.
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
/// errors. Base64 unwrapping only fires when the batch carried an error slot;
/// a batch mixing binary and JSON survivors is an error.
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
    // Typed, so a cooldown reaches the job with its kind intact.
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
pub(crate) mod tests {
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

    /// Held by the tests that open many sockets and by the one that bounds
    /// this process's descriptor growth, which the others would push over.
    static SOCKET_HEAVY: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
        /// What `/health` answers; `None` hangs.
        health: tokio::sync::watch::Sender<Option<StatusCode>>,
    }

    impl ConcurrencyProbe {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                in_flight: std::sync::atomic::AtomicUsize::new(0),
                peak: std::sync::atomic::AtomicUsize::new(0),
                peers: StdMutex::new(std::collections::HashSet::new()),
                gate: tokio::sync::watch::channel(false).0,
                health: tokio::sync::watch::channel(Some(StatusCode::OK)).0,
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
    /// recorded, so concurrency and sockets are measured. `/health` answers
    /// as `probe.health` says.
    async fn spawn_blocking_stub(probe: Arc<ConcurrencyProbe>, max_streams: u32) -> String {
        use std::sync::atomic::Ordering::SeqCst;

        let handler_probe = Arc::clone(&probe);
        let app = Router::new()
            .route(
                "/api/inference/cache",
                get(|| async { Json(serde_json::json!({"cache": {}})) }),
            )
            .route(
                "/api/inference/health",
                get(move || {
                    let health = *probe.health.borrow();
                    async move {
                        match health {
                            Some(status) => status,
                            None => std::future::pending().await,
                        }
                    }
                }),
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
        let _sockets = SOCKET_HEAVY.lock().await;

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

    /// A throwaway self-signed certificate for 127.0.0.1 and its PKCS#8 key,
    /// both DER.
    #[cfg(target_os = "linux")]
    fn test_certificate() -> &'static (Vec<u8>, Vec<u8>) {
        use openssl::{asn1, bn, ec, hash, nid, pkey, x509};

        static CERTIFICATE: OnceLock<(Vec<u8>, Vec<u8>)> = OnceLock::new();
        CERTIFICATE.get_or_init(|| {
            let group = ec::EcGroup::from_curve_name(nid::Nid::X9_62_PRIME256V1).unwrap();
            let key = pkey::PKey::from_ec_key(ec::EcKey::generate(&group).unwrap()).unwrap();
            let mut name = x509::X509NameBuilder::new().unwrap();
            name.append_entry_by_text("CN", "127.0.0.1").unwrap();
            let name = name.build();
            let mut cert = x509::X509::builder().unwrap();
            cert.set_version(2).unwrap();
            let serial = bn::BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap();
            cert.set_serial_number(&serial).unwrap();
            cert.set_subject_name(&name).unwrap();
            cert.set_issuer_name(&name).unwrap();
            cert.set_pubkey(&key).unwrap();
            cert.set_not_before(&asn1::Asn1Time::days_from_now(0).unwrap())
                .unwrap();
            cert.set_not_after(&asn1::Asn1Time::days_from_now(1).unwrap())
                .unwrap();
            let san = x509::extension::SubjectAlternativeName::new()
                .ip("127.0.0.1")
                .build(&cert.x509v3_context(None, None))
                .unwrap();
            cert.append_extension(san).unwrap();
            cert.sign(&key, hash::MessageDigest::sha256()).unwrap();
            (
                cert.build().to_der().unwrap(),
                key.private_key_to_pkcs8().unwrap(),
            )
        })
    }

    /// Every client this test binary builds trusts [`test_certificate`].
    #[cfg(target_os = "linux")]
    pub(super) fn trust_test_certificate(
        builder: reqwest::ClientBuilder,
    ) -> reqwest::ClientBuilder {
        let certificate = reqwest::Certificate::from_der(&test_certificate().0).unwrap();
        builder.add_root_certificate(certificate)
    }

    /// A TLS front for the cleartext `backend`: it offers `alpn`, relays the
    /// decrypted bytes to the backend, and counts the connections it accepts.
    #[cfg(target_os = "linux")]
    async fn spawn_tls_front(backend: &str, alpn: &[&[u8]]) -> (String, Arc<AtomicUsize>) {
        use tokio_rustls::rustls;

        let (cert, key) = test_certificate();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut config = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.clone().into()],
                rustls::pki_types::PrivatePkcs8KeyDer::from(key.clone()).into(),
            )
            .unwrap();
        config.alpn_protocols = alpn.iter().map(|protocol| protocol.to_vec()).collect();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let backend = backend.trim_start_matches("http://").to_owned();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&accepted);
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                counter.fetch_add(1, Relaxed);
                let (acceptor, backend) = (acceptor.clone(), backend.clone());
                tokio::spawn(async move {
                    let Ok(mut front) = acceptor.accept(socket).await else {
                        return;
                    };
                    let mut back = tokio::net::TcpStream::connect(backend).await.unwrap();
                    let _ = tokio::io::copy_bidirectional(&mut front, &mut back).await;
                });
            }
        });
        (format!("https://{addr}"), accepted)
    }

    /// Behind a TLS front the socket count is bounded the way it is in the
    /// clear. When ALPN picks h2, a burst costs one connection per recruited
    /// lane: a lane that negotiated per connection dialed once per request of
    /// a cold burst. When the front speaks only HTTP/1.1, an admitted request
    /// is one socket and no more, so the fixed gate
    /// (`both_transports_take_a_concurrency_permit`) is the bound; a burst past
    /// the gate would cost this process a thousand descriptors.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_tls_front_costs_the_sockets_the_cleartext_path_does() {
        use std::sync::atomic::Ordering::SeqCst;
        let _sockets = SOCKET_HEAVY.lock().await;

        let h2: &[&[u8]] = &[b"h2", b"http/1.1"];
        let h1: &[&[u8]] = &[b"http/1.1"];
        for (alpn, burst, transport, label) in [
            (h2, 2 * H2_STREAMS_PER_CONNECTION, Transport::H2c, "h2"),
            (h1, 32, Transport::Http11, "http/1.1"),
        ] {
            let probe = ConcurrencyProbe::new();
            let backend =
                spawn_blocking_stub(Arc::clone(&probe), crate::MAX_CONCURRENT_STREAMS).await;
            let (base_url, accepted) = spawn_tls_front(&backend, alpn).await;
            let client = InferenceApiClient::new_with_metadata_cache(base_url, false).unwrap();
            assert_eq!(client.transport().await, transport, "{label}: ALPN decides");
            let before = accepted.load(SeqCst);

            let mut inflight = tokio::task::JoinSet::new();
            for _ in 0..burst {
                let client = client.clone();
                inflight.spawn(async move {
                    client
                        .predict("g/model", "k", 1, 60, None, None, &[text_input("x")])
                        .await
                        .expect("the stub answers");
                });
            }
            let deadline = Instant::now() + Duration::from_secs(30);
            while probe.in_flight.load(SeqCst) < burst && Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            assert_eq!(probe.in_flight.load(SeqCst), burst, "{label}");
            let sockets = accepted.load(SeqCst) - before;
            probe.release(true);
            while let Some(result) = inflight.join_next().await {
                result.expect("no panic");
            }
            let expected = if transport.is_multiplexed() {
                burst.div_ceil(H2_STREAMS_PER_CONNECTION)
            } else {
                burst
            };
            assert_eq!(sockets, expected, "{label}: sockets for {burst} requests");
            let total = accepted.load(SeqCst) - before;
            assert_eq!(total, expected, "{label}: sockets once they all answered");
            let health = endpoint_health();
            let health = health
                .iter()
                .find(|endpoint| endpoint.base_url == client.api_url)
                .unwrap();
            assert_eq!(health.transport, label);
        }
    }

    /// A relay to the cleartext `backend` that stops passing bytes either way
    /// once `frozen` is set, holding both sockets open: a peer that froze, as
    /// seen from the client.
    async fn spawn_freezable_relay(
        backend: &str,
        frozen: tokio::sync::watch::Receiver<bool>,
    ) -> String {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        async fn pass(
            mut from: tokio::net::tcp::OwnedReadHalf,
            mut to: tokio::net::tcp::OwnedWriteHalf,
            frozen: tokio::sync::watch::Receiver<bool>,
        ) {
            let mut buffer = vec![0u8; 64 * 1024];
            while let Ok(read) = from.read(&mut buffer).await {
                if read == 0 {
                    return;
                }
                if *frozen.borrow() {
                    std::future::pending::<()>().await;
                }
                if to.write_all(&buffer[..read]).await.is_err() {
                    return;
                }
            }
        }

        let backend = backend.trim_start_matches("http://").to_owned();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((front, _)) = listener.accept().await {
                let back = tokio::net::TcpStream::connect(&backend).await.unwrap();
                let (front_read, front_write) = front.into_split();
                let (back_read, back_write) = back.into_split();
                tokio::spawn(pass(front_read, back_write, frozen.clone()));
                tokio::spawn(pass(back_read, front_write, frozen.clone()));
            }
        });
        format!("http://{addr}")
    }

    /// A peer that stops answering mid-request is found by the keep-alive
    /// ping instead of being waited on forever, and the request fails as a
    /// silent peer: typed for the job's re-queue, not retried in place, and
    /// no evidence about the protocol. The lane builder's own settings with
    /// the intervals shortened, so deleting the keep-alive from it fails this.
    #[tokio::test]
    async fn a_frozen_peer_fails_the_request_on_the_keep_alive_ping() {
        use std::sync::atomic::Ordering::SeqCst;

        let probe = ConcurrencyProbe::new();
        let backend = spawn_blocking_stub(Arc::clone(&probe), crate::MAX_CONCURRENT_STREAMS).await;
        let (freeze, frozen) = tokio::sync::watch::channel(false);
        let relay = spawn_freezable_relay(&backend, frozen).await;
        let short = Duration::from_millis(200);
        let client = h2_lane_builder(reqwest::Client::builder(), short, short)
            .build()
            .unwrap();
        let form = build_predict_form(&[text_input("x")]).await.unwrap();
        let request = tokio::spawn(
            client
                .post(format!("{relay}/api/inference/predict/g/model"))
                .multipart(form)
                .send(),
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        while probe.in_flight.load(SeqCst) == 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!request.is_finished(), "a busy peer answers its pings");
        freeze.send_replace(true);

        let err = tokio::time::timeout(Duration::from_secs(10), request)
            .await
            .expect("the ping finds the frozen peer")
            .expect("no panic")
            .expect_err("nothing answered");
        probe.release(true);
        assert!(is_keep_alive_timeout(&err), "{}", error_chain(&err));
        assert!(
            !should_retry_error(&err),
            "the peer has been silent already"
        );
        assert!(
            !invalidates_transport_memo(&err, false),
            "not a protocol fact"
        );
        let failure = InferenceFailure::from_transport(send_phase(&err), &err);
        assert_eq!(failure.transport_phase(), Some(TransportPhase::Headers));
        assert!(failure.warrants_resubmission());
    }

    const SHORT_HEALTH_CHECKS: HealthCheckTiming = HealthCheckTiming {
        after: Duration::from_millis(200),
        timeout: Duration::from_secs(1),
    };

    /// A client for `base_url` with [`SHORT_HEALTH_CHECKS`], on `transport`.
    pub(crate) async fn health_checked_client(
        base_url: &str,
        transport: Transport,
    ) -> InferenceApiClient {
        endpoint_runtime(
            &normalize_base_url(base_url.to_owned()),
            SHORT_HEALTH_CHECKS,
        )
        .unwrap();
        let client = InferenceApiClient::new_with_metadata_cache(base_url, false).unwrap();
        *client.endpoint.transport.write().await = Some(Remembered {
            transport,
            expires: None,
        });
        client
    }

    async fn checks_ended(client: &InferenceApiClient) {
        while client.endpoint.health_checks.lock().running {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn predict_one(client: InferenceApiClient) -> Result<PredictResponse> {
        client
            .predict("g/model", "k", 1, 60, None, None, &[text_input("x")])
            .await
    }

    /// A busy server that answers its health check is waited for however long
    /// its answer takes, on both transports, even when every other check
    /// misses: one miss is not a freeze, and an answer starts the count over.
    /// The server allows one stream per connection, so a check that shared
    /// the predict's connection would wait behind it.
    #[tokio::test]
    async fn a_busy_server_that_answers_its_health_check_is_never_cut_off() {
        for transport in [Transport::H2c, Transport::Http11] {
            let probe = ConcurrencyProbe::new();
            let url = spawn_blocking_stub(Arc::clone(&probe), 1).await;
            let client = health_checked_client(&url, transport).await;
            let request = tokio::spawn(predict_one(client.clone()));
            let mut verdict = client.endpoint.health_checks.verdict.subscribe();
            let started = Instant::now();
            // Each value is set just after a check, for the next one to read.
            for health in [502, 200, 502, 200] {
                let seen = verdict.borrow_and_update().checks;
                let now = *verdict.wait_for(|now| now.checks > seen).await.unwrap();
                assert!(now.frozen_since.is_none(), "{transport:?}: {now:?}");
                probe
                    .health
                    .send_replace(Some(StatusCode::from_u16(health).unwrap()));
            }
            assert!(
                started.elapsed() >= 3 * SHORT_HEALTH_CHECKS.timeout,
                "spaced"
            );
            assert!(!request.is_finished(), "{transport:?}: still waiting");
            probe.release(true);
            request.await.unwrap().expect("answered");
            assert_eq!(client.endpoint.health_checks.lock().stalled, 0);
        }
    }

    /// Behind a TLS front that accepts only h2, or only HTTP/1.1, the checks
    /// negotiate as the requests do, on a connection of their own: the busy
    /// server is waited for, and found frozen once it stops answering them.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn health_checks_reach_a_server_behind_a_single_version_tls_front() {
        let h2: &[&[u8]] = &[b"h2"];
        let h1: &[&[u8]] = &[b"http/1.1"];
        for (alpn, transport) in [(h2, Transport::H2c), (h1, Transport::Http11)] {
            let probe = ConcurrencyProbe::new();
            let backend = spawn_blocking_stub(Arc::clone(&probe), 1).await;
            let (url, _) = spawn_tls_front(&backend, alpn).await;
            let client = health_checked_client(&url, transport).await;
            let request = tokio::spawn(predict_one(client.clone()));
            let mut verdict = client.endpoint.health_checks.verdict.subscribe();
            for _ in 0..2 {
                let seen = verdict.borrow_and_update().checks;
                let now = *verdict.wait_for(|now| now.checks > seen).await.unwrap();
                assert!(now.frozen_since.is_none(), "{transport:?}: {now:?}");
            }
            assert!(!request.is_finished(), "{transport:?}: still waiting");
            probe.health.send_replace(None);
            let err = tokio::time::timeout(4 * SHORT_HEALTH_CHECKS.timeout, request)
                .await
                .unwrap_or_else(|_| panic!("{transport:?}: declared frozen"))
                .unwrap()
                .expect_err("cut off");
            assert!(
                inference_failure(&err).is_some_and(InferenceFailure::warrants_resubmission),
                "{transport:?}"
            );
            probe.health.send_replace(Some(StatusCode::OK));
            probe.release(true);
            checks_ended(&client).await;
        }
    }

    /// A check connection that stopped passing bytes is not asked again: once
    /// the path works, the next check dials a new connection and finds the
    /// server, on both transports.
    #[tokio::test]
    async fn a_wedged_check_connection_is_not_reused() {
        for transport in [Transport::H2c, Transport::Http11] {
            let probe = ConcurrencyProbe::new();
            let backend =
                spawn_blocking_stub(Arc::clone(&probe), crate::MAX_CONCURRENT_STREAMS).await;
            let (freeze, frozen) = tokio::sync::watch::channel(false);
            let url = spawn_freezable_relay(&backend, frozen).await;
            let client = health_checked_client(&url, transport).await;
            let request = tokio::spawn(predict_one(client.clone()));
            let mut verdict = client.endpoint.health_checks.verdict.subscribe();
            let now = *verdict.wait_for(|now| now.checks > 0).await.unwrap();
            assert!(now.frozen_since.is_none(), "{transport:?}: answered");
            freeze.send_replace(true);
            tokio::time::timeout(4 * SHORT_HEALTH_CHECKS.timeout, request)
                .await
                .unwrap_or_else(|_| panic!("{transport:?}: declared frozen"))
                .unwrap()
                .expect_err("cut off");
            checks_ended(&client).await;

            freeze.send_replace(false);
            let deadline = Instant::now() + 3 * SHORT_HEALTH_CHECKS.timeout;
            while client.recheck_if_frozen().is_some() {
                assert!(Instant::now() < deadline, "{transport:?}: found again");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            probe.release(true);
        }
    }

    /// How a case freezes the server.
    #[derive(Clone, Copy, Debug)]
    enum Freeze {
        /// Behind a proxy: requests and `/health` hang while the connection,
        /// its HTTP/2 pings included, stays answered.
        HealthHangs,
        /// The proxy answers `/health` itself with 502.
        BadGateway,
        /// No bytes pass either way, as with a stopped process.
        Relay,
    }

    /// A server that stops answering its health check is declared frozen: the
    /// request waiting on it fails as a keep-alive timeout fails it, new ones
    /// fail without being sent, and it is sent requests again once it answers.
    #[tokio::test]
    async fn a_frozen_server_fails_its_requests_until_it_answers_again() {
        let cases = [
            (Transport::H2c, Freeze::HealthHangs),
            (Transport::Http11, Freeze::HealthHangs),
            (Transport::H2c, Freeze::BadGateway),
            (Transport::Http11, Freeze::Relay),
        ];
        fn within<F: Future>(future: F) -> tokio::time::Timeout<F> {
            tokio::time::timeout(3 * SHORT_HEALTH_CHECKS.timeout, future)
        }
        let ((), log) = logs_during(async {
            for (transport, freeze) in cases {
                let case = format!("{transport:?} {freeze:?}");
                let probe = ConcurrencyProbe::new();
                let backend =
                    spawn_blocking_stub(Arc::clone(&probe), crate::MAX_CONCURRENT_STREAMS).await;
                let (relay_freeze, relay_frozen) = tokio::sync::watch::channel(false);
                let url = spawn_freezable_relay(&backend, relay_frozen).await;
                let client = health_checked_client(&url, transport).await;
                let request = tokio::spawn(predict_one(client.clone()));
                let deadline = Instant::now() + Duration::from_secs(10);
                while probe.peak() == 0 && Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                probe.health.send_replace(match freeze {
                    Freeze::HealthHangs => None,
                    Freeze::BadGateway => Some(StatusCode::BAD_GATEWAY),
                    Freeze::Relay => Some(StatusCode::OK),
                });
                relay_freeze.send_replace(matches!(freeze, Freeze::Relay));

                let err = within(request)
                    .await
                    .unwrap_or_else(|_| panic!("{case}: declared frozen"))
                    .unwrap()
                    .expect_err("cut off");
                let failure = inference_failure(&err).expect("typed");
                assert_eq!(
                    failure.transport,
                    Some(TransportFailure {
                        phase: TransportPhase::Headers,
                        class: "timeout",
                    }),
                    "{case}"
                );
                assert!(failure.warrants_resubmission(), "{case}");

                // This request carries the next check and fails on its verdict.
                checks_ended(&client).await;
                let err = within(predict_one(client.clone()))
                    .await
                    .unwrap_or_else(|_| panic!("{case}: fails fast"))
                    .expect_err("frozen");
                assert!(
                    inference_failure(&err).is_some_and(InferenceFailure::warrants_resubmission),
                    "{case}"
                );
                let err = within(client.get_metadata())
                    .await
                    .unwrap_or_else(|_| panic!("{case}: fails fast"))
                    .expect_err("frozen");
                let upstream = crate::inference_errors::UpstreamFailure::classify(&err);
                assert_eq!(
                    upstream.as_ref().map(|failure| failure.message(&url)),
                    Some(format!(
                        "Could not reach the inference server at {url}: {PeerFrozen}"
                    )),
                    "{case}"
                );
                assert_eq!(
                    upstream.map(|failure| failure.status()),
                    Some(StatusCode::GATEWAY_TIMEOUT)
                );
                assert_eq!(probe.peak(), 1, "{case}: nothing sent while frozen");

                probe.health.send_replace(Some(StatusCode::OK));
                relay_freeze.send_replace(false);
                probe.release(true);
                checks_ended(&client).await;
                predict_one(client.clone())
                    .await
                    .unwrap_or_else(|err| panic!("{case}: answered again: {err:#}"));
            }
        })
        .await;
        let count = |needle: &str| log.lines().filter(|line| line.contains(needle)).count();
        assert_eq!(count("health checks in a row;"), cases.len(), "{log}");
        assert_eq!(
            count("answers its health check again"),
            cases.len(),
            "{log}"
        );
    }

    /// A miss is not forgotten when the requests that started the checks end
    /// first, as when the keep-alive fails them: the checks go on until an
    /// answer or a verdict, so a re-sent request is cut off `after` + 2 x
    /// `timeout` into the first stall, not a whole stall later. Paused time,
    /// against a peer that never answers, so every check misses at its deadline.
    #[tokio::test(start_paused = true)]
    async fn a_miss_outlives_the_requests_that_started_the_checks() {
        let url = format!("http://{}", spawn_raw_peer(RawPeer::Silent).await);
        let client = InferenceApiClient::new_with_metadata_cache(url, false).unwrap();
        let stall = |client: InferenceApiClient| {
            tokio::spawn(async move { client.until_answered(std::future::pending::<()>()).await })
        };
        let started = tokio::time::Instant::now();
        let first = stall(client.clone());
        // Ends between the first check's start and its miss.
        tokio::time::sleep(HEALTH_CHECKS.after + HEALTH_CHECKS.timeout / 2).await;
        first.abort();
        let resent = stall(client.clone());
        assert!(resent.await.unwrap().is_err(), "cut off by the verdict");
        let bound = HEALTH_CHECKS.after + (HEALTH_CHECK_MISSES + 1) * HEALTH_CHECKS.timeout;
        assert!(started.elapsed() <= bound, "{:?}", started.elapsed());
    }

    /// A request with no answer is reported each time its wait doubles and
    /// awaited to its end.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_request_is_reported_and_never_cut_off() {
        let every = Duration::from_secs(60);
        let mut waited = Vec::new();
        let answer = await_warning_while_stalled(
            async {
                tokio::time::sleep(Duration::from_secs(500)).await;
                7
            },
            every,
            |elapsed| waited.push(elapsed.as_secs()),
        )
        .await;
        assert_eq!(answer, 7);
        assert_eq!(waited, vec![60, 120, 240, 480]);
        let mut calls = 0;
        await_warning_while_stalled(async {}, every, |_| calls += 1).await;
        assert_eq!(calls, 0);
    }

    /// What this thread logs at INFO and above while `body` runs.
    async fn logs_during<F: std::future::Future>(body: F) -> (F::Output, String) {
        #[derive(Clone)]
        struct Sink(Arc<StdMutex<Vec<u8>>>);
        impl std::io::Write for Sink {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        crate::test_utils::install_ask_every_event();
        let sink = Sink(Arc::new(StdMutex::new(Vec::new())));
        let writer = sink.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let output = {
            let _guard = tracing::subscriber::set_default(subscriber);
            body.await
        };
        let log = String::from_utf8_lossy(&sink.0.lock().unwrap()).into_owned();
        (output, log)
    }

    /// One INFO line per endpoint when its transport is chosen, with the
    /// reason, and none when a re-probe finds the same one.
    #[tokio::test]
    async fn the_chosen_transport_is_logged_once_with_its_reason() {
        let probe = ConcurrencyProbe::new();
        let h2c = spawn_blocking_stub(probe, crate::MAX_CONCURRENT_STREAMS).await;
        let http11 = format!("http://{}", spawn_raw_peer(RawPeer::Http11).await);
        let ((), log) = logs_during(async {
            let client = InferenceApiClient::new_with_metadata_cache(h2c, false).unwrap();
            assert_eq!(client.transport().await, Transport::H2c);
            client.forget_transport().await;
            assert_eq!(client.transport().await, Transport::H2c);
            let client = InferenceApiClient::new_with_metadata_cache(http11, false).unwrap();
            assert_eq!(client.transport().await, Transport::Http11);
        })
        .await;
        let chosen: Vec<&str> = log
            .lines()
            .filter(|line| line.contains("inference transport chosen"))
            .collect();
        assert_eq!(chosen.len(), 2, "{log}");
        assert!(
            chosen[0].contains(r#"transport="h2c""#)
                && chosen[0].contains("reason=\"the peer answered HTTP/2 with prior knowledge\""),
            "{}",
            chosen[0]
        );
        assert!(
            chosen[1].contains(r#"transport="http/1.1""#)
                && chosen[1].contains("refused HTTP/2 twice and answered HTTP/1.1"),
            "{}",
            chosen[1]
        );
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
        /// Answers the first request keep-alive and then reads the second
        /// and closes: the keep-alive race a proxy loses, deterministically.
        CloseOnReuse,
        /// Answers the request with a TCP reset, which is what a peer torn
        /// down under a request does.
        ResetOnRequest,
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
                    RawPeer::CloseOnReuse => {
                        let mut scratch = [0u8; 4096];
                        let _ = socket.read(&mut scratch).await;
                        let body = br#"{"cache":{}}"#;
                        let head = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                             content-length: {}\r\n\r\n",
                            body.len()
                        );
                        let _ = socket.write_all(head.as_bytes()).await;
                        let _ = socket.write_all(body).await;
                        let _ = socket.read(&mut scratch).await;
                        drop(socket);
                    }
                    RawPeer::ResetOnRequest => {
                        // Closed with the request still unread in the receive
                        // queue, which Linux answers with an RST, not a FIN.
                        let _ = socket.readable().await;
                        drop(socket);
                    }
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

    /// A proxy's 502, 503 and 504 are misses; any other status, and a check
    /// that cannot be made, are not.
    #[tokio::test]
    async fn which_health_check_outcomes_are_misses() {
        let probe = ConcurrencyProbe::new();
        let url = spawn_blocking_stub(Arc::clone(&probe), crate::MAX_CONCURRENT_STREAMS).await;
        let client = InferenceApiClient::new_with_metadata_cache(url, false).unwrap();
        for status in [200, 403, 404, 500, 501, 502, 503, 504] {
            probe
                .health
                .send_replace(Some(StatusCode::from_u16(status).unwrap()));
            let missed = client.health_check().await.is_err();
            assert_eq!(missed, (502..=504).contains(&status), "{status}");
        }
        let url = format!("http://{}", closed_port().await);
        let client = InferenceApiClient::new_with_metadata_cache(url, false).unwrap();
        assert_eq!(client.health_check().await, Ok(()), "refused");
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

    /// Both ends advertise the same two fixed windows, which is what a peer
    /// reads off the wire: the stream window as a SETTING, the connection
    /// window as the WINDOW_UPDATE that opens it past the spec's 65 535.
    /// Lanes are recruited by load, so below 64 concurrent predicts every
    /// body shares one connection and one window, and over a LAN that window
    /// is the throughput per round trip. `adaptive_window` would put both of
    /// these back to 65 535 and grow them only as its pings are acked, which
    /// cost 35-50 % on loopback.
    #[tokio::test]
    async fn both_ends_advertise_the_fixed_windows() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        /// RFC 9113 §6.9.2's initial window, which every connection starts at.
        const SPEC_WINDOW: u32 = 65_535;
        /// `SETTINGS_INITIAL_WINDOW_SIZE`.
        const INITIAL_WINDOW_SIZE: u16 = 0x0004;

        /// The stream window a peer names in its SETTINGS and the connection
        /// window it opens on stream 0. Only the first is a setting: the
        /// connection's is the spec's window plus the WINDOW_UPDATE the peer
        /// sends with it, and reading that frame is what ends the wait.
        async fn advertised_windows(
            socket: &mut tokio::net::TcpStream,
            preface: usize,
        ) -> (u32, u32) {
            let mut skip = vec![0u8; preface];
            socket.read_exact(&mut skip).await.expect("the peer writes");
            let mut stream_window = SPEC_WINDOW;
            loop {
                let mut header = [0u8; 9];
                socket
                    .read_exact(&mut header)
                    .await
                    .expect("a frame header");
                let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
                let mut payload = vec![0u8; length];
                socket
                    .read_exact(&mut payload)
                    .await
                    .expect("a frame payload");
                match header[3] {
                    // SETTINGS.
                    0x04 => {
                        if let Some(entry) = payload.as_chunks::<6>().0.iter().find(|entry| {
                            u16::from_be_bytes([entry[0], entry[1]]) == INITIAL_WINDOW_SIZE
                        }) {
                            stream_window =
                                u32::from_be_bytes([entry[2], entry[3], entry[4], entry[5]]);
                        }
                    }
                    // WINDOW_UPDATE, which for the connection is stream 0.
                    0x08 if header[5..9] == [0, 0, 0, 0] => {
                        let increment =
                            u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                        return (stream_window, SPEC_WINDOW + increment);
                    }
                    _ => {}
                }
            }
        }

        let windows = async |socket: &mut tokio::net::TcpStream, preface: usize| {
            tokio::time::timeout(
                std::time::Duration::from_secs(10),
                advertised_windows(socket, preface),
            )
            .await
            .expect("the peer opens its windows")
        };

        // This binary's own server, answering a bare h2 preface.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            crate::serve_with_stream_limit(listener, Router::new(), std::future::pending()).await
        });
        let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
        socket
            .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\0\0\0\x04\0\0\0\0\0")
            .await
            .unwrap();
        assert_eq!(
            windows(&mut socket, 0).await,
            (crate::H2_STREAM_WINDOW, crate::H2_CONNECTION_WINDOW),
            "the windows this server advertises"
        );

        // This binary's own inference client, whose preface comes first.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let client =
            InferenceApiClient::new_with_metadata_cache(format!("http://{peer}"), false).unwrap();
        let connecting = tokio::spawn(async move {
            let _ = client.get_cached_models().await;
        });
        let (mut socket, _) = listener.accept().await.unwrap();
        assert_eq!(
            windows(&mut socket, 24).await,
            (crate::H2_STREAM_WINDOW, crate::H2_CONNECTION_WINDOW),
            "the windows this client advertises"
        );
        connecting.abort();
    }

    /// One probe per endpoint, however many callers find the memo empty at
    /// once. Without it a dropped memo costs one three-request probe per
    /// request in flight — 256 of them for one closed idle connection.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_transport_probe_is_single_flighted() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::SeqCst;

        const CALLERS: usize = 64;
        let probes = Arc::new(AtomicUsize::new(0));
        let handler_probes = Arc::clone(&probes);
        let app = Router::new()
            .route(
                "/api/inference/cache",
                get(move || {
                    let probes = Arc::clone(&handler_probes);
                    async move {
                        probes.fetch_add(1, SeqCst);
                        // Long enough that every caller below is waiting on
                        // the memo rather than arriving after it.
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        Json(json!({"cache": {}}))
                    }
                }),
            )
            .route("/api/inference/metadata", get(|| async { Json(json!({})) }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            crate::serve_with_stream_limit(listener, app, std::future::pending()).await
        });

        let client = InferenceApiClient::new_with_metadata_cache(base_url, false).unwrap();
        let mut calls = tokio::task::JoinSet::new();
        for _ in 0..CALLERS {
            let client = client.clone();
            calls.spawn(async move { client.get_metadata().await });
        }
        while let Some(result) = calls.join_next().await {
            result.expect("no panic").expect("the stub answers");
        }
        assert_eq!(probes.load(SeqCst), 1, "{CALLERS} callers, one probe");
        assert_eq!(client.known_transport(), Some(Transport::H2c));
    }

    /// A connection closed under a request that had been sent is a keep-alive
    /// race, not a verdict on the protocol. It is retried, and it only speaks
    /// about the memo once the retries are out.
    #[tokio::test]
    async fn a_closed_connection_is_retried_before_it_speaks_about_the_memo() {
        let addr = spawn_raw_peer(RawPeer::CloseOnReuse).await;
        let client = reqwest::Client::builder().http1_only().build().unwrap();
        let url = format!("http://{addr}/cache");
        let first = client
            .get(&url)
            .send()
            .await
            .expect("the first is answered");
        // Read to the end, so the connection goes back to the pool and the
        // second request is the one that races the close.
        first.text().await.expect("the first body arrives");

        let err = client
            .get(&url)
            .send()
            .await
            .expect_err("the peer closed the connection under the second");
        assert!(is_connection_closed(&err), "{}", error_chain(&err));
        assert!(should_retry_error(&err), "the request reached no handler");
        assert!(
            !invalidates_transport_memo(&err, true),
            "a retry is still to come, so it is not evidence yet"
        );
        assert!(
            invalidates_transport_memo(&err, false),
            "out of retries, the same failure is evidence"
        );
    }

    /// The probe is taken under `probe_lock` on clients with no request
    /// timeout, so a peer that accepts and never answers would hold every
    /// caller of that endpoint behind the prober. The probe has a deadline.
    #[tokio::test]
    async fn a_silent_peer_cannot_hold_the_probe_open() {
        let addr = spawn_raw_peer(RawPeer::Silent).await;
        let client =
            InferenceApiClient::new_with_metadata_cache(format!("http://{addr}"), false).unwrap();
        let started = std::time::Instant::now();
        let transport = tokio::time::timeout(PROBE_TIMEOUT * 3, client.transport())
            .await
            .expect("the probe answers within its own deadline");
        assert!(started.elapsed() >= PROBE_TIMEOUT, "it waited for the peer");
        assert_eq!(
            transport,
            Transport::Http11,
            "the caller is not left waiting"
        );
        assert_eq!(
            client.known_transport(),
            Some(Transport::Http11),
            "a timeout is a network fact, so what it records is provisional"
        );
    }

    /// A peer persistently slower than the probe's deadline. Without a memo
    /// every non-coalesced call pays a fresh probe and the endpoint never
    /// multiplexes; the memo is provisional, so the peer is asked again once
    /// it expires rather than being written off for the process.
    #[tokio::test]
    async fn a_slow_peer_is_probed_once_and_then_again_after_the_memo_expires() {
        let (addr, requests) = spawn_slow_peer().await;
        let client =
            InferenceApiClient::new_with_metadata_cache(format!("http://{addr}"), false).unwrap();
        assert_eq!(client.transport().await, Transport::Http11);
        assert_eq!(requests.load(Relaxed), 1, "one probe, and it timed out");
        let expires = client
            .endpoint
            .transport
            .read()
            .await
            .expect("the timeout was recorded")
            .expires
            .expect("provisionally, not settled");
        assert!(
            expires > Instant::now() && expires <= Instant::now() + PROVISIONAL_MEMO_TTL,
            "the memo carries the TTL"
        );
        // The memo stands: the next call pays no probe, and the peer answers
        // it over HTTP/1.1 in its own time.
        assert_eq!(
            client.get_cached_models().await.expect("HTTP/1.1 answers"),
            serde_json::json!({"cache": {}})
        );
        assert_eq!(
            requests.load(Relaxed),
            2,
            "the request itself, and no second probe"
        );
        // Past the expiry the peer is asked again rather than written off.
        *client.endpoint.transport.write().await = Some(Remembered {
            transport: Transport::Http11,
            expires: Some(Instant::now() - Duration::from_secs(1)),
        });
        assert_eq!(client.known_transport(), None, "the memo lapsed");
        assert_eq!(client.transport().await, Transport::Http11);
        assert_eq!(requests.load(Relaxed), 3, "which costs a fresh probe");
    }

    /// A peer that answers `/cache` correctly, but always a second later than
    /// the probe is willing to wait. Hands back its address and the number of
    /// requests it has been sent.
    async fn spawn_slow_peer() -> (SocketAddr, Arc<AtomicUsize>) {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&seen);
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let counter = Arc::clone(&counter);
                tokio::spawn(async move {
                    let mut scratch = [0u8; 4096];
                    if socket.read(&mut scratch).await.unwrap_or(0) == 0 {
                        return;
                    }
                    counter.fetch_add(1, Relaxed);
                    tokio::time::sleep(PROBE_TIMEOUT + Duration::from_secs(1)).await;
                    let body = br#"{"cache":{}}"#;
                    let head = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                         content-length: {}\r\nconnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(head.as_bytes()).await;
                    let _ = socket.write_all(body).await;
                    let _ = socket.shutdown().await;
                });
            }
        });
        (addr, seen)
    }

    /// A peer that reads the request and then resets the connection is the
    /// same transient class as a close, and the middleware every non-predict
    /// call goes through has to see it that way: without it `load_model`,
    /// `unload`, `clear_cache` and `metadata` fail on the first reset.
    #[tokio::test]
    async fn a_reset_under_a_request_is_retried_by_the_middleware() {
        let addr = spawn_raw_peer(RawPeer::ResetOnRequest).await;
        let client = reqwest::Client::builder().http1_only().build().unwrap();
        let err = client
            .get(format!("http://{addr}/cache"))
            .send()
            .await
            .expect_err("the peer resets the connection");
        assert!(
            !is_connection_closed(&err),
            "a reset is not hyper's IncompleteMessage: {}",
            error_chain(&err)
        );
        assert!(is_connection_lost(&err), "{}", error_chain(&err));
        assert!(should_retry_error(&err), "{}", error_chain(&err));
        assert!(
            matches!(
                InferenceRetryStrategy.handle(&Err(reqwest_middleware::Error::Reqwest(err))),
                Some(Retryable::Transient)
            ),
            "the middleware retries it"
        );
    }

    /// The non-predict endpoints answer through the retry middleware, and a
    /// load refusal must reach the caller on the first answer: a `503` is the
    /// cooldown naming when to come back, and a `500` can be a load that just
    /// spent the worker's load deadline, so three more are three more spawns.
    #[tokio::test]
    async fn a_refused_load_is_answered_once_and_not_retried() {
        use axum::extract::Path;
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::SeqCst;

        let attempts = Arc::new(AtomicUsize::new(0));
        let handler_attempts = Arc::clone(&attempts);
        let app = Router::new().route(
            "/api/inference/load/{group}/{model}",
            axum::routing::put(move |Path((_group, model)): Path<(String, String)>| {
                let attempts = Arc::clone(&handler_attempts);
                async move {
                    attempts.fetch_add(1, SeqCst);
                    let json = [(axum::http::header::CONTENT_TYPE, "application/json")];
                    if model == "cooling" {
                        return (
                            StatusCode::SERVICE_UNAVAILABLE,
                            json,
                            r#"{"detail":{"kind":"load_cooldown","model":"g/cooling"}}"#,
                        );
                    }
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        json,
                        r#"{"detail":"Failed to load model"}"#,
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            crate::serve_with_stream_limit(listener, app, std::future::pending()).await
        });

        let client = InferenceApiClient::new_with_metadata_cache(base_url, false).unwrap();
        for (model, cooldown) in [("cooling", true), ("broken", false)] {
            attempts.store(0, SeqCst);
            let err = client
                .load_model(&format!("g/{model}"), "k", 1, 60, None)
                .await
                .expect_err("the load is refused");
            assert_eq!(attempts.load(SeqCst), 1, "{model}: asked once");
            let failure = inference_failure(&err).expect("typed through the context chain");
            assert_eq!(failure.is_load_cooldown(), cooldown, "{model}: {failure}");
        }
    }

    /// Over TLS the version is ALPN's to choose, so the probe negotiates: a
    /// front that chose HTTP/1.1 would be handed the h2 preface by a
    /// prior-knowledge client. The lanes assume h2 on both schemes, and are
    /// used only once the probe has found it. Asserted against an
    /// HTTP/1.1-only peer, which is what such a front looks like from here.
    #[tokio::test]
    async fn only_tls_endpoints_negotiate() {
        assert!(is_tls_endpoint("HTTPS://mixed-case"));
        assert!(!is_tls_endpoint("http://cleartext"));
        assert!(!is_tls_endpoint("https:/"));

        let addr = spawn_raw_peer(RawPeer::Http11).await;
        let url = format!("http://{addr}/cache");
        for (base_url, negotiates) in [
            ("https://tls-endpoint.invalid", true),
            ("http://cleartext-endpoint.invalid", false),
        ] {
            let client = InferenceApiClient::new_with_metadata_cache(base_url, false).unwrap();
            assert_eq!(client.endpoint.tls, negotiates, "{base_url}");
            let negotiating = client.endpoint.negotiating.as_ref();
            assert_eq!(negotiating.is_some(), negotiates, "{base_url}");
            if let Some(negotiating) = negotiating {
                assert!(negotiating.get(&url).send().await.is_ok(), "{base_url}");
            }
            let lane = client.endpoint.h2_seed.raw.get(&url).send().await;
            assert!(lane.is_err(), "{base_url}: a lane assumes h2");
        }
    }

    /// A request takes a gate permit on **both** transports, from a gate that
    /// starts at [`INFERENCE_MAX_CONCURRENT_REQUESTS`], asserted on the permit
    /// itself.
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
            *runtime.transport.write().await = Some(Remembered {
                transport,
                expires: None,
            });

            let gate = Arc::clone(&runtime.gate(Some(transport)).permits);
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

    /// Both gates follow the endpoint's published figure between the floor and
    /// their ceilings, the HTTP/1.1 one within the descriptor budget, and a
    /// shrink lands even while every permit is out: `forget_permits` can only
    /// take what is available, so the deficit is repaid on the release path.
    #[tokio::test]
    async fn a_gate_shrink_lands_through_releases_not_only_through_free_permits() {
        let client =
            InferenceApiClient::new_with_metadata_cache("http://gate-shrink-test", false).unwrap();
        let runtime = Arc::clone(&client.endpoint);
        *runtime.transport.write().await = Some(Remembered {
            transport: Transport::H2c,
            expires: None,
        });
        let permits = || runtime.h2_gate.permits.available_permits();
        let h1_permits = || runtime.h1_gate.permits.available_permits();
        assert_eq!(permits(), INFERENCE_MAX_CONCURRENT_REQUESTS);
        assert_eq!(h1_permits(), INFERENCE_MAX_CONCURRENT_REQUESTS);

        // Growth up to the ceiling and no further, then back to the floor —
        // the constant every deployment already runs at, so a small published
        // figure can never throttle one.
        client.observe_desired_in_flight(1_632);
        assert_eq!(permits(), 1_632);
        let h1_ceiling = http1_gate_ceiling(crate::rlimit::soft_nofile_limit());
        assert_eq!(h1_permits(), 1_632.min(h1_ceiling));
        client.observe_desired_in_flight(u64::MAX);
        assert_eq!(permits(), INFERENCE_MAX_CONCURRENT_STREAMS);
        assert_eq!(h1_permits(), h1_ceiling);
        client.observe_desired_in_flight(1);
        assert_eq!(permits(), INFERENCE_MAX_CONCURRENT_REQUESTS);
        assert_eq!(h1_permits(), INFERENCE_MAX_CONCURRENT_REQUESTS);
        // The HTTP/1.1 ceiling: two descriptors a request after the reserve,
        // between the floor and the h2c ceiling.
        let (floor, ceiling) = (
            INFERENCE_MAX_CONCURRENT_REQUESTS,
            INFERENCE_MAX_CONCURRENT_STREAMS,
        );
        let unknown = crate::rlimit::NOFILE_LIMIT_UNKNOWN;
        for (soft_nofile, expected) in [
            (512, floor),
            (1024, 384),
            (8448, ceiling),
            (unknown, ceiling),
        ] {
            assert_eq!(http1_gate_ceiling(soft_nofile), expected, "{soft_nofile}");
        }

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
            runtime.h2_gate.snapshot(),
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
            runtime.h2_gate.snapshot(),
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
        let (_target, in_flight) = runtime.h2_gate.snapshot();
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
