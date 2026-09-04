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
/// position of the *input* it belongs to, which is not the position of any
/// output: erroring slots are removed from `PredictResponse::outputs`, so the
/// survivors close ranks.
#[derive(Debug, Clone)]
pub(crate) struct PredictSlotError {
    pub index: usize,
    pub class: SlotErrorClass,
    pub message: String,
}

/// A predict response: the outputs of the inputs that succeeded, plus the
/// typed per-slot failures of the ones that did not. `errors` is empty for
/// every response an inference server without per-item error slots can
/// produce, which is what keeps this backward compatible.
#[derive(Debug)]
pub(crate) struct PredictResponse {
    pub outputs: PredictOutput,
    pub errors: Vec<PredictSlotError>,
    /// The orchestrator's desired in-flight figure for this model, in items
    /// ([`DESIRED_IN_FLIGHT_HEADER`]). `None` when the server did not say —
    /// a Python-era inference server, a model that has not dispatched a
    /// window yet, or a value that is unparsable or zero (the orchestrator
    /// never publishes zero, and reading one as a figure would ask a caller
    /// to keep no work in flight at all). Callers treat `None` as "no
    /// opinion" and keep their own floor.
    pub desired_in_flight_items: Option<u64>,
}

/// Response header the local orchestrator publishes the figure on
/// (`inferio::http::DESIRED_IN_FLIGHT_HEADER`; documented in
/// `docs/inferio-worker-protocol.md`).
pub(crate) const DESIRED_IN_FLIGHT_HEADER: &str = "x-panoptikon-desired-in-flight-items";

/// `detail.kind` of a predict that failed because the inference worker
/// process died with the request in flight
/// (`inferio::http::WORKER_DIED_KIND`). The request's items were never
/// attempted, so re-submitting them is correct — see run1 finding F7.
pub(crate) const WORKER_DIED_KIND: &str = "worker_died";

/// `detail.kind` of a request refused because the model is inside its
/// per-model load-failure cooldown. Unlike every other 503 this must **not**
/// be retried: the server is telling the caller when to come back, and a job
/// that keeps asking only burns the cooldown's whole window one request at a
/// time.
pub(crate) const LOAD_COOLDOWN_KIND: &str = "load_cooldown";

/// `detail.kind` of a predict the server never parsed because its **request
/// body did not arrive in full** (`inferio::http::REQUEST_INCOMPLETE_KIND`).
///
/// It rides on a 400, and it is the one 400 that must not be read as a
/// verdict: the status is right about the request and says nothing about the
/// items, which were never handed to a model. Run2 defect P2 was one of
/// these, recorded against an image that is perfectly fine.
pub(crate) const REQUEST_INCOMPLETE_KIND: &str = "request_incomplete";

/// `detail.kind` of a predict the server refused to **read** because it was
/// already holding its whole predict-body budget in memory
/// (`inferio::http::BODY_BUDGET_KIND`).
///
/// It rides on a 503 with a `Retry-After`, and it says the same thing the
/// other two unattempted kinds say: the body was never parsed, so the items
/// in it were never handed to a model. The difference is only in what the
/// caller should expect — this one clears on its own as the bodies ahead of
/// it finish parsing, which is why the server can name a retry delay at all.
pub(crate) const BODY_BUDGET_KIND: &str = "body_budget_exhausted";

/// A request the inference server refused, with the machine-readable half of
/// its `{"detail": …}` body parsed out.
///
/// It is a typed error (attached to the returned `anyhow::Error`, so callers
/// reach it with `downcast_ref`) rather than a string, because two callers
/// act on it: an extraction job re-queues a [`WORKER_DIED_KIND`] request's
/// items once instead of recording them as failures, and aborts outright on
/// [`LOAD_COOLDOWN_KIND`]. Both decisions have to survive the error being
/// wrapped in context on the way up, and neither may depend on prose.
///
/// `kind` is `None` for every failure that answered with the plain string
/// detail — an older server, an unrelated 4xx/5xx — which is the
/// "no machine-readable opinion" case every caller already handled.
#[derive(Debug, Clone)]
pub(crate) struct InferenceFailure {
    /// HTTP status of the refusal.
    pub status: u16,
    /// `detail.kind`, when the body carried a structured detail.
    pub kind: Option<String>,
    /// Human-readable summary: `detail.message` for a structured detail, the
    /// whole `detail` for a plain string one, and the raw body when it is
    /// neither.
    pub message: String,
    /// The model the failure is about, `group/name`.
    pub model: Option<String>,
    /// The last error that put the model in this state (a cooldown), or the
    /// fatal error chain (a worker death).
    pub last_error: Option<String>,
    /// RFC 3339 instant the model may be retried at.
    pub retry_at: Option<String>,
    /// Consecutive load failures counted so far.
    pub failures: Option<u32>,
    /// `Retry-After`, in seconds, when the server sent one.
    pub retry_after_secs: Option<u64>,
}

impl InferenceFailure {
    /// Parse one refused response. Never fails: a body this cannot read is
    /// still a failure, it just carries no machine-readable half.
    ///
    /// Visible to the rest of the crate so the local service's own tests can
    /// check what they answer *as the job's client reads it* — the two sides
    /// of a wire contract are only tested together if one test runs both.
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

    /// **The request's items were never attempted.** The three kinds that say
    /// so are different accidents — a worker that died holding the request, a
    /// request body that stopped arriving, and a body the server had no room
    /// to read — but they are one fact for every caller: nothing was run,
    /// nothing was decided about the media, and re-submitting the work is the
    /// only answer that is not a lie about it. Callers act on this rather
    /// than on any one kind, which is what made the third one a constant and
    /// a line rather than a change of policy.
    ///
    /// Deliberately keyed on the typed kind and never on the status. An
    /// untyped 4xx is not evidence of anything — a stock FastAPI upstream
    /// answers 400 for a genuinely bad request too — and retrying every one
    /// of them would double the request cost of any systematic client bug
    /// while fixing nothing.
    pub fn is_unattempted(&self) -> bool {
        self.is_worker_death() || self.is_request_incomplete() || self.is_body_budget_exhausted()
    }

    /// The model is inside its per-model load-failure cooldown.
    pub fn is_load_cooldown(&self) -> bool {
        self.kind.as_deref() == Some(LOAD_COOLDOWN_KIND)
    }
}

impl std::fmt::Display for InferenceFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "inference request failed ({})", self.status)?;
        if let Some(kind) = &self.kind {
            write!(f, " [{kind}]")?;
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

/// How this client talks to one inference endpoint.
///
/// Local inference is loopback HTTP *inside this process*, so an in-flight
/// predict has always cost two descriptors in one table — the client socket
/// and the accepted server socket. Under HTTP/1.1 that cost is per concurrent
/// request, which is what made a 2 000-item job exhaust the shipped
/// container's descriptor table (run1 blocker F6: 983 sockets, 1 849 items
/// unprocessed). Under HTTP/2 every request is a *stream* on a pooled
/// connection, so the cost stops scaling with the window.
///
/// One path, not two: the gateway and the inference server genuinely run on
/// different machines in real deployments (a NAS driving a GPU box), so the
/// answer has to be the same locally and remotely. Prior knowledge rather
/// than an h2c upgrade because there is no TLS to carry ALPN and the upgrade
/// dance costs a round trip per connection; a server that does not speak it
/// answers nothing an HTTP/2 client can read, which is exactly the signal the
/// one-time probe uses to fall back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Transport {
    /// HTTP/2 cleartext with prior knowledge.
    H2c,
    /// HTTP/1.1, one connection per concurrent request.
    Http11,
}

impl Transport {
    /// Whether requests share connections. The extraction job's descriptor
    /// clamp is a different quantity in the two modes.
    pub fn is_multiplexed(self) -> bool {
        matches!(self, Transport::H2c)
    }
}

/// Independent HTTP/2 connections ("lanes") this client may hold to one
/// inference endpoint.
///
/// **This used to be `pool_max_idle_per_host` on a single `reqwest::Client`,
/// and it bought exactly one socket.** For HTTP/2, hyper-util's pool hands
/// every caller the *same* connection — `Reservation::Shared`, with
/// `can_share() == is_http2()` — and its readiness test is "the dispatch
/// channel is open", not "there is stream capacity", so it never opens a
/// second connection under load however wide the window gets. Run2's
/// `S2-wdvit` leg is the receipt: a pool of 4, one socket, and every predict
/// on it queueing behind the peer's stream limit. The constant's own doc
/// comment described behaviour the code did not have.
///
/// So each lane is now its own `reqwest::Client` with its own pool, which is
/// the only way to make the number real. One lane is one connection —
/// hyper-util also dedups concurrent h2 connects per pool key, so a burst
/// cannot fan a lane into several sockets.
///
/// **Why 64.** It is the multiplier that makes the client's own gate
/// (`lanes x` [`H2_STREAMS_PER_CONNECTION`] = 4 096 requests) reach the job's
/// own in-flight ceiling — `jobs::extraction::in_flight_unit_ceiling` admits
/// 4 096 units at the shipped defaults, and for an `item`/`count` model
/// (every image tagger and CLIP embedder) one unit is one request. Below that
/// the client would be a ceiling nothing else in the system knows about,
/// which is the entire defect this fixes.
///
/// **What it costs.** Lanes are recruited by load, not spread across
/// ([`EndpointRuntime::pick_lane`]): the concurrency has to exceed
/// `H2_STREAMS_PER_CONNECTION` before a second socket is opened at all, so
/// the descriptor cost is `ceil(in_flight / 64)` sockets and never more than
/// 64. Local inference is loopback inside this process, so the worst case is
/// `2 x 64 = 128` descriptors, against `jobs::extraction`'s `FD_RESERVE` of
/// 256 and the shipped container's soft limit of 1 024.
pub(crate) const INFERENCE_CONNECTION_LANES: usize = 64;

/// Streams this client offers **one** h2 connection before recruiting the
/// next lane.
///
/// It is now enforced rather than assumed: [`EndpointRuntime::pick_lane`]
/// recruits lanes by load, so a lane is offered more than this only once
/// every lane is at it. That matters because a peer's real limit is invisible
/// to us — `reqwest` exposes no way to read a peer's
/// `SETTINGS_MAX_CONCURRENT_STREAMS`, and offering more streams than it
/// allows does not fail, it silently *queues inside h2* where neither the
/// dispatcher nor `/health` can see it. That invisible queue is precisely
/// what froze run2's calibration ramp.
///
/// 64 is below every common server default (nginx 128, Envoy 100, hyper 200,
/// and this binary's own [`crate::MAX_CONCURRENT_STREAMS`] of 512), so the
/// streams we offer one connection are ones a peer will actually run.
const H2_STREAMS_PER_CONNECTION: usize = 64;

/// The **floor** of the h2c concurrency gate, and the fixed HTTP/1.1 gate.
///
/// Everything past the gate queues on a semaphore — which is the point: a
/// queued request holds no socket, where an admitted HTTP/1.1 one does.
///
/// Under **HTTP/1.1** this is the whole story and it does not move: each
/// admitted request is a socket, this is the bound run1 blocker F6 needed
/// (983 sockets, 1 849 items unprocessed), and it must never follow a model's
/// batching advice. `256 = 4 x 64`, four connections' worth, which is what
/// this constant meant before lanes existed.
///
/// Under **h2c** it is only the floor: see
/// [`EndpointRuntime::set_in_flight_target`]. A queued request there costs a
/// *stream*, not a descriptor, and the descriptor cost is bounded by
/// [`INFERENCE_CONNECTION_LANES`] however high the gate goes — so keeping the
/// gate at a constant did not protect descriptors, it only capped throughput
/// at a number the rest of the system could not see.
pub(crate) const INFERENCE_MAX_CONCURRENT_REQUESTS: usize = 4 * H2_STREAMS_PER_CONNECTION;

/// The **ceiling** of the h2c gate: the point past which admitting more would
/// mean offering some lane more streams than [`H2_STREAMS_PER_CONNECTION`],
/// i.e. handing the peer a queue it never advertised room for.
pub(crate) const INFERENCE_MAX_CONCURRENT_STREAMS: usize =
    INFERENCE_CONNECTION_LANES * H2_STREAMS_PER_CONNECTION;

/// One independent HTTP/2 connection to an endpoint, and how much work is on
/// it right now.
#[derive(Debug)]
struct Lane {
    clients: EndpointClients,
    in_flight: AtomicUsize,
}

/// The permits the h2c gate wants to exist, and the shrink it has not been
/// able to apply yet. Same rule as `jobs::extraction::UnitBudget`: a shrink
/// never takes a permit away from a request already in flight, it withholds
/// permits as they come back.
#[derive(Debug)]
struct GateState {
    target: usize,
    pending_shrink: usize,
}

/// The clients, the lanes and the shared state of one inference endpoint.
///
/// Shared per base URL across every [`InferenceApiClient`] for that endpoint,
/// which is load-bearing rather than tidy: a connection pool that is not
/// shared is not a bound. Before this, each client instance built its own
/// `reqwest::Client` with its own pool.
#[derive(Debug)]
struct EndpointRuntime {
    /// [`INFERENCE_CONNECTION_LANES`] independent h2 clients, each its own
    /// pool and therefore its own connection.
    h2: Vec<Lane>,
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
    /// The lane a new request goes on: the least loaded of the lanes the
    /// current load actually requires.
    ///
    /// **Not** the least loaded of all lanes, which is the obvious reading
    /// and the wrong one: spreading 64 concurrent predicts over 64 lanes
    /// costs 64 sockets, which is HTTP/1.1's price with extra steps and
    /// undoes the whole reason this transport was adopted (run1 blocker F6).
    /// So lanes are *recruited*: the prefix `0..k` is the smallest that can
    /// hold the load at [`H2_STREAMS_PER_CONNECTION`] streams each, and the
    /// choice is least-loaded within it. Concurrency of 64 uses one socket,
    /// 4 096 uses all 64, and the descriptor cost tracks the work instead of
    /// the lane count.
    ///
    /// Least-loaded *within* the prefix rather than "first with room"
    /// because h2 streams on one connection share a TCP window: a lane
    /// carrying a slow batch should not also be handed the next one.
    ///
    /// Racy by design — loads move under it — and harmlessly so: every
    /// outcome is a valid lane, and the counters are exact again as soon as
    /// the burst settles.
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

    /// Follow the desired-in-flight figure the endpoint published, in
    /// requests.
    ///
    /// Track E made the gate a fixed constant for three reasons, and this
    /// leg falsified one of them: "256 is four times a job's in-flight budget
    /// (4 096 units at 64 units per request)" is only true for a model whose
    /// items carry 64 work units each. An image item carries **one**, so the
    /// job sends one item per request and 4 096 units is 4 096 concurrent
    /// requests — for every image tagger and CLIP embedder, which are exactly
    /// the models this feature exists for. 256 was 1/16 of the budget, not
    /// 4x it. The other two reasons stand, and they are what the clamps here
    /// preserve:
    ///
    /// - **the floor** is [`INFERENCE_MAX_CONCURRENT_REQUESTS`], so this can
    ///   only ever *raise* the gate above the value every existing deployment
    ///   already runs at;
    /// - **the ceiling** is [`INFERENCE_MAX_CONCURRENT_STREAMS`], so a
    ///   published figure can never make this client offer a lane more
    ///   streams than it was designed to, and never moves the descriptor cost
    ///   at all (that is bounded by the lane count, not by the gate);
    /// - **HTTP/1.1 is untouched.** Its gate is a different semaphore, fixed,
    ///   because there an admitted request is a socket and a model's batching
    ///   advice must never move this process's descriptor usage.
    ///
    /// The figure is in *items* and the gate counts *requests*. Using it
    /// directly is deliberate and conservative in the safe direction: for the
    /// models that matter one item is one request, and for a model that packs
    /// several units per item it over-provisions a bound whose only cost is
    /// permits, never sockets. The job's own `UnitBudget` remains the
    /// throttle; this is a safety bound that had become a throughput cap.
    ///
    /// Several models share one endpoint, so this is last-writer-wins. That
    /// is acceptable exactly because of the floor: the worst a small model
    /// can do to a large one is put the gate back to the constant this code
    /// shipped with.
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
    /// a shrink is outstanding.
    ///
    /// Dropping the permit would not do: `Semaphore` hands a released permit
    /// straight to a waiter, and a saturated job always has waiters, so
    /// `forget_permits` alone can never land a shrink (the same defect S1b
    /// fixes in `jobs::extraction::UnitBudget`).
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

    /// The gate's current target and how many of it are in use, for
    /// `/health`.
    fn gate_snapshot(&self, transport: Option<Transport>) -> (usize, usize) {
        match transport {
            Some(Transport::Http11) => (
                INFERENCE_MAX_CONCURRENT_REQUESTS,
                INFERENCE_MAX_CONCURRENT_REQUESTS.saturating_sub(self.h1_gate.available_permits()),
            ),
            _ => {
                let target = self
                    .h2_gate_state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .target;
                (
                    target,
                    target.saturating_sub(self.h2_gate.available_permits()),
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

    /// Lanes currently carrying at least one request — the number of sockets
    /// this endpoint is actually using, for `/health`.
    fn lanes_in_use(&self) -> usize {
        self.h2
            .iter()
            .filter(|lane| lane.in_flight.load(Relaxed) > 0)
            .count()
    }
}

/// What one inference endpoint's client is doing right now, for `/health`.
///
/// It exists because run2's S1 could not be diagnosed from `/health` at all:
/// the transport in force, the connections it was really using and the
/// concurrency it was really allowing were each either absent or, in the one
/// log line that mentioned them, wrong. Everything here is a *measured*
/// quantity, not a constant restated.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct InferenceTransportHealth {
    /// The endpoint this describes.
    pub base_url: String,
    /// `h2c` | `http/1.1` | `unknown` (nothing has talked to it yet, so the
    /// job-side descriptor budget reads it as the conservative HTTP/1.1
    /// case).
    pub transport: String,
    /// Independent connections this client may hold to the endpoint; `null`
    /// under HTTP/1.1, where a connection is a request rather than a pool
    /// slot.
    pub pool_connections: Option<usize>,
    /// Of those, how many are carrying at least one request right now — the
    /// sockets actually in use. `null` under HTTP/1.1.
    pub connections_in_use: Option<usize>,
    /// Requests the gate currently admits. Under h2c this follows the
    /// endpoint's own published desired-in-flight figure between a floor and
    /// a ceiling; under HTTP/1.1 it is fixed.
    pub max_concurrent_requests: usize,
    /// Of those, how many are in flight right now.
    pub in_flight_requests: usize,
}

/// One admitted request's claim on an endpoint: the gate permit it holds and
/// the lane it was placed on. Both are returned by `Drop`, so every exit path
/// — success, error, retry backoff, cancellation — accounts for itself.
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
    let mut lanes = Vec::with_capacity(INFERENCE_CONNECTION_LANES);
    for _ in 0..INFERENCE_CONNECTION_LANES {
        lanes.push(Lane {
            // One idle connection per lane: a lane *is* a connection, and
            // hyper-util shares it across every request on that client.
            clients: EndpointClients::build(reqwest::ClientBuilder::http2_prior_knowledge, 1)?,
            in_flight: AtomicUsize::new(0),
        });
    }
    let runtime = Arc::new(EndpointRuntime {
        h2: lanes,
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
///
/// Read off the shared endpoint registry rather than off any one pool, so the
/// report covers the job pool, the PQL path and the preload loop alike — they
/// are all the same runtime per base URL, which is the whole point of the
/// registry. Empty on a node that only *serves* inference (`panoptikon
/// inferio`), which is correct: it has no client.
///
/// The registry mutex is taken normally — it is a `std::sync::Mutex` held for
/// the few instructions it takes to look up or insert a runtime, never across
/// an await — but each endpoint's *transport* is read with `try_read`: a
/// health probe must never wait on a transport probe that is mid-flight, so an
/// endpoint being resolved right now reports `unknown` rather than delaying
/// the report. (`try_lock` on the registry was tried first and is wrong: under
/// any concurrent client construction it reports an empty client section,
/// which is exactly the kind of silently-absent number S1 was about.)
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
    /// The probe is a real request to a real endpoint (`GET /cache`, the
    /// cheapest thing this surface serves) sent with HTTP/2 prior knowledge.
    /// *Any* answer proves the peer speaks it — a 404 or a 500 is as good as
    /// a 200, because reading the status at all means the frames parsed.
    ///
    /// **A downgrade is only ever recorded on positive evidence**, because it
    /// is recorded once and nothing but a predict-time connection error clears
    /// it: one wrong memo costs the endpoint its multiplexing for the lifetime
    /// of the process, and halves every job's in-flight window with it through
    /// `requests_are_multiplexed`. That matters most in the deployment this
    /// exists for — the gateway and the inference server on different machines
    /// — where first contact crosses a network and blips are ordinary.
    ///
    /// So a failed probe is not evidence on its own. `reqwest` cannot tell
    /// "the peer rejected the h2 preface" from "the connection died mid-stream"
    /// — both are `Kind::Request` — so the ambiguous class
    /// ([`Self::could_be_an_http2_refusal`]) is resolved by asking twice more:
    /// the h2 probe is repeated (a reset that happens twice in a row is not a
    /// blip), and then the peer must *answer over HTTP/1.1*, which proves it is
    /// alive and therefore that its refusal was about the protocol. Anything
    /// short of that records nothing and re-probes on the next call.
    async fn transport(&self) -> Transport {
        if let Some(transport) = *self.endpoint.transport.read().await {
            return transport;
        }
        let transport = match self.probe_h2c().await {
            Ok(()) => Transport::H2c,
            Err(err) if !Self::could_be_an_http2_refusal(&err) => {
                // Unreachable, not un-multiplexed. Nothing is remembered, so
                // the next call probes again; this attempt uses HTTP/1.1
                // because it is the half of the guess that also works against
                // an h2c server (the auto builder serves both), so a peer that
                // comes back healthy is never stuck on a wrong memo.
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
            // Every figure here is one this client can actually deliver: the
            // lanes are independent connections, `max_concurrent` is the
            // gate's floor and `max_concurrent_ceiling` the most a published
            // desired-in-flight figure can raise it to. The line this
            // replaces claimed a concurrency of 256 while the transport was
            // silently allowing 200.
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

    /// One h2c probe: `GET /cache`, the cheapest thing this surface serves,
    /// sent with HTTP/2 prior knowledge. The body is never read — a status is
    /// already proof that the frames parsed.
    async fn probe_h2c(&self) -> reqwest::Result<()> {
        // Lane 0: the probe is one request, and the lane it opens is the one
        // the first real requests will land on anyway.
        self.endpoint.h2[0]
            .clients
            .raw
            .get(format!("{}/cache", self.base_url))
            .send()
            .await
            .map(|_| ())
    }

    /// Whether the peer answers the same request over HTTP/1.1 — the proof
    /// that it is alive, and therefore that its refusal of the h2 preface was
    /// about the protocol rather than about the network. Any status counts,
    /// for the same reason the h2c probe accepts any status.
    async fn peer_answers_http11(&self) -> bool {
        self.endpoint
            .h1
            .raw
            .get(format!("{}/cache", self.base_url))
            .send()
            .await
            .is_ok()
    }

    /// Whether a failed probe *could* be the peer refusing HTTP/2, as opposed
    /// to the peer not being reachable at all.
    ///
    /// An HTTP/1.1-only server accepts the TCP connection and then rejects the
    /// h2 preface, so the failure happens after connect: it is neither
    /// `is_connect` nor `is_timeout`. Connection-refused, DNS failure and a
    /// timeout all say something about the *network*, and a network fact must
    /// never be recorded as a protocol fact — reqwest classifies a timeout as
    /// `Kind::Request` too, so without this check a slow endpoint would
    /// permanently downgrade itself.
    ///
    /// Only "could": the same `Kind::Request` also covers a connection that
    /// died mid-stream, which says nothing about the protocol either. That is
    /// why a true answer is the *start* of the decision in [`Self::transport`],
    /// never the whole of it.
    fn could_be_an_http2_refusal(err: &reqwest::Error) -> bool {
        !err.is_connect() && !err.is_timeout()
    }

    /// The transport already resolved for this endpoint, without probing.
    /// `None` means nothing has talked to it yet, which callers that size
    /// resource budgets must read as the conservative HTTP/1.1 case.
    pub fn known_transport(&self) -> Option<Transport> {
        self.endpoint
            .transport
            .try_read()
            .ok()
            .and_then(|guard| *guard)
    }

    /// Clears the remembered transport so the next request re-probes. Called
    /// on a connection error: a server can be restarted into a build that
    /// speaks a different protocol, and a fallback that never re-examines
    /// itself is a permanent downgrade after one blip.
    async fn forget_transport(&self) {
        *self.endpoint.transport.write().await = None;
    }

    /// Every non-predict call's send result, funnelled through one place so
    /// that a transport-level failure invalidates the memo here too.
    ///
    /// `predict` does this inline (it owns a retry loop and needs the error
    /// afterwards); without the same rule on the rest, a memo can be stale
    /// *upward* and stay that way forever. Concretely: a peer remembered as
    /// h2c that comes back behind an HTTP/1.1-only proxy — the NAS-to-GPU-box
    /// deployment can grow one — fails `load_model` on every job from then on,
    /// and a job that fails at load never reaches the predict that would have
    /// cleared the memo. One gateway restart is the only other way out.
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
    /// keeps requests queueing on the pool instead of opening sockets.
    ///
    /// The permit is taken in **both** transports, on
    /// [`INFERENCE_MAX_CONCURRENT_REQUESTS`]. Gating only h2c would leave the
    /// HTTP/1.1 path — the one where a request costs a whole socket — as the
    /// only unbounded one, and it is reachable *after* a job has already sized
    /// its in-flight window for multiplexing: `in_flight_unit_ceiling` is
    /// evaluated once, before the item loop, so a peer restarted mid-job into
    /// a build without HTTP/2 flips the transport under a window sized at the
    /// full byte budget. That is run1 blocker F6's `EMFILE` with extra steps.
    ///
    /// It cannot throttle an existing deployment: 256 concurrent requests is
    /// four times a job's default in-flight budget (4096 units at 64 units
    /// per request), so nothing that fits the descriptor clamp can reach it —
    /// only the case the clamp does *not* cover, a model whose requests carry
    /// very few units each, which is exactly the case that exhausts sockets.
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
                let clients = self.endpoint.h2[lane].clients.clone();
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
        // Per-request cap on server-side batch merging (design doc §6):
        // only sent when the caller has an opinion. Old Python inference
        // servers (FastAPI) ignore unknown query params, so sending this
        // to a pre-max_batch upstream is harmless.
        if let Some(max_batch) = max_batch {
            query.push(("max_batch", max_batch.to_string()));
        }
        // Lazy prewarm hint (design doc §8): absent = true on the server,
        // so only callers with an opinion (extraction jobs: false) send it.
        // Equally ignored by old Python servers.
        if let Some(prewarm) = prewarm {
            query.push(("prewarm", prewarm.to_string()));
        }
        let mut attempts: u32 = 0;
        loop {
            let form = build_predict_form(inputs).await?;
            // Resolved per attempt, and the lease is held for exactly the
            // request — every `continue` below drops it *before* it waits out
            // its backoff. A retry that kept its gate permit and its lane
            // claim across the wait would hold a concurrency slot for seconds
            // while doing nothing, and would do it precisely when the server
            // has just said it is overloaded. Re-resolving also matters: a
            // connection error between attempts may have changed the
            // transport.
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
                        // Additive, optional: absent (or unparsable) leaves
                        // the caller on its own floor. Read before the body
                        // is consumed, which drops the response.
                        let desired = response
                            .headers()
                            .get(DESIRED_IN_FLIGHT_HEADER)
                            .and_then(|value| value.to_str().ok())
                            .and_then(|value| value.trim().parse::<u64>().ok())
                            .filter(|value| *value > 0);
                        let body = response.bytes().await?.to_vec();
                        let mut parsed = parse_predict_response(&content_type, &body)?;
                        parsed.desired_in_flight_items = desired;
                        return Ok(parsed);
                    }

                    let status = response.status();
                    let retry_after = retry_after_secs(response.headers());
                    // Read before deciding: the body is what says whether a
                    // 503 is the transient one this loop retries or the
                    // cooldown that must be handed straight to the caller.
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
                    // A transport failure invalidates what the probe learned:
                    // the peer may have been restarted into a build that
                    // speaks the other protocol, and a fallback nothing ever
                    // re-examines is a permanent downgrade after one blip.
                    //
                    // A refused stream is the exception, and the memo has to
                    // be *kept* for it: it is `Kind::Request` like the rest,
                    // but it is positive proof the peer speaks HTTP/2 — only
                    // an h2 peer can send RST_STREAM. Forgetting the memo
                    // there would make a peer with a small stream limit
                    // re-probe on every burst.
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
                    warn!(%url, error = %err, "inference predict request failed");
                    return Err(err).context("inference predict request failed");
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
        // Lazy prewarm hint (design doc §8), same absent-means-true rule as
        // on predict; old Python servers ignore it.
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
    /// endpoint. Only a genuine 404 means an older unsupported server;
    /// availability, authorization, and decoding failures remain errors.
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

/// Whether the peer refused to *open* the stream — HTTP/2 `REFUSED_STREAM`.
///
/// RFC 9113 §8.7 makes this the one reset that is unambiguously safe to
/// retry: it means the request "was not processed", which is the same
/// assertion `worker_died` and `request_incomplete` make (`7e96de62`). So it
/// earns the same recovery.
///
/// **It is reachable in ordinary operation, not only under abuse.** hyper's
/// client opens up to `DEFAULT_INITIAL_MAX_SEND_STREAMS` = 100 streams on a
/// new connection *before* the peer's `SETTINGS_MAX_CONCURRENT_STREAMS` frame
/// has been read (`hyper::proto::h2::client`), and `reqwest` exposes no way
/// to lower that. A burst opened the instant a lane connects to a peer — or a
/// proxy — that advertises fewer than 100 therefore has some of its streams
/// refused, every time, until the SETTINGS land. This was measured, not
/// imagined: `a_peer_with_a_small_stream_limit_costs_no_extra_sockets` failed
/// exactly this way against a stub advertising 16, and the job would have
/// recorded those items as permanently failed.
///
/// The chain is walked rather than matched on a string, so it depends on h2's
/// types instead of h2's prose.
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

/// Also used by the local orchestrator's HTTP tests as the parity oracle:
/// whatever `inferio::http` encodes must be parseable by this exact logic.
///
/// The binary encodings cannot carry a typed error slot, so only the JSON
/// envelope is inspected for them; a malformed one is an error rather than a
/// payload, mirroring the orchestrator's strictness on the msgpack hop.
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
/// errors.
///
/// The base64 unwrapping only happens when the batch *did* carry an error
/// slot: without error slots an all-binary batch is encoded as
/// `multipart/mixed`, never as this envelope, so the rule can only ever fire
/// on the new shape and every response an older server can produce is passed
/// through byte-identically to before.
///
/// Whether a survivor is binary is decided *per slot* — the encoder wraps
/// every binary output and leaves every JSON output alone, so wrappedness is
/// the per-slot record of what the model returned for that input. Since
/// `PredictOutput` is one type for the whole response, a batch mixing the two
/// is a response this client cannot represent, and it is reported as such
/// rather than handed on: passed through as JSON, the wrapper map reaches an
/// output handler that reads no `transcription` from it and silently drops
/// the payload.
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
    // job with its kind intact, exactly like a predict that does.
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

    /// Descriptors this process currently holds. Linux only, which is where
    /// the descriptor budget is a real limit and where run1's blocker F6 was
    /// measured; the count includes the accepted server ends, because the
    /// stub server in these tests runs in this same process exactly as local
    /// inference does.
    #[cfg(target_os = "linux")]
    fn open_fds() -> usize {
        std::fs::read_dir("/proc/self/fd")
            .expect("/proc/self/fd is readable on Linux")
            .count()
    }

    /// A local inference stub over `axum::serve`, i.e. the same hyper-util
    /// auto builder the gateway serves its own inference surface with. It
    /// answers `/cache` (the client's transport probe) and `/predict/...`
    /// after a delay, so requests overlap.
    async fn spawn_stub_service(delay: Duration) -> String {
        let app = Router::new()
            .route(
                "/api/inference/cache",
                get(|| async { Json(serde_json::json!({"cache": {}})) }),
            )
            .route(
                "/api/inference/predict/{group}/{id}",
                post(move || async move {
                    tokio::time::sleep(delay).await;
                    Json(serde_json::json!({"outputs": [{"ok": true}]}))
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    /// R10': concurrent predicts against an h2c endpoint are streams on a
    /// bounded connection pool, so they cost a *constant* number of
    /// descriptors instead of two each.
    ///
    /// This is run1 blocker F6 turned into an assertion: there, 2 000 items
    /// drove the gateway to 983 sockets against a 1024 limit and the job
    /// could not finish. Here 64 concurrent predicts must cost far fewer
    /// descriptors than the 128 that one-socket-pair-per-request would need
    /// — the bound being `2 x INFERENCE_POOL_CONNECTIONS`, plus slack for
    /// the runtime's own churn while the sample is taken.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_predicts_share_the_connection_pool() {
        const CONCURRENT: usize = 64;

        let base_url = spawn_stub_service(Duration::from_millis(400)).await;
        let client = InferenceApiClient::new_with_metadata_cache(base_url, false).unwrap();
        // The probe, and one request, so the pool and every lazy allocation
        // the runtime makes on the first round trip are already paid for.
        client
            .predict("g/model", "k", 1, 60, None, None, &[text_input("warm")])
            .await
            .expect("the stub answers");
        assert_eq!(
            client.known_transport(),
            Some(Transport::H2c),
            "axum::serve must accept HTTP/2 cleartext with prior knowledge"
        );
        let baseline = open_fds();

        let mut inflight = tokio::task::JoinSet::new();
        for _ in 0..CONCURRENT {
            let client = client.clone();
            inflight.spawn(async move {
                client
                    .predict("g/model", "k", 1, 60, None, None, &[text_input("x")])
                    .await
                    .map(|_| ())
            });
        }
        // Sampled while the requests are in flight: the stub's delay is long
        // enough that they overlap, and the peak is what matters.
        let mut peak = baseline;
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            peak = peak.max(open_fds());
        }
        while let Some(result) = inflight.join_next().await {
            result.expect("no panic").expect("the stub answers");
        }

        let growth = peak.saturating_sub(baseline);
        let per_request_cost = CONCURRENT * 2;
        assert!(
            growth < per_request_cost,
            "{CONCURRENT} concurrent predicts grew the descriptor table by {growth}, \
             which is not better than one socket pair each ({per_request_cost})"
        );
        // The real bound: both ends of at most the pooled connections, plus
        // a little slack for whatever else the runtime opened in the window
        // the sample covers.
        let pool_cost = 2 * INFERENCE_CONNECTION_LANES;
        assert!(
            growth <= pool_cost + 8,
            "descriptor growth {growth} exceeds the pool's {pool_cost} plus slack"
        );
    }

    /// `hyper`'s default `SETTINGS_MAX_CONCURRENT_STREAMS` for an HTTP/2
    /// *server* (`hyper::proto::h2::server`), which `axum::serve` does not
    /// override. Not a panoptikon number and not a panoptikon decision: it is
    /// written down here only so the test that measures it can name what it
    /// found.
    const HYPER_DEFAULT_MAX_CONCURRENT_STREAMS: usize = 200;

    /// What one endpoint's concurrency actually is, measured at the server's
    /// own handler: how many predicts are inside it at once, and over how
    /// many TCP connections they arrived.
    struct ConcurrencyProbe {
        in_flight: std::sync::atomic::AtomicUsize,
        peak: std::sync::atomic::AtomicUsize,
        entered: std::sync::atomic::AtomicUsize,
        peers: StdMutex<std::collections::HashSet<SocketAddr>>,
        release: tokio::sync::watch::Sender<bool>,
    }

    impl ConcurrencyProbe {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                in_flight: std::sync::atomic::AtomicUsize::new(0),
                peak: std::sync::atomic::AtomicUsize::new(0),
                entered: std::sync::atomic::AtomicUsize::new(0),
                peers: StdMutex::new(std::collections::HashSet::new()),
                release: tokio::sync::watch::channel(false).0,
            })
        }

        fn peak(&self) -> usize {
            self.peak.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn sockets(&self) -> usize {
            self.peers.lock().expect("probe mutex").len()
        }
    }

    /// A stub inference endpoint whose predict handler *blocks* until the
    /// test releases it, served through the very serve loop the gateway uses
    /// for `/api/inference`. Every predict that reaches the handler is
    /// counted, and its peer address recorded, so the test can read both the
    /// stream concurrency the server allowed and the number of connections
    /// the shipped client opened to carry it.
    async fn spawn_blocking_stub(probe: Arc<ConcurrencyProbe>) -> String {
        spawn_blocking_stub_with_streams(probe, crate::MAX_CONCURRENT_STREAMS).await
    }

    /// The same stub, advertising a stream limit of the caller's choosing —
    /// the only way to stand in for a peer (or a proxy) less generous than
    /// this binary.
    async fn spawn_blocking_stub_with_streams(
        probe: Arc<ConcurrencyProbe>,
        max_streams: u32,
    ) -> String {
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
                            probe.entered.fetch_add(1, SeqCst);
                            let now = probe.in_flight.fetch_add(1, SeqCst) + 1;
                            probe.peak.fetch_max(now, SeqCst);
                            let mut released = probe.release.subscribe();
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

    /// **S1: what actually bounds concurrent predicts, measured rather than
    /// assumed.**
    ///
    /// The run2 `S2-wdvit` leg published a desired-in-flight figure of 1 632
    /// items and never got more than 200 predicts onto the wire, on every
    /// window, for the whole job. 200 is not a panoptikon number: it was
    /// `hyper`'s default server `SETTINGS_MAX_CONCURRENT_STREAMS`, which
    /// `axum::serve` never overrode, and `INFERENCE_POOL_CONNECTIONS = 4` did
    /// not multiply it because hyper-util's pool shares **one** h2 connection
    /// per host. Neither number was written down anywhere, logged, or visible
    /// on `/health`; the client's own log line claimed 256.
    ///
    /// This test is that ceiling turned into an assertion, at both ends at
    /// once: the server's handler counts how many predicts are inside it
    /// concurrently, and records the peer address of each so the connection
    /// count is measured rather than inferred. It offers far more requests
    /// than any of the bounds involved, so whichever bound is smallest is
    /// what it reads back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_server_and_the_pool_bound_concurrent_predicts() {
        // Comfortably above every bound in play (the client gate, the
        // server's stream limit, the pool), so the smallest one is what the
        // handler sees.
        const OFFERED: usize = 400;

        let probe = ConcurrencyProbe::new();
        let base_url = spawn_blocking_stub(Arc::clone(&probe)).await;
        let client = InferenceApiClient::new_with_metadata_cache(base_url, false).unwrap();

        let mut inflight = tokio::task::JoinSet::new();
        for _ in 0..OFFERED {
            let client = client.clone();
            inflight.spawn(async move {
                client
                    .predict("g/model", "k", 1, 60, None, None, &[text_input("x")])
                    .await
                    .map(|_| ())
            });
        }

        // Let the concurrency settle: sample until the peak stops moving for
        // a full second, with a hard deadline so a regression cannot hang the
        // suite.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let mut last = usize::MAX;
        let mut stable = 0;
        while std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let peak = probe.peak();
            if peak == last && peak > 0 {
                stable += 1;
                if stable >= 10 {
                    break;
                }
            } else {
                stable = 0;
                last = peak;
            }
        }

        let peak = probe.peak();
        let sockets = probe.sockets();
        probe.release.send_replace(true);
        let mut answered = 0usize;
        while let Some(result) = inflight.join_next().await {
            result.expect("no panic").expect("the stub answers");
            answered += 1;
        }
        assert_eq!(answered, OFFERED, "every offered predict must complete");

        // Before S1-2 this read `HYPER_DEFAULT_MAX_CONCURRENT_STREAMS` (200):
        // hyper's silent server default, below the client's own gate and
        // named nowhere. With the limit made explicit at
        // `crate::MAX_CONCURRENT_STREAMS` the server is no longer the bound,
        // and what the handler sees is the number this process actually
        // decided on.
        assert!(
            crate::MAX_CONCURRENT_STREAMS as usize > INFERENCE_MAX_CONCURRENT_REQUESTS,
            "the server's stream limit must not be the tightest bound on our \
             own client's concurrency"
        );
        assert_eq!(
            peak, INFERENCE_MAX_CONCURRENT_REQUESTS,
            "the concurrency that reached the handler must be the client's own \
             gate, not the transport's silent default \
             ({HYPER_DEFAULT_MAX_CONCURRENT_STREAMS}) and not what was offered \
             ({OFFERED})"
        );
        // Before S1-3 this read `sockets == 1`: `INFERENCE_POOL_CONNECTIONS`
        // was `pool_max_idle_per_host` on a single client, and hyper-util
        // shares one h2 connection per host, so the pool of four was one
        // socket. Lanes are independent clients, and they are recruited by
        // load rather than spread across — so the connection count is the
        // concurrency divided by the per-lane stream budget, which is the
        // multiplexing promise stated as an equation.
        assert_eq!(
            sockets,
            peak.div_ceil(H2_STREAMS_PER_CONNECTION),
            "lanes must be recruited by load: {peak} concurrent requests at \
             {H2_STREAMS_PER_CONNECTION} streams each"
        );
        assert!(
            sockets <= INFERENCE_CONNECTION_LANES,
            "{sockets} connections exceeds the lane count"
        );
    }

    /// A peer that advertises **less** than we offer a lane.
    ///
    /// This is the deployment the memo warns about and the one no test
    /// covered: the gateway on a NAS, the inference server on a GPU box, and
    /// a proxy in between that advertises its own
    /// `SETTINGS_MAX_CONCURRENT_STREAMS`. The client cannot read that number
    /// — `reqwest` exposes no way to — so the requirement is not "match it"
    /// but "survive it": the surplus waits inside h2 rather than failing, and
    /// it must not turn into sockets.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_peer_with_a_small_stream_limit_costs_no_extra_sockets() {
        const OFFERED: usize = 200;
        /// Far below `H2_STREAMS_PER_CONNECTION`, so every lane is
        /// over-offered by design.
        const PEER_STREAMS: u32 = 16;

        let probe = ConcurrencyProbe::new();
        let base_url = spawn_blocking_stub_with_streams(Arc::clone(&probe), PEER_STREAMS).await;
        let client = InferenceApiClient::new_with_metadata_cache(base_url, false).unwrap();

        let mut inflight = tokio::task::JoinSet::new();
        for _ in 0..OFFERED {
            let client = client.clone();
            inflight.spawn(async move {
                client
                    .predict("g/model", "k", 1, 60, None, None, &[text_input("x")])
                    .await
                    .map(|_| ())
            });
        }
        // Enough for every request the peer will admit to have arrived.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let peak = probe.peak();
        let sockets = probe.sockets();
        probe.release.send_replace(true);
        let mut answered = 0usize;
        while let Some(result) = inflight.join_next().await {
            // The whole point: a stream limit below what we offer is a
            // *wait*, not an error.
            result.expect("no panic").expect("the stub answers");
            answered += 1;
        }
        assert_eq!(answered, OFFERED);
        assert_eq!(
            client.known_transport(),
            Some(Transport::H2c),
            "a stingy peer is still an h2c peer"
        );
        assert!(
            peak <= PEER_STREAMS as usize * INFERENCE_CONNECTION_LANES,
            "the peer's own limit must bound what reaches its handler: {peak}"
        );
        assert!(
            sockets <= INFERENCE_CONNECTION_LANES,
            "a peer advertising {PEER_STREAMS} streams must not multiply our \
             connections: {sockets} sockets"
        );
    }

    /// The descriptor bound `jobs::extraction::in_flight_unit_ceiling` relies
    /// on, measured rather than reasoned about.
    ///
    /// Its multiplexed arm no longer clamps the window by descriptors at all;
    /// what makes that safe is that the whole endpoint costs at most
    /// `FDS_PER_POOLED_CONNECTION x INFERENCE_CONNECTION_LANES` = 128, and
    /// nothing about the window's width changes it. Run1 blocker F6 was 983
    /// sockets for a window of the same shape.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_endpoints_whole_socket_cost_is_the_lane_count() {
        const OFFERED: usize = 400;

        let probe = ConcurrencyProbe::new();
        let base_url = spawn_blocking_stub(Arc::clone(&probe)).await;
        let client = InferenceApiClient::new_with_metadata_cache(base_url, false).unwrap();
        // Probe and one round trip first, so every lazy allocation the
        // runtime makes on first contact is already paid for.
        probe.release.send_replace(true);
        client
            .predict("g/model", "k", 1, 60, None, None, &[text_input("warm")])
            .await
            .expect("the stub answers");
        probe.release.send_replace(false);
        let baseline = open_fds();

        let mut inflight = tokio::task::JoinSet::new();
        for _ in 0..OFFERED {
            let client = client.clone();
            inflight.spawn(async move {
                client
                    .predict("g/model", "k", 1, 60, None, None, &[text_input("x")])
                    .await
                    .map(|_| ())
            });
        }
        let mut peak_fds = baseline;
        for _ in 0..30 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            peak_fds = peak_fds.max(open_fds());
        }
        probe.release.send_replace(true);
        while let Some(result) = inflight.join_next().await {
            result.expect("no panic").expect("the stub answers");
        }

        let growth = peak_fds.saturating_sub(baseline);
        // Both ends of every lane, because the stub runs in this process
        // exactly as local inference does, plus slack for whatever else the
        // runtime opened inside the sampling window.
        let bound = 2 * INFERENCE_CONNECTION_LANES + 8;
        assert!(
            growth <= bound,
            "{OFFERED} concurrent predicts grew the descriptor table by \
             {growth}, past the lane budget of {bound}"
        );
        assert!(
            growth < OFFERED,
            "{growth} descriptors for {OFFERED} requests is not better than \
             one socket each"
        );
    }

    /// The fallback: a server that does not speak HTTP/2 cleartext is
    /// detected once, remembered, and served over HTTP/1.1 — with the
    /// request that discovered it still succeeding.
    ///
    /// The stub is deliberately not an HTTP server library: it answers every
    /// connection with a fixed HTTP/1.1 response, which is exactly what an
    /// HTTP/1.1-only peer does to an HTTP/2 preface, and what the client's
    /// probe has to survive.
    #[tokio::test]
    async fn an_http1_only_endpoint_falls_back_and_stays_usable() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut buffer = [0u8; 4096];
                    let _ = socket.read(&mut buffer).await;
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

        let client =
            InferenceApiClient::new_with_metadata_cache(format!("http://{addr}"), false).unwrap();
        assert_eq!(
            client.known_transport(),
            None,
            "nothing is assumed before the first request"
        );
        let cached = client.get_cached_models().await.expect("HTTP/1.1 answers");
        assert_eq!(cached, serde_json::json!({"cache": {}}));
        assert_eq!(
            client.known_transport(),
            Some(Transport::Http11),
            "the fallback must be remembered, not re-probed per request"
        );
        // Remembered: a second call answers without another probe.
        assert!(client.get_cached_models().await.is_ok());
        assert_eq!(client.known_transport(), Some(Transport::Http11));
    }

    /// The concurrency gate admits at most
    /// [`INFERENCE_MAX_CONCURRENT_REQUESTS`] requests in **both** transports.
    ///
    /// Gating only h2c would leave the transport where a request costs a whole
    /// socket as the only unbounded one — and it is reachable after a job has
    /// already sized its in-flight window for multiplexing, because
    /// `in_flight_unit_ceiling` runs once before the item loop. This asserts
    /// the property on the permit itself rather than on socket counts, which
    /// `concurrent_predicts_share_the_connection_pool` already measures.
    #[tokio::test]
    async fn both_transports_take_a_concurrency_permit() {
        for transport in [Transport::H2c, Transport::Http11] {
            let client = InferenceApiClient::new_with_metadata_cache(
                format!("http://gate-test-{transport:?}"),
                false,
            )
            .unwrap();
            // Pinned rather than probed: this is about the gate, and there is
            // nothing listening on that name.
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
                "{transport:?} must be admitted through the gate, not around it"
            );
            assert_eq!(
                gate.available_permits(),
                before - 1,
                "{transport:?} must hold a permit while its request is in flight"
            );
            // The lane is claimed on the multiplexed path only; under
            // HTTP/1.1 there is nothing for a lane to buy.
            assert_eq!(lease.lane.is_some(), transport.is_multiplexed());
            drop(lease);
            assert_eq!(gate.available_permits(), before);
            assert!(
                runtime
                    .h2
                    .iter()
                    .all(|lane| lane.in_flight.load(Relaxed) == 0),
                "the lane claim is returned with the lease"
            );
        }
    }

    /// The h2c gate follows the figure the endpoint published; the HTTP/1.1
    /// gate does not move at all.
    ///
    /// Track E made the gate a fixed 256 partly on the arithmetic "256 is
    /// four times a job's in-flight budget (4 096 units at 64 units per
    /// request)". That premise holds only for a model whose items carry 64
    /// work units each; an image item carries one, so the job sends one item
    /// per request and 4 096 units is 4 096 concurrent requests. 256 was a
    /// sixteenth of the budget, not four times it — which is why this now
    /// moves, and why the floor is exactly the old constant so that nothing
    /// existing can be throttled by it.
    #[tokio::test]
    async fn the_h2c_gate_follows_the_published_figure_and_http1_does_not() {
        let client =
            InferenceApiClient::new_with_metadata_cache("http://gate-target-test", false).unwrap();
        let runtime = Arc::clone(&client.endpoint);

        assert_eq!(
            runtime.h2_gate.available_permits(),
            INFERENCE_MAX_CONCURRENT_REQUESTS
        );

        // Growth, up to the ceiling and no further: past it we would be
        // offering some lane more streams than it is designed to carry.
        client.observe_desired_in_flight(1_632);
        assert_eq!(runtime.h2_gate.available_permits(), 1_632);
        client.observe_desired_in_flight(u64::MAX);
        assert_eq!(
            runtime.h2_gate.available_permits(),
            INFERENCE_MAX_CONCURRENT_STREAMS
        );

        // The floor is the constant every existing deployment already runs
        // at, so a small figure can never throttle one.
        client.observe_desired_in_flight(1);
        assert_eq!(
            runtime.h2_gate.available_permits(),
            INFERENCE_MAX_CONCURRENT_REQUESTS
        );

        // HTTP/1.1 is a different semaphore and never moves: there an
        // admitted request is a socket, and a model's batching advice must
        // not move this process's descriptor usage.
        assert_eq!(
            runtime.h1_gate.available_permits(),
            INFERENCE_MAX_CONCURRENT_REQUESTS
        );
    }

    /// A shrink of the h2c gate must land even while every permit is out.
    ///
    /// `Semaphore::forget_permits` can only take permits that are *available*,
    /// and a saturated endpoint has none — every release goes straight to a
    /// waiter. So the shrink is repaid on the release path instead: each
    /// returning permit is retired until the deficit is gone. Same rule, and
    /// the same reason, as `jobs::extraction::UnitBudget` (S1b).
    #[tokio::test]
    async fn a_gate_shrink_lands_through_releases_not_only_through_free_permits() {
        let client =
            InferenceApiClient::new_with_metadata_cache("http://gate-shrink-test", false).unwrap();
        let runtime = Arc::clone(&client.endpoint);
        *runtime.transport.write().await = Some(Transport::H2c);

        client.observe_desired_in_flight(512);
        // Saturate: every permit held by an in-flight request.
        let mut held = Vec::new();
        for _ in 0..512 {
            let (_transport, _clients, lease) = client.active().await;
            held.push(lease);
        }
        assert_eq!(runtime.h2_gate.available_permits(), 0);

        // Shrink to the floor. Nothing is free, so nothing can be forgotten
        // now — the deficit is 512 - 256 = 256.
        client.observe_desired_in_flight(u64::from(INFERENCE_MAX_CONCURRENT_REQUESTS as u32));
        assert_eq!(runtime.h2_gate.available_permits(), 0);

        // Every release repays the deficit before it re-issues anything.
        for _ in 0..256 {
            held.pop();
        }
        assert_eq!(
            runtime.h2_gate.available_permits(),
            0,
            "the first 256 releases must be retired, not re-issued"
        );
        while held.pop().is_some() {}
        assert_eq!(
            runtime.h2_gate.available_permits(),
            INFERENCE_MAX_CONCURRENT_REQUESTS,
            "and the gate settles at exactly the new target"
        );
        assert!(
            runtime
                .h2
                .iter()
                .all(|lane| lane.in_flight.load(Relaxed) == 0),
            "every lane claim is returned with its lease"
        );
    }

    /// **A retry does not sit on a concurrency slot while it waits.**
    ///
    /// The gate permit and the lane claim are what bound this client's
    /// concurrency; a request that is doing nothing but waiting out a backoff
    /// must hold neither, and the case it matters in is the one where the
    /// server has *just said* it is overloaded (a 503, which this loop
    /// retries). Measured from outside, on the endpoint's own health
    /// snapshot, while the client is provably mid-backoff.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_backoff_holds_no_gate_permit_and_no_lane() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::SeqCst;

        // 503 once, then answer. The first attempt therefore ends in the
        // retry path, and the client sleeps `PREDICT_MIN_DELAY` (1s).
        let attempts = Arc::new(AtomicUsize::new(0));
        let handler_attempts = Arc::clone(&attempts);
        let app = axum::Router::new().route(
            "/api/inference/predict/{group}/{model}",
            axum::routing::post(move || {
                let attempts = Arc::clone(&handler_attempts);
                async move {
                    if attempts.fetch_add(1, SeqCst) == 0 {
                        return (
                            axum::http::StatusCode::SERVICE_UNAVAILABLE,
                            [(axum::http::header::CONTENT_TYPE, "application/json")],
                            "{\"detail\":{\"kind\":\"body_budget_exhausted\"}}",
                        );
                    }
                    (
                        axum::http::StatusCode::OK,
                        [(axum::http::header::CONTENT_TYPE, "application/json")],
                        "{\"outputs\":[]}",
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
        let runtime = Arc::clone(&client.endpoint);
        let predicting = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .predict("g/model", "k", 1, 60, None, None, &[text_input("x")])
                    .await
            }
        });

        // Sample inside the backoff: the first attempt has been answered and
        // the second has not been sent. The window is a whole second, so a
        // sample a fifth of the way in is not a race.
        while attempts.load(SeqCst) < 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            attempts.load(SeqCst),
            1,
            "the sample must land between the refusal and the retry"
        );
        let (_target, in_flight) = runtime.gate_snapshot(Some(Transport::H2c));
        assert_eq!(in_flight, 0, "a waiting retry holds no gate permit");
        assert_eq!(runtime.lanes_in_use(), 0, "and no lane claim either");

        predicting
            .await
            .expect("no panic")
            .expect("the second attempt is answered");
        assert_eq!(attempts.load(SeqCst), 2);
    }

    /// Lanes are recruited by load, not spread across: the socket cost of an
    /// endpoint tracks the work in flight, which is the whole point of
    /// multiplexing. Asserted on the chooser directly, where the arithmetic
    /// is visible.
    #[tokio::test]
    async fn lanes_are_recruited_by_load() {
        let client =
            InferenceApiClient::new_with_metadata_cache("http://lane-pick-test", false).unwrap();
        let runtime = Arc::clone(&client.endpoint);

        // Empty: one lane.
        assert_eq!(runtime.pick_lane(), 0);
        // Below one lane's stream budget: still that lane.
        runtime.h2[0]
            .in_flight
            .store(H2_STREAMS_PER_CONNECTION - 1, Relaxed);
        assert_eq!(runtime.pick_lane(), 0);
        // Full: the next lane is recruited.
        runtime.h2[0]
            .in_flight
            .store(H2_STREAMS_PER_CONNECTION, Relaxed);
        assert_eq!(runtime.pick_lane(), 1);
        // Within the recruited prefix the choice is least-loaded, not "the
        // first with room", so a lane carrying a slow batch is not handed the
        // next request too. Here lane 0 still has room and is not chosen.
        runtime.h2[0].in_flight.store(50, Relaxed);
        runtime.h2[1].in_flight.store(20, Relaxed);
        assert_eq!(runtime.pick_lane(), 1);
        // A third lane is recruited only once two are full.
        runtime.h2[1]
            .in_flight
            .store(H2_STREAMS_PER_CONNECTION, Relaxed);
        runtime.h2[0]
            .in_flight
            .store(H2_STREAMS_PER_CONNECTION, Relaxed);
        assert_eq!(runtime.pick_lane(), 2);
        // And it never leaves the array.
        for lane in &runtime.h2 {
            lane.in_flight.store(H2_STREAMS_PER_CONNECTION, Relaxed);
        }
        assert!(runtime.pick_lane() < INFERENCE_CONNECTION_LANES);
    }

    /// An endpoint that could not be reached at all must not be recorded as
    /// HTTP/1.1.
    ///
    /// A transport memo is written once and only cleared by a *predict*
    /// failure, so a blip during the first contact — which in the deployment
    /// this exists for crosses a network — would otherwise cost the endpoint
    /// its multiplexing for the life of the process, and halve every job's
    /// in-flight window with it through `requests_are_multiplexed`. The
    /// endpoint here is a closed port, which is the cheapest honest version of
    /// "not reachable".
    #[tokio::test]
    async fn an_unreachable_endpoint_is_not_remembered_as_http11() {
        // A port nothing in this process can be listening on. Binding and
        // dropping an *ephemeral* port was the obvious way to get one and is
        // not sound: the port goes straight back to the pool this binary's
        // other tests bind from, so the "closed" port is occasionally a
        // neighbour's stub service and the call under test succeeds. Port 1
        // is below `ip_local_port_range` and cannot be bound without
        // privileges, so no test can take it.
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        assert!(
            tokio::net::TcpStream::connect(addr).await.is_err(),
            "the premise of this test is that {addr} refuses connections; \
             something on this host is listening on it"
        );
        let client =
            InferenceApiClient::new_with_metadata_cache(format!("http://{addr}"), false).unwrap();

        assert!(
            client.get_cached_models().await.is_err(),
            "nothing is listening, so the call itself must fail"
        );
        assert_eq!(
            client.known_transport(),
            None,
            "an unreachable peer says nothing about which HTTP version it speaks"
        );

        // The classifier that decides it, on the two error shapes directly.
        let connect_err = reqwest::Client::builder()
            .build()
            .unwrap()
            .get(format!("http://{addr}/cache"))
            .send()
            .await
            .expect_err("the port is closed");
        assert!(connect_err.is_connect());
        assert!(
            !InferenceApiClient::could_be_an_http2_refusal(&connect_err),
            "a connect failure is a network fact, not a protocol one"
        );
    }

    /// A peer that accepts the connection and then drops it must not be
    /// remembered as HTTP/1.1 either.
    ///
    /// This is the shape the connect/timeout check alone cannot catch: a
    /// mid-stream reset, a peer restarting, a proxy closing an idle socket.
    /// `reqwest` reports it exactly as it reports an HTTP/1.1-only server's
    /// refusal of the h2 preface — neither `is_connect` nor `is_timeout` — so
    /// the only thing that separates the two is whether the peer will answer
    /// *anything* over HTTP/1.1. This one will not, so nothing is recorded and
    /// the endpoint is free to multiplex again the moment it is healthy.
    #[tokio::test]
    async fn a_peer_that_answers_nothing_is_not_remembered_as_http11() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                // Accept, then drop: the TCP connection succeeds and the
                // request dies on it, which is the ambiguous class.
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                drop(socket);
            }
        });

        let client =
            InferenceApiClient::new_with_metadata_cache(format!("http://{addr}"), false).unwrap();
        assert!(
            client.get_cached_models().await.is_err(),
            "the peer answers nothing, so the call itself must fail"
        );
        assert_eq!(
            client.known_transport(),
            None,
            "a peer that answers neither protocol is not evidence for either"
        );
    }

    /// Two clients for the same endpoint share one connection pool and one
    /// transport decision. A pool that is not shared is not a bound, and the
    /// gateway builds several clients for the same inference endpoint (the
    /// job pool, the PQL path, the preload loop).
    #[tokio::test]
    async fn clients_for_one_endpoint_share_their_pool() {
        let base_url = spawn_stub_service(Duration::ZERO).await;
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
    /// unsupported server; other HTTP failures remain visible to callers.
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

        let missing =
            InferenceApiClient::new_with_metadata_cache(format!("http://{addr}/missing"), false)
                .unwrap();
        assert_eq!(missing.get_external_inputs_optional().await.unwrap(), None);

        let broken =
            InferenceApiClient::new_with_metadata_cache(format!("http://{addr}/broken"), false)
                .unwrap();
        assert!(broken.get_external_inputs_optional().await.is_err());
    }

    /// The predict request must carry `max_batch` (design §6) and `prewarm`
    /// (design §8) as query params exactly when the caller passes Some, and
    /// omit them entirely when None — so callers with no opinion (PQL
    /// search embeds pass None for both) leave the server defaults in
    /// charge, and the params never appear as spurious empty values. Same
    /// contract on PUT /load for `prewarm` (extraction jobs pass
    /// Some(false); cron preload passes None). Captured off a stub server
    /// because the client builds the URLs internally.
    #[tokio::test]
    async fn urls_carry_max_batch_and_prewarm_only_when_some() {
        let captured: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let predict_sink = Arc::clone(&captured);
        let load_sink = Arc::clone(&captured);
        let app = Router::new()
            .route(
                "/api/inference/predict/{group}/{id}",
                post(move |RawQuery(query): RawQuery| {
                    let sink = Arc::clone(&predict_sink);
                    async move {
                        sink.lock().unwrap().push(query.unwrap_or_default());
                        Json(json!({"outputs": [{"ok": true}]}))
                    }
                }),
            )
            .route(
                "/api/inference/load/{group}/{id}",
                axum::routing::put(move |RawQuery(query): RawQuery| {
                    let sink = Arc::clone(&load_sink);
                    async move {
                        sink.lock().unwrap().push(query.unwrap_or_default());
                        Json(json!({"status": "loaded"}))
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let client = InferenceApiClient::new_with_metadata_cache(format!("http://{addr}"), false)
            .expect("client builds");
        let inputs = [InferenceInput::new(json!({"text": "x"}), None)];
        client
            .predict("group/model", "key", 10, -1, Some(7), Some(false), &inputs)
            .await
            .expect("capped no-prewarm predict");
        client
            .predict("group/model", "key", 10, -1, None, None, &inputs)
            .await
            .expect("no-opinion predict");
        client
            .load_model("group/model", "key", 10, -1, Some(false))
            .await
            .expect("no-prewarm load");
        client
            .load_model("group/model", "key", 10, -1, None)
            .await
            .expect("no-opinion load");

        let queries = captured.lock().unwrap().clone();
        assert_eq!(queries.len(), 4, "all four requests reached the stub");
        assert!(
            queries[0].contains("max_batch=7") && queries[0].contains("prewarm=false"),
            "Some values serialize as query params: {}",
            queries[0]
        );
        assert!(
            queries[0].contains("cache_key=key")
                && queries[0].contains("lru_size=10")
                && queries[0].contains("ttl_seconds=-1"),
            "existing params still present alongside the additive ones: {}",
            queries[0]
        );
        assert!(
            !queries[1].contains("max_batch") && !queries[1].contains("prewarm"),
            "None omits both params entirely: {}",
            queries[1]
        );
        assert!(
            queries[2].contains("prewarm=false"),
            "load with Some(false) carries prewarm: {}",
            queries[2]
        );
        assert!(
            !queries[3].contains("prewarm"),
            "load with None omits prewarm: {}",
            queries[3]
        );
    }

    fn envelope(outputs: Vec<Value>) -> (String, Vec<u8>) {
        (
            "application/json".to_string(),
            serde_json::to_vec(&json!({ "outputs": outputs })).unwrap(),
        )
    }

    /// Wrappedness is decided per slot, not all-or-nothing: the encoder wraps
    /// every binary output and leaves every JSON output alone, so a batch
    /// with both is a response `PredictOutput` — one type for the whole
    /// response — cannot represent. It is reported instead of handed on:
    /// passed through as JSON, the wrapper map reaches an output handler that
    /// finds no `transcription` in it and drops the payload silently.
    #[test]
    fn a_mixed_binary_and_json_batch_is_reported_not_guessed() {
        let (content_type, body) = envelope(vec![
            json!({"__error__": {"class": "input", "message": "Unreadable image"}}),
            json!({"__type__": "base64", "content": "QUFB"}),
            json!({"transcription": "hello"}),
        ]);
        let err = parse_predict_response(&content_type, &body)
            .expect_err("a response with no common representation must not be guessed at");
        assert!(
            is_protocol_violation(&err),
            "and it is deterministic, so isolation must not retry it: {err:#}"
        );
        assert!(
            format!("{err:#}").contains("mixes 1 binary and 1 JSON"),
            "{err:#}"
        );
    }

    /// The two unmixed shapes still round-trip: all survivors wrapped is a
    /// binary batch (an embedding model whose item had one bad frame), none
    /// wrapped is a JSON batch. Both keep the slot error at its *input's*
    /// index while the survivors close ranks.
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
    }

    /// The legacy no-slot path is untouched: a JSON envelope without error
    /// slots is passed through verbatim, wrapper maps and all. (The encoder
    /// never produces that shape — an all-binary batch goes out as
    /// `multipart/mixed` — so this is only pinning the byte-identical
    /// behaviour of every response an older server can send.)
    #[test]
    fn the_legacy_envelope_is_passed_through_verbatim() {
        let values = vec![
            json!({"__type__": "base64", "content": "QUFB"}),
            json!({"transcription": "hello"}),
        ];
        let (content_type, body) = envelope(values.clone());
        let parsed = parse_predict_response(&content_type, &body).unwrap();
        assert!(parsed.errors.is_empty());
        match parsed.outputs {
            PredictOutput::Json(parsed) => assert_eq!(parsed, values),
            other => panic!("client parsed {other:?}"),
        }
    }

    /// A malformed error slot is a protocol violation rather than a payload
    /// or a guessed class, and it is *typed* so the extraction job can skip
    /// the isolation pass that would only ask the same broken server again.
    #[test]
    fn a_malformed_error_slot_is_a_typed_protocol_violation() {
        let (content_type, body) = envelope(vec![
            json!({"transcription": "hello"}),
            json!({"__error__": {"class": "blocked", "message": "not ours"}}),
        ]);
        let err = parse_predict_response(&content_type, &body).expect_err("malformed");
        assert!(is_protocol_violation(&err), "{err:#}");
        assert!(format!("{err:#}").contains("predict output 1"), "{err:#}");
    }

    fn is_protocol_violation(err: &anyhow::Error) -> bool {
        err.downcast_ref::<ProtocolViolation>().is_some()
    }
}
