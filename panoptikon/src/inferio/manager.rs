//! Model manager: exact port of the legacy Python `inferio/manager.py`
//! semantics (python-legacy branch; design
//! doc §5) on top of [`Worker`] supervision and the per-model dispatcher
//! (`dispatch.rs`, design §6).
//!
//! State model (all bookkeeping under one std `Mutex`, never held across
//! await):
//! - `lru_caches[cache_key]` is an insertion-ordered map `inference_id ->
//!   expiration` (Python's `OrderedDict`); `lru_size` is enforced on every
//!   load, evicting oldest-first.
//! - `cache_refs[inference_id]` is the set of cache keys referencing the
//!   model; the model unloads only when the last reference disappears (LRU
//!   eviction, TTL expiry, explicit DELETE, cache clear).
//! - TTL: `ttl_seconds >= 0` -> now + ttl; negative -> never. A sweeper task
//!   ticks every `sweep_interval` (Python: 10 s), expiring entries and
//!   unloading unreferenced models.
//! - Repeated load renews the TTL *and* moves the entry to the
//!   most-recently-used position — Python explicitly `move_to_end`s before
//!   reassigning (manager.py:73-74); the cron preload loop depends on this.
//! - Predict pins the model with a refcount (design §5 delta): while any
//!   predict is in flight the sweeper skips the model entirely, and each
//!   completing predict restores `now + requested ttl` on its own cache-key
//!   entry (Python's `finally: load_model(ttl)`), so overlapping predicts
//!   through different cache keys cannot unpin each other — the refcount
//!   fixes Python's latent same-key race where the first predict to finish
//!   could let the second expire mid-inference.
//!
//! # Locking (R6)
//!
//! Python holds one manager-wide lock for the entire `load_model` call,
//! including the slow process spawn, and this port used to mirror that with
//! a single async `load_lock` taken at the top of every load **and every
//! predict**. That is finding P5-3/B18 of the batch-calibration run1 report:
//! an 11.865 s load of one model stalled every in-flight predict to every
//! *other* resident model for 11.885–11.894 s — 100.2 % of the load, 28× the
//! p50 — and the load deadline is 600 s. Change R6 retires that lock. What
//! replaces it, in **the one order every path takes them in**:
//!
//! 0. **Nothing at all** for a predict or a load whose model is already
//!    resident. The fast path of [`ModelManager::ensure_loaded`] is one
//!    `state` critical section (touch the LRU, check `models`, take the
//!    predict pin) and returns without ever awaiting a lock, so no load
//!    anywhere on the host can delay it. This is the fix for B18.
//! 1. **`load_barrier`** (async `RwLock`) — read-guarded by the *slow* path
//!    of `ensure_loaded` for as long as a spawn may be in flight,
//!    write-guarded once by [`ModelManager::shutdown`]. This is the one job
//!    the global lock did that was not serialization: shutdown must not
//!    drain `state.draining` before a load still in flight has decided what
//!    to do with the workers it is spawning, or those workers are abandoned
//!    mid-stop. Read guards do not exclude each other, so it costs
//!    concurrent loads nothing.
//! 2. **`load_locks[inference_id]`** (async `Mutex`, one per model, created
//!    on demand and dropped when the last holder lets go) — the one piece of
//!    serialization the global lock is kept for: two callers must not spawn
//!    the same model twice. A load of model A and a predict to resident
//!    model B share no lock at all.
//! 3. **`load_admission[board]`** (`Semaphore`, `max_concurrent_loads`
//!    permits per board) — the board-admission gate: how many models may be
//!    streaming weights into *one board* at once. Held around the spawn +
//!    `load` round trip inside [`ModelManager::spawn_model`], where the
//!    ledger's load reservations are charged; a replica set spanning several
//!    boards takes one permit per distinct board, acquired in sorted key
//!    order. A replica whose board key does not resolve (no inventory at
//!    all, a pin the ledger cannot place) counts as landing on *every*
//!    board: it takes a shared "unresolved" bucket and one permit per board
//!    in the inventory, so it can neither overlap another such load nor a
//!    load onto the board it may well have landed on. A host with no GPUs
//!    therefore keeps exactly the host-wide serialization it has today.
//! 4. **`state`** (std `Mutex`) — all bookkeeping. Never held across an
//!    await and never held while 1–3 are acquired, so the sync accessors
//!    (`cached_models`, `cache_expirations`, `health`) stay cheap.
//!
//! The `load_locks` table's own std mutex, the `load_admission` table's, and
//! the `prewarm`, `ledger` and `registry` mutexes are **leaves**: each is
//! taken and released without acquiring anything else.
//!
//! **No deadlock.** A cycle needs two tasks holding these in opposite
//! orders. The acquisition sites are exactly: `ensure_loaded` (4, released;
//! then 1 → 2 → 4 → [3 inside `spawn_model`, released] → 4), `shutdown`
//! (4, released; then 1; then 4 again) and every other manager method (4
//! alone). No site ever waits for a lower-numbered lock while holding a
//! higher-numbered one, so the numbering is a total order over every held
//! set and no cycle can form. Within 3, the several permits of a multi-board
//! set are acquired in sorted board-key order, which is a total order over
//! the permits themselves. `shutdown` taking 4 before 1 is worth spelling
//! out: it *releases* 4 before acquiring 1, so the pair is never held
//! together. Leaves cannot participate in a cycle because nothing is
//! acquired while one is held. Every lock is released on cancellation too
//! (RAII guards throughout), so a client that disconnects mid-load strands
//! none of them.
//!
//! What the retired global lock protected, and where each invariant lives
//! now: **no double load of one model** — lock 2 plus the `state.models`
//! re-check taken under lock 4 *inside* it (the fast path's check is the
//! first half of a double-checked load); **unload/expiry vs. a load in
//! flight** — unchanged, the spawn-phase pin and the post-spawn
//! `refs_non_empty` re-check, both under lock 4; **the TTL sweeper** —
//! unchanged, models are pinned while they spawn so an entry cannot expire
//! mid-load (a real race in Python, whose sweeper uses a *different* lock
//! than `load_model`); **generation bumps on death** — unchanged, lock 4
//! alone; **prewarm claims** — the pool's own mutex, which was never the
//! load lock's job (`claim` removes the slot atomically, so two loads of the
//! same impl class cannot both take the parked worker); **`/health`** —
//! lock 4 alone, and it never saw intermediate load state anyway (a model
//! appears in `models` only once its whole set has loaded); **concurrent
//! VRAM accounting** — the ledger's load reservations, which are charged and
//! priced inside one ledger critical section (so a second concurrent
//! reserver sees the first's charge), plus lock 3.
//!
//! Deliberate deviations from Python (each also noted inline):
//! - Failed loads never leave a phantom id in `/cache`: Python's
//!   `_unload_model` only deletes the `_cache_key_map` entry when the model
//!   was actually loaded, so a failed load leaves `id -> []` in
//!   `list_loaded_models()` forever. We keep refs tidy instead.
//! - `lru_size <= 0` refuses the load with an error. Python evicts the
//!   just-inserted entry and then loads the model anyway, leaking the
//!   process with no reference to ever unload it.
//! - Explicit unload during an in-flight predict lets the running batch
//!   finish before the worker shuts down (the dispatcher processes the
//!   shutdown after the batch). Python terminates the process mid-predict
//!   and fails the request.
//! - The post-predict TTL restore only updates the expiration; Python's
//!   `finally: load_model(...)` also re-runs move-to-end/resize and would
//!   even *respawn* the model if it had been unloaded mid-predict — an
//!   accidental side effect, not ported.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Local, Timelike};
use hashlink::LinkedHashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::{
    Mutex as TokioMutex, OwnedMutexGuard, OwnedSemaphorePermit, RwLock as TokioRwLock, Semaphore,
    mpsc, oneshot,
};
use tokio::task::JoinHandle;

use super::calibration::CalibrationProfiles;
use super::cost::{CostDimension, CostUnit};
use super::dispatch::{
    DispatchMsg, DispatchRequest, DispatcherContext, ModelStats, Replica, run_dispatcher,
};
use super::gpu::{GpuInfo, GpuInventory};
use super::ledger::{Admission, GpuBudgetHealth, LoadReservation, VramBudgets, VramLedger};
use super::prewarm::{PrewarmConfig, PrewarmHealth, PrewarmPool};
use super::registry::{Registry, RegistryCache, SpawnSpec};
use super::slot_error::Unattempted;
use super::worker::{
    LoadReport, MemorySample, TelemetryHandle, Worker, WorkerError, WorkerInput, WorkerOutput,
    WorkerSpawnConfig,
};

/// Manager configuration.
pub struct ManagerConfig {
    /// How worker processes are spawned (python, impl dirs, env, deadlines).
    pub spawn: WorkerSpawnConfig,
    /// Fixed batch size for the **unpriced** dispatch path (`none`-class
    /// models, hosts with no inventory at all), used when the registry
    /// declares no `default_batch_size`. Priced models are sized by the VRAM
    /// ledger instead — this is no longer a safety cap. Note the path is
    /// narrower than it once was: MPS and CPU hosts have admission boards of
    /// their own (docs/unified-memory-admission.md), so a host reaches this
    /// only when its inventory could not be built.
    pub default_max_batch: u32,
    /// TTL sweeper period (Python: 10 s).
    pub sweep_interval: Duration,
    /// Load-path policy: the board-admission gate's width
    /// (`[inference_local] max_concurrent_loads`, R6). Its own struct so the
    /// R9 cooldown levers can join it without touching every construction
    /// site.
    pub loads: LoadPolicy,
    /// Prewarm pool policy (design §8; `[inference_local.prewarm]`).
    pub prewarm: PrewarmConfig,
    /// Visible GPUs, probed once at startup. Universal worker→GPU pinning
    /// resolves every replica's visibility pin against this — written to
    /// `CUDA_VISIBLE_DEVICES` on CUDA hosts and `HIP_VISIBLE_DEVICES` on ROCm
    /// ones — and the per-GPU ledger is keyed by these UUIDs. Unknown
    /// whenever the backend's own probe answers nothing (no nvidia-smi; on
    /// ROCm, unreadable KFD topology or an ambient visibility restriction),
    /// which keeps today's unpinned behaviour.
    pub gpus: GpuInventory,
    /// VRAM admission limits (`[inference_local.vram]`): the server default
    /// plus per-board-UUID overrides. Defaults are margin 0.10 on,
    /// `cap_fraction` off.
    pub vram: VramBudgets,
    /// The calibration store: shipped baselines and the local generated
    /// profile file. `None` leaves the ledger unprimed and unpersisted,
    /// which is exactly the pre-store behaviour (tests, and any embedder
    /// that does not want a file).
    pub calibration: Option<Arc<dyn CalibrationProfiles>>,
}

/// Load-path policy (`[inference_local]`, R6). The defaults here are the
/// shipped ones; [`From<&crate::config::InferenceLocalConfig>`] is the
/// bridge the HTTP layer uses, so config knowledge stays out of the manager
/// proper.
#[derive(Debug, Clone, Copy)]
pub struct LoadPolicy {
    /// How many models may be spawning and streaming weights into **one
    /// board** at the same time (module docs, lock 3).
    ///
    /// Default 1, which is what the retired global load lock enforced: one
    /// set of weights in flight per board, so the ledger's load reservation
    /// for that board covers exactly one incoming footprint. Every unpinned
    /// model resolves to the same default board, so on the shipped
    /// configuration this *is* the old host-wide serialization; what it stops
    /// doing is serializing loads that cannot collide because they land on
    /// different boards (or on none at all, where the bucket is shared and
    /// the behaviour is again the old one).
    ///
    /// Raising it shortens a cold start that touches several models on one
    /// board, at the cost of several sets of weights landing against one
    /// headroom reading. That is safe rather than merely tolerable: each load
    /// charges its expected base as a `LoadReservation` inside the same
    /// ledger critical section that reads the headroom, so the second
    /// concurrent reserver prices itself against the first's charge. 0 is
    /// read as 1.
    pub max_concurrent_loads: usize,
    /// First window of the per-model load-failure cooldown (R9), doubled per
    /// consecutive failure up to [`LoadPolicy::cooldown_max`].
    ///
    /// `Duration::ZERO` disables the cooldown entirely, which is the only
    /// off switch: there is no separate boolean, because "how long" already
    /// expresses "not at all".
    pub cooldown_base: Duration,
    /// Ceiling on the cooldown window.
    pub cooldown_max: Duration,
}

/// Ceiling on the configured cooldown seconds ([`LoadPolicy::cooldown_base`]
/// and [`LoadPolicy::cooldown_max`]).
///
/// `Instant + Duration` **panics** on overflow, and the cooldown does that
/// twice over: once to turn a window into a deadline
/// ([`LoadCooldowns::note_failure`]) and once to decide when to forget the
/// history ([`LoadCooldowns::prune`], which adds `cooldown_max` on top of a
/// deadline). Both run under the state mutex — on the sweeper tick, among
/// other places — so an unrepresentable configured value would not merely
/// misbehave, it would poison the manager. A cooldown longer than a year
/// outlives any process it could apply to, so clamping there is a distinction
/// no operator can observe.
const MAX_COOLDOWN_SECS: u64 = 366 * 24 * 60 * 60;

impl Default for LoadPolicy {
    fn default() -> Self {
        Self {
            max_concurrent_loads: 1,
            cooldown_base: Duration::from_secs(2),
            cooldown_max: Duration::from_secs(300),
        }
    }
}

impl From<&crate::config::InferenceLocalConfig> for LoadPolicy {
    fn from(local: &crate::config::InferenceLocalConfig) -> Self {
        Self {
            max_concurrent_loads: local.max_concurrent_loads,
            cooldown_base: Duration::from_secs(
                local.load_failure_cooldown_secs.min(MAX_COOLDOWN_SECS),
            ),
            cooldown_max: Duration::from_secs(
                local.load_failure_cooldown_max_secs.min(MAX_COOLDOWN_SECS),
            ),
        }
    }
}

/// Machine-readable `kind` of the load-failure cooldown error on the wire
/// (`{"detail": {"kind": "load_cooldown", …}}`, answered with 503 and a
/// `Retry-After` header by `http.rs`). A job aborts on it rather than
/// retrying every one of its items into the same wall.
pub(crate) const LOAD_COOLDOWN_KIND: &str = "load_cooldown";

/// Bound on the stored text of the last load failure. It is a Python
/// traceback plus a stderr tail — tens of kilobytes is normal (run1 Q10
/// measured 57 MB of forwarded tracebacks in 118 s) — and this copy is
/// repeated on every refused request and in every `/health` poll, while the
/// full text is in the log already. 2000 bytes matches the clamp the
/// extraction ledger puts on its own audit strings.
const MAX_COOLDOWN_ERROR_BYTES: usize = 2000;

fn clamp_cooldown_error(text: &str) -> String {
    if text.len() <= MAX_COOLDOWN_ERROR_BYTES {
        return text.to_owned();
    }
    let mut end = MAX_COOLDOWN_ERROR_BYTES;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

/// The error a load refused by the cooldown returns (R9). `http.rs` matches
/// it out of the `anyhow` chain and renders the pinned 503.
#[derive(Debug, Clone)]
pub(crate) struct LoadCooldownError {
    /// `group/name`.
    pub model: String,
    /// Consecutive failed loads counted so far.
    pub failures: u32,
    /// The failure that (re)armed the cooldown, clamped.
    pub last_error: String,
    /// Wall-clock instant the model may be retried at. Rendered from the
    /// monotonic deadline at the moment of refusal, so a clock that steps
    /// while a cooldown is running cannot lengthen or shorten it.
    pub retry_at: DateTime<Local>,
    /// The same interval in whole seconds, for `Retry-After`. At least 1: a
    /// `Retry-After: 0` invites exactly the hammering this exists to stop.
    pub retry_after_secs: u64,
}

impl std::fmt::Display for LoadCooldownError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "model {} is unavailable for another {} s: {} consecutive load \
             failures put it in a cooldown until {}. Last failure: {}",
            self.model,
            self.retry_after_secs,
            self.failures,
            self.retry_at.to_rfc3339(),
            self.last_error
        )
    }
}

impl std::error::Error for LoadCooldownError {}

/// Why a `spawn_model` failed, in the one dimension R9's cooldown cares
/// about: whether the attempt cost a worker process.
///
/// A load that never got past the registry — an unknown inference id, an
/// external input the environment does not provide, unparseable registry
/// TOML — is deterministic, costs microseconds, and is fixed by the user
/// editing config or setting a variable. Counting it would refuse the
/// corrected retry that follows a second later, which is exactly the flow the
/// external-inputs UI drives. Everything after it — spawn, handshake,
/// configure, `load`, a worker that dies streaming its weights — costs a
/// process, heavy imports and possibly gigabytes of transfers, and is what
/// finding Q5/B15 measured 93 of in 182 s.
struct LoadFailure {
    error: anyhow::Error,
    costed_worker: bool,
}

impl LoadFailure {
    fn config(error: anyhow::Error) -> Self {
        Self {
            error,
            costed_worker: false,
        }
    }

    fn worker(error: anyhow::Error) -> Self {
        Self {
            error,
            costed_worker: true,
        }
    }
}

/// One model's load-failure history (R9).
struct CooldownEntry {
    failures: u32,
    last_error: String,
    /// Monotonic deadline. The wall clock is only ever *rendered* from it.
    until: Instant,
    /// The window `until` was computed with, for `/health` and for pruning.
    window: Duration,
}

/// Per-model load-failure cooldowns (R9, finding Q5/B15: a model dying on
/// load was reloaded once per request with no counter, backoff or cap — 93
/// loads in 182 s, each one a process spawn and a torch import).
///
/// A pure state machine over an injected clock, like [`CacheState`]: the
/// schedule is the part worth testing and it should not need a worker to
/// test it.
///
/// The schedule is `base × 2^(failures−1)`, capped at `max`: with the
/// shipped 2 s/300 s that is 2, 4, 8, 16, 32, 64, 128, 256, 300, 300 … —
/// nine attempts and 8.5 minutes to reach the ceiling, which turns run1's 93
/// loads in 182 s into 6. It escalates from the *first* failure deliberately:
/// the load already failed, the next attempt costs another spawn and another
/// weight stream, and 2 s is small enough that a genuinely transient failure
/// (a claimed prewarmed worker that died in the gap) costs nothing worth
/// naming. No jitter: this is one host's own retry ladder, not a fleet
/// stampede, and every caller that arrives during a window is refused rather
/// than queued, so there is no thundering herd to spread out.
#[derive(Default)]
struct LoadCooldowns {
    entries: HashMap<String, CooldownEntry>,
}

impl LoadCooldowns {
    /// Record a failed load and return the window it armed (`None` when
    /// cooldowns are disabled).
    fn note_failure(
        &mut self,
        inference_id: &str,
        error: &str,
        policy: &LoadPolicy,
        now: Instant,
    ) -> Option<Duration> {
        if policy.cooldown_base.is_zero() {
            return None;
        }
        let entry = self
            .entries
            .entry(inference_id.to_owned())
            .or_insert(CooldownEntry {
                failures: 0,
                last_error: String::new(),
                until: now,
                window: Duration::ZERO,
            });
        entry.failures = entry.failures.saturating_add(1);
        entry.last_error = clamp_cooldown_error(error);
        // `failures - 1` doublings, clamped at the widest shift a `u32` can
        // represent. 32 would not be one: `1u32 << 32` is an overflowing
        // shift, which panics where overflow checks are on and *wraps to 1*
        // where they are not — silently dropping the window back to
        // `cooldown_base` from the 33rd consecutive failure onwards, i.e.
        // handing finding B15 its hammering back after a couple of hours of
        // a model that will not load. 31 doublings already put every
        // realistic base far past any realistic cap, and `checked_mul`
        // catches the rest.
        let doublings = (entry.failures - 1).min(31);
        let window = policy
            .cooldown_base
            .checked_mul(1u32 << doublings)
            .unwrap_or(policy.cooldown_max)
            .min(policy.cooldown_max)
            // Same reason as [`MAX_COOLDOWN_SECS`], applied to the window
            // itself so this holds for a hand-built [`LoadPolicy`] too: the
            // deadline below is an `Instant + Duration`, which panics rather
            // than saturates.
            .min(Duration::from_secs(MAX_COOLDOWN_SECS));
        entry.window = window;
        entry.until = now + window;
        Some(window)
    }

    /// A successful load clears the history: the ladder is about
    /// *consecutive* failures.
    fn clear(&mut self, inference_id: &str) {
        self.entries.remove(inference_id);
    }

    fn active(&self, inference_id: &str, now: Instant) -> Option<&CooldownEntry> {
        self.entries
            .get(inference_id)
            .filter(|entry| entry.until > now)
    }

    /// Forget a model whose cooldown expired longer ago than the ceiling.
    ///
    /// Two reasons, one of them a bound: the counter is only meaningful while
    /// the retries it is counting are still coming (a model that has been
    /// left alone for longer than the longest window deserves the full ladder
    /// again), and the keys come off the URL, so a map that only ever grew
    /// would be an unbounded allocation any client could drive with one
    /// failing load per made-up id.
    ///
    /// `checked_add` rather than `+`: `Instant + Duration` panics on
    /// overflow, this runs under the state mutex on every sweeper tick, and a
    /// forget-time too far away to represent means "not yet" — the direction
    /// that keeps the counter rather than the one that loses it.
    /// [`MAX_COOLDOWN_SECS`] already makes that unreachable from config; this
    /// keeps the function total for any [`LoadPolicy`] at all.
    fn prune(&mut self, policy: &LoadPolicy, now: Instant) {
        self.entries.retain(|_, entry| {
            entry
                .until
                .checked_add(policy.cooldown_max)
                .is_none_or(|forget_at| forget_at > now)
        });
    }
}

/// `GET /health` response (design §7, additive — Python has no such
/// endpoint, so this shape is ours to define). Serialized as-is by the HTTP
/// layer; `Deserialize` exists so tests can round-trip the wire shape.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct HealthReport {
    /// `"ok"` normally, `"shutting_down"` once shutdown has begun.
    pub status: String,
    /// Same signal as `status`, machine-friendly.
    pub shutting_down: bool,
    /// Whether the inference registry currently loads (see `health()` docs
    /// for exactly what this checks).
    pub registry_ok: bool,
    /// Number of loaded models (== `models.len()`).
    pub model_count: usize,
    /// Per loaded model liveness/queue snapshot, sorted by inference_id.
    pub models: Vec<ModelHealth>,
    /// Prewarm pool snapshot (design §8): master/lazy switches plus one
    /// entry per impl class held (state "warm" | "spawning" |
    /// "failed_prepare").
    pub prewarm: PrewarmHealth,
    /// Visible GPUs by board UUID (batch-calibration step 1a); empty when
    /// the host has no GPU inventory, in which case workers are not pinned.
    pub gpus: Vec<GpuInfo>,
    /// Per-GPU VRAM ledger: budgets, footprints, outstanding grants, ramp and
    /// deflation state and the fitted cost model (batch-calibration step 1b).
    /// Empty on a host with no GPU inventory, where nothing is admitted and
    /// every model takes the unpriced dispatch path.
    pub vram: Vec<GpuBudgetHealth>,
    /// Models whose loads are failing (R9), sorted by inference_id. An entry
    /// exists from the first failed load until a load succeeds or the history
    /// is pruned; `retry_after_secs` is 0 for one that has cooled down but is
    /// still counting (the next failure waits twice as long).
    pub load_cooldowns: Vec<LoadCooldownHealth>,
    /// The inference **client** side: one entry per endpoint this process
    /// holds a client for, sorted by base URL. Empty on a node that only
    /// serves inference (`panoptikon inferio`), which has no client — and on a
    /// gateway that has not yet talked to its endpoints.
    ///
    /// The models above describe what the orchestrator wants; this describes
    /// what the transport under it will actually carry. Run2's S1 was
    /// precisely a disagreement between the two that neither half could see.
    pub inference_clients: Vec<crate::inferio_client::InferenceTransportHealth>,
}

/// One model's load-failure cooldown in the [`HealthReport`] (R9). This is
/// the only place the state is visible when the model is *not* loaded — and
/// it never is, which is why it cannot live in `models[]`.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct LoadCooldownHealth {
    pub inference_id: String,
    /// Consecutive failed loads.
    pub failures: u32,
    /// The failure that armed the current window (clamped to 2000 bytes).
    pub last_error: String,
    /// RFC 3339 wall-clock instant the model may be retried at, rendered from
    /// the monotonic deadline when this report was built.
    pub retry_at: String,
    /// Whole seconds until then; 0 once the window has passed.
    pub retry_after_secs: u64,
    /// The window this failure count earned, in seconds — `base × 2^(n−1)`
    /// capped at the configured maximum.
    pub window_secs: u64,
}

/// One loaded model in the [`HealthReport`].
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ModelHealth {
    pub inference_id: String,
    /// Monotonic load generation (bumps on every respawn).
    pub generation: u64,
    /// Cache keys currently referencing the model, sorted.
    pub cache_keys: Vec<String>,
    /// WorkerSet occupancy: `free < total` means replicas are running
    /// windows right now.
    pub replicas: ReplicaHealth,
    /// Requests waiting in the model's FIFO queue.
    pub queue_depth: usize,
    /// Windows currently executing on replicas.
    pub in_flight_windows: usize,
    /// Unit budget of the grant on the most recently dispatched window;
    /// `null` until a window carries one (nothing dispatched yet, or this
    /// model is on the unpriced path — see `dispatch.rs`).
    pub last_grant_units: Option<u64>,
    /// Inputs in the most recently dispatched window; `null` until the first
    /// window dispatches. This is what a user cap bounds on the unpriced path.
    pub last_window_items: Option<u32>,
    /// Items the orchestrator is asking callers to keep inside their
    /// in-flight predict requests for this model — the same figure the
    /// `x-panoptikon-desired-in-flight-items` response header carries
    /// (`dispatch.rs`, `desired_in_flight_items`). `null` until a window has
    /// been formed.
    ///
    /// It is here because it was *not*: run2's S1 had to reconstruct this
    /// column arithmetically from `ramp_step`, `unit_budget` and
    /// `max_units_measured` in the logs, because the one number that crosses
    /// the core/orchestrator boundary was visible on neither side. With it,
    /// "the server asked for 1 632 and the job delivered 200" is a two-field
    /// comparison instead of a phase of analysis.
    pub desired_in_flight_items: Option<u64>,
    /// Predict requests ever queued on this model's dispatcher.
    pub total_predict_requests: u64,
    /// Windows ever dispatched to a replica.
    pub total_batches: u64,
    /// Of those, the ones formed short of the unit budget the ledger allowed:
    /// the queue, not the board, decided their size. A ramp that is not
    /// advancing while this climbs is being starved, not squeezed.
    pub queue_bound_windows: u64,
    /// Cost dimension resolved from registry metadata at load time
    /// (batch-calibration step 1a).
    pub cost: CostHealth,
    /// One entry per replica: which GPU it sits on and the freshest memory
    /// sensing it reported. This is the raw material step 1b's per-GPU
    /// ledger is built from.
    pub replicas_detail: Vec<ReplicaTelemetryHealth>,
}

/// Replica occupancy of one model's WorkerSet.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ReplicaHealth {
    pub total: usize,
    pub free: usize,
}

/// A model's cost dimension, as resolved from `metadata.cost`.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct CostHealth {
    /// `item` | `pixel` | `token` | `audio-second` | `none`.
    pub unit: String,
    /// `count` | `sum` | `max-times-count`; absent for the `none` class.
    pub aggregation: Option<String>,
    pub epoch: u32,
    /// First-touch batch before calibration; absent for the `none` class.
    pub seed_units: Option<u32>,
    /// True when the registry declared nothing usable and the conservative
    /// `(item, count)` fallback is in force.
    pub degraded: bool,
    /// The per-item **pixel canvas** this model's inputs are priced against
    /// (`metadata.cost.canvas_pixels`, or a canvas filled in from the model's
    /// own load report), or `null` for uncapped. Only `pixel`-priced models
    /// ever carry one.
    ///
    /// It is the difference between a grant that means what the operator
    /// thinks it means and one that does not: under a canvas the worker
    /// prices every input at `min(raw_pixels, canvas_pixels)`, so the same
    /// `last_grant_units` describes a very different batch depending on
    /// whether a canvas is in force — and the *effective* canvas may be one
    /// the registry never stated.
    pub canvas_pixels: Option<u32>,
}

impl From<CostDimension> for CostHealth {
    fn from(cost: CostDimension) -> Self {
        Self {
            unit: cost.unit.as_str().to_owned(),
            aggregation: cost.aggregation.map(|value| value.as_str().to_owned()),
            epoch: cost.epoch,
            seed_units: cost.seed_units,
            degraded: cost.degraded,
            canvas_pixels: cost.canvas_pixels,
        }
    }
}

/// Per-replica GPU placement plus its freshest memory report. Every field
/// after `gpu` is `null` until the worker reports it (no torch, a remote-API
/// impl, or no predict yet). A CPU or MPS host does report: its figures are
/// denominated in system RAM and Metal's budget respectively, not in VRAM
/// (docs/unified-memory-admission.md).
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ReplicaTelemetryHealth {
    /// Resolved device pin the worker was *spawned* with — a board UUID on a
    /// known CUDA inventory, a HIP device index on a known ROCm one (the two
    /// backends' visibility variables accept different vocabularies; see
    /// `gpu::pin_env_var`).
    pub gpu: Option<String>,
    /// The board the worker itself reports being on: the pin above can be an
    /// index, absent, or a UUID CUDA reordered, and only the worker can see
    /// what it actually got. `null` on a ROCm replica — torch's HIP-rendered
    /// UUID is a third vocabulary the worker deliberately suppresses, and
    /// those replicas are admitted to the ledger by PCI address instead
    /// (docs/rocm-batch-calibration-parity.md, D3), which this view does not
    /// surface yet.
    pub gpu_uuid: Option<String>,
    pub gpu_name: Option<String>,
    /// The worker venv's torch, part of the calibration profile key.
    pub torch_version: Option<String>,
    /// Process-level load footprint and how it was measured.
    pub base_mb: Option<u64>,
    pub base_method: Option<String>,
    pub reserved_at_load_mb: Option<u64>,
    /// Negotiated load precision, part of the calibration profile key.
    pub dtype: Option<String>,
    /// Freshest device sample, and how long ago it was recorded.
    pub free_mb: Option<u64>,
    pub total_mb: Option<u64>,
    /// Which driver reported `free_mb`/`total_mb` (`"nvml"` |
    /// `"amdgpu-sysfs"` | `"torch"`); they disagree by gigabytes, so a reader
    /// comparing samples needs it.
    pub free_source: Option<String>,
    pub reserved_mb: Option<u64>,
    pub allocated_mb: Option<u64>,
    pub memory_age_ms: Option<u64>,
    /// Measurements this replica has reported since it loaded, including any
    /// the bounded ring has since evicted.
    pub measurements_recorded: u64,
    /// The tail of the measurement ring, oldest first — a sample of what 1b's
    /// cost fit consumes, not the whole ring (health is a status page).
    pub recent_batches: Vec<BatchHealth>,
}

/// One measured GPU batch in [`ReplicaTelemetryHealth`].
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct BatchHealth {
    /// Per-worker sequence number; strictly increasing, gaps mean the ring
    /// evicted samples between reads.
    pub seq: u64,
    pub age_ms: u64,
    /// Inputs in the batch (not cost-dimension units — see the worker
    /// protocol's "Memory sensing").
    pub items: Option<u64>,
    pub reserved_before_mb: Option<u64>,
    pub peak_reserved_mb: Option<u64>,
    pub allocated_before_mb: Option<u64>,
    pub peak_allocated_mb: Option<u64>,
    pub duration_ms: Option<f64>,
}

impl ReplicaTelemetryHealth {
    /// How many ring entries `/health` shows per replica.
    const RECENT_BATCHES: usize = 4;

    fn snapshot(handle: &TelemetryHandle) -> Self {
        let mut telemetry = match handle.lock() {
            Ok(telemetry) => telemetry.clone(),
            // A poisoned telemetry mutex must not take the health endpoint
            // down; it is advisory data.
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        let now = Instant::now();
        let age_ms =
            |captured_at: Instant| now.saturating_duration_since(captured_at).as_millis() as u64;
        // The load report's own timestamp is kept in the telemetry for 1b
        // (a base measured long ago on a busy board is a weaker prior);
        // health only needs the values.
        let load = telemetry
            .load
            .take()
            .map_or_else(LoadReport::default, |stamped| stamped.value);
        let (memory, memory_age_ms) = match telemetry.memory.take() {
            Some(stamped) => (stamped.value, Some(age_ms(stamped.captured_at))),
            None => (MemorySample::default(), None),
        };
        let measurements_recorded = telemetry.recorded_measurements();
        let gpu = telemetry.gpu.take();
        // Read the ring without draining it: 1b's ledger is the other reader
        // and consumes by watermark (see `WorkerTelemetry::measurements`).
        let mut recent: Vec<BatchHealth> = telemetry
            .measurements()
            .rev()
            .take(Self::RECENT_BATCHES)
            .map(|sample| BatchHealth {
                seq: sample.seq,
                age_ms: age_ms(sample.captured_at),
                items: sample.measurement.items,
                reserved_before_mb: sample.measurement.reserved_before_mb,
                peak_reserved_mb: sample.measurement.peak_reserved_mb,
                allocated_before_mb: sample.measurement.allocated_before_mb,
                peak_allocated_mb: sample.measurement.peak_allocated_mb,
                duration_ms: sample.measurement.duration_ms,
            })
            .collect();
        recent.reverse();
        Self {
            gpu,
            gpu_uuid: load.gpu_uuid,
            gpu_name: load.gpu_name,
            torch_version: load.torch_version,
            base_mb: load.base_mb,
            base_method: load.base_method,
            reserved_at_load_mb: load.reserved_at_load_mb,
            dtype: load.dtype,
            free_mb: memory.free_mb,
            total_mb: memory.total_mb,
            free_source: memory.free_source,
            reserved_mb: memory.reserved_mb,
            allocated_mb: memory.allocated_mb,
            memory_age_ms,
            measurements_recorded,
            recent_batches: recent,
        }
    }
}

/// Per-cache-key entry expiration. `Never` is Python's `datetime.max`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expiration {
    Never,
    At(DateTime<Local>),
}

impl Expiration {
    /// `ttl_seconds >= 0` -> now + ttl; negative (-1 by convention) -> never
    /// (manager.py:77-81). `ttl_seconds` is an attacker-controlled query
    /// param, so the addition uses checked arithmetic: a value chrono cannot
    /// represent saturates to `Never` instead of panicking while the state
    /// mutex is held (a poisoned mutex would brick the whole manager).
    fn new(ttl_seconds: i64, now: DateTime<Local>) -> Self {
        if ttl_seconds < 0 {
            return Expiration::Never;
        }
        match chrono::Duration::try_seconds(ttl_seconds).and_then(|ttl| now.checked_add_signed(ttl))
        {
            Some(at) => Expiration::At(at),
            None => Expiration::Never,
        }
    }

    /// Rendering for `GET /cache/{key}`: Python serializes each expiration
    /// with `datetime.isoformat()` (router.py:219). "Never" is
    /// `datetime.max`, which Python renders as
    /// `"9999-12-31T23:59:59.999999"`; we return `None` and the HTTP layer
    /// maps it to that literal for wire parity.
    fn render(&self) -> Option<String> {
        match self {
            Expiration::Never => None,
            Expiration::At(at) => Some(isoformat(at)),
        }
    }
}

/// `datetime.isoformat()` for a naive local datetime: seconds precision
/// when the microsecond component is zero, otherwise exactly six fractional
/// digits; never a UTC offset (Python's `datetime.now()` is naive).
fn isoformat(at: &DateTime<Local>) -> String {
    let micros = (at.nanosecond() % 1_000_000_000) / 1_000;
    if micros == 0 {
        at.format("%Y-%m-%dT%H:%M:%S").to_string()
    } else {
        at.format("%Y-%m-%dT%H:%M:%S%.6f").to_string()
    }
}

/// Result of removing one (cache_key, inference_id) entry.
struct RemoveOutcome {
    /// Whether the entry existed in that cache key's LRU.
    was_present: bool,
    /// The model to unload when this was its last reference.
    unload: Option<String>,
}

/// Pure LRU/TTL/refcount state machine — no workers, no clocks (callers
/// inject `now`), so the exact port of manager.py's bookkeeping is unit
/// testable in isolation. Methods return the inference ids whose last
/// reference disappeared; the caller owns actually unloading them.
#[derive(Default)]
struct CacheState {
    /// Python `_lru_caches`: per cache key, insertion-ordered id -> expiry.
    lru_caches: HashMap<String, LinkedHashMap<String, Expiration>>,
    /// Python `_cache_key_map`: id -> cache keys referencing it.
    cache_refs: HashMap<String, HashSet<String>>,
    /// Predict/load pin refcounts (design §5): pinned models are skipped by
    /// TTL expiry entirely.
    pins: HashMap<String, u32>,
}

impl CacheState {
    /// The `load_model` bookkeeping (manager.py:69-85): add the cache-key
    /// reference, move the entry to most-recent and renew its expiration
    /// (OrderedDict `move_to_end` + assignment == remove + insert-at-back),
    /// then enforce `lru_size`. Returns models to unload due to eviction.
    fn touch_load(
        &mut self,
        inference_id: &str,
        cache_key: &str,
        lru_size: i64,
        ttl_seconds: i64,
        now: DateTime<Local>,
    ) -> Vec<String> {
        self.cache_refs
            .entry(inference_id.to_owned())
            .or_default()
            .insert(cache_key.to_owned());
        let lru = self.lru_caches.entry(cache_key.to_owned()).or_default();
        lru.remove(inference_id);
        lru.insert(inference_id.to_owned(), Expiration::new(ttl_seconds, now));
        self.resize(cache_key, lru_size)
    }

    /// `_resize_lru` (manager.py:100-112): evict oldest while over size.
    /// Python's `while len > lru_size` runs for negative sizes too, which
    /// would evict everything including the entry just added — the caller
    /// treats that as a refused load (see module docs).
    fn resize(&mut self, cache_key: &str, lru_size: i64) -> Vec<String> {
        let mut unloads = Vec::new();
        let Some(lru) = self.lru_caches.get_mut(cache_key) else {
            return unloads;
        };
        while lru.len() as i64 > lru_size {
            let Some((evicted, _)) = lru.pop_front() else {
                break;
            };
            if let Some(refs) = self.cache_refs.get_mut(&evicted) {
                refs.remove(cache_key);
                if refs.is_empty() {
                    self.cache_refs.remove(&evicted);
                    unloads.push(evicted);
                }
            }
        }
        unloads
    }

    /// `_remove_from_lru` (manager.py:41-52): drop one entry and its
    /// reference; report the model for unload when that was the last ref.
    fn remove(&mut self, cache_key: &str, inference_id: &str) -> RemoveOutcome {
        let was_present = self
            .lru_caches
            .get_mut(cache_key)
            .is_some_and(|lru| lru.remove(inference_id).is_some());
        if !was_present {
            return RemoveOutcome {
                was_present: false,
                unload: None,
            };
        }
        let mut unload = None;
        if let Some(refs) = self.cache_refs.get_mut(inference_id) {
            refs.remove(cache_key);
            if refs.is_empty() {
                self.cache_refs.remove(inference_id);
                unload = Some(inference_id.to_owned());
            }
        }
        RemoveOutcome {
            was_present: true,
            unload,
        }
    }

    /// `clear_cache` (manager.py:120-132): drop a whole cache key. Returns
    /// (entries removed, models to unload).
    fn clear(&mut self, cache_key: &str) -> (usize, Vec<String>) {
        let Some(lru) = self.lru_caches.remove(cache_key) else {
            return (0, Vec::new());
        };
        let count = lru.len();
        let mut unloads = Vec::new();
        for (inference_id, _) in lru {
            if let Some(refs) = self.cache_refs.get_mut(&inference_id) {
                refs.remove(cache_key);
                if refs.is_empty() {
                    self.cache_refs.remove(&inference_id);
                    unloads.push(inference_id);
                }
            }
        }
        (count, unloads)
    }

    /// `check_ttl_expired` (manager.py:143-153): strict `now > expiration`,
    /// but pinned models are skipped entirely (design §5: a model can't
    /// expire mid-inference or mid-load).
    fn expire(&mut self, now: DateTime<Local>) -> Vec<String> {
        let mut expired: Vec<(String, String)> = Vec::new();
        for (cache_key, lru) in &self.lru_caches {
            for (inference_id, expiration) in lru {
                if self.pins.get(inference_id).copied().unwrap_or(0) > 0 {
                    continue;
                }
                if let Expiration::At(at) = expiration
                    && now > *at
                {
                    expired.push((cache_key.clone(), inference_id.clone()));
                }
            }
        }
        let mut unloads = Vec::new();
        for (cache_key, inference_id) in expired {
            tracing::debug!(model = %inference_id, cache_key = %cache_key, "TTL expired");
            if let Some(id) = self.remove(&cache_key, &inference_id).unload {
                unloads.push(id);
            }
        }
        unloads
    }

    fn pin(&mut self, inference_id: &str) {
        *self.pins.entry(inference_id.to_owned()).or_insert(0) += 1;
    }

    /// Drop one pin without touching expirations (the load-phase pin).
    fn unpin(&mut self, inference_id: &str) {
        if let Some(count) = self.pins.get_mut(inference_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.pins.remove(inference_id);
            }
        }
    }

    /// Post-predict unpin + TTL restore: each completing predict restores
    /// `now + requested ttl` on its own cache-key entry (Python's `finally`
    /// re-load, router.py:117-124), so the last completed predict's TTL is
    /// what stands. No effect when the entry was removed meanwhile
    /// (explicit unload wins; we never resurrect).
    fn unpin_restore(
        &mut self,
        inference_id: &str,
        cache_key: &str,
        ttl_seconds: i64,
        now: DateTime<Local>,
    ) {
        self.unpin(inference_id);
        if let Some(lru) = self.lru_caches.get_mut(cache_key)
            && let Some(expiration) = lru.get_mut(inference_id)
        {
            *expiration = Expiration::new(ttl_seconds, now);
        }
    }

    /// Fatal-worker-death cleanup: drop the model from every LRU and the
    /// ref map (pins are left to unwind naturally as in-flight predicts
    /// observe their errors).
    fn remove_everywhere(&mut self, inference_id: &str) {
        for lru in self.lru_caches.values_mut() {
            lru.remove(inference_id);
        }
        self.cache_refs.remove(inference_id);
    }

    fn refs_non_empty(&self, inference_id: &str) -> bool {
        self.cache_refs
            .get(inference_id)
            .is_some_and(|refs| !refs.is_empty())
    }

    /// `list_loaded_models` (manager.py:134-138): id -> cache keys.
    fn cached_models(&self) -> BTreeMap<String, Vec<String>> {
        self.cache_refs
            .iter()
            .map(|(inference_id, refs)| {
                let mut keys: Vec<String> = refs.iter().cloned().collect();
                keys.sort();
                (inference_id.clone(), keys)
            })
            .collect()
    }

    /// Sorted cache keys referencing one model (health reporting); empty
    /// when the model has no references.
    fn cache_keys(&self, inference_id: &str) -> Vec<String> {
        let mut keys: Vec<String> = self
            .cache_refs
            .get(inference_id)
            .map(|refs| refs.iter().cloned().collect())
            .unwrap_or_default();
        keys.sort();
        keys
    }

    /// `get_ttl_expiration` (manager.py:140-141): unknown keys yield an
    /// empty map (Python's defaultdict).
    fn expirations(&self, cache_key: &str) -> BTreeMap<String, Option<String>> {
        self.lru_caches
            .get(cache_key)
            .map(|lru| {
                lru.iter()
                    .map(|(inference_id, expiration)| (inference_id.clone(), expiration.render()))
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// A freshly spawned WorkerSet plus everything the model entry needs to
/// record about it.
struct SpawnedModel {
    workers: Vec<Worker>,
    /// One per worker, in the same order: `Some` when the replica landed on a
    /// board the ledger knows and the model's cost dimension scales.
    admissions: Vec<Option<Admission>>,
    registry_default_batch: Option<u32>,
    impl_class: String,
    /// Whether any replica's resolved pin matched the prewarm pool's, i.e.
    /// whether keeping a warm worker for this class can ever pay off.
    claim_eligible: bool,
    cost: CostDimension,
}

/// A loaded model: the dispatcher queue plus the task owning its WorkerSet.
struct ModelHandle {
    tx: mpsc::UnboundedSender<DispatchMsg>,
    task: JoinHandle<()>,
    /// Monotonic load generation, for death-cleanup races and (in tests)
    /// respawn detection.
    generation: u64,
    /// Health counters shared with the dispatcher task (design §7): the
    /// dispatcher writes, `health()` reads — Relaxed atomics, no locking.
    stats: Arc<ModelStats>,
    /// Cost dimension resolved from registry metadata when this entry loaded
    /// (batch-calibration step 1a). Resolved at load, not per request, so a
    /// running model keeps the dimension it was priced with.
    cost: CostDimension,
    /// One telemetry handle per replica, shared with the workers the
    /// dispatcher owns. Step 1b's ledger reads these.
    telemetry: Vec<TelemetryHandle>,
}

#[derive(Default)]
struct ManagerState {
    cache: CacheState,
    /// Python `_models`: inference_id -> loaded model.
    models: HashMap<String, ModelHandle>,
    /// Dispatcher tasks still draining after an unload; awaited on shutdown.
    draining: Vec<JoinHandle<()>>,
    next_generation: u64,
    shutting_down: bool,
    /// Per-model load-failure cooldowns (R9). Under the state mutex rather
    /// than beside the load locks because every read of it is already taking
    /// this lock for the loaded-check next to it.
    cooldowns: LoadCooldowns,
}

/// RAII handle for a pin refcount taken in [`CacheState`]. Every pin
/// (predict-duration and spawn-phase alike) is wrapped in one of these
/// immediately, so any early return or *future cancellation* (a client
/// disconnecting at `reply_rx.await`, or mid-spawn) still releases the pin
/// — a leaked pin would exempt the model from TTL expiry forever.
///
/// For predict pins `restore` carries the requested (cache_key, ttl): Drop
/// runs `unpin_restore`, preserving the last-completed-predict-wins TTL
/// semantics (Python's `finally: load_model(ttl)`). Spawn-phase pins carry
/// no restore and Drop is a plain `unpin`. Drop is sync — it only takes the
/// state mutex, never awaits.
struct PinGuard {
    /// Weak so a guard alive past manager teardown is a no-op.
    manager: Weak<ModelManager>,
    inference_id: String,
    /// `Some((cache_key, ttl_seconds))` for predict pins: restore the
    /// requested TTL on release.
    restore: Option<(String, i64)>,
}

impl PinGuard {
    /// Wrap a pin the caller already took (under the state lock, so the
    /// pin stays atomic with the loaded-check). Does not lock.
    fn adopt(manager: &ModelManager, inference_id: &str, restore: Option<(String, i64)>) -> Self {
        Self {
            manager: manager
                .weak
                .get()
                .cloned()
                .expect("weak self is set in new()"),
            inference_id: inference_id.to_owned(),
            restore,
        }
    }

    /// Release the pin now, under a state lock the caller already holds
    /// (Drop re-locks and would deadlock), and defuse the guard.
    fn release_locked(mut self, cache: &mut CacheState) {
        Self::release(&mut self.restore, &self.inference_id, cache);
        // Defused: Drop upgrades an empty Weak and does nothing.
        self.manager = Weak::new();
    }

    fn release(restore: &mut Option<(String, i64)>, inference_id: &str, cache: &mut CacheState) {
        match restore.take() {
            Some((cache_key, ttl_seconds)) => {
                cache.unpin_restore(inference_id, &cache_key, ttl_seconds, Local::now());
            }
            None => cache.unpin(inference_id),
        }
    }
}

impl Drop for PinGuard {
    fn drop(&mut self) {
        let Some(manager) = self.manager.upgrade() else {
            return;
        };
        // Ignore a poisoned mutex: panicking inside Drop would abort, and a
        // poisoned manager is already beyond caring about one refcount.
        if let Ok(mut state) = manager.state.lock() {
            Self::release(&mut self.restore, &self.inference_id, &mut state.cache);
        }
    }
}

/// Board-admission bucket for a replica whose board key does not resolve (a
/// host with no inventory, a pin the ledger cannot place). Never a real board
/// key — `GpuInventory::resolve_board_key` only ever answers a uuid it
/// probed — and never taken *instead of* the real boards: such a replica
/// takes this bucket **and** every board's permit
/// ([`ModelManager::acquire_load_admission`]).
///
/// It sorts before every uuid, which keeps the sorted acquisition order a
/// total order once it is mixed in with them.
const UNRESOLVED_BOARD_ADMISSION_KEY: &str = "";

/// What one pass of [`ModelManager::touch_and_check`] decided.
enum TouchOutcome {
    /// The model is loaded and the caller is done: `Some` carries the
    /// dispatcher sender and the predict pin, `None` is a plain `PUT /load`.
    Ready(Option<(mpsc::UnboundedSender<DispatchMsg>, PinGuard)>),
    /// The model is not loaded. `Some` is the spawn-phase pin, taken in the
    /// same critical section that found it missing (second pass only).
    NeedsSpawn(Option<PinGuard>),
}

/// RAII handle for one model's load lock (R6, module docs lock 2).
///
/// It owns the lock guard *and* the table entry's lifetime: on drop it
/// releases the mutex and then removes the entry when nobody else is holding
/// or waiting on it, so `load_locks` never accumulates one entry per model id
/// this process has ever been asked for. The guard is an `Option` so the
/// handle can be built *before* the (possibly long) wait for the mutex: a
/// caller cancelled while queued behind another load of the same model still
/// runs this Drop and still tidies the table.
///
/// Drop is sync and takes only the leaf `load_locks` mutex, so it can run
/// from any context, cancellation included.
struct ModelLoadGuard<'a> {
    manager: &'a ModelManager,
    inference_id: &'a str,
    /// The same `Arc` the table holds. Counting strong references against
    /// this one is what decides whether the entry may go.
    lock: Arc<TokioMutex<()>>,
    guard: Option<OwnedMutexGuard<()>>,
}

impl Drop for ModelLoadGuard<'_> {
    fn drop(&mut self) {
        // Release the mutex first: the owned guard holds a strong reference
        // of its own, so the count below only means what it says once it is
        // gone.
        drop(self.guard.take());
        // A poisoned table is not worth aborting a Drop over; what it holds
        // is a mutex, not state.
        let mut locks = match self.manager.load_locks.lock() {
            Ok(locks) => locks,
            Err(poisoned) => poisoned.into_inner(),
        };
        // Exactly two strong references *under the table lock* — the table's
        // and this handle's — proves nobody else can be waiting: every other
        // holder took its clone from the table, which requires this lock, and
        // `Mutex::lock_owned` cannot be awaited without holding a clone.
        if Arc::strong_count(&self.lock) == 2 {
            locks.remove(self.inference_id);
        }
    }
}

/// The model manager. Construct with [`ModelManager::new`] (requires a
/// running tokio runtime — it spawns the sweeper task).
pub struct ModelManager {
    cfg: ManagerConfig,
    registry: Arc<StdMutex<RegistryCache>>,
    state: StdMutex<ManagerState>,
    /// Prewarm pool (design §8): one parked warm worker per impl class.
    /// Its own mutex, never held together with `state`.
    prewarm: Arc<PrewarmPool>,
    /// Per-GPU VRAM budget arbiter (batch-calibration step 1b). Its own
    /// mutex, never held together with `state`; every operation on it is
    /// synchronous bounded arithmetic, so it is safe to touch from the
    /// dispatcher's hot path.
    ledger: Arc<VramLedger>,
    /// One load lock per model id (module docs, lock 2): the serialization
    /// that stops two callers spawning the same model twice, and *only*
    /// that. Entries are created on demand and removed again by
    /// [`ModelLoadGuard`] as soon as the last holder lets go, so the table's
    /// size tracks the models being loaded right now rather than every id
    /// this process was ever asked for — the id comes straight off the URL,
    /// so a table that only grew would be an unbounded allocation any client
    /// could drive. The std mutex around it is a leaf: held for a lookup, an
    /// insert or a removal, never across anything.
    load_locks: StdMutex<HashMap<String, Arc<TokioMutex<()>>>>,
    /// The board-admission gate (module docs, lock 3): one semaphore of
    /// `max_concurrent_loads` permits per board key, plus one shared bucket
    /// for replicas whose board key does not resolve. Created on demand; the
    /// keyspace is closed (`GpuInventory::resolve_board_key` only ever
    /// answers a uuid from the one-shot inventory probe, or `None`), so
    /// unlike `load_locks` this table needs no pruning.
    load_admission: StdMutex<HashMap<String, Arc<Semaphore>>>,
    /// Shutdown barrier (module docs, lock 1): read-guarded by every load
    /// that may spawn, write-guarded once by [`ModelManager::shutdown`],
    /// which therefore waits for every in-flight load to have decided what
    /// to do with the workers it spawned.
    load_barrier: TokioRwLock<()>,
    /// Self-reference handed to dispatcher tasks for death cleanup.
    weak: OnceLock<Weak<ModelManager>>,
    sweeper: StdMutex<Option<JoinHandle<()>>>,
}

impl ModelManager {
    pub fn new(cfg: ManagerConfig, registry: Arc<StdMutex<RegistryCache>>) -> Arc<Self> {
        let sweep_interval = cfg.sweep_interval;
        // Pooled workers are pinned to the same default GPU an unpinned
        // replica resolves to, or the pool's workers could never be claimed
        // (claim eligibility is pin equality — see `spawn_model`).
        let prewarm = PrewarmPool::new(cfg.spawn.clone(), cfg.prewarm.clone(), cfg.gpus.clone());
        // The ledger's board set comes from the same one-shot probe the pins
        // do, so a grant's board key and the board a worker's spawn pin
        // selects can never describe different hardware. The calibration store primes
        // it (fit, expected base, and — from local profiles only — the
        // ratchet anchor) and receives its updates.
        let ledger = VramLedger::new(&cfg.gpus, cfg.vram.clone(), cfg.calibration.clone());
        let manager = Arc::new(Self {
            cfg,
            registry,
            state: StdMutex::new(ManagerState::default()),
            prewarm,
            ledger,
            load_locks: StdMutex::new(HashMap::new()),
            load_admission: StdMutex::new(HashMap::new()),
            load_barrier: TokioRwLock::new(()),
            weak: OnceLock::new(),
            sweeper: StdMutex::new(None),
        });
        // The always_warm whitelist warms at startup in every launch mode
        // (gateway and the `inferio` subcommand construct a manager; the
        // eager DB-scan loop is gateway-only and started by main.rs).
        manager.prewarm.warm_always();
        manager
            .weak
            .set(Arc::downgrade(&manager))
            .expect("weak self is set exactly once");
        // The sweeper holds only a Weak so dropping the last Arc (without an
        // explicit shutdown) also ends the task on its next tick.
        let weak = Arc::downgrade(&manager);
        let sweeper = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(sweep_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let Some(manager) = weak.upgrade() else { break };
                manager.sweep();
            }
        });
        *manager.sweeper.lock().unwrap() = Some(sweeper);
        manager
    }

    /// `PUT /load/{group}/{id}`: idempotent load — spawns the worker when
    /// the model isn't loaded, always renews TTL + LRU position and
    /// enforces `lru_size` (the cron preload loop and UI eager-load rely on
    /// the renewal). `prewarm_hint` is the request's optional `prewarm`
    /// query param (absent = true): `Some(false)` suppresses the lazy-warm
    /// rule for this load (design §8 — extraction jobs pass it so
    /// batch-only families don't hold warm workers).
    pub async fn load_model(
        &self,
        inference_id: &str,
        cache_key: &str,
        lru_size: i64,
        ttl_seconds: i64,
        prewarm_hint: Option<bool>,
    ) -> Result<()> {
        self.ensure_loaded(
            inference_id,
            cache_key,
            lru_size,
            ttl_seconds,
            false,
            prewarm_hint.unwrap_or(true),
        )
        .await
        .map(|_| ())
    }

    /// `POST /predict/{group}/{id}`: auto-loads like Python (router.py:107
    /// calls `load_model` first), pins the model for the duration, queues
    /// the request on the model's dispatcher, and restores the requested
    /// TTL afterwards whether the predict succeeded or not (Python's
    /// `finally`). `prewarm_hint` as on [`ModelManager::load_model`]; it
    /// only matters when this predict is the one that auto-loads the model.
    #[allow(clippy::too_many_arguments)]
    pub async fn predict(
        &self,
        inference_id: &str,
        cache_key: &str,
        lru_size: i64,
        ttl_seconds: i64,
        max_batch: Option<u32>,
        prewarm_hint: Option<bool>,
        inputs: Vec<WorkerInput>,
    ) -> Result<Vec<WorkerOutput>> {
        let (tx, pin) = self
            .ensure_loaded(
                inference_id,
                cache_key,
                lru_size,
                ttl_seconds,
                true,
                prewarm_hint.unwrap_or(true),
            )
            .await?
            .expect("ensure_loaded returns a sender when pinning");
        let (reply_tx, reply_rx) = oneshot::channel();
        let request = DispatchRequest {
            inputs,
            max_batch,
            reply: reply_tx,
        };
        // Both arms are typed [`Unattempted`] (R2a): the send failed because
        // the dispatch task had already ended — the fatal arm closes the
        // receiver, so the tail of a dying model's window lands here
        // microseconds after its siblings — and a dropped reply sender is how
        // a window running on a *surviving* replica learns that a sibling
        // died (`in_flight.shutdown()` aborts those tasks). Neither request
        // ran; both may be re-submitted once.
        let result = if tx.send(DispatchMsg::Predict(request)).is_err() {
            Err(Unattempted::error(format!(
                "model {inference_id} was unloaded before the request could be queued"
            )))
        } else {
            match reply_rx.await {
                Ok(result) => result,
                Err(_) => Err(Unattempted::error(format!(
                    "the dispatcher for model {inference_id} dropped the request"
                ))),
            }
        };
        // The guard's Drop does the unpin + requested-TTL restore (Python's
        // `finally`); dropping explicitly keeps the restore at completion
        // time, and cancellation at `reply_rx.await` runs the same Drop.
        drop(pin);
        // A window just settled, which is when the ledger's contention picture
        // is freshest — and this window's own grant request is what may have
        // flagged an idle neighbour. Waiting for the sweep tick would delay
        // relief by up to `sweep_interval`.
        self.deliver_pending_trims();
        result
    }

    /// Items the orchestrator would like a caller to keep inside in-flight
    /// predict requests for this model (test protocol §8 G7, brief (b)).
    ///
    /// Written by the model's dispatcher on every window formation
    /// (`dispatch::desired_in_flight_items`) and read here without
    /// disturbing it — the same Relaxed-atomic contract `health()` uses. The
    /// HTTP layer reads it right after a predict and puts it on the response;
    /// `None` (model not loaded, or loaded but nothing dispatched yet) is
    /// reported as an absent field, which callers read as "no opinion".
    pub fn desired_in_flight_items(&self, inference_id: &str) -> Option<u64> {
        let state = self.state.lock().unwrap();
        let handle = state.models.get(inference_id)?;
        match handle.stats.desired_in_flight_items.load(Relaxed) {
            0 => None,
            value => Some(value),
        }
    }

    /// `DELETE /cache/{key}/{group}/{id}`: remove one entry; unload the
    /// model when that was its last reference. Returns whether the entry
    /// existed.
    pub async fn unload_model(&self, cache_key: &str, inference_id: &str) -> Result<bool> {
        let mut state = self.state.lock().unwrap();
        tracing::debug!(model = %inference_id, cache_key = %cache_key, "unload requested");
        let outcome = state.cache.remove(cache_key, inference_id);
        if let Some(id) = outcome.unload {
            Self::begin_unload(&mut state, &id);
        }
        Ok(outcome.was_present)
    }

    /// `DELETE /cache/{key}`: clear a whole cache key; unload models whose
    /// last reference lived there. Returns the number of entries removed.
    pub async fn clear_cache(&self, cache_key: &str) -> Result<usize> {
        let mut state = self.state.lock().unwrap();
        tracing::debug!(cache_key = %cache_key, "clearing cache");
        let (count, unloads) = state.cache.clear(cache_key);
        for id in &unloads {
            Self::begin_unload(&mut state, id);
        }
        Ok(count)
    }

    /// `GET /cache`: inference_id -> cache keys referencing it.
    pub fn cached_models(&self) -> BTreeMap<String, Vec<String>> {
        self.state.lock().unwrap().cache.cached_models()
    }

    /// `GET /cache/{key}`: inference_id -> expiration rendered like Python's
    /// `datetime.isoformat()`; `None` means never (Python renders
    /// `datetime.max`, i.e. `"9999-12-31T23:59:59.999999"` — the HTTP layer
    /// maps `None` to that literal for wire parity).
    pub fn cache_expirations(&self, cache_key: &str) -> BTreeMap<String, Option<String>> {
        self.state.lock().unwrap().cache.expirations(cache_key)
    }

    /// `GET /health` (design §7, additive): a snapshot of orchestrator and
    /// per-model state, assembled from the shared [`ModelStats`] atomics
    /// without disturbing any dispatcher.
    ///
    /// `registry_ok`: the cheapest *correct* signal is `RegistryCache::get()`
    /// — it is mtime-gated, so when nothing changed on disk it only stats
    /// the config dirs and returns the cached snapshot; when a file did
    /// change, the reload it performs is exactly the one `/metadata` and
    /// the next spawn would run anyway (no extra work is ever forced). A
    /// broken registry TOML therefore surfaces as `registry_ok: false`
    /// without affecting already-loaded models. The registry lock is taken
    /// and released before the state lock (the two are never held together).
    pub fn health(&self) -> HealthReport {
        let registry_ok = self.registry.lock().unwrap().get().is_ok();
        // Pool and ledger snapshots before the state lock: none of these
        // mutexes are ever held together.
        let prewarm = self.prewarm.health();
        let vram = self.ledger.health();
        let state = self.state.lock().unwrap();
        let mut models: Vec<ModelHealth> = state
            .models
            .iter()
            .map(|(inference_id, handle)| {
                let stats = &handle.stats;
                ModelHealth {
                    inference_id: inference_id.clone(),
                    generation: handle.generation,
                    cache_keys: state.cache.cache_keys(inference_id),
                    replicas: ReplicaHealth {
                        total: stats.replicas_total.load(Relaxed),
                        free: stats.replicas_free.load(Relaxed),
                    },
                    queue_depth: stats.queue_len.load(Relaxed),
                    in_flight_windows: stats.in_flight_windows.load(Relaxed),
                    last_grant_units: match stats.last_grant_units.load(Relaxed) {
                        // 0 = no grant yet (real budgets are >= 1).
                        0 => None,
                        units => Some(units),
                    },
                    last_window_items: match stats.last_window_items.load(Relaxed) {
                        0 => None,
                        items => Some(items),
                    },
                    desired_in_flight_items: match stats.desired_in_flight_items.load(Relaxed) {
                        // 0 = not computed yet (nothing dispatched).
                        0 => None,
                        items => Some(items),
                    },
                    total_predict_requests: stats.total_predict_requests.load(Relaxed),
                    total_batches: stats.total_batches.load(Relaxed),
                    queue_bound_windows: stats.queue_bound_windows.load(Relaxed),
                    cost: handle.cost.into(),
                    replicas_detail: handle
                        .telemetry
                        .iter()
                        .map(ReplicaTelemetryHealth::snapshot)
                        .collect(),
                }
            })
            .collect();
        models.sort_by(|a, b| a.inference_id.cmp(&b.inference_id));
        // R9: the models whose loads are failing, which by construction are
        // never in `models` above.
        let now = Instant::now();
        let wall_now = Local::now();
        let mut load_cooldowns: Vec<LoadCooldownHealth> = state
            .cooldowns
            .entries
            .iter()
            .map(|(inference_id, entry)| {
                let remaining = entry.until.saturating_duration_since(now);
                LoadCooldownHealth {
                    inference_id: inference_id.clone(),
                    failures: entry.failures,
                    last_error: entry.last_error.clone(),
                    retry_at: (wall_now
                        + chrono::Duration::from_std(remaining)
                            .unwrap_or_else(|_| chrono::Duration::zero()))
                    .to_rfc3339(),
                    retry_after_secs: remaining.as_secs(),
                    window_secs: entry.window.as_secs(),
                }
            })
            .collect();
        load_cooldowns.sort_by(|a, b| a.inference_id.cmp(&b.inference_id));
        HealthReport {
            status: if state.shutting_down {
                "shutting_down"
            } else {
                "ok"
            }
            .to_owned(),
            shutting_down: state.shutting_down,
            registry_ok,
            model_count: models.len(),
            models,
            prewarm,
            gpus: self
                .cfg
                .gpus
                .gpus()
                .map(<[GpuInfo]>::to_vec)
                .unwrap_or_default(),
            vram,
            load_cooldowns,
            inference_clients: crate::inferio_client::endpoint_health(),
        }
    }

    /// The prewarm pool (design §8), for the eager task and tests.
    pub(crate) fn prewarm_pool(&self) -> &Arc<PrewarmPool> {
        &self.prewarm
    }

    /// The registry cache, for the eager task's setter -> impl-class
    /// mapping (same mtime-gated snapshot `/metadata` and spawns use).
    pub(crate) fn registry_cache(&self) -> &Arc<StdMutex<RegistryCache>> {
        &self.registry
    }

    /// Graceful shutdown: stop the sweeper, refuse new loads/predicts, fail
    /// queued requests, and run every worker's graceful stop ladder. A load
    /// still in flight when the flag flips finishes its spawn, observes
    /// `shutting_down`, and parks a worker-discard task in `draining` —
    /// write-locking `load_barrier` below waits for that decision (every
    /// load that may spawn holds a read guard for its whole slow phase) so
    /// the second drain awaits the discard instead of abandoning the worker
    /// mid-stop. A load that has not reached its slow phase yet queues
    /// behind the write lock — tokio's `RwLock` is write-preferring — and
    /// then bails on the `shutting_down` check without spawning anything.
    pub async fn shutdown(&self) {
        if let Some(handle) = self.sweeper.lock().unwrap().take() {
            handle.abort();
        }
        let mut handles = Vec::new();
        {
            let mut state = self.state.lock().unwrap();
            state.shutting_down = true;
            for (_, handle) in state.models.drain() {
                let _ = handle.tx.send(DispatchMsg::Shutdown);
                handles.push(handle.task);
            }
            handles.append(&mut state.draining);
            state.cache = CacheState::default();
        }
        {
            let _drain_guard = self.load_barrier.write().await;
            let mut state = self.state.lock().unwrap();
            handles.append(&mut state.draining);
        }
        // Parked prewarmed workers get the same graceful unload ladder,
        // concurrently with the dispatcher drains (design §8; both inside
        // the caller's existing shutdown envelope).
        let drain = async {
            for handle in handles {
                if let Err(err) = handle.await
                    && err.is_panic()
                {
                    tracing::error!("inferio dispatcher task panicked during shutdown: {err}");
                }
            }
        };
        tokio::join!(drain, self.prewarm.shutdown());
        // Last: the calibration a window earned seconds before the quit is
        // still sitting behind the store's write debounce, and losing it
        // would silently mean re-ramping on the next run.
        if let Some(calibration) = self.cfg.calibration.clone() {
            let _ = tokio::task::spawn_blocking(move || calibration.flush()).await;
        }
    }

    /// Called by a dispatcher task after a fatal worker death: drop the
    /// model from all bookkeeping so the next predict auto-loads a fresh
    /// worker. The generation guards against a dispatcher that lost a race
    /// with a respawn removing the newer entry.
    pub(crate) fn handle_worker_death(&self, inference_id: &str, generation: u64) {
        let mut state = self.state.lock().unwrap();
        let matches = state
            .models
            .get(inference_id)
            .is_some_and(|handle| handle.generation == generation);
        if !matches {
            return;
        }
        tracing::warn!(model = %inference_id, "worker died fatally; dropping model from all caches");
        let handle = state
            .models
            .remove(inference_id)
            .expect("presence checked above");
        // The dispatcher task is about to exit; keep its handle so shutdown
        // still awaits it.
        state.draining.push(handle.task);
        state.cache.remove_everywhere(inference_id);
    }

    /// Sweeper tick: expire TTLs, unload models whose last reference
    /// expired, reap finished drain tasks, and ask every surviving model's
    /// dispatcher to check that its idle replicas are still alive.
    ///
    /// The liveness ask is what stops a dead *grantless* model from being
    /// advertised forever (P5-6): a death is otherwise only discovered by a
    /// request failing on the pipe, and a model nobody predicts against
    /// never reads from it. Sent after the TTL expiry so a model already
    /// unloading is not asked (`begin_unload` removed it), and as a message
    /// rather than a direct probe because the dispatcher — not the manager —
    /// owns the workers.
    fn sweep(&self) {
        let mut state = self.state.lock().unwrap();
        if state.shutting_down {
            return;
        }
        state.draining.retain(|handle| !handle.is_finished());
        // R9: forget the load-failure history of a model nobody has retried
        // for longer than the longest cooldown window (see `prune`).
        let policy = self.cfg.loads;
        state.cooldowns.prune(&policy, Instant::now());
        let unloads = state.cache.expire(Local::now());
        for id in unloads {
            Self::begin_unload(&mut state, &id);
        }
        for handle in state.models.values() {
            let _ = handle.tx.send(DispatchMsg::ReapIdle);
        }
        drop(state);
        self.deliver_pending_trims();
    }

    /// Route the ledger's idle-resident trim requests to the dispatchers that
    /// own those replicas (docs/batch-calibration-design.md, "Trim for idle
    /// residents").
    ///
    /// The ledger is the only component that sees a squeezed worker and its
    /// idle neighbour's pool slack at once, but it cannot call a worker —
    /// dispatchers own workers. So it raises a signal and this drains it. Two
    /// callers, deliberately: the sweep tick guarantees delivery on an
    /// otherwise quiet server, and the predict path makes it prompt on a busy
    /// one (the drain costs one uncontended lock and a `Vec::is_empty` when
    /// there is nothing to do, which is the normal case).
    ///
    /// A model that is no longer in `state.models` — unloaded, mid-teardown,
    /// respawned under a new generation — simply gets no message: the entry is
    /// removed before its dispatcher is told to shut down, so the lookup here
    /// *is* the generation guard, and a stale send would land on a closed
    /// channel regardless.
    fn deliver_pending_trims(&self) {
        let trims = self.ledger.take_pending_trims();
        if trims.is_empty() {
            return;
        }
        let state = self.state.lock().unwrap();
        for trim in trims {
            if let Some(handle) = state.models.get(&trim.inference_id) {
                let _ = handle.tx.send(DispatchMsg::Trim(trim.worker));
            }
        }
    }

    /// Start unloading a model whose last reference is gone: hand its
    /// dispatcher a Shutdown (it drains, runs the worker's graceful stop
    /// ladder, and exits) and keep the task handle for shutdown to await.
    fn begin_unload(state: &mut ManagerState, inference_id: &str) {
        if let Some(handle) = state.models.remove(inference_id) {
            tracing::debug!(model = %inference_id, "unloading model");
            let _ = handle.tx.send(DispatchMsg::Shutdown);
            state.draining.push(handle.task);
        }
    }

    /// One pass of the load bookkeeping, entirely under the state mutex
    /// (module docs, lock 4): renew the LRU entry and its TTL, run the
    /// evictions that causes, and decide whether this caller is done.
    ///
    /// Called twice per load — once on the fast path, once again under the
    /// model's load lock — because every step of it has to be atomic with
    /// the loaded-check that follows: `touch_load` is what puts the
    /// reference back that stops the sweeper expiring the model between the
    /// check and the pin, and the pin is what stops it expiring the model
    /// between the check and the enqueue. Running it twice is harmless
    /// (`touch_load` is a remove+insert plus a resize) and is what makes the
    /// slow path a proper double-checked load.
    ///
    /// `take_spawn_pin` is set by the second call only: the spawn-phase pin
    /// has to be taken in the same critical section that found the model
    /// missing.
    fn touch_and_check(
        &self,
        inference_id: &str,
        cache_key: &str,
        lru_size: i64,
        ttl_seconds: i64,
        pin_for_predict: bool,
        take_spawn_pin: bool,
    ) -> Result<TouchOutcome> {
        let mut state = self.state.lock().unwrap();
        if state.shutting_down {
            bail!("the model manager is shutting down");
        }
        let unloads =
            state
                .cache
                .touch_load(inference_id, cache_key, lru_size, ttl_seconds, Local::now());
        for id in &unloads {
            Self::begin_unload(&mut state, id);
        }
        if !state.cache.refs_non_empty(inference_id) {
            // The just-inserted entry was evicted by its own resize
            // (lru_size <= 0). Python loads anyway and leaks the
            // process forever; we refuse (see module docs).
            bail!(
                "lru_size {lru_size} evicted {inference_id} from cache '{cache_key}' immediately; refusing to load"
            );
        }
        if let Some(handle) = state.models.get(inference_id) {
            if pin_for_predict {
                let tx = handle.tx.clone();
                state.cache.pin(inference_id);
                let guard = PinGuard::adopt(
                    self,
                    inference_id,
                    Some((cache_key.to_owned(), ttl_seconds)),
                );
                return Ok(TouchOutcome::Ready(Some((tx, guard))));
            }
            return Ok(TouchOutcome::Ready(None));
        }
        // Pin across the spawn so the sweeper cannot expire the entry
        // mid-load (Python has this race: its sweeper uses a separate
        // lock from load_model). The guard releases the pin even when
        // the calling future is cancelled mid-spawn.
        let spawn_pin = take_spawn_pin.then(|| {
            state.cache.pin(inference_id);
            PinGuard::adopt(self, inference_id, None)
        });
        Ok(TouchOutcome::NeedsSpawn(spawn_pin))
    }

    /// R9: refuse the load outright while this model is inside its
    /// load-failure cooldown, with everything the caller needs to say so
    /// (`http.rs` renders the 503 + `Retry-After`; a job aborts rather than
    /// walking every one of its items into the same wall).
    ///
    /// Deliberately *not* consulted for a model that is already loaded: the
    /// cooldown is about the load path only, and a resident model's predicts
    /// are none of its business.
    fn check_load_cooldown(&self, inference_id: &str) -> Result<()> {
        let now = Instant::now();
        let state = self.state.lock().unwrap();
        let Some(entry) = state.cooldowns.active(inference_id, now) else {
            return Ok(());
        };
        let remaining = entry.until.saturating_duration_since(now);
        let error = LoadCooldownError {
            model: inference_id.to_owned(),
            failures: entry.failures,
            last_error: entry.last_error.clone(),
            retry_at: Local::now()
                + chrono::Duration::from_std(remaining)
                    .unwrap_or_else(|_| chrono::Duration::zero()),
            // Round up, and never below 1: `Retry-After: 0` invites exactly
            // the hammering the cooldown exists to stop.
            retry_after_secs: remaining.as_secs_f64().ceil().max(1.0) as u64,
        };
        drop(state);
        tracing::debug!(
            model = %inference_id,
            failures = error.failures,
            retry_after_secs = error.retry_after_secs,
            "refusing a load: the model is in its load-failure cooldown"
        );
        Err(anyhow::Error::new(error))
    }

    /// Undo the bookkeeping of a load that was refused before it ever ran
    /// (R9). Python's rule for a load that did not happen is that no LRU
    /// entry is left behind (manager.py:89-95), and a refusal is exactly
    /// that; without this, a cooling-down model would accumulate cache-key
    /// references it can never serve. `pin` is the spawn-phase pin when the
    /// refusal happened after it was taken — released under the same lock, as
    /// everywhere else.
    fn forget_refused_load(&self, inference_id: &str, cache_key: &str, pin: Option<PinGuard>) {
        let mut state = self.state.lock().unwrap();
        if let Some(pin) = pin {
            pin.release_locked(&mut state.cache);
        }
        let outcome = state.cache.remove(cache_key, inference_id);
        if let Some(id) = outcome.unload {
            Self::begin_unload(&mut state, &id);
        }
    }

    /// This model's load lock (module docs, lock 2), created on demand.
    ///
    /// The handle is built *before* the wait so that a caller cancelled while
    /// queued behind another load of the same model still runs the table
    /// cleanup in [`ModelLoadGuard::drop`].
    async fn lock_model_load<'a>(&'a self, inference_id: &'a str) -> ModelLoadGuard<'a> {
        let lock = {
            let mut locks = self.load_locks.lock().unwrap();
            Arc::clone(
                locks
                    .entry(inference_id.to_owned())
                    .or_insert_with(|| Arc::new(TokioMutex::new(()))),
            )
        };
        let mut handle = ModelLoadGuard {
            manager: self,
            inference_id,
            lock,
            guard: None,
        };
        handle.guard = Some(Arc::clone(&handle.lock).lock_owned().await);
        handle
    }

    /// One admission permit per **distinct board** this replica set will land
    /// on (module docs, lock 3), acquired in sorted key order.
    ///
    /// Sorting is the deadlock argument for the multi-board case: every
    /// caller takes the permits of the boards it needs in the same total
    /// order, so two loads that overlap on two boards can never each hold the
    /// other's.
    ///
    /// **A replica whose board key did not resolve counts as landing on every
    /// board.** `resolve_board_key` answers `None` for a handful of strings
    /// `resolve_pin` still hands to the backend's visibility variable —
    /// an ambiguous UUID prefix, an index this host cannot see, a device
    /// *list*, a `MIG-` instance — so such a replica really does spawn and
    /// really does take memory; the ledger simply cannot say whose. Charging
    /// it only against a shared "unresolved" bucket would let it stream its
    /// weights beside an unpinned load onto the very board it landed on,
    /// which is a guarantee the retired host-wide lock did give. So it takes
    /// the shared bucket *and* one permit per board in the inventory: at
    /// `max_concurrent_loads = 1` that is host-wide serialization, exactly as
    /// before, and it is paid only by a pin nobody could resolve. On a host
    /// with no inventory there are no boards to add and the shared bucket
    /// alone is that same serialization.
    async fn acquire_load_admission(
        &self,
        inference_id: &str,
        board_keys: &[Option<String>],
    ) -> Vec<OwnedSemaphorePermit> {
        let mut wanted: Vec<&str> = board_keys.iter().flatten().map(String::as_str).collect();
        if board_keys.iter().any(Option::is_none) {
            wanted.push(UNRESOLVED_BOARD_ADMISSION_KEY);
            wanted.extend(
                self.cfg
                    .gpus
                    .gpus()
                    .unwrap_or_default()
                    .iter()
                    .map(|gpu| gpu.uuid.as_str()),
            );
        }
        wanted.sort_unstable();
        wanted.dedup();
        let permits = self.cfg.loads.max_concurrent_loads.max(1);
        let mut held = Vec::with_capacity(wanted.len());
        for board in wanted {
            let gate = {
                let mut gates = self.load_admission.lock().unwrap();
                Arc::clone(
                    gates
                        .entry(board.to_owned())
                        .or_insert_with(|| Arc::new(Semaphore::new(permits))),
                )
            };
            if gate.available_permits() == 0 {
                tracing::debug!(
                    model = %inference_id,
                    gpu = %board,
                    max_concurrent_loads = permits,
                    "waiting for the board's load-admission gate"
                );
            }
            held.push(
                gate.acquire_owned()
                    .await
                    .expect("the load admission semaphores are never closed"),
            );
        }
        held
    }

    /// The shared load path, in two phases (module docs, "Locking").
    ///
    /// **Fast path**: one `state` critical section. A model that is already
    /// resident is served from it without awaiting a single lock, which is
    /// what makes a predict immune to any load happening elsewhere on the
    /// host (finding P5-3/B18).
    ///
    /// **Slow path**: the shutdown barrier, then this model's own load lock,
    /// then the same bookkeeping again (the second half of the double-checked
    /// load: another caller may have loaded the model while we queued), then
    /// the spawn under the board-admission gate.
    ///
    /// With `pin_for_predict` the model is pinned *atomically* with the
    /// loaded-check and the dispatcher sender is returned (paired with the
    /// RAII [`PinGuard`] that owns the pin), so a predict can never observe
    /// its model expiring between load and enqueue — and a cancelled caller
    /// can never leak the pin.
    async fn ensure_loaded(
        &self,
        inference_id: &str,
        cache_key: &str,
        lru_size: i64,
        ttl_seconds: i64,
        pin_for_predict: bool,
        prewarm_hint: bool,
    ) -> Result<Option<(mpsc::UnboundedSender<DispatchMsg>, PinGuard)>> {
        if let TouchOutcome::Ready(sender) = self.touch_and_check(
            inference_id,
            cache_key,
            lru_size,
            ttl_seconds,
            pin_for_predict,
            false,
        )? {
            return Ok(sender);
        }
        // R9, before the queueing starts: a model whose loads are failing is
        // refused now rather than after each caller has waited its turn to
        // spawn one more doomed worker.
        if let Err(cooldown) = self.check_load_cooldown(inference_id) {
            self.forget_refused_load(inference_id, cache_key, None);
            return Err(cooldown);
        }

        // Lock order (module docs): barrier, then this model's load lock,
        // then state, then — inside `spawn_model` — the admission gate.
        let _drain_guard = self.load_barrier.read().await;
        let _model_guard = self.lock_model_load(inference_id).await;

        let spawn_pin = match self.touch_and_check(
            inference_id,
            cache_key,
            lru_size,
            ttl_seconds,
            pin_for_predict,
            true,
        )? {
            // Another caller loaded it while we waited for the model lock.
            TouchOutcome::Ready(sender) => return Ok(sender),
            TouchOutcome::NeedsSpawn(pin) => pin.expect("the second pass takes the spawn pin"),
        };
        // Again under the model lock: what we queued behind may have been the
        // very load that failed. Without this second check a burst of N
        // requests still costs N spawns, which is exactly finding B15.
        if let Err(cooldown) = self.check_load_cooldown(inference_id) {
            self.forget_refused_load(inference_id, cache_key, Some(spawn_pin));
            return Err(cooldown);
        }

        let spawn_result = self.spawn_model(inference_id).await;
        let mut state = self.state.lock().unwrap();
        // Release the spawn pin under the same lock as the bookkeeping
        // below so the sweeper cannot expire the fresh entry in between.
        spawn_pin.release_locked(&mut state.cache);
        let SpawnedModel {
            workers,
            admissions,
            registry_default_batch,
            impl_class,
            claim_eligible,
            cost,
        } = match spawn_result {
            Ok(spawned) => spawned,
            Err(LoadFailure {
                error,
                costed_worker,
            }) => {
                // Python removes the requesting cache key's entry and
                // re-raises (manager.py:89-95): no LRU entry is left behind
                // after a failed load.
                let outcome = state.cache.remove(cache_key, inference_id);
                if let Some(id) = outcome.unload {
                    Self::begin_unload(&mut state, &id);
                }
                // R9: this is the one place a *load* is known to have failed
                // — a worker that raised in `load()`, a spawn that could not
                // start, a process that died streaming its weights (the
                // respawn-after-death loop enters here too, since a respawn is
                // a load like any other). The bookkeeping refusals above are
                // deliberately not counted either: an lru_size refusal or an
                // unload that raced the spawn says nothing about the model's
                // ability to come up.
                let chain = format!("{error:#}");
                let window = costed_worker
                    .then(|| {
                        state.cooldowns.note_failure(
                            inference_id,
                            &chain,
                            &self.cfg.loads,
                            Instant::now(),
                        )
                    })
                    .flatten();
                let failures = state
                    .cooldowns
                    .entries
                    .get(inference_id)
                    .map_or(0, |entry| entry.failures);
                drop(state);
                if let Some(window) = window {
                    tracing::warn!(
                        model = %inference_id,
                        failures,
                        cooldown_secs = window.as_secs_f64(),
                        "load failed; refusing further loads of this model until the \
                         cooldown expires"
                    );
                }
                return Err(error.context(format!("failed to load model {inference_id}")));
            }
        };
        if state.shutting_down || !state.cache.refs_non_empty(inference_id) {
            // Explicitly unloaded (or the manager shut down) while the
            // workers were spawning: discard the whole set instead of
            // registering it. The discard task is parked in `draining` so
            // shutdown() (which re-checks after write-locking the barrier
            // this load holds a read guard on) awaits the graceful stops
            // instead of abandoning them on a detached task.
            // Dropping `admissions` here is what un-charges the replicas in
            // the ledger — a set that was never registered as a model must
            // not keep holding its footprint.
            drop(admissions);
            let discard = tokio::spawn(async move {
                futures_util::future::join_all(workers.into_iter().map(Worker::shutdown)).await;
            });
            state.draining.push(discard);
            drop(state);
            bail!("model {inference_id} was unloaded while it was loading");
        }
        let generation = state.next_generation;
        state.next_generation += 1;
        let (tx, rx) = mpsc::unbounded_channel();
        // Health counters (design §7): replica counts are seeded here so a
        // health() call between registration and the dispatcher's first
        // poll already reports the true WorkerSet size.
        let stats = Arc::new(ModelStats::default());
        stats.replicas_total.store(workers.len(), Relaxed);
        stats.replicas_free.store(workers.len(), Relaxed);
        // Grab the telemetry handles before the dispatcher takes the workers:
        // the manager is the budget arbiter, so it keeps its own read path
        // into every replica's memory reports.
        let telemetry: Vec<TelemetryHandle> = workers.iter().map(Worker::telemetry).collect();
        let context = DispatcherContext {
            inference_id: inference_id.to_owned(),
            generation,
            cost,
            // The registry's `default_batch_size` keeps exactly one job: the
            // fixed window size of the unpriced path. Priced models are sized
            // by the ledger.
            unpriced_window_items: registry_default_batch.unwrap_or(self.cfg.default_max_batch),
            manager: self.weak.get().cloned().expect("weak self is set in new()"),
            stats: Arc::clone(&stats),
            unload_grace: self.cfg.spawn.deadlines.unload_grace,
        };
        // The dispatcher owns the whole WorkerSet (design §8): every replica
        // serves the one shared FIFO queue behind this sender, and carries
        // its ledger handle so window sizing and grants are per replica (two
        // replicas can sit on boards with different headroom).
        let replicas: Vec<Replica> = workers
            .into_iter()
            .zip(admissions)
            .map(|(worker, admission)| Replica { worker, admission })
            .collect();
        let task = tokio::spawn(run_dispatcher(context, replicas, rx));
        let sender = if pin_for_predict {
            state.cache.pin(inference_id);
            let guard = PinGuard::adopt(
                self,
                inference_id,
                Some((cache_key.to_owned(), ttl_seconds)),
            );
            Some((tx.clone(), guard))
        } else {
            None
        };
        state.models.insert(
            inference_id.to_owned(),
            ModelHandle {
                tx,
                task,
                generation,
                stats,
                cost,
                telemetry,
            },
        );
        // R9: the ladder counts *consecutive* failures, and this model just
        // came up.
        state.cooldowns.clear(inference_id);
        drop(state);
        // Lazy warm (design §8): a model of this class just loaded (claim
        // or fresh spawn) — keep one warm worker of the class for next
        // time, unless the request said prewarm=false. Respawn-on-claim is
        // exactly this rule firing after a claim emptied the slot. Runs
        // outside the state lock (the pool has its own mutex) and only
        // schedules a background task. Skipped when the spec has no
        // unpinned replica: claim() can never hand an (unpinned) pooled
        // worker to a fully device-pinned family, so a warm worker would
        // sit unclaimable forever — pure RAM burn.
        if prewarm_hint && claim_eligible {
            self.prewarm.lazy_warm(&impl_class);
        }
        Ok(sender)
    }

    /// Spawn + handshake + configure + load the model's whole WorkerSet
    /// (design §8, protocol v2 flow — handshake carries the impl class
    /// identity, `configure` binds the model's kwargs and instantiates):
    /// one worker per entry of the spec's `device_pins`, each pinned via the
    /// backend's device-visibility variable at spawn, all spawned and loaded
    /// *concurrently*. Any replica failing kills the others — a load either
    /// yields the complete set or nothing (no partial sets to reason
    /// about). The registry is re-resolved at every spawn (design §4:
    /// workers are always born on current config). Errors carry the
    /// worker's traceback/stderr context from `Worker`.
    ///
    /// Universal worker→GPU pinning (batch-calibration design, "Every worker
    /// is pinned to exactly one GPU"): the registry has no GPU knowledge, so
    /// each replica's pin — including the "no pin" default — is resolved
    /// here against the probed inventory, normally to a board UUID. An
    /// unknown inventory resolves to exactly what the registry said
    /// (including `None`), i.e. today's behaviour.
    ///
    /// Prewarm claim (design §8): at most one replica is served from the
    /// pool's parked worker for the impl class, if one is alive (the pool
    /// pings before handing it over). Eligibility is **pin equality**: a
    /// pooled worker sits on the default GPU, so it can serve any replica
    /// whose resolved pin is that same GPU (both-`None` on an unknown
    /// inventory included). The claimed worker skips spawn + handshake +
    /// heavy imports and only needs `configure` + `load`; the remaining
    /// replicas fresh-spawn as before.
    async fn spawn_model(&self, inference_id: &str) -> Result<SpawnedModel, LoadFailure> {
        // The registry phase, before any process exists. Its failures are
        // config errors (unknown id, an unresolved external input, broken
        // registry TOML) and are marked as costing no worker, so R9's
        // cooldown does not refuse the user's corrected retry a second later.
        let (spec, registry_default_batch, cost) = {
            let mut registry = self.registry.lock().unwrap();
            let resolved = registry
                .get()
                .context("failed to load the inference registry")
                .and_then(|snapshot| {
                    let spec = snapshot.spawn_spec(inference_id)?;
                    Ok((
                        spec,
                        registry_default_batch(&snapshot, inference_id),
                        CostDimension::resolve(&snapshot, inference_id),
                    ))
                });
            match resolved {
                Ok(resolved) => resolved,
                Err(error) => return Err(LoadFailure::config(error)),
            }
        };
        tracing::debug!(
            model = %inference_id,
            unit = cost.unit.as_str(),
            aggregation = cost.aggregation.map(|value| value.as_str()),
            epoch = cost.epoch,
            seed_units = cost.seed_units,
            priced = cost.scales(),
            degraded = cost.degraded,
            "resolved cost dimension"
        );
        let replica_count = spec.device_pins.len();
        let device_pins: Vec<Option<String>> = spec
            .device_pins
            .iter()
            .map(|pin| self.cfg.gpus.resolve_pin(pin.as_deref()))
            .collect();
        // The same registry entries resolved into the *other* vocabulary:
        // the ledger's board keys. Pin and key are a pair and must be
        // resolved from the same request — the pin is what the worker's
        // visibility variable gets, the key is what the ledger's board map
        // is keyed by, and on ROCm (index pins) or with an abbreviated CUDA
        // UUID the two are different strings for one board
        // (`GpuInventory::resolve_board_key`).
        let board_keys: Vec<Option<String>> = spec
            .device_pins
            .iter()
            .map(|pin| self.cfg.gpus.resolve_board_key(pin.as_deref()))
            .collect();
        // The board-admission gate (R6, module docs lock 3). Everything from
        // here to the end of the function is the phase the retired global
        // load lock existed for: a prewarm claim, a process spawn, the
        // ledger's load reservations and the multi-second `load` round trip
        // that streams the weights those reservations cover. Bounding it per
        // board rather than per host is the whole point — a load onto board 1
        // cannot collide with the weights landing on board 0 — and one permit
        // per board (the default) is exactly the serialization the global
        // lock gave every board that had a load in flight.
        //
        // Held for the rest of the function via the RAII permits; a cancelled
        // caller releases them like every other guard here.
        let _admission = self.acquire_load_admission(inference_id, &board_keys).await;
        // And into the third thing one registry entry decides: the address of
        // the board it names, when that board is a unified one whose worker
        // has to count GTT as its own (DP-5). Resolved from the same entries
        // as the other two so the three cannot disagree about which board a
        // replica is meant to land on — and handed to the worker as an
        // address rather than a flag, so it can tell whether it did.
        let unified_boards: Vec<Option<String>> = spec
            .device_pins
            .iter()
            .map(|pin| self.cfg.gpus.unified_pin_bdf(pin.as_deref()))
            .collect();
        // A prewarmed process was spawned before this model's just-in-time
        // external inputs were resolved. Models with explicit worker env
        // must therefore use a fresh process.
        let pool_pin = self.cfg.gpus.default_pin();
        let claim_replica = (spec.env.is_empty() && spec.env_remove.is_empty())
            .then(|| device_pins.iter().position(|pin| *pin == pool_pin))
            .flatten();
        let mut claimed = match claim_replica {
            Some(_) => {
                self.prewarm
                    .claim(&spec.impl_class, pool_pin.as_deref())
                    .await
            }
            // Either this model needs explicit worker env, or every replica
            // sits on a GPU other than the pool's — handing a pooled worker
            // to one of those would violate its pin.
            None => None,
        };
        // Load reservations (design: "A load in progress is a reservation
        // too"). Charged here — under the manager's load lock, before any
        // worker is spawned, so a window granted to a *different* model
        // during this multi-second load cannot collide with the incoming
        // weights. `dtype` is unknown on a first load (Package-1 negotiation
        // resolves during it), which is why the ledger reserves at its most
        // conservative tier in that case. Released when these guards drop,
        // which happens on every exit path including a cancelled future.
        //
        // `cost` goes in because a model that will never be granted a window is
        // not worth reserving for: the ledger answers `None` for the
        // `none`-class, and likewise for a model whose earlier load in this run
        // reported no device footprint at all.
        //
        // Keyed by board key, never by the pin: the pin is written in the
        // backend's visibility vocabulary and only coincides with the ledger
        // key on a CUDA host with a full-UUID pin.
        //
        // Sequential rather than joined: the reservations are microseconds of
        // bookkeeping apart from the host probe one of them may run, and that
        // probe is single-flight per board anyway — the first board's answer
        // enumerates every other one, so a second concurrent probe would be
        // suppressed rather than parallel.
        let mut _load_reservations: Vec<LoadReservation> = Vec::new();
        for gpu in board_keys.iter().flatten() {
            if let Some(reservation) = self
                .ledger
                .reserve_load(inference_id, cost, gpu, None)
                .await
            {
                _load_reservations.push(reservation);
            }
        }
        let spawns: Vec<_> = device_pins
            .iter()
            .enumerate()
            .map(|(replica, device)| {
                let claimed = if Some(replica) == claim_replica {
                    claimed.take()
                } else {
                    None
                };
                let spec = &spec;
                let device = device.clone();
                let unified = unified_boards[replica].clone();
                async move {
                    let spawn = self.cfg.spawn.for_unified_board(unified.as_deref());
                    let mut worker = match claimed {
                        Some(worker) => {
                            match self
                                .configure_claimed(
                                    worker,
                                    inference_id,
                                    spec,
                                    device.clone(),
                                    &spawn,
                                )
                                .await
                            {
                                Ok(worker) => worker,
                                Err(err) => return Err(err),
                            }
                        }
                        None => {
                            Worker::spawn_configured(&spawn, inference_id, spec, device).await?
                        }
                    };
                    if let Err(err) = worker.load().await {
                        // A load `error` frame leaves the worker alive; kill it
                        // either way so a failed load never leaks a process
                        // (fatal paths already reaped the child — kill is
                        // idempotent).
                        worker.kill().await;
                        return Err(err);
                    }
                    anyhow::Ok((replica, worker))
                }
            })
            .collect();
        let mut workers: Vec<Worker> = Vec::with_capacity(replica_count);
        let mut admissions: Vec<Option<Admission>> = Vec::with_capacity(replica_count);
        let mut first_error: Option<anyhow::Error> = None;
        let results = futures_util::future::join_all(spawns).await;
        // The one place the loaded model's per-item pixel canvas is settled
        // (run2 change R7), before anything downstream is told a number: the
        // ledger's registration below carries it onto every grant, and the
        // dispatcher context built by the caller prices its windows with it.
        // A replica that failed reports nothing; the survivors are replicas of
        // one model, so the first figure any of them resolved is the model's.
        let reported_canvas = results
            .iter()
            .filter_map(|result| result.as_ref().ok())
            .find_map(|(_, worker)| {
                let handle = worker.telemetry();
                let telemetry = match handle.lock() {
                    Ok(telemetry) => telemetry,
                    Err(poisoned) => poisoned.into_inner(),
                };
                telemetry
                    .load
                    .as_ref()
                    .and_then(|stamped| stamped.value.canvas_pixels)
            });
        let cost = canvas_in_force(inference_id, cost, reported_canvas);
        for result in results {
            match result {
                Ok((replica, worker)) => {
                    tracing::debug!(
                        model = %inference_id,
                        replica,
                        device = device_pins[replica].as_deref().unwrap_or("<unpinned>"),
                        "replica loaded"
                    );
                    // Register with the ledger now that the load response has
                    // landed: the board identity, the measured base and the
                    // pool size at load all come from it, and the board the
                    // *worker* reports is the authoritative one. `None` means
                    // this replica gets no admission (a `none`-class model, a
                    // worker with no GPU, a board outside the inventory) and
                    // its dispatcher takes the unpriced path.
                    //
                    // The board key this replica's *pin* named goes in as
                    // well, purely as a diagnostic: the ledger admits under
                    // what the worker reports either way, but a divergence
                    // between the two is the one observable symptom of a
                    // board-row order that is not the backend's device order
                    // (docs/rocm-batch-calibration-parity.md, D2/D3).
                    admissions.push(self.ledger.register_worker(
                        inference_id,
                        cost,
                        &worker.telemetry(),
                        board_keys[replica].as_deref(),
                    ));
                    workers.push(worker);
                }
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
        }
        if let Some(err) = first_error {
            // Whole-set load atomicity: kill the replicas that did come up
            // and un-charge them (dropping the admissions).
            drop(admissions);
            futures_util::future::join_all(workers.into_iter().map(Worker::kill)).await;
            return Err(LoadFailure::worker(err));
        }
        Ok(SpawnedModel {
            workers,
            admissions,
            registry_default_batch,
            impl_class: spec.impl_class,
            claim_eligible: claim_replica.is_some(),
            cost,
        })
    }

    /// Bind a claimed prewarmed worker to the concrete model. A
    /// [`WorkerError`] from `configure` (bad kwargs, failing `__init__`) is
    /// a genuine failure a fresh spawn would reproduce — kill the worker
    /// and propagate. A *fatal* error (the worker died between the claim
    /// ping and configure) falls back to one fresh `spawn_configured`, so a
    /// stale pooled worker can never fail a load that would otherwise have
    /// succeeded.
    async fn configure_claimed(
        &self,
        mut worker: Worker,
        inference_id: &str,
        spec: &SpawnSpec,
        device: Option<String>,
        // The caller's per-replica spawn config (`for_unified_board`), so a
        // respawn after a dead pooled worker gets the same environment the
        // fresh path would have given it.
        spawn: &WorkerSpawnConfig,
    ) -> Result<Worker> {
        match worker.configure(inference_id, &spec.config_kwargs).await {
            Ok(()) => Ok(worker),
            Err(err) if err.downcast_ref::<WorkerError>().is_some() => {
                worker.kill().await;
                Err(err)
            }
            Err(err) => {
                tracing::warn!(
                    model = %inference_id,
                    "claimed prewarmed worker died before configure; falling back to a fresh spawn: {err:#}"
                );
                // The fatal path already killed and reaped the child.
                Worker::spawn_configured(spawn, inference_id, spec, device).await
            }
        }
    }

    /// Test hook: the load generation of a currently-loaded model, for
    /// asserting worker reuse vs. respawn without touching timing.
    #[cfg(test)]
    pub(crate) fn loaded_generation(&self, inference_id: &str) -> Option<u64> {
        self.state
            .lock()
            .unwrap()
            .models
            .get(inference_id)
            .map(|handle| handle.generation)
    }
}

/// The per-item pixel canvas the loaded model is priced against (run2 change
/// R7), folded into its cost dimension once, here, where the registry's
/// declaration and the workers' load reports are both in hand.
///
/// **The registry wins.** A declared figure is a maintainer's statement about
/// the model's geometry, derived from its source and reviewed; the reported
/// one is an attribute read off an object graph nobody here controls, and the
/// worker's introspection is deliberately floored rather than trusted
/// (`packing.CANVAS_FLOOR_PIXELS`). Letting a reading override a declaration
/// would also make a *wrong* attribute unfixable from config — the one place
/// a maintainer can act.
///
/// The report is what covers the model the registry cannot state statically:
/// `doctr/dots_ocr`'s canvas lives in an `AutoProcessor` config downloaded
/// with the weights, so nothing outside a loaded process can know it. Without
/// this fold the host would price that model's windows in raw submitted
/// pixels while the worker priced its batches in capped ones.
///
/// Only for a `pixel`-priced model: the cap is an area, and capping a token
/// count or an item count by an area is meaningless (and inert anyway under
/// `count`, where `min(1, cap)` is 1). A worker reports what it found without
/// knowing its own unit — the dimension only reaches it on a grant — so the
/// gate is here.
fn canvas_in_force(
    inference_id: &str,
    cost: CostDimension,
    reported: Option<u32>,
) -> CostDimension {
    if cost.unit != CostUnit::Pixel {
        return CostDimension {
            canvas_pixels: None,
            ..cost
        };
    }
    let (canvas_pixels, source) = match (cost.canvas_pixels, reported) {
        (Some(declared), _) => (Some(declared), "the registry"),
        (None, Some(measured)) => (Some(measured), "the loaded impl, via its load report"),
        (None, None) => (None, "nothing"),
    };
    match canvas_pixels {
        Some(pixels) => tracing::debug!(
            model = %inference_id,
            canvas_pixels = pixels,
            "pricing each input at min(raw pixels, {pixels}), the canvas {source} states"
        ),
        None => tracing::debug!(
            model = %inference_id,
            "no per-item pixel canvas declared or reported; pricing raw \
             submitted pixels, as before run2"
        ),
    }
    CostDimension {
        canvas_pixels,
        ..cost
    }
}

/// The model's `default_batch_size` from registry metadata, resolved the
/// way Python consumers do (models.py:66-78 / extraction.rs merge_metadata):
/// group metadata overlaid by id metadata, id wins. Non-positive values are
/// treated as absent.
fn registry_default_batch(registry: &Registry, full_inference_id: &str) -> Option<u32> {
    let (group_name, inference_id) = full_inference_id.split_once('/')?;
    let group = registry.groups.get(group_name)?;
    let entry = group.inference_ids.get(inference_id)?;
    let value = entry
        .metadata
        .get("default_batch_size")
        .or_else(|| group.group_metadata.get("default_batch_size"))?;
    let value = value.as_i64()?;
    u32::try_from(value).ok().filter(|value| *value > 0)
}

#[cfg(test)]
mod tests {
    use super::super::calibration::{ProfileQuery, ProfileSeed, ProfileUpdate};
    use super::super::registry::RegistryConfig;
    use super::super::worker::WorkerDeadlines;
    use super::*;
    use serde_json::json;
    use std::fs;
    use std::path::{Path, PathBuf};

    // ------------------------------------------------------------------
    // Pure state-machine tests (no workers, injected clock).
    // ------------------------------------------------------------------

    fn at(now: DateTime<Local>, seconds: i64) -> DateTime<Local> {
        now + chrono::Duration::seconds(seconds)
    }

    /// LRU eviction is oldest-first on insert: with lru_size 2, loading a
    /// third model evicts the first-inserted one, and since that was its
    /// only cache-key reference it is reported for unload.
    #[test]
    fn lru_evicts_oldest_first() {
        let now = Local::now();
        let mut cache = CacheState::default();
        assert!(cache.touch_load("g/a", "k", 2, -1, now).is_empty());
        assert!(cache.touch_load("g/b", "k", 2, -1, now).is_empty());
        let unloads = cache.touch_load("g/c", "k", 2, -1, now);
        assert_eq!(
            unloads,
            vec!["g/a".to_string()],
            "oldest entry evicted and unloaded"
        );
        assert!(!cache.refs_non_empty("g/a"));
        assert!(cache.refs_non_empty("g/b") && cache.refs_non_empty("g/c"));
    }

    /// Repeated load renews the LRU position (Python move_to_end,
    /// manager.py:73-74): after re-loading `a`, adding a third model evicts
    /// `b` — the now-oldest — not `a`.
    #[test]
    fn reload_moves_entry_to_most_recent() {
        let now = Local::now();
        let mut cache = CacheState::default();
        cache.touch_load("g/a", "k", 2, -1, now);
        cache.touch_load("g/b", "k", 2, -1, now);
        cache.touch_load("g/a", "k", 2, -1, now); // renew: a becomes most recent
        let unloads = cache.touch_load("g/c", "k", 2, -1, now);
        assert_eq!(
            unloads,
            vec!["g/b".to_string()],
            "b was oldest after a's renewal"
        );
    }

    /// A model referenced by two cache keys survives eviction/removal from
    /// one of them; it is unloaded only when the last reference disappears.
    #[test]
    fn model_unloads_only_when_last_ref_removed() {
        let now = Local::now();
        let mut cache = CacheState::default();
        cache.touch_load("g/a", "k1", 10, -1, now);
        cache.touch_load("g/a", "k2", 10, -1, now);

        let outcome = cache.remove("k1", "g/a");
        assert!(outcome.was_present);
        assert_eq!(outcome.unload, None, "still referenced by k2");
        assert!(cache.refs_non_empty("g/a"));

        let outcome = cache.remove("k2", "g/a");
        assert_eq!(outcome.unload, Some("g/a".to_string()), "last ref gone");
        assert!(!cache.refs_non_empty("g/a"));

        // Removing a non-existent entry reports absence (unload_model's
        // `false if not cached` contract).
        assert!(!cache.remove("k2", "g/a").was_present);
    }

    /// clear() drops a whole cache key: models whose only reference lived
    /// there are unloaded, models still referenced elsewhere survive; the
    /// returned count is the number of entries removed.
    #[test]
    fn clear_cache_respects_other_refs() {
        let now = Local::now();
        let mut cache = CacheState::default();
        cache.touch_load("g/only", "k1", 10, -1, now);
        cache.touch_load("g/shared", "k1", 10, -1, now);
        cache.touch_load("g/shared", "k2", 10, -1, now);

        let (count, unloads) = cache.clear("k1");
        assert_eq!(count, 2);
        assert_eq!(unloads, vec!["g/only".to_string()]);
        assert!(cache.refs_non_empty("g/shared"));

        let (count, unloads) = cache.clear("nope");
        assert_eq!((count, unloads.len()), (0, 0), "unknown key clears nothing");
    }

    /// TTL expiry semantics: strictly-past finite expirations are removed,
    /// ttl -1 (never) survives any amount of time, and pinned models are
    /// skipped entirely even when their expiration is past (a model can't
    /// expire mid-predict/mid-load).
    #[test]
    fn expire_honors_never_and_pins() {
        let now = Local::now();
        let mut cache = CacheState::default();
        cache.touch_load("g/expired", "k", 10, 1, now);
        cache.touch_load("g/never", "k", 10, -1, now);
        cache.touch_load("g/pinned", "k", 10, 1, now);
        cache.pin("g/pinned");

        let unloads = cache.expire(at(now, 5));
        assert_eq!(unloads, vec!["g/expired".to_string()]);
        assert!(cache.refs_non_empty("g/never"), "ttl -1 never expires");
        assert!(
            cache.refs_non_empty("g/pinned"),
            "pinned skipped while expired"
        );

        // Unpinning with a fresh TTL restores the window: not expired right
        // after, expired once the restored TTL passes.
        cache.unpin_restore("g/pinned", "k", 10, at(now, 5));
        assert!(cache.expire(at(now, 6)).is_empty());
        let unloads = cache.expire(at(now, 16));
        assert_eq!(unloads, vec!["g/pinned".to_string()]);
    }

    /// The pin is a refcount (design §5): with two overlapping predicts the
    /// first unpin must not expose the model to expiry — only after the
    /// last unpin does the sweeper see it again.
    #[test]
    fn overlapping_pins_do_not_unpin_each_other() {
        let now = Local::now();
        let mut cache = CacheState::default();
        cache.touch_load("g/a", "k", 10, 1, now);
        cache.pin("g/a"); // predict 1 (via key k)
        cache.pin("g/a"); // predict 2 (via another path)

        cache.unpin_restore("g/a", "k", 1, now);
        assert!(
            cache.expire(at(now, 60)).is_empty(),
            "still pinned by the second predict"
        );

        cache.unpin_restore("g/a", "k", 1, at(now, 60));
        assert!(
            cache.expire(at(now, 61)).is_empty(),
            "restored ttl not yet past"
        );
        assert_eq!(cache.expire(at(now, 62)), vec!["g/a".to_string()]);
    }

    /// Expiration rendering matches Python datetime.isoformat(): six
    /// fractional digits when microseconds are non-zero, none when zero,
    /// and `None` for never (datetime.max on the wire).
    #[test]
    fn expiration_renders_like_python_isoformat() {
        use chrono::TimeZone;
        let base = Local.with_ymd_and_hms(2026, 7, 5, 12, 34, 56).unwrap();
        assert_eq!(isoformat(&base), "2026-07-05T12:34:56");
        let with_micros = base + chrono::Duration::microseconds(123456);
        assert_eq!(isoformat(&with_micros), "2026-07-05T12:34:56.123456");
        assert_eq!(Expiration::Never.render(), None);
    }

    /// Expiration::new must never panic on huge ttl_seconds (a raw i64
    /// query param under attacker control — a panic here poisons the state
    /// mutex and bricks the manager): values chrono cannot represent
    /// saturate to Never, while ordinary TTLs still yield finite
    /// expirations.
    #[test]
    fn huge_ttl_saturates_to_never_instead_of_panicking() {
        let now = Local::now();
        assert_eq!(Expiration::new(i64::MAX, now), Expiration::Never);
        assert_eq!(Expiration::new(9_000_000_000_000, now), Expiration::Never);
        assert!(matches!(Expiration::new(60, now), Expiration::At(_)));
        assert_eq!(Expiration::new(-1, now), Expiration::Never);
    }

    /// registry default_batch_size resolution follows the Python consumers'
    /// merge (group metadata overlaid by id metadata, id wins); missing or
    /// non-positive values yield None.
    #[test]
    fn registry_default_batch_merges_group_and_id_metadata() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("a.toml"),
            r#"
[group.g]
config.impl_class = "cls"
[group.g.metadata]
default_batch_size = 8
[group.g.inference_ids.plain]
[group.g.inference_ids.override]
metadata.default_batch_size = 4
[group.g.inference_ids.zero]
metadata.default_batch_size = 0

[group.bare]
config.impl_class = "cls"
[group.bare.inference_ids.x]
"#,
        )
        .unwrap();
        let registry = Registry::load(&RegistryConfig {
            config_dirs: vec![dir.path().to_path_buf()],
        })
        .unwrap();

        assert_eq!(registry_default_batch(&registry, "g/plain"), Some(8));
        assert_eq!(registry_default_batch(&registry, "g/override"), Some(4));
        assert_eq!(
            registry_default_batch(&registry, "g/zero"),
            None,
            "non-positive treated as absent"
        );
        assert_eq!(registry_default_batch(&registry, "bare/x"), None);
        assert_eq!(registry_default_batch(&registry, "missing/x"), None);
    }

    // ------------------------------------------------------------------
    // Integration tests with real worker subprocesses.
    // ------------------------------------------------------------------

    /// Repo root = CARGO_MANIFEST_DIR/.. (the panoptikon crate lives one level
    /// below the workspace root).
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

    /// Same spawn setup as the worker.rs tests: repo venv python, cwd =
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
                "inferio manager tests need the repo venv interpreter at {} — create the dev venv first",
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

    /// Synthetic registry covering every fixture impl, so the manager path
    /// exercises RegistryCache -> spawn_spec for real.
    const TEST_REGISTRY_TOML: &str = r#"
[external_inputs.manager_test_value]
label = "Manager test value"
required = true
[external_inputs.manager_test_value.source]
type = "environment"
variable = "INFERIO_MANAGER_EXTERNAL_INPUT_XYZ"

[group.echo]
config.impl_class = "echo_test"
[group.echo.inference_ids.test]
[group.echo.inference_ids.second]

[group.slow]
config.impl_class = "slow_test"
[group.slow.inference_ids.test]

# Slow *load* (R6): the phase the retired global load lock was held across.
# Two ids so a test can load two different models concurrently and watch the
# board-admission gate decide whether they overlap.
[group.slowload]
config.impl_class = "slow_load_test"
config.load_seconds = 3.0
[group.slowload.inference_ids.test]
[group.slowload.inference_ids.second]

[group.hang]
config.impl_class = "hang_test"
[group.hang.inference_ids.test]

[group.batch]
config.impl_class = "batchsize_test"
[group.batch.inference_ids.test]

[group.failbatch]
config.impl_class = "failbatch_test"
[group.failbatch.inference_ids.test]

[group.dying]
config.impl_class = "dying_test"
[group.dying.inference_ids.test]

# Dies while idle, a second after load: the liveness-sweep fixture (P5-6).
[group.idledeath]
config.impl_class = "idle_death_test"
config.die_after_seconds = 1.0
[group.idledeath.inference_ids.test]

[group.nan]
config.impl_class = "nan_test"
[group.nan.inference_ids.test]

[group.externalenv]
config.impl_class = "external_env_test"
[group.externalenv.inference_ids.test.external_inputs.manager_test_value]

[group.missing]
config.impl_class = "does_not_exist"
[group.missing.inference_ids.test]

# Multi-replica WorkerSets (design §8 / Phase 3). devices pins are just env
# strings the device_test fixture reads back — no GPU involved.
[group.device]
config.impl_class = "device_test"
config.devices = ["3", "7"]
metadata.cost.unit = "pixel"
metadata.cost.aggregation = "sum"
metadata.cost.epoch = 4
metadata.cost.seed_units = 1000000
[group.device.inference_ids.test]

# Same fixture with no devices pin: the universal-pinning path (resolved to
# the default board's UUID when a GPU inventory is known).
[group.devplain]
config.impl_class = "device_test"
[group.devplain.inference_ids.test]

[group.slowpair]
config.impl_class = "slow_test"
config.replicas = 2
[group.slowpair.inference_ids.test]

[group.dieflag]
config.impl_class = "dieflag_test"
config.replicas = 2
[group.dieflag.inference_ids.test]
"#;

    struct TestSetup {
        manager: Arc<ModelManager>,
        _registry_dir: tempfile::TempDir,
    }

    /// Default setup: unknown GPU inventory, i.e. exactly the pre-pinning
    /// behaviour (no `CUDA_VISIBLE_DEVICES` unless the registry pins one).
    /// [`test_manager_with_gpus`] covers resolved pinning.
    fn test_manager(sweep_interval: Duration, default_max_batch: u32) -> TestSetup {
        test_manager_with_deadlines(
            sweep_interval,
            default_max_batch,
            WorkerDeadlines::default(),
        )
    }

    fn test_manager_with_deadlines(
        sweep_interval: Duration,
        default_max_batch: u32,
        deadlines: WorkerDeadlines,
    ) -> TestSetup {
        test_manager_full(
            sweep_interval,
            default_max_batch,
            deadlines,
            GpuInventory::unknown(),
            None,
            LoadPolicy::default(),
        )
    }

    /// A manager whose load-path policy is not the shipped default (R6/R9).
    fn test_manager_with_loads(loads: LoadPolicy) -> TestSetup {
        test_manager_full(
            Duration::from_secs(60),
            32,
            WorkerDeadlines::default(),
            GpuInventory::unknown(),
            None,
            loads,
        )
    }

    fn test_manager_with_gpus(gpus: GpuInventory) -> TestSetup {
        test_manager_full(
            Duration::from_secs(60),
            32,
            WorkerDeadlines::default(),
            gpus,
            None,
            LoadPolicy::default(),
        )
    }

    fn test_manager_with_calibration(calibration: Arc<dyn CalibrationProfiles>) -> TestSetup {
        test_manager_full(
            Duration::from_secs(60),
            32,
            WorkerDeadlines::default(),
            GpuInventory::unknown(),
            Some(calibration),
            LoadPolicy::default(),
        )
    }

    fn test_manager_with_gpus_and_calibration(
        gpus: GpuInventory,
        calibration: Arc<dyn CalibrationProfiles>,
    ) -> TestSetup {
        test_manager_full(
            Duration::from_secs(60),
            32,
            WorkerDeadlines::default(),
            gpus,
            Some(calibration),
            LoadPolicy::default(),
        )
    }

    fn test_manager_full(
        sweep_interval: Duration,
        default_max_batch: u32,
        deadlines: WorkerDeadlines,
        gpus: GpuInventory,
        calibration: Option<Arc<dyn CalibrationProfiles>>,
        loads: LoadPolicy,
    ) -> TestSetup {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("registry.toml"), TEST_REGISTRY_TOML).unwrap();
        let registry = Arc::new(StdMutex::new(RegistryCache::new(RegistryConfig {
            config_dirs: vec![dir.path().to_path_buf()],
        })));
        let cfg = ManagerConfig {
            spawn: WorkerSpawnConfig {
                deadlines,
                ..test_spawn_config()
            },
            default_max_batch,
            sweep_interval,
            loads,
            // Pool disabled: these tests cover the manager's Python-parity
            // semantics; the prewarm pool has its own suite (prewarm.rs).
            prewarm: PrewarmConfig {
                enabled: false,
                lazy: false,
                always_warm: Vec::new(),
            },
            gpus,
            vram: VramBudgets::default(),
            // Usually no calibration store: these tests assert manager
            // semantics, and a store would make them write a profile file.
            calibration,
        };
        TestSetup {
            manager: ModelManager::new(cfg, registry),
            _registry_dir: dir,
        }
    }

    fn data_input(value: serde_json::Value) -> WorkerInput {
        WorkerInput {
            data: Some(value),
            file: None,
        }
    }

    /// Batch size reported by a batchsize_test output.
    fn reported_batch(output: &WorkerOutput) -> u64 {
        match output {
            WorkerOutput::Json(value) => value["batch"].as_u64().expect("batch field"),
            other => panic!("unexpected output {other:?}"),
        }
    }

    /// predict auto-loads the model (spawn + handshake + load) and returns
    /// outputs; a second predict reuses the same worker — the load
    /// generation is unchanged, proving no respawn happened.
    #[tokio::test]
    async fn predict_auto_loads_and_reuses_worker() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = &setup.manager;

        let outputs = manager
            .predict(
                "echo/test",
                "key",
                10,
                60,
                None,
                None,
                vec![data_input(json!({"text": "a"}))],
            )
            .await
            .expect("first predict auto-loads");
        assert_eq!(
            outputs,
            vec![WorkerOutput::Json(json!({"echo": {"text": "a"}}))]
        );
        let generation = manager.loaded_generation("echo/test").expect("loaded");
        assert_eq!(
            manager.cached_models(),
            BTreeMap::from([("echo/test".to_string(), vec!["key".to_string()])])
        );

        let outputs = manager
            .predict(
                "echo/test",
                "key",
                10,
                60,
                None,
                None,
                vec![data_input(json!(2))],
            )
            .await
            .expect("second predict");
        assert_eq!(outputs, vec![WorkerOutput::Json(json!({"echo": 2}))]);
        assert_eq!(
            manager.loaded_generation("echo/test"),
            Some(generation),
            "same worker: no respawn between predicts"
        );

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn declared_external_input_is_resolved_and_passed_at_worker_spawn() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = &setup.manager;
        assert!(
            manager
                .load_model("externalenv/test", "key", 10, 60, None)
                .await
                .is_err(),
            "missing required input prevents worker creation"
        );

        unsafe { std::env::set_var("INFERIO_MANAGER_EXTERNAL_INPUT_XYZ", "latest-value") };
        let output = manager
            .predict(
                "externalenv/test",
                "key",
                10,
                60,
                None,
                None,
                vec![data_input(json!(null))],
            )
            .await;
        unsafe { std::env::remove_var("INFERIO_MANAGER_EXTERNAL_INPUT_XYZ") };
        assert_eq!(
            output.expect("worker receives current input"),
            vec![WorkerOutput::Json(
                json!({"external_input": "latest-value"})
            )]
        );
        manager.shutdown().await;
    }

    /// Cache-key refcounting: a model loaded under two keys survives losing
    /// one (still serves predicts on the same worker); removing the last
    /// key unloads it (cache empty), and the next predict auto-loads a
    /// fresh worker (generation increases).
    #[tokio::test]
    async fn cache_key_refcount_governs_unload() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = &setup.manager;

        manager
            .load_model("echo/test", "a", 10, -1, None)
            .await
            .expect("load via a");
        manager
            .load_model("echo/test", "b", 10, -1, None)
            .await
            .expect("load via b");
        let generation = manager.loaded_generation("echo/test").expect("loaded");

        assert!(manager.unload_model("a", "echo/test").await.unwrap());
        assert_eq!(
            manager.cached_models(),
            BTreeMap::from([("echo/test".to_string(), vec!["b".to_string()])]),
            "still referenced by b"
        );
        let outputs = manager
            .predict(
                "echo/test",
                "b",
                10,
                -1,
                None,
                None,
                vec![data_input(json!("x"))],
            )
            .await
            .expect("still serves");
        assert_eq!(outputs, vec![WorkerOutput::Json(json!({"echo": "x"}))]);
        assert_eq!(manager.loaded_generation("echo/test"), Some(generation));

        assert!(manager.unload_model("b", "echo/test").await.unwrap());
        assert!(
            manager.cached_models().is_empty(),
            "last ref gone -> unloaded"
        );
        assert_eq!(manager.loaded_generation("echo/test"), None);
        // Removing again reports "not cached".
        assert!(!manager.unload_model("b", "echo/test").await.unwrap());

        let outputs = manager
            .predict(
                "echo/test",
                "b",
                10,
                -1,
                None,
                None,
                vec![data_input(json!(1))],
            )
            .await
            .expect("respawns after unload");
        assert_eq!(outputs, vec![WorkerOutput::Json(json!({"echo": 1}))]);
        assert!(
            manager
                .loaded_generation("echo/test")
                .expect("loaded again")
                > generation,
            "a fresh worker was spawned"
        );

        manager.shutdown().await;
    }

    /// lru_size = 1: loading a second model under the same cache key evicts
    /// the first (oldest), which unloads because no other key references
    /// it; only the second stays cached.
    #[tokio::test]
    async fn lru_size_one_evicts_previous_model() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = &setup.manager;

        manager
            .load_model("echo/test", "k", 1, -1, None)
            .await
            .expect("load first");
        manager
            .load_model("echo/second", "k", 1, -1, None)
            .await
            .expect("load second");

        assert_eq!(
            manager.cached_models(),
            BTreeMap::from([("echo/second".to_string(), vec!["k".to_string()])]),
            "first model evicted and unloaded"
        );
        assert_eq!(manager.loaded_generation("echo/test"), None);
        assert!(manager.loaded_generation("echo/second").is_some());

        manager.shutdown().await;
    }

    /// End-to-end TTL: with a short sweeper interval, a ttl=1s model is
    /// unloaded after expiry while a ttl=-1 model loaded alongside it
    /// survives.
    #[tokio::test]
    async fn ttl_expiry_unloads_but_never_survives() {
        let setup = test_manager(Duration::from_millis(200), 32);
        let manager = &setup.manager;

        manager
            .load_model("echo/second", "k", 10, -1, None)
            .await
            .expect("load never");
        manager
            .load_model("echo/test", "k", 10, 1, None)
            .await
            .expect("load short ttl");

        tokio::time::sleep(Duration::from_millis(2500)).await;

        let cached = manager.cached_models();
        assert!(
            !cached.contains_key("echo/test"),
            "ttl 1s model expired and unloaded: {cached:?}"
        );
        assert!(
            cached.contains_key("echo/second"),
            "ttl -1 model survives: {cached:?}"
        );
        assert_eq!(manager.loaded_generation("echo/test"), None);

        manager.shutdown().await;
    }

    /// Predict pins the model against TTL expiry: a 1.5s predict with
    /// ttl=1s and a 100ms sweeper completes successfully and the model is
    /// still cached right after (with a restored finite expiration), then
    /// expires normally once the restored TTL passes.
    #[tokio::test]
    async fn predict_pins_model_against_expiry() {
        let setup = test_manager(Duration::from_millis(100), 32);
        let manager = &setup.manager;

        let outputs = manager
            .predict(
                "slow/test",
                "k",
                10,
                1,
                None,
                None,
                vec![data_input(json!(null))],
            )
            .await
            .expect("predict outlives its ttl thanks to the pin");
        assert_eq!(outputs, vec![WorkerOutput::Json(json!({"slow": true}))]);

        // Immediately after completion the model is still cached and its
        // expiration was restored to a finite timestamp (not never).
        let expirations = manager.cache_expirations("k");
        assert!(
            expirations
                .get("slow/test")
                .is_some_and(|exp| exp.is_some()),
            "entry present with restored finite ttl: {expirations:?}"
        );

        // ... and expires normally afterwards.
        tokio::time::sleep(Duration::from_millis(2000)).await;
        assert!(
            manager.cached_models().is_empty(),
            "restored ttl expired after the predict"
        );

        manager.shutdown().await;
    }

    /// Dispatch-time batching: while the worker is busy with the first
    /// (solo) request, concurrently fired single-input predicts queue up
    /// and merge into one batch — the batchsize_test impl reports the batch
    /// size it saw, so the first response reports 1 and the rest report a
    /// merged batch > 1 (and never above the server default cap).
    #[tokio::test]
    async fn concurrent_predicts_merge_into_batches() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = setup.manager.clone();

        manager
            .load_model("batch/test", "k", 10, -1, None)
            .await
            .expect("load");

        let first = {
            let manager = manager.clone();
            tokio::spawn(async move {
                manager
                    .predict(
                        "batch/test",
                        "k",
                        10,
                        -1,
                        None,
                        None,
                        vec![data_input(json!(0))],
                    )
                    .await
            })
        };
        // Let the first request dispatch alone (worker sleeps 300ms).
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut rest = Vec::new();
        for i in 0..5 {
            let manager = manager.clone();
            rest.push(tokio::spawn(async move {
                manager
                    .predict(
                        "batch/test",
                        "k",
                        10,
                        -1,
                        None,
                        None,
                        vec![data_input(json!(i))],
                    )
                    .await
            }));
        }

        let first_batch = reported_batch(&first.await.unwrap().expect("first predict")[0]);
        assert_eq!(first_batch, 1, "idle model dispatches immediately, alone");

        let mut merged_batches = Vec::new();
        for task in rest {
            let outputs = task.await.unwrap().expect("queued predict");
            merged_batches.push(reported_batch(&outputs[0]));
        }
        assert!(
            merged_batches.iter().any(|&batch| batch > 1),
            "queued requests merged into a batch: {merged_batches:?}"
        );
        assert!(
            merged_batches.iter().all(|&batch| batch <= 32),
            "never above the server default cap: {merged_batches:?}"
        );

        manager.shutdown().await;
    }

    /// Explicit max_batch caps merging: many queued single-input requests
    /// all carrying max_batch=2 are dispatched in batches of at most 2 —
    /// no response may report a larger batch.
    #[tokio::test]
    async fn explicit_max_batch_caps_merged_batches() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = setup.manager.clone();

        manager
            .load_model("batch/test", "k", 10, -1, None)
            .await
            .expect("load");

        let mut tasks = Vec::new();
        for i in 0..6 {
            let manager = manager.clone();
            tasks.push(tokio::spawn(async move {
                manager
                    .predict(
                        "batch/test",
                        "k",
                        10,
                        -1,
                        Some(2),
                        None,
                        vec![data_input(json!(i))],
                    )
                    .await
            }));
        }
        for task in tasks {
            let outputs = task.await.unwrap().expect("capped predict");
            let batch = reported_batch(&outputs[0]);
            assert!(batch <= 2, "batch {batch} exceeds the explicit cap of 2");
        }

        manager.shutdown().await;
    }

    /// Port of the batch-failure fallback (process_model.py
    /// `_batch_predict`): the failbatch_test impl rejects any merged batch
    /// (>1 input) but serves singles, so queued requests that got merged
    /// still all succeed — the dispatcher falls back to per-request
    /// prediction instead of failing the whole window.
    #[tokio::test]
    async fn merged_batch_failure_falls_back_to_per_request() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = setup.manager.clone();

        manager
            .load_model("failbatch/test", "k", 10, -1, None)
            .await
            .expect("load");

        let first = {
            let manager = manager.clone();
            tokio::spawn(async move {
                manager
                    .predict(
                        "failbatch/test",
                        "k",
                        10,
                        -1,
                        None,
                        None,
                        vec![data_input(json!(0))],
                    )
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut rest = Vec::new();
        for i in 0..3 {
            let manager = manager.clone();
            rest.push(tokio::spawn(async move {
                manager
                    .predict(
                        "failbatch/test",
                        "k",
                        10,
                        -1,
                        None,
                        None,
                        vec![data_input(json!(i))],
                    )
                    .await
            }));
        }

        let outputs = first.await.unwrap().expect("solo predict succeeds");
        assert_eq!(outputs, vec![WorkerOutput::Json(json!({"ok": true}))]);
        for task in rest {
            let outputs = task.await.unwrap().expect("fallback saves merged requests");
            assert_eq!(outputs, vec![WorkerOutput::Json(json!({"ok": true}))]);
        }

        manager.shutdown().await;
    }

    /// P5-6: a replica that dies while **idle** is found by the sweeper.
    ///
    /// Nothing reads an idle worker's pipe, so the EOF a death produces is
    /// never noticed by a request — a grantless model can be advertised in
    /// `/health` for as long as nobody predicts against it. The fixture dies
    /// a second after load with no request in flight; only the sweeper's
    /// `ReapIdle` tick can discover it, and when it does the model takes the
    /// normal death route: dropped from `/health`, dropped from every cache.
    #[tokio::test]
    async fn an_idle_replica_that_dies_is_found_by_the_liveness_sweep() {
        let setup = test_manager(Duration::from_millis(100), 32);
        let manager = &setup.manager;

        // One predict to make it resident (and to prove it was healthy).
        // ttl -1: it must stay loaded, so a disappearance is the sweep's
        // doing and never a TTL expiry.
        let outputs = manager
            .predict(
                "idledeath/test",
                "k",
                10,
                -1,
                None,
                None,
                vec![data_input(json!(1))],
            )
            .await
            .expect("the fixture serves normally before it dies");
        assert_eq!(outputs.len(), 1);
        let health = manager.health();
        assert_eq!(health.model_count, 1, "the model is resident");
        assert_eq!(
            health.models[0].replicas.total, 1,
            "its replica is advertised"
        );

        // Now the worker exits under the manager, idle. Poll rather than
        // sleep a fixed span: without the reap tick this never happens and
        // the test fails on the bound instead of flaking on timing.
        let mut found = false;
        for _ in 0..200 {
            if manager.health().model_count == 0 {
                found = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            found,
            "the sweep never noticed the dead idle replica; /health still says {:?}",
            manager.health().models
        );
        assert!(
            manager.cached_models().is_empty(),
            "the dead model is dropped from every cache, not just from /health"
        );
        assert_eq!(manager.loaded_generation("idledeath/test"), None);

        manager.shutdown().await;
    }

    /// Fatal worker death: a predict against a worker that dies mid-request
    /// fails with the supervision error, the model is dropped from all
    /// caches (no phantom /cache entries), and the next predict auto-loads
    /// a fresh worker instead of hitting a poisoned one.
    #[tokio::test]
    async fn worker_death_cleans_up_and_next_predict_respawns() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = &setup.manager;

        let err = manager
            .predict(
                "dying/test",
                "k",
                10,
                -1,
                None,
                None,
                vec![data_input(json!(1))],
            )
            .await
            .expect_err("worker exits mid-predict");
        assert!(
            format!("{err:#}").contains("predict"),
            "error surfaces the failed predict: {err:#}"
        );

        // Death cleanup runs in the dispatcher task right after the reply;
        // give it a beat.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            manager.cached_models().is_empty(),
            "dead model dropped from all caches"
        );
        assert_eq!(manager.loaded_generation("dying/test"), None);

        // The next predict spawns a fresh worker (which also dies — but the
        // fatal error proves a new process served it rather than a closed
        // queue or a poisoned handle).
        let err = manager
            .predict(
                "dying/test",
                "k",
                10,
                -1,
                None,
                None,
                vec![data_input(json!(2))],
            )
            .await
            .expect_err("fresh worker also dies");
        assert!(
            format!("{err:#}").contains("predict request failed"),
            "a fresh worker was spawned and failed the same way: {err:#}"
        );

        manager.shutdown().await;
    }

    /// Load failure (unknown impl class): the error propagates with the
    /// worker's own message, and no LRU entry or cache reference is left
    /// behind — Python leaves a phantom id in /cache here; we don't.
    #[tokio::test]
    async fn failed_load_leaves_no_cache_entry() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = &setup.manager;

        let err = manager
            .load_model("missing/test", "k", 10, -1, None)
            .await
            .expect_err("impl class does not exist");
        assert!(
            format!("{err:#}").contains("does_not_exist"),
            "worker's handshake error is preserved: {err:#}"
        );
        assert!(manager.cached_models().is_empty(), "no phantom cache entry");
        assert!(
            manager.cache_expirations("k").is_empty(),
            "no LRU entry left behind"
        );

        manager.shutdown().await;
    }

    /// Shutdown flushes the calibration store. A window that landed inside
    /// the write debounce is exactly the desktop case — quitting a few
    /// seconds after a ramp step — and losing it would silently mean
    /// re-ramping on the next run.
    #[tokio::test]
    async fn shutdown_flushes_the_calibration_store() {
        use super::super::calibration::{CalibrationStore, ProfileUpdate, StoreEnv, StorePaths};
        use super::super::ledger::FitSample;

        let data = tempfile::tempdir().unwrap();
        let path = data.path().join("inferio/calibration.toml");
        let store = CalibrationStore::with_debounce(
            StorePaths {
                shipped_dirs: Vec::new(),
                local_path: path.clone(),
            },
            StoreEnv {
                platform: "linux".to_owned(),
                backend: "cuda".to_owned(),
                generator: "panoptikon test".to_owned(),
            },
            Duration::from_secs(3600),
        );
        let setup =
            test_manager_with_calibration(Arc::clone(&store) as Arc<dyn CalibrationProfiles>);
        let update = |slope: f64| ProfileUpdate {
            inference_id: "echo/test".to_owned(),
            epoch: 1,
            gpu_name: "TEST 9000".to_owned(),
            torch: "2.7.1+cu128".to_owned(),
            dtype: "fp16".to_owned(),
            unit: "item",
            aggregation: "count",
            base_mb: 1000,
            base_method: Some("nvml".to_owned()),
            dtype_method: Some("selected".to_owned()),
            slope_mb_per_unit: slope,
            residual_mb: 0.0,
            samples: 3,
            knee_units: None,
            knee_withdrawn: false,
            max_units_measured: 16,
            local_samples: 3,
            knee_clean_windows: 0,
            ring: vec![FitSample {
                units: 16,
                delta_mb: 160,
            }],
        };

        // The first update has no previous write to wait behind, so it lands
        // on its own and primes the debounce.
        store.record(update(0.25));
        let mut waited = Duration::ZERO;
        while !path.exists() && waited < Duration::from_secs(5) {
            tokio::time::sleep(Duration::from_millis(20)).await;
            waited += Duration::from_millis(20);
        }
        assert!(path.exists(), "the first update reaches disk");

        // The second is an hour behind the debounce — only a flush saves it.
        store.record(update(0.75));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !fs::read_to_string(&path).unwrap().contains("0.75"),
            "still waiting out the debounce"
        );
        setup.manager.shutdown().await;
        assert!(
            fs::read_to_string(&path).unwrap().contains("0.75"),
            "the quit flushed what the debounce was holding"
        );
    }

    /// Graceful manager shutdown: workers are unloaded via the graceful
    /// ladder, the cache empties, and subsequent loads/predicts are
    /// refused.
    #[tokio::test]
    async fn shutdown_unloads_workers_and_refuses_new_requests() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = &setup.manager;

        manager
            .load_model("echo/test", "k", 10, -1, None)
            .await
            .expect("load");
        manager.shutdown().await;

        assert!(manager.cached_models().is_empty());
        let err = manager
            .load_model("echo/test", "k", 10, -1, None)
            .await
            .expect_err("loads refused after shutdown");
        assert!(format!("{err:#}").contains("shutting down"));
        let err = manager
            .predict(
                "echo/test",
                "k",
                10,
                -1,
                None,
                None,
                vec![data_input(json!(1))],
            )
            .await
            .expect_err("predicts refused after shutdown");
        assert!(format!("{err:#}").contains("shutting down"));
    }

    /// An output the orchestrator cannot convert to JSON (the nan_test
    /// fixture returns float NaN on demand) is a per-request error, not a
    /// fatal supervision error: the requesting caller gets the error, the
    /// worker survives (load generation unchanged — no respawn), and a
    /// follow-up normal predict on the very same worker succeeds.
    #[tokio::test]
    async fn unconvertible_output_fails_one_request_but_worker_survives() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = &setup.manager;

        let outputs = manager
            .predict(
                "nan/test",
                "k",
                10,
                -1,
                None,
                None,
                vec![data_input(json!("ok"))],
            )
            .await
            .expect("normal predict");
        assert_eq!(outputs, vec![WorkerOutput::Json(json!({"ok": true}))]);
        let generation = manager.loaded_generation("nan/test").expect("loaded");

        let err = manager
            .predict(
                "nan/test",
                "k",
                10,
                -1,
                None,
                None,
                vec![data_input(json!("nan"))],
            )
            .await
            .expect_err("NaN output has no JSON form");
        assert!(
            format!("{err:#}").contains("not representable as JSON"),
            "error names the unconvertible output: {err:#}"
        );

        // If this were (wrongly) classified fatal, death cleanup would drop
        // the model shortly after; give that a beat to prove it doesn't.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            manager.loaded_generation("nan/test"),
            Some(generation),
            "same worker: the conversion failure must not kill it"
        );

        let outputs = manager
            .predict(
                "nan/test",
                "k",
                10,
                -1,
                None,
                None,
                vec![data_input(json!("ok"))],
            )
            .await
            .expect("worker still serves after the failed request");
        assert_eq!(outputs, vec![WorkerOutput::Json(json!({"ok": true}))]);

        manager.shutdown().await;
    }

    /// A predict future dropped mid-flight (client disconnect) must release
    /// its TTL pin via the RAII guard: after aborting a slow predict, the
    /// model still expires and unloads once the restored TTL passes. With a
    /// leaked pin the model would be exempt from expiry forever and this
    /// poll would time out.
    #[tokio::test]
    async fn aborted_predict_releases_pin_and_model_still_expires() {
        let setup = test_manager(Duration::from_millis(100), 32);
        let manager = setup.manager.clone();

        // Load first so the abort lands mid-predict, not mid-spawn.
        manager
            .load_model("slow/test", "k", 10, 1, None)
            .await
            .expect("load");

        let task = {
            let manager = manager.clone();
            tokio::spawn(async move {
                manager
                    .predict(
                        "slow/test",
                        "k",
                        10,
                        1,
                        None,
                        None,
                        vec![data_input(json!(null))],
                    )
                    .await
            })
        };
        // Let the predict enqueue and pin (the fixture predict takes 1.5s),
        // then drop it mid-flight.
        tokio::time::sleep(Duration::from_millis(400)).await;
        task.abort();
        let _ = task.await;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            if manager.cached_models().is_empty() {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!(
                    "model never expired after the predict was aborted (leaked pin): {:?}",
                    manager.cached_models()
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        manager.shutdown().await;
    }

    /// A worker wedged in predict (the stuck-CUDA case) must never need a
    /// manual process kill: predict has no deadline — how long a model
    /// legitimately takes is unknowable — so the bound sits on the unload
    /// drain instead. Shutdown gives the in-flight window `unload_grace`,
    /// then kills the stuck worker, so Ctrl-C always converges. The hang
    /// fixture sleeps 600s; without the bounded drain this test would hang
    /// until the fixture returns.
    #[tokio::test]
    async fn shutdown_kills_worker_wedged_in_predict() {
        let deadlines = WorkerDeadlines {
            unload_grace: Duration::from_secs(1),
            ..WorkerDeadlines::default()
        };
        let setup = test_manager_with_deadlines(Duration::from_secs(60), 32, deadlines);
        let manager = setup.manager.clone();

        // Load first so shutdown lands mid-predict, not mid-spawn.
        manager
            .load_model("hang/test", "k", 10, 60, None)
            .await
            .expect("load");

        let task = {
            let manager = manager.clone();
            tokio::spawn(async move {
                manager
                    .predict(
                        "hang/test",
                        "k",
                        10,
                        60,
                        None,
                        None,
                        vec![data_input(json!(null))],
                    )
                    .await
            })
        };
        // Let the window dispatch to the worker before shutting down.
        tokio::time::sleep(Duration::from_millis(400)).await;

        let started = std::time::Instant::now();
        manager.shutdown().await;
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(10),
            "shutdown must converge on a wedged predict via the bounded drain, took {elapsed:?}"
        );
        let result = task.await.expect("predict task must not panic");
        assert!(
            result.is_err(),
            "the wedged predict must observe an error, got {result:?}"
        );
    }

    /// Device string reported by a device_test output.
    fn reported_device(output: &WorkerOutput) -> String {
        match output {
            WorkerOutput::Json(value) => value["device"]
                .as_str()
                .expect("device field is a string")
                .to_owned(),
            other => panic!("unexpected output {other:?}"),
        }
    }

    /// Multi-replica device pinning end to end (design §8): a model with
    /// `devices = ["3", "7"]` spawns two replicas, each seeing its own
    /// CUDA_VISIBLE_DEVICES, and both serve the one shared FIFO queue —
    /// enough concurrent single predicts (max_batch 1 so windows never
    /// merge) must be answered by BOTH pins, proving the set has exactly
    /// the two replicas and the dispatcher actually spreads windows across
    /// them.
    ///
    /// This is also the **unknown-inventory passthrough** case for
    /// universal pinning: with no GPU inventory and CUDA's pin vocabulary
    /// (which is what a default `GpuInventory` carries, as here) the
    /// registry's raw index strings reach the child unchanged, exactly as
    /// before pinning existed. The claim is scoped to that vocabulary: an
    /// uninventoried *ROCm* host canonicalises index pins and drops
    /// non-numeric ones instead (`gpu.rs::resolve_hip_pin_uninventoried`).
    #[tokio::test]
    async fn multi_replica_devices_serve_shared_queue() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = setup.manager.clone();

        manager
            .load_model("device/test", "k", 10, -1, None)
            .await
            .expect("load spawns both replicas");

        // 4 concurrent singles against 0.5s predicts: the first two windows
        // occupy both replicas, the rest queue and go to whichever frees.
        let mut tasks = Vec::new();
        for i in 0..4 {
            let manager = manager.clone();
            tasks.push(tokio::spawn(async move {
                manager
                    .predict(
                        "device/test",
                        "k",
                        10,
                        -1,
                        Some(1),
                        None,
                        vec![data_input(json!(i))],
                    )
                    .await
            }));
        }
        let mut devices = std::collections::BTreeSet::new();
        for task in tasks {
            let outputs = task.await.unwrap().expect("predict on a pinned replica");
            devices.insert(reported_device(&outputs[0]));
        }
        assert_eq!(
            devices,
            std::collections::BTreeSet::from(["3".to_string(), "7".to_string()]),
            "both configured device pins served requests (and no third replica exists)"
        );

        manager.shutdown().await;
    }

    /// Inventory for the pinning tests: two boards whose indices (0 and 3)
    /// cover both the default-placement and the index-mapping paths. Equal
    /// compute capability, so the default pin is decided by index (board 0).
    fn test_gpus() -> GpuInventory {
        GpuInventory::known(vec![
            GpuInfo {
                index: 0,
                uuid: "GPU-0000".into(),
                name: "Test GPU 0".into(),
                total_mb: 8192,
                compute_cap: Some("12.0".into()),
                bdf: None,
                gfx_target_version: None,
                unified_ram_mb: None,
                vram_carveout_mb: None,
            },
            GpuInfo {
                index: 3,
                uuid: "GPU-3333".into(),
                name: "Test GPU 3".into(),
                total_mb: 8192,
                compute_cap: Some("12.0".into()),
                bdf: None,
                gfx_target_version: None,
                unified_ram_mb: None,
                vram_carveout_mb: None,
            },
        ])
    }

    /// Universal pinning (batch-calibration design): a replica the registry
    /// left unpinned is spawned on the default board *by UUID*, not left
    /// seeing every device. The fixture echoes its CUDA_VISIBLE_DEVICES, so
    /// the reported value is the proof.
    #[tokio::test]
    async fn unpinned_replica_is_pinned_to_the_default_gpu() {
        let setup = test_manager_with_gpus(test_gpus());
        let manager = &setup.manager;

        let outputs = manager
            .predict(
                "devplain/test",
                "k",
                10,
                -1,
                None,
                None,
                vec![data_input(json!(1))],
            )
            .await
            .expect("predict on the pinned replica");
        assert_eq!(
            reported_device(&outputs[0]),
            "GPU-0000",
            "an unpinned replica resolves to the default board's UUID"
        );

        manager.shutdown().await;
    }

    /// An explicit index pin (`devices = ["3", "7"]`) is mapped to that
    /// board's UUID so the ledger key is stable across reboots; an index no
    /// board reports (7 here) passes through unchanged rather than being
    /// guessed at.
    #[tokio::test]
    async fn index_device_pins_map_to_board_uuids() {
        let setup = test_manager_with_gpus(test_gpus());
        let manager = setup.manager.clone();

        manager
            .load_model("device/test", "k", 10, -1, None)
            .await
            .expect("load spawns both replicas");

        let mut tasks = Vec::new();
        for i in 0..4 {
            let manager = manager.clone();
            tasks.push(tokio::spawn(async move {
                manager
                    .predict(
                        "device/test",
                        "k",
                        10,
                        -1,
                        Some(1),
                        None,
                        vec![data_input(json!(i))],
                    )
                    .await
            }));
        }
        let mut devices = std::collections::BTreeSet::new();
        for task in tasks {
            let outputs = task.await.unwrap().expect("predict on a pinned replica");
            devices.insert(reported_device(&outputs[0]));
        }
        assert_eq!(
            devices,
            std::collections::BTreeSet::from(["GPU-3333".to_string(), "7".to_string()]),
            "index 3 became its UUID; the invisible index 7 passed through"
        );

        manager.shutdown().await;
    }

    /// A calibration store that answers nothing and records which *board*
    /// each question was keyed by. `expected_base_mb` has exactly one caller
    /// — the load reservation in `spawn_model` — and it is reached only after
    /// `VramLedger::reserve_load` has found the board in its map, so a
    /// recorded name is proof the reservation resolved to a real board.
    #[derive(Default)]
    struct RecordingProfiles {
        reservation_boards: StdMutex<Vec<String>>,
    }

    impl CalibrationProfiles for RecordingProfiles {
        fn expected_base_mb(&self, query: &ProfileQuery<'_>) -> Option<u64> {
            self.reservation_boards
                .lock()
                .unwrap()
                .push(query.gpu_name.to_owned());
            None
        }

        fn lookup(&self, _query: &ProfileQuery<'_>) -> Option<ProfileSeed> {
            None
        }

        fn record(&self, _update: ProfileUpdate) {}
    }

    /// R7's precedence rule, in the one place it is decided: the registry's
    /// declaration beats the canvas a worker read off the loaded impl, the
    /// report fills in for a model the registry cannot state statically
    /// (`doctr/dots_ocr`, whose ceiling lives in a downloaded processor
    /// config), and neither means uncapped — what every model did before
    /// run2.
    #[test]
    fn the_registry_canvas_beats_the_one_a_worker_reported() {
        let pixels = |canvas_pixels| CostDimension {
            unit: CostUnit::Pixel,
            aggregation: Some(super::super::cost::CostAggregation::Sum),
            epoch: 1,
            seed_units: Some(2_000_000),
            degraded: false,
            canvas_pixels,
        };
        assert_eq!(
            canvas_in_force("clip/nemotron", pixels(Some(1_835_008)), Some(11_289_600))
                .canvas_pixels,
            Some(1_835_008),
            "a declaration is reviewed; an attribute read off an object graph \
             is not, and a reading that overrode config could not be fixed \
             from config"
        );
        assert_eq!(
            canvas_in_force("doctr/dots_ocr", pixels(None), Some(11_289_600)).canvas_pixels,
            Some(11_289_600),
            "the tier that covers a canvas only a loaded process can know"
        );
        assert_eq!(
            canvas_in_force("clip/vit", pixels(None), None).canvas_pixels,
            None
        );
        // An area prices nothing outside pixel pricing, and a worker reports
        // what it found without knowing its own unit — so the gate is here.
        let tokens = CostDimension {
            unit: CostUnit::Token,
            ..pixels(Some(1_835_008))
        };
        assert_eq!(
            canvas_in_force("clip/tokens", tokens, Some(11_289_600)).canvas_pixels,
            None
        );
    }

    /// A ROCm-shaped inventory whose row indices are the registry's own
    /// device pins (`device/test` pins "3" and "7"), so the pin vocabulary
    /// and the ledger's key vocabulary are guaranteed to differ.
    fn rocm_test_gpus() -> GpuInventory {
        let board = |index: u32, bdf: &str| GpuInfo {
            index,
            uuid: format!("GPU-BDF-{bdf}"),
            name: format!("AMD gfx1100 #{index}"),
            total_mb: 24_576,
            compute_cap: None,
            bdf: Some(bdf.to_owned()),
            gfx_target_version: Some(110_000),
            unified_ram_mb: None,
            vram_carveout_mb: None,
        };
        GpuInventory::known_rocm(vec![board(3, "0000:03:00.0"), board(7, "0000:0c:00.0")])
    }

    /// D3's manager half: the load reservation is keyed by the board key,
    /// never by the resolved pin. On ROCm the two are never the same string —
    /// the pin is a HIP device index — so keying by the pin misses the
    /// ledger's board map entirely and reserves nothing, which is precisely
    /// the gap D2 left open. The recording store is the probe: a reservation
    /// that found its board consults it, one that missed never gets that far.
    ///
    /// Venv-gated like the rest of this suite (it spawns real fixture
    /// workers via the repo interpreter), so it runs on the dev box and in
    /// CI rather than on every checkout.
    #[tokio::test]
    async fn load_reservations_are_keyed_by_board_not_by_the_hip_pin() {
        let profiles = Arc::new(RecordingProfiles::default());
        let setup = test_manager_with_gpus_and_calibration(
            rocm_test_gpus(),
            Arc::clone(&profiles) as Arc<dyn CalibrationProfiles>,
        );
        let manager = &setup.manager;

        manager
            .load_model("device/test", "k", 10, -1, None)
            .await
            .expect("load spawns both pinned replicas");

        let mut boards = profiles.reservation_boards.lock().unwrap().clone();
        boards.sort();
        assert_eq!(
            boards,
            vec!["AMD gfx1100 #3".to_string(), "AMD gfx1100 #7".to_string()],
            "both replicas reserved against the board their index pin names; \
             keying by the pin string (\"3\", \"7\") would have found no board \
             at all and reserved nothing"
        );

        manager.shutdown().await;
    }

    /// `/health` carries the batch-calibration bookkeeping: the resolved
    /// cost dimension per model (declared for the `device` group, degraded
    /// for `echo`, which declares nothing), the GPU inventory, and one
    /// telemetry row per replica carrying its resolved pin. The memory
    /// fields stay null here — the fixtures have no torch, which is exactly
    /// the old-worker tolerance the wire contract promises.
    #[tokio::test]
    async fn health_reports_cost_dimension_gpus_and_replica_pins() {
        let setup = test_manager_with_gpus(test_gpus());
        let manager = &setup.manager;

        manager
            .load_model("device/test", "k", 10, -1, None)
            .await
            .expect("load");
        manager
            .load_model("echo/test", "k", 10, -1, None)
            .await
            .expect("load");

        let health = manager.health();
        assert_eq!(
            health
                .gpus
                .iter()
                .map(|gpu| gpu.uuid.as_str())
                .collect::<Vec<_>>(),
            vec!["GPU-0000", "GPU-3333"],
            "the inventory is reported by board UUID"
        );

        let device = health
            .models
            .iter()
            .find(|model| model.inference_id == "device/test")
            .expect("device model in health");
        assert_eq!(device.cost.unit, "pixel");
        assert_eq!(device.cost.aggregation.as_deref(), Some("sum"));
        assert_eq!(device.cost.epoch, 4);
        assert_eq!(device.cost.seed_units, Some(1_000_000));
        assert!(!device.cost.degraded);
        let mut pins: Vec<Option<&str>> = device
            .replicas_detail
            .iter()
            .map(|replica| replica.gpu.as_deref())
            .collect();
        pins.sort();
        assert_eq!(
            pins,
            vec![Some("7"), Some("GPU-3333")],
            "every replica records the GPU it sits on"
        );
        assert!(
            device
                .replicas_detail
                .iter()
                .all(|replica| replica.base_mb.is_none()
                    && replica.dtype.is_none()
                    && replica.reserved_mb.is_none()
                    // The reported identity is absent too: only a worker with
                    // a live CUDA device can name its board, and the spawn pin
                    // above is deliberately not copied into it.
                    && replica.gpu_uuid.is_none()
                    && replica.torch_version.is_none()),
            "a torch-less worker reports no memory, and that is not an error: {:?}",
            device.replicas_detail
        );

        let echo = health
            .models
            .iter()
            .find(|model| model.inference_id == "echo/test")
            .expect("echo model in health");
        assert_eq!(echo.cost.unit, "item", "undeclared degrades to item/count");
        assert_eq!(echo.cost.aggregation.as_deref(), Some("count"));
        assert!(echo.cost.degraded);

        manager.shutdown().await;
    }

    /// A predict must record the measurement plumbing even with no torch in
    /// the worker: the harness always times the call and counts its inputs,
    /// so every batch lands in the telemetry ring (sequence-numbered, oldest
    /// first) while the memory columns stay null. Two predicts must produce
    /// two retained samples — the cost fit needs the set, not the last one.
    #[tokio::test]
    async fn predict_records_batch_measurements_without_torch() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = &setup.manager;

        manager
            .predict(
                "echo/test",
                "k",
                10,
                -1,
                None,
                None,
                vec![data_input(json!(1)), data_input(json!(2))],
            )
            .await
            .expect("predict");
        manager
            .predict(
                "echo/test",
                "k",
                10,
                -1,
                None,
                None,
                vec![data_input(json!(3))],
            )
            .await
            .expect("second predict");

        let health = manager.health();
        let replica = &health
            .models
            .iter()
            .find(|model| model.inference_id == "echo/test")
            .expect("echo model in health")
            .replicas_detail[0];
        assert_eq!(
            replica.measurements_recorded, 2,
            "both predicts were retained, not overwritten: {replica:?}"
        );
        assert_eq!(
            replica
                .recent_batches
                .iter()
                .map(|batch| (batch.seq, batch.items))
                .collect::<Vec<_>>(),
            vec![(1, Some(2)), (2, Some(1))],
            "oldest first, with the input counts the worker reported: {replica:?}"
        );
        assert!(
            replica
                .recent_batches
                .iter()
                .all(|batch| batch.duration_ms.is_some_and(|ms| ms >= 0.0)),
            "every batch was timed: {replica:?}"
        );
        assert!(
            replica
                .recent_batches
                .iter()
                .all(|batch| batch.peak_reserved_mb.is_none())
                && replica.free_mb.is_none()
                && replica.gpu.is_none(),
            "no torch and no GPU inventory means no memory numbers: {replica:?}"
        );

        manager.shutdown().await;
    }

    /// Throughput proof that replicas run windows concurrently: slow_test
    /// predicts take 1.5s each; with 2 replicas and 4 single-input predicts
    /// capped at max_batch 1 (so nothing merges), the work is 2 rounds of 2
    /// parallel predicts — ~3s wall, vs ~6s if the set were serialized like
    /// a single replica. Asserted generously (< 5s) to avoid flake; the
    /// single-replica behavior would need >= 6s and cannot pass.
    #[tokio::test]
    async fn multi_replica_predicts_run_concurrently() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = setup.manager.clone();

        manager
            .load_model("slowpair/test", "k", 10, -1, None)
            .await
            .expect("load");

        let started = std::time::Instant::now();
        let mut tasks = Vec::new();
        for i in 0..4 {
            let manager = manager.clone();
            tasks.push(tokio::spawn(async move {
                manager
                    .predict(
                        "slowpair/test",
                        "k",
                        10,
                        -1,
                        Some(1),
                        None,
                        vec![data_input(json!(i))],
                    )
                    .await
            }));
        }
        for task in tasks {
            let outputs = task.await.unwrap().expect("predict");
            assert_eq!(outputs, vec![WorkerOutput::Json(json!({"slow": true}))]);
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(5),
            "4 x 1.5s singles across 2 replicas must take ~2 rounds, got {elapsed:?}"
        );

        manager.shutdown().await;
    }

    /// The Phase 3 death policy: ANY replica dying fatally kills the whole
    /// model. One poison request hard-kills its replica (os._exit) while a
    /// normal request is in flight on the other replica and more are
    /// queued; every outstanding request errors (queued ones are failed,
    /// the in-flight window on the healthy replica is aborted), the model
    /// vanishes from all caches, and the next predict auto-loads a fresh
    /// 2-replica set (new generation) that serves normally.
    #[tokio::test]
    async fn replica_death_kills_whole_set_and_next_predict_respawns() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = setup.manager.clone();

        manager
            .load_model("dieflag/test", "k", 10, -1, None)
            .await
            .expect("load spawns both replicas");
        let generation = manager.loaded_generation("dieflag/test").expect("loaded");

        // The poison request dispatches first (FIFO) and holds replica A for
        // 200ms before dying; the normal requests sent right after land on
        // replica B (1s predict) and the queue — all still outstanding when
        // the death is detected.
        let die = {
            let manager = manager.clone();
            tokio::spawn(async move {
                manager
                    .predict(
                        "dieflag/test",
                        "k",
                        10,
                        -1,
                        Some(1),
                        None,
                        vec![data_input(json!({"die": true}))],
                    )
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut normals = Vec::new();
        for i in 0..3 {
            let manager = manager.clone();
            normals.push(tokio::spawn(async move {
                manager
                    .predict(
                        "dieflag/test",
                        "k",
                        10,
                        -1,
                        Some(1),
                        None,
                        vec![data_input(json!(i))],
                    )
                    .await
            }));
        }

        die.await
            .unwrap()
            .expect_err("the poison request fails with the fatal death");
        for task in normals {
            task.await.unwrap().expect_err(
                "whole-set death policy: outstanding requests on other replicas error too",
            );
        }

        // Death cleanup runs in the dispatcher task; poll briefly.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !manager.cached_models().is_empty() {
            if tokio::time::Instant::now() > deadline {
                panic!(
                    "dead model never dropped from caches: {:?}",
                    manager.cached_models()
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(manager.loaded_generation("dieflag/test"), None);

        // A fresh predict auto-loads a brand new 2-replica set and works.
        let outputs = manager
            .predict(
                "dieflag/test",
                "k",
                10,
                -1,
                Some(1),
                None,
                vec![data_input(json!("ok"))],
            )
            .await
            .expect("fresh worker set serves after the death");
        assert_eq!(outputs, vec![WorkerOutput::Json(json!({"echo": "ok"}))]);
        assert!(
            manager.loaded_generation("dieflag/test").expect("loaded") > generation,
            "the respawned set has a new generation"
        );

        manager.shutdown().await;
    }

    /// Whole-set unload: unloading a multi-replica model removes it from
    /// the cache as one unit and gracefully stops BOTH replicas (the
    /// graceful ladders run concurrently in the dispatcher's shutdown
    /// path; the drained task is awaited by manager shutdown, so a leaked
    /// replica would hang this test). A re-load spawns a fresh set —
    /// generation bump proves nothing from the old set was reused.
    #[tokio::test]
    async fn unload_tears_down_whole_replica_set() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = &setup.manager;

        manager
            .load_model("device/test", "k", 10, -1, None)
            .await
            .expect("load spawns both replicas");
        let generation = manager.loaded_generation("device/test").expect("loaded");

        assert!(manager.unload_model("k", "device/test").await.unwrap());
        assert!(
            manager.cached_models().is_empty(),
            "the set unloads as one unit"
        );
        assert_eq!(manager.loaded_generation("device/test"), None);

        manager
            .load_model("device/test", "k", 10, -1, None)
            .await
            .expect("re-load spawns a fresh set");
        assert!(
            manager.loaded_generation("device/test").expect("loaded") > generation,
            "fresh generation: no worker from the unloaded set survived"
        );

        // shutdown() awaits the draining dispatcher task of the unloaded
        // set as well — completing without a hang is the no-leak assertion.
        manager.shutdown().await;
    }

    /// Unit test of the guard itself, covering the spawn-phase pin (which
    /// flows through the same PinGuard type as the predict pin): while the
    /// guard is alive the pinned entry cannot expire; dropping the guard
    /// releases the pin and the stale entry expires normally.
    #[tokio::test]
    async fn pin_guard_drop_releases_pin() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = &setup.manager;
        let now = Local::now();
        {
            let mut state = manager.state.lock().unwrap();
            state.cache.touch_load("g/x", "k", 10, 0, now);
            state.cache.pin("g/x");
        }
        let guard = PinGuard::adopt(manager, "g/x", None);
        {
            let mut state = manager.state.lock().unwrap();
            assert!(
                state.cache.expire(at(now, 5)).is_empty(),
                "pinned entries are exempt from expiry"
            );
        }
        drop(guard);
        let mut state = manager.state.lock().unwrap();
        assert_eq!(
            state.cache.expire(at(now, 5)),
            vec!["g/x".to_string()],
            "the drop released the pin"
        );
    }

    // ------------------------------------------------------------------
    // GET /health snapshots (design §7): ModelManager::health() over the
    // shared ModelStats atomics.
    // ------------------------------------------------------------------

    /// Health of a fresh manager: status "ok", not shutting down, the test
    /// registry parses (registry_ok), and no models are reported. After
    /// shutdown() the same manager flips to status "shutting_down" with
    /// the flag set — the two fields always agree.
    #[tokio::test]
    async fn health_reports_empty_state_then_shutdown() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = &setup.manager;

        let health = manager.health();
        assert_eq!(health.status, "ok");
        assert!(!health.shutting_down);
        assert!(health.registry_ok, "the temp registry TOML parses");
        assert_eq!(health.model_count, 0);
        assert!(health.models.is_empty());

        manager.shutdown().await;
        let health = manager.health();
        assert_eq!(health.status, "shutting_down");
        assert!(health.shutting_down);
        assert_eq!(health.model_count, 0, "shutdown emptied the model map");
    }

    /// After a completed predict on the echo fixture the health snapshot
    /// shows the loaded model with its cache key, a single fully-free
    /// replica, an empty queue, and last_effective_cap = the server default
    /// (32) — neither the request nor the echo registry entry expressed a
    /// batch opinion, so the fallback chain bottoms out at the server
    /// default. The replica returns to the free pool only when the
    /// dispatcher reaps the finished window (after the reply is sent), so
    /// the idle counters are polled rather than asserted immediately.
    #[tokio::test]
    async fn health_reports_loaded_model_after_predict() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = &setup.manager;

        manager
            .predict(
                "echo/test",
                "key",
                10,
                -1,
                None,
                None,
                vec![data_input(json!({"text": "hi"}))],
            )
            .await
            .expect("predict");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let model = loop {
            let health = manager.health();
            assert_eq!(health.status, "ok");
            assert_eq!(health.model_count, 1);
            let model = health.models.into_iter().next().expect("one model");
            assert_eq!(model.inference_id, "echo/test");
            if model.replicas.free == model.replicas.total && model.in_flight_windows == 0 {
                break model;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "replica never returned to the free pool: {model:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        };
        assert_eq!(model.cache_keys, vec!["key".to_string()]);
        assert_eq!(model.replicas.total, 1, "echo has a single-replica set");
        assert_eq!(model.queue_depth, 0, "nothing left queued");
        assert_eq!(
            model.last_window_items,
            Some(1),
            "one single-input request formed the window"
        );
        assert_eq!(
            model.last_grant_units, None,
            "an unknown GPU inventory means the unpriced path: no grant"
        );
        assert_eq!(model.total_predict_requests, 1);
        assert_eq!(model.total_batches, 1);

        manager.shutdown().await;
    }

    /// While a slow predict is outstanding, health shows the activity:
    /// an in-flight window, a replica out of the free pool, or (if we
    /// sample before dispatch) a non-empty queue. The assertion is
    /// race-tolerant — any of the three proves the request is visible —
    /// and polls while the predict (1.5s in the slow_test fixture) runs.
    #[tokio::test]
    async fn health_shows_activity_during_slow_predict() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = setup.manager.clone();

        // Load first so the observation window is the predict itself, not
        // the spawn.
        manager
            .load_model("slow/test", "k", 10, -1, None)
            .await
            .expect("load");

        let task = {
            let manager = manager.clone();
            tokio::spawn(async move {
                manager
                    .predict(
                        "slow/test",
                        "k",
                        10,
                        -1,
                        None,
                        None,
                        vec![data_input(json!(null))],
                    )
                    .await
            })
        };

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let health = manager.health();
            let busy = health.models.iter().any(|model| {
                model.inference_id == "slow/test"
                    && (model.in_flight_windows >= 1
                        || model.replicas.free < model.replicas.total
                        || model.queue_depth > 0)
            });
            if busy {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the outstanding predict never became visible in health: {health:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        task.await
            .unwrap()
            .expect("the slow predict still completes normally");
        manager.shutdown().await;
    }

    /// The user cap is observable (design §10: "health endpoint exposes
    /// it"): after traffic where every request carries max_batch=2, no
    /// dispatched window held more than 2 inputs. This is the rewrite of the
    /// old `last_effective_cap` assertion — the cap now bounds the window
    /// itself (unpriced path) or the worker's packed batches (priced path),
    /// never a max-over-caps rule. Six singles capped at 2 need at least 3
    /// windows, which total_batches must reflect.
    #[tokio::test]
    async fn health_reports_the_capped_window_size() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = setup.manager.clone();

        manager
            .load_model("batch/test", "k", 10, -1, None)
            .await
            .expect("load");

        let mut tasks = Vec::new();
        for i in 0..6 {
            let manager = manager.clone();
            tasks.push(tokio::spawn(async move {
                manager
                    .predict(
                        "batch/test",
                        "k",
                        10,
                        -1,
                        Some(2),
                        None,
                        vec![data_input(json!(i))],
                    )
                    .await
            }));
        }
        for task in tasks {
            task.await.unwrap().expect("capped predict");
        }

        let health = manager.health();
        let model = health
            .models
            .iter()
            .find(|model| model.inference_id == "batch/test")
            .expect("model loaded");
        assert!(
            model.last_window_items.expect("a window dispatched") <= 2,
            "the user cap still bounds windows through the new path"
        );
        assert_eq!(model.total_predict_requests, 6);
        assert!(
            model.total_batches >= 3,
            "6 single-unit requests capped at 2 need >= 3 windows, got {}",
            model.total_batches
        );

        manager.shutdown().await;
    }

    // ------------------------------------------------------------------
    // R6: per-model load locks and the board-admission gate.
    // ------------------------------------------------------------------

    /// The B18/P5-3 regression test. A load of one model must not delay a
    /// predict to a *different*, already resident model: under the retired
    /// global load lock the predict below waited out the whole 3 s load (run1
    /// measured 11.885 s of stall for an 11.865 s load, 28x the p50, with a
    /// 600 s load deadline behind it).
    ///
    /// Two assertions, deliberately: the wall-clock bound, and the fact that
    /// the slow load was *still in flight* when the predict came back — the
    /// second one cannot be satisfied by a fast machine.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_slow_load_does_not_delay_predicts_to_a_resident_model() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = Arc::clone(&setup.manager);
        manager
            .predict(
                "echo/test",
                "k",
                10,
                -1,
                None,
                None,
                vec![data_input(json!(1))],
            )
            .await
            .expect("the resident model loads");

        let loader = {
            let manager = Arc::clone(&manager);
            tokio::spawn(async move {
                manager
                    .load_model("slowload/test", "k2", 10, -1, None)
                    .await
            })
        };
        // Let the load get past its bookkeeping and into the fixture's 3 s
        // `load()`; a spawn + handshake is well under this on any host.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            !loader.is_finished(),
            "the slow-load fixture sleeps 3 s in load(); it cannot be done yet"
        );

        let started = Instant::now();
        manager
            .predict(
                "echo/test",
                "k",
                10,
                -1,
                None,
                None,
                vec![data_input(json!(2))],
            )
            .await
            .expect("predict to the resident model");
        let elapsed = started.elapsed();
        assert!(
            !loader.is_finished(),
            "the predict must return while the other model is still loading"
        );
        assert!(
            elapsed < Duration::from_millis(500),
            "a predict to a resident model waited {elapsed:?} on another model's load"
        );

        loader
            .await
            .expect("load task")
            .expect("the slow load lands");
        manager.shutdown().await;
    }

    /// The gate is keyed by **board** and is `max_concurrent_loads` permits
    /// wide. Asserted on the gate itself rather than through two real loads:
    /// the alternative is a wall-clock race whose failure mode is a flaky
    /// test on a busy host, and the thing under test here is the keying.
    #[tokio::test]
    async fn the_load_admission_gate_is_per_board() {
        let setup = test_manager_with_loads(LoadPolicy {
            max_concurrent_loads: 1,
            ..LoadPolicy::default()
        });
        let manager = Arc::clone(&setup.manager);
        let board_a = [Some("GPU-0000".to_owned())];
        let board_b = [Some("GPU-3333".to_owned())];
        let unresolved = [None];

        let held = manager.acquire_load_admission("m/a", &board_a).await;
        assert_eq!(held.len(), 1, "one distinct board, one permit");

        // Another board, and the unresolved bucket, are not blocked by it.
        let other = tokio::time::timeout(
            Duration::from_millis(250),
            manager.acquire_load_admission("m/b", &board_b),
        )
        .await
        .expect("a load onto another board must not wait for this one");
        let none_board = tokio::time::timeout(
            Duration::from_millis(250),
            manager.acquire_load_admission("m/c", &unresolved),
        )
        .await
        .expect("the unresolved bucket is its own gate");

        // The same board is.
        assert!(
            tokio::time::timeout(
                Duration::from_millis(250),
                manager.acquire_load_admission("m/d", &board_a),
            )
            .await
            .is_err(),
            "a second load onto board A must wait at max_concurrent_loads = 1"
        );
        drop((held, other, none_board));

        // A multi-board set takes one permit per distinct board, deduped.
        let setup = test_manager_with_loads(LoadPolicy {
            max_concurrent_loads: 2,
            ..LoadPolicy::default()
        });
        let manager = Arc::clone(&setup.manager);
        let spread = [
            Some("GPU-3333".to_owned()),
            Some("GPU-0000".to_owned()),
            Some("GPU-0000".to_owned()),
        ];
        let held = manager.acquire_load_admission("m/e", &spread).await;
        assert_eq!(held.len(), 2, "two distinct boards out of three replicas");
        // Two permits per board: a second load of the same spread still fits.
        tokio::time::timeout(
            Duration::from_millis(250),
            manager.acquire_load_admission("m/f", &spread),
        )
        .await
        .expect("max_concurrent_loads = 2 admits a second load onto both boards");
    }

    /// A pin the ledger cannot place still spawns a worker that lands on
    /// *some* board — `resolve_pin` passes an ambiguous UUID prefix, an
    /// invisible index or a device list straight to the visibility variable
    /// even though `resolve_board_key` answers `None` for all three. So an
    /// unresolved replica must exclude every board's loads, not just other
    /// unresolved ones: otherwise it streams its weights beside an unpinned
    /// load onto the board it landed on, which is a collision the retired
    /// host-wide lock did prevent.
    #[tokio::test]
    async fn an_unresolved_board_key_blocks_every_board() {
        let setup = test_manager_full(
            Duration::from_secs(60),
            32,
            WorkerDeadlines::default(),
            test_gpus(),
            None,
            LoadPolicy {
                max_concurrent_loads: 1,
                ..LoadPolicy::default()
            },
        );
        let manager = Arc::clone(&setup.manager);
        let unresolved = [None];
        let board_a = [Some("GPU-0000".to_owned())];
        let board_b = [Some("GPU-3333".to_owned())];

        let held = manager.acquire_load_admission("m/a", &unresolved).await;
        assert_eq!(
            held.len(),
            3,
            "the shared bucket plus both boards of the inventory"
        );
        for (label, keys) in [("A", &board_a), ("B", &board_b)] {
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(250),
                    manager.acquire_load_admission("m/b", keys),
                )
                .await
                .is_err(),
                "an unresolved load must block board {label}"
            );
        }
        drop(held);

        // And the other way round: one resolved board is enough to make an
        // unresolved load wait.
        let held = manager.acquire_load_admission("m/c", &board_a).await;
        assert_eq!(held.len(), 1);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(250),
                manager.acquire_load_admission("m/d", &unresolved),
            )
            .await
            .is_err(),
            "a load onto a known board must block an unresolved one"
        );
        drop(held);

        // A host with no inventory has no boards to add, so the shared
        // bucket alone is the host-wide serialization it has today.
        let setup = test_manager_with_loads(LoadPolicy {
            max_concurrent_loads: 1,
            ..LoadPolicy::default()
        });
        let manager = Arc::clone(&setup.manager);
        let held = manager.acquire_load_admission("m/e", &unresolved).await;
        assert_eq!(held.len(), 1, "no inventory: just the shared bucket");
        assert!(
            tokio::time::timeout(
                Duration::from_millis(250),
                manager.acquire_load_admission("m/f", &unresolved),
            )
            .await
            .is_err(),
            "a GPU-less host still serializes every load"
        );
    }

    /// Two callers racing to use a model that is not loaded must produce one
    /// worker set, not two: that is the invariant the global lock existed for
    /// and the per-model lock now carries. The lock table must also be empty
    /// again afterwards — its keys come off the URL, so an entry that
    /// outlived its load would be an unbounded allocation any client can
    /// drive.
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_predicts_load_one_model_once() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = Arc::clone(&setup.manager);
        let started = Instant::now();
        let mut tasks = Vec::new();
        for i in 0..3 {
            let manager = Arc::clone(&manager);
            tasks.push(tokio::spawn(async move {
                manager
                    .predict(
                        "slowload/test",
                        "k",
                        10,
                        -1,
                        None,
                        None,
                        vec![data_input(json!(i))],
                    )
                    .await
            }));
        }
        for task in tasks {
            task.await.unwrap().expect("predict");
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(6),
            "three racing predicts paid {elapsed:?}, i.e. more than one 3 s load"
        );
        assert_eq!(
            manager.loaded_generation("slowload/test"),
            Some(0),
            "exactly one load generation was ever created"
        );
        assert!(
            manager.load_locks.lock().unwrap().is_empty(),
            "the per-model load lock table is emptied when the last holder lets go"
        );

        manager.shutdown().await;
    }

    // ------------------------------------------------------------------
    // R9: per-model load-failure cooldown.
    // ------------------------------------------------------------------

    /// The schedule, on the injected clock: `base × 2^(n−1)`, capped, and the
    /// cap is a ceiling on the *window*, not on the counter.
    #[test]
    fn cooldown_windows_double_and_cap() {
        let policy = LoadPolicy {
            max_concurrent_loads: 1,
            cooldown_base: Duration::from_secs(2),
            cooldown_max: Duration::from_secs(300),
        };
        let mut cooldowns = LoadCooldowns::default();
        let now = Instant::now();
        let windows: Vec<u64> = (0..12)
            .map(|_| {
                cooldowns
                    .note_failure("g/a", "boom", &policy, now)
                    .expect("cooldowns are enabled")
                    .as_secs()
            })
            .collect();
        assert_eq!(
            windows,
            vec![2, 4, 8, 16, 32, 64, 128, 256, 300, 300, 300, 300],
            "the shipped ladder: nine failures and 8.5 minutes to the cap"
        );
        assert_eq!(cooldowns.entries["g/a"].failures, 12);
        assert_eq!(cooldowns.entries["g/a"].last_error, "boom");
        // The deadline is monotonic and the window is what set it.
        assert!(
            cooldowns
                .active("g/a", now + Duration::from_secs(299))
                .is_some()
        );
        assert!(
            cooldowns
                .active("g/a", now + Duration::from_secs(301))
                .is_none()
        );
        assert!(
            cooldowns.active("g/b", now).is_none(),
            "other models untouched"
        );
    }

    /// The cap is a *floor* on the wait once it is reached: the window must
    /// never come back down, however long the model stays broken.
    ///
    /// The regression this pins: `1u32 << doublings` with `doublings` clamped
    /// at 32 is an overflowing shift, so the 33rd consecutive failure dropped
    /// the window from 300 s straight back to the 2 s base — and every
    /// failure after it. At the shipped ladder a model that keeps failing
    /// reaches 33 in under three hours, which is exactly how a run long
    /// enough to matter got finding B15's hammering back.
    #[test]
    fn the_cooldown_window_never_falls_back_off_the_cap() {
        let policy = LoadPolicy::default();
        let mut cooldowns = LoadCooldowns::default();
        let now = Instant::now();
        // Well past both the cap (9 failures) and the shift width (33).
        for failure in 1..=64u32 {
            let window = cooldowns
                .note_failure("g/a", "boom", &policy, now)
                .expect("cooldowns are enabled");
            assert!(
                window >= policy.cooldown_base && window <= policy.cooldown_max,
                "failure {failure} armed a {window:?} window, outside [base, max]"
            );
            if failure >= 9 {
                assert_eq!(
                    window, policy.cooldown_max,
                    "failure {failure} must still be at the cap"
                );
            }
        }
        assert_eq!(cooldowns.entries["g/a"].failures, 64);
    }

    /// Neither the deadline nor the pruning may overflow the monotonic clock,
    /// whatever the operator wrote in the TOML. `Instant + Duration` panics,
    /// and both additions run under the state mutex — a panic there would
    /// poison the manager, not just lose a cooldown.
    #[test]
    fn an_absurd_configured_cooldown_is_clamped_rather_than_overflowing() {
        let local = crate::config::InferenceLocalConfig {
            load_failure_cooldown_secs: u64::MAX,
            load_failure_cooldown_max_secs: u64::MAX,
            ..Default::default()
        };
        let policy = LoadPolicy::from(&local);
        assert_eq!(policy.cooldown_base.as_secs(), MAX_COOLDOWN_SECS);
        assert_eq!(policy.cooldown_max.as_secs(), MAX_COOLDOWN_SECS);

        let mut cooldowns = LoadCooldowns::default();
        let now = Instant::now();
        let window = cooldowns
            .note_failure("g/a", "boom", &policy, now)
            .expect("cooldowns are enabled");
        assert_eq!(window.as_secs(), MAX_COOLDOWN_SECS);
        assert!(cooldowns.active("g/a", now).is_some());
        // The `until + cooldown_max` a year past a deadline a year out.
        cooldowns.prune(&policy, now);
        assert_eq!(cooldowns.entries.len(), 1, "nowhere near forgettable yet");
    }

    /// A successful load clears the ladder, and a zero base disables it.
    #[test]
    fn a_successful_load_clears_the_cooldown_and_zero_disables_it() {
        let policy = LoadPolicy::default();
        let mut cooldowns = LoadCooldowns::default();
        let now = Instant::now();
        cooldowns.note_failure("g/a", "boom", &policy, now);
        cooldowns.note_failure("g/a", "boom", &policy, now);
        assert_eq!(cooldowns.entries["g/a"].failures, 2);
        cooldowns.clear("g/a");
        assert!(cooldowns.active("g/a", now).is_none());
        // The next failure starts the ladder over at the base window.
        assert_eq!(
            cooldowns.note_failure("g/a", "boom", &policy, now),
            Some(Duration::from_secs(2))
        );

        let off = LoadPolicy {
            cooldown_base: Duration::ZERO,
            ..LoadPolicy::default()
        };
        let mut cooldowns = LoadCooldowns::default();
        assert_eq!(cooldowns.note_failure("g/a", "boom", &off, now), None);
        assert!(
            cooldowns.active("g/a", now).is_none(),
            "a zero base records nothing at all"
        );
    }

    /// The history of a model nobody is retrying is forgotten, so the table
    /// cannot grow without bound on ids that come off the URL.
    #[test]
    fn an_untouched_cooldown_is_pruned_after_the_cap() {
        let policy = LoadPolicy::default();
        let mut cooldowns = LoadCooldowns::default();
        let now = Instant::now();
        cooldowns.note_failure("g/a", "boom", &policy, now);
        cooldowns.prune(&policy, now + Duration::from_secs(120));
        assert_eq!(cooldowns.entries.len(), 1, "still inside window + cap");
        cooldowns.prune(&policy, now + Duration::from_secs(303));
        assert!(
            cooldowns.entries.is_empty(),
            "expired for longer than the ceiling: the ladder resets"
        );
    }

    /// End to end on a model that cannot load: the first request pays a real
    /// spawn, the second is refused without one, `/health` says so, and the
    /// ladder escalates once the window passes. Under the old code every one
    /// of these requests spawned a worker (run1 B15: 93 loads in 182 s).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_load_puts_the_model_in_a_cooldown() {
        let setup = test_manager_with_loads(LoadPolicy {
            max_concurrent_loads: 1,
            // Short enough to watch the ladder inside a test, long enough
            // that the refusal below cannot be a race.
            cooldown_base: Duration::from_millis(600),
            cooldown_max: Duration::from_secs(300),
        });
        let manager = Arc::clone(&setup.manager);
        let predict = |manager: Arc<ModelManager>| async move {
            manager
                .predict(
                    "missing/test",
                    "k",
                    10,
                    -1,
                    None,
                    None,
                    vec![data_input(json!(1))],
                )
                .await
                .expect_err("this model has no impl class")
        };

        let attempt_started = Instant::now();
        let first = predict(Arc::clone(&manager)).await;
        let spawn_attempt = attempt_started.elapsed();
        assert!(
            format!("{first:#}").contains("failed to load model"),
            "the first request pays a real load attempt: {first:#}"
        );
        assert!(
            first
                .chain()
                .all(|source| source.downcast_ref::<LoadCooldownError>().is_none()),
            "the failure that *arms* the cooldown is still reported as itself"
        );

        let refused_started = Instant::now();
        let second = predict(Arc::clone(&manager)).await;
        let refusal = refused_started.elapsed();
        let cooldown = second
            .chain()
            .find_map(|source| source.downcast_ref::<LoadCooldownError>())
            .expect("the second request is refused by the cooldown");
        assert_eq!(cooldown.failures, 1);
        assert_eq!(cooldown.model, "missing/test");
        assert_eq!(
            cooldown.retry_after_secs, 1,
            "Retry-After rounds up and never reports 0"
        );
        assert!(
            refusal < Duration::from_millis(200) && refusal < spawn_attempt,
            "the refusal ({refusal:?}) must be far cheaper than the load attempt \
             it replaces ({spawn_attempt:?})"
        );
        assert_eq!(
            manager.cached_models().get("missing/test"),
            None,
            "a refused load leaves no cache entry either"
        );

        let health = manager.health();
        assert_eq!(health.load_cooldowns.len(), 1);
        let reported = &health.load_cooldowns[0];
        assert_eq!(reported.inference_id, "missing/test");
        assert_eq!(reported.failures, 1);
        assert!(
            !reported.last_error.is_empty(),
            "health carries the failure that armed it"
        );
        assert!(
            DateTime::parse_from_rfc3339(&reported.retry_at).is_ok(),
            "retry_at is RFC 3339: {}",
            reported.retry_at
        );

        // Past the window the model is tried again — and the second real
        // failure doubles the wait.
        tokio::time::sleep(Duration::from_millis(700)).await;
        let third = predict(Arc::clone(&manager)).await;
        assert!(
            format!("{third:#}").contains("failed to load model"),
            "the expired cooldown lets exactly one attempt through: {third:#}"
        );
        let fourth = predict(Arc::clone(&manager)).await;
        let cooldown = fourth
            .chain()
            .find_map(|source| source.downcast_ref::<LoadCooldownError>())
            .expect("refused again");
        assert_eq!(cooldown.failures, 2, "consecutive failures escalate");

        manager.shutdown().await;
    }

    /// The wire contract Track E's job side matches on (pinned in the run2
    /// brief): the kind token, an RFC 3339 `retry_at`, and a `Retry-After`
    /// that is never 0. `http.rs` assembles these into
    /// `{"detail": {"kind": "load_cooldown", …}}` with status 503.
    #[test]
    fn the_cooldown_error_carries_the_pinned_wire_fields() {
        assert_eq!(LOAD_COOLDOWN_KIND, "load_cooldown");
        let error = LoadCooldownError {
            model: "g/a".to_owned(),
            failures: 3,
            last_error: "ImportError: no such impl".to_owned(),
            retry_at: Local::now() + chrono::Duration::seconds(8),
            retry_after_secs: 8,
        };
        assert!(DateTime::parse_from_rfc3339(&error.retry_at.to_rfc3339()).is_ok());
        let rendered = error.to_string();
        assert!(
            rendered.contains("g/a") && rendered.contains("ImportError: no such impl"),
            "the human message names the model and the failure: {rendered}"
        );
        assert!(
            !rendered.contains("failed to load model"),
            "must not collide with the load-failure detail string http.rs matches"
        );
        // The stored text is clamped so /health and every refusal stay small.
        let long = "x".repeat(MAX_COOLDOWN_ERROR_BYTES * 2);
        let clamped = clamp_cooldown_error(&long);
        assert_eq!(clamped.chars().count(), MAX_COOLDOWN_ERROR_BYTES + 1);
        assert!(clamped.ends_with('…'));
        assert_eq!(clamp_cooldown_error("short"), "short");
    }
}
