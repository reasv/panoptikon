//! Model manager: the LRU/TTL model cache, the load path and worker
//! supervision on top of the per-model dispatcher (`dispatch.rs`). Ports the
//! semantics of the legacy Python `inferio/manager.py` (design doc §5, §6).
//!
//! State model (all bookkeeping under one std `Mutex`, never held across an
//! await): `lru_caches[cache_key]` is an insertion-ordered `inference_id ->
//! expiration` map with `lru_size` enforced oldest-first on every load, and
//! `cache_refs[inference_id]` holds the keys referencing a model, which unloads
//! when the last one goes. A `ttl_seconds` below zero means never; a sweeper
//! expires the rest every `sweep_interval`, skipping pinned models entirely.
//!
//! # Lock order
//!
//! 1. `load_barrier` (async `RwLock`) — read-guarded by the slow path of
//!    [`ModelManager::ensure_loaded`] while a spawn may be in flight,
//!    write-guarded once by [`ModelManager::shutdown`], which therefore cannot
//!    drain before an in-flight load has decided about its workers.
//! 2. `load_locks[inference_id]` (async `Mutex`, one per model) — the only
//!    serialization: two callers must not spawn the same model twice.
//! 3. `load_admission[gpu]` (`Semaphore`, `max_concurrent_loads` permits per
//!    GPU) — how many models may stream weights into one GPU at once; taken
//!    inside [`ModelManager::spawn_model`], in sorted GPU-key order.
//! 4. `state` (std `Mutex`) — all bookkeeping.
//!
//! The `load_locks` and `load_admission` tables' own std mutexes and the
//! `prewarm`, `ledger` and `registry` mutexes are **leaves**.
//!
//! **No deadlock.** The acquisition sites are exactly `ensure_loaded` (4,
//! released; then 1 -> 2 -> 4 -> [3 inside `spawn_model`, released] -> 4),
//! `shutdown` (4, released; then 1; then 4 again) and every other method (4
//! alone) — `shutdown` releases 4 before acquiring 1, so that pair is never
//! held together. No site waits for a lower-numbered lock while holding a
//! higher-numbered one, so the numbering is a total order over every held set
//! and no cycle can form; within 3, a multi-GPU set's permits are taken in
//! sorted key order. RAII guards throughout, so cancellation strands nothing.
//!
//! A model whose loads keep failing is put in a doubling per-model cooldown
//! ([`LoadCooldowns`]) and refused with a 503 until it expires. The deliberate
//! deviations from the Python manager are listed in
//! docs/inferio-worker-protocol.md "Lifecycle and timeouts (orchestrator side)".

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
use crate::db::ledger::truncate_error;

/// Manager configuration.
pub struct ManagerConfig {
    /// How worker processes are spawned (python, impl dirs, env, deadlines).
    pub spawn: WorkerSpawnConfig,
    /// Window size for the unpriced dispatch path when the registry declares
    /// none; priced models are sized by the ledger.
    pub default_max_batch: u32,
    /// TTL sweeper period.
    pub sweep_interval: Duration,
    /// Admission-gate width and cooldown ladder (`[inference_local]`).
    pub loads: LoadPolicy,
    /// Prewarm pool policy (design §8; `[inference_local.prewarm]`).
    pub prewarm: PrewarmConfig,
    /// Visible GPUs, probed once at startup: replica pins resolve against this
    /// and the ledger is keyed by these UUIDs. Unknown leaves workers unpinned.
    pub gpus: GpuInventory,
    /// VRAM admission limits: the server default plus per-GPU overrides.
    pub vram: VramBudgets,
    /// Shipped baselines plus the local profile file; `None` leaves the ledger
    /// unprimed.
    pub calibration: Option<Arc<dyn CalibrationProfiles>>,
}

/// Load-path policy (`[inference_local]`), built from the config by a `From`
/// impl.
#[derive(Debug, Clone, Copy)]
pub struct LoadPolicy {
    /// How many models may be streaming weights into **one GPU** at once
    /// (module docs, lock 3); 0 is read as 1. Raising it is safe because a load
    /// charges its expected base in the same ledger section that reads headroom.
    pub max_concurrent_loads: usize,
    /// First cooldown window, doubled per consecutive failure up to
    /// [`LoadPolicy::cooldown_max`]. `Duration::ZERO` disables the cooldown.
    pub cooldown_base: Duration,
    /// Ceiling on the cooldown window.
    pub cooldown_max: Duration,
}

/// Ceiling on the configured cooldown seconds: a window becomes an `Instant +
/// Duration` deadline, which panics on overflow.
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

/// Wire `kind` of the load-failure cooldown error; `http.rs` answers 503.
pub(crate) const LOAD_COOLDOWN_KIND: &str = "load_cooldown";

/// The error a cooldown-refused load returns; `http.rs` matches it out of the
/// `anyhow` chain.
#[derive(Debug, Clone)]
pub(crate) struct LoadCooldownError {
    /// `group/name`.
    pub model: String,
    pub failures: u32,
    /// The failure that (re)armed the cooldown, clamped.
    pub last_error: String,
    /// Rendered from the monotonic deadline, so a clock step cannot move it.
    pub retry_at: DateTime<Local>,
    /// The same interval in whole seconds, for `Retry-After`; at least 1,
    /// because `Retry-After: 0` invites the hammering this exists to stop.
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

/// Why a `spawn_model` failed, in the one dimension the cooldown cares about.
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

/// One model's load-failure history.
struct CooldownEntry {
    failures: u32,
    last_error: String,
    /// Monotonic deadline; the wall clock is only ever *rendered* from it.
    until: Instant,
    /// The window `until` was computed with, for `/health` and pruning.
    window: Duration,
}

/// Per-model load-failure cooldowns: `base × 2^(failures−1)` capped at `max`,
/// escalating from the first failure and without jitter. A pure state machine
/// over an injected clock, like [`CacheState`]. See
/// docs/inferio-worker-protocol.md "Lifecycle and timeouts (orchestrator side)".
#[derive(Default)]
struct LoadCooldowns {
    entries: HashMap<String, CooldownEntry>,
}

impl LoadCooldowns {
    /// Record a failed load; `None` when cooldowns are disabled.
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
        // Clamped: the text is repeated on every refused request and every
        // `/health` poll.
        entry.last_error = truncate_error(error).into_owned();
        // `failures - 1` doublings, clamped at the widest shift a `u32` can
        // represent: `1u32 << 32` panics, or wraps to 1 with checks off.
        let doublings = (entry.failures - 1).min(31);
        let window = policy
            .cooldown_base
            .checked_mul(1u32 << doublings)
            .unwrap_or(policy.cooldown_max)
            .min(policy.cooldown_max)
            // As [`MAX_COOLDOWN_SECS`], for a hand-built [`LoadPolicy`] too.
            .min(Duration::from_secs(MAX_COOLDOWN_SECS));
        entry.window = window;
        entry.until = now + window;
        Some(window)
    }

    /// A successful load clears the history: the ladder counts consecutive.
    fn clear(&mut self, inference_id: &str) {
        self.entries.remove(inference_id);
    }

    fn active(&self, inference_id: &str, now: Instant) -> Option<&CooldownEntry> {
        self.entries
            .get(inference_id)
            .filter(|entry| entry.until > now)
    }

    /// Forget a cooldown that expired longer ago than the ceiling: the ladder
    /// starts over, and a map keyed off the URL that only grew is unbounded.
    fn prune(&mut self, policy: &LoadPolicy, now: Instant) {
        self.entries.retain(|_, entry| {
            entry
                .until
                .checked_add(policy.cooldown_max)
                .is_none_or(|forget_at| forget_at > now)
        });
    }
}

/// `GET /health` response (design §7). Serialized as-is by the HTTP layer;
/// `Deserialize` exists so tests can round-trip the wire shape.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct HealthReport {
    /// `"ok"` normally, `"shutting_down"` once shutdown has begun.
    pub status: String,
    /// Same signal as `status`, machine-friendly.
    pub shutting_down: bool,
    /// Whether the inference registry currently loads (see `health()`).
    pub registry_ok: bool,
    /// Number of loaded models (== `models.len()`).
    pub model_count: usize,
    /// Per loaded model liveness/queue snapshot, sorted by inference_id.
    pub models: Vec<ModelHealth>,
    /// Prewarm snapshot: the switches plus one entry per impl class held.
    pub prewarm: PrewarmHealth,
    /// Visible GPUs by UUID; empty when the host has no inventory.
    pub gpus: Vec<GpuInfo>,
    /// Per-GPU VRAM ledger: budgets, footprints, grants, ramp and deflation
    /// state, the fitted cost model. Empty with no GPU inventory.
    pub vram: Vec<GpuBudgetHealth>,
    /// Models whose loads are failing, sorted by inference_id; an entry lives
    /// from the first failed load until one succeeds or the history is pruned.
    pub load_cooldowns: Vec<LoadCooldownHealth>,
    /// The inference **client** side: one entry per endpoint this process holds
    /// a client for, sorted by base URL.
    pub inference_clients: Vec<crate::inferio_client::InferenceTransportHealth>,
    /// How much of the predict-body budget is spoken for, and refusals so far.
    pub predict_body_budget: crate::inferio::http::PredictBodyBudgetHealth,
}

/// One model's load-failure cooldown in the [`HealthReport`]. A cooling-down
/// model is by construction not loaded, so this cannot live in `models[]`.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct LoadCooldownHealth {
    pub inference_id: String,
    /// Consecutive failed loads.
    pub failures: u32,
    /// The failure that armed the current window (clamped to 2000 bytes).
    pub last_error: String,
    /// RFC 3339 instant the model may be retried at.
    pub retry_at: String,
    /// Whole seconds until then; 0 once the window has passed.
    pub retry_after_secs: u64,
    /// The window this failure count earned, in seconds.
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
    /// WorkerSet occupancy: `free < total` means replicas are running windows.
    pub replicas: ReplicaHealth,
    /// Requests waiting in the model's FIFO queue.
    pub queue_depth: usize,
    /// Windows currently executing on replicas.
    pub in_flight_windows: usize,
    /// Unit budget of the grant on the most recently dispatched window; `null`
    /// until one carries a grant, and always on the unpriced path.
    pub last_grant_units: Option<u64>,
    /// Inputs in the most recently dispatched window; `null` until the first.
    pub last_window_items: Option<u32>,
    /// Items the orchestrator wants callers to keep in flight — the figure
    /// `x-panoptikon-desired-in-flight-items` carries. `null` before the first
    /// window.
    pub desired_in_flight_items: Option<u64>,
    /// Predict requests ever queued on this model's dispatcher.
    pub total_predict_requests: u64,
    /// Windows ever dispatched to a replica.
    pub total_batches: u64,
    /// Of those, the ones the queue rather than the GPU decided the size of.
    pub queue_bound_windows: u64,
    /// Cost dimension resolved from registry metadata at load time.
    pub cost: CostHealth,
    /// Per replica: its GPU and the freshest memory sensing it reported.
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
    /// True when the registry declared nothing usable and `(item, count)` is
    /// in force.
    pub degraded: bool,
    /// The per-item **pixel canvas** this model's inputs are priced against
    /// (`metadata.cost.canvas_pixels`, or one from the model's own load
    /// report), or `null` for uncapped. Under a canvas the worker prices every
    /// input at `min(raw_pixels, canvas_pixels)`.
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

/// Per-replica GPU placement plus its freshest memory report; every field after
/// `gpu` is `null` until the worker reports it. On a CPU or MPS host the figures
/// are system RAM and Metal's budget.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ReplicaTelemetryHealth {
    /// Resolved device pin the worker was *spawned* with — a GPU UUID on CUDA,
    /// a HIP device index on ROCm; the visibility variables differ.
    pub gpu: Option<String>,
    /// The GPU the worker itself reports being on; only it can see what it got.
    /// `null` on ROCm, which the ledger admits by PCI address instead.
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
    /// Driver behind `free_mb`/`total_mb`; they disagree by gigabytes.
    pub free_source: Option<String>,
    pub reserved_mb: Option<u64>,
    pub allocated_mb: Option<u64>,
    pub memory_age_ms: Option<u64>,
    /// Measurements reported since load, including ones the ring evicted.
    pub measurements_recorded: u64,
    /// The tail of the measurement ring, oldest first — a sample, not all.
    pub recent_batches: Vec<BatchHealth>,
}

/// One measured GPU batch in [`ReplicaTelemetryHealth`].
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct BatchHealth {
    /// Per-worker sequence number; a gap means the ring evicted samples.
    pub seq: u64,
    pub age_ms: u64,
    /// Inputs in the batch, not cost-dimension units.
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
            // Advisory data: a poisoned mutex must not fail /health.
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        let now = Instant::now();
        let age_ms =
            |captured_at: Instant| now.saturating_duration_since(captured_at).as_millis() as u64;
        // The load report's timestamp matters to the ledger, not here.
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
        // Read the ring without draining: the ledger consumes by watermark.
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

/// Per-cache-key entry expiration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expiration {
    Never,
    At(DateTime<Local>),
}

impl Expiration {
    /// `ttl_seconds >= 0` -> now + ttl; negative -> never. It comes off a query
    /// param, so an unrepresentable value saturates instead of panicking.
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

    /// For `GET /cache/{key}`: `None` is never, which the HTTP layer maps to
    /// the wire's `"9999-12-31T23:59:59.999999"`.
    fn render(&self) -> Option<String> {
        match self {
            Expiration::Never => None,
            Expiration::At(at) => Some(isoformat(at)),
        }
    }
}

/// The wire's `datetime.isoformat()`: seconds precision, or six fractional
/// digits when there are microseconds; never a UTC offset.
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
    was_present: bool,
    /// The model to unload when this was its last reference.
    unload: Option<String>,
}

/// Pure LRU/TTL/refcount state machine. Methods return the ids whose last
/// reference went; the caller owns unloading them.
#[derive(Default)]
struct CacheState {
    /// Per cache key, insertion-ordered id -> expiry.
    lru_caches: HashMap<String, LinkedHashMap<String, Expiration>>,
    /// id -> the cache keys referencing it.
    cache_refs: HashMap<String, HashSet<String>>,
    /// Predict/load pin refcounts: a pinned model never expires.
    pins: HashMap<String, u32>,
}

impl CacheState {
    /// `load_model`: add the reference, move the entry to most-recent, renew
    /// it, enforce `lru_size`. Returns what eviction frees.
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

    /// Evict oldest while over size. A non-positive `lru_size` evicts even the
    /// entry just added, which the caller reads as a refused load.
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

    /// Drop one entry and its reference; report the model when it was the last.
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

    /// Drop a whole cache key. Returns (entries removed, models to unload).
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

    /// Strict `now > expiration`; a pinned model cannot expire mid-inference.
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

    /// Post-predict unpin + TTL restore on the predict's own cache-key entry;
    /// no effect when the entry went meanwhile.
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

    /// Fatal-worker-death cleanup: drop the model from every LRU and the ref
    /// map; pins unwind as in-flight predicts observe their errors.
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

    /// `GET /cache`: id -> the cache keys referencing it.
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

    /// Sorted cache keys referencing one model, for health reporting.
    fn cache_keys(&self, inference_id: &str) -> Vec<String> {
        let mut keys: Vec<String> = self
            .cache_refs
            .get(inference_id)
            .map(|refs| refs.iter().cloned().collect())
            .unwrap_or_default();
        keys.sort();
        keys
    }

    /// Unknown cache keys yield an empty map.
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

/// A freshly spawned WorkerSet plus what the model entry records about it.
struct SpawnedModel {
    workers: Vec<Worker>,
    /// One per worker: `Some` when the replica landed on a GPU the ledger
    /// knows and the model's cost dimension scales.
    admissions: Vec<Option<Admission>>,
    registry_default_batch: Option<u32>,
    impl_class: String,
    /// Whether keeping a warm worker for this class can ever pay off.
    claim_eligible: bool,
    cost: CostDimension,
}

/// A loaded model: the dispatcher queue plus the task owning its WorkerSet.
struct ModelHandle {
    tx: mpsc::UnboundedSender<DispatchMsg>,
    task: JoinHandle<()>,
    /// Monotonic load generation, for death-cleanup races.
    generation: u64,
    /// Shared with the dispatcher: it writes, `health()` reads, Relaxed.
    stats: Arc<ModelStats>,
    /// Resolved at load, so a running model keeps the dimension it was priced
    /// with.
    cost: CostDimension,
    /// One handle per replica, shared with the dispatcher's workers.
    telemetry: Vec<TelemetryHandle>,
}

#[derive(Default)]
struct ManagerState {
    cache: CacheState,
    /// inference_id -> loaded model.
    models: HashMap<String, ModelHandle>,
    /// Dispatcher tasks still draining after an unload; awaited on shutdown.
    draining: Vec<JoinHandle<()>>,
    next_generation: u64,
    shutting_down: bool,
    /// Per-model load-failure cooldowns; under the state mutex because every
    /// read already takes it for the loaded-check beside it.
    cooldowns: LoadCooldowns,
}

/// RAII handle for a pin refcount taken in [`CacheState`]: every pin is wrapped
/// in one immediately, so an early return or a *future cancellation* still
/// releases it — a leaked pin would exempt the model from TTL expiry forever.
/// Predict pins carry the requested (cache_key, ttl) and Drop restores it.
struct PinGuard {
    /// Weak so a guard alive past manager teardown is a no-op.
    manager: Weak<ModelManager>,
    inference_id: String,
    /// `Some((cache_key, ttl))` for predict pins: restore it on release.
    restore: Option<(String, i64)>,
}

impl PinGuard {
    /// Wrap a pin the caller already took under the state lock. Does not lock.
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

    /// Release under a lock the caller holds (Drop re-locks) and defuse.
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
        // Ignore a poisoned mutex: panicking inside Drop would abort.
        if let Ok(mut state) = manager.state.lock() {
            Self::release(&mut self.restore, &self.inference_id, &mut state.cache);
        }
    }
}

/// Device-admission bucket for a replica whose device key does not resolve;
/// taken **as well as** every GPU's permit, never instead
/// ([`ModelManager::acquire_load_admission`]). It sorts before every uuid, so
/// the acquisition order stays total.
const UNRESOLVED_DEVICE_ADMISSION_KEY: &str = "";

/// What one pass of [`ModelManager::touch_and_check`] decided.
enum TouchOutcome {
    /// Loaded and the caller is done: `Some` carries the dispatcher sender and
    /// the predict pin, `None` is a plain `PUT /load`.
    Ready(Option<(mpsc::UnboundedSender<DispatchMsg>, PinGuard)>),
    /// Not loaded; `Some` is the spawn-phase pin (second pass only).
    NeedsSpawn(Option<PinGuard>),
}

/// RAII handle for one model's load lock (module docs, lock 2). It owns the
/// guard *and* the table entry's lifetime: on drop it releases the mutex and
/// removes the entry when nobody else holds or waits on it. The guard is an
/// `Option` so the handle can be built *before* the wait.
struct ModelLoadGuard<'a> {
    manager: &'a ModelManager,
    inference_id: &'a str,
    /// The same `Arc` the table holds; its strong count decides the removal.
    lock: Arc<TokioMutex<()>>,
    guard: Option<OwnedMutexGuard<()>>,
}

impl Drop for ModelLoadGuard<'_> {
    fn drop(&mut self) {
        // Release the mutex first: the owned guard holds a reference of its
        // own, so the count below only means anything once it is gone.
        drop(self.guard.take());
        // A poisoned table holds a mutex, not state; not worth aborting Drop.
        let mut locks = match self.manager.load_locks.lock() {
            Ok(locks) => locks,
            Err(poisoned) => poisoned.into_inner(),
        };
        // Two strong references under the table lock — the table's and this
        // handle's — proves nobody else can be waiting or holding a clone.
        if Arc::strong_count(&self.lock) == 2 {
            locks.remove(self.inference_id);
        }
    }
}

/// The model manager. [`ModelManager::new`] needs a running tokio runtime.
pub struct ModelManager {
    cfg: ManagerConfig,
    registry: Arc<StdMutex<RegistryCache>>,
    state: StdMutex<ManagerState>,
    /// One parked warm worker per impl class (design §8); its own mutex.
    prewarm: Arc<PrewarmPool>,
    /// Per-GPU VRAM budget arbiter; its own mutex, and every operation is
    /// synchronous bounded arithmetic.
    ledger: Arc<VramLedger>,
    /// One load lock per model id (module docs, lock 2), created on demand and
    /// removed by [`ModelLoadGuard`]: an id-keyed table that grew is unbounded.
    load_locks: StdMutex<HashMap<String, Arc<TokioMutex<()>>>>,
    /// The device-admission gate (module docs, lock 3): `max_concurrent_loads`
    /// permits per device key, plus a bucket for unresolved keys.
    load_admission: StdMutex<HashMap<String, Arc<Semaphore>>>,
    /// Shutdown barrier (module docs, lock 1).
    load_barrier: TokioRwLock<()>,
    /// Self-reference handed to dispatcher tasks for death cleanup.
    weak: OnceLock<Weak<ModelManager>>,
    sweeper: StdMutex<Option<JoinHandle<()>>>,
}

impl ModelManager {
    pub fn new(cfg: ManagerConfig, registry: Arc<StdMutex<RegistryCache>>) -> Arc<Self> {
        let sweep_interval = cfg.sweep_interval;
        // Pooled workers take the default GPU an unpinned replica resolves to,
        // or they could never be claimed: eligibility is pin equality.
        let prewarm = PrewarmPool::new(cfg.spawn.clone(), cfg.prewarm.clone(), cfg.gpus.clone());
        // The ledger's GPUs come from the probe the pins do, so a key and a pin
        // cannot describe different hardware.
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
        // always_warm warms at startup in every launch mode; the eager DB-scan
        // loop is gateway-only and started by main.rs.
        manager.prewarm.warm_always();
        manager
            .weak
            .set(Arc::downgrade(&manager))
            .expect("weak self is set exactly once");
        // Only a Weak, so dropping the last Arc ends the task on its next tick.
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

    /// `PUT /load/{group}/{id}`: idempotent load — spawns the worker when the
    /// model isn't loaded, and always renews TTL + LRU position and enforces
    /// `lru_size`. `prewarm_hint` is the query param; `Some(false)` skips the
    /// lazy warm.
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

    /// `POST /predict/{group}/{id}`: auto-loads, pins the model, queues the
    /// request, and restores the requested TTL whether it succeeded or not.
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
        // Both arms are typed [`Unattempted`] — the dispatch task had ended, or
        // a sibling replica died — so neither request ran and both may re-run.
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
        // Explicit drop keeps the unpin + TTL restore at completion time.
        drop(pin);
        // A window just settled, when the ledger's picture is freshest.
        self.deliver_pending_trims();
        result
    }

    /// Items the orchestrator would like a caller to keep inside in-flight
    /// predict requests, written by the dispatcher on every window formation.
    /// The HTTP layer puts it on the predict response; `None` is "no opinion".
    pub fn desired_in_flight_items(&self, inference_id: &str) -> Option<u64> {
        let state = self.state.lock().unwrap();
        let handle = state.models.get(inference_id)?;
        match handle.stats.desired_in_flight_items.load(Relaxed) {
            0 => None,
            value => Some(value),
        }
    }

    /// `DELETE /cache/{key}/{group}/{id}`: remove one entry, unloading the model
    /// when it was the last reference. Returns whether it existed.
    pub async fn unload_model(&self, cache_key: &str, inference_id: &str) -> Result<bool> {
        let mut state = self.state.lock().unwrap();
        tracing::debug!(model = %inference_id, cache_key = %cache_key, "unload requested");
        let outcome = state.cache.remove(cache_key, inference_id);
        if let Some(id) = outcome.unload {
            Self::begin_unload(&mut state, &id);
        }
        Ok(outcome.was_present)
    }

    /// `DELETE /cache/{key}`: clear a whole cache key, unloading the models
    /// whose last reference lived there.
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

    /// `GET /cache/{key}`: inference_id -> rendered expiration, `None` never.
    pub fn cache_expirations(&self, cache_key: &str) -> BTreeMap<String, Option<String>> {
        self.state.lock().unwrap().cache.expirations(cache_key)
    }

    /// `GET /health` (design §7): a snapshot of orchestrator and per-model
    /// state, from the shared [`ModelStats`] atomics without disturbing any
    /// dispatcher. `registry_ok` is the mtime-gated `RegistryCache::get()`, so
    /// it costs a stat unless the registry actually changed.
    pub fn health(&self) -> HealthReport {
        let registry_ok = self.registry.lock().unwrap().get().is_ok();
        // Pool and ledger snapshots first: never held with the state lock.
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
        // Failing loads, which by construction are never in `models` above.
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
            predict_body_budget: crate::inferio::http::predict_body_budget_health(),
        }
    }

    /// The prewarm pool (design §8), for the eager task and tests.
    pub(crate) fn prewarm_pool(&self) -> &Arc<PrewarmPool> {
        &self.prewarm
    }

    /// The registry cache, for the eager task's setter -> impl-class mapping.
    pub(crate) fn registry_cache(&self) -> &Arc<StdMutex<RegistryCache>> {
        &self.registry
    }

    /// Graceful shutdown: stop the sweeper, refuse new loads/predicts, fail
    /// queued requests, and run every worker's graceful stop ladder. A load in
    /// flight when the flag flips finishes its spawn, observes `shutting_down`
    /// and parks a worker-discard task in `draining`; write-locking
    /// `load_barrier` waits for that decision, so the second drain awaits the
    /// discard instead of abandoning the worker mid-stop.
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
        // Parked prewarmed workers get the same ladder, concurrently.
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
        // Last: calibration earned just before the quit is still behind the
        // store's write debounce, and losing it costs a re-ramp.
        if let Some(calibration) = self.cfg.calibration.clone() {
            let _ = tokio::task::spawn_blocking(move || calibration.flush()).await;
        }
    }

    /// Called by a dispatcher after a fatal worker death: drop the model from
    /// all bookkeeping so the next predict auto-loads a fresh worker. The
    /// generation stops a dispatcher that lost a respawn race.
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
        // The task is about to exit; keep its handle so shutdown awaits it.
        state.draining.push(handle.task);
        state.cache.remove_everywhere(inference_id);
    }

    /// Sweeper tick: expire TTLs, unload models whose last reference expired,
    /// reap finished drain tasks, and ask every surviving dispatcher to check
    /// that its idle replicas are alive — a death is otherwise only discovered
    /// by a request failing on the pipe.
    fn sweep(&self) {
        let mut state = self.state.lock().unwrap();
        if state.shutting_down {
            return;
        }
        state.draining.retain(|handle| !handle.is_finished());
        // Forget histories nobody has retried within the longest window.
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
    /// own those replicas: the ledger raises a signal because dispatchers, not
    /// it, own workers. Two callers — the sweep tick guarantees delivery on a
    /// quiet server, the predict path makes it prompt on a busy one. A model no
    /// longer in `state.models` gets no message, so the lookup here *is* the
    /// generation guard. See docs/batch-calibration-design.md.
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

    /// Start unloading a model whose last reference is gone: its dispatcher gets
    /// a Shutdown, and the task handle is kept for shutdown to await.
    fn begin_unload(state: &mut ManagerState, inference_id: &str) {
        if let Some(handle) = state.models.remove(inference_id) {
            tracing::debug!(model = %inference_id, "unloading model");
            let _ = handle.tx.send(DispatchMsg::Shutdown);
            state.draining.push(handle.task);
        }
    }

    /// One pass of the load bookkeeping, entirely under the state mutex
    /// (module docs, lock 4): renew the LRU entry and its TTL, run the evictions
    /// that causes, and decide whether this caller is done.
    ///
    /// Called twice per load — once on the fast path, once under the model's
    /// load lock, which is what makes the slow path a double-checked load. Every
    /// step is atomic with the loaded-check that follows: `touch_load` restores
    /// the reference, and the pin stops the sweeper expiring the model before
    /// the enqueue. `take_spawn_pin` is set by the second call only.
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
            // Evicted by its own resize (lru_size <= 0): refuse the load.
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
        // Pin across the spawn so the entry cannot expire mid-load.
        let spawn_pin = take_spawn_pin.then(|| {
            state.cache.pin(inference_id);
            PinGuard::adopt(self, inference_id, None)
        });
        Ok(TouchOutcome::NeedsSpawn(spawn_pin))
    }

    /// Refuse the load while this model is inside its cooldown; `http.rs`
    /// renders the 503. Not consulted for an already-loaded model.
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
            // Never below 1: `Retry-After: 0` invites more hammering.
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

    /// Undo the bookkeeping of a load refused before it ran: a cooling-down
    /// model must not accumulate cache-key references it can never serve.
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

    /// This model's load lock (module docs, lock 2). Built *before* the wait,
    /// so a cancelled caller still tidies the table.
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

    /// One admission permit per **distinct GPU** this replica set will land on
    /// (module docs, lock 3), acquired in sorted key order so two loads
    /// overlapping on two GPUs can never each hold the other's. A replica whose
    /// device key did not resolve counts as landing on **every** GPU — the pin
    /// still reaches the visibility variable, so it does spawn and take memory —
    /// and takes the shared "unresolved" bucket as well as every GPU's permit.
    /// See docs/inferio-worker-protocol.md "Lifecycle and timeouts".
    async fn acquire_load_admission(
        &self,
        inference_id: &str,
        device_keys: &[Option<String>],
    ) -> Vec<OwnedSemaphorePermit> {
        let mut wanted: Vec<&str> = device_keys.iter().flatten().map(String::as_str).collect();
        if device_keys.iter().any(Option::is_none) {
            wanted.push(UNRESOLVED_DEVICE_ADMISSION_KEY);
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
        for gpu in wanted {
            let gate = {
                let mut gates = self.load_admission.lock().unwrap();
                Arc::clone(
                    gates
                        .entry(gpu.to_owned())
                        .or_insert_with(|| Arc::new(Semaphore::new(permits))),
                )
            };
            if gate.available_permits() == 0 {
                tracing::debug!(
                    model = %inference_id,
                    gpu = %gpu,
                    max_concurrent_loads = permits,
                    "waiting for the GPU's load-admission gate"
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

    /// The shared load path, in two phases (module docs, "Lock order").
    ///
    /// **Fast path**: one `state` critical section, which serves an
    /// already-resident model without awaiting a single lock — that is what
    /// makes a predict immune to a load happening elsewhere on the host.
    /// **Slow path**: the shutdown barrier, this model's load lock, the same
    /// bookkeeping again (the second half of the double-checked load), then the
    /// spawn under the device-admission gate.
    ///
    /// With `pin_for_predict` the model is pinned *atomically* with the
    /// loaded-check and the sender comes back with the [`PinGuard`] owning the
    /// pin, so a predict cannot observe its model expiring before the enqueue.
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
        // Refuse a failing model before the queueing, not after the wait.
        if let Err(cooldown) = self.check_load_cooldown(inference_id) {
            self.forget_refused_load(inference_id, cache_key, None);
            return Err(cooldown);
        }

        // Lock order: barrier, model lock, state, then the admission gate.
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
        // Again under the model lock: what we queued behind may be the very
        // load that failed, and a burst of N would still cost N spawns.
        if let Err(cooldown) = self.check_load_cooldown(inference_id) {
            self.forget_refused_load(inference_id, cache_key, Some(spawn_pin));
            return Err(cooldown);
        }

        let spawn_result = self.spawn_model(inference_id).await;
        let mut state = self.state.lock().unwrap();
        // Release the spawn pin under the lock the bookkeeping below takes, so
        // the sweeper cannot expire the fresh entry in between.
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
                // No LRU entry is left behind after a failed load.
                let outcome = state.cache.remove(cache_key, inference_id);
                if let Some(id) = outcome.unload {
                    Self::begin_unload(&mut state, &id);
                }
                // The one place a *load* is known to have failed; the
                // bookkeeping refusals above do not count.
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
            // Unloaded (or shut down) mid-spawn: discard the whole set. The
            // task is parked in `draining`; dropping `admissions` un-charges.
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
        // Seeded here so health() before the dispatcher's first poll already
        // reports the true WorkerSet size.
        let stats = Arc::new(ModelStats::default());
        stats.replicas_total.store(workers.len(), Relaxed);
        stats.replicas_free.store(workers.len(), Relaxed);
        // Take the telemetry handles before the dispatcher takes the workers.
        let telemetry: Vec<TelemetryHandle> = workers.iter().map(Worker::telemetry).collect();
        let context = DispatcherContext {
            inference_id: inference_id.to_owned(),
            generation,
            cost,
            // `default_batch_size` sizes unpriced windows only.
            unpriced_window_items: registry_default_batch.unwrap_or(self.cfg.default_max_batch),
            manager: self.weak.get().cloned().expect("weak self is set in new()"),
            stats: Arc::clone(&stats),
            unload_grace: self.cfg.spawn.deadlines.unload_grace,
        };
        // The dispatcher owns the whole WorkerSet (design §8): every replica
        // serves this FIFO queue but is sized against its own GPU.
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
        // The ladder counts *consecutive* failures, and this model came up.
        state.cooldowns.clear(inference_id);
        drop(state);
        // Lazy warm (design §8), unless the request said prewarm=false or no
        // replica is claim-eligible and it would sit unclaimable forever.
        if prewarm_hint && claim_eligible {
            self.prewarm.lazy_warm(&impl_class);
        }
        Ok(sender)
    }

    /// Spawn + handshake + configure + load the model's whole WorkerSet
    /// (design §8): one worker per entry of the spec's `device_pins`, each
    /// pinned via the backend's device-visibility variable, all spawned and
    /// loaded *concurrently*. Any replica failing kills the others, so a load
    /// yields the complete set or nothing, and the registry is re-resolved at
    /// every spawn (design §4).
    ///
    /// Every replica's pin — including the "no pin" default — resolves against
    /// the probed inventory, normally to a GPU UUID (see
    /// docs/batch-calibration-design.md, "Every worker is pinned to exactly one
    /// GPU"). At most one replica is served from the prewarm pool's parked
    /// worker for the impl class; eligibility is **pin equality**, since a
    /// pooled worker sits on the default GPU.
    async fn spawn_model(&self, inference_id: &str) -> Result<SpawnedModel, LoadFailure> {
        // The registry phase, before any process exists: its failures are config
        // errors, marked as costing no worker so a corrected retry is not
        // refused.
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
        // The same entries in the ledger's vocabulary rather than the backend's.
        let device_keys: Vec<Option<String>> = spec
            .device_pins
            .iter()
            .map(|pin| self.cfg.gpus.resolve_device_key(pin.as_deref()))
            .collect();
        // The device-admission gate (module docs, lock 3) bounds everything from
        // here on, up to and including the multi-second `load`.
        let _admission = self
            .acquire_load_admission(inference_id, &device_keys)
            .await;
        // And its address, when the GPU it names is a unified one whose worker
        // counts GTT as its own.
        let unified_devices: Vec<Option<String>> = spec
            .device_pins
            .iter()
            .map(|pin| self.cfg.gpus.unified_pin_bdf(pin.as_deref()))
            .collect();
        // A prewarmed process predates this model's external inputs.
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
            // Explicit worker env, or no replica on the pool's GPU.
            None => None,
        };
        // Load reservations, charged before any worker is spawned so a window
        // granted to a *different* model during this multi-second load cannot
        // collide with the incoming weights; released when the guards drop, on
        // every exit path including a cancelled future. `dtype` is unknown on a
        // first load, so the ledger reserves at its most conservative tier.
        let mut _load_reservations: Vec<LoadReservation> = Vec::new();
        for gpu in device_keys.iter().flatten() {
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
                let unified = unified_devices[replica].clone();
                async move {
                    let spawn = self.cfg.spawn.for_unified_device(unified.as_deref());
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
                        // A load `error` frame leaves the worker alive.
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
        // Where the model's per-item pixel canvas is settled: the survivors are
        // replicas of one model, so the first figure resolved is it.
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
                    // landed: the GPU identity, the measured base and the pool
                    // size all come from it, and the GPU the *worker* reports is
                    // authoritative. `None` means no admission and the unpriced
                    // path; the key the *pin* named is a diagnostic only.
                    admissions.push(self.ledger.register_worker(
                        inference_id,
                        cost,
                        &worker.telemetry(),
                        device_keys[replica].as_deref(),
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
            // Whole-set atomicity: kill the replicas that came up, un-charge.
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

    /// Bind a claimed prewarmed worker to the concrete model. A [`WorkerError`]
    /// from `configure` is a genuine failure a fresh spawn would reproduce; a
    /// *fatal* error falls back to one fresh `spawn_configured`, so a stale
    /// pooled worker never fails a load.
    async fn configure_claimed(
        &self,
        mut worker: Worker,
        inference_id: &str,
        spec: &SpawnSpec,
        device: Option<String>,
        // The caller's per-replica spawn config, so a respawn after a dead
        // pooled worker gets what a fresh spawn would have.
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

    /// Test hook: a loaded model's generation, for asserting reuse vs respawn.
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

/// The per-item pixel canvas the loaded model is priced against, folded into its
/// cost dimension once, here, where the registry's declaration and the workers'
/// load reports are both in hand. Only for a `pixel`-priced model: the cap is an
/// area.
///
/// **The registry wins**: a declared figure is reviewed, the reported one is an
/// attribute read off an object graph nobody here controls, and a reading that
/// overrode a declaration would make a wrong attribute unfixable from config.
/// The report covers the model whose canvas only a loaded process can see, where
/// the host would otherwise price windows in raw pixels while the worker priced
/// its batches in capped ones.
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

/// `default_batch_size` from registry metadata: group overlaid by id.
/// Non-positive values are treated as absent.
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
    use crate::db::ledger::MAX_ERROR_BYTES;
    use serde_json::json;
    use std::fs;
    use std::path::{Path, PathBuf};

    // Pure state-machine tests (no workers, injected clock).

    fn at(now: DateTime<Local>, seconds: i64) -> DateTime<Local> {
        now + chrono::Duration::seconds(seconds)
    }

    /// Eviction is oldest-first, a repeated load renews the position, and the
    /// evicted model is reported for unload.
    #[test]
    fn lru_evicts_oldest_first() {
        let now = Local::now();
        let mut cache = CacheState::default();
        assert!(cache.touch_load("g/a", "k", 2, -1, now).is_empty());
        assert!(cache.touch_load("g/b", "k", 2, -1, now).is_empty());
        let unloads = cache.touch_load("g/c", "k", 2, -1, now);
        assert_eq!(unloads, vec!["g/a".to_string()], "oldest evicted");
        assert!(!cache.refs_non_empty("g/a"));
        assert!(cache.refs_non_empty("g/b") && cache.refs_non_empty("g/c"));

        // Renewing `b` makes `c` the oldest, so the next insert evicts it.
        cache.touch_load("g/b", "k", 2, -1, now);
        let unloads = cache.touch_load("g/d", "k", 2, -1, now);
        assert_eq!(unloads, vec!["g/c".to_string()], "renewal reorders");
    }

    /// A model unloads only when its last cache-key reference goes, whether
    /// that happens through `remove` or through `clear` of a whole key.
    #[test]
    fn model_unloads_only_when_last_ref_removed() {
        let now = Local::now();
        let mut cache = CacheState::default();
        cache.touch_load("g/a", "k1", 10, -1, now);
        cache.touch_load("g/a", "k2", 10, -1, now);

        let outcome = cache.remove("k1", "g/a");
        assert!(outcome.was_present);
        assert_eq!(outcome.unload, None, "still referenced by k2");
        let outcome = cache.remove("k2", "g/a");
        assert_eq!(outcome.unload, Some("g/a".to_string()), "last ref gone");
        assert!(!cache.refs_non_empty("g/a"));
        // Removing a non-existent entry reports absence.
        assert!(!cache.remove("k2", "g/a").was_present);

        cache.touch_load("g/only", "k1", 10, -1, now);
        cache.touch_load("g/shared", "k1", 10, -1, now);
        cache.touch_load("g/shared", "k2", 10, -1, now);
        let (count, unloads) = cache.clear("k1");
        assert_eq!((count, unloads), (2, vec!["g/only".to_string()]));
        assert!(cache.refs_non_empty("g/shared"), "k2 still references it");
        let (count, unloads) = cache.clear("nope");
        assert_eq!((count, unloads.len()), (0, 0), "unknown key clears nothing");
    }

    /// Expiry removes past finite entries, skips ttl -1, and skips pinned ones.
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

        // Unpinning with a fresh TTL restores the window.
        cache.unpin_restore("g/pinned", "k", 10, at(now, 5));
        assert!(cache.expire(at(now, 6)).is_empty());
        let unloads = cache.expire(at(now, 16));
        assert_eq!(unloads, vec!["g/pinned".to_string()]);
    }

    /// The pin is a refcount: the first of two unpins must not expose the model.
    #[test]
    fn overlapping_pins_do_not_unpin_each_other() {
        let now = Local::now();
        let mut cache = CacheState::default();
        cache.touch_load("g/a", "k", 10, 1, now);
        cache.pin("g/a");
        cache.pin("g/a");

        cache.unpin_restore("g/a", "k", 1, now);
        assert!(
            cache.expire(at(now, 60)).is_empty(),
            "still pinned by the second predict"
        );
        cache.unpin_restore("g/a", "k", 1, at(now, 60));
        assert!(
            cache.expire(at(now, 61)).is_empty(),
            "restored ttl not past"
        );
        assert_eq!(cache.expire(at(now, 62)), vec!["g/a".to_string()]);
    }

    /// Rendering matches the wire's isoformat, `None` is never, and a ttl chrono
    /// cannot represent saturates to Never instead of panicking.
    #[test]
    fn expiration_renders_like_python_isoformat() {
        use chrono::TimeZone;
        let base = Local.with_ymd_and_hms(2026, 7, 5, 12, 34, 56).unwrap();
        assert_eq!(isoformat(&base), "2026-07-05T12:34:56");
        let with_micros = base + chrono::Duration::microseconds(123456);
        assert_eq!(isoformat(&with_micros), "2026-07-05T12:34:56.123456");
        assert_eq!(Expiration::Never.render(), None);

        let now = Local::now();
        assert_eq!(Expiration::new(i64::MAX, now), Expiration::Never);
        assert_eq!(Expiration::new(9_000_000_000_000, now), Expiration::Never);
        assert!(matches!(Expiration::new(60, now), Expiration::At(_)));
        assert_eq!(Expiration::new(-1, now), Expiration::Never);
    }

    /// default_batch_size: group metadata overlaid by id, non-positive absent.
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

    // Integration tests with real worker subprocesses.

    /// Repo root: the crate lives one level below the workspace root.
    fn workspace_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
    }

    /// The managed venv if present, else the legacy root `.venv`.
    fn test_venv_python(root: &Path, rel: &str) -> PathBuf {
        let managed = root.join("python/.venv").join(rel);
        if managed.is_file() {
            managed
        } else {
            root.join(".venv").join(rel)
        }
    }

    /// The same spawn setup as the worker.rs tests.
    fn test_spawn_config() -> WorkerSpawnConfig {
        let root = workspace_root();
        // PANOPTIKON_TEST_PYTHON overrides the repo venv (any python with
        // msgpack works), e.g. running the suite under WSL.
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
            // The fixtures echo `CUDA_VISIBLE_DEVICES`.
            pin_env_var: crate::inferio::gpu::CUDA_PIN_ENV_VAR,
        }
    }

    /// Synthetic registry covering every fixture impl.
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

# Slow *load*: two ids so a test can load two models concurrently.
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

# Dies while idle, a second after load: the liveness-sweep fixture.
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

# Multi-replica WorkerSets; the device pins are env strings the fixture echoes.
[group.device]
config.impl_class = "device_test"
config.devices = ["3", "7"]
metadata.cost.unit = "pixel"
metadata.cost.aggregation = "sum"
metadata.cost.epoch = 4
metadata.cost.seed_units = 1000000
[group.device.inference_ids.test]

# Same fixture with no devices pin: the universal-pinning path.
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

    /// Overrides for [`test_manager_with`]. The defaults are the shipped
    /// policies with an unknown GPU inventory, i.e. no pinning.
    struct ManagerOpts {
        sweep_interval: Duration,
        default_max_batch: u32,
        deadlines: WorkerDeadlines,
        gpus: GpuInventory,
        calibration: Option<Arc<dyn CalibrationProfiles>>,
        loads: LoadPolicy,
    }

    impl Default for ManagerOpts {
        fn default() -> Self {
            Self {
                sweep_interval: Duration::from_secs(60),
                default_max_batch: 32,
                deadlines: WorkerDeadlines::default(),
                gpus: GpuInventory::unknown(),
                calibration: None,
                loads: LoadPolicy::default(),
            }
        }
    }

    fn test_manager(sweep_interval: Duration, default_max_batch: u32) -> TestSetup {
        test_manager_with(ManagerOpts {
            sweep_interval,
            default_max_batch,
            ..Default::default()
        })
    }

    fn test_manager_with(opts: ManagerOpts) -> TestSetup {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("registry.toml"), TEST_REGISTRY_TOML).unwrap();
        let registry = Arc::new(StdMutex::new(RegistryCache::new(RegistryConfig {
            config_dirs: vec![dir.path().to_path_buf()],
        })));
        let cfg = ManagerConfig {
            spawn: WorkerSpawnConfig {
                deadlines: opts.deadlines,
                ..test_spawn_config()
            },
            default_max_batch: opts.default_max_batch,
            sweep_interval: opts.sweep_interval,
            loads: opts.loads,
            // Pool disabled: prewarm.rs has its own suite.
            prewarm: PrewarmConfig {
                enabled: false,
                lazy: false,
                always_warm: Vec::new(),
            },
            gpus: opts.gpus,
            vram: VramBudgets::default(),
            // Usually no store: it would write a profile file.
            calibration: opts.calibration,
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

    /// `load_model` with lru_size 10 and no prewarm hint.
    async fn load(
        manager: &ModelManager,
        inference_id: &str,
        cache_key: &str,
        ttl_seconds: i64,
    ) -> Result<()> {
        manager
            .load_model(inference_id, cache_key, 10, ttl_seconds, None)
            .await
    }

    /// A load onto `keys` must not get a permit within 250 ms.
    async fn admission_blocks(manager: &ModelManager, keys: &[Option<String>], label: &str) {
        assert!(
            tokio::time::timeout(
                Duration::from_millis(250),
                manager.acquire_load_admission("m/blocked", keys),
            )
            .await
            .is_err(),
            "{label}"
        );
    }

    /// `predict` with the arguments these tests never vary: lru_size 10, no
    /// prewarm hint, one data input.
    async fn predict_one(
        manager: &ModelManager,
        inference_id: &str,
        cache_key: &str,
        ttl_seconds: i64,
        max_batch: Option<u32>,
        value: serde_json::Value,
    ) -> Result<Vec<WorkerOutput>> {
        manager
            .predict(
                inference_id,
                cache_key,
                10,
                ttl_seconds,
                max_batch,
                None,
                vec![data_input(value)],
            )
            .await
    }

    /// Batch size reported by a batchsize_test output.
    fn reported_batch(output: &WorkerOutput) -> u64 {
        match output {
            WorkerOutput::Json(value) => value["batch"].as_u64().expect("batch field"),
            other => panic!("unexpected output {other:?}"),
        }
    }

    #[tokio::test]
    async fn declared_external_input_is_resolved_and_passed_at_worker_spawn() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = &setup.manager;
        assert!(
            load(manager, "externalenv/test", "key", 60).await.is_err(),
            "missing required input prevents worker creation"
        );

        unsafe { std::env::set_var("INFERIO_MANAGER_EXTERNAL_INPUT_XYZ", "latest-value") };
        let output = predict_one(manager, "externalenv/test", "key", 60, None, json!(null)).await;
        unsafe { std::env::remove_var("INFERIO_MANAGER_EXTERNAL_INPUT_XYZ") };
        assert_eq!(
            output.expect("worker receives current input"),
            vec![WorkerOutput::Json(
                json!({"external_input": "latest-value"})
            )]
        );
        manager.shutdown().await;
    }

    /// predict auto-loads the model; losing one of two cache keys keeps that
    /// worker, and losing the last unloads it.
    #[tokio::test]
    async fn cache_key_refcount_governs_unload() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = &setup.manager;

        load(manager, "echo/test", "a", -1)
            .await
            .expect("load via a");
        load(manager, "echo/test", "b", -1)
            .await
            .expect("load via b");
        let generation = manager.loaded_generation("echo/test").expect("loaded");

        assert!(manager.unload_model("a", "echo/test").await.unwrap());
        assert_eq!(
            manager.cached_models(),
            BTreeMap::from([("echo/test".to_string(), vec!["b".to_string()])]),
            "still referenced by b"
        );
        let outputs = predict_one(manager, "echo/test", "b", -1, None, json!("x"))
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

        let outputs = predict_one(manager, "echo/test", "b", -1, None, json!(1))
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

    /// End to end over the whole cache lifecycle on the echo fixture:
    /// `lru_size = 1` evicts and unloads the previous model under the same key,
    /// and a ttl=1s model is swept away while a ttl=-1 one loaded alongside it
    /// survives.
    #[tokio::test]
    async fn ttl_expiry_unloads_but_never_survives() {
        let setup = test_manager(Duration::from_millis(200), 32);
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
            "lru_size 1: the first model was evicted and unloaded"
        );
        assert_eq!(manager.loaded_generation("echo/test"), None);
        assert!(manager.loaded_generation("echo/second").is_some());

        load(manager, "echo/second", "k", -1)
            .await
            .expect("load never");
        load(manager, "echo/test", "k", 1)
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

    /// A predict outlives its own TTL, then expires on the restored one.
    #[tokio::test]
    async fn predict_pins_model_against_expiry() {
        let setup = test_manager(Duration::from_millis(100), 32);
        let manager = &setup.manager;

        let outputs = predict_one(manager, "slow/test", "k", 1, None, json!(null))
            .await
            .expect("predict outlives its ttl thanks to the pin");
        assert_eq!(outputs, vec![WorkerOutput::Json(json!({"slow": true}))]);

        // Still cached, with a restored *finite* expiration.
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

    /// Requests queued behind a busy worker merge into one window, under the cap.
    #[tokio::test]
    async fn concurrent_predicts_merge_into_batches() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = setup.manager.clone();

        load(&manager, "batch/test", "k", -1).await.expect("load");

        let first = {
            let manager = manager.clone();
            tokio::spawn(async move {
                predict_one(&manager, "batch/test", "k", -1, None, json!(0)).await
            })
        };
        // Let the first request dispatch alone (worker sleeps 300ms).
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut rest = Vec::new();
        for i in 0..5 {
            let manager = manager.clone();
            rest.push(tokio::spawn(async move {
                predict_one(&manager, "batch/test", "k", -1, None, json!(i)).await
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

    /// A window a worker rejects falls back to per-request prediction.
    #[tokio::test]
    async fn merged_batch_failure_falls_back_to_per_request() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = setup.manager.clone();

        load(&manager, "failbatch/test", "k", -1)
            .await
            .expect("load");

        let first = {
            let manager = manager.clone();
            tokio::spawn(async move {
                predict_one(&manager, "failbatch/test", "k", -1, None, json!(0)).await
            })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut rest = Vec::new();
        for i in 0..3 {
            let manager = manager.clone();
            rest.push(tokio::spawn(async move {
                predict_one(&manager, "failbatch/test", "k", -1, None, json!(i)).await
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

    /// A replica that dies while **idle** is found by the sweeper's `ReapIdle`
    /// tick — nothing reads an idle worker's pipe, so no request ever sees the
    /// EOF — and then takes the normal death route.
    #[tokio::test]
    async fn an_idle_replica_that_dies_is_found_by_the_liveness_sweep() {
        let setup = test_manager(Duration::from_millis(100), 32);
        let manager = &setup.manager;

        // Resident at ttl -1, so a disappearance is the sweep and not expiry.
        let outputs = predict_one(manager, "idledeath/test", "k", -1, None, json!(1))
            .await
            .expect("the fixture serves normally before it dies");
        assert_eq!(outputs.len(), 1);
        let health = manager.health();
        assert_eq!(health.model_count, 1, "the model is resident");
        assert_eq!(
            health.models[0].replicas.total, 1,
            "its replica is advertised"
        );

        // Poll: without the reap tick this never happens, so the test fails on
        // the bound rather than flaking on timing.
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

    /// A worker that dies mid-predict drops the model everywhere and respawns.
    #[tokio::test]
    async fn worker_death_cleans_up_and_next_predict_respawns() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = &setup.manager;

        let err = predict_one(manager, "dying/test", "k", -1, None, json!(1))
            .await
            .expect_err("worker exits mid-predict");
        assert!(
            format!("{err:#}").contains("predict"),
            "error surfaces the failed predict: {err:#}"
        );

        // Death cleanup runs in the dispatcher right after the reply.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            manager.cached_models().is_empty(),
            "dead model dropped from all caches"
        );
        assert_eq!(manager.loaded_generation("dying/test"), None);

        // The next predict spawns a fresh worker, which also dies — but a
        // *predict* failure proves a new process served it.
        let err = predict_one(manager, "dying/test", "k", -1, None, json!(2))
            .await
            .expect_err("fresh worker also dies");
        assert!(
            format!("{err:#}").contains("predict request failed"),
            "a fresh worker was spawned and failed the same way: {err:#}"
        );

        manager.shutdown().await;
    }

    /// Shutdown flushes a calibration update still inside the write debounce.
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
        let setup = test_manager_with(ManagerOpts {
            calibration: Some(Arc::clone(&store) as Arc<dyn CalibrationProfiles>),
            ..Default::default()
        });
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

        // The first update lands on its own and primes the debounce.
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

    /// A fresh manager reports "ok" and no models; shutdown unloads gracefully,
    /// empties the cache, flips both status fields and refuses new work.
    #[tokio::test]
    async fn shutdown_unloads_workers_and_refuses_new_requests() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = &setup.manager;

        let health = manager.health();
        assert_eq!(health.status, "ok");
        assert!(!health.shutting_down);
        assert!(health.registry_ok, "the temp registry TOML parses");
        assert_eq!(health.model_count, 0);
        assert!(health.models.is_empty());

        load(manager, "echo/test", "k", -1).await.expect("load");
        manager.shutdown().await;

        assert!(manager.cached_models().is_empty());
        let err = load(manager, "echo/test", "k", -1)
            .await
            .expect_err("loads refused after shutdown");
        assert!(format!("{err:#}").contains("shutting down"));
        let err = predict_one(manager, "echo/test", "k", -1, None, json!(1))
            .await
            .expect_err("predicts refused after shutdown");
        assert!(format!("{err:#}").contains("shutting down"));
    }

    /// An unconvertible output is a per-request error, not a fatal one.
    #[tokio::test]
    async fn unconvertible_output_fails_one_request_but_worker_survives() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = &setup.manager;

        let outputs = predict_one(manager, "nan/test", "k", -1, None, json!("ok"))
            .await
            .expect("normal predict");
        assert_eq!(outputs, vec![WorkerOutput::Json(json!({"ok": true}))]);
        let generation = manager.loaded_generation("nan/test").expect("loaded");

        let err = predict_one(manager, "nan/test", "k", -1, None, json!("nan"))
            .await
            .expect_err("NaN output has no JSON form");
        assert!(
            format!("{err:#}").contains("not representable as JSON"),
            "error names the unconvertible output: {err:#}"
        );

        // A beat to prove death cleanup does *not* run.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            manager.loaded_generation("nan/test"),
            Some(generation),
            "same worker: the conversion failure must not kill it"
        );

        let outputs = predict_one(manager, "nan/test", "k", -1, None, json!("ok"))
            .await
            .expect("worker still serves after the failed request");
        assert_eq!(outputs, vec![WorkerOutput::Json(json!({"ok": true}))]);

        manager.shutdown().await;
    }

    /// A predict dropped mid-flight releases its pin through [`PinGuard`]'s
    /// Drop, so the model still expires; with a leaked pin it never would.
    #[tokio::test]
    async fn aborted_predict_releases_pin_and_model_still_expires() {
        let setup = test_manager(Duration::from_millis(100), 32);
        let manager = setup.manager.clone();

        // Load first so the abort lands mid-predict, not mid-spawn.
        load(&manager, "slow/test", "k", 1).await.expect("load");

        let task = {
            let manager = manager.clone();
            tokio::spawn(async move {
                predict_one(&manager, "slow/test", "k", 1, None, json!(null)).await
            })
        };
        // Let the predict enqueue and pin, then drop it mid-flight.
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

    /// A worker wedged in predict never needs a manual kill: predict has no
    /// deadline, so the bound sits on the unload drain — shutdown gives the
    /// in-flight window `unload_grace` and then kills it.
    #[tokio::test]
    async fn shutdown_kills_worker_wedged_in_predict() {
        let deadlines = WorkerDeadlines {
            unload_grace: Duration::from_secs(1),
            ..WorkerDeadlines::default()
        };
        let setup = test_manager_with(ManagerOpts {
            deadlines,
            ..Default::default()
        });
        let manager = setup.manager.clone();

        // Load first so shutdown lands mid-predict, not mid-spawn.
        load(&manager, "hang/test", "k", 60).await.expect("load");

        let task = {
            let manager = manager.clone();
            tokio::spawn(async move {
                predict_one(&manager, "hang/test", "k", 60, None, json!(null)).await
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

    /// Two GPUs (indices 0 and 3) of equal capability, so index 0 is default.
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

    /// The two replicas of `device/test` serve one shared FIFO queue and both
    /// answer, and each one's `CUDA_VISIBLE_DEVICES` says how its registry pin
    /// resolved: with no inventory the raw index strings reach the child
    /// unchanged, and with one, index 3 becomes its GPU's UUID while the
    /// invisible index 7 passes through rather than being guessed at. A replica
    /// the registry left unpinned lands on the default GPU, also by UUID.
    #[tokio::test]
    async fn index_device_pins_map_to_gpu_uuids() {
        for (label, gpus, expected) in [
            ("no inventory", GpuInventory::unknown(), ["3", "7"]),
            ("known inventory", test_gpus(), ["GPU-3333", "7"]),
        ] {
            let setup = test_manager_with(ManagerOpts {
                gpus,
                ..Default::default()
            });
            let manager = setup.manager.clone();
            load(&manager, "device/test", "k", -1)
                .await
                .expect("load spawns both replicas");

            // 4 singles against 0.5s predicts: two occupy both replicas, two
            // queue and go to whichever frees first.
            let mut tasks = Vec::new();
            for i in 0..4 {
                let manager = manager.clone();
                tasks.push(tokio::spawn(async move {
                    predict_one(&manager, "device/test", "k", -1, Some(1), json!(i)).await
                }));
            }
            let mut devices = std::collections::BTreeSet::new();
            for task in tasks {
                let outputs = task.await.unwrap().expect("predict on a pinned replica");
                devices.insert(reported_device(&outputs[0]));
            }
            assert_eq!(
                devices,
                expected.map(str::to_owned).into_iter().collect(),
                "{label}: both pins served, and no third replica exists"
            );
            manager.shutdown().await;
        }

        let setup = test_manager_with(ManagerOpts {
            gpus: test_gpus(),
            ..Default::default()
        });
        let manager = &setup.manager;
        let outputs = predict_one(manager, "devplain/test", "k", -1, None, json!(1))
            .await
            .expect("predict on the unpinned replica");
        assert_eq!(
            reported_device(&outputs[0]),
            "GPU-0000",
            "an unpinned replica resolves to the default GPU's UUID"
        );
        manager.shutdown().await;
    }

    /// A store that answers nothing and records which *GPU* each question was
    /// keyed by. `expected_base_mb` is reached only once `reserve_load` has
    /// found the GPU in its map, so a recorded name proves the reservation
    /// resolved.
    #[derive(Default)]
    struct RecordingProfiles {
        reservation_gpus: StdMutex<Vec<String>>,
    }

    impl CalibrationProfiles for RecordingProfiles {
        fn expected_base_mb(&self, query: &ProfileQuery<'_>) -> Option<u64> {
            self.reservation_gpus
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

    /// Canvas precedence: the registry's declaration beats the worker's report,
    /// the report fills in when the registry states nothing, and neither means
    /// uncapped.
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
        // An area prices nothing outside pixel pricing.
        let tokens = CostDimension {
            unit: CostUnit::Token,
            ..pixels(Some(1_835_008))
        };
        assert_eq!(
            canvas_in_force("clip/tokens", tokens, Some(11_289_600)).canvas_pixels,
            None
        );
    }

    /// A ROCm-shaped inventory whose row indices are the registry's own pins,
    /// so the pin and ledger-key vocabularies are guaranteed to differ.
    fn rocm_test_gpus() -> GpuInventory {
        let gpu = |index: u32, bdf: &str| GpuInfo {
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
        GpuInventory::known_rocm(vec![gpu(3, "0000:03:00.0"), gpu(7, "0000:0c:00.0")])
    }

    /// The load reservation is keyed by the device key, never by the resolved
    /// pin: on ROCm the two are different strings, and keying by the pin misses
    /// the ledger's GPU map entirely. The recording store is the probe.
    #[tokio::test]
    async fn load_reservations_are_keyed_by_gpu_not_by_the_hip_pin() {
        let profiles = Arc::new(RecordingProfiles::default());
        let setup = test_manager_with(ManagerOpts {
            gpus: rocm_test_gpus(),
            calibration: Some(Arc::clone(&profiles) as Arc<dyn CalibrationProfiles>),
            ..Default::default()
        });
        let manager = &setup.manager;

        load(manager, "device/test", "k", -1)
            .await
            .expect("load spawns both pinned replicas");

        let mut gpus = profiles.reservation_gpus.lock().unwrap().clone();
        gpus.sort();
        assert_eq!(
            gpus,
            vec!["AMD gfx1100 #3".to_string(), "AMD gfx1100 #7".to_string()],
            "both replicas reserved against the GPU their index pin names; \
             keying by the pin string (\"3\", \"7\") would have found no GPU \
             at all and reserved nothing"
        );

        manager.shutdown().await;
    }

    /// `/health` carries the resolved cost dimension per model, the GPU
    /// inventory, and one telemetry row per replica with its pin. The memory
    /// fields stay null: the fixtures have no torch, which the wire tolerates.
    #[tokio::test]
    async fn health_reports_cost_dimension_gpus_and_replica_pins() {
        let setup = test_manager_with(ManagerOpts {
            gpus: test_gpus(),
            ..Default::default()
        });
        let manager = &setup.manager;

        load(manager, "device/test", "k", -1).await.expect("load");
        load(manager, "echo/test", "k", -1).await.expect("load");

        let health = manager.health();
        assert_eq!(
            health
                .gpus
                .iter()
                .map(|gpu| gpu.uuid.as_str())
                .collect::<Vec<_>>(),
            vec!["GPU-0000", "GPU-3333"],
            "the inventory is reported by GPU UUID"
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
                    // Absent too: only a worker with a live CUDA device can
                    // name its GPU, and the spawn pin is not copied into it.
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

    /// Every batch lands in the telemetry ring even with no torch: the harness
    /// times the call and counts its inputs. Two predicts, two retained samples
    /// — the cost fit needs the set, not the last one.
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
        predict_one(manager, "echo/test", "k", -1, None, json!(3))
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

    /// Replicas run windows concurrently: 4 uncapped-merge predicts of 1.5 s on
    /// 2 replicas are two rounds (~3 s), where a serialized set would need ~6 s.
    #[tokio::test]
    async fn multi_replica_predicts_run_concurrently() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = setup.manager.clone();

        load(&manager, "slowpair/test", "k", -1)
            .await
            .expect("load");

        let started = std::time::Instant::now();
        let mut tasks = Vec::new();
        for i in 0..4 {
            let manager = manager.clone();
            tasks.push(tokio::spawn(async move {
                predict_one(&manager, "slowpair/test", "k", -1, Some(1), json!(i)).await
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

    /// ANY replica dying fatally kills the whole model: every outstanding
    /// request errors, the model vanishes from all caches, and the next predict
    /// auto-loads a fresh set that serves normally.
    #[tokio::test]
    async fn replica_death_kills_whole_set_and_next_predict_respawns() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = setup.manager.clone();

        load(&manager, "dieflag/test", "k", -1)
            .await
            .expect("load spawns both replicas");
        let generation = manager.loaded_generation("dieflag/test").expect("loaded");

        // The poison request dispatches first and holds replica A for 200 ms;
        // the rest land on replica B and the queue, still outstanding at the death.
        let die = {
            let manager = manager.clone();
            tokio::spawn(async move {
                predict_one(
                    &manager,
                    "dieflag/test",
                    "k",
                    -1,
                    Some(1),
                    json!({"die": true}),
                )
                .await
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut normals = Vec::new();
        for i in 0..3 {
            let manager = manager.clone();
            normals.push(tokio::spawn(async move {
                predict_one(&manager, "dieflag/test", "k", -1, Some(1), json!(i)).await
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
        let outputs = predict_one(&manager, "dieflag/test", "k", -1, Some(1), json!("ok"))
            .await
            .expect("fresh worker set serves after the death");
        assert_eq!(outputs, vec![WorkerOutput::Json(json!({"echo": "ok"}))]);
        assert!(
            manager.loaded_generation("dieflag/test").expect("loaded") > generation,
            "the respawned set has a new generation"
        );

        manager.shutdown().await;
    }

    /// Unloading a multi-replica model removes it as one unit and gracefully
    /// stops both replicas; a re-load spawns a fresh set.
    #[tokio::test]
    async fn unload_tears_down_whole_replica_set() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = &setup.manager;

        load(manager, "device/test", "k", -1)
            .await
            .expect("load spawns both replicas");
        let generation = manager.loaded_generation("device/test").expect("loaded");

        assert!(manager.unload_model("k", "device/test").await.unwrap());
        assert!(
            manager.cached_models().is_empty(),
            "the set unloads as one unit"
        );
        assert_eq!(manager.loaded_generation("device/test"), None);

        load(manager, "device/test", "k", -1)
            .await
            .expect("re-load spawns a fresh set");
        assert!(
            manager.loaded_generation("device/test").expect("loaded") > generation,
            "fresh generation: no worker from the unloaded set survived"
        );

        // shutdown() awaits the drained task too: no hang is the assertion.
        manager.shutdown().await;
    }

    // GET /health snapshots.

    /// One slow predict, watched from both sides. While it is outstanding health
    /// shows it as an in-flight window, a busy replica or a queued request — any
    /// of the three, since the sample can land before dispatch. Once it
    /// completes, the same snapshot shows the loaded model with its cache key, a
    /// fully free single-replica set, an empty queue and the counters the one
    /// request produced. The replica returns to the free pool only when the
    /// dispatcher reaps the window, so both phases are polled.
    #[tokio::test]
    async fn health_reports_loaded_model_after_predict() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = setup.manager.clone();

        // Load first: the observation window is the predict, not the spawn.
        load(&manager, "slow/test", "key", -1).await.expect("load");
        let task = {
            let manager = manager.clone();
            tokio::spawn(async move {
                predict_one(&manager, "slow/test", "key", -1, None, json!(null)).await
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

        let model = loop {
            let health = manager.health();
            assert_eq!(health.status, "ok");
            assert_eq!(health.model_count, 1);
            let model = health.models.into_iter().next().expect("one model");
            assert_eq!(model.inference_id, "slow/test");
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
        assert_eq!(model.replicas.total, 1, "a single-replica set");
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

    /// The user cap bounds every window and is observable in `/health`: with
    /// every request at max_batch=2 no worker saw a larger batch and no window
    /// held more than 2 inputs, and six singles need at least three windows.
    #[tokio::test]
    async fn health_reports_the_capped_window_size() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = setup.manager.clone();

        load(&manager, "batch/test", "k", -1).await.expect("load");

        let mut tasks = Vec::new();
        for i in 0..6 {
            let manager = manager.clone();
            tasks.push(tokio::spawn(async move {
                predict_one(&manager, "batch/test", "k", -1, Some(2), json!(i)).await
            }));
        }
        for task in tasks {
            let outputs = task.await.unwrap().expect("capped predict");
            let batch = reported_batch(&outputs[0]);
            assert!(batch <= 2, "batch {batch} exceeds the explicit cap of 2");
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

    // Per-model load locks and the device-admission gate.

    /// A load of one model must not delay a predict to a *different*, already
    /// resident one. Two assertions: the wall-clock bound, and that the slow
    /// load was still in flight when the predict came back — a fast machine
    /// cannot satisfy the second.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_slow_load_does_not_delay_predicts_to_a_resident_model() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = Arc::clone(&setup.manager);
        predict_one(&manager, "echo/test", "k", -1, None, json!(1))
            .await
            .expect("the resident model loads");

        let loader = {
            let manager = Arc::clone(&manager);
            tokio::spawn(async move { load(&manager, "slowload/test", "k2", -1).await })
        };
        // Past the bookkeeping and into the fixture's 3 s `load()`.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            !loader.is_finished(),
            "the slow-load fixture sleeps 3 s in load(); it cannot be done yet"
        );

        let started = Instant::now();
        predict_one(&manager, "echo/test", "k", -1, None, json!(2))
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

    /// The gate is keyed by **GPU** and is `max_concurrent_loads` permits wide,
    /// and a replica whose device key did not resolve takes the shared bucket
    /// *and* every GPU's permit: the pin still reaches the visibility variable,
    /// so the worker lands on some GPU the ledger cannot name, and letting it
    /// stream its weights beside an unpinned load onto that GPU is exactly the
    /// collision the retired host-wide lock did prevent.
    ///
    /// Asserted on the gate itself rather than through two real loads: the
    /// alternative is a wall-clock race whose failure mode is a flaky test.
    #[tokio::test]
    async fn the_load_admission_gate_is_per_gpu() {
        let gpu_a = [Some("GPU-0000".to_owned())];
        let gpu_b = [Some("GPU-3333".to_owned())];
        let unresolved = [None];
        let one_load = LoadPolicy {
            max_concurrent_loads: 1,
            ..LoadPolicy::default()
        };
        // One GPU's permit blocks only that GPU.
        let setup = test_manager_with(ManagerOpts {
            loads: one_load,
            ..Default::default()
        });
        let manager = Arc::clone(&setup.manager);
        let held = manager.acquire_load_admission("m/a", &gpu_a).await;
        assert_eq!(held.len(), 1, "one distinct GPU, one permit");
        let other = tokio::time::timeout(
            Duration::from_millis(250),
            manager.acquire_load_admission("m/b", &gpu_b),
        )
        .await
        .expect("a load onto another GPU must not wait for this one");
        let none_gpu = tokio::time::timeout(
            Duration::from_millis(250),
            manager.acquire_load_admission("m/c", &unresolved),
        )
        .await
        .expect("the unresolved bucket is its own gate");
        admission_blocks(&manager, &gpu_a, "a second load onto GPU A must wait").await;
        drop((held, other, none_gpu));

        // A multi-GPU set takes one permit per distinct GPU, deduped, and
        // `max_concurrent_loads = 2` admits a second load onto both.
        let setup = test_manager_with(ManagerOpts {
            loads: LoadPolicy {
                max_concurrent_loads: 2,
                ..LoadPolicy::default()
            },
            ..Default::default()
        });
        let manager = Arc::clone(&setup.manager);
        let spread = [
            Some("GPU-3333".to_owned()),
            Some("GPU-0000".to_owned()),
            Some("GPU-0000".to_owned()),
        ];
        let held = manager.acquire_load_admission("m/e", &spread).await;
        assert_eq!(held.len(), 2, "two distinct GPUs out of three replicas");
        tokio::time::timeout(
            Duration::from_millis(250),
            manager.acquire_load_admission("m/f", &spread),
        )
        .await
        .expect("two permits per GPU admit a second load onto both");

        // With an inventory, an unresolved key blocks every GPU and vice versa.
        let setup = test_manager_with(ManagerOpts {
            gpus: test_gpus(),
            loads: one_load,
            ..Default::default()
        });
        let manager = Arc::clone(&setup.manager);
        let held = manager.acquire_load_admission("m/a", &unresolved).await;
        assert_eq!(held.len(), 3, "the shared bucket plus both GPUs");
        admission_blocks(&manager, &gpu_a, "an unresolved load must block GPU A").await;
        admission_blocks(&manager, &gpu_b, "an unresolved load must block GPU B").await;
        drop(held);
        let held = manager.acquire_load_admission("m/c", &gpu_a).await;
        assert_eq!(held.len(), 1);
        admission_blocks(
            &manager,
            &unresolved,
            "a known GPU must block an unresolved load",
        )
        .await;
        drop(held);

        // With no inventory the shared bucket alone is the serialization.
        let setup = test_manager_with(ManagerOpts {
            loads: one_load,
            ..Default::default()
        });
        let manager = Arc::clone(&setup.manager);
        let held = manager.acquire_load_admission("m/e", &unresolved).await;
        assert_eq!(held.len(), 1, "no inventory: just the shared bucket");
        admission_blocks(
            &manager,
            &unresolved,
            "a GPU-less host still serializes every load",
        )
        .await;
        drop(held);
    }

    /// Racing callers produce one worker set, not two, and the lock table is
    /// empty again afterwards.
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_predicts_load_one_model_once() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = Arc::clone(&setup.manager);
        let started = Instant::now();
        let mut tasks = Vec::new();
        for i in 0..3 {
            let manager = Arc::clone(&manager);
            tasks.push(tokio::spawn(async move {
                predict_one(&manager, "slowload/test", "k", -1, None, json!(i)).await
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

    // Per-model load-failure cooldown.

    /// The ladder on the injected clock, in one pass over the rules it obeys:
    /// `base × 2^(n−1)` capped at `max`; the cap is a floor once reached (the
    /// regression is an overflowing `1u32 << 32`, which dropped the 33rd
    /// consecutive failure straight back to the base window); a configured
    /// value too large to represent is clamped rather than overflowing the
    /// `Instant + Duration` deadlines, which run under the state mutex where a
    /// panic would poison the manager; a successful load clears the history and
    /// a zero base disables the cooldown outright; and a history nobody has
    /// retried within window + cap is forgotten, so the table cannot grow
    /// unbounded on ids that come off the URL.
    #[test]
    fn cooldown_windows_double_and_cap() {
        let policy = LoadPolicy::default();
        let mut cooldowns = LoadCooldowns::default();
        let now = Instant::now();

        // The shipped ladder: nine failures and 8.5 minutes to the cap, and
        // well past both the cap and the 32-bit shift width it never falls off.
        let mut windows = Vec::new();
        for failure in 1..=64u32 {
            let window = cooldowns
                .note_failure("g/a", "boom", &policy, now)
                .expect("cooldowns are enabled");
            if failure <= 12 {
                windows.push(window.as_secs());
            }
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
        assert_eq!(
            windows,
            vec![2, 4, 8, 16, 32, 64, 128, 256, 300, 300, 300, 300]
        );
        assert_eq!(cooldowns.entries["g/a"].failures, 64);
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

        // A successful load clears the ladder; the next failure starts over.
        cooldowns.clear("g/a");
        assert!(cooldowns.active("g/a", now).is_none());
        assert_eq!(
            cooldowns.note_failure("g/a", "boom", &policy, now),
            Some(Duration::from_secs(2))
        );

        // The history survives window + cap, and is forgotten past it.
        cooldowns.prune(&policy, now + Duration::from_secs(120));
        assert_eq!(cooldowns.entries.len(), 1, "still inside window + cap");
        cooldowns.prune(&policy, now + Duration::from_secs(303));
        assert!(cooldowns.entries.is_empty(), "the ladder resets");

        // A zero base records nothing at all.
        let off = LoadPolicy {
            cooldown_base: Duration::ZERO,
            ..LoadPolicy::default()
        };
        let mut cooldowns = LoadCooldowns::default();
        assert_eq!(cooldowns.note_failure("g/a", "boom", &off, now), None);
        assert!(cooldowns.active("g/a", now).is_none());

        // An absurd configured value is clamped, and nothing overflows.
        let local = crate::config::InferenceLocalConfig {
            load_failure_cooldown_secs: u64::MAX,
            load_failure_cooldown_max_secs: u64::MAX,
            ..Default::default()
        };
        let policy = LoadPolicy::from(&local);
        assert_eq!(policy.cooldown_base.as_secs(), MAX_COOLDOWN_SECS);
        assert_eq!(policy.cooldown_max.as_secs(), MAX_COOLDOWN_SECS);
        let mut cooldowns = LoadCooldowns::default();
        let window = cooldowns
            .note_failure("g/a", "boom", &policy, now)
            .expect("cooldowns are enabled");
        assert_eq!(window.as_secs(), MAX_COOLDOWN_SECS);
        assert!(cooldowns.active("g/a", now).is_some());
        cooldowns.prune(&policy, now);
        assert_eq!(cooldowns.entries.len(), 1, "nowhere near forgettable yet");
    }

    /// End to end on a model that cannot load: the first request pays a real
    /// spawn, the second is refused without one, `/health` says so, and the
    /// ladder escalates once the window passes.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_load_puts_the_model_in_a_cooldown() {
        let setup = test_manager_with(ManagerOpts {
            loads: LoadPolicy {
                max_concurrent_loads: 1,
                // Short enough to watch, long enough that the refusal is not a
                // race.
                cooldown_base: Duration::from_millis(600),
                cooldown_max: Duration::from_secs(300),
            },
            ..Default::default()
        });
        let manager = Arc::clone(&setup.manager);
        let predict = |manager: Arc<ModelManager>| async move {
            predict_one(&manager, "missing/test", "k", -1, None, json!(1))
                .await
                .expect_err("this model has no impl class")
        };

        let attempt_started = Instant::now();
        let first = predict(Arc::clone(&manager)).await;
        let spawn_attempt = attempt_started.elapsed();
        assert!(
            format!("{first:#}").contains("failed to load model")
                && format!("{first:#}").contains("does_not_exist"),
            "the first request pays a real load attempt, and the worker's own \
             message survives: {first:#}"
        );
        assert!(
            manager.cached_models().is_empty() && manager.cache_expirations("k").is_empty(),
            "a failed load leaves neither a cache entry nor an LRU entry"
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

        // Past the window one attempt gets through, and doubles the wait.
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

    /// The wire contract the job side matches on: the kind token, an RFC 3339
    /// `retry_at`, and a `Retry-After` that is never 0.
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
        let long = "x".repeat(MAX_ERROR_BYTES * 2);
        let clamped = truncate_error(&long);
        assert_eq!(clamped.chars().count(), MAX_ERROR_BYTES + 1);
        assert!(clamped.ends_with('…'));
        assert_eq!(truncate_error("short"), "short");
    }
}
