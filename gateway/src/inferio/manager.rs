//! Model manager: exact port of `src/inferio/manager.py` semantics (design
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
//! Locking: Python holds one manager-wide lock for the entire `load_model`
//! call, including the slow process spawn. Here the slow phase (spawn +
//! `load`) is serialized by a dedicated async `load_lock` — same observable
//! ordering (loads are serialized; nothing else observes intermediate
//! state), but the bookkeeping mutex is never held across await so sync
//! accessors (`cached_models`, `cache_expirations`) stay cheap. Models are
//! additionally pinned while they spawn so the sweeper cannot expire an
//! entry mid-load (a real race in Python, whose sweeper uses a *different*
//! lock than `load_model`).
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
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Local, Timelike};
use hashlink::LinkedHashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex as TokioMutex, mpsc, oneshot};
use tokio::task::JoinHandle;

use super::dispatch::{DispatchMsg, DispatchRequest, DispatcherContext, ModelStats, run_dispatcher};
use super::prewarm::{PrewarmConfig, PrewarmHealth, PrewarmPool};
use super::registry::{Registry, RegistryCache, SpawnSpec};
use super::worker::{Worker, WorkerError, WorkerInput, WorkerOutput, WorkerSpawnConfig};

/// Manager configuration.
pub struct ManagerConfig {
    /// How worker processes are spawned (python, impl dirs, env, deadlines).
    pub spawn: WorkerSpawnConfig,
    /// Server-wide default batch cap, applied when neither the drain window
    /// nor the registry expresses an opinion (replaces `MAX_COMBINED_BATCH`).
    pub default_max_batch: u32,
    /// TTL sweeper period (Python: 10 s).
    pub sweep_interval: Duration,
    /// Prewarm pool policy (design §8; `[inference_local.prewarm]`).
    pub prewarm: PrewarmConfig,
}

/// `GET /health` response (design §7, additive — Python has no such
/// endpoint, so this shape is ours to define). Serialized as-is by the HTTP
/// layer; `Deserialize` exists so tests can round-trip the wire shape.
#[derive(Debug, Serialize, Deserialize)]
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
}

/// One loaded model in the [`HealthReport`].
#[derive(Debug, Serialize, Deserialize)]
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
    /// Effective cap of the most recently dispatched window (design §6);
    /// `null` until the first window dispatches.
    pub last_effective_cap: Option<u32>,
    /// Predict requests ever queued on this model's dispatcher.
    pub total_predict_requests: u64,
    /// Windows ever dispatched to a replica.
    pub total_batches: u64,
}

/// Replica occupancy of one model's WorkerSet.
#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicaHealth {
    pub total: usize,
    pub free: usize,
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
        match chrono::Duration::try_seconds(ttl_seconds)
            .and_then(|ttl| now.checked_add_signed(ttl))
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
                if let Expiration::At(at) = expiration {
                    if now > *at {
                        expired.push((cache_key.clone(), inference_id.clone()));
                    }
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
        if let Some(lru) = self.lru_caches.get_mut(cache_key) {
            if let Some(expiration) = lru.get_mut(inference_id) {
                *expiration = Expiration::new(ttl_seconds, now);
            }
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
            manager: manager.weak.get().cloned().expect("weak self is set in new()"),
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

/// The model manager. Construct with [`ModelManager::new`] (requires a
/// running tokio runtime — it spawns the sweeper task).
pub struct ModelManager {
    cfg: ManagerConfig,
    registry: Arc<StdMutex<RegistryCache>>,
    state: StdMutex<ManagerState>,
    /// Prewarm pool (design §8): one parked warm worker per impl class.
    /// Its own mutex, never held together with `state`.
    prewarm: Arc<PrewarmPool>,
    /// Serializes the slow spawn+load phase, mirroring Python's manager-wide
    /// lock held for the whole `load_model` (see module docs).
    load_lock: TokioMutex<()>,
    /// Self-reference handed to dispatcher tasks for death cleanup.
    weak: OnceLock<Weak<ModelManager>>,
    sweeper: StdMutex<Option<JoinHandle<()>>>,
}

impl ModelManager {
    pub fn new(cfg: ManagerConfig, registry: Arc<StdMutex<RegistryCache>>) -> Arc<Self> {
        let sweep_interval = cfg.sweep_interval;
        let prewarm = PrewarmPool::new(cfg.spawn.clone(), cfg.prewarm.clone());
        let manager = Arc::new(Self {
            cfg,
            registry,
            state: StdMutex::new(ManagerState::default()),
            prewarm,
            load_lock: TokioMutex::new(()),
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
        let result = if tx.send(DispatchMsg::Predict(request)).is_err() {
            Err(anyhow!(
                "model {inference_id} was unloaded before the request could be queued"
            ))
        } else {
            match reply_rx.await {
                Ok(result) => result,
                Err(_) => Err(anyhow!(
                    "the dispatcher for model {inference_id} dropped the request"
                )),
            }
        };
        // The guard's Drop does the unpin + requested-TTL restore (Python's
        // `finally`); dropping explicitly keeps the restore at completion
        // time, and cancellation at `reply_rx.await` runs the same Drop.
        drop(pin);
        result
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
        // Pool snapshot before the state lock: the two mutexes are never
        // held together.
        let prewarm = self.prewarm.health();
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
                    last_effective_cap: match stats.last_effective_cap.load(Relaxed) {
                        // 0 = no window dispatched yet (real caps are >= 1).
                        0 => None,
                        cap => Some(cap),
                    },
                    total_predict_requests: stats.total_predict_requests.load(Relaxed),
                    total_batches: stats.total_batches.load(Relaxed),
                }
            })
            .collect();
        models.sort_by(|a, b| a.inference_id.cmp(&b.inference_id));
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
    /// taking `load_lock` below waits for that decision so the second drain
    /// awaits the discard instead of abandoning the worker mid-stop.
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
            let _load_guard = self.load_lock.lock().await;
            let mut state = self.state.lock().unwrap();
            handles.append(&mut state.draining);
        }
        // Parked prewarmed workers get the same graceful unload ladder,
        // concurrently with the dispatcher drains (design §8; both inside
        // the caller's existing shutdown envelope).
        let drain = async {
            for handle in handles {
                if let Err(err) = handle.await {
                    if err.is_panic() {
                        tracing::error!("inferio dispatcher task panicked during shutdown: {err}");
                    }
                }
            }
        };
        tokio::join!(drain, self.prewarm.shutdown());
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
    /// expired, and reap finished drain tasks.
    fn sweep(&self) {
        let mut state = self.state.lock().unwrap();
        if state.shutting_down {
            return;
        }
        state.draining.retain(|handle| !handle.is_finished());
        let unloads = state.cache.expire(Local::now());
        for id in unloads {
            Self::begin_unload(&mut state, &id);
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

    /// The shared load path. Bookkeeping runs under the state mutex; the
    /// slow spawn+load runs under `load_lock` only. With `pin_for_predict`
    /// the model is pinned *atomically* with the loaded-check and the
    /// dispatcher sender is returned (paired with the RAII [`PinGuard`]
    /// that owns the pin), so a predict can never observe its model
    /// expiring between load and enqueue — and a cancelled caller can never
    /// leak the pin.
    async fn ensure_loaded(
        &self,
        inference_id: &str,
        cache_key: &str,
        lru_size: i64,
        ttl_seconds: i64,
        pin_for_predict: bool,
        prewarm_hint: bool,
    ) -> Result<Option<(mpsc::UnboundedSender<DispatchMsg>, PinGuard)>> {
        let _load_guard = self.load_lock.lock().await;

        let spawn_pin = {
            let mut state = self.state.lock().unwrap();
            if state.shutting_down {
                bail!("the model manager is shutting down");
            }
            let unloads = state.cache.touch_load(
                inference_id,
                cache_key,
                lru_size,
                ttl_seconds,
                Local::now(),
            );
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
                    return Ok(Some((tx, guard)));
                }
                return Ok(None);
            }
            // Pin across the spawn so the sweeper cannot expire the entry
            // mid-load (Python has this race: its sweeper uses a separate
            // lock from load_model). The guard releases the pin even when
            // the calling future is cancelled mid-spawn.
            state.cache.pin(inference_id);
            PinGuard::adopt(self, inference_id, None)
        };

        let spawn_result = self.spawn_model(inference_id).await;
        let mut state = self.state.lock().unwrap();
        // Release the spawn pin under the same lock as the bookkeeping
        // below so the sweeper cannot expire the fresh entry in between.
        spawn_pin.release_locked(&mut state.cache);
        let (workers, registry_default_batch, impl_class, claim_eligible) = match spawn_result {
            Ok(spawned) => spawned,
            Err(err) => {
                // Python removes the requesting cache key's entry and
                // re-raises (manager.py:89-95): no LRU entry is left behind
                // after a failed load.
                let outcome = state.cache.remove(cache_key, inference_id);
                if let Some(id) = outcome.unload {
                    Self::begin_unload(&mut state, &id);
                }
                return Err(err.context(format!("failed to load model {inference_id}")));
            }
        };
        if state.shutting_down || !state.cache.refs_non_empty(inference_id) {
            // Explicitly unloaded (or the manager shut down) while the
            // workers were spawning: discard the whole set instead of
            // registering it. The discard task is parked in `draining` so
            // shutdown() (which re-checks after taking load_lock) awaits
            // the graceful stops instead of abandoning them on a detached
            // task.
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
        let context = DispatcherContext {
            inference_id: inference_id.to_owned(),
            generation,
            registry_default_batch,
            server_default_batch: self.cfg.default_max_batch,
            manager: self.weak.get().cloned().expect("weak self is set in new()"),
            stats: Arc::clone(&stats),
        };
        // The dispatcher owns the whole WorkerSet (design §8): every
        // replica serves the one shared FIFO queue behind this sender.
        let task = tokio::spawn(run_dispatcher(context, workers, rx));
        let sender = if pin_for_predict {
            state.cache.pin(inference_id);
            let guard =
                PinGuard::adopt(self, inference_id, Some((cache_key.to_owned(), ttl_seconds)));
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
            },
        );
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
    /// one worker per entry of the spec's `device_pins`, each pinned via
    /// `CUDA_VISIBLE_DEVICES` at spawn, all spawned and loaded
    /// *concurrently*. Any replica failing kills the others — a load either
    /// yields the complete set or nothing (no partial sets to reason
    /// about). The registry is re-resolved at every spawn (design §4:
    /// workers are always born on current config). Errors carry the
    /// worker's traceback/stderr context from `Worker`.
    ///
    /// Prewarm claim (design §8): at most one replica — the first with no
    /// device pin, since pooled workers are spawned unpinned — is served
    /// from the pool's parked worker for the impl class, if one is alive
    /// (the pool pings before handing it over). The claimed worker skips
    /// spawn + handshake + heavy imports and only needs `configure` +
    /// `load`; the remaining replicas fresh-spawn as before.
    async fn spawn_model(
        &self,
        inference_id: &str,
    ) -> Result<(Vec<Worker>, Option<u32>, String, bool)> {
        let (spec, registry_default_batch) = {
            let mut registry = self.registry.lock().unwrap();
            let snapshot = registry
                .get()
                .context("failed to load the inference registry")?;
            let spec = snapshot.spawn_spec(inference_id)?;
            (spec, registry_default_batch(&snapshot, inference_id))
        };
        let replica_count = spec.device_pins.len();
        let claim_replica = spec.device_pins.iter().position(Option::is_none);
        let mut claimed = match claim_replica {
            Some(_) => self.prewarm.claim(&spec.impl_class).await,
            // Every replica is device-pinned; a pooled worker (spawned
            // without CUDA_VISIBLE_DEVICES) would violate the pin.
            None => None,
        };
        let spawns: Vec<_> = spec
            .device_pins
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
                async move {
                    let mut worker = match claimed {
                        Some(worker) => {
                            match self
                                .configure_claimed(worker, inference_id, spec, device.clone())
                                .await
                            {
                                Ok(worker) => worker,
                                Err(err) => return Err(err),
                            }
                        }
                        None => {
                            Worker::spawn_configured(&self.cfg.spawn, inference_id, spec, device)
                                .await?
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
        let mut first_error: Option<anyhow::Error> = None;
        for result in futures_util::future::join_all(spawns).await {
            match result {
                Ok((replica, worker)) => {
                    if replica_count > 1 {
                        tracing::debug!(
                            model = %inference_id,
                            replica,
                            device = spec.device_pins[replica].as_deref().unwrap_or("<unpinned>"),
                            "replica loaded"
                        );
                    }
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
            // Whole-set load atomicity: kill the replicas that did come up.
            futures_util::future::join_all(workers.into_iter().map(Worker::kill)).await;
            return Err(err);
        }
        Ok((
            workers,
            registry_default_batch,
            spec.impl_class,
            claim_replica.is_some(),
        ))
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
                Worker::spawn_configured(&self.cfg.spawn, inference_id, spec, device).await
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

    /// Repo root = CARGO_MANIFEST_DIR/.. (gateway crate lives one level
    /// below the workspace root).
    fn workspace_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
    }

    /// Same spawn setup as the worker.rs tests: repo venv python, cwd =
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
                "inferio manager tests need the repo venv interpreter at {} — create the dev venv first",
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

    /// Synthetic registry covering every fixture impl, so the manager path
    /// exercises RegistryCache -> spawn_spec for real.
    const TEST_REGISTRY_TOML: &str = r#"
[group.echo]
config.impl_class = "echo_test"
[group.echo.inference_ids.test]
[group.echo.inference_ids.second]

[group.slow]
config.impl_class = "slow_test"
[group.slow.inference_ids.test]

[group.batch]
config.impl_class = "batchsize_test"
[group.batch.inference_ids.test]

[group.failbatch]
config.impl_class = "failbatch_test"
[group.failbatch.inference_ids.test]

[group.dying]
config.impl_class = "dying_test"
[group.dying.inference_ids.test]

[group.nan]
config.impl_class = "nan_test"
[group.nan.inference_ids.test]

[group.missing]
config.impl_class = "does_not_exist"
[group.missing.inference_ids.test]

# Multi-replica WorkerSets (design §8 / Phase 3). devices pins are just env
# strings the device_test fixture reads back — no GPU involved.
[group.device]
config.impl_class = "device_test"
config.devices = ["3", "7"]
[group.device.inference_ids.test]

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

    fn test_manager(sweep_interval: Duration, default_max_batch: u32) -> TestSetup {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("registry.toml"), TEST_REGISTRY_TOML).unwrap();
        let registry = Arc::new(StdMutex::new(RegistryCache::new(RegistryConfig {
            config_dirs: vec![dir.path().to_path_buf()],
        })));
        let cfg = ManagerConfig {
            spawn: test_spawn_config(),
            default_max_batch,
            sweep_interval,
            // Pool disabled: these tests cover the manager's Python-parity
            // semantics; the prewarm pool has its own suite (prewarm.rs).
            prewarm: PrewarmConfig {
                enabled: false,
                lazy: false,
                always_warm: Vec::new(),
            },
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
            .predict("echo/test", "key", 10, 60, None, None, vec![data_input(json!(2))])
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
            .predict("echo/test", "b", 10, -1, None, None, vec![data_input(json!("x"))])
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
            .predict("echo/test", "b", 10, -1, None, None, vec![data_input(json!(1))])
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
            .predict("slow/test", "k", 10, 1, None, None, vec![data_input(json!(null))])
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
                    .predict("batch/test", "k", 10, -1, None, None, vec![data_input(json!(0))])
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
                    .predict("batch/test", "k", 10, -1, None, None, vec![data_input(json!(i))])
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

    /// Fatal worker death: a predict against a worker that dies mid-request
    /// fails with the supervision error, the model is dropped from all
    /// caches (no phantom /cache entries), and the next predict auto-loads
    /// a fresh worker instead of hitting a poisoned one.
    #[tokio::test]
    async fn worker_death_cleans_up_and_next_predict_respawns() {
        let setup = test_manager(Duration::from_secs(60), 32);
        let manager = &setup.manager;

        let err = manager
            .predict("dying/test", "k", 10, -1, None, None, vec![data_input(json!(1))])
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
            .predict("dying/test", "k", 10, -1, None, None, vec![data_input(json!(2))])
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
            .predict("echo/test", "k", 10, -1, None, None, vec![data_input(json!(1))])
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
            .predict("nan/test", "k", 10, -1, None, None, vec![data_input(json!("ok"))])
            .await
            .expect("normal predict");
        assert_eq!(outputs, vec![WorkerOutput::Json(json!({"ok": true}))]);
        let generation = manager.loaded_generation("nan/test").expect("loaded");

        let err = manager
            .predict("nan/test", "k", 10, -1, None, None, vec![data_input(json!("nan"))])
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
            .predict("nan/test", "k", 10, -1, None, None, vec![data_input(json!("ok"))])
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
                    .predict("slow/test", "k", 10, 1, None, None, vec![data_input(json!(null))])
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
            model.last_effective_cap,
            Some(32),
            "no explicit or registry opinion -> server default"
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
                    .predict("slow/test", "k", 10, -1, None, None, vec![data_input(json!(null))])
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

    /// The Phase 2 cap is observable (design §10: "health endpoint exposes
    /// it"): after traffic where every request carries max_batch=2, the
    /// last dispatched window was capped at 2 and health reports exactly
    /// that — not the server default of 32. Six singles capped at 2 need
    /// at least 3 windows, which total_batches must reflect.
    #[tokio::test]
    async fn health_reports_last_effective_cap_from_capped_traffic() {
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
        assert_eq!(
            model.last_effective_cap,
            Some(2),
            "every window carried the explicit cap of 2"
        );
        assert_eq!(model.total_predict_requests, 6);
        assert!(
            model.total_batches >= 3,
            "6 single-unit requests capped at 2 need >= 3 windows, got {}",
            model.total_batches
        );

        manager.shutdown().await;
    }
}
