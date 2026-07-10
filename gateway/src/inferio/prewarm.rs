//! Prewarm pool: one parked, `prepare()`-warmed worker per impl class
//! (design doc §8 "Prewarming", policy decided 2026-07-05, protocol v2).
//!
//! Measured reality: process start + heavy library imports dominate model
//! load latency, not weights. A prewarmed worker has completed the v2
//! identity handshake and run the impl's optional `prepare()` classmethod
//! (imports only — no weights, no GPU allocation), and is parked until the
//! manager claims it for a concrete model of that family. The pool is keyed
//! by **impl class** precisely because v2 split identity (handshake) from
//! configuration (claim-time `configure`).
//!
//! Policy, implemented exactly as decided:
//! - Master switch (`[inference_local.prewarm].enabled`, default ON). The
//!   pool never TTLs out — its entire purpose is to be there after the
//!   loaded model has TTLed away; if prewarm is enabled the RAM is spent.
//! - Eager set: the same selection logic as `preload_embedding_models`
//!   (search-usable embedding setters WITH DATA — the shared
//!   `db::extraction_log::get_search_embedding_setters`), mapped to impl
//!   classes via the registry, unioned with `always_warm`, refreshed at
//!   startup and on a minute tick ([`run_eager_prewarm_loop`]). Gated
//!   per-DB by `SystemConfig::prewarm_embedding_models` (default true).
//!   Classes that drop out of the set stay warm — no TTL by design.
//! - Lazy warm (`lazy`, default ON): after a model of class C loads, keep
//!   one warm C worker for next time. Respawn-on-claim is this same rule
//!   firing after a claim. Excluded when the triggering request carried an
//!   explicit `prewarm=false` hint (extraction jobs), so batch-only model
//!   families don't burn RAM on warm workers nobody is waiting for.
//! - `always_warm`: impl classes warmed unconditionally at manager startup —
//!   the only eager mechanism available to the standalone `inferio`
//!   subcommand, which may have no index DBs (the subcommand never scans
//!   DBs; [`run_eager_prewarm_loop`] is started in gateway mode only).
//! - Claiming: ping the parked worker first (it may have died while
//!   parked); on ping failure discard it and fall back to a fresh spawn. A
//!   failed `prepare()` is per-request and non-fatal — the worker is parked
//!   anyway (health state `failed_prepare`) and a later claim just pays the
//!   imports at `load`.
//!
//! Locking: the pool has its own mutex, never held together with the
//! manager's state mutex, and never across await. Warm workers spawn on
//! background tasks; the only pool work on the model-load path is the O(1)
//! slot lookup in `claim` plus the bounded ping of an already-parked worker
//! (and the load path is the slow path by definition). Predict hot paths
//! for already-loaded models never touch the pool.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

use super::manager::ModelManager;
use super::worker::{Worker, WorkerError, WorkerSpawnConfig};
use crate::db::extraction_log::get_search_embedding_setters;
use crate::db::info::{db_defaults, db_lists};
use crate::db::open_index_db_read;
use crate::db::system_config::SystemConfigStore;

/// Eager-set refresh period (design §8: "refreshed on the existing minute
/// tick" — same cadence as the cron scheduler, but the prewarm loop is its
/// own task so the inferio module doesn't reach into jobs:: internals).
const EAGER_TICK_INTERVAL: Duration = Duration::from_secs(60);

/// Pool policy, resolved from `[inference_local.prewarm]`.
#[derive(Debug, Clone)]
pub struct PrewarmConfig {
    pub enabled: bool,
    pub lazy: bool,
    pub always_warm: Vec<String>,
}

impl Default for PrewarmConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            lazy: true,
            always_warm: Vec::new(),
        }
    }
}

/// `prewarm` section of the `GET /health` report.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct PrewarmHealth {
    pub enabled: bool,
    pub lazy: bool,
    /// One entry per impl class the pool holds (or is spawning), sorted.
    pub warm: Vec<PrewarmWorkerHealth>,
}

#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct PrewarmWorkerHealth {
    pub impl_class: String,
    /// `"warm"` (parked, prepare() succeeded or was absent), `"spawning"`
    /// (background warm-up in flight), or `"failed_prepare"` (parked, but
    /// prepare() raised — claims still work, load pays the imports).
    pub state: String,
}

/// One impl class's pool slot.
enum Slot {
    /// A background warm-up task is in flight; `claim` treats this as
    /// absent (fresh spawn) and `ensure_warm` as present (no double spawn).
    Spawning,
    /// A parked worker: spawned, handshaken, `prewarm` sent.
    Parked {
        worker: Worker,
        failed_prepare: bool,
    },
}

#[derive(Default)]
struct PoolState {
    /// impl_class -> slot. At most one worker per class by construction.
    slots: BTreeMap<String, Slot>,
    /// Handles of in-flight warm-up tasks, aborted on shutdown (an aborted
    /// task's Worker is reaped by kill_on_drop + the Job Object).
    tasks: Vec<JoinHandle<()>>,
    shutting_down: bool,
}

/// The pool itself. Owned by [`ModelManager`] as an `Arc`; background tasks
/// hold only a `Weak` so dropping the manager also lets warm-ups die.
pub struct PrewarmPool {
    cfg: PrewarmConfig,
    spawn: WorkerSpawnConfig,
    state: StdMutex<PoolState>,
    weak: std::sync::OnceLock<Weak<PrewarmPool>>,
}

impl PrewarmPool {
    pub(crate) fn new(spawn: WorkerSpawnConfig, cfg: PrewarmConfig) -> Arc<Self> {
        let pool = Arc::new(Self {
            cfg,
            spawn,
            state: StdMutex::new(PoolState::default()),
            weak: std::sync::OnceLock::new(),
        });
        pool.weak
            .set(Arc::downgrade(&pool))
            .expect("weak self is set exactly once");
        pool
    }

    pub(crate) fn enabled(&self) -> bool {
        self.cfg.enabled
    }

    /// Warm every `always_warm` class. Called at manager construction (so
    /// gateway mode and the `inferio` subcommand both warm the whitelist at
    /// startup) and again by every eager tick (a no-op once warm).
    pub(crate) fn warm_always(&self) {
        for impl_class in &self.cfg.always_warm.clone() {
            self.ensure_warm(impl_class);
        }
    }

    /// Ensure the pool has (or is spawning) a warm worker for `impl_class`.
    /// No-op when the master switch is off, during shutdown, or when a slot
    /// already exists. The actual spawn + prewarm runs on a background task
    /// — this never blocks and never awaits.
    pub(crate) fn ensure_warm(&self, impl_class: &str) {
        if !self.cfg.enabled {
            return;
        }
        let mut state = self.state.lock().unwrap();
        if state.shutting_down || state.slots.contains_key(impl_class) {
            return;
        }
        state.tasks.retain(|task| !task.is_finished());
        state.slots.insert(impl_class.to_owned(), Slot::Spawning);
        let weak = self
            .weak
            .get()
            .cloned()
            .expect("weak self is set in new()");
        let task = tokio::spawn(warm_worker_task(
            weak,
            self.spawn.clone(),
            impl_class.to_owned(),
        ));
        state.tasks.push(task);
    }

    /// The lazy-warm rule (design §8): fires after a model of `impl_class`
    /// was loaded (claim or fresh) when the master switch AND the lazy
    /// switch are on AND the request's prewarm hint was not `false` (the
    /// caller resolves the hint before calling).
    pub(crate) fn lazy_warm(&self, impl_class: &str) {
        if self.cfg.enabled && self.cfg.lazy {
            self.ensure_warm(impl_class);
        }
    }

    /// Claim the parked worker for `impl_class`, if any: remove it from the
    /// pool and ping it (it may have died while parked). Ping failure
    /// discards the worker and returns None — the caller falls back to a
    /// fresh spawn. A `Spawning` slot is left alone (the warm-up lands in
    /// the pool for next time).
    pub(crate) async fn claim(&self, impl_class: &str) -> Option<Worker> {
        if !self.cfg.enabled {
            return None;
        }
        let slot = {
            let mut state = self.state.lock().unwrap();
            match state.slots.get(impl_class) {
                Some(Slot::Parked { .. }) => state.slots.remove(impl_class),
                _ => None,
            }
        };
        let Some(Slot::Parked {
            mut worker,
            failed_prepare,
        }) = slot
        else {
            return None;
        };
        match worker.ping().await {
            Ok(()) => {
                tracing::debug!(
                    impl_class,
                    failed_prepare,
                    "claimed a prewarmed worker from the pool"
                );
                Some(worker)
            }
            Err(err) => {
                tracing::warn!(
                    impl_class,
                    "parked prewarmed worker failed its claim ping; discarding: {err:#}"
                );
                // Fatal ping paths already killed/reaped the child; kill()
                // is idempotent and covers the (unlikely) WorkerError case.
                worker.kill().await;
                None
            }
        }
    }

    /// Health snapshot (`GET /health` `prewarm` section).
    pub(crate) fn health(&self) -> PrewarmHealth {
        let state = self.state.lock().unwrap();
        PrewarmHealth {
            enabled: self.cfg.enabled,
            lazy: self.cfg.lazy,
            warm: state
                .slots
                .iter()
                .map(|(impl_class, slot)| PrewarmWorkerHealth {
                    impl_class: impl_class.clone(),
                    state: match slot {
                        Slot::Spawning => "spawning",
                        Slot::Parked {
                            failed_prepare: false,
                            ..
                        } => "warm",
                        Slot::Parked {
                            failed_prepare: true,
                            ..
                        } => "failed_prepare",
                    }
                    .to_owned(),
                })
                .collect(),
        }
    }

    /// Shutdown: refuse new warm-ups, abort in-flight warm-up tasks (their
    /// Workers are reaped by kill_on_drop + the Job Object — they are cache
    /// warmers, not state), and run the graceful unload ladder on every
    /// parked worker, concurrently. Called from [`ModelManager::shutdown`]
    /// inside the existing shutdown envelope.
    pub(crate) async fn shutdown(&self) {
        let (workers, tasks) = {
            let mut state = self.state.lock().unwrap();
            state.shutting_down = true;
            let workers: Vec<Worker> = std::mem::take(&mut state.slots)
                .into_values()
                .filter_map(|slot| match slot {
                    Slot::Parked { worker, .. } => Some(worker),
                    Slot::Spawning => None,
                })
                .collect();
            (workers, std::mem::take(&mut state.tasks))
        };
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = task.await;
        }
        let results =
            futures_util::future::join_all(workers.into_iter().map(Worker::shutdown)).await;
        for result in results {
            if let Err(err) = result {
                tracing::warn!("prewarmed worker did not shut down gracefully: {err:#}");
            }
        }
    }

    /// Test hook: kill the parked worker's process out-of-band (simulating
    /// death while parked) without touching pool bookkeeping, so the
    /// claim-time ping-failure path is exercised. Returns false when no
    /// worker is parked for the class.
    #[cfg(test)]
    pub(crate) async fn kill_parked_worker_for_test(&self, impl_class: &str) -> bool {
        let slot = {
            let mut state = self.state.lock().unwrap();
            match state.slots.get(impl_class) {
                Some(Slot::Parked { .. }) => state.slots.remove(impl_class),
                _ => None,
            }
        };
        let Some(Slot::Parked {
            mut worker,
            failed_prepare,
        }) = slot
        else {
            return false;
        };
        worker.kill_child_externally_for_test().await;
        self.state.lock().unwrap().slots.insert(
            impl_class.to_owned(),
            Slot::Parked {
                worker,
                failed_prepare,
            },
        );
        true
    }
}

/// Background warm-up: spawn (identity handshake), `prewarm`, then park.
/// A failed `prepare()` (per-request `error` frame) parks the worker anyway
/// per the design — the imports just weren't saved; the claim still skips
/// process start + handshake. Fatal failures (spawn error, protocol
/// violation, death) drop the slot so a later ensure_warm retries.
async fn warm_worker_task(pool: Weak<PrewarmPool>, spawn: WorkerSpawnConfig, impl_class: String) {
    let outcome = async {
        let mut worker = Worker::spawn(&spawn, &impl_class, None).await?;
        let failed_prepare = match worker.prewarm().await {
            Ok(()) => false,
            Err(err) if err.downcast_ref::<WorkerError>().is_some() => {
                tracing::warn!(
                    impl_class = %impl_class,
                    "prepare() failed in prewarmed worker; parking it anyway \
                     (a claim just pays the imports at load): {err:#}"
                );
                true
            }
            // Fatal: the worker is already killed and reaped by the fatal
            // path; dropping it here is a no-op.
            Err(err) => return Err(err),
        };
        anyhow::Ok((worker, failed_prepare))
    }
    .await;

    let Some(pool) = pool.upgrade() else {
        // Manager torn down mid-warm-up: the Worker (if any) drops here and
        // kill_on_drop + the Job Object reap the child.
        return;
    };
    match outcome {
        Ok((worker, failed_prepare)) => {
            let stale = {
                let mut state = pool.state.lock().unwrap();
                if state.shutting_down {
                    Some(worker)
                } else {
                    tracing::info!(impl_class = %impl_class, failed_prepare, "prewarmed worker parked");
                    state.slots.insert(
                        impl_class.clone(),
                        Slot::Parked {
                            worker,
                            failed_prepare,
                        },
                    );
                    None
                }
            };
            if let Some(worker) = stale {
                // Shutdown raced the warm-up: dismiss the fresh worker via
                // the same graceful ladder parked workers get.
                let _ = worker.shutdown().await;
            }
        }
        Err(err) => {
            tracing::warn!(
                impl_class = %impl_class,
                "failed to spawn a prewarmed worker: {err:#}"
            );
            let mut state = pool.state.lock().unwrap();
            if matches!(state.slots.get(&impl_class), Some(Slot::Spawning)) {
                state.slots.remove(&impl_class);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Eager set (gateway mode only; the subcommand has no index DBs)
// ---------------------------------------------------------------------------

/// The startup + minute-tick eager task (design §8): started from main.rs
/// in gateway mode when `inference_local.enabled && prewarm.enabled`. Holds
/// only a Weak on the manager so process teardown ends the loop.
pub(crate) async fn run_eager_prewarm_loop(manager: Weak<ModelManager>) {
    loop {
        {
            let Some(manager) = manager.upgrade() else {
                break;
            };
            eager_prewarm_tick(&manager).await;
        }
        tokio::time::sleep(EAGER_TICK_INTERVAL).await;
    }
}

/// One eager pass: enumerate index DBs; for each DB whose SystemConfig has
/// `prewarm_embedding_models` (default true), select the search-usable
/// embedding setters WITH DATA (the exact `preload_embedding_models`
/// filter, shared via `db::extraction_log`), map setter -> impl class via
/// the registry, union with `always_warm`, and ensure the pool has a warm
/// worker per class. Per-DB failures (config, open, query) log and skip
/// that DB — the task never crashes. Classes that drop out of the set stay
/// warm (no TTL by design).
pub(crate) async fn eager_prewarm_tick(manager: &ModelManager) {
    let pool = manager.prewarm_pool();
    if !pool.enabled() {
        return;
    }
    pool.warm_always();

    let registry = {
        let snapshot = manager.registry_cache().lock().unwrap().get();
        match snapshot {
            Ok(registry) => registry,
            Err(err) => {
                tracing::warn!("eager prewarm: inference registry failed to load: {err:#}");
                return;
            }
        }
    };
    let (index_dbs, _) = match db_lists() {
        Ok(lists) => lists,
        Err(err) => {
            tracing::warn!("eager prewarm: failed to enumerate index DBs: {err:#}");
            return;
        }
    };
    let store = SystemConfigStore::from_env();
    let user_data_db = db_defaults().1;
    let mut classes: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for index_db in index_dbs {
        let config = match store.load(&index_db) {
            Ok(config) => config,
            Err(err) => {
                tracing::warn!(index_db, "eager prewarm: failed to load config, skipping: {err:?}");
                continue;
            }
        };
        if !config.prewarm_embedding_models {
            continue;
        }
        let mut conn = match open_index_db_read(&index_db, &user_data_db).await {
            Ok(conn) => conn,
            Err(err) => {
                tracing::warn!(index_db, "eager prewarm: failed to open index DB, skipping: {err:?}");
                continue;
            }
        };
        let setters = match get_search_embedding_setters(&mut conn).await {
            Ok(setters) => setters,
            Err(err) => {
                tracing::warn!(index_db, "eager prewarm: failed to list setters, skipping: {err:?}");
                continue;
            }
        };
        for setter in setters {
            match registry.spawn_spec(&setter) {
                Ok(spec) => {
                    classes.insert(spec.impl_class);
                }
                Err(err) => tracing::debug!(
                    setter,
                    index_db,
                    "eager prewarm: setter has no usable registry entry: {err:#}"
                ),
            }
        }
    }
    for impl_class in classes {
        pool.ensure_warm(&impl_class);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inferio::manager::{ManagerConfig, ModelManager};
    use crate::inferio::registry::{RegistryCache, RegistryConfig};
    use crate::inferio::worker::{WorkerDeadlines, WorkerInput, WorkerOutput};
    use serde_json::json;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    fn workspace_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
    }

    /// Same spawn setup as the worker/manager/http tests: repo venv python,
    /// cwd = repo root, PYTHONPATH=src, NO_CUDNN, fixture impl dir.
    fn test_spawn_config() -> WorkerSpawnConfig {
        let root = workspace_root();
        // PANOPTIKON_TEST_PYTHON overrides the repo-venv interpreter (any
        // python with msgpack works), e.g. running the suite under WSL
        // against a Windows checkout, whose .venv is a Windows venv.
        let python = match std::env::var_os("PANOPTIKON_TEST_PYTHON") {
            Some(explicit) => PathBuf::from(explicit),
            None if cfg!(windows) => root.join(".venv/Scripts/python.exe"),
            None => root.join(".venv/bin/python"),
        };
        if !python.is_file() {
            panic!(
                "inferio prewarm tests need the repo venv interpreter at {} — create the dev venv first",
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

    /// Fixture registry: the prepare_test family (its predict reports
    /// whether prepare() ran in-process — the claim-proof oracle), the
    /// prepare_fail_test family (prepare() raises), and echo_test as the
    /// contrast class. warmgrp/coldgrp are the eager-test mapping targets.
    const TEST_REGISTRY_TOML: &str = r#"
[group.prep]
config.impl_class = "prepare_test"
[group.prep.inference_ids.test]

[group.failprep]
config.impl_class = "prepare_fail_test"
[group.failprep.inference_ids.test]

[group.echo]
config.impl_class = "echo_test"
[group.echo.inference_ids.test]

[group.warmgrp]
config.impl_class = "prepare_test"
[group.warmgrp.inference_ids.model]

[group.coldgrp]
config.impl_class = "echo_test"
[group.coldgrp.inference_ids.model]
"#;

    struct TestSetup {
        manager: Arc<ModelManager>,
        _registry_dir: tempfile::TempDir,
    }

    fn test_manager(prewarm: PrewarmConfig) -> TestSetup {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("registry.toml"), TEST_REGISTRY_TOML).unwrap();
        let registry = Arc::new(StdMutex::new(RegistryCache::new(RegistryConfig {
            config_dirs: vec![dir.path().to_path_buf()],
        })));
        let cfg = ManagerConfig {
            spawn: test_spawn_config(),
            default_max_batch: 32,
            sweep_interval: Duration::from_secs(60),
            prewarm,
        };
        TestSetup {
            manager: ModelManager::new(cfg, registry),
            _registry_dir: dir,
        }
    }

    fn enabled(lazy: bool, always_warm: &[&str]) -> PrewarmConfig {
        PrewarmConfig {
            enabled: true,
            lazy,
            always_warm: always_warm.iter().map(|class| class.to_string()).collect(),
        }
    }

    /// Poll the pool until `impl_class` reaches `want` ("warm" /
    /// "spawning" / "failed_prepare"). Warm-ups run on background tasks, so
    /// every observation of a *reached* state has to poll.
    async fn wait_for_pool_state(manager: &ModelManager, impl_class: &str, want: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            let health = manager.prewarm_pool().health();
            if health
                .warm
                .iter()
                .any(|entry| entry.impl_class == impl_class && entry.state == want)
            {
                return;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("pool never reached state {want:?} for {impl_class}: {health:?}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// The `prepared` flag from a prepare_test predict output: true iff the
    /// serving worker ran `prepare()` before the model was bound — i.e. it
    /// came from the pool, not a fresh spawn.
    fn reported_prepared(outputs: &[WorkerOutput]) -> bool {
        match &outputs[0] {
            WorkerOutput::Json(value) => value["prepared"].as_bool().expect("prepared flag"),
            other => panic!("unexpected output {other:?}"),
        }
    }

    fn data_input(value: serde_json::Value) -> WorkerInput {
        WorkerInput {
            data: Some(value),
            file: None,
        }
    }

    /// Claim proof + always_warm at startup, in one arc: constructing the
    /// manager with `always_warm = ["prepare_test"]` warms the pool without
    /// any DB or request (state "warm" after the background spawn), and a
    /// predict on a prep-family model then reports `prepared: true` — the
    /// worker that served it MUST be the pooled one, because a fresh spawn
    /// reports false (proven by the contrast test below). With lazy off,
    /// the claim consumes the slot and nothing respawns.
    #[tokio::test]
    async fn always_warm_parks_worker_and_claim_serves_prepared_model() {
        let setup = test_manager(enabled(false, &["prepare_test"]));
        let manager = &setup.manager;

        wait_for_pool_state(manager, "prepare_test", "warm").await;

        let outputs = manager
            .predict("prep/test", "k", 10, -1, None, None, vec![data_input(json!(1))])
            .await
            .expect("predict auto-loads via the claimed worker");
        assert!(
            reported_prepared(&outputs),
            "prepared:true proves the claimed pooled worker (which ran prepare()) served the model"
        );

        let health = manager.prewarm_pool().health();
        assert!(
            health.warm.is_empty(),
            "lazy off: the claim consumed the slot and nothing respawned: {health:?}"
        );

        manager.shutdown().await;
    }

    /// Contrast half of the claim proof: with the pool disabled the same
    /// predict is served by a fresh spawn_configured worker whose process
    /// never ran prepare() — `prepared: false`. Health reports the disabled
    /// pool (enabled=false, no entries).
    #[tokio::test]
    async fn without_pool_fresh_worker_reports_unprepared() {
        let setup = test_manager(PrewarmConfig {
            enabled: false,
            lazy: false,
            always_warm: vec!["prepare_test".to_string()],
        });
        let manager = &setup.manager;

        let outputs = manager
            .predict("prep/test", "k", 10, -1, None, None, vec![data_input(json!(1))])
            .await
            .expect("predict via fresh spawn");
        assert!(
            !reported_prepared(&outputs),
            "no pool -> fresh worker -> prepare() never ran"
        );

        let health = manager.prewarm_pool().health();
        assert!(!health.enabled, "master switch off is visible in health");
        assert!(
            health.warm.is_empty(),
            "disabled pool ignores always_warm: {health:?}"
        );

        manager.shutdown().await;
    }

    /// The lazy-warm rule end to end: a load with the hint absent leaves a
    /// warm worker of the model's class behind; after unload, the next load
    /// CLAIMS that worker (prepared:true) and lazy respawns another one —
    /// respawn-on-claim is the same rule firing again.
    #[tokio::test]
    async fn lazy_warm_after_load_then_claim_and_respawn() {
        let setup = test_manager(enabled(true, &[]));
        let manager = &setup.manager;

        // First load: fresh spawn (pool empty), then the lazy rule fires.
        manager
            .load_model("prep/test", "k", 10, -1, None)
            .await
            .expect("first load (fresh spawn)");
        // The slot is inserted synchronously during the load (Spawning),
        // then flips to warm when the background task parks the worker.
        wait_for_pool_state(manager, "prepare_test", "warm").await;

        assert!(manager.unload_model("k", "prep/test").await.unwrap());

        // Second load: claims the warm worker...
        manager
            .load_model("prep/test", "k", 10, -1, None)
            .await
            .expect("second load (claim)");
        let outputs = manager
            .predict("prep/test", "k", 10, -1, None, None, vec![data_input(json!(1))])
            .await
            .expect("predict on the claimed worker");
        assert!(
            reported_prepared(&outputs),
            "the second load was served by the lazily-warmed worker"
        );
        // ... and the lazy rule fires again after the claim: a fresh warm
        // worker replaces the consumed one.
        wait_for_pool_state(manager, "prepare_test", "warm").await;

        manager.shutdown().await;
    }

    /// The explicit `prewarm=false` hint (extraction jobs) suppresses the
    /// lazy rule: the slot insertion is synchronous when it does happen, so
    /// an empty pool immediately after the load is a deterministic
    /// assertion, not a timing window.
    #[tokio::test]
    async fn prewarm_false_hint_suppresses_lazy_warm() {
        let setup = test_manager(enabled(true, &[]));
        let manager = &setup.manager;

        manager
            .load_model("echo/test", "k", 10, -1, Some(false))
            .await
            .expect("load with prewarm=false");
        let health = manager.prewarm_pool().health();
        assert!(
            health.warm.is_empty(),
            "prewarm=false must not leave a warm worker behind: {health:?}"
        );

        // The hint is per-request: a later hint-absent load of the same
        // class (after unload) does warm.
        assert!(manager.unload_model("k", "echo/test").await.unwrap());
        manager
            .load_model("echo/test", "k", 10, -1, None)
            .await
            .expect("load with the hint absent");
        assert!(
            !manager.prewarm_pool().health().warm.is_empty(),
            "absent hint means true: the lazy slot exists synchronously after the load"
        );

        manager.shutdown().await;
    }

    /// A parked worker that died while parked (killed out-of-band via the
    /// test hook) fails the claim-time ping; the load falls back to a fresh
    /// spawn — it succeeds promptly (no hang) and the serving worker never
    /// ran prepare() (prepared:false), proving the dead pooled worker was
    /// discarded rather than used.
    #[tokio::test]
    async fn dead_parked_worker_is_discarded_and_load_fresh_spawns() {
        let setup = test_manager(enabled(false, &["prepare_test"]));
        let manager = &setup.manager;

        wait_for_pool_state(manager, "prepare_test", "warm").await;
        assert!(
            manager
                .prewarm_pool()
                .kill_parked_worker_for_test("prepare_test")
                .await,
            "a worker was parked to kill"
        );

        manager
            .load_model("prep/test", "k", 10, -1, None)
            .await
            .expect("load succeeds via fresh spawn despite the dead parked worker");
        let outputs = manager
            .predict("prep/test", "k", 10, -1, None, None, vec![data_input(json!(1))])
            .await
            .expect("predict");
        assert!(
            !reported_prepared(&outputs),
            "the dead pooled worker was discarded; a fresh (unprepared) worker serves"
        );
        assert!(
            manager.prewarm_pool().health().warm.is_empty(),
            "the dead worker's slot is gone (and lazy is off)"
        );

        manager.shutdown().await;
    }

    /// A failing `prepare()` (prepare_fail_test raises) parks the worker
    /// anyway per the design — health reports the slot as
    /// `failed_prepare` — and the claim still works: the load succeeds on
    /// that very worker (it just pays the imports at load), consuming the
    /// slot.
    #[tokio::test]
    async fn failed_prepare_parks_anyway_and_claim_still_loads() {
        let setup = test_manager(enabled(false, &["prepare_fail_test"]));
        let manager = &setup.manager;

        wait_for_pool_state(manager, "prepare_fail_test", "failed_prepare").await;

        let outputs = manager
            .predict(
                "failprep/test",
                "k",
                10,
                -1,
                None,
                None,
                vec![data_input(json!(1))],
            )
            .await
            .expect("load + predict succeed on the failed-prepare worker");
        assert_eq!(outputs, vec![WorkerOutput::Json(json!({"ok": true}))]);
        assert!(
            manager.prewarm_pool().health().warm.is_empty(),
            "the failed-prepare slot was consumed by the claim (lazy off)"
        );

        manager.shutdown().await;
    }

    /// Graceful shutdown with parked workers: both always_warm classes are
    /// parked, then ModelManager::shutdown runs their graceful unload
    /// ladder concurrently with the (empty) dispatcher drains. The test
    /// completing without a hang plus the emptied health report is the
    /// no-leak assertion (the workers additionally sit under kill-on-drop +
    /// the Job Object, so a leak would also fail the process exit).
    #[tokio::test]
    async fn shutdown_with_parked_workers_is_clean() {
        let setup = test_manager(enabled(true, &["prepare_test", "echo_test"]));
        let manager = &setup.manager;

        wait_for_pool_state(manager, "prepare_test", "warm").await;
        wait_for_pool_state(manager, "echo_test", "warm").await;

        manager.shutdown().await;

        let health = manager.prewarm_pool().health();
        assert!(
            health.warm.is_empty(),
            "shutdown drained every parked worker: {health:?}"
        );
        // New warm-ups are refused after shutdown.
        manager.prewarm_pool().ensure_warm("echo_test");
        assert!(manager.prewarm_pool().health().warm.is_empty());
    }

    // ------------------------------------------------------------------
    // Eager DB selection, against real on-disk index DBs (the same
    // migrate_databases_on_disk + test data-folder fixture the
    // continuous-scan tests use).
    // ------------------------------------------------------------------

    fn unique_db_name(prefix: &str) -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        format!("{prefix}-{}", COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    /// Insert one item with a `clip` embedding row for `setter` — the
    /// minimal "search-usable embedding setter WITH DATA" shape the shared
    /// selection query looks for.
    async fn seed_clip_setter(index_db: &str, setter: &str) {
        let mut conn = crate::db::open_index_db_write_no_user_data(index_db)
            .await
            .expect("open index db for seeding");
        sqlx::query(
            "INSERT INTO items (id, sha256, md5, type, time_added) \
             VALUES (1, 'sha_seed', 'md5_seed', 'image/png', '2026-01-01T00:00:00')",
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query("INSERT INTO setters (id, name) VALUES (1, ?1)")
            .bind(setter)
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO item_data (id, item_id, setter_id, data_type, idx, is_origin) \
             VALUES (1, 1, 1, 'clip', 0, 1)",
        )
        .execute(&mut conn)
        .await
        .unwrap();
    }

    /// The eager tick against two REAL index DBs: both hold a clip setter
    /// with data, but one DB's SystemConfig sets
    /// `prewarm_embedding_models = false`. Only the enabled DB's setter is
    /// selected, mapped setter -> impl class through the registry
    /// (warmgrp/model -> prepare_test), and warmed; the disabled DB's class
    /// (coldgrp/model -> echo_test) must never appear in the pool. This
    /// exercises enumeration, the per-DB flag gate, the shared
    /// with-data selection query against the real schema, and the registry
    /// mapping in one pass.
    #[tokio::test]
    async fn eager_tick_warms_only_enabled_dbs_with_embedding_data() {
        let test_env = crate::test_utils::test_data_dir();
        let _root = test_env.path().to_path_buf();
        let warm_db = unique_db_name("prewarm-on");
        let cold_db = unique_db_name("prewarm-off");
        // Creates the index/storage DBs for both names plus the "default"
        // user_data DB the eager tick attaches (db_defaults().1).
        crate::db::migrations::migrate_databases_on_disk(Some(&warm_db), None)
            .await
            .expect("migrate warm db");
        crate::db::migrations::migrate_databases_on_disk(Some(&cold_db), None)
            .await
            .expect("migrate cold db");
        seed_clip_setter(&warm_db, "warmgrp/model").await;
        seed_clip_setter(&cold_db, "coldgrp/model").await;

        // Disable the flag on the cold DB only (warm DB keeps default true).
        let store = SystemConfigStore::from_env();
        let mut cold_config = store.load(&cold_db).expect("load cold config");
        assert!(
            cold_config.prewarm_embedding_models,
            "the flag defaults to true"
        );
        cold_config.prewarm_embedding_models = false;
        store.save(&cold_db, &cold_config).expect("save cold config");

        let setup = test_manager(enabled(false, &[]));
        let manager = &setup.manager;
        eager_prewarm_tick(manager).await;

        wait_for_pool_state(manager, "prepare_test", "warm").await;
        let health = manager.prewarm_pool().health();
        assert!(
            !health
                .warm
                .iter()
                .any(|entry| entry.impl_class == "echo_test"),
            "the disabled DB's class must not warm: {health:?}"
        );

        manager.shutdown().await;
    }
}
