//! Cron job scheduler and preload loop.
//!
//! Port of `panoptikon.api.cronjob` (schedule.py + job.py) and
//! `panoptikon.api.preload`. A single actor ticks once a minute over every
//! index DB, re-reading each DB's config so schedule changes apply without a
//! restart (a config-save also notifies the actor directly). Scheduling state
//! is in-memory only: runs missed while the process is down are deliberately
//! not caught up on startup, so launching the app never kicks off a GPU-heavy
//! extraction run on its own.
//!
//! The scheduled work itself (`run_cronjob`) doubles as the manual-trigger
//! endpoint's implementation: the cron_jobs list is the user's standing set of
//! models to run on new data, so the endpoint runs it even when automatic
//! scheduling is disabled.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use chrono::{DateTime, Duration as ChronoDuration, Local};
use croner::Cron;
use ractor::concurrency::Duration as RactorDuration;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tokio::sync::{OnceCell, oneshot};

use crate::api_error::ApiError;
use crate::db::extraction_log::get_search_embedding_setters;
use crate::db::info::{db_defaults, db_lists};
use crate::db::open_index_db_read;
use crate::db::system_config::{SystemConfig, SystemConfigStore};
use crate::jobs::extraction::resolve_model_metadata;
use crate::jobs::inference_pool::job_inference_context;
use crate::jobs::queue::{
    BatchDedup, BatchEnqueueResult, JobModel, JobRequest, JobType, enqueue_jobs_with_dedup,
};

type ApiResult<T> = std::result::Result<T, ApiError>;

pub(crate) const CRON_TAG: &str = "cronjob";

const TICK_INTERVAL_SECS: u64 = 60;
const PRELOAD_TTL_SECS: i64 = 3600;

/// Validates a cron schedule string with the same parser the scheduler uses.
pub(crate) fn validate_cron_schedule(schedule: &str) -> Result<(), String> {
    Cron::from_str(schedule)
        .map(|_| ())
        .map_err(|err| err.to_string())
}

/// Returns the first occurrence strictly after now, using the same local-time
/// parser and semantics as the scheduler.
pub(crate) fn next_cron_occurrence(schedule: &str) -> Result<DateTime<Local>, String> {
    let cron = Cron::from_str(schedule).map_err(|err| err.to_string())?;
    cron.find_next_occurrence(&Local::now(), false)
        .map_err(|err| err.to_string())
}

// ---------------------------------------------------------------------------
// Pure scheduling core (port of schedule.py)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
struct DbCronState {
    cron_string: String,
    next_fire: DateTime<Local>,
}

/// One scheduler tick for one DB, mirroring `try_cronjob`/`update_schedule`:
/// `cron_string` is `None` when scheduling is disabled (state is cleared); a
/// changed string recomputes the next fire from `now`; when `now` reaches the
/// stored fire time the run fires exactly once and the next fire is recomputed
/// from `now` (missed intervals are not caught up). An unparseable string
/// behaves like a disabled schedule.
fn plan_tick(
    prev: Option<DbCronState>,
    cron_string: Option<&str>,
    now: DateTime<Local>,
) -> (Option<DbCronState>, bool) {
    let state = reschedule(prev, cron_string, now);
    match state {
        Some(state) if now >= state.next_fire => (reschedule(None, cron_string, now), true),
        other => (other, false),
    }
}

fn reschedule(
    prev: Option<DbCronState>,
    cron_string: Option<&str>,
    now: DateTime<Local>,
) -> Option<DbCronState> {
    let cron_string = cron_string?;
    if let Some(prev) = prev {
        if prev.cron_string == cron_string {
            return Some(prev);
        }
    }
    let cron = Cron::from_str(cron_string).ok()?;
    // inclusive=false: strictly after `now`, like croniter's get_next.
    let next_fire = cron.find_next_occurrence(&now, false).ok()?;
    Some(DbCronState {
        cron_string: cron_string.to_string(),
        next_fire,
    })
}

// ---------------------------------------------------------------------------
// Cron run (port of job.py run_cronjob)
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) enum CronRunOutcome {
    Enqueued(Vec<JobModel>),
    /// A previous cronjob-tagged job for this DB is still queued or running.
    Skipped,
}

/// Dependency stage of a cron request. Scan work must precede extraction;
/// source-data models (target entities exactly `["items"]` or `["files"]`)
/// must precede derived-data models, which consume what the source models
/// wrote. The classification comes from global inference metadata and is
/// therefore identical across index DBs, which is what makes merging several
/// DBs' batches safe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum CronPhase {
    Scan,
    Source,
    Derived,
    /// Unclassifiable: the inference server was unreachable, so the phase of
    /// every model in this tick is unknown. Merging then has no dependency
    /// information to work with and falls back to concatenating each DB's
    /// block in config order (see [`merge_cron_batches`]). Metadata is fetched
    /// once per tick and shared, so `Unknown` never mixes with the others in
    /// one batch; it sorts last purely as a safe default.
    Unknown,
}

/// Classifies a model by its resolved target entities.
fn cron_phase(target_entities: &[String]) -> CronPhase {
    if target_entities == ["items"] || target_entities == ["files"] {
        CronPhase::Source
    } else {
        CronPhase::Derived
    }
}

/// Builds one index DB's cron request list: a scan job first, then one
/// data-extraction job per configured model, each tagged with the phase the
/// merge sorts on.
///
/// `metadata` is the inference server's registry dump, fetched once per tick.
/// When it is `None` (server unreachable) nothing is dropped and every model
/// is classified `Unknown`, which the merge concatenates per DB in config
/// order — the pre-merge fallback behaviour, for any number of DBs. Extraction
/// jobs re-resolve metadata at execution time, so an unreachable inference
/// server only costs the ordering.
fn build_cron_requests(
    index_db: &str,
    user_data_db: &str,
    scan_job_type: JobType,
    config: &SystemConfig,
    metadata: Option<&serde_json::Value>,
) -> Vec<(CronPhase, JobRequest)> {
    let mut requests = vec![(
        CronPhase::Scan,
        cron_request(scan_job_type, index_db, user_data_db, None),
    )];

    for job in &config.cron_jobs {
        let phase = match metadata {
            Some(metadata) => match resolve_model_metadata(metadata, &job.inference_id) {
                Ok(model) => cron_phase(&model.target_entities),
                Err(_) => {
                    tracing::error!(
                        inference_id = %job.inference_id,
                        index_db,
                        "model is in the cron schedule but not available on the inference server, skipping"
                    );
                    continue;
                }
            },
            None => CronPhase::Unknown,
        };
        let mut request = cron_request(
            JobType::DataExtraction,
            index_db,
            user_data_db,
            Some(job.inference_id.clone()),
        );
        request.batch_size = job.batch_size;
        request.threshold = job.threshold;
        requests.push((phase, request));
    }
    requests
}

/// Merges several DBs' cron request lists into one enqueue order: all scans,
/// then all source-model jobs, then all derived-model jobs, grouping identical
/// setters together so consecutive jobs can reuse a loaded batch model.
///
/// The sort is stable on `(phase, setter first appearance within that phase)`,
/// with the flattened batch order as the implicit tiebreak. Per-DB
/// dependencies survive because they are entirely expressed by the phase; two
/// jobs of one DB in the same phase have no ordering requirement between them,
/// so setter grouping may interleave them with another DB's.
///
/// `CronPhase::Unknown` (inference metadata unavailable) is the exception: the
/// phase then carries no dependency information, so grouping by setter could
/// hoist a DB's derived model above the source model it consumes. Those
/// requests get one rank each, in flattened order, which concatenates the DBs'
/// blocks with every DB's config order intact — at the cost of the cross-DB
/// setter grouping, which is the right trade when we cannot prove it is safe.
fn merge_cron_batches(batches: Vec<Vec<(CronPhase, JobRequest)>>) -> Vec<JobRequest> {
    let mut ranks: HashMap<(CronPhase, Option<String>), usize> = HashMap::new();
    let mut next_rank: HashMap<CronPhase, usize> = HashMap::new();
    let mut keyed: Vec<((CronPhase, usize), JobRequest)> = Vec::new();
    for (phase, request) in batches.into_iter().flatten() {
        let mut fresh_rank = |phase| {
            let next = next_rank.entry(phase).or_insert(0);
            let rank = *next;
            *next += 1;
            rank
        };
        let rank = if phase == CronPhase::Unknown {
            fresh_rank(phase)
        } else {
            let key = (phase, request.metadata.clone());
            match ranks.get(&key) {
                Some(rank) => *rank,
                None => {
                    let rank = fresh_rank(phase);
                    ranks.insert(key, rank);
                    rank
                }
            }
        };
        keyed.push(((phase, rank), request));
    }
    // Stable: equal keys keep the flattened (batch, position) order.
    keyed.sort_by_key(|(key, _)| *key);
    keyed.into_iter().map(|(_, request)| request).collect()
}

/// Enqueues the cron job set for `index_db`: a folder rescan first, then one
/// data-extraction job per configured model. The batch is enqueued atomically
/// and skipped when a previous cronjob for this DB is still queued or running.
/// Runs regardless of `enable_cron_job` — the manual trigger uses the cron set
/// as "the jobs to run now".
pub(crate) async fn run_cronjob(index_db: &str, user_data_db: &str) -> ApiResult<CronRunOutcome> {
    run_cronjob_with_scan(index_db, user_data_db, JobType::FolderRescan).await
}

/// Enqueues the wizard's first processing run. FolderUpdate both registers and
/// scans a new configuration; using FolderRescan here would call that update
/// and then immediately scan the same new roots a second time.
pub(crate) async fn run_initial_cronjob(
    index_db: &str,
    user_data_db: &str,
) -> ApiResult<CronRunOutcome> {
    run_cronjob_with_scan(index_db, user_data_db, JobType::FolderUpdate).await
}

/// The single-DB case of the merged tick: one batch, one dedup.
async fn run_cronjob_with_scan(
    index_db: &str,
    user_data_db: &str,
    scan_job_type: JobType,
) -> ApiResult<CronRunOutcome> {
    tracing::info!(index_db, "running cronjob");
    let store = SystemConfigStore::from_env();
    let config = store.load(index_db)?;
    let metadata = fetch_cron_metadata().await;
    let requests = build_cron_requests(
        index_db,
        user_data_db,
        scan_job_type,
        &config,
        metadata.as_ref(),
    );
    let result = enqueue_cron_batches(vec![(index_db.to_string(), requests)]).await?;
    if result.was_skipped(index_db) {
        log_cron_skip(index_db);
        Ok(CronRunOutcome::Skipped)
    } else {
        Ok(CronRunOutcome::Enqueued(result.enqueued))
    }
}

/// Fetches the inference registry once per tick. `None` means "unavailable";
/// see [`build_cron_requests`] for what that does to the ordering.
async fn fetch_cron_metadata() -> Option<serde_json::Value> {
    match job_inference_context().primary.get_metadata().await {
        Ok(metadata) => Some(metadata),
        Err(err) => {
            tracing::error!(
                error = %err,
                "inference metadata unavailable; enqueueing cron jobs in config order"
            );
            None
        }
    }
}

/// Merges every fired DB's requests and enqueues them as one batch, with one
/// cron-tag dedup per DB so a DB whose previous cronjob is still pending drops
/// out on its own without holding the others back.
async fn enqueue_cron_batches(
    per_db: Vec<(String, Vec<(CronPhase, JobRequest)>)>,
) -> ApiResult<BatchEnqueueResult> {
    let dedups = per_db
        .iter()
        .map(|(index_db, _)| BatchDedup {
            tag: CRON_TAG.to_string(),
            index_db: index_db.clone(),
        })
        .collect();
    let merged = merge_cron_batches(per_db.into_iter().map(|(_, requests)| requests).collect());
    enqueue_jobs_with_dedup(merged, dedups).await
}

fn log_cron_skip(index_db: &str) {
    tracing::info!(
        index_db,
        "a previous cronjob for this index DB is still queued or running, skipping"
    );
}

fn cron_request(
    job_type: JobType,
    index_db: &str,
    user_data_db: &str,
    metadata: Option<String>,
) -> JobRequest {
    JobRequest {
        job_type,
        index_db: index_db.to_string(),
        user_data_db: user_data_db.to_string(),
        metadata,
        batch_size: None,
        threshold: None,
        log_id: None,
        tag: Some(CRON_TAG.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Scheduler actor
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub(crate) struct CronScheduleStatus {
    pub next_run: Option<DateTime<Local>>,
    /// Last scheduler-fired run this process (manual triggers not included).
    pub last_run: Option<DateTime<Local>>,
}

pub(crate) enum CronSchedulerMessage {
    Tick,
    ConfigChanged {
        index_db: String,
    },
    GetStatus {
        index_db: String,
        reply: oneshot::Sender<CronScheduleStatus>,
    },
}

pub(crate) struct CronSchedulerActor;

pub(crate) struct CronSchedulerState {
    config_store: SystemConfigStore,
    schedules: HashMap<String, DbCronState>,
    /// Last invalid cron string logged per DB, to log once instead of every
    /// minute.
    invalid_logged: HashMap<String, String>,
    last_run: HashMap<String, DateTime<Local>>,
    preload: PreloadState,
}

impl Actor for CronSchedulerActor {
    type Msg = CronSchedulerMessage;
    type State = CronSchedulerState;
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let _ = myself.send_interval(RactorDuration::from_secs(TICK_INTERVAL_SECS), || {
            CronSchedulerMessage::Tick
        });
        // Initial tick right away so schedules and preload initialize at
        // startup, mirroring the Python lifespan's immediate first run.
        let _ = myself.cast(CronSchedulerMessage::Tick);
        Ok(CronSchedulerState {
            config_store: SystemConfigStore::from_env(),
            schedules: HashMap::new(),
            invalid_logged: HashMap::new(),
            last_run: HashMap::new(),
            preload: PreloadState::default(),
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            CronSchedulerMessage::Tick => {
                tick_all(state).await;
            }
            CronSchedulerMessage::ConfigChanged { index_db } => {
                tick_db(state, &index_db).await;
            }
            CronSchedulerMessage::GetStatus { index_db, reply } => {
                let status = CronScheduleStatus {
                    next_run: state.schedules.get(&index_db).map(|s| s.next_fire),
                    last_run: state.last_run.get(&index_db).copied(),
                };
                let _ = reply.send(status);
            }
        }
        Ok(())
    }
}

async fn tick_all(state: &mut CronSchedulerState) {
    let (index_dbs, _) = match db_lists() {
        Ok(lists) => lists,
        Err(err) => {
            tracing::error!(error = %err, "cron scheduler failed to enumerate index DBs");
            return;
        }
    };
    let known: HashSet<&String> = index_dbs.iter().collect();
    state.schedules.retain(|db, _| known.contains(db));
    state.invalid_logged.retain(|db, _| known.contains(db));
    state.last_run.retain(|db, _| known.contains(db));
    state.preload.retain(&known);

    // Two passes: tick every DB's schedule first, then enqueue every DB that
    // fired as one merged batch. The typical same-time-each-night schedule
    // makes DBs fire together, and merging is what lets consecutive jobs for
    // the same model reuse a loaded batch model across DBs.
    let mut fired = Vec::new();
    for index_db in &index_dbs {
        if let Some(config) = tick_schedule(state, index_db).await {
            fired.push((index_db.clone(), config));
        }
    }
    if !fired.is_empty() {
        run_cron_batch(fired).await;
    }
}

/// The `ConfigChanged` path: one DB, which degenerates to a one-batch merge.
async fn tick_db(state: &mut CronSchedulerState, index_db: &str) {
    if let Some(config) = tick_schedule(state, index_db).await {
        run_cron_batch(vec![(index_db.to_string(), config)]).await;
    }
}

/// Builds and enqueues the merged cron batch for every DB that fired.
async fn run_cron_batch(fired: Vec<(String, SystemConfig)>) {
    let index_dbs: Vec<&str> = fired.iter().map(|(db, _)| db.as_str()).collect();
    tracing::info!(?index_dbs, "running cronjob");
    let user_data_db = db_defaults().1;
    let metadata = fetch_cron_metadata().await;
    let per_db: Vec<(String, Vec<(CronPhase, JobRequest)>)> = fired
        .iter()
        .map(|(index_db, config)| {
            let requests = build_cron_requests(
                index_db,
                &user_data_db,
                JobType::FolderRescan,
                config,
                metadata.as_ref(),
            );
            (index_db.clone(), requests)
        })
        .collect();

    // One call for every fired DB: a failure here (queue actor gone or
    // shutting down) costs all of them the slot their schedule already
    // consumed, where it used to cost one. Accepted — both error sources are
    // global, so a per-DB call would fail for every DB anyway.
    match enqueue_cron_batches(per_db).await {
        Ok(result) => {
            for index_db in &result.skipped_dbs {
                log_cron_skip(index_db);
            }
            if !result.enqueued.is_empty() {
                let enqueued_dbs: Vec<&str> = index_dbs
                    .iter()
                    .copied()
                    .filter(|index_db| !result.was_skipped(index_db))
                    .collect();
                tracing::info!(
                    index_dbs = ?enqueued_dbs,
                    jobs = result.enqueued.len(),
                    "cronjob enqueued"
                );
            }
        }
        Err(err) => {
            tracing::error!(error = ?err, "error running cronjob");
        }
    }
}

/// Ticks one DB's schedule (and its model preload). Returns the DB's config
/// when the cron schedule fired this tick — the caller does the enqueueing, so
/// several DBs firing in the same tick can be merged into one batch. The
/// schedule slot is consumed here regardless of what the run does with it,
/// matching Python (run_cronjob swallows its own errors there).
async fn tick_schedule(state: &mut CronSchedulerState, index_db: &str) -> Option<SystemConfig> {
    let config = match state.config_store.load(index_db) {
        Ok(config) => config,
        Err(err) => {
            tracing::error!(error = ?err, index_db, "cron scheduler failed to load config");
            return None;
        }
    };

    let cron_string = config
        .enable_cron_job
        .then_some(config.cron_schedule.as_str());
    if let Some(schedule) = cron_string {
        if let Err(err) = validate_cron_schedule(schedule) {
            if state.invalid_logged.get(index_db).map(String::as_str) != Some(schedule) {
                tracing::error!(
                    index_db,
                    cron_schedule = schedule,
                    error = %err,
                    "invalid cron schedule; automatic cron runs disabled for this DB"
                );
                state
                    .invalid_logged
                    .insert(index_db.to_string(), schedule.to_string());
            }
        } else {
            state.invalid_logged.remove(index_db);
        }
    } else {
        state.invalid_logged.remove(index_db);
    }

    let now = Local::now();
    let prev = state.schedules.remove(index_db);
    let had_schedule = prev.is_some();
    let (next, fire) = plan_tick(prev, cron_string, now);
    let next_fire = next.as_ref().map(|s| s.next_fire);
    if let Some(next) = next {
        state.schedules.insert(index_db.to_string(), next);
    }
    if !had_schedule {
        if let Some(next_fire) = next_fire {
            tracing::info!(index_db, next_run = %next_fire, "cron schedule active");
        }
    }

    if fire {
        state.last_run.insert(index_db.to_string(), now);
        if let Some(next_fire) = next_fire {
            tracing::info!(index_db, next_run = %next_fire, "next scheduled cron run");
        }
    }

    preload_tick(state, index_db, &config).await;
    fire.then_some(config)
}

// ---------------------------------------------------------------------------
// Embedding model preload (port of preload.py)
// ---------------------------------------------------------------------------

#[derive(Default)]
struct PreloadState {
    /// Per DB, per setter: earliest time the model lease should be renewed.
    renewal_times: HashMap<String, HashMap<String, DateTime<Local>>>,
    /// DBs whose models we are currently keeping loaded; used to clear the
    /// inference cache exactly once when preloading gets disabled.
    active: HashSet<String>,
}

impl PreloadState {
    fn retain(&mut self, known: &HashSet<&String>) {
        self.renewal_times.retain(|db, _| known.contains(db));
        self.active.retain(|db| known.contains(db));
    }
}

async fn preload_tick(state: &mut CronSchedulerState, index_db: &str, config: &SystemConfig) {
    let client = &job_inference_context().primary;
    let cache_key = format!("preload[{index_db}]");

    if !config.preload_embedding_models {
        if state.preload.active.remove(index_db) {
            tracing::info!(index_db, "disabling model preloading, clearing cache");
            state.preload.renewal_times.remove(index_db);
            if let Err(err) = client.clear_cache(&cache_key).await {
                tracing::error!(error = %err, index_db, "failed to clear preload cache");
            }
        }
        return;
    }
    state.preload.active.insert(index_db.to_string());

    let user_data_db = db_defaults().1;
    let mut conn = match open_index_db_read(index_db, &user_data_db).await {
        Ok(conn) => conn,
        Err(err) => {
            tracing::error!(error = ?err, index_db, "preload failed to open index DB");
            return;
        }
    };
    // Shared selection rule with the prewarm eager set (extraction_log.rs).
    let embedding_setters = match get_search_embedding_setters(&mut conn).await {
        Ok(setters) => setters,
        Err(err) => {
            tracing::error!(error = ?err, index_db, "preload failed to list setters");
            return;
        }
    };
    if embedding_setters.is_empty() {
        return;
    }

    let now = Local::now();
    let renewals = state
        .preload
        .renewal_times
        .entry(index_db.to_string())
        .or_default();
    for setter in &embedding_setters {
        let due = renewals.get(setter).is_none_or(|renew_at| now >= *renew_at);
        if !due {
            continue;
        }
        match client
            .load_model(
                setter,
                &cache_key,
                embedding_setters.len() as i64,
                PRELOAD_TTL_SECS,
                // No prewarm opinion (absent = true): preloaded embedding
                // models are exactly what the warm pool exists to back up.
                None,
            )
            .await
        {
            Ok(_) => {
                // Renew comfortably before the inference-side TTL expires.
                let renew_secs = (PRELOAD_TTL_SECS - 130).max(60);
                renewals.insert(setter.clone(), now + ChronoDuration::seconds(renew_secs));
            }
            Err(err) => {
                tracing::error!(
                    error = %err,
                    setter,
                    index_db,
                    "failed to preload embedding model"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

static SCHEDULER: OnceCell<ActorRef<CronSchedulerMessage>> = OnceCell::const_new();

pub(crate) async fn ensure_cron_scheduler() -> ApiResult<ActorRef<CronSchedulerMessage>> {
    SCHEDULER
        .get_or_try_init(|| async {
            let (actor, _handle) =
                Actor::spawn(Some("cron-scheduler".to_string()), CronSchedulerActor, ())
                    .await
                    .map_err(|err| {
                        ApiError::internal(format!("Failed to spawn cron scheduler: {err:?}"))
                    })?;
            Ok(actor)
        })
        .await
        .map(Clone::clone)
}

/// Stops the scheduler at process shutdown so no further tick can enqueue
/// jobs. A tick already in progress finishes first; anything it enqueues after
/// the queue enters shutdown mode is refused there.
pub(crate) fn stop_cron_scheduler() {
    if let Some(actor) = SCHEDULER.get() {
        actor.stop(None);
    }
}

pub(crate) async fn notify_config_change(index_db: &str) -> ApiResult<()> {
    let scheduler = ensure_cron_scheduler().await?;
    scheduler
        .cast(CronSchedulerMessage::ConfigChanged {
            index_db: index_db.to_string(),
        })
        .map_err(|_| ApiError::internal("Cron scheduler unavailable"))
}

pub(crate) async fn get_schedule_status(index_db: &str) -> ApiResult<CronScheduleStatus> {
    let scheduler = ensure_cron_scheduler().await?;
    let (reply, rx) = oneshot::channel();
    scheduler
        .cast(CronSchedulerMessage::GetStatus {
            index_db: index_db.to_string(),
            reply,
        })
        .map_err(|_| ApiError::internal("Cron scheduler unavailable"))?;
    rx.await
        .map_err(|_| ApiError::internal("Cron scheduler dropped response"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::system_config::CronJob;
    use chrono::TimeZone;

    fn local(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Local> {
        Local.with_ymd_and_hms(y, mo, d, h, mi, s).unwrap()
    }

    fn state(cron_string: &str, next_fire: DateTime<Local>) -> DbCronState {
        DbCronState {
            cron_string: cron_string.to_string(),
            next_fire,
        }
    }

    // Disabled scheduling clears any existing state and never fires.
    #[test]
    fn plan_tick_disabled_clears_state() {
        let prev = state("0 3 * * *", local(2026, 7, 5, 3, 0, 0));
        let (next, fire) = plan_tick(Some(prev), None, local(2026, 7, 5, 4, 0, 0));
        assert!(next.is_none());
        assert!(!fire);
    }

    // First sighting of a schedule computes the next fire strictly after now.
    #[test]
    fn plan_tick_initializes_schedule() {
        let now = local(2026, 7, 4, 12, 30, 0);
        let (next, fire) = plan_tick(None, Some("0 3 * * *"), now);
        let next = next.unwrap();
        assert_eq!(next.next_fire, local(2026, 7, 5, 3, 0, 0));
        assert!(!fire);
    }

    // An unchanged string keeps the stored fire time (no recomputation drift).
    #[test]
    fn plan_tick_keeps_state_for_unchanged_string() {
        let fire_at = local(2026, 7, 5, 3, 0, 0);
        let prev = state("0 3 * * *", fire_at);
        let (next, fire) = plan_tick(
            Some(prev.clone()),
            Some("0 3 * * *"),
            local(2026, 7, 4, 13, 0, 0),
        );
        assert_eq!(next.unwrap(), prev);
        assert!(!fire);
    }

    // A changed string resets the next fire from now (Python parity), even if
    // the old fire time was closer.
    #[test]
    fn plan_tick_changed_string_reschedules_from_now() {
        let prev = state("0 3 * * *", local(2026, 7, 5, 3, 0, 0));
        let now = local(2026, 7, 4, 13, 0, 0);
        let (next, fire) = plan_tick(Some(prev), Some("0 5 * * *"), now);
        assert_eq!(next.unwrap().next_fire, local(2026, 7, 5, 5, 0, 0));
        assert!(!fire);
    }

    // Reaching the stored fire time fires exactly once and recomputes the
    // next occurrence from now — a large gap does not queue catch-up runs.
    #[test]
    fn plan_tick_fires_once_and_recomputes() {
        let prev = state("0 3 * * *", local(2026, 7, 1, 3, 0, 0));
        // Three days of missed intervals.
        let now = local(2026, 7, 4, 13, 0, 0);
        let (next, fire) = plan_tick(Some(prev), Some("0 3 * * *"), now);
        assert!(fire);
        assert_eq!(next.unwrap().next_fire, local(2026, 7, 5, 3, 0, 0));
    }

    // Firing exactly at the scheduled minute (now == next_fire) triggers.
    #[test]
    fn plan_tick_fires_at_exact_time() {
        let fire_at = local(2026, 7, 5, 3, 0, 0);
        let prev = state("0 3 * * *", fire_at);
        let (next, fire) = plan_tick(Some(prev), Some("0 3 * * *"), fire_at);
        assert!(fire);
        assert_eq!(next.unwrap().next_fire, local(2026, 7, 6, 3, 0, 0));
    }

    // Invalid strings behave like a disabled schedule instead of wedging the
    // scheduler (Python would raise into the ticker every minute).
    #[test]
    fn plan_tick_invalid_string_is_inert() {
        let prev = state("0 3 * * *", local(2026, 7, 5, 3, 0, 0));
        let (next, fire) = plan_tick(
            Some(prev),
            Some("not a cron string"),
            local(2026, 7, 6, 0, 0, 0),
        );
        assert!(next.is_none());
        assert!(!fire);
    }

    // croniter-style inputs the UI and docs mention must all parse.
    #[test]
    fn validate_accepts_croniter_style_patterns() {
        for pattern in [
            "0 3 * * *",
            "*/15 * * * *",
            "@daily",
            "0 4 * * sun",
            "0 4 * * 7",
        ] {
            assert!(
                validate_cron_schedule(pattern).is_ok(),
                "pattern should be valid: {pattern}"
            );
        }
        assert!(validate_cron_schedule("61 3 * * *").is_err());
        assert!(validate_cron_schedule("").is_err());
        assert!(validate_cron_schedule("not a cron string").is_err());
        assert!(next_cron_occurrence("0 3 * * *").is_ok());
        assert!(next_cron_occurrence("not a cron string").is_err());
    }

    // -----------------------------------------------------------------
    // Cron batch building and merging
    // -----------------------------------------------------------------

    /// A registry dump shaped like the inference server's, mapping each
    /// `group/id` to its target entities.
    fn metadata(models: &[(&str, &[&str])]) -> serde_json::Value {
        let mut groups = serde_json::Map::new();
        for (inference_id, entities) in models {
            let (group, short_id) = inference_id.split_once('/').unwrap();
            let model = serde_json::json!({
                "input_spec": { "handler": "test" },
                "target_entities": entities,
            });
            let entry = groups
                .entry(group.to_string())
                .or_insert_with(|| serde_json::json!({ "inference_ids": {} }));
            entry["inference_ids"][short_id] = model;
        }
        serde_json::Value::Object(groups)
    }

    fn config_with(models: &[&str]) -> SystemConfig {
        SystemConfig {
            cron_jobs: models
                .iter()
                .map(|id| CronJob {
                    inference_id: id.to_string(),
                    batch_size: None,
                    threshold: None,
                })
                .collect(),
            ..SystemConfig::default()
        }
    }

    fn batch(
        index_db: &str,
        models: &[&str],
        metadata: Option<&serde_json::Value>,
    ) -> Vec<(CronPhase, JobRequest)> {
        build_cron_requests(
            index_db,
            "user",
            JobType::FolderRescan,
            &config_with(models),
            metadata,
        )
    }

    /// `(index_db, setter-or-"scan")` per merged request, in queue order.
    fn merged_order(batches: Vec<Vec<(CronPhase, JobRequest)>>) -> Vec<(String, String)> {
        merge_cron_batches(batches)
            .into_iter()
            .map(|request| {
                (
                    request.index_db,
                    request
                        .metadata
                        .unwrap_or_else(|| format!("{:?}", request.job_type)),
                )
            })
            .collect()
    }

    // Source-data models (items/files) run before derived-data models;
    // unknown models are dropped; config order is otherwise preserved. (The
    // behaviours the old `order_cron_jobs` covered, now expressed through the
    // build + merge pair.)
    #[test]
    fn sources_run_before_derived_and_unknown_models_are_dropped() {
        let metadata = metadata(&[
            ("derived/a", &["text"]),
            ("src/b", &["items"]),
            ("src/d", &["files"]),
            // Multi-entity models are derived even if they include items.
            ("derived/e", &["items", "text"]),
        ]);
        let order = merged_order(vec![batch(
            "one",
            &["derived/a", "src/b", "missing/c", "src/d", "derived/e"],
            Some(&metadata),
        )]);
        let setters: Vec<&str> = order.iter().map(|(_, setter)| setter.as_str()).collect();
        assert_eq!(
            setters,
            ["FolderRescan", "src/b", "src/d", "derived/a", "derived/e"]
        );
    }

    // Each DB's dependency order survives the merge: its scan first, then its
    // source models, then its derived models — and same-setter jobs from
    // different DBs come out adjacent, which is the point of merging.
    #[test]
    fn merge_groups_setters_while_keeping_each_dbs_dependency_order() {
        let metadata = metadata(&[
            ("src/a", &["items"]),
            ("derived/b", &["text"]),
            ("derived/c", &["text"]),
        ]);
        // DB "two" lists its derived models in the opposite order, so plain
        // phase ordering would interleave them (b1, c1, c2, b2); grouping is
        // what pulls each setter's jobs together.
        let order = merged_order(vec![
            batch("one", &["src/a", "derived/b", "derived/c"], Some(&metadata)),
            batch("two", &["src/a", "derived/c", "derived/b"], Some(&metadata)),
        ]);
        assert_eq!(
            order,
            [
                ("one".into(), "FolderRescan".into()),
                ("two".into(), "FolderRescan".into()),
                ("one".into(), "src/a".into()),
                ("two".into(), "src/a".into()),
                ("one".into(), "derived/b".into()),
                ("two".into(), "derived/b".into()),
                ("one".into(), "derived/c".into()),
                ("two".into(), "derived/c".into()),
            ]
        );

        // Per DB: scan first, then source before derived.
        for db in ["one", "two"] {
            let phases: Vec<&str> = order
                .iter()
                .filter(|(index_db, _)| index_db == db)
                .map(|(_, setter)| match setter.as_str() {
                    "FolderRescan" => "scan",
                    "src/a" => "source",
                    _ => "derived",
                })
                .collect();
            let mut sorted = phases.clone();
            sorted.sort_by_key(|phase| match *phase {
                "scan" => 0,
                "source" => 1,
                _ => 2,
            });
            assert_eq!(phases, sorted, "dependency order broken for {db}");
        }
    }

    // Disjoint setters have nothing to group, so the merge is a pure stable
    // interleave: within each phase the DBs keep their batch order and each
    // DB keeps its config order.
    #[test]
    fn merge_is_stable_for_disjoint_setters() {
        let metadata = metadata(&[
            ("src/a1", &["items"]),
            ("src/a2", &["files"]),
            ("derived/b1", &["text"]),
            ("derived/b2", &["text"]),
        ]);
        let order = merged_order(vec![
            batch("one", &["derived/b1", "src/a1"], Some(&metadata)),
            batch("two", &["src/a2", "derived/b2"], Some(&metadata)),
        ]);
        assert_eq!(
            order,
            [
                ("one".into(), "FolderRescan".into()),
                ("two".into(), "FolderRescan".into()),
                ("one".into(), "src/a1".into()),
                ("two".into(), "src/a2".into()),
                ("one".into(), "derived/b1".into()),
                ("two".into(), "derived/b2".into()),
            ]
        );
    }

    // Without inference metadata nothing is dropped and nothing is reordered:
    // scan first, then config order, exactly as before the merge existed.
    #[test]
    fn metadata_unavailable_keeps_config_order_for_one_db() {
        let order = merged_order(vec![batch(
            "one",
            &["derived/a", "src/b", "missing/c"],
            None,
        )]);
        let setters: Vec<&str> = order.iter().map(|(_, setter)| setter.as_str()).collect();
        assert_eq!(setters, ["FolderRescan", "derived/a", "src/b", "missing/c"]);
    }

    // Same fallback, several DBs: with nothing classified the merge has no
    // dependency information, so it must concatenate the DBs' blocks instead
    // of grouping setters. Grouping would run `b`'s embedding model before the
    // OCR model that feeds it, purely because `a` happens to list them the
    // other way round.
    #[test]
    fn metadata_unavailable_never_permutes_a_dbs_config_order() {
        let order = merged_order(vec![
            batch("a", &["derived/embed", "src/ocr"], None),
            batch("b", &["src/ocr", "derived/embed"], None),
        ]);
        assert_eq!(
            order,
            [
                ("a".into(), "FolderRescan".into()),
                ("b".into(), "FolderRescan".into()),
                ("a".into(), "derived/embed".into()),
                ("a".into(), "src/ocr".into()),
                ("b".into(), "src/ocr".into()),
                ("b".into(), "derived/embed".into()),
            ]
        );
    }

    // Degenerate inputs: nothing in, nothing out.
    #[test]
    fn merge_handles_empty_batches() {
        assert!(merge_cron_batches(Vec::new()).is_empty());
        assert!(merge_cron_batches(vec![Vec::new(), Vec::new()]).is_empty());
        // A DB with no cron models still contributes its scan job.
        let order = merged_order(vec![Vec::new(), batch("one", &[], None), Vec::new()]);
        assert_eq!(order, [("one".to_string(), "FolderRescan".to_string())]);
    }
}
