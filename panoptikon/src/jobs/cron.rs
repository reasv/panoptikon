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
use crate::db::system_config::{CronJob, SystemConfig, SystemConfigStore};
use crate::jobs::extraction::resolve_model_metadata;
use crate::jobs::inference_pool::job_inference_context;
use crate::jobs::queue::{BatchDedup, JobModel, JobRequest, JobType, enqueue_jobs_unless_tagged};

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

/// Splits cron jobs into source-data models (target entities exactly
/// `["items"]` or `["files"]`) followed by derived-data models; a job with no
/// resolved target entities (model unknown to the inference server) is
/// dropped.
fn order_cron_jobs(jobs: Vec<(CronJob, Option<Vec<String>>)>) -> Vec<CronJob> {
    let mut source = Vec::new();
    let mut derived = Vec::new();
    for (job, entities) in jobs {
        match entities {
            None => {}
            Some(entities) if entities == ["items"] || entities == ["files"] => source.push(job),
            Some(_) => derived.push(job),
        }
    }
    source.extend(derived);
    source
}

/// Enqueues the cron job set for `index_db`: a folder rescan first, then one
/// data-extraction job per configured model. The whole batch is enqueued
/// atomically and skipped when a previous cronjob for this DB is still queued
/// or running. Runs regardless of `enable_cron_job` — the manual trigger uses
/// the cron set as "the jobs to run now".
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

async fn run_cronjob_with_scan(
    index_db: &str,
    user_data_db: &str,
    scan_job_type: JobType,
) -> ApiResult<CronRunOutcome> {
    tracing::info!(index_db, "running cronjob");
    let store = SystemConfigStore::from_env();
    let config = store.load(index_db)?;

    let mut requests = vec![cron_request(scan_job_type, index_db, user_data_db, None)];

    let ordered = match job_inference_context().primary.get_metadata().await {
        Ok(metadata) => {
            let annotated = config
                .cron_jobs
                .iter()
                .map(|job| {
                    let entities = match resolve_model_metadata(&metadata, &job.inference_id) {
                        Ok(model) => Some(model.target_entities),
                        Err(_) => {
                            tracing::error!(
                                inference_id = %job.inference_id,
                                index_db,
                                "model is in the cron schedule but not available on the inference server, skipping"
                            );
                            None
                        }
                    };
                    (job.clone(), entities)
                })
                .collect::<Vec<_>>();
            order_cron_jobs(annotated)
        }
        Err(err) => {
            // Unlike Python (which would skip every job and consume the
            // night's schedule), enqueue in config order: extraction jobs
            // re-resolve metadata at execution time, so a temporarily
            // unreachable inference server only costs the ordering.
            tracing::error!(
                error = %err,
                index_db,
                "inference metadata unavailable; enqueueing cron jobs in config order"
            );
            config.cron_jobs.clone()
        }
    };

    for job in ordered {
        let mut request = cron_request(
            JobType::DataExtraction,
            index_db,
            user_data_db,
            Some(job.inference_id),
        );
        request.batch_size = job.batch_size;
        request.threshold = job.threshold;
        requests.push(request);
    }

    let dedup = BatchDedup {
        tag: CRON_TAG.to_string(),
        index_db: index_db.to_string(),
    };
    match enqueue_jobs_unless_tagged(requests, Some(dedup)).await? {
        Some(jobs) => Ok(CronRunOutcome::Enqueued(jobs)),
        None => {
            tracing::info!(
                index_db,
                "a previous cronjob for this index DB is still queued or running, skipping"
            );
            Ok(CronRunOutcome::Skipped)
        }
    }
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

    for index_db in &index_dbs {
        tick_db(state, index_db).await;
    }
}

async fn tick_db(state: &mut CronSchedulerState, index_db: &str) {
    let config = match state.config_store.load(index_db) {
        Ok(config) => config,
        Err(err) => {
            tracing::error!(error = ?err, index_db, "cron scheduler failed to load config");
            return;
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
        // The schedule slot is consumed regardless of the run's outcome,
        // matching Python (run_cronjob swallows its own errors there).
        match run_cronjob(index_db, &db_defaults().1).await {
            Ok(CronRunOutcome::Enqueued(jobs)) => {
                tracing::info!(index_db, jobs = jobs.len(), "cronjob enqueued");
            }
            Ok(CronRunOutcome::Skipped) => {}
            Err(err) => {
                tracing::error!(error = ?err, index_db, "error running cronjob");
            }
        }
        state.last_run.insert(index_db.to_string(), now);
        if let Some(next_fire) = next_fire {
            tracing::info!(index_db, next_run = %next_fire, "next scheduled cron run");
        }
    }

    preload_tick(state, index_db, &config).await;
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

    // Source-data models (items/files) run before derived-data models;
    // unknown models are dropped; config order is otherwise preserved.
    #[test]
    fn order_cron_jobs_sources_first_unknown_dropped() {
        let job = |id: &str| CronJob {
            inference_id: id.to_string(),
            batch_size: None,
            threshold: None,
        };
        let entities = |names: &[&str]| Some(names.iter().map(|s| s.to_string()).collect());
        let ordered = order_cron_jobs(vec![
            (job("derived/a"), entities(&["text"])),
            (job("src/b"), entities(&["items"])),
            (job("missing/c"), None),
            (job("src/d"), entities(&["files"])),
            // Multi-entity models are derived even if they include items.
            (job("derived/e"), entities(&["items", "text"])),
        ]);
        let ids: Vec<&str> = ordered.iter().map(|j| j.inference_id.as_str()).collect();
        assert_eq!(ids, ["src/b", "src/d", "derived/a", "derived/e"]);
    }
}
