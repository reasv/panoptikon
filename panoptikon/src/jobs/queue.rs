use std::collections::{HashMap, VecDeque};

use ractor::{Actor, ActorProcessingErr, ActorRef};
use serde::{Deserialize, Serialize};
use tokio::sync::{OnceCell, oneshot};
use utoipa::ToSchema;

use crate::api_error::ApiError;
use crate::db::index_writer::IndexDbWriterMessage;
use crate::db::index_writer::call_index_db_writer;
use crate::jobs::continuous_scan;
use crate::jobs::extraction;
use crate::jobs::files::FileScanService;
use crate::jobs::vector_quants;

type ApiResult<T> = std::result::Result<T, ApiError>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JobType {
    DataExtraction,
    DataDeletion,
    FolderRescan,
    FolderUpdate,
    JobDataDeletion,
    VectorQuantReconcile,
    /// Deferred per-DB maintenance (recount/ANALYZE/checkpoint, optional
    /// VACUUM). Synthesized by the queue actor at a job boundary; there is
    /// deliberately no API surface to enqueue one.
    DbMaintenance,
    #[cfg(test)]
    #[serde(rename = "test_sleep")]
    TestSleep,
    #[cfg(test)]
    #[serde(rename = "test_panic")]
    TestPanic,
    /// Reports the change summary encoded in `tag` (`"<delay_ms>:<flags>"`,
    /// flags `w` = wrote_data, `d` = deleted_data, `t` = tags_changed), so
    /// boundary scheduling is testable without touching a database.
    #[cfg(test)]
    #[serde(rename = "test_report")]
    TestReport,
}

/// What a finished job changed in its index DB, which is what decides whether
/// deferred maintenance is owed for that DB (and whether it should VACUUM).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ChangeSummary {
    pub wrote_data: bool,
    pub deleted_data: bool,
    /// `tags_items` may have changed, so `tags.item_count` needs rebuilding.
    /// Only the recount is gated on this; it is the one maintenance step
    /// expensive enough to matter and user-visible when skipped (autocomplete
    /// counts and ordering). Deletions imply it — see [`merge_owed`], which
    /// derives it centrally so no producer has to remember.
    pub tags_changed: bool,
}

impl ChangeSummary {
    fn is_empty(self) -> bool {
        !self.wrote_data && !self.deleted_data && !self.tags_changed
    }

    pub(crate) fn or_with(&mut self, other: Self) {
        self.wrote_data |= other.wrote_data;
        self.deleted_data |= other.deleted_data;
        self.tags_changed |= other.tags_changed;
    }

    /// Human-readable flag list carried in the synthesized job's `metadata`,
    /// both for queue display and to tell the maintenance arm what to run.
    fn to_metadata(self) -> String {
        let mut flags = Vec::new();
        if self.wrote_data {
            flags.push("wrote_data");
        }
        if self.deleted_data {
            flags.push("deleted_data");
        }
        if self.tags_changed {
            flags.push("tags_changed");
        }
        flags.join(",")
    }

    fn from_metadata(metadata: Option<&str>) -> Self {
        let mut summary = Self::default();
        for flag in metadata.unwrap_or_default().split(',') {
            match flag.trim() {
                "wrote_data" => summary.wrote_data = true,
                "deleted_data" => summary.deleted_data = true,
                "tags_changed" => summary.tags_changed = true,
                _ => {}
            }
        }
        summary
    }

    /// Every flag set: what the manual trigger enqueues. The recount then runs
    /// unconditionally (that is the point of asking for it), while VACUUM
    /// stays behind its free-page gate so a misclick cannot start a
    /// multi-minute rewrite of a 10 GB database.
    fn all() -> Self {
        Self {
            wrote_data: true,
            deleted_data: true,
            tags_changed: true,
        }
    }
}

/// What a job reports back to the queue when it finishes successfully.
pub(crate) struct JobSuccess {
    pub summary: ChangeSummary,
    /// The batch-cache model the job left loaded, if any. Drives the
    /// boundary's model-continuity rule.
    pub loaded_model: Option<String>,
}

impl JobSuccess {
    fn from_summary(summary: ChangeSummary) -> Self {
        Self {
            summary,
            loaded_model: None,
        }
    }

    fn from_extraction(outcome: extraction::ExtractionOutcome) -> Self {
        Self {
            summary: outcome.summary,
            loaded_model: outcome.loaded_model,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Job {
    pub queue_id: i64,
    pub job_type: JobType,
    pub index_db: String,
    pub user_data_db: String,
    pub metadata: Option<String>,
    pub batch_size: Option<i64>,
    pub threshold: Option<f64>,
    pub log_id: Option<i64>,
    pub tag: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct JobModel {
    pub queue_id: i64,
    pub job_type: JobType,
    pub index_db: String,
    pub metadata: Option<String>,
    pub batch_size: Option<i64>,
    pub threshold: Option<f64>,
    pub log_id: Option<i64>,
    pub running: bool,
    pub tag: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct QueueStatusModel {
    pub queue: Vec<JobModel>,
    /// Bounded, process-local outcomes for jobs that recently left the queue.
    pub outcomes: Vec<JobOutcomeModel>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JobOutcomeStatus {
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct JobOutcomeModel {
    pub queue_id: i64,
    pub status: JobOutcomeStatus,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct JobRequest {
    pub job_type: JobType,
    pub index_db: String,
    pub user_data_db: String,
    pub metadata: Option<String>,
    pub batch_size: Option<i64>,
    pub threshold: Option<f64>,
    pub log_id: Option<i64>,
    pub tag: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct JobRunResult {
    success: bool,
    error: Option<String>,
    /// `None` when the job ended without reporting (cancelled, panicked, or
    /// failed); the boundary then falls back to the pessimistic rule.
    summary: Option<ChangeSummary>,
    /// The batch-cache model the job left loaded, when it reported one.
    loaded_model: Option<String>,
}

impl JobRunResult {
    fn failed(error: String) -> Self {
        Self {
            success: false,
            error: Some(error),
            summary: None,
            loaded_model: None,
        }
    }
}

/// Dedup condition for batch enqueueing, evaluated per index DB: when a queued
/// or running job carries this tag for this index DB, the batch's requests
/// *for that DB* are dropped. Requests for other DBs still enqueue — one batch
/// can carry work for several DBs (the merged cron tick), and one DB's stale
/// run must not hold the others back.
#[derive(Debug, Clone)]
pub(crate) struct BatchDedup {
    pub tag: String,
    pub index_db: String,
}

/// Outcome of an [`JobQueueMessage::EnqueueBatch`]: the jobs that made it into
/// the queue, plus the index DBs whose requests a dedup entry dropped.
#[derive(Debug, Clone, Default)]
pub(crate) struct BatchEnqueueResult {
    pub enqueued: Vec<JobModel>,
    pub skipped_dbs: Vec<String>,
}

impl BatchEnqueueResult {
    /// Whether `index_db`'s requests were dropped by a dedup conflict.
    pub(crate) fn was_skipped(&self, index_db: &str) -> bool {
        self.skipped_dbs.iter().any(|db| db == index_db)
    }
}

impl JobModel {
    fn from_job(job: &Job, running: bool) -> Self {
        Self {
            queue_id: job.queue_id,
            job_type: job.job_type.clone(),
            index_db: job.index_db.clone(),
            metadata: job.metadata.clone(),
            batch_size: job.batch_size,
            threshold: job.threshold,
            log_id: job.log_id,
            running,
            tag: job.tag.clone(),
        }
    }
}

pub(crate) enum JobQueueMessage {
    Enqueue {
        request: JobRequest,
        reply: oneshot::Sender<ApiResult<JobModel>>,
    },
    /// Atomic enqueue of several jobs, with per-DB dedup checks evaluated
    /// inside the actor (a check-then-enqueue done by the caller would race
    /// concurrent triggers). A conflicting [`BatchDedup`] drops only the
    /// requests whose `index_db` matches it; everything else still enqueues.
    EnqueueBatch {
        requests: Vec<JobRequest>,
        dedups: Vec<BatchDedup>,
        reply: oneshot::Sender<ApiResult<BatchEnqueueResult>>,
    },
    /// The one narrow way to enqueue a `DbMaintenance` job from outside the
    /// boundary (`POST /api/jobs/maintenance`): all flags set, back of the
    /// queue, `Ok(None)` when a maintenance job for that DB is already queued
    /// or running. Deliberately not reachable through `JobRequest` — every
    /// other maintenance job exists because the boundary synthesized it and
    /// cleared owed flags for it, and that pairing must stay closed.
    EnqueueMaintenance {
        index_db: String,
        user_data_db: String,
        reply: oneshot::Sender<ApiResult<Option<JobModel>>>,
    },
    GetQueueStatus {
        reply: oneshot::Sender<ApiResult<QueueStatusModel>>,
    },
    /// `suppress_maintenance` stops *this* cancel's boundary from synthesizing
    /// a maintenance job; the owed flags are kept for the next boundary.
    CancelQueued {
        queue_ids: Vec<i64>,
        suppress_maintenance: bool,
        reply: oneshot::Sender<ApiResult<Vec<i64>>>,
    },
    CancelRunning {
        suppress_maintenance: bool,
        reply: oneshot::Sender<ApiResult<Option<i64>>>,
    },
    RunnerFinished {
        queue_id: i64,
        result: JobRunResult,
    },
    /// Fire-and-forget report of work a *still running* job has already
    /// committed, so the owed flags survive that job later failing or being
    /// cancelled. Never schedules maintenance by itself — the job that sent it
    /// is still running, and its own boundary will pick the flags up.
    RecordOwed {
        index_db: String,
        summary: ChangeSummary,
    },
    /// Process shutdown: drops every queued job, cancels the running one, and
    /// puts the queue into a mode where new enqueues are refused — an HTTP
    /// request still in flight during the graceful drain must not start a job
    /// that would then be killed with the process. Replies with the cancelled
    /// running job's id, if any.
    Shutdown {
        reply: oneshot::Sender<Option<i64>>,
    },
    /// Test-only snapshot of the boundary bookkeeping, so tests can observe
    /// synthesis decisions without racing the synthesized job's execution.
    #[cfg(test)]
    DebugState {
        reply: oneshot::Sender<QueueDebugState>,
    },
}

#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub(crate) struct QueueDebugState {
    owed: HashMap<String, ChangeSummary>,
    synthesized: Vec<(String, ChangeSummary)>,
    batch_loaded: Option<String>,
    /// Setters the boundary decided to unload, in order. Recorded instead of
    /// performed under `cfg(test)`: queue tests run without an inference
    /// context, and the decision is what these tests are about.
    unloads: Vec<String>,
    /// How many times the actor has tried to start the next job. Lets a test
    /// see that a no-op enqueue really did not touch the queue, which is
    /// otherwise indistinguishable from a `start_next_job` that found nothing
    /// to do.
    start_attempts: usize,
    /// Index DBs the boundary cast a set-marker message for, in order.
    /// Recorded instead of sent under `cfg(test)`, like `unloads`: the queue
    /// tests use index DBs that do not exist, and spawning a writer for one
    /// would create a stray database.
    marked_dirty: Vec<String>,
}

pub(crate) struct JobQueueActor;

pub(crate) struct JobQueueArgs {
    pub runner_name: Option<String>,
}

pub(crate) struct JobQueueState {
    queue: VecDeque<Job>,
    queued_jobs: HashMap<i64, Job>,
    running_job: Option<Job>,
    outcomes: VecDeque<JobOutcomeModel>,
    job_counter: i64,
    runner: ActorRef<JobRunnerMessage>,
    shutting_down: bool,
    /// Per-index-DB maintenance owed by jobs that already finished. Cleared
    /// when a maintenance job is synthesized for that DB; dies with the
    /// process (the queue is not persistent, by design).
    owed: HashMap<String, ChangeSummary>,
    /// The setter currently loaded under the inferio `batch` cache key, as far
    /// as the queue knows. Exact rather than approximate: batch loads use
    /// `lru_size = 1`, so loading a setter evicts whatever was there.
    batch_loaded: Option<String>,
    #[cfg(test)]
    synthesized: Vec<(String, ChangeSummary)>,
    #[cfg(test)]
    unloads: Vec<String>,
    #[cfg(test)]
    start_attempts: usize,
    #[cfg(test)]
    marked_dirty: Vec<String>,
}

pub(crate) enum JobRunnerMessage {
    RunJob {
        job: Job,
        reply: oneshot::Sender<ApiResult<()>>,
    },
    CancelRunning {
        reply: oneshot::Sender<ApiResult<Option<i64>>>,
    },
    /// Sent by the watcher task when the job task finishes (success, error,
    /// panic, or abort). The runner clears its own busy state *before*
    /// forwarding completion to the queue, so the follow-up `RunJob` from the
    /// queue can never race a stale busy state.
    JobCompleted { queue_id: i64, result: JobRunResult },
}

pub(crate) struct JobRunnerActor;

pub(crate) struct JobRunnerArgs {
    pub queue: ActorRef<JobQueueMessage>,
}

pub(crate) struct JobRunnerState {
    queue: ActorRef<JobQueueMessage>,
    running: Option<RunningJob>,
}

struct RunningJob {
    queue_id: i64,
    abort: tokio::task::AbortHandle,
}

impl Actor for JobQueueActor {
    type Msg = JobQueueMessage;
    type State = JobQueueState;
    type Arguments = JobQueueArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let runner_name = args.runner_name;
        let (runner, _handle) = Actor::spawn(
            runner_name,
            JobRunnerActor,
            JobRunnerArgs {
                queue: myself.clone(),
            },
        )
        .await
        .map_err(|err| {
            tracing::error!(error = ?err, "failed to start job runner");
            ActorProcessingErr::from("failed to start job runner")
        })?;
        Ok(JobQueueState {
            queue: VecDeque::new(),
            queued_jobs: HashMap::new(),
            running_job: None,
            outcomes: VecDeque::new(),
            job_counter: 0,
            runner,
            shutting_down: false,
            owed: HashMap::new(),
            batch_loaded: None,
            #[cfg(test)]
            synthesized: Vec::new(),
            #[cfg(test)]
            unloads: Vec::new(),
            #[cfg(test)]
            start_attempts: 0,
            #[cfg(test)]
            marked_dirty: Vec::new(),
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            JobQueueMessage::Enqueue { request, reply } => {
                if state.shutting_down {
                    let _ = reply.send(Err(ApiError::internal("Job queue is shutting down")));
                    return Ok(());
                }
                let model = push_job(state, request);
                if state.running_job.is_none() {
                    start_next_job(state).await;
                }
                let _ = reply.send(Ok(model));
            }
            JobQueueMessage::EnqueueBatch {
                requests,
                dedups,
                reply,
            } => {
                if state.shutting_down {
                    let _ = reply.send(Err(ApiError::internal("Job queue is shutting down")));
                    return Ok(());
                }
                let mut skipped_dbs: Vec<String> = Vec::new();
                for dedup in &dedups {
                    let conflict = state
                        .running_job
                        .iter()
                        .chain(state.queue.iter())
                        .any(|job| {
                            job.tag.as_deref() == Some(dedup.tag.as_str())
                                && job.index_db == dedup.index_db
                        });
                    if conflict && !skipped_dbs.contains(&dedup.index_db) {
                        skipped_dbs.push(dedup.index_db.clone());
                    }
                }
                let enqueued: Vec<JobModel> = requests
                    .into_iter()
                    .filter(|request| !skipped_dbs.contains(&request.index_db))
                    .map(|request| push_job(state, request))
                    .collect();
                // A batch that enqueued nothing leaves the queue exactly as it
                // found it, as the all-or-nothing skip path always did.
                if !enqueued.is_empty() && state.running_job.is_none() {
                    start_next_job(state).await;
                }
                let _ = reply.send(Ok(BatchEnqueueResult {
                    enqueued,
                    skipped_dbs,
                }));
            }
            JobQueueMessage::EnqueueMaintenance {
                index_db,
                user_data_db,
                reply,
            } => {
                if state.shutting_down {
                    let _ = reply.send(Err(ApiError::internal("Job queue is shutting down")));
                    return Ok(());
                }
                // Dedup inside the actor for the same reason batch dedup is
                // here: a check-then-enqueue by the caller would race a
                // boundary synthesizing one between the two steps.
                let pending = state
                    .running_job
                    .iter()
                    .chain(state.queue.iter())
                    .any(|job| job.job_type == JobType::DbMaintenance && job.index_db == index_db);
                if pending {
                    let _ = reply.send(Ok(None));
                    return Ok(());
                }
                let model =
                    push_maintenance_job(state, &index_db, &user_data_db, ChangeSummary::all());
                tracing::info!(
                    index_db,
                    queue_id = model.queue_id,
                    "enqueued database maintenance on request"
                );
                if state.running_job.is_none() {
                    start_next_job(state).await;
                }
                let _ = reply.send(Ok(Some(model)));
            }
            JobQueueMessage::GetQueueStatus { reply } => {
                let mut queue = Vec::new();
                if let Some(running) = state.running_job.as_ref() {
                    queue.push(JobModel::from_job(running, true));
                }
                for job in state.queue.iter() {
                    queue.push(JobModel::from_job(job, false));
                }
                let _ = reply.send(Ok(QueueStatusModel {
                    queue,
                    outcomes: state.outcomes.iter().cloned().collect(),
                }));
            }
            JobQueueMessage::CancelQueued {
                queue_ids,
                suppress_maintenance,
                reply,
            } => {
                let mut cancelled = Vec::new();
                let mut removed = Vec::new();
                // Queued ids first, running id last. Cancelling the running job
                // starts the next queued one, so interleaving the two would
                // promote-then-abort every job in the list — and each of those
                // aborts would contribute pessimistic owed flags for a job that
                // never executed a single instruction.
                let mut running_target = None;
                for queue_id in queue_ids {
                    if state
                        .running_job
                        .as_ref()
                        .is_some_and(|running| running.queue_id == queue_id)
                    {
                        running_target = Some(queue_id);
                        continue;
                    }
                    if let Some(job) = state.queued_jobs.remove(&queue_id) {
                        state.queue.retain(|entry| entry.queue_id != queue_id);
                        record_outcome(state, job.queue_id, JobOutcomeStatus::Cancelled, None);
                        cancelled.push(job.queue_id);
                        removed.push(job);
                    }
                }
                if let Some(queue_id) = running_target {
                    // Only report it cancelled if the runner actually confirmed
                    // the cancellation.
                    if cancel_running_job_inner(state, suppress_maintenance).await == Some(queue_id)
                    {
                        cancelled.push(queue_id);
                    }
                }
                // Removing the last queued job for a DB is a boundary too: the
                // maintenance its predecessors owe has nothing left to wait for.
                if !suppress_maintenance {
                    for job in &removed {
                        maybe_schedule_maintenance(state, job);
                    }
                }
                // Unreachable in normal operation: `batch_loaded` is only
                // `Some` while a job runs, because every boundary either
                // unloads or immediately starts the job that keeps the model
                // warm — and this decision no-ops while a job runs. Kept as
                // belt-and-braces for the degenerate state where
                // `start_next_job` could not hand a job to the runner and left
                // the queue with nothing running.
                maybe_unload_batch_model(state);
                // Unconditional: a suppressed cancel must still not leave the
                // queue holding work while the runner sits idle.
                start_next_job(state).await;
                let _ = reply.send(Ok(cancelled));
            }
            JobQueueMessage::CancelRunning {
                suppress_maintenance,
                reply,
            } => {
                let result = cancel_running_job_inner(state, suppress_maintenance).await;
                let _ = reply.send(Ok(result));
            }
            JobQueueMessage::RunnerFinished { queue_id, result } => {
                if let Some(running) = state.running_job.as_ref() {
                    if running.queue_id == queue_id {
                        let finished = running.clone();
                        let error = result.error.clone();
                        record_outcome(
                            state,
                            queue_id,
                            if result.success {
                                JobOutcomeStatus::Completed
                            } else {
                                JobOutcomeStatus::Failed
                            },
                            error.clone(),
                        );
                        if !result.success {
                            tracing::error!(
                                error = %error.unwrap_or_else(|| "unknown job error".to_string()),
                                queue_id,
                                "job failed"
                            );
                        }
                        state.running_job = None;
                        record_owed(state, &finished, result.summary);
                        record_batch_load(state, &finished, result.success, result.loaded_model);
                        // Before starting the next job, so the synthesized
                        // job is in the queue the two decisions below read.
                        // It goes to the *back*, so a drained queue runs it
                        // now and a busy one runs it after everything else.
                        maybe_schedule_maintenance(state, &finished);
                        // After synthesis so the model-continuity rule reads
                        // the queue it will actually run. Belt-and-braces
                        // rather than load-bearing: `next_batch_setter` skips
                        // `DbMaintenance`, so today the decision is the same
                        // either way and no test can tell the orders apart.
                        maybe_unload_batch_model(state);
                        start_next_job(state).await;
                    }
                }
            }
            JobQueueMessage::RecordOwed { index_db, summary } => {
                // Structural guard for the invariant "owed[X] cannot be re-set
                // while a maintenance job runs". The message carries no job
                // type, so the running job is the only thing that can say where
                // it came from: a report from inside a maintenance pass would
                // repopulate the flags that pass was synthesized to clear, and
                // its own boundary would then synthesize a replacement — an
                // unbounded synthesize/record loop.
                if state
                    .running_job
                    .as_ref()
                    .is_some_and(|running| running.job_type == JobType::DbMaintenance)
                {
                    tracing::warn!(
                        index_db,
                        "ignoring an owed report made while maintenance is running"
                    );
                    return Ok(());
                }
                merge_owed(state, &index_db, summary);
            }
            JobQueueMessage::Shutdown { reply } => {
                state.shutting_down = true;
                let dropped = state.queue.len();
                state.queue.clear();
                state.queued_jobs.clear();
                if dropped > 0 {
                    tracing::info!(dropped, "dropped queued jobs for shutdown");
                }
                // Owed maintenance dies with the process (design: accepted
                // performance-only staleness), so shutdown never synthesizes.
                let cancelled = cancel_running_job_inner(state, true).await;
                // The batch model does not die with the process: desktop
                // shutdown leaves the inference workers running for a moment.
                // This is where the cancelled job's model is unloaded (the
                // cancel path defers to here while shutting down), waited on
                // with a short timeout rather than detached.
                unload_batch_model_on_shutdown(state).await;
                let _ = reply.send(cancelled);
            }
            #[cfg(test)]
            JobQueueMessage::DebugState { reply } => {
                let _ = reply.send(QueueDebugState {
                    owed: state.owed.clone(),
                    synthesized: state.synthesized.clone(),
                    batch_loaded: state.batch_loaded.clone(),
                    unloads: state.unloads.clone(),
                    start_attempts: state.start_attempts,
                    marked_dirty: state.marked_dirty.clone(),
                });
            }
        }
        Ok(())
    }
}

fn push_job(state: &mut JobQueueState, request: JobRequest) -> JobModel {
    state.job_counter += 1;
    let job = Job {
        queue_id: state.job_counter,
        job_type: request.job_type,
        index_db: request.index_db,
        user_data_db: request.user_data_db,
        metadata: request.metadata,
        batch_size: request.batch_size,
        threshold: request.threshold,
        log_id: request.log_id,
        tag: request.tag,
    };
    let model = JobModel::from_job(&job, false);
    state.queue.push_back(job.clone());
    state.queued_jobs.insert(job.queue_id, job);
    model
}

fn record_outcome(
    state: &mut JobQueueState,
    queue_id: i64,
    status: JobOutcomeStatus,
    error: Option<String>,
) {
    const MAX_RECENT_OUTCOMES: usize = 256;
    state.outcomes.push_back(JobOutcomeModel {
        queue_id,
        status,
        error,
    });
    while state.outcomes.len() > MAX_RECENT_OUTCOMES {
        state.outcomes.pop_front();
    }
}

async fn start_next_job(state: &mut JobQueueState) {
    #[cfg(test)]
    {
        state.start_attempts += 1;
    }
    if state.shutting_down || state.running_job.is_some() {
        return;
    }
    let job = match state.queue.pop_front() {
        Some(job) => job,
        None => return,
    };
    state.queued_jobs.remove(&job.queue_id);
    let (reply, rx) = oneshot::channel();
    if state
        .runner
        .send_message(JobRunnerMessage::RunJob {
            job: job.clone(),
            reply,
        })
        .is_err()
    {
        tracing::error!(queue_id = job.queue_id, "job runner unavailable");
        record_outcome(
            state,
            job.queue_id,
            JobOutcomeStatus::Failed,
            Some("job runner unavailable".into()),
        );
        return;
    }
    match rx.await {
        Ok(Ok(())) => {
            state.running_job = Some(job);
        }
        Ok(Err(err)) => {
            tracing::error!(error = ?err, queue_id = job.queue_id, "job runner rejected job");
            record_outcome(
                state,
                job.queue_id,
                JobOutcomeStatus::Failed,
                Some(format!("{err:?}")),
            );
        }
        Err(_) => {
            tracing::error!(queue_id = job.queue_id, "job runner dropped response");
            record_outcome(
                state,
                job.queue_id,
                JobOutcomeStatus::Failed,
                Some("job runner dropped its response".into()),
            );
        }
    }
}

async fn cancel_running_job_inner(
    state: &mut JobQueueState,
    suppress_maintenance: bool,
) -> Option<i64> {
    let running = state.running_job.clone()?;
    let (reply, rx) = oneshot::channel();
    if state
        .runner
        .send_message(JobRunnerMessage::CancelRunning { reply })
        .is_err()
    {
        return None;
    }
    match rx.await {
        Ok(Ok(Some(queue_id))) => {
            if running.queue_id == queue_id {
                state.running_job = None;
                record_outcome(state, queue_id, JobOutcomeStatus::Cancelled, None);
                // A cancelled job reports nothing, so the owed flags are
                // pessimistic — but the maintenance job itself can be
                // suppressed by the caller (this cancel only).
                record_owed(state, &running, None);
                // A cancelled extraction job reports nothing either, so the
                // model tracking is pessimistic too: it may have loaded.
                record_unreported_batch_load(state, &running);
                if !suppress_maintenance {
                    maybe_schedule_maintenance(state, &running);
                }
                // Not gated on `suppress_maintenance`: that flag is about the
                // maintenance job only. During shutdown the `Shutdown` handler
                // does it instead, awaited — a task detached here as the
                // runtime tears down would usually never be polled.
                if !state.shutting_down {
                    maybe_unload_batch_model(state);
                }
                start_next_job(state).await;
                Some(queue_id)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Folds a finished job's change report into the owed flags for its index DB.
/// A job that ended without reporting (cancelled, panicked, failed) is treated
/// pessimistically: it wrote something, and scan/deletion jobs may already have
/// cascaded deletes. Maintenance jobs never owe maintenance.
fn pessimistic_summary(job_type: &JobType) -> ChangeSummary {
    match job_type {
        // Writes only the quant tables, which recount/ANALYZE/VACUUM do not
        // serve. It never ran post-job maintenance before the boundary existed
        // and must not start owing it now — an aborted reconcile scheduling a
        // full ANALYZE over a 10 GB index is exactly the starvation this
        // design set out to avoid.
        JobType::VectorQuantReconcile => ChangeSummary::default(),
        _ => {
            let deleting = matches!(
                job_type,
                JobType::FolderRescan
                    | JobType::FolderUpdate
                    | JobType::DataDeletion
                    | JobType::JobDataDeletion
            );
            ChangeSummary {
                wrote_data: true,
                deleted_data: deleting,
                // Same set as `deleted_data`, and for the same reason: a
                // cancelled scan may already have cascaded item deletions
                // into `tags_items`. A cancelled *tagging* job needs nothing
                // here — its writes set the durable marker as they commit.
                tags_changed: deleting,
            }
        }
    }
}

fn record_owed(state: &mut JobQueueState, job: &Job, summary: Option<ChangeSummary>) {
    if job.job_type == JobType::DbMaintenance {
        return;
    }
    let summary = summary.unwrap_or_else(|| pessimistic_summary(&job.job_type));
    merge_owed(state, &job.index_db, summary);
}

fn merge_owed(state: &mut JobQueueState, index_db: &str, mut summary: ChangeSummary) {
    // Deleting items and item data cascades into `tags_items`, so anything
    // that reports a deletion also dirties the tag counts. Derived here — the
    // one funnel every reported and pessimistic summary passes through —
    // rather than at each producer, where it would be a rule to remember.
    summary.tags_changed |= summary.deleted_data;
    if summary.is_empty() {
        return;
    }
    let entry = state.owed.entry(index_db.to_string()).or_default();
    let already_dirty = entry.tags_changed;
    entry.or_with(summary);
    if entry.tags_changed && !already_dirty {
        mark_tags_dirty(state, index_db);
    }
}

/// Mirrors a newly owed `tags_changed` into the DB's durable marker, so the
/// recount survives the process losing its in-memory flags. The writer-side
/// sets (tag writes, orphan-item deletions) are the primary guarantee; this
/// covers what they cannot see — job-end bulk deletions, which the job reports
/// as a count rather than writing row by row.
///
/// Fire-and-forget, and only on the false→true transition: a lost cast costs
/// at most one skipped recount, which the next change re-owes.
#[cfg(not(test))]
fn mark_tags_dirty(_state: &mut JobQueueState, index_db: &str) {
    let index_db = index_db.to_string();
    tokio::spawn(async move {
        if let Err(err) = call_index_db_writer(&index_db, |reply| {
            IndexDbWriterMessage::MarkTagsDirty { reply }
        })
        .await
        {
            tracing::warn!(error = ?err, index_db, "could not set the tags-dirty marker");
        }
    });
}

/// Recorded, not sent: queue tests drive the boundary against index DBs that
/// do not exist, and spawning a writer for one would create a stray database
/// (the same reason `run_db_maintenance` is stubbed here).
#[cfg(test)]
fn mark_tags_dirty(state: &mut JobQueueState, index_db: &str) {
    state.marked_dirty.push(index_db.to_string());
}

/// The job boundary: when nothing else in the queue targets the finished job's
/// index DB, the maintenance its finished jobs owe is synthesized as a real
/// queue job at the *back* of the queue (visible, cancellable, and serialized
/// against other jobs like everything else).
///
/// Back, not front: maintenance can run for minutes (VACUUM, and a recount
/// that scales with `tags_items`), and inserting that between two GPU jobs
/// stalls the pipeline and outlives the 60 s model TTL that makes cross-job
/// model reuse work. At the back it accumulates at the tail of a drain and
/// runs after the last extraction. The cost accepted is that an
/// early-finishing DB's statistics and WAL truncation wait for the drain to
/// end. A drained queue is unaffected: the job starts immediately either way.
fn maybe_schedule_maintenance(state: &mut JobQueueState, finished: &Job) {
    if state.shutting_down {
        return;
    }
    let index_db = finished.index_db.as_str();
    let Some(owed) = state.owed.get(index_db).copied() else {
        return;
    };
    if owed.is_empty() {
        state.owed.remove(index_db);
        return;
    }
    let db_busy = state
        .running_job
        .iter()
        .chain(state.queue.iter())
        .any(|job| job.index_db == index_db);
    if db_busy {
        return;
    }
    state.owed.remove(index_db);
    let model = push_maintenance_job(state, index_db, &finished.user_data_db, owed);
    tracing::info!(
        index_db,
        queue_id = model.queue_id,
        owed = %owed.to_metadata(),
        "scheduling deferred database maintenance"
    );
    #[cfg(test)]
    state.synthesized.push((finished.index_db.clone(), owed));
}

/// Appends a `DbMaintenance` job carrying `flags` in its `metadata` (both the
/// queue's display text and how the maintenance arm learns what to run).
/// Shared by the boundary and the manual trigger; neither owns the queue slot
/// decision, both append.
fn push_maintenance_job(
    state: &mut JobQueueState,
    index_db: &str,
    user_data_db: &str,
    flags: ChangeSummary,
) -> JobModel {
    state.job_counter += 1;
    let job = Job {
        queue_id: state.job_counter,
        job_type: JobType::DbMaintenance,
        index_db: index_db.to_string(),
        user_data_db: user_data_db.to_string(),
        metadata: Some(flags.to_metadata()),
        batch_size: None,
        threshold: None,
        log_id: None,
        tag: None,
    };
    let model = JobModel::from_job(&job, false);
    state.queue.push_back(job.clone());
    state.queued_jobs.insert(job.queue_id, job);
    model
}

/// The setter the next batch extraction in the queue will load, if the queue
/// starts with one. `DbMaintenance` jobs are skipped over — a synthesized
/// maintenance pass between two jobs for the same setter must not cost a model
/// reload — but nothing else is: any other job type means real work (a scan
/// can run for hours) stands between here and the next extraction, and the
/// model should not sit in VRAM through it.
fn next_batch_setter(queue: &VecDeque<Job>) -> Option<&str> {
    for job in queue {
        match job.job_type {
            JobType::DbMaintenance => continue,
            JobType::DataExtraction => return job.metadata.as_deref(),
            _ => return None,
        }
    }
    None
}

/// Folds a finished job's model report into `batch_loaded`. A job that ended
/// without reporting is treated like a cancel (see
/// [`record_unreported_batch_load`]); a job that reported "loaded nothing"
/// (the no-data early return) leaves the previous model tracked, because it
/// did not evict it.
fn record_batch_load(
    state: &mut JobQueueState,
    job: &Job,
    success: bool,
    loaded_model: Option<String>,
) {
    match loaded_model {
        Some(setter) => state.batch_loaded = Some(setter),
        None if !success => record_unreported_batch_load(state, job),
        None => {}
    }
}

fn record_unreported_batch_load(state: &mut JobQueueState, job: &Job) {
    if let Some(setter) = unreported_batch_load(job) {
        state.batch_loaded = Some(setter);
    }
}

/// An extraction job that ended without reporting (cancelled, failed, panicked)
/// may have loaded its model before dying, and `setter_name == inference_id ==
/// metadata`. Assume it did: the cost of being wrong is one no-op unload call,
/// while the cost of not tracking it is a model left in VRAM until the TTL
/// sweep. Any other job type loads nothing under the batch cache key.
fn unreported_batch_load(job: &Job) -> Option<String> {
    if job.job_type != JobType::DataExtraction {
        return None;
    }
    job.metadata.clone()
}

/// The model-continuity half of the boundary: the batch model stays loaded
/// exactly when the next extraction in the queue wants the same setter.
/// Returns the setter to unload (and stops tracking it) otherwise.
fn take_batch_unload(state: &mut JobQueueState) -> Option<String> {
    // Only a real boundary: while a job runs, `batch_loaded` describes a model
    // that job may itself be using (or have already evicted).
    if state.running_job.is_some() {
        return None;
    }
    let loaded = state.batch_loaded.clone()?;
    if next_batch_setter(&state.queue) == Some(loaded.as_str()) {
        return None;
    }
    state.batch_loaded = None;
    tracing::info!(setter = %loaded, "unloading batch model at job boundary");
    Some(loaded)
}

/// Unloads without blocking the actor on an HTTP round trip — a lost call is
/// covered by the inferio TTL sweep, the same backstop that has always been
/// the real guarantee here.
fn maybe_unload_batch_model(state: &mut JobQueueState) {
    let Some(setter) = take_batch_unload(state) else {
        return;
    };
    if record_test_unload(state, &setter) {
        return;
    }
    // The generation is captured *here*, in the actor, so a load that starts
    // after this point invalidates the unload rather than being undone by it.
    tokio::spawn(unload_batch_model(
        setter,
        extraction::batch_load_generation(),
    ));
}

/// Shutdown variant: waits, briefly. A task detached while the runtime tears
/// down is usually never polled, and the inference workers outlive the process
/// by long enough for the unload to matter (desktop quit).
async fn unload_batch_model_on_shutdown(state: &mut JobQueueState) {
    const SHUTDOWN_UNLOAD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
    let Some(setter) = take_batch_unload(state) else {
        return;
    };
    if record_test_unload(state, &setter) {
        return;
    }
    let generation = extraction::batch_load_generation();
    let _ = tokio::time::timeout(
        SHUTDOWN_UNLOAD_TIMEOUT,
        unload_batch_model(setter, generation),
    )
    .await;
}

async fn unload_batch_model(setter: String, generation: u64) {
    // Absent when no inference endpoints were ever configured; then there is
    // nothing loaded and nothing to unload.
    let Some(context) = crate::jobs::inference_pool::try_job_inference_context() else {
        return;
    };
    let _slot = extraction::lock_batch_slot().await;
    if !extraction::batch_unload_is_current(generation) {
        // A newer job loaded a batch model while this unload was in flight.
        // Landing now would kill everything queued on that model's dispatcher.
        tracing::debug!(setter = %setter, "skipping a stale batch model unload");
        return;
    }
    let _ = context
        .pool
        .unload_model_all(&setter, extraction::CACHE_KEY)
        .await;
}

/// Always `false`: the unload really happens.
#[cfg(not(test))]
fn record_test_unload(_state: &mut JobQueueState, _setter: &str) -> bool {
    false
}

/// Queue tests have neither an inference context nor an inference server, and
/// the *decision* is what they assert — so it is recorded instead of performed.
#[cfg(test)]
fn record_test_unload(state: &mut JobQueueState, setter: &str) -> bool {
    state.unloads.push(setter.to_string());
    true
}

impl Actor for JobRunnerActor {
    type Msg = JobRunnerMessage;
    type State = JobRunnerState;
    type Arguments = JobRunnerArgs;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(JobRunnerState {
            queue: args.queue,
            running: None,
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            JobRunnerMessage::RunJob { job, reply } => {
                if state.running.is_some() {
                    let _ = reply.send(Err(ApiError::internal("Job runner busy")));
                    return Ok(());
                }
                let queue_id = job.queue_id;
                let inner = tokio::spawn(execute_job(job));
                let abort = inner.abort_handle();
                // Watcher task: observes the job no matter how it ends
                // (return, panic, or abort) and reports through the runner,
                // so the busy state is always cleared and a panicking job
                // cannot wedge the queue.
                let runner = myself.clone();
                tokio::spawn(async move {
                    let result = match inner.await {
                        Ok(Ok(success)) => JobRunResult {
                            success: true,
                            error: None,
                            summary: Some(success.summary),
                            loaded_model: success.loaded_model,
                        },
                        Ok(Err(err)) => JobRunResult::failed(err),
                        Err(join_err) if join_err.is_cancelled() => {
                            JobRunResult::failed("Job cancelled".to_string())
                        }
                        Err(join_err) => {
                            JobRunResult::failed(format!("Job panicked: {join_err}"))
                        }
                    };
                    let _ =
                        runner.send_message(JobRunnerMessage::JobCompleted { queue_id, result });
                });
                state.running = Some(RunningJob { queue_id, abort });
                let _ = reply.send(Ok(()));
            }
            JobRunnerMessage::CancelRunning { reply } => {
                if let Some(running) = state.running.take() {
                    running.abort.abort();
                    // The watcher still delivers JobCompleted for the aborted
                    // task; by then this id is stale and gets ignored.
                    let _ = reply.send(Ok(Some(running.queue_id)));
                } else {
                    let _ = reply.send(Ok(None));
                }
            }
            JobRunnerMessage::JobCompleted { queue_id, result } => {
                let matches = state
                    .running
                    .as_ref()
                    .is_some_and(|running| running.queue_id == queue_id);
                if matches {
                    state.running = None;
                }
                // Clear the busy state before the queue learns of completion:
                // the queue reacts by immediately sending the next RunJob.
                // Stale ids (cancelled jobs) are forwarded too; the queue
                // ignores completions for jobs it no longer tracks.
                let _ = state
                    .queue
                    .send_message(JobQueueMessage::RunnerFinished { queue_id, result });
            }
        }
        Ok(())
    }
}

/// The deferred maintenance pass, gated by the flags the boundary recorded in
/// `metadata`. Boxed at the call site: it opens connections and is otherwise
/// inlined into `execute_job`'s state machine.
#[cfg(not(test))]
async fn run_db_maintenance(job: &Job) {
    let summary = ChangeSummary::from_metadata(job.metadata.as_deref());
    // Paused for the same reason every write-heavy job pauses: a VACUUM must
    // not stall continuous-scan writes mid-flight.
    let guard = match continuous_scan::pause_for_job_guarded(&job.index_db).await {
        Ok(guard) => Some(guard),
        Err(err) => {
            tracing::warn!(
                error = ?err,
                index_db = %job.index_db,
                "could not pause continuous scan for maintenance; running it anyway"
            );
            None
        }
    };
    crate::jobs::files::run_post_job_maintenance(
        &job.index_db,
        summary.deleted_data,
        summary.tags_changed,
    )
    .await;
    if let Some(guard) = guard {
        guard.resume().await;
    }
}

/// Queue tests drive the boundary against index DBs that do not exist. Running
/// the real pass there would create stray databases and pull the
/// continuous-scan supervisor into every test; the scheduling decision itself
/// is asserted through `QueueDebugState`.
///
/// The stub honors a per-index-DB delay so a maintenance job can be observed
/// *running* (and cancelled there). Keyed by DB rather than global so it stays
/// scoped to the one test that registers it.
#[cfg(test)]
async fn run_db_maintenance(job: &Job) {
    let delay = test_maintenance_delays()
        .lock()
        .expect("test maintenance delay registry poisoned")
        .get(&job.index_db)
        .copied();
    if let Some(millis) = delay {
        tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
    }
}

#[cfg(test)]
fn test_maintenance_delays() -> &'static std::sync::Mutex<HashMap<String, u64>> {
    static DELAYS: std::sync::OnceLock<std::sync::Mutex<HashMap<String, u64>>> =
        std::sync::OnceLock::new();
    DELAYS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Always `None`: extraction jobs run their real body. (Under `cfg(test)` the
/// stub below takes over — the seam is here rather than around the whole arm so
/// that the real call stays compiled in test builds, which is what keeps the
/// extraction module from looking dead to the compiler.)
#[cfg(not(test))]
async fn extraction_stub(_job: &Job) -> Option<Result<JobSuccess, String>> {
    None
}

/// The boundary's model-continuity rule is about real `DataExtraction` jobs, so
/// the queue tests enqueue real ones — but the real body needs an inference
/// server and a populated database. The stub reports what matters: the setter
/// it "loaded", which is the job's `metadata`, exactly as the real job does
/// (`setter_name == inference_id == metadata`). Tag: `"<delay_ms>"`, plus
/// `":noload"` for the no-data early return (which loads nothing) or `":fail"`
/// for the error exits that happen *after* the model is loaded (all items
/// failed, or a write error).
#[cfg(test)]
async fn extraction_stub(job: &Job) -> Option<Result<JobSuccess, String>> {
    let tag = job.tag.clone().unwrap_or_default();
    let (delay, flags) = match tag.split_once(':') {
        Some((delay, flags)) => (delay, flags),
        None => (tag.as_str(), ""),
    };
    tokio::time::sleep(std::time::Duration::from_millis(
        delay.parse::<u64>().unwrap_or(0),
    ))
    .await;
    if flags.contains("fail") {
        return Some(Err("test extraction failure".to_string()));
    }
    let loaded = !flags.contains("noload");
    Some(Ok(JobSuccess {
        summary: ChangeSummary {
            wrote_data: loaded,
            deleted_data: false,
            tags_changed: false,
        },
        loaded_model: loaded.then(|| job.metadata.clone()).flatten(),
    }))
}

async fn execute_job(job: Job) -> Result<JobSuccess, String> {
    match job.job_type {
        JobType::FolderRescan => {
            let guard = continuous_scan::pause_for_job_guarded(&job.index_db)
                .await
                .map_err(|err| format!("{err:?}"))?;
            let service = FileScanService::from_env(job.index_db.clone(), job.user_data_db);
            let result = service.rescan_folders().await;
            guard.resume().await;
            let result = result.map_err(|err| format!("{err:?}"))?;
            vector_quants::finishing_phase(&job.index_db).await;
            Ok(JobSuccess::from_summary(result.summary))
        }
        JobType::FolderUpdate => {
            let guard = continuous_scan::pause_for_job_guarded(&job.index_db)
                .await
                .map_err(|err| format!("{err:?}"))?;
            let service = FileScanService::from_env(job.index_db.clone(), job.user_data_db);
            let result = service.run_folder_update().await;
            guard.resume().await;
            let result = result.map_err(|err| format!("{err:?}"))?;
            vector_quants::finishing_phase(&job.index_db).await;
            Ok(JobSuccess::from_summary(result.summary))
        }
        JobType::DataExtraction => {
            if let Some(stubbed) = Box::pin(extraction_stub(&job)).await {
                return stubbed;
            }
            let outcome = extraction::run_extraction_job(job.clone())
                .await
                .map_err(|err| format!("{err}"))?;
            vector_quants::finishing_phase(&job.index_db).await;
            Ok(JobSuccess::from_extraction(outcome))
        }
        JobType::DataDeletion => {
            let summary = extraction::run_data_deletion_job(job.clone())
                .await
                .map_err(|err| format!("{err}"))?;
            vector_quants::finishing_phase(&job.index_db).await;
            Ok(JobSuccess::from_summary(summary))
        }
        JobType::JobDataDeletion => {
            let log_id = job.log_id.ok_or_else(|| "Log ID required".to_string())?;
            // Paused like every other write-heavy job so the deferred
            // VACUUM doesn't stall continuous-scan writes mid-flight.
            let guard = continuous_scan::pause_for_job_guarded(&job.index_db)
                .await
                .map_err(|err| format!("{err:?}"))?;
            let deleted = call_index_db_writer(&job.index_db, |reply| {
                IndexDbWriterMessage::DeleteJobData { log_id, reply }
            })
            .await;
            guard.resume().await;
            let deleted = deleted.map_err(|err| format!("{err:?}"))?;
            vector_quants::finishing_phase(&job.index_db).await;
            Ok(JobSuccess::from_summary(ChangeSummary {
                wrote_data: false,
                deleted_data: deleted > 0,
                // Deleting a job's data removes its `item_data` rows, and
                // `tags_items` hangs off those.
                tags_changed: deleted > 0,
            }))
        }
        JobType::VectorQuantReconcile => {
            // No continuous-scan pause: the reconcile touches only quant
            // tables and serializes with extraction via the job queue
            // itself; continuous scan writes no embeddings.
            crate::jobs::vector_quants::run_reconcile(&job.index_db)
                .await
                .map_err(|err| format!("{err:?}"))?;
            // Reports nothing: the reconcile never ran post-job maintenance
            // and its quant tables are outside what recount/ANALYZE serve.
            Ok(JobSuccess::from_summary(ChangeSummary::default()))
        }
        JobType::DbMaintenance => {
            // Never fails: this is the same contract the maintenance pass has
            // always had (its work is bookkeeping on top of already-committed
            // job output), and a failure here would only be noise in the
            // queue's outcome list.
            Box::pin(run_db_maintenance(&job)).await;
            // Maintenance changes no indexed data, so it never owes more.
            Ok(JobSuccess::from_summary(ChangeSummary::default()))
        }
        #[cfg(test)]
        JobType::TestSleep => {
            let delay = job
                .tag
                .as_deref()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(200);
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            Ok(JobSuccess::from_summary(ChangeSummary::default()))
        }
        #[cfg(test)]
        JobType::TestPanic => panic!("test job panic"),
        #[cfg(test)]
        JobType::TestReport => {
            let tag = job.tag.clone().unwrap_or_default();
            let (delay, flags) = tag.split_once(':').unwrap_or(("0", tag.as_str()));
            let delay = delay.parse::<u64>().unwrap_or(0);
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            Ok(JobSuccess::from_summary(ChangeSummary {
                wrote_data: flags.contains('w'),
                deleted_data: flags.contains('d'),
                tags_changed: flags.contains('t'),
            }))
        }
    }
}

static JOB_QUEUE: OnceCell<ActorRef<JobQueueMessage>> = OnceCell::const_new();

pub(crate) async fn enqueue_job(request: JobRequest) -> ApiResult<JobModel> {
    let queue = ensure_job_queue().await?;
    let (reply, rx) = oneshot::channel();
    queue
        .send_message(JobQueueMessage::Enqueue { request, reply })
        .map_err(|_| ApiError::internal("Job queue unavailable"))?;
    rx.await
        .map_err(|_| ApiError::internal("Job queue dropped response"))?
}

/// Enqueues all `requests` in one atomic step, minus the requests belonging to
/// an index DB whose [`BatchDedup`] matches a queued or running job. The reply
/// names the DBs that were dropped that way.
pub(crate) async fn enqueue_jobs_with_dedup(
    requests: Vec<JobRequest>,
    dedups: Vec<BatchDedup>,
) -> ApiResult<BatchEnqueueResult> {
    let queue = ensure_job_queue().await?;
    let (reply, rx) = oneshot::channel();
    queue
        .send_message(JobQueueMessage::EnqueueBatch {
            requests,
            dedups,
            reply,
        })
        .map_err(|_| ApiError::internal("Job queue unavailable"))?;
    rx.await
        .map_err(|_| ApiError::internal("Job queue dropped response"))?
}

/// Enqueues a `DbMaintenance` job with every flag set for `index_db`, at the
/// back of the queue. `Ok(None)` means one was already queued or running for
/// that DB and nothing was added.
pub(crate) async fn enqueue_db_maintenance(
    index_db: &str,
    user_data_db: &str,
) -> ApiResult<Option<JobModel>> {
    let queue = ensure_job_queue().await?;
    let (reply, rx) = oneshot::channel();
    queue
        .send_message(JobQueueMessage::EnqueueMaintenance {
            index_db: index_db.to_string(),
            user_data_db: user_data_db.to_string(),
            reply,
        })
        .map_err(|_| ApiError::internal("Job queue unavailable"))?;
    rx.await
        .map_err(|_| ApiError::internal("Job queue dropped response"))?
}

pub(crate) async fn get_queue_status() -> ApiResult<QueueStatusModel> {
    let queue = ensure_job_queue().await?;
    let (reply, rx) = oneshot::channel();
    queue
        .send_message(JobQueueMessage::GetQueueStatus { reply })
        .map_err(|_| ApiError::internal("Job queue unavailable"))?;
    rx.await
        .map_err(|_| ApiError::internal("Job queue dropped response"))?
}

/// `suppress_maintenance` keeps the boundary this cancel triggers from
/// synthesizing a deferred maintenance job (the owed flags survive for the
/// next boundary).
pub(crate) async fn cancel_queued_jobs(
    queue_ids: Vec<i64>,
    suppress_maintenance: bool,
) -> ApiResult<Vec<i64>> {
    let queue = ensure_job_queue().await?;
    let (reply, rx) = oneshot::channel();
    queue
        .send_message(JobQueueMessage::CancelQueued {
            queue_ids,
            suppress_maintenance,
            reply,
        })
        .map_err(|_| ApiError::internal("Job queue unavailable"))?;
    rx.await
        .map_err(|_| ApiError::internal("Job queue dropped response"))?
}

/// See [`cancel_queued_jobs`] for `suppress_maintenance`.
pub(crate) async fn cancel_running_job(suppress_maintenance: bool) -> ApiResult<Option<i64>> {
    let queue = ensure_job_queue().await?;
    let (reply, rx) = oneshot::channel();
    queue
        .send_message(JobQueueMessage::CancelRunning {
            suppress_maintenance,
            reply,
        })
        .map_err(|_| ApiError::internal("Job queue unavailable"))?;
    rx.await
        .map_err(|_| ApiError::internal("Job queue dropped response"))?
}

/// Reports work a still-running job has already committed to its index DB, so
/// the owed flags survive that job failing or being cancelled afterwards.
/// Fire-and-forget, and a no-op when the queue was never started (nothing can
/// be owed if no job ever ran).
///
/// **Never call this from a `DbMaintenance` job.** The flags a maintenance pass
/// is paying off are cleared when it is synthesized; re-owing them from inside
/// the pass makes its own boundary synthesize a replacement, forever. The actor
/// drops such reports (the running job's type is the only signal it has), but
/// the call site is where this has to be got right.
pub(crate) fn record_owed_now(index_db: &str, summary: ChangeSummary) {
    if summary.is_empty() {
        return;
    }
    let Some(queue) = JOB_QUEUE.get() else {
        return;
    };
    let _ = queue.send_message(JobQueueMessage::RecordOwed {
        index_db: index_db.to_string(),
        summary,
    });
}

/// Cancels the running job, drops all queued jobs, and makes the queue refuse
/// new enqueues. Returns the cancelled running job's id, if there was one.
/// Deliberately does not spawn the queue when it was never started.
pub(crate) async fn shutdown_job_queue() -> Option<i64> {
    let queue = JOB_QUEUE.get()?;
    let (reply, rx) = oneshot::channel();
    queue
        .send_message(JobQueueMessage::Shutdown { reply })
        .ok()?;
    rx.await.ok().flatten()
}

async fn ensure_job_queue() -> ApiResult<ActorRef<JobQueueMessage>> {
    JOB_QUEUE
        .get_or_try_init(|| async {
            let (actor, _handle) = Actor::spawn(
                Some("job-queue".to_string()),
                JobQueueActor,
                JobQueueArgs {
                    runner_name: Some("job-runner".to_string()),
                },
            )
            .await
            .map_err(|err| {
                tracing::error!(error = ?err, "failed to start job queue");
                ApiError::internal("Failed to start job queue")
            })?;
            Ok(actor)
        })
        .await
        .map(Clone::clone)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ractor::Actor;

    async fn spawn_test_queue() -> (
        ActorRef<JobQueueMessage>,
        ractor::concurrency::JoinHandle<()>,
    ) {
        // A monotonic counter, not a timestamp: parallel tests can spawn
        // within the same clock tick and collide on the actor name.
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let unique = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Actor::spawn(
            Some(format!("job-queue-test-{unique}")),
            JobQueueActor,
            JobQueueArgs {
                runner_name: Some(format!("job-runner-test-{unique}")),
            },
        )
        .await
        .expect("failed to spawn test queue")
    }

    async fn enqueue_on(queue: &ActorRef<JobQueueMessage>, request: JobRequest) -> JobModel {
        let (reply, rx) = oneshot::channel();
        queue
            .send_message(JobQueueMessage::Enqueue { request, reply })
            .unwrap();
        rx.await.unwrap().unwrap()
    }

    async fn status_on(queue: &ActorRef<JobQueueMessage>) -> QueueStatusModel {
        let (reply, rx) = oneshot::channel();
        queue
            .send_message(JobQueueMessage::GetQueueStatus { reply })
            .unwrap();
        rx.await.unwrap().unwrap()
    }

    async fn cancel_on(queue: &ActorRef<JobQueueMessage>, ids: Vec<i64>) -> Vec<i64> {
        cancel_on_with(queue, ids, false).await
    }

    async fn cancel_on_with(
        queue: &ActorRef<JobQueueMessage>,
        ids: Vec<i64>,
        suppress_maintenance: bool,
    ) -> Vec<i64> {
        let (reply, rx) = oneshot::channel();
        queue
            .send_message(JobQueueMessage::CancelQueued {
                queue_ids: ids,
                suppress_maintenance,
                reply,
            })
            .unwrap();
        rx.await.unwrap().unwrap()
    }

    async fn debug_on(queue: &ActorRef<JobQueueMessage>) -> QueueDebugState {
        let (reply, rx) = oneshot::channel();
        queue
            .send_message(JobQueueMessage::DebugState { reply })
            .unwrap();
        rx.await.unwrap()
    }

    /// A job that reports the change summary encoded in its tag, so the
    /// boundary logic can be driven without touching a database.
    fn report_job(index_db: &str, tag: &str) -> JobRequest {
        JobRequest {
            job_type: JobType::TestReport,
            index_db: index_db.to_string(),
            user_data_db: "default".to_string(),
            metadata: None,
            batch_size: None,
            threshold: None,
            log_id: None,
            tag: Some(tag.to_string()),
        }
    }

    /// A `DataExtraction` job whose body is the `cfg(test)` stub: it reports
    /// `metadata` as the setter it loaded (`"<delay_ms>:noload"` to report
    /// loading nothing, as the no-data early return does).
    fn extraction_job(index_db: &str, setter: &str, tag: &str) -> JobRequest {
        JobRequest {
            job_type: JobType::DataExtraction,
            index_db: index_db.to_string(),
            user_data_db: "default".to_string(),
            metadata: Some(setter.to_string()),
            batch_size: None,
            threshold: None,
            log_id: None,
            tag: Some(tag.to_string()),
        }
    }

    /// A queue entry for the pure decision-function tests; never executed.
    fn queued_job(job_type: JobType, metadata: Option<&str>) -> Job {
        Job {
            queue_id: 1,
            job_type,
            index_db: "db".to_string(),
            user_data_db: "default".to_string(),
            metadata: metadata.map(str::to_string),
            batch_size: None,
            threshold: None,
            log_id: None,
            tag: None,
        }
    }

    fn sleep_job(index_db: &str, millis: u64) -> JobRequest {
        JobRequest {
            job_type: JobType::TestSleep,
            index_db: index_db.to_string(),
            user_data_db: "default".to_string(),
            metadata: None,
            batch_size: None,
            threshold: None,
            log_id: None,
            tag: Some(millis.to_string()),
        }
    }

    /// Per-test index DB name: the synthesized maintenance job really runs,
    /// and its writer must not collide with another test's database.
    fn unique_db(prefix: &str) -> String {
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        format!(
            "{prefix}-{}",
            NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        )
    }

    /// Makes the `#[cfg(test)]` maintenance stub for `index_db` block, so a
    /// synthesized `DbMaintenance` job can be observed (and cancelled) while
    /// it is the running job.
    fn set_test_maintenance_delay(index_db: &str, millis: u64) {
        test_maintenance_delays()
            .lock()
            .expect("test maintenance delay registry poisoned")
            .insert(index_db.to_string(), millis);
    }

    async fn cancel_running_on(queue: &ActorRef<JobQueueMessage>, suppress: bool) -> Option<i64> {
        let (reply, rx) = oneshot::channel();
        queue
            .send_message(JobQueueMessage::CancelRunning {
                suppress_maintenance: suppress,
                reply,
            })
            .unwrap();
        rx.await.unwrap().unwrap()
    }

    async fn wait_for_running(queue: &ActorRef<JobQueueMessage>, queue_id: i64) {
        for _ in 0..200 {
            let status = status_on(queue).await;
            if status
                .queue
                .iter()
                .any(|entry| entry.queue_id == queue_id && entry.running)
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("job {queue_id} never started running");
    }

    async fn wait_for_running_maintenance(queue: &ActorRef<JobQueueMessage>) -> i64 {
        for _ in 0..200 {
            let status = status_on(queue).await;
            if let Some(entry) = status
                .queue
                .iter()
                .find(|entry| entry.job_type == JobType::DbMaintenance && entry.running)
            {
                return entry.queue_id;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("maintenance job never started running");
    }

    async fn wait_for<F: Fn(&QueueDebugState) -> bool>(
        queue: &ActorRef<JobQueueMessage>,
        predicate: F,
    ) -> QueueDebugState {
        for _ in 0..200 {
            let state = debug_on(queue).await;
            if predicate(&state) {
                return state;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("condition not reached: {:?}", debug_on(queue).await);
    }

    #[tokio::test]
    async fn queue_tracks_running_job() {
        let (queue, handle) = spawn_test_queue().await;
        let job = JobRequest {
            job_type: JobType::TestSleep,
            index_db: "default".to_string(),
            user_data_db: "default".to_string(),
            metadata: None,
            batch_size: None,
            threshold: None,
            log_id: None,
            tag: Some("200".to_string()),
        };
        let job2 = JobRequest {
            tag: Some("50".to_string()),
            ..job.clone()
        };
        let first = enqueue_on(&queue, job).await;
        let second = enqueue_on(&queue, job2).await;

        let status = status_on(&queue).await;
        assert_eq!(status.queue.len(), 2);
        assert_eq!(status.queue[0].queue_id, first.queue_id);
        assert!(status.queue[0].running);
        assert_eq!(status.queue[1].queue_id, second.queue_id);
        assert!(!status.queue[1].running);

        queue.stop(None);
        handle.await.unwrap();
    }

    // Regression test: the runner must clear its busy state when a job
    // completes normally, so the next queued job actually starts running
    // instead of being rejected as "runner busy" and silently dropped.
    #[tokio::test]
    async fn second_job_runs_after_first_completes() {
        let (queue, handle) = spawn_test_queue().await;
        let job = JobRequest {
            job_type: JobType::TestSleep,
            index_db: "default".to_string(),
            user_data_db: "default".to_string(),
            metadata: None,
            batch_size: None,
            threshold: None,
            log_id: None,
            tag: Some("200".to_string()),
        };
        let job2 = JobRequest {
            tag: Some("400".to_string()),
            ..job.clone()
        };
        let first = enqueue_on(&queue, job).await;
        let second = enqueue_on(&queue, job2).await;

        // At t=300ms the first job (200ms) has finished and the second
        // (400ms) must be the running job.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let status = status_on(&queue).await;
        assert_eq!(
            status.queue.len(),
            1,
            "expected second job to be running, queue: {status:?}"
        );
        assert_eq!(status.queue[0].queue_id, second.queue_id);
        assert!(status.queue[0].running);
        assert!(status.outcomes.iter().any(|outcome| {
            outcome.queue_id == first.queue_id && outcome.status == JobOutcomeStatus::Completed
        }));

        queue.stop(None);
        handle.await.unwrap();
    }

    // Process shutdown: the running job is cancelled, queued jobs are
    // dropped without starting, and new enqueues are refused so an HTTP
    // request still draining can't start a job that would die with the
    // process.
    #[tokio::test]
    async fn shutdown_cancels_everything_and_rejects_new_jobs() {
        let (queue, handle) = spawn_test_queue().await;
        let job = JobRequest {
            job_type: JobType::TestSleep,
            index_db: "default".to_string(),
            user_data_db: "default".to_string(),
            metadata: None,
            batch_size: None,
            threshold: None,
            log_id: None,
            tag: Some("60000".to_string()),
        };
        let running = enqueue_on(&queue, job.clone()).await;
        let _queued = enqueue_on(&queue, job.clone()).await;

        let (reply, rx) = oneshot::channel();
        queue
            .send_message(JobQueueMessage::Shutdown { reply })
            .unwrap();
        let cancelled = rx.await.unwrap();
        assert_eq!(cancelled, Some(running.queue_id));

        let status = status_on(&queue).await;
        assert!(
            status.queue.is_empty(),
            "queue should be empty after shutdown, got: {status:?}"
        );

        let (reply, rx) = oneshot::channel();
        queue
            .send_message(JobQueueMessage::Enqueue {
                request: job,
                reply,
            })
            .unwrap();
        assert!(
            rx.await.unwrap().is_err(),
            "enqueue after shutdown should be refused"
        );

        queue.stop(None);
        handle.await.unwrap();
    }

    // Regression test: a panicking job must not wedge the queue. The watcher
    // task reports the panic, the busy state clears, and the next queued job
    // runs normally.
    #[tokio::test]
    async fn panicking_job_does_not_wedge_queue() {
        let (queue, handle) = spawn_test_queue().await;
        let panic_job = JobRequest {
            job_type: JobType::TestPanic,
            index_db: "default".to_string(),
            user_data_db: "default".to_string(),
            metadata: None,
            batch_size: None,
            threshold: None,
            log_id: None,
            tag: None,
        };
        let sleep_job = JobRequest {
            job_type: JobType::TestSleep,
            tag: Some("400".to_string()),
            ..panic_job.clone()
        };
        let _first = enqueue_on(&queue, panic_job).await;
        let second = enqueue_on(&queue, sleep_job).await;

        // The panic job dies immediately; shortly after, the sleep job must
        // be running.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let status = status_on(&queue).await;
        assert_eq!(
            status.queue.len(),
            1,
            "expected sleep job to be running after panic, queue: {status:?}"
        );
        assert_eq!(status.queue[0].queue_id, second.queue_id);
        assert!(status.queue[0].running);

        queue.stop(None);
        handle.await.unwrap();
    }

    async fn enqueue_batch_on(
        queue: &ActorRef<JobQueueMessage>,
        requests: Vec<JobRequest>,
        dedups: Vec<BatchDedup>,
    ) -> BatchEnqueueResult {
        let (reply, rx) = oneshot::channel();
        queue
            .send_message(JobQueueMessage::EnqueueBatch {
                requests,
                dedups,
                reply,
            })
            .unwrap();
        rx.await.unwrap().unwrap()
    }

    fn cron_dedup(index_db: &str) -> BatchDedup {
        BatchDedup {
            tag: "cronjob".to_string(),
            index_db: index_db.to_string(),
        }
    }

    // The batch dedup must be atomic in the actor: while a tagged job for the
    // same index DB is queued or running, that DB's requests are skipped; other
    // index DBs are unaffected; once the tagged jobs are gone the batch goes
    // through.
    #[tokio::test]
    async fn batch_enqueue_skips_while_tagged_job_active() {
        let (queue, handle) = spawn_test_queue().await;
        // Unparseable TestSleep tags fall back to a 200ms sleep.
        let job = JobRequest {
            job_type: JobType::TestSleep,
            index_db: "default".to_string(),
            user_data_db: "default".to_string(),
            metadata: None,
            batch_size: None,
            threshold: None,
            log_id: None,
            tag: Some("cronjob".to_string()),
        };

        let first = enqueue_batch_on(
            &queue,
            vec![job.clone(), job.clone()],
            vec![cron_dedup("default")],
        )
        .await;
        assert_eq!(first.enqueued.len(), 2);
        assert!(first.skipped_dbs.is_empty());

        // One of the batch is running, one queued: a second batch is skipped.
        let second = enqueue_batch_on(&queue, vec![job.clone()], vec![cron_dedup("default")]).await;
        assert!(second.enqueued.is_empty());
        assert_eq!(second.skipped_dbs, ["default"]);
        assert!(second.was_skipped("default"));

        // A different index DB does not collide with the dedup condition.
        let other_db = JobRequest {
            index_db: "other".to_string(),
            ..job.clone()
        };
        let other = enqueue_batch_on(&queue, vec![other_db], vec![cron_dedup("other")]).await;
        assert_eq!(other.enqueued.len(), 1);
        assert!(other.skipped_dbs.is_empty());

        // After all tagged jobs have drained, the batch enqueues again.
        tokio::time::sleep(std::time::Duration::from_millis(900)).await;
        let status = status_on(&queue).await;
        assert!(status.queue.is_empty(), "queue should be idle: {status:?}");
        let third = enqueue_batch_on(&queue, vec![job.clone()], vec![cron_dedup("default")]).await;
        assert_eq!(third.enqueued.len(), 1);

        queue.stop(None);
        handle.await.unwrap();
    }

    // The merged cron batch carries several DBs at once: a conflict on one of
    // them drops only that DB's requests, the rest of the batch still enqueues
    // atomically, and the reply names the skipped DB.
    #[tokio::test]
    async fn batch_enqueue_drops_only_the_conflicting_db() {
        let (queue, handle) = spawn_test_queue().await;
        let job = |index_db: &str| JobRequest {
            job_type: JobType::TestSleep,
            index_db: index_db.to_string(),
            user_data_db: "default".to_string(),
            metadata: None,
            batch_size: None,
            threshold: None,
            log_id: None,
            // Unparseable tag: a 200ms sleep, and the cron dedup tag.
            tag: Some("cronjob".to_string()),
        };

        // Occupy "one" with a tagged job (running plus one queued behind it).
        let first = enqueue_batch_on(
            &queue,
            vec![job("one"), job("one")],
            vec![cron_dedup("one")],
        )
        .await;
        assert_eq!(first.enqueued.len(), 2);

        // A merged batch for both DBs: "one" conflicts, "two" does not.
        let merged = enqueue_batch_on(
            &queue,
            vec![job("one"), job("two"), job("one"), job("two")],
            vec![cron_dedup("one"), cron_dedup("two")],
        )
        .await;
        assert_eq!(merged.skipped_dbs, ["one"]);
        let dbs: Vec<&str> = merged
            .enqueued
            .iter()
            .map(|job| job.index_db.as_str())
            .collect();
        assert_eq!(dbs, ["two", "two"], "only the conflicting DB is dropped");

        let status = status_on(&queue).await;
        assert_eq!(
            status.queue.len(),
            4,
            "two pre-existing + two newly enqueued: {status:?}"
        );

        queue.stop(None);
        handle.await.unwrap();
    }

    // With no dedup entries at all a batch is unconditional, and an empty
    // request list is a well-formed no-op rather than a skip.
    #[tokio::test]
    async fn batch_enqueue_without_dedups_never_skips() {
        let (queue, handle) = spawn_test_queue().await;
        let job = JobRequest {
            job_type: JobType::TestSleep,
            index_db: "default".to_string(),
            user_data_db: "default".to_string(),
            metadata: None,
            batch_size: None,
            threshold: None,
            log_id: None,
            tag: Some("cronjob".to_string()),
        };
        let first = enqueue_batch_on(&queue, vec![job.clone()], Vec::new()).await;
        assert_eq!(first.enqueued.len(), 1);
        let second = enqueue_batch_on(&queue, vec![job.clone()], Vec::new()).await;
        assert_eq!(second.enqueued.len(), 1);
        assert!(second.skipped_dbs.is_empty());

        let empty = enqueue_batch_on(&queue, Vec::new(), vec![cron_dedup("nothing")]).await;
        assert!(empty.enqueued.is_empty());
        assert!(empty.skipped_dbs.is_empty());

        queue.stop(None);
        handle.await.unwrap();
    }

    // A batch that enqueues nothing must leave the queue alone — the property
    // the old all-or-nothing skip path had for free. Observed through the
    // start-attempt counter, because "started nothing" and "never looked" are
    // otherwise the same from outside.
    #[tokio::test]
    async fn a_fully_skipped_batch_does_not_touch_the_queue() {
        let (queue, handle) = spawn_test_queue().await;
        let db = unique_db("skipped-batch");

        // On an idle queue the difference is visible: a batch that enqueued
        // nothing must not even reach for the next job.
        assert_eq!(debug_on(&queue).await.start_attempts, 0);
        let empty = enqueue_batch_on(&queue, Vec::new(), vec![cron_dedup(&db)]).await;
        assert!(empty.enqueued.is_empty() && empty.skipped_dbs.is_empty());
        assert_eq!(
            debug_on(&queue).await.start_attempts,
            0,
            "an enqueue that added nothing must not drive the queue"
        );

        // Same for a batch whose every DB is deduped away: the queue is left
        // exactly as it was found.
        let job = JobRequest {
            job_type: JobType::TestSleep,
            index_db: db.clone(),
            user_data_db: "default".to_string(),
            metadata: None,
            batch_size: None,
            threshold: None,
            log_id: None,
            // Unparseable TestSleep tag: a 200ms sleep, so it is still the
            // running job for the second batch.
            tag: Some("cronjob".to_string()),
        };
        let first = enqueue_batch_on(&queue, vec![job.clone()], vec![cron_dedup(&db)]).await;
        assert_eq!(first.enqueued.len(), 1);
        let before = status_on(&queue).await;
        let skipped = enqueue_batch_on(&queue, vec![job.clone()], vec![cron_dedup(&db)]).await;
        assert_eq!(skipped.skipped_dbs, [db.as_str()]);
        let after = status_on(&queue).await;
        assert_eq!(
            before
                .queue
                .iter()
                .map(|entry| (entry.queue_id, entry.running))
                .collect::<Vec<_>>(),
            after
                .queue
                .iter()
                .map(|entry| (entry.queue_id, entry.running))
                .collect::<Vec<_>>()
        );

        queue.stop(None);
        handle.await.unwrap();
    }

    // The flagship phase-1+2+3 interaction, in the shape a merged cron tick
    // produces: two DBs interleaved by setter in one batch. Same-setter jobs
    // from different DBs keep the model loaded across the boundary, and both
    // DBs' maintenance jobs accumulate at the tail of the drain instead of
    // interrupting it; the model is unloaded only at the setter change and at
    // the drain.
    #[tokio::test]
    async fn a_merged_cron_batch_reuses_models_across_dbs_and_maintenance() {
        let (queue, handle) = spawn_test_queue().await;
        let db_a = unique_db("merged-a");
        let db_b = unique_db("merged-b");
        let shared = "group/model-shared";
        let other = "group/model-other";

        // What `merge_cron_batches` would emit for these two DBs: the shared
        // setter's jobs adjacent, DB A's work finishing first.
        let batch = enqueue_batch_on(
            &queue,
            vec![
                extraction_job(&db_a, shared, "10"),
                extraction_job(&db_b, shared, "10"),
                extraction_job(&db_b, other, "10"),
            ],
            Vec::new(),
        )
        .await;
        assert_eq!(batch.enqueued.len(), 3);

        // Both DBs owe maintenance (the stub reports wrote_data), so both get
        // a synthesized job — A's as soon as its last job finishes, B's at the
        // drain, and both behind the extraction work that was still queued.
        let state = wait_for(&queue, |state| {
            state.synthesized.len() == 2 && state.unloads.len() == 2
        })
        .await;
        let synthesized: Vec<&str> = state
            .synthesized
            .iter()
            .map(|(index_db, _)| index_db.as_str())
            .collect();
        assert_eq!(
            synthesized,
            [db_a.as_str(), db_b.as_str()],
            "A's maintenance is owed as soon as its last job finishes: {state:?}"
        );
        assert_eq!(
            state.unloads,
            vec![shared.to_string(), other.to_string()],
            "the shared model must survive A's boundary and B's first job, \
             and unload only at the setter change: {state:?}"
        );
        assert!(state.owed.is_empty(), "{state:?}");

        queue.stop(None);
        handle.await.unwrap();
    }

    async fn enqueue_maintenance_on(
        queue: &ActorRef<JobQueueMessage>,
        index_db: &str,
    ) -> Option<JobModel> {
        let (reply, rx) = oneshot::channel();
        queue
            .send_message(JobQueueMessage::EnqueueMaintenance {
                index_db: index_db.to_string(),
                user_data_db: "default".to_string(),
                reply,
            })
            .unwrap();
        rx.await.unwrap().unwrap()
    }

    // Placement: a synthesized maintenance job goes to the *back*. Maintenance
    // can run for minutes, and at the front it would run between two jobs of a
    // drain — stalling the GPU pipeline and outliving the 60 s model TTL that
    // makes cross-job model reuse work. Jobs already queued for other DBs must
    // therefore all run before it.
    #[tokio::test]
    async fn maintenance_queues_behind_the_jobs_already_waiting() {
        let (queue, handle) = spawn_test_queue().await;
        let db = unique_db("placement-owed");
        let other_db = unique_db("placement-other");
        let running = enqueue_on(&queue, sleep_job(&db, 60_000)).await;
        let first_other = enqueue_on(&queue, sleep_job(&other_db, 60_000)).await;
        let second_other = enqueue_on(&queue, sleep_job(&other_db, 60_000)).await;
        wait_for_running(&queue, running.queue_id).await;

        // Cancelling the running job is a boundary: `db` owes pessimistic
        // maintenance and nothing left in the queue targets it.
        assert_eq!(
            cancel_running_on(&queue, false).await,
            Some(running.queue_id)
        );
        let state = debug_on(&queue).await;
        assert_eq!(state.synthesized.len(), 1, "expected synthesis: {state:?}");

        let status = status_on(&queue).await;
        let ids: Vec<i64> = status.queue.iter().map(|entry| entry.queue_id).collect();
        assert_eq!(
            &ids[..2],
            &[first_other.queue_id, second_other.queue_id],
            "the queued other-DB jobs keep their slots: {status:?}"
        );
        assert!(status.queue[0].running, "{status:?}");
        let last = status.queue.last().expect("queue is not empty");
        assert_eq!(
            last.job_type,
            JobType::DbMaintenance,
            "maintenance must be last, not ahead of the drain: {status:?}"
        );
        assert_eq!(last.index_db, db);
        assert!(!last.running);

        queue.stop(None);
        handle.await.unwrap();
    }

    // The manual trigger (`POST /api/jobs/maintenance`): a maintenance job
    // with every flag set, at the back like all maintenance, and deduped
    // against one that is already queued for the same DB.
    #[tokio::test]
    async fn manual_maintenance_enqueues_at_the_back_and_dedups() {
        let (queue, handle) = spawn_test_queue().await;
        let db = unique_db("manual-maint");
        let other_db = unique_db("manual-maint-other");
        let running = enqueue_on(&queue, sleep_job(&other_db, 60_000)).await;
        let queued = enqueue_on(&queue, sleep_job(&other_db, 60_000)).await;
        wait_for_running(&queue, running.queue_id).await;

        let job = enqueue_maintenance_on(&queue, &db)
            .await
            .expect("the first request enqueues");
        assert_eq!(job.job_type, JobType::DbMaintenance);
        assert_eq!(job.index_db, db);
        assert_eq!(
            job.metadata.as_deref(),
            Some("wrote_data,deleted_data,tags_changed"),
            "an explicit request always recounts (vacuum stays freelist-gated)"
        );

        let status = status_on(&queue).await;
        assert_eq!(
            status
                .queue
                .iter()
                .map(|entry| entry.queue_id)
                .collect::<Vec<_>>(),
            vec![running.queue_id, queued.queue_id, job.queue_id],
            "the request must not jump the queue: {status:?}"
        );

        // A second request while the first is still queued adds nothing.
        assert!(
            enqueue_maintenance_on(&queue, &db).await.is_none(),
            "a queued maintenance job must deduplicate the next request"
        );
        // Per DB, though: another database is unaffected.
        assert!(enqueue_maintenance_on(&queue, &other_db).await.is_some());

        queue.stop(None);
        handle.await.unwrap();
    }

    // The other half of the dedup condition: a maintenance job that is already
    // *running* also absorbs the request, so an impatient second click cannot
    // stack a second full pass behind the first.
    #[tokio::test]
    async fn manual_maintenance_is_skipped_while_one_is_running() {
        let (queue, handle) = spawn_test_queue().await;
        let db = unique_db("manual-maint-running");
        set_test_maintenance_delay(&db, 60_000);

        let job = enqueue_maintenance_on(&queue, &db)
            .await
            .expect("the first request enqueues");
        wait_for_running(&queue, job.queue_id).await;
        assert!(enqueue_maintenance_on(&queue, &db).await.is_none());

        queue.stop(None);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn cancel_queued_job_removes_it() {
        let (queue, handle) = spawn_test_queue().await;
        let job = JobRequest {
            job_type: JobType::TestSleep,
            index_db: "default".to_string(),
            user_data_db: "default".to_string(),
            metadata: None,
            batch_size: None,
            threshold: None,
            log_id: None,
            tag: Some("200".to_string()),
        };
        let job2 = JobRequest {
            tag: Some("200".to_string()),
            ..job.clone()
        };
        let _ = enqueue_on(&queue, job).await;
        let queued = enqueue_on(&queue, job2).await;

        let cancelled = cancel_on(&queue, vec![queued.queue_id]).await;
        assert_eq!(cancelled, vec![queued.queue_id]);

        let status = status_on(&queue).await;
        assert!(
            status
                .queue
                .iter()
                .all(|entry| entry.queue_id != queued.queue_id)
        );

        queue.stop(None);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn cancel_running_job_clears_state() {
        let (queue, handle) = spawn_test_queue().await;
        // Own DB name: cancelling a running job owes pessimistic maintenance,
        // which the boundary immediately synthesizes and runs for real.
        let running = enqueue_on(&queue, sleep_job(&unique_db("cancel-running"), 500)).await;

        let (reply, rx) = oneshot::channel();
        queue
            .send_message(JobQueueMessage::CancelRunning {
                suppress_maintenance: false,
                reply,
            })
            .unwrap();
        let cancelled = rx.await.unwrap().unwrap();
        assert_eq!(cancelled, Some(running.queue_id));

        let status = status_on(&queue).await;
        assert!(
            status
                .queue
                .iter()
                .all(|entry| entry.queue_id != running.queue_id)
        );

        queue.stop(None);
        handle.await.unwrap();
    }

    // Deferred maintenance is per DB and per queue drain: while more jobs for
    // the same DB are queued the owed flags just accumulate, and only the last
    // one's completion synthesizes a single maintenance job carrying the
    // union of what the finished jobs changed.
    #[tokio::test]
    async fn maintenance_waits_until_the_db_has_no_more_jobs() {
        let (queue, handle) = spawn_test_queue().await;
        let db = unique_db("owed-batch");
        // The second job is deliberately slow so the "owed but not yet
        // scheduled" window is observable without racing it.
        let _first = enqueue_on(&queue, report_job(&db, "10:w")).await;
        let _second = enqueue_on(&queue, report_job(&db, "600:d")).await;

        // After the first job: owed, but nothing synthesized — its successor
        // still targets the same DB.
        let state = wait_for(&queue, |state| state.owed.contains_key(&db)).await;
        assert_eq!(
            state.owed.get(&db).copied(),
            Some(ChangeSummary {
                wrote_data: true,
                deleted_data: false,
                tags_changed: false
            })
        );
        assert!(state.synthesized.is_empty(), "too early: {state:?}");
        assert!(
            state.marked_dirty.is_empty(),
            "a pure write owes no recount: {state:?}"
        );

        // After the second: one maintenance job for the union of both. The
        // deletion also dirties the tag counts, which the boundary mirrors
        // into the DB's durable marker.
        let state = wait_for(&queue, |state| !state.synthesized.is_empty()).await;
        assert_eq!(
            state.synthesized,
            vec![(
                db.clone(),
                ChangeSummary {
                    wrote_data: true,
                    deleted_data: true,
                    tags_changed: true
                }
            )]
        );
        assert_eq!(state.marked_dirty, vec![db.clone()]);
        assert!(!state.owed.contains_key(&db), "owed not cleared: {state:?}");

        queue.stop(None);
        handle.await.unwrap();
    }

    // A job that reports "nothing changed" owes nothing, so an idle cron pass
    // over an unchanged database never schedules a maintenance job.
    #[tokio::test]
    async fn no_maintenance_when_nothing_changed() {
        let (queue, handle) = spawn_test_queue().await;
        let db = unique_db("owed-empty");
        let job = enqueue_on(&queue, report_job(&db, "10:")).await;

        // Give the completion (and any boundary it would trigger) time to land.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let state = debug_on(&queue).await;
        assert!(state.owed.is_empty(), "nothing should be owed: {state:?}");
        assert!(
            state.synthesized.is_empty(),
            "nothing should be synthesized: {state:?}"
        );
        let status = status_on(&queue).await;
        assert!(status.queue.is_empty(), "queue should be idle: {status:?}");
        assert!(status.outcomes.iter().any(|outcome| {
            outcome.queue_id == job.queue_id && outcome.status == JobOutcomeStatus::Completed
        }));

        queue.stop(None);
        handle.await.unwrap();
    }

    // `suppress_maintenance` only suppresses the maintenance job this cancel
    // would trigger: the work stays owed and the next natural boundary — the
    // completion of a later job for that DB — schedules it.
    #[tokio::test]
    async fn suppressed_cancel_keeps_owed_for_the_next_boundary() {
        let (queue, handle) = spawn_test_queue().await;
        let db = unique_db("owed-suppress");
        let other_db = unique_db("owed-suppress-other");
        // A blocker on a *different* DB holds the runner so the target job
        // stays queued: without it the actor promotes the target to running
        // inside the same `RunnerFinished` handler that sets `owed`, and the
        // cancel would take the `CancelRunning` branch instead of the
        // suppressed `CancelQueued` removal path this test is about.
        let _first = enqueue_on(&queue, report_job(&db, "10:w")).await;
        let blocker = enqueue_on(&queue, sleep_job(&other_db, 60_000)).await;
        let queued = enqueue_on(&queue, sleep_job(&db, 60_000)).await;

        let state = wait_for(&queue, |state| state.owed.contains_key(&db)).await;
        assert!(state.synthesized.is_empty(), "too early: {state:?}");
        wait_for_running(&queue, blocker.queue_id).await;

        // Cancelling the last queued job for the DB is a boundary, but this
        // cancel opted out.
        let cancelled = cancel_on_with(&queue, vec![queued.queue_id], true).await;
        assert_eq!(cancelled, vec![queued.queue_id]);
        let status = status_on(&queue).await;
        assert!(
            status
                .queue
                .iter()
                .all(|entry| entry.queue_id != queued.queue_id),
            "the queued job must be gone: {status:?}"
        );
        let state = debug_on(&queue).await;
        assert!(
            state.owed.contains_key(&db),
            "owed must survive a suppressed cancel: {state:?}"
        );
        assert!(state.synthesized.is_empty(), "suppressed: {state:?}");

        // Free the runner (also suppressed, so `other_db` owes silently) and
        // let the next completed job for `db` pay the debt.
        assert_eq!(
            cancel_running_on(&queue, true).await,
            Some(blocker.queue_id)
        );
        let _later = enqueue_on(&queue, report_job(&db, "10:")).await;
        let state = wait_for(&queue, |state| !state.synthesized.is_empty()).await;
        assert_eq!(
            state.synthesized,
            vec![(
                db.clone(),
                ChangeSummary {
                    wrote_data: true,
                    deleted_data: false,
                    tags_changed: false
                }
            )]
        );

        queue.stop(None);
        handle.await.unwrap();
    }

    // The "is this DB busy?" test must look at the running job, not only at
    // the queue: synthesizing maintenance while a job for the same DB is still
    // running would put an ANALYZE/VACUUM immediately ahead of it.
    #[tokio::test]
    async fn maintenance_waits_for_a_running_job_on_the_same_db() {
        let (queue, handle) = spawn_test_queue().await;
        let db = unique_db("owed-running-busy");
        let _reporting = enqueue_on(&queue, report_job(&db, "10:w")).await;
        let running = enqueue_on(&queue, sleep_job(&db, 60_000)).await;
        let queued = enqueue_on(&queue, sleep_job(&db, 60_000)).await;

        let state = wait_for(&queue, |state| state.owed.contains_key(&db)).await;
        assert!(state.synthesized.is_empty(), "too early: {state:?}");
        wait_for_running(&queue, running.queue_id).await;

        // Removing the only *queued* job for the DB empties the queue, but a
        // job for it is still running.
        let cancelled = cancel_on(&queue, vec![queued.queue_id]).await;
        assert_eq!(cancelled, vec![queued.queue_id]);
        let state = debug_on(&queue).await;
        assert!(
            state.synthesized.is_empty(),
            "a running job for the DB still blocks maintenance: {state:?}"
        );
        assert!(
            state.owed.contains_key(&db),
            "owed must be kept for the real boundary: {state:?}"
        );

        // Once that job ends, the boundary finally fires.
        assert_eq!(
            cancel_running_on(&queue, false).await,
            Some(running.queue_id)
        );
        let state = wait_for(&queue, |state| !state.synthesized.is_empty()).await;
        assert_eq!(state.synthesized.len(), 1, "exactly one pass: {state:?}");
        assert_eq!(state.synthesized[0].0, db);

        queue.stop(None);
        handle.await.unwrap();
    }

    // The `DbMaintenance` exemption in `record_owed` has exactly one real
    // path: a maintenance job cancelled (or failed, or panicked) while
    // running. Without it, the job's pessimistic summary would re-own the
    // flags it was synthesized to pay off and the boundary would immediately
    // schedule a replacement — a cancel that cannot be honoured.
    #[tokio::test]
    async fn cancelling_running_maintenance_does_not_resurrect_it() {
        let (queue, handle) = spawn_test_queue().await;
        let db = unique_db("owed-running-maint");
        set_test_maintenance_delay(&db, 60_000);

        let _reporting = enqueue_on(&queue, report_job(&db, "10:w")).await;
        let state = wait_for(&queue, |state| !state.synthesized.is_empty()).await;
        assert_eq!(state.synthesized.len(), 1, "expected synthesis: {state:?}");
        let maintenance_id = wait_for_running_maintenance(&queue).await;

        assert_eq!(
            cancel_running_on(&queue, false).await,
            Some(maintenance_id),
            "the maintenance job should be the cancelled running job"
        );

        let state = debug_on(&queue).await;
        assert!(
            state.owed.is_empty(),
            "cancelled maintenance must not re-own its flags: {state:?}"
        );
        assert_eq!(state.synthesized.len(), 1, "no second synthesis: {state:?}");

        queue.stop(None);
        handle.await.unwrap();
    }

    // Mass cancel ("Cancel Selected" with a running job and its queued
    // successors) must remove the queued ids outright, never promote them into
    // the runner slot only to abort them again: a job that executed no
    // instruction has nothing to owe. The queued jobs here are of a *deleting*
    // type, so a promoted-then-aborted one would pessimistically owe the
    // VACUUM that nothing in this queue ever earned.
    #[tokio::test]
    async fn mass_cancel_does_not_start_the_jobs_it_is_cancelling() {
        let (queue, handle) = spawn_test_queue().await;
        let db = unique_db("owed-mass-cancel");
        let running = enqueue_on(&queue, sleep_job(&db, 60_000)).await;
        // `log_id: None` makes this job fail immediately *if* it is ever
        // started, without touching a database — which is what makes the old
        // promote-then-abort behaviour observable rather than merely slow.
        let deleting = JobRequest {
            job_type: JobType::JobDataDeletion,
            index_db: db.clone(),
            user_data_db: "default".to_string(),
            metadata: None,
            batch_size: None,
            threshold: None,
            log_id: None,
            tag: None,
        };
        let first = enqueue_on(&queue, deleting.clone()).await;
        let second = enqueue_on(&queue, deleting).await;
        wait_for_running(&queue, running.queue_id).await;

        let cancelled = cancel_on(
            &queue,
            vec![running.queue_id, first.queue_id, second.queue_id],
        )
        .await;
        assert_eq!(cancelled.len(), 3, "all three cancelled: {cancelled:?}");

        let status = status_on(&queue).await;
        for queue_id in [running.queue_id, first.queue_id, second.queue_id] {
            assert!(
                status.outcomes.iter().any(|outcome| {
                    outcome.queue_id == queue_id && outcome.status == JobOutcomeStatus::Cancelled
                }),
                "missing cancel outcome for {queue_id}: {status:?}"
            );
        }

        let state = debug_on(&queue).await;
        assert_eq!(state.synthesized.len(), 1, "one pass for the DB: {state:?}");
        assert_eq!(
            state.synthesized[0],
            (
                db.clone(),
                ChangeSummary {
                    wrote_data: true,
                    deleted_data: false,
                    tags_changed: false
                }
            ),
            "only the job that actually ran may owe anything"
        );

        queue.stop(None);
        handle.await.unwrap();
    }

    // A long-running job can commit deletions (an extraction job's embedded
    // folder resync does) and only fail afterwards. Reporting them as they
    // happen is what keeps that debt from dying with the failed job.
    #[tokio::test]
    async fn recorded_owed_survives_the_reporting_job() {
        let (queue, handle) = spawn_test_queue().await;
        let db = unique_db("owed-mid-job");
        let running = enqueue_on(&queue, sleep_job(&db, 60_000)).await;
        wait_for_running(&queue, running.queue_id).await;

        queue
            .send_message(JobQueueMessage::RecordOwed {
                index_db: db.clone(),
                summary: ChangeSummary {
                    wrote_data: true,
                    deleted_data: true,
                    tags_changed: false,
                },
            })
            .unwrap();

        // Merged, but never scheduled on its own: the reporting job is still
        // running.
        let state = wait_for(&queue, |state| state.owed.contains_key(&db)).await;
        assert!(state.synthesized.is_empty(), "too early: {state:?}");

        // Cancelling the reporting job on its own would only owe `wrote_data`;
        // the deletion flag (and the recount it implies) can only have come
        // from the mid-job report.
        assert_eq!(
            cancel_running_on(&queue, false).await,
            Some(running.queue_id)
        );
        let state = wait_for(&queue, |state| !state.synthesized.is_empty()).await;
        assert_eq!(
            state.synthesized,
            vec![(
                db.clone(),
                ChangeSummary {
                    wrote_data: true,
                    deleted_data: true,
                    tags_changed: true
                }
            )]
        );

        queue.stop(None);
        handle.await.unwrap();
    }

    // A cancelled job reports nothing, so the boundary assumes the worst: it
    // wrote, and — being a scan — may already have cascaded deletes, which is
    // what decides whether the maintenance job vacuums.
    #[tokio::test]
    async fn cancelled_job_owes_pessimistic_maintenance() {
        let (queue, handle) = spawn_test_queue().await;
        let db = unique_db("owed-pessimistic");
        let running = enqueue_on(&queue, sleep_job(&db, 60_000)).await;

        // Suppressed so the owed flags stay observable instead of being
        // consumed by the maintenance job this cancel would otherwise start.
        let cancelled = cancel_on_with(&queue, vec![running.queue_id], true).await;
        assert_eq!(cancelled, vec![running.queue_id]);

        let state = debug_on(&queue).await;
        assert_eq!(
            state.owed.get(&db).copied(),
            Some(ChangeSummary {
                wrote_data: true,
                deleted_data: false,
                tags_changed: false
            }),
            "a cancelled job must be assumed to have written: {state:?}"
        );
        assert!(state.synthesized.is_empty(), "suppressed: {state:?}");

        queue.stop(None);
        handle.await.unwrap();
    }

    // The synthesized job carries its owed flags in `metadata` — that string is
    // both the queue's display text and how the maintenance arm learns whether
    // it may VACUUM, so it has to survive the round trip.
    #[test]
    fn change_summary_metadata_round_trips() {
        for summary in [
            ChangeSummary {
                wrote_data: true,
                deleted_data: false,
                tags_changed: false,
            },
            ChangeSummary {
                wrote_data: false,
                deleted_data: true,
                tags_changed: false,
            },
            ChangeSummary {
                wrote_data: false,
                deleted_data: false,
                tags_changed: true,
            },
            ChangeSummary::all(),
        ] {
            let metadata = summary.to_metadata();
            assert_eq!(ChangeSummary::from_metadata(Some(&metadata)), summary);
        }
        assert_eq!(ChangeSummary::from_metadata(None), ChangeSummary::default());
        assert_eq!(
            ChangeSummary::from_metadata(Some("nonsense")),
            ChangeSummary::default()
        );
        // The manual trigger's job carries every flag, so its `metadata` is
        // what the maintenance arm reads to run everything.
        assert_eq!(
            ChangeSummary::all().to_metadata(),
            "wrote_data,deleted_data,tags_changed"
        );
    }

    // The pessimistic fallback also has to assume deletions for the job types
    // that cascade them, since a cancelled scan may already have removed rows
    // and only a VACUUM reclaims their pages.
    #[test]
    fn pessimistic_summary_assumes_deletes_for_deleting_job_types() {
        for job_type in [
            JobType::FolderRescan,
            JobType::FolderUpdate,
            JobType::DataDeletion,
            JobType::JobDataDeletion,
        ] {
            assert_eq!(
                pessimistic_summary(&job_type),
                ChangeSummary {
                    wrote_data: true,
                    deleted_data: true,
                    tags_changed: true
                },
                "{job_type:?} may have cascaded deletes"
            );
        }
        assert_eq!(
            pessimistic_summary(&JobType::DataExtraction),
            ChangeSummary {
                wrote_data: true,
                deleted_data: false,
                // A cancelled tagging job needs no pessimism here: every tag
                // it committed set the durable marker as it committed.
                tags_changed: false
            },
            "a partially completed extraction really did write item data"
        );
        // The reconcile writes only quant tables and never ran post-job
        // maintenance; cancelling it must not start scheduling one.
        assert_eq!(
            pessimistic_summary(&JobType::VectorQuantReconcile),
            ChangeSummary::default(),
            "the reconcile owes no maintenance, even when it is cancelled"
        );
    }

    // The model-continuity decision, without actors: the batch model survives
    // exactly as long as the next extraction in the queue wants the same
    // setter, and a synthesized maintenance job in between does not count.
    #[test]
    fn next_batch_setter_skips_maintenance_but_nothing_else() {
        let extraction = |setter| queued_job(JobType::DataExtraction, Some(setter));
        let maintenance = || queued_job(JobType::DbMaintenance, Some("wrote_data"));

        assert_eq!(next_batch_setter(&VecDeque::new()), None, "empty queue");
        assert_eq!(
            next_batch_setter(&VecDeque::from(vec![extraction("group/a")])),
            Some("group/a")
        );
        assert_eq!(
            next_batch_setter(&VecDeque::from(vec![extraction("group/b")])),
            Some("group/b"),
            "a different setter is reported as-is; the caller compares"
        );
        assert_eq!(
            next_batch_setter(&VecDeque::from(vec![maintenance(), extraction("group/a")])),
            Some("group/a"),
            "the model must survive a deferred maintenance pass between two \
             jobs for the same setter"
        );
        assert_eq!(
            next_batch_setter(&VecDeque::from(vec![
                maintenance(),
                maintenance(),
                extraction("group/a"),
            ])),
            Some("group/a"),
            "several DBs can owe maintenance at the same boundary"
        );
        assert_eq!(
            next_batch_setter(&VecDeque::from(vec![
                queued_job(JobType::FolderRescan, None),
                extraction("group/a"),
            ])),
            None,
            "a scan can run for hours; the model must not wait in VRAM"
        );
        assert_eq!(
            next_batch_setter(&VecDeque::from(vec![queued_job(
                JobType::DataExtraction,
                None
            )])),
            None,
            "an extraction job without a setter cannot keep anything warm"
        );
    }

    // The conservative rule for jobs that end without reporting: only a
    // `DataExtraction` can have loaded a batch model, and its `metadata` is the
    // setter (`setter_name == inference_id`).
    #[test]
    fn only_extraction_jobs_track_an_unreported_load() {
        assert_eq!(
            unreported_batch_load(&queued_job(JobType::DataExtraction, Some("group/a"))),
            Some("group/a".to_string())
        );
        assert_eq!(
            unreported_batch_load(&queued_job(JobType::DataExtraction, None)),
            None
        );
        for job_type in [
            JobType::FolderRescan,
            JobType::DataDeletion,
            JobType::DbMaintenance,
            JobType::VectorQuantReconcile,
        ] {
            assert_eq!(
                unreported_batch_load(&queued_job(job_type.clone(), Some("group/a"))),
                None,
                "{job_type:?} never loads a batch model"
            );
        }
    }

    // The point of the whole phase: consecutive extraction jobs for the same
    // setter — different databases, with a deferred maintenance pass for the
    // first DB in between — reuse one loaded model instead of reloading it.
    #[tokio::test]
    async fn same_setter_chain_keeps_the_model_loaded_across_jobs() {
        let (queue, handle) = spawn_test_queue().await;
        let db = unique_db("batch-chain-a");
        let other_db = unique_db("batch-chain-b");
        let setter = "group/model-a";
        let _first = enqueue_on(&queue, extraction_job(&db, setter, "10")).await;
        let second = enqueue_on(&queue, extraction_job(&other_db, setter, "60000")).await;

        // The second job running means the first one's boundary (and the
        // maintenance job synthesized there) has already been through the
        // model-continuity decision.
        wait_for_running(&queue, second.queue_id).await;
        let state = debug_on(&queue).await;
        assert_eq!(
            state.batch_loaded.as_deref(),
            Some(setter),
            "the first job's model must still be tracked: {state:?}"
        );
        assert!(
            state.unloads.is_empty(),
            "the model must survive both the boundary and the maintenance job \
             in between: {state:?}"
        );

        // Nothing follows the second job, so its boundary unloads.
        assert_eq!(cancel_running_on(&queue, true).await, Some(second.queue_id));
        let state = wait_for(&queue, |state| !state.unloads.is_empty()).await;
        assert_eq!(state.unloads, vec![setter.to_string()]);

        queue.stop(None);
        handle.await.unwrap();
    }

    // The end of a queue drain: with nothing left to reuse the model, the
    // boundary unloads it — the explicit unload the jobs used to do
    // themselves, now made at a point that can see what comes next.
    #[tokio::test]
    async fn last_extraction_unloads_its_model() {
        let (queue, handle) = spawn_test_queue().await;
        let db = unique_db("batch-last");
        let setter = "group/model-a";
        let _job = enqueue_on(&queue, extraction_job(&db, setter, "10")).await;

        let state = wait_for(&queue, |state| !state.unloads.is_empty()).await;
        assert_eq!(state.unloads, vec![setter.to_string()]);
        assert!(
            state.batch_loaded.is_none(),
            "the unloaded model must stop being tracked: {state:?}"
        );

        queue.stop(None);
        handle.await.unwrap();
    }

    // Any other job type ends the run of extraction jobs: a scan can take
    // hours, and holding VRAM across it to save one model load is a bad trade.
    #[tokio::test]
    async fn a_non_extraction_job_in_between_unloads_the_model() {
        let (queue, handle) = spawn_test_queue().await;
        let db = unique_db("batch-interrupted");
        let other_db = unique_db("batch-interrupted-scan");
        let setter = "group/model-a";
        let _first = enqueue_on(&queue, extraction_job(&db, setter, "10")).await;
        let blocker = enqueue_on(&queue, sleep_job(&other_db, 60_000)).await;
        let _later = enqueue_on(&queue, extraction_job(&other_db, setter, "10")).await;

        wait_for_running(&queue, blocker.queue_id).await;
        let state = debug_on(&queue).await;
        assert_eq!(
            state.unloads,
            vec![setter.to_string()],
            "the queued job between the two extractions is not an extraction: {state:?}"
        );
        assert!(state.batch_loaded.is_none(), "{state:?}");

        queue.stop(None);
        handle.await.unwrap();
    }

    // The other half of "ended without reporting": a *failed* extraction job.
    // Both error exits that matter (all items failed; the model load itself
    // failing) happen after the model is loaded, so the boundary has to assume
    // it is resident — this is the arm that covers them.
    #[tokio::test]
    async fn failed_extraction_tracks_and_unloads_its_model() {
        let (queue, handle) = spawn_test_queue().await;
        let db = unique_db("batch-failed");
        let setter = "group/model-a";
        let job = enqueue_on(&queue, extraction_job(&db, setter, "10:fail")).await;

        let state = wait_for(&queue, |state| !state.unloads.is_empty()).await;
        assert_eq!(
            state.unloads,
            vec![setter.to_string()],
            "a failed extraction must be assumed to have loaded: {state:?}"
        );
        assert!(state.batch_loaded.is_none(), "{state:?}");
        let status = status_on(&queue).await;
        assert!(
            status.outcomes.iter().any(|outcome| {
                outcome.queue_id == job.queue_id && outcome.status == JobOutcomeStatus::Failed
            }),
            "the job really has to have failed, not been cancelled: {status:?}"
        );

        queue.stop(None);
        handle.await.unwrap();
    }

    // Shutdown unloads the model of the job it just cancelled. The cancel path
    // defers to the shutdown handler, which waits for the call instead of
    // detaching it into a runtime that is about to stop polling.
    #[tokio::test]
    async fn shutdown_unloads_the_batch_model() {
        let (queue, handle) = spawn_test_queue().await;
        let db = unique_db("batch-shutdown");
        let setter = "group/model-a";
        let running = enqueue_on(&queue, extraction_job(&db, setter, "60000")).await;
        wait_for_running(&queue, running.queue_id).await;

        let (reply, rx) = oneshot::channel();
        queue
            .send_message(JobQueueMessage::Shutdown { reply })
            .unwrap();
        assert_eq!(rx.await.unwrap(), Some(running.queue_id));

        let state = debug_on(&queue).await;
        assert_eq!(state.unloads, vec![setter.to_string()], "{state:?}");
        assert!(state.batch_loaded.is_none(), "{state:?}");

        queue.stop(None);
        handle.await.unwrap();
    }

    // The mapping from a real extraction job's outcome onto the queue's report.
    // The queue tests stub the job body, so this is the only place the field
    // wiring is checked.
    #[test]
    fn extraction_outcome_maps_onto_the_job_report() {
        let success = JobSuccess::from_extraction(extraction::ExtractionOutcome {
            summary: ChangeSummary {
                wrote_data: true,
                deleted_data: true,
                tags_changed: true,
            },
            loaded_model: Some("group/model-a".to_string()),
        });
        assert_eq!(
            success.summary,
            ChangeSummary {
                wrote_data: true,
                deleted_data: true,
                tags_changed: true
            }
        );
        assert_eq!(success.loaded_model.as_deref(), Some("group/model-a"));

        let no_data = JobSuccess::from_extraction(extraction::ExtractionOutcome {
            summary: ChangeSummary::default(),
            loaded_model: None,
        });
        assert_eq!(no_data.summary, ChangeSummary::default());
        assert_eq!(no_data.loaded_model, None);
    }

    // A cancelled extraction job reports nothing, so the queue assumes it
    // loaded its model and unloads it at the boundary — an explicit unload the
    // cancel path never had before (it relied entirely on the TTL sweep).
    #[tokio::test]
    async fn cancelled_extraction_tracks_and_unloads_its_model() {
        let (queue, handle) = spawn_test_queue().await;
        let db = unique_db("batch-cancel");
        let setter = "group/model-a";
        let running = enqueue_on(&queue, extraction_job(&db, setter, "60000")).await;
        wait_for_running(&queue, running.queue_id).await;

        // Suppressed: this is about the model, and the maintenance job the
        // cancel would otherwise synthesize is not part of it.
        assert_eq!(
            cancel_running_on(&queue, true).await,
            Some(running.queue_id)
        );
        let state = debug_on(&queue).await;
        assert_eq!(
            state.unloads,
            vec![setter.to_string()],
            "a cancelled extraction must be assumed to have loaded: {state:?}"
        );
        assert!(state.batch_loaded.is_none(), "{state:?}");

        queue.stop(None);
        handle.await.unwrap();
    }

    // The maintenance job is a normal queue row: cancelling it does not put
    // its flags back, so it is not immediately resurrected by the next
    // boundary. Its work is simply skipped until something changes again.
    #[tokio::test]
    async fn cancelling_maintenance_does_not_resurrect_it() {
        let (queue, handle) = spawn_test_queue().await;
        let db = unique_db("owed-cancel-maint");
        let other_db = unique_db("owed-cancel-other");
        // Order matters: the reporting job runs first, a long job for another
        // DB then occupies the runner, and the last job for `db` is cancelled
        // while queued — so the synthesized maintenance job stays queued and
        // can be observed and cancelled instead of running immediately.
        let _reporting = enqueue_on(&queue, report_job(&db, "10:w")).await;
        let _blocker = enqueue_on(&queue, sleep_job(&other_db, 60_000)).await;
        let last = enqueue_on(&queue, sleep_job(&db, 60_000)).await;

        wait_for(&queue, |state| state.owed.contains_key(&db)).await;
        let cancelled = cancel_on(&queue, vec![last.queue_id]).await;
        assert_eq!(cancelled, vec![last.queue_id]);

        let state = debug_on(&queue).await;
        assert_eq!(state.synthesized.len(), 1, "expected synthesis: {state:?}");
        let status = status_on(&queue).await;
        let maintenance = status
            .queue
            .iter()
            .find(|entry| entry.job_type == JobType::DbMaintenance)
            .expect("maintenance job should be queued");
        assert_eq!(maintenance.index_db, db);
        assert_eq!(maintenance.metadata.as_deref(), Some("wrote_data"));
        assert!(!maintenance.running);

        let cancelled = cancel_on(&queue, vec![maintenance.queue_id]).await;
        assert_eq!(cancelled, vec![maintenance.queue_id]);
        let state = debug_on(&queue).await;
        assert!(
            !state.owed.contains_key(&db),
            "cancelled maintenance must not re-own its flags: {state:?}"
        );
        assert_eq!(state.synthesized.len(), 1, "no second synthesis: {state:?}");

        queue.stop(None);
        handle.await.unwrap();
    }
}
