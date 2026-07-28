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
    /// flags `w` = wrote_data, `d` = deleted_data), so boundary scheduling is
    /// testable without touching a database.
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
}

impl ChangeSummary {
    fn is_empty(self) -> bool {
        !self.wrote_data && !self.deleted_data
    }

    pub(crate) fn or_with(&mut self, other: Self) {
        self.wrote_data |= other.wrote_data;
        self.deleted_data |= other.deleted_data;
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
        flags.join(",")
    }

    fn from_metadata(metadata: Option<&str>) -> Self {
        let mut summary = Self::default();
        for flag in metadata.unwrap_or_default().split(',') {
            match flag.trim() {
                "wrote_data" => summary.wrote_data = true,
                "deleted_data" => summary.deleted_data = true,
                _ => {}
            }
        }
        summary
    }
}

/// What a job reports back to the queue when it finishes successfully.
pub(crate) struct JobSuccess {
    pub summary: ChangeSummary,
    /// The batch-cache model the job left loaded, if any. Recorded now;
    /// consumed by the boundary's model-continuity rule (design phase 2).
    #[allow(dead_code)]
    pub loaded_model: Option<String>,
}

impl JobSuccess {
    fn from_summary(summary: ChangeSummary) -> Self {
        Self {
            summary,
            loaded_model: None,
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
    /// Recorded for the phase-2 model-continuity rule; nothing reads it yet.
    #[allow(dead_code)]
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

/// Dedup condition for batch enqueueing: the whole batch is skipped when any
/// queued or running job carries this tag for this index DB.
#[derive(Debug, Clone)]
pub(crate) struct BatchDedup {
    pub tag: String,
    pub index_db: String,
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
    /// All-or-nothing enqueue of several jobs, with an optional dedup check
    /// evaluated atomically inside the actor (a check-then-enqueue done by the
    /// caller would race concurrent triggers). Replies `None` when skipped.
    EnqueueBatch {
        requests: Vec<JobRequest>,
        dedup: Option<BatchDedup>,
        reply: oneshot::Sender<ApiResult<Option<Vec<JobModel>>>>,
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
    #[cfg(test)]
    synthesized: Vec<(String, ChangeSummary)>,
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
            #[cfg(test)]
            synthesized: Vec::new(),
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
                dedup,
                reply,
            } => {
                if state.shutting_down {
                    let _ = reply.send(Err(ApiError::internal("Job queue is shutting down")));
                    return Ok(());
                }
                let conflict = dedup.as_ref().is_some_and(|dedup| {
                    state
                        .running_job
                        .iter()
                        .chain(state.queue.iter())
                        .any(|job| {
                            job.tag.as_deref() == Some(dedup.tag.as_str())
                                && job.index_db == dedup.index_db
                        })
                });
                if conflict {
                    let _ = reply.send(Ok(None));
                } else {
                    let models = requests
                        .into_iter()
                        .map(|request| push_job(state, request))
                        .collect();
                    if state.running_job.is_none() {
                        start_next_job(state).await;
                    }
                    let _ = reply.send(Ok(Some(models)));
                }
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
                        // Before starting the next job: a maintenance job goes
                        // to the front of the queue, which is exactly the slot
                        // the maintenance work occupied before it was deferred.
                        maybe_schedule_maintenance(state, &finished);
                        start_next_job(state).await;
                    }
                }
            }
            JobQueueMessage::RecordOwed { index_db, summary } => {
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
                let _ = reply.send(cancelled);
            }
            #[cfg(test)]
            JobQueueMessage::DebugState { reply } => {
                let _ = reply.send(QueueDebugState {
                    owed: state.owed.clone(),
                    synthesized: state.synthesized.clone(),
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
                if !suppress_maintenance {
                    maybe_schedule_maintenance(state, &running);
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
        _ => ChangeSummary {
            wrote_data: true,
            deleted_data: matches!(
                job_type,
                JobType::FolderRescan
                    | JobType::FolderUpdate
                    | JobType::DataDeletion
                    | JobType::JobDataDeletion
            ),
        },
    }
}

fn record_owed(state: &mut JobQueueState, job: &Job, summary: Option<ChangeSummary>) {
    if job.job_type == JobType::DbMaintenance {
        return;
    }
    let summary = summary.unwrap_or_else(|| pessimistic_summary(&job.job_type));
    merge_owed(state, &job.index_db, summary);
}

fn merge_owed(state: &mut JobQueueState, index_db: &str, summary: ChangeSummary) {
    if summary.is_empty() {
        return;
    }
    state
        .owed
        .entry(index_db.to_string())
        .or_default()
        .or_with(summary);
}

/// The job boundary: when nothing else in the queue targets the finished job's
/// index DB, the maintenance its finished jobs owe is synthesized as a real
/// queue job at the front of the queue (visible, cancellable, and serialized
/// against other jobs like everything else).
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
    state.job_counter += 1;
    let job = Job {
        queue_id: state.job_counter,
        job_type: JobType::DbMaintenance,
        index_db: finished.index_db.clone(),
        user_data_db: finished.user_data_db.clone(),
        metadata: Some(owed.to_metadata()),
        batch_size: None,
        threshold: None,
        log_id: None,
        tag: None,
    };
    tracing::info!(
        index_db,
        queue_id = job.queue_id,
        owed = %owed.to_metadata(),
        "scheduling deferred database maintenance"
    );
    #[cfg(test)]
    state.synthesized.push((finished.index_db.clone(), owed));
    state.queue.push_front(job.clone());
    state.queued_jobs.insert(job.queue_id, job);
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
    crate::jobs::files::run_post_job_maintenance(&job.index_db, summary.deleted_data).await;
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
            let outcome = extraction::run_extraction_job(job.clone())
                .await
                .map_err(|err| format!("{err}"))?;
            vector_quants::finishing_phase(&job.index_db).await;
            Ok(JobSuccess {
                summary: outcome.summary,
                loaded_model: outcome.loaded_model,
            })
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

/// Enqueues all `requests` atomically, unless `dedup` matches a queued or
/// running job — in which case nothing is enqueued and `None` is returned.
pub(crate) async fn enqueue_jobs_unless_tagged(
    requests: Vec<JobRequest>,
    dedup: Option<BatchDedup>,
) -> ApiResult<Option<Vec<JobModel>>> {
    let queue = ensure_job_queue().await?;
    let (reply, rx) = oneshot::channel();
    queue
        .send_message(JobQueueMessage::EnqueueBatch {
            requests,
            dedup,
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
        dedup: Option<BatchDedup>,
    ) -> Option<Vec<JobModel>> {
        let (reply, rx) = oneshot::channel();
        queue
            .send_message(JobQueueMessage::EnqueueBatch {
                requests,
                dedup,
                reply,
            })
            .unwrap();
        rx.await.unwrap().unwrap()
    }

    // The batch dedup must be atomic in the actor: while a tagged job for the
    // same index DB is queued or running, the whole batch is skipped; other
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
        let dedup = || {
            Some(BatchDedup {
                tag: "cronjob".to_string(),
                index_db: "default".to_string(),
            })
        };

        let first = enqueue_batch_on(&queue, vec![job.clone(), job.clone()], dedup()).await;
        assert_eq!(first.map(|jobs| jobs.len()), Some(2));

        // One of the batch is running, one queued: a second batch is skipped.
        let second = enqueue_batch_on(&queue, vec![job.clone()], dedup()).await;
        assert!(second.is_none());

        // A different index DB does not collide with the dedup condition.
        let other_db = JobRequest {
            index_db: "other".to_string(),
            ..job.clone()
        };
        let other = enqueue_batch_on(
            &queue,
            vec![other_db],
            Some(BatchDedup {
                tag: "cronjob".to_string(),
                index_db: "other".to_string(),
            }),
        )
        .await;
        assert_eq!(other.map(|jobs| jobs.len()), Some(1));

        // After all tagged jobs have drained, the batch enqueues again.
        tokio::time::sleep(std::time::Duration::from_millis(900)).await;
        let status = status_on(&queue).await;
        assert!(status.queue.is_empty(), "queue should be idle: {status:?}");
        let third = enqueue_batch_on(&queue, vec![job.clone()], dedup()).await;
        assert_eq!(third.map(|jobs| jobs.len()), Some(1));

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
                deleted_data: false
            })
        );
        assert!(state.synthesized.is_empty(), "too early: {state:?}");

        // After the second: one maintenance job for the union of both.
        let state = wait_for(&queue, |state| !state.synthesized.is_empty()).await;
        assert_eq!(
            state.synthesized,
            vec![(
                db.clone(),
                ChangeSummary {
                    wrote_data: true,
                    deleted_data: true
                }
            )]
        );
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
                    deleted_data: false
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
                },
            })
            .unwrap();

        // Merged, but never scheduled on its own: the reporting job is still
        // running.
        let state = wait_for(&queue, |state| state.owed.contains_key(&db)).await;
        assert!(state.synthesized.is_empty(), "too early: {state:?}");

        // Cancelling the reporting job on its own would only owe `wrote_data`;
        // the deletion flag can only have come from the mid-job report.
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
                    deleted_data: true
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
                deleted_data: false
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
            },
            ChangeSummary {
                wrote_data: false,
                deleted_data: true,
            },
            ChangeSummary {
                wrote_data: true,
                deleted_data: true,
            },
        ] {
            let metadata = summary.to_metadata();
            assert_eq!(ChangeSummary::from_metadata(Some(&metadata)), summary);
        }
        assert_eq!(ChangeSummary::from_metadata(None), ChangeSummary::default());
        assert_eq!(
            ChangeSummary::from_metadata(Some("nonsense")),
            ChangeSummary::default()
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
                    deleted_data: true
                },
                "{job_type:?} may have cascaded deletes"
            );
        }
        assert_eq!(
            pessimistic_summary(&JobType::DataExtraction),
            ChangeSummary {
                wrote_data: true,
                deleted_data: false
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
