use std::collections::{HashMap, VecDeque};

use ractor::{Actor, ActorProcessingErr, ActorRef};
use serde::{Deserialize, Serialize};
use tokio::sync::{OnceCell, oneshot};
use tokio::task::JoinHandle;
use utoipa::ToSchema;

use crate::api_error::ApiError;
use crate::db::index_writer::IndexDbWriterMessage;
use crate::db::index_writer::call_index_db_writer;
use crate::jobs::continuous_scan;
use crate::jobs::files::FileScanService;
use crate::jobs::extraction;

type ApiResult<T> = std::result::Result<T, ApiError>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JobType {
    DataExtraction,
    DataDeletion,
    FolderRescan,
    FolderUpdate,
    JobDataDeletion,
    #[cfg(test)]
    #[serde(rename = "test_sleep")]
    TestSleep,
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
    GetQueueStatus {
        reply: oneshot::Sender<ApiResult<QueueStatusModel>>,
    },
    CancelQueued {
        queue_ids: Vec<i64>,
        reply: oneshot::Sender<ApiResult<Vec<i64>>>,
    },
    CancelRunning {
        reply: oneshot::Sender<ApiResult<Option<i64>>>,
    },
    RunnerFinished {
        queue_id: i64,
        result: JobRunResult,
    },
}

pub(crate) struct JobQueueActor;

pub(crate) struct JobQueueArgs {
    pub runner_name: Option<String>,
}

pub(crate) struct JobQueueState {
    queue: VecDeque<Job>,
    queued_jobs: HashMap<i64, Job>,
    running_job: Option<Job>,
    job_counter: i64,
    runner: ActorRef<JobRunnerMessage>,
}

pub(crate) enum JobRunnerMessage {
    RunJob {
        job: Job,
        reply: oneshot::Sender<ApiResult<()>>,
    },
    CancelRunning {
        reply: oneshot::Sender<ApiResult<Option<i64>>>,
    },
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
    handle: JoinHandle<()>,
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
            job_counter: 0,
            runner,
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
                if state.running_job.is_none() {
                    start_next_job(state).await;
                }
                let _ = reply.send(Ok(model));
            }
            JobQueueMessage::GetQueueStatus { reply } => {
                let mut queue = Vec::new();
                if let Some(running) = state.running_job.as_ref() {
                    queue.push(JobModel::from_job(running, true));
                }
                for job in state.queue.iter() {
                    queue.push(JobModel::from_job(job, false));
                }
                let _ = reply.send(Ok(QueueStatusModel { queue }));
            }
            JobQueueMessage::CancelQueued { queue_ids, reply } => {
                let mut cancelled = Vec::new();
                for queue_id in queue_ids {
                    if let Some(running) = state.running_job.as_ref() {
                        if running.queue_id == queue_id {
                            let _ = cancel_running_job_inner(state).await;
                            cancelled.push(queue_id);
                            continue;
                        }
                    }
                    if let Some(job) = state.queued_jobs.remove(&queue_id) {
                        state.queue.retain(|entry| entry.queue_id != queue_id);
                        cancelled.push(job.queue_id);
                    }
                }
                let _ = reply.send(Ok(cancelled));
            }
            JobQueueMessage::CancelRunning { reply } => {
                let result = cancel_running_job_inner(state).await;
                let _ = reply.send(Ok(result));
            }
            JobQueueMessage::RunnerFinished { queue_id, result } => {
                if let Some(running) = state.running_job.as_ref() {
                    if running.queue_id == queue_id {
                        if !result.success {
                            tracing::error!(
                                error = %result.error.unwrap_or_else(|| "unknown job error".to_string()),
                                queue_id,
                                "job failed"
                            );
                        }
                        state.running_job = None;
                        start_next_job(state).await;
                    }
                }
            }
        }
        Ok(())
    }
}

async fn start_next_job(state: &mut JobQueueState) {
    if state.running_job.is_some() {
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
        return;
    }
    match rx.await {
        Ok(Ok(())) => {
            state.running_job = Some(job);
        }
        Ok(Err(err)) => {
            tracing::error!(error = ?err, queue_id = job.queue_id, "job runner rejected job");
        }
        Err(_) => {
            tracing::error!(queue_id = job.queue_id, "job runner dropped response");
        }
    }
}

async fn cancel_running_job_inner(state: &mut JobQueueState) -> Option<i64> {
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
                start_next_job(state).await;
                Some(queue_id)
            } else {
                None
            }
        }
        _ => None,
    }
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
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            JobRunnerMessage::RunJob { job, reply } => {
                if state.running.is_some() {
                    let _ = reply.send(Err(ApiError::internal("Job runner busy")));
                    return Ok(());
                }
                let queue = state.queue.clone();
                let queue_id = job.queue_id;
                let handle = tokio::spawn(async move {
                    let result = execute_job(job).await;
                    let run_result = match result {
                        Ok(()) => JobRunResult {
                            success: true,
                            error: None,
                        },
                        Err(err) => JobRunResult {
                            success: false,
                            error: Some(err),
                        },
                    };
                    let _ = queue.send_message(JobQueueMessage::RunnerFinished {
                        queue_id,
                        result: run_result,
                    });
                });
                state.running = Some(RunningJob { queue_id, handle });
                let _ = reply.send(Ok(()));
            }
            JobRunnerMessage::CancelRunning { reply } => {
                if let Some(running) = state.running.take() {
                    running.handle.abort();
                    let _ = state.queue.send_message(JobQueueMessage::RunnerFinished {
                        queue_id: running.queue_id,
                        result: JobRunResult {
                            success: false,
                            error: Some("Job cancelled".to_string()),
                        },
                    });
                    let _ = reply.send(Ok(Some(running.queue_id)));
                } else {
                    let _ = reply.send(Ok(None));
                }
            }
        }
        Ok(())
    }
}

async fn execute_job(job: Job) -> Result<(), String> {
    match job.job_type {
        JobType::FolderRescan => {
            continuous_scan::pause_for_job(&job.index_db)
                .await
                .map_err(|err| format!("{err:?}"))?;
            let service = FileScanService::from_env(job.index_db.clone(), job.user_data_db);
            let result = service.rescan_folders().await;
            let _ = continuous_scan::resume_after_job(&job.index_db).await;
            result.map_err(|err| format!("{err:?}"))?;
            Ok(())
        }
        JobType::FolderUpdate => {
            continuous_scan::pause_for_job(&job.index_db)
                .await
                .map_err(|err| format!("{err:?}"))?;
            let service = FileScanService::from_env(job.index_db.clone(), job.user_data_db);
            let result = service.run_folder_update().await;
            let _ = continuous_scan::resume_after_job(&job.index_db).await;
            result.map_err(|err| format!("{err:?}"))?;
            Ok(())
        }
        JobType::DataExtraction => {
            extraction::run_extraction_job(job)
                .await
                .map_err(|err| format!("{err}"))?;
            Ok(())
        }
        JobType::DataDeletion => {
            extraction::run_data_deletion_job(job)
                .await
                .map_err(|err| format!("{err}"))?;
            Ok(())
        }
        JobType::JobDataDeletion => {
            let log_id = job.log_id.ok_or_else(|| "Log ID required".to_string())?;
            call_index_db_writer(&job.index_db, |reply| IndexDbWriterMessage::DeleteJobData {
                log_id,
                reply,
            })
            .await
            .map_err(|err| format!("{err:?}"))?;
            Ok(())
        }
        #[cfg(test)]
        JobType::TestSleep => {
            let delay = job
                .tag
                .as_deref()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(200);
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            Ok(())
        }
        _ => Err("Job type not implemented".to_string()),
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

pub(crate) async fn get_queue_status() -> ApiResult<QueueStatusModel> {
    let queue = ensure_job_queue().await?;
    let (reply, rx) = oneshot::channel();
    queue
        .send_message(JobQueueMessage::GetQueueStatus { reply })
        .map_err(|_| ApiError::internal("Job queue unavailable"))?;
    rx.await
        .map_err(|_| ApiError::internal("Job queue dropped response"))?
}

pub(crate) async fn cancel_queued_jobs(queue_ids: Vec<i64>) -> ApiResult<Vec<i64>> {
    let queue = ensure_job_queue().await?;
    let (reply, rx) = oneshot::channel();
    queue
        .send_message(JobQueueMessage::CancelQueued { queue_ids, reply })
        .map_err(|_| ApiError::internal("Job queue unavailable"))?;
    rx.await
        .map_err(|_| ApiError::internal("Job queue dropped response"))?
}

pub(crate) async fn cancel_running_job() -> ApiResult<Option<i64>> {
    let queue = ensure_job_queue().await?;
    let (reply, rx) = oneshot::channel();
    queue
        .send_message(JobQueueMessage::CancelRunning { reply })
        .map_err(|_| ApiError::internal("Job queue unavailable"))?;
    rx.await
        .map_err(|_| ApiError::internal("Job queue dropped response"))?
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
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
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
        let (reply, rx) = oneshot::channel();
        queue
            .send_message(JobQueueMessage::CancelQueued {
                queue_ids: ids,
                reply,
            })
            .unwrap();
        rx.await.unwrap().unwrap()
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
        let job = JobRequest {
            job_type: JobType::TestSleep,
            index_db: "default".to_string(),
            user_data_db: "default".to_string(),
            metadata: None,
            batch_size: None,
            threshold: None,
            log_id: None,
            tag: Some("500".to_string()),
        };
        let running = enqueue_on(&queue, job).await;

        let (reply, rx) = oneshot::channel();
        queue
            .send_message(JobQueueMessage::CancelRunning { reply })
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
}
