//! The transcode worker pool: one ractor actor owning the FIFO queue, the
//! in-flight dedup map, and the job event channels
//! (docs/video-transcoding-design.md §2, implementation plan §1 pool.rs).
//!
//! Deliberately **not** the job queue. `JobQueueActor` is serial
//! process-wide, so a transcode behind a folder scan would wait minutes for a
//! two-second encode — and a scan behind a transcode would be worse. This pool
//! has its own bound (`[transcode] max_concurrent_jobs`) covering both CPU and
//! the hardware encoder's own session limit.
//!
//! The actor never encodes and never blocks on one: dispatch spawns a task
//! that runs the (blocking) encode on the blocking pool, commits the artifact,
//! and reports back with a `Finished` message. Everything the actor itself
//! awaits is a single-row SQLite query.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use ractor::{Actor, ActorProcessingErr, ActorRef};
use serde::Serialize;
use tokio::sync::{OnceCell, oneshot, watch};
use utoipa::ToSchema;
use uuid::Uuid;

use super::TranscodeParams;
use super::cache::{CachedArtifact, NewArtifact, TranscodeCache};
use super::compose::{self, ComposeParams};
use super::presets::ResolvedPreset;
use super::run::{self, ComposeJobSpec, EncodeError, EncodeJobSpec, EncodeTask};
use crate::api_error::ApiError;

type ApiResult<T> = std::result::Result<T, ApiError>;

/// How long a finished job stays queryable. Long enough for a client that
/// missed the terminal event to come back and ask, short enough that the map
/// is not a leak.
const TERMINAL_TTL: Duration = Duration::from_secs(15 * 60);
/// Ceiling on remembered terminal jobs, whatever the TTL says.
const TERMINAL_CAPACITY: usize = 512;
/// Floor on the interval between progress messages, per job. ffmpeg reports
/// about twice a second and a mosaic may have several jobs in flight; the
/// mailbox (and every SSE stream behind it) is better served by a rate the UI
/// can actually render.
const PROGRESS_INTERVAL: Duration = Duration::from_millis(250);
/// Bound on waiting for running encodes at shutdown. Their cancel flags are
/// already set and the runner's watchdog kills the child within a poll
/// interval, so in practice this is milliseconds; past it the pool stops
/// regardless rather than holding the process open.
const SHUTDOWN_DRAIN: Duration = Duration::from_secs(5);

/// A finished artifact, as every client-facing shape refers to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub(crate) struct ArtifactRef {
    /// Cache key; also the artifact's ETag and its `?key=` query value.
    pub(crate) key: String,
    pub(crate) mime_type: String,
    pub(crate) size_bytes: i64,
    /// Ready-to-use URL for `GET /api/video/artifact`.
    pub(crate) url: String,
    /// The name a download should be saved under, computed server-side from
    /// the request that produced the artifact (implementation plan §3 S3).
    ///
    /// It rides here because [`Self::url`] is the `key=` form, and that form
    /// cannot name a download: a key knows the source hash and the settings,
    /// never the file's path or whether the request was trimmed. The client
    /// hangs this on its `<a download>` (the §0.4 precedent — naming inputs
    /// belong to the server, so clients keep no lookup tables).
    ///
    /// A *joined* job answers with the first submitter's name. The bytes are
    /// identical by construction (the key covers the source hash), so the most
    /// this can cost is the stem of one of several files with the same
    /// content.
    pub(crate) filename: String,
}

impl ArtifactRef {
    pub(crate) fn new(artifact: &CachedArtifact, filename: String) -> Self {
        Self {
            key: artifact.key.clone(),
            mime_type: artifact.mime_type.clone(),
            size_bytes: artifact.size_bytes,
            url: format!("/api/video/artifact?key={}", artifact.key),
            filename,
        }
    }
}

/// Job state, as both the SSE payload and the snapshot body. Deliberately
/// generic (no transcode-specific fields): `jobs/queue.rs`'s polled status is
/// expected to migrate onto the same envelope.
#[derive(Debug, Clone, PartialEq, Serialize, ToSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum TranscodeJobEvent {
    /// 1-based position in the queue.
    Queued { position: usize },
    /// `None` while the output length is unknown (a source with no recorded
    /// duration): the job is running, the percentage is not knowable.
    Running { progress: Option<f32> },
    Done { artifact: ArtifactRef },
    Failed { error: String, cancelled: bool },
}

impl TranscodeJobEvent {
    pub(crate) fn is_terminal(&self) -> bool {
        matches!(
            self,
            TranscodeJobEvent::Done { .. } | TranscodeJobEvent::Failed { .. }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, ToSchema)]
pub(crate) struct TranscodeJobSnapshot {
    pub(crate) id: String,
    #[serde(flatten)]
    pub(crate) event: TranscodeJobEvent,
}

/// How much of the pool one job occupies. A composition of many inputs runs
/// alone: its filtergraph holds every input's loop buffer at once, so pairing
/// it with anything else is how a host runs out of memory. Single-file jobs
/// are always light; the heavy consumer arrives with `compose` in phase 4.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JobWeight {
    Light,
    Exclusive,
}

/// What is to be encoded: one file with a preset, or a composition of many.
///
/// Everything the pool does with a job — key it, dedup it, name its download,
/// publish its artifact — goes through the accessors below, so the two kinds
/// share the queue, the event channels and the cache without the actor ever
/// matching on them.
#[derive(Debug, Clone)]
pub(crate) enum JobRequest {
    Single {
        params: Box<TranscodeParams>,
        source_path: PathBuf,
        source_duration_s: Option<f64>,
    },
    Compose {
        params: Box<ComposeParams>,
        /// Source paths, parallel to the document's items.
        sources: Vec<PathBuf>,
    },
}

impl JobRequest {
    pub(crate) fn cache_key(&self) -> String {
        match self {
            JobRequest::Single { params, .. } => params.cache_key(),
            JobRequest::Compose { params, .. } => params.cache_key(),
        }
    }

    fn params_hash(&self) -> String {
        match self {
            JobRequest::Single { params, .. } => params.params_hash(),
            JobRequest::Compose { params, .. } => params.params_hash(),
        }
    }

    /// The `source_sha256` column of the artifact row. A composition has as
    /// many sources as it has items and no one of them names it, so it records
    /// the same sentinel its key is prefixed with.
    fn source_sha256(&self) -> String {
        match self {
            JobRequest::Single { params, .. } => params.source_sha256.clone(),
            JobRequest::Compose { .. } => compose::COMPOSE_KEY_PREFIX.to_string(),
        }
    }

    /// Whether a failed encode of this job is a verdict worth remembering.
    ///
    /// Only a single file's is. A composition's key hashes N inputs, so one
    /// transient failure among them — a mount that dropped, a file replaced
    /// mid-encode, a source another process was still writing — would put a
    /// strike against the *whole* document, and two of those would refuse
    /// every later attempt at a mosaic that is perfectly renderable. There is
    /// no per-input verdict to record instead (the failure is ffmpeg's on the
    /// graph, not on any one item), so a composition is simply always
    /// retryable.
    fn records_failures(&self) -> bool {
        matches!(self, JobRequest::Single { .. })
    }

    fn preset(&self) -> &ResolvedPreset {
        match self {
            JobRequest::Single { params, .. } => &params.preset,
            JobRequest::Compose { params, .. } => &params.preset,
        }
    }

    fn transcoder_version(&self) -> i64 {
        match self {
            JobRequest::Single { params, .. } => params.transcoder_version,
            JobRequest::Compose { params, .. } => params.transcoder_version,
        }
    }

    fn artifact_file_name(&self) -> String {
        match self {
            JobRequest::Single { params, .. } => params.artifact_file_name(),
            JobRequest::Compose { params, .. } => params.artifact_file_name(),
        }
    }

    fn mime_type(&self) -> &'static str {
        match self {
            JobRequest::Single { params, .. } => params.mime_type(),
            JobRequest::Compose { params, .. } => params.mime_type(),
        }
    }

    /// The name a download of this job's artifact carries. A composition has
    /// no source stem to build one from, so its scheme is fixed.
    fn download_file_name(&self, stem: Option<&str>) -> String {
        match self {
            JobRequest::Single { params, .. } => params.download_file_name(stem),
            JobRequest::Compose { params, .. } => params.download_file_name(),
        }
    }

    /// The dispatched form, once the cache has named the temporary to write.
    fn into_task(self, output: PathBuf) -> EncodeTask {
        match self {
            JobRequest::Single {
                params,
                source_path,
                source_duration_s,
            } => EncodeTask::Single(Box::new(EncodeJobSpec {
                input: source_path,
                output,
                params: *params,
                source_duration_s,
            })),
            JobRequest::Compose { params, sources } => {
                EncodeTask::Compose(Box::new(ComposeJobSpec {
                    sources,
                    output,
                    params: *params,
                }))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SubmitRequest {
    pub(crate) job: JobRequest,
    pub(crate) weight: JobWeight,
    /// Stem the download name is built from, decided by the caller.
    ///
    /// Deliberately *not* derived from the source path: that path is whichever
    /// of the item's files turned out to be readable, so naming a download
    /// after it would make the name depend on which copy of the same content
    /// answered — including on whether a network mount happened to be up. The
    /// API layer passes the item's first file's stem for both this and the
    /// artifact route's `Content-Disposition`, so one item names its downloads
    /// one way. Compositions ignore it: they have no single source.
    pub(crate) download_stem: Option<String>,
}

/// What a submit resolved to. Every variant except `Hit` carries a job the
/// client can follow, including `KnownFailure` — a settled negative verdict is
/// reported as a job that is already `Failed`, so the client has one flow.
#[derive(Debug, Clone)]
pub(crate) enum SubmitOutcome {
    Hit(ArtifactRef),
    Created(TranscodeJobSnapshot),
    Joined(TranscodeJobSnapshot),
    KnownFailure(TranscodeJobSnapshot),
}

impl SubmitOutcome {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            SubmitOutcome::Hit(_) => "hit",
            SubmitOutcome::Created(_) => "created",
            SubmitOutcome::Joined(_) => "joined",
            SubmitOutcome::KnownFailure(_) => "known_failure",
        }
    }
}

/// The encode step, injectable so the actor's own behaviour (ordering,
/// dedup, cancellation, retention) is testable without a toolchain.
pub(crate) type EncodeRunner = Arc<
    dyn Fn(
            EncodeTask,
            Arc<AtomicBool>,
            Box<dyn FnMut(Option<f32>) + Send>,
        ) -> Result<(), EncodeError>
        + Send
        + Sync,
>;

pub(crate) fn ffmpeg_runner() -> EncodeRunner {
    Arc::new(|task, cancel, mut progress| {
        run::run_task(&task, &cancel, &mut |value| progress(value))
    })
}

pub(crate) enum PoolMsg {
    Submit {
        request: Box<SubmitRequest>,
        reply: oneshot::Sender<SubmitOutcome>,
    },
    Snapshot {
        id: Uuid,
        reply: oneshot::Sender<Option<TranscodeJobSnapshot>>,
    },
    /// The SSE subscription. The receiver's current value is the snapshot, so
    /// a stream's first event needs no separate round trip.
    Subscribe {
        id: Uuid,
        reply: oneshot::Sender<Option<watch::Receiver<TranscodeJobSnapshot>>>,
    },
    Cancel {
        id: Uuid,
        reply: oneshot::Sender<bool>,
    },
    /// The in-flight job for a cache key, if any. Lets an artifact miss point
    /// at the job that is already producing it instead of saying only "no".
    JobForKey {
        key: String,
        reply: oneshot::Sender<Option<TranscodeJobSnapshot>>,
    },
    Progress {
        id: Uuid,
        progress: Option<f32>,
    },
    Finished {
        id: Uuid,
        outcome: JobOutcome,
    },
    /// Settles the queue, flags every running encode, and replies once the
    /// last of them has actually finished — so nothing relies on the runtime
    /// being dropped to reap an ffmpeg child.
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

pub(crate) enum JobOutcome {
    Done(ArtifactRef),
    Failed { error: String, cancelled: bool },
}

/// What a queued job still needs to be dispatched. Taken (not cloned) at
/// dispatch, so a job cannot be started twice.
struct PendingEncode {
    job: JobRequest,
    download_stem: Option<String>,
}

struct JobEntry {
    key: String,
    weight: JobWeight,
    cancel: Arc<AtomicBool>,
    /// The event channel *is* the job's state: the snapshot lives in the
    /// watch value, so a read and a broadcast cannot drift apart.
    events: watch::Sender<TranscodeJobSnapshot>,
    pending: Option<PendingEncode>,
}

impl JobEntry {
    fn snapshot(&self) -> TranscodeJobSnapshot {
        self.events.borrow().clone()
    }

    fn publish(&self, event: TranscodeJobEvent) {
        let id = self.events.borrow().id.clone();
        self.events.send_replace(TranscodeJobSnapshot { id, event });
    }
}

pub(crate) struct TranscodePool;

pub(crate) struct PoolArgs {
    pub(crate) cache: Arc<TranscodeCache>,
    pub(crate) runner: EncodeRunner,
    pub(crate) max_concurrent: usize,
    pub(crate) terminal_ttl: Duration,
    pub(crate) terminal_capacity: usize,
}

pub(crate) struct PoolState {
    cache: Arc<TranscodeCache>,
    runner: EncodeRunner,
    max_concurrent: usize,
    terminal_ttl: Duration,
    terminal_capacity: usize,
    /// FIFO. Position in this deque is the queue position clients see.
    queued: VecDeque<Uuid>,
    jobs: HashMap<Uuid, JobEntry>,
    /// Cache key → in-flight job, the dedup/join map.
    by_key: HashMap<String, Uuid>,
    /// Finished jobs, oldest first, with the instant they finished.
    terminal: VecDeque<(Uuid, Instant)>,
    running: usize,
    /// An exclusive job holds the whole pool: nothing else dispatches until
    /// it finishes.
    exclusive_running: bool,
    shutting_down: bool,
    /// Held from [`PoolMsg::Shutdown`] until the last running encode reports
    /// back, which is what makes the shutdown wait bounded by the encodes
    /// rather than by a guess.
    drained: Option<oneshot::Sender<()>>,
}

impl Actor for TranscodePool {
    type Msg = PoolMsg;
    type State = PoolState;
    type Arguments = PoolArgs;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(PoolState {
            cache: args.cache,
            runner: args.runner,
            max_concurrent: args.max_concurrent.max(1),
            terminal_ttl: args.terminal_ttl,
            terminal_capacity: args.terminal_capacity,
            queued: VecDeque::new(),
            jobs: HashMap::new(),
            by_key: HashMap::new(),
            terminal: VecDeque::new(),
            running: 0,
            exclusive_running: false,
            shutting_down: false,
            drained: None,
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            PoolMsg::Submit { request, reply } => {
                let outcome = submit_job(state, &myself, *request).await;
                let _ = reply.send(outcome);
            }
            PoolMsg::Snapshot { id, reply } => {
                let _ = reply.send(state.jobs.get(&id).map(JobEntry::snapshot));
            }
            PoolMsg::Subscribe { id, reply } => {
                let _ = reply.send(state.jobs.get(&id).map(|entry| entry.events.subscribe()));
            }
            PoolMsg::Cancel { id, reply } => {
                let _ = reply.send(cancel_job(state, id));
            }
            PoolMsg::JobForKey { key, reply } => {
                let snapshot = state
                    .by_key
                    .get(&key)
                    .and_then(|id| state.jobs.get(id))
                    .map(JobEntry::snapshot);
                let _ = reply.send(snapshot);
            }
            PoolMsg::Progress { id, progress } => {
                if let Some(entry) = state.jobs.get(&id) {
                    // A progress message that raced a terminal transition must
                    // not resurrect the job.
                    if !entry.snapshot().event.is_terminal() {
                        entry.publish(TranscodeJobEvent::Running { progress });
                    }
                }
            }
            PoolMsg::Finished { id, outcome } => {
                finish_job(state, &myself, id, outcome);
            }
            PoolMsg::Shutdown { reply } => {
                state.shutting_down = true;
                // Queued jobs never started, so they end here; running ones
                // are asked to stop and end through their own completion.
                for id in std::mem::take(&mut state.queued) {
                    let Some(entry) = state.jobs.get(&id) else {
                        continue;
                    };
                    entry.publish(TranscodeJobEvent::Failed {
                        error: "server is shutting down".to_string(),
                        cancelled: true,
                    });
                    let key = entry.key.clone();
                    if state.by_key.get(&key) == Some(&id) {
                        state.by_key.remove(&key);
                    }
                    // Terminal like any other settled job: a client that was
                    // mid-request must still be able to read the verdict.
                    state.terminal.push_back((id, Instant::now()));
                    prune_terminal(state);
                }
                for entry in state.jobs.values() {
                    entry.cancel.store(true, Ordering::Relaxed);
                }
                if state.running == 0 {
                    let _ = reply.send(());
                } else {
                    state.drained = Some(reply);
                }
            }
        }
        Ok(())
    }
}

async fn submit_job(
    state: &mut PoolState,
    myself: &ActorRef<PoolMsg>,
    request: SubmitRequest,
) -> SubmitOutcome {
    let key = request.job.cache_key();

    // Bytes on disk answer outright (and count as a hit, which is what keeps
    // a warm rendition out of the eviction pass).
    if let Some(artifact) = state.cache.lookup(&key).await {
        return SubmitOutcome::Hit(ArtifactRef::new(
            &artifact,
            request
                .job
                .download_file_name(request.download_stem.as_deref()),
        ));
    }

    // An in-flight job for the same key is joined rather than duplicated: two
    // clients pressing play on the same unplayable file must not run two
    // encodes into the same artifact.
    if let Some(existing) = state.by_key.get(&key)
        && let Some(entry) = state.jobs.get(existing)
    {
        return SubmitOutcome::Joined(entry.snapshot());
    }

    if state.shutting_down {
        let snapshot = insert_terminal_job(
            state,
            key,
            TranscodeJobEvent::Failed {
                error: "server is shutting down".to_string(),
                cancelled: true,
            },
        );
        return SubmitOutcome::KnownFailure(snapshot);
    }

    // A settled verdict is reported as a job that is already failed, so the
    // client follows one flow whether the file failed a second ago or a month
    // ago.
    if let Some(error) = state.cache.known_failure(&key).await {
        let snapshot = insert_terminal_job(
            state,
            key,
            TranscodeJobEvent::Failed {
                error,
                cancelled: false,
            },
        );
        return SubmitOutcome::KnownFailure(snapshot);
    }

    let id = Uuid::new_v4();
    let position = state.queued.len() + 1;
    let (events, _) = watch::channel(TranscodeJobSnapshot {
        id: id.to_string(),
        event: TranscodeJobEvent::Queued { position },
    });
    let entry = JobEntry {
        key: key.clone(),
        weight: request.weight,
        cancel: Arc::new(AtomicBool::new(false)),
        events,
        pending: Some(PendingEncode {
            job: request.job,
            download_stem: request.download_stem,
        }),
    };
    let snapshot = entry.snapshot();
    state.jobs.insert(id, entry);
    state.by_key.insert(key, id);
    state.queued.push_back(id);
    dispatch(state, myself);
    // The dispatch may have started this very job, so the outcome carries the
    // state as it is now rather than the queued one built above.
    let snapshot = state
        .jobs
        .get(&id)
        .map(JobEntry::snapshot)
        .unwrap_or(snapshot);
    SubmitOutcome::Created(snapshot)
}

/// A job that is born terminal (a settled negative verdict, or a submit
/// arriving during shutdown). It goes straight into the retention ring.
fn insert_terminal_job(
    state: &mut PoolState,
    key: String,
    event: TranscodeJobEvent,
) -> TranscodeJobSnapshot {
    let id = Uuid::new_v4();
    let (events, _) = watch::channel(TranscodeJobSnapshot {
        id: id.to_string(),
        event,
    });
    let entry = JobEntry {
        key,
        weight: JobWeight::Light,
        cancel: Arc::new(AtomicBool::new(false)),
        events,
        pending: None,
    };
    let snapshot = entry.snapshot();
    state.jobs.insert(id, entry);
    state.terminal.push_back((id, Instant::now()));
    prune_terminal(state);
    snapshot
}

/// Starts whatever the head of the queue allows. FIFO is strict: a job that
/// cannot run yet blocks the ones behind it rather than being skipped, which
/// is what makes the queue position a promise.
fn dispatch(state: &mut PoolState, myself: &ActorRef<PoolMsg>) {
    while !state.shutting_down && state.running < state.max_concurrent {
        if state.exclusive_running {
            break;
        }
        let Some(&id) = state.queued.front() else {
            break;
        };
        let weight = state.jobs.get(&id).map(|entry| entry.weight);
        if weight == Some(JobWeight::Exclusive) && state.running > 0 {
            break;
        }
        state.queued.pop_front();
        if !start_job(state, myself, id) {
            fail_undispatchable(state, id);
            continue;
        }
        state.running += 1;
        if weight == Some(JobWeight::Exclusive) {
            state.exclusive_running = true;
        }
    }
    rebroadcast_positions(state);
}

/// Spawns the encode for `id`. `false` when there was nothing to start (the
/// entry vanished), which the caller treats as "this slot is still free".
fn start_job(state: &mut PoolState, myself: &ActorRef<PoolMsg>, id: Uuid) -> bool {
    let Some(entry) = state.jobs.get_mut(&id) else {
        return false;
    };
    let Some(pending) = entry.pending.take() else {
        return false;
    };
    let cancel = Arc::clone(&entry.cancel);
    entry.publish(TranscodeJobEvent::Running { progress: None });

    let cache = Arc::clone(&state.cache);
    let runner = Arc::clone(&state.runner);
    let actor = myself.clone();
    tokio::spawn(async move {
        let outcome = run_one(cache, runner, actor.clone(), id, pending, cancel).await;
        let _ = actor.cast(PoolMsg::Finished { id, outcome });
    });
    true
}

/// Settles a job the dispatch could not start. Defensive: nothing takes an
/// entry's `pending` or removes it while it is queued, so this is unreachable
/// today — but silently dropping such a job would hold its key forever and
/// leave its client watching a channel that never moves again.
fn fail_undispatchable(state: &mut PoolState, id: Uuid) {
    let Some(entry) = state.jobs.get(&id) else {
        return;
    };
    entry.publish(TranscodeJobEvent::Failed {
        error: "job vanished before dispatch".to_string(),
        cancelled: false,
    });
    let key = entry.key.clone();
    if state.by_key.get(&key) == Some(&id) {
        state.by_key.remove(&key);
    }
    state.terminal.push_back((id, Instant::now()));
    prune_terminal(state);
}

/// The whole off-actor half of a job: encode, then publish the artifact.
async fn run_one(
    cache: Arc<TranscodeCache>,
    runner: EncodeRunner,
    actor: ActorRef<PoolMsg>,
    id: Uuid,
    pending: PendingEncode,
    cancel: Arc<AtomicBool>,
) -> JobOutcome {
    let job = pending.job;
    let download_stem = pending.download_stem;
    let temp = cache.temp_path(job.preset().container.ext());
    let task = job.clone().into_task(temp.clone());

    let mut last_sent = Instant::now() - PROGRESS_INTERVAL;
    let progress_actor = actor.clone();
    let progress: Box<dyn FnMut(Option<f32>) + Send> = Box::new(move |value| {
        // Throttled at the source: an unthrottled ffmpeg reports faster than
        // any client can render, and every message is also a broadcast to
        // every open SSE stream.
        if last_sent.elapsed() < PROGRESS_INTERVAL {
            return;
        }
        last_sent = Instant::now();
        let _ = progress_actor.cast(PoolMsg::Progress {
            id,
            progress: value,
        });
    });

    let encoded = tokio::task::spawn_blocking(move || runner(task, cancel, progress)).await;

    let outcome = match encoded {
        Ok(Ok(())) => publish(&cache, &job, download_stem.as_deref(), &temp).await,
        Ok(Err(EncodeError::Cancelled)) => JobOutcome::Failed {
            error: "cancelled".to_string(),
            cancelled: true,
        },
        Ok(Err(EncodeError::Spawn(err))) => {
            // Never a verdict on the file: a missing toolchain must not
            // suppress this rendition once ffmpeg is installed.
            let error = crate::media_tools::spawn_error("ffmpeg", &err);
            JobOutcome::Failed {
                error: error.detail().to_string(),
                cancelled: false,
            }
        }
        Ok(Err(EncodeError::Failed(detail))) => {
            // ffmpeg ran and refused: the two-strike negative cache decides
            // whether a later submit is short-circuited — for the jobs whose
            // failure is a verdict at all (see `records_failures`).
            if job.records_failures()
                && let Err(err) = cache
                    .record_failure(
                        &job.cache_key(),
                        &job.source_sha256(),
                        &job.preset().id,
                        &detail,
                        job.transcoder_version(),
                    )
                    .await
            {
                tracing::warn!(error = %err, "failed to record a transcode verdict");
            }
            JobOutcome::Failed {
                error: detail,
                cancelled: false,
            }
        }
        Err(err) => JobOutcome::Failed {
            error: format!("the transcode task failed: {err}"),
            cancelled: false,
        },
    };

    if matches!(outcome, JobOutcome::Failed { .. }) {
        // Nothing else would claim the partial output for a day.
        let _ = tokio::fs::remove_file(&temp).await;
    }
    // Opportunistic: a long-running process never re-opens the cache, so
    // without this the only sweep of abandoned temporaries is at startup.
    cache.sweep_stale_temp_files().await;
    outcome
}

async fn publish(
    cache: &TranscodeCache,
    job: &JobRequest,
    download_stem: Option<&str>,
    temp: &std::path::Path,
) -> JobOutcome {
    let key = job.cache_key();
    let file_name = job.artifact_file_name();
    let source_sha256 = job.source_sha256();
    let new = NewArtifact {
        key: &key,
        source_sha256: &source_sha256,
        params_hash: &job.params_hash(),
        preset: &job.preset().id,
        file_name: &file_name,
        mime_type: job.mime_type(),
        transcoder_version: job.transcoder_version(),
    };
    match cache.commit(new, temp).await {
        // The same name a cache hit for this key would have carried: both
        // sides build it from the caller's stem, never from the encode input.
        Ok(artifact) => JobOutcome::Done(ArtifactRef::new(
            &artifact,
            job.download_file_name(download_stem),
        )),
        // A cache that cannot store the bytes is this machine's problem, not
        // a verdict on the file: deliberately not recorded as a failure.
        Err(err) => {
            tracing::error!(error = %err, key, "failed to publish a transcode artifact");
            JobOutcome::Failed {
                error: format!("failed to store the encoded artifact: {err}"),
                cancelled: false,
            }
        }
    }
}

fn finish_job(state: &mut PoolState, myself: &ActorRef<PoolMsg>, id: Uuid, outcome: JobOutcome) {
    let Some(entry) = state.jobs.get(&id) else {
        return;
    };
    let event = match outcome {
        JobOutcome::Done(artifact) => TranscodeJobEvent::Done { artifact },
        JobOutcome::Failed { error, cancelled } => TranscodeJobEvent::Failed { error, cancelled },
    };
    entry.publish(event);
    // The key is free again the moment the job is terminal: the next submit
    // hits the cache (on success) or re-runs / short-circuits on the verdict.
    let key = entry.key.clone();
    if state.by_key.get(&key) == Some(&id) {
        state.by_key.remove(&key);
    }
    if entry.weight == JobWeight::Exclusive {
        state.exclusive_running = false;
    }
    state.running = state.running.saturating_sub(1);
    state.terminal.push_back((id, Instant::now()));
    prune_terminal(state);
    // The last encode to stop is what releases the shutdown wait.
    if state.running == 0
        && let Some(drained) = state.drained.take()
    {
        let _ = drained.send(());
    }
    dispatch(state, myself);
}

fn cancel_job(state: &mut PoolState, id: Uuid) -> bool {
    let Some(entry) = state.jobs.get(&id) else {
        return false;
    };
    if entry.snapshot().event.is_terminal() {
        return false;
    }
    entry.cancel.store(true, Ordering::Relaxed);
    // The key is free the moment the cancel lands, running or not: this job
    // is doomed, so a later submit for it must create a fresh one rather than
    // join a job whose only remaining outcome is `cancelled`. Two encodes of
    // one key overlapping is safe on disk — each writes its own nonce-named
    // temporary, the publish renames over whatever is there, and the row is
    // an upsert.
    let key = entry.key.clone();
    if state.by_key.get(&key) == Some(&id) {
        state.by_key.remove(&key);
    }
    if let Some(at) = state.queued.iter().position(|queued| *queued == id) {
        // A queued job has no child to kill, so it settles here and now.
        state.queued.remove(at);
        entry.publish(TranscodeJobEvent::Failed {
            error: "cancelled".to_string(),
            cancelled: true,
        });
        state.terminal.push_back((id, Instant::now()));
        prune_terminal(state);
        rebroadcast_positions(state);
    }
    // A running job ends through its own completion, once the runner notices
    // the flag and kills the child.
    true
}

/// Queue positions are relative, so every transition changes them for
/// everyone behind it. Only queued jobs are re-published; a running or
/// terminal job has no position at all.
fn rebroadcast_positions(state: &PoolState) {
    for (index, id) in state.queued.iter().enumerate() {
        let Some(entry) = state.jobs.get(id) else {
            continue;
        };
        let position = index + 1;
        if entry.snapshot().event != (TranscodeJobEvent::Queued { position }) {
            entry.publish(TranscodeJobEvent::Queued { position });
        }
    }
}

fn prune_terminal(state: &mut PoolState) {
    while let Some((id, finished_at)) = state.terminal.front().copied() {
        let expired = finished_at.elapsed() >= state.terminal_ttl;
        if !expired && state.terminal.len() <= state.terminal_capacity {
            break;
        }
        state.terminal.pop_front();
        state.jobs.remove(&id);
    }
}

// --- process-wide handles --------------------------------------------------

static CACHE: OnceCell<Arc<TranscodeCache>> = OnceCell::const_new();
static POOL: OnceCell<ActorRef<PoolMsg>> = OnceCell::const_new();

/// The artifact cache, opened (and reconciled) on first use.
pub(crate) async fn transcode_cache() -> ApiResult<Arc<TranscodeCache>> {
    CACHE
        .get_or_try_init(|| async {
            let cache = TranscodeCache::open_from_config()
                .await
                .map(Arc::new)
                .map_err(|err| {
                    tracing::error!(error = %err, "failed to open the transcode artifact cache");
                    ApiError::internal("Failed to open the transcode artifact cache")
                })?;
            // The hardware probe spawns ffmpeg twice on its first call and is
            // cached for the life of the process. Warmed here rather than
            // alongside the pool because the pool starts only when something
            // *encodes*: after a restart, a GET whose artifact is already
            // cached resolves through this function alone, and the encoder
            // identity it needs to name that artifact would otherwise be
            // probed on the request's own thread.
            tokio::task::spawn_blocking(|| {
                let _ = super::hw::fast_h264_encoder();
            });
            Ok(cache)
        })
        .await
        .map(Arc::clone)
}

/// The runtime the process-global pool actor is started on.
///
/// In production that is simply the caller's — the server's runtime, which
/// lives as long as the process. Under `cfg(test)` it is a runtime owned by
/// the *process*, because an actor does not outlive the runtime it was spawned
/// on and every `#[tokio::test]` builds (and drops) its own: without this, the
/// pool would be a corpse for every test after the first one to touch it, and
/// which test that is depends on scheduling. It stands in for exactly the
/// property production gets for free.
#[cfg(test)]
fn pool_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new().expect("a runtime for the process-global transcode pool")
    })
}

pub(crate) async fn ensure_pool() -> ApiResult<ActorRef<PoolMsg>> {
    POOL.get_or_try_init(|| async {
        let cache = transcode_cache().await?;
        let start = Actor::spawn(
            Some("transcode-pool".to_string()),
            TranscodePool,
            PoolArgs {
                cache,
                runner: ffmpeg_runner(),
                max_concurrent: crate::config::runtime().transcode.max_concurrent_jobs,
                terminal_ttl: TERMINAL_TTL,
                terminal_capacity: TERMINAL_CAPACITY,
            },
        );
        #[cfg(not(test))]
        let started = start.await;
        #[cfg(test)]
        let started = pool_runtime()
            .spawn(start)
            .await
            .expect("the pool spawn task");
        let (actor, _handle) = started.map_err(|err| {
            tracing::error!(error = ?err, "failed to start the transcode pool");
            ApiError::internal("Failed to start the transcode pool")
        })?;
        Ok(actor)
    })
    .await
    .cloned()
}

/// Stops the pool: queued jobs settle as cancelled, running encodes are told
/// to stop (their children are killed by the runner's watchdog), and the wait
/// runs until the last of them has reported back. No-op when no transcode was
/// ever requested. Used at process shutdown.
pub(crate) async fn shutdown_transcode_pool() {
    let Some(pool) = POOL.get() else {
        return;
    };
    let (reply, rx) = oneshot::channel();
    if pool.cast(PoolMsg::Shutdown { reply }).is_ok()
        && tokio::time::timeout(SHUTDOWN_DRAIN, rx).await.is_err()
    {
        tracing::warn!(
            grace_secs = SHUTDOWN_DRAIN.as_secs(),
            "transcodes did not stop within the shutdown grace; stopping the pool anyway"
        );
    }
    pool.stop(None);
}

async fn ask<T>(
    build: impl FnOnce(oneshot::Sender<T>) -> PoolMsg,
    what: &'static str,
) -> ApiResult<T> {
    let pool = ensure_pool().await?;
    let (reply, rx) = oneshot::channel();
    pool.cast(build(reply))
        .map_err(|_| ApiError::internal(format!("The transcode pool is unavailable ({what})")))?;
    rx.await
        .map_err(|_| ApiError::internal(format!("The transcode pool dropped a {what} request")))
}

pub(crate) async fn submit(request: SubmitRequest) -> ApiResult<SubmitOutcome> {
    ask(
        |reply| PoolMsg::Submit {
            request: Box::new(request),
            reply,
        },
        "submit",
    )
    .await
}

pub(crate) async fn job_snapshot(id: Uuid) -> ApiResult<Option<TranscodeJobSnapshot>> {
    ask(|reply| PoolMsg::Snapshot { id, reply }, "snapshot").await
}

pub(crate) async fn subscribe(
    id: Uuid,
) -> ApiResult<Option<watch::Receiver<TranscodeJobSnapshot>>> {
    ask(|reply| PoolMsg::Subscribe { id, reply }, "subscribe").await
}

pub(crate) async fn cancel(id: Uuid) -> ApiResult<bool> {
    ask(|reply| PoolMsg::Cancel { id, reply }, "cancel").await
}

pub(crate) async fn job_for_key(key: String) -> ApiResult<Option<TranscodeJobSnapshot>> {
    ask(|reply| PoolMsg::JobForKey { key, reply }, "job lookup").await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media_tools::transcode::presets::{builtin_presets, find_preset};
    use crate::media_tools::transcode::run::ENCODER_X264_QUALITY;

    fn params(preset_id: &str, start_cs: Option<i64>) -> TranscodeParams {
        let presets = builtin_presets();
        let preset = find_preset(&presets, preset_id).expect("a built-in").clone();
        TranscodeParams::new(
            "a".repeat(64),
            preset,
            ENCODER_X264_QUALITY.to_string(),
            start_cs,
            None,
        )
    }

    fn request(params: TranscodeParams, weight: JobWeight) -> SubmitRequest {
        SubmitRequest {
            job: JobRequest::Single {
                params: Box::new(params),
                // Deliberately a different name from the stem below: the
                // download name follows the caller's stem, never the encode
                // input.
                source_path: PathBuf::from("readable-copy.mp4"),
                source_duration_s: Some(10.0),
            },
            weight,
            download_stem: Some("source".to_string()),
        }
    }

    /// A runner that writes a byte to the output and returns immediately.
    fn instant_runner() -> EncodeRunner {
        Arc::new(|task: EncodeTask, _cancel, mut progress| {
            progress(Some(0.5));
            std::fs::write(task.output(), b"artifact").expect("write the fixture artifact");
            Ok(())
        })
    }

    /// Releases parked jobs one at a time. A plain semaphore will not do: a
    /// `try_acquire` permit is returned the moment it drops, which would let
    /// one release wave every parked job through.
    #[derive(Default)]
    struct Gate(std::sync::atomic::AtomicUsize);

    impl Gate {
        fn release(&self, jobs: usize) {
            self.0.fetch_add(jobs, Ordering::Relaxed);
        }

        fn take(&self) -> bool {
            self.0
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                    left.checked_sub(1)
                })
                .is_ok()
        }
    }

    /// A runner that parks until the gate releases it, so tests can observe
    /// the queue while a job is running. Honours cancellation, and gives up
    /// on its own: a blocking task that never returns would hang the runtime
    /// at the end of the test rather than fail it.
    fn parked_runner(
        started: tokio::sync::mpsc::UnboundedSender<String>,
        gate: Arc<Gate>,
    ) -> EncodeRunner {
        Arc::new(move |task: EncodeTask, cancel: Arc<AtomicBool>, _progress| {
            let _ = started.send(task.cache_key());
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if cancel.load(Ordering::Relaxed) || Instant::now() >= deadline {
                    return Err(EncodeError::Cancelled);
                }
                if gate.take() {
                    std::fs::write(task.output(), b"artifact").expect("write the artifact");
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        })
    }

    struct TestPool {
        actor: ActorRef<PoolMsg>,
        _dir: tempfile::TempDir,
        cache: Arc<TranscodeCache>,
    }

    impl TestPool {
        async fn new(runner: EncodeRunner, max_concurrent: usize) -> Self {
            Self::with_retention(runner, max_concurrent, TERMINAL_TTL, TERMINAL_CAPACITY).await
        }

        async fn with_retention(
            runner: EncodeRunner,
            max_concurrent: usize,
            terminal_ttl: Duration,
            terminal_capacity: usize,
        ) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let cache = Arc::new(
                TranscodeCache::open(dir.path().to_path_buf(), 64, 64)
                    .await
                    .unwrap(),
            );
            let (actor, _handle) = Actor::spawn(
                None,
                TranscodePool,
                PoolArgs {
                    cache: Arc::clone(&cache),
                    runner,
                    max_concurrent,
                    terminal_ttl,
                    terminal_capacity,
                },
            )
            .await
            .unwrap();
            Self {
                actor,
                _dir: dir,
                cache,
            }
        }

        async fn submit(&self, request: SubmitRequest) -> SubmitOutcome {
            let (reply, rx) = oneshot::channel();
            self.actor
                .cast(PoolMsg::Submit {
                    request: Box::new(request),
                    reply,
                })
                .unwrap();
            rx.await.unwrap()
        }

        async fn snapshot(&self, id: &str) -> Option<TranscodeJobSnapshot> {
            let (reply, rx) = oneshot::channel();
            self.actor
                .cast(PoolMsg::Snapshot {
                    id: Uuid::parse_str(id).unwrap(),
                    reply,
                })
                .unwrap();
            rx.await.unwrap()
        }

        async fn cancel(&self, id: &str) -> bool {
            let (reply, rx) = oneshot::channel();
            self.actor
                .cast(PoolMsg::Cancel {
                    id: Uuid::parse_str(id).unwrap(),
                    reply,
                })
                .unwrap();
            rx.await.unwrap()
        }

        async fn subscribe(&self, id: &str) -> Option<watch::Receiver<TranscodeJobSnapshot>> {
            let (reply, rx) = oneshot::channel();
            self.actor
                .cast(PoolMsg::Subscribe {
                    id: Uuid::parse_str(id).unwrap(),
                    reply,
                })
                .unwrap();
            rx.await.unwrap()
        }

        /// Polls until `id` is terminal, so tests never sleep on a guess.
        async fn await_terminal(&self, id: &str) -> TranscodeJobEvent {
            for _ in 0..400 {
                if let Some(snapshot) = self.snapshot(id).await
                    && snapshot.event.is_terminal()
                {
                    return snapshot.event;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("job {id} never reached a terminal state");
        }
    }

    fn job_id(outcome: &SubmitOutcome) -> String {
        match outcome {
            SubmitOutcome::Created(job)
            | SubmitOutcome::Joined(job)
            | SubmitOutcome::KnownFailure(job) => job.id.clone(),
            SubmitOutcome::Hit(_) => panic!("expected a job, got a cache hit"),
        }
    }

    /// A finished encode becomes a cache hit for the next identical submit —
    /// the whole point of the key.
    #[tokio::test]
    async fn a_finished_job_becomes_a_cache_hit() {
        let pool = TestPool::new(instant_runner(), 1).await;
        let first = pool.submit(request(params("clip", None), JobWeight::Light)).await;
        assert!(matches!(first, SubmitOutcome::Created(_)));
        let event = pool.await_terminal(&job_id(&first)).await;
        let TranscodeJobEvent::Done { artifact } = event else {
            panic!("expected a finished artifact, got {event:?}");
        };
        assert_eq!(artifact.size_bytes, 8);
        assert_eq!(artifact.mime_type, "video/mp4");
        assert_eq!(artifact.url, format!("/api/video/artifact?key={}", artifact.key));
        // The download name is computed from the request, not from the key:
        // the `key=` URL above could never carry the source's own name. And it
        // follows the caller's stem rather than the encode input, whose name
        // (`readable-copy.mp4`) is an accident of which copy was readable.
        assert_eq!(artifact.filename, "source-clip.mp4");

        let second = pool.submit(request(params("clip", None), JobWeight::Light)).await;
        match second {
            SubmitOutcome::Hit(hit) => {
                assert_eq!(hit.key, artifact.key);
                assert_eq!(
                    hit.filename, artifact.filename,
                    "a cache hit names the download exactly as the job that filled it did"
                );
            }
            other => panic!("expected a cache hit, got {}", other.as_str()),
        }
        // The dedup map does not hold a finished job's key.
        assert!(pool.cache.lookup(&artifact.key).await.is_some());
    }

    /// FIFO with live queue positions, in-flight dedup, and a concurrency
    /// bound of one: the three properties a client's "Queued (#N)" depends on.
    #[tokio::test]
    async fn queue_is_fifo_with_broadcast_positions_and_dedup() {
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let gate = Arc::new(Gate::default());
        let pool = TestPool::new(parked_runner(started_tx, Arc::clone(&gate)), 1).await;

        let first = pool.submit(request(params("clip", None), JobWeight::Light)).await;
        let second = pool
            .submit(request(params("clip", Some(100)), JobWeight::Light))
            .await;
        let third = pool
            .submit(request(params("clip", Some(200)), JobWeight::Light))
            .await;

        // Only one job runs; the others queue behind it in submit order.
        let running = started_rx.recv().await.expect("the first job started");
        assert_eq!(running, params("clip", None).cache_key());
        assert!(matches!(
            pool.snapshot(&job_id(&first)).await.unwrap().event,
            TranscodeJobEvent::Running { .. }
        ));
        assert_eq!(
            pool.snapshot(&job_id(&second)).await.unwrap().event,
            TranscodeJobEvent::Queued { position: 1 }
        );
        assert_eq!(
            pool.snapshot(&job_id(&third)).await.unwrap().event,
            TranscodeJobEvent::Queued { position: 2 }
        );

        // A fourth submit for a key already in flight joins it.
        let joined = pool
            .submit(request(params("clip", Some(100)), JobWeight::Light))
            .await;
        assert!(matches!(joined, SubmitOutcome::Joined(_)));
        assert_eq!(job_id(&joined), job_id(&second));

        // Watchers see the position change as the queue drains, without
        // polling.
        let mut watcher = pool.subscribe(&job_id(&third)).await.expect("a live job");
        assert_eq!(
            watcher.borrow().event,
            TranscodeJobEvent::Queued { position: 2 }
        );
        gate.release(1);
        pool.await_terminal(&job_id(&first)).await;
        watcher.changed().await.expect("the position was rebroadcast");
        assert_eq!(
            watcher.borrow().event,
            TranscodeJobEvent::Queued { position: 1 }
        );

        gate.release(2);
        pool.await_terminal(&job_id(&second)).await;
        pool.await_terminal(&job_id(&third)).await;
    }

    /// Cancelling a queued job settles it immediately (there is no child to
    /// kill) and closes the gap for the jobs behind it; cancelling a running
    /// one goes through the runner's flag.
    #[tokio::test]
    async fn cancel_settles_queued_jobs_at_once_and_flags_running_ones() {
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let gate = Arc::new(Gate::default());
        let pool = TestPool::new(parked_runner(started_tx, gate), 1).await;

        let running = pool.submit(request(params("clip", None), JobWeight::Light)).await;
        let queued = pool
            .submit(request(params("clip", Some(100)), JobWeight::Light))
            .await;
        let behind = pool
            .submit(request(params("clip", Some(200)), JobWeight::Light))
            .await;
        started_rx.recv().await.expect("the first job started");

        assert!(pool.cancel(&job_id(&queued)).await);
        assert_eq!(
            pool.snapshot(&job_id(&queued)).await.unwrap().event,
            TranscodeJobEvent::Failed {
                error: "cancelled".to_string(),
                cancelled: true
            }
        );
        assert_eq!(
            pool.snapshot(&job_id(&behind)).await.unwrap().event,
            TranscodeJobEvent::Queued { position: 1 },
            "the queue closed the gap"
        );
        assert!(
            !pool.cancel(&job_id(&queued)).await,
            "a terminal job cannot be cancelled again"
        );

        // The cancelled key is free again, so a resubmit is a new job rather
        // than a join onto a dead one.
        let resubmitted = pool
            .submit(request(params("clip", Some(100)), JobWeight::Light))
            .await;
        assert!(matches!(resubmitted, SubmitOutcome::Created(_)));

        assert!(pool.cancel(&job_id(&running)).await);
        // A *running* job frees its key at the cancel too, not at its later
        // completion: the encode is doomed, so joining it would hand the new
        // client a job whose only outcome is "cancelled".
        let after_cancel = pool.submit(request(params("clip", None), JobWeight::Light)).await;
        assert!(
            matches!(after_cancel, SubmitOutcome::Created(_)),
            "a cancelled running job is not joinable, got {}",
            after_cancel.as_str()
        );
        assert_ne!(job_id(&after_cancel), job_id(&running));
        assert!(pool.cancel(&job_id(&after_cancel)).await);

        assert_eq!(
            pool.await_terminal(&job_id(&running)).await,
            TranscodeJobEvent::Failed {
                error: "cancelled".to_string(),
                cancelled: true
            }
        );
    }

    /// An exclusive job waits for the pool to empty and holds it while it
    /// runs, even with room to spare — the forward-compat hook for compose.
    #[tokio::test]
    async fn an_exclusive_job_never_shares_the_pool() {
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let gate = Arc::new(Gate::default());
        let pool = TestPool::new(parked_runner(started_tx, Arc::clone(&gate)), 2).await;

        let light = pool.submit(request(params("clip", None), JobWeight::Light)).await;
        let heavy = pool
            .submit(request(params("clip", Some(100)), JobWeight::Exclusive))
            .await;
        let after = pool
            .submit(request(params("clip", Some(200)), JobWeight::Light))
            .await;
        started_rx.recv().await.expect("the light job started");

        // Two slots, but the exclusive job is next in line, so neither it nor
        // the light job behind it may start.
        assert_eq!(
            pool.snapshot(&job_id(&heavy)).await.unwrap().event,
            TranscodeJobEvent::Queued { position: 1 }
        );
        assert_eq!(
            pool.snapshot(&job_id(&after)).await.unwrap().event,
            TranscodeJobEvent::Queued { position: 2 }
        );

        gate.release(1);
        pool.await_terminal(&job_id(&light)).await;
        assert_eq!(
            started_rx.recv().await.unwrap(),
            params("clip", Some(100)).cache_key(),
            "the exclusive job runs once the pool is empty"
        );
        assert_eq!(
            pool.snapshot(&job_id(&after)).await.unwrap().event,
            TranscodeJobEvent::Queued { position: 1 },
            "and blocks the job behind it despite the free slot"
        );

        gate.release(2);
        pool.await_terminal(&job_id(&heavy)).await;
        pool.await_terminal(&job_id(&after)).await;
    }

    /// A composition through the actor: keyed by its document, published under
    /// the fixed composition name, and — at the weight the API gives a mosaic
    /// past the light threshold — holding the pool while it runs.
    #[tokio::test]
    async fn a_composition_is_keyed_by_its_document_and_holds_the_pool() {
        let compose = compose_request(JobWeight::Exclusive);
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let gate = Arc::new(Gate::default());
        let pool = TestPool::new(parked_runner(started_tx, Arc::clone(&gate)), 2).await;
        let heavy = pool.submit(compose.clone()).await;
        let light = pool.submit(request(params("clip", None), JobWeight::Light)).await;
        let key = started_rx.recv().await.expect("the composition started");
        assert!(key.starts_with("compose-"), "{key}");
        assert_eq!(
            pool.snapshot(&job_id(&light)).await.unwrap().event,
            TranscodeJobEvent::Queued { position: 1 },
            "the composition holds the pool despite the free slot"
        );

        gate.release(2);
        let event = pool.await_terminal(&job_id(&heavy)).await;
        let TranscodeJobEvent::Done { artifact } = event else {
            panic!("expected a published composition, got {event:?}");
        };
        assert_eq!(artifact.key, key);
        assert_eq!(artifact.filename, "mosaic-2items.mp4");
        assert_eq!(artifact.mime_type, "video/mp4");
        pool.await_terminal(&job_id(&light)).await;

        // The document is the identity: the same one submitted again is a hit.
        match pool.submit(compose).await {
            SubmitOutcome::Hit(hit) => assert_eq!(hit.key, artifact.key),
            other => panic!("expected a cache hit, got {}", other.as_str()),
        }
    }

    /// Terminal jobs are retained for late joiners, but only so many: the ring
    /// is what keeps the map from being a leak.
    #[tokio::test]
    async fn terminal_jobs_are_retained_up_to_the_ring_size() {
        let pool =
            TestPool::with_retention(instant_runner(), 1, TERMINAL_TTL, 2).await;
        let mut ids = Vec::new();
        for index in 0..3 {
            let outcome = pool
                .submit(request(params("clip", Some(index)), JobWeight::Light))
                .await;
            let id = job_id(&outcome);
            pool.await_terminal(&id).await;
            ids.push(id);
        }
        assert!(
            pool.snapshot(&ids[0]).await.is_none(),
            "the oldest terminal job aged out of the ring"
        );
        assert!(pool.snapshot(&ids[1]).await.is_some());
        assert!(pool.snapshot(&ids[2]).await.is_some());

        // A zero TTL is the same story on the other axis: the job is gone the
        // moment it finishes, so it has to be followed through its event
        // channel — which still carries the last value after the job is
        // dropped, exactly as a client mid-stream would see it.
        let pool = TestPool::with_retention(instant_runner(), 1, Duration::ZERO, 512).await;
        let outcome = pool.submit(request(params("clip", None), JobWeight::Light)).await;
        let id = job_id(&outcome);
        let mut events = pool.subscribe(&id).await.expect("a live job");
        loop {
            if events.borrow_and_update().event.is_terminal() {
                break;
            }
            if events.changed().await.is_err() {
                break;
            }
        }
        assert!(events.borrow().event.is_terminal());
        assert!(pool.snapshot(&id).await.is_none());
    }

    /// A settled verdict short-circuits the submit into a job that is already
    /// failed, and the failing encode itself records the strikes.
    #[tokio::test]
    async fn a_settled_verdict_is_reported_as_a_failed_job() {
        let runner: EncodeRunner =
            Arc::new(|_spec, _cancel, _progress| Err(EncodeError::Failed("bad input".to_string())));
        let pool = TestPool::new(runner, 1).await;

        for _ in 0..2 {
            let outcome = pool.submit(request(params("clip", None), JobWeight::Light)).await;
            assert!(matches!(outcome, SubmitOutcome::Created(_)));
            assert_eq!(
                pool.await_terminal(&job_id(&outcome)).await,
                TranscodeJobEvent::Failed {
                    error: "bad input".to_string(),
                    cancelled: false
                }
            );
        }

        // Two strikes settle it: the third submit spawns nothing.
        let outcome = pool.submit(request(params("clip", None), JobWeight::Light)).await;
        match &outcome {
            SubmitOutcome::KnownFailure(job) => assert_eq!(
                job.event,
                TranscodeJobEvent::Failed {
                    error: "bad input".to_string(),
                    cancelled: false
                }
            ),
            other => panic!("expected a known failure, got {}", other.as_str()),
        }
        assert_eq!(outcome.as_str(), "known_failure");
    }

    /// A composition, as a submit. Two items, one document, the fixed
    /// composition download name.
    fn compose_request(weight: JobWeight) -> SubmitRequest {
        use crate::media_tools::transcode::compose::{
            ComposeItem, ItemTime, Rect, ResolvedCompose, Transform,
        };

        let doc = ResolvedCompose {
            canvas_w: 320,
            canvas_h: 240,
            background: "0x000000".to_string(),
            fps: 25,
            target_cs: 500,
            items: (0..2)
                .map(|index| ComposeItem {
                    sha256: "a".repeat(64),
                    src: Rect {
                        x: 0,
                        y: 0,
                        w: 320,
                        h: 240,
                    },
                    transform: Transform::default(),
                    dest: Rect {
                        x: index * 160,
                        y: 0,
                        w: 160,
                        h: 240,
                    },
                    time: ItemTime::Image,
                    audio: false,
                })
                .collect(),
        };
        let preset = find_preset(&builtin_presets(), "mosaic-mp4")
            .expect("a built-in")
            .clone();
        SubmitRequest {
            job: JobRequest::Compose {
                params: Box::new(ComposeParams::new(
                    doc,
                    preset,
                    ENCODER_X264_QUALITY.to_string(),
                )),
                sources: vec![PathBuf::from("a.mp4"), PathBuf::from("b.mp4")],
            },
            weight,
            // Deliberately set: a composition has no single source, so its
            // name must ignore this.
            download_stem: Some("some-pin".to_string()),
        }
    }

    /// A composition's failures are never recorded, however many of them there
    /// are: its key hashes N inputs, so one transient failure among them —
    /// a dropped mount, a file another process was still writing — would put a
    /// strike against the whole document and two would refuse it forever.
    /// There is no per-input verdict to record instead, so the answer is that a
    /// composition is always retryable. Single files are unchanged, which is
    /// what the test above asserts.
    #[tokio::test]
    async fn a_composition_never_earns_a_negative_verdict() {
        let runner: EncodeRunner = Arc::new(|_spec, _cancel, _progress| {
            Err(EncodeError::Failed("one input was unreadable".to_string()))
        });
        let pool = TestPool::new(runner, 1).await;

        for attempt in 0..3 {
            let outcome = pool.submit(compose_request(JobWeight::Light)).await;
            assert!(
                matches!(outcome, SubmitOutcome::Created(_)),
                "attempt {attempt} must run rather than short-circuit, got {}",
                outcome.as_str()
            );
            assert_eq!(
                pool.await_terminal(&job_id(&outcome)).await,
                TranscodeJobEvent::Failed {
                    error: "one input was unreadable".to_string(),
                    cancelled: false
                }
            );
        }
        assert!(
            pool.cache
                .known_failure(&compose_request(JobWeight::Light).job.cache_key())
                .await
                .is_none(),
            "three failures, and still nothing recorded against the document"
        );
    }

    /// The whole pipeline against the real toolchain: a lavfi fixture is
    /// encoded, published, and the second submit of the same request is
    /// answered from the cache without spawning anything. Skips (never fails)
    /// where there is no ffmpeg.
    #[tokio::test]
    async fn a_real_encode_publishes_an_artifact_the_next_submit_hits() {
        if !crate::media_tools::ffmpeg_available() {
            return;
        }
        let fixtures = tempfile::tempdir().unwrap();
        let source = fixtures.path().join("source.mp4");
        if !crate::jobs::files::write_clip(&source, None, None) {
            return;
        }

        let pool = TestPool::new(ffmpeg_runner(), 1).await;
        let request = SubmitRequest {
            job: JobRequest::Single {
                params: Box::new(params("playback", Some(100))),
                source_path: source,
                source_duration_s: Some(7.0),
            },
            weight: JobWeight::Light,
            download_stem: Some("source".to_string()),
        };

        let first = pool.submit(request.clone()).await;
        assert!(matches!(first, SubmitOutcome::Created(_)));
        let event = pool.await_terminal(&job_id(&first)).await;
        let TranscodeJobEvent::Done { artifact } = event else {
            panic!("the fixture must encode, got {event:?}");
        };
        assert_eq!(artifact.mime_type, "video/mp4");
        assert!(artifact.size_bytes > 0);
        assert_eq!(artifact.key, request.job.cache_key());

        match pool.submit(request).await {
            SubmitOutcome::Hit(hit) => assert_eq!(hit.key, artifact.key),
            other => panic!("expected a cache hit, got {}", other.as_str()),
        }
    }

    /// A cancellation is never a verdict: it must not be recorded, or a
    /// client pressing stop twice would suppress the rendition forever.
    #[tokio::test]
    async fn cancelling_never_records_a_verdict() {
        let runner: EncodeRunner =
            Arc::new(|_spec, _cancel, _progress| Err(EncodeError::Cancelled));
        let pool = TestPool::new(runner, 1).await;
        for _ in 0..3 {
            let outcome = pool.submit(request(params("clip", None), JobWeight::Light)).await;
            pool.await_terminal(&job_id(&outcome)).await;
        }
        let outcome = pool.submit(request(params("clip", None), JobWeight::Light)).await;
        assert!(
            matches!(outcome, SubmitOutcome::Created(_)),
            "a cancelled encode leaves nothing behind to short-circuit on"
        );
    }
}
