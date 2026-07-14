use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use notify::event::{ModifyKind, RenameMode};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use ractor::concurrency::Duration as RactorDuration;
use ractor::factory::{
    Factory, FactoryArguments, FactoryMessage, Job, JobOptions, Worker, WorkerBuilder, queues,
    routing,
};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tokio::sync::{OnceCell, oneshot};

use crate::api_error::ApiError;
use crate::db::files::has_blurhash;
use crate::db::{
    file_scans::{FileScanUpdate, get_open_file_scan_id},
    files::{
        FileDeleteInfo, FileUpsertResult, count_files_for_item, get_all_file_paths_with_mtime,
        get_file_by_path, get_file_delete_info,
    },
    index_writer::{IndexDbWriterMessage, call_index_db_writer},
    open_index_db_read,
    storage::{has_frame, has_thumbnail},
    system_config::{SystemConfig, SystemConfigStore},
};
use crate::jobs::dir_poller::{
    FileMeta, PollFilters, PollOutcome, PollerSnapshot, run_poll_pass, seed_snapshot,
};
use crate::jobs::files::{
    FRAME_PROCESS_VERSION, FileProcessError, PreparedFile, SCAN_PROGRESS_INTERVAL, ScanOptions,
    ScanTimers, THUMBNAIL_PROCESS_VERSION, build_extension_set, build_file_scan_data,
    check_folder_validity, current_iso_timestamp, deduplicate_paths, folder_is_empty,
    get_last_modified_time_and_size, has_allowed_extension, is_excluded, is_hidden_or_temp,
    normalize_path, parse_filescan_filter, process_file, run_post_job_maintenance,
};
use crate::pql::model::Match;

type ApiResult<T> = Result<T, ApiError>;

const CONTINUOUS_PATH_SENTINEL: &str = "<continuous>";
const SUPERVISOR_RESYNC_INTERVAL: Duration = Duration::from_secs(300);
// Watcher deletions happen outside any job, so no post-job maintenance pass
// ever accounts for them; compact once this many rows have been removed.
const MAINTENANCE_DELETION_THRESHOLD: u64 = 1000;
// A file detected by the poller must keep the same mtime and size across this
// window before it is processed, so half-written files aren't hashed.
const POLL_SETTLE_DELAY: Duration = Duration::from_secs(2);
// Backoff ceiling for files that keep changing (e.g. a long copy in progress).
const SETTLE_MAX_DELAY: Duration = Duration::from_secs(60);

#[derive(Clone)]
struct FileWork {
    path: PathBuf,
    filescan_filter: Option<Arc<Match>>,
    epoch: u64,
    scan_time: String,
    index_db: String,
    user_data_db: String,
    timers: ScanTimers,
    reply_to: ActorRef<ContinuousScanMessage>,
}

struct ContinuousWorker;
impl Worker for ContinuousWorker {
    type Key = ();
    type Message = FileWork;
    type Arguments = ();
    type State = ();

    async fn pre_start(
        &self,
        _wid: ractor::factory::WorkerId,
        _factory: &ActorRef<FactoryMessage<Self::Key, Self::Message>>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(())
    }

    async fn handle(
        &self,
        _wid: ractor::factory::WorkerId,
        _factory: &ActorRef<FactoryMessage<Self::Key, Self::Message>>,
        job: Job<Self::Key, Self::Message>,
        _state: &mut Self::State,
    ) -> Result<Self::Key, ActorProcessingErr> {
        let FileWork {
            path,
            filescan_filter,
            epoch,
            scan_time,
            index_db,
            user_data_db,
            timers,
            reply_to,
        } = job.msg;

        // Skip hashing when the on-disk mtime matches the DB record, mirroring
        // the full-scan dedup: spurious watcher events and re-dispatched paths
        // cost one stat and one point query instead of a full re-process.
        let stat_path = path.clone();
        let disk_mtime = tokio::task::spawn_blocking(move || {
            get_last_modified_time_and_size(&stat_path).map(|(mtime, _)| mtime)
        })
        .await
        .ok()
        .and_then(|res| res.ok());
        if let Some(disk_mtime) = &disk_mtime {
            if let Ok(mut conn) = open_index_db_read(&index_db, &user_data_db).await {
                if let Ok(Some(existing)) =
                    get_file_by_path(&mut conn, path.to_string_lossy().as_ref()).await
                {
                    if &existing.last_modified == disk_mtime {
                        let _ = reply_to.cast(ContinuousScanMessage::WorkerResult {
                            epoch,
                            scan_time,
                            result: Err(FileProcessError::Unchanged),
                        });
                        return Ok(job.key);
                    }
                }
            }
        }

        let result =
            tokio::task::spawn_blocking(move || process_file(path, filescan_filter, &timers))
                .await
                .map_err(|err| FileProcessError::Worker(err.to_string()))
                .and_then(|res| res);

        let _ = reply_to.cast(ContinuousScanMessage::WorkerResult {
            epoch,
            scan_time,
            result,
        });
        Ok(job.key)
    }
}

struct ContinuousWorkerBuilder;

impl WorkerBuilder<ContinuousWorker, ()> for ContinuousWorkerBuilder {
    fn build(&mut self, _wid: usize) -> (ContinuousWorker, ()) {
        (ContinuousWorker, ())
    }
}

#[derive(Debug)]
pub(crate) enum FsEvent {
    Create(PathBuf),
    Modify(PathBuf),
    Remove(PathBuf),
    Rename { from: PathBuf, to: PathBuf },
    Overflow,
}

pub(crate) enum ContinuousScanMessage {
    Pause {
        reply: oneshot::Sender<()>,
    },
    Resume,
    UpdateConfig {
        config: SystemConfig,
    },
    FsEvent(FsEvent),
    /// Starts a poll pass on the blocking pool unless one is already running.
    PollTick {
        epoch: u64,
    },
    /// A poll pass finished: restore the snapshot, act on the diff, reschedule.
    PollCompleted {
        epoch: u64,
        outcome: PollOutcome,
    },
    /// Re-stat a detected file after the settle delay; dispatch once stable.
    SettleCheck {
        epoch: u64,
        path: PathBuf,
        meta: FileMeta,
        attempts: u32,
    },
    /// A settle check confirmed the file is stable; dispatch it to a worker.
    DispatchStable {
        epoch: u64,
        path: PathBuf,
    },
    WorkerResult {
        epoch: u64,
        scan_time: String,
        result: Result<PreparedFile, FileProcessError>,
    },
    /// Point-in-time state for the status endpoint.
    GetStatus {
        reply: oneshot::Sender<ContinuousScanSnapshot>,
    },
}

/// Live scanner state reported to the status endpoint. Paths are stringified
/// here so the API layer never handles `PathBuf`s from actor state.
pub(crate) struct ContinuousScanSnapshot {
    pub paused: bool,
    pub paused_for_job: bool,
    pub watch_roots: Vec<String>,
    pub invalid_includes: Vec<String>,
    pub roots_valid: bool,
}

pub(crate) struct ContinuousScanActor;

pub(crate) struct ContinuousScanActorArgs {
    pub index_db: String,
    pub user_data_db: String,
    pub data_dir: PathBuf,
    pub enable_watcher: bool,
}

pub(crate) struct WatchRootsOutcome {
    pub watch_roots: Vec<PathBuf>,
    pub excluded_roots: Vec<PathBuf>,
    pub valid: bool,
    pub invalid_includes: Vec<String>,
}

struct ScanStats {
    new_items: i64,
    unchanged_files: i64,
    new_files: i64,
    modified_files: i64,
    marked_unavailable: i64,
    errors: i64,
    total_available: i64,
    false_changes: i64,
}

impl ScanStats {
    fn new() -> Self {
        Self {
            new_items: 0,
            unchanged_files: 0,
            new_files: 0,
            modified_files: 0,
            marked_unavailable: 0,
            errors: 0,
            total_available: 0,
            false_changes: 0,
        }
    }
}

pub(crate) fn compute_watch_roots(config: &SystemConfig) -> WatchRootsOutcome {
    compute_watch_roots_with_safe_empty(config, &HashSet::new())
}

fn compute_watch_roots_with_safe_empty(
    config: &SystemConfig,
    safe_empty: &HashSet<String>,
) -> WatchRootsOutcome {
    let mut included = config.included_folders.clone();
    included.retain(|folder| check_folder_validity(folder) || safe_empty.contains(folder));
    let global_included = deduplicate_paths(&included);
    let global_included_roots: Vec<PathBuf> = global_included
        .iter()
        .map(|path| PathBuf::from(path))
        .collect();

    let global_excluded_roots: Vec<PathBuf> = config
        .excluded_folders
        .iter()
        .map(|path| normalize_path(path, true))
        .collect();

    let mut watch_roots: Vec<PathBuf> = Vec::new();
    let mut invalid_includes: Vec<String> = Vec::new();
    let continuous_includes = &config.continuous_filescan.included_folders;
    if continuous_includes.is_empty() {
        watch_roots = global_included_roots.clone();
    } else {
        let mut continuous = continuous_includes.clone();
        continuous.retain(|folder| check_folder_validity(folder) || safe_empty.contains(folder));
        let deduped = deduplicate_paths(&continuous);
        for folder in &deduped {
            let path = PathBuf::from(folder);
            let under_global = global_included_roots
                .iter()
                .any(|root| path.starts_with(root));
            let under_excluded = is_excluded(&path, &global_excluded_roots);
            if !under_global || under_excluded {
                invalid_includes.push(folder.clone());
                continue;
            }
            watch_roots.push(path);
        }
    }

    if !watch_roots.is_empty() {
        let watch_strings: Vec<String> = watch_roots
            .iter()
            .map(|path| path.to_string_lossy().to_string())
            .collect();
        let deduped = deduplicate_paths(&watch_strings);
        watch_roots = deduped.into_iter().map(PathBuf::from).collect();
    }

    let invalid = !continuous_includes.is_empty() && watch_roots.is_empty();

    WatchRootsOutcome {
        watch_roots,
        excluded_roots: global_excluded_roots,
        valid: !invalid,
        invalid_includes,
    }
}

/// Runtime state for the directory-mtime poller. In poll mode it replaces
/// notify's `PollWatcher` (which re-stats every file each tick) and runs on a
/// recurring interval. In native watcher mode it runs one-shot passes: a
/// catch-up diff at watcher startup/resume and a recovery pass after an event
/// overflow.
struct PollerRuntime {
    /// Recurring tick interval; None means one-shot passes only (native mode).
    interval: Option<Duration>,
    filters: Arc<PollFilters>,
    /// Taken while a pass runs on the blocking pool; restored on completion.
    snapshot: Option<PollerSnapshot>,
}

pub(crate) struct ContinuousScanState {
    index_db: String,
    user_data_db: String,
    config_store: SystemConfigStore,
    config: SystemConfig,
    watch_roots: Vec<PathBuf>,
    excluded_roots: Vec<PathBuf>,
    /// Latest root-validation outcome, kept for the status endpoint.
    invalid_includes: Vec<String>,
    roots_valid: bool,
    allowed_extensions: HashSet<String>,
    filescan_filter: Option<Arc<Match>>,
    scan_id: Option<i64>,
    scan_time: Option<String>,
    stats: ScanStats,
    timers: ScanTimers,
    last_progress: Instant,
    epoch: u64,
    paused: bool,
    /// Number of jobs currently holding a pause. A refcount, not a bool: a
    /// cancelled job's Drop-spawned resume can arrive after the *next* job's
    /// pause, and must not un-pause the scan underneath it.
    job_pauses: u32,
    actor_ref: ActorRef<ContinuousScanMessage>,
    factory: ActorRef<FactoryMessage<(), FileWork>>,
    factory_handle: Option<ractor::concurrency::JoinHandle<()>>,
    watcher: Option<RecommendedWatcher>,
    poller: Option<PollerRuntime>,
    enable_watcher: bool,
    deletions_since_maintenance: u64,
}
impl ContinuousScanState {
    fn reset_stats(&mut self) {
        self.stats = ScanStats::new();
        // Fresh timers per scan record; workers still running on the old scan
        // keep their clones and their spans stay attributed to the old record.
        self.timers = ScanTimers::default();
    }

    /// Throttled mid-scan write of the running counters so the open
    /// continuous-scan record shows progress. end_time stays NULL — that is
    /// what marks the scan as open. Write failures are ignored: progress rows
    /// are cosmetic.
    async fn maybe_report_progress(&mut self) {
        let Some(scan_id) = self.scan_id else {
            return;
        };
        if self.last_progress.elapsed() < SCAN_PROGRESS_INTERVAL {
            return;
        }
        self.last_progress = Instant::now();
        let update = FileScanUpdate {
            end_time: None,
            new_items: self.stats.new_items,
            unchanged_files: self.stats.unchanged_files,
            new_files: self.stats.new_files,
            modified_files: self.stats.modified_files,
            marked_unavailable: self.stats.marked_unavailable,
            errors: self.stats.errors,
            total_available: self.stats.total_available,
            false_changes: self.stats.false_changes,
            metadata_time: self.timers.metadata.busy_secs(),
            hashing_time: self.timers.hashing.busy_secs(),
            thumbgen_time: self.timers.thumbgen.busy_secs(),
            blurhash_time: self.timers.blurhash.busy_secs(),
        };
        let _ = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::UpdateFileScan {
                scan_id,
                update: update.clone(),
                reply,
            }
        })
        .await;
    }

    async fn refresh_roots(&mut self) -> bool {
        let mut safe_empty = HashSet::new();
        if let Ok(mut conn) = open_index_db_read(&self.index_db, &self.user_data_db).await {
            for folder in self
                .config
                .included_folders
                .iter()
                .chain(self.config.continuous_filescan.included_folders.iter())
            {
                if folder_is_empty(folder)
                    && crate::db::setup::has_indexed_files_under(&mut conn, folder)
                        .await
                        .is_ok_and(|indexed| !indexed)
                {
                    safe_empty.insert(folder.clone());
                }
            }
        }
        let outcome = compute_watch_roots_with_safe_empty(&self.config, &safe_empty);
        self.watch_roots = outcome.watch_roots;
        self.excluded_roots = outcome.excluded_roots;
        self.invalid_includes = outcome.invalid_includes;
        self.roots_valid = outcome.valid;
        self.allowed_extensions = build_extension_set(&self.config);
        self.filescan_filter = parse_filescan_filter(&self.config).map(Arc::new);
        if !outcome.valid {
            tracing::warn!(
                index_db = %self.index_db,
                invalid_includes = ?self.invalid_includes,
                "continuous scan disabled: includes must be within global included roots and not under excluded roots"
            );
        }
        outcome.valid
    }

    async fn start_scan(&mut self) -> ApiResult<()> {
        let scan_time = current_iso_timestamp();
        let scan_id =
            call_index_db_writer(&self.index_db, |reply| IndexDbWriterMessage::AddFileScan {
                scan_time: scan_time.clone(),
                path: CONTINUOUS_PATH_SENTINEL.to_string(),
                reply,
            })
            .await?;
        self.scan_id = Some(scan_id);
        self.scan_time = Some(scan_time);
        self.reset_stats();
        Ok(())
    }

    async fn close_scan(&mut self) -> ApiResult<()> {
        let Some(scan_id) = self.scan_id.take() else {
            return Ok(());
        };
        let end_time = current_iso_timestamp();
        // Stored times are phase wall-clock (busy) from the shared timers, not
        // sums of per-file spans across concurrent workers.
        let update = FileScanUpdate {
            end_time: Some(end_time),
            new_items: self.stats.new_items,
            unchanged_files: self.stats.unchanged_files,
            new_files: self.stats.new_files,
            modified_files: self.stats.modified_files,
            marked_unavailable: self.stats.marked_unavailable,
            errors: self.stats.errors,
            total_available: self.stats.total_available,
            false_changes: self.stats.false_changes,
            metadata_time: self.timers.metadata.busy_secs(),
            hashing_time: self.timers.hashing.busy_secs(),
            thumbgen_time: self.timers.thumbgen.busy_secs(),
            blurhash_time: self.timers.blurhash.busy_secs(),
        };
        call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::UpdateFileScan {
                scan_id,
                update: update.clone(),
                reply,
            }
        })
        .await?;
        self.scan_time = None;
        self.reset_stats();
        Ok(())
    }

    async fn close_stale_scan(&self) -> ApiResult<()> {
        let mut conn = open_index_db_read(&self.index_db, &self.user_data_db).await?;
        if let Some(scan_id) = get_open_file_scan_id(&mut conn, CONTINUOUS_PATH_SENTINEL).await? {
            let end_time = current_iso_timestamp();
            call_index_db_writer(&self.index_db, |reply| {
                IndexDbWriterMessage::CloseFileScan {
                    scan_id,
                    end_time: end_time.clone(),
                    reply,
                }
            })
            .await?;
        }
        Ok(())
    }

    fn should_process_path(&self, path: &Path) -> bool {
        if self.watch_roots.is_empty() {
            return false;
        }
        if is_hidden_or_temp(path) {
            return false;
        }
        if !has_allowed_extension(path, &self.allowed_extensions) {
            return false;
        }
        let is_included = self.watch_roots.iter().any(|root| path.starts_with(root));
        if !is_included {
            return false;
        }
        if is_excluded(path, &self.excluded_roots) {
            return false;
        }
        true
    }

    async fn handle_remove(&mut self, path: PathBuf) -> ApiResult<()> {
        if self.paused {
            return Ok(());
        }
        if !self.config.remove_unavailable_files {
            return Ok(());
        }
        if path.exists() {
            return Ok(());
        }
        if !self.should_process_path(&path) {
            return Ok(());
        }
        let mut conn = open_index_db_read(&self.index_db, &self.user_data_db).await?;
        let Some(FileDeleteInfo {
            item_id, scan_id, ..
        }) = get_file_delete_info(&mut conn, path.to_string_lossy().as_ref()).await?
        else {
            return Ok(());
        };

        let current_scan = self.scan_id.unwrap_or_default();
        let safe_delete =
            scan_id == current_scan || count_files_for_item(&mut conn, item_id).await? > 1;
        if !safe_delete {
            return Ok(());
        }

        let files_deleted = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::DeleteFileByPath {
                path: path.to_string_lossy().to_string(),
                reply,
            }
        })
        .await?;
        let item_deleted = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::DeleteItemIfOrphan { item_id, reply }
        })
        .await?;
        self.deletions_since_maintenance += files_deleted + u64::from(item_deleted);
        if self.deletions_since_maintenance >= MAINTENANCE_DELETION_THRESHOLD {
            self.deletions_since_maintenance = 0;
            run_post_job_maintenance(&self.index_db, true).await;
        }
        Ok(())
    }

    async fn handle_rename(&mut self, from: PathBuf, to: PathBuf) -> ApiResult<()> {
        if self.paused {
            return Ok(());
        }
        if !self.should_process_path(&to) {
            return Ok(());
        }
        if !to.exists() {
            return Ok(());
        }
        let scan_id = match self.scan_id {
            Some(scan_id) => scan_id,
            None => return Ok(()),
        };
        let last_modified = match get_last_modified_time_and_size(&to) {
            Ok((time, _)) => time,
            Err(_) => return Ok(()),
        };
        let renamed = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::RenameFilePath {
                old_path: from.to_string_lossy().to_string(),
                new_path: to.to_string_lossy().to_string(),
                scan_id,
                last_modified: last_modified.clone(),
                reply,
            }
        })
        .await?;
        if renamed {
            self.stats.unchanged_files += 1;
            self.stats.total_available += 1;
            return Ok(());
        }

        self.dispatch_path(to);
        Ok(())
    }

    fn dispatch_path(&self, path: PathBuf) {
        if self.paused {
            return;
        }
        if !self.should_process_path(&path) {
            return;
        }
        if let Ok(metadata) = std::fs::metadata(&path) {
            if !metadata.is_file() {
                return;
            }
        } else {
            return;
        }
        let scan_time = match &self.scan_time {
            Some(value) => value.clone(),
            None => current_iso_timestamp(),
        };
        let msg = FileWork {
            path,
            filescan_filter: self.filescan_filter.clone(),
            epoch: self.epoch,
            scan_time,
            index_db: self.index_db.clone(),
            user_data_db: self.user_data_db.clone(),
            timers: self.timers.clone(),
            reply_to: self.actor_ref.clone(),
        };
        let _ = self.factory.cast(FactoryMessage::Dispatch(Job {
            key: (),
            msg,
            options: JobOptions::default(),
            accepted: None,
        }));
    }

    /// Starts change detection for the current roots: the hierarchical mtime
    /// poller when `poll_interval_secs` is set, the native OS watcher
    /// otherwise.
    async fn start_watching(&mut self) {
        self.watcher = None;
        self.poller = None;
        if !self.enable_watcher {
            return;
        }
        let poll_interval = self
            .config
            .continuous_filescan
            .poll_interval_secs
            .filter(|secs| *secs > 0);
        if let Some(secs) = poll_interval {
            if let Err(err) = self.start_poller(Some(Duration::from_secs(secs))).await {
                tracing::error!(
                    index_db = %self.index_db,
                    error = ?err,
                    "failed to start continuous scan poller"
                );
            }
            return;
        }
        match start_watcher(self.actor_ref.clone(), &self.watch_roots) {
            Ok(watcher) => {
                self.watcher = Some(watcher);
                // One-shot catch-up pass: diffs the disk against the index so
                // changes made while nothing was watching (app offline, actor
                // paused for a job) are picked up instead of waiting for the
                // next cron scan. The retained snapshot also enables recovery
                // passes after watcher overflow.
                if let Err(err) = self.start_poller(None).await {
                    tracing::warn!(
                        index_db = %self.index_db,
                        error = ?err,
                        "failed to run continuous scan catch-up pass"
                    );
                }
            }
            Err(err) => {
                tracing::error!(
                    index_db = %self.index_db,
                    error = ?err,
                    "failed to start continuous scan watcher"
                );
            }
        }
    }

    /// Seeds the poller snapshot from the DB so the first pass diffs the disk
    /// against the index: files added or changed while watching was down are
    /// picked up immediately, while unchanged files are never re-dispatched.
    /// With an interval the pass reschedules itself (poll mode); without one
    /// it runs once and further passes only fire on demand (overflow).
    async fn start_poller(&mut self, interval: Option<Duration>) -> ApiResult<()> {
        let filters = Arc::new(PollFilters {
            roots: self.watch_roots.clone(),
            excluded_roots: self.excluded_roots.clone(),
            allowed_extensions: self.allowed_extensions.clone(),
        });
        let mut conn = open_index_db_read(&self.index_db, &self.user_data_db).await?;
        let rows = get_all_file_paths_with_mtime(&mut conn).await?;
        let snapshot = seed_snapshot(&rows, &filters);
        self.poller = Some(PollerRuntime {
            interval,
            filters,
            snapshot: Some(snapshot),
        });
        let _ = self
            .actor_ref
            .cast(ContinuousScanMessage::PollTick { epoch: self.epoch });
        Ok(())
    }
}
impl ContinuousScanActor {
    async fn build_factory(
        worker_count: usize,
    ) -> Result<
        (
            ActorRef<FactoryMessage<(), FileWork>>,
            ractor::concurrency::JoinHandle<()>,
        ),
        ActorProcessingErr,
    > {
        let factory_def = Factory::<
            (),
            FileWork,
            (),
            ContinuousWorker,
            routing::QueuerRouting<(), FileWork>,
            queues::DefaultQueue<(), FileWork>,
        >::default();

        let args = FactoryArguments::builder()
            .worker_builder(Box::new(ContinuousWorkerBuilder))
            .queue(Default::default())
            .router(Default::default())
            .num_initial_workers(worker_count)
            .build();

        let (factory, handle) = Actor::spawn(None, factory_def, args)
            .await
            .map_err(|err| ActorProcessingErr::from(format!("factory spawn failed: {err:?}")))?;
        Ok((factory, handle))
    }

    fn map_event(event: Event) -> Vec<FsEvent> {
        match event.kind {
            EventKind::Create(_) => event.paths.into_iter().map(FsEvent::Create).collect(),
            EventKind::Modify(ModifyKind::Name(RenameMode::Both)) => {
                if event.paths.len() >= 2 {
                    vec![FsEvent::Rename {
                        from: event.paths[0].clone(),
                        to: event.paths[1].clone(),
                    }]
                } else {
                    Vec::new()
                }
            }
            EventKind::Modify(ModifyKind::Name(RenameMode::From)) => {
                event.paths.into_iter().map(FsEvent::Remove).collect()
            }
            EventKind::Modify(ModifyKind::Name(RenameMode::To)) => {
                event.paths.into_iter().map(FsEvent::Create).collect()
            }
            EventKind::Modify(_) => event.paths.into_iter().map(FsEvent::Modify).collect(),
            EventKind::Remove(_) => event.paths.into_iter().map(FsEvent::Remove).collect(),
            EventKind::Other => vec![FsEvent::Overflow],
            _ => Vec::new(),
        }
    }
}

impl Actor for ContinuousScanActor {
    type Msg = ContinuousScanMessage;
    type State = ContinuousScanState;
    type Arguments = ContinuousScanActorArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let config_store = SystemConfigStore::new(args.data_dir);
        let config = config_store
            .load(&args.index_db)
            .map_err(|err| ActorProcessingErr::from(format!("{err:?}")))?;

        let options = ScanOptions::default();
        let (factory, handle) = Self::build_factory(options.worker_count).await?;

        let mut state = ContinuousScanState {
            index_db: args.index_db,
            user_data_db: args.user_data_db,
            config_store,
            config,
            watch_roots: Vec::new(),
            excluded_roots: Vec::new(),
            invalid_includes: Vec::new(),
            roots_valid: true,
            allowed_extensions: HashSet::new(),
            filescan_filter: None,
            scan_id: None,
            scan_time: None,
            stats: ScanStats::new(),
            timers: ScanTimers::default(),
            last_progress: Instant::now(),
            epoch: 0,
            paused: false,
            job_pauses: 0,
            actor_ref: myself.clone(),
            factory,
            factory_handle: Some(handle),
            watcher: None,
            poller: None,
            enable_watcher: args.enable_watcher,
            deletions_since_maintenance: 0,
        };

        let roots_ok = state.refresh_roots().await;
        let _ = state.close_stale_scan().await;
        if state.config.continuous_filescan.enabled && roots_ok {
            let _ = state.start_scan().await;
            state.start_watching().await;
        } else {
            state.paused = true;
        }

        Ok(state)
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            ContinuousScanMessage::Pause { reply } => {
                state.paused = true;
                state.job_pauses += 1;
                state.epoch = state.epoch.wrapping_add(1);
                state.watcher = None;
                state.poller = None;
                let _ = state.close_scan().await;
                let _ = reply.send(());
            }
            ContinuousScanMessage::Resume => {
                state.job_pauses = state.job_pauses.saturating_sub(1);
                if state.job_pauses > 0 {
                    return Ok(());
                }
                state.config = match state.config_store.load(&state.index_db) {
                    Ok(config) => config,
                    Err(err) => {
                        tracing::error!(error = ?err, "failed to reload continuous scan config");
                        return Ok(());
                    }
                };
                let roots_ok = state.refresh_roots().await;
                if !state.config.continuous_filescan.enabled || !roots_ok {
                    state.paused = true;
                    state.watcher = None;
                    state.poller = None;
                    if !roots_ok {
                        state.epoch = state.epoch.wrapping_add(1);
                        let _ = state.close_scan().await;
                    }
                    return Ok(());
                }
                state.paused = false;
                state.epoch = state.epoch.wrapping_add(1);
                let _ = state.start_scan().await;
                state.start_watching().await;
            }
            ContinuousScanMessage::UpdateConfig { config } => {
                let was_enabled = state.config.continuous_filescan.enabled;
                // Snapshot the parameters the poller/scan actually depends on
                // so we can tell a real change from a spurious config reload.
                let prev_roots = state.watch_roots.clone();
                let prev_excluded = state.excluded_roots.clone();
                let prev_extensions = state.allowed_extensions.clone();
                let prev_interval = state.config.continuous_filescan.poll_interval_secs;

                state.config = config;
                let roots_ok = state.refresh_roots().await;
                let now_enabled = state.config.continuous_filescan.enabled;
                if !now_enabled || !roots_ok {
                    state.paused = true;
                    // Only tear down when something was actually running, so a
                    // reload for an already-disabled DB is a no-op.
                    let was_active = state.poller.is_some()
                        || state.watcher.is_some()
                        || state.scan_id.is_some();
                    if was_active {
                        state.epoch = state.epoch.wrapping_add(1);
                        state.watcher = None;
                        state.poller = None;
                        let _ = state.close_scan().await;
                    }
                    return Ok(());
                }

                if state.job_pauses == 0 {
                    // The supervisor's config watcher covers `data/index`, and
                    // the index DBs live there — so SQLite's own WAL/shm and
                    // checkpoint writes during scanning arrive here as "config
                    // changed". Restarting rebuilds the poller by reloading the
                    // entire file-path snapshot from the DB
                    // (get_all_file_paths_with_mtime), so restart ONLY when a
                    // scan-relevant parameter changed or nothing is running.
                    // Other config (filescan filter, cron, scan-type flags) is
                    // already applied by refresh_roots above and takes effect on
                    // the next dispatch without a reseed.
                    let scan_relevant_changed = !was_enabled
                        || state.watch_roots != prev_roots
                        || state.excluded_roots != prev_excluded
                        || state.allowed_extensions != prev_extensions
                        || state.config.continuous_filescan.poll_interval_secs != prev_interval;
                    let needs_restart = scan_relevant_changed
                        || state.paused
                        || (state.enable_watcher && state.poller.is_none());
                    if needs_restart {
                        state.paused = false;
                        state.epoch = state.epoch.wrapping_add(1);
                        if !was_enabled {
                            let _ = state.start_scan().await;
                        }
                        state.start_watching().await;
                    }
                }
            }
            ContinuousScanMessage::FsEvent(event) => {
                if state.paused {
                    return Ok(());
                }
                match event {
                    FsEvent::Create(path) => state.dispatch_path(path),
                    FsEvent::Modify(path) => state.dispatch_path(path),
                    FsEvent::Remove(path) => {
                        let _ = state.handle_remove(path).await;
                    }
                    FsEvent::Rename { from, to } => {
                        let _ = state.handle_rename(from, to).await;
                    }
                    FsEvent::Overflow => {
                        tracing::warn!(
                            index_db = %state.index_db,
                            "continuous scan watcher overflow; scheduling recovery pass"
                        );
                        // Events were dropped; a poll pass re-diffs the tree
                        // against the snapshot and recovers anything missed.
                        // Delayed so the burst that caused the overflow can
                        // finish first; an already-running pass makes this a
                        // no-op and redundant dispatches are absorbed by the
                        // worker's mtime check.
                        if state.poller.is_some() {
                            let epoch = state.epoch;
                            let _ = state.actor_ref.send_after(POLL_SETTLE_DELAY, move || {
                                ContinuousScanMessage::PollTick { epoch }
                            });
                        }
                    }
                }
            }
            ContinuousScanMessage::PollTick { epoch } => {
                if state.paused || epoch != state.epoch {
                    return Ok(());
                }
                let Some(poller) = state.poller.as_mut() else {
                    return Ok(());
                };
                // A pass already in flight will schedule the next tick itself.
                let Some(snapshot) = poller.snapshot.take() else {
                    return Ok(());
                };
                let filters = poller.filters.clone();
                let reply = state.actor_ref.clone();
                tokio::task::spawn_blocking(move || {
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        run_poll_pass(snapshot, &filters)
                    }))
                    .unwrap_or_else(|_| {
                        // Losing the snapshot re-dispatches everything next
                        // pass; the worker's mtime check makes that cheap.
                        tracing::error!("continuous scan poll pass panicked");
                        PollOutcome {
                            snapshot: PollerSnapshot::default(),
                            changes: Vec::new(),
                            removals: Vec::new(),
                            degraded: true,
                        }
                    });
                    let _ = reply.cast(ContinuousScanMessage::PollCompleted { epoch, outcome });
                });
            }
            ContinuousScanMessage::PollCompleted { epoch, outcome } => {
                if epoch != state.epoch {
                    return Ok(());
                }
                let Some(poller) = state.poller.as_mut() else {
                    return Ok(());
                };
                poller.snapshot = Some(outcome.snapshot);
                let interval = poller.interval;
                if outcome.degraded {
                    tracing::warn!(
                        index_db = %state.index_db,
                        "poll pass degraded: some directories could not be inspected"
                    );
                }
                // Defer removals past the settle window so a move detected in
                // one pass indexes the new path first: the item then has two
                // file rows and the removal deletes just the stale one instead
                // of orphaning the item (which would drop its tags).
                for path in outcome.removals {
                    let _ = state.actor_ref.send_after(POLL_SETTLE_DELAY * 2, move || {
                        ContinuousScanMessage::FsEvent(FsEvent::Remove(path))
                    });
                }
                for change in outcome.changes {
                    let path = change.path;
                    let meta = change.meta;
                    let _ = state.actor_ref.send_after(POLL_SETTLE_DELAY, move || {
                        ContinuousScanMessage::SettleCheck {
                            epoch,
                            path,
                            meta,
                            attempts: 0,
                        }
                    });
                }
                if let Some(interval) = interval {
                    let _ = state
                        .actor_ref
                        .send_after(interval, move || ContinuousScanMessage::PollTick { epoch });
                }
            }
            ContinuousScanMessage::SettleCheck {
                epoch,
                path,
                meta,
                attempts,
            } => {
                if state.paused || epoch != state.epoch {
                    return Ok(());
                }
                // Re-stat off the actor so a slow network mount can't block
                // event processing; dispatch happens via DispatchStable.
                let reply = state.actor_ref.clone();
                tokio::spawn(async move {
                    let stat_path = path.clone();
                    let current = tokio::task::spawn_blocking(move || {
                        get_last_modified_time_and_size(&stat_path)
                    })
                    .await;
                    let Ok(Ok((last_modified, size))) = current else {
                        // Vanished or unreadable: drop it; a later poll pass
                        // or full scan picks it up if it comes back.
                        return;
                    };
                    let stable = last_modified == meta.last_modified
                        && meta.size.map_or(true, |prev| prev == size);
                    if stable {
                        let _ = reply.cast(ContinuousScanMessage::DispatchStable { epoch, path });
                        return;
                    }
                    // Still being written: retry with backoff until it settles.
                    let delay = POLL_SETTLE_DELAY
                        .saturating_mul(2u32.saturating_pow(attempts.min(5)))
                        .min(SETTLE_MAX_DELAY);
                    let _ = reply.send_after(delay, move || ContinuousScanMessage::SettleCheck {
                        epoch,
                        path,
                        meta: FileMeta {
                            last_modified,
                            size: Some(size),
                        },
                        attempts: attempts.saturating_add(1),
                    });
                });
            }
            ContinuousScanMessage::DispatchStable { epoch, path } => {
                if state.paused || epoch != state.epoch {
                    return Ok(());
                }
                state.dispatch_path(path);
            }
            ContinuousScanMessage::WorkerResult {
                epoch,
                scan_time,
                result,
            } => {
                if state.paused || epoch != state.epoch {
                    return Ok(());
                }
                let processed = match result {
                    Ok(processed) => processed,
                    Err(FileProcessError::Unchanged) => {
                        state.stats.unchanged_files += 1;
                        state.maybe_report_progress().await;
                        return Ok(());
                    }
                    Err(_) => {
                        state.stats.errors += 1;
                        state.maybe_report_progress().await;
                        return Ok(());
                    }
                };

                let mut conn = match open_index_db_read(&state.index_db, &state.user_data_db).await
                {
                    Ok(conn) => conn,
                    Err(err) => {
                        tracing::error!(error = ?err, "failed to open read connection");
                        state.stats.errors += 1;
                        return Ok(());
                    }
                };
                let file_data = match build_file_scan_data(&mut conn, processed, &scan_time).await {
                    Ok(data) => data,
                    Err(err) => {
                        tracing::error!(error = ?err, "failed to build file scan data");
                        state.stats.errors += 1;
                        return Ok(());
                    }
                };

                let false_change = file_data.new_file_hash == false && file_data.new_file_timestamp;
                if false_change {
                    state.stats.false_changes += 1;
                }

                if !file_data.thumbnails.is_empty() {
                    if let Ok(mut thumb_conn) =
                        open_index_db_read(&state.index_db, &state.user_data_db).await
                    {
                        if let Ok(has_thumb) = has_thumbnail(
                            &mut thumb_conn,
                            &file_data.sha256,
                            THUMBNAIL_PROCESS_VERSION,
                        )
                        .await
                        {
                            if !has_thumb {
                                let _ = call_index_db_writer(&state.index_db, |reply| {
                                    IndexDbWriterMessage::StoreThumbnails {
                                        sha256: file_data.sha256.clone(),
                                        mime_type: file_data.mime_type.clone(),
                                        process_version: THUMBNAIL_PROCESS_VERSION,
                                        thumbnails: file_data.thumbnails.clone(),
                                        reply,
                                    }
                                })
                                .await;
                            }
                        }
                    }
                }

                if !file_data.frames.is_empty() {
                    if let Ok(mut frame_conn) =
                        open_index_db_read(&state.index_db, &state.user_data_db).await
                    {
                        if let Ok(has_frame) =
                            has_frame(&mut frame_conn, &file_data.sha256, FRAME_PROCESS_VERSION)
                                .await
                        {
                            if !has_frame {
                                let _ = call_index_db_writer(&state.index_db, |reply| {
                                    IndexDbWriterMessage::StoreFrames {
                                        sha256: file_data.sha256.clone(),
                                        mime_type: file_data.mime_type.clone(),
                                        process_version: FRAME_PROCESS_VERSION,
                                        frames: file_data.frames.clone(),
                                        reply,
                                    }
                                })
                                .await;
                            }
                        }
                    }
                }

                if let Some(blurhash) = &file_data.blurhash {
                    if let Ok(mut blur_conn) =
                        open_index_db_read(&state.index_db, &state.user_data_db).await
                    {
                        if let Ok(has_value) = has_blurhash(&mut blur_conn, &file_data.sha256).await
                        {
                            if !has_value {
                                let _ = call_index_db_writer(&state.index_db, |reply| {
                                    IndexDbWriterMessage::SetBlurhash {
                                        sha256: file_data.sha256.clone(),
                                        blurhash: blurhash.clone(),
                                        reply,
                                    }
                                })
                                .await;
                            }
                        }
                    }
                }

                let update_result = call_index_db_writer(&state.index_db, |reply| {
                    IndexDbWriterMessage::UpdateFileData {
                        time_added: file_data.time_added.clone(),
                        scan_id: state.scan_id.unwrap_or_default(),
                        data: file_data.data.clone(),
                        reply,
                    }
                })
                .await;

                match update_result {
                    Ok(FileUpsertResult {
                        item_inserted,
                        file_updated,
                        file_deleted,
                        file_inserted,
                    }) => {
                        if item_inserted {
                            state.stats.new_items += 1;
                        }
                        if file_updated {
                            state.stats.unchanged_files += 1;
                        } else if file_deleted {
                            state.stats.modified_files += 1;
                        } else if file_inserted {
                            state.stats.new_files += 1;
                        }
                        state.stats.total_available += 1;
                    }
                    Err(err) => {
                        tracing::error!(error = ?err, "failed to update file data");
                        state.stats.errors += 1;
                    }
                }
                state.maybe_report_progress().await;
            }
            ContinuousScanMessage::GetStatus { reply } => {
                let _ = reply.send(ContinuousScanSnapshot {
                    paused: state.paused,
                    paused_for_job: state.job_pauses > 0,
                    watch_roots: state
                        .watch_roots
                        .iter()
                        .map(|root| root.to_string_lossy().to_string())
                        .collect(),
                    invalid_includes: state.invalid_includes.clone(),
                    roots_valid: state.roots_valid,
                });
            }
        }
        Ok(())
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let _ = state.close_scan().await;
        state.watcher = None;
        if let Some(handle) = state.factory_handle.take() {
            state.factory.stop(None);
            let _ = handle.await;
        }
        Ok(())
    }
}
fn start_watcher(
    actor: ActorRef<ContinuousScanMessage>,
    roots: &[PathBuf],
) -> Result<RecommendedWatcher, notify::Error> {
    let handler = move |res| match res {
        Ok(event) => {
            for mapped in ContinuousScanActor::map_event(event) {
                let _ = actor.cast(ContinuousScanMessage::FsEvent(mapped));
            }
        }
        Err(err) => {
            tracing::error!(error = ?err, "continuous scan watcher error");
        }
    };

    let mut watcher = RecommendedWatcher::new(handler, notify::Config::default())?;
    for root in roots {
        watcher.watch(root, RecursiveMode::Recursive)?;
    }

    Ok(watcher)
}

pub(crate) enum ContinuousScanSupervisorMessage {
    ResyncFromDisk,
    ConfigChanged {
        index_db: String,
    },
    PauseForJob {
        index_db: String,
        reply: oneshot::Sender<()>,
    },
    ResumeAfterJob {
        index_db: String,
    },
    /// Live state of one DB's scanner; None when no actor is running for it.
    GetStatus {
        index_db: String,
        reply: oneshot::Sender<Option<ContinuousScanSnapshot>>,
    },
    /// Process shutdown: stops every per-DB scan actor and refuses to spawn
    /// new ones. The scan actors are not linked to the supervisor, so merely
    /// stopping the supervisor would leave them running.
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

pub(crate) struct ContinuousScanSupervisor;

pub(crate) struct ContinuousScanSupervisorArgs {
    pub data_dir: PathBuf,
}

pub(crate) struct ContinuousScanSupervisorState {
    data_dir: PathBuf,
    config_store: SystemConfigStore,
    actors: HashMap<String, ActorRef<ContinuousScanMessage>>,
    watcher: Option<RecommendedWatcher>,
    shutting_down: bool,
}

impl Actor for ContinuousScanSupervisor {
    type Msg = ContinuousScanSupervisorMessage;
    type State = ContinuousScanSupervisorState;
    type Arguments = ContinuousScanSupervisorArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let config_store = SystemConfigStore::new(args.data_dir.clone());
        let watcher = start_supervisor_watcher(myself.clone(), &args.data_dir).ok();
        let mut state = ContinuousScanSupervisorState {
            data_dir: args.data_dir,
            config_store,
            actors: HashMap::new(),
            watcher,
            shutting_down: false,
        };
        let _ = myself.send_interval(
            RactorDuration::from_secs(SUPERVISOR_RESYNC_INTERVAL.as_secs()),
            || ContinuousScanSupervisorMessage::ResyncFromDisk,
        );
        let _ = resync_from_disk(&mut state).await;
        Ok(state)
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        if state.shutting_down {
            // No message may spawn or resume scan actors once shutdown began;
            // a queued ResyncFromDisk would otherwise respawn what Shutdown
            // just stopped.
            if let ContinuousScanSupervisorMessage::Shutdown { reply } = message {
                let _ = reply.send(());
            } else if let ContinuousScanSupervisorMessage::PauseForJob { reply, .. } = message {
                let _ = reply.send(());
            } else if let ContinuousScanSupervisorMessage::GetStatus { reply, .. } = message {
                let _ = reply.send(None);
            }
            return Ok(());
        }
        match message {
            ContinuousScanSupervisorMessage::ResyncFromDisk => {
                let _ = resync_from_disk(state).await;
            }
            ContinuousScanSupervisorMessage::ConfigChanged { index_db } => {
                let _ = sync_single_db(state, &index_db).await;
            }
            ContinuousScanSupervisorMessage::PauseForJob { index_db, reply } => {
                if let Some(actor) = state.actors.get(&index_db) {
                    let (tx, rx) = oneshot::channel();
                    let _ = actor.cast(ContinuousScanMessage::Pause { reply: tx });
                    let _ = rx.await;
                }
                let _ = reply.send(());
            }
            ContinuousScanSupervisorMessage::ResumeAfterJob { index_db } => {
                if let Some(actor) = state.actors.get(&index_db) {
                    let _ = actor.cast(ContinuousScanMessage::Resume);
                } else {
                    let _ = sync_single_db(state, &index_db).await;
                }
            }
            ContinuousScanSupervisorMessage::GetStatus { index_db, reply } => {
                // Awaited inline like PauseForJob: the child answers from
                // in-memory state, so this cannot stall the supervisor.
                let snapshot = match state.actors.get(&index_db) {
                    Some(actor) => {
                        let (tx, rx) = oneshot::channel();
                        let _ = actor.cast(ContinuousScanMessage::GetStatus { reply: tx });
                        rx.await.ok()
                    }
                    None => None,
                };
                let _ = reply.send(snapshot);
            }
            ContinuousScanSupervisorMessage::Shutdown { reply } => {
                state.shutting_down = true;
                // Dropping the watcher stops filesystem events from queueing
                // further resyncs.
                state.watcher = None;
                let stopped = state.actors.len();
                for (_, actor) in state.actors.drain() {
                    actor.stop(None);
                }
                if stopped > 0 {
                    tracing::info!(stopped, "stopped continuous scan actors for shutdown");
                }
                let _ = reply.send(());
            }
        }
        Ok(())
    }
}

async fn resync_from_disk(state: &mut ContinuousScanSupervisorState) -> ApiResult<()> {
    let index_dir = state.data_dir.join("index");
    let mut desired = HashMap::new();
    if index_dir.is_dir() {
        for entry in std::fs::read_dir(&index_dir).map_err(|err| {
            tracing::error!(error = %err, path = %index_dir.display(), "failed to read index dir");
            ApiError::internal("Failed to read index directory")
        })? {
            let entry = entry.map_err(|err| {
                tracing::error!(error = %err, "failed to read index dir entry");
                ApiError::internal("Failed to read index directory")
            })?;
            if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let index_db = entry.file_name().to_string_lossy().to_string();
            let index_db_file = entry.path().join("index.db");
            if !index_db_file.is_file() {
                continue;
            }
            let config = state.config_store.load(&index_db)?;
            if config.continuous_filescan.enabled {
                desired.insert(index_db, config);
            }
        }
    }

    let mut to_stop = Vec::new();
    for existing in state.actors.keys() {
        if !desired.contains_key(existing) {
            to_stop.push(existing.clone());
        }
    }
    for name in to_stop {
        if let Some(actor) = state.actors.remove(&name) {
            actor.stop(None);
        }
    }

    for (index_db, config) in desired {
        if let Some(actor) = state.actors.get(&index_db) {
            let _ = actor.cast(ContinuousScanMessage::UpdateConfig { config });
            continue;
        }
        let args = ContinuousScanActorArgs {
            index_db: index_db.clone(),
            user_data_db: index_db.clone(),
            data_dir: state.data_dir.clone(),
            enable_watcher: true,
        };
        let (actor, _handle) = Actor::spawn(
            Some(format!("continuous-scan-{index_db}")),
            ContinuousScanActor,
            args,
        )
        .await
        .map_err(|err| ApiError::internal(format!("Failed to spawn continuous scan: {err:?}")))?;
        let _ = actor.cast(ContinuousScanMessage::UpdateConfig { config });
        state.actors.insert(index_db, actor);
    }

    Ok(())
}

async fn sync_single_db(
    state: &mut ContinuousScanSupervisorState,
    index_db: &str,
) -> ApiResult<()> {
    let config = state.config_store.load(index_db)?;
    let index_db_file = state.data_dir.join("index").join(index_db).join("index.db");
    if !index_db_file.is_file() {
        if let Some(actor) = state.actors.remove(index_db) {
            actor.stop(None);
        }
        return Ok(());
    }
    if !config.continuous_filescan.enabled {
        if let Some(actor) = state.actors.remove(index_db) {
            actor.stop(None);
        }
        return Ok(());
    }

    if let Some(actor) = state.actors.get(index_db) {
        let _ = actor.cast(ContinuousScanMessage::UpdateConfig { config });
        return Ok(());
    }

    let args = ContinuousScanActorArgs {
        index_db: index_db.to_string(),
        user_data_db: index_db.to_string(),
        data_dir: state.data_dir.clone(),
        enable_watcher: true,
    };
    let (actor, _handle) = Actor::spawn(
        Some(format!("continuous-scan-{index_db}")),
        ContinuousScanActor,
        args,
    )
    .await
    .map_err(|err| ApiError::internal(format!("Failed to spawn continuous scan: {err:?}")))?;
    let _ = actor.cast(ContinuousScanMessage::UpdateConfig { config });
    state.actors.insert(index_db.to_string(), actor);
    Ok(())
}

/// True for the SQLite files the scanner rewrites constantly: the main DB plus
/// its WAL/SHM/journal sidecars. Events touching only these are DB activity,
/// not configuration changes.
fn is_sqlite_db_file(path: &Path) -> bool {
    match path.file_name().and_then(|name| name.to_str()) {
        Some(name) => {
            name.ends_with(".db")
                || name.ends_with(".db-wal")
                || name.ends_with(".db-shm")
                || name.ends_with(".db-journal")
        }
        None => false,
    }
}

/// Whether a supervisor-watcher event should trigger a resync. The index DBs
/// live under the watched tree, so scanning rewrites their DB files on every
/// transaction and checkpoint; an event touching only those is skipped. Events
/// touching anything else (a `config.toml`, or a DB directory being added or
/// removed) are relevant, as are path-less events some backends emit.
fn event_is_relevant(event: &Event) -> bool {
    event.paths.is_empty() || event.paths.iter().any(|p| !is_sqlite_db_file(p))
}

fn start_supervisor_watcher(
    actor: ActorRef<ContinuousScanSupervisorMessage>,
    data_dir: &Path,
) -> Result<RecommendedWatcher, notify::Error> {
    let watch_root = data_dir.join("index");
    let _ = std::fs::create_dir_all(&watch_root);
    let mut watcher = RecommendedWatcher::new(
        move |res| match res {
            Ok(event) => {
                // Ignore the scanner's own DB writes; real config saves also
                // arrive out-of-band via notify_config_change, so this watcher
                // is only a backstop for on-disk edits and DB dir changes.
                if event_is_relevant(&event) {
                    let _ = actor.cast(ContinuousScanSupervisorMessage::ResyncFromDisk);
                }
            }
            Err(err) => {
                tracing::error!(error = ?err, "continuous scan supervisor watcher error");
            }
        },
        notify::Config::default(),
    )?;
    watcher.watch(&watch_root, RecursiveMode::Recursive)?;
    Ok(watcher)
}

static SUPERVISOR: OnceCell<ActorRef<ContinuousScanSupervisorMessage>> = OnceCell::const_new();

pub(crate) async fn ensure_continuous_supervisor()
-> ApiResult<ActorRef<ContinuousScanSupervisorMessage>> {
    SUPERVISOR
        .get_or_try_init(|| async {
            let data_dir = crate::config::runtime().data_folder.clone();
            let (actor, _handle) = Actor::spawn(
                Some("continuous-scan-supervisor".to_string()),
                ContinuousScanSupervisor,
                ContinuousScanSupervisorArgs { data_dir },
            )
            .await
            .map_err(|err| {
                tracing::error!(error = ?err, "failed to start continuous scan supervisor");
                ApiError::internal("Failed to start continuous scan supervisor")
            })?;
            Ok(actor)
        })
        .await
        .map(Clone::clone)
}

/// Stops every continuous scan actor and then the supervisor itself. No-op
/// when continuous scanning was never started. Used at process shutdown.
pub(crate) async fn stop_continuous_scanning() {
    let Some(supervisor) = SUPERVISOR.get() else {
        return;
    };
    let (reply, rx) = oneshot::channel();
    if supervisor
        .cast(ContinuousScanSupervisorMessage::Shutdown { reply })
        .is_ok()
    {
        let _ = rx.await;
    }
    supervisor.stop(None);
}

pub(crate) async fn notify_config_change(index_db: &str) -> ApiResult<()> {
    let supervisor = ensure_continuous_supervisor().await?;
    supervisor
        .cast(ContinuousScanSupervisorMessage::ConfigChanged {
            index_db: index_db.to_string(),
        })
        .map_err(|_| ApiError::internal("Failed to notify continuous scan supervisor"))?;
    Ok(())
}

/// Live scanner state for the status endpoint; None when no scanner actor is
/// running for this DB (continuous scanning disabled, or DB missing on disk).
pub(crate) async fn get_scan_status(index_db: &str) -> ApiResult<Option<ContinuousScanSnapshot>> {
    let supervisor = ensure_continuous_supervisor().await?;
    let (reply, rx) = oneshot::channel();
    supervisor
        .cast(ContinuousScanSupervisorMessage::GetStatus {
            index_db: index_db.to_string(),
            reply,
        })
        .map_err(|_| ApiError::internal("Failed to query continuous scan status"))?;
    rx.await
        .map_err(|_| ApiError::internal("Continuous scan supervisor dropped status request"))
}

pub(crate) async fn pause_for_job(index_db: &str) -> ApiResult<()> {
    let supervisor = ensure_continuous_supervisor().await?;
    let (reply, rx) = oneshot::channel();
    supervisor
        .cast(ContinuousScanSupervisorMessage::PauseForJob {
            index_db: index_db.to_string(),
            reply,
        })
        .map_err(|_| ApiError::internal("Failed to pause continuous scan"))?;
    let _ = rx.await;
    Ok(())
}

pub(crate) async fn resume_after_job(index_db: &str) -> ApiResult<()> {
    let supervisor = ensure_continuous_supervisor().await?;
    supervisor
        .cast(ContinuousScanSupervisorMessage::ResumeAfterJob {
            index_db: index_db.to_string(),
        })
        .map_err(|_| ApiError::internal("Failed to resume continuous scan"))?;
    Ok(())
}

/// Pauses continuous scanning for a job and guarantees resumption even when
/// the owning task is aborted (job cancellation) or panics: `Drop` spawns the
/// resume, so the scan cannot be left paused by a cancelled job.
pub(crate) struct JobPauseGuard {
    index_db: Option<String>,
}

pub(crate) async fn pause_for_job_guarded(index_db: &str) -> ApiResult<JobPauseGuard> {
    pause_for_job(index_db).await?;
    Ok(JobPauseGuard {
        index_db: Some(index_db.to_string()),
    })
}

impl JobPauseGuard {
    /// Resumes the scan inline; use on the normal completion path so the
    /// resume happens before any follow-up work instead of via `Drop`.
    pub(crate) async fn resume(mut self) {
        if let Some(index_db) = self.index_db.take() {
            let _ = resume_after_job(&index_db).await;
        }
    }
}

impl Drop for JobPauseGuard {
    fn drop(&mut self) {
        if let Some(index_db) = self.index_db.take() {
            tokio::spawn(async move {
                let _ = resume_after_job(&index_db).await;
            });
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::migrate_databases_on_disk;
    use crate::test_utils::test_data_dir;
    use image::{ImageBuffer, Rgb};
    use ractor::Actor;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tempfile::TempDir;

    fn unique_db_name(prefix: &str) -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        format!("{prefix}-{}", COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    fn write_test_image(path: &std::path::Path) {
        let img: ImageBuffer<Rgb<u8>, Vec<u8>> = ImageBuffer::from_pixel(2, 2, Rgb([255, 0, 0]));
        img.save(path).unwrap();
    }

    #[tokio::test]
    async fn continuous_scan_writes_and_closes_scan() {
        let test_env = test_data_dir();
        let root = test_env.path().to_path_buf();
        let index_db = unique_db_name("cont");
        let _ = migrate_databases_on_disk(Some(&index_db), Some(&index_db))
            .await
            .unwrap();

        let watch_dir = root.join("watch");
        std::fs::create_dir_all(&watch_dir).unwrap();
        let file_path = watch_dir.join("test.png");
        write_test_image(&file_path);

        let store = SystemConfigStore::new(root.clone());
        let mut config = store.load(&index_db).unwrap();
        config.continuous_filescan.enabled = true;
        config.included_folders = vec![watch_dir.to_string_lossy().to_string()];
        store.save(&index_db, &config).unwrap();

        let (actor, _handle) = Actor::spawn(
            None,
            ContinuousScanActor,
            ContinuousScanActorArgs {
                index_db: index_db.clone(),
                user_data_db: index_db.clone(),
                data_dir: root.clone(),
                enable_watcher: false,
            },
        )
        .await
        .unwrap();

        let prepared = process_file(
            file_path.clone(),
            parse_filescan_filter(&config).map(Arc::new),
            &ScanTimers::default(),
        )
        .unwrap();
        actor
            .cast(ContinuousScanMessage::WorkerResult {
                epoch: 0,
                scan_time: current_iso_timestamp(),
                result: Ok(prepared),
            })
            .unwrap();

        let mut attempts = 0;
        loop {
            let mut conn = crate::db::open_index_db_read_no_user_data(&index_db)
                .await
                .unwrap();
            let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files")
                .fetch_one(&mut conn)
                .await
                .unwrap();
            if count.0 > 0 || attempts > 20 {
                break;
            }
            attempts += 1;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let (tx, rx) = oneshot::channel();
        actor
            .cast(ContinuousScanMessage::Pause { reply: tx })
            .unwrap();
        let _ = rx.await;

        let mut conn = crate::db::open_index_db_read_no_user_data(&index_db)
            .await
            .unwrap();
        let row: Option<(Option<String>,)> =
            sqlx::query_as("SELECT end_time FROM file_scans WHERE path = ?1")
                .bind(CONTINUOUS_PATH_SENTINEL)
                .fetch_optional(&mut conn)
                .await
                .unwrap();
        assert!(row.is_some());
        assert!(row.unwrap().0.is_some());
    }

    #[tokio::test]
    async fn epoch_gating_drops_results() {
        let test_env = test_data_dir();
        let root = test_env.path().to_path_buf();
        let index_db = unique_db_name("cont");
        let _ = migrate_databases_on_disk(Some(&index_db), Some(&index_db))
            .await
            .unwrap();

        let watch_dir = root.join("watch2");
        std::fs::create_dir_all(&watch_dir).unwrap();
        let file_path = watch_dir.join("test2.png");
        write_test_image(&file_path);

        let store = SystemConfigStore::new(root.clone());
        let mut config = store.load(&index_db).unwrap();
        config.continuous_filescan.enabled = true;
        config.included_folders = vec![watch_dir.to_string_lossy().to_string()];
        store.save(&index_db, &config).unwrap();

        let (actor, _handle) = Actor::spawn(
            None,
            ContinuousScanActor,
            ContinuousScanActorArgs {
                index_db: index_db.clone(),
                user_data_db: index_db.clone(),
                data_dir: root.clone(),
                enable_watcher: false,
            },
        )
        .await
        .unwrap();

        let (tx, rx) = oneshot::channel();
        actor
            .cast(ContinuousScanMessage::Pause { reply: tx })
            .unwrap();
        let _ = rx.await;

        let prepared = process_file(
            file_path.clone(),
            parse_filescan_filter(&config).map(Arc::new),
            &ScanTimers::default(),
        )
        .unwrap();
        actor
            .cast(ContinuousScanMessage::WorkerResult {
                epoch: 0,
                scan_time: current_iso_timestamp(),
                result: Ok(prepared),
            })
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut conn = crate::db::open_index_db_read_no_user_data(&index_db)
            .await
            .unwrap();
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(count.0, 0);
    }

    // Native watcher mode runs a one-shot catch-up pass at startup: a file
    // created before the watcher existed (no FS event ever fires for it) must
    // still get indexed by diffing the disk against the DB.
    #[tokio::test]
    async fn native_mode_catch_up_indexes_preexisting_files() {
        let test_env = test_data_dir();
        let root = test_env.path().to_path_buf();
        let index_db = unique_db_name("catchup");
        let _ = migrate_databases_on_disk(Some(&index_db), Some(&index_db))
            .await
            .unwrap();

        let watch_dir = root.join("catchupwatch");
        std::fs::create_dir_all(&watch_dir).unwrap();
        write_test_image(&watch_dir.join("offline.png"));

        let store = SystemConfigStore::new(root.clone());
        let mut config = store.load(&index_db).unwrap();
        config.continuous_filescan.enabled = true;
        // poll_interval_secs stays None: native watcher mode.
        config.included_folders = vec![watch_dir.to_string_lossy().to_string()];
        store.save(&index_db, &config).unwrap();

        let (actor, _handle) = Actor::spawn(
            None,
            ContinuousScanActor,
            ContinuousScanActorArgs {
                index_db: index_db.clone(),
                user_data_db: index_db.clone(),
                data_dir: root.clone(),
                enable_watcher: true,
            },
        )
        .await
        .unwrap();

        let mut found = false;
        for _ in 0..120 {
            let mut conn = crate::db::open_index_db_read_no_user_data(&index_db)
                .await
                .unwrap();
            let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files")
                .fetch_one(&mut conn)
                .await
                .unwrap();
            if count.0 > 0 {
                found = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        actor.stop(None);
        assert!(found, "catch-up pass did not index the pre-existing file");
    }

    // End-to-end poll mode: seed → poll pass → settle → worker → DB row.
    #[tokio::test]
    async fn poll_mode_picks_up_new_files() {
        let test_env = test_data_dir();
        let root = test_env.path().to_path_buf();
        let index_db = unique_db_name("poll");
        let _ = migrate_databases_on_disk(Some(&index_db), Some(&index_db))
            .await
            .unwrap();

        let watch_dir = root.join("pollwatch");
        std::fs::create_dir_all(&watch_dir).unwrap();
        let file_path = watch_dir.join("new.png");
        write_test_image(&file_path);

        let store = SystemConfigStore::new(root.clone());
        let mut config = store.load(&index_db).unwrap();
        config.continuous_filescan.enabled = true;
        config.continuous_filescan.poll_interval_secs = Some(1);
        config.included_folders = vec![watch_dir.to_string_lossy().to_string()];
        store.save(&index_db, &config).unwrap();

        let (actor, _handle) = Actor::spawn(
            None,
            ContinuousScanActor,
            ContinuousScanActorArgs {
                index_db: index_db.clone(),
                user_data_db: index_db.clone(),
                data_dir: root.clone(),
                enable_watcher: true,
            },
        )
        .await
        .unwrap();

        // First tick + settle delay + processing; poll interval is 1s and the
        // settle window is 2s, so this normally completes within ~5s.
        let mut found = false;
        for _ in 0..120 {
            let mut conn = crate::db::open_index_db_read_no_user_data(&index_db)
                .await
                .unwrap();
            let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files")
                .fetch_one(&mut conn)
                .await
                .unwrap();
            if count.0 > 0 {
                found = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        actor.stop(None);
        assert!(found, "poll mode did not index the new file in time");
    }

    #[test]
    fn continuous_includes_subset_of_global() {
        let tmp = TempDir::new().unwrap();
        let global_root = tmp.path().join("global");
        let subset = global_root.join("subset");
        fs::create_dir_all(&subset).unwrap();
        fs::write(global_root.join("dummy.txt"), "x").unwrap();
        fs::write(subset.join("dummy.txt"), "x").unwrap();

        let mut config = SystemConfig::default();
        config.included_folders = vec![global_root.to_string_lossy().to_string()];
        config.continuous_filescan.included_folders = vec![subset.to_string_lossy().to_string()];

        let outcome = compute_watch_roots(&config);
        assert!(outcome.valid);
        assert_eq!(outcome.watch_roots.len(), 1);
        assert!(outcome.watch_roots[0].starts_with(&subset));
    }

    #[test]
    fn continuous_includes_outside_global_disables() {
        let tmp = TempDir::new().unwrap();
        let global_root = tmp.path().join("global");
        let outside_root = tmp.path().join("outside");
        fs::create_dir_all(&global_root).unwrap();
        fs::create_dir_all(&outside_root).unwrap();
        fs::write(global_root.join("dummy.txt"), "x").unwrap();
        fs::write(outside_root.join("dummy.txt"), "x").unwrap();

        let mut config = SystemConfig::default();
        config.included_folders = vec![global_root.to_string_lossy().to_string()];
        config.continuous_filescan.included_folders =
            vec![outside_root.to_string_lossy().to_string()];

        let outcome = compute_watch_roots(&config);
        assert!(!outcome.valid);
        assert!(outcome.watch_roots.is_empty());
    }

    /// GetStatus reflects the evaluated roots and the pause refcount: a valid
    /// subset include is watched, an existing-but-outside include is reported
    /// as invalid, and a job pause flips both paused flags.
    #[tokio::test]
    async fn get_status_reports_roots_and_pauses() {
        let test_env = test_data_dir();
        let root = test_env.path().to_path_buf();
        let index_db = unique_db_name("status");
        let _ = migrate_databases_on_disk(Some(&index_db), Some(&index_db))
            .await
            .unwrap();

        let watch_dir = root.join("statuswatch");
        let subset = watch_dir.join("subset");
        let outside = root.join("statusoutside");
        fs::create_dir_all(&subset).unwrap();
        fs::create_dir_all(&outside).unwrap();
        // Folder validity requires non-empty directories.
        fs::write(subset.join("dummy.txt"), "x").unwrap();
        fs::write(outside.join("dummy.txt"), "x").unwrap();

        let store = SystemConfigStore::new(root.clone());
        let mut config = store.load(&index_db).unwrap();
        config.continuous_filescan.enabled = true;
        config.included_folders = vec![watch_dir.to_string_lossy().to_string()];
        config.continuous_filescan.included_folders = vec![
            subset.to_string_lossy().to_string(),
            outside.to_string_lossy().to_string(),
        ];
        store.save(&index_db, &config).unwrap();

        let (actor, _handle) = Actor::spawn(
            None,
            ContinuousScanActor,
            ContinuousScanActorArgs {
                index_db: index_db.clone(),
                user_data_db: index_db.clone(),
                data_dir: root.clone(),
                enable_watcher: false,
            },
        )
        .await
        .unwrap();

        let (tx, rx) = oneshot::channel();
        actor
            .cast(ContinuousScanMessage::GetStatus { reply: tx })
            .unwrap();
        let snapshot = rx.await.unwrap();
        assert!(!snapshot.paused);
        assert!(!snapshot.paused_for_job);
        assert!(snapshot.roots_valid);
        assert_eq!(snapshot.watch_roots.len(), 1);
        assert_eq!(snapshot.invalid_includes.len(), 1);

        let (tx, rx) = oneshot::channel();
        actor
            .cast(ContinuousScanMessage::Pause { reply: tx })
            .unwrap();
        let _ = rx.await;

        let (tx, rx) = oneshot::channel();
        actor
            .cast(ContinuousScanMessage::GetStatus { reply: tx })
            .unwrap();
        let snapshot = rx.await.unwrap();
        assert!(snapshot.paused);
        assert!(snapshot.paused_for_job);

        actor.stop(None);
    }

    #[test]
    fn continuous_includes_under_global_excluded_disables() {
        let tmp = TempDir::new().unwrap();
        let global_root = tmp.path().join("global");
        let excluded_root = global_root.join("excluded");
        fs::create_dir_all(&excluded_root).unwrap();
        fs::write(global_root.join("dummy.txt"), "x").unwrap();
        fs::write(excluded_root.join("dummy.txt"), "x").unwrap();

        let mut config = SystemConfig::default();
        config.included_folders = vec![global_root.to_string_lossy().to_string()];
        config.excluded_folders = vec![excluded_root.to_string_lossy().to_string()];
        config.continuous_filescan.included_folders =
            vec![excluded_root.to_string_lossy().to_string()];

        let outcome = compute_watch_roots(&config);
        assert!(!outcome.valid);
        assert!(outcome.watch_roots.is_empty());
    }

    #[test]
    fn sqlite_db_files_are_recognized() {
        let dir = std::path::Path::new("data/index/mydb");
        for name in [
            "index.db",
            "index.db-wal",
            "index.db-shm",
            "index.db-journal",
            "storage.db",
        ] {
            assert!(is_sqlite_db_file(&dir.join(name)), "{name} should match");
        }
        // Config and directory entries must not be mistaken for DB files.
        for name in ["config.toml", "mydb", "index.db.pkl"] {
            assert!(
                !is_sqlite_db_file(&dir.join(name)),
                "{name} should not match"
            );
        }
    }

    #[test]
    fn supervisor_watcher_skips_db_only_events() {
        let db = std::path::Path::new("data/index/mydb/index.db-wal").to_path_buf();
        let cfg = std::path::Path::new("data/index/mydb/config.toml").to_path_buf();

        // The scanner's own DB writes must not trigger a resync.
        assert!(!event_is_relevant(
            &Event::new(EventKind::Any).add_path(db.clone())
        ));
        // A config.toml change must.
        assert!(event_is_relevant(
            &Event::new(EventKind::Any).add_path(cfg.clone())
        ));
        // A DB write coinciding with a config write still counts as relevant.
        assert!(event_is_relevant(
            &Event::new(EventKind::Any).add_path(db).add_path(cfg)
        ));
        // Path-less events (some backends emit them) are treated as relevant.
        assert!(event_is_relevant(&Event::new(EventKind::Any)));
    }
}
