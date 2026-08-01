use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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

use crate::api_error::{ApiError, Blocker};
use crate::db::files::has_blurhash;
use crate::db::{
    file_scans::{FileScanUpdate, get_open_file_scan_id},
    files::{
        FileDeleteInfo, FileUpsertResult, count_files_for_item, get_all_file_paths_with_mtime,
        get_file_by_path, get_file_delete_info,
    },
    index_writer::{IndexDbWriterMessage, call_index_db_writer},
    open_index_db_read,
    scan_errors::{ScanErrorRecord, get_scan_error},
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
    format_system_time, get_last_modified_time_and_size, has_allowed_extension, infer_mime_type,
    is_excluded, is_hidden_or_temp, normalize_path, parse_filescan_filter, process_file,
    run_post_job_maintenance,
};
use crate::pql::model::Match;

type ApiResult<T> = Result<T, ApiError>;

const CONTINUOUS_PATH_SENTINEL: &str = "<continuous>";
const SUPERVISOR_RESYNC_INTERVAL: Duration = Duration::from_secs(300);
// Watcher-driven resyncs are coalesced into one pass per window: a config save
// arrives as a burst (temp file created, written, renamed over the target), and
// a resync per event is pure waste.
const SUPERVISOR_RESYNC_DEBOUNCE: Duration = Duration::from_secs(1);
// Watcher deletions happen outside any job, so no post-job maintenance pass
// ever accounts for them; compact once this many rows have been removed.
const MAINTENANCE_DELETION_THRESHOLD: u64 = 1000;
// A file detected by the poller must keep the same mtime and size across this
// window before it is processed, so half-written files aren't hashed.
const POLL_SETTLE_DELAY: Duration = Duration::from_secs(2);
// Backoff ceiling for files that keep changing (e.g. a long copy in progress).
const SETTLE_MAX_DELAY: Duration = Duration::from_secs(60);
// Poll interval used when the native watcher was requested but failed to start
// (commonly the OS watch-descriptor limit on a large tree). Polling is heavier
// than the watcher, so this only ever applies as a degraded fallback, and the
// status endpoint reports it so the choice is visible rather than silent.
const WATCHER_FALLBACK_POLL_INTERVAL: Duration = Duration::from_secs(60);

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
        let stat = tokio::task::spawn_blocking(move || get_last_modified_time_and_size(&stat_path))
            .await
            .ok()
            .and_then(|res| res.ok());
        let path_str = path.to_string_lossy().to_string();
        // The path *as the ledger stores it*, when it has a row at all. Gates
        // the success-side delete below and carries the stored casing, which
        // on Windows need not be the casing this event reported. The batch
        // walker gets the same thing from its per-root preload; an
        // event-driven scan gets it from the lookup it is doing here anyway.
        let mut ledger_path: Option<String> = None;

        if let Some((disk_mtime, file_size)) = &stat
            && let Ok(mut conn) = open_index_db_read(&index_db, &user_data_db).await
        {
            // The recorded-failure check comes first: a file the scan
            // already gave up on has no `files` row for the mtime shortcut
            // to find, so this is the only thing between a watcher event
            // and a full re-hash of a file that will fail again.
            match get_scan_error(&mut conn, &path_str).await {
                Ok(Some(skip)) => {
                    let suppressed = skip.suppresses(disk_mtime, *file_size);
                    ledger_path = Some(skip.path);
                    if suppressed {
                        let _ = reply_to.cast(ContinuousScanMessage::WorkerResult {
                            epoch,
                            scan_time,
                            path,
                            stat,
                            ledger_path,
                            result: Err(FileProcessError::KnownBad),
                        });
                        return Ok(job.key);
                    }
                }
                Ok(None) => {}
                // Advisory: without the verdict the file is simply
                // processed, which is exactly the old behavior.
                Err(err) => {
                    tracing::warn!(error = ?err, path = %path.display(), "scan failure lookup failed")
                }
            }
            if let Ok(Some(existing)) = get_file_by_path(&mut conn, &path_str).await
                && &existing.last_modified == disk_mtime
            {
                let _ = reply_to.cast(ContinuousScanMessage::WorkerResult {
                    epoch,
                    scan_time,
                    path,
                    stat,
                    ledger_path,
                    result: Err(FileProcessError::Unchanged),
                });
                return Ok(job.key);
            }
        }

        let work_path = path.clone();
        let result =
            tokio::task::spawn_blocking(move || process_file(work_path, filescan_filter, &timers))
                .await
                .map_err(|err| FileProcessError::Worker(err.to_string()))
                .and_then(|res| res);

        let _ = reply_to.cast(ContinuousScanMessage::WorkerResult {
            epoch,
            scan_time,
            path,
            stat,
            ledger_path,
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
        /// The path the worker handled. Carried because the failure half needs
        /// an identity: `FileProcessError` says what went wrong, not to what.
        path: PathBuf,
        /// The file as the worker saw it — the `scan_errors` retry key. `None`
        /// when the stat itself failed, which is transient and never recorded.
        stat: Option<(String, i64)>,
        /// The path the ledger stores for this file, when it had a row at all.
        /// Gates the success-side delete (so a healthy file costs no writer
        /// round-trip) and is what that delete binds.
        ledger_path: Option<String>,
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
    /// Whether change detection is actually running (a watcher or a poller).
    /// Reported separately from `paused` because a failed start leaves the
    /// actor unpaused with nothing watching — which used to read as healthy.
    pub watching: bool,
    /// The native watcher was requested but failed; the poller is standing in.
    pub watcher_fallback: bool,
    /// Interval of the poller actually running, including the fallback one.
    /// None in watcher mode.
    pub effective_poll_interval_secs: Option<u64>,
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
    /// Events skipped on an active recorded scan failure. Deliberately not
    /// folded into `errors` (nothing was attempted) and not persisted on the
    /// scan row (`file_scans` has no column for it); logged when the scan
    /// record closes.
    known_bad: i64,
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
            known_bad: 0,
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
    /// True when the native watcher was configured but failed to start and the
    /// poller is standing in for it. Surfaced through the status endpoint: a
    /// log line alone would leave most users unaware their chosen mode is not
    /// the one in effect.
    watcher_fallback: bool,
    enable_watcher: bool,
    deletions_since_maintenance: u64,
    /// Files this session already classified as failing, with the
    /// `(last_modified, file_size)` they failed at.
    ///
    /// The ledger alone cannot stop the re-attempt storm here: an ambiguous
    /// verdict needs two attempts, and `attempts` is deduped on the *scan id*,
    /// which for a continuous scan spans the whole session — so a file that
    /// keeps generating watcher events would be re-hashed, re-decoded and
    /// re-upserted (bumping the search-cache epoch each time) forever without
    /// its count ever moving. This is the per-session half of that gate: the
    /// second event for an unchanged failing file costs one stat.
    ///
    /// Not persistence, and not a substitute for the ledger: it is dropped on
    /// every scan restart, so "attempts stays 1 per session" is unchanged.
    failed_stats: HashMap<PathBuf, (String, i64)>,
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
        // Before anything is processed, exactly like the batch scan: a
        // dependency that was missing when these files failed may be installed
        // now, and this is the only place a long-lived watching session ever
        // re-probes. Cheap — one indexed query returning nothing — when the
        // ledger has no blocked rows, which is the normal case.
        if let Err(err) = crate::jobs::files::heal_blocked_scan_errors(&self.index_db).await {
            tracing::warn!(error = ?err, "failed to re-probe blocked scan failures");
        }

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
        // The session cache is scoped to the scan record it counts attempts
        // against, so a new record starts with an empty one.
        self.failed_stats.clear();
        Ok(())
    }

    /// Logs one file failure and, when its class is one the ledger stores,
    /// records it. Before this the error variant was discarded outright: the
    /// counter moved and nothing said which file or why.
    ///
    /// The scan id is the continuous scan's own `file_scans` row, which is
    /// what dedups `attempts`. That row is long-lived — it spans the whole
    /// watching session — so a `skip_after = 2` verdict written here is
    /// normally confirmed by the next *batch* scan (or by the next session)
    /// rather than by a second event. That is the intended conservatism: an
    /// ambiguous verdict from an external tool should not be settled by two
    /// events seconds apart on the same flaky mount.
    async fn record_file_failure(
        &mut self,
        path: &Path,
        stat: Option<(String, i64)>,
        error: &FileProcessError,
    ) {
        if matches!(error, FileProcessError::Filtered) {
            tracing::debug!(
                path = %path.display(),
                "file does not match the filescan filter, skipping"
            );
            return;
        }
        let classified = error.classified();
        tracing::error!(
            error = ?error,
            path = %path.display(),
            stage = classified.map(|failure| failure.stage).unwrap_or("-"),
            error_class = classified
                .and_then(|failure| failure.kind.persisted_class())
                .unwrap_or("transient"),
            blocker = classified
                .and_then(|failure| failure.kind.blocker())
                .map(Blocker::as_str)
                .unwrap_or("none"),
            "failed to process file"
        );

        // Transient classes are never recorded: the file simply fails this
        // event and is retried untouched.
        let (Some(failure), Some((last_modified, file_size))) = (classified, stat) else {
            return;
        };
        // The session cache goes in *with* the ledger row, so the next event
        // for these same bytes is answered by one stat instead of another full
        // process + upsert + epoch bump. See `failed_stats`.
        self.failed_stats
            .insert(path.to_path_buf(), (last_modified.clone(), file_size));
        let record = ScanErrorRecord {
            path: path.to_string_lossy().to_string(),
            last_modified,
            file_size,
            stage: failure.stage.to_string(),
            kind: failure.kind,
            // Re-guessed rather than threaded back from the worker: it is a
            // pure function of the file name and only runs on the failure
            // path. `None` when the guess is what failed.
            mime_type: infer_mime_type(path).ok(),
            error: failure.message.clone(),
            skip_after: failure.skip_after,
        };
        let scan_id = self.scan_id;
        if let Err(err) = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::UpsertScanError {
                record: record.clone(),
                scan_id,
                reply,
            }
        })
        .await
        {
            tracing::warn!(
                error = ?err,
                path = %path.display(),
                "failed to record a scan failure; it will be re-attempted"
            );
        }
    }

    /// Success path: the file made it through, so its verdict goes away.
    /// Called only when the worker's lookup found a row, so a healthy file
    /// never pays a writer round-trip (or a search-cache epoch bump) for it.
    /// `path` is the *stored* path the worker's lookup returned, which on
    /// Windows need not be the casing the event reported; the delete binds
    /// bytes.
    async fn clear_file_failure(&self, path: String) {
        match call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::DeleteScanError {
                path: path.clone(),
                reply,
            }
        })
        .await
        {
            Ok(cleared) => {
                if cleared > 0 {
                    tracing::info!(path, "cleared a recorded scan failure after a good pass");
                }
            }
            // Advisory: a lost delete costs one wasted re-attempt, never
            // correctness.
            Err(err) => tracing::warn!(error = ?err, path, "failed to clear a scan failure"),
        }
    }

    async fn close_scan(&mut self) -> ApiResult<()> {
        let Some(scan_id) = self.scan_id.take() else {
            return Ok(());
        };
        if self.stats.known_bad > 0 {
            tracing::info!(
                known_bad = self.stats.known_bad,
                "skipped files with an active recorded scan failure"
            );
        }
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
            // `tags_changed: false` — the continuous scan has no owed flags to
            // pass. Its item deletions set the durable marker as they commit
            // (`DeleteItemIfOrphan`), which is what makes the recount run.
            run_post_job_maintenance(&self.index_db, true, false).await;
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

    fn dispatch_path(&mut self, path: PathBuf) {
        if self.paused {
            return;
        }
        if !self.should_process_path(&path) {
            return;
        }
        let Ok(metadata) = std::fs::metadata(&path) else {
            return;
        };
        if !metadata.is_file() {
            return;
        }

        // The session cache, consulted on the stat this function already had
        // to take. A file that failed earlier in this session and has not
        // moved since is not dispatched at all: no worker, no re-hash, no
        // re-upsert, no epoch bump. See `failed_stats`.
        if let Some((failed_mtime, failed_size)) = self.failed_stats.get(&path) {
            let current = metadata
                .modified()
                .ok()
                .and_then(format_system_time)
                .map(|mtime| (mtime, metadata.len() as i64));
            match current {
                Some((mtime, size)) if &mtime == failed_mtime && size == *failed_size => {
                    tracing::debug!(
                        path = %path.display(),
                        "skipping a file that already failed in this session"
                    );
                    self.stats.known_bad += 1;
                    return;
                }
                // Its bytes moved (or the stat is unreadable): the verdict was
                // about different content, so it is re-attempted and the cache
                // entry goes.
                _ => {
                    self.failed_stats.remove(&path);
                }
            }
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
        self.watcher_fallback = false;
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
                // Degrade to polling rather than leaving the DB with no change
                // detection at all. Without this the actor keeps `poller` unset,
                // which both disables continuous scanning silently and latches
                // `needs_restart` on for every later config reload.
                tracing::error!(
                    index_db = %self.index_db,
                    error = ?err,
                    fallback_poll_interval_secs = WATCHER_FALLBACK_POLL_INTERVAL.as_secs(),
                    "failed to start continuous scan watcher; falling back to polling"
                );
                match self
                    .start_poller(Some(WATCHER_FALLBACK_POLL_INTERVAL))
                    .await
                {
                    Ok(()) => self.watcher_fallback = true,
                    Err(err) => tracing::error!(
                        index_db = %self.index_db,
                        error = ?err,
                        "continuous scan fallback poller also failed to start"
                    ),
                }
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
            watcher_fallback: false,
            enable_watcher: args.enable_watcher,
            deletions_since_maintenance: 0,
            failed_stats: HashMap::new(),
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
                path,
                stat,
                ledger_path,
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
                    Err(FileProcessError::KnownBad) => {
                        // Nothing was attempted, so this is not an error of
                        // this run; the file keeps its recorded verdict until
                        // its bytes move.
                        state.stats.known_bad += 1;
                        state.maybe_report_progress().await;
                        return Ok(());
                    }
                    Err(error) => {
                        // Used to be discarded outright — no log line, no path,
                        // no class. Now it is both visible and, when the class
                        // is one the ledger stores, remembered.
                        state.stats.errors += 1;
                        state.record_file_failure(&path, stat, &error).await;
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

                // What that same pass concluded about the kinds it produced
                // nothing for, so the next batch scan does not regenerate the
                // same nothing. Empty for every healthy file, and checked
                // before the writer is touched.
                //
                // This half writes but does not consult, unlike the batch
                // walker. Not because the key is unavailable — `process_file`
                // hashes before it generates — but because of where the
                // generation happens: the whole pass runs inside one
                // synchronous worker with no database handle, so a consult
                // would mean either handing every worker a connection or
                // splitting the pass in two around one. The cost of not doing
                // it is bounded and small: an event fires only when a file's
                // mtime moved, so this is one regeneration per file the user
                // actually touched. Every path that re-attempts visuals for
                // content the index already has goes through the batch scan's
                // `maybe_dispatch_backfill`, which does consult.
                if !file_data.visual_verdicts.is_empty() {
                    let records = crate::jobs::files::visual_attempt_records(
                        &file_data.visual_verdicts,
                        &file_data.sha256,
                        &file_data.mime_type,
                    );
                    // The continuous scan's own long-lived `file_scans` row,
                    // which is what dedups `attempts`. A `skip_after = 2`
                    // verdict written here is therefore normally confirmed by
                    // the next batch scan rather than by a second event —
                    // deliberately, for the same reason as the scan ledger.
                    let scan_id = state.scan_id;
                    if let Err(err) = call_index_db_writer(&state.index_db, |reply| {
                        IndexDbWriterMessage::UpsertVisualAttempts {
                            records: records.clone(),
                            scan_id,
                            reply,
                        }
                    })
                    .await
                    {
                        tracing::warn!(
                            error = ?err,
                            path = %path.display(),
                            "failed to record a visuals attempt; it will be regenerated"
                        );
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
                        // The file made it through, so it owes no verdict.
                        // Gated on the worker's lookup, so the healthy path
                        // pays nothing.
                        state.failed_stats.remove(&path);
                        if let Some(stored) = ledger_path {
                            state.clear_file_failure(stored).await;
                        }
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
                    watching: state.watcher.is_some() || state.poller.is_some(),
                    watcher_fallback: state.watcher_fallback,
                    effective_poll_interval_secs: state
                        .poller
                        .as_ref()
                        .and_then(|poller| poller.interval)
                        .map(|interval| interval.as_secs()),
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
    /// Set by the watcher callback when it has queued a `ResyncFromDisk`, and
    /// cleared when that resync *starts*. The callback drops events while it is
    /// set, so at most one watcher-driven resync is ever in the mailbox — the
    /// mailbox is unbounded, and an event source that outruns the actor would
    /// otherwise grow it without limit. Clearing on entry rather than on
    /// completion is what makes this safe: an event arriving mid-resync queues
    /// the next pass instead of being swallowed.
    resync_pending: Arc<AtomicBool>,
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
        let resync_pending = Arc::new(AtomicBool::new(false));
        let watcher =
            start_supervisor_watcher(myself.clone(), &args.data_dir, resync_pending.clone()).ok();
        let mut state = ContinuousScanSupervisorState {
            data_dir: args.data_dir,
            config_store,
            actors: HashMap::new(),
            watcher,
            resync_pending,
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
                // Re-open the gate before the pass, not after: this resync
                // cannot observe changes made while it is running, so an event
                // arriving now must be able to queue the next one.
                state.resync_pending.store(false, Ordering::Release);
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
            // One unreadable or malformed config.toml must not abort the whole
            // pass: `desired` drives every start/stop below, so propagating
            // here would leave every *other* database unmanaged too, on every
            // resync, forever.
            let config = match state.config_store.load(&index_db) {
                Ok(config) => config,
                Err(err) => {
                    tracing::error!(
                        index_db = %index_db,
                        error = ?err,
                        "failed to load system config; skipping this database"
                    );
                    continue;
                }
            };
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
///
/// Access events (open/read/close-without-write) are never a configuration
/// change, and treating them as one is self-sustaining: `resync_from_disk`
/// reads the very tree being watched — `read_dir` over `data/index` plus a
/// `config.toml` read per DB — and inotify reports those reads back as
/// `IN_OPEN`/`IN_ACCESS`/`IN_CLOSE_NOWRITE`. Since `config.toml` is not a DB
/// file, the name filter above cannot catch them, so each resync scheduled the
/// next one and the supervisor spun at ~3k resyncs/s (issue #18). Only inotify
/// reports reads at all, which is why this never appeared on Windows
/// (ReadDirectoryChangesW) or macOS (FSEvents). Real edits still arrive as
/// Create/Modify/Remove/rename.
fn event_is_relevant(event: &Event) -> bool {
    if matches!(event.kind, EventKind::Access(_)) {
        return false;
    }
    event.paths.is_empty() || event.paths.iter().any(|p| !is_sqlite_db_file(p))
}

/// Queue one debounced `ResyncFromDisk` unless one is already pending.
///
/// Coalesce: queue a resync only when none is already pending, so a burst of
/// events (or a misclassified one) can never grow the mailbox. The delay lets
/// a multi-event save settle into a single pass. Deliberately not a filter of
/// last resort — the gate bounds the damage of any event we fail to classify.
///
/// This runs on notify's own event thread, which has no ambient tokio runtime
/// — `ActorRef::send_after` is `tokio::spawn` under the hood and would panic
/// there, killing the watcher thread. The delayed send is therefore spawned
/// onto an explicitly captured runtime handle, which is safe from any thread.
fn schedule_supervisor_resync(
    runtime: &tokio::runtime::Handle,
    actor: &ActorRef<ContinuousScanSupervisorMessage>,
    resync_pending: &AtomicBool,
) {
    if resync_pending.swap(true, Ordering::AcqRel) {
        return;
    }
    let actor = actor.clone();
    runtime.spawn(async move {
        tokio::time::sleep(SUPERVISOR_RESYNC_DEBOUNCE).await;
        let _ = actor.cast(ContinuousScanSupervisorMessage::ResyncFromDisk);
    });
}

fn start_supervisor_watcher(
    actor: ActorRef<ContinuousScanSupervisorMessage>,
    data_dir: &Path,
    resync_pending: Arc<AtomicBool>,
) -> Result<RecommendedWatcher, notify::Error> {
    let watch_root = data_dir.join("index");
    let _ = std::fs::create_dir_all(&watch_root);
    // Captured inside the runtime (pre_start); the callback runs outside it.
    let runtime = tokio::runtime::Handle::current();
    let mut watcher = RecommendedWatcher::new(
        move |res| match res {
            Ok(event) => {
                // Ignore the scanner's own DB writes; real config saves also
                // arrive out-of-band via notify_config_change, so this watcher
                // is only a backstop for on-disk edits and DB dir changes.
                if event_is_relevant(&event) {
                    schedule_supervisor_resync(&runtime, &actor, &resync_pending);
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
                path: file_path.clone(),
                stat: None,
                ledger_path: None,
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

    // The continuous scan's own visuals-marker write. It deliberately never
    // *consults* the negative cache (the pass runs in a worker with no database
    // handle), but it must still record what it concluded — otherwise a file
    // the user touched arrives in the index with a nothing that the next batch
    // scan has to rediscover the hard way.
    #[tokio::test]
    async fn the_continuous_scan_records_visual_attempts() {
        let test_env = test_data_dir();
        let root = test_env.path().to_path_buf();
        let index_db = unique_db_name("cont-visuals");
        migrate_databases_on_disk(Some(&index_db), Some(&index_db))
            .await
            .unwrap();

        let watch_dir = root.join("watch-visuals");
        fs::create_dir_all(&watch_dir).unwrap();
        // A PDF nothing can render: `blocked` where pdfium is missing, `failed`
        // where it is present — either way a verdict the pass owes a marker
        // for, with no toolchain to depend on.
        let file_path = watch_dir.join("broken.pdf");
        fs::write(&file_path, b"%PDF-1.7\nnothing in here parses\n").unwrap();

        let store = SystemConfigStore::new(root.clone());
        let mut config = store.load(&index_db).unwrap();
        config.continuous_filescan.enabled = true;
        config.scan_pdf = true;
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
        assert!(
            !prepared.visual_verdicts.is_empty(),
            "the premise: this file's visuals are a verdict, not a success"
        );
        actor
            .cast(ContinuousScanMessage::WorkerResult {
                epoch: 0,
                scan_time: current_iso_timestamp(),
                path: file_path.clone(),
                stat: None,
                ledger_path: None,
                result: Ok(prepared),
            })
            .unwrap();

        let mut markers = 0;
        for _ in 0..40 {
            let mut conn = crate::db::open_index_db_read_no_user_data(&index_db)
                .await
                .unwrap();
            markers = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM storage.visual_attempts WHERE kind = 'thumbnail'",
            )
            .fetch_one(&mut conn)
            .await
            .unwrap();
            if markers > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        actor.stop(None);
        assert_eq!(markers, 1, "the pass's conclusion must reach the cache");
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
                path: file_path.clone(),
                stat: None,
                ledger_path: None,
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

        // A running poller reports itself as watching, at its real interval,
        // and not as a degraded fallback from the native watcher.
        let (tx, rx) = oneshot::channel();
        actor
            .cast(ContinuousScanMessage::GetStatus { reply: tx })
            .unwrap();
        let snapshot = rx.await.unwrap();
        assert!(snapshot.watching);
        assert!(!snapshot.watcher_fallback);
        assert_eq!(snapshot.effective_poll_interval_secs, Some(1));

        actor.stop(None);
        assert!(found, "poll mode did not index the new file in time");
    }

    // The continuous scan used to throw the error variant away entirely — the
    // counter moved and nothing said which file or why, so a broken file was
    // re-hashed on every watcher event forever. It now records the verdict and
    // consults it before dispatching the next event for the same path.
    #[tokio::test]
    async fn continuous_scan_records_and_then_skips_a_broken_file() {
        let test_env = test_data_dir();
        let root = test_env.path().to_path_buf();
        let index_db = unique_db_name("contledger");
        let _ = migrate_databases_on_disk(Some(&index_db), Some(&index_db))
            .await
            .unwrap();

        let watch_dir = root.join("ledgerwatch");
        std::fs::create_dir_all(&watch_dir).unwrap();
        let broken = watch_dir.join("broken.png");
        std::fs::write(&broken, b"this claims to be a png and is not").unwrap();

        let store = SystemConfigStore::new(root.clone());
        let mut config = store.load(&index_db).unwrap();
        config.continuous_filescan.enabled = true;
        config.included_folders = vec![watch_dir.to_string_lossy().to_string()];
        store.save(&index_db, &config).unwrap();

        let (actor, handle) = Actor::spawn(
            None,
            ContinuousScanActor,
            ContinuousScanActorArgs {
                index_db: index_db.clone(),
                user_data_db: index_db.clone(),
                data_dir: root.clone(),
                // No watcher and no poller: the events under test are cast by
                // hand, so nothing else can dispatch this path.
                enable_watcher: false,
            },
        )
        .await
        .unwrap();

        async fn row(index_db: &str) -> Option<(String, String, i64, i64)> {
            let mut conn = crate::db::open_index_db_read_no_user_data(index_db)
                .await
                .unwrap();
            sqlx::query_as("SELECT stage, error_class, attempts, skip_after FROM scan_errors")
                .fetch_optional(&mut conn)
                .await
                .unwrap()
        }

        actor
            .cast(ContinuousScanMessage::DispatchStable {
                epoch: 0,
                path: broken.clone(),
            })
            .unwrap();

        let mut recorded = None;
        for _ in 0..120 {
            if let Some(found) = row(&index_db).await {
                recorded = Some(found);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(
            recorded,
            Some(("decode".to_string(), "input".to_string(), 1, 2)),
            "an undecodable image is an unconfirmed input verdict"
        );

        // A second event for the same unchanged file must not re-process it.
        //
        // The ledger cannot do this on its own here: the verdict is still
        // unconfirmed (one attempt of two), and `attempts` dedups on the scan
        // id, which for a continuous scan spans the whole session — so without
        // the session cache this event would re-hash, re-decode and re-upsert
        // the file, bumping the search-cache epoch, and would do it again for
        // every event after that. The recorded row is unchanged either way;
        // the scan's own error counter, asserted after the stop below, is what
        // distinguishes "suppressed" from "silently redone".
        actor
            .cast(ContinuousScanMessage::DispatchStable {
                epoch: 0,
                path: broken.clone(),
            })
            .unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            row(&index_db).await,
            Some(("decode".to_string(), "input".to_string(), 1, 2)),
            "a suppressed file is not re-attempted"
        );

        // Repairing it clears the verdict and indexes the file.
        write_test_image(&broken);
        actor
            .cast(ContinuousScanMessage::DispatchStable {
                epoch: 0,
                path: broken.clone(),
            })
            .unwrap();
        let mut cleared = false;
        for _ in 0..120 {
            if row(&index_db).await.is_none() {
                cleared = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        actor.stop(None);
        let _ = handle.await;
        assert!(cleared, "a file that works owes no verdict");

        // Stopping closes the scan record, which is where the counters land.
        // Exactly one failure was ever *processed*: the second event was
        // answered from the session cache with a single stat.
        let mut conn = crate::db::open_index_db_read_no_user_data(&index_db)
            .await
            .unwrap();
        let errors: i64 = sqlx::query_scalar(
            "SELECT errors FROM file_scans WHERE path = ? ORDER BY id DESC LIMIT 1",
        )
        .bind(CONTINUOUS_PATH_SENTINEL)
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(
            errors, 1,
            "the second event must be suppressed, not re-processed"
        );
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
        // Change detection is off here (`enable_watcher: false`), and the
        // snapshot says so rather than letting an unpaused actor read as
        // healthy — which is what hid a failed watcher from the status page.
        assert!(!snapshot.watching);
        assert!(!snapshot.watcher_fallback);

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

    /// Regression test for issue #18: `resync_from_disk` reads the watched
    /// tree, inotify reports those reads back as access events, and treating
    /// them as config changes made every resync schedule the next one.
    #[test]
    fn supervisor_watcher_skips_access_events() {
        use notify::event::{AccessKind, AccessMode, DataChange};

        let cfg = std::path::Path::new("data/index/mydb/config.toml").to_path_buf();
        let index_dir = std::path::Path::new("data/index").to_path_buf();

        // Opening and reading config.toml is what resync_from_disk itself
        // does; neither may schedule another resync.
        assert!(!event_is_relevant(
            &Event::new(EventKind::Access(AccessKind::Open(AccessMode::Read)))
                .add_path(cfg.clone())
        ));
        assert!(!event_is_relevant(
            &Event::new(EventKind::Access(AccessKind::Read)).add_path(cfg.clone())
        ));
        assert!(!event_is_relevant(
            &Event::new(EventKind::Access(AccessKind::Close(AccessMode::Read)))
                .add_path(cfg.clone())
        ));
        // read_dir over the index root reports against the directory itself.
        assert!(!event_is_relevant(
            &Event::new(EventKind::Access(AccessKind::Open(AccessMode::Any))).add_path(index_dir)
        ));
        // An actual edit is still a Modify, and still counts.
        assert!(event_is_relevant(
            &Event::new(EventKind::Modify(ModifyKind::Data(DataChange::Any))).add_path(cfg)
        ));
    }

    /// Counts `ResyncFromDisk` deliveries so the scheduling path can be
    /// observed without a full supervisor (whose resync has no visible effect
    /// on an empty data dir).
    struct ResyncProbe;

    impl Actor for ResyncProbe {
        type Msg = ContinuousScanSupervisorMessage;
        type State = Arc<AtomicU64>;
        type Arguments = Arc<AtomicU64>;

        async fn pre_start(
            &self,
            _myself: ActorRef<Self::Msg>,
            args: Self::Arguments,
        ) -> Result<Self::State, ActorProcessingErr> {
            Ok(args)
        }

        async fn handle(
            &self,
            _myself: ActorRef<Self::Msg>,
            message: Self::Msg,
            state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            if matches!(message, ContinuousScanSupervisorMessage::ResyncFromDisk) {
                state.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        }
    }

    /// notify invokes its event handler on a thread of its own, with no
    /// ambient tokio runtime. Scheduling a resync from such a thread must
    /// neither panic (as `ActorRef::send_after` = `tokio::spawn` would) nor
    /// drop the message, and the pending-gate must still hold follow-up
    /// requests back.
    #[tokio::test]
    async fn supervisor_resync_schedules_from_non_runtime_thread() {
        let resyncs = Arc::new(AtomicU64::new(0));
        let (actor, _handle) = Actor::spawn(None, ResyncProbe, resyncs.clone())
            .await
            .unwrap();
        let runtime = tokio::runtime::Handle::current();
        let gate = Arc::new(AtomicBool::new(false));

        let events = std::thread::spawn({
            let (runtime, actor, gate) = (runtime.clone(), actor.clone(), gate.clone());
            move || {
                schedule_supervisor_resync(&runtime, &actor, &gate);
                // A second event during the debounce window is coalesced.
                schedule_supervisor_resync(&runtime, &actor, &gate);
            }
        });
        events
            .join()
            .expect("scheduling a resync panicked on a non-runtime thread");

        let deadline = Instant::now() + Duration::from_secs(10);
        while resyncs.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            resyncs.load(Ordering::SeqCst),
            1,
            "exactly one gated resync must be delivered"
        );
        assert!(gate.load(Ordering::SeqCst), "probe never re-opens the gate");
        actor.stop(None);
    }
}
