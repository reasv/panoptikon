use std::{
    collections::{HashMap, HashSet},
    env, fs,
    io::{self, Read},
    path::{Component, Path, PathBuf},
    process::Command,
    sync::{
        Arc, Condvar, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use ab_glyph::{FontVec, PxScale};
use blurhash::encode as blurhash_encode;
use image::ColorType;
use image::codecs::jpeg::JpegEncoder;
use image::{DynamicImage, GenericImageView, Rgb, RgbImage};
use imageproc::drawing::{draw_text_mut, text_size};
use lofty::prelude::{Accessor, TaggedFileExt};
use md5::{Digest, Md5};
use mime_guess::MimeGuess;
use pdfium_render::prelude::{PdfRenderConfig, Pdfium};
use serde::Deserialize;
use sha2::Sha256;
use time::{OffsetDateTime, format_description::FormatItem};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use walkdir::WalkDir;

use crate::{
    api_error::ApiError,
    db::{
        file_scans::{FileScanUpdate, get_completed_scan_paths, get_open_file_scan_id},
        files::{
            FileScanData, FileUpsertResult, ItemScanMeta, get_file_by_path, get_item_id,
            get_item_dimensions, get_item_visual_meta, has_blurhash,
        },
        folders::get_folders_from_database,
        index_writer::{IndexDbWriterMessage, call_index_db_writer},
        open_index_db_read,
        storage::{StoredImage, get_frames_bytes, get_thumbnail_bytes, has_frame, has_thumbnail},
        system_config::{SystemConfig, SystemConfigStore},
    },
    jobs::timing::PhaseTimer,
    pql::builder::filters::evaluate_match,
    pql::model::{Match, MatchValue},
};

type ApiResult<T> = std::result::Result<T, ApiError>;

#[derive(Clone, Copy)]
pub(crate) struct ScanOptions {
    pub worker_count: usize,
}

impl Default for ScanOptions {
    fn default() -> Self {
        let worker_count = std::thread::available_parallelism()
            .map(|count| count.get())
            .unwrap_or(4);
        Self { worker_count }
    }
}

pub(crate) struct RescanResult {
    // Only read by tests; production callers ignore the result.
    #[allow(dead_code)]
    pub scan_ids: Vec<i64>,
}

pub(crate) struct FolderUpdateResult {
    // Only read by tests; production callers ignore the result.
    #[allow(dead_code)]
    pub included_added: Vec<String>,
    #[allow(dead_code)]
    pub scan_ids: Vec<i64>,
}

pub(crate) struct FileScanService {
    index_db: String,
    user_data_db: String,
    config_store: SystemConfigStore,
    options: ScanOptions,
}

impl FileScanService {
    pub(crate) fn from_env(index_db: impl Into<String>, user_data_db: impl Into<String>) -> Self {
        Self {
            index_db: index_db.into(),
            user_data_db: user_data_db.into(),
            config_store: SystemConfigStore::from_env(),
            options: ScanOptions::default(),
        }
    }

    // Only used by tests, which need an explicit data_dir and worker count.
    #[allow(dead_code)]
    pub(crate) fn new(
        index_db: impl Into<String>,
        user_data_db: impl Into<String>,
        data_dir: PathBuf,
        options: ScanOptions,
    ) -> Self {
        Self {
            index_db: index_db.into(),
            user_data_db: user_data_db.into(),
            config_store: SystemConfigStore::new(data_dir),
            options,
        }
    }

    pub(crate) async fn rescan_folders(&self) -> ApiResult<RescanResult> {
        let config = self.config_store.load(&self.index_db)?;
        if is_resync_needed(&self.index_db, &self.user_data_db, &config).await? {
            let _ = self.run_folder_update().await?;
        }

        let mut conn = open_index_db_read(&self.index_db, &self.user_data_db).await?;
        let included_folders = get_folders_from_database(&mut conn, true).await?;
        let excluded_folders = get_folders_from_database(&mut conn, false).await?;
        drop(conn);

        let scan_ids = execute_folder_scan(
            &self.index_db,
            &self.user_data_db,
            &config,
            &included_folders,
            &excluded_folders,
            self.options,
        )
        .await?;

        let unavailable_files_deleted = if config.remove_unavailable_files {
            call_index_db_writer(&self.index_db, |reply| {
                IndexDbWriterMessage::DeleteUnavailableFiles { reply }
            })
            .await?
        } else {
            0
        };
        let rule_files_deleted = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::DeleteFilesNotAllowed {
                job_filters: config.job_filters.clone(),
                reply,
            }
        })
        .await?;
        let orphan_items_deleted = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::DeleteItemsWithoutFiles {
                batch_size: 10_000,
                reply,
            }
        })
        .await?;
        let orphan_frames_deleted = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::DeleteOrphanedFrames { reply }
        })
        .await?;
        let orphan_thumbnails_deleted = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::DeleteOrphanedThumbnails { reply }
        })
        .await?;

        let vacuum = unavailable_files_deleted > 0
            || rule_files_deleted > 0
            || orphan_items_deleted > 0
            || orphan_frames_deleted > 0
            || orphan_thumbnails_deleted > 0;
        run_post_job_maintenance(&self.index_db, vacuum).await;

        Ok(RescanResult { scan_ids })
    }

    pub(crate) async fn run_folder_update(&self) -> ApiResult<FolderUpdateResult> {
        let config = self.config_store.load(&self.index_db)?;
        self.config_store.save(&self.index_db, &config)?;

        call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::DeleteFoldersNotInList {
                folder_paths: config.included_folders.clone(),
                included: true,
                reply,
            }
        })
        .await?;
        call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::DeleteFoldersNotInList {
                folder_paths: config.excluded_folders.clone(),
                included: false,
                reply,
            }
        })
        .await?;

        // Folders are registered before scanning so that file rows can never
        // be inserted for a folder missing from the folders table.
        let scan_time = current_iso_timestamp();
        let mut included_added = Vec::new();
        for folder in &config.included_folders {
            let inserted = call_index_db_writer(&self.index_db, |reply| {
                IndexDbWriterMessage::AddFolderToDatabase {
                    time_added: scan_time.clone(),
                    path: folder.clone(),
                    included: true,
                    reply,
                }
            })
            .await?;
            if inserted {
                included_added.push(folder.clone());
            }
        }
        for folder in &config.excluded_folders {
            call_index_db_writer(&self.index_db, |reply| {
                IndexDbWriterMessage::AddFolderToDatabase {
                    time_added: scan_time.clone(),
                    path: folder.clone(),
                    included: false,
                    reply,
                }
            })
            .await?;
        }

        // Folder registration and scanning are separate committed writes, so
        // a folder update that failed mid-scan leaves folders registered but
        // never scanned — and re-running the update would skip them, since
        // INSERT OR IGNORE no longer reports them as new. Pick up any
        // included folder not yet covered by a completed scan (its own or an
        // ancestor's, since nested folders are scanned via their parent).
        {
            let mut conn = open_index_db_read(&self.index_db, &self.user_data_db).await?;
            let registered = get_folders_from_database(&mut conn, true).await?;
            let completed = get_completed_scan_paths(&mut conn).await?;
            drop(conn);
            let completed_roots: Vec<PathBuf> = completed
                .iter()
                .map(|scan_path| normalize_path(scan_path, false))
                .collect();
            for folder in registered {
                let normalized = normalize_path(&folder, false);
                let covered = completed_roots
                    .iter()
                    .any(|root| normalized.starts_with(root));
                if !covered && !included_added.contains(&folder) {
                    tracing::info!(
                        folder,
                        "included folder has no completed scan; scheduling scan"
                    );
                    included_added.push(folder);
                }
            }
        }

        let scan_ids = execute_folder_scan(
            &self.index_db,
            &self.user_data_db,
            &config,
            &included_added,
            &config.excluded_folders,
            self.options,
        )
        .await?;

        let unavailable_files_deleted = if config.remove_unavailable_files {
            call_index_db_writer(&self.index_db, |reply| {
                IndexDbWriterMessage::DeleteUnavailableFiles { reply }
            })
            .await?
        } else {
            0
        };
        let excluded_folder_files_deleted = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::DeleteFilesUnderExcludedFolders { reply }
        })
        .await?;
        let orphan_files_deleted = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::DeleteFilesNotUnderIncludedFolders { reply }
        })
        .await?;
        let rule_files_deleted = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::DeleteFilesNotAllowed {
                job_filters: config.job_filters.clone(),
                reply,
            }
        })
        .await?;
        let orphan_items_deleted = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::DeleteItemsWithoutFiles {
                batch_size: 10_000,
                reply,
            }
        })
        .await?;
        let orphan_frames_deleted = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::DeleteOrphanedFrames { reply }
        })
        .await?;
        let orphan_thumbnails_deleted = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::DeleteOrphanedThumbnails { reply }
        })
        .await?;

        let vacuum = unavailable_files_deleted > 0
            || excluded_folder_files_deleted > 0
            || orphan_files_deleted > 0
            || rule_files_deleted > 0
            || orphan_items_deleted > 0
            || orphan_frames_deleted > 0
            || orphan_thumbnails_deleted > 0;
        run_post_job_maintenance(&self.index_db, vacuum).await;

        Ok(FolderUpdateResult {
            included_added,
            scan_ids,
        })
    }
}

pub(crate) async fn is_resync_needed(
    index_db: &str,
    user_data_db: &str,
    config: &SystemConfig,
) -> ApiResult<bool> {
    let mut conn = open_index_db_read(index_db, user_data_db).await?;
    let mut current_included = get_folders_from_database(&mut conn, true).await?;
    let mut current_excluded = get_folders_from_database(&mut conn, false).await?;
    current_included.sort();
    current_excluded.sort();
    let mut new_included = config.included_folders.clone();
    let mut new_excluded = config.excluded_folders.clone();
    new_included.sort();
    new_excluded.sort();

    Ok(current_included != new_included || current_excluded != new_excluded)
}

/// Post-job VACUUM/ANALYZE. Failures are logged but never fail the job: the
/// job's own work has already been committed at this point.
pub(crate) async fn run_post_job_maintenance(index_db: &str, vacuum: bool) {
    if vacuum {
        if let Err(err) =
            call_index_db_writer(index_db, |reply| IndexDbWriterMessage::Vacuum { reply }).await
        {
            tracing::error!(error = ?err, index_db, "failed to vacuum index database");
        }
    }
    if let Err(err) =
        call_index_db_writer(index_db, |reply| IndexDbWriterMessage::Analyze { reply }).await
    {
        tracing::error!(error = ?err, index_db, "failed to analyze index database");
    }
}

async fn execute_folder_scan(
    index_db: &str,
    user_data_db: &str,
    config: &SystemConfig,
    included_folders: &[String],
    excluded_folders: &[String],
    options: ScanOptions,
) -> ApiResult<Vec<i64>> {
    let mut conn = open_index_db_read(index_db, user_data_db).await?;
    let mut all_included = Vec::new();
    for folder in included_folders {
        if check_folder_validity(folder)
            || (folder_is_empty(folder)
                && !crate::db::setup::has_indexed_files_under(&mut conn, folder).await?)
        {
            all_included.push(folder.clone());
        } else if folder_is_empty(folder) {
            tracing::warn!(
                folder,
                "empty folder still has indexed files; skipping to protect indexed entries"
            );
        }
    }
    let starting_points = deduplicate_paths(&all_included);

    // Scans interrupted before completion leave rows with a NULL end_time;
    // close them so they are not reported as still running.
    for folder in &starting_points {
        while let Some(stale_scan_id) = get_open_file_scan_id(&mut conn, folder).await? {
            call_index_db_writer(index_db, |reply| IndexDbWriterMessage::CloseFileScan {
                scan_id: stale_scan_id,
                end_time: current_iso_timestamp(),
                reply,
            })
            .await?;
        }
    }
    drop(conn);

    let scan_time = current_iso_timestamp();
    let mut scan_ids = Vec::new();

    for folder in starting_points {
        let scan_id = call_index_db_writer(index_db, |reply| IndexDbWriterMessage::AddFileScan {
            scan_time: scan_time.clone(),
            path: folder.clone(),
            reply,
        })
        .await?;
        scan_ids.push(scan_id);

        let excluded_paths = excluded_folders
            .iter()
            .map(|folder| normalize_path(folder, true))
            .collect::<Vec<_>>();

        let stats = scan_single_folder(
            index_db,
            user_data_db,
            config,
            &folder,
            &excluded_paths,
            scan_id,
            &scan_time,
            options,
        )
        .await?;

        call_index_db_writer(index_db, |reply| IndexDbWriterMessage::UpdateFileScan {
            scan_id,
            update: FileScanUpdate {
                end_time: Some(current_iso_timestamp()),
                new_items: stats.new_items,
                unchanged_files: stats.unchanged_files,
                new_files: stats.new_files,
                modified_files: stats.modified_files,
                marked_unavailable: stats.marked_unavailable,
                errors: stats.errors,
                total_available: stats.total_available,
                false_changes: stats.false_changes,
                metadata_time: stats.metadata_time,
                hashing_time: stats.hashing_time,
                thumbgen_time: stats.thumbgen_time,
                blurhash_time: stats.blurhash_time,
            },
            reply,
        })
        .await?;
    }

    Ok(scan_ids)
}

pub(crate) const THUMBNAIL_PROCESS_VERSION: i64 = 1;
pub(crate) const FRAME_PROCESS_VERSION: i64 = 1;
/// Minimum interval between mid-scan writes of the running counters to the
/// file_scans row (progress display only; the final update is unconditional).
pub(crate) const SCAN_PROGRESS_INTERVAL: Duration = Duration::from_secs(1);
/// Images at or below this size never get a stored thumbnail.
const SMALL_IMAGE_FILE_SIZE: u64 = 5 * 1024 * 1024;
/// Images within this pixel size are served from the original file.
const MAX_SERVED_IMAGE_DIMENSION: i64 = 4096;
/// Images above this file size get a thumbnail even when their pixel
/// dimensions are modest.
const MAX_SERVED_IMAGE_FILE_SIZE: u64 = 24 * 1024 * 1024;

struct FolderStats {
    new_items: i64,
    unchanged_files: i64,
    new_files: i64,
    modified_files: i64,
    marked_unavailable: i64,
    errors: i64,
    total_available: i64,
    false_changes: i64,
    metadata_time: f64,
    hashing_time: f64,
    thumbgen_time: f64,
    blurhash_time: f64,
}

/// One [`PhaseTimer`] per timed scan phase. The stored per-scan times are the
/// timers' busy (wall-clock union) totals, so they stay comparable to the
/// scan duration regardless of worker count; aggregate worker time is only
/// logged.
#[derive(Clone, Default)]
pub(crate) struct ScanTimers {
    pub(crate) metadata: PhaseTimer,
    pub(crate) hashing: PhaseTimer,
    pub(crate) thumbgen: PhaseTimer,
    pub(crate) blurhash: PhaseTimer,
}

impl FolderStats {
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
            metadata_time: 0.0,
            hashing_time: 0.0,
            thumbgen_time: 0.0,
            blurhash_time: 0.0,
        }
    }
}

struct HashedFile {
    path: PathBuf,
    last_modified: String,
    reported_size: i64,
    mime_type: String,
    existing_sha256: Option<String>,
    md5: String,
    sha256: String,
    real_size: i64,
}

struct NewItemData {
    path: PathBuf,
    last_modified: String,
    file_size: i64,
    sha256: String,
    mime_type: String,
    metadata: ItemScanMeta,
    thumbnails: Vec<StoredImage>,
    frames: Vec<StoredImage>,
    blurhash: Option<String>,
}

struct BackfillResult {
    sha256: String,
    mime_type: String,
    thumbnails: Vec<StoredImage>,
    extracted_frames: Vec<StoredImage>,
    blurhash: Option<String>,
}

struct FailedFile {
    path: PathBuf,
    error: FileProcessError,
}

enum TaskOutcome {
    Hashed(HashedFile),
    NewItem(NewItemData),
    Backfill(BackfillResult),
    Failed(FailedFile),
}

struct ScanContext {
    index_db: String,
    scan_id: i64,
    scan_time: String,
    filescan_filter: Option<Arc<Match>>,
    semaphore: Arc<Semaphore>,
    tasks: JoinSet<TaskOutcome>,
    // Path (and whether the task is a visuals backfill) per in-flight task, so
    // a task that dies without producing an outcome can still be accounted to
    // its file.
    task_paths: HashMap<tokio::task::Id, TrackedTask>,
    // Content hashes with an in-flight visuals task. Visuals are keyed by
    // sha256, so a second file with identical content would regenerate (and
    // then fail to store) the exact same data.
    in_flight_visuals: HashSet<String>,
    stats: FolderStats,
    timers: ScanTimers,
    last_progress: Instant,
    error_paths: Vec<String>,
    conn: sqlx::SqliteConnection,
}

struct TrackedTask {
    path: String,
    // Some(sha256) when the task is a visuals backfill, None for file
    // processing (hash / new-item preparation).
    backfill_sha256: Option<String>,
}

async fn scan_single_folder(
    index_db: &str,
    user_data_db: &str,
    config: &SystemConfig,
    folder: &str,
    excluded_paths: &[PathBuf],
    scan_id: i64,
    scan_time: &str,
    options: ScanOptions,
) -> ApiResult<FolderStats> {
    let allowed_extensions = build_extension_set(config);
    let conn = open_index_db_read(index_db, user_data_db).await?;
    let mut ctx = ScanContext {
        index_db: index_db.to_string(),
        scan_id,
        scan_time: scan_time.to_string(),
        filescan_filter: parse_filescan_filter(config).map(Arc::new),
        semaphore: Arc::new(Semaphore::new(options.worker_count)),
        tasks: JoinSet::new(),
        task_paths: HashMap::new(),
        in_flight_visuals: HashSet::new(),
        stats: FolderStats::new(),
        timers: ScanTimers::default(),
        last_progress: Instant::now(),
        error_paths: Vec::new(),
        conn,
    };

    for entry in WalkDir::new(folder)
        .follow_links(true)
        .into_iter()
        .filter_entry(|entry| !is_excluded(entry.path(), excluded_paths))
    {
        // Drain finished work before taking on more, so completed results
        // are persisted as the walk progresses instead of piling up in memory.
        while let Some(joined) = ctx.tasks.try_join_next_with_id() {
            ctx.handle_joined(joined).await?;
        }

        let entry = match entry {
            Ok(entry) => entry,
            Err(err) => {
                tracing::error!(error = %err, "error walking directory");
                continue;
            }
        };
        if entry.file_type().is_dir() {
            continue;
        }

        let path = entry.path().to_path_buf();
        if is_hidden_or_temp(&path) {
            continue;
        }

        if !has_allowed_extension(&path, &allowed_extensions) {
            continue;
        }

        ctx.scan_path(path).await?;
        ctx.maybe_report_progress().await;
    }

    while let Some(joined) = ctx.tasks.join_next_with_id().await {
        ctx.handle_joined(joined).await?;
        ctx.maybe_report_progress().await;
    }

    let ScanContext {
        mut stats,
        timers,
        error_paths,
        ..
    } = ctx;

    let (marked_unavailable, total_available) = call_index_db_writer(index_db, |reply| {
        IndexDbWriterMessage::MarkUnavailableFiles {
            scan_id,
            path: folder.to_string(),
            excluded_paths: error_paths.clone(),
            reply,
        }
    })
    .await?;
    stats.marked_unavailable = marked_unavailable;
    stats.total_available = total_available;

    // Stored times are phase wall-clock (busy); aggregate worker time only
    // goes to the log, where work / busy reads as average parallelism.
    stats.metadata_time = timers.metadata.busy_secs();
    stats.hashing_time = timers.hashing.busy_secs();
    stats.thumbgen_time = timers.thumbgen.busy_secs();
    stats.blurhash_time = timers.blurhash.busy_secs();
    tracing::info!(
        folder,
        hashing_busy_secs = stats.hashing_time,
        hashing_work_secs = timers.hashing.work_secs(),
        metadata_busy_secs = stats.metadata_time,
        metadata_work_secs = timers.metadata.work_secs(),
        thumbgen_busy_secs = stats.thumbgen_time,
        thumbgen_work_secs = timers.thumbgen.work_secs(),
        blurhash_busy_secs = stats.blurhash_time,
        blurhash_work_secs = timers.blurhash.work_secs(),
        "file scan phase timing"
    );

    Ok(stats)
}

impl ScanContext {
    /// Throttled mid-scan write of the running counters so the UI shows
    /// progress while a folder scans. end_time stays NULL — that is what
    /// marks the scan as still open. Write failures are ignored: progress
    /// rows are cosmetic and must not abort the scan.
    async fn maybe_report_progress(&mut self) {
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
        let scan_id = self.scan_id;
        let _ = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::UpdateFileScan {
                scan_id,
                update: update.clone(),
                reply,
            }
        })
        .await;
    }

    /// Handles one candidate path from the walk: files whose mtime matches the
    /// database record are updated directly without hashing or decoding;
    /// everything else is dispatched to the worker pool.
    async fn scan_path(&mut self, path: PathBuf) -> ApiResult<()> {
        let (last_modified, file_size) = match get_last_modified_time_and_size(&path) {
            Ok(value) => value,
            Err(err) => {
                tracing::info!(error = %err, path = %path.display(), "failed to stat file");
                self.stats.errors += 1;
                self.error_paths.push(path.to_string_lossy().to_string());
                return Ok(());
            }
        };
        let mime_type = match infer_mime_type(&path) {
            Ok(mime) => mime,
            Err(_) => {
                tracing::error!(path = %path.display(), "could not determine mime type");
                self.stats.errors += 1;
                self.error_paths.push(path.to_string_lossy().to_string());
                return Ok(());
            }
        };
        if !passes_filescan_filter_stage1(
            self.filescan_filter.as_deref(),
            &path,
            &last_modified,
            file_size,
            &mime_type,
        ) {
            tracing::debug!(
                path = %path.display(),
                "file does not match the filescan filter (stage 1), skipping"
            );
            self.stats.errors += 1;
            return Ok(());
        }

        let path_str = path.to_string_lossy().to_string();
        let existing = get_file_by_path(&mut self.conn, &path_str).await?;

        if let Some(existing) = &existing {
            if existing.last_modified == last_modified {
                let sha256 = existing.sha256.clone();
                let data = FileScanData {
                    sha256: sha256.clone(),
                    last_modified: existing.last_modified.clone(),
                    path: path_str,
                    new_file_hash: false,
                    file_size: None,
                    item_metadata: None,
                    blurhash: None,
                };
                let result = self.update_file_data(data).await?;
                self.tally(&result);
                return self.maybe_dispatch_backfill(sha256, mime_type, path).await;
            }
        }

        self.dispatch_hash(
            path,
            last_modified,
            file_size,
            mime_type,
            existing.map(|record| record.sha256),
        )
        .await
    }

    async fn handle_joined(
        &mut self,
        joined: Result<(tokio::task::Id, TaskOutcome), tokio::task::JoinError>,
    ) -> ApiResult<()> {
        match joined {
            Ok((id, outcome)) => {
                self.task_paths.remove(&id);
                self.handle_outcome(outcome).await
            }
            Err(err) => {
                match self.task_paths.remove(&err.id()) {
                    // Backfill failures are not scan errors: the file itself
                    // was already recorded, only its visuals are missing, and
                    // the next scan retries them (matching Python, which
                    // catches ensure_thumbnail/blurhash errors per file).
                    Some(TrackedTask {
                        path,
                        backfill_sha256: Some(sha256),
                    }) => {
                        self.in_flight_visuals.remove(&sha256);
                        tracing::error!(error = %err, path, "visuals backfill task failed");
                    }
                    // A file whose task died without an outcome must be kept
                    // out of unavailable-marking, or cleanup could delete a
                    // file that is still on disk.
                    Some(TrackedTask {
                        path,
                        backfill_sha256: None,
                    }) => {
                        tracing::error!(error = %err, path, "file processing task failed");
                        self.stats.errors += 1;
                        self.error_paths.push(path);
                    }
                    None => {
                        tracing::error!(error = %err, "file processing task failed");
                        self.stats.errors += 1;
                    }
                }
                Ok(())
            }
        }
    }

    async fn handle_outcome(&mut self, outcome: TaskOutcome) -> ApiResult<()> {
        match outcome {
            TaskOutcome::Hashed(hashed) => self.handle_hashed(hashed).await,
            TaskOutcome::NewItem(item) => self.handle_new_item(item).await,
            TaskOutcome::Backfill(backfill) => {
                self.handle_backfill(backfill).await;
                Ok(())
            }
            TaskOutcome::Failed(failed) => {
                self.stats.errors += 1;
                match &failed.error {
                    FileProcessError::Filtered => {
                        tracing::debug!(
                            path = %failed.path.display(),
                            "file does not match the filescan filter (stage 2), skipping"
                        );
                    }
                    error => {
                        tracing::error!(
                            error = ?error,
                            path = %failed.path.display(),
                            "failed to process file"
                        );
                        self.error_paths
                            .push(failed.path.to_string_lossy().to_string());
                    }
                }
                Ok(())
            }
        }
    }

    async fn handle_hashed(&mut self, hashed: HashedFile) -> ApiResult<()> {
        let HashedFile {
            path,
            last_modified,
            reported_size,
            mime_type,
            existing_sha256,
            md5,
            sha256,
            real_size,
        } = hashed;
        if real_size != reported_size {
            tracing::warn!(path = %path.display(), real_size, reported_size, "file size mismatch");
        }
        let path_str = path.to_string_lossy().to_string();

        if existing_sha256.as_deref() == Some(sha256.as_str()) {
            // The timestamp changed but the contents did not.
            tracing::warn!(path = %path.display(), "file has a new timestamp but the same hash");
            let data = FileScanData {
                sha256: sha256.clone(),
                last_modified,
                path: path_str,
                new_file_hash: false,
                file_size: Some(real_size),
                item_metadata: None,
                blurhash: None,
            };
            let result = self.update_file_data(data).await?;
            self.stats.false_changes += 1;
            self.tally(&result);
            return self.maybe_dispatch_backfill(sha256, mime_type, path).await;
        }

        if get_item_id(&mut self.conn, &sha256).await?.is_some() {
            tracing::info!(path = %path.display(), "item already exists");
            let data = FileScanData {
                sha256: sha256.clone(),
                last_modified,
                path: path_str,
                new_file_hash: true,
                file_size: Some(real_size),
                item_metadata: None,
                blurhash: None,
            };
            let result = self.update_file_data(data).await?;
            self.tally(&result);
            return self.maybe_dispatch_backfill(sha256, mime_type, path).await;
        }

        self.dispatch_prepare(path, last_modified, real_size, mime_type, md5, sha256)
            .await
    }

    async fn handle_new_item(&mut self, item: NewItemData) -> ApiResult<()> {
        if !item.thumbnails.is_empty()
            && !has_thumbnail(&mut self.conn, &item.sha256, THUMBNAIL_PROCESS_VERSION).await?
        {
            if let Err(err) = call_index_db_writer(&self.index_db, |reply| {
                IndexDbWriterMessage::StoreThumbnails {
                    sha256: item.sha256.clone(),
                    mime_type: item.mime_type.clone(),
                    process_version: THUMBNAIL_PROCESS_VERSION,
                    thumbnails: item.thumbnails.clone(),
                    reply,
                }
            })
            .await
            {
                tracing::error!(error = ?err, "failed to store thumbnails");
            }
        }

        if !item.frames.is_empty()
            && !has_frame(&mut self.conn, &item.sha256, FRAME_PROCESS_VERSION).await?
        {
            if let Err(err) =
                call_index_db_writer(&self.index_db, |reply| IndexDbWriterMessage::StoreFrames {
                    sha256: item.sha256.clone(),
                    mime_type: item.mime_type.clone(),
                    process_version: FRAME_PROCESS_VERSION,
                    frames: item.frames.clone(),
                    reply,
                })
                .await
            {
                tracing::error!(error = ?err, "failed to store frames");
            }
        }

        let data = FileScanData {
            sha256: item.sha256.clone(),
            last_modified: item.last_modified.clone(),
            path: item.path.to_string_lossy().to_string(),
            new_file_hash: true,
            file_size: Some(item.file_size),
            item_metadata: Some(item.metadata.clone()),
            blurhash: item.blurhash.clone(),
        };
        let result = self.update_file_data(data).await?;
        self.tally(&result);
        Ok(())
    }

    async fn handle_backfill(&mut self, backfill: BackfillResult) {
        self.in_flight_visuals.remove(&backfill.sha256);

        // Another task may have stored visuals for the same content while
        // this one was running; re-check before writing so a duplicate store
        // cannot violate the (item_sha256, idx) uniqueness. Read failures
        // fall through to storing, which was the previous behavior.
        let already_stored =
            has_thumbnail(&mut self.conn, &backfill.sha256, THUMBNAIL_PROCESS_VERSION)
                .await
                .unwrap_or(false);

        // Storage failures for backfilled visuals are logged and skipped so a
        // single bad file cannot abort the scan; the next scan retries them.
        if !backfill.thumbnails.is_empty() && !already_stored {
            if let Err(err) = call_index_db_writer(&self.index_db, |reply| {
                IndexDbWriterMessage::StoreThumbnails {
                    sha256: backfill.sha256.clone(),
                    mime_type: backfill.mime_type.clone(),
                    process_version: THUMBNAIL_PROCESS_VERSION,
                    thumbnails: backfill.thumbnails.clone(),
                    reply,
                }
            })
            .await
            {
                tracing::error!(error = ?err, "failed to store thumbnails");
            }
        }

        let frames_stored = has_frame(&mut self.conn, &backfill.sha256, FRAME_PROCESS_VERSION)
            .await
            .unwrap_or(false);
        if !backfill.extracted_frames.is_empty() && !frames_stored {
            if let Err(err) =
                call_index_db_writer(&self.index_db, |reply| IndexDbWriterMessage::StoreFrames {
                    sha256: backfill.sha256.clone(),
                    mime_type: backfill.mime_type.clone(),
                    process_version: FRAME_PROCESS_VERSION,
                    frames: backfill.extracted_frames.clone(),
                    reply,
                })
                .await
            {
                tracing::error!(error = ?err, "failed to store frames");
            }
        }

        if let Some(blurhash) = &backfill.blurhash {
            if let Err(err) =
                call_index_db_writer(&self.index_db, |reply| IndexDbWriterMessage::SetBlurhash {
                    sha256: backfill.sha256.clone(),
                    blurhash: blurhash.clone(),
                    reply,
                })
                .await
            {
                tracing::error!(error = ?err, "failed to set blurhash");
            }
        }
    }

    /// Regenerates missing thumbnails or blurhashes for files whose contents
    /// are already indexed. Dispatches a worker task only when something is
    /// actually missing, mirroring the Python `ensure_*` early returns.
    async fn maybe_dispatch_backfill(
        &mut self,
        sha256: String,
        mime_type: String,
        path: PathBuf,
    ) -> ApiResult<()> {
        let mut needs_thumb =
            !has_thumbnail(&mut self.conn, &sha256, THUMBNAIL_PROCESS_VERSION).await?;
        let needs_blurhash = !has_blurhash(&mut self.conn, &sha256).await?;
        if needs_thumb && mime_type.starts_with("image") {
            // Images served from the original file never get a stored
            // thumbnail, so `has_thumbnail` stays false for them forever.
            // Decide from the indexed dimensions instead of decoding, or every
            // rescan re-decodes every such image to produce nothing.
            needs_thumb = match fs::metadata(&path) {
                // Unreadable now: leave the visuals to a later scan.
                Err(_) => false,
                Ok(metadata) => {
                    let file_size = metadata.len();
                    match get_item_dimensions(&mut self.conn, &sha256).await? {
                        Some((Some(width), Some(height))) => {
                            !image_is_served_directly(file_size, width, height)
                        }
                        // Dimensions were never recorded; fall back to the
                        // size-only check and let the worker decode.
                        _ => file_size > SMALL_IMAGE_FILE_SIZE,
                    }
                }
            };
        }
        if !needs_thumb && !needs_blurhash {
            return Ok(());
        }
        // Identical content elsewhere in this scan already has a visuals task
        // in flight; its results apply to this sha256 as well.
        if self.in_flight_visuals.contains(&sha256) {
            return Ok(());
        }

        let mut existing_frames = Vec::new();
        let mut video_duration = 0.0_f64;
        if needs_thumb && mime_type.starts_with("video") {
            // Frames already stored in the database can rebuild the thumbnail
            // even when the item's duration metadata is missing; only a fresh
            // ffmpeg extraction needs a usable duration (matching Python,
            // which consults metadata only when no frames exist).
            existing_frames = get_frames_bytes(&mut self.conn, &sha256).await?;
            if existing_frames.is_empty() {
                if let Some((duration, video_tracks)) =
                    get_item_visual_meta(&mut self.conn, &sha256).await?
                {
                    let duration = duration.unwrap_or(0.0);
                    if duration <= 0.0 || video_tracks.unwrap_or(0) <= 0 {
                        tracing::debug!(
                            path = %path.display(),
                            "skipping video thumbnail generation due to missing video track"
                        );
                        return Ok(());
                    }
                    video_duration = duration;
                }
            }
        }
        let existing_thumb = if !needs_thumb && needs_blurhash {
            get_thumbnail_bytes(&mut self.conn, &sha256, 0).await?
        } else {
            None
        };
        // A blurhash can only come from a stored thumbnail or the image itself.
        if !needs_thumb
            && needs_blurhash
            && existing_thumb.is_none()
            && !mime_type.starts_with("image")
        {
            return Ok(());
        }

        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ApiError::internal("Failed to schedule scan work"))?;
        self.in_flight_visuals.insert(sha256.clone());
        let tracked = TrackedTask {
            path: path.to_string_lossy().to_string(),
            backfill_sha256: Some(sha256.clone()),
        };
        let timers = self.timers.clone();
        let handle = self.tasks.spawn(async move {
            let _permit = permit;
            let outer_path = path.clone();
            let outer_sha256 = sha256.clone();
            let outer_mime = mime_type.clone();
            let joined = tokio::task::spawn_blocking(move || {
                generate_backfill_visuals(
                    &path,
                    &mime_type,
                    sha256,
                    needs_thumb,
                    needs_blurhash,
                    existing_frames,
                    existing_thumb,
                    video_duration,
                    &timers,
                )
            })
            .await;
            match joined {
                Ok(backfill) => TaskOutcome::Backfill(backfill),
                // The file itself was already recorded before this task was
                // dispatched; a dead visuals worker only means the visuals
                // stay missing until the next scan. Do not surface it as a
                // file error, which would double-count the file and put it
                // on the unavailable-marking exclusion list for no reason.
                Err(err) => {
                    tracing::error!(
                        error = %err,
                        path = %outer_path.display(),
                        "visuals backfill worker failed"
                    );
                    TaskOutcome::Backfill(BackfillResult {
                        sha256: outer_sha256,
                        mime_type: outer_mime,
                        thumbnails: Vec::new(),
                        extracted_frames: Vec::new(),
                        blurhash: None,
                    })
                }
            }
        });
        self.task_paths.insert(handle.id(), tracked);
        Ok(())
    }

    async fn dispatch_hash(
        &mut self,
        path: PathBuf,
        last_modified: String,
        reported_size: i64,
        mime_type: String,
        existing_sha256: Option<String>,
    ) -> ApiResult<()> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ApiError::internal("Failed to schedule scan work"))?;
        let tracked = TrackedTask {
            path: path.to_string_lossy().to_string(),
            backfill_sha256: None,
        };
        let hash_timer = self.timers.hashing.clone();
        let handle = self.tasks.spawn(async move {
            let _permit = permit;
            let hash_path = path.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let _span = hash_timer.start();
                calculate_hashes(&hash_path)
            })
            .await;
            match joined {
                Ok(Ok((md5, sha256, real_size))) => TaskOutcome::Hashed(HashedFile {
                    path,
                    last_modified,
                    reported_size,
                    mime_type,
                    existing_sha256,
                    md5,
                    sha256,
                    real_size,
                }),
                Ok(Err(err)) => TaskOutcome::Failed(FailedFile {
                    path,
                    error: FileProcessError::Io(err.to_string()),
                }),
                Err(err) => TaskOutcome::Failed(FailedFile {
                    path,
                    error: FileProcessError::Worker(err.to_string()),
                }),
            }
        });
        self.task_paths.insert(handle.id(), tracked);
        Ok(())
    }

    /// Runs full metadata extraction, the stage-2 filter, and visual
    /// generation for files whose content is new to the index.
    async fn dispatch_prepare(
        &mut self,
        path: PathBuf,
        last_modified: String,
        file_size: i64,
        mime_type: String,
        md5: String,
        sha256: String,
    ) -> ApiResult<()> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ApiError::internal("Failed to schedule scan work"))?;
        let filter = self.filescan_filter.clone();
        let tracked = TrackedTask {
            path: path.to_string_lossy().to_string(),
            backfill_sha256: None,
        };
        let timers = self.timers.clone();
        let handle = self.tasks.spawn(async move {
            let _permit = permit;
            let outer_path = path.clone();
            let joined = tokio::task::spawn_blocking(move || {
                prepare_new_item(
                    path,
                    last_modified,
                    file_size,
                    mime_type,
                    md5,
                    sha256,
                    filter,
                    &timers,
                )
            })
            .await;
            match joined {
                Ok(outcome) => outcome,
                Err(err) => TaskOutcome::Failed(FailedFile {
                    path: outer_path,
                    error: FileProcessError::Worker(err.to_string()),
                }),
            }
        });
        self.task_paths.insert(handle.id(), tracked);
        Ok(())
    }

    async fn update_file_data(&mut self, data: FileScanData) -> ApiResult<FileUpsertResult> {
        call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::UpdateFileData {
                time_added: self.scan_time.clone(),
                scan_id: self.scan_id,
                data: data.clone(),
                reply,
            }
        })
        .await
    }

    fn tally(&mut self, result: &FileUpsertResult) {
        if result.item_inserted {
            self.stats.new_items += 1;
        }
        if result.file_updated {
            self.stats.unchanged_files += 1;
        } else if result.file_deleted {
            self.stats.modified_files += 1;
        } else if result.file_inserted {
            self.stats.new_files += 1;
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn prepare_new_item(
    path: PathBuf,
    last_modified: String,
    file_size: i64,
    mime_type: String,
    md5: String,
    sha256: String,
    filter: Option<Arc<Match>>,
    timers: &ScanTimers,
) -> TaskOutcome {
    let metadata_span = timers.metadata.start();
    let preloaded_image = if mime_type.starts_with("image") {
        match open_image(&path) {
            Ok(image) => Some(image),
            Err(err) => {
                return TaskOutcome::Failed(FailedFile {
                    path,
                    error: FileProcessError::Unsupported(err.to_string()),
                });
            }
        }
    } else {
        None
    };
    let metadata =
        match extract_item_metadata_inner(&path, &mime_type, md5, preloaded_image.as_ref()) {
            Ok(metadata) => metadata,
            Err(error) => {
                return TaskOutcome::Failed(FailedFile { path, error });
            }
        };
    drop(metadata_span);

    if !passes_filescan_filter_stage2(
        filter.as_deref(),
        &path,
        &last_modified,
        file_size,
        &mime_type,
        &metadata.md5,
        &sha256,
        &metadata,
    ) {
        return TaskOutcome::Failed(FailedFile {
            path,
            error: FileProcessError::Filtered,
        });
    }

    let (thumbnails, frames, blurhash) =
        match generate_new_item_visuals(&path, &mime_type, &metadata, preloaded_image, timers) {
            Ok(result) => result,
            Err(err) => {
                tracing::error!(error = ?err, path = %path.display(), "failed to generate visuals");
                (Vec::new(), Vec::new(), None)
            }
        };

    TaskOutcome::NewItem(NewItemData {
        path,
        last_modified,
        file_size,
        sha256,
        mime_type,
        metadata,
        thumbnails,
        frames,
        blurhash,
    })
}

pub(crate) struct PreparedFile {
    pub(crate) path: PathBuf,
    pub(crate) last_modified: String,
    pub(crate) file_size: i64,
    pub(crate) sha256: String,
    pub(crate) mime_type: String,
    pub(crate) metadata: ItemScanMeta,
    pub(crate) thumbnails: Vec<StoredImage>,
    pub(crate) frames: Vec<StoredImage>,
    pub(crate) blurhash: Option<String>,
}

pub(crate) struct FileWriteData {
    pub(crate) sha256: String,
    pub(crate) mime_type: String,
    pub(crate) data: FileScanData,
    pub(crate) new_file_timestamp: bool,
    pub(crate) new_file_hash: bool,
    pub(crate) thumbnails: Vec<StoredImage>,
    pub(crate) frames: Vec<StoredImage>,
    pub(crate) blurhash: Option<String>,
    pub(crate) time_added: String,
}

impl FileWriteData {
    fn new(
        sha256: String,
        mime_type: String,
        data: FileScanData,
        new_file_timestamp: bool,
        new_file_hash: bool,
        prepared: PreparedFile,
        time_added: String,
    ) -> Self {
        Self {
            sha256,
            mime_type,
            data,
            new_file_timestamp,
            new_file_hash,
            thumbnails: prepared.thumbnails,
            frames: prepared.frames,
            blurhash: prepared.blurhash,
            time_added,
        }
    }
}

pub(crate) async fn build_file_scan_data(
    conn: &mut sqlx::SqliteConnection,
    prepared: PreparedFile,
    scan_time: &str,
) -> ApiResult<FileWriteData> {
    let existing = get_file_by_path(conn, prepared.path.to_string_lossy().as_ref()).await?;
    let time_added = scan_time.to_string();

    if let Some(existing) = existing {
        if existing.last_modified == prepared.last_modified {
            let mime_type = prepared.mime_type.clone();
            let data = FileScanData {
                sha256: existing.sha256.clone(),
                last_modified: existing.last_modified,
                path: prepared.path.to_string_lossy().to_string(),
                new_file_hash: false,
                file_size: None,
                item_metadata: None,
                blurhash: prepared.blurhash.clone(),
            };
            return Ok(FileWriteData::new(
                existing.sha256,
                mime_type,
                data,
                false,
                false,
                prepared,
                time_added,
            ));
        }

        if existing.sha256 == prepared.sha256 {
            let sha256 = prepared.sha256.clone();
            let mime_type = prepared.mime_type.clone();
            let data = FileScanData {
                sha256: sha256.clone(),
                last_modified: prepared.last_modified.clone(),
                path: prepared.path.to_string_lossy().to_string(),
                new_file_hash: false,
                file_size: Some(prepared.file_size),
                item_metadata: None,
                blurhash: prepared.blurhash.clone(),
            };
            return Ok(FileWriteData::new(
                sha256, mime_type, data, true, false, prepared, time_added,
            ));
        }
    }

    let item_id = get_item_id(conn, &prepared.sha256).await?;
    let item_metadata = if item_id.is_some() {
        None
    } else {
        Some(prepared.metadata.clone())
    };
    let sha256 = prepared.sha256.clone();
    let mime_type = prepared.mime_type.clone();
    let data = FileScanData {
        sha256: sha256.clone(),
        last_modified: prepared.last_modified.clone(),
        path: prepared.path.to_string_lossy().to_string(),
        new_file_hash: true,
        file_size: Some(prepared.file_size),
        item_metadata,
        blurhash: prepared.blurhash.clone(),
    };

    Ok(FileWriteData::new(
        sha256, mime_type, data, true, true, prepared, time_added,
    ))
}

#[derive(Debug)]
pub(crate) enum FileProcessError {
    // The String payloads are only read through the derived Debug impl when
    // scan errors are logged, which the dead_code lint doesn't count.
    Worker(#[allow(dead_code)] String),
    Io(#[allow(dead_code)] String),
    Unsupported(#[allow(dead_code)] String),
    /// The file was rejected by the user's filescan filter.
    Filtered,
    /// The file's mtime matches the DB record, so hashing was skipped.
    Unchanged,
}

pub(crate) fn process_file(
    path: PathBuf,
    filescan_filter: Option<Arc<Match>>,
    timers: &ScanTimers,
) -> Result<PreparedFile, FileProcessError> {
    let (last_modified, file_size) = get_last_modified_time_and_size(&path)
        .map_err(|err| FileProcessError::Io(err.to_string()))?;

    let mime_type = infer_mime_type(&path)?;
    if !passes_filescan_filter_stage1(
        filescan_filter.as_deref(),
        &path,
        &last_modified,
        file_size,
        &mime_type,
    ) {
        return Err(FileProcessError::Filtered);
    }

    let hash_span = timers.hashing.start();
    let (md5, sha256, real_size) =
        calculate_hashes(&path).map_err(|err| FileProcessError::Io(err.to_string()))?;
    drop(hash_span);

    if real_size != file_size {
        tracing::warn!(path = %path.display(), real_size, file_size, "file size mismatch");
    }
    let file_size = real_size;

    let metadata_span = timers.metadata.start();
    let metadata = extract_item_metadata(&path, &mime_type, md5.clone())?;
    drop(metadata_span);

    if !passes_filescan_filter_stage2(
        filescan_filter.as_deref(),
        &path,
        &last_modified,
        file_size,
        &mime_type,
        &md5,
        &sha256,
        &metadata,
    ) {
        return Err(FileProcessError::Filtered);
    }

    let (thumbnails, frames, blurhash) =
        match generate_new_item_visuals(&path, &mime_type, &metadata, None, timers) {
            Ok(result) => result,
            Err(err) => {
                tracing::error!(error = ?err, path = %path.display(), "failed to generate visuals");
                (Vec::new(), Vec::new(), None)
            }
        };

    Ok(PreparedFile {
        path,
        last_modified,
        file_size,
        sha256,
        mime_type,
        metadata,
        thumbnails,
        frames,
        blurhash,
    })
}

fn passes_filescan_filter_stage1(
    filter: Option<&Match>,
    path: &Path,
    last_modified: &str,
    file_size: i64,
    mime_type: &str,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("")
        .to_string();
    let value = MatchValue {
        last_modified: Some(last_modified.to_string()),
        size: Some(file_size),
        path: Some(path.to_string_lossy().to_string()),
        filename: Some(filename),
        r#type: Some(mime_type.to_string()),
        ..Default::default()
    };
    evaluate_match(filter, &value)
}

fn passes_filescan_filter_stage2(
    filter: Option<&Match>,
    path: &Path,
    last_modified: &str,
    file_size: i64,
    mime_type: &str,
    md5: &str,
    sha256: &str,
    metadata: &ItemScanMeta,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("")
        .to_string();
    let value = MatchValue {
        last_modified: Some(last_modified.to_string()),
        size: Some(file_size),
        path: Some(path.to_string_lossy().to_string()),
        filename: Some(filename),
        r#type: Some(mime_type.to_string()),
        md5: Some(md5.to_string()),
        sha256: Some(sha256.to_string()),
        width: metadata.width,
        height: metadata.height,
        duration: metadata.duration,
        audio_tracks: metadata.audio_tracks,
        video_tracks: metadata.video_tracks,
        subtitle_tracks: metadata.subtitle_tracks,
        ..Default::default()
    };
    evaluate_match(filter, &value)
}

pub(crate) fn parse_filescan_filter(config: &SystemConfig) -> Option<Match> {
    config.filescan_filter.clone()
}

fn infer_mime_type(path: &Path) -> Result<String, FileProcessError> {
    let guess = MimeGuess::from_path(path);
    let mime = guess
        .first()
        .ok_or_else(|| FileProcessError::Unsupported("missing mime type".to_string()))?;
    Ok(mime.essence_str().to_string())
}

/// Decodes an image like PIL does: the format is sniffed from the file's
/// magic bytes (the extension is only a fallback when the content is
/// unrecognized) and the crate's default 512 MiB allocation cap is replaced
/// by the configurable `[jobs].image_decode_memory_limit_mb` ceiling.
/// Archives contain mis-named files (WebP saved as .png) and very large
/// images (20k x 20k collages) that Python indexed fine.
pub(crate) fn open_image(path: impl AsRef<Path>) -> image::ImageResult<DynamicImage> {
    let mut reader = image::ImageReader::open(path)?.with_guessed_format()?;
    reader.limits(decode_limits());
    reader.decode()
}

/// In-memory counterpart of [`open_image`]: content-sniffed, same ceiling.
pub(crate) fn decode_image_bytes(bytes: &[u8]) -> image::ImageResult<DynamicImage> {
    let mut reader = image::ImageReader::new(std::io::Cursor::new(bytes)).with_guessed_format()?;
    reader.limits(decode_limits());
    reader.decode()
}

fn decode_limits() -> image::Limits {
    let mut limits = image::Limits::no_limits();
    let limit_mb = crate::config::runtime().image_decode_memory_limit_mb;
    if limit_mb > 0 {
        limits.max_alloc = Some(limit_mb.saturating_mul(1024 * 1024));
    }
    limits
}

fn extract_item_metadata(
    path: &Path,
    mime_type: &str,
    md5: String,
) -> Result<ItemScanMeta, FileProcessError> {
    extract_item_metadata_inner(path, mime_type, md5, None)
}

fn extract_item_metadata_inner(
    path: &Path,
    mime_type: &str,
    md5: String,
    preloaded_image: Option<&DynamicImage>,
) -> Result<ItemScanMeta, FileProcessError> {
    let mut metadata = ItemScanMeta {
        md5,
        mime_type: mime_type.to_string(),
        width: None,
        height: None,
        duration: None,
        audio_tracks: None,
        video_tracks: None,
        subtitle_tracks: None,
    };

    if mime_type.starts_with("image") {
        let (width, height) = match preloaded_image {
            Some(image) => image.dimensions(),
            None => open_image(path)
                .map_err(|err| FileProcessError::Unsupported(err.to_string()))?
                .dimensions(),
        };
        metadata.width = Some(width as i64);
        metadata.height = Some(height as i64);
        return Ok(metadata);
    }

    if mime_type.starts_with("video") || mime_type.starts_with("audio") {
        let info = extract_media_info(path)?;
        if mime_type.starts_with("video") {
            if let Some(video) = info.video_track {
                metadata.width = video.width.map(|width| width as i64);
                metadata.height = video.height.map(|height| height as i64);
                metadata.duration = Some(video.duration);
                metadata.video_tracks = Some(1);
            }
            metadata.audio_tracks = Some(info.audio_tracks.len() as i64);
            metadata.subtitle_tracks = Some(info.subtitle_tracks.len() as i64);
        } else {
            metadata.duration = Some(info.audio_tracks.iter().map(|track| track.duration).sum());
            metadata.audio_tracks = Some(info.audio_tracks.len() as i64);
            metadata.video_tracks = Some(0);
            metadata.subtitle_tracks = Some(info.subtitle_tracks.len() as i64);
        }
    }

    Ok(metadata)
}

fn generate_new_item_visuals(
    path: &Path,
    mime_type: &str,
    metadata: &ItemScanMeta,
    preloaded_image: Option<DynamicImage>,
    timers: &ScanTimers,
) -> Result<(Vec<StoredImage>, Vec<StoredImage>, Option<String>), FileProcessError> {
    let thumb_span = timers.thumbgen.start();
    let mut thumbnails = Vec::new();
    let mut frames = Vec::new();
    let mut blurhash_source: Option<DynamicImage> = None;

    if mime_type.starts_with("video") {
        let duration = metadata.duration.unwrap_or(0.0);
        if metadata.video_tracks.unwrap_or(0) > 0 && duration > 0.0 {
            let extracted_frames = extract_video_frames(path, 4, duration)?;
            if !extracted_frames.is_empty() {
                let grid = overlay_mime_label(build_image_grid(&extracted_frames), mime_type);
                thumbnails.push(encode_image(0, &grid)?);
                let labeled_first = overlay_mime_label(extracted_frames[0].clone(), mime_type);
                thumbnails.push(encode_image(1, &labeled_first)?);
                frames = extracted_frames
                    .iter()
                    .enumerate()
                    .map(|(idx, frame)| encode_image(idx as i64, frame))
                    .collect::<Result<Vec<_>, _>>()?;
                blurhash_source = Some(grid);
            }
        } else {
            tracing::debug!(
                path = %path.display(),
                "skipping video thumbnail generation due to missing video track"
            );
        }
    } else if mime_type.starts_with("audio") {
        let thumb = get_audio_thumbnail(path, mime_type);
        thumbnails.push(encode_image(0, &thumb)?);
        blurhash_source = Some(thumb);
    } else if mime_type.starts_with("image") {
        let image = match preloaded_image {
            Some(image) => image,
            None => {
                open_image(path).map_err(|err| FileProcessError::Unsupported(err.to_string()))?
            }
        };
        if let Some(thumb) = generate_thumbnail(path, &image)? {
            thumbnails.push(encode_image(0, &thumb)?);
            blurhash_source = Some(thumb);
        } else {
            blurhash_source = Some(image);
        }
    } else if mime_type.starts_with("application/pdf") {
        // Renders nothing when pdfium is unavailable or the PDF is broken;
        // the item is then indexed without visuals, like any unsupported type.
        if let Some(page) = render_pdf_first_page(path) {
            thumbnails.push(encode_image(0, &page)?);
            blurhash_source = Some(page);
        }
    } else if mime_type.starts_with("text/html") {
        // Renders nothing when no headless browser is installed or the page
        // fails to render; the item is then indexed without visuals.
        if let Some(shot) = render_html_screenshot(path) {
            thumbnails.push(encode_image(0, &shot)?);
            blurhash_source = Some(shot);
        }
    }

    drop(thumb_span);

    let blurhash_span = timers.blurhash.start();
    let blurhash = if let Some(image) = blurhash_source {
        compute_blurhash(&image).ok()
    } else {
        None
    };
    drop(blurhash_span);

    Ok((thumbnails, frames, blurhash))
}

/// Regenerates only the visuals a file is missing. Never fails hard: partial
/// or failed generation degrades to empty results, matching the Python
/// behavior of catching thumbnail/blurhash errors per file.
#[allow(clippy::too_many_arguments)]
fn generate_backfill_visuals(
    path: &Path,
    mime_type: &str,
    sha256: String,
    needs_thumb: bool,
    needs_blurhash: bool,
    existing_frames: Vec<Vec<u8>>,
    existing_thumb: Option<Vec<u8>>,
    video_duration: f64,
    timers: &ScanTimers,
) -> BackfillResult {
    let thumb_span = timers.thumbgen.start();
    let mut thumbnails = Vec::new();
    let mut extracted_frames = Vec::new();
    let mut blurhash_source: Option<DynamicImage> = None;

    if needs_thumb {
        match build_backfill_thumbnails(path, mime_type, &existing_frames, video_duration) {
            Ok((thumbs, extracted, source)) => {
                thumbnails = thumbs;
                extracted_frames = extracted;
                blurhash_source = source;
            }
            Err(err) => {
                tracing::error!(error = ?err, path = %path.display(), "failed to generate thumbnails");
            }
        }
    }
    drop(thumb_span);

    let blurhash_span = timers.blurhash.start();
    let mut blurhash = None;
    if needs_blurhash {
        let source = blurhash_source.or_else(|| {
            existing_thumb
                .as_deref()
                .and_then(|bytes| decode_image_bytes(bytes).ok())
        });
        let source = match source {
            Some(source) => Some(source),
            None if mime_type.starts_with("image") => open_image(path).ok(),
            None => None,
        };
        blurhash = source
            .as_ref()
            .and_then(|image| compute_blurhash(image).ok());
    }
    drop(blurhash_span);

    BackfillResult {
        sha256,
        mime_type: mime_type.to_string(),
        thumbnails,
        extracted_frames,
        blurhash,
    }
}

fn build_backfill_thumbnails(
    path: &Path,
    mime_type: &str,
    existing_frames: &[Vec<u8>],
    video_duration: f64,
) -> Result<(Vec<StoredImage>, Vec<StoredImage>, Option<DynamicImage>), FileProcessError> {
    let mut thumbnails = Vec::new();
    let mut extracted = Vec::new();
    let mut source = None;

    if mime_type.starts_with("video") {
        // Reuse frames already stored in the database before re-running ffmpeg.
        let mut frames: Vec<DynamicImage> = existing_frames
            .iter()
            .filter_map(|bytes| decode_image_bytes(bytes).ok())
            .collect();
        let mut fresh = false;
        if frames.is_empty() {
            frames = extract_video_frames(path, 4, video_duration)?;
            fresh = true;
        }
        if !frames.is_empty() {
            let grid = overlay_mime_label(build_image_grid(&frames), mime_type);
            thumbnails.push(encode_image(0, &grid)?);
            let labeled_first = overlay_mime_label(frames[0].clone(), mime_type);
            thumbnails.push(encode_image(1, &labeled_first)?);
            if fresh {
                extracted = frames
                    .iter()
                    .enumerate()
                    .map(|(idx, frame)| encode_image(idx as i64, frame))
                    .collect::<Result<Vec<_>, _>>()?;
            }
            source = Some(grid);
        }
    } else if mime_type.starts_with("audio") {
        let thumb = get_audio_thumbnail(path, mime_type);
        thumbnails.push(encode_image(0, &thumb)?);
        source = Some(thumb);
    } else if mime_type.starts_with("image") {
        let file_size = fs::metadata(path)
            .map_err(|err| FileProcessError::Io(err.to_string()))?
            .len();
        // Only decode when the image is large enough to warrant a thumbnail;
        // the blurhash fallback opens the image separately when needed.
        if file_size > SMALL_IMAGE_FILE_SIZE {
            let image =
                open_image(path).map_err(|err| FileProcessError::Unsupported(err.to_string()))?;
            if let Some(thumb) = generate_thumbnail(path, &image)? {
                thumbnails.push(encode_image(0, &thumb)?);
                source = Some(thumb);
            } else {
                source = Some(image);
            }
        }
    } else if mime_type.starts_with("application/pdf") {
        if let Some(page) = render_pdf_first_page(path) {
            thumbnails.push(encode_image(0, &page)?);
            source = Some(page);
        }
    } else if mime_type.starts_with("text/html") {
        if let Some(shot) = render_html_screenshot(path) {
            thumbnails.push(encode_image(0, &shot)?);
            source = Some(shot);
        }
    }

    Ok((thumbnails, extracted, source))
}

fn compute_blurhash(image: &DynamicImage) -> Result<String, FileProcessError> {
    let resized = resize_for_blurhash(image);
    let rgba = resized.to_rgba8();
    blurhash_encode(4, 4, rgba.width(), rgba.height(), rgba.as_raw())
        .map_err(|err| FileProcessError::Unsupported(err.to_string()))
}

fn resize_for_blurhash(image: &DynamicImage) -> DynamicImage {
    let (width, height) = image.dimensions();
    let max_dim = 128u32;
    if width <= max_dim && height <= max_dim {
        return image.clone();
    }
    // The source is often a full-resolution image (small files never get a
    // stored thumbnail), and the result only feeds a 4x4-component blurhash,
    // so use the fast single-pass box filter instead of a quality resampler.
    image.thumbnail(max_dim, max_dim)
}

/// Whether an image is served from its original file and therefore gets no
/// stored thumbnail. Kept separate from [`generate_thumbnail`] so a rescan can
/// answer the question from indexed metadata instead of decoding the file:
/// nothing is stored for these images, so `has_thumbnail` stays false forever
/// and an unguarded backfill would decode them on every single scan.
fn image_is_served_directly(file_size: u64, width: i64, height: i64) -> bool {
    file_size <= SMALL_IMAGE_FILE_SIZE
        || (width <= MAX_SERVED_IMAGE_DIMENSION
            && height <= MAX_SERVED_IMAGE_DIMENSION
            && file_size <= MAX_SERVED_IMAGE_FILE_SIZE)
}

fn generate_thumbnail(
    path: &Path,
    image: &DynamicImage,
) -> Result<Option<DynamicImage>, FileProcessError> {
    let metadata = fs::metadata(path).map_err(|err| FileProcessError::Io(err.to_string()))?;
    let file_size = metadata.len();
    let (width, height) = image.dimensions();
    if image_is_served_directly(file_size, width as i64, height as i64) {
        return Ok(None);
    }

    let max_dimension = MAX_SERVED_IMAGE_DIMENSION as u32;
    Ok(Some(image.resize(
        max_dimension,
        max_dimension,
        image::imageops::FilterType::Lanczos3,
    )))
}

fn encode_image(idx: i64, image: &DynamicImage) -> Result<StoredImage, FileProcessError> {
    let rgb = image.to_rgb8();
    let mut buffer = Vec::new();
    let mut encoder = JpegEncoder::new_with_quality(&mut buffer, 85);
    encoder
        .encode(&rgb, rgb.width(), rgb.height(), ColorType::Rgb8.into())
        .map_err(|err| FileProcessError::Unsupported(err.to_string()))?;

    Ok(StoredImage {
        idx,
        width: image.width() as i64,
        height: image.height() as i64,
        bytes: buffer,
    })
}

static PDFIUM: OnceLock<Option<Pdfium>> = OnceLock::new();
// The pdfium C library is not thread-safe, and pdfium-render's `sync` feature
// only makes the `Pdfium` handle Send+Sync (its internal mutex guards nothing
// but library init/destroy). Every FFI call — document load, page access,
// rendering — must be externally serialized or concurrent scan workers cause
// undefined behavior inside pdfium.
static PDFIUM_CALL_LOCK: Mutex<()> = Mutex::new(());

/// Lazily binds the pdfium dynamic library, mirroring the Python dependency
/// on pypdfium2. Degrades gracefully: when the library cannot be found, PDF
/// thumbnails are skipped (warned once) and all other scanning is unaffected.
fn pdfium() -> Option<&'static Pdfium> {
    PDFIUM
        .get_or_init(|| {
            let mut candidates: Vec<PathBuf> = Vec::new();
            if let Some(custom) = &crate::config::runtime().pdfium {
                candidates.push(custom.clone());
            }
            if let Some(exe_dir) = env::current_exe()
                .ok()
                .and_then(|exe| exe.parent().map(Path::to_path_buf))
            {
                candidates.push(exe_dir);
            }
            if let Ok(cwd) = env::current_dir() {
                candidates.push(cwd);
            }
            for dir in &candidates {
                let library = Pdfium::pdfium_platform_library_name_at_path(dir);
                match Pdfium::bind_to_library(&library) {
                    Ok(bindings) => return Some(Pdfium::new(bindings)),
                    Err(err) => {
                        tracing::debug!(
                            error = %err,
                            path = %library.display(),
                            "failed to bind pdfium library"
                        );
                    }
                }
            }
            match Pdfium::bind_to_system_library() {
                Ok(bindings) => Some(Pdfium::new(bindings)),
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        searched = ?candidates,
                        "pdfium library not found; PDF thumbnails are disabled"
                    );
                    None
                }
            }
        })
        .as_ref()
}

/// Renders the first page of a PDF at 2x its point size, matching the Python
/// pypdfium2 loader (`scale=2`, i.e. 144 dpi).
fn render_pdf_first_page(path: &Path) -> Option<DynamicImage> {
    let pdfium = pdfium()?;
    let _serialized = PDFIUM_CALL_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let document = match pdfium.load_pdf_from_file(path, None) {
        Ok(document) => document,
        Err(err) => {
            tracing::error!(error = %err, path = %path.display(), "failed to load PDF");
            return None;
        }
    };
    let page = match document.pages().first() {
        Ok(page) => page,
        Err(err) => {
            tracing::error!(error = %err, path = %path.display(), "PDF has no readable pages");
            return None;
        }
    };
    match page.render_with_config(&PdfRenderConfig::new().scale_page_by_factor(2.0)) {
        Ok(bitmap) => Some(bitmap.as_image()),
        Err(err) => {
            tracing::error!(error = %err, path = %path.display(), "failed to render PDF page");
            None
        }
    }
}

/// Renders every page of a PDF at 2x its point size (144 dpi), matching the
/// Python pypdfium2 loader used by data extraction (`scale=2`). Unlike
/// thumbnail generation this fails hard: extraction must record the item as
/// failed (and retry it next run) rather than mark it processed.
pub(crate) fn render_pdf_pages(path: &Path) -> Result<Vec<DynamicImage>, String> {
    let pdfium = pdfium().ok_or_else(|| "pdfium library not available".to_string())?;
    let _serialized = PDFIUM_CALL_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let document = pdfium
        .load_pdf_from_file(path, None)
        .map_err(|err| format!("failed to load PDF: {err}"))?;
    let mut pages = Vec::new();
    for page in document.pages().iter() {
        let bitmap = page
            .render_with_config(&PdfRenderConfig::new().scale_page_by_factor(2.0))
            .map_err(|err| format!("failed to render PDF page: {err}"))?;
        pages.push(bitmap.as_image());
    }
    Ok(pages)
}

static HTML_RENDERER: OnceLock<Option<PathBuf>> = OnceLock::new();

/// Lazily locates a locally installed Chromium-family browser for headless
/// HTML screenshots. Degrades gracefully: when no browser is found, HTML
/// thumbnails are skipped (warned once) and all other scanning is unaffected.
fn html_renderer() -> Option<&'static PathBuf> {
    HTML_RENDERER
        .get_or_init(|| {
            let mut candidates: Vec<PathBuf> = Vec::new();
            if let Some(custom) = &crate::config::runtime().html_renderer {
                candidates.push(custom.clone());
            }
            candidates.extend(
                [
                    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
                    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
                    "/usr/bin/chromium",
                    "/usr/bin/chromium-browser",
                    "/usr/bin/google-chrome",
                    "/usr/bin/google-chrome-stable",
                ]
                .iter()
                .map(PathBuf::from),
            );
            for candidate in &candidates {
                if candidate.is_file() {
                    return Some(candidate.clone());
                }
            }
            tracing::warn!(
                searched = ?candidates,
                "no headless browser found; HTML thumbnails are disabled"
            );
            None
        })
        .as_ref()
}

/// Builds a percent-encoded file:// URL from a canonicalized path, so names
/// containing `#`, `?`, `%`, or spaces are not misparsed by the browser as
/// fragment/query/escape syntax. On Windows, canonicalize yields a \\?\C:\...
/// verbatim path; the prefix is stripped first because the url crate would
/// encode it into the result.
fn html_file_url(path: &Path) -> Option<String> {
    let canonical = match path.canonicalize() {
        Ok(canonical) => canonical,
        Err(err) => {
            tracing::error!(error = %err, path = %path.display(), "failed to canonicalize HTML path");
            return None;
        }
    };
    let text = canonical.to_string_lossy().to_string();
    let plain = text.strip_prefix(r"\\?\").unwrap_or(&text);
    match url::Url::from_file_path(plain) {
        Ok(url) => Some(url.into()),
        Err(()) => {
            tracing::error!(path = %path.display(), "failed to build file URL for HTML path");
            None
        }
    }
}

/// A headless browser is a multi-process tree weighing hundreds of MB;
/// scan-worker parallelism (CPU count) is the wrong unit for it. At most this
/// many renders run at once, independent of the scan semaphore.
static BROWSER_SLOTS: BlockingSemaphore = BlockingSemaphore::new(2);

/// A minimal counting semaphore usable from spawn_blocking threads, where the
/// tokio async semaphore cannot be awaited.
struct BlockingSemaphore {
    permits: Mutex<usize>,
    available: Condvar,
}

impl BlockingSemaphore {
    const fn new(permits: usize) -> Self {
        Self {
            permits: Mutex::new(permits),
            available: Condvar::new(),
        }
    }

    fn acquire(&self) -> BlockingSemaphoreGuard<'_> {
        let mut permits = self
            .permits
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while *permits == 0 {
            permits = self
                .available
                .wait(permits)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        *permits -= 1;
        BlockingSemaphoreGuard { semaphore: self }
    }
}

struct BlockingSemaphoreGuard<'a> {
    semaphore: &'a BlockingSemaphore,
}

impl Drop for BlockingSemaphoreGuard<'_> {
    fn drop(&mut self) {
        let mut permits = self
            .semaphore
            .permits
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *permits += 1;
        self.semaphore.available.notify_one();
    }
}

/// Screenshots an HTML file with a locally installed headless browser. This
/// intentionally replaces the Python weasyprint HTML->PDF pipeline with a
/// browser viewport capture. Never fails hard: any error degrades to None.
/// (Extraction callers treat None as an item failure so the item is retried.)
pub(crate) fn render_html_screenshot(path: &Path) -> Option<DynamicImage> {
    let browser = html_renderer()?;
    let url = html_file_url(path)?;
    let _slot = BROWSER_SLOTS.acquire();
    let temp_dir = temp_dir_path();
    // The browser resolves its path arguments itself, so they must not
    // depend on the inherited working directory.
    let temp_dir = if temp_dir.is_absolute() {
        temp_dir
    } else {
        env::current_dir().ok()?.join(temp_dir)
    };
    // Headless browsers refuse to share a live profile; give each render its
    // own throwaway --user-data-dir.
    let profile_dir = temp_dir.join("profile");
    if let Err(err) = fs::create_dir_all(&profile_dir) {
        tracing::error!(error = %err, path = %profile_dir.display(), "failed to create temp screenshot dir");
        return None;
    }
    let screenshot = temp_dir.join("shot.png");
    // A leftover file here (crashed previous run, reused directory) would
    // decode as this file's screenshot before the browser writes anything.
    let _ = fs::remove_file(&screenshot);

    let result = run_html_screenshot(browser, &url, &profile_dir, &screenshot, path);
    if let Err(err) = fs::remove_dir_all(&temp_dir) {
        tracing::debug!(error = %err, path = %temp_dir.display(), "failed to remove temp screenshot dir");
    }
    result
}

fn run_html_screenshot(
    browser: &Path,
    url: &str,
    profile_dir: &Path,
    screenshot: &Path,
    path: &Path,
) -> Option<DynamicImage> {
    // Scanned HTML lives in user-approved folders, but a saved page can still
    // carry live script and remote references, so all network traffic
    // (including localhost, via the <-loopback> bypass override) is routed
    // into a dead proxy — no beaconing, no SSRF. file:// subresources are
    // unaffected, matching what the Python weasyprint pipeline could load.
    // JavaScript stays enabled: --blink-settings=scriptEnabled=false makes
    // Edge's new headless mode never produce a screenshot (verified against
    // Edge 13x), and with the network dead a runaway script can only burn CPU
    // until the deadline, where the job object kills the process tree.
    let mut command = Command::new(browser);
    command
        .arg("--headless")
        .arg("--disable-gpu")
        .arg("--no-first-run")
        .arg("--hide-scrollbars")
        .arg("--proxy-server=127.0.0.1:0")
        .arg("--proxy-bypass-list=<-loopback>")
        .arg("--window-size=1280,2000")
        .arg("--default-background-color=FFFFFFFF")
        .arg(format!("--user-data-dir={}", profile_dir.display()))
        .arg(format!("--screenshot={}", screenshot.display()));
    for extra in &crate::config::runtime().html_renderer_args {
        command.arg(extra);
    }
    command
        .arg(url)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => {
            tracing::error!(error = %err, browser = %browser.display(), "failed to launch headless browser");
            return None;
        }
    };
    // Chromium is a process tree, and on Windows msedge.exe is a launcher
    // whose real browser detaches from it entirely; killing the direct child
    // leaves the tree running. A kill-on-close job object captures every
    // descendant, so dropping the guard (any return path) reaps them all.
    let _job = crate::process_tree::JobGuard::assign(&child);

    // Poll instead of wait() so a wedged renderer cannot stall a scan worker
    // forever; these run inside spawn_blocking, so sleeping here is fine.
    let deadline = Instant::now() + std::time::Duration::from_secs(30);
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    tracing::error!(path = %path.display(), "headless browser timed out rendering HTML");
                    return None;
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            Err(err) => {
                tracing::error!(error = %err, path = %path.display(), "failed to wait for headless browser");
                return None;
            }
        }
    };
    if !status.success() {
        tracing::error!(status = %status, path = %path.display(), "headless browser exited with an error");
        return None;
    }
    // On Windows the spawned executable can be a launcher that exits at once
    // while a detached browser process writes the screenshot, so poll until
    // the file decodes (a partially written PNG fails to decode) or time is
    // up.
    let mut last_err = None;
    while Instant::now() < deadline {
        match open_image(screenshot) {
            Ok(image) => return Some(image),
            Err(err) => last_err = Some(err),
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    tracing::error!(error = ?last_err, path = %path.display(), "failed to read HTML screenshot");
    None
}

static LABEL_FONT: OnceLock<Option<FontVec>> = OnceLock::new();

/// Lazily loads a system font for thumbnail text. Mirrors the Python code,
/// which tries arial.ttf and degrades gracefully: when no font is found the
/// text helpers become no-ops and images are produced without labels.
fn label_font() -> Option<&'static FontVec> {
    LABEL_FONT
        .get_or_init(|| {
            let mut candidates: Vec<PathBuf> = Vec::new();
            if let Some(custom) = &crate::config::runtime().thumbnail_font {
                candidates.push(custom.clone());
            }
            candidates.extend(
                [
                    r"C:\Windows\Fonts\segoeui.ttf",
                    r"C:\Windows\Fonts\arial.ttf",
                    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                    "/usr/share/fonts/TTF/DejaVuSans.ttf",
                    "/usr/share/fonts/dejavu/DejaVuSans.ttf",
                ]
                .iter()
                .map(PathBuf::from),
            );
            for candidate in candidates {
                if let Ok(bytes) = fs::read(&candidate) {
                    match FontVec::try_from_vec(bytes) {
                        Ok(font) => return Some(font),
                        Err(err) => {
                            tracing::debug!(
                                error = %err,
                                path = %candidate.display(),
                                "failed to parse font file"
                            );
                        }
                    }
                }
            }
            tracing::warn!("no usable system font found; thumbnail text labels will be omitted");
            None
        })
        .as_ref()
}

fn draw_text(image: &mut RgbImage, text: &str, x: i32, y: i32, scale: f32, color: Rgb<u8>) {
    if text.is_empty() {
        return;
    }
    let Some(font) = label_font() else {
        return;
    };
    draw_text_mut(image, color, x, y, PxScale::from(scale), font, text);
}

fn draw_label(image: &mut RgbImage, text: &str, x: i32, y: i32, scale: f32) {
    // Python draws the outline as eight 1px-offset black copies of the text
    // underneath the white text; replicate that exactly.
    for dx in -1..=1 {
        for dy in -1..=1 {
            if dx != 0 || dy != 0 {
                draw_text(image, text, x + dx, y + dy, scale, Rgb([0, 0, 0]));
            }
        }
    }
    draw_text(image, text, x, y, scale, Rgb([255, 255, 255]));
}

/// Writes the mime type bottom-left, matching Python's write_text_on_image
/// (font size 20, 10px margins). Applied to copies before encoding so the
/// stored clean frames are never mutated.
fn overlay_mime_label(image: DynamicImage, mime_type: &str) -> DynamicImage {
    let mut rgb = image.into_rgb8();
    let font_size = 20.0f32;
    let y = rgb.height() as i32 - font_size as i32 - 10;
    draw_label(&mut rgb, mime_type, 10, y, font_size);
    DynamicImage::ImageRgb8(rgb)
}

/// Returns embedded cover art when the file has any, otherwise a generated
/// placeholder. Infallible: tag read failures degrade to a placeholder with
/// empty metadata, matching the Python get_audio_thumbnail.
fn get_audio_thumbnail(path: &Path, mime_type: &str) -> DynamicImage {
    let mut artist = String::new();
    let mut album = String::new();
    let mut title = String::new();
    match lofty::read_from_path(path) {
        Ok(tagged) => {
            if let Some(tag) = tagged.primary_tag().or_else(|| tagged.first_tag()) {
                artist = tag
                    .artist()
                    .map(|value| value.to_string())
                    .unwrap_or_default();
                album = tag
                    .album()
                    .map(|value| value.to_string())
                    .unwrap_or_default();
                title = tag
                    .title()
                    .map(|value| value.to_string())
                    .unwrap_or_default();
                if let Some(picture) = tag.pictures().first() {
                    if let Ok(cover) = decode_image_bytes(picture.data()) {
                        // Cover art gets no text overlay, but is capped in
                        // size: embedded art can be arbitrarily large and
                        // would otherwise be stored full-resolution in the
                        // database.
                        return downscale_cover_art(cover);
                    }
                }
            }
        }
        Err(err) => {
            tracing::debug!(
                error = %err,
                path = %path.display(),
                "failed to read audio tags for thumbnail"
            );
        }
    }
    build_audio_placeholder(mime_type, &artist, &album, &title)
}

/// Caps cover art at the placeholder's dimensions. Matches the 1024x1024
/// canvas used by `build_audio_placeholder`, so stored audio thumbnails have
/// a consistent upper bound regardless of the embedded art's resolution.
fn downscale_cover_art(cover: DynamicImage) -> DynamicImage {
    const MAX_COVER_DIM: u32 = 1024;
    let (width, height) = cover.dimensions();
    if width <= MAX_COVER_DIM && height <= MAX_COVER_DIM {
        return cover;
    }
    cover.resize(
        MAX_COVER_DIM,
        MAX_COVER_DIM,
        image::imageops::FilterType::Lanczos3,
    )
}

fn build_audio_placeholder(
    mime_type: &str,
    artist: &str,
    album: &str,
    title: &str,
) -> DynamicImage {
    let width = 1024u32;
    let height = 1024u32;
    // Fixed colors within the ranges Python randomizes.
    let top = [35.0f32, 35.0, 75.0];
    let bottom = [175.0f32, 225.0, 225.0];
    let mut image = RgbImage::new(width, height);
    for y in 0..height {
        let t = y as f32 / height as f32;
        let pixel = Rgb([
            (top[0] + (bottom[0] - top[0]) * t) as u8,
            (top[1] + (bottom[1] - top[1]) * t) as u8,
            (top[2] + (bottom[2] - top[2]) * t) as u8,
        ]);
        for x in 0..width {
            image.put_pixel(x, y, pixel);
        }
    }

    if let Some(font) = label_font() {
        let note = "\u{266a}";
        let scale = PxScale::from(400.0);
        let (note_w, note_h) = text_size(scale, font, note);
        let x = (width as i32 - note_w as i32) / 2;
        let y = (height as i32 - note_h as i32) / 2;
        draw_text_mut(&mut image, Rgb([255, 255, 255]), x, y, scale, font, note);
    }

    draw_text(
        &mut image,
        mime_type,
        10,
        height as i32 - 60,
        50.0,
        Rgb([255, 255, 255]),
    );
    draw_text(&mut image, artist, 10, 10, 60.0, Rgb([255, 255, 255]));
    draw_text(&mut image, album, 10, 80, 60.0, Rgb([255, 255, 255]));
    draw_text(&mut image, title, 10, 150, 60.0, Rgb([255, 255, 255]));

    DynamicImage::ImageRgb8(image)
}

fn build_image_grid(frames: &[DynamicImage]) -> DynamicImage {
    let frame = &frames[0];
    let (w, h) = frame.dimensions();
    let cols = 2_u32;
    let rows = 2_u32;
    let mut grid = image::RgbImage::new(w * cols, h * rows);

    for (idx, frame) in frames.iter().take(4).enumerate() {
        let idx = idx as u32;
        let x = (idx % cols) * w;
        let y = (idx / cols) * h;
        let resized = frame.resize(w, h, image::imageops::FilterType::Lanczos3);
        let rgb = resized.to_rgb8();
        image::imageops::overlay(&mut grid, &rgb, x as i64, y as i64);
    }

    DynamicImage::ImageRgb8(grid)
}

fn extract_video_frames(
    path: &Path,
    num_frames: usize,
    duration: f64,
) -> Result<Vec<DynamicImage>, FileProcessError> {
    if duration <= 0.0 {
        return Ok(Vec::new());
    }

    let interval = duration / num_frames as f64;
    let temp_dir = temp_dir_path();
    fs::create_dir_all(&temp_dir).map_err(|err| FileProcessError::Io(err.to_string()))?;

    let result = extract_video_frames_into(path, num_frames, interval, &temp_dir);
    if let Err(err) = fs::remove_dir_all(&temp_dir) {
        tracing::debug!(error = %err, path = %temp_dir.display(), "failed to remove temp frame dir");
    }
    result
}

/// Renders the last portion of a captured stderr for error messages, keeping
/// diagnostics without dumping pages of encoder output into the log.
pub(crate) fn stderr_tail(stderr: &[u8]) -> String {
    const MAX_LEN: usize = 500;
    let text = String::from_utf8_lossy(stderr);
    let trimmed = text.trim();
    match trimmed.char_indices().nth_back(MAX_LEN - 1) {
        Some((idx, _)) => format!("...{}", &trimmed[idx..]),
        None => trimmed.to_string(),
    }
}

fn extract_video_frames_into(
    path: &Path,
    num_frames: usize,
    interval: f64,
    temp_dir: &Path,
) -> Result<Vec<DynamicImage>, FileProcessError> {
    let output_pattern = temp_dir.join("frame_%04d.png");
    // stdout is silenced, but stderr is captured so a failure can say why
    // (corrupt file, missing codec, disk full) instead of just "ffmpeg
    // failed"; it is only surfaced on a non-zero exit.
    let output = Command::new(crate::media_tools::ffmpeg())
        .arg("-i")
        .arg(path)
        .arg("-vf")
        .arg(format!("fps=1/{}", interval))
        .arg("-vsync")
        .arg("vfr")
        .arg(&output_pattern)
        .stdout(std::process::Stdio::null())
        .output()
        .map_err(|err| FileProcessError::Unsupported(err.to_string()))?;

    if !output.status.success() {
        return Err(FileProcessError::Unsupported(format!(
            "ffmpeg failed: {}",
            stderr_tail(&output.stderr)
        )));
    }

    let mut frames = Vec::new();
    let mut entries =
        fs::read_dir(temp_dir).map_err(|err| FileProcessError::Io(err.to_string()))?;
    let mut paths = Vec::new();
    while let Some(entry) = entries.next() {
        let entry = entry.map_err(|err| FileProcessError::Io(err.to_string()))?;
        if entry.path().extension().and_then(|ext| ext.to_str()) == Some("png") {
            paths.push(entry.path());
        }
    }
    paths.sort();

    for frame_path in paths.into_iter().take(num_frames) {
        if let Ok(image) = open_image(&frame_path) {
            frames.push(image);
        }
    }

    Ok(frames)
}

// Unread fields mirror the ffprobe JSON schema and are kept for Debug output.
#[derive(Debug, Deserialize)]
struct FfprobeStream {
    #[allow(dead_code)]
    index: Option<u64>,
    codec_type: Option<String>,
    #[allow(dead_code)]
    codec_name: Option<String>,
    duration: Option<String>,
    width: Option<u64>,
    height: Option<u64>,
    #[allow(dead_code)]
    tags: Option<FfprobeTags>,
}

#[derive(Debug, Deserialize)]
struct FfprobeTags {
    #[allow(dead_code)]
    language: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FfprobeFormat {
    duration: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FfprobeOutput {
    streams: Vec<FfprobeStream>,
    format: Option<FfprobeFormat>,
}

struct AudioTrack {
    duration: f64,
}

struct VideoTrack {
    duration: f64,
    width: Option<u64>,
    height: Option<u64>,
}

struct SubtitleTrack;

struct MediaInfo {
    audio_tracks: Vec<AudioTrack>,
    video_track: Option<VideoTrack>,
    subtitle_tracks: Vec<SubtitleTrack>,
}

fn extract_media_info(path: &Path) -> Result<MediaInfo, FileProcessError> {
    let output = Command::new(crate::media_tools::ffprobe())
        .arg("-v")
        .arg("error")
        .arg("-show_entries")
        .arg("stream=index,codec_type,codec_name,duration,width,height,tags:format=duration")
        .arg("-of")
        .arg("json")
        .arg(path)
        .output()
        .map_err(|err| FileProcessError::Unsupported(err.to_string()))?;

    if !output.status.success() {
        return Err(FileProcessError::Unsupported("ffprobe failed".to_string()));
    }

    let data: FfprobeOutput = serde_json::from_slice(&output.stdout)
        .map_err(|err| FileProcessError::Unsupported(err.to_string()))?;

    let format_duration = data
        .format
        .and_then(|format| format.duration)
        .and_then(|duration| duration.parse::<f64>().ok())
        .unwrap_or(0.0);

    let mut audio_tracks = Vec::new();
    let mut video_track = None;
    let mut subtitle_tracks = Vec::new();

    // Streams sometimes report a zero duration; fall back to the container's
    // format duration in that case, as ffprobe does not always populate both.
    let stream_duration = |stream: &FfprobeStream| {
        stream
            .duration
            .as_deref()
            .and_then(|value| value.parse::<f64>().ok())
            .filter(|value| *value > 0.0)
            .unwrap_or(format_duration)
    };

    for stream in data.streams {
        match stream.codec_type.as_deref() {
            Some("audio") => {
                audio_tracks.push(AudioTrack {
                    duration: stream_duration(&stream),
                });
            }
            Some("video") => {
                video_track = Some(VideoTrack {
                    duration: stream_duration(&stream),
                    width: stream.width,
                    height: stream.height,
                });
            }
            Some("subtitle") => {
                subtitle_tracks.push(SubtitleTrack);
            }
            _ => {}
        }
    }

    Ok(MediaInfo {
        audio_tracks,
        video_track,
        subtitle_tracks,
    })
}

fn calculate_hashes(path: &Path) -> Result<(String, String, i64), io::Error> {
    let mut file = fs::File::open(path)?;
    let mut md5 = Md5::new();
    let mut sha = Sha256::new();
    let mut total_size = 0_i64;
    let mut buffer = vec![0u8; 4096];

    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        total_size += read as i64;
        md5.update(&buffer[..read]);
        sha.update(&buffer[..read]);
    }

    let md5 = format!("{:x}", md5.finalize());
    let sha256 = format!("{:x}", sha.finalize());
    Ok((md5, sha256, total_size))
}

pub(crate) fn get_last_modified_time_and_size(path: &Path) -> Result<(String, i64), io::Error> {
    let metadata = fs::metadata(path)?;
    let size = metadata.len() as i64;
    let modified = metadata.modified()?;
    let formatted = format_system_time(modified)
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "unrepresentable mtime"))?;
    Ok((formatted, size))
}

/// Formats a filesystem timestamp with the same truncation and format used for
/// `files.last_modified`, so strings from disk and from the DB compare equal.
pub(crate) fn format_system_time(time: std::time::SystemTime) -> Option<String> {
    let duration = time.duration_since(std::time::UNIX_EPOCH).ok()?;
    let seconds = duration.as_secs() as i64;
    let dt = OffsetDateTime::from_unix_timestamp(seconds).ok()?;
    dt.format(iso_format()).ok()
}

pub(crate) fn current_iso_timestamp() -> String {
    let now = OffsetDateTime::now_local().unwrap_or_else(|_| OffsetDateTime::now_utc());
    now.format(iso_format())
        .unwrap_or_else(|_| OffsetDateTime::now_utc().format(iso_format()).unwrap())
}

fn iso_format() -> &'static [FormatItem<'static>] {
    static ISO_FORMAT: std::sync::OnceLock<Vec<FormatItem<'static>>> = std::sync::OnceLock::new();
    ISO_FORMAT.get_or_init(|| {
        time::format_description::parse_borrowed::<2>(
            "[year]-[month]-[day]T[hour]:[minute]:[second]",
        )
        .expect("invalid time format")
    })
}

/// Returns a temp directory path that is unique across processes and process
/// restarts. A bare counter is not enough: after a crash, leftover files from
/// a previous run's `frames-0` would be picked up as the *wrong file's*
/// output (a stale screenshot decodes fine and gets stored keyed by sha256).
fn temp_dir_path() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    static STARTUP_NONCE: OnceLock<u64> = OnceLock::new();
    let nonce = STARTUP_NONCE.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos() as u64)
            .unwrap_or(0)
    });
    let base = crate::config::runtime().temp_dir.clone();
    let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
    base.join(format!("frames-{}-{nonce:x}-{unique}", std::process::id()))
}

pub(crate) fn check_folder_validity(folder: &str) -> bool {
    let path = Path::new(folder);
    if !path.exists() {
        tracing::error!(path = %path.display(), "path does not exist");
        return false;
    }
    if !path.is_dir() {
        tracing::error!(path = %path.display(), "path is not a directory");
        return false;
    }
    match fs::read_dir(path) {
        Ok(mut entries) => {
            if entries.next().is_some() {
                true
            } else {
                tracing::warn!(path = %path.display(), "folder is empty, skipping");
                false
            }
        }
        Err(err) => {
            tracing::error!(error = %err, path = %path.display(), "failed to read directory");
            false
        }
    }
}

pub(crate) fn folder_is_empty(folder: &str) -> bool {
    let path = Path::new(folder);
    path.is_dir()
        && fs::read_dir(path)
            .ok()
            .is_some_and(|mut entries| entries.next().is_none())
}

pub(crate) fn deduplicate_paths(paths: &[String]) -> Vec<String> {
    let mut normalized = paths
        .iter()
        .map(|path| normalize_path(path, true).to_string_lossy().to_string())
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();

    let mut deduped = Vec::new();
    for path in normalized {
        if deduped.last().is_some_and(|last| path.starts_with(last)) {
            continue;
        }
        deduped.push(path);
    }
    deduped
}

pub(crate) fn normalize_path(path: &str, trailing: bool) -> PathBuf {
    let mut buf = PathBuf::from(path.trim());
    if !buf.is_absolute() {
        if let Ok(cwd) = env::current_dir() {
            buf = cwd.join(buf);
        }
    }

    let mut normalized = PathBuf::new();
    for component in buf.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                let _ = normalized.pop();
            }
            Component::Prefix(_) | Component::RootDir => normalized.push(component.as_os_str()),
            Component::Normal(part) => normalized.push(part),
        }
    }

    if trailing {
        normalized.push("");
    }

    normalized
}

pub(crate) fn build_extension_set(config: &SystemConfig) -> HashSet<String> {
    let mut extensions = HashSet::new();
    if config.scan_images {
        for ext in [".jpg", ".jpeg", ".png", ".bmp", ".gif", ".tiff", ".webp"] {
            extensions.insert(ext.to_string());
        }
    }
    if config.scan_video {
        for ext in [".mp4", ".avi", ".mkv", ".mov", ".wmv", ".flv", ".webm"] {
            extensions.insert(ext.to_string());
        }
    }
    if config.scan_audio {
        for ext in [".mp3", ".wav", ".flac", ".aac", ".ogg", ".wma", ".m4a"] {
            extensions.insert(ext.to_string());
        }
    }
    if config.scan_html {
        for ext in [".html", ".htm"] {
            extensions.insert(ext.to_string());
        }
    }
    if config.scan_pdf {
        extensions.insert(".pdf".to_string());
    }
    extensions
}

pub(crate) fn has_allowed_extension(path: &Path, extensions: &HashSet<String>) -> bool {
    let ext = path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| format!(".{}", ext.to_lowercase()));
    match ext {
        Some(ext) => extensions.contains(&ext),
        None => false,
    }
}

pub(crate) fn is_hidden_or_temp(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    name.starts_with('.') || name.starts_with('~')
}

pub(crate) fn is_excluded(path: &Path, excluded: &[PathBuf]) -> bool {
    excluded.iter().any(|prefix| path.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::migrate_databases_on_disk;
    use crate::test_utils::test_data_dir;

    fn next_db_name() -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        format!("testdb_{}", COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    // Folder validity gates which configured folders get scanned: missing
    // paths, non-directories, and empty directories are all skipped (the
    // empty-dir skip matches Python, which never scanned empty folders).
    #[test]
    fn folder_validity_rejects_missing_nondir_and_empty() {
        let root = tempfile::TempDir::new().unwrap();

        let missing = root.path().join("missing");
        assert!(!check_folder_validity(&missing.to_string_lossy()));

        let file_path = root.path().join("file.txt");
        fs::write(&file_path, b"x").unwrap();
        assert!(!check_folder_validity(&file_path.to_string_lossy()));

        let empty_dir = root.path().join("empty");
        fs::create_dir_all(&empty_dir).unwrap();
        assert!(!check_folder_validity(&empty_dir.to_string_lossy()));

        let populated = root.path().join("populated");
        fs::create_dir_all(&populated).unwrap();
        fs::write(populated.join("f.txt"), b"x").unwrap();
        assert!(check_folder_validity(&populated.to_string_lossy()));
    }

    // Ensures rescans persist items, files, and blurhash data.
    #[tokio::test]
    async fn rescan_creates_items_and_files() {
        let test_env = test_data_dir();
        let root = test_env.path();
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();

        let media_dir = root.join("media");
        fs::create_dir_all(&media_dir).unwrap();
        let image_path = media_dir.join("sample.png");
        let image = image::RgbImage::new(8, 8);
        image.save(&image_path).unwrap();

        let store = SystemConfigStore::new(root.to_path_buf());
        let mut config = SystemConfig::default();
        config.included_folders = vec![media_dir.to_string_lossy().to_string()];
        store.save(&index_db, &config).unwrap();

        let service = FileScanService::new(
            index_db.clone(),
            user_data_db.clone(),
            root.to_path_buf(),
            ScanOptions { worker_count: 2 },
        );

        let result = service.rescan_folders().await.unwrap();
        assert!(!result.scan_ids.is_empty());

        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let file_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        let item_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM items")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        let blurhash: Option<(Option<String>,)> =
            sqlx::query_as("SELECT blurhash FROM items LIMIT 1")
                .fetch_optional(&mut conn)
                .await
                .unwrap();

        assert_eq!(file_count.0, 1);
        assert_eq!(item_count.0, 1);
        assert!(blurhash.and_then(|value| value.0).is_some());
    }

    async fn latest_scan_record(conn: &mut sqlx::SqliteConnection) -> (i64, i64, i64, i64, i64) {
        sqlx::query_as(
            r#"
SELECT unchanged_files, new_files, modified_files, errors, marked_unavailable
FROM file_scans
ORDER BY id DESC
LIMIT 1
            "#,
        )
        .fetch_one(conn)
        .await
        .unwrap()
    }

    // Unchanged files must be updated without reprocessing: a file whose
    // contents can no longer be decoded but whose mtime is unchanged has to
    // survive a rescan as "unchanged" instead of being marked unavailable
    // and deleted. Also verifies missing blurhashes are backfilled and that
    // genuinely modified files are replaced.
    #[tokio::test]
    async fn rescan_skips_unchanged_files_and_backfills() {
        let test_env = test_data_dir();
        let root = test_env.path();
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();

        let media_dir = root.join("media");
        fs::create_dir_all(&media_dir).unwrap();
        let image_path = media_dir.join("sample.png");
        let image = image::RgbImage::new(8, 8);
        image.save(&image_path).unwrap();

        let store = SystemConfigStore::new(root.to_path_buf());
        let mut config = SystemConfig::default();
        config.included_folders = vec![media_dir.to_string_lossy().to_string()];
        store.save(&index_db, &config).unwrap();

        let service = FileScanService::new(
            index_db.clone(),
            user_data_db.clone(),
            root.to_path_buf(),
            ScanOptions { worker_count: 2 },
        );

        service.rescan_folders().await.unwrap();
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        // The first rescan triggers a folder update (which scans the new
        // folder) followed by a full rescan: the file is new in the first
        // pass and already unchanged in the second.
        let totals: (i64, i64, i64) = sqlx::query_as(
            "SELECT SUM(new_files), SUM(unchanged_files), SUM(errors) FROM file_scans",
        )
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(totals, (1, 1, 0));
        let original_sha: (String,) = sqlx::query_as("SELECT sha256 FROM files LIMIT 1")
            .fetch_one(&mut conn)
            .await
            .unwrap();

        // A missing blurhash is backfilled on the next scan without the file
        // counting as new or modified.
        drop(conn);
        let mut write_conn = crate::db::open_index_db_write_no_user_data(&index_db)
            .await
            .unwrap();
        sqlx::query("UPDATE items SET blurhash = NULL")
            .execute(&mut write_conn)
            .await
            .unwrap();
        drop(write_conn);
        service.rescan_folders().await.unwrap();
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let (unchanged, new_files, modified, errors, marked) = latest_scan_record(&mut conn).await;
        assert_eq!(
            (unchanged, new_files, modified, errors, marked),
            (1, 0, 0, 0, 0)
        );
        let blurhash: (Option<String>,) = sqlx::query_as("SELECT blurhash FROM items LIMIT 1")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert!(blurhash.0.is_some());
        drop(conn);

        // Corrupt the contents but keep the mtime: the scan must treat the
        // file as unchanged and never attempt to decode it.
        let mtime = fs::metadata(&image_path).unwrap().modified().unwrap();
        fs::write(&image_path, b"this is not a png").unwrap();
        fs::File::options()
            .write(true)
            .open(&image_path)
            .unwrap()
            .set_modified(mtime)
            .unwrap();
        service.rescan_folders().await.unwrap();
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let (unchanged, new_files, modified, errors, marked) = latest_scan_record(&mut conn).await;
        assert_eq!(
            (unchanged, new_files, modified, errors, marked),
            (1, 0, 0, 0, 0)
        );
        let row: (String, i64) = sqlx::query_as("SELECT sha256, available FROM files LIMIT 1")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(row.0, original_sha.0);
        assert_eq!(row.1, 1);
        drop(conn);

        // A genuinely modified file (new content, newer mtime) is replaced and
        // the orphaned item is cleaned up.
        let new_image = image::RgbImage::new(16, 16);
        new_image.save(&image_path).unwrap();
        fs::File::options()
            .write(true)
            .open(&image_path)
            .unwrap()
            .set_modified(mtime + std::time::Duration::from_secs(10))
            .unwrap();
        service.rescan_folders().await.unwrap();
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let (unchanged, new_files, modified, errors, _) = latest_scan_record(&mut conn).await;
        assert_eq!((unchanged, new_files, modified, errors), (0, 0, 1, 0));
        let row: (String,) = sqlx::query_as("SELECT sha256 FROM files LIMIT 1")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_ne!(row.0, original_sha.0);
        let item_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM items")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(item_count.0, 1);
    }

    #[test]
    fn served_directly_matches_the_thumbnail_decision() {
        // Small file: never thumbnailed, whatever the dimensions.
        assert!(image_is_served_directly(SMALL_IMAGE_FILE_SIZE, 9000, 9000));
        // Large pixels or a large file force a thumbnail.
        assert!(!image_is_served_directly(
            SMALL_IMAGE_FILE_SIZE + 1,
            MAX_SERVED_IMAGE_DIMENSION + 1,
            10
        ));
        assert!(!image_is_served_directly(
            MAX_SERVED_IMAGE_FILE_SIZE + 1,
            10,
            10
        ));
        // The common case that used to be re-decoded on every scan: bigger
        // than the small-file cutoff, but modest in both other dimensions.
        assert!(image_is_served_directly(
            SMALL_IMAGE_FILE_SIZE + 1,
            MAX_SERVED_IMAGE_DIMENSION,
            MAX_SERVED_IMAGE_DIMENSION
        ));
        assert!(image_is_served_directly(MAX_SERVED_IMAGE_FILE_SIZE, 10, 10));
    }

    // An image that is served from its original file stores no thumbnail, so
    // `has_thumbnail` stays false for it forever. The backfill must decide
    // from the indexed dimensions instead of decoding the file again on every
    // scan; the on-disk contents are swapped for an image that *would* need a
    // thumbnail, so any stray decode leaves a visible thumbnail row.
    #[tokio::test]
    async fn rescan_does_not_redecode_directly_served_images() {
        let test_env = test_data_dir();
        let root = test_env.path();
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();

        // A dedicated folder: the temp root is shared by every test in the
        // process, so a leftover file would show up in another scan.
        let media_dir = root.join("media-served-directly");
        fs::create_dir_all(&media_dir).unwrap();
        let image_path = media_dir.join("large.bmp");
        // 1400x1400 uncompressed = 5.88 MB: over the small-file cutoff, but
        // well inside the dimension and file-size limits.
        image::RgbImage::new(1400, 1400).save(&image_path).unwrap();

        let store = SystemConfigStore::new(root.to_path_buf());
        let mut config = SystemConfig::default();
        config.included_folders = vec![media_dir.to_string_lossy().to_string()];
        store.save(&index_db, &config).unwrap();

        let service = FileScanService::new(
            index_db.clone(),
            user_data_db.clone(),
            root.to_path_buf(),
            ScanOptions { worker_count: 2 },
        );

        service.rescan_folders().await.unwrap();
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let thumbnails: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM storage.thumbnails")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(thumbnails.0, 0);
        drop(conn);

        // Same mtime, same byte count, but 4900 pixels wide: a decode would
        // now produce a thumbnail. The indexed dimensions still say 1400x1400.
        let mtime = fs::metadata(&image_path).unwrap().modified().unwrap();
        image::RgbImage::new(4900, 400).save(&image_path).unwrap();
        fs::File::options()
            .write(true)
            .open(&image_path)
            .unwrap()
            .set_modified(mtime)
            .unwrap();

        service.rescan_folders().await.unwrap();
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let (unchanged, new_files, modified, errors, marked) = latest_scan_record(&mut conn).await;
        assert_eq!(
            (unchanged, new_files, modified, errors, marked),
            (1, 0, 0, 0, 0)
        );
        let thumbnails: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM storage.thumbnails")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(thumbnails.0, 0, "the backfill re-decoded the image");
    }

    // VACUUM must run outside the writer's usual transaction wrapper; both
    // maintenance messages have to succeed against a real on-disk database.
    #[tokio::test]
    async fn vacuum_and_analyze_writer_messages_succeed() {
        let test_env = test_data_dir();
        let root = test_env.path();
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();

        let media_dir = root.join("media");
        fs::create_dir_all(&media_dir).unwrap();
        let image_path = media_dir.join("sample.png");
        let image = image::RgbImage::new(8, 8);
        image.save(&image_path).unwrap();

        let store = SystemConfigStore::new(root.to_path_buf());
        let mut config = SystemConfig::default();
        config.included_folders = vec![media_dir.to_string_lossy().to_string()];
        store.save(&index_db, &config).unwrap();

        let service = FileScanService::new(
            index_db.clone(),
            user_data_db.clone(),
            root.to_path_buf(),
            ScanOptions { worker_count: 2 },
        );
        service.rescan_folders().await.unwrap();

        call_index_db_writer(&index_db, |reply| IndexDbWriterMessage::Vacuum { reply })
            .await
            .unwrap();
        call_index_db_writer(&index_db, |reply| IndexDbWriterMessage::Analyze { reply })
            .await
            .unwrap();

        // The writer connection must remain usable after maintenance.
        let scan_id = call_index_db_writer(&index_db, |reply| IndexDbWriterMessage::AddFileScan {
            scan_time: current_iso_timestamp(),
            path: media_dir.to_string_lossy().to_string(),
            reply,
        })
        .await
        .unwrap();
        assert!(scan_id > 0);
    }

    // An interrupted scan leaves a file_scans row with a NULL end_time; the
    // next scan of the same folder must close it.
    #[tokio::test]
    async fn rescan_closes_stale_open_scans() {
        let test_env = test_data_dir();
        let root = test_env.path();
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();

        let media_dir = root.join("media");
        fs::create_dir_all(&media_dir).unwrap();
        let image_path = media_dir.join("sample.png");
        let image = image::RgbImage::new(8, 8);
        image.save(&image_path).unwrap();

        let store = SystemConfigStore::new(root.to_path_buf());
        let mut config = SystemConfig::default();
        config.included_folders = vec![media_dir.to_string_lossy().to_string()];
        store.save(&index_db, &config).unwrap();

        let service = FileScanService::new(
            index_db.clone(),
            user_data_db.clone(),
            root.to_path_buf(),
            ScanOptions { worker_count: 2 },
        );

        service.rescan_folders().await.unwrap();
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let scan_path: (String,) = sqlx::query_as("SELECT path FROM file_scans LIMIT 1")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        drop(conn);

        let mut write_conn = crate::db::open_index_db_write_no_user_data(&index_db)
            .await
            .unwrap();
        sqlx::query("INSERT INTO file_scans (start_time, path) VALUES (?, ?)")
            .bind("2024-01-01T00:00:00")
            .bind(&scan_path.0)
            .execute(&mut write_conn)
            .await
            .unwrap();
        drop(write_conn);

        service.rescan_folders().await.unwrap();
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let open_scans: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM file_scans WHERE end_time IS NULL")
                .fetch_one(&mut conn)
                .await
                .unwrap();
        assert_eq!(open_scans.0, 0);
    }

    // The folders table is updated before scanning, so a repeated folder
    // update adds no folders and starts no scans.
    #[tokio::test]
    async fn folder_update_is_idempotent() {
        let test_env = test_data_dir();
        let root = test_env.path();
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();

        let media_dir = root.join("media");
        fs::create_dir_all(&media_dir).unwrap();
        let image_path = media_dir.join("sample.png");
        let image = image::RgbImage::new(8, 8);
        image.save(&image_path).unwrap();

        let store = SystemConfigStore::new(root.to_path_buf());
        let mut config = SystemConfig::default();
        config.included_folders = vec![media_dir.to_string_lossy().to_string()];
        store.save(&index_db, &config).unwrap();

        let service = FileScanService::new(
            index_db.clone(),
            user_data_db.clone(),
            root.to_path_buf(),
            ScanOptions { worker_count: 2 },
        );

        let result = service.run_folder_update().await.unwrap();
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let folders = get_folders_from_database(&mut conn, true).await.unwrap();
        drop(conn);
        assert_eq!(folders.len(), 1);
        assert_eq!(result.included_added, folders);
        assert!(!result.scan_ids.is_empty());

        let result = service.run_folder_update().await.unwrap();
        assert!(result.included_added.is_empty());
        assert!(result.scan_ids.is_empty());
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let folders_after = get_folders_from_database(&mut conn, true).await.unwrap();
        assert_eq!(folders_after, folders);
    }

    #[tokio::test]
    async fn folder_update_starts_a_scan_for_a_safe_empty_folder() {
        let test_env = test_data_dir();
        let root = test_env.path();
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();

        let empty_dir = root.join("empty-watch-target");
        fs::create_dir_all(&empty_dir).unwrap();
        let store = SystemConfigStore::new(root.to_path_buf());
        let mut config = SystemConfig::default();
        config.included_folders = vec![empty_dir.to_string_lossy().into_owned()];
        store.save(&index_db, &config).unwrap();

        let service = FileScanService::new(
            index_db.clone(),
            user_data_db.clone(),
            root.to_path_buf(),
            ScanOptions { worker_count: 1 },
        );
        let result = service.run_folder_update().await.unwrap();
        assert_eq!(result.scan_ids.len(), 1);

        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let scans: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM file_scans WHERE path != '<continuous>'")
                .fetch_one(&mut conn)
                .await
                .unwrap();
        assert_eq!(scans, 1);
    }

    // A folder registered by an update that failed before its scan completed
    // must be picked up by the next update, even though INSERT OR IGNORE no
    // longer reports it as newly added.
    #[tokio::test]
    async fn folder_update_scans_registered_but_unscanned_folders() {
        let test_env = test_data_dir();
        let root = test_env.path();
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();

        let media_dir = root.join("media");
        fs::create_dir_all(&media_dir).unwrap();
        let image_path = media_dir.join("sample.png");
        let image = image::RgbImage::new(8, 8);
        image.save(&image_path).unwrap();

        let store = SystemConfigStore::new(root.to_path_buf());
        let mut config = SystemConfig::default();
        config.included_folders = vec![media_dir.to_string_lossy().to_string()];
        store.save(&index_db, &config).unwrap();

        // Simulate an update that committed the folder registration and then
        // failed before completing a scan.
        call_index_db_writer(&index_db, |reply| {
            IndexDbWriterMessage::AddFolderToDatabase {
                time_added: "2024-01-01T00:00:00".to_string(),
                path: media_dir.to_string_lossy().to_string(),
                included: true,
                reply,
            }
        })
        .await
        .unwrap();

        let service = FileScanService::new(
            index_db.clone(),
            user_data_db.clone(),
            root.to_path_buf(),
            ScanOptions { worker_count: 2 },
        );

        let result = service.run_folder_update().await.unwrap();
        assert!(
            !result.scan_ids.is_empty(),
            "stranded folder was not scanned"
        );
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let files: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(files.0, 1);
    }

    // A file that is not valid audio must still yield a 1024x1024 placeholder.
    #[test]
    fn audio_thumbnail_falls_back_to_placeholder() {
        let test_env = test_data_dir();
        let path = test_env.path().join("not_audio.mp3");
        fs::write(&path, b"definitely not audio data").unwrap();
        let thumb = get_audio_thumbnail(&path, "audio/mpeg");
        assert_eq!(thumb.dimensions(), (1024, 1024));
    }

    /// Builds a minimal valid mono 16-bit PCM WAV byte stream.
    fn minimal_wav_bytes() -> Vec<u8> {
        let samples: [u8; 8] = [0; 8];
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"RIFF");
        bytes.extend_from_slice(&(36u32 + samples.len() as u32).to_le_bytes());
        bytes.extend_from_slice(b"WAVE");
        bytes.extend_from_slice(b"fmt ");
        bytes.extend_from_slice(&16u32.to_le_bytes());
        bytes.extend_from_slice(&1u16.to_le_bytes()); // PCM
        bytes.extend_from_slice(&1u16.to_le_bytes()); // mono
        bytes.extend_from_slice(&8000u32.to_le_bytes()); // sample rate
        bytes.extend_from_slice(&16000u32.to_le_bytes()); // byte rate
        bytes.extend_from_slice(&2u16.to_le_bytes()); // block align
        bytes.extend_from_slice(&16u16.to_le_bytes()); // bits per sample
        bytes.extend_from_slice(b"data");
        bytes.extend_from_slice(&(samples.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&samples);
        bytes
    }

    // Embedded cover art must be returned as the thumbnail unchanged.
    #[test]
    fn audio_thumbnail_uses_embedded_cover_art() {
        use lofty::config::WriteOptions;
        use lofty::picture::{MimeType, Picture, PictureType};
        use lofty::prelude::TagExt;
        use lofty::tag::{Tag, TagType};

        let test_env = test_data_dir();
        let path = test_env.path().join("with_cover.wav");
        fs::write(&path, minimal_wav_bytes()).unwrap();

        let cover = image::RgbImage::from_pixel(6, 4, Rgb([255, 0, 0]));
        let mut png_bytes = Vec::new();
        DynamicImage::ImageRgb8(cover)
            .write_to(
                &mut io::Cursor::new(&mut png_bytes),
                image::ImageFormat::Png,
            )
            .unwrap();

        let mut tag = Tag::new(TagType::Id3v2);
        tag.push_picture(Picture::new_unchecked(
            PictureType::CoverFront,
            Some(MimeType::Png),
            None,
            png_bytes,
        ));
        tag.save_to_path(&path, WriteOptions::default()).unwrap();

        let thumb = get_audio_thumbnail(&path, "audio/wav");
        assert_eq!(thumb.dimensions(), (6, 4));
        assert_eq!(thumb.to_rgb8().get_pixel(0, 0), &Rgb([255, 0, 0]));
    }

    // Text drawing must never panic, with or without a usable system font.
    #[test]
    fn draw_label_does_not_panic() {
        let mut image = RgbImage::new(64, 64);
        draw_label(&mut image, "video/mp4", 10, 34, 20.0);
    }

    // A minimal one-page PDF (200x100pt, no content stream) with a consistent
    // xref table, built programmatically so the byte offsets stay correct.
    fn minimal_pdf_bytes() -> Vec<u8> {
        let objects = [
            "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
            "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
            "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 100] >>\nendobj\n",
        ];
        let mut pdf = String::from("%PDF-1.4\n");
        let mut offsets = Vec::new();
        for object in objects {
            offsets.push(pdf.len());
            pdf.push_str(object);
        }
        let xref_offset = pdf.len();
        pdf.push_str("xref\n0 4\n0000000000 65535 f \n");
        for offset in offsets {
            pdf.push_str(&format!("{offset:010} 00000 n \n"));
        }
        pdf.push_str(&format!(
            "trailer\n<< /Size 4 /Root 1 0 R >>\nstartxref\n{xref_offset}\n%%EOF\n"
        ));
        pdf.into_bytes()
    }

    // PDF rendering depends on the pdfium dynamic library, which may not be
    // installed; the scan pipeline (and this test) must degrade gracefully.
    #[test]
    fn render_pdf_first_page_renders_when_pdfium_available() {
        if pdfium().is_none() {
            eprintln!("pdfium not available, skipping");
            return;
        }
        let test_env = test_data_dir();
        let path = test_env.path().join("minimal.pdf");
        fs::write(&path, minimal_pdf_bytes()).unwrap();

        let page = render_pdf_first_page(&path).expect("first page should render");
        assert!(page.width() > 0 && page.height() > 0);
        // scale_page_by_factor(2.0) doubles the page's point size.
        assert_eq!(page.dimensions(), (400, 200));
    }

    // HTML rendering depends on a locally installed Chromium-family browser;
    // the scan pipeline (and this test) must degrade gracefully without one.
    #[test]
    fn render_html_screenshot_captures_page_when_browser_available() {
        if html_renderer().is_none() {
            eprintln!("no headless browser available, skipping");
            return;
        }
        let test_env = test_data_dir();
        let path = test_env.path().join("sample.html");
        fs::write(
            &path,
            "<html><body style=\"background:#ff0000\"><h1>hello</h1></body></html>",
        )
        .unwrap();

        let shot = render_html_screenshot(&path).expect("screenshot should render");
        // --window-size fixes the viewport width; the height can vary.
        assert_eq!(shot.width(), 1280);
    }
}
