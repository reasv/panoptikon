use std::{
    collections::HashSet,
    env, fs,
    io::{self, Read},
    path::{Component, Path, PathBuf},
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use blurhash::encode as blurhash_encode;
use image::ColorType;
use image::codecs::jpeg::JpegEncoder;
use image::{DynamicImage, GenericImageView};
use md5::{Digest, Md5};
use mime_guess::MimeGuess;
use serde::Deserialize;
use sha2::Sha256;
use time::{OffsetDateTime, format_description::FormatItem};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use walkdir::WalkDir;

use crate::{
    api_error::ApiError,
    db::{
        file_scans::{
            FileScanUpdate, get_open_file_scan_id,
        },
        files::{
            FileScanData, FileUpsertResult, ItemScanMeta,
            get_file_by_path, get_item_id, get_item_visual_meta, has_blurhash,
        },
        folders::{
            get_folders_from_database,
        },
        index_writer::{call_index_db_writer, IndexDbWriterMessage},
        open_index_db_read,
        storage::{
            StoredImage, get_frames_bytes, get_thumbnail_bytes, has_frame, has_thumbnail,
        },
        system_config::{SystemConfig, SystemConfigStore},
    },
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
    pub scan_ids: Vec<i64>,
    pub unavailable_files_deleted: u64,
    pub orphan_items_deleted: u64,
    pub rule_files_deleted: u64,
}

pub(crate) struct FolderUpdateResult {
    pub included_deleted: u64,
    pub excluded_deleted: u64,
    pub included_added: Vec<String>,
    pub excluded_added: Vec<String>,
    pub unavailable_files_deleted: u64,
    pub excluded_folder_files_deleted: u64,
    pub orphan_files_deleted: u64,
    pub orphan_items_deleted: u64,
    pub rule_files_deleted: u64,
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

        Ok(RescanResult {
            scan_ids,
            unavailable_files_deleted,
            orphan_items_deleted,
            rule_files_deleted,
        })
    }

    pub(crate) async fn run_folder_update(&self) -> ApiResult<FolderUpdateResult> {
        let config = self.config_store.load(&self.index_db)?;
        self.config_store.save(&self.index_db, &config)?;

        let included_deleted = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::DeleteFoldersNotInList {
                folder_paths: config.included_folders.clone(),
                included: true,
                reply,
            }
        })
        .await?;
        let excluded_deleted = call_index_db_writer(&self.index_db, |reply| {
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
        let mut excluded_added = Vec::new();
        for folder in &config.excluded_folders {
            let inserted = call_index_db_writer(&self.index_db, |reply| {
                IndexDbWriterMessage::AddFolderToDatabase {
                    time_added: scan_time.clone(),
                    path: folder.clone(),
                    included: false,
                    reply,
                }
            })
            .await?;
            if inserted {
                excluded_added.push(folder.clone());
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
            included_deleted,
            excluded_deleted,
            included_added,
            excluded_added,
            unavailable_files_deleted,
            excluded_folder_files_deleted,
            orphan_files_deleted,
            orphan_items_deleted,
            rule_files_deleted,
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
    let mut all_included = included_folders.to_vec();
    all_included.retain(|folder| check_folder_validity(folder));
    let starting_points = deduplicate_paths(&all_included);

    // Scans interrupted before completion leave rows with a NULL end_time;
    // close them so they are not reported as still running.
    let mut conn = open_index_db_read(index_db, user_data_db).await?;
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
        let scan_id = call_index_db_writer(index_db, |reply| {
            IndexDbWriterMessage::AddFileScan {
                scan_time: scan_time.clone(),
                path: folder.clone(),
                reply,
            }
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

        call_index_db_writer(index_db, |reply| {
            IndexDbWriterMessage::UpdateFileScan {
                scan_id,
                update: FileScanUpdate {
                    end_time: current_iso_timestamp(),
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
            }
        })
        .await?;
    }

    Ok(scan_ids)
}

pub(crate) const THUMBNAIL_PROCESS_VERSION: i64 = 1;
pub(crate) const FRAME_PROCESS_VERSION: i64 = 1;
/// Images at or below this size never get a stored thumbnail.
const SMALL_IMAGE_FILE_SIZE: u64 = 5 * 1024 * 1024;

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
    hash_time: f64,
}

struct NewItemData {
    path: PathBuf,
    last_modified: String,
    file_size: i64,
    sha256: String,
    mime_type: String,
    metadata: ItemScanMeta,
    metadata_time: f64,
    thumb_time: f64,
    blurhash_time: f64,
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
    thumb_time: f64,
    blurhash_time: f64,
}

struct FailedFile {
    path: PathBuf,
    hash_time: f64,
    metadata_time: f64,
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
    stats: FolderStats,
    error_paths: Vec<String>,
    conn: sqlx::SqliteConnection,
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
        stats: FolderStats::new(),
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
        while let Some(joined) = ctx.tasks.try_join_next() {
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
    }

    while let Some(joined) = ctx.tasks.join_next().await {
        ctx.handle_joined(joined).await?;
    }

    let ScanContext {
        mut stats,
        error_paths,
        ..
    } = ctx;

    let (marked_unavailable, total_available) =
        call_index_db_writer(index_db, |reply| IndexDbWriterMessage::MarkUnavailableFiles {
            scan_id,
            path: folder.to_string(),
            excluded_paths: error_paths.clone(),
            reply,
        })
        .await?;
    stats.marked_unavailable = marked_unavailable;
    stats.total_available = total_available;

    Ok(stats)
}

impl ScanContext {
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
        joined: Result<TaskOutcome, tokio::task::JoinError>,
    ) -> ApiResult<()> {
        match joined {
            Ok(outcome) => self.handle_outcome(outcome).await,
            Err(err) => {
                tracing::error!(error = %err, "file processing task failed");
                self.stats.errors += 1;
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
                self.stats.hashing_time += failed.hash_time;
                self.stats.metadata_time += failed.metadata_time;
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
            hash_time,
        } = hashed;
        self.stats.hashing_time += hash_time;
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
        self.stats.metadata_time += item.metadata_time;
        self.stats.thumbgen_time += item.thumb_time;
        self.stats.blurhash_time += item.blurhash_time;

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
            if let Err(err) = call_index_db_writer(&self.index_db, |reply| {
                IndexDbWriterMessage::StoreFrames {
                    sha256: item.sha256.clone(),
                    mime_type: item.mime_type.clone(),
                    process_version: FRAME_PROCESS_VERSION,
                    frames: item.frames.clone(),
                    reply,
                }
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
        self.stats.thumbgen_time += backfill.thumb_time;
        self.stats.blurhash_time += backfill.blurhash_time;

        // Storage failures for backfilled visuals are logged and skipped so a
        // single bad file cannot abort the scan; the next scan retries them.
        if !backfill.thumbnails.is_empty() {
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

        if !backfill.extracted_frames.is_empty() {
            if let Err(err) = call_index_db_writer(&self.index_db, |reply| {
                IndexDbWriterMessage::StoreFrames {
                    sha256: backfill.sha256.clone(),
                    mime_type: backfill.mime_type.clone(),
                    process_version: FRAME_PROCESS_VERSION,
                    frames: backfill.extracted_frames.clone(),
                    reply,
                }
            })
            .await
            {
                tracing::error!(error = ?err, "failed to store frames");
            }
        }

        if let Some(blurhash) = &backfill.blurhash {
            if let Err(err) = call_index_db_writer(&self.index_db, |reply| {
                IndexDbWriterMessage::SetBlurhash {
                    sha256: backfill.sha256.clone(),
                    blurhash: blurhash.clone(),
                    reply,
                }
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
            // Small images never get stored thumbnails; skip without decoding.
            let small = fs::metadata(&path)
                .map(|metadata| metadata.len() <= SMALL_IMAGE_FILE_SIZE)
                .unwrap_or(true);
            if small {
                needs_thumb = false;
            }
        }
        if !needs_thumb && !needs_blurhash {
            return Ok(());
        }

        let mut existing_frames = Vec::new();
        let mut video_duration = 0.0_f64;
        if needs_thumb && mime_type.starts_with("video") {
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
            existing_frames = get_frames_bytes(&mut self.conn, &sha256).await?;
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
        self.tasks.spawn(async move {
            let _permit = permit;
            let outer_path = path.clone();
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
                )
            })
            .await;
            match joined {
                Ok(backfill) => TaskOutcome::Backfill(backfill),
                Err(err) => TaskOutcome::Failed(FailedFile {
                    path: outer_path,
                    hash_time: 0.0,
                    metadata_time: 0.0,
                    error: FileProcessError::Worker(err.to_string()),
                }),
            }
        });
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
        self.tasks.spawn(async move {
            let _permit = permit;
            let hash_path = path.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let start = Instant::now();
                let result = calculate_hashes(&hash_path);
                (result, start.elapsed().as_secs_f64())
            })
            .await;
            match joined {
                Ok((Ok((md5, sha256, real_size)), hash_time)) => {
                    TaskOutcome::Hashed(HashedFile {
                        path,
                        last_modified,
                        reported_size,
                        mime_type,
                        existing_sha256,
                        md5,
                        sha256,
                        real_size,
                        hash_time,
                    })
                }
                Ok((Err(err), hash_time)) => TaskOutcome::Failed(FailedFile {
                    path,
                    hash_time,
                    metadata_time: 0.0,
                    error: FileProcessError::Io(err.to_string()),
                }),
                Err(err) => TaskOutcome::Failed(FailedFile {
                    path,
                    hash_time: 0.0,
                    metadata_time: 0.0,
                    error: FileProcessError::Worker(err.to_string()),
                }),
            }
        });
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
        self.tasks.spawn(async move {
            let _permit = permit;
            let outer_path = path.clone();
            let joined = tokio::task::spawn_blocking(move || {
                prepare_new_item(path, last_modified, file_size, mime_type, md5, sha256, filter)
            })
            .await;
            match joined {
                Ok(outcome) => outcome,
                Err(err) => TaskOutcome::Failed(FailedFile {
                    path: outer_path,
                    hash_time: 0.0,
                    metadata_time: 0.0,
                    error: FileProcessError::Worker(err.to_string()),
                }),
            }
        });
        Ok(())
    }

    async fn update_file_data(&mut self, data: FileScanData) -> ApiResult<FileUpsertResult> {
        call_index_db_writer(&self.index_db, |reply| IndexDbWriterMessage::UpdateFileData {
            time_added: self.scan_time.clone(),
            scan_id: self.scan_id,
            data: data.clone(),
            reply,
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

fn prepare_new_item(
    path: PathBuf,
    last_modified: String,
    file_size: i64,
    mime_type: String,
    md5: String,
    sha256: String,
    filter: Option<Arc<Match>>,
) -> TaskOutcome {
    let metadata_start = Instant::now();
    let preloaded_image = if mime_type.starts_with("image") {
        match image::open(&path) {
            Ok(image) => Some(image),
            Err(err) => {
                return TaskOutcome::Failed(FailedFile {
                    path,
                    hash_time: 0.0,
                    metadata_time: metadata_start.elapsed().as_secs_f64(),
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
                return TaskOutcome::Failed(FailedFile {
                    path,
                    hash_time: 0.0,
                    metadata_time: metadata_start.elapsed().as_secs_f64(),
                    error,
                });
            }
        };
    let metadata_time = metadata_start.elapsed().as_secs_f64();

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
            hash_time: 0.0,
            metadata_time,
            error: FileProcessError::Filtered,
        });
    }

    let (thumbnails, frames, blurhash, thumb_time, blurhash_time) =
        match generate_new_item_visuals(&path, &mime_type, &metadata, preloaded_image) {
            Ok(result) => result,
            Err(err) => {
                tracing::error!(error = ?err, path = %path.display(), "failed to generate visuals");
                (Vec::new(), Vec::new(), None, 0.0, 0.0)
            }
        };

    TaskOutcome::NewItem(NewItemData {
        path,
        last_modified,
        file_size,
        sha256,
        mime_type,
        metadata,
        metadata_time,
        thumb_time,
        blurhash_time,
        thumbnails,
        frames,
        blurhash,
    })
}

pub(crate) struct PreparedFile {
    pub(crate) path: PathBuf,
    pub(crate) last_modified: String,
    pub(crate) file_size: i64,
    pub(crate) md5: String,
    pub(crate) sha256: String,
    pub(crate) mime_type: String,
    pub(crate) metadata: ItemScanMeta,
    pub(crate) hash_time: f64,
    pub(crate) metadata_time: f64,
    pub(crate) thumb_time: f64,
    pub(crate) blurhash_time: f64,
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
    pub(crate) hash_time: f64,
    pub(crate) metadata_time: f64,
    pub(crate) thumb_time: f64,
    pub(crate) blurhash_time: f64,
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
            hash_time: prepared.hash_time,
            metadata_time: prepared.metadata_time,
            thumb_time: prepared.thumb_time,
            blurhash_time: prepared.blurhash_time,
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
    Worker(String),
    Io(String),
    Unsupported(String),
    /// The file was rejected by the user's filescan filter.
    Filtered,
}

pub(crate) fn process_file(
    path: PathBuf,
    filescan_filter: Option<Arc<Match>>,
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

    let hash_start = Instant::now();
    let (md5, sha256, real_size) =
        calculate_hashes(&path).map_err(|err| FileProcessError::Io(err.to_string()))?;
    let hash_time = hash_start.elapsed().as_secs_f64();

    if real_size != file_size {
        tracing::warn!(path = %path.display(), real_size, file_size, "file size mismatch");
    }
    let file_size = real_size;

    let metadata_start = Instant::now();
    let metadata = extract_item_metadata(&path, &mime_type, md5.clone())?;
    let metadata_time = metadata_start.elapsed().as_secs_f64();

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

    let (thumbnails, frames, blurhash, thumb_time, blurhash_time) =
        match generate_new_item_visuals(&path, &mime_type, &metadata, None) {
            Ok(result) => result,
            Err(err) => {
                tracing::error!(error = ?err, path = %path.display(), "failed to generate visuals");
                (Vec::new(), Vec::new(), None, 0.0, 0.0)
            }
        };

    Ok(PreparedFile {
        path,
        last_modified,
        file_size,
        md5,
        sha256,
        mime_type,
        metadata,
        hash_time,
        metadata_time,
        thumb_time,
        blurhash_time,
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
            None => image::open(path)
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
) -> Result<(Vec<StoredImage>, Vec<StoredImage>, Option<String>, f64, f64), FileProcessError> {
    let thumb_start = Instant::now();
    let mut thumbnails = Vec::new();
    let mut frames = Vec::new();
    let mut blurhash_source: Option<DynamicImage> = None;

    if mime_type.starts_with("video") {
        let duration = metadata.duration.unwrap_or(0.0);
        if metadata.video_tracks.unwrap_or(0) > 0 && duration > 0.0 {
            let extracted_frames = extract_video_frames(path, 4, duration)?;
            if !extracted_frames.is_empty() {
                let grid = build_image_grid(&extracted_frames);
                thumbnails.push(encode_image(0, &grid)?);
                thumbnails.push(encode_image(1, &extracted_frames[0])?);
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
        let placeholder = create_audio_placeholder();
        thumbnails.push(encode_image(0, &placeholder)?);
        blurhash_source = Some(placeholder);
    } else if mime_type.starts_with("image") {
        let image = match preloaded_image {
            Some(image) => image,
            None => {
                image::open(path).map_err(|err| FileProcessError::Unsupported(err.to_string()))?
            }
        };
        if let Some(thumb) = generate_thumbnail(path, &image)? {
            thumbnails.push(encode_image(0, &thumb)?);
            blurhash_source = Some(thumb);
        } else {
            blurhash_source = Some(image);
        }
    }

    let thumb_time = thumb_start.elapsed().as_secs_f64();

    let blurhash_start = Instant::now();
    let blurhash = if let Some(image) = blurhash_source {
        compute_blurhash(&image).ok()
    } else {
        None
    };
    let blurhash_time = blurhash_start.elapsed().as_secs_f64();

    Ok((thumbnails, frames, blurhash, thumb_time, blurhash_time))
}

/// Regenerates only the visuals a file is missing. Never fails hard: partial
/// or failed generation degrades to empty results, matching the Python
/// behavior of catching thumbnail/blurhash errors per file.
fn generate_backfill_visuals(
    path: &Path,
    mime_type: &str,
    sha256: String,
    needs_thumb: bool,
    needs_blurhash: bool,
    existing_frames: Vec<Vec<u8>>,
    existing_thumb: Option<Vec<u8>>,
    video_duration: f64,
) -> BackfillResult {
    let thumb_start = Instant::now();
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
    let thumb_time = thumb_start.elapsed().as_secs_f64();

    let blurhash_start = Instant::now();
    let mut blurhash = None;
    if needs_blurhash {
        let source = blurhash_source.or_else(|| {
            existing_thumb
                .as_deref()
                .and_then(|bytes| image::load_from_memory(bytes).ok())
        });
        let source = match source {
            Some(source) => Some(source),
            None if mime_type.starts_with("image") => image::open(path).ok(),
            None => None,
        };
        blurhash = source.as_ref().and_then(|image| compute_blurhash(image).ok());
    }
    let blurhash_time = blurhash_start.elapsed().as_secs_f64();

    BackfillResult {
        sha256,
        mime_type: mime_type.to_string(),
        thumbnails,
        extracted_frames,
        blurhash,
        thumb_time,
        blurhash_time,
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
            .filter_map(|bytes| image::load_from_memory(bytes).ok())
            .collect();
        let mut fresh = false;
        if frames.is_empty() {
            frames = extract_video_frames(path, 4, video_duration)?;
            fresh = true;
        }
        if !frames.is_empty() {
            let grid = build_image_grid(&frames);
            thumbnails.push(encode_image(0, &grid)?);
            thumbnails.push(encode_image(1, &frames[0])?);
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
        let placeholder = create_audio_placeholder();
        thumbnails.push(encode_image(0, &placeholder)?);
        source = Some(placeholder);
    } else if mime_type.starts_with("image") {
        let file_size = fs::metadata(path)
            .map_err(|err| FileProcessError::Io(err.to_string()))?
            .len();
        // Only decode when the image is large enough to warrant a thumbnail;
        // the blurhash fallback opens the image separately when needed.
        if file_size > SMALL_IMAGE_FILE_SIZE {
            let image =
                image::open(path).map_err(|err| FileProcessError::Unsupported(err.to_string()))?;
            if let Some(thumb) = generate_thumbnail(path, &image)? {
                thumbnails.push(encode_image(0, &thumb)?);
                source = Some(thumb);
            } else {
                source = Some(image);
            }
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
    let (new_w, new_h) = if width >= height {
        (
            max_dim,
            (max_dim as f64 * height as f64 / width as f64) as u32,
        )
    } else {
        (
            (max_dim as f64 * width as f64 / height as f64) as u32,
            max_dim,
        )
    };
    image.resize(new_w, new_h, image::imageops::FilterType::Lanczos3)
}

fn generate_thumbnail(
    path: &Path,
    image: &DynamicImage,
) -> Result<Option<DynamicImage>, FileProcessError> {
    let metadata = fs::metadata(path).map_err(|err| FileProcessError::Io(err.to_string()))?;
    let file_size = metadata.len();
    if file_size <= SMALL_IMAGE_FILE_SIZE {
        return Ok(None);
    }

    let (width, height) = image.dimensions();
    let max_dimensions = (4096u32, 4096u32);
    let max_file_size = 24 * 1024 * 1024u64;

    if width <= max_dimensions.0 && height <= max_dimensions.1 && file_size <= max_file_size {
        return Ok(None);
    }

    Ok(Some(image.resize(
        max_dimensions.0,
        max_dimensions.1,
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

fn create_audio_placeholder() -> DynamicImage {
    let width = 1024u32;
    let height = 1024u32;
    let mut image = image::RgbImage::new(width, height);
    for (x, y, pixel) in image.enumerate_pixels_mut() {
        let r = ((x as f32 / width as f32) * 200.0) as u8;
        let g = ((y as f32 / height as f32) * 200.0) as u8;
        let b = 64u8;
        *pixel = image::Rgb([r, g, b]);
    }
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

fn extract_video_frames_into(
    path: &Path,
    num_frames: usize,
    interval: f64,
    temp_dir: &Path,
) -> Result<Vec<DynamicImage>, FileProcessError> {
    let output_pattern = temp_dir.join("frame_%04d.png");
    let status = Command::new("ffmpeg")
        .arg("-i")
        .arg(path)
        .arg("-vf")
        .arg(format!("fps=1/{}", interval))
        .arg("-vsync")
        .arg("vfr")
        .arg(&output_pattern)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map_err(|err| FileProcessError::Unsupported(err.to_string()))?;

    if !status.success() {
        return Err(FileProcessError::Unsupported("ffmpeg failed".to_string()));
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
        if let Ok(image) = image::open(&frame_path) {
            frames.push(image);
        }
    }

    Ok(frames)
}

#[derive(Debug, Deserialize)]
struct FfprobeStream {
    index: Option<u64>,
    codec_type: Option<String>,
    codec_name: Option<String>,
    duration: Option<String>,
    width: Option<u64>,
    height: Option<u64>,
    tags: Option<FfprobeTags>,
}

#[derive(Debug, Deserialize)]
struct FfprobeTags {
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
    let output = Command::new("ffprobe")
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
    let duration = modified
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
    let seconds = duration.as_secs() as i64;
    let dt = OffsetDateTime::from_unix_timestamp(seconds)
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
    let formatted = dt
        .format(iso_format())
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
    Ok((formatted, size))
}

pub(crate) fn current_iso_timestamp() -> String {
    let now = OffsetDateTime::now_local().unwrap_or_else(|_| OffsetDateTime::now_utc());
    now.format(iso_format())
        .unwrap_or_else(|_| OffsetDateTime::now_utc().format(iso_format()).unwrap())
}

fn iso_format() -> &'static [FormatItem<'static>] {
    static ISO_FORMAT: std::sync::OnceLock<Vec<FormatItem<'static>>> = std::sync::OnceLock::new();
    ISO_FORMAT.get_or_init(|| {
        time::format_description::parse("[year]-[month]-[day]T[hour]:[minute]:[second]")
            .expect("invalid time format")
    })
}

fn temp_dir_path() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let base = env::var("TEMP_DIR").unwrap_or_else(|_| "data/tmp".to_string());
    let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
    PathBuf::from(base).join(format!("frames-{unique}"))
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
        Ok(mut entries) => entries.next().is_some(),
        Err(err) => {
            tracing::error!(error = %err, path = %path.display(), "failed to read directory");
            false
        }
    }
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

    async fn latest_scan_record(
        conn: &mut sqlx::SqliteConnection,
    ) -> (i64, i64, i64, i64, i64) {
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
        let (unchanged, new_files, modified, errors, marked) =
            latest_scan_record(&mut conn).await;
        assert_eq!((unchanged, new_files, modified, errors, marked), (1, 0, 0, 0, 0));
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
        let (unchanged, new_files, modified, errors, marked) =
            latest_scan_record(&mut conn).await;
        assert_eq!((unchanged, new_files, modified, errors, marked), (1, 0, 0, 0, 0));
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
}
