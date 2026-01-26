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
        file_scans::{FileScanUpdate, add_file_scan, mark_unavailable_files, update_file_scan},
        files::{
            FileScanData, ItemScanMeta, delete_files_not_allowed_stub, delete_items_without_files,
            get_file_by_path, get_item_id, has_blurhash, set_blurhash, update_file_data,
        },
        folders::{
            add_folder_to_database, delete_files_not_under_included_folders,
            delete_files_under_excluded_folders, delete_folders_not_in_list,
            get_folders_from_database,
        },
        open_index_db_read, open_index_db_write,
        storage::{
            StoredImage, delete_orphaned_frames, delete_orphaned_thumbnails, has_frame,
            has_thumbnail, store_frames, store_thumbnails,
        },
        system_config::{SystemConfig, SystemConfigStore},
    },
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

        let mut conn = open_index_db_write(&self.index_db, &self.user_data_db).await?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "failed to begin rescan finalization");
                ApiError::internal("Failed to finalize rescan")
            })?;
        let unavailable_files_deleted = if config.remove_unavailable_files {
            delete_unavailable_files(&mut conn).await?
        } else {
            0
        };
        let rule_files_deleted = delete_files_not_allowed_stub(&mut conn).await?;
        let orphan_items_deleted = delete_items_without_files(&mut conn, 10_000).await?;
        let _ = delete_orphaned_frames(&mut conn).await?;
        let _ = delete_orphaned_thumbnails(&mut conn).await?;
        sqlx::query("COMMIT")
            .execute(&mut conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "failed to commit rescan finalization");
                ApiError::internal("Failed to finalize rescan")
            })?;

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

        let mut conn = open_index_db_read(&self.index_db, &self.user_data_db).await?;
        let current_included = get_folders_from_database(&mut conn, true).await?;
        let current_excluded = get_folders_from_database(&mut conn, false).await?;
        drop(conn);

        let included_added = difference(&config.included_folders, &current_included);
        let excluded_added = difference(&config.excluded_folders, &current_excluded);

        let scan_ids = execute_folder_scan(
            &self.index_db,
            &self.user_data_db,
            &config,
            &included_added,
            &config.excluded_folders,
            self.options,
        )
        .await?;

        let scan_time = current_iso_timestamp();
        let mut conn = open_index_db_write(&self.index_db, &self.user_data_db).await?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "failed to begin folder update finalization");
                ApiError::internal("Failed to finalize folder update")
            })?;

        let included_deleted =
            delete_folders_not_in_list(&mut conn, &config.included_folders, true).await?;
        let excluded_deleted =
            delete_folders_not_in_list(&mut conn, &config.excluded_folders, false).await?;

        for folder in &config.included_folders {
            let _ = add_folder_to_database(&mut conn, &scan_time, folder, true).await?;
        }
        for folder in &config.excluded_folders {
            let _ = add_folder_to_database(&mut conn, &scan_time, folder, false).await?;
        }

        let unavailable_files_deleted = if config.remove_unavailable_files {
            delete_unavailable_files(&mut conn).await?
        } else {
            0
        };
        let excluded_folder_files_deleted = delete_files_under_excluded_folders(&mut conn).await?;
        let orphan_files_deleted = delete_files_not_under_included_folders(&mut conn).await?;
        let rule_files_deleted = delete_files_not_allowed_stub(&mut conn).await?;
        let orphan_items_deleted = delete_items_without_files(&mut conn, 10_000).await?;
        let _ = delete_orphaned_frames(&mut conn).await?;
        let _ = delete_orphaned_thumbnails(&mut conn).await?;

        sqlx::query("COMMIT")
            .execute(&mut conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "failed to commit folder update finalization");
                ApiError::internal("Failed to finalize folder update")
            })?;

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

async fn is_resync_needed(
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

    let scan_time = current_iso_timestamp();
    let mut scan_ids = Vec::new();

    for folder in starting_points {
        let mut conn = open_index_db_write(index_db, user_data_db).await?;
        let scan_id = add_file_scan(&mut conn, &scan_time, &folder).await?;
        scan_ids.push(scan_id);
        drop(conn);

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

        let mut conn = open_index_db_write(index_db, user_data_db).await?;
        update_file_scan(
            &mut conn,
            scan_id,
            FileScanUpdate {
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
        )
        .await?;
    }

    Ok(scan_ids)
}

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
    let semaphore = Arc::new(Semaphore::new(options.worker_count));
    let mut tasks = JoinSet::new();
    let config = Arc::new(config.clone());

    for entry in WalkDir::new(folder)
        .follow_links(true)
        .into_iter()
        .filter_entry(|entry| !is_excluded(entry.path(), excluded_paths))
    {
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

        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ApiError::internal("Failed to schedule scan work"))?;
        let config = Arc::clone(&config);
        tasks.spawn(async move {
            let _permit = permit;
            tokio::task::spawn_blocking(move || process_file(path, &config))
                .await
                .map_err(|err| FileProcessError::Worker(err.to_string()))?
        });
    }

    let mut stats = FolderStats {
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
    };

    let mut conn = open_index_db_write(index_db, user_data_db).await?;

    while let Some(result) = tasks.join_next().await {
        let processed = match result {
            Ok(Ok(processed)) => processed,
            Ok(Err(_)) => {
                stats.errors += 1;
                continue;
            }
            Err(err) => {
                tracing::error!(error = %err, "file processing task failed");
                stats.errors += 1;
                continue;
            }
        };

        stats.hashing_time += processed.hash_time;
        stats.metadata_time += processed.metadata_time;
        stats.thumbgen_time += processed.thumb_time;
        stats.blurhash_time += processed.blurhash_time;

        let file_data = match build_file_scan_data(&mut conn, processed, scan_time).await {
            Ok(data) => data,
            Err(err) => {
                tracing::error!(error = ?err, "failed to build file scan data");
                stats.errors += 1;
                continue;
            }
        };

        let false_change = file_data.new_file_hash == false && file_data.new_file_timestamp;
        if false_change {
            stats.false_changes += 1;
        }

        if !file_data.thumbnails.is_empty() {
            match has_thumbnail(&mut conn, &file_data.sha256, 1).await {
                Ok(has_thumb) => {
                    if !has_thumb {
                        if let Err(err) = store_thumbnails(
                            &mut conn,
                            &file_data.sha256,
                            &file_data.mime_type,
                            1,
                            &file_data.thumbnails,
                        )
                        .await
                        {
                            tracing::error!(error = ?err, "failed to store thumbnails");
                        }
                    }
                }
                Err(err) => {
                    tracing::error!(error = ?err, "failed to check thumbnails");
                }
            }
        }

        if !file_data.frames.is_empty() {
            match has_frame(&mut conn, &file_data.sha256, 1).await {
                Ok(has_frame) => {
                    if !has_frame {
                        if let Err(err) = store_frames(
                            &mut conn,
                            &file_data.sha256,
                            &file_data.mime_type,
                            1,
                            &file_data.frames,
                        )
                        .await
                        {
                            tracing::error!(error = ?err, "failed to store frames");
                        }
                    }
                }
                Err(err) => {
                    tracing::error!(error = ?err, "failed to check frames");
                }
            }
        }

        if let Some(blurhash) = &file_data.blurhash {
            match has_blurhash(&mut conn, &file_data.sha256).await {
                Ok(has_value) => {
                    if !has_value {
                        if let Err(err) = set_blurhash(&mut conn, &file_data.sha256, blurhash).await
                        {
                            tracing::error!(error = ?err, "failed to set blurhash");
                        }
                    }
                }
                Err(err) => {
                    tracing::error!(error = ?err, "failed to check blurhash");
                }
            }
        }

        if let Err(err) = sqlx::query("BEGIN IMMEDIATE").execute(&mut conn).await {
            tracing::error!(error = %err, "failed to begin file update transaction");
            stats.errors += 1;
            continue;
        }
        let result = match update_file_data(
            &mut conn,
            &file_data.time_added,
            scan_id,
            &file_data.data,
        )
        .await
        {
            Ok(result) => result,
            Err(err) => {
                tracing::error!(error = ?err, "failed to update file data");
                let _ = sqlx::query("ROLLBACK").execute(&mut conn).await;
                stats.errors += 1;
                continue;
            }
        };
        if let Err(err) = sqlx::query("COMMIT").execute(&mut conn).await {
            tracing::error!(error = %err, "failed to commit file update transaction");
            let _ = sqlx::query("ROLLBACK").execute(&mut conn).await;
            stats.errors += 1;
            continue;
        }

        if result.item_inserted {
            stats.new_items += 1;
        }
        if result.file_updated {
            stats.unchanged_files += 1;
        } else if result.file_deleted {
            stats.modified_files += 1;
        } else if result.file_inserted {
            stats.new_files += 1;
        }
    }

    let (marked_unavailable, total_available) =
        mark_unavailable_files(&mut conn, scan_id, folder).await?;
    stats.marked_unavailable = marked_unavailable;
    stats.total_available = total_available;

    Ok(stats)
}

struct PreparedFile {
    path: PathBuf,
    last_modified: String,
    file_size: i64,
    md5: String,
    sha256: String,
    mime_type: String,
    metadata: ItemScanMeta,
    hash_time: f64,
    metadata_time: f64,
    thumb_time: f64,
    blurhash_time: f64,
    thumbnails: Vec<StoredImage>,
    frames: Vec<StoredImage>,
    blurhash: Option<String>,
}

struct FileWriteData {
    sha256: String,
    mime_type: String,
    data: FileScanData,
    new_file_timestamp: bool,
    new_file_hash: bool,
    hash_time: f64,
    metadata_time: f64,
    thumb_time: f64,
    blurhash_time: f64,
    thumbnails: Vec<StoredImage>,
    frames: Vec<StoredImage>,
    blurhash: Option<String>,
    time_added: String,
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

async fn build_file_scan_data(
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
enum FileProcessError {
    Worker(String),
    Io(String),
    Unsupported(String),
}

fn process_file(path: PathBuf, config: &SystemConfig) -> Result<PreparedFile, FileProcessError> {
    let (last_modified, file_size) = get_last_modified_time_and_size(&path)
        .map_err(|err| FileProcessError::Io(err.to_string()))?;

    if !passes_filescan_filter_stage1(config) {
        return Err(FileProcessError::Unsupported("filtered".to_string()));
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
    let mime_type = infer_mime_type(&path)?;
    let metadata = extract_item_metadata(&path, &mime_type, md5.clone())?;
    let metadata_time = metadata_start.elapsed().as_secs_f64();

    if !passes_filescan_filter_stage2(config) {
        return Err(FileProcessError::Unsupported("filtered".to_string()));
    }

    let (thumbnails, frames, blurhash, thumb_time, blurhash_time) =
        match generate_visuals(&path, &mime_type) {
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

fn passes_filescan_filter_stage1(_config: &SystemConfig) -> bool {
    // TODO: Implement filescan_filter stage 1 (PQL).
    true
}

fn passes_filescan_filter_stage2(_config: &SystemConfig) -> bool {
    // TODO: Implement filescan_filter stage 2 (PQL).
    true
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
        let image =
            image::open(path).map_err(|err| FileProcessError::Unsupported(err.to_string()))?;
        metadata.width = Some(image.width() as i64);
        metadata.height = Some(image.height() as i64);
        return Ok(metadata);
    }

    if mime_type.starts_with("video") || mime_type.starts_with("audio") {
        let info = extract_media_info(path)?;
        if mime_type.starts_with("video") {
            if let Some(video) = info.video_track {
                metadata.width = Some(video.width as i64);
                metadata.height = Some(video.height as i64);
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

fn generate_visuals(
    path: &Path,
    mime_type: &str,
) -> Result<(Vec<StoredImage>, Vec<StoredImage>, Option<String>, f64, f64), FileProcessError> {
    let thumb_start = Instant::now();
    let mut thumbnails = Vec::new();
    let mut frames = Vec::new();
    let mut blurhash_source: Option<DynamicImage> = None;

    if mime_type.starts_with("video") {
        let extracted_frames = extract_video_frames(path, 4)?;
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
    } else if mime_type.starts_with("audio") {
        let placeholder = create_audio_placeholder();
        thumbnails.push(encode_image(0, &placeholder)?);
        blurhash_source = Some(placeholder);
    } else if mime_type.starts_with("image") {
        if let Some(thumb) = generate_thumbnail(path)? {
            thumbnails.push(encode_image(0, &thumb)?);
            blurhash_source = Some(thumb);
        } else {
            let image =
                image::open(path).map_err(|err| FileProcessError::Unsupported(err.to_string()))?;
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

fn generate_thumbnail(path: &Path) -> Result<Option<DynamicImage>, FileProcessError> {
    let metadata = fs::metadata(path).map_err(|err| FileProcessError::Io(err.to_string()))?;
    let file_size = metadata.len();
    let really_small_file_size = 5 * 1024 * 1024u64;
    if file_size <= really_small_file_size {
        return Ok(None);
    }

    let image = image::open(path).map_err(|err| FileProcessError::Unsupported(err.to_string()))?;
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
) -> Result<Vec<DynamicImage>, FileProcessError> {
    let duration = probe_duration_seconds(path)?;
    if duration <= 0.0 {
        return Ok(Vec::new());
    }

    let interval = duration / num_frames as f64;
    let temp_dir = temp_dir_path();
    fs::create_dir_all(&temp_dir).map_err(|err| FileProcessError::Io(err.to_string()))?;

    let output_pattern = temp_dir.join("frame_%04d.png");
    let status = Command::new("ffmpeg")
        .arg("-i")
        .arg(path)
        .arg("-vf")
        .arg(format!("fps=1/{}", interval))
        .arg("-vsync")
        .arg("vfr")
        .arg(&output_pattern)
        .status()
        .map_err(|err| FileProcessError::Unsupported(err.to_string()))?;

    if !status.success() {
        return Err(FileProcessError::Unsupported("ffmpeg failed".to_string()));
    }

    let mut frames = Vec::new();
    let mut entries =
        fs::read_dir(&temp_dir).map_err(|err| FileProcessError::Io(err.to_string()))?;
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
        let _ = fs::remove_file(&frame_path);
    }

    Ok(frames)
}

fn probe_duration_seconds(path: &Path) -> Result<f64, FileProcessError> {
    let output = Command::new("ffprobe")
        .arg("-v")
        .arg("error")
        .arg("-show_entries")
        .arg("format=duration")
        .arg("-of")
        .arg("default=noprint_wrappers=1:nokey=1")
        .arg(path)
        .output()
        .map_err(|err| FileProcessError::Unsupported(err.to_string()))?;

    if !output.status.success() {
        return Err(FileProcessError::Unsupported("ffprobe failed".to_string()));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .trim()
        .parse::<f64>()
        .map_err(|err| FileProcessError::Unsupported(err.to_string()))
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
    width: u64,
    height: u64,
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

    for stream in data.streams {
        match stream.codec_type.as_deref() {
            Some("audio") => {
                let duration = stream
                    .duration
                    .as_deref()
                    .and_then(|value| value.parse::<f64>().ok())
                    .unwrap_or(format_duration);
                audio_tracks.push(AudioTrack { duration });
            }
            Some("video") => {
                let duration = stream
                    .duration
                    .as_deref()
                    .and_then(|value| value.parse::<f64>().ok())
                    .unwrap_or(format_duration);
                if let (Some(width), Some(height)) = (stream.width, stream.height) {
                    video_track = Some(VideoTrack {
                        duration,
                        width,
                        height,
                    });
                }
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

fn get_last_modified_time_and_size(path: &Path) -> Result<(String, i64), io::Error> {
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

fn current_iso_timestamp() -> String {
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

fn check_folder_validity(folder: &str) -> bool {
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

fn deduplicate_paths(paths: &[String]) -> Vec<String> {
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

fn normalize_path(path: &str, trailing: bool) -> PathBuf {
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

fn build_extension_set(config: &SystemConfig) -> HashSet<String> {
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

fn has_allowed_extension(path: &Path, extensions: &HashSet<String>) -> bool {
    let ext = path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| format!(".{}", ext.to_lowercase()));
    match ext {
        Some(ext) => extensions.contains(&ext),
        None => false,
    }
}

fn is_hidden_or_temp(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    name.starts_with('.') || name.starts_with('~')
}

fn is_excluded(path: &Path, excluded: &[PathBuf]) -> bool {
    excluded.iter().any(|prefix| path.starts_with(prefix))
}

fn difference(current: &[String], existing: &[String]) -> Vec<String> {
    current
        .iter()
        .filter(|entry| !existing.contains(entry))
        .cloned()
        .collect()
}

async fn delete_unavailable_files(conn: &mut sqlx::SqliteConnection) -> ApiResult<u64> {
    let result = sqlx::query(
        r#"
DELETE FROM files
WHERE available = 0
        "#,
    )
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to delete unavailable files");
        ApiError::internal("Failed to delete unavailable files")
    })?;

    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::migrate_databases_on_disk;
    use std::sync::OnceLock;
    use tempfile::TempDir;

    fn test_root() -> &'static TempDir {
        static ROOT: OnceLock<TempDir> = OnceLock::new();
        ROOT.get_or_init(|| {
            let dir = TempDir::new().unwrap();
            unsafe {
                std::env::set_var("DATA_FOLDER", dir.path());
            }
            dir
        })
    }

    fn next_db_name() -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        format!("testdb_{}", COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    // Ensures rescans persist items, files, and blurhash data.
    #[tokio::test]
    async fn rescan_creates_items_and_files() {
        let root = test_root();
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();

        let media_dir = root.path().join("media");
        fs::create_dir_all(&media_dir).unwrap();
        let image_path = media_dir.join("sample.png");
        let image = image::RgbImage::new(8, 8);
        image.save(&image_path).unwrap();

        let store = SystemConfigStore::new(root.path().to_path_buf());
        let mut config = SystemConfig::default();
        config.included_folders = vec![media_dir.to_string_lossy().to_string()];
        store.save(&index_db, &config).unwrap();

        let service = FileScanService::new(
            index_db.clone(),
            user_data_db.clone(),
            root.path().to_path_buf(),
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
}
