//! The folder scan: walk, hash, index, and generate every visual an item
//! carries.
//!
//! A map of the module, in the order the code runs rather than the order it is
//! written. Everything marked *(visuals)* lives in the [`visuals`]
//! submodule; the rest is here.
//!
//! * **The walk** — `execute_folder_scan` / `scan_single_folder` /
//!   `ScanContext::scan_path`. One `ScanContext` per root owns the connection,
//!   the worker `JoinSet`, the stats and the `scan_errors` ledger it preloaded.
//! * **Per file** — `dispatch_hash` (the mtime shortcut, then hashing),
//!   `dispatch_prepare` (metadata plus first-generation visuals for a *new*
//!   item), and `maybe_dispatch_backfill` for content that is already indexed.
//! * **The dispatcher's questions** — `maybe_dispatch_backfill` asks one
//!   question per thing an item can owe: the thumbnail, the blurhash, and the
//!   five `pending_*` questions (outro, codec, animation, orientation,
//!   ladder) whose answers become one [`PendingBackfillWork`]. Every one is
//!   answered from *indexed metadata*; nothing here may decode a file to
//!   decide whether it needs decoding.
//! * **The generators** *(visuals)* — `generate_new_item_visuals` and
//!   `generate_backfill_visuals`, both blocking and both entered through
//!   `spawn_blocking`, with the `build_*_renditions` builders and the ladder
//!   rules under them. The per-type renderers they call (video frames, audio
//!   cover art, pdfium, the headless browser) stay here, next to the process
//!   and library plumbing they own.
//! * **The verdict vocabulary** — `FileProcessError`/`ScanFailure` here,
//!   `VisualsError` *(visuals)*, and the `visual_attempts` markers they become,
//!   which is what keeps a broken file from being retried on every scan
//!   forever (docs/failed-media-retry-design.md).
//! * **The writes** — `handle_new_item`/`handle_backfill`/`fail_file`, all
//!   through the index-db writer task.

/// The rendition ladder: what a visuals pass produces and the rules that
/// decide what it owes. Split out for size; see its own module docs.
mod visuals;

use visuals::*;

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    env,
    ffi::OsStr,
    fs,
    io::{self, Read},
    net::{SocketAddr, TcpStream},
    path::{Component, Path, PathBuf},
    process::{Child, Command},
    sync::{
        Arc, Condvar, Mutex, OnceLock, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use ab_glyph::{FontVec, PxScale};
use base64::Engine;
use blurhash::encode as blurhash_encode;
use image::metadata::Orientation;
use image::{DynamicImage, GenericImageView, ImageDecoder, Rgb, RgbImage};
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
    api_error::{ApiError, ApiErrorKind, Blocker, SKIP_AFTER_AMBIGUOUS, SKIP_AFTER_CONFIRMED},
    db::{
        file_scans::{FileScanUpdate, get_completed_scan_paths, get_open_file_scan_id},
        files::{
            FileScanData, FileUpsertResult, ItemScanMeta, PendingOutroItem,
            get_file_by_path, get_item_content_end_ms, get_item_id, get_item_visual_facts,
            get_item_visual_meta, get_pending_outro_item, has_blurhash, item_animation_pending,
            item_codec_pending, item_rotation_pending, item_transparency_pending,
        },
        folders::get_folders_from_database,
        index_writer::{IndexDbWriterMessage, call_index_db_writer},
        open_index_db_read, open_index_db_read_no_user_data,
        scan_errors::{
            STAGE_DECODE, STAGE_HEADER, STAGE_METADATA, STAGE_MIME, ScanErrorRecord, ScanErrorSkip,
            fold_scan_path, list_distinct_scan_blockers, load_scan_errors_under,
            stage_blocks_indexing,
        },
        storage::{
            StoredImage, StoredTier, ThumbnailGeometry, TierGeometry, TierPayload,
            get_frames_bytes, get_thumbnail_bytes, get_thumbnail_geometry,
            get_thumbnail_tier_geometry, has_any_frame, has_frame, has_thumbnail,
            has_thumbnail_tiers,
        },
        system_config::{SystemConfig, SystemConfigStore},
        visual_attempts::{
            VisualAttemptRecord, VisualFailure, VisualKind, VisualVerdict,
            list_distinct_visual_blockers, visuals_suppressed,
        },
    },
    jobs::queue::ChangeSummary,
    jobs::timing::PhaseTimer,
    media_tools::outro::{
        OUTRO_DETECTOR_VERSION, OutroProbeError, OutroVerdict, RejectReason, detect_outro,
    },
    pql::builder::filters::evaluate_match,
    pql::model::{Match, MatchValue},
    media_tools::animated_loop::LoopError,
    media_tools::transcode::compose::Transform,
    visual_tiers::{
        DisplayPlan, FormatPolicy, GENERATED_STILL_FORMAT, LOOP_MEDIA_TYPE, RenditionFormat,
        RenditionKind, RenditionRung, ThumbnailTier, TierPlan, UNDECODABLE_HAS_TRANSPARENCY,
        WantedRendition, animated_plans, animated_rendition_set, animated_serves_original,
        display_bytes_trigger, display_plan, encode_rendition, grid_plans,
        grid_plans_for_stored_thumbnail, grid_renditions, has_alpha_pixels, is_animated_image,
        loop_keeps_original, poster_plans, render, static_rendition_set, still_keeps_original,
        stored_thumbnail_rendition_set, tier_format,
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
    // Only read by tests; production callers ignore the scan ids.
    #[allow(dead_code)]
    pub scan_ids: Vec<i64>,
    /// What the scan changed, for the queue's deferred maintenance boundary.
    pub summary: ChangeSummary,
}

pub(crate) struct FolderUpdateResult {
    // Only read by tests; production callers ignore these.
    #[allow(dead_code)]
    pub included_added: Vec<String>,
    #[allow(dead_code)]
    pub scan_ids: Vec<i64>,
    /// What the update changed, for the queue's deferred maintenance boundary.
    pub summary: ChangeSummary,
}

/// Rows written by a folder scan, aggregated over every scanned root. Any of
/// them being non-zero means the DB changed and maintenance is owed.
#[derive(Default, Clone, Copy)]
struct ScanTotals {
    new_items: i64,
    /// Files whose contents did not change. They are *not* free: every one of
    /// them still gets an `UPDATE files SET scan_id …`, so a rescan that finds
    /// nothing new still grows the WAL and still owes a checkpoint.
    unchanged_files: i64,
    new_files: i64,
    modified_files: i64,
    marked_unavailable: i64,
    /// Thumbnail/frame/blurhash rows written for already-indexed content.
    backfilled_visuals: i64,
    /// Files skipped on an active `scan_errors` verdict. Not a write and not
    /// an error, so it stays out of [`Self::wrote_data`] and off the scan row;
    /// it exists so the job summary can say the scan deliberately left files
    /// alone instead of silently doing less than the user expected.
    known_bad: i64,
    /// Visuals generations skipped on an active `visual_attempts` marker. The
    /// twin of `known_bad` one layer down: the file itself is processed
    /// normally, only the regeneration of a known nothing is skipped.
    visuals_suppressed: i64,
}

impl ScanTotals {
    fn wrote_data(self) -> bool {
        self.new_items > 0
            || self.unchanged_files > 0
            || self.new_files > 0
            || self.modified_files > 0
            || self.marked_unavailable > 0
            || self.backfilled_visuals > 0
    }

    fn add(&mut self, stats: &FolderStats) {
        self.new_items += stats.new_items;
        self.unchanged_files += stats.unchanged_files;
        self.new_files += stats.new_files;
        self.modified_files += stats.modified_files;
        self.marked_unavailable += stats.marked_unavailable;
        self.backfilled_visuals += stats.backfilled_visuals;
        self.known_bad += stats.known_bad;
        self.visuals_suppressed += stats.visuals_suppressed;
    }
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
        // The embedded update's changes are this job's changes: it no longer
        // runs maintenance of its own, so its summary has to travel up.
        let mut summary = ChangeSummary::default();
        if is_resync_needed(&self.index_db, &self.user_data_db, &config).await? {
            summary.or_with(self.run_folder_update().await?.summary);
        }

        let mut conn = open_index_db_read(&self.index_db, &self.user_data_db).await?;
        let included_folders = get_folders_from_database(&mut conn, true).await?;
        let excluded_folders = get_folders_from_database(&mut conn, false).await?;
        drop(conn);

        let (scan_ids, totals) = execute_folder_scan(
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
        sweep_orphaned_visual_attempts(&self.index_db).await;

        let deleted_data = unavailable_files_deleted > 0
            || rule_files_deleted > 0
            || orphan_items_deleted > 0
            || orphan_frames_deleted > 0
            || orphan_thumbnails_deleted > 0;
        // Maintenance is no longer run here: the queue defers it to the job
        // boundary, where consecutive jobs on the same DB share one pass.
        summary.or_with(ChangeSummary {
            wrote_data: totals.wrote_data() || deleted_data,
            deleted_data,
            // Adding files cannot change a tag count; removing items can, via
            // the cascade into `tags_items`.
            tags_changed: deleted_data,
        });

        Ok(RescanResult { scan_ids, summary })
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

        let (scan_ids, totals) = execute_folder_scan(
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
        sweep_orphaned_visual_attempts(&self.index_db).await;

        let deleted_data = unavailable_files_deleted > 0
            || excluded_folder_files_deleted > 0
            || orphan_files_deleted > 0
            || rule_files_deleted > 0
            || orphan_items_deleted > 0
            || orphan_frames_deleted > 0
            || orphan_thumbnails_deleted > 0;
        // See `rescan_folders`: reported, not run, so the job boundary can
        // batch it.
        let summary = ChangeSummary {
            wrote_data: totals.wrote_data() || deleted_data,
            deleted_data,
            // See `rescan_folders`: only the deletions reach `tags_items`.
            tags_changed: deleted_data,
        };

        Ok(FolderUpdateResult {
            included_added,
            scan_ids,
            summary,
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

/// Free-page share above which rewriting the whole database is worth it. This
/// is the primary signal: the cost of a VACUUM scales with the file, so the
/// payoff has to as well.
const VACUUM_FREE_RATIO: f64 = 0.10;
/// Floor under the ratio (`AND`, not `OR`): on a small database 10% is a
/// handful of pages, and rewriting it buys nothing. ~10 MB at the 4 KiB
/// default page size.
const VACUUM_FREE_PAGES_FLOOR: i64 = 2_500;
/// Free-page count that justifies a VACUUM on its own, for the databases where
/// 10% is never reached but gigabytes are still reclaimable (~1 GB at 4 KiB
/// pages).
const VACUUM_FREE_PAGES_ABSOLUTE: i64 = 250_000;

/// Post-job VACUUM/recount/ANALYZE/checkpoint. Failures are logged but never
/// fail the job: the job's own work has already been committed at this point.
///
/// `vacuum` means "something was deleted"; whether that is worth a multi-minute
/// rewrite of a multi-GB file is decided from the actual free-page counts.
///
/// `tags_changed` is the owed flag the boundary recorded; the durable marker
/// stands in for everything that flag cannot know (writes committed by a job
/// that was then killed, and the continuous scan, which is not a queue job and
/// has no boundary). Either one runs the recount.
pub(crate) async fn run_post_job_maintenance(index_db: &str, vacuum: bool, tags_changed: bool) {
    // Boxed: opening a connection is a large future, and this one sits inside
    // the job queue's `execute_job` state machine, which is stack-allocated
    // before the task is spawned.
    if vacuum && Box::pin(vacuum_is_worthwhile(index_db)).await {
        if let Err(err) =
            call_index_db_writer(index_db, |reply| IndexDbWriterMessage::Vacuum { reply }).await
        {
            tracing::error!(error = ?err, index_db, "failed to vacuum index database");
        }
    }
    // Before ANALYZE, so the statistics are sampled from the updated table.
    // Gated, unlike ANALYZE and the checkpoint: this is a full rebuild of
    // every `tags.item_count` and the only step whose cost scales with the
    // size of the tag data. A drain of pure no-deletion rescans now skips it
    // entirely. The recount clears the marker in its own transaction.
    if (tags_changed || Box::pin(tags_are_dirty(index_db)).await)
        && let Err(err) = call_index_db_writer(index_db, |reply| {
            IndexDbWriterMessage::RecountTagItems { reply }
        })
        .await
    {
        tracing::error!(error = ?err, index_db, "failed to recount tag item counts");
    }
    if let Err(err) =
        call_index_db_writer(index_db, |reply| IndexDbWriterMessage::Analyze { reply }).await
    {
        tracing::error!(error = ?err, index_db, "failed to analyze index database");
    }
    // Last so it also reclaims what VACUUM/ANALYZE themselves pushed through
    // the log.
    if let Err(err) =
        call_index_db_writer(index_db, |reply| IndexDbWriterMessage::Checkpoint { reply }).await
    {
        tracing::error!(error = ?err, index_db, "failed to checkpoint index database");
    }
}

/// The durable tags-dirty marker: "something changed `tags_items` since the
/// last successful recount". Read on a plain read connection — this runs
/// inside the maintenance job, which is serialized against every other job and
/// pauses the continuous scan, so there is nothing to race.
///
/// A failed read answers "yes", which is the pre-gate behavior: recounting
/// when it was not needed costs one rebuild, while skipping when it was needed
/// leaves the counts (and the autocomplete ordering they drive) wrong.
async fn tags_are_dirty(index_db: &str) -> bool {
    let mut conn = match open_index_db_read_no_user_data(index_db).await {
        Ok(conn) => conn,
        Err(err) => {
            tracing::warn!(error = ?err, index_db, "could not read the tags-dirty marker; recounting");
            return true;
        }
    };
    match crate::db::maintenance_state::read_tags_dirty(&mut conn).await {
        Ok(dirty) => dirty,
        Err(err) => {
            tracing::warn!(error = ?err, index_db, "could not read the tags-dirty marker; recounting");
            true
        }
    }
}

/// True when either the index or the attached storage database carries enough
/// free pages to be worth reclaiming. Both are checked because the writer's
/// VACUUM rewrites both, and the blobs that dominate the file size live in
/// `storage`. A failed measurement answers "yes" — that is the behavior every
/// caller had before the gate existed.
async fn vacuum_is_worthwhile(index_db: &str) -> bool {
    let mut conn = match open_index_db_read_no_user_data(index_db).await {
        Ok(conn) => conn,
        Err(err) => {
            tracing::warn!(error = ?err, index_db, "could not measure free pages; vacuuming");
            return true;
        }
    };
    for (schema, free_sql, pages_sql) in [
        ("main", "PRAGMA main.freelist_count", "PRAGMA main.page_count"),
        (
            "storage",
            "PRAGMA storage.freelist_count",
            "PRAGMA storage.page_count",
        ),
    ] {
        let free: i64 = match sqlx::query_scalar(free_sql).fetch_one(&mut conn).await {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(error = %err, index_db, schema, "free page query failed");
                return true;
            }
        };
        let pages: i64 = match sqlx::query_scalar(pages_sql).fetch_one(&mut conn).await {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(error = %err, index_db, schema, "page count query failed");
                return true;
            }
        };
        let ratio_worthwhile = pages > 0
            && (free as f64) / (pages as f64) >= VACUUM_FREE_RATIO
            && free >= VACUUM_FREE_PAGES_FLOOR;
        if ratio_worthwhile || free >= VACUUM_FREE_PAGES_ABSOLUTE {
            tracing::debug!(index_db, schema, free, pages, "vacuum gate passed");
            return true;
        }
    }
    tracing::info!(index_db, "skipping VACUUM: too few free pages to reclaim");
    false
}

async fn execute_folder_scan(
    index_db: &str,
    user_data_db: &str,
    config: &SystemConfig,
    included_folders: &[String],
    excluded_folders: &[String],
    options: ScanOptions,
) -> ApiResult<(Vec<i64>, ScanTotals)> {
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

    // Before any root is walked: a dependency that was missing when these
    // files failed may be installed now, and the files waiting on it become
    // scannable again in this same run. Usually one indexed query returning
    // nothing.
    if !starting_points.is_empty() {
        if let Err(err) = heal_blocked_scan_errors(index_db).await {
            tracing::warn!(error = ?err, "failed to re-probe blocked scan failures");
        }
    }

    let scan_time = current_iso_timestamp();
    let mut scan_ids = Vec::new();
    let mut totals = ScanTotals::default();
    // One attempt token for the whole run. Both ledgers' `attempts` count
    // *runs* that saw the same conclusion and dedup on this value, but a run
    // opens one `file_scans` row per root — so identical content indexed under
    // two roots (`visual_attempts`, keyed by sha), or one file walked twice
    // because two registered roots alias on a case-insensitive filesystem
    // (`scan_errors`, keyed by path), would be counted twice by a single run,
    // confirming a `skip_after = 2` verdict the run only saw fail once. The
    // first root's id is a token every root of this run shares and no later
    // run reuses.
    let mut attempt_scan_id: Option<i64> = None;

    for folder in starting_points {
        let scan_id = call_index_db_writer(index_db, |reply| IndexDbWriterMessage::AddFileScan {
            scan_time: scan_time.clone(),
            path: folder.clone(),
            reply,
        })
        .await?;
        scan_ids.push(scan_id);
        let attempt_scan_id = *attempt_scan_id.get_or_insert(scan_id);

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
            attempt_scan_id,
            &scan_time,
            options,
        )
        .await?;
        totals.add(&stats);

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

    // The job's own summary line. `known_bad` has no column on any scan row —
    // it is not something a scan did, it is something it deliberately did not
    // do — so this log is the only place the whole run's count surfaces until
    // the failures API lands.
    if totals.known_bad > 0 {
        tracing::info!(
            known_bad = totals.known_bad,
            roots = scan_ids.len(),
            "file scan skipped files with an active recorded scan failure"
        );
    }
    // The visuals negative cache's payoff, and the only place it is visible:
    // this is the count of thumbnail generations the scan did *not* run
    // because it already knows they produce nothing — the 4m49s of thumbgen
    // on a zero-new-file scan that the cache exists to remove.
    if totals.visuals_suppressed > 0 {
        tracing::info!(
            visuals_suppressed = totals.visuals_suppressed,
            roots = scan_ids.len(),
            "file scan skipped visuals generation with an active recorded attempt"
        );
    }

    Ok((scan_ids, totals))
}

/// `blocked` auto-heal for the scan-side markers
/// (docs/failed-media-retry-design.md, req 10). The twin of the extraction
/// job's `heal_blocked_errors`, and the same shape: read the dependencies the
/// markers are actually waiting on — the usual answer is none, one indexed
/// query each — probe only those, and clear the rows of the ones that now bind.
///
/// Two tables, one pass: the `scan_errors` ledger (a video ffprobe could not
/// read at all) and the `visual_attempts` cache (a PDF pdfium was not there to
/// render). They wait on the same set of backends, and probing is the
/// expensive half, so splitting them would mean binding pdfium twice.
///
/// Backend cache lifetime is backend-specific. In particular, HTML renderer
/// absence is not cached, so installing a browser can clear its markers on the
/// next scan without restarting the gateway.
pub(crate) async fn heal_blocked_scan_errors(index_db: &str) -> ApiResult<()> {
    let waiting = {
        let mut conn = open_index_db_read_no_user_data(index_db).await?;
        let mut waiting = list_distinct_scan_blockers(&mut conn).await?;
        for blocker in list_distinct_visual_blockers(&mut conn).await? {
            if !waiting.contains(&blocker) {
                waiting.push(blocker);
            }
        }
        waiting
    };
    if waiting.is_empty() {
        return Ok(());
    }
    // Binding pdfium and spawning ffmpeg both block; the probes run off the
    // async runtime.
    let present = tokio::task::spawn_blocking(move || {
        waiting
            .into_iter()
            .filter(|blocker| probe_blocker(*blocker))
            .collect::<Vec<_>>()
    })
    .await
    .map_err(|_| ApiError::internal("Blocker probe task failed"))?;
    heal_blocked_scan(index_db, present).await.map(|_| ())
}

/// The write half, with the probe results handed in: probing real binaries is
/// what makes this untestable, and the clearing is what has to be right.
///
/// Returns the total rows cleared across both tables.
async fn heal_blocked_scan(index_db: &str, present: Vec<Blocker>) -> ApiResult<u64> {
    if present.is_empty() {
        return Ok(0);
    }
    let cleared = call_index_db_writer(index_db, |reply| {
        IndexDbWriterMessage::ClearBlockedScanErrors {
            blockers: present.clone(),
            reply,
        }
    })
    .await?;
    let visuals_cleared = call_index_db_writer(index_db, |reply| {
        IndexDbWriterMessage::ClearBlockedVisualAttempts {
            blockers: present.clone(),
            reply,
        }
    })
    .await?;
    tracing::info!(
        cleared,
        visuals_cleared,
        blockers = ?present.iter().map(|blocker| blocker.as_str()).collect::<Vec<_>>(),
        "dependencies are available again; cleared their blocked scan failures"
    );
    Ok(cleared + visuals_cleared)
}

/// The negative cache's half of the orphan sweep, run right after the two
/// positive ones.
///
/// Deliberately not folded into the caller's "something was deleted" flag: that
/// flag gates the post-job VACUUM, and VACUUM is warranted by reclaiming blob
/// pages. These rows carry no blobs, so letting a handful of them trigger a
/// multi-minute rewrite of a multi-GB database would be a strictly worse trade
/// than leaving their pages on the freelist for the next real deletion.
/// Failures are logged, never propagated — a stale marker suppresses a
/// regeneration for content that is no longer indexed, which costs nothing.
async fn sweep_orphaned_visual_attempts(index_db: &str) {
    match call_index_db_writer(index_db, |reply| {
        IndexDbWriterMessage::DeleteOrphanedVisualAttempts { reply }
    })
    .await
    {
        Ok(0) => {}
        Ok(deleted) => tracing::info!(
            deleted,
            "cleared visuals attempts for content that left the index"
        ),
        Err(err) => tracing::warn!(error = ?err, "failed to sweep visuals attempts"),
    }
}

/// Reports what one write-once `items` stamp did.
///
/// The three outcomes are the same for every such column, and none of them is
/// a failure of the scan: `Ok(0)` is the ordinary race — another pass over
/// identical content measured the same thing first, and the statement's own
/// `IS NULL` guard is what makes the two agree by construction — and an error
/// leaves the column unexamined for the next scan to re-measure.
fn stamp_item_fact(sha256: &str, fact: &'static str, wrote: ApiResult<u64>) {
    match wrote {
        Ok(0) => tracing::debug!(sha256, fact, "no unexamined item to stamp"),
        Ok(_) => {}
        Err(err) => tracing::warn!(
            error = ?err,
            sha256,
            fact,
            "failed to stamp an item fact; it will be re-measured next scan"
        ),
    }
}

/// The generator versions, and what a bump to each one regenerates.
///
/// | constant | stamped on | a bump regenerates |
/// |---|---|---|
/// | `THUMBNAIL_PROCESS_VERSION` | `storage.thumbnails` | every display rendition, and for a video every frame extraction that feeds one |
/// | `FRAME_PROCESS_VERSION` | `storage.frames` | every stored video frame |
/// | `TIER_PROCESS_VERSION` | the **still** rows of `storage.thumbnail_tiers` | every grid tier and every animated poster — a re-encode of pictures the pass already holds |
/// | `LOOP_PROCESS_VERSION` | the `loop`/`loop-display` rows | every H.264 loop — an ffmpeg run per animated item |
///
/// [`RenditionKind::process_version`] reads the bottom two, and
/// [`visual_process_version`] reads the version a *marker* of each kind is
/// stamped with and consulted at. Kept in one block because the whole point
/// of splitting them is the cost difference in the right-hand column, and a
/// reader deciding which to bump has to see all four.
pub(crate) const THUMBNAIL_PROCESS_VERSION: i64 = 1;
pub(crate) const FRAME_PROCESS_VERSION: i64 = 1;
/// The grid tier generator's version, stamped on every **still**
/// `storage.thumbnail_tiers` row.
///
/// Its own constant, independent of [`THUMBNAIL_PROCESS_VERSION`], because
/// the two regenerate completely different work: bumping the tier version
/// must not re-extract every video's frames to rebuild a display rendition
/// that has not changed, and bumping the thumbnail version must not be the
/// only way to retire a tier.
///
/// Bump it for a generator change the *stored geometry cannot see* — a
/// different crop anchor, a different resampling filter, a different JPEG
/// quality. A change that moves the dimensions needs no bump: the
/// dispatcher's geometry comparison already catches it
/// ([`tier_geometry_matches`]).
pub(crate) const TIER_PROCESS_VERSION: i64 = 2;


/// The H.264 loop generator's version, stamped on the `loop`/`loop-display`
/// rows and on nothing else.
///
/// Split out of [`TIER_PROCESS_VERSION`] because the two regenerate work of
/// completely different cost: a still-encoder change (a new JPEG library, a
/// new quality, a new tier) is a re-encode of pictures the pass already holds,
/// while a loop is an ffmpeg run per animated item. Bumping the still tiers
/// must not drag every animation in the library through one
/// (docs/thumbnail-format-implementation.md §3).
///
/// Starts at 1 rather than inheriting the tier version's number: the loops
/// stored today were produced by exactly this encoder, so they are current.
pub(crate) const LOOP_PROCESS_VERSION: i64 = 1;
/// Minimum interval between mid-scan writes of the running counters to the
/// file_scans row (progress display only; the final update is unconditional).
pub(crate) const SCAN_PROGRESS_INTERVAL: Duration = Duration::from_secs(1);

struct FolderStats {
    new_items: i64,
    unchanged_files: i64,
    new_files: i64,
    modified_files: i64,
    marked_unavailable: i64,
    /// Not persisted on the scan row; only feeds [`ScanTotals::wrote_data`].
    backfilled_visuals: i64,
    /// Files the walk skipped on an active `scan_errors` verdict. Deliberately
    /// *not* folded into `errors`, which means "failed during this run": these
    /// files were not attempted at all. Logged at the end of the folder scan;
    /// `file_scans` has no column for them, and adding one would say a scan
    /// went worse than it did.
    known_bad: i64,
    /// Visuals generations this folder scan did not run because the negative
    /// cache already knows they produce nothing. Same reasoning as
    /// `known_bad`: not an error, not a write, no column.
    visuals_suppressed: i64,
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
            backfilled_visuals: 0,
            known_bad: 0,
            visuals_suppressed: 0,
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
    /// See [`GeneratedVisuals::tiers`].
    tiers: Vec<StoredTier>,
    frames: Vec<StoredImage>,
    blurhash: Option<String>,
    /// See [`GeneratedVisuals::transparency`].
    transparency: Option<bool>,
    /// What the visuals pass concluded about the kinds it produced nothing
    /// for. Empty on the healthy path.
    visual_verdicts: Vec<VisualVerdict>,
    /// The audit row a visuals failure owes, for the one class of failure that
    /// used to keep a file out of the index entirely. See
    /// [`visuals_audit_failure`].
    visuals_scan_error: Option<ScanErrorRecord>,
    /// Where this file's real content ends, when the outro pass produced a
    /// verdict (docs/video-outro-detection-design.md). `None` whenever the
    /// stage did not run or failed — the column holds verdicts only.
    outro: Option<OutroRecord>,
}

struct BackfillResult {
    sha256: String,
    mime_type: String,
    thumbnails: Vec<StoredImage>,
    /// See [`ProducedVisuals::tiers`] — `None` means this pass never
    /// considered tiers, `Some` (empty included) replaces the stored set.
    tiers: Option<Vec<StoredTier>>,
    /// See [`ProducedVisuals::drop_thumbnails`].
    drop_thumbnails: bool,
    extracted_frames: Vec<StoredImage>,
    blurhash: Option<String>,
    /// See [`NewItemData::visual_verdicts`].
    visual_verdicts: Vec<VisualVerdict>,
    /// See [`NewItemData::visuals_scan_error`]. Written here too so the row's
    /// `attempts` tracks the marker's instead of freezing at the new-item
    /// pass's 1 — see [`backfill_scan_error`].
    visuals_scan_error: Option<ScanErrorRecord>,
    /// See [`NewItemData::outro`].
    outro: Option<OutroRecord>,
    /// This file's stream codecs, when the backfill probed for them
    /// (docs/video-transcoding-design.md §6). `None` whenever the stage did not
    /// run or the probe failed — the columns hold answers only. There is no
    /// new-item twin: those codecs ride the `items` INSERT itself.
    codecs: Option<CodecRecord>,
    /// This file's measured animation length in seconds
    /// (docs/animated-image-spans-design.md §4), when the backfill measured
    /// one. `None` whenever the stage did not run or the file would not read
    /// — the column holds verdicts only, and 0.0 *is* one ("measured: still,
    /// or unparseable"). Like the codecs there is no new-item twin: the
    /// measurement rides the `items` INSERT itself.
    animation: Option<f64>,
    /// What this pass measured about the item's pixels (R4), or `None` when
    /// it decoded nothing. See [`GeneratedVisuals::transparency`].
    transparency: Option<bool>,
    /// This file's measured orientation in clockwise quarter turns
    /// (docs/display-dimensions-design.md §4), when the backfill examined one.
    /// `None` whenever the stage did not run or the probe failed — the column
    /// holds answers only, and the write it drives is the one non-idempotent
    /// write in the whole backfill, so a guess here would transpose an item's
    /// dimensions for good. Like the codecs there is no new-item twin: the
    /// measurement rides the `items` INSERT itself.
    rotation: Option<i64>,
    /// Design §7.1: this item newly turned positive *and* already had visuals,
    /// so the ones this pass produced replace them rather than filling a gap.
    /// The store guards are bypassed for exactly this case and no other.
    replace_visuals: bool,
}

impl BackfillResult {
    /// A pass that concluded *nothing at all*, for the one caller that has no
    /// pass to report: the visuals worker died.
    ///
    /// Every field is the "nothing was learned" value, and each is load-bearing
    /// rather than merely empty. No visual verdict, so the generation is
    /// retried next scan unmarked and unrecorded. `tiers: None` writes no set,
    /// so the stored one stays and the ladder question asks again. The
    /// animation length and the orientation stay `None` for the same reason —
    /// and for the orientation that is the only safe failure mode at all, its
    /// write being the single non-idempotent one in the backfill.
    fn inconclusive(sha256: String, mime_type: String) -> Self {
        Self {
            sha256,
            mime_type,
            thumbnails: Vec::new(),
            tiers: None,
            drop_thumbnails: false,
            extracted_frames: Vec::new(),
            blurhash: None,
            visual_verdicts: Vec::new(),
            visuals_scan_error: None,
            outro: None,
            codecs: None,
            animation: None,
            transparency: None,
            rotation: None,
            replace_visuals: false,
        }
    }
}

struct FailedFile {
    path: PathBuf,
    error: FileProcessError,
    /// The file exactly as the worker saw it — a verdict is only ever about
    /// these bytes, and `(last_modified, file_size)` is what the ledger
    /// re-checks before suppressing anything. `None` when the failure happened
    /// before the stat, or when the task itself died; both are transient and
    /// never recorded, so nothing is lost.
    stat: Option<(String, i64)>,
    /// The extension guess, so retry directives can target a format. `None`
    /// when guessing it is what failed.
    mime_type: Option<String>,
}

impl FailedFile {
    /// A failure with no recordable identity: every caller of this is a
    /// transient class (a dead worker task, a read that never got as far as a
    /// stat), which the ledger refuses anyway.
    fn transient(path: PathBuf, error: FileProcessError) -> Self {
        Self {
            path,
            error,
            stat: None,
            mime_type: None,
        }
    }
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
    /// The attempt token for every `visual_attempts` and `scan_errors` write
    /// of this *run*, which is the first root's scan id — see
    /// [`execute_folder_scan`]. Equal to `scan_id` for the first root, and
    /// for every single-root scan.
    attempt_scan_id: i64,
    scan_time: String,
    filescan_filter: Option<Arc<Match>>,
    /// Whether this scan examines videos for an appended outro
    /// (docs/video-outro-detection-design.md §8). Subordinate to `scan_video`:
    /// the pair is folded once, here, so every site downstream asks one
    /// question. Off leaves already-stored verdicts alone — it only stops
    /// future examinations.
    detect_outros: bool,
    /// This database's rendition format policy (R5), folded from
    /// `SystemConfig::thumbnail_formats` once per scan so every question and
    /// every generator in it reads the same one. Folding per item would let a
    /// mid-scan config edit make the dispatcher's prediction and the
    /// generator's output disagree, which is the one thing that stops the
    /// backfill terminating.
    formats: FormatPolicy,
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
    /// Every `scan_errors` row under this root, read once before the walk and
    /// keyed by [`fold_scan_path`]. Normally empty, so the healthy path costs
    /// one lookup in an empty map per file and no query at all — the whole
    /// point of preloading rather than asking per file across 90k files on a
    /// network mount.
    scan_errors: HashMap<String, ScanErrorSkip>,
    /// Which of those keys the walk actually reached (skipped or not). The
    /// difference against `scan_errors` is the end-of-root sweep, so the sweep
    /// is a set difference over ledger rows only — never over every file.
    seen_scan_errors: HashSet<String>,
    /// Keys whose row this run must not clear even though the file processes
    /// fine: an audit-only row (`decode`) whose bytes have not moved. Its file
    /// is indexed on every scan, so "the file succeeded" is not evidence about
    /// it — see [`ScanErrorSkip::cleared_by_success`]. Empty for every scan
    /// that has no such row, which is nearly all of them.
    pinned_scan_errors: HashSet<String>,
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
    attempt_scan_id: i64,
    scan_time: &str,
    options: ScanOptions,
) -> ApiResult<FolderStats> {
    let allowed_extensions = build_extension_set(config);
    let mut conn = open_index_db_read(index_db, user_data_db).await?;
    // One indexed read per root, before the walk. A library with no recorded
    // failures — the normal case — gets an empty map and pays nothing per file
    // from here on.
    let scan_errors = load_scan_errors_under(&mut conn, folder).await?;
    if !scan_errors.is_empty() {
        tracing::info!(
            folder,
            rows = scan_errors.len(),
            "loaded recorded scan failures for this root"
        );
    }
    let mut ctx = ScanContext {
        index_db: index_db.to_string(),
        scan_id,
        attempt_scan_id,
        scan_time: scan_time.to_string(),
        filescan_filter: parse_filescan_filter(config).map(Arc::new),
        detect_outros: config.scan_video && config.detect_outros,
        formats: config.format_policy(),
        semaphore: Arc::new(Semaphore::new(options.worker_count)),
        tasks: JoinSet::new(),
        task_paths: HashMap::new(),
        in_flight_visuals: HashSet::new(),
        stats: FolderStats::new(),
        timers: ScanTimers::default(),
        last_progress: Instant::now(),
        error_paths: Vec::new(),
        scan_errors,
        seen_scan_errors: HashSet::new(),
        pinned_scan_errors: HashSet::new(),
        conn,
    };

    // Every directory the walk could not read. A single one of these makes
    // "the walk never reached this path" ambiguous, which is what the sweep
    // reads as "the file is gone" — see `sweepable_scan_errors`.
    let mut walk_errors: u64 = 0;
    // Junk directories pruned below this root. Pruning one is how content
    // indexed under it in an earlier run gets retired — the walk no longer
    // reaches it, so it is marked unavailable and, if the user has that on,
    // deleted with its tags — and that is not something to do silently.
    let mut pruned_junk_dirs: u64 = 0;
    for entry in WalkDir::new(folder)
        .follow_links(true)
        .into_iter()
        .filter_entry(|entry| {
            if is_excluded(entry.path(), excluded_paths) {
                return false;
            }
            // Junk directories are pruned here rather than skipped per file,
            // so nothing under them is ever stat'd. Only directories are
            // judged: the rules for a file's own name live in the loop below,
            // and the root arrives at depth 0 exempt from both — a user who
            // registered a dot-named folder as an included root still gets it
            // scanned.
            let junk = entry.depth() > 0
                && entry.file_type().is_dir()
                && is_junk_dir_name(entry.file_name());
            if junk {
                pruned_junk_dirs += 1;
            }
            !junk
        })
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
                walk_errors += 1;
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

    if pruned_junk_dirs > 0 {
        tracing::info!(
            folder,
            pruned_dirs = pruned_junk_dirs,
            "skipped junk directories; anything indexed under them becomes unavailable"
        );
    }

    while let Some(joined) = ctx.tasks.join_next_with_id().await {
        ctx.handle_joined(joined).await?;
        ctx.maybe_report_progress().await;
    }

    let ScanContext {
        mut stats,
        timers,
        error_paths,
        scan_errors,
        seen_scan_errors,
        ..
    } = ctx;

    let vanished = sweepable_scan_errors(walk_errors, &scan_errors, &seen_scan_errors);
    if !vanished.is_empty() {
        match call_index_db_writer(index_db, |reply| IndexDbWriterMessage::DeleteScanErrors {
            paths: vanished.clone(),
            reply,
        })
        .await
        {
            Ok(swept) => {
                tracing::info!(folder, swept, "cleared scan failures for vanished files")
            }
            // Advisory: a lost sweep leaves a row describing a file that is no
            // longer there, which suppresses nothing and costs nothing.
            Err(err) => tracing::warn!(error = ?err, folder, "failed to sweep scan failures"),
        }
    }

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
    if stats.known_bad > 0 {
        tracing::info!(
            folder,
            known_bad = stats.known_bad,
            "skipped files with an active recorded scan failure"
        );
    }
    if stats.visuals_suppressed > 0 {
        tracing::info!(
            folder,
            visuals_suppressed = stats.visuals_suppressed,
            "skipped visuals generation with an active recorded attempt"
        );
    }
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

/// Which ledger rows the end-of-root sweep may clear: the stored paths of the
/// rows the walk never reached.
///
/// The sweep reads "not reached" as "the file is no longer there", which is
/// only sound when the walk actually saw the whole tree. A `WalkDir` error is
/// an unreadable directory (a permission change, a dropped mount, a network
/// share that timed out), and every file beneath it looks exactly like a file
/// that was deleted. Clearing then throws away verdicts for files that are
/// still on disk and hands the next scan all of their work back — the one
/// failure mode a *cache* must not have. So a walk with any error sweeps
/// nothing and the rows wait for a clean one; a stale row suppresses nothing
/// and costs nothing.
fn sweepable_scan_errors(
    walk_errors: u64,
    scan_errors: &HashMap<String, ScanErrorSkip>,
    seen: &HashSet<String>,
) -> Vec<String> {
    if scan_errors.is_empty() {
        return Vec::new();
    }
    if walk_errors > 0 {
        tracing::warn!(
            walk_errors,
            rows = scan_errors.len(),
            "the walk could not read part of the tree; deferring the scan-failure sweep"
        );
        return Vec::new();
    }
    scan_errors
        .iter()
        .filter(|(key, _)| !seen.contains(*key))
        // The *stored* path, not the folded key: on Windows they differ, and
        // the delete binds bytes.
        .map(|(_, entry)| entry.path.clone())
        .collect()
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
        let path_str = path.to_string_lossy().to_string();
        // The whole ledger side of the walk is gated on the map being
        // non-empty, which it is for essentially every scan there has ever
        // been: no fold, no hash, no lookup per file.
        //
        // Mark the path reached before anything can fail, so the end-of-root
        // sweep only ever clears rows for files the walk genuinely did not
        // see. A file that is here but unreadable today keeps its verdict —
        // and so does one this scan will reject at a *later* gate (the
        // filescan filter, a mime guess that no longer resolves). Such a row
        // suppresses nothing that would otherwise be processed, and keeping
        // it means a filter the user narrows and widens again does not cost a
        // full re-attempt of every broken file it swept out. That protection
        // only reaches gates from here on: a file the walk loop filtered out
        // *before* this point (a scan-type toggle dropping its extension, a
        // newly excluded folder) is never marked, so its rows are swept as if
        // the file had vanished, and re-enabling costs a fresh confirmation
        // per broken file. Bounded and rare, so deliberately not defended —
        // teaching the sweep the filter config is the cure worse than the
        // disease.
        let ledger_entry = if self.scan_errors.is_empty() {
            None
        } else {
            let key = fold_scan_path(&path_str);
            let found = self.scan_errors.get(&key).cloned();
            if found.is_some() {
                self.seen_scan_errors.insert(key);
            }
            found
        };

        let (last_modified, file_size) = match get_last_modified_time_and_size(&path) {
            Ok(value) => value,
            Err(err) => {
                tracing::info!(error = %err, path = %path.display(), "failed to stat file");
                self.stats.errors += 1;
                self.error_paths.push(path_str);
                return Ok(());
            }
        };

        // The whole point of the ledger: a file whose verdict is confirmed and
        // whose bytes have not moved is not hashed, probed or decoded again.
        // It is *not* an error of this run — nothing was attempted — but it
        // still has to stay out of unavailable-marking, exactly like a file
        // that failed, or a previously indexed copy of it would be marked gone
        // and then deleted.
        // An audit-only row (a visuals decode failure on a file that *is*
        // indexed) never suppresses, and a run that gets the file through is
        // no evidence against it either — only bytes that moved are. Pin it so
        // the success-path delete leaves it alone; a row whose bytes did move
        // is left unpinned and cleared normally, because it describes content
        // that no longer exists.
        if let Some(skip) = &ledger_entry
            && !skip.cleared_by_success(&last_modified, file_size)
        {
            self.pinned_scan_errors.insert(fold_scan_path(&path_str));
        }

        let suppressed = ledger_entry
            .as_ref()
            .is_some_and(|skip| skip.suppresses(&last_modified, file_size));
        if suppressed {
            tracing::debug!(
                path = %path.display(),
                "skipping a file with an active recorded scan failure"
            );
            self.stats.known_bad += 1;
            self.error_paths.push(path_str);
            return Ok(());
        }

        let mime_type = match infer_mime_type(&path) {
            Ok(mime) => mime,
            Err(error) => {
                self.fail_file(FailedFile {
                    path,
                    error,
                    stat: Some((last_modified, file_size)),
                    mime_type: None,
                })
                .await;
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
        if let Err(error) = require_html_renderer_for_indexing(&mime_type) {
            self.fail_file(FailedFile {
                path,
                error,
                stat: Some((last_modified, file_size)),
                mime_type: Some(mime_type),
            })
            .await;
            return Ok(());
        }

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
                self.fail_file(failed).await;
                Ok(())
            }
        }
    }

    /// The one place a file failure is accounted: the counters, the
    /// unavailable-marking exclusion, the log line, and — for the classes the
    /// ledger stores — the row that stops the next scan repeating the work.
    ///
    /// Never returns an error. A failed ledger write is advisory here (unlike
    /// the extraction job, whose completion classifier depends on it): the
    /// worst case is that the same file fails again next scan, which is
    /// exactly today's behavior.
    async fn fail_file(&mut self, failed: FailedFile) {
        let FailedFile {
            path,
            error,
            stat,
            mime_type,
        } = failed;
        self.stats.errors += 1;

        if matches!(error, FileProcessError::Filtered) {
            tracing::debug!(
                path = %path.display(),
                "file does not match the filescan filter (stage 2), skipping"
            );
            return;
        }

        let path_str = path.to_string_lossy().to_string();
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
        self.error_paths.push(path_str.clone());

        // Transient classes are never recorded: the file simply fails this run
        // and is retried untouched, which is what "transient" means.
        let (Some(failure), Some((last_modified, file_size))) = (classified, stat) else {
            return;
        };
        let mut record = ScanErrorRecord {
            path: path_str,
            last_modified,
            file_size,
            stage: failure.stage.to_string(),
            kind: failure.kind,
            mime_type,
            error: failure.message.clone(),
            skip_after: failure.skip_after,
        };
        // The mime type recorded so far is the *name's* guess. Where the bytes
        // can contradict it outright, ask the bytes.
        override_mime_from_content(&mut record, &path);
        let scan_id = self.attempt_scan_id;
        if let Err(err) = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::UpsertScanError {
                record: record.clone(),
                // The run's token, not this root's scan row: `attempts` dedups
                // on it (a scan-less write would stop counting across runs),
                // and two registered roots that alias the same file on a
                // case-insensitive filesystem would otherwise let one run
                // confirm a `skip_after = 2` verdict it only saw fail once.
                scan_id: Some(scan_id),
                reply,
            }
        })
        .await
        {
            tracing::warn!(
                error = ?err,
                path = %path.display(),
                "failed to record a scan failure; it will be re-attempted next scan"
            );
        }
    }

    /// The success path: a file the scan just got through owes no ledger row.
    ///
    /// Gated on the preloaded map, so a healthy library pays no writer
    /// round-trip (and no search-cache epoch bump) per successful file — the
    /// map is empty for almost every scan there has ever been. Deliberately
    /// not gated on the row being *active*: the only rows a success can own
    /// are the sub-threshold ones a single blip left behind, and leaving those
    /// would let a second blip years later confirm a verdict on a file that
    /// has succeeded a thousand times in between.
    async fn clear_scan_error(&mut self, path: &str) {
        if self.scan_errors.is_empty() {
            return;
        }
        let key = fold_scan_path(path);
        // Pinned by the walk: the row survives its own file's success (see
        // `scan_path`).
        if self.pinned_scan_errors.contains(&key) {
            return;
        }
        // The row is deleted by the path that is actually stored, which on
        // Windows need not be the casing the walk produced.
        let Some(stored) = self.scan_errors.get(&key).map(|entry| entry.path.clone()) else {
            return;
        };
        match call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::DeleteScanError {
                path: stored.clone(),
                reply,
            }
        })
        .await
        {
            Ok(cleared) => {
                if cleared > 0 {
                    self.scan_errors.remove(&key);
                    tracing::info!(path, "cleared a recorded scan failure after a good pass");
                }
            }
            // Advisory: a lost delete costs one wasted re-attempt after the
            // file has already succeeded, never correctness.
            Err(err) => tracing::warn!(error = ?err, path, "failed to clear a scan failure"),
        }
    }

    /// The false-change disproof: a re-hash under a new mtime produced the
    /// same sha256, so the bytes a ledger row describes are still the bytes
    /// on disk. An audit-only row is re-keyed to the new stat and pinned
    /// against this pass's success-side clear — without this, a touch (mtime
    /// moved, bytes identical) deletes the audit record while the sha-keyed
    /// `visual_attempts` marker keeps suppressing: silent suppression with
    /// nothing on the failures surface, which is the state the audit rows
    /// exist to prevent. A blocking-stage row is left alone: reaching the
    /// false-change branch means the file just cleared every gate it used to
    /// fail, so the normal success-side clear is the right outcome for it.
    async fn rekey_audit_scan_error(&mut self, path: &str, last_modified: &str, file_size: i64) {
        if self.scan_errors.is_empty() {
            return;
        }
        let key = fold_scan_path(path);
        let Some(entry) = self.scan_errors.get_mut(&key) else {
            return;
        };
        if stage_blocks_indexing(&entry.stage) {
            return;
        }
        // The in-memory copy moves with the stored row, so every later
        // decision this run makes sees the row it just wrote.
        entry.last_modified = last_modified.to_string();
        entry.file_size = file_size;
        let stored = entry.path.clone();
        self.pinned_scan_errors.insert(key);
        if let Err(err) = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::RekeyScanError {
                path: stored.clone(),
                last_modified: last_modified.to_string(),
                file_size,
                reply,
            }
        })
        .await
        {
            // Advisory, like the success-side clear: a lost rekey costs one
            // stale-keyed audit row, never correctness.
            tracing::warn!(
                error = ?err,
                path,
                "failed to rekey an audit row after a false change"
            );
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
            // Before `update_file_data` below, whose success-side clear the
            // pin this takes has to beat.
            self.rekey_audit_scan_error(&path_str, &last_modified, real_size)
                .await;
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

        self.dispatch_prepare(
            path,
            last_modified,
            real_size,
            reported_size,
            mime_type,
            md5,
            sha256,
        )
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

        // No positive-cache guard, unlike the two stores around it: the tier
        // write replaces the item's whole set, so identical content racing
        // this rewrites the same bytes rather than colliding.
        //
        // An empty set is skipped only when there is also nothing stored to
        // contradict. "A new item has nothing stored" is *nearly* always
        // true and not quite: `storage.thumbnail_tiers` is keyed by content
        // hash and outlives the `items` row until the orphan sweep runs, so
        // content that was deindexed and reappears can meet a set from an
        // older rule here. One EXISTS read per new item, and only when the
        // pass produced no tier at all, buys the guarantee that this path
        // never leaves one behind.
        if !item.tiers.is_empty()
            || has_thumbnail_tiers(&mut self.conn, &item.sha256)
                .await
                .unwrap_or(false)
        {
            self.store_tiers(&item.sha256, &item.mime_type, item.tiers.clone())
                .await;
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

        // After the stores, so a marker can never be written for a kind this
        // very pass just stored (the store's own delete runs in its
        // transaction, but ordering the two makes that irrelevant rather than
        // load-bearing).
        self.record_visual_attempts(&item.visual_verdicts, &item.sha256, &item.mime_type)
            .await;

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
        // Strictly after the write that inserts the `items` row this updates.
        self.record_outro_verdict(&item.sha256, item.outro.as_ref())
            .await;
        // Likewise: the column lives on the row the write above just inserted.
        self.record_item_transparency(&item.sha256, item.transparency)
            .await;
        // Strictly after the file write, because that write clears this path's
        // ledger row: the item indexed, so whatever verdict the path carried
        // from a previous version of itself is spent — and the row this pass
        // owes describes the bytes that just went in.
        self.record_visuals_scan_error(item.visuals_scan_error)
            .await;
        Ok(())
    }

    /// Records the audit row an indexed-without-visuals image owes, so a
    /// failure that no longer blocks anything is still visible in the failures
    /// surface (requirement 4) rather than only in a log line.
    ///
    /// Never fails the file — nothing about the item depends on it — and never
    /// touched at all on the healthy path.
    async fn record_visuals_scan_error(&mut self, record: Option<ScanErrorRecord>) {
        let Some(record) = record else {
            return;
        };
        let scan_id = self.attempt_scan_id;
        if let Err(err) = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::UpsertScanError {
                record: record.clone(),
                // The run's token, for the same aliased-roots reason as every
                // other attempt-counting write of this walker.
                scan_id: Some(scan_id),
                reply,
            }
        })
        .await
        {
            tracing::warn!(
                error = ?err,
                path = %record.path,
                "failed to record a visuals decode failure for auditing"
            );
        }
    }

    /// Whether an active marker settles the full decode of this content.
    ///
    /// The thumbnail kind is the one that answers "can this content be decoded
    /// at all": the thumbnail, the frames and the blurhash all come out of the
    /// same pass, so suppressing it suppresses them.
    ///
    /// Markers are advisory: a read that fails costs one regenerated nothing,
    /// and must never abort the folder scan over a cache whose whole purpose
    /// is saving work.
    async fn thumbnail_marker_suppresses(&mut self, sha256: &str, path: &Path) -> bool {
        self.marker_suppresses(sha256, path, VisualKind::Thumbnail)
            .await
    }

    /// The same consult for the **animated loop**
    /// (docs/grid-scroll-performance-implementation.md §2, step B2), which is
    /// its own kind and its own version.
    ///
    /// Scoped as narrowly as it is written: a `loop` marker suppresses the
    /// animated ladder and nothing else. It must never reach the display
    /// rendition, because a loop fails on files whose pixels decoded
    /// perfectly — the posters came out of that decode — and the display rule
    /// can start wanting a still for such an item at any time.
    ///
    /// The converse coupling is real and deliberate: a *thumbnail* marker
    /// does suppress the animated ladder, because the ladder starts with the
    /// very decode that marker is a verdict about.
    async fn loop_marker_suppresses(&mut self, sha256: &str, path: &Path) -> bool {
        self.marker_suppresses(sha256, path, VisualKind::Loop).await
    }

    /// One kind's negative-cache consult, at that kind's own generator
    /// version ([`visual_process_version`]) — never another's, or a marker
    /// would expire on the wrong bump.
    ///
    /// Markers are advisory: a read that fails costs one regenerated nothing,
    /// and must never abort the folder scan over a cache whose whole purpose
    /// is saving work.
    async fn marker_suppresses(&mut self, sha256: &str, path: &Path, kind: VisualKind) -> bool {
        match visuals_suppressed(&mut self.conn, sha256, kind, visual_process_version(kind)).await {
            Ok(suppressed) => suppressed,
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    path = %path.display(),
                    kind = kind.as_str(),
                    "failed to read the visuals negative cache; regenerating"
                );
                false
            }
        }
    }

    /// Counts (and logs) one whole dispatch the negative cache removed.
    ///
    /// Deliberately not called at the marker check itself: clearing
    /// `needs_thumb` is not yet a saved generation — the file can still owe a
    /// blurhash that only a dispatch can produce — and a stat that counted
    /// those would claim work that was in fact still done.
    fn note_suppressed_visuals(&mut self, path: &Path) {
        tracing::debug!(
            path = %path.display(),
            "skipping visuals generation with an active recorded attempt"
        );
        self.stats.visuals_suppressed += 1;
    }

    /// Writes what a visuals pass concluded, so the next scan does not repeat
    /// it. Never fails the file: a lost marker costs one regenerated nothing,
    /// which is exactly the behaviour this whole cache replaces.
    ///
    /// Empty for every healthy file, and the emptiness is checked before the
    /// writer is touched — a library with visuals pays nothing here, not even
    /// a message.
    async fn record_visual_attempts(
        &mut self,
        verdicts: &[VisualVerdict],
        sha256: &str,
        mime_type: &str,
    ) {
        if verdicts.is_empty() {
            return;
        }
        let records = visual_attempt_records(verdicts, sha256, mime_type);
        let scan_id = self.attempt_scan_id;
        if let Err(err) = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::UpsertVisualAttempts {
                records: records.clone(),
                // The run's token, not this root's scan row: markers are keyed
                // by content, and content duplicated under two roots would
                // otherwise be counted twice by a single run — confirming a
                // `skip_after = 2` verdict that has only failed once.
                scan_id: Some(scan_id),
                reply,
            }
        })
        .await
        {
            tracing::warn!(
                error = ?err,
                sha256,
                "failed to record a visuals attempt; it will be regenerated next scan"
            );
        }
    }

    /// Stores one genuine outro verdict, which also drops the probe's failure
    /// marker in the same transaction
    /// (docs/video-outro-detection-design.md §7.2).
    ///
    /// Never fails the file: a lost verdict costs one re-probe on the next
    /// scan, which is the same trade every writer of the negative cache makes.
    /// Empty for every non-video and for every item already examined, and the
    /// emptiness is checked before the writer is touched.
    async fn record_outro_verdict(&mut self, sha256: &str, record: Option<&OutroRecord>) {
        let Some(record) = record else {
            return;
        };
        match call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::SetOutroVerdict {
                sha256: sha256.to_string(),
                outro_kind: record.kind.clone(),
                content_end_ms: record.content_end_ms,
                reply,
            }
        })
        .await
        {
            // The item went away between the pass and this write (deleted, or
            // never inserted because its file write failed). Nothing to fix:
            // the next scan re-examines whatever is there.
            Ok(0) => tracing::debug!(sha256, "no item to store an outro verdict on"),
            Ok(_) => {}
            Err(err) => tracing::warn!(
                error = ?err,
                sha256,
                "failed to store an outro verdict; it will be re-probed next scan"
            ),
        }
    }

    /// Stores one item's stream codecs (docs/video-transcoding-design.md §6).
    ///
    /// Never fails the file, for the same reason its outro twin does not: a
    /// lost record costs one re-probe on the next scan. Empty for every
    /// non-video and every item already probed, and the emptiness is checked
    /// before the writer is touched.
    async fn record_item_codecs(&mut self, sha256: &str, record: Option<&CodecRecord>) {
        let Some(record) = record else {
            return;
        };
        match call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::SetItemCodecs {
                sha256: sha256.to_string(),
                video_codec: record.video_codec.clone(),
                audio_codec: record.audio_codec.clone(),
                reply,
            }
        })
        .await
        {
            // The item went away between the pass and this write (deleted, or
            // never inserted because its file write failed). Nothing to fix:
            // the next scan re-probes whatever is there.
            Ok(0) => tracing::debug!(sha256, "no item to store codecs on"),
            Ok(_) => {}
            Err(err) => tracing::warn!(
                error = ?err,
                sha256,
                "failed to store item codecs; they will be re-probed next scan"
            ),
        }
    }

    /// Stores one item's measured animation length
    /// (docs/animated-image-spans-design.md §4).
    ///
    /// Never fails the file, for the same reason its codec twin does not: a
    /// lost record costs one re-measure on the next scan. Empty for
    /// everything but a gif/webp/avif this pass measured, and the emptiness
    /// is checked before the writer is touched. The write itself is guarded
    /// on `duration IS NULL`, so 0 affected rows also covers the item another
    /// task measured (or that left the index) meanwhile.
    async fn record_item_animation(&mut self, sha256: &str, seconds: Option<f64>) {
        let Some(seconds) = seconds else {
            return;
        };
        match call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::SetAnimationDuration {
                sha256: sha256.to_string(),
                seconds,
                reply,
            }
        })
        .await
        {
            Ok(0) => tracing::debug!(sha256, "no unmeasured item to store an animation length on"),
            Ok(_) => {}
            Err(err) => tracing::warn!(
                error = ?err,
                sha256,
                "failed to store an animation length; it will be re-measured next scan"
            ),
        }
    }

    /// Stamps one item's measured orientation, which also transposes its
    /// dimensions when the turn is an odd one
    /// (docs/display-dimensions-design.md §4).
    ///
    /// `Ok(0)` is the ordinary race, not an error: another pass over identical
    /// content stamped the column first, and because the swap and the stamp
    /// are the same guarded statement, that pass transposed the dimensions
    /// exactly once too.
    async fn record_item_rotation(&mut self, sha256: &str, quarter_turns: Option<i64>) {
        let Some(quarter_turns) = quarter_turns else {
            return;
        };
        let wrote = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::SetItemRotation {
                sha256: sha256.to_string(),
                quarter_turns,
                reply,
            }
        })
        .await;
        stamp_item_fact(sha256, "rotation", wrote);
    }

    /// Stamps one item's measured transparency (R4).
    async fn record_item_transparency(&mut self, sha256: &str, has_transparency: Option<bool>) {
        let Some(has_transparency) = has_transparency else {
            return;
        };
        let wrote = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::SetItemTransparency {
                sha256: sha256.to_string(),
                has_transparency,
                reply,
            }
        })
        .await;
        stamp_item_fact(sha256, "transparency", wrote);
    }

    /// Writes one item's whole grid tier set. Failures are logged and skipped,
    /// like every other visuals store: the next scan re-derives them, and a
    /// missing tier degrades to the next larger rendition on the serving
    /// ladder rather than to a broken image.
    async fn store_tiers(&mut self, sha256: &str, mime_type: &str, tiers: Vec<StoredTier>) {
        if let Err(err) = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::StoreThumbnailTiers {
                sha256: sha256.to_string(),
                mime_type: mime_type.to_string(),
                tiers: tiers.clone(),
                reply,
            }
        })
        .await
        {
            tracing::error!(error = ?err, "failed to store thumbnail tiers");
        }
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

        // Counted whether or not the individual stores succeed: an attempted
        // write is enough for the WAL to have grown, which is what the
        // boundary's `wrote_data` flag is about.
        let mut wrote_visuals = false;

        // Storage failures for backfilled visuals are logged and skipped so a
        // single bad file cannot abort the scan; the next scan retries them.
        // `replace_visuals` is the one case that writes over a stored visual:
        // this item newly turned positive, so what is stored was sampled
        // across the card and the pass just rebuilt it against the clamped
        // range (design §7.1). `store_thumbnails`/`store_frames` already
        // delete-then-insert at the same version, so bypassing the guard is
        // all it takes.
        if !backfill.thumbnails.is_empty() && (backfill.replace_visuals || !already_stored) {
            wrote_visuals = true;
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

        // The one write that *removes* a stored visual without replacing it:
        // the display rule is short-side based now, so an item it serves from
        // its original can still be carrying the rendition the old long-side
        // rule stored — an 800x20000 webtoon crushed to 163x4096, which the
        // serving path would go on preferring to the original forever. Guarded
        // on something actually being stored, so the overwhelmingly common
        // case (a directly-served image that never had a rendition) issues no
        // write at all.
        if backfill.drop_thumbnails && already_stored {
            wrote_visuals = true;
            if let Err(err) =
                call_index_db_writer(&self.index_db, |reply| {
                    IndexDbWriterMessage::DeleteThumbnails {
                        sha256: backfill.sha256.clone(),
                        reply,
                    }
                })
                .await
            {
                tracing::error!(error = ?err, "failed to drop stale thumbnails");
            }
        }

        // Unlike the two stores around it, an *empty* set is a real
        // instruction here — "this item wants no stored tier" — and the
        // delete it carries is how a set from an older rule is retired. Only
        // `None` means the pass never considered tiers.
        if let Some(tiers) = &backfill.tiers {
            wrote_visuals = true;
            self.store_tiers(&backfill.sha256, &backfill.mime_type, tiers.clone())
                .await;
        }

        let frames_stored = has_frame(&mut self.conn, &backfill.sha256, FRAME_PROCESS_VERSION)
            .await
            .unwrap_or(false);
        if !backfill.extracted_frames.is_empty() && (backfill.replace_visuals || !frames_stored) {
            wrote_visuals = true;
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
            wrote_visuals = true;
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

        self.record_visual_attempts(
            &backfill.visual_verdicts,
            &backfill.sha256,
            &backfill.mime_type,
        )
        .await;
        // The confirmation half of the audit row the new-item pass opened. The
        // path's ledger row is pinned by the walker before this point (an
        // audit-only row is not spent by its file's success), so this upsert
        // finds the row it means to increment rather than re-inserting one.
        self.record_visuals_scan_error(backfill.visuals_scan_error)
            .await;
        // The item already exists on this path, so there is no write to order
        // against; last only because the marker delete it carries should not
        // race the marker write above (they are mutually exclusive, and this
        // makes that irrelevant rather than load-bearing).
        self.record_outro_verdict(&backfill.sha256, backfill.outro.as_ref())
            .await;
        self.record_item_codecs(&backfill.sha256, backfill.codecs.as_ref())
            .await;
        self.record_item_animation(&backfill.sha256, backfill.animation)
            .await;
        self.record_item_rotation(&backfill.sha256, backfill.rotation)
            .await;
        self.record_item_transparency(&backfill.sha256, backfill.transparency)
            .await;

        if wrote_visuals {
            self.stats.backfilled_visuals += 1;
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
        // The positive cache's answer, and only that: is there a stored
        // rendition of this content? Two rules read it as such — §7.1's "is
        // there anything here to replace" and the orientation question's "does
        // this item have visuals a transformed picture would make stale" — and
        // both are asked long after the branches below have moved
        // `generate_thumbnail` on to a different question.
        let thumbnail_stored =
            has_thumbnail(&mut self.conn, &sha256, THUMBNAIL_PROCESS_VERSION).await?;
        // "Run the thumbnail half of a pass", which starts out as the positive
        // cache's answer inverted and is then decided by the image rule, the
        // negative cache and the ladder question in turn.
        let mut generate_thumbnail = !thumbnail_stored;
        let needs_blurhash = !has_blurhash(&mut self.conn, &sha256).await?;
        // *Why* a thumbnail is not needed decides what the rest of this
        // function may still do: a stored or served-directly thumbnail says
        // nothing about the blurhash, while a marker-suppressed one is a
        // verdict on decoding this content at all.
        let mut thumb_suppressed = false;
        // One stat and one index read for the whole image half of this
        // dispatch. The served-directly predicate below and the ladder
        // question after it are both pure functions of `(bytes, width,
        // height)` about the same file, and this scans a library over SMB: a
        // second stat per image per scan is a network round trip for an
        // answer already in hand.
        let image_facts = if mime_type.starts_with("image") {
            self.image_facts(&sha256, &path).await
        } else {
            None
        };
        if generate_thumbnail && mime_type.starts_with("image") {
            // Images served from the original file never get a stored
            // thumbnail, so `has_thumbnail` stays false for them forever.
            // Decide from the indexed dimensions instead of decoding, or every
            // rescan re-decodes every such image to produce nothing.
            generate_thumbnail = match &image_facts {
                // Unreadable now: leave the visuals to a later scan.
                None => false,
                Some(facts) => !image_is_served_directly(&mime_type, facts, self.formats),
            };
        }
        // Which rendition ladder this item's grid tiers come from. Answered
        // here rather than inside the ladder question because the generator
        // needs it even when the ladder question found no work: an animated
        // image missing its display rendition still runs the image pass, and
        // that pass must produce the animated set rather than static tiers.
        let ladder = grid_ladder(&mime_type, image_facts.as_ref());
        // The ladder question
        // (docs/grid-scroll-performance-implementation.md §3, B1): "does this
        // item carry the renditions the current ladder would produce?" Asked
        // before the negative cache, because for an image the answer *is* a
        // decode of the original — the same decode a marker settles — and
        // suppression has to cover it.
        // Asked for *every* item, animated ones included.
        // [`GridLadder::Nothing`] is not "no question": it is the answer "the
        // set this item wants is empty", and an item carrying a stale set has
        // to be told so (see [`TierWork::Retire`]).
        let question = self
            .pending_tier_work(&sha256, &mime_type, &path, image_facts.as_ref(), ladder)
            .await;
        let mut tier_work = question.work;
        // The transparency question
        // (docs/thumbnail-format-implementation.md §2, R4): "is this an image
        // whose pixels nothing has examined?" Asked here because its answer is
        // only ever reachable through a pass that decodes, and because an
        // image that owes nothing else must still be examined once — or the
        // partial index's population never drains and the item is reconsidered
        // on every scan forever.
        //
        // It travels as itself, never disguised as a ladder verdict: an image
        // with nothing else owing rides the ordinary image pass to have its
        // pixels looked at, and that pass owes no rendition at all — not the
        // display one, not the tier set the geometry comparison already found
        // correct. Answering it with a `TierWork` said the opposite, and a
        // whole-set delete-and-insert per already-correct image is the
        // library-wide cost of saying it.
        let mut transparency_work = self
            .pending_transparency_work(&sha256, &mime_type, &path)
            .await;
        if matches!(tier_work, Some(TierWork::Image { .. })) {
            generate_thumbnail = true;
        }
        // Last, after the positive cache missed *and* the served-directly
        // predicate said a thumbnail would be stored: the negative cache. This
        // is the whole point of the table — a broken PDF, a video with no
        // video track, a page no installed browser can render would otherwise
        // be regenerated on every scan, forever.
        //
        // Only the thumbnail kind is consulted here because only the thumbnail
        // is dispatched: video frames come out of the same pass, so suppressing
        // the thumbnail suppresses their extraction too.
        //
        // The animated ladder is asked the same question even when no
        // thumbnail is: its posters come from a decode of the *original*,
        // which is exactly what a marker has a verdict about, and an animated
        // item is usually served directly at the display tier (so
        // `needs_thumb` is false for it and the gate would never be reached).
        //
        // The transparency question joins them for the same reason: its only
        // answer is that decode, so an item whose decode a marker has already
        // settled must be turned away here or the question is asked on every
        // scan for the rest of the file's life.
        let animated_ladder = matches!(tier_work, Some(TierWork::Animated));
        if (generate_thumbnail || animated_ladder || transparency_work)
            && self.thumbnail_marker_suppresses(&sha256, &path).await
        {
            if generate_thumbnail {
                generate_thumbnail = false;
                thumb_suppressed = true;
            }
            // The image and animated ladders both *start* with that decode,
            // so a thumbnail marker settles them too. A `Derived` set is
            // untouched: it decodes stored q85 JPEGs, not the file the marker
            // has a verdict about, and `Retire` is a delete that needs no
            // source at all.
            if matches!(
                tier_work,
                Some(TierWork::Image { .. } | TierWork::Animated)
            ) {
                tier_work = None;
            }
            // The transparency question goes with it, and it goes *answered*.
            // Its only answer is a decode of the original, which is precisely
            // what the marker has a verdict about — so leaving the column NULL
            // would dispatch a visuals task with nothing to do on every scan
            // for the rest of this file's life, and never drain the pending
            // index. `0` is the same terminal verdict `rotation_pass_for`
            // writes for a header this build deterministically cannot read: a
            // picture nobody can decode has no renditions for anything to be
            // transparent in. Through the same write-once setter, so a pass
            // over identical content that *can* decode still wins the race by
            // construction.
            if transparency_work {
                transparency_work = false;
                self.record_item_transparency(&sha256, Some(UNDECODABLE_HAS_TRANSPARENCY))
                    .await;
            }
        }
        // The animated ladder's own marker, and deliberately a separate
        // consult rather than another use of the one above. It runs *after*,
        // on whatever survived: a `loop` marker means the decode was fine and
        // the encode was not, so it may retire the ladder and nothing else —
        // never `needs_thumb`, never the display rendition this file is
        // perfectly capable of producing (and may start owing the moment the
        // display rule flips for it).
        let mut ladder = ladder;
        if matches!(tier_work, Some(TierWork::Animated))
            && self.loop_marker_suppresses(&sha256, &path).await
        {
            tier_work = None;
            // The generator's obligation goes with it. `ladder` is threaded
            // separately from `tier_work` so that an animated image running
            // the image pass for its *display* half still produces the
            // animated set out of that one decode — which is exactly the case
            // this marker has to reach, or a suppressed item would re-run
            // ffmpeg on every scan that touches it for any other reason.
            ladder = GridLadder::Unknown;
        }
        // The outro question (docs/video-outro-detection-design.md
        // §7): "is this a video nothing has examined for an appended outro?"
        // Unlike the thumbnail and blurhash questions it is not about a
        // missing visual, so a video
        // with both a thumbnail and a blurhash is still work — that is exactly
        // what backfills an existing library, with no migration and no
        // separate job.
        let mut outro_work = self.pending_outro_work(&sha256, &mime_type, &path).await;
        if let Some(pending) = &outro_work
            && !outro_needs_probe(pending.video_tracks)
        {
            // Answered from the item's own metadata, so it needs no worker and
            // no ffmpeg: settled here, before any of the early returns below
            // can drop it on the floor. The verdict is constructed rather than
            // taken from `run_outro_pass`, which is documented blocking-only
            // and must not be reachable from this async thread at all.
            self.record_outro_verdict(&sha256, Some(&outro_verdict_without_a_probe()))
                .await;
            outro_work = None;
        }
        // The codec question (docs/video-transcoding-design.md
        // §6): "is this a video whose stream codecs nothing has recorded?"
        // Like the outro question it is not about a missing visual, so a video
        // with every visual it needs is still work — that is what backfills an
        // existing library, with no separate job. Unlike it, nothing switches
        // it off: codecs are scan metadata like width and height, and — unlike
        // the outro question above — there is no metadata shortcut either. A
        // container the index records no video track for still has an *audio*
        // codec worth recording, and only the probe can name it; settling it
        // here from the track count would write `'none'` with a NULL audio
        // column and terminate the backfill on a half answer.
        let codec_work = self
            .pending_codec_work(&sha256, &mime_type, &path)
            .await
            .is_some();
        // The animation question (docs/animated-image-spans-design.md
        // §4): "is this a gif/webp/avif whose animation length nothing has
        // measured?" Cut from the same cloth as the codec question — not a
        // missing visual, so an image with every visual it needs is still
        // work, which is exactly what backfills an existing library with no
        // migration and no separate job. Answered from the index; a stamped
        // value (the 0.0 verdict included) costs nothing on every scan after.
        let animation_work = self
            .pending_animation_work(&sha256, &mime_type, &path)
            .await;
        // The orientation question (docs/display-dimensions-design.md
        // §4): "is this an image or a video whose orientation nothing has
        // examined?" Cut from the same cloth as the codec and animation
        // questions — not a missing visual, so an item with every visual it
        // needs is still work, which is what backfills an existing library
        // with no separate job.
        //
        // It carries what the *worker* will need and cannot ask for itself:
        // whether this item has visuals that a transformed picture would make
        // stale (§4.1) — the positive cache's own answer, for exactly the
        // reason the outro's replacement rule reads it.
        let rotation_work = self
            .pending_rotation_work(&sha256, &mime_type, &path)
            .await
            .then_some(RotationBackfill {
                thumbnail_stored,
                blurhash_stored: !needs_blurhash,
            });
        // Every question is answered now, so the rest of the dispatch works
        // from one value. The three early returns below share [`
        // PendingBackfillWork::any`] rather than each spelling the same chain
        // of questions out, which is how one of them came to omit one.
        let mut work = PendingBackfillWork {
            thumbnail: generate_thumbnail,
            blurhash: needs_blurhash,
            outro: outro_work.map(|item| OutroBackfill {
                // §7.1 needs facts this dispatch has not gathered yet; filled
                // in below, once they are.
                replaces_visuals: false,
                item,
            }),
            codec: codec_work,
            animation: animation_work,
            rotation: rotation_work,
            transparency: transparency_work,
            indexed: image_facts,
            tier: tier_work,
            formats: self.formats,
            ladder,
            reusable_loops: question.reusable_loops,
            existing_frames: Vec::new(),
            existing_thumb: None,
            frames_stored: false,
            video_duration: 0.0,
            stored_content_end_ms: None,
        };
        if !work.thumbnail && !work.blurhash {
            // Counted before the remaining questions are consulted: the marker
            // did remove a whole visuals dispatch, whether or not a probe still
            // owes this file a run of its own.
            if thumb_suppressed {
                self.note_suppressed_visuals(&path);
            }
            if !work.any() {
                return Ok(());
            }
        }
        // Identical content elsewhere in this scan already has a visuals task
        // in flight; its results apply to this sha256 as well.
        if self.in_flight_visuals.contains(&sha256) {
            return Ok(());
        }

        // Whether the fetch below actually ran, which decides whether
        // `work.existing_frames` is evidence about `storage.frames` or merely
        // the default. See `frames_stored`.
        let mut frames_fetched = false;
        if work.thumbnail && mime_type.starts_with("video") {
            // Frames already stored in the database can rebuild the thumbnail
            // even when the item's duration metadata is missing; only a fresh
            // ffmpeg extraction needs a usable duration (matching Python,
            // which consults metadata only when no frames exist).
            work.existing_frames = get_frames_bytes(&mut self.conn, &sha256).await?;
            frames_fetched = true;
            if work.existing_frames.is_empty() {
                if let Some((duration, video_tracks)) =
                    get_item_visual_meta(&mut self.conn, &sha256).await?
                {
                    let duration = duration.unwrap_or(0.0);
                    if duration <= 0.0 || video_tracks.unwrap_or(0) <= 0 {
                        tracing::debug!(
                            path = %path.display(),
                            "skipping video thumbnail generation due to missing video track"
                        );
                        // The same conclusion `build_new_item_renditions`
                        // records for this video, and the one that matters
                        // most: an item indexed before this cache existed has
                        // never been through the new-item path, so without
                        // this write every track-less video in an existing
                        // library is re-dispatched and re-decided on every
                        // scan, forever. Both kinds, honestly — this branch is
                        // inside `existing_frames.is_empty()`, so nothing is
                        // stored for either, and the decision came from
                        // indexed metadata that only a re-index (new content,
                        // new key) or a generator bump can change.
                        self.record_visual_attempts(
                            &[
                                VisualVerdict::nothing(VisualKind::Thumbnail),
                                VisualVerdict::nothing(VisualKind::Frame),
                            ],
                            &sha256,
                            &mime_type,
                        )
                        .await;
                        if !work.any() {
                            return Ok(());
                        }
                        // A probe is still owed and needs neither frames nor a
                        // duration to reach a verdict, so the dispatch goes
                        // ahead with the thumbnail half switched off.
                        work.thumbnail = false;
                    } else {
                        work.video_duration = duration;
                    }
                }
            }
        }
        if !work.thumbnail && work.blurhash {
            work.existing_thumb = get_thumbnail_bytes(&mut self.conn, &sha256, 0).await?;
        }
        // A blurhash can only come from a stored thumbnail or the image itself.
        // With no stored thumbnail, a non-image has no second source at all —
        // and neither, in practice, does a *suppressed* image: its only
        // remaining source is a fresh full decode of the original, which is
        // precisely the decode the thumbnail marker's verdict already settled.
        // Falling through here would re-open and re-decode the file on every
        // single scan, the exact waste this table exists to kill.
        if !work.thumbnail && work.blurhash && work.existing_thumb.is_none() {
            let mut no_source = !mime_type.starts_with("image");
            // Reaching here with an unsuppressed image means the marker was
            // never consulted above, because the image would never store a
            // thumbnail (it is served from its original file) — the majority
            // of every image library. Its blurhash costs the same full decode
            // all the same, and since undecodable images are indexed rather
            // than rejected, that decode is exactly the one a marker can
            // already have settled. One query, and only for an image that
            // still owes a blurhash.
            if !no_source && !thumb_suppressed {
                thumb_suppressed = self.thumbnail_marker_suppresses(&sha256, &path).await;
            }
            no_source = no_source || thumb_suppressed;
            if no_source {
                // As above: the suppression is counted even when one of the
                // remaining questions keeps the dispatch alive.
                if thumb_suppressed {
                    self.note_suppressed_visuals(&path);
                }
                if !work.any() {
                    return Ok(());
                }
                // A question is still owed and none of them needs a picture, so
                // the dispatch goes ahead with the blurhash half switched off —
                // the same move the track-less video above makes with the
                // thumbnail half, and for the same reason. There is no source
                // here but a full decode of the original, which for a
                // suppressed image is precisely the decode its marker settled
                // and for everything else produces nothing at all.
                work.blurhash = false;
            }
        }

        // Whether `storage.frames` holds anything for this content. Two
        // questions need it and they must not disagree: §7.1's "is there
        // anything here to replace", and — threaded into the worker — the fact
        // `build_backfill_renditions` classifies a failed extraction by.
        //
        // `work.existing_frames` answers it for free wherever the thumbnail
        // half fetched it (`get_frames_bytes` is unversioned, so empty means no
        // rows). The query is only needed where it did not, which is exactly
        // the replace path: a stored thumbnail switches the thumbnail half off
        // at dispatch, so nothing fetches the frames, yet a positive verdict
        // may be about to re-extract them. Deriving the fact from the unfetched
        // default there would call an item with frames frameless — and then a
        // failed re-extraction writes a `Frame` failure marker, or a
        // zero-frame one writes a `nothing(Frame)` verdict, for a video that
        // has frames.
        work.frames_stored = if frames_fetched {
            !work.existing_frames.is_empty()
        } else if work.outro.is_some() && mime_type.starts_with("video") {
            has_any_frame(&mut self.conn, &sha256).await?
        } else {
            false
        };
        // §7.1: replacement, not first generation — this item already carries
        // visuals sampled across the card. Frames count as well as thumbnails:
        // extraction stores frames of its own, so an item can have those and
        // no thumbnail. A marker-suppressed item is left alone whatever the
        // probe finds; the verdict that settled its decode still stands.
        let outro_replaces_visuals =
            !thumb_suppressed && (thumbnail_stored || work.frames_stored);
        if let Some(outro) = &mut work.outro {
            outro.replaces_visuals = outro_replaces_visuals;
        }
        // The boundary this item was *already* examined for. Without it, every
        // regeneration after the verdict was stored — a storage.db rebuild, a
        // `THUMBNAIL_PROCESS_VERSION`/`FRAME_PROCESS_VERSION` bump, a store
        // that failed transiently — would sample the whole file again and put
        // the card back permanently, because the pass's own verdict only ever
        // exists while `outro_kind IS NULL`.
        //
        // Clamp-only, and deliberately so: it must never reach §7.1's
        // replacement rule or the stored-frame discard, or an ordinary
        // backfill of an already-positive item would re-extract on every scan
        // — "not on every scan, and not for negatives".
        //
        // Gated on the config pair like every other read of this metadata
        // (design §8: "consumers ignore the metadata"), which is also what
        // makes turning detection off the escape hatch for a false positive:
        // off, then regenerate, and the visuals come back untrimmed.
        if self.detect_outros
            && work.outro.is_none()
            && work.thumbnail
            && mime_type.starts_with("video")
        {
            work.stored_content_end_ms = get_item_content_end_ms(&mut self.conn, &sha256).await?;
        }
        if work.video_duration <= 0.0 {
            // The probe carries the duration for the case the thumbnail half
            // never asked for one: an item whose visuals are complete and
            // whose outro turns out positive still has to re-extract.
            work.video_duration = work
                .outro
                .as_ref()
                .and_then(|outro| outro.item.duration)
                .unwrap_or(0.0);
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
                generate_backfill_visuals(&path, &mime_type, sha256, work, &timers)
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
                    // A dead worker is no verdict on anything: every column it
                    // would have written stays as it was and the next scan asks
                    // the same questions again.
                    TaskOutcome::Backfill(BackfillResult::inconclusive(outer_sha256, outer_mime))
                }
            }
        });
        self.task_paths.insert(handle.id(), tracked);
        Ok(())
    }

    /// The outro dispatch question, answered against the index and the
    /// negative cache (docs/video-outro-detection-design.md §7, §7.2).
    ///
    /// `None` means nothing to probe: detection is off (or `scan_video` is),
    /// the file is not a video, the item already carries a verdict, or a
    /// confirmed probe failure at this detector version suppresses it.
    async fn pending_outro_work(
        &mut self,
        sha256: &str,
        mime_type: &str,
        path: &Path,
    ) -> Option<PendingOutroItem> {
        if !self.detect_outros || !mime_type.starts_with("video") {
            return None;
        }
        let pending = match get_pending_outro_item(&mut self.conn, sha256).await {
            Ok(pending) => pending?,
            // Advisory, like every other read on this path: without the answer
            // the file is simply left alone this run.
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    path = %path.display(),
                    "failed to read the outro state; skipping detection"
                );
                return None;
            }
        };
        match visuals_suppressed(
            &mut self.conn,
            sha256,
            VisualKind::Outro,
            OUTRO_DETECTOR_VERSION,
        )
        .await
        {
            Ok(true) => {
                tracing::debug!(
                    path = %path.display(),
                    "skipping the outro probe with an active recorded attempt"
                );
                None
            }
            Ok(false) => Some(pending),
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    path = %path.display(),
                    "failed to read the visuals negative cache; probing anyway"
                );
                Some(pending)
            }
        }
    }

    /// The codec dispatch question, answered against the index alone
    /// (docs/video-transcoding-design.md §6).
    ///
    /// `None` means nothing to probe: the file is not a video, or the item
    /// already carries a `video_codec`. That is the whole predicate — there is
    /// no config gate (a codec name is scan metadata like width and height, not
    /// a feature), no metadata shortcut, and no negative-cache consult, because
    /// the pass writes no markers; see [`codec_pass_for`] for why a failure is
    /// simply retried.
    async fn pending_codec_work(
        &mut self,
        sha256: &str,
        mime_type: &str,
        path: &Path,
    ) -> Option<()> {
        if !mime_type.starts_with("video") {
            return None;
        }
        match item_codec_pending(&mut self.conn, sha256).await {
            Ok(pending) => pending.then_some(()),
            // Advisory, like every other read on this path: without the answer
            // the file is simply left alone this run.
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    path = %path.display(),
                    "failed to read the codec state; skipping the probe"
                );
                None
            }
        }
    }

    /// The animation-length dispatch question, answered against the index
    /// alone (docs/animated-image-spans-design.md §4).
    ///
    /// `false` means nothing to measure: the file is not one of the three
    /// animated-image containers, or the item already carries a duration —
    /// 0.0 included, which is the "measured: still or unparseable" verdict
    /// and terminates the backfill exactly like a real length. Like the
    /// codec question there is no config gate and no negative-cache consult,
    /// because the pass writes no markers: a file that cannot be *read*
    /// records nothing and is retried, while one that reads but does not
    /// parse records the 0.0 verdict itself.
    async fn pending_animation_work(&mut self, sha256: &str, mime_type: &str, path: &Path) -> bool {
        // The mime gate first, so the overwhelmingly common case — an image
        // that is not one of the three — costs no query at all.
        if !crate::media_tools::animation::measures_animation(mime_type) {
            return false;
        }
        match item_animation_pending(&mut self.conn, sha256).await {
            Ok(pending) => pending,
            // Advisory, like every other read on this path: without the answer
            // the file is simply left alone this run.
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    path = %path.display(),
                    "failed to read the animation state; skipping the measurement"
                );
                false
            }
        }
    }

    /// The rotation dispatch question (docs/display-dimensions-design.md §4),
    /// answered against the index alone.
    ///
    /// `false` means the item has been examined already, has no picture to
    /// orient, or is not indexed.
    async fn pending_rotation_work(&mut self, sha256: &str, mime_type: &str, path: &Path) -> bool {
        // The mime gate first, so an audio file — the one media type with no
        // picture — costs no query at all.
        if !mime_type.starts_with("image") && !mime_type.starts_with("video") {
            return false;
        }
        match item_rotation_pending(&mut self.conn, sha256).await {
            Ok(pending) => pending,
            // Advisory, like every other read on this path: without the answer
            // the file is simply left alone this run.
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    path = %path.display(),
                    "failed to read the rotation state; skipping the measurement"
                );
                false
            }
        }
    }

    /// The transparency dispatch question
    /// (docs/thumbnail-format-implementation.md §2, R4), answered against the
    /// index alone.
    ///
    /// `false` means the item has been examined already, has no pixels of its
    /// own, or is not indexed. Like the codec and animation questions there is
    /// no config gate and no negative-cache consult of its own: the pass that
    /// answers it is the image pass, whose decode the thumbnail marker already
    /// governs.
    async fn pending_transparency_work(
        &mut self,
        sha256: &str,
        mime_type: &str,
        path: &Path,
    ) -> bool {
        // The mime gate first, so every non-image — most of a general-purpose
        // library — costs no query at all.
        if !mime_type.starts_with("image") {
            return false;
        }
        match item_transparency_pending(&mut self.conn, sha256).await {
            Ok(pending) => pending,
            // Advisory, like every other read on this path: without the answer
            // the file is simply left alone this run.
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    path = %path.display(),
                    "failed to read the transparency state; skipping the measurement"
                );
                false
            }
        }
    }

    /// The rendition-ladder dispatch question
    /// (docs/grid-scroll-performance-implementation.md §3, B1), and the
    /// seventh the dispatcher asks: "does this item carry exactly the
    /// renditions the current ladder would produce?"
    ///
    /// Answered against indexed metadata and *stored geometry* — the width
    /// and height columns of `thumbnails`/`thumbnail_tiers` — never by
    /// decoding anything. That is the same invariant the served-directly
    /// predicate has always carried, and it is load-bearing twice over here:
    /// grid tiers are legitimately absent for small originals, so "no row"
    /// can never mean "work to do" on its own, and the answer has to be
    /// *exact* or an item is re-dispatched on every scan forever. Which is
    /// why every dimension the generator writes is one this function can
    /// predict: [`display_plan`] and [`grid_plans`] are pure functions of
    /// `(bytes, width, height)`, and the generator resizes to exactly what
    /// they name.
    ///
    /// `None` means nothing to do — including for every item whose dimensions
    /// were never indexed, which cannot be decided without a decode and so is
    /// deliberately left alone rather than re-examined forever.
    ///
    /// The comparison is geometry *and* [`TIER_PROCESS_VERSION`]: a generator
    /// change that keeps the dimensions is invisible to the geometry alone,
    /// and without the version stamp nothing could ever trigger its
    /// regeneration.
    ///
    /// `image_facts` is what the caller already stat'd and read for this file;
    /// nothing here re-fetches it. `ladder` is [`grid_ladder`]'s answer, and
    /// [`GridLadder::Nothing`] does **not** mean "skip the question": it means
    /// the wanted set is *empty*, which is a verdict like any other and the
    /// only thing that can retire a stale set.
    async fn pending_tier_work(
        &mut self,
        sha256: &str,
        mime_type: &str,
        path: &Path,
        image_facts: Option<&ImageFacts>,
        ladder: GridLadder,
    ) -> TierQuestion {
        // Before any storage read at all. This question is asked once per
        // file per scan, forever, and most of the files in a general-purpose
        // library are types no generator has ever produced a picture for — a
        // .txt, a .zip, a .docx. Their answer is structurally `None` (a tier
        // is derived from a rendition, and they have none), so paying two
        // geometry queries each to rediscover that, over SMB, is pure loss.
        //
        // Accepted trade, and the same shape `thumbnails` has always had: an
        // item whose mime type is re-classified *out* of the rendition
        // families (a content sniff correcting an extension, a mime override)
        // keeps its stored tier rows until the item itself is deleted and the
        // orphan sweep takes them. Noticing would cost a storage query for
        // every non-visual file in the library on every scan, to catch a
        // reclassification that also has to survive re-hashing to reach this
        // point at all.
        if !mime_can_have_renditions(mime_type) {
            return TierQuestion::none();
        }
        let stored_tiers = match get_thumbnail_tier_geometry(&mut self.conn, sha256).await {
            Ok(tiers) => tiers,
            // Advisory, like every other read on this path: without the answer
            // the file is simply left alone this run.
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    path = %path.display(),
                    "failed to read the stored tiers; skipping the ladder question"
                );
                return TierQuestion::none();
            }
        };
        match ladder {
            GridLadder::Nothing => {
                // The wanted set is empty. A stale one really can be stored:
                // the ladder question runs *before* the animation question
                // that stamps `items.duration`, so a scan that met an item
                // indexed without that measurement sees `duration IS NULL`,
                // concludes "still", and writes static tiers for what the
                // very same scan then records as animated. Retiring them here
                // is what closes that window — otherwise they are frozen for
                // good and, being stored renditions, served immutably.
                //
                // Answered before the display geometry is read: this branch
                // has no use for it, and raw-floor animated items should not
                // pay a second query to be told nothing.
                if stored_tiers.is_empty() {
                    return TierQuestion::none();
                }
                return TierQuestion::work(TierWork::Retire);
            }
            // Not enough indexed metadata to say what this item wants. Left
            // exactly as it is: guessing "empty" would retire a correct set,
            // and guessing "animated" would need geometry nobody measured.
            GridLadder::Unknown => return TierQuestion::none(),
            GridLadder::Animated => {
                return self
                    .animated_tier_question(sha256, path, image_facts, &stored_tiers)
                    .await;
            }
            GridLadder::Static => {}
        }
        match self
            .static_tier_work(sha256, mime_type, path, image_facts, &stored_tiers)
            .await
        {
            Some(work) => TierQuestion::work(work),
            None => TierQuestion::none(),
        }
    }

    /// The ladder question for an animated item above the raw floor.
    ///
    /// Split out because it is the one branch that answers two things: what
    /// this dispatch owes, and which stored loop rows the current plan still
    /// wants. The reuse is computed on **every** path out of here, the ones
    /// that owe no [`TierWork::Animated`] included — a stale still to retire,
    /// or nothing at all while some *other* question drags the item through
    /// the image pass — because all of them end in `build_animated_tiers`,
    /// and one that arrived without the list re-ran ffmpeg over a loop that
    /// was already correct.
    async fn animated_tier_question(
        &mut self,
        sha256: &str,
        path: &Path,
        image_facts: Option<&ImageFacts>,
        stored_tiers: &[TierGeometry],
    ) -> TierQuestion {
        let Some(facts) = image_facts else {
            return TierQuestion::none();
        };
        let Some((width, height)) = facts.dimensions else {
            return TierQuestion::none();
        };
        let (Ok(width), Ok(height)) = (u32::try_from(width), u32::try_from(height)) else {
            return TierQuestion::none();
        };
        let set = animated_rendition_set(
            facts.file_size,
            width,
            height,
            tier_format(facts.has_transparency, self.formats),
        );
        let reusable_loops = reusable_loop_rows(stored_tiers, &set);
        let wanted = wanted_tier_geometry(0, &set);
        let tiers_match = tier_geometry_matches(stored_tiers, &wanted);
        // A moving picture never carries a still display rendition: its
        // `display` answer is its own file or a stored loop (R3). The one
        // thing that can leave one behind is an older rule — an animated WebP
        // big enough that the pre-format display rule froze it into a static
        // JPEG, which the serving path would go on preferring to the
        // animation forever.
        let Some(stored_thumbnails) = self.stored_geometry(sha256, path).await else {
            return TierQuestion::none();
        };
        let work = if !stored_thumbnails.is_empty() {
            // The image pass, whose ladder is `Animated`: it produces the
            // animated set out of the decode it performs *and* retires the
            // stale still in the same pass.
            Some(TierWork::Image {
                replace_display: true,
            })
        } else if tiers_match {
            None
        } else {
            Some(TierWork::Animated)
        };
        TierQuestion {
            work,
            reusable_loops,
        }
    }

    /// The ladder question for everything on the static ladder: an image,
    /// whose whole ladder comes from one decode of the original, and every
    /// other kind, whose tiers are derived from the display renditions
    /// already stored.
    async fn static_tier_work(
        &mut self,
        sha256: &str,
        mime_type: &str,
        path: &Path,
        image_facts: Option<&ImageFacts>,
        stored_tiers: &[TierGeometry],
    ) -> Option<TierWork> {
        let stored_thumbnails = self.stored_geometry(sha256, path).await?;

        if mime_type.starts_with("image") {
            // Whatever the caller already stat'd and read; nothing is fetched
            // a second time here.
            let facts = image_facts?;
            let file_size = facts.file_size;
            // Never measured, or unreadable: undecidable without a decode,
            // which is exactly what this question must never do.
            let (width, height) = facts.dimensions?;
            let (Ok(width), Ok(height)) = (u32::try_from(width), u32::try_from(height)) else {
                return None;
            };
            if width == 0 || height == 0 {
                return None;
            }
            let plan = display_plan(
                mime_type,
                false,
                facts.has_transparency,
                file_size,
                width,
                height,
                self.formats,
            );
            let display_matches = match (&plan, stored_thumbnails.as_slice()) {
                // A still item never reaches the loop verdict; the arm is
                // here so the match stays total.
                (DisplayPlan::Original | DisplayPlan::Loop { .. }, rows) => rows.is_empty(),
                (DisplayPlan::Thumbnail { .. }, [stored]) => display_row_matches(stored, &plan),
                // A rendition is owed and there is not exactly one row to be
                // it: none yet, or a set an older rule left behind.
                (DisplayPlan::Thumbnail { .. }, _) => false,
            };
            let wanted = wanted_tier_geometry(
                0,
                &static_rendition_set(
                    file_size,
                    width,
                    height,
                    tier_format(facts.has_transparency, self.formats),
                ),
            );
            if display_matches && tier_geometry_matches(stored_tiers, &wanted) {
                return None;
            }
            return Some(TierWork::Image {
                // The *stored display rendition disagrees with the current
                // display plan* — not merely "something is stored". An item
                // that owes only grid tiers must not drag the display
                // rendition and the blurhash into a rewrite: in a
                // library-wide ladder backfill that is a re-encode and a
                // re-store of an identical picture for every already-correct
                // item in the library, which is most of them.
                replace_display: !display_matches,
            });
        }

        // A non-image with no stored rendition has no source for a tier and
        // no question to answer: the *thumbnail* question owns that gap, and
        // the pass that fills it produces the tiers in the same breath.
        if stored_thumbnails.is_empty() {
            return None;
        }
        let mut wanted = Vec::new();
        for stored in &stored_thumbnails {
            let (Ok(width), Ok(height)) =
                (u32::try_from(stored.width), u32::try_from(stored.height))
            else {
                return None;
            };
            wanted.extend(wanted_tier_geometry(
                stored.idx,
                &stored_thumbnail_rendition_set(width, height),
            ));
        }
        if tier_geometry_matches(stored_tiers, &wanted) {
            return None;
        }
        // Only now, with work established, are the blobs worth reading.
        let mut sources = Vec::with_capacity(stored_thumbnails.len());
        for stored in &stored_thumbnails {
            let idx = &stored.idx;
            match get_thumbnail_bytes(&mut self.conn, sha256, *idx).await {
                Ok(Some(bytes)) => sources.push((*idx, bytes)),
                Ok(None) => return None,
                Err(err) => {
                    tracing::warn!(
                        error = ?err,
                        path = %path.display(),
                        "failed to read a stored thumbnail; skipping its tiers"
                    );
                    return None;
                }
            }
        }
        Some(TierWork::Derived(sources))
    }

    /// Everything the dimension-first questions need about one image, from a
    /// single stat and a single index read.
    ///
    /// `None` means the file could not be stat'd now — the same "leave the
    /// visuals to a later scan" answer the served-directly branch has always
    /// given for an unreadable file.
    async fn image_facts(&mut self, sha256: &str, path: &Path) -> Option<ImageFacts> {
        let file_size = fs::metadata(path).ok()?.len();
        let facts = match get_item_visual_facts(&mut self.conn, sha256).await {
            Ok(facts) => facts,
            // Advisory, like every other read on this path: without the answer
            // the questions below fall back to what they do for an item whose
            // dimensions were never measured.
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    path = %path.display(),
                    "failed to read an item's visual facts"
                );
                None
            }
        };
        Some(ImageFacts {
            file_size,
            dimensions: facts
                .as_ref()
                .and_then(|facts| facts.width.zip(facts.height)),
            duration: facts.as_ref().and_then(|facts| facts.duration),
            rotation: facts.as_ref().and_then(|facts| facts.rotation),
            has_transparency: facts.and_then(|facts| facts.has_transparency),
        })
    }

    /// The stored display renditions of an item, geometry and media type.
    /// `None` on a read failure, which leaves the file alone this run.
    async fn stored_geometry(
        &mut self,
        sha256: &str,
        path: &Path,
    ) -> Option<Vec<ThumbnailGeometry>> {
        match get_thumbnail_geometry(&mut self.conn, sha256).await {
            Ok(rows) => Some(rows),
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    path = %path.display(),
                    "failed to read the stored thumbnail geometry"
                );
                None
            }
        }
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
                // Both are transient: a read that failed is this run's
                // problem, and a dead task is no verdict at all. Neither is
                // recorded, so neither needs the ledger's identity.
                Ok(Err(err)) => TaskOutcome::Failed(FailedFile::transient(
                    path,
                    FileProcessError::Io(err.to_string()),
                )),
                Err(err) => TaskOutcome::Failed(FailedFile::transient(
                    path,
                    FileProcessError::Worker(err.to_string()),
                )),
            }
        });
        self.task_paths.insert(handle.id(), tracked);
        Ok(())
    }

    /// Runs full metadata extraction, the stage-2 filter, and visual
    /// generation for files whose content is new to the index.
    ///
    /// `file_size` is the byte count the hasher read (what gets stored on the
    /// file row); `stat_size` is what the walker's stat reported, which is the
    /// half of the ledger's retry key. They differ only for files that changed
    /// under the scan, and the two must not be swapped — see `prepare_new_item`.
    #[allow(clippy::too_many_arguments)]
    async fn dispatch_prepare(
        &mut self,
        path: PathBuf,
        last_modified: String,
        file_size: i64,
        stat_size: i64,
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
        let detect_outros = self.detect_outros;
        let formats = self.formats;
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
                    stat_size,
                    mime_type,
                    md5,
                    sha256,
                    filter,
                    detect_outros,
                    formats,
                    &timers,
                )
            })
            .await;
            match joined {
                Ok(outcome) => outcome,
                Err(err) => TaskOutcome::Failed(FailedFile::transient(
                    outer_path,
                    FileProcessError::Worker(err.to_string()),
                )),
            }
        });
        self.task_paths.insert(handle.id(), tracked);
        Ok(())
    }

    /// The single choke point every success path goes through (unchanged
    /// files, false changes, already-known content, and new items alike),
    /// which is why the ledger's success-side delete hangs off it.
    async fn update_file_data(&mut self, data: FileScanData) -> ApiResult<FileUpsertResult> {
        let path = data.path.clone();
        let result = call_index_db_writer(&self.index_db, |reply| {
            IndexDbWriterMessage::UpdateFileData {
                time_added: self.scan_time.clone(),
                scan_id: self.scan_id,
                data: data.clone(),
                reply,
            }
        })
        .await?;
        self.clear_scan_error(&path).await;
        Ok(result)
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
    stat_size: i64,
    mime_type: String,
    md5: String,
    sha256: String,
    filter: Option<Arc<Match>>,
    detect_outros: bool,
    formats: FormatPolicy,
    timers: &ScanTimers,
) -> TaskOutcome {
    // Every failure below is about this exact file, so they all carry the same
    // ledger identity.
    //
    // The size is the walker's *stat* size, not the byte count the hasher
    // actually read: the ledger's retry key is re-checked against a stat, so
    // storing the read size would make every verdict written here look like a
    // different file to the next scan (and to the continuous scan, which only
    // ever has the stat), resetting `attempts` forever.
    let failed = |path: PathBuf, error: FileProcessError| {
        TaskOutcome::Failed(FailedFile {
            path,
            error,
            stat: Some((last_modified.clone(), stat_size)),
            mime_type: Some(mime_type.clone()),
        })
    };

    let metadata_span = timers.metadata.start();
    // The metadata phase reads an image's header and nothing more: whether the
    // pixels decode is the visuals phase's question, and its answer no longer
    // decides whether the file is indexed.
    let metadata = match extract_item_metadata(&path, &mime_type, md5) {
        Ok(metadata) => metadata,
        Err(error) => {
            return failed(path, error);
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
        return failed(path, FileProcessError::Filtered);
    }

    if let Err(error) = require_html_renderer_for_indexing(&mime_type) {
        return failed(path, error);
    }

    // The outro probe runs inside this call, before the generation it clamps
    // (design §7) and inside the same blocking task.
    let visuals = match generate_new_item_visuals(
        &path,
        &mime_type,
        &metadata,
        detect_outros,
        formats,
        timers,
    ) {
        Ok(visuals) => visuals,
        Err(error) => return failed(path, error),
    };
    // Built here because this is the last place that holds the file's ledger
    // identity — the same stat pair the `failed` closure above uses, for the
    // same reason.
    let visuals_scan_error = visuals.audit.map(|failure| ScanErrorRecord {
        path: path.to_string_lossy().to_string(),
        last_modified: last_modified.clone(),
        file_size: stat_size,
        stage: failure.stage.to_string(),
        kind: failure.kind,
        mime_type: Some(mime_type.clone()),
        error: failure.message,
        skip_after: failure.skip_after,
    });

    TaskOutcome::NewItem(NewItemData {
        path,
        last_modified,
        file_size,
        sha256,
        mime_type,
        metadata,
        thumbnails: visuals.thumbnails,
        tiers: visuals.tiers,
        frames: visuals.frames,
        blurhash: visuals.blurhash,
        transparency: visuals.transparency,
        visual_verdicts: visuals.verdicts,
        visuals_scan_error,
        outro: visuals.outro,
    })
}

/// The outro stage of a *new-item* pass: gated on the config pair and on the
/// file being a video, and answered from the metadata this pass just measured.
fn outro_pass_for(
    path: &Path,
    mime_type: &str,
    metadata: &ItemScanMeta,
    detect_outros: bool,
) -> OutroPass {
    if !detect_outros || !mime_type.starts_with("video") {
        return OutroPass::default();
    }
    run_outro_pass(
        path,
        metadata.duration,
        metadata.video_tracks,
        outro_source_dims(metadata.width, metadata.height),
    )
}

pub(crate) struct PreparedFile {
    pub(crate) path: PathBuf,
    pub(crate) last_modified: String,
    pub(crate) file_size: i64,
    pub(crate) sha256: String,
    pub(crate) mime_type: String,
    pub(crate) metadata: ItemScanMeta,
    pub(crate) thumbnails: Vec<StoredImage>,
    /// See [`GeneratedVisuals::tiers`].
    pub(crate) tiers: Vec<StoredTier>,
    pub(crate) frames: Vec<StoredImage>,
    pub(crate) blurhash: Option<String>,
    /// See [`GeneratedVisuals::transparency`].
    pub(crate) transparency: Option<bool>,
    /// What the visuals pass concluded about the kinds it produced nothing
    /// for. Empty on the healthy path.
    pub(crate) visual_verdicts: Vec<VisualVerdict>,
    /// See [`NewItemData::visuals_scan_error`].
    pub(crate) visuals_scan_error: Option<ScanErrorRecord>,
    /// See [`NewItemData::outro`].
    pub(crate) outro: Option<OutroRecord>,
}

pub(crate) struct FileWriteData {
    pub(crate) sha256: String,
    pub(crate) mime_type: String,
    pub(crate) data: FileScanData,
    pub(crate) new_file_timestamp: bool,
    pub(crate) new_file_hash: bool,
    pub(crate) thumbnails: Vec<StoredImage>,
    /// See [`GeneratedVisuals::tiers`].
    pub(crate) tiers: Vec<StoredTier>,
    pub(crate) frames: Vec<StoredImage>,
    pub(crate) blurhash: Option<String>,
    /// See [`GeneratedVisuals::transparency`]. Written *after* the file/item
    /// write, which is what creates the row it updates.
    pub(crate) transparency: Option<bool>,
    /// See [`PreparedFile::visual_verdicts`].
    pub(crate) visual_verdicts: Vec<VisualVerdict>,
    /// See [`NewItemData::visuals_scan_error`].
    pub(crate) visuals_scan_error: Option<ScanErrorRecord>,
    /// See [`NewItemData::outro`]. Written *after* the file/item write, which
    /// is what creates the row it updates.
    pub(crate) outro: Option<OutroRecord>,
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
            tiers: prepared.tiers,
            frames: prepared.frames,
            blurhash: prepared.blurhash,
            transparency: prepared.transparency,
            visual_verdicts: prepared.visual_verdicts,
            visuals_scan_error: prepared.visuals_scan_error,
            outro: prepared.outro,
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
    /// A worker task died without producing an outcome. Transient.
    Worker(#[allow(dead_code)] String),
    /// The gateway's own read/stat failed, or a tool that exited successfully
    /// produced output the gateway could not use. Transient: never recorded,
    /// the file simply fails this run and is retried untouched next scan.
    Io(#[allow(dead_code)] String),
    /// A non-transient verdict on the *file*, which the `scan_errors` ledger
    /// stores so the next scan does not repeat the work
    /// (docs/failed-media-retry-design.md).
    Classified(ScanFailure),
    /// A visuals-grade failure (thumbnail, frames, encode). Never fails the
    /// file: it is indexed without visuals, exactly as before. What is new is
    /// that the verdict is *kept* — it becomes a `visual_attempts` marker, so
    /// the next scan does not regenerate the same nothing. Deliberately not
    /// the `scan_errors` ledger, which is only about files that never reach an
    /// item at all.
    Visuals(VisualFailure),
    /// The file was rejected by the user's filescan filter.
    Filtered,
    /// The file's mtime matches the DB record, so hashing was skipped.
    Unchanged,
    /// The file has an active `scan_errors` verdict and its bytes have not
    /// moved since, so nothing was attempted. Not an error of this run.
    /// Produced by the continuous scan, which decides this per event; the
    /// batch walker decides it from its preloaded map before a task is ever
    /// dispatched, so it never travels as an outcome there.
    KnownBad,
}

/// A non-transient scan verdict: everything `scan_errors` needs except the
/// file's identity (path, mtime, size), which only the walker knows.
#[derive(Debug, Clone)]
pub(crate) struct ScanFailure {
    /// [`STAGE_MIME`], [`STAGE_METADATA`], [`STAGE_HEADER`] or
    /// [`STAGE_DECODE`].
    pub(crate) stage: &'static str,
    pub(crate) kind: ApiErrorKind,
    /// The confirmation threshold this *site* earned, not the class's: an
    /// `input` verdict from a decode of bytes the gateway already read is
    /// settled at 1, the same class from a tool that did its own file I/O is
    /// not. See [`SKIP_AFTER_AMBIGUOUS`].
    pub(crate) skip_after: i64,
    pub(crate) message: String,
}

impl ScanFailure {
    /// The pipeline's own decoder rejected the payload, on bytes the gateway
    /// had already read successfully. One failure settles it.
    fn input(stage: &'static str, message: impl Into<String>) -> FileProcessError {
        FileProcessError::Classified(Self {
            stage,
            kind: ApiErrorKind::Input,
            skip_after: SKIP_AFTER_CONFIRMED,
            message: message.into(),
        })
    }

    /// An `input` verdict from a stage where an external tool did its own file
    /// I/O, so a transient mount hiccup and a corrupt file are indistinguishable
    /// and a single failure does not settle it.
    fn input_unconfirmed(stage: &'static str, message: impl Into<String>) -> FileProcessError {
        FileProcessError::Classified(Self {
            stage,
            kind: ApiErrorKind::Input,
            skip_after: SKIP_AFTER_AMBIGUOUS,
            message: message.into(),
        })
    }
}

impl FileProcessError {
    /// Adopts an already-classified [`ApiError`] — the shape
    /// [`crate::media_tools::spawn_error`] produces, which distinguishes "the
    /// toolchain is missing" (`blocked`, recorded) from "this machine refused
    /// to start it" (transient, not recorded).
    fn from_api_error(stage: &'static str, err: ApiError) -> Self {
        if err.persisted_class().is_none() {
            return FileProcessError::Io(err.detail().to_string());
        }
        FileProcessError::Classified(ScanFailure {
            stage,
            kind: err.kind(),
            skip_after: err.skip_after(),
            message: err.detail().to_string(),
        })
    }

    /// Classifies an image-crate failure by *where* it happened, not by which
    /// variant it is.
    ///
    /// Opening and format-sniffing do their own file I/O and decode nothing,
    /// so every failure there is this machine's problem and stays transient.
    ///
    /// A header the crate cannot parse is the metadata phase's own verdict on
    /// bytes it read successfully, so it is `input` and the file is not
    /// indexed. Not settled at one attempt, though: the parse itself is
    /// deterministic, but the walker may have read a file another process was
    /// still writing — a copier that preallocates the final size has the
    /// destination at its final (path, mtime, size) key before the header
    /// bytes exist, and a one-shot verdict would suppress the finished file
    /// forever. The ambiguity is in the bytes read, not the reader, and it is
    /// the same mid-flight ambiguity that gives every stage doing its own
    /// file I/O `SKIP_AFTER_AMBIGUOUS`. (Truncation is *not* the header case —
    /// a truncated JPEG's header is intact; that failure surfaces one phase
    /// later, in the visuals, and no longer blocks indexing.)
    ///
    /// The decode arm survives for the ledger's benefit rather than the
    /// metadata phase's: since the un-fusing, no caller reaches it from a
    /// gate that blocks indexing (the visuals half has its own classifier,
    /// [`Self::visuals_from_image_error`]), but a decode verdict that did
    /// reach the ledger must still be the ambiguous class — a decoder that
    /// reads as it goes cannot tell a truncated file from a mount that
    /// dropped mid-read.
    ///
    /// `Limits` is a verdict on this machine's budget rather than on the file,
    /// so it is `resource` at *either* stage — settled at one attempt and
    /// clearable by a retry directive after the ceiling is raised. It reaches
    /// the decode from the configurable `image_decode_memory_limit_mb`, and the
    /// header parse from the image crate's own default 512 MiB allocation cap,
    /// which that parse runs under untouched: setting no limits means the
    /// crate's defaults, not none, and one row of a wide enough image is all it
    /// takes to exceed them.
    fn from_image_error(stage: ImageStage, err: image::ImageError) -> Self {
        match (stage, err) {
            (ImageStage::Open, err) => FileProcessError::Io(err.to_string()),
            (ImageStage::Header, image::ImageError::Limits(limit_err)) => {
                FileProcessError::Classified(ScanFailure {
                    stage: STAGE_HEADER,
                    kind: ApiErrorKind::Resource,
                    skip_after: SKIP_AFTER_CONFIRMED,
                    message: limit_err.to_string(),
                })
            }
            (ImageStage::Header, err) => {
                ScanFailure::input_unconfirmed(STAGE_HEADER, err.to_string())
            }
            (ImageStage::Decode, image::ImageError::Limits(limit_err)) => {
                FileProcessError::Classified(ScanFailure {
                    stage: STAGE_DECODE,
                    kind: ApiErrorKind::Resource,
                    skip_after: SKIP_AFTER_CONFIRMED,
                    message: limit_err.to_string(),
                })
            }
            (ImageStage::Decode, err) => {
                ScanFailure::input_unconfirmed(STAGE_DECODE, err.to_string())
            }
        }
    }

    /// The verdict to record, or `None` when this failure is transient and
    /// must not be recorded at all.
    pub(crate) fn classified(&self) -> Option<&ScanFailure> {
        match self {
            FileProcessError::Classified(failure) => Some(failure),
            _ => None,
        }
    }

    /// The visuals verdict to mark, or `None` when this failure says nothing
    /// about the content (a read that failed, a dead task) and the generation
    /// must simply be retried next scan.
    fn visual_failure(&self) -> Option<&VisualFailure> {
        match self {
            FileProcessError::Visuals(failure) => Some(failure),
            _ => None,
        }
    }

    /// The visuals-phase twin of [`Self::from_image_error`], with the same
    /// stage-based reasoning: opening and format-sniffing decode nothing, so a
    /// failure there is this machine's problem; the decode itself is a verdict
    /// on the content, unconfirmed because a decoder that reads as it goes
    /// cannot tell a truncated file from a mount that dropped mid-read; and
    /// the configurable memory ceiling is a property of this machine's budget,
    /// so it is `resource` and settles at one attempt.
    ///
    /// A header failure cannot reach here in practice — the metadata phase
    /// parses the header first and a file that fails it is never indexed, so
    /// no visuals pass is ever run on one — but it is the *content's* verdict
    /// wherever it surfaces, and one attempt settles it for the same reason it
    /// does one phase earlier: nothing was decoded, so nothing is ambiguous.
    /// Its `Limits` half is the budget verdict all the same, for the reason
    /// spelled out on [`Self::from_image_error`]: the header parse runs under
    /// the image crate's default 512 MiB cap, so a wide enough single row
    /// exceeds it there too.
    fn visuals_from_image_error(stage: ImageStage, err: image::ImageError) -> Self {
        match (stage, err) {
            (ImageStage::Open, err) => FileProcessError::Io(err.to_string()),
            (ImageStage::Header, image::ImageError::Limits(limit_err)) => {
                FileProcessError::Visuals(VisualFailure {
                    kind: ApiErrorKind::Resource,
                    skip_after: SKIP_AFTER_CONFIRMED,
                    message: limit_err.to_string(),
                })
            }
            (ImageStage::Header, err) => visuals_input(err.to_string()),
            (ImageStage::Decode, image::ImageError::Limits(limit_err)) => {
                FileProcessError::Visuals(VisualFailure {
                    kind: ApiErrorKind::Resource,
                    skip_after: SKIP_AFTER_CONFIRMED,
                    message: limit_err.to_string(),
                })
            }
            (ImageStage::Decode, err) => visuals_input_unconfirmed(err.to_string()),
        }
    }

    /// Adopts an already-classified [`ApiError`] on the visuals path — the
    /// shape [`crate::media_tools::spawn_error`] produces, which distinguishes
    /// "the toolchain is missing" (`blocked`, marked, self-healing) from "this
    /// machine refused to start it" (transient, not marked).
    fn visuals_from_api_error(err: ApiError) -> Self {
        if err.persisted_class().is_none() {
            return FileProcessError::Io(err.detail().to_string());
        }
        FileProcessError::Visuals(VisualFailure {
            kind: err.kind(),
            skip_after: err.skip_after(),
            message: err.detail().to_string(),
        })
    }
}

/// A visuals failure the generator itself decided, on data it already held.
/// One attempt settles it.
fn visuals_input(message: impl Into<String>) -> FileProcessError {
    FileProcessError::Visuals(VisualFailure {
        kind: ApiErrorKind::Input,
        skip_after: SKIP_AFTER_CONFIRMED,
        message: message.into(),
    })
}

/// A visuals failure from a stage where an external tool did its own file I/O
/// (ffmpeg, pdfium, the headless browser), so a transient mount hiccup and
/// broken content are indistinguishable and a single failure does not settle
/// it. See [`SKIP_AFTER_AMBIGUOUS`].
fn visuals_input_unconfirmed(message: impl Into<String>) -> FileProcessError {
    FileProcessError::Visuals(VisualFailure {
        kind: ApiErrorKind::Input,
        skip_after: SKIP_AFTER_AMBIGUOUS,
        message: message.into(),
    })
}

/// A generator whose backend is not installed. Self-heals at the next scan
/// after the dependency appears; never `input`.
fn visuals_blocked(blocker: Blocker, message: impl Into<String>) -> FileProcessError {
    FileProcessError::Visuals(VisualFailure {
        kind: ApiErrorKind::Blocked { blocker },
        skip_after: SKIP_AFTER_CONFIRMED,
        message: message.into(),
    })
}

/// The generator version a marker of this kind is stamped with, and compared
/// against when it is consulted (see the version table above). Exhaustive on
/// purpose: a new kind must fail to compile here rather than silently inherit
/// the thumbnail generator's version, which would make every one of its
/// markers expire on the wrong bump.
fn visual_process_version(kind: VisualKind) -> i64 {
    match kind {
        VisualKind::Thumbnail => THUMBNAIL_PROCESS_VERSION,
        VisualKind::Frame => FRAME_PROCESS_VERSION,
        // The same number the kind string's `/N` suffix carries, so a detector
        // bump retires failure markers through the ledger's existing
        // `version >= ?` consult while the unrecognised suffix recovers the
        // negatives (docs/video-outro-detection-design.md §7.2).
        VisualKind::Outro => OUTRO_DETECTOR_VERSION,
        // The *loop* row's own version, never the thumbnail's: a loop is a
        // `thumbnail_tiers` rendition, and §2 forbids bumping
        // `THUMBNAIL_PROCESS_VERSION` for tier work (that would regenerate
        // every video thumbnail in the library to fix an encoder setting).
        // This is also the only thing that gives a loop failure a heal path:
        // bump the loop version and the ledger's `version >= ?` consult
        // retires every one of these markers for free. Read from the row's
        // own kind, so a still-encoder bump neither retires a loop marker nor
        // re-runs the ffmpeg the marker exists to stop.
        VisualKind::Loop => RenditionKind::Loop.process_version(),
    }
}

/// One examination's answer about where a file's real content ends
/// (docs/video-outro-detection-design.md §6). Written to `items`, never to the
/// ledger.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OutroRecord {
    /// `items.outro_kind`, detector version included.
    pub(crate) kind: String,
    /// `items.content_end_ms`. Legitimately `None` on a *positive* verdict
    /// whose duration is missing or nonsense, which is why "has an outro" is
    /// this column being non-null and never the kind string (§6.3).
    pub(crate) content_end_ms: Option<i64>,
}

/// One probe's answer about a file's stream codecs
/// (docs/video-transcoding-design.md §6). Written to `items`, never to the
/// ledger — a failed ffprobe records nothing at all.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CodecRecord {
    /// `items.video_codec`, sentinels included. Never empty: the column is the
    /// backfill's termination predicate, so there is no "probed but no answer"
    /// state to represent.
    pub(crate) video_codec: String,
    /// `items.audio_codec`, `None` for a container with no audio stream.
    pub(crate) audio_codec: Option<String>,
}

/// The codec stage of a *backfill* pass: one ffprobe run over an item that was
/// indexed before these columns existed.
///
/// Blocking, like every other generator on this path, so the caller is inside
/// `spawn_blocking`.
///
/// A failure returns `None` and is retried on the next scan, with no marker
/// written. That is the deliberate fallback documented in
/// `docs/video-transcoding-implementation.md` §2 A3: guarding permanent
/// corruption through `visual_attempts` would need a `VisualKind::Codec`
/// variant, a process-version constant for a pass that *has* no version (a
/// codec name is a fact, not a detector verdict), a converter from the
/// metadata phase's failure vocabulary into the visuals one, and a marker
/// delete on the success path — well past the containment the plan allowed for.
/// The cost of doing without is one ffprobe per scan for a file that has rotted
/// since it was indexed; the outro pass, which probes the same population with
/// two ffmpeg starts instead of one, measured 0.37% failures.
fn codec_pass_for(path: &Path, mime_type: &str) -> Option<CodecRecord> {
    match extract_media_info(path) {
        Ok(info) => {
            let (video_codec, audio_codec) = media_codecs(&info, mime_type);
            // `None` is unreachable: the dispatcher only asks about `video/`
            // items. Falling through rather than defaulting keeps the sentinel
            // honest — nothing may claim a file was examined as a video when
            // it was not.
            video_codec.map(|video_codec| CodecRecord {
                video_codec,
                audio_codec,
            })
        }
        Err(err) => {
            tracing::debug!(path = %path.display(), error = ?err, "codec probe failed");
            None
        }
    }
}

/// What the outro stage of one pass concluded. Exactly one half is ever
/// populated: a probe either produces a verdict or owes a marker.
#[derive(Default)]
pub(crate) struct OutroPass {
    /// `None` when the stage did not run, or when it ran and failed — the
    /// column only ever holds genuine verdicts (§7.2).
    record: Option<OutroRecord>,
    /// The negative-cache marker a failed probe owes.
    verdict: Option<VisualVerdict>,
}

impl OutroPass {
    /// Where frame sampling must stop, when this pass found a boundary.
    fn content_end_ms(&self) -> Option<i64> {
        self.record
            .as_ref()
            .and_then(|record| record.content_end_ms)
    }
}

/// The item's stored dimensions in the shape the detector wants them.
fn outro_source_dims(width: Option<i64>, height: Option<i64>) -> Option<(u32, u32)> {
    match (width, height) {
        (Some(width), Some(height)) if width > 0 && height > 0 => {
            Some((width as u32, height as u32))
        }
        _ => None,
    }
}

/// Whether a probe is worth spawning at all, or the answer already follows
/// from the item's own metadata.
///
/// A container with no video stream has no card to carry: probing it would
/// burn two ffmpeg starts before the ledger settled, and leave the item
/// dispatched on every scan in between.
///
/// `None` counts as no track, and what makes that safe rather than a guess is
/// [`extract_media_info`]: *every* way ffprobe can fail — it will not start,
/// it exits non-zero, its output will not parse — returns `Err`, which
/// [`extract_item_metadata`] propagates and which keeps the file out of the
/// index entirely. So for an item that is indexed with a `video/` type,
/// `video_tracks` being unset can only mean "ffprobe ran, succeeded, and
/// found no video stream" — [`extract_item_metadata`] leaves the column unset
/// in that case rather than storing a zero. (Items inherited from the Python
/// indexer are outside that invariant; the backfill dispatcher already reads
/// them the same way, concluding a permanent nothing for both visual kinds.)
///
/// Not the same gate as [`build_new_item_renditions`], which needs
/// `video_tracks > 0 && duration > 0.0` because it has to place sample points
/// inside a known length. The probe needs no duration at all: it works from
/// the end of the stream backwards, and a missing duration only costs it a
/// `content_end_ms`, never the verdict. So this gates on tracks alone, and is
/// deliberately the looser of the two.
fn outro_needs_probe(video_tracks: Option<i64>) -> bool {
    video_tracks.unwrap_or(0) > 0
}

/// The verdict a container with no video stream earns without any probe.
///
/// Shared by [`run_outro_pass`] and the dispatcher's synchronous shortcut so
/// the two can never drift. Recorded rather than left `NULL`: without a stored
/// verdict the dispatcher would ask about the file on every scan forever. The
/// version suffix still applies, so a future detector re-examines it with
/// every other negative (design §6.2).
fn outro_verdict_without_a_probe() -> OutroRecord {
    OutroRecord {
        kind: OutroVerdict::None(RejectReason::Gate).kind_value(),
        content_end_ms: None,
    }
}

/// Runs the detector over one video and maps its two error variants onto the
/// ledger vocabulary of design §7.2.
///
/// Blocking (a process spawn plus its own file I/O), so every caller is
/// already inside `spawn_blocking` — the same rule the frame extractor and the
/// PDF/HTML renderers follow.
fn run_outro_pass(
    path: &Path,
    duration: Option<f64>,
    video_tracks: Option<i64>,
    source_dims: Option<(u32, u32)>,
) -> OutroPass {
    if !outro_needs_probe(video_tracks) {
        return OutroPass {
            record: Some(outro_verdict_without_a_probe()),
            verdict: None,
        };
    }

    match detect_outro(path, source_dims) {
        Ok(verdict) => {
            let content_end_ms = match (verdict.k_seconds(), duration) {
                (Some(k_seconds), Some(duration)) => {
                    crate::media_tools::outro::content_end_ms(duration, k_seconds)
                }
                _ => None,
            };
            match &verdict {
                OutroVerdict::TiktokCard { k_seconds } => tracing::debug!(
                    path = %path.display(),
                    k_seconds,
                    content_end_ms,
                    "detected an appended outro"
                ),
                OutroVerdict::None(reason) => tracing::debug!(
                    path = %path.display(),
                    reason = reason.as_str(),
                    "no appended outro"
                ),
            }
            OutroPass {
                record: Some(OutroRecord {
                    kind: verdict.kind_value(),
                    content_end_ms,
                }),
                verdict: None,
            }
        }
        Err(err) => {
            // Debug rather than error: unlike a failed thumbnail this costs no
            // visible output at all — the consumer simply behaves as if no
            // outro exists — and the measured failure rate is 0.37%, which at
            // library scale is thousands of log lines for nothing.
            tracing::debug!(path = %path.display(), error = %err, "outro probe failed");
            let failure = match err {
                // Never a verdict on the media: a missing toolchain is
                // `blocked` and heals when it binds, anything else about this
                // machine stays transient and is retried untouched.
                OutroProbeError::Spawn(err) => FileProcessError::visuals_from_api_error(
                    crate::media_tools::spawn_error("ffmpeg", &err),
                ),
                // ffmpeg ran and failed: the probe does its own file I/O, so a
                // broken file and a transient mount hiccup exit identically
                // and one failure does not settle it.
                OutroProbeError::Decode(detail) => {
                    visuals_input_unconfirmed(format!("outro probe failed: {detail}"))
                }
            };
            OutroPass {
                record: None,
                verdict: failure
                    .visual_failure()
                    .map(|failure| VisualVerdict::failed(VisualKind::Outro, failure.clone())),
            }
        }
    }
}

/// What one visuals generation pass produced, plus what the negative cache
/// owes because of it.
///
/// Ordinary visuals failures do not fail the file: the item is indexed without
/// visuals and the verdict is remembered. Opt-in HTML is the exception at the
/// caller: its first screenshot must succeed before a new item is inserted.
#[derive(Default)]
pub(crate) struct GeneratedVisuals {
    pub(crate) thumbnails: Vec<StoredImage>,
    pub(crate) frames: Vec<StoredImage>,
    /// The grid tiers of the renditions above. Flat rather than
    /// `Option<Vec<_>>` like [`ProducedVisuals::tiers`]: on the new-item path
    /// "produced none" and "did not consider tiers" reach the same guard,
    /// which writes the (empty) set only when there is something stored to
    /// retire — `storage.thumbnail_tiers` is keyed by content hash and can
    /// outlive a deindexed item.
    pub(crate) tiers: Vec<StoredTier>,
    pub(crate) blurhash: Option<String>,
    /// What this pass measured about the item's pixels
    /// (docs/thumbnail-format-implementation.md §2, R4), or `None` when it
    /// decoded nothing. Written to `items.has_transparency` after the row
    /// exists.
    pub(crate) transparency: Option<bool>,
    /// Empty on the healthy path, which is what keeps this free: a pass that
    /// stored something owes no marker, and the store clears any stale one.
    pub(crate) verdicts: Vec<VisualVerdict>,
    /// The failure that also owes a `scan_errors` audit row. See
    /// [`visuals_audit_failure`].
    pub(crate) audit: Option<ScanFailure>,
    /// What the outro stage of this pass concluded, which is what the frame
    /// sampling above was clamped to. See [`NewItemData::outro`].
    pub(crate) outro: Option<OutroRecord>,
}

/// Turns a pass's verdicts into markers for one item. Each kind is stamped
/// with its own generator version, so bumping one does not retire the other's
/// markers.
pub(crate) fn visual_attempt_records(
    verdicts: &[VisualVerdict],
    sha256: &str,
    mime_type: &str,
) -> Vec<VisualAttemptRecord> {
    verdicts
        .iter()
        .map(|verdict| {
            verdict
                .clone()
                .into_record(sha256, mime_type, visual_process_version(verdict.kind))
        })
        .collect()
}

pub(crate) fn process_file(
    path: PathBuf,
    filescan_filter: Option<Arc<Match>>,
    detect_outros: bool,
    formats: FormatPolicy,
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
    require_html_renderer_for_indexing(&mime_type)?;

    let hash_span = timers.hashing.start();
    let (md5, sha256, real_size) =
        calculate_hashes(&path).map_err(|err| FileProcessError::Io(err.to_string()))?;
    drop(hash_span);

    if real_size != file_size {
        tracing::warn!(path = %path.display(), real_size, file_size, "file size mismatch");
    }
    // The stat pair is the ledger's retry key and must outlive the swap to the
    // hasher's byte count below — see `prepare_new_item` for why the two must
    // not be confused.
    let stat_size = file_size;
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
    require_html_renderer_for_indexing(&mime_type)?;

    // The probe runs inside this call, before the generation it clamps,
    // exactly as in `prepare_new_item`.
    let visuals =
        generate_new_item_visuals(&path, &mime_type, &metadata, detect_outros, formats, timers)?;
    let visuals_scan_error = visuals.audit.map(|failure| ScanErrorRecord {
        path: path.to_string_lossy().to_string(),
        last_modified: last_modified.clone(),
        file_size: stat_size,
        stage: failure.stage.to_string(),
        kind: failure.kind,
        mime_type: Some(mime_type.clone()),
        error: failure.message,
        skip_after: failure.skip_after,
    });

    Ok(PreparedFile {
        path,
        last_modified,
        file_size,
        sha256,
        mime_type,
        metadata,
        thumbnails: visuals.thumbnails,
        tiers: visuals.tiers,
        frames: visuals.frames,
        blurhash: visuals.blurhash,
        transparency: visuals.transparency,
        visual_verdicts: visuals.verdicts,
        visuals_scan_error,
        outro: visuals.outro,
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

pub(crate) fn infer_mime_type(path: &Path) -> Result<String, FileProcessError> {
    let guess = MimeGuess::from_path(path);
    // A pure function of the file name — no I/O, nothing ambiguous — so one
    // failure settles it. The path keeps its extension, so the verdict is
    // stable until the file is renamed, which is a new path and a new row.
    let mime = guess
        .first()
        .ok_or_else(|| ScanFailure::input(STAGE_MIME, "missing mime type"))?;
    Ok(mime.essence_str().to_string())
}

/// Decodes an image like PIL does: the format is sniffed from the file's
/// magic bytes (the extension is only a fallback when the content is
/// unrecognized) and the crate's default 512 MiB allocation cap is replaced
/// by the configurable `[jobs].image_decode_memory_limit_mb` ceiling.
/// Archives contain mis-named files (WebP saved as .png) and very large
/// images (20k x 20k collages) that Python indexed fine.
pub(crate) fn open_image(path: impl AsRef<Path>) -> image::ImageResult<DynamicImage> {
    open_image_staged(path).map_err(|(_, err)| err)
}

/// Which half of [`open_image`] failed.
///
/// The distinction is the classification: the image crate reports a truncated
/// file, a mid-read mount drop and a missing file all as
/// `ImageError::IoError`, so the variant alone cannot say whether the bytes
/// were the problem. Where it surfaced can.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImageStage {
    /// Opening the file and sniffing its format — no payload was decoded, so
    /// nothing here is ever a verdict on the file.
    Open,
    /// Parsing the header, on a file that opened fine: no pixel data is
    /// touched, so a failure means the bytes do not describe an image at all.
    Header,
    /// The decode itself, on a file whose header parsed.
    Decode,
}

/// The metadata phase's whole contact with an image: the *display*
/// dimensions and the orientation that produced them, straight out of the
/// header (docs/display-dimensions-design.md §3).
///
/// The dimensions the header states are the *coded* ones — the pixels as
/// stored, before the EXIF orientation a browser applies before painting. The
/// index records what the picture looks like, so a transposing orientation
/// swaps them here, and the turn itself is kept so `items.rotation` can record
/// what was applied.
///
/// Deliberately *not* a decode (docs/failed-media-retry-design.md, "Scan policy
/// for undecodable images"): the metadata phase decides whether a file is
/// indexed at all, and pixel-level damage is a visuals-grade problem. It is
/// the same check extraction makes before handing bytes to a worker
/// (`ensure_image_readable`) and the same one PIL's `verify()` makes, so the
/// scan's indexing gate is no stricter than the consumer's — and marginally
/// looser: opening by path seeds the format from the extension, where
/// extraction sniffs a buffer with no such seed, so a file whose content is
/// unrecognizable but whose extension is not can still pass here. That is the
/// safe direction (the file is indexed and the consumer decides), which is why
/// the seed is left alone.
///
/// No limits are set *by this function*, for the same parity reason: reading a
/// header allocates nothing worth a configurable ceiling. That is not the same
/// as no limits at all — the image crate applies its own defaults, including a
/// 512 MiB allocation cap that a header declaring an absurd width exceeds, and
/// [`FileProcessError::from_image_error`] classifies that as `resource`.
fn image_header_geometry(
    path: impl AsRef<Path>,
) -> Result<((u32, u32), Orientation), (ImageStage, image::ImageError)> {
    let reader = image::ImageReader::open(path).map_err(|err| (ImageStage::Open, err.into()))?;
    // `into_dimensions()` is `into_decoder().map(|d| d.dimensions())`, so this
    // is the same read with the same failure stages — it just keeps the
    // decoder long enough to ask it one more question.
    let mut decoder = reader
        .with_guessed_format()
        .map_err(|err| (ImageStage::Open, err.into()))?
        .into_decoder()
        .map_err(|err| (ImageStage::Header, err))?;
    let coded = decoder.dimensions();
    // An orientation this build cannot read is never a reason to reject a file
    // whose header parsed. `NoTransforms` is the answer every *consumer* of
    // this file reaches for the same reason, so recording it keeps the index
    // and the picture agreeing; failing here would move the indexing gate,
    // which docs/failed-media-retry-design.md deliberately puts at the header
    // read and nowhere further.
    let orientation = decoder.orientation().unwrap_or(Orientation::NoTransforms);
    Ok((oriented_dimensions(coded, orientation), orientation))
}

/// The clockwise quarter turns an orientation applies, which is all
/// `items.rotation` records: mirroring never changes a dimension, and the
/// column exists to explain the dimensions. The one consumer that would like
/// the full transform — the animated ladder's crop
/// ([`indexed_display_transform`]) — accepts the turn without the flip rather
/// than re-read the file for it.
fn orientation_quarter_turns(orientation: Orientation) -> i64 {
    match orientation {
        Orientation::NoTransforms | Orientation::FlipHorizontal => 0,
        Orientation::Rotate90 | Orientation::Rotate90FlipH => 90,
        Orientation::Rotate180 | Orientation::FlipVertical => 180,
        Orientation::Rotate270 | Orientation::Rotate270FlipH => 270,
    }
}

/// Whether a quarter turn swaps width and height. The single rule both the
/// image and the video half of the scan apply, so they cannot drift apart.
fn quarter_turns_transpose(quarter_turns: i64) -> bool {
    quarter_turns == 90 || quarter_turns == 270
}

/// Coded dimensions as they are painted.
pub(crate) fn oriented_dimensions(
    (width, height): (u32, u32),
    orientation: Orientation,
) -> (u32, u32) {
    if quarter_turns_transpose(orientation_quarter_turns(orientation)) {
        (height, width)
    } else {
        (width, height)
    }
}

/// [`open_image`] with the failing stage attached, for the scan's classifier.
fn open_image_staged(
    path: impl AsRef<Path>,
) -> Result<DynamicImage, (ImageStage, image::ImageError)> {
    let reader = image::ImageReader::open(path).map_err(|err| (ImageStage::Open, err.into()))?;
    let mut reader = reader
        .with_guessed_format()
        .map_err(|err| (ImageStage::Open, err.into()))?;
    reader.limits(decode_limits());
    reader.decode().map_err(|err| (ImageStage::Decode, err))
}

/// [`open_image_staged`] plus the orientation its header asks for — the
/// picture, rather than the pixels as stored
/// (docs/display-dimensions-design.md §1.1).
///
/// Every visuals decode goes through this, because everything downstream of it
/// destroys the evidence: `encode_image` re-encodes through `to_rgb8()` + JPEG
/// and drops the EXIF, so a stored thumbnail that skipped this step is
/// sideways forever, against an original the browser paints upright and
/// against its own item's dimensions.
///
/// The orientation comes from a second, header-only pass rather than from the
/// decode itself: [`image::ImageReader::decode`] performs the allocation-cap
/// reservation this codebase classifies `resource` failures by, and there is
/// no way to keep that while holding the decoder. A header read next to a full
/// decode costs nothing.
///
/// A failed orientation read never fails the decode. The pixels are good; the
/// worst case is the picture this build already produced before orientation
/// was read at all.
fn open_image_oriented(
    path: impl AsRef<Path>,
) -> Result<DynamicImage, (ImageStage, image::ImageError)> {
    let path = path.as_ref();
    let mut image = open_image_staged(path)?;
    match image_header_geometry(path) {
        Ok((_, orientation)) => image.apply_orientation(orientation),
        Err((stage, err)) => tracing::debug!(
            error = %err,
            stage = ?stage,
            path = %path.display(),
            "decoded an image but could not read its orientation"
        ),
    }
    Ok(image)
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

/// The metadata phase, which is also the indexing gate: a file this rejects is
/// not indexed at all (docs/failed-media-retry-design.md, "Scan policy for
/// undecodable images").
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
        rotation: None,
        duration: None,
        audio_tracks: None,
        video_tracks: None,
        subtitle_tracks: None,
        video_codec: None,
        audio_codec: None,
    };

    if mime_type.starts_with("image") {
        // Header only. The dimensions are all the metadata an image
        // contributes, and they sit in the header of every format the crate
        // reads — so an image indexed through this path always has them, and a
        // file whose pixels are damaged is still indexed with its real size.
        // They are the *display* dimensions: the header's numbers with the
        // EXIF orientation already applied, because that is the picture
        // (docs/display-dimensions-design.md).
        let ((width, height), orientation) = image_header_geometry(path)
            .map_err(|(stage, err)| FileProcessError::from_image_error(stage, err))?;
        metadata.width = Some(width as i64);
        metadata.height = Some(height as i64);
        metadata.rotation = Some(orientation_quarter_turns(orientation));
        // The three animated-image containers also record their animation
        // length (docs/animated-image-spans-design.md §3): a positive value is
        // what lets the compose builder classify the item as a span, 0.0 is
        // the measured "still or unparseable" verdict, and `None` — any other
        // image mime, or a file that stopped reading between the header above
        // and here — leaves the column NULL for the backfill to retry. Still
        // no pixel decode: it is a structure walk, in the same spirit as the
        // header read.
        metadata.duration =
            crate::media_tools::animation::animation_duration_seconds(path, mime_type);
        return Ok(metadata);
    }

    if mime_type.starts_with("video") || mime_type.starts_with("audio") {
        let info = extract_media_info(path)?;
        // Before the track counts are folded away: the codecs come from the
        // streams themselves, and this is the only pass that sees them.
        (metadata.video_codec, metadata.audio_codec) = media_codecs(&info, mime_type);
        if mime_type.starts_with("video") {
            if let Some(video) = info.video_track {
                // Display dimensions, like the image branch above: ffprobe
                // reports the coded size of a stream carrying a rotating
                // display matrix, while every decoder — ffmpeg's included,
                // which is what extracts this file's frames — autorotates it
                // (docs/display-dimensions-design.md §3).
                let transposed = quarter_turns_transpose(video.rotation);
                let (width, height) = if transposed {
                    (video.height, video.width)
                } else {
                    (video.width, video.height)
                };
                metadata.width = width.map(|width| width as i64);
                metadata.height = height.map(|height| height as i64);
                metadata.rotation = Some(video.rotation);
                metadata.duration = Some(video.duration);
                metadata.video_tracks = Some(1);
            }
            // A video container with no video stream still records an
            // *examined* orientation: leaving it NULL would put the item back
            // in the backfill population on every scan for a picture it does
            // not have.
            metadata.rotation = metadata.rotation.or(Some(0));
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
            let mut file_candidates: Vec<PathBuf> = Vec::new();
            let mut dir_candidates: Vec<PathBuf> = Vec::new();
            if let Some(custom) = &crate::config::runtime().pdfium {
                if custom.is_file() {
                    file_candidates.push(custom.clone());
                } else if custom.is_dir() {
                    dir_candidates.push(custom.clone());
                } else {
                    // A configured path that resolves nowhere is a config
                    // error the user should see, not a debug-level shrug
                    // before discovery quietly picks something else.
                    tracing::warn!(
                        path = %custom.display(),
                        "configured pdfium path does not exist; falling back \
                         to discovery"
                    );
                }
            }
            if let Some(venv_lib) = crate::host_paths::find_pdfium_in_venvs() {
                file_candidates.push(venv_lib);
            }
            if let Some(exe_dir) = env::current_exe()
                .ok()
                .and_then(|exe| exe.parent().map(Path::to_path_buf))
            {
                dir_candidates.push(exe_dir);
            }
            if let Ok(cwd) = env::current_dir() {
                dir_candidates.push(cwd);
            }
            for library in &file_candidates {
                match Pdfium::bind_to_library(library) {
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
            for dir in &dir_candidates {
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
                        files = ?file_candidates,
                        dirs = ?dir_candidates,
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
///
/// The failure reason is kept rather than flattened to "no thumbnail": the
/// scan still indexes the item without visuals either way, but a missing
/// pdfium and an unparseable document are cleared by different events (a
/// dependency probe versus a retry directive), so the negative cache has to be
/// able to tell them apart.
fn render_pdf_first_page(path: &Path) -> Result<DynamicImage, PdfRenderError> {
    let pdfium = pdfium().ok_or(PdfRenderError::Unavailable)?;
    let _serialized = PDFIUM_CALL_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let document = pdfium.load_pdf_from_file(path, None).map_err(|err| {
        tracing::error!(error = %err, path = %path.display(), "failed to load PDF");
        PdfRenderError::Document(format!("failed to load PDF: {err}"))
    })?;
    let page = document.pages().first().map_err(|err| {
        tracing::error!(error = %err, path = %path.display(), "PDF has no readable pages");
        PdfRenderError::Document(format!("PDF has no readable pages: {err}"))
    })?;
    page.render_with_config(&PdfRenderConfig::new().scale_page_by_factor(2.0))
        .map(|bitmap| bitmap.as_image())
        .map_err(|err| {
            tracing::error!(error = %err, path = %path.display(), "failed to render PDF page");
            PdfRenderError::Document(format!("failed to render PDF page: {err}"))
        })
}

/// Why a PDF render failed. The distinction is the whole point: a missing
/// pdfium is a `blocked` verdict that self-heals once the library appears,
/// while a document pdfium refuses to parse is an `input` verdict on the file
/// (docs/failed-media-retry-design.md). Callers classify by variant — never
/// by matching the message.
#[derive(Debug)]
pub(crate) enum PdfRenderError {
    /// The pdfium library is not installed (or could not be bound).
    Unavailable,
    /// pdfium ran and rejected the document.
    Document(String),
}

impl std::fmt::Display for PdfRenderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PdfRenderError::Unavailable => f.write_str("pdfium library not available"),
            PdfRenderError::Document(detail) => f.write_str(detail),
        }
    }
}

/// Renders every page of a PDF at 2x its point size (144 dpi), matching the
/// Python pypdfium2 loader used by data extraction (`scale=2`). Unlike
/// thumbnail generation this fails hard: extraction must record the item as
/// failed rather than mark it processed.
pub(crate) fn render_pdf_pages(path: &Path) -> Result<Vec<DynamicImage>, PdfRenderError> {
    let pdfium = pdfium().ok_or(PdfRenderError::Unavailable)?;
    let _serialized = PDFIUM_CALL_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let document = pdfium
        .load_pdf_from_file(path, None)
        .map_err(|err| PdfRenderError::Document(format!("failed to load PDF: {err}")))?;
    let mut pages = Vec::new();
    for page in document.pages().iter() {
        let bitmap = page
            .render_with_config(&PdfRenderConfig::new().scale_page_by_factor(2.0))
            .map_err(|err| PdfRenderError::Document(format!("failed to render PDF page: {err}")))?;
        pages.push(bitmap.as_image());
    }
    Ok(pages)
}

/// Whether a dependency the extraction/scan ledger is waiting on now binds.
/// Only the blockers with rows in the ledger are probed, so a run never loads
/// a library it has no use for. Backend cache lifetime is backend-specific;
/// HTML renderer absence is deliberately re-probed on each relevant scan.
pub(crate) fn probe_blocker(blocker: crate::api_error::Blocker) -> bool {
    match blocker {
        crate::api_error::Blocker::Pdfium => pdfium().is_some(),
        crate::api_error::Blocker::HtmlRenderer => html_renderer().is_some(),
        crate::api_error::Blocker::Ffmpeg => crate::media_tools::ffmpeg_available(),
    }
}

static HTML_RENDERER: RwLock<Option<PathBuf>> = RwLock::new(None);
static HTML_RENDERER_MISSING_LOGGED: AtomicBool = AtomicBool::new(false);

/// Lazily locates a locally installed Chromium-family browser for headless
/// HTML screenshots. Successful discovery is cached while the executable
/// exists; absence is deliberately not cached, so installing a browser makes
/// blocked HTML files eligible on the next scan without restarting Panoptikon.
fn html_renderer() -> Option<PathBuf> {
    if let Some(custom) = &crate::config::runtime().html_renderer {
        if let Some(path) = crate::host_paths::resolve_configured_executable(custom) {
            HTML_RENDERER_MISSING_LOGGED.store(false, Ordering::Relaxed);
            tracing::debug!(path = %path.display(), "html renderer from config");
            return Some(path);
        }
        if !HTML_RENDERER_MISSING_LOGGED.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                path = %custom.display(),
                "configured html_renderer not found; falling back to discovery"
            );
        }
    }

    if let Some(path) = HTML_RENDERER
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .as_ref()
        .filter(|path| path.is_file())
        .cloned()
    {
        return Some(path);
    }

    if let Some(path) = crate::host_paths::find_html_renderer() {
        tracing::debug!(path = %path.display(), "html renderer resolved");
        HTML_RENDERER_MISSING_LOGGED.store(false, Ordering::Relaxed);
        *HTML_RENDERER
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(path.clone());
        return Some(path);
    }

    *HTML_RENDERER
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    if !HTML_RENDERER_MISSING_LOGGED.swap(true, Ordering::Relaxed) {
        tracing::warn!("no headless browser found; HTML files will not be indexed");
    }
    None
}

fn require_html_renderer_for_indexing(mime_type: &str) -> Result<(), FileProcessError> {
    require_html_renderer_for_indexing_with(mime_type, html_renderer().is_some())
}

fn require_html_renderer_for_indexing_with(
    mime_type: &str,
    renderer_available: bool,
) -> Result<(), FileProcessError> {
    if !mime_type.starts_with("text/html") || renderer_available {
        return Ok(());
    }
    Err(FileProcessError::Classified(ScanFailure {
        stage: STAGE_METADATA,
        kind: ApiErrorKind::Blocked {
            blocker: Blocker::HtmlRenderer,
        },
        skip_after: SKIP_AFTER_CONFIRMED,
        message: "HTML indexing requires Chrome, Chromium, Brave, Edge, or another configured compatible browser"
            .to_string(),
    }))
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

/// Why an HTML screenshot failed, for callers that classify. Same split as
/// [`PdfRenderError`]: a missing browser is `blocked` and self-heals, a
/// browser that ran and produced nothing usable is an `input` verdict on the
/// page, and the gateway's own I/O around the render is transient.
#[derive(Debug)]
pub(crate) enum HtmlRenderError {
    /// No headless browser is installed (or the configured one is gone).
    NoBrowser,
    /// The gateway's own file handling failed (canonicalize, temp dir).
    Io(String),
    /// The browser ran and did not produce a usable screenshot.
    Render(String),
}

impl std::fmt::Display for HtmlRenderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HtmlRenderError::NoBrowser => f.write_str("no headless browser available"),
            HtmlRenderError::Io(detail) | HtmlRenderError::Render(detail) => f.write_str(detail),
        }
    }
}

/// Screenshots an HTML file with a locally installed headless browser. This
/// intentionally replaces the Python weasyprint HTML->PDF pipeline with a
/// browser viewport capture.
///
/// The reason is classified so dependency absence and page/render failures can
/// be retried for different reasons. New HTML items require this first render
/// to succeed; already-indexed items retain the ordinary visuals-marker path.
pub(crate) fn render_html_screenshot_classified(
    path: &Path,
) -> Result<DynamicImage, HtmlRenderError> {
    let browser = html_renderer().ok_or(HtmlRenderError::NoBrowser)?;
    let url = html_file_url(path).ok_or_else(|| {
        HtmlRenderError::Io(format!("failed to build a file URL for {}", path.display()))
    })?;
    let _slot = BROWSER_SLOTS.acquire();
    let temp_dir = temp_dir_path();
    // The browser resolves its path arguments itself, so they must not
    // depend on the inherited working directory.
    let temp_dir = if temp_dir.is_absolute() {
        temp_dir
    } else {
        env::current_dir()
            .map_err(|err| HtmlRenderError::Io(format!("no working directory: {err}")))?
            .join(temp_dir)
    };
    // Headless browsers refuse to share a live profile; give each render its
    // own throwaway --user-data-dir.
    let profile_dir = temp_dir.join("profile");
    if let Err(err) = fs::create_dir_all(&profile_dir) {
        tracing::error!(error = %err, path = %profile_dir.display(), "failed to create temp screenshot dir");
        return Err(HtmlRenderError::Io(format!(
            "failed to create the temp screenshot dir: {err}"
        )));
    }
    let result = run_html_screenshot(&browser, &url, &profile_dir, path);
    if let Err(err) = fs::remove_dir_all(&temp_dir) {
        tracing::debug!(error = %err, path = %temp_dir.display(), "failed to remove temp screenshot dir");
    }
    result
}

fn run_html_screenshot(
    browser: &Path,
    url: &str,
    profile_dir: &Path,
    path: &Path,
) -> Result<DynamicImage, HtmlRenderError> {
    // Scanned HTML lives in user-approved folders, but a saved page can still
    // carry live script and remote references, so all network traffic
    // (including localhost, via the <-loopback> bypass override) is routed
    // into a dead proxy — no beaconing, no SSRF. file:// subresources are
    // unaffected, matching what the Python weasyprint pipeline could load.
    // JavaScript stays enabled. With the network dead, a runaway script can
    // only burn CPU until the deadline, where the process guard kills the
    // browser tree.
    let mut command = Command::new(browser);
    command
        .arg("--headless=new")
        .arg("--disable-gpu")
        .arg("--no-first-run")
        .arg("--no-default-browser-check")
        .arg("--disable-background-networking")
        .arg("--disable-component-update")
        .arg("--disable-default-apps")
        .arg("--disable-extensions")
        .arg("--disable-sync")
        .arg("--metrics-recording-only")
        .arg("--hide-scrollbars")
        .arg("--proxy-server=127.0.0.1:0")
        .arg("--proxy-bypass-list=<-loopback>")
        .arg("--default-background-color=FFFFFFFF")
        .arg("--remote-debugging-address=127.0.0.1")
        .arg("--remote-debugging-port=0")
        .arg(format!("--user-data-dir={}", profile_dir.display()));
    for extra in &crate::config::runtime().html_renderer_args {
        command.arg(extra);
    }
    command
        .arg("about:blank")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let child = match command.spawn() {
        Ok(child) => child,
        Err(err) => {
            tracing::error!(error = %err, browser = %browser.display(), "failed to launch headless browser");
            // A browser that resolved at discovery time but is gone at spawn
            // time is the dependency being missing, not a bad page.
            return Err(if err.kind() == std::io::ErrorKind::NotFound {
                HtmlRenderError::NoBrowser
            } else {
                HtmlRenderError::Render(format!("failed to launch the headless browser: {err}"))
            });
        }
    };
    let mut process = BrowserProcess::new(child);
    let deadline = Instant::now() + Duration::from_secs(30);
    let endpoint = wait_for_devtools_endpoint(&mut process.child, profile_dir, deadline)
        .map_err(|detail| html_render_protocol_error(path, detail))?;
    capture_html_over_devtools(&endpoint, url, deadline)
        .map_err(|detail| html_render_protocol_error(path, detail))
}

struct BrowserProcess {
    child: Child,
    _job: crate::process_tree::JobGuard,
}

impl BrowserProcess {
    fn new(child: Child) -> Self {
        let job = crate::process_tree::JobGuard::assign(&child);
        Self { child, _job: job }
    }
}

impl Drop for BrowserProcess {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            #[cfg(unix)]
            unsafe {
                libc::kill(-(self.child.id() as libc::pid_t), libc::SIGKILL);
            }
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

struct DevtoolsEndpoint {
    port: u16,
    browser_path: String,
}

fn wait_for_devtools_endpoint(
    child: &mut Child,
    profile_dir: &Path,
    deadline: Instant,
) -> Result<DevtoolsEndpoint, String> {
    let active_port = profile_dir.join("DevToolsActivePort");
    let mut exited = None;
    while Instant::now() < deadline {
        if let Ok(contents) = fs::read_to_string(&active_port) {
            let mut lines = contents.lines();
            let port = lines
                .next()
                .ok_or_else(|| "DevToolsActivePort contains no port".to_string())?
                .parse::<u16>()
                .map_err(|err| format!("invalid DevTools port: {err}"))?;
            let browser_path = lines
                .next()
                .filter(|line| line.starts_with("/devtools/browser/"))
                .ok_or_else(|| "DevToolsActivePort contains no browser endpoint".to_string())?
                .to_string();
            return Ok(DevtoolsEndpoint { port, browser_path });
        }
        if exited.is_none() {
            match child.try_wait() {
                // Edge on Windows can be a launcher whose browser child owns
                // DevTools, so keep polling the profile after the direct
                // process exits.
                Ok(Some(status)) => exited = Some(status),
                Ok(None) => {}
                Err(err) => return Err(format!("failed to inspect the headless browser: {err}")),
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Err(match exited {
        Some(status) => {
            format!("the headless browser exited with {status} before DevTools started")
        }
        None => "the headless browser timed out starting DevTools".to_string(),
    })
}

fn capture_html_over_devtools(
    endpoint: &DevtoolsEndpoint,
    url: &str,
    deadline: Instant,
) -> Result<DynamicImage, String> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    let address = SocketAddr::from(([127, 0, 0, 1], endpoint.port));
    let stream = TcpStream::connect_timeout(&address, remaining.min(Duration::from_secs(5)))
        .map_err(|err| format!("failed to connect to browser DevTools: {err}"))?;
    stream
        .set_read_timeout(Some(remaining))
        .map_err(|err| format!("failed to set the DevTools read timeout: {err}"))?;
    stream
        .set_write_timeout(Some(remaining))
        .map_err(|err| format!("failed to set the DevTools write timeout: {err}"))?;
    let websocket_url = format!("ws://127.0.0.1:{}{}", endpoint.port, endpoint.browser_path);
    let (mut socket, _) = tungstenite::client(websocket_url.as_str(), stream)
        .map_err(|err| format!("failed to open browser DevTools: {err}"))?;
    let mut next_id = 1_u64;

    let created = devtools_command(
        &mut socket,
        &mut next_id,
        "Target.createTarget",
        serde_json::json!({ "url": "about:blank" }),
        None,
        deadline,
    )?;
    let target_id = created["targetId"]
        .as_str()
        .ok_or_else(|| "DevTools did not return a target id".to_string())?;
    let attached = devtools_command(
        &mut socket,
        &mut next_id,
        "Target.attachToTarget",
        serde_json::json!({ "targetId": target_id, "flatten": true }),
        None,
        deadline,
    )?;
    let session_id = attached["sessionId"]
        .as_str()
        .ok_or_else(|| "DevTools did not return a session id".to_string())?
        .to_string();
    devtools_command(
        &mut socket,
        &mut next_id,
        "Page.enable",
        serde_json::json!({}),
        Some(&session_id),
        deadline,
    )?;
    devtools_command(
        &mut socket,
        &mut next_id,
        "Page.setLifecycleEventsEnabled",
        serde_json::json!({ "enabled": true }),
        Some(&session_id),
        deadline,
    )?;
    devtools_command(
        &mut socket,
        &mut next_id,
        "Emulation.setDeviceMetricsOverride",
        serde_json::json!({
            "width": 1280,
            "height": 2000,
            "deviceScaleFactor": 1,
            "mobile": false
        }),
        Some(&session_id),
        deadline,
    )?;
    let navigated = devtools_command(
        &mut socket,
        &mut next_id,
        "Page.navigate",
        serde_json::json!({ "url": url }),
        Some(&session_id),
        deadline,
    )?;
    if let Some(error_text) = navigated["errorText"].as_str() {
        return Err(format!(
            "the browser could not load the HTML file: {error_text}"
        ));
    }
    let loader_id = navigated["loaderId"]
        .as_str()
        .ok_or_else(|| "DevTools navigation returned no loader id".to_string())?;
    wait_for_devtools_load(&mut socket, &session_id, loader_id, deadline)?;
    let screenshot = devtools_command(
        &mut socket,
        &mut next_id,
        "Page.captureScreenshot",
        serde_json::json!({
            "format": "png",
            "fromSurface": true,
            "captureBeyondViewport": false
        }),
        Some(&session_id),
        deadline,
    )?;
    let encoded = screenshot["data"]
        .as_str()
        .ok_or_else(|| "DevTools returned no screenshot data".to_string())?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|err| format!("DevTools returned invalid screenshot data: {err}"))?;
    image::load_from_memory(&bytes)
        .map_err(|err| format!("DevTools returned an invalid screenshot: {err}"))
}

fn devtools_command(
    socket: &mut tungstenite::WebSocket<TcpStream>,
    next_id: &mut u64,
    method: &str,
    params: serde_json::Value,
    session_id: Option<&str>,
    deadline: Instant,
) -> Result<serde_json::Value, String> {
    let id = *next_id;
    *next_id += 1;
    let mut request = serde_json::json!({
        "id": id,
        "method": method,
        "params": params
    });
    if let Some(session_id) = session_id {
        request["sessionId"] = serde_json::Value::String(session_id.to_string());
    }
    socket
        .send(tungstenite::Message::text(request.to_string()))
        .map_err(|err| format!("failed to send DevTools command {method}: {err}"))?;
    loop {
        let message = read_devtools_message(socket, deadline)
            .map_err(|err| format!("failed reading DevTools response to {method}: {err}"))?;
        let Ok(text) = message.to_text() else {
            continue;
        };
        let response: serde_json::Value = serde_json::from_str(text)
            .map_err(|err| format!("DevTools returned invalid JSON: {err}"))?;
        if response["id"].as_u64() != Some(id) {
            continue;
        }
        if let Some(error) = response.get("error") {
            return Err(format!("DevTools {method} failed: {error}"));
        }
        return Ok(response.get("result").cloned().unwrap_or_default());
    }
}

fn wait_for_devtools_load(
    socket: &mut tungstenite::WebSocket<TcpStream>,
    session_id: &str,
    loader_id: &str,
    deadline: Instant,
) -> Result<(), String> {
    loop {
        let message = read_devtools_message(socket, deadline)
            .map_err(|err| format!("failed waiting for the HTML page to load: {err}"))?;
        let Ok(text) = message.to_text() else {
            continue;
        };
        let event: serde_json::Value = serde_json::from_str(text)
            .map_err(|err| format!("DevTools returned invalid JSON: {err}"))?;
        if event["sessionId"].as_str() == Some(session_id)
            && event["method"].as_str() == Some("Page.lifecycleEvent")
            && event["params"]["name"].as_str() == Some("load")
            && event["params"]["loaderId"].as_str() == Some(loader_id)
        {
            return Ok(());
        }
    }
}

fn read_devtools_message(
    socket: &mut tungstenite::WebSocket<TcpStream>,
    deadline: Instant,
) -> Result<tungstenite::Message, String> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err("the headless browser timed out rendering the page".to_string());
    }
    socket
        .get_mut()
        .set_read_timeout(Some(remaining))
        .map_err(|err| format!("failed to update the DevTools timeout: {err}"))?;
    socket.read().map_err(|err| match err {
        tungstenite::Error::Io(io_error)
            if matches!(
                io_error.kind(),
                std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
            ) =>
        {
            "the headless browser timed out rendering the page".to_string()
        }
        other => other.to_string(),
    })
}

fn html_render_protocol_error(path: &Path, detail: String) -> HtmlRenderError {
    tracing::error!(path = %path.display(), error = %detail, "headless browser failed rendering HTML");
    HtmlRenderError::Render(detail)
}

static LABEL_FONT: OnceLock<Option<FontVec>> = OnceLock::new();

/// Config font, then host_paths discovery, then Windows fixed paths. Optional.
fn label_font() -> Option<&'static FontVec> {
    LABEL_FONT
        .get_or_init(|| {
            let mut candidates: Vec<PathBuf> = Vec::new();
            if let Some(custom) = &crate::config::runtime().thumbnail_font {
                if custom.is_file() {
                    candidates.push(custom.clone());
                } else {
                    tracing::warn!(
                        path = %custom.display(),
                        "configured thumbnail_font not found; falling back \
                         to discovery"
                    );
                }
            }
            if let Some(found) = crate::host_paths::find_label_font() {
                candidates.push(found);
            }
            #[cfg(windows)]
            {
                candidates.extend(
                    [
                        r"C:\Windows\Fonts\segoeui.ttf",
                        r"C:\Windows\Fonts\arial.ttf",
                    ]
                    .iter()
                    .map(PathBuf::from),
                );
            }
            for candidate in candidates {
                if let Ok(bytes) = fs::read(&candidate) {
                    match FontVec::try_from_vec(bytes) {
                        Ok(font) => {
                            tracing::debug!(
                                path = %candidate.display(),
                                "thumbnail label font resolved"
                            );
                            return Some(font);
                        }
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

/// The window frame sampling may draw from, given the item's duration and
/// where its real content ends (docs/video-outro-detection-design.md §7).
///
/// Returns `(seconds, bounded)`. `bounded` says the verdict actually shortens
/// this file, and it is what becomes ffmpeg's decode bound. Both halves are
/// needed: the interval is `seconds / num_frames`, so the N frames spread
/// across the *content* instead of the whole file — but `fps=1/interval`
/// keeps emitting frames to the end of the stream, so a shortened interval
/// alone still puts a card frame in the sample. The bound is what stops it.
///
/// A `content_end_ms` at or past the duration clamps nothing, which is also
/// what a missing or nonsensical value does: absent behaviour, never wrong
/// behaviour.
pub(crate) fn frame_sampling_window(duration: f64, content_end_ms: Option<i64>) -> (f64, bool) {
    let Some(content_end_ms) = content_end_ms else {
        return (duration, false);
    };
    let content_end = content_end_ms as f64 / 1000.0;
    if !content_end.is_finite() || content_end <= 0.0 || content_end >= duration {
        return (duration, false);
    }
    (content_end, true)
}

/// `content_end_ms` is the item's stored outro boundary, when it has one; see
/// [`frame_sampling_window`].
fn extract_video_frames(
    path: &Path,
    num_frames: usize,
    duration: f64,
    content_end_ms: Option<i64>,
) -> Result<Vec<DynamicImage>, FileProcessError> {
    if duration <= 0.0 {
        return Ok(Vec::new());
    }

    let (window, bounded) = frame_sampling_window(duration, content_end_ms);
    let interval = window / num_frames as f64;
    let temp_dir = temp_dir_path();
    fs::create_dir_all(&temp_dir).map_err(|err| FileProcessError::Io(err.to_string()))?;

    let result = extract_video_frames_into(
        path,
        num_frames,
        interval,
        bounded.then_some(window),
        &temp_dir,
    );
    if let Err(err) = fs::remove_dir_all(&temp_dir) {
        tracing::debug!(error = %err, path = %temp_dir.display(), "failed to remove temp frame dir");
    }
    result
}

/// `decode_limit` is the outro clamp, in seconds. Passed as an *input* option
/// (`-t` before `-i`) so it bounds the decode itself rather than only what the
/// image muxer writes; the filter graph then never sees a frame past the
/// boundary, which is what design §7 asks for over a recomputed interval.
fn extract_video_frames_into(
    path: &Path,
    num_frames: usize,
    interval: f64,
    decode_limit: Option<f64>,
    temp_dir: &Path,
) -> Result<Vec<DynamicImage>, FileProcessError> {
    let output_pattern = temp_dir.join("frame_%04d.png");
    // stdout is silenced, but stderr is captured so a failure can say why
    // (corrupt file, missing codec, disk full) instead of just "ffmpeg
    // failed"; it is only surfaced on a non-zero exit.
    let mut args: Vec<std::ffi::OsString> = Vec::new();
    if let Some(limit) = decode_limit {
        args.push("-t".into());
        args.push(format!("{limit}").into());
    }
    args.push("-i".into());
    args.push(path.into());
    args.push("-vf".into());
    args.push(format!("fps=1/{}", interval).into());
    args.push("-vsync".into());
    args.push("vfr".into());
    args.push(output_pattern.clone().into());
    // The retry wrapper, not a bare spawn: the decode clamp above is an
    // *input* `-t`, which is one of the triggers of the Windows/SMB
    // input-open bug (see `media_tools::cache_wrapped_args`) — so without it
    // a clamped video on a network mount fails to open at all.
    let output = crate::media_tools::ffmpeg_output_with_input_retry(&args, |command| {
        command.stdout(std::process::Stdio::null());
    })
    // Failing to *start* ffmpeg is never a verdict on the media: a missing
    // toolchain is `blocked` and self-heals when it appears, anything else
    // about this machine stays transient and is retried next scan.
    .map_err(|err| {
        FileProcessError::visuals_from_api_error(crate::media_tools::spawn_error("ffmpeg", &err))
    })?;

    if !output.status.success() {
        let detail = crate::media_tools::stderr_tail(&output.stderr);
        // The generator's own classified line, the twin of pdfium's and the
        // browser's: the callers log this at debug (one verdict, one log), so
        // this is where a video that will not yield frames becomes visible.
        tracing::error!(
            path = %path.display(),
            error = %detail,
            "ffmpeg failed to extract frames"
        );
        // ffmpeg did its own file I/O, so a broken file and a transient mount
        // hiccup exit identically: this needs a second failure in a later scan
        // before it suppresses anything.
        return Err(visuals_input_unconfirmed(format!("ffmpeg failed: {detail}")));
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
    codec_name: Option<String>,
    duration: Option<String>,
    width: Option<u64>,
    height: Option<u64>,
    /// ffprobe's own `stream_disposition` section, nested under this key in its
    /// JSON. Absent for a build (or a demuxer) that reports none, which reads
    /// as "not cover art" — the conservative half, since the alternative would
    /// be discarding a real video stream.
    disposition: Option<FfprobeDisposition>,
    #[allow(dead_code)]
    tags: Option<FfprobeTags>,
    /// ffprobe's `side_data_list`, requested only for the display matrix it
    /// carries on a rotated capture (docs/display-dimensions-design.md §3).
    side_data_list: Option<Vec<FfprobeSideData>>,
}

/// One entry of a stream's `side_data_list`. Only the rotation is read; every
/// other kind of side data deserializes to a `None` here and is dropped.
#[derive(Debug, Deserialize)]
struct FfprobeSideData {
    rotation: Option<f64>,
}

/// The one disposition flag any of this cares about: `attached_pic` marks a
/// video stream that is cover art (an mp3's album picture, an mp4's poster
/// frame), not moving pictures.
#[derive(Debug, Deserialize)]
struct FfprobeDisposition {
    attached_pic: Option<i64>,
}

impl FfprobeStream {
    /// The **clockwise** quarter turns from this stream's coded pixels to its
    /// picture, normalized to 0/90/180/270, so a video's `items.rotation`
    /// means the same thing an image's does (where it comes from EXIF, whose
    /// turns are clockwise by definition).
    ///
    /// Negated, because ffprobe's `rotation` is counter-clockwise. Measured
    /// rather than assumed: a frame with a marker in its top-left corner,
    /// muxed with `-display_rotation 90`, probes as `rotation: 90` and decodes
    /// with the marker in the **bottom** left — which is a 90 degree
    /// counter-clockwise turn, i.e. 270 clockwise. `-display_rotation -90`
    /// probes as `-90` and decodes with the marker top-**right**, 90
    /// clockwise, which is the common portrait phone capture.
    ///
    /// Only the transposition is consumed (`quarter_turns_transpose`), and on
    /// that both conventions agree — which is why
    /// `media_tools::transcode::compose::parse_probe` gets away with an
    /// `abs()` here. This column is read by people too, so it is signed
    /// correctly.
    ///
    /// A matrix that is not a quarter turn (a shear, or one ffprobe reports a
    /// fractional angle for) is dropped rather than rounded: no transposition
    /// it implies is one this column can express, and 0 is what every decoder
    /// in this codebase does with such a frame anyway.
    fn quarter_turns(&self) -> i64 {
        self.side_data_list
            .as_deref()
            .unwrap_or_default()
            .iter()
            .find_map(|entry| entry.rotation)
            .filter(|rotation| rotation.is_finite())
            .map(|rotation| (-rotation.round() as i64).rem_euclid(360))
            .filter(|turns| turns % 90 == 0)
            .unwrap_or(0)
    }

    /// Whether this stream is cover art rather than content.
    fn is_attached_pic(&self) -> bool {
        self.disposition
            .as_ref()
            .and_then(|disposition| disposition.attached_pic)
            .unwrap_or(0)
            != 0
    }
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
    /// ffprobe's `codec_name` for this stream, absent when it reported none.
    codec_name: Option<String>,
}

struct VideoTrack {
    duration: f64,
    /// ffprobe's `width`, which is the *coded* width — see
    /// [`FfprobeStream::quarter_turns`].
    width: Option<u64>,
    /// ffprobe's `height`, coded. See [`VideoTrack::width`].
    height: Option<u64>,
    /// See [`AudioTrack::codec_name`].
    codec_name: Option<String>,
    /// Clockwise quarter turns from the coded pixels to the picture, read from
    /// the stream's display matrix (0 when it carries none).
    rotation: i64,
}

struct SubtitleTrack;

struct MediaInfo {
    audio_tracks: Vec<AudioTrack>,
    /// The file's *content* video stream, chosen by
    /// [`content_video_stream`] — `None` for a container whose only video
    /// streams are cover art.
    video_track: Option<VideoTrack>,
    subtitle_tracks: Vec<SubtitleTrack>,
}

/// Which of ffprobe's streams is the file's video track: the **first** video
/// stream that is not `attached_pic`.
///
/// Not symmetric with the audio side, which simply takes the first audio
/// stream. Cover art is carried as a video stream — an album picture in an
/// mp3, a poster frame in an mp4 — and it is a still image, not moving
/// pictures: taking it would record `mjpeg`/`png` as the file's video codec,
/// its thumbnail's dimensions as the file's dimensions, and its single frame's
/// duration as the file's length. A container with nothing but cover art has
/// no video track at all, which for a `video/` mime is the `'none'` sentinel.
fn content_video_stream(streams: &[FfprobeStream]) -> Option<usize> {
    streams.iter().position(|stream| {
        stream.codec_type.as_deref() == Some("video") && !stream.is_attached_pic()
    })
}

/// `items.video_codec` for a container ffprobe found no video stream in.
pub(crate) const CODEC_NONE: &str = "none";

/// `items.video_codec`/`audio_codec` for a stream that exists but that ffprobe
/// reported no `codec_name` for. Distinct from [`CODEC_NONE`]: there *is*
/// something to decode here, this build just cannot name it — so a client must
/// treat it as "may need transcoding", not "unplayable".
pub(crate) const CODEC_UNKNOWN: &str = "unknown";

/// The two codec columns for one probed file, per the sentinel convention in
/// `migrations/index/20260809120000_item_codecs.sql`.
///
/// The single place either column is derived, shared by the new-item metadata
/// phase and the backfill probe so the two can never disagree about what a
/// missing stream means.
///
/// The two columns are *not* filled symmetrically: `audio_codec` is the first
/// audio stream's, while `video_codec` is the first video stream that is not
/// cover art (see [`content_video_stream`]), and a container holding nothing
/// but cover art records `'none'` — it has no moving pictures to transcode.
///
/// `video_codec` is `None` only for a non-video mime: an audio file records
/// its audio codec and leaves the video column NULL, because `'none'` there
/// would claim the file was examined *as a video*, and the backfill's
/// termination predicate is that column.
fn media_codecs(info: &MediaInfo, mime_type: &str) -> (Option<String>, Option<String>) {
    let audio_codec = info
        .audio_tracks
        .first()
        .map(|track| track.codec_name.clone().unwrap_or(CODEC_UNKNOWN.to_string()));
    if !mime_type.starts_with("video") {
        return (None, audio_codec);
    }
    let video_codec = match &info.video_track {
        Some(video) => video.codec_name.clone().unwrap_or(CODEC_UNKNOWN.to_string()),
        None => CODEC_NONE.to_string(),
    };
    (Some(video_codec), audio_codec)
}

fn extract_media_info(path: &Path) -> Result<MediaInfo, FileProcessError> {
    let output = Command::new(crate::media_tools::ffprobe())
        .arg("-v")
        .arg("error")
        .arg("-show_entries")
        // `stream_disposition` is its own ffprobe section, not a `stream=`
        // field; it lands nested under `disposition` in the JSON all the same.
        .arg(
            "stream=index,codec_type,codec_name,duration,width,height,tags\
             :stream_side_data=rotation\
             :stream_disposition=attached_pic:format=duration",
        )
        .arg("-of")
        .arg("json")
        .arg(path)
        .output()
        // Failing to *start* ffprobe is never a verdict on the media: a
        // missing toolchain is `blocked` and self-heals when it appears,
        // anything else about this machine stays transient.
        .map_err(|err| {
            FileProcessError::from_api_error(
                STAGE_METADATA,
                crate::media_tools::spawn_error("ffprobe", &err),
            )
        })?;

    if !output.status.success() {
        // ffprobe did its own file I/O, so a corrupt file and a transient
        // mount hiccup exit identically: this needs a second failure in a
        // later scan before it suppresses anything.
        return Err(ScanFailure::input_unconfirmed(
            STAGE_METADATA,
            format!(
                "ffprobe failed: {}",
                crate::media_tools::stderr_tail(&output.stderr)
            ),
        ));
    }

    // Exit 0 with output the gateway cannot parse is not a verdict on the
    // media — it is this build and that ffprobe disagreeing — so it stays
    // transient rather than permanently suppressing a file ffprobe accepted.
    let data: FfprobeOutput = serde_json::from_slice(&output.stdout)
        .map_err(|err| FileProcessError::Io(format!("ffprobe produced unusable output: {err}")))?;

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

    // Decided before the streams are consumed, and by index: every other video
    // stream — cover art, or a second angle — contributes nothing, so the
    // dimensions and duration below come from the same stream the codec does.
    let content_video = content_video_stream(&data.streams);

    for (index, stream) in data.streams.into_iter().enumerate() {
        match stream.codec_type.as_deref() {
            Some("audio") => {
                audio_tracks.push(AudioTrack {
                    duration: stream_duration(&stream),
                    codec_name: stream.codec_name,
                });
            }
            Some("video") if content_video == Some(index) => {
                video_track = Some(VideoTrack {
                    duration: stream_duration(&stream),
                    width: stream.width,
                    height: stream.height,
                    rotation: stream.quarter_turns(),
                    codec_name: stream.codec_name,
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

/// [`current_iso_timestamp`] shifted back by `ago`, in the same format, so the
/// two compare as strings — which is how stored timestamps are windowed in SQL.
pub(crate) fn iso_timestamp_ago(ago: std::time::Duration) -> String {
    let now = OffsetDateTime::now_local().unwrap_or_else(|_| OffsetDateTime::now_utc());
    let then = now - ago;
    then.format(iso_format())
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
/// restarts. A bare counter is not enough: after a crash, a previous run's
/// `frames-0` could still contain media output or a locked browser profile.
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

/// Whether a *directory* name is filesystem junk nothing under it is worth
/// looking at: the dot-directories macOS scatters over a share (`.Trashes`,
/// `.TemporaryItems`, `.Spotlight-V100`, `.fseventsd`) and the `__MACOSX`
/// directory a zip extraction leaves behind. What is inside them is sidecar
/// and resource-fork litter under ordinary-looking names, which is precisely
/// what [`is_hidden_or_temp`] — a check on the file's own name — cannot catch.
///
/// The `.` prefix only, deliberately not `~`: a `~`-prefixed *file* is an
/// editor's temporary copy, but a `~`-prefixed *directory* is an ordinary
/// folder name a user may well have chosen.
///
/// `__MACOSX` is matched case-insensitively because the extraction tools that
/// create it do not agree on its casing, and Windows/SMB name lookup would
/// resolve any of those spellings to the same directory anyway.
pub(crate) fn is_junk_dir_name(name: &OsStr) -> bool {
    let name = name.to_string_lossy();
    name.starts_with('.') || name.eq_ignore_ascii_case("__MACOSX")
}

/// Whether an absolute file path sits under a junk directory, for the callers
/// that are handed a path instead of walking into it — watcher events and
/// database rows, which never see the directories they passed through.
///
/// The final component is the file's own name, which [`is_hidden_or_temp`]
/// answers for, and the root itself is exempt: a user who registered a
/// dot-named folder as a root asked for that folder to be scanned. The longest
/// matching root wins, so a root nested inside another is judged from its own
/// perspective rather than its ancestor's — [`deduplicate_paths`] collapses
/// nested roots before any caller gets here, so that is a property of this
/// function rather than a case in flight today.
pub(crate) fn is_under_junk_dir(path: &Path, roots: &[PathBuf]) -> bool {
    let Some(root) = roots
        .iter()
        .filter(|root| path.starts_with(root))
        .max_by_key(|root| root.as_os_str().len())
    else {
        return false;
    };
    let Ok(below_root) = path.strip_prefix(root) else {
        return false;
    };
    let mut directories = below_root.components();
    directories.next_back();
    directories.any(|component| is_junk_dir_name(component.as_os_str()))
}

/// What a macOS AppleDouble sidecar or AppleSingle file is, whatever name it
/// arrived under.
pub(crate) const MIME_APPLEFILE: &str = "application/applefile";

/// How many leading bytes the post-failure sniff reads. Every signature it
/// knows sits in the first handful; the rest is slack for a byte-order mark
/// and the whitespace an HTML document may start with.
const SNIFF_BYTES: usize = 256;

/// The content type a file's leading bytes claim, or `None` for anything the
/// sniff does not recognise — which includes every file it could not read.
///
/// Runs on the failure path only, so a read error here is not a failure of its
/// own: it simply leaves the name's guess in place.
fn sniff_junk_mime(path: &Path) -> Option<&'static str> {
    let mut file = fs::File::open(path).ok()?;
    let mut buffer = [0u8; SNIFF_BYTES];
    let mut filled = 0;
    while filled < buffer.len() {
        match file.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => return None,
        }
    }
    sniff_junk_bytes(&buffer[..filled])
}

/// The signature half of [`sniff_junk_mime`], split out so it can be exercised
/// on bytes rather than on files.
fn sniff_junk_bytes(bytes: &[u8]) -> Option<&'static str> {
    // AppleDouble (the `._name` sidecars, and the flattened copies a share
    // hands back with the directory separators folded into the name) and
    // AppleSingle differ only in the last magic byte.
    if bytes.starts_with(&[0x00, 0x05, 0x16, 0x07]) || bytes.starts_with(&[0x00, 0x05, 0x16, 0x00])
    {
        return Some(MIME_APPLEFILE);
    }
    let text = bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(bytes);
    let text = text.trim_ascii_start();
    [b"<!doctype".as_slice(), b"<html", b"<!--"]
        .iter()
        .any(|tag| {
            text.get(..tag.len())
                .is_some_and(|head| head.eq_ignore_ascii_case(tag))
        })
        .then_some("text/html")
}

/// Replaces a recorded failure's mime type with what the file's first bytes
/// say, for the verdicts where the name's guess is the part most likely to be
/// a lie.
///
/// The scope is deliberately narrow. An `input` verdict at the header or the
/// metadata stage means a reader looked at real bytes and rejected them, and a
/// mime-family retry directive — "re-attempt the images now that the decoder
/// handles them" — is exactly what must never resurrect a file whose bytes are
/// an AppleDouble resource fork wearing a `.png` name. A `mime` verdict has no
/// guess to correct, a `decode` row is audit-only, and the other classes are
/// verdicts on this machine rather than on the bytes.
///
/// A match also settles the verdict at one attempt: the ambiguity the header
/// stage otherwise allows for is a file another process is still writing, and
/// what the gateway just read is a complete signature for a format the name
/// never promised.
///
/// `stage`, the class and the error message are left exactly as classified:
/// the row still records the failure that actually happened.
pub(crate) fn override_mime_from_content(record: &mut ScanErrorRecord, path: &Path) {
    if record.stage != STAGE_HEADER && record.stage != STAGE_METADATA {
        return;
    }
    if !matches!(record.kind, ApiErrorKind::Input) {
        return;
    }
    let Some(mime) = sniff_junk_mime(path) else {
        return;
    };
    record.mime_type = Some(mime.to_string());
    record.skip_after = SKIP_AFTER_CONFIRMED;
}

/// A 7s clip: 5s of flat grey, then `card_seconds` of the TikTok card
/// background with a white box where the logo sits. Returns `false` when
/// this machine cannot build it, so the tests that need real media skip
/// rather than fail. Lives outside `mod tests` because the extraction-side
/// sampler has tests that clamp the same fixture through an ffmpeg invocation
/// of its own.
///
/// `vcodec` overrides the encoder, for the codec-column tests that need a
/// container whose stream is *not* h264. `Some("libx265")` also stamps the
/// `hvc1` tag, which is what an mp4 needs before a browser will look at the
/// stream at all — and, on a build without libx265, is exactly the case the
/// `false` return exists for.
#[cfg(test)]
pub(crate) fn write_clip(path: &Path, card_seconds: Option<u32>, vcodec: Option<&str>) -> bool {
    if !crate::media_tools::ffmpeg_available() {
        return false;
    }
    let mut command = Command::new(crate::media_tools::ffmpeg());
    command.args(["-y", "-v", "error", "-f", "lavfi"]);
    match card_seconds {
        Some(seconds) => {
            command.args([
                "-i",
                "color=c=0x404040:s=576x1024:d=5:r=30",
                "-f",
                "lavfi",
                "-i",
                &format!("color=c=0x0C0D19:s=576x1024:d={seconds}:r=30"),
                "-filter_complex",
                "[1:v]drawbox=x=100:y=480:w=376:h=60:color=white:t=fill[card];\
                 [0:v][card]concat=n=2:v=1:a=0[out]",
                "-map",
                "[out]",
            ]);
        }
        None => {
            command.args(["-i", "color=c=0x404040:s=576x1024:d=7:r=30"]);
        }
    }
    command.args(["-pix_fmt", "yuv420p", "-crf", "18"]);
    if let Some(vcodec) = vcodec {
        command.args(["-c:v", vcodec]);
        if vcodec == "libx265" {
            command.args(["-tag:v", "hvc1"]);
        }
    }
    command.arg(path);
    matches!(command.status(), Ok(status) if status.success())
}

/// The colour of a frame's top-left corner, which is flat background in
/// both halves of the fixture: grey in the content, the card colour in the
/// card.
#[cfg(test)]
pub(crate) fn corner_is_card(image: &DynamicImage) -> bool {
    let pixel = image.to_rgb8();
    let pixel = pixel.get_pixel(4, 4);
    (0..3)
        .all(|channel| (i32::from(pixel[channel]) - i32::from([12u8, 13, 25][channel])).abs() <= 10)
}

/// A JPEG carrying an EXIF orientation, built rather than committed: the
/// image crate can encode a JPEG but not its EXIF, and a binary fixture
/// would hide the one thing these tests are about.
///
/// The segment is the minimum a decoder needs: an APP1 marker, the `Exif`
/// signature, a little-endian TIFF header, and an IFD of exactly one entry
/// — tag 0x0112 (Orientation), type SHORT, count 1.
#[cfg(test)]
pub(crate) fn jpeg_with_exif_orientation(
    width: u32,
    height: u32,
    exif_orientation: u16,
) -> Vec<u8> {
    let mut base = Vec::new();
    // Noise rather than a flat colour, so a transposed decode is visible
    // in the pixels and not only in the dimensions.
    let mut pixels = image::RgbImage::new(width, height);
    for (x, y, pixel) in pixels.enumerate_pixels_mut() {
        *pixel = image::Rgb([(x * 7 % 251) as u8, (y * 13 % 241) as u8, 32]);
    }
    image::DynamicImage::ImageRgb8(pixels)
        .write_to(&mut std::io::Cursor::new(&mut base), image::ImageFormat::Jpeg)
        .expect("the fixture encodes");

    let mut tiff = Vec::new();
    tiff.extend_from_slice(b"II\x2a\x00"); // little-endian, magic 42
    tiff.extend_from_slice(&8u32.to_le_bytes()); // IFD0 at offset 8
    tiff.extend_from_slice(&1u16.to_le_bytes()); // one entry
    tiff.extend_from_slice(&0x0112u16.to_le_bytes()); // Orientation
    tiff.extend_from_slice(&3u16.to_le_bytes()); // SHORT
    tiff.extend_from_slice(&1u32.to_le_bytes()); // count
    tiff.extend_from_slice(&exif_orientation.to_le_bytes());
    tiff.extend_from_slice(&[0, 0]); // the value field is four bytes wide
    tiff.extend_from_slice(&0u32.to_le_bytes()); // no IFD1

    let mut payload = b"Exif\x00\x00".to_vec();
    payload.extend_from_slice(&tiff);
    let mut app1 = vec![0xFF, 0xE1];
    // The length counts itself but not the marker.
    app1.extend_from_slice(&((payload.len() + 2) as u16).to_be_bytes());
    app1.extend_from_slice(&payload);

    // Straight after SOI, which is where a segment may always go.
    let mut out = base[..2].to_vec();
    out.extend_from_slice(&app1);
    out.extend_from_slice(&base[2..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::migrate_databases_on_disk;
    use crate::test_utils::test_data_dir;
    use crate::visual_tiers::LOOP_TIER;

    fn next_db_name() -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        format!("testdb_{}", COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    #[test]
    fn html_indexing_requires_a_renderer_but_other_types_do_not() {
        assert!(require_html_renderer_for_indexing_with("image/png", false).is_ok());
        assert!(require_html_renderer_for_indexing_with("text/plain", false).is_ok());
        assert!(require_html_renderer_for_indexing_with("text/html", true).is_ok());

        let error = require_html_renderer_for_indexing_with("text/html", false).unwrap_err();
        let failure = error.classified().expect("missing renderer is persistent");
        assert_eq!(failure.stage, STAGE_METADATA);
        assert_eq!(failure.kind.blocker(), Some(Blocker::HtmlRenderer));
        assert_eq!(failure.skip_after, SKIP_AFTER_CONFIRMED);
        assert!(failure.message.contains("HTML indexing requires"));
    }

    #[test]
    fn html_render_failures_are_indexing_failures() {
        let error = html_visuals_error_blocks_indexing(VisualsError::thumbnail(
            html_visuals_failure(HtmlRenderError::Render("browser exited".to_string())),
        ));
        let failure = error
            .classified()
            .expect("HTML render failure must block insertion");
        assert_eq!(failure.stage, STAGE_METADATA);
        assert_eq!(failure.kind, ApiErrorKind::Input);
        assert_eq!(failure.skip_after, SKIP_AFTER_AMBIGUOUS);
        assert_eq!(failure.message, "browser exited");
    }

    // The VACUUM gate decides from a measurement (`PRAGMA freelist_count` vs
    // `page_count`, on `main` and the attached `storage`), and that
    // measurement had no coverage anywhere: a freshly migrated database must
    // not be rewritten, a heavily deleted one must be, and the full
    // maintenance pass must survive a real database end to end.
    #[tokio::test]
    async fn vacuum_gate_follows_the_free_page_counts() {
        let _test_env = test_data_dir();
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();

        assert!(
            !vacuum_is_worthwhile(&index_db).await,
            "a freshly migrated database has nothing worth reclaiming"
        );

        // Thousands of pages of payload, then deleted: with auto_vacuum off
        // the pages stay on the freelist until a VACUUM reclaims them, which
        // clears both the 2,500-page floor and the 10% ratio.
        {
            let mut conn = crate::db::open_index_db_write_no_user_data(&index_db)
                .await
                .unwrap();
            sqlx::query("CREATE TABLE vacuum_gate_scratch (payload BLOB)")
                .execute(&mut conn)
                .await
                .unwrap();
            let payload = vec![0_u8; 8192];
            for _ in 0..3_000 {
                sqlx::query("INSERT INTO vacuum_gate_scratch (payload) VALUES (?)")
                    .bind(payload.as_slice())
                    .execute(&mut conn)
                    .await
                    .unwrap();
            }
            sqlx::query("DELETE FROM vacuum_gate_scratch")
                .execute(&mut conn)
                .await
                .unwrap();
        }

        assert!(
            vacuum_is_worthwhile(&index_db).await,
            "thousands of freed pages must pass the gate"
        );

        // The pass never reports failure, so this is the only way a broken
        // step shows up at all — and the reclaimed pages prove the VACUUM
        // really ran.
        run_post_job_maintenance(&index_db, true, true).await;
        assert!(
            !vacuum_is_worthwhile(&index_db).await,
            "the VACUUM should have reclaimed the freed pages"
        );
    }

    // The recount is the one maintenance step that is gated, and both halves
    // of the gate matter: the owed flag is what a job that knows it wrote tags
    // passes in, and the durable marker is what covers everything the flags
    // cannot survive (a killed job, the continuous scan). `tags.item_count` is
    // the observable: the recount rewrites it, so a deliberately wrong value
    // that survives a pass proves the pass skipped it.
    #[tokio::test]
    async fn the_recount_runs_only_for_the_flag_or_the_marker() {
        let _test_env = test_data_dir();
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();

        // One tagged item, written directly: this test is about the gate, not
        // about the writer paths that set the marker.
        {
            let mut conn = crate::db::open_index_db_write_no_user_data(&index_db)
                .await
                .unwrap();
            for statement in [
                "INSERT INTO items (sha256, md5, type, time_added) \
                 VALUES ('sha-gate', 'md5-gate', 'image/png', '2026-01-01')",
                "INSERT INTO setters (name) VALUES ('gate/tagger')",
                "INSERT INTO item_data \
                     (item_id, job_id, setter_id, data_type, idx, is_origin, is_placeholder) \
                 SELECT items.id, NULL, setters.id, 'tags', 0, 1, 0 FROM items, setters",
                "INSERT INTO tags (namespace, name) VALUES ('general', 'gate-tag')",
                "INSERT INTO tags_items (item_data_id, tag_id, confidence, item_id) \
                 SELECT item_data.id, tags.id, 1.0, item_data.item_id FROM item_data, tags",
            ] {
                sqlx::query(statement)
                    .execute(&mut conn)
                    .await
                    .unwrap_or_else(|err| panic!("{statement} failed: {err}"));
            }
        }
        async fn item_count(index_db: &str) -> i64 {
            let mut conn = open_index_db_read_no_user_data(index_db).await.unwrap();
            sqlx::query_scalar("SELECT item_count FROM tags WHERE name = 'gate-tag'")
                .fetch_one(&mut conn)
                .await
                .unwrap()
        }

        // The migration seeds the marker dirty, so the first pass recounts
        // even though nothing told it to — the property that heals counts a
        // process death took the owed flags with.
        assert_eq!(item_count(&index_db).await, 0, "inserted, never counted");
        run_post_job_maintenance(&index_db, false, false).await;
        assert_eq!(
            item_count(&index_db).await,
            1,
            "the durable marker alone must run the recount"
        );

        // The recount cleared the marker, so an identical pass now skips it.
        assert!(
            !tags_are_dirty(&index_db).await,
            "a successful recount must clear the marker"
        );
        {
            let mut conn = crate::db::open_index_db_write_no_user_data(&index_db)
                .await
                .unwrap();
            sqlx::query("UPDATE tags SET item_count = 99 WHERE name = 'gate-tag'")
                .execute(&mut conn)
                .await
                .unwrap();
        }
        run_post_job_maintenance(&index_db, false, false).await;
        assert_eq!(
            item_count(&index_db).await,
            99,
            "neither flag nor marker: the recount must not run"
        );

        // The owed flag runs it without any marker.
        run_post_job_maintenance(&index_db, false, true).await;
        assert_eq!(
            item_count(&index_db).await,
            1,
            "the owed tags_changed flag must run the recount"
        );
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

    /// (path, stage, error_class, attempts, skip_after, mime_type) per row.
    async fn scan_error_rows(
        conn: &mut sqlx::SqliteConnection,
    ) -> Vec<(String, String, String, i64, i64, Option<String>)> {
        sqlx::query_as(
            "SELECT path, stage, error_class, attempts, skip_after, mime_type \
             FROM scan_errors ORDER BY path",
        )
        .fetch_all(conn)
        .await
        .unwrap()
    }

    // The end-to-end contract of the scan ledger: a file the scan cannot get
    // an item out of is recorded once, skipped on every later scan without
    // being hashed or decoded again, re-attempted the moment its bytes move,
    // cleared when it finally works, and swept when it disappears.
    //
    // Before this, none of it happened: no `files` row meant no mtime
    // shortcut, so every scan re-read and re-decoded every broken file
    // forever, and the only trace was an integer in `file_scans.errors`.
    #[tokio::test]
    async fn rescan_records_skips_and_clears_a_broken_file() {
        let test_env = test_data_dir();
        let root = test_env.path();
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();

        // Its own folder: `test_data_dir()` hands every test the same
        // process-wide temp root (serialized by a mutex), so the shared
        // `media` directory still carries whatever earlier scan tests left in
        // it — and this test counts files.
        let media_dir = root.join("ledger_media");
        fs::create_dir_all(&media_dir).unwrap();
        // A healthy neighbour, so every assertion below has to discriminate
        // rather than merely observe an empty database.
        image::RgbImage::new(8, 8)
            .save(media_dir.join("good.png"))
            .unwrap();
        // Garbage, not a truncated image: the header is what fails here, which
        // is the only image failure that still keeps a file out of the index.
        // (A file whose header parses and whose pixels do not is indexed
        // without visuals — `a_corrupt_image_is_indexed_without_visuals_once`.)
        let broken = media_dir.join("broken.png");
        fs::write(&broken, b"this claims to be a png and is not").unwrap();
        let broken_path = broken.to_string_lossy().to_string();

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

        // First pass: the folder update scans the new folder and the file
        // fails there once. A header verdict is ambiguous — the walker may
        // have read a file another process was still writing — so it takes a
        // second run to confirm, and the full rescan this same call performs
        // afterwards is that second run: two attempts, confirmed.
        service.rescan_folders().await.unwrap();
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let rows = scan_error_rows(&mut conn).await;
        assert_eq!(rows.len(), 1, "only the broken file owes a row: {rows:?}");
        assert_eq!(
            (
                rows[0].0.as_str(),
                rows[0].1.as_str(),
                rows[0].2.as_str(),
                rows[0].3,
                rows[0].4,
                rows[0].5.as_deref()
            ),
            (
                broken_path.as_str(),
                STAGE_HEADER,
                "input",
                2,
                SKIP_AFTER_AMBIGUOUS,
                Some("image/png")
            ),
            "an unparseable header is an input verdict confirmed by the second run"
        );
        // The retry key is the *stat* pair, not the byte count the hasher
        // read: the continuous scan only ever has a stat, so a batch failure
        // that stored the read size would look like a different file to it and
        // the two writers would reset each other's attempts forever.
        let stored_key: (String, i64) =
            sqlx::query_as("SELECT last_modified, file_size FROM scan_errors WHERE path = ?")
                .bind(&broken_path)
                .fetch_one(&mut conn)
                .await
                .unwrap();
        assert_eq!(
            stored_key,
            get_last_modified_time_and_size(&broken).unwrap(),
            "the recorded retry key is what a stat produces"
        );
        let files: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(files.0, 1, "the broken file is still not indexed");
        drop(conn);

        // Now that it is confirmed, a later scan does not touch it at all —
        // the attempt count is the proof: it would have gone up again if the
        // file had been re-decoded.
        service.rescan_folders().await.unwrap();
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let rows = scan_error_rows(&mut conn).await;
        assert_eq!((rows.len(), rows[0].3), (1, 2), "no re-attempt: {rows:?}");
        let (_, _, _, errors, marked) = latest_scan_record(&mut conn).await;
        assert_eq!(
            (errors, marked),
            (0, 0),
            "a skipped file is neither an error nor unavailable"
        );
        drop(conn);

        // Touching it (same garbage, new mtime) is a new verdict: the retry
        // key moved, so the file is processed again and the count restarts.
        let mtime = fs::metadata(&broken).unwrap().modified().unwrap();
        fs::File::options()
            .write(true)
            .open(&broken)
            .unwrap()
            .set_modified(mtime + std::time::Duration::from_secs(10))
            .unwrap();
        service.rescan_folders().await.unwrap();
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let (_, _, _, errors, _) = latest_scan_record(&mut conn).await;
        assert_eq!(errors, 1, "a modified file is always re-attempted");
        let rows = scan_error_rows(&mut conn).await;
        assert_eq!(rows[0].3, 1, "the new bytes start their own count");
        drop(conn);

        // Repairing it clears the row and indexes the file.
        image::RgbImage::new(4, 4).save(&broken).unwrap();
        fs::File::options()
            .write(true)
            .open(&broken)
            .unwrap()
            .set_modified(mtime + std::time::Duration::from_secs(20))
            .unwrap();
        service.rescan_folders().await.unwrap();
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        assert!(
            scan_error_rows(&mut conn).await.is_empty(),
            "a file that works owes no verdict"
        );
        let files: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(files.0, 2);
        drop(conn);

        // And the sweep: a row whose file the walk never reaches describes
        // nothing, so it goes away rather than accumulating forever.
        let vanishing = media_dir.join("vanishing.png");
        fs::write(&vanishing, b"not a png either").unwrap();
        service.rescan_folders().await.unwrap();
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        assert_eq!(scan_error_rows(&mut conn).await.len(), 1);
        drop(conn);
        fs::remove_file(&vanishing).unwrap();
        service.rescan_folders().await.unwrap();
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        assert!(
            scan_error_rows(&mut conn).await.is_empty(),
            "the sweep clears rows for files that are gone"
        );
    }

    /// Every indexed path, so a traversal test can say what was *not* reached.
    async fn indexed_paths(conn: &mut sqlx::SqliteConnection) -> Vec<String> {
        sqlx::query_scalar("SELECT path FROM files ORDER BY path")
            .fetch_all(conn)
            .await
            .unwrap()
    }

    // macOS junk directories are pruned on the way down, not per file: the
    // names *inside* `.Trashes` and `__MACOSX` are ordinary, so the file-name
    // rules (`is_hidden_or_temp`) never saw them and every sidecar in there was
    // scanned — and, being resource forks, failed.
    //
    // The root itself is exempt in the same walk: a user who registers a
    // dot-named folder as an included root asked for that folder to be
    // scanned, and WalkDir hands it over at depth 0 where the pruning does not
    // apply.
    #[tokio::test]
    async fn a_scan_prunes_junk_directories_but_not_a_dot_named_root() {
        let test_env = test_data_dir();
        let root = test_env.path();
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();

        // Its own folder: `test_data_dir()` hands every test the same
        // process-wide temp root, and this test counts what was reached.
        let media_dir = root.join("junk_dir_media");
        // A dot-named *root*, deliberately outside the folder above so the
        // exemption is tested as a root rather than as a subdirectory.
        let dot_root = root.join(".dot_named_root");
        for dir in [
            media_dir.join("sub"),
            media_dir.join(".Trashes"),
            // A junk directory's whole subtree goes, not just its own entries.
            media_dir.join("__MACOSX").join("deep"),
            dot_root.clone(),
        ] {
            fs::create_dir_all(&dir).unwrap();
        }
        // Joined component by component: the stored path carries the platform
        // separator, and this test compares stored paths.
        let reachable = [
            media_dir.join("good.png"),
            media_dir.join("sub").join("nested.png"),
        ];
        let pruned = [
            media_dir.join(".Trashes").join("trashed.png"),
            media_dir.join("__MACOSX").join("resource.png"),
            media_dir.join("__MACOSX").join("deep").join("deeper.png"),
        ];
        for path in reachable.iter().chain(pruned.iter()) {
            image::RgbImage::new(8, 8).save(path).unwrap();
        }
        let in_dot_root = dot_root.join("inside.png");
        image::RgbImage::new(8, 8).save(&in_dot_root).unwrap();

        let store = SystemConfigStore::new(root.to_path_buf());
        let mut config = SystemConfig::default();
        config.included_folders = vec![
            media_dir.to_string_lossy().to_string(),
            dot_root.to_string_lossy().to_string(),
        ];
        store.save(&index_db, &config).unwrap();
        execute_folder_scan(
            &index_db,
            &user_data_db,
            &config,
            &config.included_folders,
            &[],
            ScanOptions { worker_count: 2 },
        )
        .await
        .unwrap();

        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let mut expected: Vec<String> = reachable
            .iter()
            .chain(std::iter::once(&in_dot_root))
            .map(|path| path.to_string_lossy().to_string())
            .collect();
        expected.sort();
        assert_eq!(
            indexed_paths(&mut conn).await,
            expected,
            "everything under a junk directory must be unreachable, and a \
             dot-named root must still be walked"
        );
    }

    // The ledger half of the junk work: a file whose *name* says `.png` and
    // whose *bytes* are an AppleDouble resource fork is recorded as what it
    // is. Without this it lands in the ledger as `image/png`, and a retry
    // directive aimed at the image failures ("the decoder handles them now")
    // would resurrect a file no decoder will ever read.
    //
    // The threshold moves with it: the header stage is otherwise ambiguous
    // because the walker may have read a file another process was still
    // writing, and a complete AppleDouble signature is not a half-written PNG.
    #[tokio::test]
    async fn a_sniffed_junk_file_is_recorded_as_what_its_bytes_are() {
        let test_env = test_data_dir();
        let root = test_env.path();
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();

        let media_dir = root.join("sniff_media");
        fs::create_dir_all(&media_dir).unwrap();
        // The real shape of the observed rows: a share flattened the sidecar's
        // directory into its name, so no path rule can help and only the bytes
        // can say what this is.
        let sidecar = media_dir.join("batch 01-._Boop.png");
        let mut apple_double = vec![0x00, 0x05, 0x16, 0x07, 0x00, 0x02, 0x00, 0x00];
        apple_double.extend_from_slice(&[0u8; 64]);
        fs::write(&sidecar, &apple_double).unwrap();
        // The discriminator: a broken PNG whose bytes match nothing keeps the
        // name's guess and the stage's own threshold.
        let garbage = media_dir.join("garbage.png");
        fs::write(&garbage, b"this claims to be a png and is not").unwrap();

        let store = SystemConfigStore::new(root.to_path_buf());
        let mut config = SystemConfig::default();
        config.included_folders = vec![media_dir.to_string_lossy().to_string()];
        store.save(&index_db, &config).unwrap();
        execute_folder_scan(
            &index_db,
            &user_data_db,
            &config,
            &config.included_folders,
            &[],
            ScanOptions { worker_count: 2 },
        )
        .await
        .unwrap();

        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let rows = scan_error_rows(&mut conn).await;
        assert_eq!(rows.len(), 2, "both files owe a row: {rows:?}");
        let row = |name: &str| {
            rows.iter()
                .find(|row| row.0.ends_with(name))
                .unwrap_or_else(|| panic!("no row for {name}: {rows:?}"))
                .clone()
        };

        let sniffed = row("._Boop.png");
        assert_eq!(
            (
                sniffed.1.as_str(),
                sniffed.2.as_str(),
                sniffed.4,
                sniffed.5.as_deref()
            ),
            (
                STAGE_HEADER,
                "input",
                SKIP_AFTER_CONFIRMED,
                Some(MIME_APPLEFILE)
            ),
            "the bytes name the format and settle the verdict: {sniffed:?}"
        );
        let honest = row("garbage.png");
        assert_eq!(
            (
                honest.1.as_str(),
                honest.2.as_str(),
                honest.4,
                honest.5.as_deref()
            ),
            (
                STAGE_HEADER,
                "input",
                SKIP_AFTER_AMBIGUOUS,
                Some("image/png")
            ),
            "an unrecognised failure is left exactly as classified: {honest:?}"
        );
        let error: String =
            sqlx::query_scalar("SELECT error FROM scan_errors WHERE path LIKE '%Boop.png'")
                .fetch_one(&mut conn)
                .await
                .unwrap();
        assert!(
            !error.is_empty() && !error.contains(MIME_APPLEFILE),
            "the recorded error stays the real one: {error}"
        );
    }

    /// A real animated GIF of `frames` frames, written by the `image` crate's
    /// encoder so the LZW data is valid and ffmpeg will decode it. `pattern`
    /// picks the pixels; the caller uses it to choose content whose GIF and
    /// H.264 sizes it cares about.
    pub(super) fn write_animated_gif(
        path: &Path,
        side: u32,
        frames: usize,
        delay_ms: u32,
        pattern: impl Fn(u32, u32, usize) -> image::Rgba<u8>,
    ) {
        let file = fs::File::create(path).unwrap();
        let mut encoder = image::codecs::gif::GifEncoder::new(file);
        encoder
            .set_repeat(image::codecs::gif::Repeat::Infinite)
            .unwrap();
        let built: Vec<image::Frame> = (0..frames)
            .map(|index| {
                let buffer = image::RgbaImage::from_fn(side, side, |x, y| pattern(x, y, index));
                image::Frame::from_parts(
                    buffer,
                    0,
                    0,
                    image::Delay::from_numer_denom_ms(delay_ms, 1),
                )
            })
            .collect();
        encoder.encode_frames(built).unwrap();
    }

    /// A 1400x1400 picture with an alpha channel that is actually *used* — a
    /// transparent 32px corner — over a cheap gradient.
    ///
    /// The size is the point twice over: past 1.25x every grid tier, so all
    /// three are stored, and the corner makes R4's verdict `1` from the
    /// pixels rather than from the header (only 2.3% of PNGs that carry an
    /// alpha channel use it).
    fn alpha_logo(side: u32) -> image::RgbaImage {
        image::RgbaImage::from_fn(side, side, |x, y| {
            let alpha = if x < 32 && y < 32 { 0 } else { 255 };
            image::Rgba([(x / 8) as u8, (y / 8) as u8, 40, alpha])
        })
    }

    /// [`alpha_logo`] filled with noise instead of a gradient, so the PNG is
    /// far past the lossless class's 2 MiB byte bound and a display rendition
    /// is genuinely owed rather than merely allowed.
    ///
    /// A fixed xorshift rather than a random source: a fixture whose byte
    /// count moves between runs is a byte-bound test that passes by luck.
    fn noise_rgba(side: u32) -> image::RgbaImage {
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        image::RgbaImage::from_fn(side, side, |x, y| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let bytes = state.to_le_bytes();
            let alpha = if x < 32 && y < 32 { 0 } else { 255 };
            image::Rgba([bytes[0], bytes[1], bytes[2], alpha])
        })
    }

    /// One row of `storage.thumbnail_tiers` as the tests read it back.
    #[derive(Clone, Debug, PartialEq, Eq, sqlx::FromRow)]
    struct TierSnapshot {
        id: i64,
        idx: i64,
        tier: String,
        width: i64,
        height: i64,
        version: i64,
        media_type: String,
        thumbnail: Vec<u8>,
    }

    /// Every stored grid rendition, in the dispatcher's own order.
    ///
    /// One read behind every projection below. Seven queries for seven views
    /// of one table is seven chances for two assertions in the same test to
    /// be looking at rows in different orders — and the rowid assertions in
    /// particular are only evidence while the order is fixed.
    async fn tier_snapshot(conn: &mut sqlx::SqliteConnection) -> Vec<TierSnapshot> {
        sqlx::query_as(
            "SELECT id, idx, tier, width, height, version, media_type, thumbnail \
             FROM storage.thumbnail_tiers ORDER BY idx, tier, id",
        )
        .fetch_all(conn)
        .await
        .unwrap()
    }

    /// One row of `storage.thumbnails` as the tests read it back.
    #[derive(Clone, Debug, PartialEq, Eq, sqlx::FromRow)]
    struct DisplaySnapshot {
        id: i64,
        idx: i64,
        media_type: String,
        thumbnail: Vec<u8>,
    }

    /// Every stored display rendition, by index. See [`tier_snapshot`].
    async fn display_snapshot(conn: &mut sqlx::SqliteConnection) -> Vec<DisplaySnapshot> {
        sqlx::query_as(
            "SELECT id, idx, media_type, thumbnail FROM storage.thumbnails ORDER BY idx, id",
        )
        .fetch_all(conn)
        .await
        .unwrap()
    }

    /// (kind, outcome, attempts, skip_after, version) per marker, by kind.
    async fn visual_attempt_rows(
        conn: &mut sqlx::SqliteConnection,
    ) -> Vec<(String, String, i64, i64, i64)> {
        sqlx::query_as(
            "SELECT kind, outcome, attempts, skip_after, version \
             FROM storage.visual_attempts ORDER BY kind",
        )
        .fetch_all(conn)
        .await
        .unwrap()
    }

    async fn thumbnail_count(conn: &mut sqlx::SqliteConnection) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM storage.thumbnails")
            .fetch_one(conn)
            .await
            .unwrap()
    }

    /// The rowids of the stored grid tiers. The tier write is a
    /// delete-then-insert of the item's whole set, so an unchanged list is
    /// proof no pass rewrote them — which is exactly what the ladder's
    /// write-once discipline claims.
    async fn tier_ids(conn: &mut sqlx::SqliteConnection) -> Vec<i64> {
        tier_snapshot(conn)
            .await
            .into_iter()
            .map(|row| row.id)
            .collect()
    }

    fn tier(name: &str, width: i64, height: i64) -> (String, i64, i64) {
        (name.to_string(), width, height)
    }

    // The ladder on the new-item path, and its write-once discipline. A
    // 1400x1400 JPEG is inside every bound of its class, so the original
    // serves the display tier and nothing is stored for it — and it is past
    // 1.25x all three grid tiers, so all three are. That is the shape most of
    // a photo library is in.
    #[tokio::test]
    async fn a_scanned_image_stores_its_grid_tiers_exactly_once() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-tier-new"]).await;
        image::RgbImage::new(1400, 1400)
            .save(env.media_dirs[0].join("photo.jpg"))
            .unwrap();

        env.scan().await;
        let mut conn = env.read().await;
        assert_eq!(
            thumbnail_count(&mut conn).await,
            0,
            "the display tier serves this original"
        );
        assert_eq!(
            tier_rows(&mut conn).await,
            vec![
                tier("grid-m", 1024, 1024),
                tier("grid-s", 512, 512),
                tier("grid-xs", 256, 256)
            ]
        );
        let ids = tier_ids(&mut conn).await;
        drop(conn);

        // The rescan reaches the file and concludes there is nothing to do —
        // from indexed dimensions and stored geometry, with no decode.
        let (_, totals) = env.scan().await;
        assert_eq!(totals.unchanged_files, 1, "the walk did reach the file");
        assert_eq!(
            (totals.backfilled_visuals, totals.visuals_suppressed),
            (0, 0),
            "an item with the renditions it wants is invisible to the dispatcher"
        );
        let mut conn = env.read().await;
        assert_eq!(
            tier_ids(&mut conn).await,
            ids,
            "the rescan rewrote tiers it already had"
        );
    }

    // A library indexed before the ladder existed: display renditions decided
    // by the old rule, no tiers at all. One scan brings it up to date, and the
    // next does nothing — the same convergence the animation and rotation
    // questions have.
    #[tokio::test]
    async fn an_existing_library_backfills_its_grid_tiers() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-tier-backfill"]).await;
        image::RgbImage::new(1400, 1400)
            .save(env.media_dirs[0].join("photo.jpg"))
            .unwrap();
        env.scan().await;

        {
            let mut conn = env.write().await;
            sqlx::query("DELETE FROM storage.thumbnail_tiers")
                .execute(&mut conn)
                .await
                .unwrap();
        }

        let (_, totals) = env.scan().await;
        assert_eq!(totals.unchanged_files, 1);
        assert_eq!(
            totals.backfilled_visuals, 1,
            "the ladder question dispatched the file"
        );
        let mut conn = env.read().await;
        assert_eq!(
            tier_rows(&mut conn).await,
            vec![
                tier("grid-m", 1024, 1024),
                tier("grid-s", 512, 512),
                tier("grid-xs", 256, 256)
            ]
        );
        let ids = tier_ids(&mut conn).await;
        drop(conn);

        let (_, totals) = env.scan().await;
        assert_eq!(
            totals.backfilled_visuals, 0,
            "and settles: the second rescan writes nothing"
        );
        let mut conn = env.read().await;
        assert_eq!(tier_ids(&mut conn).await, ids);
    }

    // The display rule is short-side based now, so an item it serves from its
    // original can be carrying a rendition the old long-side rule stored — a
    // webtoon crushed to 163x4096, which the serving path would go on
    // preferring to the original forever. The ladder retires it.
    #[tokio::test]
    async fn a_superseded_display_rendition_is_dropped() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-tier-superseded"]).await;
        // A 900x3000 JPEG: every bound of its class clear, so the original
        // serves. A 10:3 strip, so every grid tier is a top crop.
        image::RgbImage::new(900, 3000)
            .save(env.media_dirs[0].join("strip.jpg"))
            .unwrap();
        env.scan().await;

        let sha256: String = {
            let mut conn = env.read().await;
            assert_eq!(thumbnail_count(&mut conn).await, 0);
            assert_eq!(
                tier_rows(&mut conn).await,
                vec![
                    tier("grid-m", 900, 2048),
                    tier("grid-s", 512, 1024),
                    tier("grid-xs", 256, 512)
                ]
            );
            sqlx::query_scalar("SELECT sha256 FROM items")
                .fetch_one(&mut conn)
                .await
                .unwrap()
        };

        // What the old rule left behind, planted by hand: a long-side-fitted
        // rendition of a strip.
        let stale = encode_generated_still(0, &DynamicImage::ImageRgb8(image::RgbImage::new(163, 4096)))
            .expect("a 163x4096 image encodes");
        call_index_db_writer(&env.index_db, |reply| {
            IndexDbWriterMessage::StoreThumbnails {
                sha256: sha256.clone(),
                mime_type: "image/bmp".to_string(),
                process_version: THUMBNAIL_PROCESS_VERSION,
                thumbnails: vec![stale.clone()],
                reply,
            }
        })
        .await
        .unwrap();

        let (_, totals) = env.scan().await;
        assert_eq!(totals.backfilled_visuals, 1);
        let mut conn = env.read().await;
        assert_eq!(
            thumbnail_count(&mut conn).await,
            0,
            "the rendition the current rule does not want is gone"
        );
        assert_eq!(
            tier_rows(&mut conn).await,
            vec![
                tier("grid-m", 900, 2048),
                tier("grid-s", 512, 1024),
                tier("grid-xs", 256, 512)
            ]
        );
        let ids = tier_ids(&mut conn).await;
        drop(conn);

        let (_, totals) = env.scan().await;
        assert_eq!(
            totals.backfilled_visuals, 0,
            "and the drop settles rather than repeating every scan"
        );
        let mut conn = env.read().await;
        assert_eq!(tier_ids(&mut conn).await, ids);
    }


    /// The library-wide upgrade this release costs, in **one pass per image**.
    ///
    /// The pre-upgrade state is all three changes at once: a display rendition
    /// stored as JPEG where the rule now wants WebP, a tier set at the
    /// superseded generator version and without `grid-xs`, and
    /// `has_transparency` unexamined. One scan converges every one of them,
    /// and the next writes nothing — which is what makes the whole thing a
    /// single decode of each file rather than three.
    #[tokio::test]
    async fn the_format_upgrade_converges_in_one_pass_per_image() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-format-upgrade"]).await;
        // 1400x1400 uncompressed is 5.88 MB: past the lossless class's 2 MiB
        // byte bound, so this item owes a display rendition, and past 1.25x
        // every grid tier, so it owes all three of those too.
        image::RgbImage::new(1400, 1400)
            .save(env.media_dirs[0].join("art.bmp"))
            .unwrap();
        env.scan().await;

        {
            let mut conn = env.read().await;
            assert_eq!(
                display_media_types(&mut conn).await,
                vec!["image/webp".to_string()],
                "a lossless source's display rendition is a WebP"
            );
            assert_eq!(transparency_flags(&mut conn).await, vec![Some(0)]);
        }

        // Rewound to what a pre-upgrade library holds.
        {
            let mut conn = env.write().await;
            sqlx::query("UPDATE storage.thumbnails SET media_type = 'image/jpeg'")
                .execute(&mut conn)
                .await
                .unwrap();
            sqlx::query("DELETE FROM storage.thumbnail_tiers WHERE tier = 'grid-xs'")
                .execute(&mut conn)
                .await
                .unwrap();
            sqlx::query("UPDATE storage.thumbnail_tiers SET version = ?1")
                .bind(TIER_PROCESS_VERSION - 1)
                .execute(&mut conn)
                .await
                .unwrap();
            sqlx::query("UPDATE items SET has_transparency = NULL")
                .execute(&mut conn)
                .await
                .unwrap();
        }

        let (_, totals) = env.scan().await;
        assert_eq!(
            totals.backfilled_visuals, 1,
            "one dispatch, and therefore one decode, for all three changes"
        );
        {
            let mut conn = env.read().await;
            assert_eq!(
                display_media_types(&mut conn).await,
                vec!["image/webp".to_string()]
            );
            assert_eq!(
                tier_rows(&mut conn).await,
                vec![
                    tier("grid-m", 1024, 1024),
                    tier("grid-s", 512, 512),
                    tier("grid-xs", 256, 256)
                ]
            );
            assert_eq!(
                tier_versions(&mut conn).await,
                vec![TIER_PROCESS_VERSION; 3]
            );
            assert_eq!(transparency_flags(&mut conn).await, vec![Some(0)]);
        }

        let (_, totals) = env.scan().await;
        assert_eq!(
            totals.backfilled_visuals, 0,
            "and it settles: nothing is rewritten on the next scan"
        );
    }

    /// R4 end to end: the measurement is taken from pixels, written once, and
    /// only moves a rendition when it actually changes the verdict.
    #[tokio::test]
    async fn transparency_is_measured_once_and_only_rewrites_on_a_changed_verdict() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-transparency"]).await;
        alpha_logo(1400).save(env.media_dirs[0].join("logo.png")).unwrap();
        env.scan().await;

        {
            let mut conn = env.read().await;
            assert_eq!(transparency_flags(&mut conn).await, vec![Some(1)]);
            assert!(
                tier_media_types(&mut conn)
                    .await
                    .iter()
                    .all(|(_, media)| media == "image/webp"),
                "every rendition of a transparent item is a WebP with alpha"
            );
        }

        // The write is guarded on `has_transparency IS NULL`, so a scan over
        // an examined item never touches the column — and, the column being
        // unchanged, never rewrites a rendition either.
        let ids = {
            let mut conn = env.read().await;
            tier_ids(&mut conn).await
        };
        let (_, totals) = env.scan().await;
        assert_eq!(totals.backfilled_visuals, 0);
        {
            let mut conn = env.read().await;
            assert_eq!(tier_ids(&mut conn).await, ids);
        }

        // Unexamined again: the measurement is re-taken, reaches the same
        // answer, and the verdict it feeds is unchanged — so nothing is
        // rewritten. The rowids are the assertion that matters: the tier
        // write is a whole-set delete and insert, so a set re-emitted for an
        // unchanged verdict would show up here even though every byte in it
        // is identical.
        {
            let mut conn = env.write().await;
            sqlx::query("UPDATE items SET has_transparency = NULL")
                .execute(&mut conn)
                .await
                .unwrap();
        }
        env.scan().await;
        {
            let mut conn = env.read().await;
            assert_eq!(transparency_flags(&mut conn).await, vec![Some(1)]);
            assert!(
                tier_media_types(&mut conn)
                    .await
                    .iter()
                    .all(|(_, media)| media == "image/webp")
            );
            assert_eq!(
                tier_ids(&mut conn).await,
                ids,
                "a re-measurement that changed no verdict rewrote the set"
            );
        }
        let (_, totals) = env.scan().await;
        assert_eq!(totals.backfilled_visuals, 0, "and it settles");

        // The other direction, planted by hand: a stored verdict of "opaque"
        // moves every rendition to JPEG, and the generator follows the
        // *column* rather than its own decode — which is what stops the two
        // disagreeing forever.
        {
            let mut conn = env.write().await;
            sqlx::query("UPDATE items SET has_transparency = 0")
                .execute(&mut conn)
                .await
                .unwrap();
        }
        let (_, totals) = env.scan().await;
        assert_eq!(totals.backfilled_visuals, 1, "the verdict changed");
        {
            let mut conn = env.read().await;
            assert!(
                tier_media_types(&mut conn)
                    .await
                    .iter()
                    .all(|(_, media)| media == "image/jpeg")
            );
            assert_eq!(
                transparency_flags(&mut conn).await,
                vec![Some(0)],
                "the pass measured 1 and wrote nothing: the column is written once"
            );
        }
        let (_, totals) = env.scan().await;
        assert_eq!(totals.backfilled_visuals, 0, "and it settles");
    }

    /// R5: a policy edit regenerates exactly the rows whose format it moves,
    /// and leaves every other item's set alone.
    #[tokio::test]
    async fn a_policy_flip_regenerates_only_the_rows_it_moves() {
        let test_env = test_data_dir();
        let mut env = visuals_env(test_env.path(), &["media-policy"]).await;
        alpha_logo(1400).save(env.media_dirs[0].join("logo.png")).unwrap();
        image::RgbImage::new(1400, 1400)
            .save(env.media_dirs[0].join("photo.jpg"))
            .unwrap();
        env.scan().await;

        let (opaque_sha, opaque_ids) = {
            let mut conn = env.read().await;
            let sha: String = sqlx::query_scalar(
                "SELECT sha256 FROM items WHERE has_transparency = 0 LIMIT 1",
            )
            .fetch_one(&mut conn)
            .await
            .unwrap();
            let ids: Vec<i64> = sqlx::query_scalar(
                "SELECT id FROM storage.thumbnail_tiers WHERE item_sha256 = ?1 ORDER BY id",
            )
            .bind(&sha)
            .fetch_all(&mut conn)
            .await
            .unwrap();
            assert_eq!(ids.len(), 3, "the opaque item's three JPEG tiers");
            (sha, ids)
        };

        // The storage-constrained deployment's opposite: `webp` withdrawn, so
        // every WebP verdict becomes a flattened JPEG.
        env.config.thumbnail_formats = vec!["jpeg".to_string()];
        SystemConfigStore::new(env.root.clone())
            .save(&env.index_db, &env.config)
            .unwrap();

        let (_, totals) = env.scan().await;
        assert_eq!(
            totals.backfilled_visuals, 1,
            "only the item whose rows the policy moves is dispatched"
        );
        {
            let mut conn = env.read().await;
            assert!(
                tier_media_types(&mut conn)
                    .await
                    .iter()
                    .all(|(_, media)| media == "image/jpeg"),
                "with webp absent the alpha is flattened, as before the feature"
            );
            let ids: Vec<i64> = sqlx::query_scalar(
                "SELECT id FROM storage.thumbnail_tiers WHERE item_sha256 = ?1 ORDER BY id",
            )
            .bind(&opaque_sha)
            .fetch_all(&mut conn)
            .await
            .unwrap();
            assert_eq!(ids, opaque_ids, "the already-JPEG item is untouched");
        }

        let (_, totals) = env.scan().await;
        assert_eq!(totals.backfilled_visuals, 0, "and it settles");
    }

    /// A **JPEG** item's JPEG display rendition still follows the policy.
    ///
    /// The regression this pins: the dispatcher used to accept any display row
    /// whose media type was the item's own, as a stand-in for recognising the
    /// keep-the-original sentinel. For every JPEG source with a real JPEG
    /// rendition — most of a photo library — that made the display row match
    /// *every* verdict, so a `thumbnail_formats` edit moved the grid tiers and
    /// silently left the display rendition on the old format forever.
    #[tokio::test]
    async fn a_jpeg_display_rendition_follows_the_format_policy() {
        let test_env = test_data_dir();
        let mut env = visuals_env(test_env.path(), &["media-jpeg-display"]).await;
        // Past the 4096 short-side trigger, so a rendition is owed; capped at
        // 2560, so it is a real downscale the encode comfortably wins.
        image::RgbImage::from_fn(4100, 4100, |x, y| {
            image::Rgb([(x / 16) as u8, (y / 16) as u8, 128])
        })
        .save(env.media_dirs[0].join("big.jpg"))
        .unwrap();
        env.scan().await;

        {
            let mut conn = env.read().await;
            assert_eq!(
                display_media_types(&mut conn).await,
                vec!["image/jpeg".to_string()],
                "a JPEG source keeps JPEG: a WebP of one decodes 2.33x slower"
            );
            assert!(
                display_has_bytes(&mut conn).await,
                "the premise: a real rendition, not the keep-the-original verdict"
            );
        }

        // The storage-constrained deployment: `jpeg` withdrawn, so every JPEG
        // verdict — the display rendition included — becomes a WebP.
        env.config.thumbnail_formats = vec!["webp".to_string()];
        SystemConfigStore::new(env.root.clone())
            .save(&env.index_db, &env.config)
            .unwrap();

        let (_, totals) = env.scan().await;
        assert_eq!(totals.backfilled_visuals, 1);
        {
            let mut conn = env.read().await;
            assert_eq!(
                display_media_types(&mut conn).await,
                vec!["image/webp".to_string()],
                "the display row moves with the policy, not only the grid tiers"
            );
        }
        let (_, totals) = env.scan().await;
        assert_eq!(totals.backfilled_visuals, 0, "and it settles");

        // ... and back, which is the half a one-way comparison can pass by
        // accident.
        env.config.thumbnail_formats = SystemConfig::default().thumbnail_formats;
        SystemConfigStore::new(env.root.clone())
            .save(&env.index_db, &env.config)
            .unwrap();
        let (_, totals) = env.scan().await;
        assert_eq!(totals.backfilled_visuals, 1);
        {
            let mut conn = env.read().await;
            assert_eq!(
                display_media_types(&mut conn).await,
                vec!["image/jpeg".to_string()]
            );
        }
        let (_, totals) = env.scan().await;
        assert_eq!(totals.backfilled_visuals, 0, "and it settles again");
    }

    /// A display **sentinel** is an answer about one encoder, not about the
    /// item forever.
    ///
    /// Two halves. First the row a pre-fix build wrote — empty bytes carrying
    /// the *source's* mime type — which the dispatcher used to read as "this
    /// item is settled" under every verdict there is: a database that visited
    /// `["jpeg"]` once, where a flattened JPEG of a transparent PNG need not
    /// come out comfortably smaller, went on serving the multi-megabyte
    /// original after the policy was put back. It has to be retried exactly
    /// once and replaced by the WebP the current rule wants.
    ///
    /// Then the row this build writes, which names the format it attempted:
    /// honoured while that format is still the verdict, re-attempted the
    /// moment the verdict moves. (The writer's half of that convention is
    /// `visuals::tests::a_display_sentinel_names_the_format_it_attempted`.)
    #[tokio::test]
    async fn a_display_sentinel_is_retried_when_the_format_verdict_moves() {
        let test_env = test_data_dir();
        let mut env = visuals_env(test_env.path(), &["media-sentinel-retry"]).await;
        // Past the lossless class's 2 MiB bound, so a rendition is genuinely
        // owed; the alpha corner makes it WebP by R4 as well as by class.
        noise_rgba(1400)
            .save(env.media_dirs[0].join("logo.png"))
            .unwrap();
        env.scan().await;

        {
            let mut conn = env.read().await;
            assert_eq!(
                display_media_types(&mut conn).await,
                vec!["image/webp".to_string()]
            );
            assert!(display_has_bytes(&mut conn).await);
        }

        // Rewound to what a pre-fix build left behind: the keep-the-original
        // verdict, wearing the item's own type.
        {
            let mut conn = env.write().await;
            sqlx::query(
                "UPDATE storage.thumbnails SET thumbnail = X'', media_type = 'image/png'",
            )
            .execute(&mut conn)
            .await
            .unwrap();
        }
        let (_, totals) = env.scan().await;
        assert_eq!(
            totals.backfilled_visuals, 1,
            "a verdict about a format nobody can name is no verdict at all"
        );
        {
            let mut conn = env.read().await;
            assert_eq!(
                display_media_types(&mut conn).await,
                vec!["image/webp".to_string()]
            );
            assert!(
                display_has_bytes(&mut conn).await,
                "the rendition is restored rather than the original served forever"
            );
        }
        let (_, totals) = env.scan().await;
        assert_eq!(totals.backfilled_visuals, 0, "and it settles");

        // The row this build writes: the same verdict, naming the encoder that
        // reached it. Unchanged policy, so it is final and nothing is asked
        // for again.
        //
        // Planted rather than provoked: reaching this state honestly needs a
        // source whose WebP re-encode comes out over three quarters of its
        // bytes, which is a property of a real photograph's entropy and not
        // of anything a fixture can be written to have. The verdict is what
        // the test is about; how the encoder arrived at it is not.
        {
            let mut conn = env.write().await;
            sqlx::query("UPDATE storage.thumbnails SET thumbnail = X''")
                .execute(&mut conn)
                .await
                .unwrap();
        }
        let (_, totals) = env.scan().await;
        assert_eq!(
            totals.backfilled_visuals, 0,
            "a sentinel naming the current verdict is an answer, not a stale row"
        );
        {
            let mut conn = env.read().await;
            assert!(!display_has_bytes(&mut conn).await, "still the sentinel");
        }

        // Move the verdict and it is re-attempted, which is the whole
        // difference between "this encoder could not win" and "nothing can".
        env.config.thumbnail_formats = vec!["jpeg".to_string()];
        SystemConfigStore::new(env.root.clone())
            .save(&env.index_db, &env.config)
            .unwrap();
        let (_, totals) = env.scan().await;
        assert_eq!(totals.backfilled_visuals, 1);
        {
            let mut conn = env.read().await;
            assert_eq!(
                display_media_types(&mut conn).await,
                vec!["image/jpeg".to_string()],
                "the other encoder gets its turn"
            );
        }
        let (_, totals) = env.scan().await;
        assert_eq!(totals.backfilled_visuals, 0, "and it settles");
    }

    /// An image nothing can decode still **answers** the transparency
    /// question, rather than asking it forever.
    ///
    /// Its header reads, so it is indexed and `has_transparency` starts NULL;
    /// its pixels do not, so the one pass that could measure them is exactly
    /// the pass the negative cache exists to stop running. Without a terminal
    /// verdict the column stays NULL, the partial index never drains, and
    /// every scan for the rest of this file's life dispatches a visuals task
    /// with nothing in it — the Wave-5 rotation bug, one column over.
    #[tokio::test]
    async fn an_undecodable_image_settles_the_transparency_question() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-undecodable"]).await;
        // A real PNG header over pixel data that stops mid-stream: the
        // metadata phase reads the dimensions out of IHDR and indexes the
        // file, and every decode after that fails. (The indexing gate is a
        // header read on purpose — docs/failed-media-retry-design.md, "Scan
        // policy for undecodable images".)
        let mut encoded = Vec::new();
        image::RgbImage::from_fn(1400, 1400, |x, y| {
            image::Rgb([(x / 8) as u8, (y / 8) as u8, 40])
        })
        .write_to(
            &mut std::io::Cursor::new(&mut encoded),
            image::ImageFormat::Png,
        )
        .unwrap();
        encoded.truncate(200);
        fs::write(env.media_dirs[0].join("truncated.png"), &encoded).unwrap();

        // This item's own column, by path: the shared scan root is not this
        // test's alone, and the question is about one file.
        async fn flag(conn: &mut sqlx::SqliteConnection) -> Option<i64> {
            sqlx::query_scalar(
                "SELECT i.has_transparency FROM items i \
                 JOIN files f ON f.sha256 = i.sha256 \
                 WHERE f.path LIKE '%truncated.png'",
            )
            .fetch_one(conn)
            .await
            .unwrap()
        }

        // The ledger takes as many attempts to settle as the failure's class
        // is worth — what matters is that the verdict lands at all, and that
        // the marker's arrival is what lands it.
        let mut settled = None;
        for scan in 1..=6 {
            env.scan().await;
            let mut conn = env.read().await;
            let measured = flag(&mut conn).await;
            drop(conn);
            if measured.is_some() {
                settled = Some((scan, measured));
                break;
            }
        }
        assert_eq!(
            settled.map(|(_, measured)| measured),
            Some(Some(0)),
            "a picture nobody can decode has no rendition for anything to be \
             transparent in — without that verdict the partial index never \
             drains and every scan dispatches a pass that cannot answer"
        );

        // Written once and never revisited: the item owes nothing more, and
        // the marker keeps the decode from being attempted again. The marker
        // is half the claim — without one there is nothing for the next
        // scan's consult to find, and the verdict above would be re-derived
        // by a fresh decode every time.
        let (_, totals) = env.scan().await;
        assert_eq!(totals.backfilled_visuals, 0);
        let mut conn = env.read().await;
        assert_eq!(flag(&mut conn).await, Some(0));
        assert!(
            visual_attempt_rows(&mut conn)
                .await
                .iter()
                .any(|(kind, _, _, _, _)| kind == "thumbnail"),
            "the settle rides a marker; without one the decode is retried forever"
        );
    }

    /// Whether every stored display rendition carries bytes — the difference
    /// between a picture and the keep-the-original verdict.
    async fn display_has_bytes(conn: &mut sqlx::SqliteConnection) -> bool {
        let rows = display_snapshot(conn).await;
        !rows.is_empty() && rows.iter().all(|row| !row.thumbnail.is_empty())
    }

    /// The rowids of the stored H.264 loop rows.
    ///
    /// A rewritten set deletes and re-inserts, so a *new* rowid is a
    /// re-encode however the bytes compare — which is the property the
    /// retained payload exists for, and the one a byte comparison alone would
    /// miss for a deterministic encoder.
    async fn loop_tier_ids(conn: &mut sqlx::SqliteConnection) -> Vec<i64> {
        tier_snapshot(conn)
            .await
            .into_iter()
            .filter(|row| row.tier.starts_with("loop"))
            .map(|row| row.id)
            .collect()
    }

    /// The whole point of `LOOP_PROCESS_VERSION` being separate: a
    /// still-encoder bump regenerates every poster in the library and re-runs
    /// ffmpeg over none of it.
    ///
    /// Proven twice over: bytes no encoder would produce are planted in the
    /// loop row, and the row's identity is watched. Both matter — the bytes
    /// catch an encode, the rowid catches a rewrite that would have produced
    /// the same bytes.
    #[tokio::test]
    async fn a_tier_version_bump_regenerates_posters_and_never_the_loop() {
        if !crate::media_tools::ffmpeg_available() {
            return;
        }
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-loop-version"]).await;
        write_animated_gif(
            &env.media_dirs[0].join("clip.gif"),
            1400,
            2,
            100,
            |x, y, index| {
                let shade = (index as u8).wrapping_mul(90);
                image::Rgba([shade, (x / 64) as u8, (y / 64) as u8, 255])
            },
        );
        env.scan().await;

        const PLANTED: &[u8] = b"not an mp4, planted";
        let loop_ids = {
            let mut conn = env.read().await;
            let ids = loop_tier_ids(&mut conn).await;
            assert!(!ids.is_empty(), "the premise: this item is on the animated ladder");
            ids
        };
        {
            let mut conn = env.write().await;
            // Every still row rewound to the superseded generator; the loop
            // rows left exactly as the current one wrote them.
            sqlx::query("UPDATE storage.thumbnail_tiers SET version = ?1 WHERE tier LIKE 'grid-%'")
                .bind(TIER_PROCESS_VERSION - 1)
                .execute(&mut conn)
                .await
                .unwrap();
            sqlx::query("UPDATE storage.thumbnail_tiers SET thumbnail = ?1 WHERE tier LIKE 'loop%'")
                .bind(PLANTED.to_vec())
                .execute(&mut conn)
                .await
                .unwrap();
        }

        let (_, totals) = env.scan().await;
        assert_eq!(totals.backfilled_visuals, 1);
        {
            let mut conn = env.read().await;
            let rows: Vec<(String, i64, Vec<u8>)> = sqlx::query_as(
                "SELECT tier, version, thumbnail FROM storage.thumbnail_tiers ORDER BY tier",
            )
            .fetch_all(&mut conn)
            .await
            .unwrap();
            let is_loop = |tier: &str| {
                [RenditionKind::Loop, RenditionKind::LoopDisplay]
                    .iter()
                    .any(|kind| kind.as_str() == tier)
            };
            for (tier, version, bytes) in &rows {
                if is_loop(tier) {
                    assert_eq!(
                        bytes.as_slice(),
                        PLANTED,
                        "{tier} was re-encoded when it should have been reused"
                    );
                    assert_eq!(*version, LOOP_PROCESS_VERSION);
                } else {
                    assert_eq!(*version, TIER_PROCESS_VERSION, "{tier} restamped");
                    assert!(!bytes.is_empty(), "{tier} carries a real picture");
                }
            }
            assert_eq!(
                loop_tier_ids(&mut conn).await,
                loop_ids,
                "the loop rows were rewritten, not retained"
            );
        }
        let bumped_ids = {
            let mut conn = env.read().await;
            tier_ids(&mut conn).await
        };

        let (_, totals) = env.scan().await;
        assert_eq!(totals.backfilled_visuals, 0, "and it settles");

        // The other pass that reaches `build_animated_tiers` without ever
        // being `TierWork::Animated`: the transparency question, which decodes
        // for one column and used to arrive with no loops to reuse at all.
        {
            let mut conn = env.write().await;
            sqlx::query("UPDATE items SET has_transparency = NULL")
                .execute(&mut conn)
                .await
                .unwrap();
        }
        env.scan().await;
        {
            let mut conn = env.read().await;
            assert_eq!(
                loop_tier_ids(&mut conn).await,
                loop_ids,
                "a transparency pass re-ran ffmpeg over an already correct loop"
            );
            assert_eq!(
                tier_ids(&mut conn).await,
                bumped_ids,
                "a re-measurement that changed no verdict rewrote the set"
            );
            let planted: Vec<Vec<u8>> = sqlx::query_scalar(
                "SELECT thumbnail FROM storage.thumbnail_tiers WHERE tier LIKE 'loop%'",
            )
            .fetch_all(&mut conn)
            .await
            .unwrap();
            assert!(planted.iter().all(|bytes| bytes.as_slice() == PLANTED));
        }
    }

    /// `items.has_transparency`, in `items` order.
    async fn transparency_flags(conn: &mut sqlx::SqliteConnection) -> Vec<Option<i64>> {
        sqlx::query_scalar("SELECT has_transparency FROM items ORDER BY id")
            .fetch_all(conn)
            .await
            .unwrap()
    }

    /// The media type of every stored display rendition, by index.
    async fn display_media_types(conn: &mut sqlx::SqliteConnection) -> Vec<String> {
        display_snapshot(conn)
            .await
            .into_iter()
            .map(|row| row.media_type)
            .collect()
    }

    /// The `version` stamp on every tier row.
    async fn tier_versions(conn: &mut sqlx::SqliteConnection) -> Vec<i64> {
        tier_snapshot(conn)
            .await
            .into_iter()
            .map(|row| row.version)
            .collect()
    }

    // A generator change the stored geometry cannot see — a different crop
    // anchor, filter or quality — is what `TIER_PROCESS_VERSION` exists for.
    // Without the version in the comparison the column would be written and
    // never read, and nothing could ever trigger such a regeneration.
    #[tokio::test]
    async fn a_stale_tier_version_is_regenerated() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-tier-version"]).await;
        image::RgbImage::new(1400, 1400)
            .save(env.media_dirs[0].join("photo.jpg"))
            .unwrap();
        env.scan().await;

        let wanted = vec![
            tier("grid-m", 1024, 1024),
            tier("grid-s", 512, 512),
            tier("grid-xs", 256, 256),
        ];
        {
            let mut conn = env.read().await;
            assert_eq!(tier_rows(&mut conn).await, wanted);
            assert_eq!(
                tier_versions(&mut conn).await,
                vec![TIER_PROCESS_VERSION; 3]
            );
        }

        // The state a version bump leaves behind: identical geometry, an
        // older stamp.
        {
            let mut conn = env.write().await;
            sqlx::query("UPDATE storage.thumbnail_tiers SET version = ?1")
                .bind(TIER_PROCESS_VERSION - 1)
                .execute(&mut conn)
                .await
                .unwrap();
        }

        let (_, totals) = env.scan().await;
        assert_eq!(
            totals.backfilled_visuals, 1,
            "an out-of-date stamp is work even when the geometry matches"
        );
        let fresh_ids = {
            let mut conn = env.read().await;
            assert_eq!(tier_rows(&mut conn).await, wanted, "same geometry");
            // The restamp is the proof the set was rewritten. Not the rowids:
            // SQLite hands back the same `INTEGER PRIMARY KEY` values after a
            // delete that empties the table, so they are only evidence in the
            // *negative* direction (an unchanged list next to a zero write
            // count).
            assert_eq!(
                tier_versions(&mut conn).await,
                vec![TIER_PROCESS_VERSION; 3],
                "restamped at the current version"
            );
            assert_eq!(
                thumbnail_count(&mut conn).await,
                0,
                "a tier regeneration does not invent a display rendition"
            );
            tier_ids(&mut conn).await
        };

        let (_, totals) = env.scan().await;
        assert_eq!(
            totals.backfilled_visuals, 0,
            "and it settles: a matching stamp is not work"
        );
        let mut conn = env.read().await;
        assert_eq!(tier_ids(&mut conn).await, fresh_ids);
    }

    // The ordering window, and what closes it. The ladder question runs
    // before the animation question that stamps `items.duration`, so a scan
    // meeting an item indexed without that measurement sees `duration IS
    // NULL`, concludes "still", and writes static tiers for content the very
    // same scan then records as animated. Those stills are wrong — an
    // animated item's grid rendition is a loop — and being stored renditions
    // they are served immutably, which makes "frozen forever" a real outcome
    // rather than a cosmetic one.
    //
    // The post-window state is reproduced exactly: tiers written by a scan
    // that believed the item still, and a positive `duration` recorded after
    // them. A `duration` is a `duration` — the animated verdict is a
    // predicate on the column, not on the container — so a BMP carrying one
    // exercises the identical path an animated WebP does, deterministically
    // and without an encoder.
    #[tokio::test]
    async fn a_stale_static_tier_set_is_rebuilt_once_the_item_is_known_animated() {
        if !crate::media_tools::ffmpeg_available() {
            return;
        }
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-tier-animated"]).await;
        image::RgbImage::new(1400, 1400)
            .save(env.media_dirs[0].join("loop.bmp"))
            .unwrap();
        env.scan().await;

        {
            let mut conn = env.read().await;
            assert_eq!(
                tier_rows(&mut conn).await,
                vec![
                    tier("grid-m", 1024, 1024),
                    tier("grid-s", 512, 512),
                    tier("grid-xs", 256, 256)
                ],
                "the premise: a scan that believed this item still wrote stills"
            );
        }

        // What the animation question records once it runs.
        {
            let mut conn = env.write().await;
            sqlx::query("UPDATE items SET duration = 3.5")
                .execute(&mut conn)
                .await
                .unwrap();
        }

        let (_, totals) = env.scan().await;
        assert_eq!(
            totals.backfilled_visuals, 1,
            "the ladder question is still asked for an animated item"
        );
        let mut conn = env.read().await;
        // 5.88 MB uncompressed at 1400x1400: past the raw floor on both
        // clauses, so this item wants the animated ladder — posters at the
        // same geometry the stills had, the one loop that answers every grid
        // tier, and (past the animated class's 5 MiB display bound, with a
        // grid loop that is downscaled rather than native) a display loop.
        assert_eq!(
            tier_rows(&mut conn).await,
            vec![
                tier("grid-m", 1024, 1024),
                tier("grid-s", 512, 512),
                tier("grid-xs", 256, 256),
                tier(LOOP_TIER, 1024, 1024),
                tier(crate::visual_tiers::LOOP_DISPLAY_TIER, 1400, 1400),
            ],
            "the stale still set is replaced by the animated one"
        );
        assert_eq!(
            tier_media_types(&mut conn).await,
            vec![
                ("grid-m".to_string(), "image/jpeg".to_string()),
                ("grid-s".to_string(), "image/jpeg".to_string()),
                ("grid-xs".to_string(), "image/jpeg".to_string()),
                (LOOP_TIER.to_string(), LOOP_MEDIA_TYPE.to_string()),
                (
                    crate::visual_tiers::LOOP_DISPLAY_TIER.to_string(),
                    LOOP_MEDIA_TYPE.to_string(),
                ),
            ],
            "an uncompressed 5.88 MB source dwarfs its H.264 encode, so the \
             loop is stored rather than the keep-the-original verdict"
        );
        assert_eq!(
            thumbnail_count(&mut conn).await,
            0,
            "and the display half of the ladder is not touched"
        );
        drop(conn);

        let (_, totals) = env.scan().await;
        assert_eq!(
            totals.backfilled_visuals, 0,
            "and it settles: the animated set is the one the ladder wants"
        );
        let mut conn = env.read().await;
        assert_eq!(tier_rows(&mut conn).await.len(), 5);
    }

    // The raw floor's other side: an animated item small enough to serve as
    // its own file wants *nothing* stored, and a set an older rule left
    // behind is retired rather than frozen. A GIF is animated by mime, so
    // this isolates the floor itself — no duration measurement, no encoder,
    // and no still-ladder verdict that could reach the same answer by
    // accident.
    #[tokio::test]
    async fn a_raw_floor_animated_item_retires_whatever_it_carries() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-tier-floor"]).await;
        let gif = env.media_dirs[0].join("tiny.gif");
        image::RgbImage::from_fn(200, 200, |x, y| {
            image::Rgb([(x % 256) as u8, (y % 256) as u8, 0])
        })
        .save(&gif)
        .unwrap();
        assert!(
            fs::metadata(&gif).unwrap().len() <= 1024 * 1024,
            "the fixture has to be under the floor's byte clause"
        );
        env.scan().await;

        {
            let mut conn = env.read().await;
            assert!(
                tier_rows(&mut conn).await.is_empty(),
                "nothing is stored for an item under the raw floor"
            );
        }

        // A set from an older rule, planted the way an upgrade leaves one.
        {
            let mut conn = env.write().await;
            let sha: String = sqlx::query_scalar("SELECT sha256 FROM items LIMIT 1")
                .fetch_one(&mut conn)
                .await
                .unwrap();
            sqlx::query(
                r#"
INSERT INTO storage.thumbnail_tiers (
    item_sha256, idx, tier, item_mime_type, media_type, width, height, version, thumbnail
)
VALUES (?1, 0, 'grid-m', 'image/gif', 'image/jpeg', 200, 200, ?2, X'00')
                "#,
            )
            .bind(&sha)
            .bind(TIER_PROCESS_VERSION)
            .execute(&mut conn)
            .await
            .unwrap();
        }

        let (_, totals) = env.scan().await;
        assert_eq!(totals.backfilled_visuals, 1);
        let mut conn = env.read().await;
        assert!(
            tier_rows(&mut conn).await.is_empty(),
            "an item under the raw floor wants no rendition at all"
        );
        drop(conn);

        let (_, totals) = env.scan().await;
        assert_eq!(
            totals.backfilled_visuals, 0,
            "and it settles: with nothing stored there is nothing to retire"
        );
    }

    /// The reason the loop has a kind of its own: a loop failure retires the
    /// animated ladder and **nothing else**. The dispatcher consults the two
    /// markers separately, so the thumbnail consult is untouched — which is
    /// what keeps a file ffmpeg cannot encode eligible for the still ladder
    /// the moment the animation question reclassifies it.
    ///
    /// The item is deliberately over the display loop trigger (R3), so its
    /// wanted set holds both loop rows: the `display` answer for a moving
    /// picture is its own file or a loop, never a stored still.
    #[tokio::test]
    async fn a_loop_marker_retires_the_ladder_and_nothing_else() {
        if !crate::media_tools::ffmpeg_available() {
            return;
        }
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-loop-marker"]).await;
        let path = env.media_dirs[0].join("clip.gif");
        write_animated_gif(&path, 900, 2, 100, |x, y, index| {
            let shade = (index as u8).wrapping_mul(90);
            image::Rgba([shade, (x / 32) as u8, (y / 32) as u8, 255])
        });
        // Past the animated class's byte bound, so this item's display answer
        // is a loop of its own (R3) rather than its file. GIF decoders stop at
        // the trailer, so padding changes the file's size and nothing else
        // about it; both the dispatcher and the generator read the same
        // `image_file_size`, so they agree.
        {
            use std::io::Write;
            let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&vec![
                0_u8;
                (crate::visual_tiers::DISPLAY_MAX_FILE_SIZE_ANIMATED
                    + 1) as usize
            ])
            .unwrap();
        }
        env.scan().await;
        let sha: String = {
            let mut conn = env.read().await;
            assert_eq!(
                thumbnail_count(&mut conn).await,
                0,
                "a moving picture never stores a still display rendition"
            );
            let stored = tier_rows(&mut conn).await;
            assert!(
                stored.iter().any(|(tier, _, _)| tier == LOOP_TIER),
                "the premise: it is on the animated ladder"
            );
            assert!(
                !stored
                    .iter()
                    .any(|(tier, _, _)| tier == crate::visual_tiers::LOOP_DISPLAY_TIER),
                "900x900 is already the whole picture at native resolution, so \
                 the grid loop IS the display loop and no second row is stored"
            );
            sqlx::query_scalar("SELECT sha256 FROM items LIMIT 1")
                .fetch_one(&mut conn)
                .await
                .unwrap()
        };

        // Two strikes of the one outcome that can settle anything, and both
        // stored renditions cleared so the next scan owes each half again.
        {
            let mut conn = env.write().await;
            sqlx::query("DELETE FROM storage.thumbnail_tiers")
                .execute(&mut conn)
                .await
                .unwrap();
            sqlx::query("DELETE FROM storage.thumbnails")
                .execute(&mut conn)
                .await
                .unwrap();
            sqlx::query(
                r#"
INSERT INTO storage.visual_attempts (
    item_sha256, kind, item_mime_type, version, outcome, skip_after, attempts,
    first_seen, last_attempt
)
VALUES (?1, 'loop', 'image/gif', ?2, 'failed', 2, 2, '2026-01-01', '2026-01-01')
                "#,
            )
            .bind(&sha)
            .bind(LOOP_PROCESS_VERSION)
            .execute(&mut conn)
            .await
            .unwrap();
        }

        env.scan().await;
        let mut conn = env.read().await;
        assert!(
            tier_rows(&mut conn).await.is_empty(),
            "the marker retires the animated ladder, including the pass the \
             item is running for its other questions"
        );

        // The heal path the kind exists for: the *loop* version moves and the
        // ledger's `version >= ?` consult stops finding the marker. Keyed to
        // `LOOP_PROCESS_VERSION` rather than the still tiers', so a
        // still-encoder bump neither retires this nor re-runs the ffmpeg it
        // exists to stop.
        assert!(
            visuals_suppressed(&mut conn, &sha, VisualKind::Loop, LOOP_PROCESS_VERSION)
                .await
                .unwrap(),
            "the marker suppresses at the version it was written for"
        );
        assert!(
            !visuals_suppressed(&mut conn, &sha, VisualKind::Loop, LOOP_PROCESS_VERSION + 1)
                .await
                .unwrap(),
            "a LOOP_PROCESS_VERSION bump retires it for free"
        );
        // And it is invisible to the thumbnail consult by construction: the
        // ledger's key is (content, kind).
        assert!(
            !visuals_suppressed(
                &mut conn,
                &sha,
                VisualKind::Thumbnail,
                THUMBNAIL_PROCESS_VERSION
            )
            .await
            .unwrap()
        );
    }

    /// The reclassification HG3 introduced, end to end: a GIF the animation
    /// question later measures as **still** leaves the animated ladder, and
    /// the retirement machinery is what moves it — the loop row goes and the
    /// static tiers arrive, in one pass, and the next scan writes nothing.
    #[tokio::test]
    async fn a_gif_measured_still_swaps_its_loop_for_static_tiers() {
        if !crate::media_tools::ffmpeg_available() {
            return;
        }
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-tier-restill"]).await;
        write_animated_gif(
            &env.media_dirs[0].join("clip.gif"),
            1400,
            2,
            100,
            |x, y, index| {
                let shade = (index as u8).wrapping_mul(90);
                image::Rgba([shade, (x / 64) as u8, (y / 64) as u8, 255])
            },
        );
        env.scan().await;

        {
            let mut conn = env.read().await;
            assert_eq!(
                tier_rows(&mut conn).await,
                vec![
                    tier("grid-m", 1024, 1024),
                    tier("grid-s", 512, 512),
                    tier("grid-xs", 256, 256),
                    tier(LOOP_TIER, 1024, 1024),
                ],
                "the premise: a measured animation takes the animated ladder"
            );
        }

        // What the animation question records for a single-frame GIF, or for
        // one whose structure does not parse: measured, and still.
        {
            let mut conn = env.write().await;
            sqlx::query("UPDATE items SET duration = 0.0")
                .execute(&mut conn)
                .await
                .unwrap();
        }

        let (_, totals) = env.scan().await;
        assert_eq!(totals.backfilled_visuals, 1);
        let mut conn = env.read().await;
        assert_eq!(
            tier_rows(&mut conn).await,
            vec![
                tier("grid-m", 1024, 1024),
                tier("grid-s", 512, 512),
                tier("grid-xs", 256, 256)
            ],
            "a measured-still GIF wants static tiers and no loop"
        );
        assert!(
            !tier_media_types(&mut conn)
                .await
                .iter()
                .any(|(tier, _)| tier == LOOP_TIER),
            "the loop row is retired, not left behind to be served immutably"
        );
        drop(conn);

        let (_, totals) = env.scan().await;
        assert_eq!(
            totals.backfilled_visuals, 0,
            "and it settles: the still set is the one the ladder now wants"
        );
    }

    /// Every stored grid rendition as `(tier, media_type)`, in the
    /// dispatcher's own order.
    async fn tier_media_types(conn: &mut sqlx::SqliteConnection) -> Vec<(String, String)> {
        tier_snapshot(conn)
            .await
            .into_iter()
            .map(|row| (row.tier, row.media_type))
            .collect()
    }

    // An item whose display rendition is already the one the current rule
    // wants, missing only its grid tiers. The tier backfill must not escalate
    // to a full visuals replacement: re-encoding and re-storing an identical
    // display rendition (and its blurhash) for every already-correct item is
    // the bulk of a library-wide upgrade, and it buys nothing.
    //
    // 9000x1000 uncompressed is 27 MB: past the lossless class's 2 MiB byte
    // bound while every pixel bound is clear, so the display rendition is a
    // plain re-encode at the original dimensions — stored, and stable across
    // rescans.
    #[tokio::test]
    async fn a_tier_only_backfill_leaves_the_display_rendition_alone() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-tier-display-intact"]).await;
        image::RgbImage::new(9000, 1000)
            .save(env.media_dirs[0].join("wide.bmp"))
            .unwrap();
        env.scan().await;

        let wanted = vec![
            tier("grid-m", 2048, 1000),
            tier("grid-s", 1024, 512),
            tier("grid-xs", 512, 256),
        ];
        let (display_before, blurhash_before) = {
            let mut conn = env.read().await;
            assert_eq!(thumbnail_count(&mut conn).await, 1);
            assert_eq!(tier_rows(&mut conn).await, wanted);
            (display_rows(&mut conn).await, blurhash_of(&mut conn).await)
        };
        assert!(blurhash_before.is_some());

        // The state an existing library is in: display renditions from before
        // the ladder, no tiers.
        {
            let mut conn = env.write().await;
            sqlx::query("DELETE FROM storage.thumbnail_tiers")
                .execute(&mut conn)
                .await
                .unwrap();
        }

        let (_, totals) = env.scan().await;
        assert_eq!(totals.backfilled_visuals, 1, "the tiers were dispatched");
        let mut conn = env.read().await;
        assert_eq!(tier_rows(&mut conn).await, wanted, "and written");
        assert_eq!(
            display_rows(&mut conn).await,
            display_before,
            "the display rendition was neither re-encoded nor re-stored \
             (same rowid, same bytes)"
        );
        assert_eq!(blurhash_of(&mut conn).await, blurhash_before);
    }

    /// The stored display renditions as `(rowid, bytes)`. The store is a
    /// delete-then-insert, so an unchanged rowid is proof nothing rewrote it.
    async fn display_rows(conn: &mut sqlx::SqliteConnection) -> Vec<(i64, Vec<u8>)> {
        display_snapshot(conn)
            .await
            .into_iter()
            .map(|row| (row.id, row.thumbnail))
            .collect()
    }

    async fn blurhash_of(conn: &mut sqlx::SqliteConnection) -> Option<String> {
        sqlx::query_scalar("SELECT blurhash FROM items")
            .fetch_one(conn)
            .await
            .unwrap()
    }

    // A video's grid tiers come from the pictures already in the database —
    // its 2x2 frame grid and its first frame — not from a fresh ffmpeg run
    // over the source. The clip's frames are 576x1024, so the grid is
    // 1152x2048: past 1.25x `grid-s` and inside 1.25x `grid-m`, while the
    // first frame is inside both of those and past `grid-xs`. Three rows
    // across the two pictures, and the dispatcher predicts exactly that.
    #[tokio::test]
    async fn a_video_backfills_tiers_from_its_stored_thumbnails() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-tier-video"]).await;
        let clip = env.media_dirs[0].join("clip.mp4");
        if !write_clip(&clip, None, None) {
            return;
        }
        env.scan().await;
        {
            let mut conn = env.read().await;
            assert_eq!(thumbnail_count(&mut conn).await, 2, "the grid and frame 0");
            assert_eq!(
                tier_rows(&mut conn).await,
                vec![
                    tier("grid-s", 512, 910),
                    tier("grid-xs", 256, 455),
                    tier("grid-xs", 256, 455)
                ]
            );
        }

        // The state an existing library is in, and the one that proves the
        // derivation: the tiers are gone, the source file is replaced by bytes
        // no decoder will touch, and the stored pictures are all that is left.
        {
            let mut conn = env.write().await;
            sqlx::query("DELETE FROM storage.thumbnail_tiers")
                .execute(&mut conn)
                .await
                .unwrap();
        }
        let mtime = fs::metadata(&clip).unwrap().modified().unwrap();
        let length = fs::metadata(&clip).unwrap().len();
        fs::write(&clip, vec![0_u8; length as usize]).unwrap();
        fs::File::options()
            .write(true)
            .open(&clip)
            .unwrap()
            .set_modified(mtime)
            .unwrap();

        let (_, totals) = env.scan().await;
        assert_eq!(totals.unchanged_files, 1);
        let mut conn = env.read().await;
        assert_eq!(
            tier_rows(&mut conn).await,
            vec![
                tier("grid-s", 512, 910),
                tier("grid-xs", 256, 455),
                tier("grid-xs", 256, 455)
            ],
            "the tiers came back from the stored pictures, not from the file"
        );
    }

    // The end-to-end contract of the visuals negative cache, at the level the
    // production symptom lives at: a scan that finds nothing new must not
    // regenerate visuals it already knows produce nothing.
    //
    // Every gate is exercised in the order the dispatcher applies them — the
    // confirmation threshold, then the marker itself, then the version stamp —
    // and the run that finally succeeds proves the store retires the marker.
    #[tokio::test]
    async fn a_recorded_visuals_attempt_suppresses_the_next_generation() {
        use crate::db::visual_attempts::{VisualFailure, upsert_visual_attempts};

        let test_env = test_data_dir();
        let root = test_env.path();
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();

        // Its own folder: the temp root is shared by every test in the process.
        let media_dir = root.join("media-visual-attempts");
        fs::create_dir_all(&media_dir).unwrap();
        // An image the display rule really does store a rendition for, so a
        // missing one is meaningful rather than the normal state of most
        // images. Under the dimension-first rule that takes *bytes*: 9000x1000
        // uncompressed is 27 MB, past the 24 MB bound while every pixel bound
        // is clear, so the rendition is a plain re-encode.
        image::RgbImage::new(9000, 1000)
            .save(media_dir.join("large.bmp"))
            .unwrap();

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
        assert_eq!(thumbnail_count(&mut conn).await, 1);
        assert!(
            visual_attempt_rows(&mut conn).await.is_empty(),
            "a pass that stored something owes no marker"
        );
        let sha256: String = sqlx::query_scalar("SELECT sha256 FROM items")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        drop(conn);

        // Drop the stored thumbnail: from here on the positive cache misses
        // and the negative one is the only thing that can answer.
        let clear_thumbnails = || async {
            let mut conn = crate::db::open_index_db_write_no_user_data(&index_db)
                .await
                .unwrap();
            sqlx::query("DELETE FROM storage.thumbnails")
                .execute(&mut conn)
                .await
                .unwrap();
        };
        let seed = |failure: Option<VisualFailure>| {
            let sha256 = sha256.clone();
            let index_db = index_db.clone();
            async move {
                let mut conn = crate::db::open_index_db_write_no_user_data(&index_db)
                    .await
                    .unwrap();
                let verdict = match failure {
                    None => VisualVerdict::nothing(VisualKind::Thumbnail),
                    Some(failure) => VisualVerdict::failed(VisualKind::Thumbnail, failure),
                };
                upsert_visual_attempts(
                    &mut conn,
                    &[verdict.into_record(&sha256, "image/bmp", THUMBNAIL_PROCESS_VERSION)],
                    Some(1),
                )
                .await
                .unwrap();
            }
        };

        // An *unconfirmed* verdict (an external tool that read the file
        // itself, failing once) must not suppress anything yet: the whole
        // point of the threshold is that one transient hiccup never becomes a
        // permanent skip.
        clear_thumbnails().await;
        seed(Some(VisualFailure {
            kind: ApiErrorKind::Input,
            skip_after: SKIP_AFTER_AMBIGUOUS,
            message: "ffmpeg failed".to_string(),
        }))
        .await;
        let (_, totals) = execute_folder_scan(
            &index_db,
            &user_data_db,
            &config,
            &config.included_folders,
            &[],
            ScanOptions { worker_count: 2 },
        )
        .await
        .unwrap();
        assert_eq!(
            totals.visuals_suppressed, 0,
            "an unconfirmed verdict must be re-attempted"
        );
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        assert_eq!(thumbnail_count(&mut conn).await, 1);
        assert!(
            visual_attempt_rows(&mut conn).await.is_empty(),
            "storing the thumbnail retires the marker, in the same transaction"
        );
        drop(conn);

        // A confirmed verdict does suppress: no dispatch, no decode, no
        // thumbnail — and the marker survives, because nothing overwrote it.
        clear_thumbnails().await;
        seed(None).await;
        let (_, totals) = execute_folder_scan(
            &index_db,
            &user_data_db,
            &config,
            &config.included_folders,
            &[],
            ScanOptions { worker_count: 2 },
        )
        .await
        .unwrap();
        assert_eq!(totals.visuals_suppressed, 1);
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        assert_eq!(
            thumbnail_count(&mut conn).await,
            0,
            "the generation must not have run"
        );
        let rows = visual_attempt_rows(&mut conn).await;
        assert_eq!(
            (
                rows.len(),
                rows[0].0.as_str(),
                rows[0].1.as_str(),
                rows[0].3
            ),
            (1, VisualKind::Thumbnail.as_str(), "none", 1)
        );
        drop(conn);

        // A generator version bump retires every marker for free — the consult
        // is `version >= ?`, so a marker stamped by an older generator is
        // simply ignored. Simulated from the marker's side, which is exactly
        // what raising the constant produces.
        {
            let mut conn = crate::db::open_index_db_write_no_user_data(&index_db)
                .await
                .unwrap();
            sqlx::query("UPDATE storage.visual_attempts SET version = ?")
                .bind(THUMBNAIL_PROCESS_VERSION - 1)
                .execute(&mut conn)
                .await
                .unwrap();
        }
        let (_, totals) = execute_folder_scan(
            &index_db,
            &user_data_db,
            &config,
            &config.included_folders,
            &[],
            ScanOptions { worker_count: 2 },
        )
        .await
        .unwrap();
        assert_eq!(
            totals.visuals_suppressed, 0,
            "a marker from an older generator must not suppress the new one"
        );
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        assert_eq!(thumbnail_count(&mut conn).await, 1);
        assert!(visual_attempt_rows(&mut conn).await.is_empty());
    }

    /// (kind, outcome, attempts, skip_after) per marker, keyed by the content
    /// it belongs to — two broken files in one folder need telling apart.
    async fn markers_by_sha(
        conn: &mut sqlx::SqliteConnection,
        sha256: &str,
    ) -> Vec<(String, String, i64, i64)> {
        sqlx::query_as(
            "SELECT kind, outcome, attempts, skip_after FROM storage.visual_attempts \
             WHERE item_sha256 = ? ORDER BY kind",
        )
        .bind(sha256)
        .fetch_all(conn)
        .await
        .unwrap()
    }

    // The whole point of un-fusing the image decode, end to end: an image whose
    // pixels do not decode is *indexed* — with its real dimensions, from its
    // header — and simply has no visuals, exactly like a PDF pdfium cannot
    // parse. Before this it was rejected outright, silently, and re-decoded on
    // every scan forever, because a file with no `files` row has no mtime
    // shortcut to skip it.
    //
    // Then the chain that stops the work: the failed decode is remembered, the
    // next run confirms it (the verdict is ambiguous — a decoder that reads as
    // it goes cannot tell a truncated file from a mount that dropped), and the
    // run after that does not open the file at all.
    //
    // Both halves of the image population are here, because they reach the
    // decode by different routes: one large enough to warrant a stored
    // thumbnail, and one served from its original file, whose only remaining
    // decode is the blurhash's. The second used to escape the marker entirely.
    #[tokio::test]
    async fn a_corrupt_image_is_indexed_without_visuals_once() {
        let test_env = test_data_dir();
        let root = test_env.path();
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();

        // Its own folder: the temp root is shared by every test in the process.
        let media_dir = root.join("media-undecodable");
        fs::create_dir_all(&media_dir).unwrap();

        // 9000x1000 uncompressed = 27 MB. Truncating 100 KB leaves it well
        // past the display rule's 24 MB byte bound, so this one really would
        // get a stored rendition.
        let large = media_dir.join("large.bmp");
        image::RgbImage::new(9000, 1000).save(&large).unwrap();
        let full_len = fs::metadata(&large).unwrap().len();
        fs::OpenOptions::new()
            .write(true)
            .open(&large)
            .unwrap()
            .set_len(full_len - 100_000)
            .unwrap();

        // And a small one, which no thumbnail would ever be stored for. Noise,
        // not a flat colour: a PNG of one colour compresses to so few bytes
        // that a 60% prefix can still hold the whole image.
        let small = media_dir.join("small.png");
        let mut noise = image::RgbImage::new(64, 64);
        for (x, y, pixel) in noise.enumerate_pixels_mut() {
            *pixel = image::Rgb([
                (x * 7 % 251) as u8,
                (y * 13 % 241) as u8,
                (x * y % 239) as u8,
            ]);
        }
        noise.save(&small).unwrap();
        let bytes = fs::read(&small).unwrap();
        fs::write(&small, &bytes[..bytes.len() * 6 / 10]).unwrap();

        let store = SystemConfigStore::new(root.to_path_buf());
        let mut config = SystemConfig::default();
        config.included_folders = vec![media_dir.to_string_lossy().to_string()];
        store.save(&index_db, &config).unwrap();
        let scan = || {
            execute_folder_scan(
                &index_db,
                &user_data_db,
                &config,
                &config.included_folders,
                &[],
                ScanOptions { worker_count: 2 },
            )
        };

        // Run 1: both files enter the index.
        let (_, totals) = scan().await.unwrap();
        assert_eq!(totals.new_items, 2, "an undecodable image is still indexed");
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let indexed: Vec<(String, String, Option<i64>, Option<i64>, Option<String>)> =
            sqlx::query_as(
                "SELECT files.path, items.sha256, items.width, items.height, items.blurhash \
                 FROM items JOIN files ON files.item_id = items.id ORDER BY files.path",
            )
            .fetch_all(&mut conn)
            .await
            .unwrap();
        assert_eq!(indexed.len(), 2);
        let (large_sha, small_sha) = (indexed[0].1.clone(), indexed[1].1.clone());
        assert_eq!(
            (indexed[0].2, indexed[0].3, indexed[1].2, indexed[1].3),
            (Some(9000), Some(1000), Some(64), Some(64)),
            "the dimensions come from the header, which truncation leaves intact"
        );
        assert!(
            indexed.iter().all(|row| row.4.is_none()),
            "no decode means no blurhash — cosmetic, and the item is indexed"
        );
        assert_eq!(
            thumbnail_count(&mut conn).await,
            0,
            "and no visuals of any kind"
        );

        // The visuals verdict is remembered — once per content, unconfirmed —
        // and the failure is visible in the audit surface even though it no
        // longer blocks anything.
        for sha in [&large_sha, &small_sha] {
            assert_eq!(
                markers_by_sha(&mut conn, sha).await,
                vec![(
                    VisualKind::Thumbnail.as_str().to_string(),
                    "failed".to_string(),
                    1,
                    SKIP_AFTER_AMBIGUOUS
                )],
                "the decode is remembered for {sha}"
            );
        }
        let rows = scan_error_rows(&mut conn).await;
        assert_eq!(rows.len(), 2, "both failures are auditable: {rows:?}");
        assert!(
            rows.iter()
                .all(|row| row.1 == STAGE_DECODE && row.2 == "input"),
            "recorded as what they are: {rows:?}"
        );
        drop(conn);

        // Run 2: still unconfirmed, so both are re-attempted exactly once more.
        let (_, totals) = scan().await.unwrap();
        assert_eq!(
            (totals.visuals_suppressed, totals.known_bad),
            (0, 0),
            "an unconfirmed verdict is re-attempted, and an *indexed* file is \
             never skipped by its own audit row"
        );
        assert_eq!(totals.unchanged_files, 2);
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        for sha in [&large_sha, &small_sha] {
            assert_eq!(
                markers_by_sha(&mut conn, sha).await[0].2,
                2,
                "the second run confirms the verdict for {sha}"
            );
        }
        let rows = scan_error_rows(&mut conn).await;
        assert_eq!(
            rows.len(),
            2,
            "a file that indexes fine every time does not spend its audit row"
        );
        assert!(
            rows.iter()
                .all(|row| (row.3, row.4) == (2, SKIP_AFTER_AMBIGUOUS)),
            "the row counts with the marker — the backfill decode confirms both \
             halves or the audit surface says 'will retry' about a file nothing \
             will retry: {rows:?}"
        );
        drop(conn);

        // Run 3: confirmed. Nothing is opened, nothing is decoded, and the
        // files stay in the index and stay available.
        let (_, totals) = scan().await.unwrap();
        assert_eq!(
            (
                totals.visuals_suppressed,
                totals.known_bad,
                totals.marked_unavailable,
                totals.unchanged_files
            ),
            (2, 0, 0, 2)
        );
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        for sha in [&large_sha, &small_sha] {
            assert_eq!(
                markers_by_sha(&mut conn, sha).await[0].2,
                2,
                "no further attempt was made for {sha}"
            );
        }
        let available: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM files WHERE available = 1")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(available, 2, "the files remain findable throughout");
        let rows = scan_error_rows(&mut conn).await;
        assert!(
            rows.iter().all(|row| row.3 == 2),
            "and once the marker suppresses, nothing decodes and nothing \
             writes: {rows:?}"
        );
        drop(conn);

        // Repairing one of them clears everything about it. Note *how*: the
        // repair is new content under the same path, so it is a new sha256 —
        // the old marker is not retired, it is orphaned (its content left the
        // index) and belongs to the sweep, while the audit row is retired
        // directly, by the moved bytes under the path that keys it. The mtime
        // is set by hand because the walker's shortcut compares it and a
        // rewrite inside the same clock tick would read as unchanged.
        image::RgbImage::new(9000, 1000).save(&large).unwrap();
        let mtime = fs::metadata(&large).unwrap().modified().unwrap();
        fs::File::options()
            .write(true)
            .open(&large)
            .unwrap()
            .set_modified(mtime + std::time::Duration::from_secs(10))
            .unwrap();
        // Through the service, not `scan()`: the orphan sweep runs in
        // `rescan_folders`, after the scan and after the items without files
        // are deleted, so the marker's disappearance is only observable from
        // there.
        FileScanService::new(
            index_db.clone(),
            user_data_db.clone(),
            root.to_path_buf(),
            ScanOptions { worker_count: 2 },
        )
        .rescan_folders()
        .await
        .unwrap();
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        assert_eq!(thumbnail_count(&mut conn).await, 1);
        let rows = scan_error_rows(&mut conn).await;
        assert_eq!(
            rows.len(),
            1,
            "only the still-broken file owes one: {rows:?}"
        );
        assert!(rows[0].0.ends_with("small.png"), "{rows:?}");
        assert!(
            markers_by_sha(&mut conn, &large_sha).await.is_empty(),
            "the repaired file's old content left the index, so its marker is \
             swept rather than kept for a sha256 nothing will ever ask about"
        );
        assert_eq!(
            markers_by_sha(&mut conn, &small_sha).await.len(),
            1,
            "and the sweep discriminates: the file that is still broken keeps \
             the marker that suppresses it"
        );
    }

    /// A 64x64 PNG of noise truncated to `fraction` of its length: the header
    /// survives (so the file is indexed with its real dimensions) and the pixel
    /// data does not. Noise, not a flat colour, because a single-colour PNG
    /// compresses to so few bytes that a prefix can still hold the whole image.
    fn write_undecodable_png(path: &Path, fraction: f64) {
        let mut noise = image::RgbImage::new(64, 64);
        for (x, y, pixel) in noise.enumerate_pixels_mut() {
            *pixel = image::Rgb([
                (x * 7 % 251) as u8,
                (y * 13 % 241) as u8,
                (x * y % 239) as u8,
            ]);
        }
        noise.save(path).unwrap();
        let bytes = fs::read(path).unwrap();
        let keep = (bytes.len() as f64 * fraction) as usize;
        fs::write(path, &bytes[..keep]).unwrap();
    }

    /// (path, attempts, last_modified, file_size) per ledger row.
    async fn scan_error_keys(conn: &mut sqlx::SqliteConnection) -> Vec<(String, i64, String, i64)> {
        sqlx::query_as(
            "SELECT path, attempts, last_modified, file_size FROM scan_errors ORDER BY path",
        )
        .fetch_all(conn)
        .await
        .unwrap()
    }

    // The hazard an audit-only row exists to avoid, from the other side: the
    // walker keeps every path it *failed* on out of unavailable-marking, or a
    // file that is still on disk gets marked gone and then deleted. A `decode`
    // row is not that kind of row — its file was walked, hashed and indexed
    // like any other — so it must not put its path on the exclusion list, and
    // above all must not make the scan treat the folder as partially seen.
    //
    // Asserted against a file that really did vanish in the same run, so the
    // marking is proven to still work rather than merely to have been skipped.
    #[tokio::test]
    async fn an_audit_only_row_does_not_exclude_its_file_from_unavailable_marking() {
        let test_env = test_data_dir();
        let root = test_env.path();
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();

        // Its own folder: the temp root is shared by every test in the process.
        let media_dir = root.join("media-audit-unavailable");
        fs::create_dir_all(&media_dir).unwrap();
        let corrupt = media_dir.join("corrupt.png");
        write_undecodable_png(&corrupt, 0.6);
        let vanishing = media_dir.join("vanishing.png");
        image::RgbImage::new(8, 8).save(&vanishing).unwrap();

        let store = SystemConfigStore::new(root.to_path_buf());
        let mut config = SystemConfig::default();
        config.included_folders = vec![media_dir.to_string_lossy().to_string()];
        store.save(&index_db, &config).unwrap();
        let scan = || {
            execute_folder_scan(
                &index_db,
                &user_data_db,
                &config,
                &config.included_folders,
                &[],
                ScanOptions { worker_count: 2 },
            )
        };

        let (_, totals) = scan().await.unwrap();
        assert_eq!(totals.new_items, 2);
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let rows = scan_error_rows(&mut conn).await;
        assert_eq!(
            rows.len(),
            1,
            "one audit row, for the corrupt file: {rows:?}"
        );
        assert!(rows[0].0.ends_with("corrupt.png"), "{rows:?}");
        drop(conn);

        fs::remove_file(&vanishing).unwrap();
        let (_, totals) = scan().await.unwrap();
        assert_eq!(
            (totals.marked_unavailable, totals.known_bad),
            (1, 0),
            "the file that is gone is marked, and the one with an audit row is \
             walked normally rather than skipped"
        );

        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let files: Vec<(String, i64)> =
            sqlx::query_as("SELECT path, available FROM files ORDER BY path")
                .fetch_all(&mut conn)
                .await
                .unwrap();
        assert_eq!(files.len(), 2);
        assert!(
            files[0].0.ends_with("corrupt.png") && files[0].1 == 1,
            "{files:?}"
        );
        assert!(
            files[1].0.ends_with("vanishing.png") && files[1].1 == 0,
            "{files:?}"
        );
        assert_eq!(
            scan_error_rows(&mut conn).await.len(),
            1,
            "and the audit row is still there: nothing about the run spent it"
        );
    }

    // The retry key does its job on an audit-only row too: the row describes
    // *those* bytes, so a file whose content moved gets one row, not two, and
    // its confirmations start over. Without the reset a file that is repeatedly
    // re-saved and repeatedly broken would inherit a confirmation it never
    // earned; without the single-row guarantee the ledger would grow a verdict
    // per revision of every broken file.
    #[tokio::test]
    async fn a_changed_undecodable_image_replaces_its_audit_row() {
        let test_env = test_data_dir();
        let root = test_env.path();
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();

        // Its own folder: the temp root is shared by every test in the process.
        let media_dir = root.join("media-audit-rewritten");
        fs::create_dir_all(&media_dir).unwrap();
        let corrupt = media_dir.join("corrupt.png");
        write_undecodable_png(&corrupt, 0.6);

        let store = SystemConfigStore::new(root.to_path_buf());
        let mut config = SystemConfig::default();
        config.included_folders = vec![media_dir.to_string_lossy().to_string()];
        store.save(&index_db, &config).unwrap();
        let scan = || {
            execute_folder_scan(
                &index_db,
                &user_data_db,
                &config,
                &config.included_folders,
                &[],
                ScanOptions { worker_count: 2 },
            )
        };

        scan().await.unwrap();
        // Run 2 confirms it, so the reset below has something to undo.
        scan().await.unwrap();
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let before = scan_error_keys(&mut conn).await;
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].1, 2, "confirmed against the original bytes");
        drop(conn);

        // Truncated further: still an undecodable image, but a different one.
        // The mtime is set by hand because the walker's shortcut compares it
        // and a rewrite inside the same clock tick would read as unchanged.
        let bytes = fs::read(&corrupt).unwrap();
        fs::write(&corrupt, &bytes[..bytes.len() * 8 / 10]).unwrap();
        let mtime = fs::metadata(&corrupt).unwrap().modified().unwrap();
        fs::File::options()
            .write(true)
            .open(&corrupt)
            .unwrap()
            .set_modified(mtime + std::time::Duration::from_secs(10))
            .unwrap();
        let (_, totals) = scan().await.unwrap();
        assert_eq!(
            (totals.modified_files, totals.known_bad),
            (1, 0),
            "moved bytes are re-attempted, whatever the row says"
        );

        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let after = scan_error_keys(&mut conn).await;
        assert_eq!(
            after.len(),
            1,
            "one path still owes exactly one row: {after:?}"
        );
        assert_eq!(after[0].0, before[0].0);
        assert_eq!(
            after[0].1, 1,
            "the confirmations were about bytes that are gone"
        );
        assert_eq!(
            after[0].3,
            fs::metadata(&corrupt).unwrap().len() as i64,
            "and the row is keyed to what is on disk now"
        );
        assert_ne!(
            (after[0].2.as_str(), after[0].3),
            (before[0].2.as_str(), before[0].3),
            "both halves of the retry key moved: {before:?} -> {after:?}"
        );
    }

    // The other side of the retry key's job: a *false* change (mtime moved,
    // bytes identical — a backup restore, a `touch`) must not spend the audit
    // row. The re-hash proves the bytes did not move, the sha-keyed marker
    // keeps suppressing regardless of mtime, and clearing the row on the stat
    // proxy would leave that suppression with no record on the failures
    // surface. The row follows the stat instead, counters intact.
    #[tokio::test]
    async fn a_touched_undecodable_image_keeps_its_audit_row() {
        let test_env = test_data_dir();
        let root = test_env.path();
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();

        let media_dir = root.join("media-audit-touched");
        fs::create_dir_all(&media_dir).unwrap();
        let corrupt = media_dir.join("corrupt.png");
        write_undecodable_png(&corrupt, 0.6);

        let store = SystemConfigStore::new(root.to_path_buf());
        let mut config = SystemConfig::default();
        config.included_folders = vec![media_dir.to_string_lossy().to_string()];
        store.save(&index_db, &config).unwrap();
        let scan = || {
            execute_folder_scan(
                &index_db,
                &user_data_db,
                &config,
                &config.included_folders,
                &[],
                ScanOptions { worker_count: 2 },
            )
        };

        // Two runs confirm the verdict on both ledgers.
        scan().await.unwrap();
        scan().await.unwrap();
        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let before = scan_error_keys(&mut conn).await;
        assert_eq!((before.len(), before[0].1), (1, 2), "{before:?}");
        drop(conn);

        // Same bytes, new mtime.
        let mtime = fs::metadata(&corrupt).unwrap().modified().unwrap();
        fs::File::options()
            .write(true)
            .open(&corrupt)
            .unwrap()
            .set_modified(mtime + std::time::Duration::from_secs(10))
            .unwrap();
        let (_, totals) = scan().await.unwrap();
        assert_eq!(
            (totals.modified_files, totals.known_bad),
            (0, 0),
            "a touch is neither a modification nor a suppressed skip"
        );

        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let after = scan_error_keys(&mut conn).await;
        assert_eq!(after.len(), 1, "the audit row survived the touch: {after:?}");
        assert_eq!(after[0].0, before[0].0);
        assert_eq!(
            after[0].1, 2,
            "nothing was attempted, so nothing was learned: the count stands"
        );
        assert_ne!(after[0].2, before[0].2, "the key follows the mtime");
        assert_eq!(after[0].3, before[0].3, "the bytes did not move");
        let marker: (i64,) = sqlx::query_as(
            "SELECT attempts FROM storage.visual_attempts WHERE kind = 'thumbnail'",
        )
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(marker.0, 2, "the sha-keyed marker never noticed the mtime");
    }

    // Markers for content that left the index are swept with the blobs — but
    // their count must stay out of the "something was deleted" flag, which is
    // what gates the post-job VACUUM. A handful of marker rows must never
    // trigger a multi-minute rewrite of a multi-GB database.
    #[tokio::test]
    async fn the_orphan_sweep_takes_markers_without_arming_the_vacuum_gate() {
        use crate::db::visual_attempts::upsert_visual_attempts;

        let test_env = test_data_dir();
        let root = test_env.path();
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();

        let media_dir = root.join("media-visual-sweep");
        fs::create_dir_all(&media_dir).unwrap();
        image::RgbImage::new(8, 8)
            .save(media_dir.join("small.png"))
            .unwrap();

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

        // A marker for content that is not in the index, plus one for content
        // that is: the sweep has to discriminate.
        let indexed_sha: String = {
            let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
            sqlx::query_scalar("SELECT sha256 FROM items")
                .fetch_one(&mut conn)
                .await
                .unwrap()
        };
        {
            let mut conn = crate::db::open_index_db_write_no_user_data(&index_db)
                .await
                .unwrap();
            upsert_visual_attempts(
                &mut conn,
                &[
                    VisualVerdict::nothing(VisualKind::Thumbnail).into_record(
                        "sha_that_left",
                        "video/mp4",
                        THUMBNAIL_PROCESS_VERSION,
                    ),
                    VisualVerdict::nothing(VisualKind::Frame).into_record(
                        "sha_that_left",
                        "video/mp4",
                        FRAME_PROCESS_VERSION,
                    ),
                    VisualVerdict::nothing(VisualKind::Thumbnail).into_record(
                        &indexed_sha,
                        "image/png",
                        THUMBNAIL_PROCESS_VERSION,
                    ),
                ],
                Some(1),
            )
            .await
            .unwrap();
        }

        let result = service.rescan_folders().await.unwrap();
        assert!(
            !result.summary.deleted_data,
            "swept markers carry no blobs and must not arm the VACUUM gate"
        );
        assert!(
            !result.summary.tags_changed,
            "and must not arm the tag recount either — it is derived from the same flag"
        );

        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let rows = visual_attempt_rows(&mut conn).await;
        assert_eq!(rows.len(), 1, "only the orphaned markers go: {rows:?}");
        let remaining: String =
            sqlx::query_scalar("SELECT item_sha256 FROM storage.visual_attempts")
                .fetch_one(&mut conn)
                .await
                .unwrap();
        assert_eq!(remaining, indexed_sha);
    }

    /// A migrated database pair plus one media folder per name — the temp root
    /// is shared by every test in the process — with the config saved where
    /// [`FileScanService`] will look for it.
    struct VisualsEnv {
        index_db: String,
        user_data_db: String,
        config: SystemConfig,
        media_dirs: Vec<PathBuf>,
        root: PathBuf,
    }

    impl VisualsEnv {
        async fn scan(&self) -> (Vec<i64>, ScanTotals) {
            execute_folder_scan(
                &self.index_db,
                &self.user_data_db,
                &self.config,
                &self.config.included_folders,
                &[],
                ScanOptions { worker_count: 2 },
            )
            .await
            .unwrap()
        }

        fn service(&self) -> FileScanService {
            FileScanService::new(
                self.index_db.clone(),
                self.user_data_db.clone(),
                self.root.clone(),
                ScanOptions { worker_count: 2 },
            )
        }

        async fn read(&self) -> sqlx::SqliteConnection {
            open_index_db_read(&self.index_db, &self.user_data_db)
                .await
                .unwrap()
        }

        async fn write(&self) -> sqlx::SqliteConnection {
            crate::db::open_index_db_write_no_user_data(&self.index_db)
                .await
                .unwrap()
        }
    }

    async fn visuals_env(root: &Path, folders: &[&str]) -> VisualsEnv {
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();
        let media_dirs: Vec<PathBuf> = folders.iter().map(|name| root.join(name)).collect();
        for dir in &media_dirs {
            fs::create_dir_all(dir).unwrap();
        }
        let mut config = SystemConfig::default();
        // Off by default, and the only indexable type whose generator fails
        // without a fixture the repository would have to carry.
        config.scan_pdf = true;
        config.included_folders = media_dirs
            .iter()
            .map(|dir| dir.to_string_lossy().to_string())
            .collect();
        SystemConfigStore::new(root.to_path_buf())
            .save(&index_db, &config)
            .unwrap();
        VisualsEnv {
            index_db,
            user_data_db,
            config,
            media_dirs,
            root: root.to_path_buf(),
        }
    }

    /// A PDF whose bytes no renderer will ever parse. `blocked` where pdfium
    /// is missing, `failed` where it is present — either way a verdict about
    /// content that a scan reaches through the ordinary walk.
    const UNRENDERABLE_PDF: &[u8] = b"%PDF-1.7\nnothing in here parses\n";

    /// One second of silent 16-bit mono PCM, written under an `.mp4` name on
    /// purpose: the mime type is guessed from the extension, so the scan
    /// treats it as a video, while ffprobe reports the truth — a container
    /// with no video stream. That is the canonical `none`: the generator ran,
    /// correctly produced nothing, and nothing will change until it does.
    fn silent_wav_bytes() -> Vec<u8> {
        let sample_rate: u32 = 8000;
        let data_len: u32 = sample_rate * 2;
        let mut out = Vec::with_capacity(44 + data_len as usize);
        out.extend_from_slice(b"RIFF");
        out.extend_from_slice(&(36 + data_len).to_le_bytes());
        out.extend_from_slice(b"WAVEfmt ");
        out.extend_from_slice(&16u32.to_le_bytes()); // PCM header size
        out.extend_from_slice(&1u16.to_le_bytes()); // PCM
        out.extend_from_slice(&1u16.to_le_bytes()); // mono
        out.extend_from_slice(&sample_rate.to_le_bytes());
        out.extend_from_slice(&(sample_rate * 2).to_le_bytes()); // byte rate
        out.extend_from_slice(&2u16.to_le_bytes()); // block align
        out.extend_from_slice(&16u16.to_le_bytes()); // bits per sample
        out.extend_from_slice(b"data");
        out.extend_from_slice(&data_len.to_le_bytes());
        out.resize(44 + data_len as usize, 0);
        out
    }

    // A suppressed image must not fall through to the blurhash fallback. The
    // fallback's only source for an image with no stored thumbnail is a full
    // decode of the original — the exact decode the thumbnail marker's verdict
    // already settled — so letting it through would re-open and re-decode the
    // file on every single scan while the stat happily reported a skip.
    #[tokio::test]
    async fn a_suppressed_image_does_not_redecode_for_a_blurhash() {
        use crate::db::visual_attempts::upsert_visual_attempts;

        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-blurhash-suppressed"]).await;
        // 27 MB uncompressed: past the display rule's byte bound, so this
        // image really does store a rendition. See
        // `a_recorded_visuals_attempt_suppresses_the_next_generation`.
        image::RgbImage::new(9000, 1000)
            .save(env.media_dirs[0].join("large.bmp"))
            .unwrap();

        env.scan().await;
        let mut conn = env.read().await;
        assert_eq!(thumbnail_count(&mut conn).await, 1);
        let sha256: String = sqlx::query_scalar("SELECT sha256 FROM items")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        drop(conn);

        // The state that makes the fallback dangerous: no stored thumbnail (so
        // the positive cache misses), no blurhash (so there is still work to
        // do), and a marker that settles the decode.
        {
            let mut conn = env.write().await;
            sqlx::query("DELETE FROM storage.thumbnails")
                .execute(&mut conn)
                .await
                .unwrap();
            sqlx::query("UPDATE items SET blurhash = NULL")
                .execute(&mut conn)
                .await
                .unwrap();
            upsert_visual_attempts(
                &mut conn,
                &[VisualVerdict::nothing(VisualKind::Thumbnail).into_record(
                    &sha256,
                    "image/bmp",
                    THUMBNAIL_PROCESS_VERSION,
                )],
                Some(1),
            )
            .await
            .unwrap();
        }

        let (_, totals) = env.scan().await;
        assert_eq!(
            totals.visuals_suppressed, 1,
            "the whole dispatch is what the marker skipped"
        );
        let mut conn = env.read().await;
        assert_eq!(thumbnail_count(&mut conn).await, 0);
        let blurhash: Option<String> = sqlx::query_scalar("SELECT blurhash FROM items")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert!(
            blurhash.is_none(),
            "a blurhash here could only have come from re-decoding the original"
        );
        let rows = visual_attempt_rows(&mut conn).await;
        assert_eq!(
            (rows.len(), rows[0].2),
            (1, 1),
            "no pass ran, so the marker learned nothing: {rows:?}"
        );
    }

    // The dispatch's early returns all ask the same question — "is any of the
    // non-visual backfill questions still owed?" — and the copy guarding the
    // no-blurhash-source branch used to leave the orientation question out of
    // it. A marker-suppressed image whose *only* remaining work is its
    // orientation therefore returned early on every scan and `items.rotation`
    // stayed NULL forever, which is the one thing a backfill question must
    // never do: not terminate.
    //
    // The other half of the same branch is what it must *not* wake up. The
    // dispatch it keeps alive is a header read; the blurhash half has no source
    // but a full decode of the original, which is exactly the decode this
    // file's marker already settled (see the test above).
    #[tokio::test]
    async fn rotation_alone_keeps_a_suppressed_dispatch_alive() {
        use crate::db::visual_attempts::upsert_visual_attempts;

        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-rotation-suppressed"]).await;
        // 27 MB uncompressed: past the display rule's byte bound, so this image
        // really does store a rendition and the positive cache is what the
        // marker has to beat.
        image::RgbImage::new(9000, 1000)
            .save(env.media_dirs[0].join("large.bmp"))
            .unwrap();

        env.scan().await;
        let mut conn = env.read().await;
        let sha256: String = sqlx::query_scalar("SELECT sha256 FROM items")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        drop(conn);

        // No stored thumbnail (the positive cache misses), no blurhash and no
        // orientation (both still owed), and a marker that settles the decode.
        {
            let mut conn = env.write().await;
            sqlx::query("DELETE FROM storage.thumbnails")
                .execute(&mut conn)
                .await
                .unwrap();
            sqlx::query("UPDATE items SET blurhash = NULL, rotation = NULL")
                .execute(&mut conn)
                .await
                .unwrap();
            upsert_visual_attempts(
                &mut conn,
                &[VisualVerdict::nothing(VisualKind::Thumbnail).into_record(
                    &sha256,
                    "image/bmp",
                    THUMBNAIL_PROCESS_VERSION,
                )],
                Some(1),
            )
            .await
            .unwrap();
        }

        env.scan().await;

        assert_eq!(
            geometry_columns(&env, &sha256).await.2,
            Some(0),
            "the orientation question was the only work left, and it still has to run"
        );
        assert!(
            stored_blurhash(&env, &sha256).await.is_none(),
            "a blurhash here could only have come from re-decoding the settled original"
        );
    }

    // `attempts` counts *runs*, but a run opens one `file_scans` row per root.
    // Markers are keyed by content, so the same file under two roots would be
    // counted twice by one run — confirming a `skip_after = 2` verdict that has
    // only failed once, which is precisely the transient-mount case the second
    // attempt exists to survive.
    //
    #[tokio::test]
    async fn duplicate_content_under_two_roots_counts_one_attempt() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-dup-a", "media-dup-b"]).await;
        for dir in &env.media_dirs {
            fs::write(dir.join("broken.pdf"), UNRENDERABLE_PDF).unwrap();
        }
        // A second, *distinct* unrenderable PDF under the second root only.
        // Where pdfium is missing every verdict is `blocked` and settles at one
        // attempt, so the duplicate above is suppressed under the second root
        // and the token its second write would have carried is unobservable —
        // this file's marker is necessarily written while the second root is
        // walked, which is exactly where a per-root token would show through.
        fs::write(
            env.media_dirs[1].join("other.pdf"),
            b"%PDF-1.4\nalso not a document\n",
        )
        .unwrap();

        let (scan_ids, _) = env.scan().await;
        assert_eq!(scan_ids.len(), 2, "one scan row per root is the premise");

        let mut conn = env.read().await;
        let markers: Vec<(String, i64, Option<i64>)> = sqlx::query_as(
            "SELECT item_sha256, attempts, last_scan_id FROM storage.visual_attempts",
        )
        .fetch_all(&mut conn)
        .await
        .unwrap();
        assert_eq!(
            markers.len(),
            2,
            "one marker per distinct content, not per file: {markers:?}"
        );
        for (sha256, attempts, token) in &markers {
            assert_eq!(*attempts, 1, "one run, one attempt ({sha256})");
            assert_eq!(
                *token,
                Some(scan_ids[0]),
                "every write of this run shares its first root's scan id ({sha256})"
            );
        }
    }

    // The scan ledger's twin of the marker test above: `scan_errors.attempts`
    // dedups on the same run-shared token. The path key makes the aliasing
    // case harder to hit than the content-keyed markers (it takes two
    // registered roots that name the same file, e.g. case-drifted roots on
    // Windows), but the invariant is the same — one run must never be able to
    // count itself twice, whatever the walk order — and the token is what
    // pins it.
    #[tokio::test]
    async fn scan_error_writes_share_one_attempt_token_per_run() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-tok-a", "media-tok-b"]).await;
        for dir in &env.media_dirs {
            fs::write(dir.join("broken.png"), b"not a png at all").unwrap();
        }

        let (scan_ids, _) = env.scan().await;
        assert_eq!(scan_ids.len(), 2, "one scan row per root is the premise");

        let mut conn = env.read().await;
        let rows: Vec<(String, i64, Option<i64>)> =
            sqlx::query_as("SELECT path, attempts, last_scan_id FROM scan_errors ORDER BY path")
                .fetch_all(&mut conn)
                .await
                .unwrap();
        assert_eq!(rows.len(), 2, "one row per broken path: {rows:?}");
        for (path, attempts, token) in &rows {
            assert_eq!(*attempts, 1, "one run, one attempt ({path})");
            assert_eq!(
                *token,
                Some(scan_ids[0]),
                "every ledger write of this run shares its first root's scan id ({path})"
            );
        }
    }

    // The new-item write path, through the service entry point rather than a
    // seeded verdict: a real generation pass that correctly produces nothing
    // has to leave the marker behind, or the whole cache is unreachable in
    // production.
    #[tokio::test]
    async fn a_real_generation_pass_records_its_marker() {
        // ffprobe is what turns the container into "no video track"; without
        // the toolchain the file never indexes at all.
        if !crate::media_tools::ffmpeg_available() {
            return;
        }
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-no-video-track"]).await;
        fs::write(
            env.media_dirs[0].join("audio-only.mp4"),
            silent_wav_bytes(),
        )
        .unwrap();

        env.service().rescan_folders().await.unwrap();

        let mut conn = env.read().await;
        let items: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM items")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(items, 1, "the file itself indexes fine; only visuals are nothing");
        let rows = visual_attempt_rows(&mut conn).await;
        assert_eq!(
            rows.iter()
                .map(|row| (row.0.as_str(), row.1.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (VisualKind::Frame.as_str(), "none"),
                (VisualKind::Thumbnail.as_str(), "none"),
            ],
            "both kinds concluded a permanent nothing: {rows:?}"
        );
        assert_eq!(thumbnail_count(&mut conn).await, 0);
        // And the outro stage concluded the same nothing from the same
        // metadata, without an ffmpeg start of its own — no third marker, a
        // stored negative, and therefore no re-dispatch on the next scan.
        let sha256: String = sqlx::query_scalar("SELECT sha256 FROM items")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(
            outro_columns(&mut conn, &sha256).await,
            (Some("none/1".to_string()), None)
        );
    }

    // The backfill write path, which is a different record site from the
    // new-item one: an already-indexed file whose visuals are missing is
    // re-attempted by `maybe_dispatch_backfill`, and what *that* pass concludes
    // has to be remembered too — otherwise the second scan re-attempts forever
    // and the cache only ever covers files indexed after it shipped.
    #[tokio::test]
    async fn a_backfill_pass_records_its_marker() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-backfill-marker"]).await;
        fs::write(env.media_dirs[0].join("broken.pdf"), UNRENDERABLE_PDF).unwrap();

        env.scan().await;
        // Drop what the new-item pass concluded. The file is unchanged from
        // here on, so the only thing that can put a marker back is the backfill
        // dispatcher's own record path.
        {
            let mut conn = env.write().await;
            sqlx::query("DELETE FROM storage.visual_attempts")
                .execute(&mut conn)
                .await
                .unwrap();
        }

        let (_, totals) = env.scan().await;
        assert_eq!(
            totals.unchanged_files, 1,
            "the second run must reach the file as unchanged, not as new"
        );
        let mut conn = env.read().await;
        let rows = visual_attempt_rows(&mut conn).await;
        assert_eq!(
            rows.iter().map(|row| row.0.as_str()).collect::<Vec<_>>(),
            vec![VisualKind::Thumbnail.as_str()],
            "the backfill pass records what it concluded: {rows:?}"
        );
    }

    // Frame markers are written but never consulted, because nothing ever asks
    // for frames alone: a video with a thumbnail, a blurhash and no frames is
    // invisible to the dispatcher — no work, and no marker read.
    //
    // The outro question is the third the dispatcher asks
    // (docs/video-outro-detection-design.md §7), and it is *not* about a
    // missing visual, so the second half of this test pins the difference: the
    // very same file, with its verdict cleared, is dispatched — for the probe
    // and for nothing else. That is the backfill, and it is why the frames-only
    // invariant had to be re-stated rather than simply kept.
    #[tokio::test]
    async fn a_video_missing_only_frames_is_not_dispatched() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-frames-only"]).await;
        let clip = env.media_dirs[0].join("clip.mp4");
        // Bytes no tool could read — which is the point: if anything dispatched
        // a generation for this file, it would fail loudly and leave a marker.
        fs::write(&clip, b"not a container, and never opened").unwrap();

        let sha256 = "sha_frames_only";
        seed_video_item(
            &env,
            &clip,
            sha256,
            1,
            Some("LEHV6nWB2yk8pyo0adR*".to_string()),
        )
        .await;
        // Already examined, so the outro question is settled and only the
        // frames gap remains — the state this test has always been about.
        set_outro_kind(&env, sha256, Some("none/1")).await;
        let thumbnail = encode_generated_still(0, &DynamicImage::ImageRgb8(image::RgbImage::new(8, 8)))
            .expect("an 8x8 image encodes");
        call_index_db_writer(&env.index_db, |reply| {
            IndexDbWriterMessage::StoreThumbnails {
                sha256: sha256.to_string(),
                mime_type: "video/mp4".to_string(),
                process_version: THUMBNAIL_PROCESS_VERSION,
                thumbnails: vec![thumbnail.clone()],
                reply,
            }
        })
        .await
        .unwrap();

        let (_, totals) = env.scan().await;
        assert_eq!(totals.unchanged_files, 1, "the walk did reach the file");
        assert_eq!(
            (totals.backfilled_visuals, totals.visuals_suppressed),
            (0, 0),
            "the gap is invisible: not generated, and not suppressed either"
        );
        let mut conn = env.read().await;
        let frames: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM storage.frames")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(frames, 0);
        assert!(
            visual_attempt_rows(&mut conn).await.is_empty(),
            "no pass ran on this file, so nothing concluded anything about it"
        );
        drop(conn);

        // Now the same file with no verdict: the outro question alone is
        // enough to dispatch it, and the probe is all that runs. What it
        // concludes about these bytes is a probe failure either way — `failed`
        // where ffmpeg is installed, `blocked` where it is not — and either is
        // a marker of the new kind and nothing else.
        set_outro_kind(&env, sha256, None).await;
        let (_, totals) = env.scan().await;
        assert_eq!(
            totals.backfilled_visuals, 0,
            "a probe stores no visual, so nothing was backfilled"
        );
        let mut conn = env.read().await;
        let rows = visual_attempt_rows(&mut conn).await;
        assert_eq!(
            rows.iter().map(|row| row.0.as_str()).collect::<Vec<_>>(),
            vec![VisualKind::Outro.as_str()],
            "only the probe ran; the frames gap is still invisible: {rows:?}"
        );
        assert_eq!(
            rows[0].4, OUTRO_DETECTOR_VERSION,
            "the marker carries the detector version, not a generator's"
        );
        assert_eq!(
            outro_columns(&mut conn, sha256).await,
            (None, None),
            "a failed probe is never a verdict"
        );
        let frames: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM storage.frames")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(frames, 0);
    }

    /// Sets (or clears) an item's stored outro verdict, which is what the
    /// dispatcher's outro question reads.
    async fn set_outro_kind(env: &VisualsEnv, sha256: &str, kind: Option<&str>) {
        let mut conn = env.write().await;
        sqlx::query("UPDATE items SET outro_kind = ?1, content_end_ms = NULL WHERE sha256 = ?2")
            .bind(kind)
            .bind(sha256)
            .execute(&mut conn)
            .await
            .unwrap();
    }

    async fn outro_columns(
        conn: &mut sqlx::SqliteConnection,
        sha256: &str,
    ) -> (Option<String>, Option<i64>) {
        sqlx::query_as("SELECT outro_kind, content_end_ms FROM items WHERE sha256 = ?1")
            .bind(sha256)
            .fetch_one(conn)
            .await
            .unwrap()
    }

    // Detection is opt-out and subordinate to `scan_video` (design §8): with
    // either switch off nothing is examined, and an item left `NULL` is picked
    // up naturally once both are on again. The two offs are not the same
    // mechanism — `scan_video` also keeps the extension out of the walk
    // entirely — which is exactly why both are pinned here.
    #[tokio::test]
    async fn outro_detection_is_gated_on_both_config_switches() {
        let test_env = test_data_dir();
        let mut env = visuals_env(test_env.path(), &["media-outro-gate"]).await;
        let clip = env.media_dirs[0].join("clip.mp4");
        fs::write(&clip, b"not a container, and never opened").unwrap();
        let sha256 = "sha_outro_gate";
        seed_video_item(
            &env,
            &clip,
            sha256,
            1,
            Some("LEHV6nWB2yk8pyo0adR*".to_string()),
        )
        .await;
        // A stored thumbnail, so the *only* thing that could dispatch this file
        // is the outro question.
        let thumbnail = encode_generated_still(0, &DynamicImage::ImageRgb8(image::RgbImage::new(8, 8)))
            .expect("an 8x8 image encodes");
        call_index_db_writer(&env.index_db, |reply| {
            IndexDbWriterMessage::StoreThumbnails {
                sha256: sha256.to_string(),
                mime_type: "video/mp4".to_string(),
                process_version: THUMBNAIL_PROCESS_VERSION,
                thumbnails: vec![thumbnail.clone()],
                reply,
            }
        })
        .await
        .unwrap();

        env.config.detect_outros = false;
        env.scan().await;
        {
            let mut conn = env.read().await;
            assert!(
                visual_attempt_rows(&mut conn).await.is_empty(),
                "detection off: nothing examined it, so nothing concluded anything"
            );
            assert_eq!(outro_columns(&mut conn, sha256).await, (None, None));
        }

        // Detection on, video scanning off: subordinate, so still nothing.
        env.config.detect_outros = true;
        env.config.scan_video = false;
        env.scan().await;
        {
            let mut conn = env.read().await;
            assert!(
                visual_attempt_rows(&mut conn).await.is_empty(),
                "video scanning off outranks the detection switch"
            );
            assert_eq!(outro_columns(&mut conn, sha256).await, (None, None));
        }

        // Both on: the item is still `NULL`, so the next scan picks it up with
        // no migration and no separate job.
        env.config.scan_video = true;
        env.scan().await;
        let mut conn = env.read().await;
        let rows = visual_attempt_rows(&mut conn).await;
        assert_eq!(
            rows.iter().map(|row| row.0.as_str()).collect::<Vec<_>>(),
            vec![VisualKind::Outro.as_str()],
            "the probe ran on bytes it cannot read: {rows:?}"
        );
    }

    // The negative cache's consult, for the new kind. A probe that has failed
    // its way to a confirmed verdict must not be re-run at this detector
    // version — one SMB blip mid-backfill would otherwise cost a re-probe of
    // thousands of files on every scan, forever.
    #[tokio::test]
    async fn a_confirmed_outro_failure_suppresses_the_probe() {
        use crate::db::visual_attempts::upsert_visual_attempts;

        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-outro-suppressed"]).await;
        let clip = env.media_dirs[0].join("clip.mp4");
        fs::write(&clip, b"not a container, and never opened").unwrap();
        let sha256 = "sha_outro_suppressed";
        seed_video_item(
            &env,
            &clip,
            sha256,
            1,
            Some("LEHV6nWB2yk8pyo0adR*".to_string()),
        )
        .await;
        let thumbnail = encode_generated_still(0, &DynamicImage::ImageRgb8(image::RgbImage::new(8, 8)))
            .expect("an 8x8 image encodes");
        call_index_db_writer(&env.index_db, |reply| {
            IndexDbWriterMessage::StoreThumbnails {
                sha256: sha256.to_string(),
                mime_type: "video/mp4".to_string(),
                process_version: THUMBNAIL_PROCESS_VERSION,
                thumbnails: vec![thumbnail.clone()],
                reply,
            }
        })
        .await
        .unwrap();
        // Two runs' worth of the same ambiguous failure: confirmed, and
        // therefore suppressing.
        {
            let mut conn = env.write().await;
            for scan in [1_i64, 2] {
                upsert_visual_attempts(
                    &mut conn,
                    &[VisualVerdict::failed(
                        VisualKind::Outro,
                        VisualFailure {
                            kind: ApiErrorKind::Input,
                            skip_after: SKIP_AFTER_AMBIGUOUS,
                            message: "outro probe failed".to_string(),
                        },
                    )
                    .into_record(sha256, "video/mp4", OUTRO_DETECTOR_VERSION)],
                    Some(scan),
                )
                .await
                .unwrap();
            }
        }

        env.scan().await;

        let mut conn = env.read().await;
        let rows = visual_attempt_rows(&mut conn).await;
        assert_eq!(
            rows.iter()
                .map(|row| (row.0.as_str(), row.2))
                .collect::<Vec<_>>(),
            vec![(VisualKind::Outro.as_str(), 2)],
            "a suppressed scan runs no probe, so it confirms nothing: {rows:?}"
        );
        assert_eq!(outro_columns(&mut conn, sha256).await, (None, None));
    }

    // A stored verdict retires the probe's marker, and the two writes are one
    // transaction — the index-side `items` update and the `storage.` delete
    // (design §7.2, mirroring `store_thumbnails`). Exercised through the real
    // writer, which is what supplies the `BEGIN IMMEDIATE`.
    #[tokio::test]
    async fn an_outro_verdict_retires_its_probe_marker() {
        use crate::db::visual_attempts::upsert_visual_attempts;

        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-outro-verdict"]).await;
        let clip = env.media_dirs[0].join("clip.mp4");
        fs::write(&clip, b"not a container, and never opened").unwrap();
        let sha256 = "sha_outro_verdict";
        seed_video_item(&env, &clip, sha256, 1, None).await;
        {
            let mut conn = env.write().await;
            upsert_visual_attempts(
                &mut conn,
                &[VisualVerdict::failed(
                    VisualKind::Outro,
                    VisualFailure {
                        kind: ApiErrorKind::Input,
                        skip_after: SKIP_AFTER_AMBIGUOUS,
                        message: "outro probe failed".to_string(),
                    },
                )
                .into_record(sha256, "video/mp4", OUTRO_DETECTOR_VERSION)],
                Some(1),
            )
            .await
            .unwrap();
        }

        let updated = call_index_db_writer(&env.index_db, |reply| {
            IndexDbWriterMessage::SetOutroVerdict {
                sha256: sha256.to_string(),
                outro_kind: "tiktok_card/1".to_string(),
                content_end_ms: Some(6000),
                reply,
            }
        })
        .await
        .unwrap();
        assert_eq!(updated, 1);

        let mut conn = env.read().await;
        assert_eq!(
            outro_columns(&mut conn, sha256).await,
            (Some("tiktok_card/1".to_string()), Some(6000))
        );
        assert!(
            visual_attempt_rows(&mut conn).await.is_empty(),
            "the marker cannot outlive the verdict that answered it"
        );
    }

    // The clamp, against real media and at the seam that matters: the sampled
    // frames themselves. Design §7 warns that shrinking the interval is not
    // enough — `fps=1/interval` keeps emitting to the end of the stream — so
    // the unclamped half of this test is not a control, it is the bug the
    // decode bound exists to stop.
    #[tokio::test]
    async fn frame_sampling_stays_inside_the_content_range() {
        let dir = tempfile::TempDir::new().unwrap();
        let clip = dir.path().join("card.mp4");
        if !write_clip(&clip, Some(2), None) {
            return;
        }

        let unclamped = extract_video_frames(&clip, 4, 7.0, None).expect("ffmpeg runs");
        assert_eq!(unclamped.len(), 4);
        assert!(
            unclamped.iter().any(corner_is_card),
            "sampling the whole file lands in the card — that is the problem"
        );

        // 5000ms is where the content ends. Every frame must come from before
        // it, and there must still be four of them.
        let clamped = extract_video_frames(&clip, 4, 7.0, Some(5000)).expect("ffmpeg runs");
        assert_eq!(
            clamped.len(),
            4,
            "the interval shrinks with the window, so the count is unchanged"
        );
        assert!(
            !clamped.iter().any(corner_is_card),
            "no sampled frame may come from the card"
        );
    }

    // §7.1's replacement, end to end: an item indexed *before* detection was
    // on already has thumbnails and frames sampled across the card. Turning
    // detection on must not merely record the verdict — the visuals it
    // invalidates have to be rebuilt against the clamped range.
    #[tokio::test]
    async fn a_newly_positive_item_has_its_visuals_replaced() {
        let test_env = test_data_dir();
        let mut env = visuals_env(test_env.path(), &["media-outro-positive"]).await;
        let clip = env.media_dirs[0].join("card.mp4");
        if !write_clip(&clip, Some(2), None) {
            return;
        }

        // Indexed with detection off: the frames are the un-clamped ones.
        env.config.detect_outros = false;
        env.scan().await;
        let sha256: String = {
            let mut conn = env.read().await;
            let sha256: String = sqlx::query_scalar("SELECT sha256 FROM items")
                .fetch_one(&mut conn)
                .await
                .unwrap();
            assert_eq!(outro_columns(&mut conn, &sha256).await, (None, None));
            let frames = stored_frames(&mut conn, &sha256).await;
            assert_eq!(frames.len(), 4);
            assert!(
                frames.iter().any(|frame| corner_is_card(frame)),
                "the premise: a card frame is among the stored ones"
            );
            sha256
        };

        env.config.detect_outros = true;
        let (_, totals) = env.scan().await;
        assert_eq!(
            totals.backfilled_visuals, 1,
            "the replacement is a visuals write, and counted as one"
        );

        let mut conn = env.read().await;
        let (kind, content_end_ms) = outro_columns(&mut conn, &sha256).await;
        assert_eq!(kind.as_deref(), Some("tiktok_card/1"));
        assert_eq!(content_end_ms, Some(5000), "7.0s file, 2.00s card");
        let frames = stored_frames(&mut conn, &sha256).await;
        assert_eq!(frames.len(), 4, "replaced, not appended to");
        assert!(
            !frames.iter().any(|frame| corner_is_card(frame)),
            "every stored frame must now come from the content"
        );
        assert!(
            thumbnail_count(&mut conn).await > 0,
            "the thumbnail is rebuilt from the new frames, not dropped"
        );
    }

    // The other half of §7.1: a negative verdict changes nothing. The existing
    // outputs are already correct, and re-extracting them on the strength of
    // "we looked" would re-decode every video in a library on the first scan
    // after the upgrade.
    #[tokio::test]
    async fn a_negative_verdict_leaves_existing_visuals_untouched() {
        let test_env = test_data_dir();
        let mut env = visuals_env(test_env.path(), &["media-outro-negative"]).await;
        let clip = env.media_dirs[0].join("plain.mp4");
        if !write_clip(&clip, None, None) {
            return;
        }

        env.config.detect_outros = false;
        env.scan().await;
        let (sha256, before) = {
            let mut conn = env.read().await;
            let sha256: String = sqlx::query_scalar("SELECT sha256 FROM items")
                .fetch_one(&mut conn)
                .await
                .unwrap();
            let before: Vec<Vec<u8>> = get_frames_bytes(&mut conn, &sha256).await.unwrap();
            assert_eq!(before.len(), 4);
            (sha256, before)
        };

        env.config.detect_outros = true;
        let (_, totals) = env.scan().await;
        assert_eq!(
            totals.backfilled_visuals, 0,
            "nothing was regenerated: the verdict is the only new thing"
        );

        let mut conn = env.read().await;
        let (kind, content_end_ms) = outro_columns(&mut conn, &sha256).await;
        assert_eq!(kind.as_deref(), Some("none/1"));
        assert_eq!(content_end_ms, None, "no boundary, nothing to clamp to");
        assert_eq!(
            get_frames_bytes(&mut conn, &sha256).await.unwrap(),
            before,
            "the stored frames are the same bytes, not re-encoded ones"
        );
    }

    // The clamp has to survive the verdict being *stored*. A pass only ever
    // produces `content_end_ms` while `outro_kind IS NULL`, so a regeneration
    // after that — storage.db rebuilt, a generator version bumped, a store
    // that failed transiently — would sample the whole file again and put the
    // card back permanently. Here the visuals are dropped after the item has
    // been examined, which is exactly that shape.
    //
    // The second half is design §8's other promise: with detection off the
    // consumer ignores the metadata, so the same regeneration comes back
    // untrimmed. This — a *scan-side* regeneration — is what undoing a false
    // positive actually takes, because it is the only path that replaces
    // `storage.frames`. Turning detection off on the extraction side does not
    // do it: that loader serves the cached frames before it ever reaches the
    // clamp, and §7.1's recovery (erase `item_data`, re-run) leaves the cache
    // alone.
    #[tokio::test]
    async fn a_regeneration_after_the_verdict_was_stored_is_still_clamped() {
        let test_env = test_data_dir();
        let mut env = visuals_env(test_env.path(), &["media-outro-restored"]).await;
        let clip = env.media_dirs[0].join("card.mp4");
        if !write_clip(&clip, Some(2), None) {
            return;
        }

        env.scan().await;
        let sha256: String = {
            let mut conn = env.read().await;
            let sha256: String = sqlx::query_scalar("SELECT sha256 FROM items")
                .fetch_one(&mut conn)
                .await
                .unwrap();
            assert_eq!(
                outro_columns(&mut conn, &sha256).await,
                (Some("tiktok_card/1".to_string()), Some(5000)),
                "the premise: this item is examined and positive"
            );
            sha256
        };

        // Everything a storage.db rebuild or a version bump leaves behind:
        // no visuals, and an `items` row that still carries its verdict.
        async fn drop_visuals(env: &VisualsEnv) {
            let mut conn = env.write().await;
            for statement in [
                "DELETE FROM storage.thumbnails",
                "DELETE FROM storage.frames",
            ] {
                sqlx::query(statement).execute(&mut conn).await.unwrap();
            }
        }
        drop_visuals(&env).await;

        env.scan().await;
        {
            let mut conn = env.read().await;
            let frames = stored_frames(&mut conn, &sha256).await;
            assert_eq!(frames.len(), 4, "the frames were regenerated");
            assert!(
                !frames.iter().any(|frame| corner_is_card(frame)),
                "the stored boundary must clamp a regeneration, not only the \
                 scan that discovered it"
            );
            assert_eq!(
                outro_columns(&mut conn, &sha256).await,
                (Some("tiktok_card/1".to_string()), Some(5000)),
                "and nothing re-examined it"
            );
        }

        // Detection off: the consumer ignores the stored boundary, so the same
        // regeneration samples the whole file again.
        drop_visuals(&env).await;
        env.config.detect_outros = false;
        env.scan().await;
        let mut conn = env.read().await;
        let frames = stored_frames(&mut conn, &sha256).await;
        assert_eq!(frames.len(), 4);
        assert!(
            frames.iter().any(|frame| corner_is_card(frame)),
            "with detection off a scan-side regeneration brings the untrimmed \
             visuals back — the storage.frames replacement undoing a false \
             positive has to go through"
        );
        assert_eq!(
            outro_columns(&mut conn, &sha256).await,
            (Some("tiktok_card/1".to_string()), Some(5000)),
            "off skips future examinations; it never erases a stored one"
        );
    }

    async fn stored_frames(conn: &mut sqlx::SqliteConnection, sha256: &str) -> Vec<DynamicImage> {
        get_frames_bytes(conn, sha256)
            .await
            .unwrap()
            .iter()
            .map(|bytes| decode_image_bytes(bytes).expect("a stored frame decodes"))
            .collect()
    }

    // The pure half of the clamp: which window frame sampling draws from, and
    // whether it owes ffmpeg a decode bound at all.
    #[test]
    fn the_sampling_window_follows_the_content_boundary() {
        // Never examined, or examined and negative: the whole file, unbounded.
        assert_eq!(frame_sampling_window(7.0, None), (7.0, false));
        // A boundary inside the file shortens the window *and* asks for the
        // bound; the interval then spreads four frames over the content.
        assert_eq!(frame_sampling_window(7.0, Some(5000)), (5.0, true));
        assert_eq!(frame_sampling_window(7.0, Some(5000)).0 / 4.0, 1.25);
        // A boundary at or past the duration clamps nothing — including the
        // exact-equality case, where a bound would only cost a decode option.
        assert_eq!(frame_sampling_window(7.0, Some(7000)), (7.0, false));
        assert_eq!(frame_sampling_window(7.0, Some(9000)), (7.0, false));
        // Nonsense is absent behaviour, never wrong behaviour.
        assert_eq!(frame_sampling_window(7.0, Some(0)), (7.0, false));
        assert_eq!(frame_sampling_window(7.0, Some(-1)), (7.0, false));
    }

    // The metadata shortcut: a `video/` container ffprobe found no video
    // stream in has no card to carry, and probing it would burn two ffmpeg
    // starts before the ledger settled while the dispatcher re-asked on every
    // scan in between.
    #[test]
    fn a_container_with_no_video_stream_is_answered_without_a_probe() {
        assert!(outro_needs_probe(Some(1)));
        assert!(!outro_needs_probe(Some(0)));
        // `extract_item_metadata` leaves the column unset rather than storing a
        // zero, which is the shape this really has to handle.
        assert!(!outro_needs_probe(None));

        let pass = run_outro_pass(Path::new("never-opened.mp4"), Some(10.0), Some(0), None);
        assert_eq!(
            pass.record,
            Some(OutroRecord {
                kind: "none/1".to_string(),
                content_end_ms: None,
            })
        );
        assert!(pass.verdict.is_none(), "nothing ran, so nothing failed");
        assert_eq!(pass.content_end_ms(), None);
    }

    // The gate in front of the whole stage: off, or not a video, and no probe
    // is even considered.
    #[test]
    fn the_outro_stage_is_skipped_when_it_is_off_or_the_file_is_not_a_video() {
        let meta = |mime: &str| ItemScanMeta {
            md5: "md5".to_string(),
            mime_type: mime.to_string(),
            width: Some(576),
            height: Some(1024),
            rotation: None,
            duration: Some(7.0),
            audio_tracks: Some(1),
            video_tracks: Some(1),
            subtitle_tracks: Some(0),
            video_codec: Some("h264".to_string()),
            audio_codec: Some("aac".to_string()),
        };
        let path = Path::new("never-opened.mp4");
        assert!(
            outro_pass_for(path, "video/mp4", &meta("video/mp4"), false)
                .record
                .is_none()
        );
        assert!(
            outro_pass_for(path, "image/png", &meta("image/png"), true)
                .record
                .is_none()
        );
    }

    /// Indexes a file as a video with the given track count, without touching
    /// any toolchain: indexing a real video needs ffprobe, and what these tests
    /// are about is the dispatcher's decision, not the probe.
    async fn seed_video_item(
        env: &VisualsEnv,
        path: &Path,
        sha256: &str,
        video_tracks: i64,
        blurhash: Option<String>,
    ) {
        let (last_modified, file_size) = get_last_modified_time_and_size(path).unwrap();
        let scan_time = current_iso_timestamp();
        let scan_id = call_index_db_writer(&env.index_db, |reply| {
            IndexDbWriterMessage::AddFileScan {
                scan_time: scan_time.clone(),
                path: env.media_dirs[0].to_string_lossy().to_string(),
                reply,
            }
        })
        .await
        .unwrap();
        call_index_db_writer(&env.index_db, |reply| IndexDbWriterMessage::UpdateFileData {
            time_added: scan_time.clone(),
            scan_id,
            data: FileScanData {
                sha256: sha256.to_string(),
                last_modified: last_modified.clone(),
                path: path.to_string_lossy().to_string(),
                new_file_hash: true,
                file_size: Some(file_size),
                item_metadata: Some(ItemScanMeta {
                    md5: "md5".to_string(),
                    mime_type: "video/mp4".to_string(),
                    width: Some(8),
                    height: Some(8),
                    rotation: None,
                    duration: Some(10.0),
                    audio_tracks: Some(1),
                    video_tracks: Some(video_tracks),
                    subtitle_tracks: Some(0),
                    // Deliberately unprobed: these items stand in for a library
                    // indexed before the codec columns existed, which is what
                    // the backfill dispatcher is about.
                    video_codec: None,
                    audio_codec: None,
                }),
                blurhash: blurhash.clone(),
            },
            reply,
        })
        .await
        .unwrap();
    }

    // The case an existing library is made of: a video with no video track that
    // was indexed *before* this cache existed, so the new-item path never
    // recorded its nothing. The backfill dispatcher decides the same nothing
    // from the same metadata on every scan — and used to return without
    // recording it, which left exactly these items being re-decided forever.
    // The first scan writes the verdict for both kinds; the second one reads it
    // and does not dispatch at all.
    #[tokio::test]
    async fn a_track_less_video_already_in_the_index_is_recorded_then_suppressed() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-no-track-backfill"]).await;
        let clip = env.media_dirs[0].join("clip.mp4");
        // Never opened by anything: the verdict comes from indexed metadata.
        fs::write(&clip, b"not a container, and never opened").unwrap();
        // No blurhash either, which is the honest state for a video that never
        // produced a visual — and what keeps the dispatcher interested in this
        // file after the thumbnail question is settled.
        seed_video_item(&env, &clip, "sha_no_track", 0, None).await;

        let (_, totals) = env.scan().await;
        assert_eq!(totals.unchanged_files, 1, "the walk did reach the file");
        assert_eq!(
            totals.visuals_suppressed, 0,
            "the first scan is the one that decides; nothing to suppress yet"
        );
        let mut conn = env.read().await;
        let rows = visual_attempt_rows(&mut conn).await;
        assert_eq!(
            rows.iter()
                .map(|row| (row.0.as_str(), row.1.as_str(), row.2))
                .collect::<Vec<_>>(),
            vec![
                (VisualKind::Frame.as_str(), "none", 1),
                (VisualKind::Thumbnail.as_str(), "none", 1),
            ],
            "the backfill pass owes the same verdict the new-item path writes: {rows:?}"
        );
        drop(conn);

        let (_, totals) = env.scan().await;
        assert_eq!(
            totals.visuals_suppressed, 1,
            "the second scan reads the verdict instead of re-deciding it"
        );
        let mut conn = env.read().await;
        assert_eq!(thumbnail_count(&mut conn).await, 0);
        let rows = visual_attempt_rows(&mut conn).await;
        assert_eq!(
            rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            vec![1, 1],
            "a suppressed scan runs no pass, so it confirms nothing: {rows:?}"
        );
    }

    async fn codec_columns(
        conn: &mut sqlx::SqliteConnection,
        sha256: &str,
    ) -> (Option<String>, Option<String>) {
        sqlx::query_as("SELECT video_codec, audio_codec FROM items WHERE sha256 = ?1")
            .bind(sha256)
            .fetch_one(conn)
            .await
            .unwrap()
    }

    /// Stamps an item's codec columns directly, standing in for "a previous
    /// scan already probed this file".
    async fn set_item_codec_columns(env: &VisualsEnv, sha256: &str, video_codec: Option<&str>) {
        let mut conn = env.write().await;
        sqlx::query("UPDATE items SET video_codec = ?1 WHERE sha256 = ?2")
            .bind(video_codec)
            .bind(sha256)
            .execute(&mut conn)
            .await
            .unwrap();
    }

    // The scan path, against real media: the codecs ffprobe reports at the
    // metadata phase reach the `items` row, verbatim and per stream. Two
    // encoders because a single one cannot tell "the column is written" from
    // "the column is hardcoded to what libx264 happens to produce" — and hevc
    // is the case the whole feature exists for (an HEVC-in-mp4 file passes
    // today's container-mime check and then plays as a black frame).
    #[tokio::test]
    async fn scanned_videos_record_their_stream_codecs() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-codecs"]).await;
        let plain = env.media_dirs[0].join("plain.mp4");
        if !write_clip(&plain, None, None) {
            return;
        }
        // A build without libx265 skips that half rather than failing; the
        // h264 half still pins the wiring. Staged outside the scanned folder
        // and moved in only once it exists: a failed encode leaves a
        // zero-length file behind, and inside the media directory the scan
        // would walk straight into it.
        let staged = env.root.join("media-codecs-hevc.mp4");
        let hevc = env.media_dirs[0].join("hevc.mp4");
        let hevc_written = write_clip(&staged, None, Some("libx265"));
        if hevc_written {
            fs::rename(&staged, &hevc).unwrap();
        } else {
            let _ = fs::remove_file(&staged);
        }

        env.scan().await;

        let mut conn = env.read().await;
        async fn sha_of(conn: &mut sqlx::SqliteConnection, name: &str) -> String {
            sqlx::query_scalar::<_, String>(
                "SELECT items.sha256 FROM items \
                 JOIN files ON files.item_id = items.id WHERE files.filename = ?1",
            )
            .bind(name)
            .fetch_one(conn)
            .await
            .unwrap()
        }
        let plain_sha = sha_of(&mut conn, "plain.mp4").await;
        assert_eq!(
            codec_columns(&mut conn, &plain_sha).await,
            (Some("h264".to_string()), None),
            "the video codec is stored verbatim, and a stream-less audio \
             column stays NULL rather than claiming 'none'"
        );

        if hevc_written {
            let hevc_sha = sha_of(&mut conn, "hevc.mp4").await;
            assert_eq!(
                codec_columns(&mut conn, &hevc_sha).await.0,
                Some("hevc".to_string()),
                "the column follows the stream, not the container mime"
            );
        }
    }

    // The backfill's whole point: an item indexed before these columns existed
    // is a video with every visual it needs, and the codec question is the only
    // thing left that can dispatch it. A stored thumbnail, a stored blurhash
    // and a stored outro verdict silence the other three questions, so a
    // written codec here can only have come from the fourth.
    #[tokio::test]
    async fn a_pending_video_gets_its_codecs_backfilled() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-codec-backfill"]).await;
        let clip = env.media_dirs[0].join("clip.mp4");
        if !write_clip(&clip, None, None) {
            return;
        }
        let sha256 = "sha_codec_pending";
        seed_video_item(
            &env,
            &clip,
            sha256,
            1,
            Some("LEHV6nWB2yk8pyo0adR*".to_string()),
        )
        .await;
        set_outro_kind(&env, sha256, Some("none/1")).await;
        let thumbnail = encode_generated_still(0, &DynamicImage::ImageRgb8(image::RgbImage::new(8, 8)))
            .expect("an 8x8 image encodes");
        call_index_db_writer(&env.index_db, |reply| {
            IndexDbWriterMessage::StoreThumbnails {
                sha256: sha256.to_string(),
                mime_type: "video/mp4".to_string(),
                process_version: THUMBNAIL_PROCESS_VERSION,
                thumbnails: vec![thumbnail.clone()],
                reply,
            }
        })
        .await
        .unwrap();
        {
            let mut conn = env.read().await;
            assert_eq!(codec_columns(&mut conn, sha256).await, (None, None));
        }

        env.scan().await;

        let mut conn = env.read().await;
        assert_eq!(
            codec_columns(&mut conn, sha256).await,
            (Some("h264".to_string()), None)
        );
    }

    /// The state a pre-upgrade item is in: coded dimensions and a NULL
    /// `rotation`. Applied on top of one of the `seed_*` helpers, which write
    /// placeholder dimensions of their own.
    async fn set_coded_dimensions(env: &VisualsEnv, sha256: &str, width: i64, height: i64) {
        let mut conn = crate::db::open_index_db_write_no_user_data(&env.index_db)
            .await
            .expect("the index opens for writing");
        sqlx::query("UPDATE items SET width = ?1, height = ?2, rotation = NULL WHERE sha256 = ?3")
            .bind(width)
            .bind(height)
            .bind(sha256)
            .execute(&mut conn)
            .await
            .expect("the seed updates");
    }

    async fn geometry_columns(
        env: &VisualsEnv,
        sha256: &str,
    ) -> (Option<i64>, Option<i64>, Option<i64>) {
        let mut conn = env.read().await;
        sqlx::query_as("SELECT width, height, rotation FROM items WHERE sha256 = ?1")
            .bind(sha256)
            .fetch_one(&mut conn)
            .await
            .unwrap()
    }

    async fn stored_blurhash(env: &VisualsEnv, sha256: &str) -> Option<String> {
        let mut conn = env.read().await;
        sqlx::query_scalar("SELECT blurhash FROM items WHERE sha256 = ?1")
            .bind(sha256)
            .fetch_one(&mut conn)
            .await
            .unwrap()
    }

    /// A clip whose stream carries a rotating display matrix — the shape of
    /// every portrait phone capture. `false` where this machine's ffmpeg
    /// cannot produce one, checked with an ffprobe invocation of its own so a
    /// toolchain gap skips the test instead of failing it.
    fn write_rotated_clip(path: &Path, seed: &Path) -> bool {
        if !crate::media_tools::ffmpeg_available() {
            return false;
        }
        let status = Command::new(crate::media_tools::ffmpeg())
            .args(["-y", "-v", "error", "-display_rotation", "-90", "-i"])
            .arg(seed)
            .args(["-c", "copy"])
            .arg(path)
            .status();
        if !matches!(status, Ok(status) if status.success()) {
            return false;
        }
        let probed = Command::new(crate::media_tools::ffprobe())
            .args([
                "-v",
                "error",
                "-show_entries",
                "stream_side_data=rotation",
                "-of",
                "csv=p=0",
            ])
            .arg(path)
            .output();
        matches!(probed, Ok(output) if String::from_utf8_lossy(&output.stdout).contains("-90"))
    }

    // The backfill's whole point (docs/display-dimensions-design.md §4): an
    // item indexed before orientation was read is an image with every visual
    // it needs, so the rotation question is the only thing that can dispatch
    // it — and one scan later the columns describe the picture.
    //
    // The blurhash comes with it. This image is served from its original file,
    // so there is no stored thumbnail to replace, but its blurhash was
    // computed from the same unrotated decode and is just as stale (§4.1).
    #[tokio::test]
    async fn a_rotated_image_already_in_the_index_is_corrected_by_the_backfill() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-rotation-backfill"]).await;
        let photo = env.media_dirs[0].join("portrait.jpg");
        fs::write(&photo, jpeg_with_exif_orientation(64, 32, 6)).unwrap();
        let sha256 = "sha_rotation_pending";
        seed_image_item(&env, &photo, sha256, "image/jpeg", None).await;
        set_coded_dimensions(&env, sha256, 64, 32).await;
        let seeded_blurhash = stored_blurhash(&env, sha256).await;

        env.scan().await;

        assert_eq!(
            geometry_columns(&env, sha256).await,
            (Some(32), Some(64), Some(90)),
            "the coded 64x32 is the picture's 32x64, and the turn is stamped"
        );
        assert_ne!(
            stored_blurhash(&env, sha256).await,
            seeded_blurhash,
            "the blurhash described the unrotated decode and had to be replaced"
        );
    }

    // The termination half, and the only place in the scan where getting it
    // wrong is destructive: the write is not idempotent, so a second pass over
    // a stamped item would transpose its dimensions straight back. The file on
    // disk is genuinely rotated on purpose — if any scan re-measured it, 90
    // would appear in the column and the dimensions would flip.
    #[tokio::test]
    async fn a_stamped_rotation_is_never_measured_again() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-rotation-stamped"]).await;
        let photo = env.media_dirs[0].join("portrait.jpg");
        fs::write(&photo, jpeg_with_exif_orientation(64, 32, 6)).unwrap();
        let sha256 = "sha_rotation_stamped";
        seed_image_item(&env, &photo, sha256, "image/jpeg", None).await;
        set_coded_dimensions(&env, sha256, 32, 64).await;
        {
            let mut conn = crate::db::open_index_db_write_no_user_data(&env.index_db).await.unwrap();
            sqlx::query("UPDATE items SET rotation = 90 WHERE sha256 = ?1")
                .bind(sha256)
                .execute(&mut conn)
                .await
                .unwrap();
        }

        env.scan().await;
        env.scan().await;

        assert_eq!(
            geometry_columns(&env, sha256).await,
            (Some(32), Some(64), Some(90)),
            "two more scans must leave an already-answered item exactly as it was"
        );
    }

    // The video half (§1.2): the columns are corrected and nothing is
    // regenerated, because ffmpeg autorotates on decode and this item's
    // frames have always been the picture.
    #[tokio::test]
    async fn a_rotated_video_already_in_the_index_is_corrected_by_the_backfill() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-rotation-video"]).await;
        let seed = env.media_dirs[0].join("seed.mp4");
        if !write_clip(&seed, None, None) {
            return;
        }
        let clip = env.media_dirs[0].join("clip.mp4");
        if !write_rotated_clip(&clip, &seed) {
            return;
        }
        // The seed itself would be scanned as a second item; it has served its
        // purpose and must not linger in the media directory.
        fs::remove_file(&seed).unwrap();
        let sha256 = "sha_rotation_video";
        seed_video_item(
            &env,
            &clip,
            sha256,
            1,
            Some("LEHV6nWB2yk8pyo0adR*".to_string()),
        )
        .await;
        set_outro_kind(&env, sha256, Some("none/1")).await;
        // `write_clip`'s own geometry, which `seed_video_item` does not record:
        // what a pre-upgrade item holds is the *coded* size, and a square one
        // would make the assertion below vacuous.
        set_coded_dimensions(&env, sha256, 576, 1024).await;

        env.scan().await;

        assert_eq!(
            geometry_columns(&env, sha256).await,
            (Some(1024), Some(576), Some(90)),
            "a -90 display matrix is 90 clockwise, and transposes the columns"
        );
    }

    /// Indexes a file as an image with the given mime and duration, without
    /// running a scan: these items stand in for a library indexed before
    /// animation lengths were measured, which is what the fifth dispatcher
    /// question is about. The blurhash and dimensions are stored so the
    /// visuals questions are all answered — a small image with recorded
    /// dimensions is served directly and needs no thumbnail — leaving the
    /// animation question as the only thing that can dispatch.
    async fn seed_image_item(
        env: &VisualsEnv,
        path: &Path,
        sha256: &str,
        mime_type: &str,
        duration: Option<f64>,
    ) {
        let (last_modified, file_size) = get_last_modified_time_and_size(path).unwrap();
        let scan_time = current_iso_timestamp();
        let scan_id = call_index_db_writer(&env.index_db, |reply| {
            IndexDbWriterMessage::AddFileScan {
                scan_time: scan_time.clone(),
                path: env.media_dirs[0].to_string_lossy().to_string(),
                reply,
            }
        })
        .await
        .unwrap();
        call_index_db_writer(&env.index_db, |reply| IndexDbWriterMessage::UpdateFileData {
            time_added: scan_time.clone(),
            scan_id,
            data: FileScanData {
                sha256: sha256.to_string(),
                last_modified: last_modified.clone(),
                path: path.to_string_lossy().to_string(),
                new_file_hash: true,
                file_size: Some(file_size),
                item_metadata: Some(ItemScanMeta {
                    md5: "md5".to_string(),
                    mime_type: mime_type.to_string(),
                    width: Some(1),
                    height: Some(1),
                    rotation: None,
                    duration,
                    audio_tracks: None,
                    video_tracks: None,
                    subtitle_tracks: None,
                    video_codec: None,
                    audio_codec: None,
                }),
                blurhash: Some("LEHV6nWB2yk8pyo0adR*".to_string()),
            },
            reply,
        })
        .await
        .unwrap();
    }

    async fn stored_duration(env: &VisualsEnv, sha256: &str) -> Option<f64> {
        let mut conn = env.read().await;
        sqlx::query_scalar("SELECT duration FROM items WHERE sha256 = ?1")
            .bind(sha256)
            .fetch_one(&mut conn)
            .await
            .unwrap()
    }

    // The new-item path (docs/animated-image-spans-design.md §3): an animated
    // GIF walks in with its measured length, a single-frame one with the 0.0
    // verdict, and an image outside the three containers with NULL — all off
    // the metadata phase, with no ffmpeg and no backfill involved.
    #[tokio::test]
    async fn a_new_animated_gif_is_indexed_with_its_animation_length() {
        use crate::media_tools::animation::tests::gif_bytes;

        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-animation-new"]).await;
        fs::write(env.media_dirs[0].join("anim.gif"), gif_bytes(&[25, 50])).unwrap();
        fs::write(env.media_dirs[0].join("still.gif"), gif_bytes(&[25])).unwrap();
        image::RgbImage::new(4, 4)
            .save(env.media_dirs[0].join("plain.png"))
            .unwrap();

        env.scan().await;

        let mut conn = env.read().await;
        let rows: Vec<(String, Option<f64>)> = sqlx::query_as(
            "SELECT files.filename, items.duration FROM items \
             JOIN files ON files.item_id = items.id ORDER BY files.filename",
        )
        .fetch_all(&mut conn)
        .await
        .unwrap();
        assert_eq!(
            rows,
            vec![
                ("anim.gif".to_string(), Some(0.75)),
                ("plain.png".to_string(), None),
                ("still.gif".to_string(), Some(0.0)),
            ],
            "the measured length, the still verdict, and the untouched NULL"
        );
    }

    // The backfill's whole point (design §4): an item indexed before the
    // measurement existed is an image with every visual it needs, so the
    // animation question is the only thing left that can dispatch it — and
    // one scan later the column carries what the file's own structure says.
    #[tokio::test]
    async fn an_unmeasured_gif_already_in_the_index_gets_its_length_backfilled() {
        use crate::media_tools::animation::tests::gif_bytes;

        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-animation-backfill"]).await;
        let gif = env.media_dirs[0].join("anim.gif");
        fs::write(&gif, gif_bytes(&[25, 50])).unwrap();
        let sha256 = "sha_animation_pending";
        seed_image_item(&env, &gif, sha256, "image/gif", None).await;
        assert_eq!(stored_duration(&env, sha256).await, None);

        env.scan().await;

        assert_eq!(stored_duration(&env, sha256).await, Some(0.75));
    }

    // The termination half: 0.0 is a *verdict*, so a measured-still item is
    // never re-dispatched. The file on disk is genuinely animated on purpose
    // — if any scan re-measured it, the animated answer would show up in the
    // column, so the 0.0 surviving a rescan is the proof that both guards
    // (the dispatch predicate and the writer's `duration IS NULL`) hold.
    #[tokio::test]
    async fn a_measured_still_verdict_is_never_remeasured() {
        use crate::media_tools::animation::tests::gif_bytes;

        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-animation-verdict"]).await;
        let gif = env.media_dirs[0].join("anim.gif");
        fs::write(&gif, gif_bytes(&[25, 50])).unwrap();
        let sha256 = "sha_animation_still";
        seed_image_item(&env, &gif, sha256, "image/gif", Some(0.0)).await;

        env.scan().await;

        assert_eq!(stored_duration(&env, sha256).await, Some(0.0));
    }

    /// A `video/`-typed container holding nothing but an audio stream — the
    /// shape a video-typed audio-only file has, and the one the removed
    /// metadata shortcut used to answer without opening. `false` where this
    /// machine cannot build it.
    fn write_audio_only_mp4(path: &Path) -> bool {
        if !crate::media_tools::ffmpeg_available() {
            return false;
        }
        let status = Command::new(crate::media_tools::ffmpeg())
            .args([
                "-y",
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:duration=2",
                "-c:a",
                "aac",
            ])
            .arg(path)
            .status();
        matches!(status, Ok(status) if status.success())
    }

    /// A copy of `seed` carrying a still image as an `attached_pic` video
    /// stream, which is what cover art is in every container that holds it.
    /// The mp4 muxer puts the art *after* the real stream — exactly where a
    /// "last video stream wins" reading picks it up.
    fn write_clip_with_cover_art(seed: &Path, path: &Path) -> bool {
        if !crate::media_tools::ffmpeg_available() {
            return false;
        }
        let status = Command::new(crate::media_tools::ffmpeg())
            .args(["-y", "-v", "error", "-i"])
            .arg(seed)
            .args([
                "-f",
                "lavfi",
                "-i",
                "color=c=red:s=32x32:d=1:r=1",
                "-map",
                "0:v:0",
                "-map",
                "1:v:0",
                "-c:v:0",
                "copy",
                "-c:v:1",
                "mjpeg",
                "-disposition:v:1",
                "attached_pic",
            ])
            .arg(path)
            .status();
        matches!(status, Ok(status) if status.success())
    }

    // The trackless case, which has no shortcut on purpose: a `video/` item the
    // index records no video track for still goes through the probe, because
    // only the probe can name the *audio* codec such a container carries.
    // Answering it from the stored track count would write `'none'` with a NULL
    // audio column and terminate the backfill on half an answer — and would
    // disagree with what the new-item path records for the very same bytes.
    #[tokio::test]
    async fn a_track_less_video_is_probed_for_its_audio_codec() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-codec-trackless"]).await;
        let clip = env.media_dirs[0].join("clip.mp4");
        if !write_audio_only_mp4(&clip) {
            return;
        }
        let sha256 = "sha_codec_trackless";
        seed_video_item(&env, &clip, sha256, 0, None).await;

        env.scan().await;

        let mut conn = env.read().await;
        assert_eq!(
            codec_columns(&mut conn, sha256).await,
            (Some(CODEC_NONE.to_string()), Some("aac".to_string())),
            "the probe ran: `none` for the video stream that is not there, \
             beside the audio codec nothing but ffprobe could have named"
        );
    }

    // Cover art is not video. An mp4 can carry a still image as an
    // `attached_pic` video stream, and every column derived from "the video
    // stream" has to come from the moving one — the codec a client decides
    // playability from, and the dimensions beside it.
    #[tokio::test]
    async fn cover_art_is_never_taken_for_the_video_stream() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-codec-cover-art"]).await;
        // Both built outside the scanned folder: the seed is not media under
        // test, and a fixture the toolchain failed to produce must not be left
        // where the walk can trip over it.
        let seed = env.root.join("media-codec-cover-art-seed.mp4");
        if !write_clip(&seed, None, None) {
            return;
        }
        let staged = env.root.join("media-codec-cover-art-staged.mp4");
        if !write_clip_with_cover_art(&seed, &staged) {
            let _ = fs::remove_file(&staged);
            return;
        }
        let clip = env.media_dirs[0].join("with-art.mp4");
        fs::rename(&staged, &clip).unwrap();

        env.scan().await;

        let mut conn = env.read().await;
        let (sha256, width, height): (String, Option<i64>, Option<i64>) = sqlx::query_as(
            "SELECT items.sha256, items.width, items.height FROM items \
             JOIN files ON files.item_id = items.id WHERE files.filename = ?1",
        )
        .bind("with-art.mp4")
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(
            codec_columns(&mut conn, &sha256).await.0,
            Some("h264".to_string()),
            "the moving stream, not the mjpeg still muxed beside it"
        );
        assert_eq!(
            (width, height),
            (Some(576), Some(1024)),
            "and the dimensions come from the same stream the codec did, not \
             from the 32x32 thumbnail"
        );
    }

    // The selection rule, at the one function that decides it. Cover art is a
    // video stream in every container that carries it, so "the video track"
    // cannot simply be "a video stream".
    #[test]
    fn the_video_track_is_the_first_stream_that_is_not_cover_art() {
        let stream = |codec_type: &str, attached_pic: Option<i64>| FfprobeStream {
            index: None,
            codec_type: Some(codec_type.to_string()),
            codec_name: None,
            duration: None,
            width: None,
            height: None,
            side_data_list: None,
            disposition: attached_pic.map(|attached_pic| FfprobeDisposition {
                attached_pic: Some(attached_pic),
            }),
            tags: None,
        };

        // First wins among several real video streams.
        assert_eq!(
            content_video_stream(&[
                stream("audio", Some(0)),
                stream("video", Some(0)),
                stream("video", Some(0)),
            ]),
            Some(1)
        );
        // Cover art is skipped wherever it sits.
        assert_eq!(
            content_video_stream(&[stream("video", Some(1)), stream("video", Some(0))]),
            Some(1)
        );
        assert_eq!(
            content_video_stream(&[stream("video", Some(0)), stream("video", Some(1))]),
            Some(0)
        );
        // Nothing but cover art is no video track at all, which for a `video/`
        // mime is the `'none'` the backfill terminates on.
        assert_eq!(
            content_video_stream(&[stream("audio", Some(0)), stream("video", Some(1))]),
            None
        );
        // A build (or a demuxer) that reports no disposition at all reads as
        // "not cover art": discarding a real video stream is the worse half of
        // that guess.
        assert_eq!(content_video_stream(&[stream("video", None)]), Some(0));
    }

    // Termination: a stored `video_codec` is the whole backfill predicate, so
    // an item that carries one is never asked again. The stored value is a lie
    // about this file on purpose — a second probe would overwrite it with the
    // truth, which is the only way to see the re-dispatch from the outside.
    #[tokio::test]
    async fn an_already_probed_video_is_never_probed_again() {
        let test_env = test_data_dir();
        let env = visuals_env(test_env.path(), &["media-codec-settled"]).await;
        let clip = env.media_dirs[0].join("clip.mp4");
        if !write_clip(&clip, None, None) {
            return;
        }
        let sha256 = "sha_codec_settled";
        seed_video_item(
            &env,
            &clip,
            sha256,
            1,
            Some("LEHV6nWB2yk8pyo0adR*".to_string()),
        )
        .await;
        set_item_codec_columns(&env, sha256, Some(CODEC_NONE)).await;

        env.scan().await;

        let mut conn = env.read().await;
        assert_eq!(
            codec_columns(&mut conn, sha256).await.0,
            Some(CODEC_NONE.to_string()),
            "an h264 stream sits in this file; only a re-probe could have \
             replaced the stored answer"
        );
    }

    // The sentinel rules, at the one function that decides them for both the
    // new-item and the backfill path.
    #[test]
    fn codec_sentinels_follow_the_streams_and_the_mime() {
        let video = |codec_name: Option<&str>| VideoTrack {
            duration: 7.0,
            width: Some(8),
            height: Some(8),
            rotation: 0,
            codec_name: codec_name.map(str::to_string),
        };
        let audio = |codec_name: Option<&str>| AudioTrack {
            duration: 7.0,
            codec_name: codec_name.map(str::to_string),
        };
        let info = |video_track, audio_tracks| MediaInfo {
            audio_tracks,
            video_track,
            subtitle_tracks: Vec::new(),
        };

        // A named stream is stored verbatim, and the audio column takes the
        // *first* stream of several.
        assert_eq!(
            media_codecs(
                &info(
                    Some(video(Some("hevc"))),
                    vec![audio(Some("aac")), audio(Some("ac3"))]
                ),
                "video/mp4"
            ),
            (Some("hevc".to_string()), Some("aac".to_string()))
        );
        // A stream ffprobe named no codec for exists all the same: `unknown`,
        // never `none`.
        assert_eq!(
            media_codecs(&info(Some(video(None)), vec![audio(None)]), "video/mp4"),
            (
                Some(CODEC_UNKNOWN.to_string()),
                Some(CODEC_UNKNOWN.to_string())
            )
        );
        // No video stream in a video container: `none`, which is what
        // terminates the backfill. No audio stream: NULL, the accepted
        // ambiguity.
        assert_eq!(
            media_codecs(&info(None, Vec::new()), "video/mp4"),
            (Some(CODEC_NONE.to_string()), None)
        );
        // The same container with audio in it records that audio codec beside
        // the sentinel. Both halves of one answer, and the reason there is no
        // metadata shortcut: nothing short of the probe knows this name.
        assert_eq!(
            media_codecs(&info(None, vec![audio(Some("aac"))]), "video/mp4"),
            (Some(CODEC_NONE.to_string()), Some("aac".to_string()))
        );
        // An audio file records its audio codec and leaves the video column
        // NULL: `'none'` there would claim it was examined as a video, and
        // that column is the backfill's termination predicate.
        assert_eq!(
            media_codecs(&info(None, vec![audio(Some("mp3"))]), "audio/mpeg"),
            (None, Some("mp3".to_string()))
        );
    }

    // What a pass may and may not conclude. The line that keeps this table
    // small is the served-directly predicate: most images legitimately have no
    // stored thumbnail, and marking every one of them would put a row in the
    // negative cache for the majority of a library.
    #[test]
    fn only_a_real_nothing_earns_a_marker() {
        let dir = tempfile::TempDir::new().unwrap();
        let meta = |video_tracks: i64, duration: f64| ItemScanMeta {
            md5: "md5".to_string(),
            mime_type: "video/mp4".to_string(),
            width: None,
            height: None,
            rotation: None,
            duration: Some(duration),
            audio_tracks: Some(1),
            video_tracks: Some(video_tracks),
            subtitle_tracks: Some(0),
            video_codec: Some("h264".to_string()),
            audio_codec: Some("aac".to_string()),
        };

        // A video with no video track: both kinds conclude a permanent
        // nothing, without ffmpeg ever being started.
        let produced = build_new_item_renditions(
            &dir.path().join("audio-only.mp4"),
            "video/mp4",
            &meta(0, 10.0),
            None,
            FormatPolicy::default(),
        )
        .expect("a missing video track is not a failure");
        assert_eq!(produced.nothing, vec![VisualKind::Thumbnail, VisualKind::Frame]);
        assert!(produced.thumbnails.is_empty() && produced.frames.is_empty());

        // A type with no generator at all: also a permanent nothing, but only
        // for the kind that could have existed.
        let plain = dir.path().join("notes.txt");
        fs::write(&plain, b"hello").unwrap();
        let produced = build_new_item_renditions(&plain, "text/plain", &meta(0, 0.0), None, FormatPolicy::default())
            .expect("an unsupported type is not a failure");
        assert_eq!(produced.nothing, vec![VisualKind::Thumbnail]);

        // An image served from its original file: nothing stored, and no
        // marker — the predicate already answers this without decoding.
        let small = dir.path().join("small.png");
        image::RgbImage::new(8, 8).save(&small).unwrap();
        let produced = build_new_item_renditions(&small, "image/png", &meta(0, 0.0), None, FormatPolicy::default())
            .expect("a small image is not a failure");
        assert!(
            produced.nothing.is_empty() && produced.thumbnails.is_empty(),
            "the served-directly predicate is the cache for these"
        );

        // And an image that does get a thumbnail concludes nothing at all.
        // 27 MB uncompressed, past the display rule's byte bound.
        let large = dir.path().join("large.bmp");
        image::RgbImage::new(9000, 1000).save(&large).unwrap();
        let produced = build_new_item_renditions(&large, "image/bmp", &meta(0, 0.0), None, FormatPolicy::default()).unwrap();
        assert!(produced.nothing.is_empty() && produced.thumbnails.len() == 1);
        // ... and carries the grid tiers of the same decode. A 9:1 strip is
        // cropped rather than resized whole, so every tier exists.
        let tiers = produced.tiers.expect("an image pass always plans tiers");
        assert_eq!(
            tiers
                .iter()
                .map(|tier| (tier.tier.as_str(), tier.width, tier.height))
                .collect::<Vec<_>>(),
            vec![
                ("grid-m", 2048, 1000),
                ("grid-s", 1024, 512),
                ("grid-xs", 512, 256)
            ]
        );
    }

    // The classification is the whole taxonomy in one place: which failures
    // are the file's fault (recorded), which are this machine's (retried),
    // and which settle on the first failure.
    #[test]
    fn scan_failures_are_classified_by_what_actually_failed() {
        let dir = tempfile::TempDir::new().unwrap();

        // A name no mime guess covers: a pure function of the file name, so
        // one failure settles it.
        let err = infer_mime_type(&dir.path().join("thing.notamimetype"))
            .expect_err("an unguessable extension must fail");
        let failure = err.classified().expect("a mime failure is recorded");
        assert_eq!(failure.stage, STAGE_MIME);
        assert_eq!(failure.kind, ApiErrorKind::Input);
        assert_eq!(failure.skip_after, SKIP_AFTER_CONFIRMED);

        // A decode that never got to decode anything: opening does its own
        // file I/O, and an SMB blip must not become a permanent verdict on a
        // file nobody could even read. The *stage* is what decides this, not
        // the error variant.
        let (stage, err) = open_image_staged(dir.path().join("absent.png"))
            .err()
            .expect("a missing file must fail");
        assert_eq!(stage, ImageStage::Open);
        let missing = FileProcessError::from_image_error(stage, err);
        assert!(
            missing.classified().is_none(),
            "an open-stage failure is transient: {missing:?}"
        );
        assert!(matches!(missing, FileProcessError::Io(_)));

        // Bytes that were read fine and describe no image at all: the metadata
        // phase's own verdict on the payload, and the one image failure that
        // still keeps a file out of the index. Ambiguous, not settled at one
        // attempt: the walker may have read a file mid-copy (a preallocated
        // destination sits at its final stat key before its bytes exist), so a
        // one-shot verdict could suppress the finished file forever.
        let garbage = dir.path().join("garbage.png");
        fs::write(&garbage, b"definitely not an image").unwrap();
        let (stage, err) = image_header_geometry(&garbage)
            .err()
            .expect("garbage has no parseable header");
        assert_eq!(stage, ImageStage::Header);
        let err = FileProcessError::from_image_error(stage, err);
        let failure = err.classified().expect("a header failure is recorded");
        assert_eq!(failure.stage, STAGE_HEADER);
        assert_eq!(failure.kind, ApiErrorKind::Input);
        assert_eq!(failure.skip_after, SKIP_AFTER_AMBIGUOUS);

        // The decode stage still classifies as the ambiguous input class
        // wherever it reaches the ledger: a decoder that reads as it goes
        // cannot settle a verdict alone.
        let (stage, err) = open_image_staged(&garbage)
            .err()
            .expect("garbage must not decode");
        assert_eq!(stage, ImageStage::Decode);
        let err = FileProcessError::from_image_error(stage, err);
        let failure = err.classified().expect("a decode failure is recorded");
        assert_eq!(failure.stage, STAGE_DECODE);
        assert_eq!(failure.kind, ApiErrorKind::Input);
        assert_eq!(failure.skip_after, SKIP_AFTER_AMBIGUOUS);

        // The decode memory ceiling is a property of this machine's budget,
        // not of the file, so it is `resource` — clearable by a directive
        // after the limit is raised, not by calling the image corrupt.
        let limited = FileProcessError::from_image_error(
            ImageStage::Decode,
            image::ImageError::Limits(image::error::LimitError::from_kind(
                image::error::LimitErrorKind::InsufficientMemory,
            )),
        );
        let failure = limited.classified().expect("a limit failure is recorded");
        assert_eq!(failure.kind, ApiErrorKind::Resource);
        assert_eq!(failure.skip_after, SKIP_AFTER_CONFIRMED);

        // A spawn failure is never a verdict on the media: a missing
        // toolchain is `blocked` and self-heals, anything else stays
        // transient.
        let blocked = FileProcessError::from_api_error(
            STAGE_METADATA,
            crate::media_tools::spawn_error(
                "ffprobe",
                &io::Error::new(io::ErrorKind::NotFound, "no ffprobe"),
            ),
        );
        let failure = blocked.classified().expect("a missing tool is recorded");
        assert_eq!(
            failure.kind,
            ApiErrorKind::Blocked {
                blocker: Blocker::Ffmpeg
            }
        );
        let denied = FileProcessError::from_api_error(
            STAGE_METADATA,
            crate::media_tools::spawn_error(
                "ffprobe",
                &io::Error::new(io::ErrorKind::PermissionDenied, "nope"),
            ),
        );
        assert!(
            denied.classified().is_none(),
            "a machine-local spawn failure stays transient: {denied:?}"
        );
    }

    // A PNG whose IHDR is well-formed and describes an image no machine will
    // allocate. Only the header is written: nothing here ever gets as far as
    // reading pixel data.
    fn absurdly_wide_png(width: u32) -> Vec<u8> {
        fn crc32(bytes: &[u8]) -> u32 {
            let mut crc = 0xFFFF_FFFFu32;
            for byte in bytes {
                crc ^= *byte as u32;
                for _ in 0..8 {
                    let mask = (crc & 1).wrapping_neg();
                    crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
                }
            }
            !crc
        }

        let mut png = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
        let mut chunk = |kind: &[u8; 4], data: &[u8]| {
            let mut body = kind.to_vec();
            body.extend_from_slice(data);
            png.extend_from_slice(&(data.len() as u32).to_be_bytes());
            png.extend_from_slice(&body);
            png.extend_from_slice(&crc32(&body).to_be_bytes());
        };

        let mut header = Vec::new();
        header.extend_from_slice(&width.to_be_bytes());
        header.extend_from_slice(&1u32.to_be_bytes());
        // 8-bit RGB, no compression/filter/interlace variation: the smallest
        // valid header the decoder will accept before it does the arithmetic.
        header.extend_from_slice(&[8, 2, 0, 0, 0]);
        chunk(b"IHDR", &header);
        // The decoder reads on past IHDR before it answers, so the file has to
        // be a whole (if contentless) PNG rather than a header alone, or it
        // fails on EOF instead of on the arithmetic.
        chunk(b"IDAT", &[]);
        chunk(b"IEND", &[]);
        png
    }

    // The header parse is not limit-free, whatever "no limits are set" suggests:
    // not setting limits leaves the image crate's *defaults* in place, including
    // a 512 MiB allocation cap — stricter than the configurable
    // `image_decode_memory_limit_mb` the decode itself runs under. One row of a
    // wide enough image exceeds it, so `into_dimensions` really can return
    // `ImageError::Limits`.
    //
    // Which makes the classification load-bearing rather than theoretical: this
    // is a verdict on *this machine's budget*, so it must be `resource` —
    // clearable by a directive once the ceiling moves — and never `input`,
    // which would file a perfectly good file under "corrupt" forever.
    #[test]
    fn a_header_limits_failure_is_a_resource_verdict() {
        let dir = tempfile::TempDir::new().unwrap();
        let huge = dir.path().join("huge.png");
        fs::write(&huge, absurdly_wide_png(200_000_000)).unwrap();

        let (stage, err) = image_header_geometry(&huge)
            .err()
            .expect("a 200-million-pixel row must not fit the default cap");
        assert_eq!(stage, ImageStage::Header, "no pixel data was read: {err:?}");
        assert!(
            matches!(err, image::ImageError::Limits(_)),
            "the header parse runs under the crate's default limits: {err:?}"
        );
        let failure = FileProcessError::from_image_error(stage, err)
            .classified()
            .cloned()
            .expect("a limit failure is recorded");
        assert_eq!(failure.stage, STAGE_HEADER);
        assert_eq!(failure.kind, ApiErrorKind::Resource);
        assert_eq!(failure.skip_after, SKIP_AFTER_CONFIRMED);

        // The visuals twin classifies it the same way. It is unreachable in
        // practice (a file that fails the header is never indexed, so no
        // visuals pass runs on it), but the two classifiers are read as a pair
        // and a silent divergence here is how the next stage split goes wrong.
        let (stage, err) = image_header_geometry(&huge).err().unwrap();
        let visuals = FileProcessError::visuals_from_image_error(stage, err);
        let failure = visuals
            .visual_failure()
            .expect("a limit failure is a verdict to mark");
        assert_eq!(failure.kind, ApiErrorKind::Resource);
        assert_eq!(failure.skip_after, SKIP_AFTER_CONFIRMED);
    }

    // The metadata phase records the *picture*, not the pixels as stored
    // (docs/display-dimensions-design.md §3). Everything downstream — the
    // pinboard's cell shapes, the crop the client sends back — is measured
    // against what a browser paints, and a browser paints this transposed.
    #[test]
    fn an_exif_rotated_image_is_indexed_with_its_display_dimensions() {
        let dir = tempfile::TempDir::new().unwrap();
        let rotated = dir.path().join("portrait.jpg");
        // Orientation 6: rotate 90 degrees clockwise to display.
        fs::write(&rotated, jpeg_with_exif_orientation(64, 32, 6)).unwrap();

        let (dimensions, orientation) = image_header_geometry(&rotated).expect("the header parses");
        assert_eq!(orientation, Orientation::Rotate90);
        assert_eq!(
            dimensions,
            (32, 64),
            "the header codes 64x32; the picture is 32x64"
        );

        let metadata = extract_item_metadata(&rotated, "image/jpeg", "md5".to_string())
            .expect("a rotated photo is indexed");
        assert_eq!((metadata.width, metadata.height), (Some(32), Some(64)));
        assert_eq!(
            metadata.rotation,
            Some(90),
            "the turn is stamped, so the backfill never asks about this item again"
        );
    }

    // An image with no orientation at all still records an *answer*. A NULL
    // there means "never examined", and leaving one behind would put every
    // ordinary image in the library back in the backfill population on every
    // scan, forever.
    #[test]
    fn an_unrotated_image_still_stamps_a_zero_turn() {
        let dir = tempfile::TempDir::new().unwrap();
        let plain = dir.path().join("plain.png");
        image::RgbImage::new(9, 4).save(&plain).unwrap();

        let metadata = extract_item_metadata(&plain, "image/png", "md5".to_string())
            .expect("an ordinary image is indexed");
        assert_eq!((metadata.width, metadata.height), (Some(9), Some(4)));
        assert_eq!(metadata.rotation, Some(0));
    }

    // The second half of §1.1: the dimensions and the stored pixels have to
    // move together. `encode_image` re-encodes through `to_rgb8()` + JPEG and
    // drops the EXIF, so a thumbnail generated from an unoriented decode is
    // sideways forever — against the original file, which the browser paints
    // upright, and against its own item's dimensions.
    #[test]
    fn a_visuals_decode_is_the_picture_not_the_stored_pixels() {
        let dir = tempfile::TempDir::new().unwrap();
        let rotated = dir.path().join("portrait.jpg");
        fs::write(&rotated, jpeg_with_exif_orientation(64, 32, 6)).unwrap();

        let raw = open_image_staged(&rotated).expect("the fixture decodes");
        assert_eq!(
            raw.dimensions(),
            (64, 32),
            "the raw decode is the coded pixels — this is the trap"
        );

        let oriented = open_image_oriented(&rotated).expect("the fixture decodes");
        assert_eq!(
            oriented.dimensions(),
            (32, 64),
            "every visuals decode must agree with the indexed dimensions"
        );
    }

    // The eight EXIF cases collapsed onto the four turns `items.rotation`
    // records. Mirroring never moves a dimension, so it never reaches the
    // column — but it is still a transform, which is why the backfill keys its
    // stale-visuals decision on the orientation and not on this.
    #[test]
    fn quarter_turns_drop_the_mirror_and_keep_the_turn() {
        for (orientation, turns) in [
            (Orientation::NoTransforms, 0),
            (Orientation::FlipHorizontal, 0),
            (Orientation::Rotate90, 90),
            (Orientation::Rotate90FlipH, 90),
            (Orientation::Rotate180, 180),
            (Orientation::FlipVertical, 180),
            (Orientation::Rotate270, 270),
            (Orientation::Rotate270FlipH, 270),
        ] {
            assert_eq!(
                orientation_quarter_turns(orientation),
                turns,
                "{orientation:?}"
            );
            assert_eq!(
                oriented_dimensions((64, 32), orientation),
                if turns % 180 == 90 { (32, 64) } else { (64, 32) },
                "{orientation:?}"
            );
        }
    }

    // ffprobe reports the display matrix counter-clockwise; `items.rotation`
    // is clockwise, so the two disagree by a sign. Measured against the
    // bundled toolchain: `-display_rotation 90` probes as `rotation: 90` and
    // decodes with a top-left marker in the *bottom* left, which is 270
    // clockwise. Only the transposition is consumed, so a sign error would
    // never show up in a dimension — it would just make the column lie.
    #[test]
    fn a_display_matrix_is_read_clockwise() {
        let stream = |json: &str| {
            serde_json::from_str::<FfprobeStream>(json).expect("the fixture parses")
        };
        let with_rotation =
            |rotation: &str| format!(r#"{{"side_data_list":[{{"rotation":{rotation}}}]}}"#);

        assert_eq!(stream(&with_rotation("-90")).quarter_turns(), 90);
        assert_eq!(stream(&with_rotation("90")).quarter_turns(), 270);
        assert_eq!(stream(&with_rotation("180")).quarter_turns(), 180);
        assert_eq!(stream(&with_rotation("0")).quarter_turns(), 0);
        assert_eq!(
            stream(&with_rotation("-33.5")).quarter_turns(),
            0,
            "a matrix that is not a quarter turn is dropped, never rounded into one"
        );
        assert_eq!(
            stream(r#"{"side_data_list":[{"displaymatrix":"..."}]}"#).quarter_turns(),
            0,
            "side data of another kind is not a rotation"
        );
        assert_eq!(stream("{}").quarter_turns(), 0, "no side data is no turn");
    }

    // The premise the whole un-fusing rests on: a truncated image's *header*
    // is intact. It parses, it yields the real dimensions, and only the decode
    // fails — so a file like this is indexed with its true size and simply has
    // no visuals, instead of being rejected outright the way the fused decode
    // rejected it (docs/failed-media-retry-design.md, "Scan policy for
    // undecodable images"). This is also PIL's split, which is what makes the
    // scan's gate no stricter than the pipeline's consumer.
    //
    // And the reason the decode stage classifies by *where* rather than by
    // which variant: the failure surfaces as `ImageError::IoError` — the
    // decoder asked for bytes that are not there. Reading that as transient
    // (the obvious reading) would leave every half-copied file in the library
    // re-decoded on every scan forever.
    #[test]
    fn a_truncated_image_is_a_decode_verdict_not_an_io_blip() {
        let dir = tempfile::TempDir::new().unwrap();
        let whole = dir.path().join("whole.png");
        // Noise, not a flat colour: a PNG of one colour compresses to so few
        // bytes that a 60% prefix can still hold the entire image data.
        let mut image = image::RgbImage::new(64, 64);
        for (x, y, pixel) in image.enumerate_pixels_mut() {
            *pixel = image::Rgb([
                (x * 7 % 251) as u8,
                (y * 13 % 241) as u8,
                (x * y % 239) as u8,
            ]);
        }
        image.save(&whole).unwrap();
        let bytes = fs::read(&whole).unwrap();
        assert!(bytes.len() > 100, "the fixture must be a real PNG");

        let truncated = dir.path().join("truncated.png");
        fs::write(&truncated, &bytes[..bytes.len() * 6 / 10]).unwrap();

        assert_eq!(
            image_header_geometry(&truncated)
                .ok()
                .map(|(dimensions, _)| dimensions),
            Some((64, 64)),
            "the header of a truncated file is intact, so the metadata phase \
             indexes it with its real dimensions"
        );

        let (stage, err) = open_image_staged(&truncated)
            .err()
            .expect("a truncated PNG must not decode");
        assert_eq!(
            stage,
            ImageStage::Decode,
            "the file opened and sniffed fine; only the decode failed"
        );
        // The premise, pinned: this is the variant that used to be read as
        // "transient" wherever it appeared.
        assert!(
            matches!(err, image::ImageError::IoError(_)),
            "a truncated PNG surfaces as an I/O error, not a decoding one: {err:?}"
        );
        let classified = FileProcessError::from_image_error(stage, err);
        let failure = classified
            .classified()
            .expect("a truncated file is a recorded verdict, not a transient failure");
        assert_eq!(failure.stage, STAGE_DECODE);
        assert_eq!(failure.kind, ApiErrorKind::Input);
        assert_eq!(
            failure.skip_after, SKIP_AFTER_AMBIGUOUS,
            "the threshold, not the class, is what covers a mid-read mount drop"
        );
    }

    // The name rules for a *directory*, which is the whole reason this exists
    // separately from `is_hidden_or_temp`: junk is decided by the directory's
    // own name, and everything below it goes with it.
    #[test]
    fn junk_directory_names_are_dot_prefixes_and_macosx() {
        for name in [
            ".Trashes",
            ".TemporaryItems",
            ".Spotlight-V100",
            ".fseventsd",
            ".",
            "__MACOSX",
            // The mounts this arrives from are case-insensitive, so the same
            // directory shows up under either casing.
            "__macosx",
            "__MacOSX",
        ] {
            assert!(is_junk_dir_name(OsStr::new(name)), "{name} is junk");
        }
        for name in [
            "photos",
            "MACOSX",
            "__MACOSX_backup",
            "my.folder",
            // Deliberately not extended to directories: `~` marks an editor's
            // temporary *file*, and is an ordinary folder name otherwise.
            "~drafts",
        ] {
            assert!(!is_junk_dir_name(OsStr::new(name)), "{name} is not junk");
        }
    }

    // The path-shaped half, for the callers handed an absolute path instead of
    // a traversal: only the components *between* the root and the file name
    // decide, so the root is exempt and the file's own name is left to
    // `is_hidden_or_temp`.
    #[test]
    fn junk_components_are_judged_between_the_root_and_the_file_name() {
        let root = PathBuf::from(r"C:\media");
        let roots = vec![root.clone()];

        assert!(is_under_junk_dir(&root.join(".Trashes/x.png"), &roots));
        assert!(is_under_junk_dir(&root.join("__MACOSX/deep/x.png"), &roots));
        assert!(is_under_junk_dir(
            &root.join("sub/.fseventsd/x.png"),
            &roots
        ));
        assert!(!is_under_junk_dir(&root.join("sub/x.png"), &roots));
        assert!(!is_under_junk_dir(&root.join("~drafts/x.png"), &roots));
        // The file's own name is not a directory component.
        assert!(!is_under_junk_dir(&root.join("._Boop.png"), &roots));
        // A path belonging to no root is not this function's to judge.
        assert!(!is_under_junk_dir(
            Path::new(r"C:\other\.Trashes\x.png"),
            &[]
        ));

        // A dot-named root judges itself: the junk component has to be *below*
        // it. `deduplicate_paths` collapses the nested pair before any caller
        // gets here, so this pins the rule rather than a reachable case.
        let dot_root = PathBuf::from(r"C:\media\.bar");
        let nested = vec![root, dot_root.clone()];
        assert!(
            !is_under_junk_dir(&dot_root.join("x.png"), &nested),
            "the longest matching root wins, and a root is never its own junk"
        );
        assert!(is_under_junk_dir(&dot_root.join(".Trashes/x.png"), &nested));
    }

    // The signatures the post-failure sniff knows, and — just as important —
    // everything it must stay silent about, since a verdict here rewrites what
    // a retry directive can ever select.
    #[test]
    fn the_content_sniff_recognises_only_applefile_and_html() {
        let dir = tempfile::TempDir::new().unwrap();
        let sniff = |name: &str, bytes: &[u8]| {
            let path = dir.path().join(name);
            fs::write(&path, bytes).unwrap();
            sniff_junk_mime(&path)
        };

        assert_eq!(
            sniff("double.png", &[0x00, 0x05, 0x16, 0x07, 0xFF, 0xFF]),
            Some(MIME_APPLEFILE)
        );
        assert_eq!(
            sniff("single.png", &[0x00, 0x05, 0x16, 0x00, 0xFF, 0xFF]),
            Some(MIME_APPLEFILE)
        );
        for html in [
            "<!DOCTYPE html><html></html>".as_bytes(),
            b"<!doctype HTML>",
            b"<html lang=\"en\">",
            b"<HTML>",
            b"<!-- an error page a share served instead of the file -->",
            b"\r\n\t   <!DOCTYPE html>",
        ] {
            assert_eq!(sniff("page.png", html), Some("text/html"), "{html:?}");
        }
        // A byte-order mark, alone and with the whitespace behind it.
        let mut bom = vec![0xEF, 0xBB, 0xBF];
        bom.extend_from_slice(b"  <html>");
        assert_eq!(sniff("bom.png", &bom), Some("text/html"));

        let mut png = Vec::new();
        image::RgbImage::new(2, 2)
            .write_to(&mut io::Cursor::new(&mut png), image::ImageFormat::Png)
            .unwrap();
        assert_eq!(sniff("real.png", &png), None, "a real image is not junk");
        assert_eq!(sniff("empty.png", b""), None);
        // Too short to hold any signature, including the AppleDouble magic
        // whose first three bytes it does match.
        assert_eq!(sniff("short.png", &[0x00, 0x05, 0x16]), None);
        assert_eq!(sniff("bom_only.png", &[0xEF, 0xBB, 0xBF]), None);
        assert_eq!(sniff("text.png", b"<not html>"), None);
        // A file that cannot be read is not a verdict.
        assert_eq!(sniff_junk_mime(&dir.path().join("missing.png")), None);

        // And directly on bytes, which is what the split is for: the mark and
        // whitespace are stripped in that order, and a prefix too short to
        // hold a signature matches nothing even when it agrees as far as it
        // goes.
        let mut marked = vec![0xEF, 0xBB, 0xBF];
        marked.extend_from_slice(b"\n  <!DOCTYPE html>");
        assert_eq!(sniff_junk_bytes(&marked), Some("text/html"));
        assert_eq!(sniff_junk_bytes(&[0x00, 0x05, 0x16]), None);
        assert_eq!(sniff_junk_bytes(&[]), None);
    }

    // The override's scope, which is the part that has to be exactly right: it
    // rewrites the recorded *format* and settles the threshold, and touches
    // nothing else and no other kind of row.
    #[test]
    fn only_a_header_or_metadata_input_verdict_takes_the_sniffed_mime() {
        let dir = tempfile::TempDir::new().unwrap();
        // A video sidecar, the ffprobe-side twin of the image case: same
        // bytes, different name, and a stage where an external tool did the
        // reading.
        let sidecar = dir.path().join("_MACOSX_._uncenTL.mov");
        fs::write(&sidecar, [0x00, 0x05, 0x16, 0x07, 0x00, 0x02, 0x00, 0x00]).unwrap();
        let base = ScanErrorRecord {
            path: sidecar.to_string_lossy().to_string(),
            last_modified: "2026-01-01T00:00:00".to_string(),
            file_size: 8,
            stage: STAGE_METADATA.to_string(),
            kind: ApiErrorKind::Input,
            mime_type: Some("video/quicktime".to_string()),
            error: "ffprobe failed: moov atom not found".to_string(),
            skip_after: SKIP_AFTER_AMBIGUOUS,
        };

        let mut sniffed = base.clone();
        override_mime_from_content(&mut sniffed, &sidecar);
        assert_eq!(sniffed.mime_type.as_deref(), Some(MIME_APPLEFILE));
        assert_eq!(
            sniffed.skip_after, SKIP_AFTER_CONFIRMED,
            "the gateway read the magic itself, so one attempt settles it"
        );
        assert_eq!(
            (sniffed.stage.as_str(), sniffed.error.as_str()),
            (base.stage.as_str(), base.error.as_str()),
            "the recorded failure stays the one that happened"
        );
        assert_eq!(sniffed.kind, base.kind);

        let mut header = ScanErrorRecord {
            stage: STAGE_HEADER.to_string(),
            ..base.clone()
        };
        override_mime_from_content(&mut header, &sidecar);
        assert_eq!(header.mime_type.as_deref(), Some(MIME_APPLEFILE));

        // Everything outside the scope is left alone: a `mime` verdict has no
        // guess to correct, a `decode` row is audit-only and belongs to an
        // *indexed* file, and the non-input classes are verdicts on this
        // machine rather than on the bytes.
        for untouched in [
            ScanErrorRecord {
                stage: STAGE_MIME.to_string(),
                mime_type: None,
                ..base.clone()
            },
            ScanErrorRecord {
                stage: STAGE_DECODE.to_string(),
                ..base.clone()
            },
            ScanErrorRecord {
                kind: ApiErrorKind::Resource,
                ..base.clone()
            },
            ScanErrorRecord {
                kind: ApiErrorKind::Blocked {
                    blocker: Blocker::Ffmpeg,
                },
                ..base.clone()
            },
        ] {
            let mut record = untouched.clone();
            override_mime_from_content(&mut record, &sidecar);
            assert_eq!(
                (record.mime_type, record.skip_after),
                (untouched.mime_type, untouched.skip_after),
                "out of scope: {} / {:?}",
                untouched.stage,
                untouched.kind
            );
        }

        // A file the sniff does not recognise keeps its name's guess whole.
        let honest = dir.path().join("broken.mov");
        fs::write(&honest, b"not a container and not junk either").unwrap();
        let mut unchanged = ScanErrorRecord {
            path: honest.to_string_lossy().to_string(),
            ..base
        };
        let before = unchanged.clone();
        override_mime_from_content(&mut unchanged, &honest);
        assert_eq!(
            (unchanged.mime_type, unchanged.skip_after),
            (before.mime_type, before.skip_after)
        );
    }

    // ffprobe classification against the real toolchain: a file of garbage
    // bytes claiming to be a video is an *unconfirmed* payload verdict,
    // because ffprobe read the file itself and a corrupt file and a dropped
    // mount exit identically.
    #[test]
    fn ffprobe_rejecting_a_file_is_an_unconfirmed_input_verdict() {
        // `ffmpeg_available` probes both executables, which is what the
        // auto-heal relies on and what this test needs.
        if !crate::media_tools::ffmpeg_available() {
            // No toolchain on this host; the classification is unobservable.
            return;
        }
        let dir = tempfile::TempDir::new().unwrap();
        let fake_video = dir.path().join("garbage.mp4");
        fs::write(&fake_video, b"nothing here is a container").unwrap();

        // `MediaInfo` is not Debug (its fields mirror the ffprobe JSON), so
        // the success case is refuted by hand rather than with `expect_err`.
        let Err(err) = extract_media_info(&fake_video) else {
            panic!("ffprobe must reject a file that is not a container");
        };
        let failure = err.classified().expect("an ffprobe rejection is recorded");
        assert_eq!(failure.stage, STAGE_METADATA);
        assert_eq!(failure.kind, ApiErrorKind::Input);
        assert_eq!(
            failure.skip_after, SKIP_AFTER_AMBIGUOUS,
            "a tool that did its own file I/O never settles a verdict alone"
        );
    }

    // The video half joined up against the real toolchain: the AppleDouble
    // sidecars the user's share hands back under `.mov` names fail *in
    // ffprobe*, so the row they owe is the ambiguous metadata verdict — and
    // that is exactly the row the sniff has to reach.
    #[test]
    fn an_applefile_named_mov_is_an_ffprobe_verdict_the_sniff_rewrites() {
        if !crate::media_tools::ffmpeg_available() {
            // No toolchain on this host; the classification is unobservable.
            return;
        }
        let dir = tempfile::TempDir::new().unwrap();
        let sidecar = dir.path().join("_MACOSX_._uncenTL.mov");
        let mut apple_double = vec![0x00, 0x05, 0x16, 0x07, 0x00, 0x02, 0x00, 0x00];
        apple_double.extend_from_slice(&[0u8; 64]);
        fs::write(&sidecar, &apple_double).unwrap();

        let Err(err) = extract_media_info(&sidecar) else {
            panic!("ffprobe must reject a resource fork");
        };
        let failure = err.classified().expect("an ffprobe rejection is recorded");
        assert_eq!(
            (failure.stage, failure.kind),
            (STAGE_METADATA, ApiErrorKind::Input)
        );

        let mut record = ScanErrorRecord {
            path: sidecar.to_string_lossy().to_string(),
            last_modified: "2026-01-01T00:00:00".to_string(),
            file_size: apple_double.len() as i64,
            stage: failure.stage.to_string(),
            kind: failure.kind,
            mime_type: infer_mime_type(&sidecar).ok(),
            error: failure.message.clone(),
            skip_after: failure.skip_after,
        };
        override_mime_from_content(&mut record, &sidecar);
        assert_eq!(
            (record.mime_type.as_deref(), record.skip_after),
            (Some(MIME_APPLEFILE), SKIP_AFTER_CONFIRMED)
        );
    }

    // The auto-heal twin of the extraction ledger's: installing one dependency
    // must free exactly the files waiting on it. The probe results are handed
    // in because probing real binaries is what makes the caller untestable,
    // and the clearing is what has to be right.
    //
    // Both scan-side tables heal in the same pass: the ledger (a file that
    // could not be probed at all) and the visuals cache (a file that indexed
    // fine but whose thumbnail needs a backend). They wait on the same
    // dependencies, and a heal that only cleared one of them would leave the
    // other permanently suppressed.
    #[tokio::test]
    async fn healing_clears_only_the_scan_dependencies_that_came_back() {
        use crate::db::visual_attempts::{
            VisualFailure, list_distinct_visual_blockers, upsert_visual_attempts,
        };

        let _test_env = test_data_dir();
        let index_db = "scan_heal_blocked";
        migrate_databases_on_disk(Some(index_db), Some("scan_heal_blocked_user"))
            .await
            .expect("migrate test databases");
        {
            let mut conn = crate::db::open_index_db_write_no_user_data(index_db)
                .await
                .unwrap();
            // One marker per dependency, on the visuals side too.
            for (sha256, blocker) in [
                ("sha_pdf", Blocker::Pdfium),
                ("sha_html", Blocker::HtmlRenderer),
            ] {
                upsert_visual_attempts(
                    &mut conn,
                    &[VisualVerdict::failed(
                        VisualKind::Thumbnail,
                        VisualFailure {
                            kind: ApiErrorKind::Blocked { blocker },
                            skip_after: SKIP_AFTER_CONFIRMED,
                            message: "dependency missing".to_string(),
                        },
                    )
                    .into_record(
                        sha256,
                        "application/pdf",
                        THUMBNAIL_PROCESS_VERSION,
                    )],
                    Some(1),
                )
                .await
                .unwrap();
            }
            for (path, blocker) in [
                ("C:/data/1.pdf", Blocker::Pdfium),
                ("C:/data/2.mp4", Blocker::Ffmpeg),
            ] {
                let record = ScanErrorRecord {
                    path: path.to_string(),
                    last_modified: "2026-01-01T00:00:00".to_string(),
                    file_size: 10,
                    stage: STAGE_METADATA.to_string(),
                    kind: ApiErrorKind::Blocked { blocker },
                    mime_type: None,
                    error: "dependency missing".to_string(),
                    skip_after: SKIP_AFTER_CONFIRMED,
                };
                crate::db::scan_errors::upsert_scan_error(&mut conn, &record, Some(1))
                    .await
                    .unwrap();
            }
        }

        assert_eq!(
            heal_blocked_scan(index_db, Vec::new()).await.unwrap(),
            0,
            "nothing probed present means no write at all"
        );
        assert_eq!(
            heal_blocked_scan(index_db, vec![Blocker::Pdfium])
                .await
                .unwrap(),
            2,
            "one row per table, cleared in the same pass"
        );

        let mut conn = open_index_db_read_no_user_data(index_db).await.unwrap();
        assert_eq!(
            list_distinct_scan_blockers(&mut conn).await.unwrap(),
            vec![Blocker::Ffmpeg],
            "the dependency that is still missing keeps its rows"
        );
        assert_eq!(
            list_distinct_visual_blockers(&mut conn).await.unwrap(),
            vec![Blocker::HtmlRenderer],
            "and so does the one only the visuals cache is waiting on"
        );
    }

    // The user-visible loop the auto-heal exists for: a video the scan could
    // not probe because ffprobe was not installed is suppressed while that is
    // true, and offered again — without the user touching the file, the
    // config, or the ledger — once the toolchain is there. The verdict is
    // built through the scan's own classification path, so a change that
    // stopped recording spawn failures as `blocked` would fail here rather
    // than silently turning the self-heal into a permanent suppression.
    #[tokio::test]
    async fn a_missing_ffmpeg_verdict_is_cleared_once_the_toolchain_is_back() {
        let _test_env = test_data_dir();
        let index_db = "scan_heal_ffmpeg";
        migrate_databases_on_disk(Some(index_db), Some("scan_heal_ffmpeg_user"))
            .await
            .expect("migrate test databases");

        let error = FileProcessError::from_api_error(
            STAGE_METADATA,
            crate::media_tools::spawn_error(
                "ffprobe",
                &io::Error::new(io::ErrorKind::NotFound, "no ffprobe"),
            ),
        );
        let failure = error
            .classified()
            .expect("a missing toolchain is a recorded verdict");
        let record = ScanErrorRecord {
            path: r"C:\media\clip.mp4".to_string(),
            last_modified: "2026-01-01T00:00:00".to_string(),
            file_size: 4096,
            stage: failure.stage.to_string(),
            kind: failure.kind,
            mime_type: Some("video/mp4".to_string()),
            error: failure.message.clone(),
            skip_after: failure.skip_after,
        };
        {
            let mut conn = crate::db::open_index_db_write_no_user_data(index_db)
                .await
                .unwrap();
            crate::db::scan_errors::upsert_scan_error(&mut conn, &record, Some(1))
                .await
                .unwrap();
        }

        // While the dependency is missing the walk does not offer the file:
        // the preloaded verdict suppresses it on the very first attempt.
        let mut conn = open_index_db_read_no_user_data(index_db).await.unwrap();
        let loaded = load_scan_errors_under(&mut conn, r"C:\media\")
            .await
            .unwrap();
        assert!(
            loaded[&fold_scan_path(&record.path)]
                .suppresses(&record.last_modified, record.file_size),
            "a blocked verdict suppresses from one attempt"
        );
        drop(conn);

        // Installing ffmpeg clears it, and the walk offers the file again —
        // there is nothing left to suppress it.
        assert_eq!(
            heal_blocked_scan(index_db, vec![Blocker::Ffmpeg])
                .await
                .unwrap(),
            1
        );
        let mut conn = open_index_db_read_no_user_data(index_db).await.unwrap();
        assert!(
            load_scan_errors_under(&mut conn, r"C:\media\")
                .await
                .unwrap()
                .is_empty(),
            "the healed file must be re-offered, not merely un-blocked"
        );
    }

    // The sweep decides that a ledger row describes a file that is gone, from
    // the fact that the walk never reached it. That inference is only sound
    // when the walk saw the whole tree: an unreadable directory (a permission
    // change, a dropped mount) makes every file under it look deleted, and
    // clearing then hands the next scan all of that work back — the one
    // failure mode a cache must not have.
    #[test]
    fn the_sweep_defers_when_the_walk_could_not_read_the_tree() {
        let stored = [r"C:\M\Gone.png", r"C:\M\Here.png"];
        let rows: HashMap<String, ScanErrorSkip> = stored
            .iter()
            .map(|path| {
                (
                    fold_scan_path(path),
                    ScanErrorSkip {
                        path: path.to_string(),
                        last_modified: "2026-01-01T00:00:00".to_string(),
                        file_size: 1,
                        attempts: 1,
                        skip_after: 1,
                        stage: STAGE_HEADER.to_string(),
                    },
                )
            })
            .collect();
        // The walker's own casing, which on Windows need not be the stored one.
        let seen: HashSet<String> = [fold_scan_path(r"C:\m\here.png")].into_iter().collect();

        // A clean walk sweeps exactly the row it never reached, by the path
        // that is actually in the table.
        assert_eq!(
            sweepable_scan_errors(0, &rows, &seen),
            vec![r"C:\M\Gone.png".to_string()]
        );
        // One walk error and nothing is swept until a clean walk.
        assert!(sweepable_scan_errors(1, &rows, &seen).is_empty());
        // The normal case — no rows at all — costs nothing either way.
        assert!(sweepable_scan_errors(0, &HashMap::new(), &seen).is_empty());
        assert!(sweepable_scan_errors(3, &HashMap::new(), &seen).is_empty());
    }

    // A file the walk reached but a *later* gate rejected — here the filescan
    // filter — keeps its verdict. The row is marked seen before any gate runs,
    // so the sweep leaves it alone.
    //
    // This is deliberate, not an oversight: such a row suppresses nothing that
    // would otherwise be processed (the filter already excludes the file), and
    // dropping it would mean a filter the user narrows and later widens again
    // costs a full re-attempt of every broken file it had swept out. The row
    // goes when the file goes, or when it finally succeeds.
    #[tokio::test]
    async fn a_filtered_out_file_keeps_its_recorded_verdict() {
        let test_env = test_data_dir();
        let root = test_env.path();
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();

        let media_dir = root.join("filtered_ledger_media");
        fs::create_dir_all(&media_dir).unwrap();
        image::RgbImage::new(8, 8)
            .save(media_dir.join("good.png"))
            .unwrap();
        let rejected = media_dir.join("rejected.png");
        fs::write(&rejected, b"this claims to be a png and is not").unwrap();
        let rejected_path = rejected.to_string_lossy().to_string();

        // Seed the verdict the way a previous scan would have left it.
        {
            let mut conn = crate::db::open_index_db_write_no_user_data(&index_db)
                .await
                .unwrap();
            let (last_modified, file_size) = get_last_modified_time_and_size(&rejected).unwrap();
            crate::db::scan_errors::upsert_scan_error(
                &mut conn,
                &ScanErrorRecord {
                    path: rejected_path.clone(),
                    last_modified,
                    file_size,
                    stage: STAGE_DECODE.to_string(),
                    kind: ApiErrorKind::Input,
                    mime_type: Some("image/png".to_string()),
                    error: "decode failed".to_string(),
                    skip_after: SKIP_AFTER_CONFIRMED,
                },
                Some(1),
            )
            .await
            .unwrap();
        }

        // Now the user's filter rejects exactly that file, at stage 1 — before
        // anything is hashed, and after the walk has reached it.
        let store = SystemConfigStore::new(root.to_path_buf());
        let mut config = SystemConfig::default();
        config.included_folders = vec![media_dir.to_string_lossy().to_string()];
        config.filescan_filter = Some(
            serde_json::from_str(r#"{"match": {"eq": {"filename": "good.png"}}}"#)
                .expect("the test filter must parse as PQL"),
        );
        store.save(&index_db, &config).unwrap();

        FileScanService::new(
            index_db.clone(),
            user_data_db.clone(),
            root.to_path_buf(),
            ScanOptions { worker_count: 2 },
        )
        .rescan_folders()
        .await
        .unwrap();

        let mut conn = open_index_db_read(&index_db, &user_data_db).await.unwrap();
        let rows = scan_error_rows(&mut conn).await;
        assert_eq!(
            rows.iter().map(|row| row.0.as_str()).collect::<Vec<_>>(),
            vec![rejected_path.as_str()],
            "a filtered-out file keeps its verdict: {rows:?}"
        );
        let files: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(files.0, 1, "only the file the filter accepts is indexed");
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
        let result = service.rescan_folders().await.unwrap();
        // A backfilling rescan writes, even though nothing is new or modified.
        assert!(
            result.summary.wrote_data,
            "a backfilled blurhash is a write the boundary owes maintenance for"
        );
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
        let result = service.rescan_folders().await.unwrap();
        // Nothing new, nothing modified, nothing backfilled — and still a
        // write, because every unchanged file gets an `UPDATE files SET
        // scan_id`. This is the nightly-no-change-rescan case: dropping it
        // from `wrote_data` costs the WAL checkpoint on a 500k-file library.
        assert!(
            result.summary.wrote_data,
            "an all-unchanged rescan still rewrites every file row"
        );
        assert!(
            !result.summary.deleted_data,
            "an all-unchanged rescan deletes nothing"
        );
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

    // The other half of the `wrote_data` contract: a scan that touches no file
    // at all must report nothing, so an idle cron pass over a database with no
    // configured folders never schedules a maintenance job.
    #[tokio::test]
    async fn empty_folder_set_scan_reports_no_changes() {
        let test_env = test_data_dir();
        let root = test_env.path();
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .unwrap();

        let store = SystemConfigStore::new(root.to_path_buf());
        let config = SystemConfig::default();
        assert!(config.included_folders.is_empty());
        store.save(&index_db, &config).unwrap();

        let service = FileScanService::new(
            index_db.clone(),
            user_data_db.clone(),
            root.to_path_buf(),
            ScanOptions { worker_count: 2 },
        );

        let result = service.rescan_folders().await.unwrap();
        assert!(
            result.scan_ids.is_empty(),
            "no folders means no scans: {:?}",
            result.scan_ids
        );
        assert!(
            !result.summary.wrote_data && !result.summary.deleted_data,
            "a scan that saw no files owes no maintenance"
        );
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
        let image_path = media_dir.join("large.jpg");
        // A 1400x1400 JPEG: past 1.25x every grid tier, so tiers are stored,
        // and comfortably inside its own class's display bounds, so the
        // display tier serves the original and nothing is stored for it.
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
        // The grid tiers of a 1400x1400 square: whole-image resizes, no crop.
        let square_tiers = vec![
            ("grid-m".to_string(), 1024_i64, 1024_i64),
            ("grid-s".to_string(), 512, 512),
            ("grid-xs".to_string(), 256, 256),
        ];
        assert_eq!(tier_rows(&mut conn).await, square_tiers);
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
        // And the ladder question did not re-decode either: a 4900x400 strip
        // would have produced 2048x400 / 1024x400 crops, so the square tiers
        // still standing prove the answer came from the index.
        assert_eq!(
            tier_rows(&mut conn).await,
            square_tiers,
            "the ladder question re-decoded the image"
        );
    }

    /// Every stored grid tier as `(tier, width, height)`, in the order the
    /// dispatcher's own read returns them.
    async fn tier_rows(conn: &mut sqlx::SqliteConnection) -> Vec<(String, i64, i64)> {
        tier_snapshot(conn)
            .await
            .into_iter()
            .map(|row| (row.tier, row.width, row.height))
            .collect()
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

    #[tokio::test]
    async fn scan_indexes_html_after_a_real_screenshot() {
        if html_renderer().is_none() {
            eprintln!("no headless browser available, skipping");
            return;
        }
        let test_env = test_data_dir();
        let mut env = visuals_env(test_env.path(), &["media-html-render"]).await;
        env.config.scan_html = true;
        fs::write(
            env.media_dirs[0].join("sample.html"),
            "<html><body style=\"background:#ff0000\"><h1>hello</h1></body></html>",
        )
        .unwrap();

        env.scan().await;
        let mut conn = env.read().await;
        let items: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM items WHERE type = 'text/html'")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(items, 1, "the rendered HTML file must be indexed");
        assert_eq!(
            thumbnail_count(&mut conn).await,
            1,
            "successful HTML indexing must store its screenshot thumbnail"
        );
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

        let shot = render_html_screenshot_classified(&path).expect("screenshot should render");
        // --window-size fixes the viewport width; the height can vary.
        assert_eq!(shot.width(), 1280);
        let pixel = shot.get_pixel(10, 10);
        assert!(
            pixel.0[0] > 200 && pixel.0[1] < 50 && pixel.0[2] < 50,
            "the screenshot must contain the rendered red page, got {pixel:?}"
        );
    }
}
