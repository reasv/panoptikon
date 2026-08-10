use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::OnceLock,
    time::{Duration, Instant},
};

use ractor::concurrency::Duration as RactorDuration;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use sqlx::SqliteConnection;
use tokio::sync::{Mutex, oneshot};

use crate::api_error::{ApiError, Blocker};
use crate::db::connection::index_storage_paths_unchecked;
use crate::db::{
    extraction_errors::{
        ExtractionErrorRecord, delete_blocked_errors, delete_extraction_error,
        upsert_extraction_error,
    },
    extraction_log::delete_data_job_by_log_id,
    extraction_write::{
        DataLogUpdate, EmbeddingEntry, TagEntry, TagTextEntry, TextEntry, add_data_log,
        delete_orphan_tags, delete_setter_by_name, remove_incomplete_jobs, update_data_log,
        upsert_setter, write_clip_output, write_tags_output, write_text_embedding_output,
        write_text_output,
    },
    file_scans::{
        FileScanUpdate, add_file_scan, close_file_scan, delete_unavailable_files,
        mark_unavailable_files, update_file_scan,
    },
    files::{
        FileScanData, FileUpsertResult, delete_file_by_path, delete_files_not_allowed,
        delete_item_if_orphan, delete_items_without_files, rename_file_path, set_blurhash,
        set_item_codecs, set_outro_verdict, update_file_data,
    },
    folders::{
        add_folder_to_database, delete_files_not_under_included_folders,
        delete_files_under_excluded_folders, delete_folders_not_in_list,
    },
    open_index_db_read_no_user_data, open_index_db_write_no_user_data,
    scan_errors::{
        ScanErrorRecord, delete_blocked_scan_errors, delete_scan_error, delete_scan_errors,
        rekey_scan_error, upsert_scan_error,
    },
    storage::{
        StoredImage, delete_orphaned_frames, delete_orphaned_thumbnails,
        delete_orphaned_visual_attempts, store_frames, store_thumbnails,
    },
    visual_attempts::{
        VisualAttemptRecord, delete_blocked_visual_attempts, upsert_visual_attempts,
    },
};

type ApiResult<T> = std::result::Result<T, ApiError>;
type Reply<T> = oneshot::Sender<ApiResult<T>>;
type DbFuture<'a, T> = Pin<Box<dyn Future<Output = ApiResult<T>> + Send + 'a>>;
const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(300);
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const CALL_RETRY_ATTEMPTS: usize = 2;

/// Statistics refresh, run after every job by `run_post_job_maintenance`.
///
/// Unconditional and unbounded on purpose: every table is re-analyzed in full,
/// so the planner's row estimates are never staler than the last completed job.
///
/// `PRAGMA optimize` alone does **not** substitute for that, despite reading
/// like it should. It re-analyzes a table only once the row count has moved by
/// roughly an order of magnitude — measured on SQLite 3.51.3, a table seeded at
/// 1000 rows kept `sqlite_stat1` saying 1000 through +10%, +50% and +100%
/// growth, and only refreshed once it reached 16500. On indexes that grow
/// incrementally that freezes the statistics indefinitely, which is the failure
/// this call exists to prevent. It is kept here as a cheap trailing no-op.
///
/// The cost is real and worth knowing: 7.7s on the 10.6GB `default` index with
/// nothing else running, 80-102s under the load a job creates. Extraction jobs
/// finish every ~100s, so on 2026-07-21 03:07-04:07 this ran for ~90% of the
/// wall clock and dragged two concurrent searches out to 50 minutes each
/// (0.5s against an idle database). If that recurs, the lever is
/// `PRAGMA analysis_limit` — bounded sampling keeps the refresh unconditional
/// while capping what each index costs — or calling this less often than
/// once per job, not swapping in `optimize`.
const ANALYZE_STATEMENTS: &[&str] = &["ANALYZE", "PRAGMA optimize"];

/// Truncating WAL checkpoint, run after the post-job ANALYZE by
/// `run_post_job_maintenance` so a long run reclaims the log between jobs
/// rather than only when everything goes idle. The unqualified pragma covers
/// every attached schema (index + storage), unlike most pragmas. If an open
/// read snapshot blocks it, the pragma waits out sqlx's busy timeout (5s),
/// does what a passive checkpoint can, and reports busy without erroring;
/// the log is then reclaimed at a later reset via the `journal_size_limit`
/// set in connection.rs.
const CHECKPOINT_STATEMENTS: &[&str] = &["PRAGMA wal_checkpoint(TRUNCATE)"];

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct IndexDbKey {
    index_db: String,
}

pub(crate) enum IndexDbWriterMessage {
    AddFileScan {
        scan_time: String,
        path: String,
        reply: Reply<i64>,
    },
    UpdateFileScan {
        scan_id: i64,
        update: FileScanUpdate,
        reply: Reply<()>,
    },
    CloseFileScan {
        scan_id: i64,
        end_time: String,
        reply: Reply<()>,
    },
    MarkUnavailableFiles {
        scan_id: i64,
        path: String,
        excluded_paths: Vec<String>,
        reply: Reply<(i64, i64)>,
    },
    UpdateFileData {
        time_added: String,
        scan_id: i64,
        data: FileScanData,
        reply: Reply<FileUpsertResult>,
    },
    StoreThumbnails {
        sha256: String,
        mime_type: String,
        process_version: i64,
        thumbnails: Vec<StoredImage>,
        reply: Reply<()>,
    },
    StoreFrames {
        sha256: String,
        mime_type: String,
        process_version: i64,
        frames: Vec<StoredImage>,
        reply: Reply<()>,
    },
    /// One genuine outro verdict (docs/video-outro-detection-design.md §7.2).
    /// The `items` write and the probe marker's delete share this one
    /// transaction, mirroring [`Self::StoreThumbnails`] — connections have
    /// both databases attached, so the negative cache can never outlive the
    /// answer that retired it. Probe *failures* never come through here: they
    /// are `UpsertVisualAttempts` records, because `outro_kind` only ever
    /// holds verdicts.
    SetOutroVerdict {
        sha256: String,
        outro_kind: String,
        content_end_ms: Option<i64>,
        reply: Reply<u64>,
    },
    /// One item's stream codecs (docs/video-transcoding-design.md §6). Unlike
    /// [`Self::SetOutroVerdict`] this carries no marker delete: the codec pass
    /// writes nothing to the negative cache, so there is nothing to retire.
    SetItemCodecs {
        sha256: String,
        video_codec: String,
        audio_codec: Option<String>,
        reply: Reply<u64>,
    },
    RenameFilePath {
        old_path: String,
        new_path: String,
        scan_id: i64,
        last_modified: String,
        reply: Reply<bool>,
    },
    DeleteFileByPath {
        path: String,
        reply: Reply<u64>,
    },
    DeleteItemIfOrphan {
        item_id: i64,
        reply: Reply<bool>,
    },
    SetBlurhash {
        sha256: String,
        blurhash: String,
        reply: Reply<()>,
    },
    DeleteUnavailableFiles {
        reply: Reply<u64>,
    },
    DeleteItemsWithoutFiles {
        batch_size: i64,
        reply: Reply<u64>,
    },
    DeleteFilesNotAllowed {
        job_filters: Vec<crate::pql::model::JobFilter>,
        reply: Reply<u64>,
    },
    DeleteOrphanedFrames {
        reply: Reply<u64>,
    },
    DeleteOrphanedThumbnails {
        reply: Reply<u64>,
    },
    /// The negative cache's half of the orphan sweep. Its count is kept out of
    /// the caller's deletion flag — see `delete_orphaned_visual_attempts`.
    DeleteOrphanedVisualAttempts {
        reply: Reply<u64>,
    },
    /// Records what a visuals generation pass concluded
    /// (docs/failed-media-retry-design.md). One message per file: a single
    /// pass can owe both a thumbnail and a frame marker, and they are one
    /// conclusion, so they commit together and cost one search-cache epoch
    /// bump instead of two. Advisory — a failed write costs one wasted
    /// regeneration next scan, which is exactly today's behavior.
    UpsertVisualAttempts {
        records: Vec<VisualAttemptRecord>,
        scan_id: Option<i64>,
        reply: Reply<()>,
    },
    /// Auto-heal for the visuals cache, the twin of
    /// [`Self::ClearBlockedScanErrors`] one database over.
    ClearBlockedVisualAttempts {
        blockers: Vec<Blocker>,
        reply: Reply<u64>,
    },
    DeleteJobData {
        log_id: i64,
        reply: Reply<u64>,
    },
    RemoveIncompleteJobs {
        reply: Reply<()>,
    },
    AddDataLog {
        scan_time: String,
        threshold: Option<f64>,
        types: Vec<String>,
        setter: String,
        batch_size: i64,
        reply: Reply<i64>,
    },
    UpdateDataLog {
        job_id: i64,
        update: DataLogUpdate,
        reply: Reply<()>,
    },
    UpsertSetter {
        setter_name: String,
        reply: Reply<i64>,
    },
    /// Records one non-transient extraction failure
    /// (docs/failed-media-retry-design.md). The reply is what makes a failed
    /// ledger write systemic rather than input-side: the item task returns
    /// `Err`, so a DB outage can never soft-complete a job as "all corrupt
    /// media".
    UpsertExtractionError {
        record: ExtractionErrorRecord,
        reply: Reply<()>,
    },
    /// Success path: the setter can process this item after all.
    DeleteExtractionError {
        item_sha256: String,
        setter_name: String,
        reply: Reply<u64>,
    },
    /// Auto-heal: clears the `blocked` rows of every dependency that now
    /// binds, so those items become selectable in the same run.
    ClearBlockedErrors {
        blockers: Vec<Blocker>,
        reply: Reply<u64>,
    },
    /// Records one non-transient filescan failure
    /// (docs/failed-media-retry-design.md). The scan already routes every
    /// write through this actor, so the ledger rides the same serialized path
    /// rather than opening a second writer. `scan_id` is the run that saw the
    /// failure, which is what dedups `attempts`.
    UpsertScanError {
        record: ScanErrorRecord,
        scan_id: Option<i64>,
        reply: Reply<()>,
    },
    /// Success path: the scan can process this path after all.
    DeleteScanError {
        path: String,
        reply: Reply<u64>,
    },
    /// False-change path: the bytes provably did not move (same sha256 under
    /// a new mtime), so an audit-only row follows the stat instead of being
    /// cleared by it. Verdict and counters stay put.
    RekeyScanError {
        path: String,
        last_modified: String,
        file_size: i64,
        reply: Reply<u64>,
    },
    /// End-of-root sweep: rows the walk never reached, in one statement.
    DeleteScanErrors {
        paths: Vec<String>,
        reply: Reply<u64>,
    },
    /// Auto-heal for the scan ledger, the twin of [`Self::ClearBlockedErrors`].
    ClearBlockedScanErrors {
        blockers: Vec<Blocker>,
        reply: Reply<u64>,
    },
    WriteTagsOutput {
        job_id: i64,
        setter_name: String,
        item_sha256: String,
        tags: Vec<TagEntry>,
        text_entries: Vec<TagTextEntry>,
        reply: Reply<()>,
    },
    WriteTextOutput {
        job_id: i64,
        setter_name: String,
        item_sha256: String,
        entries: Vec<TextEntry>,
        reply: Reply<()>,
    },
    WriteClipOutput {
        job_id: i64,
        setter_name: String,
        item_sha256: String,
        entries: Vec<EmbeddingEntry>,
        reply: Reply<()>,
    },
    WriteTextEmbeddingOutput {
        job_id: i64,
        setter_name: String,
        item_sha256: String,
        source_data_id: Option<i64>,
        entries: Vec<EmbeddingEntry>,
        reply: Reply<()>,
    },
    /// Deletes a setter and (for tag setters) the orphaned tags it leaves
    /// behind in one transaction, so a crash can't leave dangling tag rows
    /// visible in tag lists.
    DeleteSetterData {
        setter_name: String,
        include_orphan_tags: bool,
        /// (setter rows deleted, orphan tags deleted)
        reply: Reply<(u64, u64)>,
    },
    AddFolderToDatabase {
        time_added: String,
        path: String,
        included: bool,
        reply: Reply<bool>,
    },
    DeleteFoldersNotInList {
        folder_paths: Vec<String>,
        included: bool,
        reply: Reply<u64>,
    },
    DeleteFilesUnderExcludedFolders {
        reply: Reply<u64>,
    },
    DeleteFilesNotUnderIncludedFolders {
        reply: Reply<u64>,
    },
    /// Applies the vector-quant metadata diff (profiles/coverage rows) for
    /// the given desired state in one transaction. Replies whether anything
    /// changed.
    VectorQuantSyncMetadata {
        desired: crate::db::vector_quants::DesiredState,
        reply: Reply<bool>,
    },
    /// Freezes an artifact and moves a space's pairs to `building` under one
    /// new revision (replied).
    VectorQuantStartSpaceBuild {
        profile_id: i64,
        setter_ids: Vec<i64>,
        artifact: Option<Vec<u8>>,
        dim: i64,
        reply: Reply<i64>,
    },
    /// One chunked backfill transaction, resuming after `after_id`; replies
    /// (rows written, cursor for the next chunk). Zero rows = done.
    VectorQuantBackfillChunk {
        profile_id: i64,
        setter_id: i64,
        limit: i64,
        after_id: i64,
        reply: Reply<(u64, i64)>,
    },
    /// Verifies the coverage invariant and flips a space's pairs to ready.
    VectorQuantFinishSpaceBuild {
        profile_id: i64,
        setter_ids: Vec<i64>,
        reply: Reply<()>,
    },
    /// One chunked delete of a removing profile's quants (0 = done).
    VectorQuantDeleteChunk {
        profile_id: i64,
        limit: i64,
        reply: Reply<u64>,
    },
    /// Drops a removing profile row once its quants are gone.
    VectorQuantDropProfile {
        profile_id: i64,
        reply: Reply<()>,
    },
    /// Marks a space for explicit rebuild (pending, artifact cleared).
    VectorQuantMarkRebuild {
        profile_id: i64,
        setter_ids: Vec<i64>,
        reply: Reply<()>,
    },
    Vacuum {
        reply: Reply<()>,
    },
    Analyze {
        reply: Reply<()>,
    },
    /// Refreshes every `tags.item_count`; see `db::tags::RECOUNT_TAG_ITEMS_SQL`.
    /// Clears the tags-dirty marker in the same transaction, so only a recount
    /// that actually committed pays off the debt.
    RecountTagItems {
        reply: Reply<()>,
    },
    /// Sets the durable tags-dirty marker on its own. The writes that dirty
    /// the counts set it inline (see `WriteTagsOutput` and
    /// `DeleteItemIfOrphan`); this is for the job boundary, which learns about
    /// bulk deletions from a finished job's report rather than row by row.
    MarkTagsDirty {
        reply: Reply<()>,
    },
    /// Truncating WAL checkpoint; see `CHECKPOINT_STATEMENTS`.
    Checkpoint {
        reply: Reply<()>,
    },
    /// No-op barrier: the writer handles messages in order, so a reply proves
    /// every previously queued write has committed. Used at process shutdown.
    Flush {
        reply: Reply<()>,
    },
    IdleCheck,
}

pub(crate) struct IndexDbWriter;

pub(crate) struct IndexDbWriterArgs {
    pub index_db: String,
    pub idle_timeout: Duration,
}

pub(crate) struct IndexDbWriterState {
    index_db: String,
    idle_timeout: Duration,
    last_used: Option<Instant>,
    conn: Option<SqliteConnection>,
    /// Latch for the durable tags-dirty marker: true once this writer has set
    /// it and nothing has cleared it since (only `RecountTagItems` does). A
    /// tagging job writes once per item, and the marker means the same thing
    /// after the first of them, so the rest cost nothing. Writer state rather
    /// than a read of the row: a respawned writer starts `false` and pays one
    /// redundant upsert, which is the cheap direction to be wrong in.
    tags_dirty_marked: bool,
}

impl IndexDbWriterState {
    async fn ensure_conn(&mut self) -> ApiResult<&mut SqliteConnection> {
        if self.conn.is_none() {
            let conn = open_index_db_write_no_user_data(&self.index_db).await?;
            self.conn = Some(conn);
        }
        Ok(self.conn.as_mut().expect("connection missing"))
    }

    async fn with_transaction<T, F>(&mut self, op: F) -> ApiResult<T>
    where
        F: for<'a> FnOnce(&'a mut SqliteConnection) -> DbFuture<'a, T>,
    {
        let mut drop_conn = false;
        let result = {
            let conn = self.ensure_conn().await?;
            if let Err(err) = begin_tx(conn).await {
                drop_conn = true;
                Err(err)
            } else {
                let result = op(conn).await;
                match result {
                    Ok(value) => {
                        if let Err(err) = commit_tx(conn).await {
                            drop_conn = true;
                            Err(err)
                        } else {
                            Ok(value)
                        }
                    }
                    Err(err) => {
                        if let Err(rb_err) = rollback_tx(conn).await {
                            drop_conn = true;
                            tracing::error!(error = ?rb_err, "failed to rollback transaction");
                        }
                        Err(err)
                    }
                }
            }
        };

        if drop_conn {
            self.conn = None;
            self.last_used = None;
        } else if self.conn.is_some() {
            self.last_used = Some(Instant::now());
        }

        // Every index-DB mutation commits through here, making it the single
        // choke point for search-cache invalidation. Maintenance statements
        // (VACUUM/ANALYZE) bypass it, which is fine: they don't change data.
        if result.is_ok() {
            crate::db::epochs::bump_index_epoch(&self.index_db);
        }

        result
    }

    /// Runs maintenance statements directly on the connection, outside of
    /// `with_transaction`: VACUUM cannot execute inside a transaction.
    async fn run_maintenance(&mut self, statements: &[&'static str]) -> ApiResult<()> {
        let result = async {
            let conn = self.ensure_conn().await?;
            for statement in statements {
                sqlx::query(*statement)
                    .execute(&mut *conn)
                    .await
                    .map_err(|err| {
                        tracing::error!(error = ?err, statement, "failed to run maintenance statement");
                        ApiError::internal("Failed to run database maintenance")
                    })?;
            }
            Ok(())
        }
        .await;

        match &result {
            Ok(()) => self.last_used = Some(Instant::now()),
            Err(_) => {
                self.conn = None;
                self.last_used = None;
            }
        }

        result
    }
}

impl Actor for IndexDbWriter {
    type Msg = IndexDbWriterMessage;
    type State = IndexDbWriterState;
    type Arguments = IndexDbWriterArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let _ = myself.send_interval(
            RactorDuration::from_secs(args.idle_timeout.as_secs()),
            || IndexDbWriterMessage::IdleCheck,
        );
        Ok(IndexDbWriterState {
            index_db: args.index_db,
            idle_timeout: args.idle_timeout,
            last_used: None,
            conn: None,
            tags_dirty_marked: false,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            IndexDbWriterMessage::IdleCheck => {
                if let (Some(last_used), Some(_)) = (state.last_used, state.conn.as_ref()) {
                    if last_used.elapsed() >= state.idle_timeout {
                        state.conn = None;
                        state.last_used = None;
                        tracing::info!(
                            index_db = %state.index_db,
                            "index db writer connection closed after idle timeout"
                        );
                    }
                }
            }
            IndexDbWriterMessage::AddFileScan {
                scan_time,
                path,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { add_file_scan(conn, &scan_time, &path).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::UpdateFileScan {
                scan_id,
                update,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { update_file_scan(conn, scan_id, update).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::CloseFileScan {
                scan_id,
                end_time,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { close_file_scan(conn, scan_id, &end_time).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::MarkUnavailableFiles {
                scan_id,
                path,
                excluded_paths,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            mark_unavailable_files(conn, scan_id, &path, &excluded_paths).await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::UpdateFileData {
                time_added,
                scan_id,
                data,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            update_file_data(conn, &time_added, scan_id, &data).await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::StoreThumbnails {
                sha256,
                mime_type,
                process_version,
                thumbnails,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            store_thumbnails(
                                conn,
                                &sha256,
                                &mime_type,
                                process_version,
                                &thumbnails,
                            )
                            .await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::StoreFrames {
                sha256,
                mime_type,
                process_version,
                frames,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            store_frames(conn, &sha256, &mime_type, process_version, &frames).await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::SetOutroVerdict {
                sha256,
                outro_kind,
                content_end_ms,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            set_outro_verdict(conn, &sha256, &outro_kind, content_end_ms).await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::SetItemCodecs {
                sha256,
                video_codec,
                audio_codec,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            set_item_codecs(conn, &sha256, &video_codec, audio_codec.as_deref())
                                .await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::RenameFilePath {
                old_path,
                new_path,
                scan_id,
                last_modified,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            rename_file_path(conn, &old_path, &new_path, scan_id, &last_modified)
                                .await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteFileByPath { path, reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { delete_file_by_path(conn, &path).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteItemIfOrphan { item_id, reply } => {
                // Deleting an item cascades into `tags_items`, and the
                // continuous scan — the caller here — is not a queue job, so
                // no job boundary will ever report this deletion. The marker
                // is set in the same transaction as the delete, and only when
                // a row really went away.
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            let deleted = delete_item_if_orphan(conn, item_id).await?;
                            if deleted {
                                crate::db::maintenance_state::set_tags_dirty(conn).await?;
                            }
                            Ok(deleted)
                        })
                    })
                    .await;
                if matches!(result, Ok(true)) {
                    state.tags_dirty_marked = true;
                }
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::SetBlurhash {
                sha256,
                blurhash,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { set_blurhash(conn, &sha256, &blurhash).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteUnavailableFiles { reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { delete_unavailable_files(conn).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteItemsWithoutFiles { batch_size, reply } => {
                // Bulk item deletion cascades into `tags_items`. Marked here,
                // in the deleting transaction, so the debt is durable the
                // moment it exists — the job boundary's report of the same
                // deletion is only a backstop (see `queue::mark_tags_dirty`).
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            let deleted = delete_items_without_files(conn, batch_size).await?;
                            if deleted > 0 {
                                crate::db::maintenance_state::set_tags_dirty(conn).await?;
                            }
                            Ok(deleted)
                        })
                    })
                    .await;
                if matches!(result, Ok(deleted) if deleted > 0) {
                    state.tags_dirty_marked = true;
                }
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteFilesNotAllowed { job_filters, reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { delete_files_not_allowed(conn, &job_filters).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteOrphanedFrames { reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { delete_orphaned_frames(conn).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteOrphanedThumbnails { reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { delete_orphaned_thumbnails(conn).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteOrphanedVisualAttempts { reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { delete_orphaned_visual_attempts(conn).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::UpsertVisualAttempts {
                records,
                scan_id,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(
                            async move { upsert_visual_attempts(conn, &records, scan_id).await },
                        )
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::ClearBlockedVisualAttempts { blockers, reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(
                            async move { delete_blocked_visual_attempts(conn, &blockers).await },
                        )
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteJobData { log_id, reply } => {
                // Deleting the `data_jobs` row cascades through `item_data`
                // into `tags_items`.
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            let deleted = delete_data_job_by_log_id(conn, log_id).await?;
                            if deleted > 0 {
                                crate::db::maintenance_state::set_tags_dirty(conn).await?;
                            }
                            Ok(deleted)
                        })
                    })
                    .await;
                if matches!(result, Ok(deleted) if deleted > 0) {
                    state.tags_dirty_marked = true;
                }
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::RemoveIncompleteJobs { reply } => {
                // Under `atomic_extraction_jobs` this deletes the unfinished
                // jobs' rows, which cascades into `tags_items` exactly like
                // `DeleteJobData`. The reply stays `()`; the count is only
                // needed to decide the marker.
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            let deleted = remove_incomplete_jobs(conn).await?;
                            if deleted > 0 {
                                crate::db::maintenance_state::set_tags_dirty(conn).await?;
                            }
                            Ok(deleted)
                        })
                    })
                    .await;
                if matches!(result, Ok(deleted) if deleted > 0) {
                    state.tags_dirty_marked = true;
                }
                let _ = reply.send(result.map(|_| ()));
            }
            IndexDbWriterMessage::AddDataLog {
                scan_time,
                threshold,
                types,
                setter,
                batch_size,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            add_data_log(conn, &scan_time, threshold, &types, &setter, batch_size)
                                .await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::UpdateDataLog {
                job_id,
                update,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { update_data_log(conn, job_id, &update).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::UpsertSetter { setter_name, reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { upsert_setter(conn, &setter_name).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::UpsertExtractionError { record, reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { upsert_extraction_error(conn, &record).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteExtractionError {
                item_sha256,
                setter_name,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            delete_extraction_error(conn, &item_sha256, &setter_name).await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::ClearBlockedErrors { blockers, reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { delete_blocked_errors(conn, &blockers).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::UpsertScanError {
                record,
                scan_id,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { upsert_scan_error(conn, &record, scan_id).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteScanError { path, reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { delete_scan_error(conn, &path).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::RekeyScanError {
                path,
                last_modified,
                file_size,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            rekey_scan_error(conn, &path, &last_modified, file_size).await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteScanErrors { paths, reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { delete_scan_errors(conn, &paths).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::ClearBlockedScanErrors { blockers, reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { delete_blocked_scan_errors(conn, &blockers).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::WriteTagsOutput {
                job_id,
                setter_name,
                item_sha256,
                tags,
                text_entries,
                reply,
            } => {
                // Tag writes are what make `tags.item_count` stale, and a
                // tagging job is not atomic: a shutdown halfway through has
                // already committed tags. So the marker rides along in the
                // same transaction as the first write of this writer session,
                // rather than waiting for the job to report anything.
                //
                // Content only: `write_placeholder` sends this message with
                // empty vectors to mark an item processed after its inference
                // failed, and a job whose every item failed writes nothing a
                // recount could see.
                let mark_dirty =
                    !state.tags_dirty_marked && !(tags.is_empty() && text_entries.is_empty());
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            write_tags_output(
                                conn,
                                job_id,
                                &setter_name,
                                &item_sha256,
                                &tags,
                                &text_entries,
                            )
                            .await?;
                            if mark_dirty {
                                crate::db::maintenance_state::set_tags_dirty(conn).await?;
                            }
                            Ok(())
                        })
                    })
                    .await;
                if mark_dirty && result.is_ok() {
                    state.tags_dirty_marked = true;
                }
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::WriteTextOutput {
                job_id,
                setter_name,
                item_sha256,
                entries,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            write_text_output(conn, job_id, &setter_name, &item_sha256, &entries)
                                .await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::WriteClipOutput {
                job_id,
                setter_name,
                item_sha256,
                entries,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            write_clip_output(conn, job_id, &setter_name, &item_sha256, &entries)
                                .await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::WriteTextEmbeddingOutput {
                job_id,
                setter_name,
                item_sha256,
                source_data_id,
                entries,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            write_text_embedding_output(
                                conn,
                                job_id,
                                &setter_name,
                                &item_sha256,
                                source_data_id,
                                &entries,
                            )
                            .await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteSetterData {
                setter_name,
                include_orphan_tags,
                reply,
            } => {
                // Deleting the setter cascades its `item_data` (and with it
                // `tags_items`); `delete_orphan_tags` then removes `tags` rows
                // outright. Either one invalidates every count.
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            let deleted = delete_setter_by_name(conn, &setter_name).await?;
                            let orphan_tags = if include_orphan_tags {
                                delete_orphan_tags(conn).await?
                            } else {
                                0
                            };
                            if deleted > 0 || orphan_tags > 0 {
                                crate::db::maintenance_state::set_tags_dirty(conn).await?;
                            }
                            Ok((deleted, orphan_tags))
                        })
                    })
                    .await;
                if matches!(result, Ok((deleted, orphan_tags)) if deleted > 0 || orphan_tags > 0) {
                    state.tags_dirty_marked = true;
                }
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::AddFolderToDatabase {
                time_added,
                path,
                included,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            add_folder_to_database(conn, &time_added, &path, included).await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteFoldersNotInList {
                folder_paths,
                included,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            delete_folders_not_in_list(conn, &folder_paths, included).await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteFilesUnderExcludedFolders { reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { delete_files_under_excluded_folders(conn).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::DeleteFilesNotUnderIncludedFolders { reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move { delete_files_not_under_included_folders(conn).await })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::VectorQuantSyncMetadata { desired, reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            crate::db::vector_quants::sync_metadata(conn, desired).await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::VectorQuantStartSpaceBuild {
                profile_id,
                setter_ids,
                artifact,
                dim,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            crate::db::vector_quants::start_space_build(
                                conn,
                                profile_id,
                                &setter_ids,
                                artifact.as_deref(),
                                dim,
                            )
                            .await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::VectorQuantBackfillChunk {
                profile_id,
                setter_id,
                limit,
                after_id,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            crate::db::vector_quants::backfill_chunk(
                                conn, profile_id, setter_id, limit, after_id,
                            )
                            .await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::VectorQuantFinishSpaceBuild {
                profile_id,
                setter_ids,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            crate::db::vector_quants::finish_space_build(
                                conn,
                                profile_id,
                                &setter_ids,
                            )
                            .await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::VectorQuantDeleteChunk {
                profile_id,
                limit,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            crate::db::vector_quants::delete_quants_chunk(conn, profile_id, limit)
                                .await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::VectorQuantDropProfile { profile_id, reply } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            crate::db::vector_quants::drop_profile(conn, profile_id).await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::VectorQuantMarkRebuild {
                profile_id,
                setter_ids,
                reply,
            } => {
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            crate::db::vector_quants::mark_space_rebuild(
                                conn,
                                profile_id,
                                &setter_ids,
                            )
                            .await
                        })
                    })
                    .await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::Vacuum { reply } => {
                tracing::info!(
                    index_db = %state.index_db,
                    "running VACUUM on index database; this may take a while"
                );
                // A schema-less VACUUM only compacts `main`; the attached
                // storage database holds the thumbnail/frame blobs whose
                // deletion is what usually triggers this in the first place.
                let result = state.run_maintenance(&["VACUUM", "VACUUM storage"]).await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::Analyze { reply } => {
                let result = state.run_maintenance(ANALYZE_STATEMENTS).await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::RecountTagItems { reply } => {
                // The clear commits with the rebuild or not at all: a recount
                // that failed, or that was interrupted, must leave the debt
                // where the next maintenance pass will find it.
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(async move {
                            crate::db::tags::recount_tag_items(conn).await?;
                            crate::db::maintenance_state::clear_tags_dirty(conn).await
                        })
                    })
                    .await;
                if result.is_ok() {
                    state.tags_dirty_marked = false;
                }
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::MarkTagsDirty { reply } => {
                // Unlatched: this arrives once per job boundary at most, and a
                // marker that is already set costs one no-op upsert, while a
                // stale latch would cost a skipped recount.
                let result = state
                    .with_transaction(move |conn| {
                        Box::pin(
                            async move { crate::db::maintenance_state::set_tags_dirty(conn).await },
                        )
                    })
                    .await;
                if result.is_ok() {
                    state.tags_dirty_marked = true;
                }
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::Checkpoint { reply } => {
                let result = state.run_maintenance(CHECKPOINT_STATEMENTS).await;
                let _ = reply.send(result);
            }
            IndexDbWriterMessage::Flush { reply } => {
                let _ = reply.send(Ok(()));
            }
        }
        Ok(())
    }
}

pub(crate) enum IndexDbSupervisorMessage {
    GetWriter {
        index_db: String,
        force_new: bool,
        reply: Reply<ActorRef<IndexDbWriterMessage>>,
    },
    HealthCheck,
    /// Barrier across every live writer; replies with the number of writers
    /// that acknowledged once each has processed all previously queued
    /// messages. Used at process shutdown.
    FlushAll {
        reply: oneshot::Sender<usize>,
    },
}

pub(crate) struct IndexDbSupervisor;

pub(crate) struct IndexDbSupervisorArgs {
    pub health_interval: Duration,
    pub idle_timeout: Duration,
}

pub(crate) struct IndexDbSupervisorState {
    writers: HashMap<IndexDbKey, ActorRef<IndexDbWriterMessage>>,
    idle_timeout: Duration,
}

impl Actor for IndexDbSupervisor {
    type Msg = IndexDbSupervisorMessage;
    type State = IndexDbSupervisorState;
    type Arguments = IndexDbSupervisorArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let _ = myself.send_interval(
            RactorDuration::from_secs(args.health_interval.as_secs()),
            || IndexDbSupervisorMessage::HealthCheck,
        );
        Ok(IndexDbSupervisorState {
            writers: HashMap::new(),
            idle_timeout: args.idle_timeout,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            IndexDbSupervisorMessage::GetWriter {
                index_db,
                force_new,
                reply,
            } => {
                let key = IndexDbKey {
                    index_db: index_db.clone(),
                };
                if let Some(existing) = state.writers.get(&key) {
                    if force_new {
                        // Only force a respawn if the existing writer is actually dead.
                        if existing
                            .send_message(IndexDbWriterMessage::IdleCheck)
                            .is_err()
                        {
                            if let Some(existing) = state.writers.remove(&key) {
                                existing.stop(None);
                            }
                        } else {
                            let _ = reply.send(Ok(existing.clone()));
                            return Ok(());
                        }
                    } else {
                        let _ = reply.send(Ok(existing.clone()));
                        return Ok(());
                    }
                }

                let writer = spawn_writer(&index_db, state.idle_timeout).await;
                match writer {
                    Ok(writer) => {
                        state.writers.insert(key, writer.clone());
                        let _ = reply.send(Ok(writer));
                    }
                    Err(err) => {
                        let _ = reply.send(Err(err));
                    }
                }
            }
            IndexDbSupervisorMessage::HealthCheck => {
                let mut to_remove = Vec::new();
                for (key, writer) in state.writers.iter() {
                    let paths = index_storage_paths_unchecked(&key.index_db);
                    if !paths.index_db_file.exists() || !paths.storage_db_file.exists() {
                        tracing::warn!(
                            index_db = %key.index_db,
                            "index db files missing during health check"
                        );
                        to_remove.push(key.clone());
                        continue;
                    }

                    match ping_db(&key.index_db).await {
                        Ok(()) => {}
                        Err(err) => {
                            tracing::warn!(
                                error = ?err,
                                index_db = %key.index_db,
                                "index db health check failed"
                            );
                            to_remove.push(key.clone());
                        }
                    }

                    if !to_remove.contains(key) {
                        if writer
                            .send_message(IndexDbWriterMessage::IdleCheck)
                            .is_err()
                        {
                            to_remove.push(key.clone());
                        }
                    }
                }

                for key in to_remove {
                    if let Some(writer) = state.writers.remove(&key) {
                        writer.stop(None);
                    }
                }
            }
            IndexDbSupervisorMessage::FlushAll { reply } => {
                let mut receivers = Vec::new();
                for writer in state.writers.values() {
                    let (tx, rx) = oneshot::channel();
                    if writer
                        .send_message(IndexDbWriterMessage::Flush { reply: tx })
                        .is_ok()
                    {
                        receivers.push(rx);
                    }
                }
                let mut flushed = 0;
                for rx in receivers {
                    if rx.await.is_ok() {
                        flushed += 1;
                    }
                }
                let _ = reply.send(flushed);
            }
        }
        Ok(())
    }
}

static SUPERVISOR: OnceLock<Mutex<Option<ActorRef<IndexDbSupervisorMessage>>>> = OnceLock::new();

pub(crate) async fn get_index_db_writer(
    index_db: &str,
) -> ApiResult<ActorRef<IndexDbWriterMessage>> {
    get_index_db_writer_inner(index_db, false).await
}

async fn get_index_db_writer_fresh(index_db: &str) -> ApiResult<ActorRef<IndexDbWriterMessage>> {
    get_index_db_writer_inner(index_db, true).await
}

async fn get_index_db_writer_inner(
    index_db: &str,
    force_new: bool,
) -> ApiResult<ActorRef<IndexDbWriterMessage>> {
    for attempt in 0..CALL_RETRY_ATTEMPTS {
        let supervisor = if attempt == 0 {
            ensure_supervisor().await?
        } else {
            replace_supervisor().await?
        };
        let (reply, rx) = oneshot::channel();
        if supervisor
            .send_message(IndexDbSupervisorMessage::GetWriter {
                index_db: index_db.to_string(),
                force_new,
                reply,
            })
            .is_err()
        {
            continue;
        }
        return rx
            .await
            .map_err(|_| ApiError::internal("Index DB supervisor dropped response"))?;
    }
    Err(ApiError::internal("Index DB supervisor unavailable"))
}

/// Sends a request to the writer with a single retry on writer death.
/// The builder may be called more than once; use Arc/cloneable payloads if needed.
pub(crate) async fn call_index_db_writer<T, F>(index_db: &str, mut build: F) -> ApiResult<T>
where
    F: FnMut(Reply<T>) -> IndexDbWriterMessage,
{
    let mut last_err = None;
    for attempt in 0..CALL_RETRY_ATTEMPTS {
        let writer = if attempt == 0 {
            get_index_db_writer(index_db).await?
        } else {
            get_index_db_writer_fresh(index_db).await?
        };
        let (reply, rx) = oneshot::channel();
        let msg = build(reply);
        if writer.send_message(msg).is_err() {
            last_err = Some(ApiError::internal("Index DB writer unavailable"));
            continue;
        }

        match rx.await {
            Ok(result) => return result,
            Err(_) => {
                last_err = Some(ApiError::internal("Index DB writer dropped response"));
            }
        }
    }

    Err(last_err.unwrap_or_else(|| ApiError::internal("Index DB writer unavailable")))
}

/// Drains every live index DB writer (a message-ordering barrier, not an
/// fsync). Returns the number of writers that acknowledged; 0 when the
/// supervisor was never started. Used at process shutdown.
pub(crate) async fn flush_all_writers() -> usize {
    let Some(cell) = SUPERVISOR.get() else {
        return 0;
    };
    let supervisor = cell.lock().await.clone();
    let Some(supervisor) = supervisor else {
        return 0;
    };
    let (reply, rx) = oneshot::channel();
    if supervisor
        .send_message(IndexDbSupervisorMessage::FlushAll { reply })
        .is_err()
    {
        return 0;
    }
    rx.await.unwrap_or(0)
}

async fn ensure_supervisor() -> ApiResult<ActorRef<IndexDbSupervisorMessage>> {
    let cell = SUPERVISOR.get_or_init(|| Mutex::new(None));
    let mut guard = cell.lock().await;
    if let Some(actor) = guard.as_ref() {
        return Ok(actor.clone());
    }
    let actor = spawn_supervisor().await?;
    *guard = Some(actor.clone());
    Ok(actor)
}

async fn replace_supervisor() -> ApiResult<ActorRef<IndexDbSupervisorMessage>> {
    let cell = SUPERVISOR.get_or_init(|| Mutex::new(None));
    let mut guard = cell.lock().await;
    let actor = spawn_supervisor().await?;
    *guard = Some(actor.clone());
    Ok(actor)
}

async fn spawn_supervisor() -> ApiResult<ActorRef<IndexDbSupervisorMessage>> {
    let args = IndexDbSupervisorArgs {
        health_interval: HEALTH_CHECK_INTERVAL,
        idle_timeout: IDLE_TIMEOUT,
    };
    let (actor, _handle) = Actor::spawn(None, IndexDbSupervisor, args)
        .await
        .map_err(|err| {
            tracing::error!(error = ?err, "failed to start index db supervisor");
            ApiError::internal("Failed to start index DB supervisor")
        })?;
    Ok(actor)
}

async fn spawn_writer(
    index_db: &str,
    idle_timeout: Duration,
) -> ApiResult<ActorRef<IndexDbWriterMessage>> {
    let name = format!("index-db-writer-{}", sanitize_name(index_db));
    let args = IndexDbWriterArgs {
        index_db: index_db.to_string(),
        idle_timeout,
    };
    let (actor, _handle) = Actor::spawn(Some(name), IndexDbWriter, args)
        .await
        .map_err(|err| {
            tracing::error!(error = ?err, index_db, "failed to start index db writer");
            ApiError::internal("Failed to start index DB writer")
        })?;
    Ok(actor)
}

fn sanitize_name(input: &str) -> String {
    input
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

async fn ping_db(index_db: &str) -> ApiResult<()> {
    let mut conn = open_index_db_read_no_user_data(index_db).await?;
    sqlx::query("SELECT 1")
        .execute(&mut conn)
        .await
        .map_err(|err| {
            tracing::error!(error = ?err, "failed to ping index db");
            ApiError::internal("Failed to ping index database")
        })?;
    Ok(())
}

async fn begin_tx(conn: &mut SqliteConnection) -> ApiResult<()> {
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = ?err, "failed to begin transaction");
            ApiError::internal("Failed to begin transaction")
        })?;
    Ok(())
}

async fn commit_tx(conn: &mut SqliteConnection) -> ApiResult<()> {
    sqlx::query("COMMIT")
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = ?err, "failed to commit transaction");
            ApiError::internal("Failed to commit transaction")
        })?;
    Ok(())
}

async fn rollback_tx(conn: &mut SqliteConnection) -> ApiResult<()> {
    sqlx::query("ROLLBACK")
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = ?err, "failed to rollback transaction");
            ApiError::internal("Failed to rollback transaction")
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use sqlx::Row;

    use super::*;
    use crate::db::extraction_write::TagEntry;
    use crate::db::maintenance_state::{clear_tags_dirty, read_tags_dirty};
    use crate::db::migrations::{migrate_databases_on_disk, setup_test_databases};
    use crate::test_utils::test_data_dir;

    fn next_db_name() -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        format!("writer_marker_{}", COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    /// An on-disk index DB with one setter, one data-log row and `items`
    /// count items. The writer actor is the thing under test here, so
    /// everything it does not own is inserted directly.
    async fn marker_test_db(items: usize) -> (String, i64) {
        let index_db = next_db_name();
        let user_data_db = next_db_name();
        migrate_databases_on_disk(Some(&index_db), Some(&user_data_db))
            .await
            .expect("migrate test databases");
        {
            let mut conn = crate::db::open_index_db_write_no_user_data(&index_db)
                .await
                .unwrap();
            for i in 0..items {
                sqlx::query(
                    "INSERT INTO items (sha256, md5, type, time_added) \
                     VALUES (?, ?, 'image/png', '2026-01-01')",
                )
                .bind(format!("sha{i}"))
                .bind(format!("md5{i}"))
                .execute(&mut conn)
                .await
                .unwrap();
            }
        }
        call_index_db_writer(&index_db, |reply| IndexDbWriterMessage::UpsertSetter {
            setter_name: "test/tagger".to_string(),
            reply,
        })
        .await
        .unwrap();
        let job_id = call_index_db_writer(&index_db, |reply| IndexDbWriterMessage::AddDataLog {
            scan_time: "2026-01-01T00:00:00".to_string(),
            threshold: None,
            types: vec!["tags".to_string()],
            setter: "test/tagger".to_string(),
            batch_size: 1,
            reply,
        })
        .await
        .unwrap();
        (index_db, job_id)
    }

    async fn write_one_tag(index_db: &str, job_id: i64, sha256: &str, tag: &str) {
        let tag = tag.to_string();
        call_index_db_writer(index_db, move |reply| {
            IndexDbWriterMessage::WriteTagsOutput {
                job_id,
                setter_name: "test/tagger".to_string(),
                item_sha256: sha256.to_string(),
                tags: vec![TagEntry {
                    namespace: "general".to_string(),
                    name: tag.clone(),
                    confidence: 1.0,
                }],
                text_entries: Vec::new(),
                reply,
            }
        })
        .await
        .unwrap();
    }

    async fn recount(index_db: &str) {
        call_index_db_writer(index_db, |reply| IndexDbWriterMessage::RecountTagItems { reply })
            .await
            .unwrap();
    }

    async fn marker_is_set(index_db: &str) -> bool {
        let mut conn = crate::db::open_index_db_read_no_user_data(index_db)
            .await
            .unwrap();
        read_tags_dirty(&mut conn).await.unwrap()
    }

    /// Clears the marker behind the writer's back, so the next assertion sees
    /// whether the writer wrote it *again* rather than whether it was ever set.
    async fn clear_marker_behind_the_writer(index_db: &str) {
        let mut conn = crate::db::open_index_db_write_no_user_data(index_db)
            .await
            .unwrap();
        clear_tags_dirty(&mut conn).await.unwrap();
    }

    // The durable marker's whole lifecycle on the writer side: a tag write
    // sets it in the same transaction as the tags, the latch keeps every
    // following write in that session from touching it again, a successful
    // recount clears it *and* the latch, and the next write sets it afresh.
    #[tokio::test]
    async fn tag_writes_set_the_marker_once_per_writer_session() {
        let _test_env = test_data_dir();
        let (index_db, job_id) = marker_test_db(3).await;

        // The migration seeds it dirty; a recount is the only thing that
        // clears it, and that also resets the writer's latch.
        recount(&index_db).await;
        assert!(
            !marker_is_set(&index_db).await,
            "a successful recount must clear the marker"
        );

        write_one_tag(&index_db, job_id, "sha0", "first").await;
        assert!(
            marker_is_set(&index_db).await,
            "the first tag write of a writer session must set the marker"
        );

        clear_marker_behind_the_writer(&index_db).await;
        write_one_tag(&index_db, job_id, "sha1", "second").await;
        assert!(
            !marker_is_set(&index_db).await,
            "the latch must keep later tag writes from re-writing the marker"
        );

        // A recount resets the latch, so the session starts marking again.
        recount(&index_db).await;
        write_one_tag(&index_db, job_id, "sha2", "third").await;
        assert!(
            marker_is_set(&index_db).await,
            "a recount must re-arm the marker for the writes that follow it"
        );
    }

    // The continuous scan is not a queue job and has no boundary, so its item
    // deletions have to mark the DB themselves — but only when a row really
    // went away, or every no-op call would owe a full recount.
    #[tokio::test]
    async fn orphan_item_deletion_marks_only_when_it_deletes() {
        let _test_env = test_data_dir();
        let (index_db, _job_id) = marker_test_db(1).await;
        let item_id: i64 = {
            let mut conn = crate::db::open_index_db_read_no_user_data(&index_db)
                .await
                .unwrap();
            sqlx::query_scalar("SELECT id FROM items WHERE sha256 = 'sha0'")
                .fetch_one(&mut conn)
                .await
                .unwrap()
        };
        clear_marker_behind_the_writer(&index_db).await;

        // The item has no files, so it really is an orphan.
        let deleted = call_index_db_writer(&index_db, |reply| {
            IndexDbWriterMessage::DeleteItemIfOrphan { item_id, reply }
        })
        .await
        .unwrap();
        assert!(deleted, "the orphan item should have been deleted");
        assert!(
            marker_is_set(&index_db).await,
            "an item deletion cascades into tags_items and must mark the DB"
        );

        // Second call: the row is already gone, so nothing changed.
        clear_marker_behind_the_writer(&index_db).await;
        let deleted = call_index_db_writer(&index_db, |reply| {
            IndexDbWriterMessage::DeleteItemIfOrphan { item_id, reply }
        })
        .await
        .unwrap();
        assert!(!deleted);
        assert!(
            !marker_is_set(&index_db).await,
            "a delete that removed nothing must not owe a recount"
        );
    }

    // A tagging job marks every item it processed, including the ones whose
    // inference failed: `write_placeholder` sends this message with empty
    // vectors purely to record "processed". Those writes add no tag rows, so a
    // job where every item failed must not buy a full recount.
    #[tokio::test]
    async fn a_placeholder_only_tag_write_leaves_the_marker_clean() {
        let _test_env = test_data_dir();
        let (index_db, job_id) = marker_test_db(2).await;
        recount(&index_db).await;
        assert!(!marker_is_set(&index_db).await);

        call_index_db_writer(&index_db, move |reply| {
            IndexDbWriterMessage::WriteTagsOutput {
                job_id,
                setter_name: "test/tagger".to_string(),
                item_sha256: "sha0".to_string(),
                tags: Vec::new(),
                text_entries: Vec::new(),
                reply,
            }
        })
        .await
        .unwrap();
        assert!(
            !marker_is_set(&index_db).await,
            "a placeholder write adds no tag rows and must not dirty the DB"
        );

        // The latch is untouched by it, so the first real write still marks.
        write_one_tag(&index_db, job_id, "sha1", "real").await;
        assert!(marker_is_set(&index_db).await);
    }

    // Every writer handler whose transaction removes rows the tag counts
    // depend on marks the DB inside that same transaction. Without this the
    // only record of a job-end bulk deletion is the queue's detached cast,
    // which a shutdown can lose — and the counts then overstate those tags
    // until something unrelated dirties the DB again.
    #[tokio::test]
    async fn bulk_deletions_mark_the_db_inside_their_own_transaction() {
        let _test_env = test_data_dir();
        let (index_db, job_id) = marker_test_db(2).await;
        write_one_tag(&index_db, job_id, "sha0", "bulk").await;

        // Orphan items (no files) — the folder-scan cleanup path.
        recount(&index_db).await;
        let deleted = call_index_db_writer(&index_db, |reply| {
            IndexDbWriterMessage::DeleteItemsWithoutFiles {
                batch_size: 10,
                reply,
            }
        })
        .await
        .unwrap();
        assert!(deleted > 0, "the seeded items have no files");
        assert!(
            marker_is_set(&index_db).await,
            "bulk item deletion cascades into tags_items"
        );

        // Job data — `DELETE FROM data_jobs` cascades through item_data.
        let (index_db, job_id) = marker_test_db(1).await;
        write_one_tag(&index_db, job_id, "sha0", "bulk").await;
        let log_id: i64 = {
            let mut conn = crate::db::open_index_db_read_no_user_data(&index_db)
                .await
                .unwrap();
            sqlx::query_scalar("SELECT id FROM data_log")
                .fetch_one(&mut conn)
                .await
                .unwrap()
        };
        recount(&index_db).await;
        let deleted = call_index_db_writer(&index_db, |reply| {
            IndexDbWriterMessage::DeleteJobData { log_id, reply }
        })
        .await
        .unwrap();
        assert!(deleted > 0, "the seeded job row should be deleted");
        assert!(marker_is_set(&index_db).await, "job data carries tag rows");

        // Setter data, with the orphan-tag sweep the deletion job requests.
        let (index_db, job_id) = marker_test_db(1).await;
        write_one_tag(&index_db, job_id, "sha0", "bulk").await;
        recount(&index_db).await;
        let (deleted, orphan_tags) =
            call_index_db_writer(&index_db, |reply| IndexDbWriterMessage::DeleteSetterData {
                setter_name: "test/tagger".to_string(),
                include_orphan_tags: true,
                reply,
            })
            .await
            .unwrap();
        assert!(deleted > 0 && orphan_tags > 0, "{deleted} {orphan_tags}");
        assert!(marker_is_set(&index_db).await);

        // A setter that does not exist deletes nothing and owes nothing.
        recount(&index_db).await;
        let (deleted, orphan_tags) =
            call_index_db_writer(&index_db, |reply| IndexDbWriterMessage::DeleteSetterData {
                setter_name: "test/absent".to_string(),
                include_orphan_tags: true,
                reply,
            })
            .await
            .unwrap();
        assert_eq!((deleted, orphan_tags), (0, 0));
        assert!(
            !marker_is_set(&index_db).await,
            "a deletion that removed nothing must not owe a recount"
        );
    }

    // Incomplete-job cleanup runs at the start of every extraction job and, in
    // atomic mode, deletes the unfinished jobs' rows — which cascades into
    // `tags_items` exactly like an explicit job-data deletion.
    #[tokio::test]
    async fn incomplete_job_cleanup_marks_the_db_when_it_deletes() {
        let _test_env = test_data_dir();
        let (index_db, job_id) = marker_test_db(1).await;
        write_one_tag(&index_db, job_id, "sha0", "incomplete").await;
        recount(&index_db).await;

        // The seeded data_jobs row is still `completed = 0`.
        call_index_db_writer(&index_db, |reply| {
            IndexDbWriterMessage::RemoveIncompleteJobs { reply }
        })
        .await
        .unwrap();
        let atomic = crate::config::runtime().atomic_extraction_jobs;
        assert_eq!(
            marker_is_set(&index_db).await,
            atomic,
            "atomic cleanup deletes the rows; the non-atomic mode only marks them"
        );

        // Nothing incomplete left: a second pass deletes nothing.
        recount(&index_db).await;
        call_index_db_writer(&index_db, |reply| {
            IndexDbWriterMessage::RemoveIncompleteJobs { reply }
        })
        .await
        .unwrap();
        assert!(
            !marker_is_set(&index_db).await,
            "a cleanup that removed nothing must not owe a recount"
        );
    }

    // The boundary's fire-and-forget cast: job-end bulk deletions are reported
    // as counts, never written row by row, so this is the only way they reach
    // the marker.
    #[tokio::test]
    async fn mark_tags_dirty_sets_the_marker_on_its_own() {
        let _test_env = test_data_dir();
        let (index_db, _job_id) = marker_test_db(0).await;
        clear_marker_behind_the_writer(&index_db).await;

        call_index_db_writer(&index_db, |reply| IndexDbWriterMessage::MarkTagsDirty { reply })
            .await
            .unwrap();
        assert!(marker_is_set(&index_db).await);
    }

    // The extraction ledger's three writer messages, end to end: the
    // pipelines that will send them reach the database only through here, so
    // a mis-wired handler would surface as failures that are never recorded
    // (and items retried forever) rather than as an error.
    #[tokio::test]
    async fn extraction_ledger_messages_round_trip_through_the_writer() {
        let _test_env = test_data_dir();
        let (index_db, job_id) = marker_test_db(1).await;

        let record = crate::db::extraction_errors::ExtractionErrorRecord {
            item_sha256: "sha0".to_string(),
            setter_name: "test/tagger".to_string(),
            stage: crate::db::extraction_errors::STAGE_PREPARE.to_string(),
            kind: crate::api_error::ApiErrorKind::Blocked {
                blocker: Blocker::Pdfium,
            },
            error: "pdfium unavailable".to_string(),
            skip_after: 1,
            job_id: Some(job_id),
        };
        call_index_db_writer(&index_db, |reply| {
            IndexDbWriterMessage::UpsertExtractionError {
                record: record.clone(),
                reply,
            }
        })
        .await
        .unwrap();

        let recorded = {
            let mut conn = crate::db::open_index_db_read_no_user_data(&index_db)
                .await
                .unwrap();
            crate::db::extraction_errors::list_error_sha256s_for_setter(&mut conn, "test/tagger")
                .await
                .unwrap()
                .len()
        };
        assert_eq!(recorded, 1, "the writer's upsert reached the ledger");

        // A blocker that is still missing must not clear anything.
        let cleared = call_index_db_writer(&index_db, |reply| {
            IndexDbWriterMessage::ClearBlockedErrors {
                blockers: vec![Blocker::Ffmpeg],
                reply,
            }
        })
        .await
        .unwrap();
        assert_eq!(cleared, 0);

        let cleared = call_index_db_writer(&index_db, |reply| {
            IndexDbWriterMessage::ClearBlockedErrors {
                blockers: vec![Blocker::Pdfium],
                reply,
            }
        })
        .await
        .unwrap();
        assert_eq!(
            cleared, 1,
            "the dependency appeared; the item is selectable"
        );

        // And the success path removes what is left.
        call_index_db_writer(&index_db, |reply| {
            IndexDbWriterMessage::UpsertExtractionError {
                record: record.clone(),
                reply,
            }
        })
        .await
        .unwrap();
        let deleted = call_index_db_writer(&index_db, |reply| {
            IndexDbWriterMessage::DeleteExtractionError {
                item_sha256: "sha0".to_string(),
                setter_name: "test/tagger".to_string(),
                reply,
            }
        })
        .await
        .unwrap();
        assert_eq!(deleted, 1);
    }

    // Guards the property the constant's comment argues for: post-job
    // maintenance must leave real statistics behind, on a connection with no
    // query history (the writer is always in that state after an idle
    // reconnect). Statement forms that quietly analyze nothing — a bare
    // `PRAGMA optimize`, or a pragma with a value SQLite doesn't recognize —
    // fail silently rather than erroring, so this asserts on `sqlite_stat1`
    // rather than on the statements returning Ok.
    #[tokio::test]
    async fn analyze_statements_populate_statistics_without_query_history() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        for i in 0..64 {
            sqlx::query(
                "INSERT INTO items (sha256, md5, type, time_added) \
                 VALUES (?, ?, 'image/png', '2026-01-01')",
            )
            .bind(format!("sha{i:04}"))
            .bind(format!("md5{i:04}"))
            .execute(&mut *conn)
            .await
            .expect("seed item");
        }

        for statement in ANALYZE_STATEMENTS {
            sqlx::query(*statement)
                .execute(&mut *conn)
                .await
                .unwrap_or_else(|err| panic!("{statement} failed: {err}"));
        }

        let analyzed: i64 = sqlx::query("SELECT COUNT(*) AS n FROM sqlite_stat1 WHERE tbl = 'items'")
            .fetch_one(&mut *conn)
            .await
            .expect("read sqlite_stat1")
            .get("n");
        assert!(
            analyzed > 0,
            "post-job maintenance left `items` unanalyzed: {ANALYZE_STATEMENTS:?}"
        );
    }

    // `CHECKPOINT_STATEMENTS` must actually truncate the log: SQLite ignores
    // unknown pragmas silently, so a typo would pass an Ok()-based test while
    // leaving the WAL at its high-water mark. Asserts on the -wal file size
    // instead. Needs a file-backed database — WAL does not apply to the
    // in-memory databases the other tests use.
    #[tokio::test]
    async fn checkpoint_statements_truncate_the_wal_file() {
        use sqlx::Connection;

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true);
        let mut conn = SqliteConnection::connect_with(&options)
            .await
            .expect("open test database");
        sqlx::query("PRAGMA journal_mode=WAL")
            .execute(&mut conn)
            .await
            .expect("enable WAL");
        sqlx::query("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)")
            .execute(&mut conn)
            .await
            .expect("create table");
        // ~2 MiB of committed pages: enough that the WAL is visibly non-empty,
        // below the ~4 MiB default autocheckpoint threshold.
        for _ in 0..32 {
            sqlx::query("INSERT INTO t (data) VALUES (zeroblob(65536))")
                .execute(&mut conn)
                .await
                .expect("insert blob");
        }

        let wal_path = dir.path().join("index.db-wal");
        assert!(
            std::fs::metadata(&wal_path).expect("stat wal file").len() > 0,
            "test setup failed to grow the WAL"
        );

        for statement in CHECKPOINT_STATEMENTS {
            sqlx::query(*statement)
                .execute(&mut conn)
                .await
                .unwrap_or_else(|err| panic!("{statement} failed: {err}"));
        }

        assert_eq!(
            std::fs::metadata(&wal_path).expect("stat wal file").len(),
            0,
            "checkpoint left the WAL untruncated: {CHECKPOINT_STATEMENTS:?}"
        );
    }
}
