use std::path::Path;
use std::sync::Arc;

use anyhow::Context as _;

use base64::{Engine as _, engine::general_purpose};
use sea_query::{SqliteQueryBuilder, Value as SeaValue, Values};
use serde_json::Value;
use sqlx::{
    Row,
    sqlite::{SqliteArguments, SqliteRow},
};
use tokio::sync::{Mutex, Semaphore};

use crate::api_error::{ApiError, Blocker};
use crate::db::extraction_errors::{
    ExtractionErrorRecord, list_distinct_blockers, list_error_sha256s_for_setter,
};
use crate::db::extraction_write::{DataLogUpdate, get_setter_data_types};
use crate::db::index_writer::{IndexDbWriterMessage, call_index_db_writer};
use crate::db::items::get_existing_file_for_item_id;
use crate::db::pql::run_compiled_count;
use crate::db::system_config::{SystemConfig, SystemConfigStore};
use crate::db::{open_index_db_read, open_index_db_read_no_user_data};
use crate::inferio::slot_error::{ProtocolViolation, SlotErrorClass};
use crate::inferio_client::{
    InferenceFile, InferenceInput, PredictOutput, PredictResponse, PredictSlotError,
};
use crate::jobs::continuous_scan;
use crate::jobs::files::{FileScanService, is_resync_needed};
use crate::jobs::inference_pool::{InferencePool, job_inference_context};
use crate::jobs::queue::ChangeSummary;
use crate::jobs::timing::PhaseTimer;
use crate::pql::builder::filters::OneOrMany;
use crate::pql::model::{
    AndOperator, Column, EntityType, FailedFor, Match, MatchOps, MatchValues, Matches, NotOperator,
    PqlQuery, ProcessedBy, QueryElement,
};
use crate::pql::{build_query_preprocessed, preprocess_query_async};

type ApiResult<T> = std::result::Result<T, ApiError>;

mod input_handlers;
mod output_handlers;

/// The inferio cache key every batch job loads under. Shared with the queue's
/// boundary unload, which must target the same key.
pub(crate) const CACHE_KEY: &str = "batch";

const CACHE_LRU_SIZE: i64 = 1;
const CACHE_TTL_SECS: i64 = 60;

/// Work units core keeps inside in-flight inference requests, per job.
///
/// This is request-level sizing — how much work core hands the inference
/// server at once, and how large a single item may grow before it is split
/// into several sequential requests — and it is deliberately *not* the user's
/// batch cap, which only bounds the GPU batches formed on the far side
/// (docs/batch-calibration-design.md, "Batch size UX", split #2). Requests are
/// still bounded in bytes by the intermediate-data budget; this bounds them in
/// units so a 4000-page PDF cannot become one request. A capped job chunks at
/// `min(cap, this)`, which keeps core from handing over more items at once
/// than the far side is allowed to process in one batch — it is not a batch
/// *alignment* guarantee, and does not need to be: the inference worker's
/// packer applies the cap to every batch it forms, including merged ones.
const REQUEST_UNIT_BUDGET: usize = 64;

/// Work units this job keeps in flight per chunked request: the smaller of
/// the user's cap (when set) and [`REQUEST_UNIT_BUDGET`]. Never zero — a
/// stored `0` means "unset" everywhere in the cap chain.
fn request_unit_capacity(batch_cap: Option<i64>) -> usize {
    match batch_cap {
        Some(cap) if cap > 0 => (cap as usize).min(REQUEST_UNIT_BUDGET),
        _ => REQUEST_UNIT_BUDGET,
    }
}

/// The cap as the inference API takes it: an item-count ceiling on GPU
/// batches, `None` = auto. Forwarded verbatim; never clamped to the request
/// budget, because it constrains a different thing.
fn gpu_batch_cap(batch_cap: Option<i64>) -> Option<u32> {
    batch_cap
        .filter(|cap| *cap > 0)
        .map(|cap| u32::try_from(cap).unwrap_or(u32::MAX))
}

/// The cap as `data_log.batch_size` stores it. That column is NOT NULL, so
/// auto logs as 0 — the same "unset" sentinel the threshold column uses.
fn logged_batch_size(batch_cap: Option<i64>) -> i64 {
    batch_cap.unwrap_or(0)
}

/// Rows fetched per work-query chunk. The driver drains the work query in
/// keyset chunks on short-lived read connections instead of one job-long
/// cursor: a streaming cursor holds a SQLite read snapshot for the whole job,
/// and that snapshot blocks every WAL checkpoint while the job's own commits
/// accumulate in the log (a 1.2M-item tagging job was observed at a 33 GB WAL
/// with 60-115s inserts; see docs/sqlite-wal-growth.md). Each chunk pays one
/// re-evaluation of the work query, so the value balances per-chunk query
/// overhead against snapshot lifetime and per-chunk row memory (text-target
/// rows carry extracted text payloads).
const WORK_CHUNK_ROWS: usize = 1024;

/// Serializes batch-model loads against the queue boundary's unloads. Held
/// only around the load itself (and around an unload's decision + call), never
/// for the duration of a job.
static BATCH_SLOT: Mutex<()> = Mutex::const_new(());
/// Bumped by every batch load. An unload captures it before it is spawned and
/// aborts if it changed: without this, a fire-and-forget unload issued at one
/// boundary can land *after* the next job has already loaded the same setter,
/// and `unload_model` fails everything queued on that dispatcher.
static BATCH_LOAD_GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Reads the current batch-load generation. Cheap and non-blocking: the queue
/// actor captures it before spawning an unload.
pub(crate) fn batch_load_generation() -> u64 {
    BATCH_LOAD_GENERATION.load(std::sync::atomic::Ordering::Acquire)
}

/// Locks the batch slot. Loads take it around `load_model_all`, boundary
/// unloads around the generation check plus `unload_model_all`.
pub(crate) async fn lock_batch_slot() -> tokio::sync::MutexGuard<'static, ()> {
    BATCH_SLOT.lock().await
}

/// Invalidates every unload captured so far. Must be called with the slot held
/// and immediately before the load, so that an unload waiting on the slot sees
/// the new generation and aborts instead of unloading what was just loaded.
pub(crate) fn begin_batch_load() {
    BATCH_LOAD_GENERATION.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
}

/// Whether an unload that captured `generation` may still proceed. Call with
/// the batch slot held.
pub(crate) fn batch_unload_is_current(generation: u64) -> bool {
    batch_load_generation() == generation
}

#[derive(Debug, Clone)]
pub(crate) struct ModelMetadata {
    pub group: String,
    // Mirrors the inference server's model metadata (Python parity); kept for
    // Debug logging even though nothing reads it directly.
    #[allow(dead_code)]
    pub inference_id: String,
    pub setter_name: String,
    pub input_handler: String,
    pub input_handler_opts: serde_json::Map<String, Value>,
    pub target_entities: Vec<String>,
    pub output_type: String,
    /// Mirrored from the registry, no longer consulted by core: it stopped
    /// being a batch *target* when auto became the only mode, and its
    /// surviving safety role (the first-touch seed on unknown hardware) is
    /// the inference side's (docs/batch-calibration-design.md).
    #[allow(dead_code)]
    pub default_batch_size: i64,
    pub default_threshold: Option<f64>,
    pub input_mime_types: Vec<String>,
    pub skip_processed_items: bool,
    /// Set when the serving inference host marked the model unavailable
    /// (e.g. the GPU misses the model's `min_compute_capability` floor);
    /// jobs bail before loading instead of failing with a CUDA error.
    pub unavailable_reason: Option<String>,
    // Informational metadata mirrored from the inference server's config.
    #[allow(dead_code)]
    pub name: Option<String>,
    #[allow(dead_code)]
    pub description: Option<String>,
    #[allow(dead_code)]
    pub link: Option<String>,
}

#[derive(Debug, Clone)]
struct JobInputData {
    file_id: i64,
    item_id: i64,
    path: String,
    sha256: String,
    md5: String,
    last_modified: String,
    item_type: String,
    duration: Option<f64>,
    /// Where the item's real content ends, when the scan's outro detector
    /// found a boundary (docs/video-outro-detection-design.md §7). `None` —
    /// never examined, or examined and negative — clamps nothing. Selected
    /// unconditionally; whether it is *used* is the job's `detect_outros`
    /// gate, threaded separately.
    content_end_ms: Option<i64>,
    // Loaded from the item row for parity with Python's job input record;
    // available to input handlers even though none read them yet.
    #[allow(dead_code)]
    audio_tracks: Option<i64>,
    video_tracks: Option<i64>,
    #[allow(dead_code)]
    subtitle_tracks: Option<i64>,
    width: Option<i64>,
    height: Option<i64>,
    data_id: Option<i64>,
    text: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct JobDefaults {
    /// The user's **cap** on GPU batch size, `None` = auto (no cap).
    /// Never a target: the inference side sizes batches from its own cost
    /// model and only has to stay at or below this (see
    /// docs/batch-calibration-design.md, "Batch size UX").
    pub batch_size: Option<i64>,
    pub threshold: Option<f64>,
}

#[derive(Debug, Clone)]
struct PreparedItem {
    item: JobInputData,
    inputs: Vec<InferenceInput>,
}

#[derive(Default)]
struct JobCounters {
    processed: i64,
    image_files: i64,
    video_files: i64,
    other_files: i64,
    total_segments: i64,
    errors: i64,
    /// The subset of `errors` that the item's own media caused and that has a
    /// ledger row to prove it. A job where every attempted item failed this
    /// way completes with a warning instead of being reported as an inference
    /// outage (docs/failed-media-retry-design.md).
    input_errors: i64,
    /// The subset of `input_errors` whose cause was a missing dependency, and
    /// the distinct dependencies themselves. `blocked` rows are input-side —
    /// they must not fail the job — but they are also the one input-side class
    /// the *user* can fix, so the job must say so instead of soft-completing
    /// in silence.
    blocked_errors: i64,
    blocked: std::collections::BTreeSet<Blocker>,
    data_load_time: PhaseTimer,
    inference_time: PhaseTimer,
}

/// What an item task concluded, for the counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ItemOutcome {
    /// Processed successfully; counted under its media type.
    Processed,
    /// Failed transiently or systemically: counted, still selectable next run.
    Failed,
    /// Failed on the item's own media, with a ledger row recording why.
    /// Carries the dependency when the verdict was `blocked`, so the job can
    /// name what has to be installed instead of completing quietly.
    InputFailed { blocker: Option<Blocker> },
}

/// The three ways an extraction job can end once every item has been
/// attempted. Pure, so the decision is unit-testable rather than inferred
/// from counters at the one call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobFailure {
    /// Something succeeded (or nothing was attempted): normal completion.
    None,
    /// Every attempted item failed, and every failure was the item's own
    /// media: the job completes with a warning, having done all it could.
    InputMediaOnly,
    /// Every attempted item failed and at least one failure was not
    /// input-side: a systemic cause (inference server down, model broken).
    Systemic,
}

/// `errors`/`input_errors` are the job's own counters, so `input_errors >
/// errors` cannot happen; if it ever does, the run is treated as systemic
/// rather than soft-completed on a count nobody can explain.
fn classify_extraction_job_failure(processed: i64, errors: i64, input_errors: i64) -> JobFailure {
    if processed <= 0 || errors < processed {
        return JobFailure::None;
    }
    if input_errors == errors {
        JobFailure::InputMediaOnly
    } else {
        JobFailure::Systemic
    }
}

/// What an extraction job reports back to the queue: whether it changed the
/// index (so maintenance is owed for its DB) and which batch-cache model it
/// left loaded.
pub(crate) struct ExtractionOutcome {
    pub summary: ChangeSummary,
    pub loaded_model: Option<String>,
}

pub(crate) async fn run_extraction_job(
    job: crate::jobs::queue::Job,
) -> Result<ExtractionOutcome, String> {
    let inference_id = job
        .metadata
        .clone()
        .ok_or_else(|| "Inference ID required".to_string())?;

    let guard = continuous_scan::pause_for_job_guarded(&job.index_db)
        .await
        .map_err(|err| format!("{err:?}"))?;
    let cleanup = IncompleteJobCleanup::arm(&job.index_db);

    let result = run_extraction_job_inner(&job, &inference_id).await;
    guard.resume().await;
    match result {
        Ok(outcome) => {
            cleanup.disarm();
            // Maintenance is no longer run per job: the queue's boundary hook
            // runs one pass per DB once nothing else is queued for it.
            Ok(outcome)
        }
        Err(err) => {
            cleanup.run().await;
            Err(format!("{err:?}"))
        }
    }
}

/// Marks this job's unfinished data_log row as incomplete when the job fails
/// or is cancelled, so job history doesn't show a phantom in-progress job
/// until the next extraction run's cleanup pass (Python runs
/// remove_incomplete_jobs immediately on exception). `Drop` covers the
/// cancellation path — the job task is aborted, so only a drop guard runs.
struct IncompleteJobCleanup {
    index_db: Option<String>,
}

impl IncompleteJobCleanup {
    fn arm(index_db: &str) -> Self {
        Self {
            index_db: Some(index_db.to_string()),
        }
    }

    fn disarm(mut self) {
        self.index_db = None;
    }

    async fn run(mut self) {
        if let Some(index_db) = self.index_db.take() {
            cleanup_incomplete_jobs(&index_db).await;
        }
    }
}

impl Drop for IncompleteJobCleanup {
    fn drop(&mut self) {
        if let Some(index_db) = self.index_db.take() {
            tokio::spawn(async move {
                cleanup_incomplete_jobs(&index_db).await;
            });
        }
    }
}

async fn cleanup_incomplete_jobs(index_db: &str) {
    let result = call_index_db_writer(index_db, |reply| {
        IndexDbWriterMessage::RemoveIncompleteJobs { reply }
    })
    .await;
    if let Err(err) = result {
        tracing::error!(error = ?err, index_db, "failed to clean up incomplete extraction jobs");
    }
}
async fn run_extraction_job_inner(
    job: &crate::jobs::queue::Job,
    inference_id: &str,
) -> ApiResult<ExtractionOutcome> {
    let config_store = SystemConfigStore::from_env();
    let config = config_store.load(&job.index_db)?;
    // Folded once for the whole job, exactly like the scan walker's, and
    // threaded down to the frame handler rather than re-read per item: the
    // only consumer of `items.content_end_ms` on this side is frame sampling,
    // and design §8 says a consumer ignores the metadata while detection is
    // off (`scan_video` outranking its own switch).
    let detect_outros = config.scan_video && config.detect_outros;

    // The embedded resync no longer runs maintenance of its own, so its
    // changes are folded into this job's report (which also removes the old
    // double maintenance pass: once inside the update, once in the wrapper).
    let mut summary = ChangeSummary::default();
    if is_resync_needed(&job.index_db, &job.user_data_db, &config).await? {
        let service = FileScanService::from_env(job.index_db.clone(), job.user_data_db.clone());
        let resync = service.run_folder_update().await?.summary;
        // Reported to the queue *now*, before the inference work that can fail
        // or be cancelled: the resync may have deleted tens of thousands of
        // files, and that debt must not die with this job. The success path
        // reports it again through `summary`; the flags are ORs, so a double
        // report is harmless.
        crate::jobs::queue::record_owed_now(&job.index_db, resync);
        summary.or_with(resync);
    }

    let model = load_model_metadata(inference_id).await?;
    // The /metadata availability overlay reflects the *serving* host's
    // GPUs (a remote inference server reports its own), so this covers
    // UI-, API-, and cron-triggered jobs with a clear message instead of
    // a CUDA error mid-load.
    if let Some(reason) = &model.unavailable_reason {
        return Err(ApiError::bad_request(format!(
            "Model {inference_id} is not available on this system: {reason}"
        )));
    }
    let defaults = resolve_job_defaults(&config, &model, job.batch_size, job.threshold);

    let context = job_inference_context();
    if context.pool.is_empty().await {
        return Err(ApiError::internal(
            "No inference endpoints enabled for batch jobs",
        ));
    }

    // Before the work query: a dependency that has appeared since the rows
    // were written must make its items selectable in *this* run.
    if let Err(err) = heal_blocked_errors(&job.index_db).await {
        tracing::warn!(error = ?err, "failed to re-probe blocked extraction failures");
    }
    // One read, once per job: the exact set of items that owe this setter a
    // ledger row, so a successful item pays a writer round-trip (and a
    // search-cache epoch bump) only when it is one of them. A plain "any rows
    // at all?" boolean would put that cost on *every* success as soon as a
    // single sub-threshold row existed anywhere for the setter. Read after
    // the heal above so cleared `blocked` rows are already gone, and covering
    // *all* rows, not just the active ones — an item with an active row is not
    // in the work query at all, so the only rows a success can clear are the
    // sub-threshold ones (see `list_error_sha256s_for_setter`).
    let ledger_shas = {
        let mut conn = open_index_db_read_no_user_data(&job.index_db).await?;
        Arc::new(list_error_sha256s_for_setter(&mut conn, &model.setter_name).await?)
    };

    let mut query = build_job_pql(&config, &model)?;
    if let Some(root) = query.query.take() {
        let preprocessed = preprocess_query_async(
            root,
            &context.primary,
            context.embedding_cache_size,
            Some(&job.index_db),
        )
        .await
        .map_err(|err| ApiError::bad_request(err.message))?;
        query.query = preprocessed;
    }

    let compiled = compile_pql_select(query.clone())?;
    let compiled_count = compile_pql_count(query.clone())?;

    // Clean up incomplete jobs before counting: with ATOMIC_EXTRACTION_JOBS
    // the cleanup deletes their item_data, which frees items for
    // reprocessing and must be reflected in the count (mirrors Python).
    call_index_db_writer(&job.index_db, |reply| {
        IndexDbWriterMessage::RemoveIncompleteJobs { reply }
    })
    .await?;

    let mut count_conn = open_index_db_read(&job.index_db, &job.user_data_db).await?;
    let total_remaining =
        run_compiled_count(&mut count_conn, &compiled_count.sql, &compiled_count.params).await?;
    drop(count_conn);

    if total_remaining < 1 {
        tracing::info!(inference_id, "no items to process");
        // Nothing loaded, nothing written: the common cron no-op reports only
        // whatever the resync above changed.
        return Ok(ExtractionOutcome {
            summary,
            loaded_model: None,
        });
    }

    // Same local-time format as the writer's end_time updates so
    // start_time/end_time are directly comparable (and match Python's local
    // isoformat convention).
    let scan_time = crate::db::extraction_write::current_iso_timestamp();
    let job_id = call_index_db_writer(&job.index_db, |reply| IndexDbWriterMessage::AddDataLog {
        scan_time: scan_time.clone(),
        threshold: defaults.threshold,
        types: vec![model.output_type.clone()],
        setter: model.setter_name.clone(),
        batch_size: logged_batch_size(defaults.batch_size),
        reply,
    })
    .await?;
    let _ = call_index_db_writer(&job.index_db, |reply| IndexDbWriterMessage::UpsertSetter {
        setter_name: model.setter_name.clone(),
        reply,
    })
    .await?;

    let load_result = {
        // Under the batch slot, with the generation bumped first: a boundary
        // unload spawned before this load either already ran (and this load
        // undoes it) or finds a newer generation and aborts. It can never land
        // on the model this job is about to use.
        let _slot = lock_batch_slot().await;
        begin_batch_load();
        context
            .pool
            .load_model_all(
                &model.setter_name,
                CACHE_KEY,
                CACHE_LRU_SIZE,
                CACHE_TTL_SECS,
                // Batch jobs opt out of lazy prewarming (design doc §8):
                // batch-only model families must not hold a warm worker's RAM
                // after the job ends.
                Some(false),
            )
            .await
    };
    if let Err(err) = load_result {
        return Err(ApiError::internal(format!("Failed to load model: {err}")));
    }

    let counters = Arc::new(Mutex::new(JobCounters::default()));
    // Bounds concurrent input loading (decode processes, file reads). Loaded
    // items park on the byte budget below, so loading pipelines ahead of
    // inference instead of running in lockstep with it.
    let loader_slots = Arc::new(Semaphore::new(context.loader_concurrency.max(1)));
    // Bounds loaded-but-unfinished intermediate data across in-flight items
    // (KiB permits). An item larger than the whole budget clamps to capacity
    // and runs alone; worst-case memory is roughly
    // budget + loader_concurrency × item size.
    let budget_capacity = context.intermediate_budget_kib.max(1);
    let budget_slots = Arc::new(Semaphore::new(budget_capacity as usize));
    // Bounds the total number of work units inside in-flight inference
    // requests across all items. This is core-side request sizing, and it is
    // deliberately independent of the user's batch cap: the cap constrains the
    // GPU batches inferio forms, while this constrains how much work core
    // keeps in flight (design doc "Batch size UX", split #2). A capped job
    // still chunks no larger than its cap, so no single request outruns what
    // the far side may process in one batch.
    let unit_slots = Arc::new(Semaphore::new(REQUEST_UNIT_BUDGET));
    let unit_capacity = request_unit_capacity(defaults.batch_size);
    // The cap travels with each request; `None` = auto.
    let batch_cap = gpu_batch_cap(defaults.batch_size);
    // Item tasks live in a JoinSet owned by this task: when the job is
    // cancelled (task aborted), dropping the set aborts every in-flight item
    // instead of leaving detached tasks writing to the DB.
    let mut tasks = tokio::task::JoinSet::new();

    let (cursor_column, partition_column) = work_query_keys(&model);
    let chunk_sql = chunked_work_query_sql(&compiled.sql, cursor_column, WORK_CHUNK_ROWS);
    let mut cursor = i64::MIN;
    // Partition keys already dispatched this job. The keyset cursor alone
    // makes one monotonic pass, but the GROUP BY representative row for an
    // item can in principle differ between chunk queries (bare-column GROUP
    // BY picks an arbitrary file), which could move an in-flight item ahead
    // of the cursor; and models with skip_processed_items=false never drop
    // processed rows from the predicate at all. This set is what guarantees
    // each work unit is dispatched at most once per job in both cases.
    let mut dispatched: std::collections::HashSet<i64> = std::collections::HashSet::new();
    loop {
        // The connection lives only for this fetch: the read snapshot it
        // holds is released before any processing below awaits, so WAL
        // checkpoints advance throughout the job instead of stalling behind
        // a job-long cursor.
        let rows = {
            let mut conn = open_index_db_read(&job.index_db, &job.user_data_db).await?;
            let mut query = sqlx::query(sqlx::AssertSqlSafe(chunk_sql.as_str()));
            query = bind_params(query, &compiled.params)?;
            query
                .bind(cursor)
                .fetch_all(&mut conn)
                .await
                .map_err(|err| {
                    tracing::error!(error = %err, "failed to fetch extraction rows");
                    ApiError::internal("Failed to execute extraction query")
                })?
        };
        let fetched = rows.len();
        for row in &rows {
            // Rows are ordered by the cursor key, so every row advances the
            // cursor — including rows that are skipped or fail to map, which
            // must not be re-fetched by the next chunk.
            cursor = row.try_get(cursor_column).map_err(map_row_err)?;
            let partition_key: i64 = row.try_get(partition_column).map_err(map_row_err)?;
            if !dispatched.insert(partition_key) {
                continue;
            }
            let Some(item) = map_job_input(&job.index_db, &job.user_data_db, row).await? else {
                continue;
            };
            let loader_permit = loader_slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| ApiError::internal("Extraction job semaphore closed"))?;
            let model = model.clone();
            let pool = context.pool.clone();
            let counters = Arc::clone(&counters);
            let index_db = job.index_db.clone();
            let threshold = defaults.threshold;
            let unit_slots = Arc::clone(&unit_slots);
            let budget_slots = Arc::clone(&budget_slots);
            let ledger_shas = Arc::clone(&ledger_shas);
            tasks.spawn(async move {
                let result = process_item(
                    &index_db,
                    &model,
                    job_id,
                    item,
                    threshold,
                    &pool,
                    loader_permit,
                    &budget_slots,
                    budget_capacity,
                    &unit_slots,
                    unit_capacity,
                    batch_cap,
                    counters,
                    total_remaining,
                    &ledger_shas,
                    detect_outros,
                )
                .await;
                if let Err(err) = result {
                    tracing::error!(error = ?err, "extraction item failed");
                }
            });
        }
        if fetched < WORK_CHUNK_ROWS {
            break;
        }
    }
    drop(dispatched);

    while tasks.join_next().await.is_some() {}

    let remaining_after = {
        let mut count_conn = open_index_db_read(&job.index_db, &job.user_data_db).await?;
        run_compiled_count(&mut count_conn, &compiled_count.sql, &compiled_count.params).await?
    };

    let (final_update, failure, processed_data) = {
        let guard = counters.lock().await;
        // Every attempted item failing on a cause that is *not* the item's
        // own media means a systemic problem (inference server down, model
        // broken): surface it as a job failure instead of a "completed" job
        // that did nothing, and leave the log row unfinished so the cleanup
        // pass marks it incomplete. A run where every failure was input-side
        // did all it could and completes.
        let failure =
            classify_extraction_job_failure(guard.processed, guard.errors, guard.input_errors);
        // A `blocked` verdict is input-side (it must not fail the job) but it
        // is also the one input-side class the user can act on, so it is never
        // allowed to soft-complete silently — on *either* completion path.
        let blocked: Vec<&'static str> = guard.blocked.iter().map(|b| b.as_str()).collect();
        if failure == JobFailure::InputMediaOnly {
            // No blocked-count clause here: the warn below already names the
            // blockers with their count, on both completion paths.
            tracing::warn!(
                items = guard.errors,
                setter = %model.setter_name,
                "{} items failed on input media; not an inference outage",
                guard.errors
            );
        }
        if !blocked.is_empty() {
            tracing::warn!(
                items = guard.blocked_errors,
                setter = %model.setter_name,
                blockers = ?blocked,
                "{} items are blocked on missing dependencies: {} — install them \
                 and restart the gateway",
                guard.blocked_errors,
                blocked.join(", ")
            );
        }
        let update = DataLogUpdate {
            image_files: guard.image_files,
            video_files: guard.video_files,
            other_files: guard.other_files,
            total_segments: guard.total_segments,
            errors: guard.errors,
            input_errors: guard.input_errors,
            total_remaining: remaining_after,
            data_load_time: guard.data_load_time.busy_secs(),
            inference_time: guard.inference_time.busy_secs(),
            finished: failure != JobFailure::Systemic,
        };
        // The stored times are phase wall-clock (busy); aggregate worker time
        // only goes to the log, where work / busy reads as average parallelism.
        tracing::info!(
            data_load_busy_secs = guard.data_load_time.busy_secs(),
            data_load_work_secs = guard.data_load_time.work_secs(),
            inference_busy_secs = guard.inference_time.busy_secs(),
            inference_work_secs = guard.inference_time.work_secs(),
            "extraction job phase timing"
        );
        (update, failure, guard.processed - guard.errors > 0)
    };
    let _ = call_index_db_writer(&job.index_db, |reply| IndexDbWriterMessage::UpdateDataLog {
        job_id,
        update: final_update.clone(),
        reply,
    })
    .await;

    // No unload here: the job reports the model it loaded and the queue's
    // boundary decides, so a following job for the same setter reuses it
    // instead of reloading (design §B). Every path that loses the boundary's
    // unload still falls back to the inferio TTL sweep, as it always did.

    if failure == JobFailure::Systemic {
        return Err(ApiError::internal(format!(
            "All {} attempted items failed; check the inference server",
            final_update.errors
        )));
    }
    summary.or_with(ChangeSummary {
        wrote_data: processed_data,
        deleted_data: false,
        // Tag output is the only extraction output that touches `tags_items`;
        // text, clip and embedding outputs leave the counts alone. The writer
        // has already set the durable marker for every tag write this job
        // committed — this flag is what lets the boundary decide without
        // reading the DB, and what makes a fully cancelled tagging job still
        // recount (through the marker).
        tags_changed: processed_data && model.output_type == "tags",
    });
    Ok(ExtractionOutcome {
        summary,
        loaded_model: Some(model.setter_name.clone()),
    })
}

/// Blocked auto-heal (docs/failed-media-retry-design.md req 10): the ledger's
/// items waiting on a dependency become selectable again as soon as it
/// appears. Costs one indexed query on the normal path, where nothing is
/// blocked and nothing is probed — probing eagerly would load libraries the
/// run has no use for.
async fn heal_blocked_errors(index_db: &str) -> ApiResult<()> {
    let waiting = {
        let mut conn = open_index_db_read_no_user_data(index_db).await?;
        list_distinct_blockers(&mut conn).await?
    };
    if waiting.is_empty() {
        return Ok(());
    }
    // Binding pdfium and spawning ffmpeg both block; the probes run off the
    // async runtime.
    let present = tokio::task::spawn_blocking(move || {
        waiting
            .into_iter()
            .filter(|blocker| crate::jobs::files::probe_blocker(*blocker))
            .collect::<Vec<_>>()
    })
    .await
    .map_err(|_| ApiError::internal("Blocker probe task failed"))?;
    heal_blocked(index_db, present).await.map(|_| ())
}

/// The write half, with the probe results handed in: probing real binaries is
/// what makes this untestable, and the clearing is what has to be right.
async fn heal_blocked(index_db: &str, present: Vec<Blocker>) -> ApiResult<u64> {
    if present.is_empty() {
        return Ok(0);
    }
    let cleared =
        call_index_db_writer(index_db, |reply| IndexDbWriterMessage::ClearBlockedErrors {
            blockers: present.clone(),
            reply,
        })
        .await?;
    tracing::info!(
        cleared,
        blockers = ?present.iter().map(|blocker| blocker.as_str()).collect::<Vec<_>>(),
        "dependencies are available again; cleared their blocked extraction failures"
    );
    Ok(cleared)
}

pub(crate) async fn run_data_deletion_job(
    job: crate::jobs::queue::Job,
) -> Result<ChangeSummary, String> {
    let inference_id = job
        .metadata
        .clone()
        .ok_or_else(|| "Inference ID required".to_string())?;
    let guard = continuous_scan::pause_for_job_guarded(&job.index_db)
        .await
        .map_err(|err| format!("{err:?}"))?;
    let result = run_data_deletion_job_inner(&job, &inference_id).await;
    guard.resume().await;
    result.map_err(|err| format!("{err:?}"))
}

async fn run_data_deletion_job_inner(
    job: &crate::jobs::queue::Job,
    inference_id: &str,
) -> ApiResult<ChangeSummary> {
    let mut conn = open_index_db_read(&job.index_db, &job.user_data_db).await?;
    let data_types = get_setter_data_types(&mut conn, inference_id).await?;
    drop(conn);

    let include_orphan_tags = data_types.iter().any(|entry| entry == "tags");
    let (deleted, orphan_tags_deleted) = call_index_db_writer(&job.index_db, |reply| {
        IndexDbWriterMessage::DeleteSetterData {
            setter_name: inference_id.to_string(),
            include_orphan_tags,
            reply,
        }
    })
    .await?;

    // Reported, not run: the queue's boundary hook owns maintenance now, and
    // its VACUUM is additionally gated on the actual free-page count.
    let deleted_data = deleted > 0 || orphan_tags_deleted > 0;
    Ok(ChangeSummary {
        wrote_data: false,
        deleted_data,
        // Deleting a tagger's data removes its `tags_items` rows outright, and
        // `include_orphan_tags` deletes the tags left with none.
        tags_changed: deleted_data,
    })
}

#[allow(clippy::too_many_arguments)]
async fn process_item(
    index_db: &str,
    model: &ModelMetadata,
    job_id: i64,
    item: JobInputData,
    threshold: Option<f64>,
    pool: &InferencePool,
    loader_permit: tokio::sync::OwnedSemaphorePermit,
    budget_slots: &Arc<Semaphore>,
    budget_capacity: u32,
    unit_slots: &Arc<Semaphore>,
    unit_capacity: usize,
    batch_cap: Option<u32>,
    counters: Arc<Mutex<JobCounters>>,
    total_remaining: i64,
    ledger_shas: &std::collections::HashSet<String>,
    detect_outros: bool,
) -> ApiResult<()> {
    let item_type = item.item_type.clone();
    let sha256 = item.sha256.clone();
    let path = item.path.clone();
    let load_span = counters.lock().await.data_load_time.start();
    let prepare_result = input_handlers::prepare_item(index_db, model, item, detect_outros).await;
    drop(load_span);
    let prepared = match prepare_result {
        Ok(prepared) => prepared,
        Err(err) => {
            let (outcome, returned) = record_item_failure(
                index_db,
                model,
                job_id,
                crate::db::extraction_errors::STAGE_PREPARE,
                &sha256,
                &path,
                err,
            )
            .await;
            finalize_item(
                index_db,
                job_id,
                &item_type,
                0,
                outcome,
                counters,
                total_remaining,
            )
            .await;
            return match returned {
                Some(err) => Err(err),
                // Already logged with its path, sha256, stage and class, and
                // recorded in the ledger: the item is done for this job and
                // the job continues.
                None => Ok(()),
            };
        }
    };

    if prepared.inputs.is_empty() {
        let result =
            output_handlers::write_placeholder(index_db, model, job_id, &prepared.item).await;
        if result.is_ok() {
            clear_ledger_row(index_db, model, &prepared.item.sha256, ledger_shas).await;
        }
        finalize_item(
            index_db,
            job_id,
            &prepared.item.item_type,
            0,
            if result.is_ok() {
                ItemOutcome::Processed
            } else {
                ItemOutcome::Failed
            },
            counters,
            total_remaining,
        )
        .await;
        return result.map(|_| ());
    }

    let inference_inputs = input_handlers::apply_threshold(prepared.inputs, threshold);
    // Reserve budget for the loaded data *before* releasing the loader slot:
    // when the budget is exhausted this parks with the slot still held, so
    // once every loader slot is parked no new loads start — that is the
    // backpressure that bounds memory. The clamp to capacity means an item
    // bigger than the entire budget acquires all of it and runs alone rather
    // than deadlocking.
    let kib = input_memory_kib(&inference_inputs);
    let _budget_permits = if kib > 0 {
        let want = kib.min(budget_capacity);
        Some(
            budget_slots
                .clone()
                .acquire_many_owned(want)
                .await
                .map_err(|_| ApiError::internal("Extraction budget semaphore closed"))?,
        )
    } else {
        None
    };
    drop(loader_permit);

    let segments = inference_inputs.len() as i64;
    let inference = match run_chunked_inference(
        &model.setter_name,
        pool,
        unit_slots,
        unit_capacity,
        batch_cap,
        &inference_inputs,
        &counters,
    )
    .await
    {
        Ok(inference) => inference,
        Err(err) => {
            // A failure of the predict call itself stays transient — even
            // after `run_chunked_inference`'s isolation retry gave every one
            // of the item's work units a chance alone. Only a *typed* worker
            // verdict may say an item's payload is bad; an exception text is
            // never pattern-matched into one (design doc, layer 2), so an
            // unclassified failure is retried, never suppressed.
            tracing::error!(
                path = %prepared.item.path,
                sha256 = %prepared.item.sha256,
                stage = crate::db::extraction_errors::STAGE_INFERENCE,
                error_class = "transient",
                error = %err,
                "extraction item failed"
            );
            let api_err = ApiError::internal(format!("Inference failed: {err}"));
            finalize_item(
                index_db,
                job_id,
                &prepared.item.item_type,
                segments,
                ItemOutcome::Failed,
                counters,
                total_remaining,
            )
            .await;
            return Err(api_err);
        }
    };

    // The original input position of every output that came back, needed
    // because the erroring slots are dropped from `outputs` and the survivors
    // close ranks — `idx` is a page/frame number, so renumbering it would
    // silently mis-file a partial item's rows. `None` (the overwhelmingly
    // common no-slot case) is the identity map and costs nothing.
    let survivors = surviving_input_indices(inference_inputs.len(), &inference.slot_errors);

    let outputs = match classify_slot_errors(
        inference_inputs.len(),
        &inference.slot_errors,
        targets_text_entity(model),
    ) {
        SlotVerdict::Proceed => {
            for error in &inference.slot_errors {
                // Partial: the item's media is processable, one of its work
                // units was not. Logged, counted nowhere, never persisted —
                // the ledger keys on the item, and this item is fine. This
                // log line is the *only* record of a dropped input, by design
                // (docs/failed-media-retry-design.md), so it carries the
                // item's identity and the input's index.
                tracing::warn!(
                    path = %prepared.item.path,
                    sha256 = %prepared.item.sha256,
                    stage = crate::db::extraction_errors::STAGE_INFERENCE,
                    error_class = error.class.as_str(),
                    input = error.index,
                    inputs_total = inference_inputs.len(),
                    error = %error.message,
                    "inference rejected one of the item's inputs; keeping the rest"
                );
            }
            inference.outputs
        }
        SlotVerdict::Transient(detail) => {
            tracing::error!(
                path = %prepared.item.path,
                sha256 = %prepared.item.sha256,
                stage = crate::db::extraction_errors::STAGE_INFERENCE,
                error_class = "transient",
                error = %detail,
                "extraction item failed"
            );
            finalize_item(
                index_db,
                job_id,
                &prepared.item.item_type,
                segments,
                ItemOutcome::Failed,
                counters,
                total_remaining,
            )
            .await;
            return Err(ApiError::internal(format!("Inference failed: {detail}")));
        }
        SlotVerdict::InputMedia(detail) => {
            // The worker — the component that actually decoded the bytes —
            // rejected every one of this item's inputs. That is a verdict on
            // the media, recorded at the inference stage with the confirmed
            // threshold (a decode of bytes the worker already had in hand).
            let (outcome, returned) = record_item_failure(
                index_db,
                model,
                job_id,
                crate::db::extraction_errors::STAGE_INFERENCE,
                &prepared.item.sha256,
                &prepared.item.path,
                ApiError::input(detail),
            )
            .await;
            finalize_item(
                index_db,
                job_id,
                &prepared.item.item_type,
                segments,
                outcome,
                counters,
                total_remaining,
            )
            .await;
            return match returned {
                Some(err) => Err(err),
                None => Ok(()),
            };
        }
    };

    let result = output_handlers::handle_outputs(
        index_db,
        model,
        job_id,
        prepared.item.clone(),
        outputs,
        survivors.as_deref(),
    )
    .await;
    if let Err(err) = &result {
        // Storing the output is the gateway's own DB work: never a verdict on
        // the media, so it is counted and retried like any other transient.
        tracing::error!(
            path = %prepared.item.path,
            sha256 = %prepared.item.sha256,
            stage = "output",
            error_class = "transient",
            error = %err.detail(),
            "extraction item failed"
        );
    }
    if result.is_ok() {
        clear_ledger_row(index_db, model, &prepared.item.sha256, ledger_shas).await;
    }
    finalize_item(
        index_db,
        job_id,
        &prepared.item.item_type,
        segments,
        if result.is_ok() {
            ItemOutcome::Processed
        } else {
            ItemOutcome::Failed
        },
        counters,
        total_remaining,
    )
    .await;
    result.map(|_| ())
}

/// The verdict on an item whose inference produced typed per-slot errors.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SlotVerdict {
    /// No errors, or some inputs succeeded: use the outputs that came back.
    Proceed,
    /// Every input failed, but not all of them on the class that settles a
    /// verdict about the media: counted, retried, never persisted.
    Transient(String),
    /// Every input failed with the worker's `input` class: a verdict on the
    /// item's media, which owes a ledger row.
    InputMedia(String),
}

/// Maps an item's typed slot errors onto the ledger taxonomy.
///
/// Three rules, all from docs/failed-media-retry-design.md:
///
/// - **Class first.** A `transient` slot says nothing about the payload and
///   must never be swallowed, so *any* non-`input` slot makes the whole item
///   transient — including when its batch-mates succeeded. Proceeding there
///   would write the item's partial outputs and mark it processed, which
///   permanently loses the work unit the worker asked us to retry.
/// - **Partial success proceeds — but only for `input` slots.** The verdict
///   has to be about the *item's media*, and media some of whose work units
///   decoded is processable media. Those failed units are logged and dropped;
///   only an item whose inputs *all* failed on `input` is bad media.
/// - **Text-entity models never persist a worker verdict** ("Granularity
///   caveat — text-entity models"): there, one input is one extracted text
///   segment, while the ledger and the `failed_for` anti-join key on the
///   item. Persisting would take every *other* segment of that item out of
///   the work query because a single segment was bad. Until the ledger grows
///   a nullable `data_id`, those verdicts stay transient.
fn classify_slot_errors(
    total_inputs: usize,
    errors: &[PredictSlotError],
    text_entity: bool,
) -> SlotVerdict {
    if errors.is_empty() {
        return SlotVerdict::Proceed;
    }
    let detail = summarize_slot_errors(total_inputs, errors);
    // Class before arity: a retryable slot anywhere poisons the whole item,
    // whether or not the rest of the batch came back.
    if !errors
        .iter()
        .all(|error| error.class == SlotErrorClass::Input)
    {
        return SlotVerdict::Transient(detail);
    }
    if errors.len() < total_inputs {
        return SlotVerdict::Proceed;
    }
    if text_entity {
        return SlotVerdict::Transient(format!(
            "{detail} (text-entity model: a per-segment verdict is not \
             recorded against the item)"
        ));
    }
    SlotVerdict::InputMedia(detail)
}

/// The original input positions of the outputs a partial response carries, in
/// output order.
///
/// The wire protocol drops erroring slots from `outputs`, so the *n*-th
/// surviving output is not the *n*-th input. Everything downstream that
/// stores an `index` is storing the input's identity — a video frame number
/// or a PDF page number (`item_data.idx`) — so it must use this map, not the
/// enumeration of the survivors.
///
/// `None` means "no slots errored", i.e. the identity map: every response an
/// inference server without error slots can produce takes that path and pays
/// nothing.
fn surviving_input_indices(total_inputs: usize, errors: &[PredictSlotError]) -> Option<Vec<usize>> {
    if errors.is_empty() {
        return None;
    }
    let failed: std::collections::HashSet<usize> = errors.iter().map(|error| error.index).collect();
    Some(
        (0..total_inputs)
            .filter(|index| !failed.contains(index))
            .collect(),
    )
}

/// A one-line rendering of an item's slot errors for the log and the ledger's
/// audit text. One message is the interesting one; the rest is a count, since
/// every input of a corrupt file usually fails identically. When the classes
/// are mixed the *non-`input`* one leads, because that is the one that
/// decided the verdict.
fn summarize_slot_errors(total_inputs: usize, errors: &[PredictSlotError]) -> String {
    let Some(first) = errors.first() else {
        return "inference reported no outputs".to_string();
    };
    let lead = errors
        .iter()
        .find(|error| error.class != SlotErrorClass::Input)
        .unwrap_or(first);
    let scope = if errors.len() == 1 && total_inputs <= 1 {
        "the input".to_string()
    } else if errors.len() >= total_inputs {
        format!("all {} inputs", errors.len())
    } else {
        format!("{} of {} inputs", errors.len(), total_inputs)
    };
    format!(
        "worker rejected {scope} ({}): {}",
        lead.class.as_str(),
        lead.message
    )
}

/// Whether the model's work unit is an extracted text segment rather than the
/// item's own media — the same `target_entities` discriminator `build_job_pql`
/// turns into [`EntityType::Text`].
fn targets_text_entity(model: &ModelMetadata) -> bool {
    matches!(model.target_entities.as_slice(), [entity] if entity == "text")
}

/// Logs an item failure and, when its class is one the ledger stores, records
/// it against `stage`. Returns the outcome to count and the error the item
/// task should return, if any.
///
/// A failed *ledger write* is counted systemic and returned as an error: a DB
/// outage must never soft-complete a job as "all corrupt media".
async fn record_item_failure(
    index_db: &str,
    model: &ModelMetadata,
    job_id: i64,
    stage: &str,
    sha256: &str,
    path: &str,
    err: ApiError,
) -> (ItemOutcome, Option<ApiError>) {
    let class = err.persisted_class();
    tracing::error!(
        path,
        sha256,
        stage,
        error_class = class.unwrap_or("transient"),
        blocker = err.blocker().map(Blocker::as_str).unwrap_or("none"),
        skip_after = err.skip_after(),
        error = %err.detail(),
        "extraction item failed"
    );
    if class.is_none() {
        return (ItemOutcome::Failed, Some(err));
    }

    let record = failure_record(model, job_id, stage, sha256, &err);
    match call_index_db_writer(index_db, |reply| {
        IndexDbWriterMessage::UpsertExtractionError {
            record: record.clone(),
            reply,
        }
    })
    .await
    {
        Ok(()) => (
            ItemOutcome::InputFailed {
                blocker: err.blocker(),
            },
            None,
        ),
        Err(write_err) => {
            tracing::error!(
                path,
                sha256,
                error = ?write_err,
                "failed to record an extraction failure; counting it as systemic"
            );
            (ItemOutcome::Failed, Some(write_err))
        }
    }
}

/// The ledger row a classified failure owes. Split out so the job path and
/// its tests build the same record.
fn failure_record(
    model: &ModelMetadata,
    job_id: i64,
    stage: &str,
    sha256: &str,
    err: &ApiError,
) -> ExtractionErrorRecord {
    ExtractionErrorRecord {
        item_sha256: sha256.to_string(),
        setter_name: model.setter_name.clone(),
        stage: stage.to_string(),
        kind: err.kind(),
        error: err.detail().to_string(),
        skip_after: err.skip_after(),
        // Always the real job: `attempts` dedups on it, and a job-less write
        // would stop counting across consecutive runs.
        job_id: Some(job_id),
    }
}

/// Success path: an item this setter can now process owes no ledger row —
/// *any* row, not just an active one. An item whose verdict is already active
/// is excluded by the work query and can never reach this path, so the only
/// rows a success is ever in a position to clear are the sub-threshold ones a
/// single transient blip left behind. Leaving those would let a second blip,
/// months later, confirm a verdict on a file that has succeeded in between.
///
/// Gated on the *per-item* set the job read at start, so only the items that
/// actually owe a row pay for the delete — it is a write transaction (and a
/// search-cache epoch bump) each. A row written *during* this job is not in
/// the set, but the item that wrote it failed and a failed item never reaches
/// this path; and missing a delete is advisory anyway — it costs one wasted
/// re-attempt in a later run, never correctness.
async fn clear_ledger_row(
    index_db: &str,
    model: &ModelMetadata,
    sha256: &str,
    ledger_shas: &std::collections::HashSet<String>,
) {
    if !ledger_shas.contains(sha256) {
        return;
    }
    let result = call_index_db_writer(index_db, |reply| {
        IndexDbWriterMessage::DeleteExtractionError {
            item_sha256: sha256.to_string(),
            setter_name: model.setter_name.clone(),
            reply,
        }
    })
    .await;
    // Advisory: a lost delete costs one wasted re-attempt after the item has
    // already succeeded, never correctness.
    if let Err(err) = result {
        tracing::warn!(
            sha256,
            setter = %model.setter_name,
            error = ?err,
            "failed to clear an extraction failure after a successful item"
        );
    }
}

/// In-memory footprint of an item's prepared inputs, in KiB (rounded up).
/// Only counts buffers actually held in memory: path-based inputs are read
/// transiently at request time, which the work-unit cap already bounds.
fn input_memory_kib(inputs: &[InferenceInput]) -> u32 {
    let bytes: usize = inputs
        .iter()
        .map(|input| match &input.file {
            Some(InferenceFile::Bytes(buffer)) => buffer.len(),
            _ => 0,
        })
        .sum();
    u32::try_from(bytes.div_ceil(1024)).unwrap_or(u32::MAX)
}

/// What one item's inference produced: the outputs of the work units that
/// succeeded, plus the worker's typed verdicts on the ones that did not (in
/// input order, indices relative to the item's full input list).
struct ItemInference {
    outputs: PredictOutput,
    slot_errors: Vec<PredictSlotError>,
}

/// Runs inference over one item's work units in chunks of at most
/// `unit_capacity`, holding one unit permit per work unit for the duration of
/// each request. Together with the shared semaphore this caps the total
/// number of work units inside in-flight inference requests at the job's
/// request budget, and splits oversized items (e.g. many-page PDFs) into
/// multiple sequential requests whose outputs are concatenated in order.
///
/// `batch_cap` is the user's cap and is forwarded untouched (`None` = auto):
/// it constrains the GPU batches the inference side forms, not the size of
/// the requests core sends.
///
/// Layer 2 of the batch-isolation design lives here. Extraction never puts
/// two *items* in one predict call — cross-item merging happens server-side
/// in the dispatcher, which already falls back to per-request prediction —
/// so the multi-unit boundary in this process is one item's chunk. When a
/// chunk of more than one unit fails as a whole, its units are re-submitted
/// one at a time, once (`isolate_inputs`), each advertising
/// [`ISOLATION_MAX_BATCH`] so the dispatcher never merges a retry into
/// another chunk's window:
/// a batch-level failure that is not about any single unit then still
/// completes the item. If one unit fails alone the whole item fails
/// transiently — partial data is never written for an unclassified failure,
/// since the item stays selectable and will be processed in full next run.
///
/// The one failure that is *not* isolated is a protocol violation: it is
/// deterministic, so the retry would only re-ask the same broken server.
async fn run_chunked_inference(
    setter_name: &str,
    pool: &InferencePool,
    unit_slots: &Arc<Semaphore>,
    unit_capacity: usize,
    batch_cap: Option<u32>,
    inputs: &[InferenceInput],
    counters: &Arc<Mutex<JobCounters>>,
) -> anyhow::Result<ItemInference> {
    let chunk_size = unit_capacity.max(1);
    let mut merged: Option<PredictOutput> = None;
    let mut slot_errors: Vec<PredictSlotError> = Vec::new();
    let mut base = 0usize;
    for chunk in inputs.chunks(chunk_size) {
        let response =
            match predict_units(setter_name, pool, unit_slots, batch_cap, counters, chunk).await {
                Ok(response) => response,
                // A protocol violation is deterministic: the server answered with
                // a shape this client refuses to guess at, and it will answer the
                // same way one input at a time. Isolating would burn a full extra
                // GPU pass to learn nothing, so the chunk fails transiently now.
                Err(err) if is_protocol_violation(&err) => return Err(err),
                Err(err) if chunk.len() > 1 => {
                    tracing::warn!(
                        setter = setter_name,
                        inputs = chunk.len(),
                        "inference batch failed; retrying this item's inputs one at a time: {err:#}"
                    );
                    // One isolation pass only: the retry itself is never isolated
                    // again, so the worst case is 2x the requests for this chunk.
                    isolate_inputs(chunk, |single, max_batch| async move {
                        predict_units(
                            setter_name,
                            pool,
                            unit_slots,
                            Some(max_batch),
                            counters,
                            &single,
                        )
                        .await
                    })
                    .await
                    .with_context(|| {
                        format!("batch failed and isolation did not recover it: {err:#}")
                    })?
                }
                Err(err) => return Err(err),
            };
        for mut error in response.errors {
            error.index += base;
            slot_errors.push(error);
        }
        let outputs = response.outputs;
        // A chunk whose every slot errored contributes no outputs at all;
        // merging it would be a spurious Json/Binary type clash.
        if !outputs.is_empty() {
            merged = Some(match merged {
                None => outputs,
                Some(previous) => merge_outputs(previous, outputs)?,
            });
        }
        base += chunk.len();
    }
    let outputs = match merged {
        Some(outputs) => outputs,
        // Every unit errored: the outputs are empty by construction, and the
        // caller classifies the slot errors instead of writing anything.
        None if !slot_errors.is_empty() => PredictOutput::Json(Vec::new()),
        None => return Err(anyhow::anyhow!("no inference outputs produced")),
    };
    Ok(ItemInference {
        outputs,
        slot_errors,
    })
}

/// One predict request for a slice of an item's work units, holding one unit
/// permit per unit for its duration and timing it into the job's inference
/// phase.
///
/// `max_batch` is the cap this request advertises to the server-side
/// dispatcher. Normally it is the user's cap, verbatim (`None` = auto); the
/// isolation retry passes 1 so the dispatcher cannot merge the retry back
/// into a window with other requests (see [`ISOLATION_MAX_BATCH`]).
async fn predict_units(
    setter_name: &str,
    pool: &InferencePool,
    unit_slots: &Arc<Semaphore>,
    max_batch: Option<u32>,
    counters: &Arc<Mutex<JobCounters>>,
    inputs: &[InferenceInput],
) -> anyhow::Result<PredictResponse> {
    let permits = unit_slots
        .clone()
        .acquire_many_owned(inputs.len() as u32)
        .await
        .map_err(|_| anyhow::anyhow!("inference unit semaphore closed"))?;
    let inference_span = counters.lock().await.inference_time.start();
    let response = pool
        .predict(
            setter_name,
            CACHE_KEY,
            CACHE_LRU_SIZE,
            CACHE_TTL_SECS,
            // The user's cap, verbatim: `None` (auto) lets the
            // orchestrator's cost model size GPU batches, `Some(n)` is an
            // item-count ceiling it must not exceed — `Some(1)` for an
            // isolated retry.
            max_batch,
            // Batch jobs opt out of lazy prewarming (design doc §8).
            Some(false),
            inputs,
        )
        .await;
    drop(inference_span);
    drop(permits);
    response
}

/// Whether a failure is the peer answering in a shape the protocol does not
/// define. Deterministic by nature, so it must not be retried by isolation:
/// the same server would produce the same malformed answer one input at a
/// time, at the cost of a whole extra pass over the item's units.
fn is_protocol_violation(err: &anyhow::Error) -> bool {
    err.downcast_ref::<ProtocolViolation>().is_some()
}

/// The `max_batch` an isolated retry advertises on the wire. Splitting the
/// request locally is not isolation on its own — the dispatcher merges queued
/// requests into windows — but windows never mix cap values
/// (`dispatch::window_take_count`), so a retry never shares a window with a
/// job chunk. Within a window of cap-1 requests the unpriced path sends each
/// request alone and the priced path's worker packer honours the cap as a
/// hard item count, so the retry normally runs as a GPU batch of its own.
/// The exception is an impl with its own batching switched off (the easyOCR
/// entries): it ignores the cap and runs its whole window in one call, and a
/// failure there is attributed by the dispatcher's per-request fallback
/// instead — the same verdict, one extra pass.
const ISOLATION_MAX_BATCH: u32 = 1;

/// Isolation retry: re-submit `inputs` one at a time, sequentially, and
/// assemble the result as if it had been one request. The first input that
/// still fails alone aborts the pass with its own error — never promoted to
/// an `input` verdict by pattern-matching, which is what keeps the pipeline
/// from ever being stricter than the model itself (design doc, req 1).
///
/// `predict_one` takes ownership of its slice so the closure's future does
/// not borrow the loop, which is also what makes this testable with an
/// injected predict function; its second argument is the wire cap the
/// submission must carry ([`ISOLATION_MAX_BATCH`]).
async fn isolate_inputs<F, Fut>(
    inputs: &[InferenceInput],
    mut predict_one: F,
) -> anyhow::Result<PredictResponse>
where
    F: FnMut(Vec<InferenceInput>, u32) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<PredictResponse>>,
{
    let mut merged: Option<PredictOutput> = None;
    let mut errors: Vec<PredictSlotError> = Vec::new();
    for (index, input) in inputs.iter().enumerate() {
        let response = predict_one(vec![input.clone()], ISOLATION_MAX_BATCH)
            .await
            .inspect_err(|_| {
                // The pass aborts here without writing anything, so the units
                // that already succeeded are re-run next time. That is a
                // recurring GPU cost on a recurring bad file: log how much of
                // the pass was thrown away so it is visible per run.
                tracing::warn!(
                    recovered_units = index,
                    total_units = inputs.len(),
                    "isolation pass aborted; discarding the units that had \
                     already succeeded"
                );
            })
            .with_context(|| format!("input {index} failed on its own too"))?;
        for mut error in response.errors {
            error.index += index;
            errors.push(error);
        }
        if !response.outputs.is_empty() {
            merged = Some(match merged {
                None => response.outputs,
                Some(previous) => merge_outputs(previous, response.outputs)?,
            });
        }
    }
    Ok(PredictResponse {
        outputs: merged.unwrap_or(PredictOutput::Json(Vec::new())),
        errors,
    })
}

fn merge_outputs(first: PredictOutput, second: PredictOutput) -> anyhow::Result<PredictOutput> {
    match (first, second) {
        (PredictOutput::Json(mut a), PredictOutput::Json(b)) => {
            a.extend(b);
            Ok(PredictOutput::Json(a))
        }
        (PredictOutput::Binary(mut a), PredictOutput::Binary(b)) => {
            a.extend(b);
            Ok(PredictOutput::Binary(a))
        }
        _ => Err(anyhow::anyhow!(
            "inference chunks returned mixed output types"
        )),
    }
}

async fn finalize_item(
    index_db: &str,
    job_id: i64,
    item_type: &str,
    segments: i64,
    outcome: ItemOutcome,
    counters: Arc<Mutex<JobCounters>>,
    total_remaining: i64,
) {
    let update = {
        let mut guard = counters.lock().await;
        guard.processed += 1;
        guard.total_segments += segments;

        match outcome {
            ItemOutcome::Processed => {
                if item_type.starts_with("video") {
                    guard.video_files += 1;
                } else if item_type.starts_with("image") {
                    guard.image_files += 1;
                } else {
                    guard.other_files += 1;
                }
            }
            ItemOutcome::Failed => guard.errors += 1,
            ItemOutcome::InputFailed { blocker } => {
                guard.errors += 1;
                guard.input_errors += 1;
                if let Some(blocker) = blocker {
                    guard.blocked_errors += 1;
                    guard.blocked.insert(blocker);
                }
            }
        }

        let remaining = total_remaining.saturating_sub(guard.processed);
        DataLogUpdate {
            image_files: guard.image_files,
            video_files: guard.video_files,
            other_files: guard.other_files,
            total_segments: guard.total_segments,
            errors: guard.errors,
            input_errors: guard.input_errors,
            total_remaining: remaining,
            data_load_time: guard.data_load_time.busy_secs(),
            inference_time: guard.inference_time.busy_secs(),
            finished: false,
        }
    };
    let _ = call_index_db_writer(index_db, |reply| IndexDbWriterMessage::UpdateDataLog {
        job_id,
        update: update.clone(),
        reply,
    })
    .await;
}

async fn map_job_input(
    index_db: &str,
    user_data_db: &str,
    row: &SqliteRow,
) -> ApiResult<Option<JobInputData>> {
    let file_id: i64 = row.try_get("file_id").map_err(map_row_err)?;
    let item_id: i64 = row.try_get("item_id").map_err(map_row_err)?;
    let sha256: String = row.try_get("sha256").map_err(map_row_err)?;
    let md5: String = row.try_get("md5").map_err(map_row_err)?;
    let path: String = row.try_get("path").map_err(map_row_err)?;
    let last_modified: String = row.try_get("last_modified").map_err(map_row_err)?;
    let item_type: String = row.try_get("type").map_err(map_row_err)?;
    let duration: Option<f64> = row.try_get("duration").unwrap_or(None);
    let content_end_ms: Option<i64> = row.try_get("content_end_ms").unwrap_or(None);
    let audio_tracks: Option<i64> = row.try_get("audio_tracks").unwrap_or(None);
    let video_tracks: Option<i64> = row.try_get("video_tracks").unwrap_or(None);
    let subtitle_tracks: Option<i64> = row.try_get("subtitle_tracks").unwrap_or(None);
    let width: Option<i64> = row.try_get("width").unwrap_or(None);
    let height: Option<i64> = row.try_get("height").unwrap_or(None);
    let data_id: Option<i64> = row.try_get("data_id").unwrap_or(None);
    let text: Option<String> = row.try_get("text").unwrap_or(None);

    let mut input = JobInputData {
        file_id,
        item_id,
        path,
        sha256,
        md5,
        last_modified,
        item_type,
        duration,
        content_end_ms,
        audio_tracks,
        video_tracks,
        subtitle_tracks,
        width,
        height,
        data_id,
        text,
    };

    if !Path::new(&input.path).exists() {
        let mut conn = open_index_db_read(index_db, user_data_db).await?;
        if let Some(file) = get_existing_file_for_item_id(&mut conn, input.item_id).await? {
            input.path = file.path;
            input.file_id = file.id;
            input.last_modified = file.last_modified;
        } else {
            return Ok(None);
        }
    }

    Ok(Some(input))
}

fn map_row_err(err: sqlx::Error) -> ApiError {
    tracing::error!(error = %err, "failed to read query row");
    ApiError::internal("Failed to read job input")
}
fn build_job_pql(config: &SystemConfig, model: &ModelMetadata) -> ApiResult<PqlQuery> {
    let mut filters = Vec::new();
    if !model.input_mime_types.is_empty() {
        filters.push(QueryElement::Match(Box::new(Match {
            match_: Matches::Ops(MatchOps {
                startswith: Some(MatchValues {
                    r#type: Some(OneOrMany::Many(model.input_mime_types.clone())),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        })));
    }

    if model.skip_processed_items {
        filters.push(QueryElement::Not(NotOperator {
            not_: Box::new(QueryElement::ProcessedBy(ProcessedBy {
                processed_by: model.setter_name.clone(),
            })),
        }));
    }

    // Items this setter has already rejected (and whose verdict is confirmed)
    // are wasted work every run. Unconditional: a model that reprocesses
    // everything still has nothing to gain from an item it cannot decode.
    filters.push(QueryElement::Not(NotOperator {
        not_: Box::new(QueryElement::FailedFor(FailedFor {
            failed_for: model.setter_name.clone(),
        })),
    }));

    let mut user_filters = Vec::new();
    for filter in &config.job_filters {
        if filter
            .setter_names
            .iter()
            .any(|name| name == "*" || name == &model.setter_name)
        {
            match &filter.pql_query {
                QueryElement::And(and) => user_filters.extend(and.and_.clone()),
                other => user_filters.push(other.clone()),
            }
        }
    }
    filters.extend(user_filters);

    let query = if filters.is_empty() {
        None
    } else if filters.len() == 1 {
        Some(filters.remove(0))
    } else {
        Some(QueryElement::And(AndOperator { and_: filters }))
    };

    let mut pql = PqlQuery {
        query,
        page_size: 0,
        check_path: false,
        ..Default::default()
    };

    match model.target_entities.as_slice() {
        [value] if value == "items" => {
            pql.entity = EntityType::File;
            pql.partition_by = Some(vec![Column::ItemId]);
            pql.select = vec![
                Column::Sha256,
                Column::Path,
                Column::LastModified,
                Column::Type,
                Column::Md5,
                Column::Width,
                Column::Height,
                Column::Duration,
                Column::ContentEndMs,
                Column::AudioTracks,
                Column::VideoTracks,
                Column::SubtitleTracks,
            ];
        }
        [value] if value == "files" => {
            pql.entity = EntityType::File;
            pql.partition_by = None;
            pql.select = vec![
                Column::Sha256,
                Column::Path,
                Column::LastModified,
                Column::Type,
                Column::Md5,
                Column::Width,
                Column::Height,
                Column::Duration,
                Column::ContentEndMs,
                Column::AudioTracks,
                Column::VideoTracks,
                Column::SubtitleTracks,
            ];
        }
        [value] if value == "text" => {
            pql.entity = EntityType::Text;
            pql.partition_by = Some(vec![Column::DataId]);
            pql.select = vec![
                Column::Sha256,
                Column::Path,
                Column::LastModified,
                Column::Type,
                Column::Md5,
                Column::Width,
                Column::Height,
                Column::DataId,
                Column::Text,
            ];
        }
        _ => {
            return Err(ApiError::bad_request(
                "Only items, files, and text target entities are supported",
            ));
        }
    }

    Ok(pql)
}

/// Keyset (cursor) and dedup (partition) columns for the compiled work query,
/// per target entity. Must stay in sync with `build_job_pql`: the cursor
/// column has to be unique per emitted row (`file_id` for file rows,
/// `data_id` for text rows, both selected by every variant), and the
/// partition column is the unit of work an item must not be dispatched twice
/// under (`item_id`/`data_id` mirror the query's partition_by).
fn work_query_keys(model: &ModelMetadata) -> (&'static str, &'static str) {
    match model.target_entities.as_slice() {
        [value] if value == "text" => ("data_id", "data_id"),
        [value] if value == "files" => ("file_id", "file_id"),
        _ => ("file_id", "item_id"),
    }
}

/// Wraps the compiled work query in a keyset-pagination envelope. The inner
/// SQL is emitted by our PQL compiler (possibly starting with a WITH clause,
/// which SQLite accepts inside a FROM subquery); the wrapper's cursor
/// placeholder binds *after* the inner query's own params because it appears
/// later in the SQL text.
fn chunked_work_query_sql(inner_sql: &str, cursor_column: &str, chunk_rows: usize) -> String {
    format!(
        "SELECT * FROM ({inner_sql}) AS work \
         WHERE work.\"{cursor_column}\" > ? \
         ORDER BY work.\"{cursor_column}\" ASC \
         LIMIT {chunk_rows}"
    )
}

/// Resolves the job's **cap** (`None` = auto) and threshold.
///
/// The cap chain is user intent only — explicit request value, then the
/// per-ID stored default, then the group default. The registry's
/// `default_batch_size` deliberately does *not* participate: core no longer
/// invents a batch size, and that metadata now only seeds the inference
/// side's first touch on unknown hardware (design doc "Batch size UX").
pub(crate) fn resolve_job_defaults(
    config: &SystemConfig,
    model: &ModelMetadata,
    batch_size: Option<i64>,
    threshold: Option<f64>,
) -> JobDefaults {
    let mut chosen_batch: Option<i64> = None;
    let mut chosen_threshold = model.default_threshold;

    for setting in &config.job_settings {
        if setting.group_name == model.group && setting.inference_id.is_none() {
            if let Some(default_batch) = setting.default_batch_size.filter(|value| *value > 0) {
                chosen_batch = Some(default_batch);
            }
            if model.default_threshold.is_some()
                && let Some(default_threshold) = setting.default_threshold
            {
                chosen_threshold = Some(default_threshold);
            }
        }
    }
    for setting in &config.job_settings {
        if setting.group_name == model.group
            && setting.inference_id.as_deref() == Some(&model.setter_name)
        {
            if let Some(default_batch) = setting.default_batch_size.filter(|value| *value > 0) {
                chosen_batch = Some(default_batch);
            }
            if model.default_threshold.is_some()
                && let Some(default_threshold) = setting.default_threshold
            {
                chosen_threshold = Some(default_threshold);
            }
        }
    }

    if let Some(batch) = batch_size
        && batch > 0
    {
        chosen_batch = Some(batch);
    }
    if threshold.is_some() {
        chosen_threshold = threshold;
    }

    // Mirror Python: a zero threshold anywhere along the chain means "unset"
    // and falls back to the model default (`threshold or default_threshold`),
    // and a still-zero/absent final value is omitted entirely so the
    // inference side can apply its own fallback (e.g. mcut for taggers).
    let resolved = match chosen_threshold {
        Some(value) if value != 0.0 => Some(value),
        _ => model.default_threshold,
    };
    let threshold = resolved.filter(|value| *value != 0.0);

    JobDefaults {
        batch_size: chosen_batch,
        threshold,
    }
}

pub(crate) async fn load_model_metadata(inference_id: &str) -> ApiResult<ModelMetadata> {
    let context = job_inference_context();
    let metadata = context.primary.get_metadata().await.map_err(|err| {
        tracing::error!(error = %err, "failed to load inference metadata");
        ApiError::internal("Failed to load inference metadata")
    })?;
    resolve_model_metadata(&metadata, inference_id)
}

/// Resolves a single model's metadata from an already-fetched `/metadata`
/// payload. Errors mean the model is unknown to the inference server (or its
/// entry is malformed) — the payload itself being unavailable is the caller's
/// distinction to make.
pub(crate) fn resolve_model_metadata(
    metadata: &Value,
    inference_id: &str,
) -> ApiResult<ModelMetadata> {
    let (group, short_id) = inference_id
        .split_once('/')
        .ok_or_else(|| ApiError::bad_request("Inference ID must be in group/id format"))?;

    let group_meta = metadata
        .get(group)
        .and_then(Value::as_object)
        .ok_or_else(|| ApiError::bad_request("Inference group not found"))?;

    let group_metadata = group_meta
        .get("group_metadata")
        .cloned()
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
    let inference_metadata = group_meta
        .get("inference_ids")
        .and_then(Value::as_object)
        .and_then(|map| map.get(short_id).cloned())
        .ok_or_else(|| ApiError::bad_request("Inference ID not found"))?;

    let merged = merge_metadata(group_metadata, inference_metadata);
    let input_spec = merged
        .get("input_spec")
        .and_then(Value::as_object)
        .ok_or_else(|| ApiError::bad_request("input_spec missing from metadata"))?;
    let handler = input_spec
        .get("handler")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::bad_request("input_spec.handler missing"))?;
    let opts = input_spec
        .get("opts")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    let target_entities = merged
        .get("target_entities")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|value| value.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| vec!["items".to_string()]);

    let output_type = merged
        .get("output_type")
        .and_then(Value::as_str)
        .unwrap_or("text")
        .to_string();

    let default_batch_size = merged
        .get("default_batch_size")
        .and_then(Value::as_i64)
        .unwrap_or(64);

    let default_threshold = merged.get("default_threshold").and_then(Value::as_f64);

    let input_mime_types = merged
        .get("input_mime_types")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|value| value.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let skip_processed_items = merged
        .get("skip_processed_items")
        .and_then(Value::as_bool)
        .unwrap_or(true);

    let unavailable_reason = if merged
        .get("unavailable")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        Some(
            merged
                .get("unavailable_reason")
                .and_then(Value::as_str)
                .unwrap_or("marked unavailable by the inference server")
                .to_string(),
        )
    } else {
        None
    };

    let name = merged
        .get("name")
        .and_then(Value::as_str)
        .map(|s| s.to_string());
    let description = merged
        .get("description")
        .and_then(Value::as_str)
        .map(|s| s.to_string());
    let link = merged
        .get("link")
        .and_then(Value::as_str)
        .map(|s| s.to_string());

    Ok(ModelMetadata {
        group: group.to_string(),
        inference_id: short_id.to_string(),
        setter_name: inference_id.to_string(),
        input_handler: handler.to_string(),
        input_handler_opts: opts,
        target_entities,
        output_type,
        default_batch_size,
        default_threshold,
        input_mime_types,
        skip_processed_items,
        unavailable_reason,
        name,
        description,
        link,
    })
}

fn merge_metadata(
    group_metadata: Value,
    inference_metadata: Value,
) -> serde_json::Map<String, Value> {
    let mut merged = match group_metadata {
        Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    if let Value::Object(inf_map) = inference_metadata {
        for (key, value) in inf_map {
            if key == "input_spec" {
                let mut base = merged
                    .get("input_spec")
                    .cloned()
                    .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
                deep_merge(&mut base, &value);
                merged.insert("input_spec".to_string(), base);
            } else {
                merged.insert(key, value);
            }
        }
    }
    merged
}

fn deep_merge(base: &mut Value, overlay: &Value) {
    match (base, overlay) {
        (Value::Object(base_map), Value::Object(overlay_map)) => {
            for (key, value) in overlay_map {
                match base_map.get_mut(key) {
                    Some(base_value) => deep_merge(base_value, value),
                    None => {
                        base_map.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        (base_val, overlay_val) => {
            *base_val = overlay_val.clone();
        }
    }
}

#[derive(Clone)]
struct CompiledQuery {
    sql: String,
    params: Vec<Value>,
}

fn compile_pql_select(query: PqlQuery) -> ApiResult<CompiledQuery> {
    let built =
        build_query_preprocessed(query, false).map_err(|err| ApiError::bad_request(err.message))?;
    compile_select(built)
}

fn compile_pql_count(query: PqlQuery) -> ApiResult<CompiledQuery> {
    let built =
        build_query_preprocessed(query, true).map_err(|err| ApiError::bad_request(err.message))?;
    compile_select(built)
}

fn compile_select(built: crate::pql::PqlBuilderResult) -> ApiResult<CompiledQuery> {
    let paginated = built.paginated_query();
    let (sql, values) = match built.with_clause {
        Some(with_clause) => paginated.with(with_clause).build(SqliteQueryBuilder),
        None => paginated.build(SqliteQueryBuilder),
    };
    let params = encode_values(values)?;
    Ok(CompiledQuery { sql, params })
}

fn encode_values(values: Values) -> ApiResult<Vec<Value>> {
    let mut encoded = Vec::with_capacity(values.iter().count());
    for value in values.into_iter() {
        encoded.push(encode_value(value)?);
    }
    Ok(encoded)
}

fn encode_value(value: SeaValue) -> ApiResult<Value> {
    match value {
        SeaValue::Bool(value) => Ok(value.map(Value::Bool).unwrap_or(Value::Null)),
        SeaValue::TinyInt(value) => Ok(value.map(|v| Value::from(v as i64)).unwrap_or(Value::Null)),
        SeaValue::SmallInt(value) => {
            Ok(value.map(|v| Value::from(v as i64)).unwrap_or(Value::Null))
        }
        SeaValue::Int(value) => Ok(value.map(Value::from).unwrap_or(Value::Null)),
        SeaValue::BigInt(value) => Ok(value.map(Value::from).unwrap_or(Value::Null)),
        SeaValue::TinyUnsigned(value) => {
            Ok(value.map(|v| Value::from(v as u64)).unwrap_or(Value::Null))
        }
        SeaValue::SmallUnsigned(value) => {
            Ok(value.map(|v| Value::from(v as u64)).unwrap_or(Value::Null))
        }
        SeaValue::Unsigned(value) => {
            Ok(value.map(|v| Value::from(v as u64)).unwrap_or(Value::Null))
        }
        SeaValue::BigUnsigned(value) => Ok(value.map(Value::from).unwrap_or(Value::Null)),
        SeaValue::Float(value) => Ok(match value {
            Some(v) => serde_json::Number::from_f64(v as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            None => Value::Null,
        }),
        SeaValue::Double(value) => Ok(match value {
            Some(v) => serde_json::Number::from_f64(v)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            None => Value::Null,
        }),
        SeaValue::String(value) => Ok(value.map(Value::String).unwrap_or(Value::Null)),
        SeaValue::Char(value) => Ok(value
            .map(|v| Value::String(v.to_string()))
            .unwrap_or(Value::Null)),
        SeaValue::Bytes(value) => match value {
            Some(bytes) => {
                let mut map = serde_json::Map::new();
                map.insert(
                    "__bytes__".to_string(),
                    Value::String(general_purpose::STANDARD.encode(bytes)),
                );
                Ok(Value::Object(map))
            }
            None => Ok(Value::Null),
        },
        SeaValue::Json(value) => Ok(value.map(|v| *v).unwrap_or(Value::Null)),
        _ => Err(ApiError::bad_request("Unsupported PQL parameter type")),
    }
}

fn bind_params<'q>(
    mut query: sqlx::query::Query<'q, sqlx::Sqlite, SqliteArguments>,
    params: &[Value],
) -> ApiResult<sqlx::query::Query<'q, sqlx::Sqlite, SqliteArguments>> {
    for param in params {
        query = bind_param(query, param)?;
    }
    Ok(query)
}

fn bind_param<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, SqliteArguments>,
    param: &Value,
) -> ApiResult<sqlx::query::Query<'q, sqlx::Sqlite, SqliteArguments>> {
    match param {
        Value::Null => Ok(query.bind(Option::<i64>::None)),
        Value::Bool(value) => Ok(query.bind(*value)),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(query.bind(value))
            } else if let Some(value) = value.as_u64() {
                if value <= i64::MAX as u64 {
                    Ok(query.bind(value as i64))
                } else {
                    Ok(query.bind(value as f64))
                }
            } else if let Some(value) = value.as_f64() {
                Ok(query.bind(value))
            } else {
                Ok(query.bind(value.to_string()))
            }
        }
        Value::String(value) => Ok(query.bind(value.clone())),
        Value::Object(map) => {
            if let Some(Value::String(encoded)) = map.get("__bytes__") {
                let decoded = general_purpose::STANDARD
                    .decode(encoded.as_bytes())
                    .map_err(|err| {
                        tracing::error!(error = %err, "failed to decode pql bytes param");
                        ApiError::bad_request("Invalid PQL parameters")
                    })?;
                return Ok(query.bind(decoded));
            }
            let encoded = serde_json::to_string(param).map_err(|err| {
                tracing::error!(error = %err, "failed to encode pql param");
                ApiError::bad_request("Invalid PQL parameters")
            })?;
            Ok(query.bind(encoded))
        }
        Value::Array(_) => {
            let encoded = serde_json::to_string(param).map_err(|err| {
                tracing::error!(error = %err, "failed to encode pql param");
                ApiError::bad_request("Invalid PQL parameters")
            })?;
            Ok(query.bind(encoded))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_error::{ApiErrorKind, SKIP_AFTER_AMBIGUOUS, SKIP_AFTER_CONFIRMED};
    use crate::db::extraction_errors::{STAGE_PREPARE, upsert_extraction_error};
    use crate::db::system_config::JobSettings;
    use crate::test_utils::test_data_dir;

    fn clip_model() -> ModelMetadata {
        let mut model = test_model("items", true);
        model.group = "clip".to_string();
        model.inference_id = "ViT-H-14".to_string();
        model.setter_name = "clip/ViT-H-14".to_string();
        model.output_type = "clip".to_string();
        model.default_batch_size = 64;
        model
    }

    fn config_with(settings: Vec<JobSettings>) -> SystemConfig {
        SystemConfig {
            job_settings: settings,
            ..SystemConfig::default()
        }
    }

    // Auto is the default and the registry number no longer leaks into it:
    // with nothing stored and nothing requested the job runs uncapped, and a
    // stored zero reads as "unset", not as a cap of zero.
    #[test]
    fn an_unset_batch_size_resolves_to_auto() {
        let model = clip_model();
        assert_eq!(
            resolve_job_defaults(&config_with(Vec::new()), &model, None, None).batch_size,
            None
        );
        assert_eq!(
            resolve_job_defaults(&config_with(Vec::new()), &model, Some(0), None).batch_size,
            None
        );

        let zeroed = config_with(vec![JobSettings {
            group_name: "clip".to_string(),
            inference_id: None,
            default_batch_size: Some(0),
            default_threshold: None,
        }]);
        assert_eq!(
            resolve_job_defaults(&zeroed, &model, None, None).batch_size,
            None
        );
    }

    // The cap chain is user intent only, most specific first.
    #[test]
    fn the_cap_chain_prefers_the_request_then_the_model_then_the_group() {
        let model = clip_model();
        let config = config_with(vec![
            JobSettings {
                group_name: "clip".to_string(),
                inference_id: None,
                default_batch_size: Some(8),
                default_threshold: None,
            },
            JobSettings {
                group_name: "clip".to_string(),
                inference_id: Some("clip/ViT-H-14".to_string()),
                default_batch_size: Some(4),
                default_threshold: None,
            },
        ]);
        assert_eq!(
            resolve_job_defaults(&config, &model, Some(2), None).batch_size,
            Some(2)
        );
        assert_eq!(
            resolve_job_defaults(&config, &model, None, None).batch_size,
            Some(4)
        );

        let group_only = config_with(vec![JobSettings {
            group_name: "clip".to_string(),
            inference_id: None,
            default_batch_size: Some(8),
            default_threshold: None,
        }]);
        assert_eq!(
            resolve_job_defaults(&group_only, &model, None, None).batch_size,
            Some(8)
        );
    }

    // The split the design turns on: core-side request sizing is the constant
    // and never the user's cap, while the cap goes to the inference side
    // untouched. The job body wires exactly these two: `unit_slots` is
    // `Semaphore::new(REQUEST_UNIT_BUDGET)` (no cap term at all),
    // `unit_capacity` is `request_unit_capacity`, and the value handed to
    // `pool.predict` is `gpu_batch_cap`'s — not the chunk size, which is what
    // it used to be.
    #[test]
    fn the_request_budget_is_independent_of_the_users_cap() {
        // Auto and every cap at or above the budget chunk at the budget.
        assert_eq!(request_unit_capacity(None), REQUEST_UNIT_BUDGET);
        assert_eq!(
            request_unit_capacity(Some(REQUEST_UNIT_BUDGET as i64)),
            REQUEST_UNIT_BUDGET
        );
        assert_eq!(request_unit_capacity(Some(4096)), REQUEST_UNIT_BUDGET);
        // A smaller cap also bounds the chunk, so no request outruns what the
        // far side may process at once.
        assert_eq!(request_unit_capacity(Some(8)), 8);
        // Zero is "unset" everywhere in the cap chain, never a capacity of 0.
        assert_eq!(request_unit_capacity(Some(0)), REQUEST_UNIT_BUDGET);
        assert_eq!(request_unit_capacity(Some(-1)), REQUEST_UNIT_BUDGET);

        // The cap itself is forwarded verbatim: not clamped to the budget,
        // not turned into a number when it is absent.
        assert_eq!(gpu_batch_cap(None), None);
        assert_eq!(gpu_batch_cap(Some(0)), None);
        assert_eq!(gpu_batch_cap(Some(8)), Some(8));
        assert_eq!(gpu_batch_cap(Some(4096)), Some(4096));
        assert_eq!(gpu_batch_cap(Some(i64::MAX)), Some(u32::MAX));

        // And auto reaches the NOT NULL log column as its 0 sentinel.
        assert_eq!(logged_batch_size(None), 0);
        assert_eq!(logged_batch_size(Some(8)), 8);
    }

    // The guard that keeps a boundary unload from landing on a model a newer
    // job has already loaded: a load bumps the generation under the slot, and
    // an unload holding an older generation must refuse to run.
    //
    // Serial by construction: this is the only test that touches the batch
    // slot (queue tests record the decision instead of performing it).
    #[tokio::test]
    async fn a_newer_batch_load_invalidates_a_pending_unload() {
        // What the queue actor captures when it spawns the unload.
        let captured = batch_load_generation();
        {
            let slot = lock_batch_slot().await;
            assert!(
                batch_unload_is_current(captured),
                "nothing has loaded yet, so the unload is still valid"
            );
            drop(slot);
        }

        // A new extraction job loads its model first.
        {
            let _slot = lock_batch_slot().await;
            begin_batch_load();
        }

        let _slot = lock_batch_slot().await;
        assert!(
            !batch_unload_is_current(captured),
            "the unload is stale and must abort instead of unloading the \
             model the new job just loaded"
        );
        assert!(
            batch_unload_is_current(batch_load_generation()),
            "an unload captured after that load is valid again"
        );
    }

    // The /metadata availability overlay must reach the job layer: an id
    // carrying `unavailable` resolves with its reason (falling back to a
    // generic one), and untouched ids resolve with none.
    #[test]
    fn resolve_model_metadata_picks_up_unavailable_reason() {
        let metadata = serde_json::json!({
            "doctr": {
                "group_metadata": {
                    "input_spec": {"handler": "image_frames", "opts": {}}
                },
                "inference_ids": {
                    "dots_ocr": {
                        "unavailable": true,
                        "unavailable_reason": "Requires an NVIDIA GPU with \
                         compute capability >= 8.0 (detected: 6.1)"
                    },
                    "bare_flag": {"unavailable": true},
                    "open_model": {"description": "fine"}
                }
            }
        });
        let gated = resolve_model_metadata(&metadata, "doctr/dots_ocr").unwrap();
        assert!(
            gated
                .unavailable_reason
                .as_deref()
                .unwrap()
                .contains(">= 8.0")
        );
        let bare = resolve_model_metadata(&metadata, "doctr/bare_flag").unwrap();
        assert_eq!(
            bare.unavailable_reason.as_deref(),
            Some("marked unavailable by the inference server")
        );
        let open = resolve_model_metadata(&metadata, "doctr/open_model").unwrap();
        assert_eq!(open.unavailable_reason, None);
    }

    fn test_model(target: &str, skip_processed: bool) -> ModelMetadata {
        ModelMetadata {
            group: "test".to_string(),
            inference_id: "test/tagger".to_string(),
            setter_name: "test/tagger".to_string(),
            input_handler: "image_frames".to_string(),
            input_handler_opts: serde_json::Map::new(),
            target_entities: vec![target.to_string()],
            output_type: "tags".to_string(),
            default_batch_size: 4,
            default_threshold: None,
            input_mime_types: vec!["image/".to_string()],
            skip_processed_items: skip_processed,
            unavailable_reason: None,
            name: None,
            description: None,
            link: None,
        }
    }

    #[test]
    fn work_query_keys_match_the_partitioning_build_job_pql_emits() {
        assert_eq!(
            work_query_keys(&test_model("items", true)),
            ("file_id", "item_id")
        );
        assert_eq!(
            work_query_keys(&test_model("files", true)),
            ("file_id", "file_id")
        );
        assert_eq!(
            work_query_keys(&test_model("text", true)),
            ("data_id", "data_id")
        );
    }

    // The WAL-growth regression (docs/sqlite-wal-growth.md): the driver must
    // drain the work query in keyset chunks, each row fetched at most once
    // across chunk queries, terminating even for models whose predicate never
    // excludes processed items (skip_processed_items = false — the case where
    // only the cursor prevents endless re-dispatch). Runs the *real* compiled
    // job query (WITH clause, GROUP BY partition) against the migrated
    // schema, so it also proves the wrapper SQL is valid SQLite.
    #[tokio::test]
    async fn chunked_work_query_fetches_each_item_exactly_once() {
        let mut dbs = crate::db::migrations::setup_test_databases().await;
        sqlx::query("INSERT INTO file_scans (id, start_time, path) VALUES (1, '2024-01-01T00:00:00', 'C:/data')")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        // Five image items (one with two files, so the GROUP BY partition has
        // something to collapse) and one non-image item the mime filter must
        // exclude.
        sqlx::query(
            r#"
            INSERT INTO items (id, sha256, md5, type, time_added)
            VALUES
                (1, 'sha_1', 'md5_1', 'image/png',  '2024-01-01T00:00:00'),
                (2, 'sha_2', 'md5_2', 'image/png',  '2024-01-01T00:00:00'),
                (3, 'sha_3', 'md5_3', 'image/jpeg', '2024-01-01T00:00:00'),
                (4, 'sha_4', 'md5_4', 'image/png',  '2024-01-01T00:00:00'),
                (5, 'sha_5', 'md5_5', 'image/png',  '2024-01-01T00:00:00'),
                (6, 'sha_6', 'md5_6', 'video/mp4',  '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO files (id, sha256, item_id, path, filename, last_modified, scan_id, available)
            VALUES
                (10, 'sha_1', 1, 'C:/data/1.png',  '1.png',  '2024-01-01T00:00:00', 1, 1),
                (11, 'sha_1', 1, 'C:/data/1b.png', '1b.png', '2024-01-01T00:00:00', 1, 1),
                (12, 'sha_2', 2, 'C:/data/2.png',  '2.png',  '2024-01-01T00:00:00', 1, 1),
                (13, 'sha_3', 3, 'C:/data/3.jpg',  '3.jpg',  '2024-01-01T00:00:00', 1, 1),
                (14, 'sha_4', 4, 'C:/data/4.png',  '4.png',  '2024-01-01T00:00:00', 1, 1),
                (15, 'sha_5', 5, 'C:/data/5.png',  '5.png',  '2024-01-01T00:00:00', 1, 1),
                (16, 'sha_6', 6, 'C:/data/6.mp4',  '6.mp4',  '2024-01-01T00:00:00', 1, 1)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let model = test_model("items", false);
        let config = SystemConfig::default();
        let pql = build_job_pql(&config, &model).unwrap();
        let compiled = compile_pql_select(pql).unwrap();
        let (cursor_column, partition_column) = work_query_keys(&model);
        // Chunk size 2 with 5 matching items forces multiple chunk queries.
        let chunk_sql = chunked_work_query_sql(&compiled.sql, cursor_column, 2);

        let mut cursor = i64::MIN;
        let mut dispatched = std::collections::HashSet::new();
        let mut chunk_queries = 0;
        loop {
            chunk_queries += 1;
            assert!(
                chunk_queries <= 10,
                "chunked drain failed to terminate: the cursor is not advancing"
            );
            let mut query = sqlx::query(sqlx::AssertSqlSafe(chunk_sql.as_str()));
            query = bind_params(query, &compiled.params).unwrap();
            let rows = query
                .bind(cursor)
                .fetch_all(&mut dbs.index_conn)
                .await
                .expect("wrapped work query must be valid SQLite");
            let fetched = rows.len();
            for row in &rows {
                let key: i64 = row.try_get(cursor_column).unwrap();
                assert!(key > cursor, "chunk rows must be ordered by the cursor key");
                cursor = key;
                let partition_key: i64 = row.try_get(partition_column).unwrap();
                assert!(
                    dispatched.insert(partition_key),
                    "item {partition_key} was dispatched twice — the regression \
                     this drain exists to prevent"
                );
            }
            if fetched < 2 {
                break;
            }
        }

        assert_eq!(
            {
                let mut seen: Vec<i64> = dispatched.into_iter().collect();
                seen.sort_unstable();
                seen
            },
            vec![1, 2, 3, 4, 5],
            "every matching item exactly once; the video item filtered out"
        );
    }

    // The completion decision, exhaustively. It is what stands between "12
    // corrupt files" and a job history full of phantom inference outages, and
    // between a real outage and a job that quietly reports success.
    #[test]
    fn job_failure_classifier_covers_every_combination() {
        use JobFailure::*;
        // (processed, errors, input_errors, expected)
        let cases = [
            // Nothing attempted: never a failure, whatever the counters say.
            (0, 0, 0, None),
            (0, 3, 3, None),
            // Something succeeded.
            (5, 0, 0, None),
            (5, 2, 2, None),
            (5, 4, 0, None),
            // Everything failed, every failure the item's own media.
            (1, 1, 1, InputMediaOnly),
            (5, 5, 5, InputMediaOnly),
            // Everything failed, at least one failure not input-side: the
            // ledger-write failure path lands here on purpose.
            (5, 5, 4, Systemic),
            (5, 5, 0, Systemic),
            (1, 1, 0, Systemic),
            // Impossible counts (input_errors is a subset of errors) are
            // treated as systemic rather than soft-completed.
            (5, 5, 6, Systemic),
        ];
        for (processed, errors, input_errors, expected) in cases {
            assert_eq!(
                classify_extraction_job_failure(processed, errors, input_errors),
                expected,
                "processed={processed} errors={errors} input_errors={input_errors}"
            );
        }
    }

    fn image_model() -> ModelMetadata {
        let mut model = test_model("items", true);
        model.setter_name = "test/clip".to_string();
        model
    }

    async fn seed_ledger_fixture(conn: &mut sqlx::SqliteConnection, files: &[(i64, &str, &str)]) {
        sqlx::query(
            "INSERT INTO file_scans (id, start_time, path) \
             VALUES (1, '2026-01-01T00:00:00', 'C:/data')",
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        for (id, sha256, path) in files {
            sqlx::query(
                "INSERT INTO items (id, sha256, md5, type, time_added) \
                 VALUES (?, ?, ?, 'image/png', '2026-01-01T00:00:00')",
            )
            .bind(id)
            .bind(sha256)
            .bind(format!("md5_{sha256}"))
            .execute(&mut *conn)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO files (id, sha256, item_id, path, filename, last_modified, \
                 scan_id, available) VALUES (?, ?, ?, ?, 'f.png', '2026-01-01T00:00:00', 1, 1)",
            )
            .bind(id)
            .bind(sha256)
            .bind(id)
            .bind(path)
            .execute(&mut *conn)
            .await
            .unwrap();
        }
        sqlx::query("INSERT INTO setters (id, name) VALUES (1, 'test/clip')")
            .execute(&mut *conn)
            .await
            .unwrap();
    }

    /// The item ids the model's work query currently selects.
    async fn work_query_items(
        conn: &mut sqlx::SqliteConnection,
        model: &ModelMetadata,
    ) -> Vec<i64> {
        work_query_column(conn, model, "item_id").await
    }

    /// One key column of everything the model's work query currently selects.
    async fn work_query_column(
        conn: &mut sqlx::SqliteConnection,
        model: &ModelMetadata,
        column: &str,
    ) -> Vec<i64> {
        let pql = build_job_pql(&SystemConfig::default(), model).unwrap();
        let compiled = compile_pql_select(pql).unwrap();
        let mut query = sqlx::query(sqlx::AssertSqlSafe(compiled.sql.as_str()));
        query = bind_params(query, &compiled.params).unwrap();
        let rows = query
            .fetch_all(&mut *conn)
            .await
            .expect("the work query must be valid SQLite");
        let mut ids: Vec<i64> = rows
            .iter()
            .map(|row| row.try_get::<i64, _>(column).unwrap())
            .collect();
        ids.sort_unstable();
        ids
    }

    /// A migrated on-disk index database, which is what the writer actor (and
    /// therefore every ledger write path) needs.
    async fn ledger_test_db(name: &'static str) -> &'static str {
        let user_db = format!("{name}_user");
        crate::db::migrations::migrate_databases_on_disk(Some(name), Some(&user_db))
            .await
            .expect("migrate test databases");
        name
    }

    fn image_item(item_id: i64, sha256: &str, path: &str) -> JobInputData {
        JobInputData {
            file_id: item_id,
            item_id,
            path: path.to_string(),
            sha256: sha256.to_string(),
            md5: format!("md5_{sha256}"),
            last_modified: "2026-01-01T00:00:00".to_string(),
            item_type: "image/png".to_string(),
            duration: None,
            content_end_ms: None,
            audio_tracks: None,
            video_tracks: None,
            subtitle_tracks: None,
            width: None,
            height: None,
            data_id: None,
            text: None,
        }
    }

    // End to end for the confirmed verdict: a file whose header cannot be
    // parsed is `input`, the record it owes lands in the ledger with the
    // item's mime type, and the work query stops offering it — which is the
    // entire point of the ledger.
    #[tokio::test]
    async fn a_corrupt_image_lands_in_the_ledger_and_leaves_the_work_query() {
        let dir = tempfile::TempDir::new().unwrap();
        let corrupt = dir.path().join("corrupt.png");
        std::fs::write(&corrupt, b"this is definitely not a PNG").unwrap();
        let healthy = dir.path().join("healthy.png");
        std::fs::write(&healthy, b"also not a PNG, but never attempted").unwrap();

        let mut dbs = crate::db::migrations::setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed_ledger_fixture(
            conn,
            &[
                (1, "sha_corrupt", corrupt.to_string_lossy().as_ref()),
                (2, "sha_other", healthy.to_string_lossy().as_ref()),
            ],
        )
        .await;

        let model = image_model();
        assert_eq!(
            work_query_items(conn, &model).await,
            vec![1, 2],
            "both items are work before anything failed"
        );

        let err = input_handlers::prepare_item(
            "unused",
            &model,
            image_item(1, "sha_corrupt", corrupt.to_string_lossy().as_ref()),
            true,
        )
        .await
        .expect_err("an unparseable header must fail the item");
        assert_eq!(err.kind(), ApiErrorKind::Input);
        assert_eq!(err.skip_after(), SKIP_AFTER_CONFIRMED);

        let record = failure_record(&model, 7, STAGE_PREPARE, "sha_corrupt", &err);
        upsert_extraction_error(conn, &record).await.unwrap();

        let (stage, class, attempts, mime, job): (String, String, i64, String, Option<i64>) =
            sqlx::query_as(
                "SELECT stage, error_class, attempts, mime_type, last_job_id \
                 FROM item_extraction_errors",
            )
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(
            (stage.as_str(), class.as_str(), attempts, mime.as_str(), job),
            (STAGE_PREPARE, "input", 1, "image/png", Some(7))
        );

        assert_eq!(
            work_query_items(conn, &model).await,
            vec![2],
            "a confirmed verdict takes the item out of the work query"
        );
    }

    // The ambiguous threshold: a verdict from a tool that did its own file
    // I/O must cost exactly one confirmation re-attempt, in a later run,
    // before it suppresses anything.
    #[tokio::test]
    async fn an_unconfirmed_verdict_keeps_the_item_for_one_more_run() {
        let mut dbs = crate::db::migrations::setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed_ledger_fixture(conn, &[(1, "sha_one", "C:/data/1.png")]).await;
        let model = image_model();

        let err = ApiError::input_unconfirmed("ffmpeg exited 1");
        assert_eq!(err.skip_after(), SKIP_AFTER_AMBIGUOUS);
        let mut record = failure_record(&model, 1, STAGE_PREPARE, "sha_one", &err);
        upsert_extraction_error(conn, &record).await.unwrap();
        assert_eq!(
            work_query_items(conn, &model).await,
            vec![1],
            "one failure does not settle an ambiguous verdict"
        );

        // A *later* job confirms it; a second failure inside the same job
        // would not (the upsert dedups on last_job_id).
        record.job_id = Some(1);
        upsert_extraction_error(conn, &record).await.unwrap();
        assert_eq!(
            work_query_items(conn, &model).await,
            vec![1],
            "the same job cannot confirm its own verdict"
        );
        record.job_id = Some(2);
        upsert_extraction_error(conn, &record).await.unwrap();
        assert!(
            work_query_items(conn, &model).await.is_empty(),
            "the second run confirms the verdict"
        );
    }

    // Parity guard: the gateway's own I/O failing says nothing about the
    // media, so nothing is recorded and the item stays selectable. A missing
    // file is the cheapest way to provoke exactly that.
    #[tokio::test]
    async fn a_missing_file_is_transient_and_records_nothing() {
        let dir = tempfile::TempDir::new().unwrap();
        let missing = dir.path().join("gone.png");
        let missing_gif = dir.path().join("gone.gif");
        let model = image_model();

        for (path, mime) in [
            (missing.to_string_lossy().to_string(), "image/png"),
            (missing_gif.to_string_lossy().to_string(), "image/gif"),
        ] {
            let mut item = image_item(1, "sha_one", &path);
            item.item_type = mime.to_string();
            let err = input_handlers::prepare_item("unused", &model, item, true)
                .await
                .expect_err("a missing file must fail the item");
            assert_eq!(
                err.persisted_class(),
                Option::None,
                "{mime}: a read failure is transient and must never be recorded"
            );
        }
    }

    // The setter predicate inside the FailedFor CTE, which nothing else
    // covers: a verdict belongs to one (item, setter) pair, so a tagger that
    // cannot read a file must never take that file away from CLIP.
    #[tokio::test]
    async fn a_different_setters_verdict_does_not_hide_the_item() {
        let mut dbs = crate::db::migrations::setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed_ledger_fixture(conn, &[(1, "sha_one", "C:/data/1.png")]).await;
        sqlx::query("INSERT INTO setters (id, name) VALUES (2, 'test/tagger')")
            .execute(&mut *conn)
            .await
            .unwrap();

        // A confirmed verdict, recorded by the *other* setter.
        let mut tagger = image_model();
        tagger.setter_name = "test/tagger".to_string();
        let record = failure_record(
            &tagger,
            1,
            STAGE_PREPARE,
            "sha_one",
            &ApiError::input("corrupt"),
        );
        upsert_extraction_error(conn, &record).await.unwrap();
        assert!(
            work_query_items(conn, &tagger).await.is_empty(),
            "the setter that recorded the verdict does stop seeing the item"
        );

        assert_eq!(
            work_query_items(conn, &image_model()).await,
            vec![1],
            "another setter's verdict says nothing about this one's work"
        );
    }

    // The ledger keys on the item, but the work query of a file-target model
    // yields file rows: a verdict has to take *every* file of that item out,
    // or the item comes back through its second path and fails again.
    #[tokio::test]
    async fn a_verdict_removes_every_file_of_a_multi_file_item() {
        let mut dbs = crate::db::migrations::setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed_ledger_fixture(
            conn,
            &[
                (1, "sha_one", "C:/data/1.png"),
                (2, "sha_two", "C:/data/2.png"),
            ],
        )
        .await;
        // A second path for the same item (a hardlink or a copy).
        sqlx::query(
            "INSERT INTO files (id, sha256, item_id, path, filename, last_modified, \
             scan_id, available) \
             VALUES (3, 'sha_one', 1, 'C:/data/1-copy.png', 'f.png', '2026-01-01T00:00:00', 1, 1)",
        )
        .execute(&mut *conn)
        .await
        .unwrap();

        let mut model = image_model();
        model.target_entities = vec!["files".to_string()];
        assert_eq!(
            work_query_column(conn, &model, "file_id").await,
            vec![1, 2, 3],
            "every file row is work before anything failed"
        );

        let record = failure_record(
            &model,
            1,
            STAGE_PREPARE,
            "sha_one",
            &ApiError::input("corrupt"),
        );
        upsert_extraction_error(conn, &record).await.unwrap();
        assert_eq!(
            work_query_column(conn, &model, "file_id").await,
            vec![2],
            "one item-keyed verdict removes both of that item's file rows, \
             and only that item's"
        );
    }

    // The success-path delete gate. An item with an *active* row is not in the
    // work query at all, so the only row a success can ever clear is the
    // sub-threshold one a transient blip left behind — which is exactly why
    // the gate lists all rows rather than the active ones. Without this, that
    // row survives, and a second blip months later suppresses a healthy file.
    // The gate is per-sha256: an item that owes nothing must not pay for a
    // writer round-trip just because some *other* item has a row.
    #[tokio::test]
    async fn a_successful_item_clears_its_unconfirmed_row() {
        let _test_env = test_data_dir();
        let index_db = ledger_test_db("extraction_clear_unconfirmed").await;
        {
            let mut conn = crate::db::open_index_db_write_no_user_data(index_db)
                .await
                .unwrap();
            seed_ledger_fixture(&mut conn, &[(1, "sha_one", "C:/data/1.png")]).await;
            // One ambiguous failure: attempts = 1, skip_after = 2.
            let record = failure_record(
                &image_model(),
                1,
                STAGE_PREPARE,
                "sha_one",
                &ApiError::input_unconfirmed("an SMB blip looks like this"),
            );
            upsert_extraction_error(&mut conn, &record).await.unwrap();
        }

        let model = image_model();
        // Computed exactly as the job start does it.
        let ledger_shas = {
            let mut conn = crate::db::open_index_db_read_no_user_data(index_db)
                .await
                .unwrap();
            assert_eq!(
                work_query_items(&mut conn, &model).await,
                vec![1],
                "an unconfirmed verdict leaves the item selectable, which is \
                 how it can succeed at all"
            );
            list_error_sha256s_for_setter(&mut conn, &model.setter_name)
                .await
                .unwrap()
        };
        assert!(
            ledger_shas.contains("sha_one"),
            "the gate must see the sub-threshold row; an active-only query \
             would return nothing here and skip the delete forever"
        );

        // An item outside the set never reaches the writer at all.
        clear_ledger_row(index_db, &model, "sha_absent", &ledger_shas).await;
        clear_ledger_row(index_db, &model, "sha_one", &ledger_shas).await;

        let mut conn = crate::db::open_index_db_read_no_user_data(index_db)
            .await
            .unwrap();
        assert!(
            list_error_sha256s_for_setter(&mut conn, &model.setter_name)
                .await
                .unwrap()
                .is_empty(),
            "a success wipes the item's slate"
        );
    }

    // A ledger write that fails is a database problem, never a verdict: it
    // must count systemic and return the error, or a DB outage soft-completes
    // the job as "all corrupt media".
    #[tokio::test]
    async fn a_ledger_write_failure_is_systemic() {
        let _test_env = test_data_dir();
        let index_db = ledger_test_db("extraction_ledger_write_fails").await;
        {
            let mut conn = crate::db::open_index_db_write_no_user_data(index_db)
                .await
                .unwrap();
            seed_ledger_fixture(&mut conn, &[(1, "sha_one", "C:/data/1.png")]).await;
        }

        // No `setters` row for this name, so the upsert matches nothing.
        let mut model = image_model();
        model.setter_name = "test/never-registered".to_string();
        let (outcome, returned) = record_item_failure(
            index_db,
            &model,
            1,
            STAGE_PREPARE,
            "sha_one",
            "C:/data/1.png",
            ApiError::input("corrupt"),
        )
        .await;
        assert_eq!(
            outcome,
            ItemOutcome::Failed,
            "an unrecorded failure is not an input verdict"
        );
        assert!(
            returned.is_some(),
            "the item task must propagate the write failure"
        );
    }

    // A missing dependency is input-side (it must not fail the job) but it is
    // also the one input-side class the user can fix, so the blocker has to
    // survive into the counters — a job that completes without naming it is a
    // silent no-op the user cannot diagnose.
    #[tokio::test]
    async fn a_blocked_prepare_failure_counts_input_side() {
        let _test_env = test_data_dir();
        let index_db = ledger_test_db("extraction_blocked_counts").await;
        {
            let mut conn = crate::db::open_index_db_write_no_user_data(index_db)
                .await
                .unwrap();
            seed_ledger_fixture(&mut conn, &[(1, "sha_one", "C:/data/1.pdf")]).await;
        }
        let model = image_model();
        let job_id = call_index_db_writer(index_db, |reply| IndexDbWriterMessage::AddDataLog {
            scan_time: crate::db::extraction_write::current_iso_timestamp(),
            threshold: None,
            types: vec![model.output_type.clone()],
            setter: model.setter_name.clone(),
            batch_size: 4,
            reply,
        })
        .await
        .unwrap();

        let (outcome, returned) = record_item_failure(
            index_db,
            &model,
            job_id,
            STAGE_PREPARE,
            "sha_one",
            "C:/data/1.pdf",
            ApiError::blocked(Blocker::Pdfium, "pdfium is not available"),
        )
        .await;
        assert_eq!(
            outcome,
            ItemOutcome::InputFailed {
                blocker: Some(Blocker::Pdfium)
            },
            "a blocked verdict is input-side and carries its dependency"
        );
        assert!(returned.is_none(), "the job continues past a blocked item");

        let counters = Arc::new(Mutex::new(JobCounters::default()));
        finalize_item(
            index_db,
            job_id,
            "application/pdf",
            0,
            outcome,
            Arc::clone(&counters),
            1,
        )
        .await;

        {
            let guard = counters.lock().await;
            assert_eq!((guard.errors, guard.input_errors), (1, 1));
            assert_eq!(guard.blocked_errors, 1);
            assert_eq!(
                guard.blocked.iter().copied().collect::<Vec<_>>(),
                vec![Blocker::Pdfium],
                "the job must be able to name what has to be installed"
            );
        }

        let mut conn = crate::db::open_index_db_read_no_user_data(index_db)
            .await
            .unwrap();
        let (errors, input_errors): (i64, i64) =
            sqlx::query_as("SELECT errors, input_errors FROM data_log WHERE id = ?")
                .bind(job_id)
                .fetch_one(&mut conn)
                .await
                .unwrap();
        assert_eq!(
            (errors, input_errors),
            (1, 1),
            "job history shows the split without a join"
        );
    }

    // The anti-join shape the work query depends on: the failure CTE is
    // LEFT JOINed and required to be NULL, and it selects only rows whose
    // verdict is already active.
    #[test]
    fn the_work_query_anti_joins_the_active_ledger_rows() {
        let model = image_model();
        let pql = build_job_pql(&SystemConfig::default(), &model).unwrap();
        let sql = compile_pql_select(pql).unwrap().sql;
        assert!(sql.contains("item_extraction_errors"), "{sql}");
        assert!(
            sql.contains(r#""attempts" >= "item_extraction_errors"."skip_after""#),
            "the threshold must be part of the filter: {sql}"
        );
        assert!(sql.contains("_FailedFor"), "{sql}");
        assert!(
            sql.contains("LEFT JOIN") && sql.contains("IS NULL"),
            "the filter must be composed as an anti-join: {sql}"
        );
    }

    // Auto-heal, minus the probe: installing the dependency clears exactly
    // its rows and nothing else. The probe half is a real binding/spawn,
    // which is why the clearing takes its results as an argument.
    #[tokio::test]
    async fn healing_clears_only_the_dependencies_that_came_back() {
        let _test_env = test_data_dir();
        let index_db = "extraction_heal_blocked";
        crate::db::migrations::migrate_databases_on_disk(
            Some(index_db),
            Some("extraction_heal_blocked_user"),
        )
        .await
        .expect("migrate test databases");
        {
            let mut conn = crate::db::open_index_db_write_no_user_data(index_db)
                .await
                .unwrap();
            seed_ledger_fixture(
                &mut conn,
                &[
                    (1, "sha_pdf", "C:/data/1.pdf"),
                    (2, "sha_vid", "C:/data/2.mp4"),
                ],
            )
            .await;
            for (sha256, blocker) in [("sha_pdf", Blocker::Pdfium), ("sha_vid", Blocker::Ffmpeg)] {
                let err = ApiError::blocked(blocker, "dependency missing");
                let record = failure_record(&image_model(), 1, STAGE_PREPARE, sha256, &err);
                upsert_extraction_error(&mut conn, &record).await.unwrap();
            }
        }

        assert_eq!(
            heal_blocked(index_db, Vec::new()).await.unwrap(),
            0,
            "nothing probed present means no write at all"
        );
        assert_eq!(
            heal_blocked(index_db, vec![Blocker::Pdfium]).await.unwrap(),
            1
        );

        let mut conn = crate::db::open_index_db_read_no_user_data(index_db)
            .await
            .unwrap();
        assert_eq!(
            list_distinct_blockers(&mut conn).await.unwrap(),
            vec![Blocker::Ffmpeg],
            "the dependency that is still missing keeps its rows"
        );
    }

    // ffmpeg classification against the real toolchain: a file of garbage
    // bytes claiming to be a video is an *unconfirmed* payload verdict, never
    // `blocked` — that distinction is what keeps a missing codec from being
    // mistaken for a missing install. Only ffmpeg runs on this path now: the
    // frame extractor takes the item's stored duration instead of re-probing
    // it, so a non-zero ffmpeg exit is the whole classification.
    #[tokio::test]
    async fn ffmpeg_rejecting_a_file_is_an_unconfirmed_input_verdict() {
        // `ffmpeg_available` probes both executables, which is what the
        // auto-heal relies on and what this test needs.
        if !crate::media_tools::ffmpeg_available() {
            // No toolchain on this host; the classification is unobservable.
            return;
        }
        let _test_env = test_data_dir();
        let index_db = "extraction_ffmpeg_classify";
        crate::db::migrations::migrate_databases_on_disk(
            Some(index_db),
            Some("extraction_ffmpeg_classify_user"),
        )
        .await
        .expect("migrate test databases");

        let dir = tempfile::TempDir::new().unwrap();
        let fake_video = dir.path().join("garbage.mp4");
        std::fs::write(&fake_video, b"nothing here is a container").unwrap();

        let mut item = image_item(1, "sha_vid", fake_video.to_string_lossy().as_ref());
        item.item_type = "video/mp4".to_string();
        item.duration = Some(10.0);
        item.video_tracks = Some(1);

        let err = input_handlers::prepare_item(index_db, &image_model(), item, true)
            .await
            .expect_err("ffmpeg must reject a file that is not a container");
        assert_eq!(err.kind(), ApiErrorKind::Input);
        assert_eq!(
            err.skip_after(),
            SKIP_AFTER_AMBIGUOUS,
            "a tool that did its own file I/O never settles a verdict alone"
        );
    }

    // ------------------------------------------------------------------
    // Worker-reported per-item errors (docs/failed-media-retry-design.md,
    // "Batch isolation and the worker protocol").
    // ------------------------------------------------------------------

    fn slot(index: usize, class: SlotErrorClass) -> PredictSlotError {
        PredictSlotError {
            index,
            class,
            message: "Unreadable image: truncated".to_string(),
        }
    }

    // The verdict rules, in one place. Note the partial case: the ledger and
    // the work query key on the *item*, so an item some of whose work units
    // decoded on `input` grounds is processable media and must keep its
    // successful outputs — that per-unit verdict can never suppress the item.
    #[test]
    fn slot_error_classification_covers_the_whole_grid() {
        use SlotErrorClass::{Input, Transient};

        assert_eq!(classify_slot_errors(4, &[], false), SlotVerdict::Proceed);
        assert_eq!(
            classify_slot_errors(4, &[slot(1, Input)], false),
            SlotVerdict::Proceed,
            "partial input failures keep the item's successful outputs"
        );

        // Every input failed, all on the worker's `input` class: a verdict on
        // the media, which owes a ledger row.
        let verdict = classify_slot_errors(2, &[slot(0, Input), slot(1, Input)], false);
        let SlotVerdict::InputMedia(detail) = verdict else {
            panic!("expected an input-media verdict, got {verdict:?}");
        };
        assert!(detail.contains("all 2 inputs"), "{detail}");
        assert!(
            detail.contains("truncated"),
            "the worker's own text: {detail}"
        );

        // A single-input item, same rule.
        assert!(matches!(
            classify_slot_errors(1, &[slot(0, Input)], false),
            SlotVerdict::InputMedia(_)
        ));

        // Mixed classes never settle a verdict: `transient` says nothing
        // about the payload, so the item stays selectable.
        assert!(matches!(
            classify_slot_errors(2, &[slot(0, Input), slot(1, Transient)], false),
            SlotVerdict::Transient(_)
        ));
        assert!(matches!(
            classify_slot_errors(1, &[slot(0, Transient)], false),
            SlotVerdict::Transient(_)
        ));

        // Class is decided *before* arity: a `transient` slot among healthy
        // batch-mates is a request to retry that unit, and proceeding would
        // write the item's partial outputs and mark it processed — which
        // deletes the retry the worker asked for. So a partial mix carrying
        // any non-`input` class fails the whole item transiently.
        assert!(
            matches!(
                classify_slot_errors(2, &[slot(0, Transient)], false),
                SlotVerdict::Transient(_)
            ),
            "a transient slot is never swallowed by its successful batch-mates"
        );
        assert!(matches!(
            classify_slot_errors(4, &[slot(1, Transient)], false),
            SlotVerdict::Transient(_)
        ));
        let verdict = classify_slot_errors(4, &[slot(1, Input), slot(2, Transient)], false);
        let SlotVerdict::Transient(detail) = verdict else {
            panic!("a partial mix with a transient slot must stay transient, got {verdict:?}");
        };
        assert!(
            detail.contains("2 of 4 inputs") && detail.contains("transient"),
            "the summary names the scope and the class that decided it: {detail}"
        );
    }

    // The granularity caveat: for a text-entity model one input is one
    // extracted segment, while the ledger and the `failed_for` anti-join key
    // on the item — persisting would take every *other* segment of that item
    // out of the work query because a single segment was bad. So the same
    // all-input verdict stays transient there.
    #[test]
    fn a_text_entity_model_never_persists_a_worker_verdict() {
        let errors = [slot(0, SlotErrorClass::Input)];
        assert!(matches!(
            classify_slot_errors(1, &errors, false),
            SlotVerdict::InputMedia(_)
        ));
        let verdict = classify_slot_errors(1, &errors, true);
        let SlotVerdict::Transient(detail) = verdict else {
            panic!("a text-entity verdict must not be persisted, got {verdict:?}");
        };
        assert!(detail.contains("text-entity"), "{detail}");

        // And the discriminator is the same one the work query uses.
        assert!(targets_text_entity(&test_model("text", true)));
        assert!(!targets_text_entity(&test_model("items", true)));
        assert!(!targets_text_entity(&test_model("files", true)));
    }

    // The inference-stage half of the ledger: a worker verdict on an item's
    // media lands as `stage = 'inference'`, class `input`, confirmed at one
    // attempt (the worker decoded bytes it already had), counts input-side,
    // and takes the item out of the work query.
    #[tokio::test]
    async fn a_worker_input_verdict_lands_in_the_ledger_at_the_inference_stage() {
        let _test_env = test_data_dir();
        let index_db = ledger_test_db("extraction_worker_verdict").await;
        {
            let mut conn = crate::db::open_index_db_write_no_user_data(index_db)
                .await
                .unwrap();
            seed_ledger_fixture(&mut conn, &[(1, "sha_one", "C:/data/1.png")]).await;
        }
        let model = image_model();
        let job_id = call_index_db_writer(index_db, |reply| IndexDbWriterMessage::AddDataLog {
            scan_time: crate::db::extraction_write::current_iso_timestamp(),
            threshold: None,
            types: vec![model.output_type.clone()],
            setter: model.setter_name.clone(),
            batch_size: 4,
            reply,
        })
        .await
        .unwrap();

        let SlotVerdict::InputMedia(detail) = classify_slot_errors(
            1,
            &[slot(0, SlotErrorClass::Input)],
            targets_text_entity(&model),
        ) else {
            panic!("expected an input-media verdict");
        };
        let (outcome, returned) = record_item_failure(
            index_db,
            &model,
            job_id,
            crate::db::extraction_errors::STAGE_INFERENCE,
            "sha_one",
            "C:/data/1.png",
            ApiError::input(detail),
        )
        .await;
        assert_eq!(outcome, ItemOutcome::InputFailed { blocker: None });
        assert!(returned.is_none(), "the job continues past bad media");

        let mut conn = crate::db::open_index_db_read_no_user_data(index_db)
            .await
            .unwrap();
        let (stage, class, skip_after, attempts, error): (String, String, i64, i64, String) =
            sqlx::query_as(
                "SELECT stage, error_class, skip_after, attempts, error \
                 FROM item_extraction_errors",
            )
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(
            (stage.as_str(), class.as_str(), skip_after, attempts),
            (
                crate::db::extraction_errors::STAGE_INFERENCE,
                "input",
                SKIP_AFTER_CONFIRMED,
                1
            )
        );
        assert!(
            error.contains("truncated"),
            "the worker's text is audit: {error}"
        );
        assert!(
            work_query_items(&mut conn, &model).await.is_empty(),
            "the item leaves the work query"
        );

        let counters = Arc::new(Mutex::new(JobCounters::default()));
        finalize_item(
            index_db,
            job_id,
            "image/png",
            1,
            outcome,
            Arc::clone(&counters),
            1,
        )
        .await;
        let guard = counters.lock().await;
        assert_eq!((guard.errors, guard.input_errors), (1, 1));
        assert_eq!(
            guard.blocked_errors, 0,
            "a worker verdict blocks on nothing"
        );
    }

    fn json_response(values: Vec<Value>, errors: Vec<PredictSlotError>) -> PredictResponse {
        PredictResponse {
            outputs: PredictOutput::Json(values),
            errors,
        }
    }

    fn text_input(text: &str) -> InferenceInput {
        InferenceInput::new(serde_json::json!({ "text": text }), None)
    }

    // Layer 2 of the isolation design at the boundary that actually exists in
    // this process (one item's multi-unit chunk): every unit is re-submitted
    // alone, in order, exactly once, and the outputs reassemble as if it had
    // been one request.
    #[tokio::test]
    async fn isolation_retries_each_input_alone_and_keeps_order() {
        let inputs = [text_input("a"), text_input("b"), text_input("c")];
        let calls = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let seen = Arc::clone(&calls);
        let response = isolate_inputs(&inputs, move |single, max_batch| {
            let seen = Arc::clone(&seen);
            async move {
                assert_eq!(single.len(), 1, "isolation submits one input at a time");
                assert_eq!(
                    max_batch, 1,
                    "and says so on the wire: a retry still advertising the \
                     job's chunk size can be merged straight back into a GPU \
                     batch with other requests by the dispatcher's effective_cap"
                );
                let text = single[0].data["text"].as_str().unwrap().to_string();
                seen.lock().unwrap().push(text.clone());
                Ok(json_response(vec![serde_json::json!(text)], Vec::new()))
            }
        })
        .await
        .expect("every input succeeded alone");

        assert_eq!(*calls.lock().unwrap(), vec!["a", "b", "c"]);
        match response.outputs {
            PredictOutput::Json(values) => assert_eq!(
                values,
                vec![
                    serde_json::json!("a"),
                    serde_json::json!("b"),
                    serde_json::json!("c")
                ]
            ),
            other => panic!("expected Json outputs, got {other:?}"),
        }
        assert!(response.errors.is_empty());
    }

    // A unit that still fails alone aborts the pass with its own error and is
    // never promoted to an `input` verdict by pattern-matching its text (req
    // 1: the pipeline can never be stricter than the model). The item then
    // fails transiently, so no partial data is written for it.
    #[tokio::test]
    async fn an_input_that_fails_alone_stays_transient() {
        let inputs = [text_input("a"), text_input("b")];
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = Arc::clone(&attempts);
        let err = isolate_inputs(&inputs, move |_single, _max_batch| {
            let counted = Arc::clone(&counted);
            async move {
                let index = counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if index == 1 {
                    anyhow::bail!("corrupt file: cannot identify image");
                }
                Ok(json_response(vec![serde_json::json!("ok")], Vec::new()))
            }
        })
        .await
        .expect_err("the second input fails alone too");
        let text = format!("{err:#}");
        assert!(text.contains("input 1 failed on its own too"), "{text}");
        assert!(
            !text.contains("__error__"),
            "an exception text is never turned into a typed verdict: {text}"
        );
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "the pass stops at the first input that fails alone"
        );
    }

    // Typed slots survive isolation with their index rebased onto the item's
    // input list — the whole point of the retry is that healthy units still
    // complete while the bad one keeps its verdict.
    #[tokio::test]
    async fn isolation_rebases_slot_error_indices() {
        let inputs = [text_input("a"), text_input("b")];
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        let response = isolate_inputs(&inputs, move |_single, _max_batch| {
            let counted = Arc::clone(&counted);
            async move {
                let index = counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if index == 1 {
                    // Every slot of this sub-request failed: no outputs.
                    return Ok(json_response(
                        Vec::new(),
                        vec![slot(0, SlotErrorClass::Input)],
                    ));
                }
                Ok(json_response(vec![serde_json::json!("ok")], Vec::new()))
            }
        })
        .await
        .expect("a typed slot is a successful roundtrip");

        assert_eq!(response.errors.len(), 1);
        assert_eq!(
            response.errors[0].index, 1,
            "the slot index is the item's input index, not the sub-request's"
        );
        match response.outputs {
            PredictOutput::Json(values) => assert_eq!(values, vec![serde_json::json!("ok")]),
            other => panic!("expected the survivor's output, got {other:?}"),
        }
    }

    // A protocol violation is deterministic, so isolating it would re-ask the
    // same broken server one input at a time and burn a whole extra pass over
    // the item's units to learn nothing. The marker has to survive the
    // context layers the client and the pool wrap around it, which is what
    // `downcast_ref` gives us and a string match would not.
    #[test]
    fn a_protocol_violation_is_recognisable_through_the_context_chain() {
        let raw = anyhow::Error::new(ProtocolViolation::new(
            "predict output 2 is a malformed error slot: unknown class",
        ));
        assert!(is_protocol_violation(&raw));
        let wrapped = raw
            .context("inference endpoint failed")
            .context("batch failed");
        assert!(
            is_protocol_violation(&wrapped),
            "the marker must survive the context the client and pool add"
        );

        // Everything else is retryable and must still reach isolation.
        assert!(!is_protocol_violation(&anyhow::anyhow!(
            "CUDA out of memory"
        )));
    }

    // The survivor map: `PredictResponse` drops erroring slots, so the n-th
    // output is not the n-th input. Without a map the identity is used, which
    // is what every response from a server with no error slots gets.
    #[test]
    fn surviving_input_indices_maps_outputs_back_onto_inputs() {
        assert_eq!(surviving_input_indices(4, &[]), None);
        assert_eq!(
            surviving_input_indices(4, &[slot(1, SlotErrorClass::Input)]),
            Some(vec![0, 2, 3])
        );
        assert_eq!(
            surviving_input_indices(
                3,
                &[
                    slot(0, SlotErrorClass::Input),
                    slot(2, SlotErrorClass::Input)
                ]
            ),
            Some(vec![1])
        );
        assert_eq!(
            surviving_input_indices(1, &[slot(0, SlotErrorClass::Input)]),
            Some(Vec::new())
        );
    }

    /// `item_data.idx` is documented as the page/frame number, and the CLIP
    /// handler is where a video's frames get theirs. A rejected frame must
    /// leave a *gap*, not renumber its successors: stored 0,1,2 for frames
    /// 0,2,3 would silently mis-file every later frame of the item.
    #[tokio::test]
    async fn a_partial_clip_item_stores_the_original_frame_numbers() {
        let _test_env = test_data_dir();
        let index_db = ledger_test_db("extraction_partial_clip_idx").await;
        {
            let mut conn = crate::db::open_index_db_write_no_user_data(index_db)
                .await
                .unwrap();
            seed_ledger_fixture(&mut conn, &[(1, "sha_clip", "C:/data/clip.mp4")]).await;
        }
        let mut model = image_model();
        model.output_type = "clip".to_string();
        let job_id = data_log_job(index_db, &model).await;

        // Four frames, the second of which the worker rejected.
        let errors = vec![slot(1, SlotErrorClass::Input)];
        let survivors = surviving_input_indices(4, &errors).expect("a slot errored");
        let outputs = PredictOutput::Json(vec![
            serde_json::json!([0.0, 1.0]),
            serde_json::json!([2.0, 3.0]),
            serde_json::json!([4.0, 5.0]),
        ]);

        output_handlers::handle_outputs(
            index_db,
            &model,
            job_id,
            image_item(1, "sha_clip", "C:/data/clip.mp4"),
            outputs,
            Some(&survivors),
        )
        .await
        .expect("the surviving frames are written");

        assert_eq!(
            stored_indices(index_db, "clip").await,
            vec![0, 2, 3],
            "the rejected frame leaves a gap instead of shifting the rest"
        );
    }

    /// Same for text outputs, where the index is the reading order of the
    /// page/frame the text came from. (This handler already tolerates gaps —
    /// its dedup and length filters make them — so the only question is
    /// whether the surviving rows keep their own numbers.)
    #[tokio::test]
    async fn a_partial_text_item_stores_the_original_input_numbers() {
        let _test_env = test_data_dir();
        let index_db = ledger_test_db("extraction_partial_text_idx").await;
        {
            let mut conn = crate::db::open_index_db_write_no_user_data(index_db)
                .await
                .unwrap();
            seed_ledger_fixture(&mut conn, &[(1, "sha_text", "C:/data/doc.pdf")]).await;
        }
        let mut model = image_model();
        model.output_type = "text".to_string();
        let job_id = data_log_job(index_db, &model).await;

        let errors = vec![slot(1, SlotErrorClass::Input)];
        let survivors = surviving_input_indices(4, &errors).expect("a slot errored");
        let outputs = PredictOutput::Json(vec![
            serde_json::json!({"transcription": "page zero", "confidence": 0.9}),
            serde_json::json!({"transcription": "page two", "confidence": 0.8}),
            serde_json::json!({"transcription": "page three", "confidence": 0.7}),
        ]);

        output_handlers::handle_outputs(
            index_db,
            &model,
            job_id,
            image_item(1, "sha_text", "C:/data/doc.pdf"),
            outputs,
            Some(&survivors),
        )
        .await
        .expect("the surviving pages are written");

        assert_eq!(stored_indices(index_db, "text").await, vec![0, 2, 3]);

        // And the text itself stayed with its own page: reading order is the
        // property the index exists to preserve.
        let mut conn = crate::db::open_index_db_read_no_user_data(index_db)
            .await
            .unwrap();
        let rows: Vec<(i64, String)> = sqlx::query_as(
            "SELECT item_data.idx, extracted_text.text FROM item_data \
             JOIN extracted_text ON extracted_text.id = item_data.id \
             ORDER BY item_data.idx",
        )
        .fetch_all(&mut conn)
        .await
        .unwrap();
        assert_eq!(
            rows,
            vec![
                (0, "page zero".to_string()),
                (2, "page two".to_string()),
                (3, "page three".to_string()),
            ]
        );
    }

    /// Without a survivor map (no slot errored) the mapping is the identity,
    /// so every response an inference server without error slots can produce
    /// is stored exactly as it was before.
    #[tokio::test]
    async fn a_complete_clip_item_is_numbered_exactly_as_before() {
        let _test_env = test_data_dir();
        let index_db = ledger_test_db("extraction_complete_clip_idx").await;
        {
            let mut conn = crate::db::open_index_db_write_no_user_data(index_db)
                .await
                .unwrap();
            seed_ledger_fixture(&mut conn, &[(1, "sha_full", "C:/data/full.mp4")]).await;
        }
        let mut model = image_model();
        model.output_type = "clip".to_string();
        let job_id = data_log_job(index_db, &model).await;

        assert_eq!(surviving_input_indices(3, &[]), None);
        output_handlers::handle_outputs(
            index_db,
            &model,
            job_id,
            image_item(1, "sha_full", "C:/data/full.mp4"),
            PredictOutput::Json(vec![
                serde_json::json!([0.0]),
                serde_json::json!([1.0]),
                serde_json::json!([2.0]),
            ]),
            None,
        )
        .await
        .expect("every frame is written");

        assert_eq!(stored_indices(index_db, "clip").await, vec![0, 1, 2]);
    }

    /// A data_log row to hang the written output off, as a real job would.
    async fn data_log_job(index_db: &str, model: &ModelMetadata) -> i64 {
        call_index_db_writer(index_db, |reply| IndexDbWriterMessage::AddDataLog {
            scan_time: crate::db::extraction_write::current_iso_timestamp(),
            threshold: None,
            types: vec![model.output_type.clone()],
            setter: model.setter_name.clone(),
            batch_size: 4,
            reply,
        })
        .await
        .unwrap()
    }

    /// The `idx` values actually stored for a data type, in ascending order.
    async fn stored_indices(index_db: &str, data_type: &str) -> Vec<i64> {
        let mut conn = crate::db::open_index_db_read_no_user_data(index_db)
            .await
            .unwrap();
        sqlx::query_scalar("SELECT idx FROM item_data WHERE data_type = ? ORDER BY idx")
            .bind(data_type)
            .fetch_all(&mut conn)
            .await
            .unwrap()
    }
}
