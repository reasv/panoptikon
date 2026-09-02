use axum::{Json, http::StatusCode};
// axum's own Query (serde_urlencoded) cannot deserialize repeated params
// (?inference_ids=a&inference_ids=b) into a Vec; axum-extra's can, matching
// FastAPI's List[str] query parameter behavior.
use axum_extra::extract::Query;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use utoipa::{IntoParams, ToSchema};

use crate::api::db_params::DbQueryParams;
use crate::api_error::ApiError;
use crate::db::extraction_errors::{
    ExtractionErrorFilters, count_extraction_errors, list_extraction_errors,
};
use crate::db::extraction_log::{LogRecord, get_all_data_logs, get_setters_total_data};
use crate::db::file_scans::get_all_file_scans;
use crate::db::folders::get_folders_from_database;
use crate::db::ledger::ERROR_CLASSES;
use crate::db::scan_errors::{ScanErrorFilters, count_scan_errors, list_scan_errors};
use crate::db::system_config::{SystemConfig, SystemConfigStore};
use crate::db::{DbConnection, ReadOnly};
use crate::jobs::continuous_scan;
use crate::jobs::cron::{self, CronRunOutcome};
use crate::jobs::files::is_resync_needed;
use crate::jobs::inference_pool::job_inference_context;
use crate::db::index_writer::{IndexDbWriterMessage, call_index_db_writer};
use crate::db::vector_quants::{RECONCILE_JOB_TAG, VectorQuantStatus};
use crate::jobs::queue::{
    BatchDedup, JobModel, JobRequest, JobType, QueueStatusModel, cancel_queued_jobs,
    cancel_running_job, enqueue_db_maintenance, enqueue_job, enqueue_jobs_with_dedup,
    get_queue_status,
};

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct InferenceQuery {
    /// Inference ID List
    inference_ids: Vec<String>,
    /// Batch Size
    #[param(nullable)]
    batch_size: Option<i64>,
    /// Confidence Threshold
    #[param(nullable)]
    threshold: Option<f64>,
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct LogIdQuery {
    /// List of Log Ids to delete the generated data for
    log_ids: Vec<i64>,
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct QueueCancelQuery {
    /// List of Queue IDs to cancel
    queue_ids: Vec<i64>,
    /// Run deferred DB maintenance after this cancel
    #[param(nullable, default = true)]
    run_maintenance: Option<bool>,
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct CancelRunningQuery {
    /// Run deferred DB maintenance after this cancel
    #[param(nullable, default = true)]
    run_maintenance: Option<bool>,
}

/// The queue takes the inverse: the flag suppresses the maintenance job this
/// cancel's boundary would otherwise synthesize (owed work is kept either way).
fn suppress_maintenance(run_maintenance: Option<bool>) -> bool {
    !run_maintenance.unwrap_or(true)
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct HistoryQuery {
    /// Page number
    #[param(default = 1, minimum = 1)]
    page: Option<i64>,
    /// Page size
    #[param(minimum = 1)]
    page_size: Option<i64>,
}

#[derive(serde::Serialize, ToSchema)]
pub(crate) struct QueueCancelResponse {
    cancelled_jobs: Vec<i64>,
}

#[derive(serde::Serialize, ToSchema)]
pub(crate) struct CancelResponse {
    detail: String,
}

#[derive(serde::Serialize, ToSchema)]
pub(crate) struct FoldersResponse {
    included_folders: Vec<String>,
    excluded_folders: Vec<String>,
}

#[derive(serde::Serialize, ToSchema)]
pub(crate) struct SetterDataStats {
    total_counts: Vec<(String, i64)>,
}

#[derive(serde::Serialize, ToSchema)]
pub(crate) struct CronJobResponse {
    detail: String,
}

#[utoipa::path(
    get,
    operation_id = "queue_status",
    path = "/api/jobs/queue",
    tag = "jobs",
    summary = "Get running job and queue status",
    responses(
        (status = 200, description = "Queue status", body = QueueStatusModel)
    )
)]
pub(crate) async fn queue_status() -> Result<Json<QueueStatusModel>, ApiError> {
    let status = get_queue_status().await?;
    Ok(Json(status))
}

#[utoipa::path(
    post,
    operation_id = "enqueue_data_extraction",
    path = "/api/jobs/data/extraction",
    tag = "jobs",
    summary = "Run a data extraction job",
    params(DbQueryParams, InferenceQuery),
    responses(
        (status = 202, description = "Enqueued data extraction jobs", body = [JobModel])
    )
)]
pub(crate) async fn enqueue_data_extraction(
    Query(query): Query<InferenceQuery>,
    conn: DbConnection<ReadOnly>,
) -> Result<(StatusCode, Json<Vec<JobModel>>), ApiError> {
    // Validate the models and resolve effective batch_size/threshold at
    // enqueue time (mirrors Python): a bad inference ID fails this request
    // instead of a job hours later, and the queue status shows the values
    // the job will actually run with.
    let store = SystemConfigStore::from_env();
    let config = store.load(&conn.index_db)?;
    validate_external_inputs(&query.inference_ids).await?;
    let mut jobs = Vec::new();
    for inference_id in query.inference_ids {
        let model = crate::jobs::extraction::load_model_metadata(&inference_id).await?;
        let defaults = crate::jobs::extraction::resolve_job_defaults(
            &config,
            &model,
            query.batch_size,
            query.threshold,
        );
        let job = enqueue_job(JobRequest {
            job_type: JobType::DataExtraction,
            index_db: conn.index_db.clone(),
            user_data_db: conn.user_data_db.clone(),
            metadata: Some(inference_id),
            batch_size: Some(defaults.batch_size),
            threshold: defaults.threshold,
            log_id: None,
            tag: None,
        })
        .await?;
        jobs.push(job);
    }
    Ok((StatusCode::ACCEPTED, Json(jobs)))
}

#[utoipa::path(
    delete,
    operation_id = "enqueue_delete_extracted_data",
    path = "/api/jobs/data/extraction",
    tag = "jobs",
    summary = "Delete extracted data",
    params(DbQueryParams, InferenceQuery),
    responses(
        (status = 202, description = "Enqueued data deletion jobs", body = [JobModel])
    )
)]
pub(crate) async fn enqueue_delete_extracted_data(
    Query(query): Query<InferenceQuery>,
    conn: DbConnection<ReadOnly>,
) -> Result<(StatusCode, Json<Vec<JobModel>>), ApiError> {
    let mut jobs = Vec::new();
    for inference_id in query.inference_ids {
        let job = enqueue_job(JobRequest {
            job_type: JobType::DataDeletion,
            index_db: conn.index_db.clone(),
            user_data_db: conn.user_data_db.clone(),
            metadata: Some(inference_id),
            batch_size: None,
            threshold: None,
            log_id: None,
            tag: None,
        })
        .await?;
        jobs.push(job);
    }
    Ok((StatusCode::ACCEPTED, Json(jobs)))
}

#[utoipa::path(
    post,
    operation_id = "enqueue_folder_rescan",
    path = "/api/jobs/folders/rescan",
    tag = "jobs",
    summary = "Run a folder rescan",
    params(DbQueryParams),
    responses(
        (status = 202, description = "Enqueued folder rescan job", body = JobModel)
    )
)]
pub(crate) async fn enqueue_folder_rescan(
    conn: DbConnection<ReadOnly>,
) -> Result<(StatusCode, Json<JobModel>), ApiError> {
    let job = enqueue_job(JobRequest {
        job_type: JobType::FolderRescan,
        index_db: conn.index_db.clone(),
        user_data_db: conn.user_data_db.clone(),
        metadata: None,
        batch_size: None,
        threshold: None,
        log_id: None,
        tag: None,
    })
    .await?;
    Ok((StatusCode::ACCEPTED, Json(job)))
}

#[utoipa::path(
    put,
    operation_id = "enqueue_update_folders",
    path = "/api/jobs/folders",
    tag = "jobs",
    summary = "Update the database with the current folder lists in the config",
    description = "Must be run every time after the folder lists in the config are updated,\nto ensure that the database is in sync with the config.\nIf you update the config through the API, this will be done automatically if needed.\n\nThis will remove files and items from the database that are no longer in the included folders,\nand add files and items that are now in the included folders, as well as remove files and items\nfrom the database that are now in the excluded folders.",
    params(DbQueryParams),
    responses(
        (status = 202, description = "Enqueued folder update job", body = JobModel)
    )
)]
pub(crate) async fn enqueue_update_folders(
    conn: DbConnection<ReadOnly>,
) -> Result<(StatusCode, Json<JobModel>), ApiError> {
    let job = enqueue_job(JobRequest {
        job_type: JobType::FolderUpdate,
        index_db: conn.index_db.clone(),
        user_data_db: conn.user_data_db.clone(),
        metadata: None,
        batch_size: None,
        threshold: None,
        log_id: None,
        tag: None,
    })
    .await?;
    Ok((StatusCode::ACCEPTED, Json(job)))
}

#[utoipa::path(
    post,
    operation_id = "enqueue_db_maintenance",
    path = "/api/jobs/maintenance",
    tag = "jobs",
    summary = "Run database maintenance",
    description = "Enqueues a database maintenance job for the selected database: rebuild the tag \
        item counts, refresh query statistics, truncate the write-ahead log, and reclaim free space \
        (the space reclaim is skipped unless the database actually holds enough free pages to be \
        worth rewriting). Runs at the back of the queue, after everything already queued. \
        If a maintenance job for this database is already queued, that job is upgraded to do all \
        of the above and returned instead of adding a second one. Responds 409 only when a \
        maintenance job for this database is already running, since a pass in flight may already \
        have decided what to skip; retry once it finishes.",
    params(DbQueryParams),
    responses(
        (status = 202, description = "Enqueued (or upgraded) database maintenance job", body = JobModel),
        (status = 409, description = "A maintenance job for this database is already running", body = crate::api_error::ErrorBody)
    )
)]
pub(crate) async fn enqueue_maintenance(
    conn: DbConnection<ReadOnly>,
) -> Result<(StatusCode, Json<JobModel>), ApiError> {
    // 409 rather than a 200 "skipped" body: unlike the cron and reconcile
    // triggers this route's success body is a JobModel, and there is no job to
    // report when the request adds nothing. Reachable only for a *running*
    // pass — a queued one is upgraded and returned, so the promise this
    // endpoint makes ("this will recount") is kept.
    let job = enqueue_db_maintenance(&conn.index_db, &conn.user_data_db)
        .await?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::CONFLICT,
                "A maintenance job for this database is already running.",
            )
        })?;
    Ok((StatusCode::ACCEPTED, Json(job)))
}

#[utoipa::path(
    delete,
    operation_id = "cancel_queued",
    path = "/api/jobs/queue",
    tag = "jobs",
    summary = "Cancel queued jobs",
    params(QueueCancelQuery),
    responses(
        (status = 200, description = "Queued jobs cancelled", body = QueueCancelResponse)
    )
)]
pub(crate) async fn cancel_queued(
    Query(query): Query<QueueCancelQuery>,
) -> Result<Json<QueueCancelResponse>, ApiError> {
    let cancelled = cancel_queued_jobs(
        query.queue_ids,
        suppress_maintenance(query.run_maintenance),
    )
    .await?;
    if cancelled.is_empty() {
        return Err(ApiError::not_found("No matching queued jobs found."));
    }
    Ok(Json(QueueCancelResponse {
        cancelled_jobs: cancelled,
    }))
}

#[utoipa::path(
    post,
    operation_id = "cancel_current_job",
    path = "/api/jobs/cancel",
    tag = "jobs",
    summary = "Cancel the currently running job",
    params(CancelRunningQuery),
    responses(
        (status = 200, description = "Running job cancelled", body = CancelResponse)
    )
)]
pub(crate) async fn cancel_current_job(
    Query(query): Query<CancelRunningQuery>,
) -> Result<Json<CancelResponse>, ApiError> {
    let cancelled = cancel_running_job(suppress_maintenance(query.run_maintenance)).await?;
    let job_id = cancelled.ok_or_else(|| ApiError::not_found("No job is currently running."))?;
    Ok(Json(CancelResponse {
        detail: format!("Job {job_id} cancelled."),
    }))
}

#[utoipa::path(
    get,
    operation_id = "get_folders",
    path = "/api/jobs/folders",
    tag = "jobs",
    summary = "Get the current folder lists",
    description = "Get the current included and excluded folders in the database.\nThese are the folders that are being scanned and not being scanned, respectively.\n\nThis list may differ from the config, if the database has not been updated.",
    params(DbQueryParams),
    responses(
        (status = 200, description = "Current folder lists", body = FoldersResponse)
    )
)]
pub(crate) async fn get_folders(
    mut conn: DbConnection<ReadOnly>,
) -> Result<Json<FoldersResponse>, ApiError> {
    let included = get_folders_from_database(&mut conn.conn, true).await?;
    let excluded = get_folders_from_database(&mut conn.conn, false).await?;
    Ok(Json(FoldersResponse {
        included_folders: included,
        excluded_folders: excluded,
    }))
}

#[utoipa::path(
    get,
    operation_id = "get_scan_history",
    path = "/api/jobs/folders/history",
    tag = "jobs",
    summary = "Get the scan history",
    params(DbQueryParams, HistoryQuery),
    responses(
        (status = 200, description = "Scan history", body = [crate::db::file_scans::FileScanRecord])
    )
)]
pub(crate) async fn get_scan_history(
    Query(query): Query<HistoryQuery>,
    mut conn: DbConnection<ReadOnly>,
) -> Result<Json<Vec<crate::db::file_scans::FileScanRecord>>, ApiError> {
    let page = query.page.unwrap_or(1);
    let scans = get_all_file_scans(&mut conn.conn, page, query.page_size).await?;
    Ok(Json(scans))
}

#[utoipa::path(
    delete,
    operation_id = "delete_scan_data",
    path = "/api/jobs/data/history",
    tag = "jobs",
    summary = "Deletes data generated by the scans given log ids",
    params(DbQueryParams, LogIdQuery),
    responses(
        (status = 200, description = "Enqueued data deletion jobs", body = [JobModel])
    )
)]
pub(crate) async fn delete_scan_data(
    Query(query): Query<LogIdQuery>,
    conn: DbConnection<ReadOnly>,
) -> Result<Json<Vec<JobModel>>, ApiError> {
    let mut jobs = Vec::new();
    for log_id in query.log_ids {
        let job = enqueue_job(JobRequest {
            job_type: JobType::JobDataDeletion,
            index_db: conn.index_db.clone(),
            user_data_db: conn.user_data_db.clone(),
            metadata: None,
            batch_size: None,
            threshold: None,
            log_id: Some(log_id),
            tag: None,
        })
        .await?;
        jobs.push(job);
    }
    Ok(Json(jobs))
}

#[utoipa::path(
    get,
    operation_id = "get_extraction_history",
    path = "/api/jobs/data/history",
    tag = "jobs",
    summary = "Get the extraction history",
    params(DbQueryParams, HistoryQuery),
    responses(
        (status = 200, description = "Extraction history", body = [LogRecord])
    )
)]
pub(crate) async fn get_extraction_history(
    Query(query): Query<HistoryQuery>,
    mut conn: DbConnection<ReadOnly>,
) -> Result<Json<Vec<LogRecord>>, ApiError> {
    let page = query.page.unwrap_or(1);
    let logs = get_all_data_logs(&mut conn.conn, page, query.page_size).await?;
    Ok(Json(logs))
}

#[utoipa::path(
    put,
    operation_id = "update_config",
    path = "/api/jobs/config",
    tag = "jobs",
    summary = "Update the system configuration",
    params(DbQueryParams),
    request_body(content = SystemConfig, description = "The new system configuration"),
    responses(
        (status = 200, description = "Updated system configuration", body = SystemConfig)
    )
)]
pub(crate) async fn update_config(
    conn: DbConnection<ReadOnly>,
    Json(config): Json<SystemConfig>,
) -> Result<Json<SystemConfig>, ApiError> {
    // Python accepts unparseable cron strings and fails invisibly inside the
    // scheduler forever; reject them here so typos surface at save time.
    if let Err(err) = cron::validate_cron_schedule(&config.cron_schedule) {
        return Err(ApiError::bad_request(format!(
            "Invalid cron_schedule {:?}: {err}",
            config.cron_schedule
        )));
    }
    validate_external_inputs(
        &config
            .cron_jobs
            .iter()
            .map(|job| job.inference_id.clone())
            .collect::<Vec<_>>(),
    )
    .await?;
    // Normalize retired quantizer kinds into the section that gets SAVED —
    // the load path already reads `binary` as `int8`, so rewriting the file
    // is what makes it converge and stops the load-time warning; rejecting
    // it here instead would 400 every unrelated settings save on a DB whose
    // section predates the int8 remap. Genuinely invalid sections are still
    // rejected at save time: the load-time paths treat them as inert, which
    // would silently strand the profiles.
    let mut config = config;
    if let Some(quants) = &mut config.vector_quants {
        if crate::db::vector_quants::normalize_retired(quants) {
            tracing::info!(
                index_db = %conn.index_db,
                "normalized retired 'binary' vector quant profiles to 'int8' in the saved config"
            );
        }
        if let Err(message) = crate::db::vector_quants::resolve_desired(quants) {
            return Err(ApiError::bad_request(message));
        }
    }
    let store = SystemConfigStore::from_env();
    store.save(&conn.index_db, &config)?;
    let config = store.load(&conn.index_db)?;
    let _ = continuous_scan::notify_config_change(&conn.index_db).await;
    let _ = cron::notify_config_change(&conn.index_db).await;
    // Commit semantics: the TOML write, the discrepancy check, and its
    // consequence (synchronous metadata sync or a reconcile job) are one
    // action — there is no state where the config was written but the work
    // was not scheduled.
    crate::jobs::vector_quants::check_and_schedule(&conn.index_db, &conn.user_data_db).await;
    let resync_needed = is_resync_needed(&conn.index_db, &conn.user_data_db, &config).await?;
    if resync_needed {
        let _ = enqueue_job(JobRequest {
            job_type: JobType::FolderUpdate,
            index_db: conn.index_db.clone(),
            user_data_db: conn.user_data_db.clone(),
            metadata: None,
            batch_size: None,
            threshold: None,
            log_id: None,
            tag: None,
        })
        .await?;
    }
    Ok(Json(config))
}

/// Validate declarations when the upstream supports the additive endpoint.
/// Older remote Python Inferio servers do not have it, so a 404 preserves
/// their previous behavior; every other discovery failure is surfaced.
/// Load-time Inferio validation remains authoritative for current servers.
///
/// **Known and unaddressed**: `PUT /api/jobs/config` calls this
/// unconditionally, so an unreachable inference upstream fails every settings
/// save with a 5xx, whatever the edit was. Predates this branch and exists
/// identically on master. The two candidate fixes are to skip the probe when
/// no cron job names a model, or to degrade an unreachable upstream to
/// "cannot validate, save anyway".
async fn validate_external_inputs(inference_ids: &[String]) -> Result<(), ApiError> {
    let status = match job_inference_context()
        .primary
        .get_external_inputs_optional()
        .await
    {
        Ok(Some(status)) => status,
        Ok(None) => return Ok(()),
        Err(error) => {
            tracing::error!(%error, "failed to validate inference external inputs");
            return Err(ApiError::internal(
                "Failed to validate inference external inputs",
            ));
        }
    };
    for inference_id in inference_ids {
        let Some(usages) = status
            .get("models")
            .and_then(|models| models.get(inference_id))
            .and_then(JsonValue::as_array)
        else {
            continue;
        };
        for usage in usages {
            if usage.get("required").and_then(JsonValue::as_bool) != Some(true) {
                continue;
            }
            let Some(id) = usage.get("id").and_then(JsonValue::as_str) else {
                continue;
            };
            let definition = &status["definitions"][id];
            if definition.get("configured").and_then(JsonValue::as_bool) != Some(true) {
                let label = definition
                    .get("label")
                    .and_then(JsonValue::as_str)
                    .unwrap_or(id);
                return Err(ApiError::bad_request(format!(
                    "Model {inference_id} requires additional configuration: {label}"
                )));
            }
        }
    }
    Ok(())
}

#[utoipa::path(
    get,
    operation_id = "get_config",
    path = "/api/jobs/config",
    tag = "jobs",
    summary = "Get the current system configuration",
    params(DbQueryParams),
    responses(
        (status = 200, description = "Current system configuration", body = SystemConfig)
    )
)]
pub(crate) async fn get_config(
    conn: DbConnection<ReadOnly>,
) -> Result<Json<SystemConfig>, ApiError> {
    let store = SystemConfigStore::from_env();
    let config = store.load(&conn.index_db)?;
    Ok(Json(config))
}

#[utoipa::path(
    get,
    operation_id = "get_setter_data_count",
    path = "/api/jobs/data/setters/total",
    tag = "jobs",
    summary = "Get the total count of index data entry for each setter",
    params(DbQueryParams),
    responses(
        (status = 200, description = "Total setter data counts", body = SetterDataStats)
    )
)]
pub(crate) async fn get_setter_data_count(
    mut conn: DbConnection<ReadOnly>,
) -> Result<Json<SetterDataStats>, ApiError> {
    let totals = get_setters_total_data(&mut conn.conn).await?;
    Ok(Json(SetterDataStats {
        total_counts: totals,
    }))
}

/// Filters for the extraction failure ledger. Every field is independently
/// optional; the vocabularies are the ledger's own
/// (docs/failed-media-retry-design.md).
#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct ExtractionFailuresQuery {
    /// Only failures recorded for this setter. Deliberately *not* validated
    /// against the known setters: the vocabulary is free-form and depends on
    /// which models the user has ever run, so there is no closed list to check
    /// against. A typo therefore answers "no failures", which is acceptable
    /// here — unlike `error_class`, whose vocabulary is closed and enforced,
    /// because a mistyped class silently reading as "nothing is wrong" is
    /// exactly what an audit surface must not do.
    #[param(nullable)]
    setter: Option<String>,
    /// `input`, `blocked` or `resource`. Anything else is a 400.
    #[param(nullable)]
    error_class: Option<String>,
    /// `prepare` (the gateway could not produce the model's input) or
    /// `inference` (the worker rejected it).
    #[param(nullable)]
    stage: Option<String>,
    /// Prefix of the recorded mime type, e.g. `image/`.
    #[param(nullable)]
    mime_prefix: Option<String>,
    /// Page size. Defaults to 100; values outside 1..=1000 are clamped into
    /// that range rather than rejected. Deliberately unconstrained in the
    /// schema: a generated validating client must not refuse a request the
    /// server accepts.
    #[param(nullable)]
    limit: Option<i64>,
    /// Rows to skip. Values below 0 are clamped to 0 (start at the beginning),
    /// not rejected — same reason as `limit`.
    #[param(nullable)]
    offset: Option<i64>,
}

/// Filters for the filescan failure ledger. Deliberately *not* the extraction
/// query type: a scan failure predates every setter, so offering a `setter`
/// filter here would document a parameter that can only ever answer "none".
#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct ScanFailuresQuery {
    /// `input`, `blocked` or `resource`. Anything else is a 400.
    #[param(nullable)]
    error_class: Option<String>,
    /// `mime`, `metadata`, `header` or `decode`.
    #[param(nullable)]
    stage: Option<String>,
    /// Prefix of the recorded mime type, e.g. `image/`. Rows whose mime guess
    /// is what failed have no mime type and match no prefix.
    #[param(nullable)]
    mime_prefix: Option<String>,
    /// Page size. Defaults to 100; values outside 1..=1000 are clamped into
    /// that range rather than rejected. Deliberately unconstrained in the
    /// schema: a generated validating client must not refuse a request the
    /// server accepts.
    #[param(nullable)]
    limit: Option<i64>,
    /// Rows to skip. Values below 0 are clamped to 0 (start at the beginning),
    /// not rejected — same reason as `limit`.
    #[param(nullable)]
    offset: Option<i64>,
}

/// One recorded extraction failure, as served to the audit surface.
#[derive(serde::Serialize, ToSchema)]
pub(crate) struct ExtractionFailure {
    /// Ledger row id. Stable for as long as the row lives, which is what the
    /// UI keys rows on.
    id: i64,
    sha256: String,
    /// One of the paths this item is stored under, chosen deterministically
    /// (an available file first, then the lexicographically smallest path).
    /// An item can have several files and the ledger keys on the item, so
    /// this is a representative, not the whole story. Null when every file of
    /// the item has gone away.
    path: Option<String>,
    /// The item's mime type as recorded when the failure happened.
    mime_type: String,
    setter_name: String,
    /// `prepare` or `inference`.
    stage: String,
    /// `input`, `blocked` or `resource`.
    error_class: String,
    /// The missing dependency for a `blocked` row (`pdfium`, `html-renderer`
    /// or `ffmpeg`), null otherwise.
    blocker: Option<String>,
    /// Human-readable message, clamped when it was recorded.
    error: String,
    /// Attempts needed before the verdict suppresses the item.
    skip_after: i64,
    attempts: i64,
    /// `attempts >= skip_after`: the verdict is confirmed and the work query
    /// is skipping this item. False means the verdict is recorded but
    /// unconfirmed and will be retried.
    active: bool,
    /// The last job that saw this failure. Null only when it was recorded
    /// outside a job. This is *not* a foreign key and nothing nulls it when
    /// job rows are cleaned up, so the id may name a job that no longer
    /// exists — the ledger has to outlive the job history it refers to.
    last_job_id: Option<i64>,
    first_seen: String,
    last_seen: String,
}

/// One recorded filescan failure, as served to the audit surface.
#[derive(serde::Serialize, ToSchema)]
pub(crate) struct ScanFailure {
    id: i64,
    /// The path is the key of this ledger: these failures happen before an
    /// item — or even a hash — exists.
    path: String,
    /// `mime`, `metadata`, `header` or `decode`.
    stage: String,
    /// `input`, `blocked` or `resource`.
    error_class: String,
    blocker: Option<String>,
    /// The extension-based guess, or null when the guess is what failed.
    mime_type: Option<String>,
    error: String,
    skip_after: i64,
    attempts: i64,
    /// `attempts >= skip_after`: the verdict is confirmed. Not the same as
    /// "this path will be skipped": the walker also requires the file to still
    /// have the `last_modified`/`file_size` the failure was recorded against,
    /// so a file that has been repaired or otherwise modified since is
    /// re-attempted on the next scan even though this reads true.
    ///
    /// A `decode`-stage row never suppresses anything at any `attempts`: its
    /// file *is* indexed (only the visuals failed), so the row is audit-only
    /// and retry scheduling is the visuals cache's, not this ledger's.
    active: bool,
    /// The last scan that saw this failure. Null only when it was recorded
    /// outside a scan. This is *not* a foreign key and nothing nulls it when
    /// `file_scans` rows are cleaned up, so the id may name a scan that no
    /// longer exists.
    last_scan_id: Option<i64>,
    first_seen: String,
    last_seen: String,
}

#[derive(serde::Serialize, ToSchema)]
pub(crate) struct ExtractionFailuresResponse {
    /// How many failures match the filters, ignoring the page window — the
    /// denominator for `limit`/`offset` paging.
    total: i64,
    failures: Vec<ExtractionFailure>,
}

#[derive(serde::Serialize, ToSchema)]
pub(crate) struct ScanFailuresResponse {
    /// How many failures match the filters, ignoring the page window.
    total: i64,
    failures: Vec<ScanFailure>,
}

/// A class outside the vocabulary is a typo, and silently answering "no
/// failures" to it is the one thing an audit surface must not do. Deliberately
/// *not* applied to `stage`: the two ledgers have different stage vocabularies
/// and new ones are expected to appear, so a stage filter that matches nothing
/// is a legitimate answer.
fn validate_error_class(error_class: Option<String>) -> Result<Option<String>, ApiError> {
    if let Some(class) = &error_class {
        if !ERROR_CLASSES.contains(&class.as_str()) {
            return Err(ApiError::bad_request(format!(
                "Unknown error_class {class:?}; expected one of {}",
                ERROR_CLASSES.join(", ")
            )));
        }
    }
    Ok(error_class)
}

#[utoipa::path(
    get,
    operation_id = "get_extraction_failures",
    path = "/api/jobs/data/failures",
    tag = "jobs",
    summary = "List recorded data extraction failures",
    description = "The extraction failure ledger: media a setter has already rejected, which the \
        work query therefore skips. Read-only by design — a row is cleared when the file's \
        content changes, when a missing dependency appears, or by a shipped retry directive, \
        never by an API call. Newest first, paginated with limit/offset against `total`.",
    params(DbQueryParams, ExtractionFailuresQuery),
    responses(
        (status = 200, description = "Recorded extraction failures", body = ExtractionFailuresResponse)
    )
)]
pub(crate) async fn get_extraction_failures(
    Query(query): Query<ExtractionFailuresQuery>,
    mut conn: DbConnection<ReadOnly>,
) -> Result<Json<ExtractionFailuresResponse>, ApiError> {
    let filters = ExtractionErrorFilters {
        setter: query.setter,
        error_class: validate_error_class(query.error_class)?,
        stage: query.stage,
        mime_prefix: query.mime_prefix,
        limit: query.limit,
        offset: query.offset.unwrap_or(0),
    };
    // Count first, then the page: a row written between the two shows up as a
    // total one larger than the page can explain, which is the harmless
    // direction. The reverse would page past a total that no longer covers it.
    let total = count_extraction_errors(&mut conn.conn, &filters).await?;
    let rows = list_extraction_errors(&mut conn.conn, &filters).await?;
    Ok(Json(ExtractionFailuresResponse {
        total,
        failures: rows
            .into_iter()
            .map(|row| ExtractionFailure {
                id: row.id,
                sha256: row.item_sha256,
                path: row.path,
                mime_type: row.mime_type,
                setter_name: row.setter_name,
                stage: row.stage,
                error_class: row.error_class,
                blocker: row.blocker,
                error: row.error,
                active: row.attempts >= row.skip_after,
                skip_after: row.skip_after,
                attempts: row.attempts,
                last_job_id: row.last_job_id,
                first_seen: row.first_seen,
                last_seen: row.last_seen,
            })
            .collect(),
    }))
}

#[utoipa::path(
    get,
    operation_id = "get_scan_failures",
    path = "/api/jobs/scan/failures",
    tag = "jobs",
    summary = "List recorded file scan failures",
    description = "The filescan failure ledger: paths the scan could not get as far as an item \
        for. A confirmed row (`active`) is skipped only while the file still has the mtime and \
        size the failure was recorded against, so a repaired or modified file is re-attempted on \
        the next scan regardless. Read-only by design — a row is cleared when the file's mtime or \
        size changes, when the path stops being walked, when a missing dependency appears, or by \
        a shipped retry directive. Newest first, paginated with limit/offset against `total`.",
    params(DbQueryParams, ScanFailuresQuery),
    responses(
        (status = 200, description = "Recorded scan failures", body = ScanFailuresResponse)
    )
)]
pub(crate) async fn get_scan_failures(
    Query(query): Query<ScanFailuresQuery>,
    mut conn: DbConnection<ReadOnly>,
) -> Result<Json<ScanFailuresResponse>, ApiError> {
    let filters = ScanErrorFilters {
        error_class: validate_error_class(query.error_class)?,
        stage: query.stage,
        mime_prefix: query.mime_prefix,
        limit: query.limit,
        offset: query.offset.unwrap_or(0),
    };
    let total = count_scan_errors(&mut conn.conn, &filters).await?;
    let rows = list_scan_errors(&mut conn.conn, &filters).await?;
    Ok(Json(ScanFailuresResponse {
        total,
        failures: rows
            .into_iter()
            .map(|row| ScanFailure {
                id: row.id,
                path: row.path,
                stage: row.stage,
                error_class: row.error_class,
                blocker: row.blocker,
                mime_type: row.mime_type,
                error: row.error,
                active: row.attempts >= row.skip_after,
                skip_after: row.skip_after,
                attempts: row.attempts,
                last_scan_id: row.last_scan_id,
                first_seen: row.first_seen,
                last_seen: row.last_seen,
            })
            .collect(),
    }))
}

#[derive(Debug, serde::Serialize, ToSchema)]
pub(crate) struct VectorQuantActionResponse {
    pub detail: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct VectorQuantRebuildRequest {
    /// The quant profile name to rebuild.
    pub profile: String,
    /// A setter of the embedding space to rebuild; xmodal siblings rebuild
    /// together.
    pub setter_name: String,
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct VectorQuantStatusQuery {
    /// Include per-setter vector/quantized counts (progress and size on
    /// disk). These are full index scans over each setter's rows; pass
    /// false from latency-sensitive surfaces that only need profile names
    /// and states. Defaults to true.
    #[serde(default = "default_true")]
    counts: bool,
}

fn default_true() -> bool {
    true
}

#[utoipa::path(
    get,
    operation_id = "get_vector_quants",
    path = "/api/jobs/quants",
    tag = "jobs",
    summary = "Get vector quantization status",
    description = "Desired (config.toml) merged with actual (DB) state of the vector quant profiles: per-profile setters coverage, build progress, size on disk and whether a reconcile is needed.",
    params(DbQueryParams, VectorQuantStatusQuery),
    responses(
        (status = 200, description = "Vector quantization status", body = VectorQuantStatus)
    )
)]
pub(crate) async fn get_vector_quants(
    mut conn: DbConnection<ReadOnly>,
    Query(params): Query<VectorQuantStatusQuery>,
) -> Result<Json<VectorQuantStatus>, ApiError> {
    // Invalid config is inert everywhere else (no reconcile action); here it
    // is worth surfacing, since the card is exactly where the user would fix
    // it.
    let desired = crate::db::vector_quants::load_desired_state(&conn.index_db).ok_or_else(|| {
        ApiError::bad_request(
            "The [vector_quants] section of this database's config.toml is invalid; \
             fix it to manage quant profiles.",
        )
    })?;
    // Drift alone doesn't mean the user has to do anything: every action that
    // creates drift also enqueues the reconcile that resolves it. Report the
    // in-flight job so the card can say "converging" instead of "act now".
    //
    // The two reads can't be taken atomically, so bracket the DB read with
    // them and take either. Sampling the queue only afterwards makes the
    // one failure that matters: a job that finishes *during* the DB read
    // leaves drift in the snapshot and nothing in the queue, which is
    // exactly the "act now" banner flashing as the job completes. Bracketed,
    // the worst case is the harmless direction — one extra poll reading
    // "converging" after the work is already done.
    let pending_before = reconcile_job_pending(&conn.index_db).await?;
    let mut status =
        crate::db::vector_quants::load_status(&mut conn.conn, desired, params.counts).await?;
    status.reconcile_scheduled = pending_before || reconcile_job_pending(&conn.index_db).await?;
    Ok(Json(status))
}

#[utoipa::path(
    post,
    operation_id = "enqueue_vector_quant_reconcile",
    path = "/api/jobs/quants/reconcile",
    tag = "jobs",
    summary = "Enqueue a vector quant reconcile job",
    description = "Enqueues a reconcile job for the selected database (deduplicated: no-op when one is already queued or running). The job is stateless and converges the DB to the configured desired state.",
    params(DbQueryParams),
    responses(
        (status = 200, description = "Reconcile triggered", body = VectorQuantActionResponse)
    )
)]
pub(crate) async fn enqueue_vector_quant_reconcile(
    conn: DbConnection<ReadOnly>,
) -> Result<Json<VectorQuantActionResponse>, ApiError> {
    let detail = enqueue_reconcile_deduped(&conn.index_db, &conn.user_data_db).await?;
    Ok(Json(VectorQuantActionResponse { detail }))
}

#[utoipa::path(
    post,
    operation_id = "rebuild_vector_quant_pair",
    path = "/api/jobs/quants/rebuild",
    tag = "jobs",
    summary = "Rebuild a quant profile's artifact for an embedding space",
    description = "Marks the embedding space containing the given setter for rebuild under the given profile (the int8 scale is recomputed and every code rewritten at a bumped revision) and enqueues a reconcile job. The affected setters search exact until the rebuild completes. Explicit user action by design — a recomputed scale invalidates every code already stored for the space, so search results move; that is never background-silent.",
    params(DbQueryParams),
    request_body(content = VectorQuantRebuildRequest, description = "The profile and setter to rebuild"),
    responses(
        (status = 200, description = "Rebuild scheduled", body = VectorQuantActionResponse)
    )
)]
pub(crate) async fn rebuild_vector_quant_pair(
    mut conn: DbConnection<ReadOnly>,
    Json(request): Json<VectorQuantRebuildRequest>,
) -> Result<Json<VectorQuantActionResponse>, ApiError> {
    let profile_id = crate::db::vector_quants::active_profile_id(&mut conn.conn, &request.profile)
        .await?
        .ok_or_else(|| {
            ApiError::bad_request(format!("Unknown vector quant profile: {}", request.profile))
        })?;
    let setter_ids =
        crate::db::vector_quants::space_setter_ids(&mut conn.conn, &request.setter_name).await?;
    if setter_ids.is_empty() {
        return Err(ApiError::bad_request(format!(
            "Setter has no embeddings: {}",
            request.setter_name
        )));
    }
    call_index_db_writer(&conn.index_db, |reply| {
        IndexDbWriterMessage::VectorQuantMarkRebuild {
            profile_id,
            setter_ids: setter_ids.clone(),
            reply,
        }
    })
    .await?;
    let detail = enqueue_reconcile_deduped(&conn.index_db, &conn.user_data_db).await?;
    Ok(Json(VectorQuantActionResponse {
        detail: format!("Rebuild marked. {detail}"),
    }))
}

/// True when a reconcile job for this index DB is queued or running — the
/// same condition `enqueue_reconcile_deduped` dedups on.
async fn reconcile_job_pending(index_db: &str) -> Result<bool, ApiError> {
    let queue = get_queue_status().await?;
    Ok(queue
        .queue
        .iter()
        .any(|job| job.index_db == index_db && job.tag.as_deref() == Some(RECONCILE_JOB_TAG)))
}

async fn enqueue_reconcile_deduped(index_db: &str, user_data_db: &str) -> Result<String, ApiError> {
    let request = JobRequest {
        job_type: JobType::VectorQuantReconcile,
        index_db: index_db.to_string(),
        user_data_db: user_data_db.to_string(),
        metadata: None,
        batch_size: None,
        threshold: None,
        log_id: None,
        tag: Some(RECONCILE_JOB_TAG.to_string()),
    };
    let dedup = BatchDedup {
        tag: RECONCILE_JOB_TAG.to_string(),
        index_db: index_db.to_string(),
    };
    let result = enqueue_jobs_with_dedup(vec![request], vec![dedup]).await?;
    if result.was_skipped(index_db) {
        Ok("A reconcile job for this database is already queued or running.".to_string())
    } else {
        Ok("Reconcile job enqueued.".to_string())
    }
}

#[utoipa::path(
    post,
    operation_id = "manual_trigger_cronjob",
    path = "/api/jobs/cronjob/run",
    tag = "jobs",
    summary = "Manually trigger a cronjob run",
    description = "Manually trigger the configured cronjob to run on the selected database.",
    params(DbQueryParams),
    responses(
        (status = 200, description = "Cronjob triggered", body = CronJobResponse)
    )
)]
pub(crate) async fn manual_trigger_cronjob(
    conn: DbConnection<ReadOnly>,
) -> Result<Json<CronJobResponse>, ApiError> {
    let detail = match cron::run_cronjob(&conn.index_db, &conn.user_data_db).await? {
        CronRunOutcome::Enqueued(_) => "Cronjob triggered.".to_string(),
        // Python also replies 200 here (the skip is silent); keep the status
        // code but say what happened.
        CronRunOutcome::Skipped => {
            "Cronjob skipped: a previous cronjob for this database is still queued or running."
                .to_string()
        }
    };
    Ok(Json(CronJobResponse { detail }))
}

#[derive(serde::Serialize, ToSchema)]
pub(crate) struct CronScheduleResponse {
    /// Whether automatic cron runs are enabled for this database.
    enabled: bool,
    /// The configured cron schedule string.
    cron_schedule: String,
    /// Whether the configured schedule string parses.
    valid: bool,
    /// Next automatic run (RFC 3339, local time), when scheduling is active.
    next_run: Option<String>,
    /// Last automatic run fired by this process (RFC 3339, local time).
    /// Manual triggers are not included.
    last_run: Option<String>,
}

#[utoipa::path(
    get,
    operation_id = "get_cronjob_schedule",
    path = "/api/jobs/cronjob/schedule",
    tag = "jobs",
    summary = "Get the cronjob schedule status",
    description = "Get the configured cron schedule for the selected database along with the next and last automatic run times.",
    params(DbQueryParams),
    responses(
        (status = 200, description = "Cronjob schedule status", body = CronScheduleResponse)
    )
)]
pub(crate) async fn get_cronjob_schedule(
    conn: DbConnection<ReadOnly>,
) -> Result<Json<CronScheduleResponse>, ApiError> {
    let store = SystemConfigStore::from_env();
    let config = store.load(&conn.index_db)?;
    let status = cron::get_schedule_status(&conn.index_db)
        .await
        .unwrap_or_default();
    Ok(Json(CronScheduleResponse {
        enabled: config.enable_cron_job,
        valid: cron::validate_cron_schedule(&config.cron_schedule).is_ok(),
        cron_schedule: config.cron_schedule,
        next_run: status.next_run.map(|time| time.to_rfc3339()),
        last_run: status.last_run.map(|time| time.to_rfc3339()),
    }))
}

/// Change-detection mode configured for the continuous filescan.
#[derive(serde::Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContinuousScanMode {
    /// Native OS filesystem watcher. Only reliable on local folders.
    Watcher,
    /// Recurring directory-mtime poller. Required for network mounts
    /// (NFS/SMB), where native watchers do not receive events.
    Poller,
}

#[derive(serde::Serialize, ToSchema)]
pub(crate) struct ContinuousScanStatusResponse {
    /// Whether continuous scanning is enabled in this database's config.
    enabled: bool,
    /// Whether the scanner is currently watching for changes. False when
    /// disabled, while paused for a running job, or when the configured
    /// watched folders produced no valid watch roots.
    active: bool,
    /// Whether the scanner is temporarily paused while a job runs on this
    /// database. It resumes automatically when the job finishes.
    paused_for_job: bool,
    /// Change-detection mode from the configuration. This is what was asked
    /// for, not necessarily what is running — see `watcher_fallback`.
    mode: ContinuousScanMode,
    /// True when `mode` is `watcher` but the OS watcher could not be started,
    /// so polling is standing in for it. The usual cause is the system's limit
    /// on watched paths being too low for the size of the watched tree.
    watcher_fallback: bool,
    /// Poll interval actually in effect, including when `watcher_fallback` is
    /// set. Null in watcher mode.
    poll_interval_secs: Option<u64>,
    /// The folder roots being watched for changes (the global included
    /// folders when no continuous watched folders are configured).
    watch_roots: Vec<String>,
    /// Configured watched folders that were rejected because they are not
    /// inside an included folder or fall under an excluded folder.
    invalid_includes: Vec<String>,
    /// False when every configured watched folder was rejected; continuous
    /// scanning is inactive in that case even when enabled.
    roots_valid: bool,
}

#[utoipa::path(
    get,
    operation_id = "get_continuous_scan_status",
    path = "/api/jobs/continuous/status",
    tag = "jobs",
    summary = "Get the continuous filescan status",
    description = "Report the live state of the continuous filescan for the selected database: \
        whether it is enabled and actively watching, the change-detection mode in effect, the \
        effective watch roots, and any configured watched folders that were rejected.",
    params(DbQueryParams),
    responses(
        (status = 200, description = "Continuous filescan status", body = ContinuousScanStatusResponse)
    )
)]
pub(crate) async fn get_continuous_scan_status(
    conn: DbConnection<ReadOnly>,
) -> Result<Json<ContinuousScanStatusResponse>, ApiError> {
    let store = SystemConfigStore::from_env();
    let config = store.load(&conn.index_db)?;
    let poll_interval_secs = config
        .continuous_filescan
        .poll_interval_secs
        .filter(|secs| *secs > 0);
    let mode = match poll_interval_secs {
        Some(_) => ContinuousScanMode::Poller,
        None => ContinuousScanMode::Watcher,
    };
    let snapshot = continuous_scan::get_scan_status(&conn.index_db).await?;
    let response = match snapshot {
        Some(snapshot) => ContinuousScanStatusResponse {
            enabled: config.continuous_filescan.enabled,
            // Not merely "unpaused": a watcher that failed to start leaves the
            // actor unpaused with no change detection running at all.
            active: !snapshot.paused && snapshot.watching,
            paused_for_job: snapshot.paused_for_job,
            mode,
            watcher_fallback: snapshot.watcher_fallback,
            // Prefer the interval actually running, so a fallback poller
            // reports its own interval rather than the configured null.
            poll_interval_secs: snapshot.effective_poll_interval_secs.or(poll_interval_secs),
            watch_roots: snapshot.watch_roots,
            invalid_includes: snapshot.invalid_includes,
            roots_valid: snapshot.roots_valid,
        },
        // No scanner actor: evaluate the configured roots directly so the UI
        // still gets validation feedback while scanning is disabled.
        None => {
            let outcome = continuous_scan::compute_watch_roots(&config);
            ContinuousScanStatusResponse {
                enabled: config.continuous_filescan.enabled,
                active: false,
                paused_for_job: false,
                mode,
                watcher_fallback: false,
                poll_interval_secs,
                watch_roots: outcome
                    .watch_roots
                    .iter()
                    .map(|root| root.to_string_lossy().to_string())
                    .collect(),
                invalid_includes: outcome.invalid_includes,
                roots_valid: outcome.valid,
            }
        }
    };
    Ok(Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Uri;

    /// The UI sends list params FastAPI-style
    /// (?inference_ids=a&inference_ids=b). Plain axum::extract::Query
    /// (serde_urlencoded) rejects repeated keys into a Vec with a 400, so
    /// these structs must go through axum-extra's Query.
    #[test]
    fn repeated_query_params_parse_into_vecs() {
        let uri: Uri = "/api/jobs/data/extraction\
            ?inference_ids=tags/wd-swinv2-tagger-v3\
            &inference_ids=clip/ViT-H-14-378-quickgelu_dfn5b\
            &batch_size=64"
            .parse()
            .unwrap();
        let Query(q) = Query::<InferenceQuery>::try_from_uri(&uri).unwrap();
        assert_eq!(
            q.inference_ids,
            vec![
                "tags/wd-swinv2-tagger-v3",
                "clip/ViT-H-14-378-quickgelu_dfn5b"
            ]
        );
        assert_eq!(q.batch_size, Some(64));
        assert_eq!(q.threshold, None);

        let uri: Uri = "/x?log_ids=1&log_ids=2".parse().unwrap();
        let Query(q) = Query::<LogIdQuery>::try_from_uri(&uri).unwrap();
        assert_eq!(q.log_ids, vec![1, 2]);

        let uri: Uri = "/x?queue_ids=3".parse().unwrap();
        let Query(q) = Query::<QueueCancelQuery>::try_from_uri(&uri).unwrap();
        assert_eq!(q.queue_ids, vec![3]);
    }

    /// Every failures filter is optional, so a bare request must parse into
    /// all-None rather than 400 — that is the request the UI opens the card
    /// with.
    #[test]
    fn failures_query_filters_are_all_optional() {
        let uri: Uri = "/api/jobs/data/failures".parse().unwrap();
        let Query(q) = Query::<ExtractionFailuresQuery>::try_from_uri(&uri).unwrap();
        assert!(q.setter.is_none() && q.error_class.is_none() && q.stage.is_none());
        assert!(q.mime_prefix.is_none() && q.limit.is_none() && q.offset.is_none());

        let uri: Uri = "/api/jobs/data/failures?setter=clip/ViT-H-14&error_class=blocked\
            &stage=prepare&mime_prefix=image/&limit=25&offset=50"
            .parse()
            .unwrap();
        let Query(q) = Query::<ExtractionFailuresQuery>::try_from_uri(&uri).unwrap();
        assert_eq!(q.setter.as_deref(), Some("clip/ViT-H-14"));
        assert_eq!(q.stage.as_deref(), Some("prepare"));
        assert_eq!(q.mime_prefix.as_deref(), Some("image/"));
        assert_eq!((q.limit, q.offset), (Some(25), Some(50)));
        assert_eq!(
            validate_error_class(q.error_class).unwrap().as_deref(),
            Some("blocked")
        );

        // The scan ledger has its own query type on purpose: it predates every
        // setter, so it must not document a `setter` filter it would ignore.
        let uri: Uri = "/api/jobs/scan/failures?stage=decode&mime_prefix=video/&limit=10"
            .parse()
            .unwrap();
        let Query(q) = Query::<ScanFailuresQuery>::try_from_uri(&uri).unwrap();
        assert_eq!(q.stage.as_deref(), Some("decode"));
        assert_eq!(q.mime_prefix.as_deref(), Some("video/"));
        assert_eq!((q.limit, q.offset), (Some(10), None));
    }

    /// A mistyped class must not answer "no recorded failures" — on an audit
    /// surface that reads as "nothing is wrong".
    #[test]
    fn an_unknown_error_class_is_rejected_not_silently_empty() {
        assert!(validate_error_class(None).unwrap().is_none());
        for class in ERROR_CLASSES {
            assert_eq!(
                validate_error_class(Some(class.to_string()))
                    .unwrap()
                    .as_deref(),
                Some(class)
            );
        }
        // Case-sensitive: the persisted values are data, not display strings.
        let error = validate_error_class(Some("Input".to_string())).unwrap_err();
        assert!(
            error.detail().contains("Unknown error_class"),
            "unexpected detail: {}",
            error.detail()
        );
        assert!(validate_error_class(Some(String::new())).is_err());
    }

    /// What the caller actually receives for a mistyped class. There is no
    /// handler harness here — both failure handlers take a `DbConnection`
    /// extractor — so this pins the rejection at the only layer that decides
    /// it: status 400 and the flat `{"detail": ...}` body every other endpoint
    /// returns, which is what the UI's error path reads.
    #[tokio::test]
    async fn a_rejected_error_class_is_a_400_with_a_detail_body() {
        use axum::body::to_bytes;
        use axum::response::IntoResponse;

        let response = validate_error_class(Some("Input".to_string()))
            .unwrap_err()
            .into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let parsed: JsonValue = serde_json::from_slice(&body).unwrap();
        let detail = parsed
            .get("detail")
            .and_then(JsonValue::as_str)
            .expect("the body must be {\"detail\": ...}");
        assert!(
            detail.contains("Unknown error_class") && detail.contains("\"Input\""),
            "the message must name what was rejected: {detail}"
        );
        // The vocabulary is offered back, so the caller can fix the typo.
        for class in ERROR_CLASSES {
            assert!(detail.contains(class), "{class} missing from: {detail}");
        }
    }

    /// An item whose files have all gone away still owes an audit row — the
    /// row is exactly what explains why nothing was extracted — so `path` has
    /// to serialize as an explicit JSON null rather than being skipped. A
    /// missing key would make the generated client's `string | undefined`
    /// disagree with the `string | null` the schema promises.
    #[test]
    fn a_fileless_item_serializes_an_explicit_null_path() {
        let failure = ExtractionFailure {
            id: 1,
            sha256: "sha_one".to_string(),
            path: None,
            mime_type: "image/png".to_string(),
            setter_name: "test/clip".to_string(),
            stage: "prepare".to_string(),
            error_class: "input".to_string(),
            blocker: None,
            error: "decode failed".to_string(),
            skip_after: 1,
            attempts: 1,
            active: true,
            last_job_id: None,
            first_seen: "2026-01-01T00:00:00".to_string(),
            last_seen: "2026-01-01T00:00:00".to_string(),
        };
        let json: JsonValue = serde_json::to_value(&failure).unwrap();
        assert_eq!(json.get("path"), Some(&JsonValue::Null));
        assert_eq!(json.get("blocker"), Some(&JsonValue::Null));
        assert_eq!(json.get("last_job_id"), Some(&JsonValue::Null));
        assert_eq!(json["sha256"], JsonValue::from("sha_one"));
    }
}
