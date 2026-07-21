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
use crate::db::extraction_log::{LogRecord, get_all_data_logs, get_setters_total_data};
use crate::db::file_scans::get_all_file_scans;
use crate::db::folders::get_folders_from_database;
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
    cancel_running_job, enqueue_job, enqueue_jobs_unless_tagged, get_queue_status,
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
    let cancelled = cancel_queued_jobs(query.queue_ids).await?;
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
    responses(
        (status = 200, description = "Running job cancelled", body = CancelResponse)
    )
)]
pub(crate) async fn cancel_current_job() -> Result<Json<CancelResponse>, ApiError> {
    let cancelled = cancel_running_job().await?;
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
    // Reject invalid [vector_quants] at save time; the load-time paths
    // treat an invalid section as empty, which would silently remove
    // profiles.
    if let Some(quants) = &config.vector_quants
        && let Err(message) = crate::db::vector_quants::resolve_desired(quants)
    {
        return Err(ApiError::bad_request(message));
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
    description = "Marks the embedding space containing the given setter for rebuild under the given profile (artifact recomputed at a bumped revision) and enqueues a reconcile job. The affected setters search exact until the rebuild completes. Explicit user action by design — artifact recomputation reshuffles coarse order and is never background-silent.",
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
    match enqueue_jobs_unless_tagged(vec![request], Some(dedup)).await? {
        Some(_) => Ok("Reconcile job enqueued.".to_string()),
        None => {
            Ok("A reconcile job for this database is already queued or running.".to_string())
        }
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
}
