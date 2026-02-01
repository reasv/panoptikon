use axum::{Json, extract::Query, http::StatusCode};
use serde::Deserialize;
use utoipa::{IntoParams, ToSchema};

use crate::api_error::ApiError;
use crate::db::extraction_log::{LogRecord, get_all_data_logs, get_setters_total_data};
use crate::db::file_scans::get_all_file_scans;
use crate::db::folders::get_folders_from_database;
use crate::db::system_config::{SystemConfig, SystemConfigStore};
use crate::db::{DbConnection, ReadOnly};
use crate::jobs::continuous_scan;
use crate::jobs::files::is_resync_needed;
use crate::jobs::queue::{
    JobModel, JobRequest, JobType, QueueStatusModel, cancel_queued_jobs, cancel_running_job,
    enqueue_job, get_queue_status,
};

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct InferenceQuery {
    /// Inference ID List
    inference_ids: Vec<String>,
    /// Batch Size
    batch_size: Option<i64>,
    /// Confidence Threshold
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
    path = "/api/jobs/data/extraction",
    tag = "jobs",
    summary = "Run a data extraction job",
    params(InferenceQuery),
    responses(
        (status = 202, description = "Enqueued data extraction jobs", body = [JobModel])
    )
)]
pub(crate) async fn enqueue_data_extraction(
    Query(query): Query<InferenceQuery>,
    conn: DbConnection<ReadOnly>,
) -> Result<(StatusCode, Json<Vec<JobModel>>), ApiError> {
    let mut jobs = Vec::new();
    for inference_id in query.inference_ids {
        let job = enqueue_job(JobRequest {
            job_type: JobType::DataExtraction,
            index_db: conn.index_db.clone(),
            user_data_db: conn.user_data_db.clone(),
            metadata: Some(inference_id),
            batch_size: query.batch_size,
            threshold: query.threshold,
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
    path = "/api/jobs/data/extraction",
    tag = "jobs",
    summary = "Delete extracted data",
    params(InferenceQuery),
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
    path = "/api/jobs/folders/rescan",
    tag = "jobs",
    summary = "Run a folder rescan",
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
    path = "/api/jobs/folders",
    tag = "jobs",
    summary = "Update the database with the current folder lists in the config",
    description = "Must be run every time after the folder lists in the config are updated,\nto ensure that the database is in sync with the config.\nIf you update the config through the API, this will be done automatically if needed.\n\nThis will remove files and items from the database that are no longer in the included folders,\nand add files and items that are now in the included folders, as well as remove files and items\nfrom the database that are now in the excluded folders.",
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
    path = "/api/jobs/folders",
    tag = "jobs",
    summary = "Get the current folder lists",
    description = "Get the current included and excluded folders in the database.\nThese are the folders that are being scanned and not being scanned, respectively.\n\nThis list may differ from the config, if the database has not been updated.",
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
    path = "/api/jobs/folders/history",
    tag = "jobs",
    summary = "Get the scan history",
    params(HistoryQuery),
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
    path = "/api/jobs/data/history",
    tag = "jobs",
    summary = "Deletes data generated by the scans given log ids",
    params(LogIdQuery),
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
    path = "/api/jobs/data/history",
    tag = "jobs",
    summary = "Get the extraction history",
    params(HistoryQuery),
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
    path = "/api/jobs/config",
    tag = "jobs",
    summary = "Update the system configuration",
    request_body(content = SystemConfig, description = "The new system configuration"),
    responses(
        (status = 200, description = "Updated system configuration", body = SystemConfig)
    )
)]
pub(crate) async fn update_config(
    conn: DbConnection<ReadOnly>,
    Json(config): Json<SystemConfig>,
) -> Result<Json<SystemConfig>, ApiError> {
    let store = SystemConfigStore::from_env();
    store.save(&conn.index_db, &config)?;
    let config = store.load(&conn.index_db)?;
    let _ = continuous_scan::notify_config_change(&conn.index_db).await;
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

#[utoipa::path(
    get,
    path = "/api/jobs/config",
    tag = "jobs",
    summary = "Get the current system configuration",
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
    path = "/api/jobs/data/setters/total",
    tag = "jobs",
    summary = "Get the total count of index data entry for each setter",
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

#[utoipa::path(
    post,
    path = "/api/jobs/cronjob/run",
    tag = "jobs",
    summary = "Manually trigger a cronjob run",
    description = "Manually trigger the configured cronjob to run on the selected database.",
    responses(
        (status = 200, description = "Cronjob triggered", body = CronJobResponse)
    )
)]
pub(crate) async fn manual_trigger_cronjob(
    conn: DbConnection<ReadOnly>,
) -> Result<Json<CronJobResponse>, ApiError> {
    let store = SystemConfigStore::from_env();
    let config = store.load(&conn.index_db)?;
    for job in config.cron_jobs {
        let _ = enqueue_job(JobRequest {
            job_type: JobType::DataExtraction,
            index_db: conn.index_db.clone(),
            user_data_db: conn.user_data_db.clone(),
            metadata: Some(job.inference_id),
            batch_size: job.batch_size,
            threshold: job.threshold,
            log_id: None,
            tag: None,
        })
        .await?;
    }
    Ok(Json(CronJobResponse {
        detail: "Cronjob triggered.".to_string(),
    }))
}
