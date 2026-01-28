use axum::{Json, http::StatusCode, extract::Query};
use serde::Deserialize;

use crate::api_error::ApiError;
use crate::db::{DbConnection, ReadOnly};
use crate::db::file_scans::get_all_file_scans;
use crate::db::folders::get_folders_from_database;
use crate::db::extraction_log::{get_all_data_logs, get_setters_total_data, LogRecord};
use crate::db::system_config::{SystemConfig, SystemConfigStore};
use crate::jobs::files::is_resync_needed;
use crate::jobs::continuous_scan;
use crate::jobs::queue::{
    JobModel,
    JobRequest,
    JobType,
    QueueStatusModel,
    cancel_queued_jobs,
    cancel_running_job,
    enqueue_job,
    get_queue_status,
};

#[derive(Deserialize)]
pub(crate) struct InferenceQuery {
    inference_ids: Vec<String>,
    batch_size: Option<i64>,
    threshold: Option<f64>,
}

#[derive(Deserialize)]
pub(crate) struct LogIdQuery {
    log_ids: Vec<i64>,
}

#[derive(Deserialize)]
pub(crate) struct QueueCancelQuery {
    queue_ids: Vec<i64>,
}

#[derive(Deserialize)]
pub(crate) struct HistoryQuery {
    page: Option<i64>,
    page_size: Option<i64>,
}

#[derive(serde::Serialize)]
pub(crate) struct QueueCancelResponse {
    cancelled_jobs: Vec<i64>,
}

#[derive(serde::Serialize)]
pub(crate) struct CancelResponse {
    detail: String,
}

#[derive(serde::Serialize)]
pub(crate) struct FoldersResponse {
    included_folders: Vec<String>,
    excluded_folders: Vec<String>,
}

#[derive(serde::Serialize)]
pub(crate) struct SetterDataStats {
    total_counts: Vec<(String, i64)>,
}

#[derive(serde::Serialize)]
pub(crate) struct CronJobResponse {
    detail: String,
}

pub(crate) async fn queue_status() -> Result<Json<QueueStatusModel>, ApiError> {
    let status = get_queue_status().await?;
    Ok(Json(status))
}

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

pub(crate) async fn cancel_queued(
    Query(query): Query<QueueCancelQuery>,
) -> Result<Json<QueueCancelResponse>, ApiError> {
    let cancelled = cancel_queued_jobs(query.queue_ids).await?;
    if cancelled.is_empty() {
        return Err(ApiError::not_found("No matching queued jobs found."));
    }
    Ok(Json(QueueCancelResponse { cancelled_jobs: cancelled }))
}

pub(crate) async fn cancel_current_job() -> Result<Json<CancelResponse>, ApiError> {
    let cancelled = cancel_running_job().await?;
    let job_id = cancelled.ok_or_else(|| ApiError::not_found("No job is currently running."))?;
    Ok(Json(CancelResponse { detail: format!("Job {job_id} cancelled.") }))
}

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

pub(crate) async fn get_scan_history(
    Query(query): Query<HistoryQuery>,
    mut conn: DbConnection<ReadOnly>,
) -> Result<Json<Vec<crate::db::file_scans::FileScanRecord>>, ApiError> {
    let page = query.page.unwrap_or(1);
    let scans = get_all_file_scans(&mut conn.conn, page, query.page_size).await?;
    Ok(Json(scans))
}

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

pub(crate) async fn get_extraction_history(
    Query(query): Query<HistoryQuery>,
    mut conn: DbConnection<ReadOnly>,
) -> Result<Json<Vec<LogRecord>>, ApiError> {
    let page = query.page.unwrap_or(1);
    let logs = get_all_data_logs(&mut conn.conn, page, query.page_size).await?;
    Ok(Json(logs))
}

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

pub(crate) async fn get_config(
    conn: DbConnection<ReadOnly>,
) -> Result<Json<SystemConfig>, ApiError> {
    let store = SystemConfigStore::from_env();
    let config = store.load(&conn.index_db)?;
    Ok(Json(config))
}

pub(crate) async fn get_setter_data_count(
    mut conn: DbConnection<ReadOnly>,
) -> Result<Json<SetterDataStats>, ApiError> {
    let totals = get_setters_total_data(&mut conn.conn).await?;
    Ok(Json(SetterDataStats { total_counts: totals }))
}

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
    Ok(Json(CronJobResponse { detail: "Cronjob triggered.".to_string() }))
}
