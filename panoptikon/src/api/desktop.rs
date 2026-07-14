//! Desktop-only lifecycle status. This route is mounted only for a
//! `--desktop-managed` sidecar and carries no general host privilege.

use axum::Json;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    api_error::ApiError,
    db::{
        DbConnection, ReadOnly,
        migrations::migrate_databases_on_disk,
        open_index_db_read,
        setup::{
            FolderValidation, is_ready_for_desktop, validate_continuous_folders, validate_folders,
        },
        system_config::SystemConfigStore,
    },
    jobs::{
        continuous_scan,
        queue::{JobModel, JobRequest, JobType, enqueue_job},
    },
};

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct DesktopSetupStatus {
    /// The policy-resolved default index database used for this request.
    pub index_db: String,
    /// True once a current included folder has a corresponding filescan row.
    pub ready: bool,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct DesktopFolderSelection {
    pub included_folders: Vec<String>,
    #[serde(default)]
    pub excluded_folders: Vec<String>,
    /// A new database has no indexed rows, so empty folders are safe.
    #[serde(default)]
    pub new_database: bool,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct DesktopSetupCompleteRequest {
    pub included_folders: Vec<String>,
    #[serde(default)]
    pub excluded_folders: Vec<String>,
    #[serde(default)]
    pub continuous_filescan_enabled: bool,
    #[serde(default)]
    pub continuous_filescan_poll_interval_secs: Option<u64>,
    #[serde(default)]
    pub continuous_filescan_included_folders: Vec<String>,
    /// When present, create and configure this index instead of the default.
    pub new_index_db: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct DesktopContinuousScanSelection {
    pub included_folders: Vec<String>,
    #[serde(default)]
    pub excluded_folders: Vec<String>,
    #[serde(default)]
    pub continuous_folders: Vec<String>,
    /// A new database has no indexed rows, so empty folders are safe.
    #[serde(default)]
    pub new_database: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct DesktopSetupCompleteResponse {
    pub index_db: String,
    pub job: JobModel,
}

fn ensure_desktop_managed() -> Result<(), ApiError> {
    if crate::desktop::is_managed() {
        Ok(())
    } else {
        Err(ApiError::not_found("Desktop lifecycle endpoint not found"))
    }
}

#[utoipa::path(
    get,
    operation_id = "desktop_setup_status",
    path = "/api/desktop/setup-status",
    tag = "desktop",
    params(crate::api::db_params::DbQueryParams),
    responses((status = 200, body = DesktopSetupStatus))
)]
pub(crate) async fn setup_status(
    mut conn: DbConnection<ReadOnly>,
) -> Result<Json<DesktopSetupStatus>, ApiError> {
    ensure_desktop_managed()?;
    let ready = is_ready_for_desktop(&mut conn.conn).await?;
    Ok(Json(DesktopSetupStatus {
        index_db: conn.index_db,
        ready,
    }))
}

#[utoipa::path(
    post,
    operation_id = "desktop_validate_setup_folders",
    path = "/api/desktop/setup-folders/validate",
    tag = "desktop",
    params(crate::api::db_params::DbQueryParams),
    request_body = DesktopFolderSelection,
    responses((status = 200, body = FolderValidation))
)]
pub(crate) async fn validate_setup_folders(
    mut conn: DbConnection<ReadOnly>,
    Json(request): Json<DesktopFolderSelection>,
) -> Result<Json<FolderValidation>, ApiError> {
    ensure_desktop_managed()?;
    let database = (!request.new_database).then_some(&mut conn.conn);
    Ok(Json(
        validate_folders(
            database,
            &request.included_folders,
            &request.excluded_folders,
        )
        .await?,
    ))
}

#[utoipa::path(
    post,
    operation_id = "desktop_validate_setup_continuous_folders",
    path = "/api/desktop/setup-continuous/validate",
    tag = "desktop",
    params(crate::api::db_params::DbQueryParams),
    request_body = DesktopContinuousScanSelection,
    responses((status = 200, body = FolderValidation))
)]
pub(crate) async fn validate_setup_continuous_folders(
    mut conn: DbConnection<ReadOnly>,
    Json(request): Json<DesktopContinuousScanSelection>,
) -> Result<Json<FolderValidation>, ApiError> {
    ensure_desktop_managed()?;
    let database = (!request.new_database).then_some(&mut conn.conn);
    Ok(Json(
        validate_continuous_folders(
            database,
            &request.included_folders,
            &request.excluded_folders,
            &request.continuous_folders,
        )
        .await?,
    ))
}

fn validate_new_database_name(name: &str) -> Result<(), ApiError> {
    if !(3..=32).contains(&name.len())
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(ApiError::bad_request(
            "Database names must contain 3–32 letters, numbers, or underscores",
        ));
    }
    let info = crate::db::info::load_db_info().map_err(|error| {
        tracing::error!(%error, "failed to list databases before Desktop setup");
        ApiError::internal("Failed to inspect existing databases")
    })?;
    if info
        .index
        .all
        .iter()
        .any(|existing| existing.eq_ignore_ascii_case(name))
    {
        return Err(ApiError::bad_request(format!(
            "Index database {name} already exists"
        )));
    }
    Ok(())
}

#[utoipa::path(
    post,
    operation_id = "complete_desktop_setup",
    path = "/api/desktop/setup/complete",
    tag = "desktop",
    params(crate::api::db_params::DbQueryParams),
    request_body = DesktopSetupCompleteRequest,
    responses((status = 200, body = DesktopSetupCompleteResponse))
)]
pub(crate) async fn complete_setup(
    mut conn: DbConnection<ReadOnly>,
    Json(request): Json<DesktopSetupCompleteRequest>,
) -> Result<Json<DesktopSetupCompleteResponse>, ApiError> {
    ensure_desktop_managed()?;
    if request
        .included_folders
        .iter()
        .all(|path| path.trim().is_empty())
    {
        return Err(ApiError::bad_request(
            "At least one included directory is required",
        ));
    }
    if request.continuous_filescan_poll_interval_secs == Some(0) {
        return Err(ApiError::bad_request(
            "The continuous-scan polling interval must be at least one second",
        ));
    }

    let database = request.new_index_db.is_none().then_some(&mut conn.conn);
    let validation = validate_folders(
        database,
        &request.included_folders,
        &request.excluded_folders,
    )
    .await?;
    if let Some(issue) = validation.errors.first() {
        return Err(ApiError::bad_request(format!(
            "{}: {}",
            issue.path, issue.error
        )));
    }
    let database = request.new_index_db.is_none().then_some(&mut conn.conn);
    let continuous_validation = validate_continuous_folders(
        database,
        &validation.included_folders,
        &validation.excluded_folders,
        &request.continuous_filescan_included_folders,
    )
    .await?;
    if let Some(issue) = continuous_validation.errors.first() {
        return Err(ApiError::bad_request(format!(
            "{}: {}",
            issue.path, issue.error
        )));
    }

    let (index_db, user_data_db) = if let Some(new_index_db) = request.new_index_db.as_deref() {
        validate_new_database_name(new_index_db)?;
        let new_index_db = new_index_db.to_owned();
        let selected_user_data_db = conn.user_data_db.clone();
        let handle = tokio::runtime::Handle::current();
        let paths = tokio::task::spawn_blocking(move || {
            handle.block_on(migrate_databases_on_disk(
                Some(&new_index_db),
                Some(&selected_user_data_db),
            ))
        })
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to join Desktop database creation task");
            ApiError::internal("Failed to create index database")
        })?
        .map_err(|error| {
            tracing::error!(%error, "failed to create Desktop index database");
            ApiError::internal("Failed to create index database")
        })?;
        (paths.index_db, paths.user_data_db)
    } else {
        (conn.index_db, conn.user_data_db)
    };

    // Recheck empty-folder safety against the actual target database. For a
    // newly created database this is cheap and necessarily has no file rows.
    let mut target = open_index_db_read(&index_db, &user_data_db).await?;
    let validation = validate_folders(
        Some(&mut target),
        &validation.included_folders,
        &validation.excluded_folders,
    )
    .await?;
    if let Some(issue) = validation.errors.first() {
        return Err(ApiError::bad_request(format!(
            "{}: {}",
            issue.path, issue.error
        )));
    }
    let continuous_validation = validate_continuous_folders(
        Some(&mut target),
        &validation.included_folders,
        &validation.excluded_folders,
        &continuous_validation.included_folders,
    )
    .await?;
    if let Some(issue) = continuous_validation.errors.first() {
        return Err(ApiError::bad_request(format!(
            "{}: {}",
            issue.path, issue.error
        )));
    }
    drop(target);

    let store = SystemConfigStore::from_env();
    let mut config = store.load(&index_db)?;
    config.included_folders = validation.included_folders;
    config.excluded_folders = validation.excluded_folders;
    config.continuous_filescan.enabled = request.continuous_filescan_enabled;
    config.continuous_filescan.poll_interval_secs = request.continuous_filescan_poll_interval_secs;
    config.continuous_filescan.included_folders = continuous_validation.included_folders;
    store.save(&index_db, &config)?;
    let _ = continuous_scan::notify_config_change(&index_db).await;
    let job = enqueue_job(JobRequest {
        job_type: JobType::FolderUpdate,
        index_db: index_db.clone(),
        user_data_db,
        metadata: None,
        batch_size: None,
        threshold: None,
        log_id: None,
        tag: None,
    })
    .await?;

    Ok(Json(DesktopSetupCompleteResponse { index_db, job }))
}
