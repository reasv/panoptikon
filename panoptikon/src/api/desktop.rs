//! Desktop-only lifecycle status. This route is mounted only for a
//! `--desktop-managed` sidecar and carries no general host privilege.

use std::{
    collections::{HashMap, HashSet},
    fs,
    io::Write as _,
    path::Path,
    sync::{Arc, OnceLock},
};

use axum::{
    Extension, Json,
    extract::{Path as AxumPath, State},
    http::{Method, StatusCode},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use tokio::sync::Mutex;
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
        system_config::{CronJob, SystemConfigStore},
    },
    jobs::{
        continuous_scan, cron, extraction::resolve_model_metadata,
        inference_pool::job_inference_context, queue::JobModel,
    },
    policy::PolicyContext,
    proxy::ProxyState,
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
    #[serde(default = "default_true")]
    pub scan_images: bool,
    #[serde(default = "default_true")]
    pub scan_video: bool,
    #[serde(default)]
    pub scan_audio: bool,
    #[serde(default)]
    pub scan_pdf: bool,
    #[serde(default)]
    pub scan_html: bool,
    #[serde(default)]
    pub cron_jobs: Vec<CronJob>,
    #[serde(default)]
    pub enable_cron_job: bool,
    #[serde(default = "default_cron_schedule")]
    pub cron_schedule: String,
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
    /// The immediate first run: full rescan followed by configured models.
    /// Empty only when an earlier cron-style run for this DB is still active.
    pub jobs: Vec<JobModel>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct DesktopSchedulePreviewRequest {
    pub cron_schedule: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct DesktopSchedulePreviewResponse {
    pub valid: bool,
    pub next_run: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct DesktopExternalInputUpdate {
    #[serde(default)]
    pub values: HashMap<String, String>,
    #[serde(default)]
    pub remove: Vec<String>,
}

static ENV_WRITE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// The in-process Inferio instance owned by this Desktop server, if enabled.
/// This is deliberately independent of the primary job/search upstream,
/// which may be remote in a supported mixed deployment.
#[derive(Clone)]
pub(crate) struct DesktopInferenceState(pub(crate) Option<Arc<crate::inferio::http::InferioState>>);

fn default_true() -> bool {
    true
}

fn default_cron_schedule() -> String {
    "0 3 * * *".into()
}

fn ensure_desktop_managed() -> Result<(), ApiError> {
    if crate::desktop::is_managed() {
        Ok(())
    } else {
        Err(ApiError::not_found("Desktop lifecycle endpoint not found"))
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct DesktopUpdateDismissRequest {
    pub version: String,
}

fn ensure_desktop_shell_policy(
    state: &ProxyState,
    context: &PolicyContext,
) -> Result<(String, String), ApiError> {
    ensure_desktop_managed()?;
    let allowed = state
        .settings
        .policies
        .iter()
        .find(|policy| policy.name == context.policy_name)
        .and_then(|policy| policy.client.get("desktop"))
        .and_then(JsonValue::as_bool)
        == Some(true);
    if !allowed {
        return Err(ApiError::not_found("Desktop shell endpoint not found"));
    }
    let url = std::env::var("PANOPTIKON_DESKTOP_BRIDGE_URL")
        .map_err(|_| ApiError::not_found("Desktop shell endpoint not found"))?;
    let token = std::env::var("PANOPTIKON_DESKTOP_BRIDGE_TOKEN")
        .map_err(|_| ApiError::not_found("Desktop shell endpoint not found"))?;
    Ok((url, token))
}

async fn desktop_bridge_request(
    state: &ProxyState,
    context: &PolicyContext,
    method: Method,
    path: &str,
    body: Option<JsonValue>,
) -> Result<reqwest::Response, ApiError> {
    let (base, token) = ensure_desktop_shell_policy(state, context)?;
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
        .map_err(|error| {
            tracing::error!(%error, "failed to build Desktop shell bridge client");
            ApiError::internal("Desktop shell is unavailable")
        })?;
    let mut request = client
        .request(method, format!("{base}{path}"))
        .bearer_auth(token);
    if let Some(body) = body {
        request = request.json(&body);
    }
    request.send().await.map_err(|error| {
        tracing::warn!(%error, "Desktop shell bridge request failed");
        ApiError::internal("Desktop shell is unavailable")
    })
}

#[utoipa::path(
    get,
    operation_id = "desktop_update_status",
    path = "/api/desktop/update-status",
    tag = "desktop",
    responses((status = 200, description = "Desktop update awareness state", body = JsonValue))
)]
pub(crate) async fn update_status(
    State(state): State<Arc<ProxyState>>,
    Extension(context): Extension<PolicyContext>,
) -> Result<Json<JsonValue>, ApiError> {
    let response = desktop_bridge_request(&state, &context, Method::GET, "/status", None).await?;
    if !response.status().is_success() {
        return Err(ApiError::new(
            response.status(),
            "Desktop shell rejected the request",
        ));
    }
    response.json().await.map(Json).map_err(|error| {
        tracing::warn!(%error, "Desktop shell returned invalid update status");
        ApiError::internal("Desktop shell returned invalid update status")
    })
}

async fn desktop_bridge_action(
    state: Arc<ProxyState>,
    context: PolicyContext,
    path: &'static str,
    body: Option<JsonValue>,
) -> Result<StatusCode, ApiError> {
    let response = desktop_bridge_request(&state, &context, Method::POST, path, body).await?;
    if response.status().is_success() {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::new(
            response.status(),
            "Desktop shell rejected the request",
        ))
    }
}

#[utoipa::path(post, operation_id = "open_desktop_update_window", path = "/api/desktop/update-window/open", tag = "desktop", responses((status = 204, description = "Update window opened")))]
pub(crate) async fn open_update_window(
    State(state): State<Arc<ProxyState>>,
    Extension(context): Extension<PolicyContext>,
) -> Result<StatusCode, ApiError> {
    desktop_bridge_action(state, context, "/open", None).await
}

#[utoipa::path(post, operation_id = "snooze_desktop_update_ribbon", path = "/api/desktop/update-ribbon/snooze", tag = "desktop", responses((status = 204, description = "Ribbon snoozed for 24 hours")))]
pub(crate) async fn snooze_update_ribbon(
    State(state): State<Arc<ProxyState>>,
    Extension(context): Extension<PolicyContext>,
) -> Result<StatusCode, ApiError> {
    desktop_bridge_action(state, context, "/snooze", None).await
}

#[utoipa::path(post, operation_id = "dismiss_desktop_update_ribbon", path = "/api/desktop/update-ribbon/dismiss", tag = "desktop", request_body = DesktopUpdateDismissRequest, responses((status = 204, description = "Ribbon dismissed for the selected version")))]
pub(crate) async fn dismiss_update_ribbon(
    State(state): State<Arc<ProxyState>>,
    Extension(context): Extension<PolicyContext>,
    Json(request): Json<DesktopUpdateDismissRequest>,
) -> Result<StatusCode, ApiError> {
    desktop_bridge_action(
        state,
        context,
        "/dismiss",
        Some(json!({ "version": request.version })),
    )
    .await
}

#[utoipa::path(
    get,
    operation_id = "desktop_external_inputs",
    path = "/api/desktop/external-inputs",
    tag = "desktop",
    responses((status = 200, description = "Declared external inputs, presence, and editable non-secret values", body = JsonValue))
)]
pub(crate) async fn external_inputs(
    Extension(inference): Extension<DesktopInferenceState>,
) -> Result<Json<JsonValue>, ApiError> {
    ensure_desktop_managed()?;
    let value = desktop_external_input_registry(&inference).await?;
    let values = if inference.0.is_some() {
        let snapshot =
            crate::env_template::EnvironmentSnapshot::current(true).map_err(|error| {
                tracing::error!(%error, "failed to resolve Desktop managed .env");
                ApiError::internal("Failed to resolve Desktop inference configuration")
            })?;
        value
            .get("definitions")
            .and_then(JsonValue::as_object)
            .into_iter()
            .flatten()
            .filter_map(|(_, definition)| {
                if definition.get("secret").and_then(JsonValue::as_bool) == Some(true) {
                    return None;
                }
                let variable = definition.pointer("/source/variable")?.as_str()?;
                snapshot
                    .get(variable)
                    .map(|current| (variable.to_owned(), current.to_owned()))
            })
            .collect::<HashMap<_, _>>()
    } else {
        HashMap::new()
    };
    Ok(Json(json!({
        "managed": inference.0.is_some(),
        "registry": value,
        "values": values
    })))
}

async fn desktop_external_input_registry(
    inference: &DesktopInferenceState,
) -> Result<JsonValue, ApiError> {
    if let Some(local) = &inference.0 {
        return local.external_inputs_json().map_err(|error| {
            tracing::error!(%error, "failed to read local inference external inputs");
            ApiError::internal("Failed to read inference external inputs")
        });
    }
    job_inference_context()
        .primary
        .get_external_inputs()
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to read remote inference external inputs");
            ApiError::internal("Failed to read inference external inputs")
        })
}

#[utoipa::path(
    get,
    operation_id = "reveal_desktop_external_input",
    path = "/api/desktop/external-inputs/{variable}",
    tag = "desktop",
    params(("variable" = String, Path, description = "Declared environment-variable binding")),
    responses((status = 200, description = "Current value after an explicit reveal action", body = JsonValue))
)]
pub(crate) async fn reveal_external_input(
    Extension(inference): Extension<DesktopInferenceState>,
    AxumPath(variable): AxumPath<String>,
) -> Result<Json<JsonValue>, ApiError> {
    ensure_desktop_managed()?;
    if inference.0.is_none() {
        return Err(ApiError::bad_request(
            "External inputs are managed on the configured remote Inferio host",
        ));
    }
    let declared = desktop_external_input_registry(&inference).await?;
    let allowed = declared
        .get("definitions")
        .and_then(JsonValue::as_object)
        .into_iter()
        .flatten()
        .any(|(_, definition)| {
            definition
                .pointer("/source/variable")
                .and_then(JsonValue::as_str)
                == Some(variable.as_str())
        });
    if !allowed {
        return Err(ApiError::not_found("External input is not declared"));
    }
    let snapshot = crate::env_template::EnvironmentSnapshot::current(true).map_err(|error| {
        tracing::error!(%error, "failed to reveal Desktop managed external input");
        ApiError::internal("Failed to resolve Desktop inference configuration")
    })?;
    Ok(Json(json!({"value": snapshot.get(&variable)})))
}

#[utoipa::path(
    put,
    operation_id = "update_desktop_external_inputs",
    path = "/api/desktop/external-inputs",
    tag = "desktop",
    request_body = DesktopExternalInputUpdate,
    responses((status = 200, description = "Updated external-input status", body = JsonValue))
)]
pub(crate) async fn update_external_inputs(
    Extension(inference): Extension<DesktopInferenceState>,
    Json(mut request): Json<DesktopExternalInputUpdate>,
) -> Result<Json<JsonValue>, ApiError> {
    ensure_desktop_managed()?;
    if inference.0.is_none() {
        return Err(ApiError::bad_request(
            "External inputs are managed on the configured remote Inferio host",
        ));
    }
    let declared = desktop_external_input_registry(&inference).await?;
    let allowed = declared
        .get("definitions")
        .and_then(JsonValue::as_object)
        .into_iter()
        .flatten()
        .filter_map(|(_, definition)| {
            definition
                .pointer("/source/variable")
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
        })
        .collect::<HashSet<_>>();
    for variable in request.values.keys().chain(request.remove.iter()) {
        if !allowed.contains(variable) {
            return Err(ApiError::bad_request(format!(
                "Environment variable {variable} is not declared by the inference registry"
            )));
        }
    }

    // Empty edits mean "keep the current value". Removal is represented
    // exclusively by the explicit `remove` list.
    discard_empty_updates(&mut request.values);

    let _guard = ENV_WRITE_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    update_dotenv(Path::new(".env"), &request.values, &request.remove).map_err(|error| {
        tracing::error!(%error, "failed to update Desktop managed .env");
        ApiError::internal("Failed to update Desktop inference configuration")
    })?;
    external_inputs(Extension(inference)).await
}

fn discard_empty_updates(values: &mut HashMap<String, String>) {
    values.retain(|_, value| !value.is_empty());
}

fn update_dotenv(
    path: &Path,
    values: &HashMap<String, String>,
    remove: &[String],
) -> anyhow::Result<()> {
    let existing = match fs::read_to_string(path) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error.into()),
    };
    let remove = remove.iter().cloned().collect::<HashSet<_>>();
    let mut written = HashSet::new();
    let mut output = Vec::new();
    for line in existing.lines() {
        let Some(key) = dotenv_line_key(line) else {
            output.push(line.to_owned());
            continue;
        };
        if remove.contains(key) {
            continue;
        }
        if let Some(value) = values.get(key) {
            if written.insert(key.to_owned()) {
                output.push(format!("{key}={}", encode_dotenv_value(value)));
            }
        } else {
            output.push(line.to_owned());
        }
    }
    for (key, value) in values {
        if !written.contains(key) && !remove.contains(key) {
            output.push(format!("{key}={}", encode_dotenv_value(value)));
        }
    }
    let mut rendered = output.join("\n");
    if !rendered.is_empty() {
        rendered.push('\n');
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    temp.write_all(rendered.as_bytes())?;
    temp.as_file().sync_all()?;
    temp.persist(path).map_err(|error| error.error)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn dotenv_line_key(line: &str) -> Option<&str> {
    let line = line.trim_start();
    let line = line.strip_prefix("export ").unwrap_or(line).trim_start();
    let (key, _) = line.split_once('=')?;
    let key = key.trim();
    let mut chars = key.chars();
    let first = chars.next()?;
    ((first.is_ascii_alphabetic() || first == '_')
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_'))
    .then_some(key)
}

fn encode_dotenv_value(value: &str) -> String {
    format!(
        "\"{}\"",
        value
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n")
            .replace('\r', "\\r")
    )
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

#[utoipa::path(
    post,
    operation_id = "desktop_preview_setup_schedule",
    path = "/api/desktop/setup-schedule/preview",
    tag = "desktop",
    request_body = DesktopSchedulePreviewRequest,
    responses((status = 200, body = DesktopSchedulePreviewResponse))
)]
pub(crate) async fn preview_setup_schedule(
    Json(request): Json<DesktopSchedulePreviewRequest>,
) -> Result<Json<DesktopSchedulePreviewResponse>, ApiError> {
    ensure_desktop_managed()?;
    Ok(Json(
        match cron::next_cron_occurrence(&request.cron_schedule) {
            Ok(next) => DesktopSchedulePreviewResponse {
                valid: true,
                next_run: Some(next.to_rfc3339()),
                error: None,
            },
            Err(error) => DesktopSchedulePreviewResponse {
                valid: false,
                next_run: None,
                error: Some(error),
            },
        },
    ))
}

async fn validate_cron_jobs(jobs: &[CronJob]) -> Result<(), ApiError> {
    if jobs.is_empty() {
        return Ok(());
    }
    let metadata = job_inference_context()
        .primary
        .get_metadata()
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to load model metadata for Desktop setup");
            ApiError::internal("Failed to validate the selected models")
        })?;
    let external_inputs = job_inference_context()
        .primary
        .get_external_inputs()
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to validate model external inputs for Desktop setup");
            ApiError::internal("Failed to validate additional model configuration")
        })?;
    let mut seen = HashSet::new();
    for job in jobs {
        if !seen.insert(job.inference_id.as_str()) {
            return Err(ApiError::bad_request(format!(
                "Model {} was selected more than once",
                job.inference_id
            )));
        }
        resolve_model_metadata(&metadata, &job.inference_id)?;
        if let Some(usages) = external_inputs
            .get("models")
            .and_then(|models| models.get(&job.inference_id))
            .and_then(JsonValue::as_array)
        {
            for usage in usages {
                if usage.get("required").and_then(JsonValue::as_bool) != Some(true) {
                    continue;
                }
                let Some(id) = usage.get("id").and_then(JsonValue::as_str) else {
                    continue;
                };
                let definition = &external_inputs["definitions"][id];
                if definition.get("configured").and_then(JsonValue::as_bool) != Some(true) {
                    let label = definition
                        .get("label")
                        .and_then(JsonValue::as_str)
                        .unwrap_or(id);
                    return Err(ApiError::bad_request(format!(
                        "Model {} requires additional configuration: {label}",
                        job.inference_id
                    )));
                }
            }
        }
        if job.batch_size.is_some_and(|value| value < 1) {
            return Err(ApiError::bad_request(format!(
                "Model {} has an invalid batch size",
                job.inference_id
            )));
        }
        if job
            .threshold
            .is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value))
        {
            return Err(ApiError::bad_request(format!(
                "Model {} has an invalid confidence threshold",
                job.inference_id
            )));
        }
    }
    Ok(())
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
    cron::validate_cron_schedule(&request.cron_schedule)
        .map_err(|error| ApiError::bad_request(format!("Invalid routine schedule: {error}")))?;
    validate_cron_jobs(&request.cron_jobs).await?;

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
    config.scan_images = request.scan_images;
    config.scan_video = request.scan_video;
    config.scan_audio = request.scan_audio;
    config.scan_pdf = request.scan_pdf;
    config.scan_html = request.scan_html;
    config.cron_jobs = request.cron_jobs;
    config.enable_cron_job = request.enable_cron_job;
    config.cron_schedule = request.cron_schedule;
    store.save(&index_db, &config)?;
    let _ = continuous_scan::notify_config_change(&index_db).await;
    let _ = cron::notify_config_change(&index_db).await;
    let jobs = match cron::run_initial_cronjob(&index_db, &user_data_db).await? {
        cron::CronRunOutcome::Enqueued(jobs) => jobs,
        cron::CronRunOutcome::Skipped => Vec::new(),
    };

    Ok(Json(DesktopSetupCompleteResponse { index_db, jobs }))
}

#[cfg(test)]
mod external_input_tests {
    use super::*;

    /// Updating one declaration preserves unrelated content, while an
    /// explicit removal deletes only the requested declaration.
    #[test]
    fn dotenv_update_preserves_unrelated_lines_and_removes_explicitly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".env");
        fs::write(&path, "# keep me\nOTHER=value\nAPI_KEY=old\n").unwrap();
        update_dotenv(
            &path,
            &HashMap::from([("API_KEY".into(), "new value=with symbols".into())]),
            &[],
        )
        .unwrap();
        let text = fs::read_to_string(&path).unwrap();
        assert!(text.contains("# keep me"));
        assert!(text.contains("OTHER=value"));
        assert!(text.contains("API_KEY=\"new value=with symbols\""));
        update_dotenv(&path, &HashMap::new(), &["API_KEY".into()]).unwrap();
        let text = fs::read_to_string(&path).unwrap();
        assert!(!text.contains("API_KEY="));
        assert!(text.contains("OTHER=value"));
    }

    /// Empty API edits are discarded, so they cannot replace an existing
    /// declaration; non-empty edits remain available to the dotenv writer.
    #[test]
    fn dotenv_empty_edit_keeps_existing_value() {
        let mut values = HashMap::from([
            ("API_KEY".into(), String::new()),
            ("TIMEOUT".into(), "30".into()),
        ]);
        discard_empty_updates(&mut values);
        assert_eq!(values, HashMap::from([("TIMEOUT".into(), "30".into())]));
    }

    /// Dotenv assignment parsing ignores comments and invalid identifiers,
    /// while accepting the standard optional `export` prefix.
    #[test]
    fn dotenv_key_parser_ignores_comments_and_accepts_export() {
        assert_eq!(dotenv_line_key(" export TOKEN = value"), Some("TOKEN"));
        assert_eq!(dotenv_line_key("# TOKEN=value"), None);
        assert_eq!(dotenv_line_key("BAD-NAME=value"), None);
    }
}
