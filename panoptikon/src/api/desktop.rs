//! Desktop-only lifecycle status. This route is mounted only for a
//! `--desktop-managed` sidecar and carries no general host privilege.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    net::IpAddr,
    path::Path,
    sync::{Arc, OnceLock},
};

use axum::{
    Extension, Json,
    extract::{Path as AxumPath, State},
    http::{HeaderMap, Method, StatusCode, header},
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

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct DesktopUpdateSnoozeRequest {
    pub version: String,
}

struct DesktopBridge {
    base: reqwest::Url,
    token: String,
}

fn parse_desktop_bridge_base(raw: &str) -> Option<reqwest::Url> {
    let url = reqwest::Url::parse(raw).ok()?;
    let host = url.host_str()?;
    let address = host
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host)
        .parse::<IpAddr>()
        .ok()?;
    (url.scheme() == "http"
        && address.is_loopback()
        && url.port().is_some_and(|port| port != 0)
        && url.username().is_empty()
        && url.password().is_none()
        && url.path() == "/"
        && url.query().is_none()
        && url.fragment().is_none())
    .then_some(url)
}

fn desktop_bridge_from_environment() -> Option<DesktopBridge> {
    let base = std::env::var("PANOPTIKON_DESKTOP_BRIDGE_URL")
        .ok()
        .and_then(|raw| parse_desktop_bridge_base(&raw))?;
    let token = std::env::var("PANOPTIKON_DESKTOP_BRIDGE_TOKEN").ok()?;
    (!token.is_empty()).then_some(DesktopBridge { base, token })
}

pub(crate) fn desktop_bridge_is_configured() -> bool {
    desktop_bridge_from_environment().is_some()
}

fn ensure_desktop_shell_policy(
    state: &ProxyState,
    context: &PolicyContext,
) -> Result<DesktopBridge, ApiError> {
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
    desktop_bridge_from_environment()
        .ok_or_else(|| ApiError::not_found("Desktop shell endpoint not found"))
}

async fn desktop_bridge_request(
    bridge: &DesktopBridge,
    method: Method,
    path: &str,
    body: Option<JsonValue>,
) -> Result<reqwest::Response, ApiError> {
    let url = bridge
        .base
        .join(path.strip_prefix('/').unwrap_or(path))
        .map_err(|error| {
            tracing::error!(%error, "failed to construct Desktop shell bridge URL");
            ApiError::internal("Desktop shell is unavailable")
        })?;
    let client = reqwest::Client::builder()
        // The bridge is an authenticated process-local channel. Never send
        // its bearer credential to a proxy selected from the environment.
        .no_proxy()
        // A bridge response is authoritative only for the exact loopback
        // endpoint Desktop created. Do not carry the request (or its bearer
        // credential) through an HTTP redirect.
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(3))
        .build()
        .map_err(|error| {
            tracing::error!(%error, "failed to build Desktop shell bridge client");
            ApiError::internal("Desktop shell is unavailable")
        })?;
    let mut request = client.request(method, url).bearer_auth(&bridge.token);
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
    let bridge = ensure_desktop_shell_policy(&state, &context)?;
    let response = desktop_bridge_request(&bridge, Method::GET, "/status", None).await?;
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
    headers: HeaderMap,
    path: &'static str,
    body: Option<JsonValue>,
) -> Result<StatusCode, ApiError> {
    // Preserve the policy boundary first: callers without the Desktop client
    // opt-in still see this route as unavailable, independent of headers.
    let bridge = ensure_desktop_shell_policy(&state, &context)?;
    ensure_same_origin_desktop_action(&headers)?;
    let response = desktop_bridge_request(&bridge, Method::POST, path, body).await?;
    if response.status().is_success() {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::new(
            response.status(),
            "Desktop shell rejected the request",
        ))
    }
}

fn ensure_same_origin_desktop_action(headers: &HeaderMap) -> Result<(), ApiError> {
    fn forbidden() -> ApiError {
        ApiError::new(
            StatusCode::FORBIDDEN,
            "Desktop shell actions require a same-origin browser request",
        )
    }

    let mut origin_values = headers.get_all(header::ORIGIN).iter();
    let origin = origin_values
        .next()
        .and_then(|value| value.to_str().ok())
        .and_then(|value| reqwest::Url::parse(value).ok())
        .ok_or_else(forbidden)?;
    if origin_values.next().is_some()
        || !origin.username().is_empty()
        || origin.password().is_some()
        || origin.path() != "/"
        || origin.query().is_some()
        || origin.fragment().is_some()
    {
        return Err(forbidden());
    }

    let mut host_values = headers.get_all(header::HOST).iter();
    let expected = host_values
        .next()
        .and_then(|value| value.to_str().ok())
        .and_then(|host| reqwest::Url::parse(&format!("http://{host}/")).ok())
        .ok_or_else(forbidden)?;
    let expected_is_loopback = match expected.host() {
        Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    };
    if host_values.next().is_some()
        || !expected.username().is_empty()
        || expected.password().is_some()
        || expected.path() != "/"
        || expected.query().is_some()
        || expected.fragment().is_some()
        // Origin/Host equality alone is vulnerable to DNS rebinding: an
        // attacker-owned hostname can remain same-origin while resolving to
        // this listener. Desktop opens only localhost, so require the browser
        // authority itself to name a loopback host.
        || !expected_is_loopback
        || origin.origin() != expected.origin()
    {
        return Err(forbidden());
    }

    let mut fetch_site_values = headers.get_all("sec-fetch-site").iter();
    if let Some(fetch_site) = fetch_site_values.next()
        && (fetch_site_values.next().is_some()
            || !fetch_site
                .to_str()
                .is_ok_and(|value| value.eq_ignore_ascii_case("same-origin")))
    {
        return Err(forbidden());
    }
    Ok(())
}

#[utoipa::path(post, operation_id = "open_desktop_update_window", path = "/api/desktop/update-window/open", tag = "desktop", responses((status = 204, description = "Update window opened"), (status = 403, description = "Same-origin browser request required")))]
pub(crate) async fn open_update_window(
    State(state): State<Arc<ProxyState>>,
    Extension(context): Extension<PolicyContext>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    desktop_bridge_action(state, context, headers, "/open", None).await
}

#[utoipa::path(post, operation_id = "snooze_desktop_update_ribbon", path = "/api/desktop/update-ribbon/snooze", tag = "desktop", request_body = DesktopUpdateSnoozeRequest, responses((status = 204, description = "Ribbon snoozed for 24 hours"), (status = 403, description = "Same-origin browser request required"), (status = 409, description = "Available update version changed")))]
pub(crate) async fn snooze_update_ribbon(
    State(state): State<Arc<ProxyState>>,
    Extension(context): Extension<PolicyContext>,
    headers: HeaderMap,
    Json(request): Json<DesktopUpdateSnoozeRequest>,
) -> Result<StatusCode, ApiError> {
    desktop_bridge_action(
        state,
        context,
        headers,
        "/snooze",
        Some(json!({ "version": request.version })),
    )
    .await
}

#[utoipa::path(post, operation_id = "dismiss_desktop_update_ribbon", path = "/api/desktop/update-ribbon/dismiss", tag = "desktop", request_body = DesktopUpdateDismissRequest, responses((status = 204, description = "Ribbon dismissed for the selected version"), (status = 403, description = "Same-origin browser request required"), (status = 409, description = "Available update version changed")))]
pub(crate) async fn dismiss_update_ribbon(
    State(state): State<Arc<ProxyState>>,
    Extension(context): Extension<PolicyContext>,
    headers: HeaderMap,
    Json(request): Json<DesktopUpdateDismissRequest>,
) -> Result<StatusCode, ApiError> {
    desktop_bridge_action(
        state,
        context,
        headers,
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
    let values = values
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let remove = remove.iter().cloned().collect::<BTreeSet<_>>();
    let mut document = panoptikon_config::DotenvDocument::load(path)?;
    document.apply(&values, &remove);
    document.write_private_atomic(path)
}

#[cfg(test)]
mod desktop_bridge_tests {
    use super::*;
    use axum::http::HeaderValue;
    use axum::response::Redirect;
    use axum::routing::post;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn browser_headers(origin: &str, host: &str, fetch_site: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(header::ORIGIN, HeaderValue::from_str(origin).unwrap());
        headers.insert(header::HOST, HeaderValue::from_str(host).unwrap());
        if let Some(fetch_site) = fetch_site {
            headers.insert("sec-fetch-site", HeaderValue::from_str(fetch_site).unwrap());
        }
        headers
    }

    /// The private shell hop accepts only the literal IPv4/IPv6 loopback
    /// HTTP addresses Desktop creates, with an explicit nonzero port and no
    /// URL components that could redirect a supposedly local credential.
    #[test]
    fn desktop_bridge_base_is_strictly_loopback() {
        assert!(parse_desktop_bridge_base("http://127.0.0.1:49152").is_some());
        assert!(parse_desktop_bridge_base("http://[::1]:49152/").is_some());

        for invalid in [
            "http://localhost:49152",
            "http://192.0.2.1:49152",
            "https://127.0.0.1:49152",
            "http://127.0.0.1",
            "http://127.0.0.1:0",
            "http://user@127.0.0.1:49152",
            "http://127.0.0.1:49152/status",
            "http://127.0.0.1:49152/?target=elsewhere",
            "http://127.0.0.1:49152/#fragment",
        ] {
            assert!(
                parse_desktop_bridge_base(invalid).is_none(),
                "unexpectedly accepted {invalid}"
            );
        }
    }

    /// The real reqwest bridge path reaches the loopback listener with the
    /// bearer credential and exact version JSON that the Desktop snooze
    /// handler validates, rather than dropping the browser's target.
    #[tokio::test]
    async fn desktop_bridge_request_forwards_authenticated_version_body() {
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let router = axum::Router::new().route(
            "/snooze",
            post(
                |headers: HeaderMap, Json(body): Json<JsonValue>| async move {
                    assert_eq!(
                        headers
                            .get(header::AUTHORIZATION)
                            .and_then(|value| value.to_str().ok()),
                        Some("Bearer bridge-secret")
                    );
                    assert_eq!(body, json!({ "version": "1.2.3" }));
                    StatusCode::NO_CONTENT
                },
            ),
        );
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let bridge = DesktopBridge {
            base: parse_desktop_bridge_base(&format!("http://{address}")).unwrap(),
            token: "bridge-secret".into(),
        };

        let response = desktop_bridge_request(
            &bridge,
            Method::POST,
            "/snooze",
            Some(json!({ "version": "1.2.3" })),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        server.abort();
        let _ = server.await;
    }

    /// The bearer-authenticated private hop must never follow a response to a
    /// second URL, even when the redirect remains on the loopback listener.
    #[tokio::test]
    async fn desktop_bridge_request_does_not_follow_redirects() {
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let followed = Arc::new(AtomicUsize::new(0));
        let capture_count = followed.clone();
        let router = axum::Router::new()
            .route(
                "/redirect",
                post(|| async { Redirect::temporary("/capture") }),
            )
            .route(
                "/capture",
                post(move || {
                    let capture_count = capture_count.clone();
                    async move {
                        capture_count.fetch_add(1, Ordering::SeqCst);
                        StatusCode::NO_CONTENT
                    }
                }),
            );
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let bridge = DesktopBridge {
            base: parse_desktop_bridge_base(&format!("http://{address}")).unwrap(),
            token: "bridge-secret".into(),
        };

        let response = desktop_bridge_request(&bridge, Method::POST, "/redirect", None)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(followed.load(Ordering::SeqCst), 0);
        server.abort();
        let _ = server.await;
    }

    /// A normal same-origin fetch is admitted, including URL normalization
    /// and absent optional Fetch Metadata, so supported browsers can invoke
    /// the Desktop action without an application-specific CSRF token.
    #[test]
    fn desktop_action_accepts_same_origin_browser_request() {
        let headers = browser_headers(
            "http://LOCALHOST:6342",
            "localhost:6342",
            Some("same-origin"),
        );
        ensure_same_origin_desktop_action(&headers).unwrap();

        let headers = browser_headers("http://localhost", "localhost:80", None);
        ensure_same_origin_desktop_action(&headers).unwrap();

        let headers = browser_headers("http://127.0.0.1:6342", "127.0.0.1:6342", None);
        ensure_same_origin_desktop_action(&headers).unwrap();

        let headers = browser_headers("http://[::1]:6342", "[::1]:6342", None);
        ensure_same_origin_desktop_action(&headers).unwrap();
    }

    /// Cross-origin forms/fetches, scheme mismatches, opaque or missing
    /// origins, and contradictory Fetch Metadata are all rejected before a
    /// request can receive the private bridge credential.
    #[test]
    fn desktop_action_rejects_cross_origin_browser_request() {
        for headers in [
            browser_headers("https://attacker.example", "127.0.0.1:6342", None),
            // Matching Origin and Host is insufficient when an attacker can
            // rebind its own hostname to this loopback listener.
            browser_headers(
                "http://attacker.example:6342",
                "attacker.example:6342",
                Some("same-origin"),
            ),
            browser_headers("https://localhost:6342", "localhost:6342", None),
            browser_headers("http://127.0.0.1:6342", "user@127.0.0.1:6342", None),
            browser_headers(
                "http://127.0.0.1:6342",
                "127.0.0.1:6342",
                Some("cross-site"),
            ),
            browser_headers("null", "127.0.0.1:6342", None),
            {
                let mut headers = HeaderMap::new();
                headers.insert(header::HOST, HeaderValue::from_static("127.0.0.1:6342"));
                headers
            },
            {
                let mut headers = browser_headers("http://127.0.0.1:6342", "127.0.0.1:6342", None);
                headers.append(
                    header::ORIGIN,
                    HeaderValue::from_static("http://127.0.0.1:6342"),
                );
                headers
            },
        ] {
            assert!(ensure_same_origin_desktop_action(&headers).is_err());
        }
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
    let database = (!request.new_database).then_some(&mut *conn.conn);
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
    let database = (!request.new_database).then_some(&mut *conn.conn);
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

    let database = request.new_index_db.is_none().then_some(&mut *conn.conn);
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
    let database = request.new_index_db.is_none().then_some(&mut *conn.conn);
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
        std::fs::write(&path, "# keep me\nOTHER=value\nAPI_KEY=old\n").unwrap();
        update_dotenv(
            &path,
            &HashMap::from([("API_KEY".into(), "new value=with symbols".into())]),
            &[],
        )
        .unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("# keep me"));
        assert!(text.contains("OTHER=value"));
        assert!(text.contains("API_KEY=\"new value=with symbols\""));
        update_dotenv(&path, &HashMap::new(), &["API_KEY".into()]).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
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
}
