//! Origin-bound loopback Relay v1. The HTTP surface is intentionally small:
//! discovery, expiring local-approval pairing, and two authenticated actions.

use crate::settings::atomic_write;
use anyhow::{Context as _, bail};
use argon2::{
    Argon2, PasswordHash, PasswordHasher as _, PasswordVerifier as _, password_hash::SaltString,
};
use axum::{
    Json, Router,
    extract::{Path as AxumPath, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::Engine as _;
use rand::RngCore as _;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, RwLock, oneshot};
use url::Url;
use uuid::Uuid;

const PAIRING_TTL: Duration = Duration::from_secs(5 * 60);
const RATE_WINDOW: Duration = Duration::from_secs(60);
const RATE_LIMIT: usize = 5;
const MAX_PENDING: usize = 10;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RelayConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default)]
    pub instances: Vec<RelayInstance>,
}

impl RelayConfig {
    pub fn desktop_default() -> Self {
        Self {
            bind: default_bind(),
            ..Self::default()
        }
    }
}

fn default_bind() -> String {
    "127.0.0.1:17600".into()
}

pub fn load_config(path: &Path) -> anyhow::Result<RelayConfig> {
    if !path.exists() {
        return Ok(RelayConfig::desktop_default());
    }
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read Relay settings '{}'", path.display()))?;
    match toml::from_str(&text) {
        Ok(config) => Ok(config),
        Err(error) => {
            let stamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let quarantine = path.with_extension(format!("toml.invalid-{stamp}"));
            std::fs::rename(path, &quarantine)?;
            bail!(
                "Relay settings '{}' are invalid and were quarantined as '{}': {error}",
                path.display(),
                quarantine.display()
            );
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayInstance {
    pub id: Uuid,
    pub name: String,
    pub server_url: String,
    pub origins: Vec<String>,
    pub credential_hash: String,
    #[serde(default)]
    pub mappings: Vec<PathMapping>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PathMapping {
    pub remote: String,
    pub local: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RelayStatusView {
    pub enabled: bool,
    pub bind: String,
    pub instances: Vec<RelayInstanceView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RelayInstanceView {
    pub id: Uuid,
    pub name: String,
    pub server_url: String,
    pub origins: Vec<String>,
    pub mappings: Vec<PathMapping>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PendingPairingView {
    pub id: Uuid,
    pub name: String,
    pub origin: String,
    pub server_url: String,
    pub expires_in_secs: u64,
}

#[derive(Debug)]
struct PendingPairing {
    id: Uuid,
    name: String,
    origin: String,
    server_url: String,
    created: Instant,
    approved: Option<ApprovedCredential>,
    rejected: bool,
}

#[derive(Debug)]
struct ApprovedCredential {
    instance_id: Uuid,
    credential: String,
    claimed: bool,
}

type ActionHandler = Arc<dyn Fn(RelayAction, PathBuf) -> anyhow::Result<()> + Send + Sync>;

pub struct RelayState {
    config: RwLock<RelayConfig>,
    config_path: PathBuf,
    pending: Mutex<HashMap<Uuid, PendingPairing>>,
    attempts: Mutex<HashMap<String, VecDeque<Instant>>>,
    action_handler: ActionHandler,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelayAction {
    OpenFile,
    RevealInFolder,
}

#[derive(Debug, Deserialize)]
struct PairingRequest {
    name: String,
    origin: String,
    server_url: String,
}

#[derive(Debug, Serialize)]
struct PairingRequested {
    request_id: Uuid,
    expires_in_secs: u64,
}

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum PairingStatus {
    Pending,
    Rejected,
    Approved {
        instance_id: Uuid,
        credential: String,
    },
}

#[derive(Debug, Deserialize)]
struct ActionRequest {
    action: RelayAction,
    path: String,
}

#[derive(Debug, Serialize)]
struct Health {
    protocol: &'static str,
    version: &'static str,
    pairing: bool,
}

impl RelayState {
    pub fn new(config: RelayConfig, config_path: PathBuf, action_handler: ActionHandler) -> Self {
        Self {
            config: RwLock::new(config),
            config_path,
            pending: Mutex::new(HashMap::new()),
            attempts: Mutex::new(HashMap::new()),
            action_handler,
        }
    }

    pub async fn config(&self) -> RelayConfig {
        self.config.read().await.clone()
    }

    /// Return only fields safe to expose to the bundled control UI. In
    /// particular, credential hashes never cross the Rust command boundary.
    pub async fn status(&self) -> RelayStatusView {
        let config = self.config.read().await;
        RelayStatusView {
            enabled: config.enabled,
            bind: config.bind.clone(),
            instances: config
                .instances
                .iter()
                .map(|item| RelayInstanceView {
                    id: item.id,
                    name: item.name.clone(),
                    server_url: item.server_url.clone(),
                    origins: item.origins.clone(),
                    mappings: item.mappings.clone(),
                })
                .collect(),
        }
    }

    pub async fn set_enabled(&self, enabled: bool) -> anyhow::Result<()> {
        let mut config = self.config.write().await;
        config.enabled = enabled;
        save_config(&self.config_path, &config)
    }

    pub async fn pending(&self) -> Vec<PendingPairingView> {
        let now = Instant::now();
        let mut pending = self.pending.lock().await;
        pending.retain(|_, item| {
            now.duration_since(item.created) <= PAIRING_TTL
                && !item.rejected
                && item.approved.as_ref().is_none_or(|a| !a.claimed)
        });
        pending
            .values()
            .filter(|item| item.approved.is_none())
            .map(|item| PendingPairingView {
                id: item.id,
                name: item.name.clone(),
                origin: item.origin.clone(),
                server_url: item.server_url.clone(),
                expires_in_secs: PAIRING_TTL
                    .saturating_sub(now.duration_since(item.created))
                    .as_secs(),
            })
            .collect()
    }

    pub async fn approve(&self, request_id: Uuid) -> anyhow::Result<()> {
        let (name, origin, server_url) = {
            let pending = self.pending.lock().await;
            let item = pending
                .get(&request_id)
                .context("pairing request not found or expired")?;
            if item.rejected || item.approved.is_some() || item.created.elapsed() > PAIRING_TTL {
                bail!("pairing request is no longer approvable");
            }
            (
                item.name.clone(),
                item.origin.clone(),
                item.server_url.clone(),
            )
        };
        let mut secret = [0u8; 32];
        rand::rng().fill_bytes(&mut secret);
        let credential = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret);
        let salt = SaltString::encode_b64(&secret[..16])
            .map_err(|error| anyhow::anyhow!("failed to encode credential salt: {error}"))?;
        let credential_hash = Argon2::default()
            .hash_password(credential.as_bytes(), &salt)
            .map_err(|error| anyhow::anyhow!("failed to hash Relay credential: {error}"))?
            .to_string();
        let instance_id = Uuid::new_v4();
        {
            let mut config = self.config.write().await;
            config.instances.push(RelayInstance {
                id: instance_id,
                name,
                server_url,
                origins: vec![origin],
                credential_hash,
                mappings: Vec::new(),
            });
            save_config(&self.config_path, &config)?;
        }
        let mut pending = self.pending.lock().await;
        let item = pending
            .get_mut(&request_id)
            .context("pairing request disappeared")?;
        item.approved = Some(ApprovedCredential {
            instance_id,
            credential,
            claimed: false,
        });
        Ok(())
    }

    pub async fn reject(&self, request_id: Uuid) -> anyhow::Result<()> {
        let mut pending = self.pending.lock().await;
        let item = pending
            .get_mut(&request_id)
            .context("pairing request not found")?;
        if item.approved.is_some() || item.rejected {
            bail!("pairing request is already resolved");
        }
        item.rejected = true;
        Ok(())
    }

    pub async fn revoke(&self, instance_id: Uuid) -> anyhow::Result<()> {
        let mut config = self.config.write().await;
        let old_len = config.instances.len();
        config.instances.retain(|item| item.id != instance_id);
        if config.instances.len() == old_len {
            bail!("Relay instance not found");
        }
        save_config(&self.config_path, &config)
    }

    pub async fn replace_mappings(
        &self,
        instance_id: Uuid,
        mappings: Vec<PathMapping>,
    ) -> anyhow::Result<()> {
        for mapping in &mappings {
            normalize_path(&mapping.remote)?;
            normalize_path(&mapping.local)?;
        }
        let mut config = self.config.write().await;
        let instance = config
            .instances
            .iter_mut()
            .find(|item| item.id == instance_id)
            .context("Relay instance not found")?;
        instance.mappings = mappings;
        save_config(&self.config_path, &config)
    }
}

pub struct RelayHandle {
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl RelayHandle {
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = self.task.await;
    }
}

pub async fn start(state: Arc<RelayState>) -> anyhow::Result<RelayHandle> {
    let bind = state
        .config
        .read()
        .await
        .bind
        .parse::<std::net::SocketAddr>()
        .context("invalid Relay bind address")?;
    if !bind.ip().is_loopback() {
        bail!("Relay must bind to a loopback address, not {bind}");
    }
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("failed to bind Relay on {bind}"))?;
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router(state))
            .with_graceful_shutdown(async {
                let _ = rx.await;
            })
            .await
        {
            tracing::error!(%error, "Relay listener failed");
        }
    });
    Ok(RelayHandle {
        shutdown: Some(tx),
        task,
    })
}

pub fn router(state: Arc<RelayState>) -> Router {
    Router::new()
        .route("/v1/health", get(health))
        .route(
            "/v1/pairing/request",
            post(request_pairing).options(pairing_options),
        )
        .route(
            "/v1/pairing/{id}",
            get(pairing_status).options(pairing_options),
        )
        .route("/v1/actions", post(action).options(action_options))
        .with_state(state)
}

async fn health(headers: HeaderMap) -> Response {
    let response = Json(Health {
        protocol: "panoptikon-relay-v1",
        version: env!("CARGO_PKG_VERSION"),
        pairing: true,
    })
    .into_response();
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Url::parse(value).ok().map(|url| (value, url)))
        .filter(|(value, url)| *value == serialized_origin(url));
    if let Some((origin, _)) = origin {
        with_cors(response, origin)
    } else {
        response
    }
}

async fn pairing_options(headers: HeaderMap) -> Response {
    let origin = match validated_origin(&headers, None) {
        Ok(origin) => origin,
        Err(response) => return response,
    };
    preflight(&origin, "GET, POST, OPTIONS")
}

async fn action_options(State(state): State<Arc<RelayState>>, headers: HeaderMap) -> Response {
    let origin = match validated_origin(&headers, None) {
        Ok(origin) => origin,
        Err(response) => return response,
    };
    let allowed = state
        .config
        .read()
        .await
        .instances
        .iter()
        .any(|instance| instance.origins.iter().any(|item| item == &origin));
    if !allowed {
        return error(StatusCode::FORBIDDEN, "origin is not paired", Some(&origin));
    }
    preflight(&origin, "POST, OPTIONS")
}

async fn request_pairing(
    State(state): State<Arc<RelayState>>,
    headers: HeaderMap,
    Json(request): Json<PairingRequest>,
) -> Response {
    let origin = match validated_origin(&headers, Some(&request.origin)) {
        Ok(origin) => origin,
        Err(response) => return response,
    };
    let server_url = match Url::parse(&request.server_url) {
        Ok(url) => url,
        Err(_) => return error(StatusCode::BAD_REQUEST, "invalid server URL", Some(&origin)),
    };
    if serialized_origin(&server_url) != origin {
        return error(
            StatusCode::BAD_REQUEST,
            "server URL does not match the requesting origin",
            Some(&origin),
        );
    }
    if request.name.trim().is_empty() || request.name.len() > 80 {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid instance name",
            Some(&origin),
        );
    }
    let now = Instant::now();
    {
        let mut attempts = state.attempts.lock().await;
        let values = attempts.entry(origin.clone()).or_default();
        while values
            .front()
            .is_some_and(|at| now.duration_since(*at) > RATE_WINDOW)
        {
            values.pop_front();
        }
        if values.len() >= RATE_LIMIT {
            return error(
                StatusCode::TOO_MANY_REQUESTS,
                "pairing requests are rate limited",
                Some(&origin),
            );
        }
        values.push_back(now);
    }
    let mut pending = state.pending.lock().await;
    pending.retain(|_, item| now.duration_since(item.created) <= PAIRING_TTL);
    if pending.len() >= MAX_PENDING {
        return error(
            StatusCode::TOO_MANY_REQUESTS,
            "too many pending pairing requests",
            Some(&origin),
        );
    }
    let id = Uuid::new_v4();
    pending.insert(
        id,
        PendingPairing {
            id,
            name: request.name.trim().to_owned(),
            origin: origin.clone(),
            server_url: server_url.to_string(),
            created: now,
            approved: None,
            rejected: false,
        },
    );
    with_cors(
        (
            StatusCode::ACCEPTED,
            Json(PairingRequested {
                request_id: id,
                expires_in_secs: PAIRING_TTL.as_secs(),
            }),
        )
            .into_response(),
        &origin,
    )
}

async fn pairing_status(
    State(state): State<Arc<RelayState>>,
    AxumPath(id): AxumPath<Uuid>,
    headers: HeaderMap,
) -> Response {
    let origin = match validated_origin(&headers, None) {
        Ok(origin) => origin,
        Err(response) => return response,
    };
    let mut pending = state.pending.lock().await;
    let Some(item) = pending.get_mut(&id) else {
        return error(
            StatusCode::NOT_FOUND,
            "pairing request not found",
            Some(&origin),
        );
    };
    if item.origin != origin {
        return error(
            StatusCode::FORBIDDEN,
            "origin is not authorized for this pairing request",
            Some(&origin),
        );
    }
    if item.created.elapsed() > PAIRING_TTL {
        return error(StatusCode::GONE, "pairing request expired", Some(&origin));
    }
    let status = if item.rejected {
        PairingStatus::Rejected
    } else if let Some(approved) = &mut item.approved {
        if approved.claimed {
            return error(
                StatusCode::GONE,
                "pairing credential was already claimed",
                Some(&origin),
            );
        }
        approved.claimed = true;
        PairingStatus::Approved {
            instance_id: approved.instance_id,
            credential: std::mem::take(&mut approved.credential),
        }
    } else {
        PairingStatus::Pending
    };
    with_cors(Json(status).into_response(), &origin)
}

async fn action(
    State(state): State<Arc<RelayState>>,
    headers: HeaderMap,
    Json(request): Json<ActionRequest>,
) -> Response {
    let origin = match validated_origin(&headers, None) {
        Ok(origin) => origin,
        Err(response) => return response,
    };
    let credential = match headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    {
        Some(value) if !value.is_empty() => value,
        _ => {
            return error(
                StatusCode::UNAUTHORIZED,
                "missing Relay credential",
                Some(&origin),
            );
        }
    };
    let config = state.config.read().await;
    let instance = config.instances.iter().find(|item| {
        item.origins.iter().any(|allowed| allowed == &origin)
            && verify_credential(&item.credential_hash, credential)
    });
    let Some(instance) = instance else {
        return error(
            StatusCode::UNAUTHORIZED,
            "invalid Relay credential or origin",
            Some(&origin),
        );
    };
    let mapped = match map_path(&request.path, &instance.mappings) {
        Ok(path) => path,
        Err(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "path is not covered by an authorized mapping",
                Some(&origin),
            );
        }
    };
    if !mapped.exists() {
        return error(
            StatusCode::NOT_FOUND,
            "mapped path is unavailable",
            Some(&origin),
        );
    }
    let instance_id = instance.id;
    drop(config);
    tracing::info!(%instance_id, action = ?request.action, "Relay action authorized");
    match (state.action_handler)(request.action, mapped) {
        Ok(()) => with_cors(StatusCode::NO_CONTENT.into_response(), &origin),
        Err(error_value) => {
            tracing::warn!(%instance_id, error = %error_value, "Relay action failed");
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "local action failed",
                Some(&origin),
            )
        }
    }
}

fn validated_origin(headers: &HeaderMap, body_origin: Option<&str>) -> Result<String, Response> {
    let header_origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "Origin header is required", None))?;
    let parsed = Url::parse(header_origin)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid Origin header", None))?;
    let origin = serialized_origin(&parsed);
    if origin != header_origin || body_origin.is_some_and(|value| value != origin) {
        return Err(error(StatusCode::BAD_REQUEST, "origin mismatch", None));
    }
    Ok(origin)
}

fn serialized_origin(url: &Url) -> String {
    let mut value = format!("{}://{}", url.scheme(), url.host_str().unwrap_or_default());
    if let Some(port) = url.port() {
        value.push_str(&format!(":{port}"));
    }
    value
}

fn with_cors(mut response: Response, origin: &str) -> Response {
    if let Ok(value) = HeaderValue::from_str(origin) {
        response
            .headers_mut()
            .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, value);
    }
    response
        .headers_mut()
        .insert(header::VARY, HeaderValue::from_static("Origin"));
    response
}

fn preflight(origin: &str, methods: &'static str) -> Response {
    let mut response = with_cors(StatusCode::NO_CONTENT.into_response(), origin);
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static(methods),
    );
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("Authorization, Content-Type"),
    );
    response
}

fn error(status: StatusCode, message: &str, origin: Option<&str>) -> Response {
    let response = (status, Json(serde_json::json!({"error": message}))).into_response();
    if let Some(origin) = origin {
        with_cors(response, origin)
    } else {
        response
    }
}

fn verify_credential(hash: &str, credential: &str) -> bool {
    PasswordHash::new(hash).ok().is_some_and(|parsed| {
        Argon2::default()
            .verify_password(credential.as_bytes(), &parsed)
            .is_ok()
    })
}

fn save_config(path: &Path, config: &RelayConfig) -> anyhow::Result<()> {
    atomic_write(path, toml::to_string_pretty(config)?.as_bytes())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedPath {
    prefix: String,
    components: Vec<String>,
    windows: bool,
}

fn normalize_path(input: &str) -> anyhow::Result<NormalizedPath> {
    if input.trim().is_empty() || input.contains('\0') {
        bail!("invalid empty path");
    }
    let value = input.replace('\\', "/");
    let (prefix, rest, windows) = if value.starts_with("//") {
        let mut parts = value[2..].split('/').filter(|part| !part.is_empty());
        let server = parts.next().context("UNC path has no server")?;
        let share = parts.next().context("UNC path has no share")?;
        (
            format!("//{server}/{share}"),
            parts.collect::<Vec<_>>().join("/"),
            true,
        )
    } else if value.as_bytes().get(1) == Some(&b':')
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphabetic)
    {
        (
            value[..2].to_ascii_uppercase(),
            value[2..].trim_start_matches('/').to_owned(),
            true,
        )
    } else if value.starts_with('/') {
        ("/".into(), value[1..].to_owned(), false)
    } else {
        (String::new(), value, cfg!(windows))
    };
    let mut components = Vec::new();
    for component in rest
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
    {
        if component == ".." {
            if components.pop().is_none() {
                bail!("path traversal above root");
            }
        } else {
            components.push(component.to_owned());
        }
    }
    Ok(NormalizedPath {
        prefix,
        components,
        windows,
    })
}

fn component_eq(a: &str, b: &str, windows: bool) -> bool {
    if windows {
        a.eq_ignore_ascii_case(b)
    } else {
        a == b
    }
}

pub fn map_path(remote_path: &str, mappings: &[PathMapping]) -> anyhow::Result<PathBuf> {
    let input = normalize_path(remote_path)?;
    let mut selected: Option<(&PathMapping, NormalizedPath)> = None;
    for mapping in mappings {
        let remote = normalize_path(&mapping.remote)?;
        if remote.windows != input.windows
            || !component_eq(&remote.prefix, &input.prefix, input.windows)
            || remote.components.len() > input.components.len()
        {
            continue;
        }
        if remote
            .components
            .iter()
            .zip(&input.components)
            .all(|(a, b)| component_eq(a, b, input.windows))
            && selected
                .as_ref()
                .is_none_or(|(_, old)| remote.components.len() > old.components.len())
        {
            selected = Some((mapping, remote));
        }
    }
    let (mapping, remote) = selected.context("no Relay mapping covers the path")?;
    let local = normalize_path(&mapping.local)?;
    let mut output = if local.prefix == "/" {
        PathBuf::from("/")
    } else if local.prefix.is_empty() {
        PathBuf::new()
    } else if local.prefix.len() == 2 && local.prefix.ends_with(':') {
        PathBuf::from(format!("{}/", local.prefix))
    } else {
        PathBuf::from(&local.prefix)
    };
    for component in &local.components {
        output.push(component);
    }
    for component in &input.components[remote.components.len()..] {
        output.push(component);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use tower::ServiceExt as _;

    fn test_state(temp: &tempfile::TempDir) -> Arc<RelayState> {
        Arc::new(RelayState::new(
            RelayConfig::desktop_default(),
            temp.path().join("relay.toml"),
            Arc::new(|_, _| Ok(())),
        ))
    }

    fn pairing_request(origin: &str, name: &str) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/v1/pairing/request")
            .header(header::ORIGIN, origin)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::json!({
                    "name": name,
                    "origin": origin,
                    "server_url": format!("{origin}/search")
                })
                .to_string(),
            ))
            .unwrap()
    }

    /// Mapping is component-aware and the longest valid prefix wins.
    #[test]
    fn longest_component_prefix_wins() {
        let mappings = vec![
            PathMapping {
                remote: "/srv".into(),
                local: "/mnt/base".into(),
            },
            PathMapping {
                remote: "/srv/media".into(),
                local: "/mnt/media".into(),
            },
        ];
        assert_eq!(
            map_path("/srv/media/photos/a.jpg", &mappings).unwrap(),
            PathBuf::from("/mnt/media/photos/a.jpg")
        );
        assert!(map_path("/srv-media/a.jpg", &mappings).is_err());
    }

    /// Dot components normalize before matching while traversal above an
    /// authorized root is rejected.
    #[test]
    fn traversal_cannot_escape_mapping() {
        let mappings = [PathMapping {
            remote: "/srv/media".into(),
            local: "/mnt/media".into(),
        }];
        assert_eq!(
            map_path("/srv/media/a/../b.jpg", &mappings).unwrap(),
            PathBuf::from("/mnt/media/b.jpg")
        );
        assert!(map_path("/srv/media/../../etc/passwd", &mappings).is_err());
    }

    /// Windows drive and UNC paths normalize separators and case without raw
    /// string-prefix confusion.
    #[test]
    fn windows_drive_and_unc_mapping() {
        let drive = [PathMapping {
            remote: "D:\\Archive".into(),
            local: "Z:\\Media".into(),
        }];
        assert_eq!(
            map_path("d:/archive/Set/file.jpg", &drive)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/"),
            "Z:/Media/Set/file.jpg"
        );
        let unc = [PathMapping {
            remote: "//nas/share/media".into(),
            local: "C:/cache".into(),
        }];
        assert_eq!(
            map_path("\\\\NAS\\share\\media\\x.png", &unc)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/"),
            "C:/cache/x.png"
        );
    }

    /// Credentials are unique salted password hashes and verification never
    /// relies on plaintext persistence.
    #[test]
    fn credential_hash_verification() {
        let salt = SaltString::encode_b64(b"0123456789abcdef").unwrap();
        let hash = Argon2::default()
            .hash_password(b"secret", &salt)
            .unwrap()
            .to_string();
        assert!(verify_credential(&hash, "secret"));
        assert!(!verify_credential(&hash, "wrong"));
    }

    /// Pairing reflects only a canonical matching Origin, adds CORS headers,
    /// and rejects the sixth request from one origin inside the rate window.
    #[tokio::test]
    async fn pairing_origin_cors_and_rate_limit() {
        let temp = tempfile::tempdir().unwrap();
        let state = test_state(&temp);
        for index in 0..RATE_LIMIT {
            let response = router(state.clone())
                .oneshot(pairing_request(
                    "https://remote.example",
                    &format!("remote-{index}"),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::ACCEPTED);
            assert_eq!(
                response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
                "https://remote.example"
            );
        }
        let limited = router(state.clone())
            .oneshot(pairing_request("https://remote.example", "limited"))
            .await
            .unwrap();
        assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);

        let mut mismatched = pairing_request("https://other.example", "wrong");
        *mismatched.body_mut() = Body::from(
            serde_json::json!({
                "name": "wrong",
                "origin": "https://remote.example",
                "server_url": "https://remote.example/search"
            })
            .to_string(),
        );
        let response = router(state).oneshot(mismatched).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    /// An approved credential is disclosed exactly once to its requesting
    /// origin; another origin cannot poll the request and revocation persists.
    #[tokio::test]
    async fn approved_pairing_is_origin_bound_one_time_and_revocable() {
        let temp = tempfile::tempdir().unwrap();
        let state = test_state(&temp);
        let response = router(state.clone())
            .oneshot(pairing_request("https://remote.example", "remote"))
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
        let requested: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let request_id = Uuid::parse_str(requested["request_id"].as_str().unwrap()).unwrap();
        state.approve(request_id).await.unwrap();

        let wrong_origin = Request::builder()
            .uri(format!("/v1/pairing/{request_id}"))
            .header(header::ORIGIN, "https://other.example")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            router(state.clone())
                .oneshot(wrong_origin)
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );

        let poll = || {
            Request::builder()
                .uri(format!("/v1/pairing/{request_id}"))
                .header(header::ORIGIN, "https://remote.example")
                .body(Body::empty())
                .unwrap()
        };
        let approved = router(state.clone()).oneshot(poll()).await.unwrap();
        let body = to_bytes(approved.into_body(), 16 * 1024).await.unwrap();
        let approved: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(approved["status"], "approved");
        assert!(approved["credential"].as_str().unwrap().len() >= 40);
        assert_eq!(
            router(state.clone())
                .oneshot(poll())
                .await
                .unwrap()
                .status(),
            StatusCode::GONE
        );

        let instance_id = Uuid::parse_str(approved["instance_id"].as_str().unwrap()).unwrap();
        state.revoke(instance_id).await.unwrap();
        assert!(state.status().await.instances.is_empty());
    }

    /// Expired requests return Gone even when the caller presents the exact
    /// origin that created the request.
    #[tokio::test]
    async fn expired_pairing_is_not_claimable() {
        let temp = tempfile::tempdir().unwrap();
        let state = test_state(&temp);
        let id = Uuid::new_v4();
        state.pending.lock().await.insert(
            id,
            PendingPairing {
                id,
                name: "expired".into(),
                origin: "https://remote.example".into(),
                server_url: "https://remote.example/search".into(),
                created: Instant::now() - PAIRING_TTL - Duration::from_secs(1),
                approved: None,
                rejected: false,
            },
        );
        let request = Request::builder()
            .uri(format!("/v1/pairing/{id}"))
            .header(header::ORIGIN, "https://remote.example")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            router(state).oneshot(request).await.unwrap().status(),
            StatusCode::GONE
        );
    }

    /// Authenticated actions require both the paired origin and credential,
    /// use a mapped existing path, and fail immediately after revocation.
    #[tokio::test]
    async fn action_authentication_mapping_and_revocation() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("fixture.txt");
        std::fs::write(&file, "fixture").unwrap();
        let credential = "test-credential";
        let salt = SaltString::encode_b64(b"0123456789abcdef").unwrap();
        let hash = Argon2::default()
            .hash_password(credential.as_bytes(), &salt)
            .unwrap()
            .to_string();
        let instance_id = Uuid::new_v4();
        let invoked = Arc::new(AtomicBool::new(false));
        let invoked_for_action = invoked.clone();
        let state = Arc::new(RelayState::new(
            RelayConfig {
                enabled: true,
                bind: default_bind(),
                instances: vec![RelayInstance {
                    id: instance_id,
                    name: "remote".into(),
                    server_url: "https://remote.example/search".into(),
                    origins: vec!["https://remote.example".into()],
                    credential_hash: hash,
                    mappings: vec![PathMapping {
                        remote: "/remote".into(),
                        local: temp.path().display().to_string(),
                    }],
                }],
            },
            temp.path().join("relay.toml"),
            Arc::new(move |_, _| {
                invoked_for_action.store(true, Ordering::Release);
                Ok(())
            }),
        ));
        let action = || {
            Request::builder()
                .method("POST")
                .uri("/v1/actions")
                .header(header::ORIGIN, "https://remote.example")
                .header(header::AUTHORIZATION, format!("Bearer {credential}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({"action":"open_file","path":"/remote/fixture.txt"})
                        .to_string(),
                ))
                .unwrap()
        };
        let response = router(state.clone()).oneshot(action()).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(invoked.load(Ordering::Acquire));
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://remote.example"
        );
        state.revoke(instance_id).await.unwrap();
        assert_eq!(
            router(state).oneshot(action()).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }
}
