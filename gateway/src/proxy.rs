use anyhow::{bail, Context, Result};
use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{
        header::{self, HeaderName, HeaderValue},
        Method, Request, Response, StatusCode, Uri,
    },
    response::IntoResponse,
};
use hyper::body::{to_bytes, Incoming};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};
use url::form_urlencoded;

use crate::config::{
    is_safe_identifier, DbPolicy, PolicyConfig, RuleConfig, Settings, MAX_DB_NAME_LEN,
    MAX_USERNAME_LEN,
};

#[derive(Clone)]
pub struct Upstream {
    name: String,
    base_uri: Uri,
}

impl Upstream {
    pub fn parse(name: &str, raw: &str) -> Result<Self> {
        let base_uri: Uri = raw.parse().context("invalid upstream URL")?;
        if base_uri.scheme().is_none() || base_uri.authority().is_none() {
            bail!("upstream URL must include scheme and authority");
        }
        Ok(Self {
            name: name.to_string(),
            base_uri,
        })
    }
}

pub struct ProxyState {
    pub client: Client<HttpConnector, Body>,
    pub ui: Upstream,
    pub api: Upstream,
    pub settings: Arc<Settings>,
}

impl ProxyState {
    pub fn new(ui: Upstream, api: Upstream, settings: Arc<Settings>) -> Self {
        let client = Client::builder(TokioExecutor::new()).build_http();
        Self {
            client,
            ui,
            api,
            settings,
        }
    }
}

pub async fn proxy_ui(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<Arc<ProxyState>>,
    req: Request<Body>,
) -> impl IntoResponse {
    proxy_request(addr, state, UpstreamKind::Ui, req).await
}

pub async fn proxy_api(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<Arc<ProxyState>>,
    req: Request<Body>,
) -> impl IntoResponse {
    proxy_request(addr, state, UpstreamKind::Api, req).await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UpstreamKind {
    Ui,
    Api,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DbAction {
    Skipped,
    Unchanged,
    Injected,
    Rewritten,
}

impl DbAction {
    fn combine(self, other: DbAction) -> DbAction {
        use DbAction::{Injected, Rewritten, Skipped, Unchanged};
        match (self, other) {
            (Rewritten, _) | (_, Rewritten) => Rewritten,
            (Injected, _) | (_, Injected) => Injected,
            (Unchanged, Unchanged) => Unchanged,
            (Skipped, other) => other,
            (other, Skipped) => other,
        }
    }
}

impl std::fmt::Display for DbAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let label = match self {
            DbAction::Skipped => "skipped",
            DbAction::Unchanged => "unchanged",
            DbAction::Injected => "injected",
            DbAction::Rewritten => "rewritten",
        };
        f.write_str(label)
    }
}

async fn proxy_request(
    client_addr: SocketAddr,
    state: Arc<ProxyState>,
    upstream_kind: UpstreamKind,
    mut req: Request<Body>,
) -> Response<Body> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let effective_host = resolve_effective_host(&req, state.settings.server.trust_forwarded_headers);
    let policy = match select_policy(&state.settings, effective_host.as_deref()) {
        Some(policy) => policy,
        None => {
            tracing::warn!(
                method = %method,
                path = %path,
                host = effective_host.as_deref().unwrap_or("<missing>"),
                "request denied: no policy matched"
            );
            return StatusCode::FORBIDDEN.into_response();
        }
    };

    if upstream_kind == UpstreamKind::Api {
        // Rulesets are intended to guard API surface without blocking the UI.
        if !ruleset_allows(&state.settings, policy, &method, &path) {
            tracing::warn!(
                method = %method,
                path = %path,
                policy = %policy.name,
                "request denied: ruleset"
            );
            return StatusCode::FORBIDDEN.into_response();
        }
    }

    let mut db_action = DbAction::Skipped;
    let is_db_info = upstream_kind == UpstreamKind::Api && is_db_info_path(&path);
    let is_db_create = upstream_kind == UpstreamKind::Api && is_db_create_path(&path);
    let apply_db_params = match upstream_kind {
        UpstreamKind::Api => needs_db_params(&path),
        UpstreamKind::Ui => true,
    };
    let needs_identity = apply_db_params || is_db_info || is_db_create;
    let username = if needs_identity {
        match extract_username(policy, &req) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(
                    method = %method,
                    path = %path,
                    policy = %policy.name,
                    reason = error.reason,
                    "request denied: invalid username"
                );
                return error.status.into_response();
            }
        }
    } else {
        None
    };

    if is_db_info {
        if let Err(err) = strip_query_params(&mut req, &["index_db", "user_data_db"]) {
            tracing::error!(error = %err, "failed to strip db params for db info");
            return StatusCode::BAD_GATEWAY.into_response();
        }
    }

    if apply_db_params {
        match enforce_db_params(policy, &mut req, username.as_deref()) {
            Ok(action) => db_action = action,
            Err(error) => {
                tracing::warn!(
                    method = %method,
                    path = %path,
                    policy = %policy.name,
                    reason = error.reason,
                    "request denied: db enforcement"
                );
                return error.status.into_response();
            }
        }
    }

    if is_db_create {
        match enforce_db_create_params(policy, &mut req, username.as_deref()) {
            Ok(action) => db_action = db_action.combine(action),
            Err(error) => {
                tracing::warn!(
                    method = %method,
                    path = %path,
                    policy = %policy.name,
                    reason = error.reason,
                    "request denied: db create enforcement"
                );
                return error.status.into_response();
            }
        }
    }

    let upstream = match upstream_kind {
        UpstreamKind::Ui => state.ui.clone(),
        UpstreamKind::Api => state.api.clone(),
    };

    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/")
        .to_string();
    let original_host = req
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string());

    if let Err(err) = build_upstream_request(
        &upstream,
        client_addr,
        original_host,
        &mut req,
        &path_and_query,
    ) {
        tracing::error!(error = %err, "failed to prepare upstream request");
        return StatusCode::BAD_GATEWAY.into_response();
    }

    let response = match state.client.request(req).await {
        Ok(response) => response,
        Err(err) => {
            tracing::error!(error = %err, upstream = %upstream.name, "upstream request failed");
            return StatusCode::BAD_GATEWAY.into_response();
        }
    };

    if is_db_info {
        let filtered = filter_db_info_response(response, policy, username.as_deref()).await;
        let status = filtered.status();
        tracing::info!(
            method = %method,
            path = %path_and_query,
            upstream = %upstream.name,
            status = %status,
            policy = %policy.name,
            db_params = %db_action,
            "proxied request"
        );
        return filtered;
    }

    let status = response.status();
    tracing::info!(
        method = %method,
        path = %path_and_query,
        upstream = %upstream.name,
        status = %status,
        policy = %policy.name,
        db_params = %db_action,
        "proxied request"
    );

    let (parts, body) = response.into_parts();
    Response::from_parts(parts, Body::new(body))
}

fn resolve_effective_host(req: &Request<Body>, trust_forwarded: bool) -> Option<String> {
    if trust_forwarded {
        if let Some(value) = header_to_str(req.headers().get("forwarded")) {
            if let Some(host) = parse_forwarded_host(value) {
                return Some(normalize_host(&host));
            }
        }
        if let Some(value) = header_to_str(req.headers().get("x-forwarded-host")) {
            let host = value.split(',').next().unwrap_or(value).trim();
            if !host.is_empty() {
                return Some(normalize_host(host));
            }
        }
    }

    header_to_str(req.headers().get(header::HOST)).map(normalize_host)
}

fn parse_forwarded_host(value: &str) -> Option<String> {
    let first = value.split(',').next()?.trim();
    for part in first.split(';') {
        let mut iter = part.trim().splitn(2, '=');
        let key = iter.next()?.trim();
        if !key.eq_ignore_ascii_case("host") {
            continue;
        }
        let mut host = iter.next()?.trim();
        if host.starts_with('"') && host.ends_with('"') && host.len() >= 2 {
            host = &host[1..host.len() - 1];
        }
        if host.is_empty() {
            return None;
        }
        return Some(host.to_string());
    }
    None
}

fn normalize_host(value: &str) -> String {
    let value = value.trim();
    if value.starts_with('[') {
        if let Some(end) = value.find(']') {
            return value[1..end].to_ascii_lowercase();
        }
    }
    value
        .split(':')
        .next()
        .unwrap_or(value)
        .to_ascii_lowercase()
}

fn ruleset_allows(settings: &Settings, policy: &PolicyConfig, method: &Method, path: &str) -> bool {
    let ruleset_name = match policy.ruleset.as_deref() {
        None => return true,
        Some("allow_all") => return true,
        Some(name) => name,
    };

    let ruleset = match settings.rulesets.get(ruleset_name) {
        Some(ruleset) => ruleset,
        None => return false,
    };

    if ruleset.allow_all {
        return true;
    }

    ruleset
        .allow
        .iter()
        .any(|rule| rule_matches(rule, method, path))
}

fn rule_matches(rule: &RuleConfig, method: &Method, path: &str) -> bool {
    if !rule.methods.allows(method) {
        return false;
    }
    if let Some(exact) = &rule.path {
        return path == exact;
    }
    if let Some(prefix) = &rule.path_prefix {
        return path.starts_with(prefix);
    }
    false
}

fn select_policy<'a>(settings: &'a Settings, host: Option<&str>) -> Option<&'a PolicyConfig> {
    let host = host?;
    for policy in &settings.policies {
        if policy
            .match_rule
            .hosts
            .iter()
            .any(|item| host_matches(item, host))
        {
            return Some(policy);
        }
    }
    None
}

fn host_matches(pattern: &str, host: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    normalize_host(pattern) == host
}

fn needs_db_params(path: &str) -> bool {
    if path == "/docs" || path == "/openapi.json" {
        return false;
    }
    if path == "/api/db" || path == "/api/db/create" {
        return false;
    }
    path == "/api" || path.starts_with("/api/")
}

fn is_db_info_path(path: &str) -> bool {
    path == "/api/db"
}

fn is_db_create_path(path: &str) -> bool {
    path == "/api/db/create"
}

struct EnforcementError {
    status: StatusCode,
    reason: &'static str,
}

#[derive(Debug, Serialize, Deserialize)]
struct DbInfo {
    index: SingleDbInfo,
    user_data: SingleDbInfo,
}

#[derive(Debug, Serialize, Deserialize)]
struct SingleDbInfo {
    current: String,
    all: Vec<String>,
}

fn enforce_db_params(
    policy: &PolicyConfig,
    req: &mut Request<Body>,
    username: Option<&str>,
) -> std::result::Result<DbAction, EnforcementError> {
    let mut pairs: Vec<(String, String)> = req
        .uri()
        .query()
        .map(|query| form_urlencoded::parse(query.as_bytes()).into_owned().collect())
        .unwrap_or_default();

    let mut retained: Vec<(String, String)> = Vec::with_capacity(pairs.len());
    let mut index_db = None;
    let mut user_data_db = None;

    for (key, value) in pairs.drain(..) {
        if key == "index_db" {
            if index_db.is_none() {
                index_db = Some(value);
            }
            continue;
        }
        if key == "user_data_db" {
            if user_data_db.is_none() {
                user_data_db = Some(value);
            }
            continue;
        }
        retained.push((key, value));
    }

    let index_resolution = resolve_db_param(
        "index_db",
        index_db,
        &policy.defaults.index_db,
        &policy.index_db,
        username,
    )?;
    let user_resolution = resolve_db_param(
        "user_data_db",
        user_data_db,
        &policy.defaults.user_data_db,
        &policy.user_data_db,
        username,
    )?;

    retained.push(("index_db".to_string(), index_resolution.value));
    retained.push(("user_data_db".to_string(), user_resolution.value));

    let query = build_query(retained);
    if let Err(err) = set_query(req, query.as_deref()) {
        tracing::error!(error = %err, "failed to apply db query params");
        return Err(EnforcementError {
            status: StatusCode::BAD_GATEWAY,
            reason: "query_rewrite_failed",
        });
    }

    Ok(index_resolution
        .action
        .combine(user_resolution.action))
}

fn enforce_db_create_params(
    policy: &PolicyConfig,
    req: &mut Request<Body>,
    username: Option<&str>,
) -> std::result::Result<DbAction, EnforcementError> {
    let mut pairs: Vec<(String, String)> = req
        .uri()
        .query()
        .map(|query| form_urlencoded::parse(query.as_bytes()).into_owned().collect())
        .unwrap_or_default();

    let mut retained: Vec<(String, String)> = Vec::with_capacity(pairs.len());
    let mut new_index_db = None;
    let mut new_user_data_db = None;

    for (key, value) in pairs.drain(..) {
        if key == "new_index_db" {
            if new_index_db.is_none() {
                new_index_db = Some(value);
            }
            continue;
        }
        if key == "new_user_data_db" {
            if new_user_data_db.is_none() {
                new_user_data_db = Some(value);
            }
            continue;
        }
        if key == "index_db" || key == "user_data_db" {
            continue;
        }
        retained.push((key, value));
    }

    let index_resolution = resolve_db_param(
        "new_index_db",
        new_index_db,
        &policy.defaults.index_db,
        &policy.index_db,
        username,
    )?;
    let user_resolution = resolve_db_param(
        "new_user_data_db",
        new_user_data_db,
        &policy.defaults.user_data_db,
        &policy.user_data_db,
        username,
    )?;

    retained.push(("new_index_db".to_string(), index_resolution.value));
    retained.push(("new_user_data_db".to_string(), user_resolution.value));

    let query = build_query(retained);
    if let Err(err) = set_query(req, query.as_deref()) {
        tracing::error!(error = %err, "failed to apply db create query params");
        return Err(EnforcementError {
            status: StatusCode::BAD_GATEWAY,
            reason: "query_rewrite_failed",
        });
    }

    Ok(index_resolution
        .action
        .combine(user_resolution.action))
}

async fn filter_db_info_response(
    response: Response<Incoming>,
    policy: &PolicyConfig,
    username: Option<&str>,
) -> Response<Body> {
    let status = response.status();
    let (mut parts, body) = response.into_parts();
    let bytes = match to_bytes(body).await {
        Ok(bytes) => bytes,
        Err(err) => {
            tracing::error!(error = %err, "failed to read db info response body");
            return StatusCode::BAD_GATEWAY.into_response();
        }
    };

    if !status.is_success() {
        return Response::from_parts(parts, Body::from(bytes));
    }

    let mut info: DbInfo = match serde_json::from_slice(&bytes) {
        Ok(info) => info,
        Err(err) => {
            tracing::warn!(error = %err, "failed to parse db info response");
            return Response::from_parts(parts, Body::from(bytes));
        }
    };

    let index_current = match resolve_default_db(&policy.index_db, &policy.defaults.index_db, username)
    {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(reason = error.reason, "invalid index db default");
            return error.status.into_response();
        }
    };
    let user_current =
        match resolve_default_db(&policy.user_data_db, &policy.defaults.user_data_db, username) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(reason = error.reason, "invalid user data db default");
                return error.status.into_response();
            }
        };

    info.index.current = index_current;
    info.user_data.current = user_current;
    info.index.all = filter_db_list(info.index.all, &policy.index_db, username);
    info.user_data.all = filter_db_list(info.user_data.all, &policy.user_data_db, username);

    let body = match serde_json::to_vec(&info) {
        Ok(bytes) => bytes,
        Err(err) => {
            tracing::warn!(error = %err, "failed to serialize filtered db info");
            return Response::from_parts(parts, Body::from(bytes));
        }
    };

    parts.headers.remove(header::CONTENT_LENGTH);
    tracing::info!(
        policy = %policy.name,
        "filtered db info response"
    );
    Response::from_parts(parts, Body::from(body))
}

struct DbResolution {
    value: String,
    action: DbAction,
}

fn resolve_db_param(
    _label: &'static str,
    provided: Option<String>,
    default_value: &str,
    policy: &DbPolicy,
    username: Option<&str>,
) -> std::result::Result<DbResolution, EnforcementError> {
    match provided {
        None => {
            let value = if let (Some(username), Some(template)) =
                (username, policy.tenant_default_template.as_deref())
            {
                // If {db} is present in the tenant default template, use the policy default.
                render_template(template, username, default_value)
            } else {
                default_value.to_string()
            };
            if !is_safe_identifier(&value, MAX_DB_NAME_LEN) {
                return Err(EnforcementError {
                    status: StatusCode::BAD_REQUEST,
                    reason: "invalid_db_value",
                });
            }
            Ok(DbResolution {
                value,
                action: DbAction::Injected,
            })
        }
        Some(value) => {
            if !is_safe_identifier(&value, MAX_DB_NAME_LEN) {
                return Err(EnforcementError {
                    status: StatusCode::BAD_REQUEST,
                    reason: "invalid_db_value",
                });
            }
            if policy.allow.allows(&value) {
                return Ok(DbResolution {
                    value,
                    action: DbAction::Unchanged,
                });
            }
            if let (Some(username), Some(template)) =
                (username, policy.tenant_template.as_deref())
            {
                let rewritten = render_template(template, username, &value);
                if !is_safe_identifier(&rewritten, MAX_DB_NAME_LEN) {
                    return Err(EnforcementError {
                        status: StatusCode::BAD_REQUEST,
                        reason: "invalid_db_value",
                    });
                }
                return Ok(DbResolution {
                    value: rewritten,
                    action: DbAction::Rewritten,
                });
            }
            Err(EnforcementError {
                status: StatusCode::FORBIDDEN,
                reason: "db_not_allowed",
            })
        }
    }
}

fn resolve_default_db(
    policy: &DbPolicy,
    default_value: &str,
    username: Option<&str>,
) -> std::result::Result<String, EnforcementError> {
    let value = if let (Some(username), Some(template)) =
        (username, policy.tenant_default_template.as_deref())
    {
        render_template(template, username, default_value)
    } else {
        default_value.to_string()
    };

    if !is_safe_identifier(&value, MAX_DB_NAME_LEN) {
        return Err(EnforcementError {
            status: StatusCode::BAD_REQUEST,
            reason: "invalid_db_value",
        });
    }
    Ok(value)
}

fn filter_db_list(
    names: Vec<String>,
    policy: &DbPolicy,
    username: Option<&str>,
) -> Vec<String> {
    if policy.allow.is_all() {
        return names;
    }
    names
        .into_iter()
        .filter(|name| {
            if policy.allow.allows(name) {
                return true;
            }
            let Some(username) = username else {
                return false;
            };
            matches_template(policy.tenant_template.as_deref(), username, name)
                || matches_template(policy.tenant_default_template.as_deref(), username, name)
        })
        .collect()
}

fn matches_template(template: Option<&str>, username: &str, candidate: &str) -> bool {
    let Some(template) = template else {
        return false;
    };
    if candidate.len() > MAX_DB_NAME_LEN {
        return false;
    }
    let with_user = template.replace("{username}", username);
    if let Some((prefix, suffix)) = with_user.split_once("{db}") {
        if !candidate.starts_with(prefix) || !candidate.ends_with(suffix) {
            return false;
        }
        if candidate.len() < prefix.len() + suffix.len() {
            return false;
        }
        let middle_len = candidate.len() - prefix.len() - suffix.len();
        let middle = &candidate[prefix.len()..prefix.len() + middle_len];
        return is_safe_identifier(middle, MAX_DB_NAME_LEN);
    }
    candidate == with_user
}

fn extract_username(
    policy: &PolicyConfig,
    req: &Request<Body>,
) -> std::result::Result<Option<String>, EnforcementError> {
    let identity = match &policy.identity {
        Some(identity) => identity,
        None => return Ok(None),
    };

    let header_value = match req.headers().get(&identity.user_header) {
        Some(value) => value,
        None => return Ok(None),
    };

    let raw = header_value.to_str().map_err(|_| EnforcementError {
        status: StatusCode::BAD_REQUEST,
        reason: "invalid_user_header",
    })?;
    let value = raw.split(',').next().unwrap_or(raw).trim();
    if value.is_empty() || !is_safe_identifier(value, MAX_USERNAME_LEN) {
        return Err(EnforcementError {
            status: StatusCode::BAD_REQUEST,
            reason: "invalid_username",
        });
    }

    Ok(Some(value.to_string()))
}

fn render_template(template: &str, username: &str, db: &str) -> String {
    template
        .replace("{username}", username)
        .replace("{db}", db)
}

fn strip_query_params(req: &mut Request<Body>, keys: &[&str]) -> Result<()> {
    let mut pairs: Vec<(String, String)> = req
        .uri()
        .query()
        .map(|query| form_urlencoded::parse(query.as_bytes()).into_owned().collect())
        .unwrap_or_default();
    let mut retained: Vec<(String, String)> = Vec::with_capacity(pairs.len());
    for (key, value) in pairs.drain(..) {
        if keys.iter().any(|entry| entry == &key) {
            continue;
        }
        retained.push((key, value));
    }
    let query = build_query(retained);
    set_query(req, query.as_deref())?;
    Ok(())
}

fn build_query(pairs: Vec<(String, String)>) -> Option<String> {
    if pairs.is_empty() {
        return None;
    }
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    for (key, value) in pairs {
        serializer.append_pair(&key, &value);
    }
    Some(serializer.finish())
}

fn set_query(req: &mut Request<Body>, query: Option<&str>) -> Result<()> {
    let path = req.uri().path();
    let path_and_query = match query {
        Some(query) if !query.is_empty() => format!("{}?{}", path, query),
        _ => path.to_string(),
    };

    let mut parts = req.uri().clone().into_parts();
    parts.path_and_query = Some(path_and_query.parse()?);
    *req.uri_mut() = Uri::from_parts(parts)?;
    Ok(())
}

fn header_to_str(value: Option<&HeaderValue>) -> Option<&str> {
    value.and_then(|value| value.to_str().ok())
}

fn build_upstream_request(
    upstream: &Upstream,
    client_addr: SocketAddr,
    original_host: Option<String>,
    req: &mut Request<Body>,
    path_and_query: &str,
) -> Result<()> {
    let new_uri = build_uri(&upstream.base_uri, path_and_query)?;
    *req.uri_mut() = new_uri;

    if let Some(authority) = upstream.base_uri.authority() {
        let value = HeaderValue::from_str(authority.as_str())
            .context("invalid upstream authority header")?;
        req.headers_mut().insert(header::HOST, value);
    }

    if let Some(original_host) = original_host {
        let value = HeaderValue::from_str(&original_host)
            .context("invalid original host header")?;
        req.headers_mut()
            .insert(HeaderName::from_static("x-forwarded-host"), value);
    }

    let forwarded_proto = req.uri().scheme_str().unwrap_or("http");
    let value = HeaderValue::from_str(forwarded_proto)
        .context("invalid x-forwarded-proto header")?;
    req.headers_mut()
        .insert(HeaderName::from_static("x-forwarded-proto"), value);

    append_forwarded_for(req.headers_mut(), client_addr)?;

    Ok(())
}

fn build_uri(base: &Uri, path_and_query: &str) -> Result<Uri> {
    let mut parts = base.clone().into_parts();
    parts.path_and_query = Some(path_and_query.parse()?);
    Ok(Uri::from_parts(parts)?)
}

fn append_forwarded_for(headers: &mut axum::http::HeaderMap, client_addr: SocketAddr) -> Result<()> {
    let name = HeaderName::from_static("x-forwarded-for");
    let client_ip = client_addr.ip();
    let value = match headers.get(&name).and_then(|value| value.to_str().ok()) {
        Some(existing) => format!("{}, {}", existing, client_ip),
        None => client_ip.to_string(),
    };
    headers.insert(name, HeaderValue::from_str(&value)?);
    Ok(())
}
