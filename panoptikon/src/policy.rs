use axum::{
    body::Body,
    http::{Method, Request, Response, StatusCode, Uri, header},
    response::IntoResponse,
};
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tower::{Layer, Service};
use url::form_urlencoded;
use utoipa::ToSchema;

use crate::config::{
    DbPolicy, MAX_DB_NAME_LEN, MAX_USERNAME_LEN, PolicyConfig, RuleConfig, Settings,
    is_safe_identifier,
};
use crate::policy_token::{POLICY_TOKEN_HEADER, TokenKey};

const USERNAME_HASH_LEN: usize = 32;

/// Name of the listener endpoint the connection arrived on ("default" for
/// the primary `server.host`/`server.port` listener). Inserted as a request
/// extension by the per-listener `Extension` layer in main — it reflects the
/// physical TCP listener, so unlike the Host header it cannot be spoofed.
#[derive(Clone)]
pub(crate) struct ListenerEndpoint(pub(crate) Arc<str>);

#[derive(Clone)]
pub struct PolicyLayer {
    settings: Arc<Settings>,
    token_key: Arc<TokenKey>,
}

impl PolicyLayer {
    pub fn new(settings: Arc<Settings>, token_key: Arc<TokenKey>) -> Self {
        Self {
            settings,
            token_key,
        }
    }
}

#[derive(Clone)]
pub struct PolicyService<S> {
    inner: S,
    settings: Arc<Settings>,
    token_key: Arc<TokenKey>,
}

impl<S> Layer<S> for PolicyLayer {
    type Service = PolicyService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            settings: Arc::clone(&self.settings),
            token_key: Arc::clone(&self.token_key),
        }
    }
}

impl<S> Service<Request<Body>> for PolicyService<S>
where
    S: Service<Request<Body>, Response = Response<Body>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<Body>) -> Self::Future {
        let settings = Arc::clone(&self.settings);
        let token_key = Arc::clone(&self.token_key);
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let decision = match apply_policy(&mut req, &settings, &token_key) {
                Ok(decision) => decision,
                Err(err) => {
                    tracing::warn!(
                        method = %req.method(),
                        path = %req.uri().path(),
                        reason = err.reason,
                        "request denied: policy"
                    );
                    return Ok(err.status.into_response());
                }
            };

            let response = inner.call(req).await?;
            let response = if decision.is_db_info {
                filter_db_info_response(response, &decision.policy, decision.username.as_deref())
                    .await
            } else {
                response
            };

            let status = response.status();
            tracing::debug!(
                method = %decision.method,
                path = %decision.path,
                policy = %decision.policy.name,
                selected_by = %decision.selected_by,
                endpoint = decision.endpoint.as_deref().unwrap_or("-"),
                db_params = %decision.db_action,
                status = %status,
                "policy enforced"
            );

            Ok(response)
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DbAction {
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

/// How the request's policy was selected: a verified `x-panoptikon-policy`
/// token, or the normal listener/host matching. Logged with every request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PolicySelection {
    Token,
    ListenerHost,
}

impl std::fmt::Display for PolicySelection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            PolicySelection::Token => "token",
            PolicySelection::ListenerHost => "listener/host",
        })
    }
}

#[derive(Clone)]
pub(crate) struct PolicyContext {
    pub policy_name: String,
    pub db_action: DbAction,
    pub selected_by: PolicySelection,
    /// Resolved `search_cache` policy flag, carried here because handlers
    /// never see the full `PolicyConfig`.
    pub search_cache: bool,
}

struct PolicyDecision {
    policy: PolicyConfig,
    username: Option<String>,
    db_action: DbAction,
    is_db_info: bool,
    method: Method,
    path: String,
    endpoint: Option<Arc<str>>,
    selected_by: PolicySelection,
}

#[derive(Debug)]
pub(crate) struct EnforcementError {
    pub(crate) status: StatusCode,
    pub(crate) reason: &'static str,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub(crate) struct DbInfo {
    pub(crate) index: SingleDbInfo,
    pub(crate) user_data: SingleDbInfo,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub(crate) struct SingleDbInfo {
    pub(crate) current: String,
    pub(crate) all: Vec<String>,
}

fn apply_policy(
    req: &mut Request<Body>,
    settings: &Settings,
    token_key: &TokenKey,
) -> std::result::Result<PolicyDecision, EnforcementError> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    // Policy token first: the header is consumed whether or not it verifies,
    // and verification runs on the inbound value, before the hygiene strip.
    let token_policy = consume_policy_token(req, settings, token_key);
    // Ingress hygiene: drop every other client-supplied `x-panoptikon-*`
    // header at this choke point so clients cannot smuggle gateway-internal
    // metadata (see strip_inbound_panoptikon_headers for the one exemption).
    strip_inbound_panoptikon_headers(req.headers_mut());

    let effective_host = resolve_effective_host(req, settings.server.trust_forwarded_headers);
    let endpoint = req
        .extensions()
        .get::<ListenerEndpoint>()
        .map(|endpoint| Arc::clone(&endpoint.0));
    let (policy, selected_by) = match token_policy {
        Some(policy) => (policy, PolicySelection::Token),
        None => (
            select_policy(
                settings,
                effective_host.as_deref(),
                endpoint.as_ref().map(|endpoint| endpoint.as_ref()),
            )
            .ok_or(EnforcementError {
                status: StatusCode::FORBIDDEN,
                reason: "no_policy",
            })?,
            PolicySelection::ListenerHost,
        ),
    };
    let policy = policy.clone();

    let is_inference = is_inference_path(&path);
    let is_api = is_api_surface(&path);
    let is_db_info = is_db_info_path(&path);
    let is_db_create = is_db_create_path(&path);
    // Desktop-managed routes (secret reveal, installation-wide mutations) are
    // mounted only in managed mode and additionally require the matched policy
    // to opt into the Desktop client, so a future LAN listener cannot inherit
    // local Desktop authority even if its ruleset allows every route.
    if path.starts_with("/api/desktop/")
        && policy
            .client
            .get("desktop")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
    {
        return Err(EnforcementError {
            status: StatusCode::FORBIDDEN,
            reason: "desktop_policy_required",
        });
    }
    // GET /api/client-config is exempt from ruleset enforcement: a client
    // must always be able to ask what it may do, which is how restricted UIs
    // learn which controls to hide. Local API only — in proxied-API mode the
    // exemption would forward the path upstream past a restrictive ruleset.
    let is_client_config =
        settings.upstreams.api.local && method == Method::GET && is_client_config_path(&path);
    // Relay pairing bootstrap is capability-discovery state, not an action.
    // Its handlers still enforce the matched policy's relay_enabled switch.
    let is_relay_bootstrap = settings.upstreams.api.local
        && (path.starts_with("/api/relay/pairings/")
            || path.starts_with("/api/relay/pairing-operations/"));

    if is_api
        && !is_client_config
        && !is_relay_bootstrap
        && !ruleset_allows(settings, &policy, &method, &path)
    {
        return Err(EnforcementError {
            status: StatusCode::FORBIDDEN,
            reason: "ruleset_denied",
        });
    }

    let username = extract_username(&policy, req)?;

    if is_inference {
        strip_query_params(req, &["index_db", "user_data_db"])?;
    }
    if is_db_info {
        strip_query_params(req, &["index_db", "user_data_db"])?;
    }

    let mut db_action = DbAction::Skipped;
    let apply_db_params = if is_inference || is_db_info || is_db_create {
        false
    } else if is_client_config || is_relay_bootstrap {
        // Local-mode client-config takes no DB params (same gate as its
        // ruleset exemption above; in proxied-API mode the path is treated
        // like any other upstream API route).
        false
    } else if is_api {
        needs_db_params(&path)
    } else {
        // UI-bound requests (pages, assets, HMR) keep their query string
        // untouched. Injecting the policy's DB defaults here makes the Next.js
        // server SSR against a URL the browser never had, so every
        // nuqs-serialized href carries db params the client-side render omits
        // and React fails hydration on every page load. SSR does not need
        // them: the UI's own API calls come back through this gateway with the
        // echoed policy token, and this enforcement resolves them on that hop.
        false
    };

    if apply_db_params {
        db_action = enforce_db_params(&policy, req, username.as_deref())?;
    }

    if is_db_create {
        let action = enforce_db_create_params(&policy, req, username.as_deref())?;
        db_action = db_action.combine(action);
    }

    req.extensions_mut().insert(PolicyContext {
        policy_name: policy.name.clone(),
        db_action,
        selected_by,
        search_cache: policy.search_cache,
    });

    Ok(PolicyDecision {
        policy,
        username,
        db_action,
        is_db_info,
        method,
        path,
        endpoint,
        selected_by,
    })
}

/// Remove the `x-panoptikon-policy` header and, when it carries a valid token
/// naming a configured policy, return that policy. Any failure yields `None`
/// and selection falls back to listener/host matching. The header is consumed
/// in every case: it authenticates the gateway's *own* mint (policy_token.rs)
/// and must never proceed upstream or into local handlers.
fn consume_policy_token<'a>(
    req: &mut Request<Body>,
    settings: &'a Settings,
    token_key: &TokenKey,
) -> Option<&'a PolicyConfig> {
    let value = req.headers_mut().remove(POLICY_TOKEN_HEADER)?;
    let token = match value.to_str() {
        Ok(token) => token,
        Err(_) => {
            tracing::debug!(reason = "malformed", "policy token ignored");
            return None;
        }
    };
    // Only the policy claim matters here. The origin claim is routing advice
    // for the UI server (policy_token.rs), and a legitimate SSR call may
    // arrive on a different listener than the one it names, so it is neither
    // compared nor acted on.
    let name = match token_key.verify(token) {
        Ok(claims) => claims.policy,
        Err(err) => {
            tracing::debug!(reason = err.as_str(), "policy token ignored");
            return None;
        }
    };
    match settings.policies.iter().find(|policy| policy.name == name) {
        Some(policy) => Some(policy),
        None => {
            tracing::debug!(
                reason = "unknown-policy",
                policy = name,
                "policy token ignored"
            );
            None
        }
    }
}

/// Strip inbound `x-panoptikon-*` headers from client requests at the
/// policy-layer choke point, so gateway-internal metadata can only ever be
/// set by the gateway itself.
///
/// `x-panoptikon-hops` is the one exemption and is PRESERVED: it is the
/// self-proxy loop guard (see `proxy.rs` `MAX_PROXY_HOPS`), and gateway to
/// gateway forwarding re-enters this layer on the next gateway, so stripping
/// it would reset the count every hop. A client sending a bogus value can
/// only lower its own hop budget, never bypass the guard.
/// (`x-panoptikon-policy` is not handled here: `consume_policy_token` has
/// already verified-then-removed it before this runs.)
fn strip_inbound_panoptikon_headers(headers: &mut header::HeaderMap) {
    let doomed: Vec<header::HeaderName> = headers
        .keys()
        .filter(|name| {
            let name = name.as_str();
            name.starts_with("x-panoptikon-") && name != crate::proxy::HOP_COUNT_HEADER
        })
        .cloned()
        .collect();
    for name in doomed {
        headers.remove(name);
    }
}

fn is_api_surface(path: &str) -> bool {
    path == "/api"
        || path.starts_with("/api/")
        || path == "/docs"
        || path == "/redoc"
        || path == "/openapi.json"
}

fn is_inference_path(path: &str) -> bool {
    path == "/api/inference" || path.starts_with("/api/inference/")
}

fn is_db_info_path(path: &str) -> bool {
    path == "/api/db"
}

fn is_db_create_path(path: &str) -> bool {
    path == "/api/db/create"
}

fn is_client_config_path(path: &str) -> bool {
    path == "/api/client-config"
}

fn needs_db_params(path: &str) -> bool {
    if path == "/docs" || path == "/redoc" || path == "/openapi.json" {
        return false;
    }
    if is_db_info_path(path) || is_db_create_path(path) || is_inference_path(path) {
        return false;
    }
    // /api/client-config is handled by the caller: its DB-param skip is
    // gated on upstreams.api.local, like its ruleset exemption.
    path == "/api" || path.starts_with("/api/")
}

/// The authority a request claims for itself: `host[:port]`, verbatim — no
/// case folding, no port stripping, and any deprecated `userinfo@` prefix left
/// in place so a caller that must refuse one still sees it.
///
/// The request target's authority first (an HTTP/2 `:authority`, or an
/// HTTP/1.1 absolute-form target), then the `Host` header; `None` when the
/// request named one in neither place, which every caller reads as unknown
/// rather than as a match. The single definition of "the host this request is
/// for", shared by the policy layer and the Desktop bridge guard so that one
/// request cannot be judged by two different names.
///
/// See docs/inferio-transport.md "Request authority (policy.rs)".
pub(crate) fn request_authority<'a>(
    uri: &'a Uri,
    headers: &'a header::HeaderMap,
) -> Option<&'a str> {
    if let Some(authority) = uri.authority() {
        // An `Authority` is never the empty string, but one that is only a
        // port (`":8080"`) has an empty host; that names nothing, so fall
        // through rather than report an empty host.
        if !authority.host().is_empty() {
            return Some(authority.as_str());
        }
    }
    header_to_str(headers.get(header::HOST)).filter(|value| !value.trim().is_empty())
}

/// The host a request claims for itself, normalized (userinfo, port and any
/// IPv6 brackets removed, lowercased) for `[policies.match] hosts` comparison.
///
/// `Forwarded` / `X-Forwarded-Host` win, but only when `[server]
/// trust_forwarded_headers` is set — the reverse-proxy deployment; otherwise
/// whatever [`request_authority`] found. Reading the authority introduces no
/// new trust, and non-spoofable routing remains the listener endpoint
/// ([`ListenerEndpoint`]). A request with neither an authority nor a `Host`
/// stays hostless, and `select_policy` then matches only policies that state
/// no `hosts`.
///
/// See docs/inferio-transport.md "Request authority (policy.rs)".
fn resolve_effective_host(req: &Request<Body>, trust_forwarded: bool) -> Option<String> {
    if trust_forwarded {
        if let Some(value) = header_to_str(req.headers().get("forwarded"))
            && let Some(host) = parse_forwarded_host(value)
        {
            return Some(normalize_host(&host));
        }
        if let Some(value) = header_to_str(req.headers().get("x-forwarded-host")) {
            let host = value.split(',').next().unwrap_or(value).trim();
            if !host.is_empty() {
                return Some(normalize_host(host));
            }
        }
    }

    request_authority(req.uri(), req.headers()).map(normalize_host)
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

pub(crate) fn normalize_host(value: &str) -> String {
    let value = value.trim();
    // A deprecated `userinfo@` prefix (RFC 3986 §3.2.1) is not part of the
    // host; the host follows the last `@`, the split
    // `http::uri::Authority::host` makes.
    let value = match value.rfind('@') {
        Some(at) => &value[at + 1..],
        None => value,
    };
    if value.starts_with('[')
        && let Some(end) = value.find(']')
    {
        return value[1..end].to_ascii_lowercase();
    }
    value
        .split(':')
        .next()
        .unwrap_or(value)
        .to_ascii_lowercase()
}

pub(crate) fn ruleset_allows(
    settings: &Settings,
    policy: &PolicyConfig,
    method: &Method,
    path: &str,
) -> bool {
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

/// First policy (config order) matching the effective host and the listener
/// endpoint. An empty `hosts`/`endpoints` list matches anything, including an
/// unknown host/endpoint (`None`); a non-empty list requires a known value
/// that matches. `host` is what [`resolve_effective_host`] resolved.
pub(crate) fn select_policy<'a>(
    settings: &'a Settings,
    host: Option<&str>,
    endpoint: Option<&str>,
) -> Option<&'a PolicyConfig> {
    settings.policies.iter().find(|policy| {
        let hosts = &policy.match_rule.hosts;
        let host_ok = hosts.is_empty()
            || host.is_some_and(|host| hosts.iter().any(|item| host_matches(item, host)));
        let endpoints = &policy.match_rule.endpoints;
        let endpoint_ok = endpoints.is_empty()
            || endpoint.is_some_and(|endpoint| endpoints.iter().any(|item| item == endpoint));
        host_ok && endpoint_ok
    })
}

fn host_matches(pattern: &str, host: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    normalize_host(pattern) == host
}
fn enforce_db_params(
    policy: &PolicyConfig,
    req: &mut Request<Body>,
    username: Option<&str>,
) -> std::result::Result<DbAction, EnforcementError> {
    let mut pairs: Vec<(String, String)> = req
        .uri()
        .query()
        .map(|query| {
            form_urlencoded::parse(query.as_bytes())
                .into_owned()
                .collect()
        })
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
        &policy.index_db.default,
        &policy.index_db,
        username,
    )?;
    let user_resolution = resolve_db_param(
        "user_data_db",
        user_data_db,
        &policy.user_data_db.default,
        &policy.user_data_db,
        username,
    )?;

    retained.push(("index_db".to_string(), index_resolution.value));
    retained.push(("user_data_db".to_string(), user_resolution.value));

    let query = build_query(retained);
    if let Err(err) = set_query(req, query.as_deref()) {
        tracing::error!(
            reason = err.reason,
            status = %err.status,
            "failed to apply db query params"
        );
        return Err(EnforcementError {
            status: StatusCode::BAD_GATEWAY,
            reason: "query_rewrite_failed",
        });
    }

    Ok(index_resolution.action.combine(user_resolution.action))
}

fn enforce_db_create_params(
    policy: &PolicyConfig,
    req: &mut Request<Body>,
    username: Option<&str>,
) -> std::result::Result<DbAction, EnforcementError> {
    let mut pairs: Vec<(String, String)> = req
        .uri()
        .query()
        .map(|query| {
            form_urlencoded::parse(query.as_bytes())
                .into_owned()
                .collect()
        })
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
        &policy.index_db.default,
        &policy.index_db,
        username,
    )?;
    let user_resolution = resolve_db_param(
        "new_user_data_db",
        new_user_data_db,
        &policy.user_data_db.default,
        &policy.user_data_db,
        username,
    )?;

    retained.push(("new_index_db".to_string(), index_resolution.value));
    retained.push(("new_user_data_db".to_string(), user_resolution.value));

    let query = build_query(retained);
    if let Err(err) = set_query(req, query.as_deref()) {
        tracing::error!(
            reason = err.reason,
            status = %err.status,
            "failed to apply db create query params"
        );
        return Err(EnforcementError {
            status: StatusCode::BAD_GATEWAY,
            reason: "query_rewrite_failed",
        });
    }

    Ok(index_resolution.action.combine(user_resolution.action))
}

async fn filter_db_info_response(
    response: Response<Body>,
    policy: &PolicyConfig,
    username: Option<&str>,
) -> Response<Body> {
    let status = response.status();
    let (mut parts, body) = response.into_parts();
    let bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(err) => {
            tracing::error!(error = %err, "failed to read db info response body");
            return StatusCode::BAD_GATEWAY.into_response();
        }
    };

    if !status.is_success() {
        return Response::from_parts(parts, Body::from(bytes));
    }

    let info: DbInfo = match serde_json::from_slice(bytes.as_ref()) {
        Ok(info) => info,
        Err(err) => {
            tracing::warn!(error = %err, "failed to parse db info response");
            return Response::from_parts(parts, Body::from(bytes));
        }
    };

    let info = match filter_db_info_payload(info, policy, username) {
        Ok(info) => info,
        Err(error) => {
            tracing::warn!(reason = error.reason, "invalid db info payload");
            return error.status.into_response();
        }
    };

    let body = match serde_json::to_vec(&info) {
        Ok(bytes) => bytes,
        Err(err) => {
            tracing::warn!(error = %err, "failed to serialize filtered db info");
            return Response::from_parts(parts, Body::from(bytes));
        }
    };

    parts.headers.remove(header::CONTENT_LENGTH);
    Response::from_parts(parts, Body::from(body))
}

fn filter_db_info_payload(
    mut info: DbInfo,
    policy: &PolicyConfig,
    username: Option<&str>,
) -> std::result::Result<DbInfo, EnforcementError> {
    let index_current = resolve_default_db(&policy.index_db, &policy.index_db.default, username)?;
    let user_current =
        resolve_default_db(&policy.user_data_db, &policy.user_data_db.default, username)?;

    info.index.current = index_current;
    info.user_data.current = user_current;
    info.index.all = filter_db_list(info.index.all, &policy.index_db, username);
    info.user_data.all = filter_db_list(info.user_data.all, &policy.user_data_db, username);
    Ok(info)
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
            let value = if let (Some(username), Some(tenant_default)) =
                (username, policy.tenant_default.as_deref())
            {
                format!(
                    "{}{}",
                    render_prefix(
                        policy
                            .tenant_prefix_template
                            .as_deref()
                            .ok_or(EnforcementError {
                                status: StatusCode::BAD_REQUEST,
                                reason: "invalid_db_template",
                            })?,
                        username,
                    )?,
                    tenant_default
                )
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
            if let (Some(username), Some(prefix)) =
                (username, policy.tenant_prefix_template.as_deref())
            {
                let rewritten = format!("{}{}", render_prefix(prefix, username)?, value);
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
    let value = if let (Some(_username), Some(tenant_default)) =
        (username, policy.tenant_default.as_deref())
    {
        tenant_default.to_string()
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
fn filter_db_list(names: Vec<String>, policy: &DbPolicy, username: Option<&str>) -> Vec<String> {
    if policy.allow.is_all() {
        return names;
    }
    let mut filtered: Vec<String> = names
        .into_iter()
        .filter(|name| {
            if policy.allow.allows(name) {
                return true;
            }
            let Some(username) = username else {
                return false;
            };
            matches_prefix(policy.tenant_prefix_template.as_deref(), username, name)
        })
        .collect();

    if let (Some(_username), Some(tenant_default)) = (username, policy.tenant_default.as_deref())
        && !filtered.iter().any(|entry| entry == tenant_default)
    {
        filtered.push(tenant_default.to_string());
    }

    let mut deduped = Vec::with_capacity(filtered.len());
    for name in filtered {
        if let Some(stripped) = strip_tenant_prefix(&name, policy, username) {
            if !deduped.iter().any(|entry| entry == &stripped) {
                deduped.push(stripped);
            }
            continue;
        }
        if !deduped.iter().any(|entry| entry == &name) {
            deduped.push(name);
        }
    }

    deduped
}

fn matches_prefix(template: Option<&str>, username: &str, candidate: &str) -> bool {
    let Some(prefix) = template else {
        return false;
    };
    let Ok(prefix) = render_prefix(prefix, username) else {
        return false;
    };
    if candidate.len() <= prefix.len() || candidate.len() > MAX_DB_NAME_LEN {
        return false;
    }
    if !candidate.starts_with(&prefix) {
        return false;
    }
    let rest = &candidate[prefix.len()..];
    is_safe_identifier(rest, MAX_DB_NAME_LEN)
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
    if value.is_empty() {
        return Err(EnforcementError {
            status: StatusCode::BAD_REQUEST,
            reason: "invalid_username",
        });
    }

    let hash_len = USERNAME_HASH_LEN;
    let needs_hash =
        value.len() > hash_len.saturating_sub(2) || !is_safe_identifier(value, MAX_USERNAME_LEN);
    let normalized = if needs_hash {
        hash_username(value)
    } else {
        value.to_string()
    };

    Ok(Some(normalized))
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

fn set_query(req: &mut Request<Body>, query: Option<&str>) -> Result<(), EnforcementError> {
    let path = req.uri().path();
    let path_and_query = match query {
        Some(query) if !query.is_empty() => format!("{}?{}", path, query),
        _ => path.to_string(),
    };

    let mut parts = req.uri().clone().into_parts();
    parts.path_and_query = Some(path_and_query.parse().map_err(|_| EnforcementError {
        status: StatusCode::BAD_GATEWAY,
        reason: "query_rewrite_failed",
    })?);
    *req.uri_mut() = Uri::from_parts(parts).map_err(|_| EnforcementError {
        status: StatusCode::BAD_GATEWAY,
        reason: "query_rewrite_failed",
    })?;
    Ok(())
}

fn strip_query_params(req: &mut Request<Body>, keys: &[&str]) -> Result<(), EnforcementError> {
    let mut pairs: Vec<(String, String)> = req
        .uri()
        .query()
        .map(|query| {
            form_urlencoded::parse(query.as_bytes())
                .into_owned()
                .collect()
        })
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

fn render_prefix(template: &str, username: &str) -> std::result::Result<String, EnforcementError> {
    if template.contains("{db}") {
        return Err(EnforcementError {
            status: StatusCode::BAD_REQUEST,
            reason: "invalid_db_template",
        });
    }
    let rendered = template.replace("{username}", username);
    if !is_safe_identifier(&rendered, MAX_DB_NAME_LEN) {
        return Err(EnforcementError {
            status: StatusCode::BAD_REQUEST,
            reason: "invalid_db_value",
        });
    }
    Ok(rendered)
}

fn strip_tenant_prefix(value: &str, policy: &DbPolicy, username: Option<&str>) -> Option<String> {
    let username = username?;
    let template = policy.tenant_prefix_template.as_deref()?;
    let prefix = render_prefix(template, username).ok()?;
    if !value.starts_with(&prefix) {
        return None;
    }
    let rest = &value[prefix.len()..];
    if rest.is_empty() || !is_safe_identifier(rest, MAX_DB_NAME_LEN) {
        return None;
    }
    Some(rest.to_string())
}

fn hash_username(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    let mut output = String::with_capacity(USERNAME_HASH_LEN);
    for byte in digest.iter().take(16) {
        use std::fmt::Write;
        let _ = write!(&mut output, "{:02x}", byte);
    }
    output
}

fn header_to_str(value: Option<&header::HeaderValue>) -> Option<&str> {
    value.and_then(|value| value.to_str().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AllowList, IdentityConfig, PolicyMatch};
    use axum::http::Request;
    use std::collections::BTreeMap;

    fn policy_with(index_db: DbPolicy, user_data_db: DbPolicy) -> PolicyConfig {
        PolicyConfig {
            name: "test".to_string(),
            ruleset: None,
            match_rule: PolicyMatch {
                hosts: vec!["localhost".to_string()],
                endpoints: Vec::new(),
            },
            index_db,
            user_data_db,
            identity: None,
            search_cache: true,
            client: crate::config::default_client_table(),
        }
    }

    fn db_policy(
        default: &str,
        allow: AllowList,
        tenant_default: Option<&str>,
        tenant_prefix_template: Option<&str>,
    ) -> DbPolicy {
        DbPolicy {
            default: default.to_string(),
            allow,
            tenant_default: tenant_default.map(|value| value.to_string()),
            tenant_prefix_template: tenant_prefix_template.map(|value| value.to_string()),
        }
    }

    /// Settings with one extra endpoint ("test") and three policies probing
    /// the match semantics: hosts+endpoints (AND), endpoints-only, and the
    /// usual hosts-only localhost policy, in that order.
    const ENDPOINT_SETTINGS: &str = r#"
[server]
host = "127.0.0.1"
port = 9155

[[server.endpoints]]
name = "test"
port = 9156

[upstreams.ui]
base_url = "http://127.0.0.1:6339"

[upstreams.api]
base_url = "http://127.0.0.1:6342"

[[policies]]
name = "both"

[policies.match]
hosts = ["special.local"]
endpoints = ["test"]

[policies.index_db]
default = "bothdb"
allow = ["bothdb"]

[policies.user_data_db]
default = "bothdb"
allow = ["bothdb"]

[[policies]]
name = "test-endpoint"

[policies.match]
endpoints = ["test"]

[policies.index_db]
default = "testdb"
allow = ["testdb"]

[policies.user_data_db]
default = "testdb"
allow = ["testdb"]

[[policies]]
name = "localhost"

[policies.match]
hosts = ["localhost", "127.0.0.1"]

[policies.index_db]
default = "default"
allow = "*"

[policies.user_data_db]
default = "default"
allow = "*"
"#;

    fn endpoint_settings() -> Settings {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gw.toml");
        std::fs::write(&path, ENDPOINT_SETTINGS).unwrap();
        Settings::load(Some(path)).unwrap()
    }

    /// select_policy semantics: empty hosts/endpoints lists match anything
    /// (including an unknown value), non-empty lists require a match, both
    /// non-empty means AND, and config order decides among matches.
    #[test]
    fn select_policy_endpoint_semantics() {
        let settings = endpoint_settings();
        let name = |host: Option<&str>, endpoint: Option<&str>| {
            select_policy(&settings, host, endpoint).map(|policy| policy.name.as_str())
        };

        // hosts AND endpoints both required by the first policy.
        assert_eq!(name(Some("special.local"), Some("test")), Some("both"));
        // Right host, wrong endpoint: nothing matches at all.
        assert_eq!(name(Some("special.local"), Some("default")), None);
        // Endpoint-only policy matches any host on its endpoint...
        assert_eq!(name(Some("localhost"), Some("test")), Some("test-endpoint"));
        // ...including requests with no Host header at all.
        assert_eq!(name(None, Some("test")), Some("test-endpoint"));
        // The primary endpoint falls through to the hosts-only policy,
        // which still requires a known, matching host.
        assert_eq!(name(Some("localhost"), Some("default")), Some("localhost"));
        assert_eq!(name(None, Some("default")), None);
        // Unknown endpoint (e.g. no extension): endpoint-scoped policies
        // never match, hosts-only ones are unaffected.
        assert_eq!(name(Some("localhost"), None), Some("localhost"));
        assert_eq!(name(None, None), None);
    }

    /// Every source `resolve_effective_host` reads, and their precedence.
    /// The authority case is the HTTP/2 one: hyper builds the request URI
    /// from `:authority`, and an HTTP/2 request sends no `Host` header at
    /// all (RFC 9113 §8.3.1), so a gateway that only read `Host` saw every
    /// HTTP/2 request as hostless — including its own h2c self-calls to the
    /// local inference surface (run2 defect P1).
    #[test]
    fn effective_host_reads_authority_host_and_forwarded() {
        let resolve = |req: Request<Body>, trust: bool| resolve_effective_host(&req, trust);
        let req = |build: fn(axum::http::request::Builder) -> axum::http::request::Builder| {
            build(Request::builder()).body(Body::empty()).unwrap()
        };

        // HTTP/1.1 origin-form: no authority on the URI, `Host` is it.
        assert_eq!(
            resolve(
                req(|b| b.uri("/api/items").header("host", "Example.Local:8080")),
                false
            ),
            Some("example.local".to_string())
        );
        // HTTP/2: authority on the URI, no `Host` header anywhere.
        assert_eq!(
            resolve(req(|b| b.uri("http://Example.Local:8080/api/items")), false),
            Some("example.local".to_string())
        );
        // Both present and disagreeing is malformed per RFC 9113 §8.3.1;
        // the request target's authority is what both HTTP versions call
        // the authority, so it wins. Neither value is more trusted than the
        // other — both are chosen by the same client.
        assert_eq!(
            resolve(
                req(|b| b
                    .uri("http://authority.local/api/items")
                    .header("host", "header.local")),
                false
            ),
            Some("authority.local".to_string())
        );
        // IPv6 literal authority: brackets and port stripped, like `Host`.
        assert_eq!(
            resolve(req(|b| b.uri("http://[::1]:6342/api/items")), false),
            Some("::1".to_string())
        );
        // Userinfo (deprecated, and forbidden in `:authority`) is not the
        // host: `Authority::host()` skips it.
        assert_eq!(
            resolve(
                req(|b| b.uri("http://user@authority.local/api/items")),
                false
            ),
            Some("authority.local".to_string())
        );
        // Neither source: hostless, exactly as before.
        assert_eq!(resolve(req(|b| b.uri("/api/items")), false), None);

        // Trusted forwarded headers still outrank both — the reverse-proxy
        // deployment is unchanged by any of the above.
        assert_eq!(
            resolve(
                req(|b| b
                    .uri("http://authority.local/api/items")
                    .header("host", "header.local")
                    .header("x-forwarded-host", "front.local")),
                true
            ),
            Some("front.local".to_string())
        );
        assert_eq!(
            resolve(
                req(|b| b
                    .uri("http://authority.local/api/items")
                    .header("forwarded", "host=fwd.local;proto=https")),
                true
            ),
            Some("fwd.local".to_string())
        );
        // Untrusted, the same headers are ignored and the authority stands.
        assert_eq!(
            resolve(
                req(|b| b
                    .uri("http://authority.local/api/items")
                    .header("x-forwarded-host", "front.local")),
                false
            ),
            Some("authority.local".to_string())
        );

        // A `Host` carrying userinfo is normalized the same way an authority
        // is: the host is what follows the last `@`, so the two sources can
        // never resolve one request to two different names.
        assert_eq!(
            resolve(
                req(|b| b.uri("/api/items").header("host", "user@Header.Local:80")),
                false
            ),
            Some("header.local".to_string())
        );
        // An empty `Host` names nothing, so it is hostless rather than an
        // empty host that a `hosts = ["*"]` policy would match.
        assert_eq!(
            resolve(req(|b| b.uri("/api/items").header("host", "")), false),
            None
        );
    }

    /// `request_authority` is the shared source rule — the policy layer and
    /// the Desktop bridge guard both resolve a request's host through it, so
    /// an HTTP/2 request cannot be judged by one name here and another
    /// there. It is pinned separately from `resolve_effective_host` because
    /// it returns the authority *verbatim*: the Desktop guard compares a
    /// whole browser origin (scheme, host **and port**) against it and must
    /// still be able to refuse a userinfo prefix.
    #[test]
    fn request_authority_prefers_the_request_target_over_host() {
        let req = |build: fn(axum::http::request::Builder) -> axum::http::request::Builder| {
            build(Request::builder()).body(Body::empty()).unwrap()
        };
        let authority =
            |req: &Request<Body>| request_authority(req.uri(), req.headers()).map(str::to_string);

        // HTTP/1.1 origin-form: the URI has no authority, so `Host` is it,
        // and the port survives.
        assert_eq!(
            authority(&req(|b| b
                .uri("/api/items")
                .header("host", "Example.Local:8080"))),
            Some("Example.Local:8080".to_string())
        );
        // HTTP/2 (and HTTP/1.1 absolute-form): the request target's
        // authority, which outranks any `Host` that disagrees with it.
        assert_eq!(
            authority(&req(|b| b
                .uri("http://Authority.Local:6342/api/items")
                .header("host", "header.local:6342"))),
            Some("Authority.Local:6342".to_string())
        );
        // Verbatim means verbatim: an IPv6 literal keeps its brackets, and a
        // userinfo prefix is reported rather than quietly dropped, so the
        // Desktop guard can reject it.
        assert_eq!(
            authority(&req(|b| b.uri("http://[::1]:6342/api/items"))),
            Some("[::1]:6342".to_string())
        );
        assert_eq!(
            authority(&req(|b| b.uri("http://user@authority.local/api/items"))),
            Some("user@authority.local".to_string())
        );
        // A target that is only a port names no host: fall through to `Host`
        // rather than report an empty one.
        assert_eq!(
            authority(&req(|b| b
                .uri("http://:8080/api/items")
                .header("host", "header.local"))),
            Some("header.local".to_string())
        );
        // Neither source, and an empty `Host`, are both "no authority".
        assert_eq!(authority(&req(|b| b.uri("/api/items"))), None);
        assert_eq!(
            authority(&req(|b| b.uri("/api/items").header("host", ""))),
            None
        );
    }

    /// The same through apply_policy: a request carrying only an authority
    /// (the shape hyper hands over for HTTP/2) selects the host policy it
    /// names, instead of being refused 403 `no_policy`.
    #[test]
    fn authority_only_request_selects_the_host_policy() {
        let settings = endpoint_settings();
        let key = TokenKey::random();

        let mut req = Request::builder()
            .uri("http://localhost:6342/api/items")
            .body(Body::empty())
            .unwrap();
        let decision = apply_policy(&mut req, &settings, &key).unwrap();
        assert_eq!(decision.policy.name, "localhost");

        // An authority no policy states is still refused, as a `Host` of
        // the same name would be.
        let mut req = Request::builder()
            .uri("http://unknown.local/api/items")
            .body(Body::empty())
            .unwrap();
        let err = apply_policy(&mut req, &settings, &key)
            .err()
            .expect("an unstated authority must not match a host policy");
        assert_eq!(err.status, StatusCode::FORBIDDEN);
        assert_eq!(err.reason, "no_policy");
    }

    /// Settings for the h2c integration test: one hosts-only policy, no
    /// endpoint-scoped or hostless catch-all, so the effective host is the
    /// only thing that can select a policy.
    const H2C_SETTINGS: &str = r#"
[server]
host = "127.0.0.1"
port = 9155

[upstreams.ui]
base_url = "http://127.0.0.1:6339"

[upstreams.api]
base_url = "http://127.0.0.1:6342"

[rulesets.allow_all]
allow_all = true

[[policies]]
name = "named-host"
ruleset = "allow_all"

[policies.match]
hosts = ["authority.test"]

[policies.index_db]
default = "default"
allow = "*"

[policies.user_data_db]
default = "default"
allow = "*"
"#;

    /// The real PolicyLayer over a real `axum::serve` socket, reached by a
    /// real HTTP client on both versions: HTTP/1.1 with a `Host` header and
    /// HTTP/2 cleartext with prior knowledge (`:authority`, no `Host`) must
    /// resolve the same policy for the same URL. This is the gateway's own
    /// inference self-call shape (`inferio_client.rs`), which run2 turned
    /// h2c and which every `Host`-only host resolution answered 403
    /// `no_policy`.
    #[tokio::test]
    async fn h2c_authority_selects_the_same_policy_as_http1_host() {
        async fn echo_policy(axum::Extension(context): axum::Extension<PolicyContext>) -> String {
            context.policy_name
        }

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gw.toml");
        std::fs::write(&path, H2C_SETTINGS).unwrap();
        let settings = Arc::new(Settings::load(Some(path)).unwrap());
        let token_key = Arc::new(TokenKey::random());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // `axum::serve` builds hyper-util's *auto* connection builder, which
        // sniffs the HTTP/2 client preface — the same one production uses,
        // so both versions arrive on this one port.
        let app = axum::Router::new()
            .route("/api/items", axum::routing::get(echo_policy))
            .layer(PolicyLayer::new(settings, token_key));
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        // Both clients resolve the policy's stated host to this socket, so
        // the only difference between the two legs is the HTTP version —
        // and therefore whether the authority travels in `Host` or in
        // `:authority`.
        let http1 = reqwest::Client::builder()
            .resolve("authority.test", addr)
            .build()
            .unwrap();
        let h2c = reqwest::Client::builder()
            .http2_prior_knowledge()
            .resolve("authority.test", addr)
            .build()
            .unwrap();
        let named = format!("http://authority.test:{}/api/items", addr.port());
        let literal = format!("http://127.0.0.1:{}/api/items", addr.port());

        let response = http1.get(&named).send().await.unwrap();
        assert_eq!(response.version(), reqwest::Version::HTTP_11);
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(response.text().await.unwrap(), "named-host");

        let response = h2c.get(&named).send().await.unwrap();
        // Assert the version too: a silent fallback to HTTP/1.1 would make
        // the rest of this leg vacuous.
        assert_eq!(response.version(), reqwest::Version::HTTP_2);
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(response.text().await.unwrap(), "named-host");

        // ...and the authority is matched, not merely tolerated: a request
        // naming a host no policy states is refused on both versions.
        let response = http1.get(&literal).send().await.unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
        let response = h2c.get(&literal).send().await.unwrap();
        assert_eq!(response.version(), reqwest::Version::HTTP_2);
        assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
    }

    /// End-to-end through apply_policy: the ListenerEndpoint request
    /// extension routes a request to the endpoint-scoped policy (which
    /// injects that policy's DB defaults); without the extension the same
    /// request gets the hosts-only policy.
    #[test]
    fn endpoint_extension_drives_policy_and_db_defaults() {
        let settings = endpoint_settings();
        let key = TokenKey::random();

        let mut req = Request::builder()
            .uri("http://localhost/api/items")
            .header("host", "localhost")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut()
            .insert(ListenerEndpoint(Arc::from("test")));
        let decision = apply_policy(&mut req, &settings, &key).unwrap();
        assert_eq!(decision.policy.name, "test-endpoint");
        assert_eq!(decision.endpoint.as_deref(), Some("test"));
        assert_eq!(decision.selected_by, PolicySelection::ListenerHost);
        let query = parse_query(&req);
        assert_eq!(query.get("index_db").unwrap(), &vec!["testdb".to_string()]);
        assert_eq!(
            query.get("user_data_db").unwrap(),
            &vec!["testdb".to_string()]
        );

        let mut req = Request::builder()
            .uri("http://localhost/api/items")
            .header("host", "localhost")
            .body(Body::empty())
            .unwrap();
        let decision = apply_policy(&mut req, &settings, &key).unwrap();
        assert_eq!(decision.policy.name, "localhost");
        assert_eq!(decision.endpoint, None);
    }

    #[test]
    fn desktop_routes_require_an_explicit_desktop_policy() {
        let mut settings = endpoint_settings();
        settings
            .policies
            .iter_mut()
            .find(|policy| policy.name == "localhost")
            .unwrap()
            .client = serde_json::json!({"desktop": true});
        let key = TokenKey::random();

        let mut lan_request = Request::builder()
            .uri("http://localhost/api/desktop/external-inputs")
            .header("host", "localhost")
            .body(Body::empty())
            .unwrap();
        lan_request
            .extensions_mut()
            .insert(ListenerEndpoint(Arc::from("test")));
        let error = match apply_policy(&mut lan_request, &settings, &key) {
            Ok(_) => panic!("LAN policy unexpectedly received Desktop authority"),
            Err(error) => error,
        };
        assert_eq!(error.reason, "desktop_policy_required");

        let mut local_request = Request::builder()
            .uri("http://localhost/api/desktop/external-inputs")
            .header("host", "localhost")
            .body(Body::empty())
            .unwrap();
        local_request
            .extensions_mut()
            .insert(ListenerEndpoint(Arc::from("default")));
        assert!(apply_policy(&mut local_request, &settings, &key).is_ok());
    }

    /// A valid policy token overrides listener/host selection: the same
    /// localhost request that would match the "localhost" policy gets the
    /// token-named "both" policy instead (with its DB defaults injected),
    /// and the decision records the token mechanism. Invalid tokens of
    /// every stripe — forged (wrong key), expired, unknown policy name,
    /// garbage — fall back to normal selection.
    #[test]
    fn policy_token_overrides_selection_and_falls_back() {
        let settings = endpoint_settings();
        let key = TokenKey::random();

        let request_with_token = |token: &str| {
            Request::builder()
                .uri("http://localhost/api/items")
                .header("host", "localhost")
                .header(POLICY_TOKEN_HEADER, token)
                .body(Body::empty())
                .unwrap()
        };

        // Valid token naming a policy this request would never match by
        // host/endpoint ("both" needs host special.local AND endpoint test).
        let mut req = request_with_token(&key.mint("both", "http://127.0.0.1:9155"));
        let decision = apply_policy(&mut req, &settings, &key).unwrap();
        assert_eq!(decision.policy.name, "both");
        assert_eq!(decision.selected_by, PolicySelection::Token);
        let query = parse_query(&req);
        assert_eq!(query.get("index_db").unwrap(), &vec!["bothdb".to_string()]);
        // The token header was consumed before the request proceeds.
        assert!(req.headers().get(POLICY_TOKEN_HEADER).is_none());

        // Fallback cases: all select "localhost" via listener/host.
        let other_key = TokenKey::random();
        for bad in [
            other_key.mint("both", "http://127.0.0.1:9155"), // forged: wrong key
            key.sign("both", "http://127.0.0.1:9155", 42),   // expired long ago
            key.mint("no-such-policy", "http://127.0.0.1:9155"), // unknown policy name
            "total.garbage".to_string(),                     // malformed
            format!("both.{}.{}", u64::MAX, "ab".repeat(32)), // pre-origin format
        ] {
            let mut req = request_with_token(&bad);
            let decision = apply_policy(&mut req, &settings, &key).unwrap();
            assert_eq!(decision.policy.name, "localhost", "token: {bad}");
            assert_eq!(decision.selected_by, PolicySelection::ListenerHost);
            assert!(req.headers().get(POLICY_TOKEN_HEADER).is_none());
        }
    }

    /// The origin claim is routing advice for the UI, never a selection
    /// input: a valid token whose origin names a different listener than
    /// the one the SSR call arrived on (the `api_endpoint` / explicit
    /// PANOPTIKON_API_URL case) still selects the named policy, and one
    /// naming an origin no listener has is not an error either.
    #[test]
    fn token_origin_claim_does_not_affect_selection() {
        let settings = endpoint_settings();
        let key = TokenKey::random();
        for origin in [
            "http://127.0.0.1:9155",
            "http://127.0.0.1:9156",
            "http://[::1]:1",
            "http://nowhere.invalid:80",
        ] {
            let mut req = Request::builder()
                .uri("http://localhost/api/items")
                .header("host", "localhost")
                .header(POLICY_TOKEN_HEADER, key.mint("both", origin))
                .body(Body::empty())
                .unwrap();
            req.extensions_mut()
                .insert(ListenerEndpoint(Arc::from("default")));
            let decision = apply_policy(&mut req, &settings, &key).unwrap();
            assert_eq!(decision.policy.name, "both", "origin: {origin}");
            assert_eq!(decision.selected_by, PolicySelection::Token);
        }
    }

    /// The loopback URL the proxy mints into a token's origin claim, per
    /// listener name: the primary from `[server]`, extra listeners from
    /// `[[server.endpoints]]`, and nothing for an unknown name.
    #[test]
    fn endpoint_loopback_urls_follow_the_listeners() {
        let settings = endpoint_settings();
        assert_eq!(
            settings.endpoint_loopback_url("default").as_deref(),
            Some("http://127.0.0.1:9155")
        );
        assert_eq!(
            settings.endpoint_loopback_url("test").as_deref(),
            Some("http://127.0.0.1:9156")
        );
        assert_eq!(settings.endpoint_loopback_url("nope"), None);
        assert_eq!(settings.primary_loopback_url(), "http://127.0.0.1:9155");
    }

    /// Ingress hygiene: client-supplied `x-panoptikon-*` headers are
    /// stripped at the policy layer — except `x-panoptikon-hops`,
    /// the self-proxy loop guard, which must survive gateway→gateway
    /// forwarding with its value intact. Unrelated headers pass through.
    #[test]
    fn inbound_panoptikon_headers_are_stripped_except_hops() {
        let settings = endpoint_settings();
        let key = TokenKey::random();

        let mut req = Request::builder()
            .uri("http://localhost/api/items")
            .header("host", "localhost")
            .header("x-panoptikon-junk", "1")
            .header("x-panoptikon-policy", "not-even-a-token")
            .header("x-panoptikon-hops", "2")
            .header("x-unrelated", "keep")
            .body(Body::empty())
            .unwrap();
        apply_policy(&mut req, &settings, &key).unwrap();

        assert!(req.headers().get("x-panoptikon-junk").is_none());
        assert!(req.headers().get("x-panoptikon-policy").is_none());
        assert_eq!(
            req.headers()
                .get("x-panoptikon-hops")
                .and_then(|value| value.to_str().ok()),
            Some("2"),
            "hop count must survive with its value intact"
        );
        assert_eq!(
            req.headers()
                .get("x-unrelated")
                .and_then(|value| value.to_str().ok()),
            Some("keep")
        );
    }

    /// GET /api/client-config bypasses ruleset enforcement (a client must
    /// always be able to ask what it may do) and gets no DB params
    /// injected — but only when the API is served locally, because that is
    /// the only mode where the endpoint exists; with a proxied API the
    /// exemption would forward the path upstream past a restrictive
    /// ruleset, so it must not apply. The ruleset always still applies to
    /// everything else under the same restricted policy.
    #[test]
    fn client_config_is_exempt_from_rulesets_in_local_api_mode_only() {
        let settings_with_api = |api_local: bool| {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("gw.toml");
            std::fs::write(
                &path,
                format!(
                    r#"
[server]
host = "127.0.0.1"
port = 9155

[upstreams.ui]
base_url = "http://127.0.0.1:6339"

[upstreams.api]
base_url = "http://127.0.0.1:6342"
local = {api_local}

[rulesets.nothing]
allow = [{{ methods = ["GET"], path = "/api/db" }}]

[[policies]]
name = "locked"
ruleset = "nothing"

[policies.match]
hosts = ["localhost"]

[policies.index_db]
default = "default"
allow = ["default"]

[policies.user_data_db]
default = "default"
allow = ["default"]
"#
                ),
            )
            .unwrap();
            Settings::load(Some(path)).unwrap()
        };
        let key = TokenKey::random();

        // Local API: exempt from the ruleset, no DB params injected.
        let settings = settings_with_api(true);
        let mut req = Request::builder()
            .uri("http://localhost/api/client-config")
            .header("host", "localhost")
            .body(Body::empty())
            .unwrap();
        let decision = apply_policy(&mut req, &settings, &key).unwrap();
        assert_eq!(decision.policy.name, "locked");
        assert!(parse_query(&req).is_empty(), "no DB params injected");

        // Anything else under the same ruleset is still denied.
        let mut req = Request::builder()
            .uri("http://localhost/api/search/pql")
            .method(Method::POST)
            .header("host", "localhost")
            .body(Body::empty())
            .unwrap();
        let err = apply_policy(&mut req, &settings, &key)
            .err()
            .expect("restricted ruleset must deny search");
        assert_eq!(err.status, StatusCode::FORBIDDEN);
        assert_eq!(err.reason, "ruleset_denied");

        // Proxied API: no exemption — the restrictive ruleset applies to
        // the path like to any other upstream API route.
        let settings = settings_with_api(false);
        let mut req = Request::builder()
            .uri("http://localhost/api/client-config")
            .header("host", "localhost")
            .body(Body::empty())
            .unwrap();
        let err = apply_policy(&mut req, &settings, &key)
            .err()
            .expect("proxied-API mode must not exempt client-config");
        assert_eq!(err.status, StatusCode::FORBIDDEN);
        assert_eq!(err.reason, "ruleset_denied");
    }

    fn parse_query(req: &Request<Body>) -> BTreeMap<String, Vec<String>> {
        let mut map: BTreeMap<String, Vec<String>> = BTreeMap::new();
        if let Some(query) = req.uri().query() {
            for (key, value) in form_urlencoded::parse(query.as_bytes()).into_owned() {
                map.entry(key).or_default().push(value);
            }
        }
        map
    }

    #[test]
    // Ensures requests without DB params get both defaults injected, producing explicit
    // index_db + user_data_db query values and reporting an injected action.
    fn injects_defaults_without_username() {
        let policy = policy_with(
            db_policy(
                "default",
                AllowList::List(vec!["default".to_string()]),
                None,
                None,
            ),
            db_policy(
                "default",
                AllowList::List(vec!["default".to_string()]),
                None,
                None,
            ),
        );
        let mut req = Request::builder()
            .uri("http://localhost/api/items")
            .body(Body::empty())
            .unwrap();

        let action = enforce_db_params(&policy, &mut req, None).unwrap();
        let query = parse_query(&req);

        assert!(matches!(action, DbAction::Injected));
        assert_eq!(query.get("index_db").unwrap(), &vec!["default".to_string()]);
        assert_eq!(
            query.get("user_data_db").unwrap(),
            &vec!["default".to_string()]
        );
    }

    #[test]
    // Verifies a disallowed DB name is rewritten with the tenant prefix when a username is
    // provided, while allowed names remain untouched.
    fn rewrites_disallowed_with_prefix() {
        let policy = policy_with(
            db_policy(
                "default",
                AllowList::List(vec!["default".to_string()]),
                None,
                Some("user_{username}_"),
            ),
            db_policy(
                "default",
                AllowList::List(vec!["default".to_string()]),
                None,
                Some("user_{username}_"),
            ),
        );
        let mut req = Request::builder()
            .uri("http://localhost/api/items?index_db=private&user_data_db=default")
            .body(Body::empty())
            .unwrap();

        let action = enforce_db_params(&policy, &mut req, Some("alice")).unwrap();
        let query = parse_query(&req);

        assert!(matches!(action, DbAction::Rewritten));
        assert_eq!(
            query.get("index_db").unwrap(),
            &vec!["user_alice_private".to_string()]
        );
        assert_eq!(
            query.get("user_data_db").unwrap(),
            &vec!["default".to_string()]
        );
    }

    #[test]
    // Confirms tenant_default values are used when params are missing, and the defaults are
    // prefixed with the tenant prefix for both DB kinds.
    fn tenant_default_is_prefixed_on_missing_param() {
        let policy = policy_with(
            db_policy(
                "default",
                AllowList::List(vec!["default".to_string()]),
                Some("images"),
                Some("user_{username}_"),
            ),
            db_policy(
                "default",
                AllowList::List(vec!["default".to_string()]),
                Some("bookmarks"),
                Some("user_{username}_"),
            ),
        );
        let mut req = Request::builder()
            .uri("http://localhost/api/items")
            .body(Body::empty())
            .unwrap();

        let action = enforce_db_params(&policy, &mut req, Some("alice")).unwrap();
        let query = parse_query(&req);

        assert!(matches!(action, DbAction::Injected));
        assert_eq!(
            query.get("index_db").unwrap(),
            &vec!["user_alice_images".to_string()]
        );
        assert_eq!(
            query.get("user_data_db").unwrap(),
            &vec!["user_alice_bookmarks".to_string()]
        );
    }

    #[test]
    // Ensures disallowed DB names are rejected when no tenant prefix template is configured.
    fn rejects_disallowed_without_prefix() {
        let policy = policy_with(
            db_policy(
                "default",
                AllowList::List(vec!["default".to_string()]),
                None,
                None,
            ),
            db_policy(
                "default",
                AllowList::List(vec!["default".to_string()]),
                None,
                None,
            ),
        );
        let mut req = Request::builder()
            .uri("http://localhost/api/items?index_db=private")
            .body(Body::empty())
            .unwrap();

        let err = enforce_db_params(&policy, &mut req, None).unwrap_err();
        assert_eq!(err.status, StatusCode::FORBIDDEN);
    }

    #[test]
    // Validates /api/db/create query enforcement: rewrite new_* params with tenant prefix,
    // drop any index_db/user_data_db params, and report a rewritten action.
    fn enforces_db_create_params() {
        let policy = policy_with(
            db_policy(
                "default",
                AllowList::List(vec!["default".to_string()]),
                None,
                Some("user_{username}_"),
            ),
            db_policy(
                "default",
                AllowList::List(vec!["default".to_string()]),
                None,
                Some("user_{username}_"),
            ),
        );
        let mut req = Request::builder()
            .uri("http://localhost/api/db/create?new_index_db=private&index_db=default&new_user_data_db=bookmarks")
            .body(Body::empty())
            .unwrap();

        let action = enforce_db_create_params(&policy, &mut req, Some("alice")).unwrap();
        let query = parse_query(&req);

        assert!(matches!(action, DbAction::Rewritten));
        assert_eq!(
            query.get("new_index_db").unwrap(),
            &vec!["user_alice_private".to_string()]
        );
        assert_eq!(
            query.get("new_user_data_db").unwrap(),
            &vec!["user_alice_bookmarks".to_string()]
        );
        assert!(!query.contains_key("index_db"));
        assert!(!query.contains_key("user_data_db"));
    }

    #[test]
    // Filters /api/db output for a tenant: only allowed + tenant-prefixed DBs remain and
    // all prefixed names are stripped before returning to the client, including defaults.
    fn filters_db_info_and_strips_prefix() {
        let policy = policy_with(
            db_policy(
                "default",
                AllowList::List(vec!["default".to_string()]),
                Some("images"),
                Some("user_{username}_"),
            ),
            db_policy(
                "default",
                AllowList::List(vec!["default".to_string()]),
                Some("bookmarks"),
                Some("user_{username}_"),
            ),
        );
        let info = DbInfo {
            index: SingleDbInfo {
                current: "default".to_string(),
                all: vec![
                    "default".to_string(),
                    "user_alice_images".to_string(),
                    "user_alice_private".to_string(),
                    "user_bob_images".to_string(),
                ],
            },
            user_data: SingleDbInfo {
                current: "default".to_string(),
                all: vec![
                    "default".to_string(),
                    "user_alice_bookmarks".to_string(),
                    "user_bob_bookmarks".to_string(),
                ],
            },
        };

        let filtered = filter_db_info_payload(info, &policy, Some("alice")).unwrap();

        assert_eq!(filtered.index.current, "images");
        assert_eq!(filtered.user_data.current, "bookmarks");

        let mut index_all = filtered.index.all;
        index_all.sort();
        assert_eq!(
            index_all,
            vec![
                "default".to_string(),
                "images".to_string(),
                "private".to_string()
            ]
        );

        let mut user_all = filtered.user_data.all;
        user_all.sort();
        assert_eq!(
            user_all,
            vec!["bookmarks".to_string(), "default".to_string()]
        );
    }

    #[test]
    // Verifies unsafe usernames are hashed before use, and the resulting hash is used in
    // the tenant prefix when rewriting DB parameters.
    fn hashes_unsafe_username() {
        let mut policy = policy_with(
            db_policy(
                "default",
                AllowList::List(vec!["default".to_string()]),
                None,
                Some("user_{username}_"),
            ),
            db_policy(
                "default",
                AllowList::List(vec!["default".to_string()]),
                None,
                Some("user_{username}_"),
            ),
        );
        policy.identity = Some(IdentityConfig {
            user_header: "X-Forwarded-User".to_string(),
        });
        let unsafe_name = "alice@example.com";
        let hashed = hash_username(unsafe_name);
        let mut req = Request::builder()
            .uri("http://localhost/api/items?index_db=private")
            .header("X-Forwarded-User", unsafe_name)
            .body(Body::empty())
            .unwrap();

        let username = extract_username(&policy, &req).unwrap().unwrap();
        assert_eq!(username, hashed);
        let action = enforce_db_params(&policy, &mut req, Some(&username)).unwrap();
        let query = parse_query(&req);

        assert!(matches!(action, DbAction::Rewritten));
        assert_eq!(
            query.get("index_db").unwrap(),
            &vec![format!("user_{}_private", hashed)]
        );
    }
}
