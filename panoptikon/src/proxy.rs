use anyhow::{Context, Result, bail};
use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{
        Request, Response, StatusCode, Uri,
        header::{self, HeaderName, HeaderValue},
    },
    response::IntoResponse,
};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use std::{net::SocketAddr, sync::Arc};

use crate::config::Settings;
use crate::inferio_client::InferenceApiClient;
use crate::policy::PolicyContext;
use crate::policy_token::{POLICY_TOKEN_HEADER, TokenKey};

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
    pub inference: Upstream,
    pub inference_client: InferenceApiClient,
    pub search_embedding_cache_size: usize,
    /// Full gateway settings, for handlers that need policy/ruleset config
    /// (the /api/client-config capability derivation).
    pub settings: Arc<Settings>,
    /// Mints the `x-panoptikon-policy` token injected on UI-bound requests.
    pub token_key: Arc<TokenKey>,
}

impl ProxyState {
    pub fn new(
        ui: Upstream,
        api: Upstream,
        inference: Upstream,
        inference_client: InferenceApiClient,
        search_embedding_cache_size: usize,
        settings: Arc<Settings>,
        token_key: Arc<TokenKey>,
    ) -> Self {
        let client = Client::builder(TokioExecutor::new()).build_http();
        Self {
            client,
            ui,
            api,
            inference,
            inference_client,
            search_embedding_cache_size,
            settings,
            token_key,
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

pub async fn proxy_inference(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<Arc<ProxyState>>,
    req: Request<Body>,
) -> impl IntoResponse {
    proxy_request(addr, state, UpstreamKind::Inference, req).await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UpstreamKind {
    Ui,
    Api,
    Inference,
}

/// Counts how many times a request has already passed through a panoptikon
/// gateway, so a self-referential upstream (a base_url pointing back at one
/// of our own listeners) is cut off after a few hops instead of recursing
/// until the ephemeral port range is exhausted.
pub(crate) const HOP_COUNT_HEADER: &str = "x-panoptikon-gateway-hops";
/// Legitimate gateway chains (e.g. forwarding inference to another
/// machine's gateway) are one or two hops deep; anything deeper is a
/// routing loop.
const MAX_PROXY_HOPS: u64 = 4;

async fn proxy_request(
    client_addr: SocketAddr,
    state: Arc<ProxyState>,
    upstream_kind: UpstreamKind,
    mut req: Request<Body>,
) -> Response<Body> {
    let method = req.method().clone();
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
    let policy_context = req.extensions().get::<PolicyContext>().cloned();

    let upstream = match upstream_kind {
        UpstreamKind::Ui => state.ui.clone(),
        UpstreamKind::Api => state.api.clone(),
        UpstreamKind::Inference => state.inference.clone(),
    };

    let hops = req
        .headers()
        .get(HOP_COUNT_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    if hops >= MAX_PROXY_HOPS {
        tracing::error!(
            upstream = %upstream.name,
            path = %path_and_query,
            hops,
            "proxy loop detected; refusing to forward"
        );
        return StatusCode::LOOP_DETECTED.into_response();
    }
    req.headers_mut().insert(
        HeaderName::from_static(HOP_COUNT_HEADER),
        HeaderValue::from(hops + 1),
    );

    // Policy token injection (policy_token.rs): UI-bound requests carry a
    // short-lived signed token naming the policy the policy layer matched
    // for THIS request, so the Next.js server's SSR API calls back into the
    // gateway inherit exactly that policy instead of whatever its own
    // network position would match. Any inbound value was already
    // verified-and-consumed at policy ingress, so this insert cannot be
    // forwarding a client header.
    if upstream_kind == UpstreamKind::Ui {
        if let Some(context) = &policy_context {
            let token = state.token_key.mint(&context.policy_name);
            match HeaderValue::from_str(&token) {
                Ok(value) => {
                    req.headers_mut()
                        .insert(HeaderName::from_static(POLICY_TOKEN_HEADER), value);
                }
                Err(err) => tracing::warn!(
                    error = %err,
                    policy = %context.policy_name,
                    "policy token not header-safe; UI request sent without one"
                ),
            }
        }
    }

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

    let status = response.status();
    if let Some(context) = policy_context {
        tracing::info!(
            method = %method,
            path = %path_and_query,
            upstream = %upstream.name,
            status = %status,
            policy = %context.policy_name,
            selected_by = %context.selected_by,
            db_params = %context.db_action,
            "proxied request"
        );
    } else {
        tracing::info!(
            method = %method,
            path = %path_and_query,
            upstream = %upstream.name,
            status = %status,
            "proxied request"
        );
    }

    let (parts, body) = response.into_parts();
    Response::from_parts(parts, Body::new(body))
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
        let value =
            HeaderValue::from_str(&original_host).context("invalid original host header")?;
        req.headers_mut()
            .insert(HeaderName::from_static("x-forwarded-host"), value);
    }

    let forwarded_proto = req.uri().scheme_str().unwrap_or("http");
    let value =
        HeaderValue::from_str(forwarded_proto).context("invalid x-forwarded-proto header")?;
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

fn append_forwarded_for(
    headers: &mut axum::http::HeaderMap,
    client_addr: SocketAddr,
) -> Result<()> {
    let name = HeaderName::from_static("x-forwarded-for");
    let client_ip = client_addr.ip();
    let value = match headers.get(&name).and_then(|value| value.to_str().ok()) {
        Some(existing) => format!("{}, {}", existing, client_ip),
        None => client_ip.to_string(),
    };
    headers.insert(name, HeaderValue::from_str(&value)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::any;

    /// Minimal settings for constructing a ProxyState in tests.
    fn test_settings() -> Arc<Settings> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gw.toml");
        std::fs::write(
            &path,
            r#"
[server]
host = "127.0.0.1"
port = 9155

[upstreams.ui]
base_url = "http://127.0.0.1:6339"

[upstreams.api]
base_url = "http://127.0.0.1:6342"
"#,
        )
        .unwrap();
        Arc::new(Settings::load(Some(path)).unwrap())
    }

    fn test_state(upstream: Upstream) -> Arc<ProxyState> {
        let inference_client = InferenceApiClient::new_with_metadata_cache(
            format!("http://{}", upstream.base_uri.authority().unwrap()),
            false,
        )
        .unwrap();
        Arc::new(ProxyState::new(
            upstream.clone(),
            upstream.clone(),
            upstream,
            inference_client,
            0,
            test_settings(),
            Arc::new(TokenKey::random()),
        ))
    }

    // Regression test for the /api self-proxy recursion (2026-07-07): an
    // upstream that points back at the proxy's own listener must be cut off
    // by the hop guard instead of recursing until the ephemeral port range
    // is exhausted.
    #[tokio::test]
    async fn self_referential_upstream_is_cut_off() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let upstream = Upstream::parse("api", &format!("http://{addr}")).unwrap();
        let state = test_state(upstream);
        let app = axum::Router::new()
            .route("/api/{*path}", any(proxy_api))
            .with_state(state);
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });
        let response = reqwest::get(format!("http://{addr}/api/no-such-route"))
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), StatusCode::LOOP_DETECTED.as_u16());
    }

    /// UI-bound proxied requests carry a freshly minted policy token naming
    /// the policy the policy layer matched (from the PolicyContext
    /// extension); requests without a PolicyContext get none, and API-bound
    /// requests never get one.
    #[tokio::test]
    async fn ui_requests_carry_a_minted_policy_token() {
        // Echo server: returns the received x-panoptikon-policy header (or
        // "absent") in the response body.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let echo = axum::Router::new().fallback(any(
            |req: Request<Body>| async move {
                req.headers()
                    .get(POLICY_TOKEN_HEADER)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("absent")
                    .to_string()
            },
        ));
        tokio::spawn(async move {
            axum::serve(listener, echo).await.unwrap();
        });

        let upstream = Upstream::parse("ui", &format!("http://{addr}")).unwrap();
        let state = test_state(upstream);
        let key = Arc::clone(&state.token_key);
        let client_addr: SocketAddr = "127.0.0.1:5555".parse().unwrap();

        // With a PolicyContext (as the policy layer would insert).
        let mut req = Request::builder()
            .uri("http://gateway/some/page")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut().insert(PolicyContext {
            policy_name: "demo".to_string(),
            db_action: crate::policy::DbAction::Skipped,
            selected_by: crate::policy::PolicySelection::ListenerHost,
        });
        let response =
            proxy_request(client_addr, Arc::clone(&state), UpstreamKind::Ui, req).await;
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        let token = String::from_utf8(body.to_vec()).unwrap();
        assert_eq!(key.verify(&token), Ok("demo"), "token: {token}");

        // Without a PolicyContext: no token minted.
        let req = Request::builder()
            .uri("http://gateway/some/page")
            .body(Body::empty())
            .unwrap();
        let response =
            proxy_request(client_addr, Arc::clone(&state), UpstreamKind::Ui, req).await;
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), b"absent");

        // API-bound requests never carry one, PolicyContext or not.
        let mut req = Request::builder()
            .uri("http://gateway/api/things")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut().insert(PolicyContext {
            policy_name: "demo".to_string(),
            db_action: crate::policy::DbAction::Skipped,
            selected_by: crate::policy::PolicySelection::ListenerHost,
        });
        let response = proxy_request(client_addr, state, UpstreamKind::Api, req).await;
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), b"absent");
    }
}
