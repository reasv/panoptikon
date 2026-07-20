use anyhow::{Context, Result, bail};
use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{
        HeaderMap, Request, Response, StatusCode, Uri, Version,
        header::{self, HeaderName, HeaderValue},
    },
    response::IntoResponse,
};
use hyper::upgrade::OnUpgrade;
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::{TokioExecutor, TokioIo},
};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::watch;

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
    /// Becomes `true` on the first shutdown signal (main's watch channel).
    /// Upgrade bridges select on it: axum's graceful drain never waits on
    /// upgraded connections (hyper hands the raw socket off to the bridge
    /// task), so without this a live WebSocket would outlive cleanup.
    pub shutdown_rx: watch::Receiver<bool>,
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
        shutdown_rx: watch::Receiver<bool>,
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
            shutdown_rx,
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
pub(crate) const HOP_COUNT_HEADER: &str = "x-panoptikon-hops";
/// Legitimate gateway chains (e.g. forwarding inference to another
/// machine's gateway) are one or two hops deep; anything deeper is a
/// routing loop.
const MAX_PROXY_HOPS: u64 = 4;

/// Hop-by-hop headers (RFC 9110 §7.6.1 / RFC 7230 §6.1): connection-level
/// metadata a proxy must not forward. `Transfer-Encoding` is listed because
/// hyper re-frames bodies itself on each hop, so the inbound framing header
/// must not leak through; `Keep-Alive` and `Proxy-Connection` are the
/// legacy non-standard companions of `Connection`.
const HOP_BY_HOP_HEADERS: [HeaderName; 9] = [
    header::CONNECTION,
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-connection"),
    header::PROXY_AUTHENTICATE,
    header::PROXY_AUTHORIZATION,
    header::TE,
    header::TRAILER,
    header::TRANSFER_ENCODING,
    header::UPGRADE,
];

/// Remove hop-by-hop headers: first every header nominated by the message's
/// own `Connection` header, then the fixed RFC set (including `Connection`
/// itself). End-to-end headers — `Content-Length`, `Sec-WebSocket-*`, the
/// `x-forwarded-*` family — are untouched.
fn strip_hop_by_hop_headers(headers: &mut HeaderMap) {
    let nominated: Vec<HeaderName> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|token| HeaderName::from_bytes(token.trim().as_bytes()).ok())
        .collect();
    for name in nominated {
        headers.remove(name);
    }
    for name in &HOP_BY_HOP_HEADERS {
        headers.remove(name);
    }
}

/// Strip hop-by-hop headers, optionally preserving an upgrade handshake:
/// `Connection: upgrade` + `Upgrade: <protocol>` are hop-by-hop, but on the
/// upgrade path the next hop is exactly who the handshake is addressed to,
/// so they are re-inserted (canonicalized) after the strip.
fn sanitize_proxy_headers(headers: &mut HeaderMap, upgrade: Option<&HeaderValue>) {
    strip_hop_by_hop_headers(headers);
    if let Some(protocol) = upgrade {
        headers.insert(header::UPGRADE, protocol.clone());
        headers.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
    }
}

/// The protocol this message asks to upgrade to, when it carries a
/// well-formed handshake: a `Connection` header nominating `upgrade` AND an
/// `Upgrade` header naming the protocol. `None` for plain requests.
fn requested_upgrade(headers: &HeaderMap) -> Option<HeaderValue> {
    let nominates_upgrade = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|token| token.trim().eq_ignore_ascii_case("upgrade"));
    if !nominates_upgrade {
        return None;
    }
    headers.get(header::UPGRADE).cloned()
}

/// Bridge an upgraded client connection to the upgraded upstream connection
/// until either side closes or the gateway shuts down. Runs as a detached
/// task: the proxy handler has already returned the 101 by the time both
/// `OnUpgrade` futures resolve. The shutdown select is the drain bound —
/// upgraded sockets are invisible to axum's graceful shutdown, and cutting a
/// live WebSocket at shutdown is fine (clients reconnect); a task that keeps
/// the connection past cleanup is not.
fn spawn_upgrade_bridge(
    client: OnUpgrade,
    upstream_conn: OnUpgrade,
    mut shutdown_rx: watch::Receiver<bool>,
    upstream_name: String,
    path: String,
) {
    tokio::spawn(async move {
        let (client_io, upstream_io) = match tokio::try_join!(client, upstream_conn) {
            Ok(pair) => pair,
            Err(err) => {
                tracing::debug!(
                    error = %err,
                    upstream = %upstream_name,
                    path = %path,
                    "upgrade failed after 101 handshake"
                );
                return;
            }
        };
        let mut client_io = TokioIo::new(client_io);
        let mut upstream_io = TokioIo::new(upstream_io);
        tracing::debug!(upstream = %upstream_name, path = %path, "upgrade bridge open");
        tokio::select! {
            result = tokio::io::copy_bidirectional(&mut client_io, &mut upstream_io) => {
                match result {
                    Ok((to_upstream, to_client)) => tracing::debug!(
                        upstream = %upstream_name,
                        path = %path,
                        to_upstream,
                        to_client,
                        "upgrade bridge closed"
                    ),
                    Err(err) => tracing::debug!(
                        error = %err,
                        upstream = %upstream_name,
                        path = %path,
                        "upgrade bridge closed with error"
                    ),
                }
            }
            // Fires on the first shutdown signal. A dropped sender is NOT a
            // signal: it means no signal can ever arrive (the runtime is
            // being torn down, which reaps this task itself), so treat it
            // as never instead of killing a healthy bridge.
            _ = async {
                let signalled = shutdown_rx.wait_for(|stop| *stop).await.is_ok();
                if !signalled {
                    std::future::pending::<()>().await;
                }
            } => {
                tracing::debug!(
                    upstream = %upstream_name,
                    path = %path,
                    "upgrade bridge closed for shutdown"
                );
            }
        }
    });
}

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

    // Upgrade forwarding (WebSockets — e.g. Next.js dev HMR): honored only
    // when the server connection can actually hand us the raw client stream
    // (HTTP/1.1, where hyper leaves an OnUpgrade extension on the request).
    // A request that asks for an upgrade this connection cannot deliver is
    // forwarded as a plain request with the upgrade headers stripped.
    let client_upgrade = requested_upgrade(req.headers()).and_then(|protocol| {
        let on_upgrade = req.extensions_mut().remove::<OnUpgrade>()?;
        Some((protocol, on_upgrade))
    });

    // Hop-by-hop hygiene (RFC 9110 §7.6.1) runs BEFORE the gateway's own
    // header injections (hop count, policy token, x-forwarded-*), so a
    // client nominating one of those names in its Connection header cannot
    // strip what the gateway is about to set.
    sanitize_proxy_headers(
        req.headers_mut(),
        client_upgrade.as_ref().map(|(protocol, _)| protocol),
    );

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

    if client_upgrade.is_some() {
        // The upstream handshake must go out as HTTP/1.1: protocol upgrades
        // do not exist on h2 connections.
        *req.version_mut() = Version::HTTP_11;
    }

    let mut response = match state.client.request(req).await {
        Ok(response) => response,
        Err(err) => {
            tracing::error!(error = %err, upstream = %upstream.name, "upstream request failed");
            return StatusCode::BAD_GATEWAY.into_response();
        }
    };

    let status = response.status();
    if let Some(context) = policy_context {
        tracing::debug!(
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
        tracing::debug!(
            method = %method,
            path = %path_and_query,
            upstream = %upstream.name,
            status = %status,
            "proxied request"
        );
    }

    // Upstream accepted the upgrade: hand both raw connections to a bridge
    // task and answer the client with the (sanitized) 101 so hyper performs
    // the protocol switch on our side too. A non-101 answer to an upgrade
    // request falls through and flows back as a normal response.
    if status == StatusCode::SWITCHING_PROTOCOLS {
        let Some((_, client_upgrade)) = client_upgrade else {
            // A 101 to a request that never asked to switch protocols is
            // nonsensical: there is no client handshake to complete and
            // nothing to bridge. Reject it instead of relaying a bare 101.
            tracing::warn!(
                upstream = %upstream.name,
                path = %path_and_query,
                "upstream answered 101 to a non-upgrade request; returning 502"
            );
            return StatusCode::BAD_GATEWAY.into_response();
        };
        let upstream_upgrade = hyper::upgrade::on(&mut response);
        let (mut parts, _body) = response.into_parts();
        let protocol = parts.headers.get(header::UPGRADE).cloned();
        sanitize_proxy_headers(&mut parts.headers, protocol.as_ref());
        spawn_upgrade_bridge(
            client_upgrade,
            upstream_upgrade,
            state.shutdown_rx.clone(),
            upstream.name.clone(),
            path_and_query,
        );
        return Response::from_parts(parts, Body::empty());
    }

    let (mut parts, body) = response.into_parts();
    strip_hop_by_hop_headers(&mut parts.headers);
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

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
            watch::channel(false).1,
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
        assert_eq!(
            response.status().as_u16(),
            StatusCode::LOOP_DETECTED.as_u16()
        );
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
        let echo = axum::Router::new().fallback(any(|req: Request<Body>| async move {
            req.headers()
                .get(POLICY_TOKEN_HEADER)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("absent")
                .to_string()
        }));
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
            search_cache: true,
        });
        let response = proxy_request(client_addr, Arc::clone(&state), UpstreamKind::Ui, req).await;
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
        let response = proxy_request(client_addr, Arc::clone(&state), UpstreamKind::Ui, req).await;
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
            search_cache: true,
        });
        let response = proxy_request(client_addr, state, UpstreamKind::Api, req).await;
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), b"absent");
    }

    /// Hop-by-hop stripping table (RFC 9110 §7.6.1): the fixed set goes,
    /// headers nominated by `Connection` go (including custom ones), and
    /// end-to-end headers stay.
    #[test]
    fn strips_hop_by_hop_headers_table() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONNECTION,
            "keep-alive, x-custom-hop".parse().unwrap(),
        );
        headers.insert("keep-alive", "timeout=5".parse().unwrap());
        headers.insert("proxy-connection", "keep-alive".parse().unwrap());
        headers.insert(header::PROXY_AUTHENTICATE, "Basic".parse().unwrap());
        headers.insert(header::PROXY_AUTHORIZATION, "Basic abc".parse().unwrap());
        headers.insert(header::TE, "trailers".parse().unwrap());
        headers.insert(header::TRAILER, "Expires".parse().unwrap());
        headers.insert(header::TRANSFER_ENCODING, "chunked".parse().unwrap());
        headers.insert(header::UPGRADE, "websocket".parse().unwrap());
        headers.insert("x-custom-hop", "nominated by connection".parse().unwrap());
        headers.insert(header::CONTENT_LENGTH, "12".parse().unwrap());
        headers.insert("sec-websocket-key", "abc".parse().unwrap());
        headers.insert("x-end-to-end", "keep".parse().unwrap());

        strip_hop_by_hop_headers(&mut headers);

        let survivors: Vec<&str> = headers.keys().map(HeaderName::as_str).collect();
        let mut survivors_sorted = survivors.clone();
        survivors_sorted.sort_unstable();
        assert_eq!(
            survivors_sorted,
            vec!["content-length", "sec-websocket-key", "x-end-to-end"],
            "survivors: {survivors:?}"
        );
    }

    /// Upgrade-request detection: needs BOTH a Connection header nominating
    /// `upgrade` (any position, any case) and an Upgrade header.
    #[test]
    fn detects_upgrade_requests() {
        let build = |connection: Option<&str>, upgrade: Option<&str>| {
            let mut headers = HeaderMap::new();
            if let Some(connection) = connection {
                headers.insert(header::CONNECTION, connection.parse().unwrap());
            }
            if let Some(upgrade) = upgrade {
                headers.insert(header::UPGRADE, upgrade.parse().unwrap());
            }
            requested_upgrade(&headers)
        };

        assert_eq!(
            build(Some("Upgrade"), Some("websocket")),
            Some(HeaderValue::from_static("websocket"))
        );
        // Token among several, mixed case.
        assert!(build(Some("keep-alive, UPGRADE"), Some("websocket")).is_some());
        // Upgrade header without Connection nomination: not a handshake.
        assert_eq!(build(None, Some("websocket")), None);
        assert_eq!(build(Some("keep-alive"), Some("websocket")), None);
        // Connection: upgrade without an Upgrade header: not a handshake.
        assert_eq!(build(Some("upgrade"), None), None);
        // Plain request.
        assert_eq!(build(None, None), None);
    }

    /// On the handshake path the upgrade headers are preserved through the
    /// hop-by-hop strip (re-inserted canonically), while everything else
    /// hop-by-hop still goes and end-to-end handshake headers survive.
    #[test]
    fn preserves_upgrade_headers_on_handshake_path() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONNECTION, "keep-alive, Upgrade".parse().unwrap());
        headers.insert(header::UPGRADE, "websocket".parse().unwrap());
        headers.insert("keep-alive", "timeout=5".parse().unwrap());
        headers.insert(
            "sec-websocket-key",
            "dGhlIHNhbXBsZSBub25jZQ==".parse().unwrap(),
        );
        headers.insert("sec-websocket-version", "13".parse().unwrap());

        let protocol = requested_upgrade(&headers).expect("handshake detected");
        sanitize_proxy_headers(&mut headers, Some(&protocol));

        assert_eq!(
            headers.get(header::CONNECTION),
            Some(&HeaderValue::from_static("upgrade"))
        );
        assert_eq!(
            headers.get(header::UPGRADE),
            Some(&HeaderValue::from_static("websocket"))
        );
        assert!(headers.get("keep-alive").is_none());
        assert!(headers.get("sec-websocket-key").is_some());
        assert!(headers.get("sec-websocket-version").is_some());
    }

    /// Serve the test router (fallback → proxy_ui) on an ephemeral port,
    /// exactly like the production listeners (connect-info make service).
    async fn spawn_gateway(upstream_addr: SocketAddr) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let upstream = Upstream::parse("ui", &format!("http://{upstream_addr}")).unwrap();
        let app = axum::Router::new()
            .fallback(any(proxy_ui))
            .with_state(test_state(upstream));
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });
        addr
    }

    /// Read from `stream` until the end of an HTTP message head, returning
    /// the head as a string (any bytes past the head are discarded — callers
    /// only send/expect payload after acting on the head).
    async fn read_http_head(stream: &mut TcpStream) -> String {
        let mut head = Vec::new();
        let mut buf = [0u8; 1024];
        loop {
            let n = stream.read(&mut buf).await.unwrap();
            assert!(n > 0, "connection closed before end of HTTP head");
            head.extend_from_slice(&buf[..n]);
            if let Some(end) = head.windows(4).position(|w| w == b"\r\n\r\n") {
                head.truncate(end + 4);
                return String::from_utf8_lossy(&head).into_owned();
            }
        }
    }

    /// End-to-end upgrade bridging: a raw TCP upstream that answers 101 and
    /// echoes bytes, a real gateway in front of it, and a raw client that
    /// performs a WebSocket-style handshake through the gateway. Asserts the
    /// forwarded handshake headers, the echo through the bridge, and that
    /// closing the client propagates EOF to the upstream.
    #[tokio::test]
    async fn upgrade_requests_are_bridged_to_upstream() {
        // Upstream: accept one connection, send its request head to the
        // test, answer 101, echo payload bytes until EOF, then report EOF.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = listener.local_addr().unwrap();
        let (head_tx, head_rx) = tokio::sync::oneshot::channel::<String>();
        let (eof_tx, eof_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let head = read_http_head(&mut stream).await;
            head_tx.send(head).unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 101 Switching Protocols\r\n\
                      Upgrade: websocket\r\n\
                      Connection: Upgrade\r\n\
                      Sec-WebSocket-Accept: dGVzdC1hY2NlcHQ=\r\n\
                      Keep-Alive: timeout=5\r\n\r\n",
                )
                .await
                .unwrap();
            let mut buf = [0u8; 1024];
            loop {
                let n = stream.read(&mut buf).await.unwrap();
                if n == 0 {
                    eof_tx.send(()).unwrap();
                    return;
                }
                stream.write_all(&buf[..n]).await.unwrap();
            }
        });

        let gateway_addr = spawn_gateway(upstream_addr).await;

        // Client: raw WebSocket-style handshake through the gateway.
        let mut client = TcpStream::connect(gateway_addr).await.unwrap();
        client
            .write_all(
                b"GET /hmr HTTP/1.1\r\n\
                  Host: localhost\r\n\
                  Connection: keep-alive, Upgrade\r\n\
                  Upgrade: websocket\r\n\
                  Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                  Sec-WebSocket-Version: 13\r\n\
                  Keep-Alive: timeout=5\r\n\r\n",
            )
            .await
            .unwrap();

        let response_head = read_http_head(&mut client).await;
        assert!(
            response_head.starts_with("HTTP/1.1 101"),
            "expected 101 through the gateway, got: {response_head}"
        );
        let response_lower = response_head.to_ascii_lowercase();
        assert!(response_lower.contains("upgrade: websocket"));
        assert!(response_lower.contains("connection: upgrade"));
        assert!(response_lower.contains("sec-websocket-accept: dgvzdc1hy2nlchq="));
        // Upstream's hop-by-hop response header must not reach the client.
        assert!(!response_lower.contains("keep-alive:"));

        // The forwarded handshake: upgrade headers preserved, other
        // hop-by-hop stripped, gateway metadata injected.
        let forwarded = head_rx.await.unwrap().to_ascii_lowercase();
        assert!(forwarded.contains("connection: upgrade"));
        assert!(forwarded.contains("upgrade: websocket"));
        assert!(forwarded.contains("sec-websocket-key: dghlihnhbxbszsbub25jzq=="));
        assert!(!forwarded.contains("keep-alive:"));
        assert!(forwarded.contains("x-panoptikon-hops: 1"));
        assert!(forwarded.contains("x-forwarded-for: 127.0.0.1"));

        // Bytes flow both ways through the bridge.
        client.write_all(b"hello-bridge").await.unwrap();
        let mut echo = [0u8; 12];
        client.read_exact(&mut echo).await.unwrap();
        assert_eq!(&echo, b"hello-bridge");

        // Closing the client propagates to the upstream as EOF.
        drop(client);
        tokio::time::timeout(std::time::Duration::from_secs(5), eof_rx)
            .await
            .expect("upstream must see EOF after client close")
            .unwrap();
    }

    /// Plain (non-upgrade) responses get hop-by-hop headers stripped on the
    /// way back to the client — including Connection-nominated custom
    /// headers — while end-to-end headers and the body pass through.
    #[tokio::test]
    async fn plain_responses_are_stripped_of_hop_by_hop_headers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _head = read_http_head(&mut stream).await;
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\n\
                      Content-Length: 2\r\n\
                      Connection: x-hop\r\n\
                      X-Hop: nominated\r\n\
                      Keep-Alive: timeout=5\r\n\
                      X-Keep: end-to-end\r\n\r\nok",
                )
                .await
                .unwrap();
        });

        let gateway_addr = spawn_gateway(upstream_addr).await;
        let response = reqwest::get(format!("http://{gateway_addr}/page"))
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 200);
        assert!(response.headers().get("x-hop").is_none());
        assert!(response.headers().get("keep-alive").is_none());
        assert_eq!(
            response
                .headers()
                .get("x-keep")
                .and_then(|value| value.to_str().ok()),
            Some("end-to-end")
        );
        assert_eq!(response.text().await.unwrap(), "ok");
    }

    /// A 101 from the upstream to a request that never asked to switch
    /// protocols is nonsensical — there is no client handshake to complete —
    /// so the gateway coerces it to 502 instead of relaying a bare 101.
    #[tokio::test]
    async fn unrequested_101_is_coerced_to_502() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _head = read_http_head(&mut stream).await;
            stream
                .write_all(
                    b"HTTP/1.1 101 Switching Protocols\r\n\
                      Upgrade: websocket\r\n\
                      Connection: Upgrade\r\n\r\n",
                )
                .await
                .unwrap();
        });

        let gateway_addr = spawn_gateway(upstream_addr).await;
        // A plain GET: no Connection: upgrade, no Upgrade header.
        let response = reqwest::get(format!("http://{gateway_addr}/page"))
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 502);
    }

    /// Settings for the policy-layer tests: an open allow-all policy for
    /// localhost, and a "locked" policy for host denied.local whose ruleset
    /// admits nothing but GET /api/db.
    fn policy_settings() -> Arc<Settings> {
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

[rulesets.nothing]
allow = [{ methods = ["GET"], path = "/api/db" }]

[[policies]]
name = "locked"
ruleset = "nothing"

[policies.match]
hosts = ["denied.local"]

[policies.index_db]
default = "default"
allow = "*"

[policies.user_data_db]
default = "default"
allow = "*"

[[policies]]
name = "open"

[policies.match]
hosts = ["localhost", "127.0.0.1"]

[policies.index_db]
default = "default"
allow = "*"

[policies.user_data_db]
default = "default"
allow = "*"
"#,
        )
        .unwrap();
        Arc::new(Settings::load(Some(path)).unwrap())
    }

    /// Serve a gateway wrapped in the REAL PolicyLayer (like production):
    /// /api/* → proxy_api, everything else → proxy_ui, both pointing at
    /// `upstream_addr`.
    async fn spawn_policy_gateway(upstream_addr: SocketAddr) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let settings = policy_settings();
        let token_key = Arc::new(TokenKey::random());
        let upstream = Upstream::parse("ui", &format!("http://{upstream_addr}")).unwrap();
        let inference_client =
            InferenceApiClient::new_with_metadata_cache(format!("http://{upstream_addr}"), false)
                .unwrap();
        let state = Arc::new(ProxyState::new(
            upstream.clone(),
            upstream.clone(),
            upstream,
            inference_client,
            0,
            Arc::clone(&settings),
            Arc::clone(&token_key),
            watch::channel(false).1,
        ));
        let app = axum::Router::new()
            .route("/api/{*path}", any(proxy_api))
            .fallback(any(proxy_ui))
            .with_state(state)
            .layer(crate::policy::PolicyLayer::new(settings, token_key));
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });
        addr
    }

    /// The OnUpgrade extension survives the tower PolicyLayer: an upgrade
    /// request through the real policy stack still bridges, with the
    /// policy's work visible in the forwarded handshake (injected DB params
    /// and a minted policy token).
    #[tokio::test]
    async fn upgrade_bridges_through_the_policy_layer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = listener.local_addr().unwrap();
        let (head_tx, head_rx) = tokio::sync::oneshot::channel::<String>();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let head = read_http_head(&mut stream).await;
            head_tx.send(head).unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 101 Switching Protocols\r\n\
                      Upgrade: websocket\r\n\
                      Connection: Upgrade\r\n\r\n",
                )
                .await
                .unwrap();
            let mut buf = [0u8; 1024];
            loop {
                let n = stream.read(&mut buf).await.unwrap();
                if n == 0 {
                    return;
                }
                stream.write_all(&buf[..n]).await.unwrap();
            }
        });

        let gateway_addr = spawn_policy_gateway(upstream_addr).await;
        let mut client = TcpStream::connect(gateway_addr).await.unwrap();
        client
            .write_all(
                b"GET /hmr HTTP/1.1\r\n\
                  Host: localhost\r\n\
                  Connection: Upgrade\r\n\
                  Upgrade: websocket\r\n\
                  Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                  Sec-WebSocket-Version: 13\r\n\r\n",
            )
            .await
            .unwrap();

        let response_head = read_http_head(&mut client).await;
        assert!(
            response_head.starts_with("HTTP/1.1 101"),
            "expected 101 through the policy layer, got: {response_head}"
        );

        // The policy layer ran on this request: DB defaults injected into
        // the query and a policy token minted for the UI-bound handshake.
        let forwarded = head_rx.await.unwrap().to_ascii_lowercase();
        assert!(forwarded.contains("get /hmr?"), "head: {forwarded}");
        assert!(forwarded.contains("index_db=default"), "head: {forwarded}");
        assert!(
            forwarded.contains("x-panoptikon-policy:"),
            "head: {forwarded}"
        );

        // And the bridge still works end to end.
        client.write_all(b"policy-bridge").await.unwrap();
        let mut echo = [0u8; 13];
        client.read_exact(&mut echo).await.unwrap();
        assert_eq!(&echo, b"policy-bridge");
    }

    /// A ruleset-denied upgrade request on an API-surface path is rejected
    /// with 403 by the policy layer without the upstream ever seeing a
    /// connection.
    #[tokio::test]
    async fn ruleset_denied_upgrade_gets_403_without_touching_upstream() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = listener.local_addr().unwrap();
        let touched = Arc::new(AtomicBool::new(false));
        let touched_flag = Arc::clone(&touched);
        tokio::spawn(async move {
            let _ = listener.accept().await;
            touched_flag.store(true, Ordering::SeqCst);
        });

        let gateway_addr = spawn_policy_gateway(upstream_addr).await;
        let mut client = TcpStream::connect(gateway_addr).await.unwrap();
        client
            .write_all(
                b"GET /api/anything HTTP/1.1\r\n\
                  Host: denied.local\r\n\
                  Connection: Upgrade\r\n\
                  Upgrade: websocket\r\n\
                  Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                  Sec-WebSocket-Version: 13\r\n\r\n",
            )
            .await
            .unwrap();

        let response_head = read_http_head(&mut client).await;
        assert!(
            response_head.starts_with("HTTP/1.1 403"),
            "expected 403 from the ruleset, got: {response_head}"
        );
        // Give a would-be forwarded connection time to land, then assert
        // the upstream was never touched.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(
            !touched.load(Ordering::SeqCst),
            "upstream must not be contacted for a ruleset-denied request"
        );
    }
}
