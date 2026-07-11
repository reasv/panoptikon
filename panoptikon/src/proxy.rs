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

use crate::inferio_client::InferenceApiClient;
use crate::policy::PolicyContext;

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
}

impl ProxyState {
    pub fn new(
        ui: Upstream,
        api: Upstream,
        inference: Upstream,
        inference_client: InferenceApiClient,
        search_embedding_cache_size: usize,
    ) -> Self {
        let client = Client::builder(TokioExecutor::new()).build_http();
        Self {
            client,
            ui,
            api,
            inference,
            inference_client,
            search_embedding_cache_size,
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
const HOP_COUNT_HEADER: &str = "x-panoptikon-gateway-hops";
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

    // Regression test for the /api self-proxy recursion (2026-07-07): an
    // upstream that points back at the proxy's own listener must be cut off
    // by the hop guard instead of recursing until the ephemeral port range
    // is exhausted.
    #[tokio::test]
    async fn self_referential_upstream_is_cut_off() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let upstream = Upstream::parse("api", &format!("http://{addr}")).unwrap();
        let inference_client =
            InferenceApiClient::new_with_metadata_cache(format!("http://{addr}"), false).unwrap();
        let state = Arc::new(ProxyState::new(
            upstream.clone(),
            upstream.clone(),
            upstream,
            inference_client,
            0,
        ));
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
}
