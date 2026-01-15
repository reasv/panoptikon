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
    pub settings: Arc<Settings>,
}

impl ProxyState {
    pub fn new(ui: Upstream, api: Upstream, inference: Upstream, settings: Arc<Settings>) -> Self {
        let client = Client::builder(TokioExecutor::new()).build_http();
        Self {
            client,
            ui,
            api,
            inference,
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
