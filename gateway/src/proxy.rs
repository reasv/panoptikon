use anyhow::{bail, Context, Result};
use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{
        header::{self, HeaderName, HeaderValue},
        Request, Response, StatusCode, Uri,
    },
    response::IntoResponse,
};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use std::{net::SocketAddr, sync::Arc};

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
}

impl ProxyState {
    pub fn new(ui: Upstream, api: Upstream) -> Self {
        let client = Client::builder(TokioExecutor::new()).build_http();
        Self { client, ui, api }
    }
}

pub async fn proxy_ui(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<Arc<ProxyState>>,
    req: Request<Body>,
) -> impl IntoResponse {
    proxy_request(addr, state.client.clone(), state.ui.clone(), req).await
}

pub async fn proxy_api(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<Arc<ProxyState>>,
    req: Request<Body>,
) -> impl IntoResponse {
    proxy_request(addr, state.client.clone(), state.api.clone(), req).await
}

async fn proxy_request(
    client_addr: SocketAddr,
    client: Client<HttpConnector, Body>,
    upstream: Upstream,
    mut req: Request<Body>,
) -> Response<Body> {
    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/")
        .to_string();
    let method = req.method().clone();
    let original_host = req
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string());

    match build_upstream_request(
        &upstream,
        client_addr,
        original_host,
        &mut req,
        &path_and_query,
    ) {
        Ok(()) => {}
        Err(err) => {
            tracing::error!(error = %err, "failed to prepare upstream request");
            return StatusCode::BAD_GATEWAY.into_response();
        }
    }

    let response = match client.request(req).await {
        Ok(response) => response,
        Err(err) => {
            tracing::error!(error = %err, upstream = %upstream.name, "upstream request failed");
            return StatusCode::BAD_GATEWAY.into_response();
        }
    };

    let status = response.status();
    tracing::info!(
        method = %method,
        path = %path_and_query,
        upstream = %upstream.name,
        status = %status,
        "proxied request"
    );

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
        let value = HeaderValue::from_str(&original_host)
            .context("invalid original host header")?;
        req.headers_mut()
            .insert(HeaderName::from_static("x-forwarded-host"), value);
    }

    let forwarded_proto = req
        .uri()
        .scheme_str()
        .unwrap_or("http");
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
