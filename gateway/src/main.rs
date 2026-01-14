mod config;
mod proxy;

use axum::{routing::any, Router};
use clap::Parser;
use std::{env, net::SocketAddr, path::PathBuf, sync::Arc};
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "panoptikon-gateway", about = "Panoptikon reverse proxy gateway")]
struct Args {
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    let args = Args::parse();
    let config_path = args
        .config
        .or_else(|| env::var(config::CONFIG_PATH_ENV).ok().map(PathBuf::from));
    let settings = Arc::new(config::Settings::load(config_path)?);
    let ui_upstream = proxy::Upstream::parse("ui", &settings.upstreams.ui.base_url)?;
    let api_upstream = proxy::Upstream::parse("api", &settings.upstreams.api.base_url)?;
    let state = Arc::new(proxy::ProxyState::new(
        ui_upstream,
        api_upstream,
        Arc::clone(&settings),
    ));

    let app = Router::new()
        .route("/api", any(proxy::proxy_api))
        .route("/api/*path", any(proxy::proxy_api))
        .route("/docs", any(proxy::proxy_api))
        .route("/openapi.json", any(proxy::proxy_api))
        .fallback(any(proxy::proxy_ui))
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    let listen_addr = settings.listen_addr();
    let listener = tokio::net::TcpListener::bind(&listen_addr).await?;
    tracing::info!(address = %listen_addr, "gateway listening");
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await?;
    Ok(())
}
