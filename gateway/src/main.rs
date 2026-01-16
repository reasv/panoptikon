mod api_error;
mod config;
mod db;
mod api;
mod policy;
mod proxy;

use axum::{
    Router,
    routing::{any, get},
};
use clap::Parser;
use std::{env, net::SocketAddr, path::PathBuf, sync::Arc};
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "panoptikon-gateway",
    about = "Panoptikon reverse proxy gateway"
)]
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
    let inference_config = settings
        .upstreams
        .inference
        .as_ref()
        .expect("inference upstream should be initialized");
    let inference_upstream = proxy::Upstream::parse("inference", &inference_config.base_url)?;
    let state = Arc::new(proxy::ProxyState::new(
        ui_upstream,
        api_upstream,
        inference_upstream,
    ));

    let mut app = Router::new()
        .route("/api/inference", any(proxy::proxy_inference))
        .route("/api/inference/{*path}", any(proxy::proxy_inference))
        .route("/api", any(proxy::proxy_api))
        .route("/api/{*path}", any(proxy::proxy_api))
        .route("/docs", any(proxy::proxy_api))
        .route("/openapi.json", any(proxy::proxy_api))
        .fallback(any(proxy::proxy_ui));

    if settings.upstreams.api.local {
        app = app
            .route("/api/db", get(api::db::db_info))
            .route("/api/items/item/file", get(api::items::item_file))
            .route("/api/items/item/thumbnail", get(api::items::item_thumbnail))
            .route("/api/items/item", get(api::items::item_meta))
            .route("/api/items/item/text", get(api::items::item_text))
            .route("/api/items/item/tags", get(api::items::item_tags))
            .route("/api/items/text/any", get(api::items::texts_any));
    }

    let app = app
        .with_state(state)
        .layer(TraceLayer::new_for_http())
        .layer(policy::PolicyLayer::new(Arc::clone(&settings)));

    let listen_addr = settings.listen_addr();
    let listener = tokio::net::TcpListener::bind(&listen_addr).await?;
    tracing::info!(address = %listen_addr, "gateway listening");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}
