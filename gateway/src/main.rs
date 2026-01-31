mod api;
mod api_error;
mod config;
mod db;
mod inferio_client;
mod jobs;
mod openapi;
mod policy;
mod proxy;
mod pql;
#[cfg(test)]
mod test_utils;

use axum::{
    Router,
    routing::{any, get, post},
};
use clap::Parser;
use std::{env, net::SocketAddr, path::PathBuf, sync::Arc};
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

fn env_truthy(key: &str) -> bool {
    match env::var(key) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" | "" => false,
            _ => false,
        },
        Err(_) => false,
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "panoptikon-gateway",
    about = "Panoptikon reverse proxy gateway"
)]
struct Args {
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,
}

fn main() -> anyhow::Result<()> {
    // Build a custom tokio runtime with a larger worker thread stack size.
    // The default 2MB stack can be insufficient for deeply nested async code,
    // especially in debug builds where stack frames are larger due to unoptimized
    // code and extra debug info.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(8 * 1024 * 1024) // 8MB stack for worker threads
        .build()?;

    runtime.block_on(async_main())
}

async fn async_main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
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
    let inference_client =
        inferio_client::InferenceApiClient::from_settings_with_metadata_cache(&settings, true)?;
    let state = Arc::new(proxy::ProxyState::new(
        ui_upstream,
        api_upstream,
        inference_upstream,
        inference_client,
        settings.search.embedding_cache_size,
    ));

    if env_truthy("EXPERIMENTAL_RUST_DB_AUTO_MIGRATIONS") {
        db::migrations::migrate_all_databases_on_disk().await?;
    }

    let mut app = Router::new()
        .route("/api/inference", any(proxy::proxy_inference))
        .route("/api/inference/{*path}", any(proxy::proxy_inference))
        .route("/api", any(proxy::proxy_api))
        .route("/api/{*path}", any(proxy::proxy_api))
        .route("/docs", any(proxy::proxy_api))
        .route("/openapi.json", any(proxy::proxy_api))
        .fallback(any(proxy::proxy_ui));

    if settings.upstreams.api.local {
        let enable_db_create = env_truthy("EXPERIMENTAL_RUST_DB_CREATION");
        app = app.route("/api/db", get(api::db::db_info));
        if enable_db_create {
            app = app.route("/api/db/create", post(api::db::db_create));
        }
        let _ = jobs::continuous_scan::ensure_continuous_supervisor().await;
        app = app
            .route(
                "/api/bookmarks/ns",
                get(api::bookmarks::bookmark_namespaces),
            )
            .route("/api/bookmarks/users", get(api::bookmarks::bookmark_users))
            .route(
                "/api/bookmarks/ns/{namespace}",
                get(api::bookmarks::bookmarks_by_namespace)
                    .post(api::bookmarks::add_bookmarks_by_namespace)
                    .delete(api::bookmarks::delete_bookmarks_by_namespace),
            )
            .route(
                "/api/bookmarks/item/{sha256}",
                get(api::bookmarks::bookmarks_item),
            )
            .route(
                "/api/bookmarks/ns/{namespace}/{sha256}",
                get(api::bookmarks::get_bookmark)
                    .put(api::bookmarks::add_bookmark_by_sha256)
                    .delete(api::bookmarks::delete_bookmark_by_sha256),
            )
            .route("/api/items/item/file", get(api::items::item_file))
            .route("/api/items/item/thumbnail", get(api::items::item_thumbnail))
            .route("/api/items/item", get(api::items::item_meta))
            .route("/api/items/item/text", get(api::items::item_text))
            .route("/api/items/item/tags", get(api::items::item_tags))
            .route("/api/items/text/any", get(api::items::texts_any))
            .route("/api/search/pql", post(api::search::search_pql))
            .route("/api/search/pql/build", post(api::search::search_pql_build))
            .route(
                "/api/search/embeddings/cache",
                get(api::search::get_search_cache).delete(api::search::clear_search_cache),
            )
            .route("/api/search/tags", get(api::search::get_tags))
            .route("/api/search/tags/top", get(api::search::get_top_tags))
            .route("/api/search/stats", get(api::search::get_stats));
        if env_truthy("EXPERIMENTAL_RUST_JOBS") {
            app = app
                .route(
                    "/api/jobs/queue",
                    get(api::jobs::queue_status).delete(api::jobs::cancel_queued),
                )
                .route(
                    "/api/jobs/data/extraction",
                    post(api::jobs::enqueue_data_extraction)
                        .delete(api::jobs::enqueue_delete_extracted_data),
                )
                .route("/api/jobs/folders/rescan", post(api::jobs::enqueue_folder_rescan))
                .route(
                    "/api/jobs/folders",
                    get(api::jobs::get_folders).put(api::jobs::enqueue_update_folders),
                )
                .route("/api/jobs/cancel", post(api::jobs::cancel_current_job))
                .route("/api/jobs/folders/history", get(api::jobs::get_scan_history))
                .route(
                    "/api/jobs/data/history",
                    get(api::jobs::get_extraction_history)
                        .delete(api::jobs::delete_scan_data),
                )
                .route(
                    "/api/jobs/config",
                    get(api::jobs::get_config).put(api::jobs::update_config),
                )
                .route(
                    "/api/jobs/data/setters/total",
                    get(api::jobs::get_setter_data_count),
                )
                .route("/api/jobs/cronjob/run", post(api::jobs::manual_trigger_cronjob));
        }
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
