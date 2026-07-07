// rustc ≥1.94 overflows the default query depth computing the layout of
// the largest async fn bodies (api::jobs::update_config); the compiler's
// own suggestion is to raise the limit.
#![recursion_limit = "256"]

mod api;
mod api_error;
mod config;
mod db;
mod env_template;
mod inferio;
mod inferio_client;
mod jobs;
mod logging;
mod openapi;
mod policy;
mod pql;
mod process_tree;
mod proxy;
mod shutdown;
#[cfg(test)]
mod test_utils;
mod ui;

use crate::jobs::inference_pool::{InferencePool, JobInferenceContext, set_job_inference_context};
use anyhow::Context as _;
use axum::{
    Router,
    routing::{any, get, post},
};
use clap::Parser;
use std::{env, net::SocketAddr, path::PathBuf, sync::Arc};
use tower_http::trace::TraceLayer;
use utoipa::OpenApi;
use utoipa_redoc::Redoc;
use utoipa_redoc::Servable;
use utoipa_swagger_ui::SwaggerUi;

#[derive(Parser, Debug)]
#[command(
    name = "panoptikon-gateway",
    about = "Panoptikon reverse proxy gateway"
)]
struct Args {
    /// Config file path (global: also valid after the subcommand).
    #[arg(long, value_name = "PATH", global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Run ONLY the local inference service (`/api/inference/*` + `/health`):
    /// no proxy, API, jobs, cron, or migrations. For machines that just lend
    /// their GPU to other panoptikon instances (design doc §3).
    Inferio,
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
    // `.env` still auto-loads: it is how users populate the env vars that
    // config templating (`${VAR}` in TOML values) references, and children
    // (inference workers, the UI server) inherit it.
    dotenvy::dotenv().ok();

    let args = Args::parse();
    let config_path = args
        .config
        .or_else(|| env::var(config::CONFIG_PATH_ENV).ok().map(PathBuf::from));
    // Config must load before logging init (logging is configured by
    // [logging] now); a config-load error is reported on stderr by main.
    let settings = Arc::new(config::Settings::load(config_path)?);
    config::install_runtime(&settings);
    // The guard must stay alive for the whole process: dropping it flushes
    // buffered file-log output.
    let _log_guard = logging::init(&settings);
    settings.log_warnings();

    if matches!(args.command, Some(Command::Inferio)) {
        return inferio_main(settings).await;
    }

    let ui_upstream = proxy::Upstream::parse("ui", &settings.upstreams.ui.base_url)?;
    let api_upstream = proxy::Upstream::parse("api", &settings.upstreams.api.base_url)?;
    let inference_config = settings
        .upstreams
        .inference
        .first()
        .expect("inference upstream should be initialized");
    let inference_upstream = proxy::Upstream::parse("inference", &inference_config.base_url)?;
    let inference_client =
        inferio_client::InferenceApiClient::from_settings_with_metadata_cache(&settings, true)?;
    let job_endpoints = settings
        .upstreams
        .inference
        .iter()
        .filter(|endpoint| endpoint.use_for_jobs)
        .cloned()
        .collect::<Vec<_>>();
    let inference_pool = InferencePool::new(job_endpoints)?;
    set_job_inference_context(JobInferenceContext {
        primary: inference_client.clone(),
        pool: inference_pool,
        embedding_cache_size: settings.search.embedding_cache_size,
        loader_concurrency: settings.jobs.loader_concurrency,
        intermediate_budget_kib: u32::try_from(
            settings
                .jobs
                .intermediate_data_budget_mb
                .saturating_mul(1024),
        )
        .unwrap_or(u32::MAX),
    })?;
    let state = Arc::new(proxy::ProxyState::new(
        ui_upstream,
        api_upstream,
        inference_upstream,
        inference_client,
        settings.search.embedding_cache_size,
    ));

    let local_api = settings.upstreams.api.local;

    // When the gateway is the API server it owns the databases, so it runs
    // startup migrations like the Python server does (and, like Python,
    // skips them in readonly mode): the default databases are created if
    // missing, then every other on-disk DB is brought up to date.
    // Python-created DBs are baselined, not re-migrated — see
    // db::migrations::ensure_baseline_if_needed.
    if local_api && !db::readonly_mode() {
        db::migrations::migrate_databases_on_disk(None, None).await?;
        db::migrations::migrate_all_databases_on_disk().await?;
    }

    // Local inference (design doc §3): when enabled, the /api/inference/*
    // paths that used to be proxied are served in-process by the inferio
    // orchestrator — same position in the router, so they stay behind the
    // policy layer (which strips DB params for inference paths) exactly like
    // the proxy did. When disabled, proxy exactly as before.
    let inferio_state = if settings.inference_local.enabled {
        Some(inferio::http::InferioState::from_settings(&settings)?)
    } else {
        None
    };
    // Eager prewarm set (design §8): gateway mode only — enumerate index
    // DBs at startup and on a minute tick, warm one worker per search-
    // usable embedding impl class (plus always_warm, which the manager
    // already warmed at construction). The `inferio` subcommand never scans
    // DBs; it gets always_warm only.
    if let Some(state) = &inferio_state {
        if settings.inference_local.prewarm.enabled {
            tokio::spawn(inferio::prewarm::run_eager_prewarm_loop(Arc::downgrade(
                &state.manager,
            )));
        }
    }

    // Production UI ([upstreams.ui] local = true): npm install / next build
    // when stale, then a supervised `next start` on base_url's host/port —
    // all in a background task, so gateway startup is not blocked (the proxy
    // 502s until the UI is up). Gateway mode only; the `inferio` subcommand
    // returned above.
    let ui_server = if settings.upstreams.ui.local {
        Some(ui::start(&settings)?)
    } else {
        None
    };

    let mut app = Router::new();
    match &inferio_state {
        Some(state) => {
            tracing::info!("serving /api/inference locally (inference_local.enabled)");
            app = app.nest_service("/api/inference", inferio::http::router(Arc::clone(state)));
        }
        None => {
            app = app
                .route("/api/inference", any(proxy::proxy_inference))
                .route("/api/inference/{*path}", any(proxy::proxy_inference));
        }
    }
    // With a local API there is no separate backend: upstreams.api.base_url
    // points back at this gateway, so the catch-all proxy would forward any
    // unmatched /api path to ourselves and recurse. Each hop holds a live
    // loopback connection, so one such request exhausts the ephemeral port
    // range within seconds and starves every other connection on the machine
    // (observed: GET /api/search/ — a Python-era path — reached 15k hops).
    // Unknown API paths must 404 instead.
    let app = if local_api {
        app.route("/api", any(api_not_found))
            .route("/api/{*path}", any(api_not_found))
    } else {
        app.route("/api", any(proxy::proxy_api))
            .route("/api/{*path}", any(proxy::proxy_api))
    };
    let mut app = app
        .route("/docs", any(proxy::proxy_api))
        .route("/openapi.json", any(proxy::proxy_api))
        .fallback(any(proxy::proxy_ui));

    if local_api {
        app = app
            .route("/api/db", get(api::db::db_info))
            .route("/api/db/create", post(api::db::db_create));
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
            .route(
                "/api/open/file/{sha256}",
                post(api::open::open_file_on_host),
            )
            .route(
                "/api/open/folder/{sha256}",
                post(api::open::show_in_file_manager),
            )
            .route("/api/search/pql", post(api::search::search_pql))
            .route("/api/search/pql/build", post(api::search::search_pql_build))
            .route(
                "/api/search/embeddings/cache",
                get(api::search::get_search_cache).delete(api::search::clear_search_cache),
            )
            .route("/api/search/tags", get(api::search::get_tags))
            .route("/api/search/tags/top", get(api::search::get_top_tags))
            .route("/api/search/stats", get(api::search::get_stats))
            .merge(SwaggerUi::new("/docs").url("/openapi.json", openapi::ApiDoc::openapi()))
            .merge(Redoc::with_url("/redoc", openapi::ApiDoc::openapi()));
        // Local API mode means the gateway owns jobs and cron. Do not run
        // the Python server's cron against the same databases — it would
        // double-schedule.
        let _ = jobs::cron::ensure_cron_scheduler().await;
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
            .route(
                "/api/jobs/folders/rescan",
                post(api::jobs::enqueue_folder_rescan),
            )
            .route(
                "/api/jobs/folders",
                get(api::jobs::get_folders).put(api::jobs::enqueue_update_folders),
            )
            .route("/api/jobs/cancel", post(api::jobs::cancel_current_job))
            .route(
                "/api/jobs/folders/history",
                get(api::jobs::get_scan_history),
            )
            .route(
                "/api/jobs/data/history",
                get(api::jobs::get_extraction_history).delete(api::jobs::delete_scan_data),
            )
            .route(
                "/api/jobs/config",
                get(api::jobs::get_config).put(api::jobs::update_config),
            )
            .route(
                "/api/jobs/data/setters/total",
                get(api::jobs::get_setter_data_count),
            )
            .route(
                "/api/jobs/cronjob/run",
                post(api::jobs::manual_trigger_cronjob),
            )
            .route(
                "/api/jobs/cronjob/schedule",
                get(api::jobs::get_cronjob_schedule),
            );
    }

    let app = app
        .with_state(state)
        .layer(TraceLayer::new_for_http())
        .layer(policy::PolicyLayer::new(Arc::clone(&settings)));

    // Bind every configured listener (primary + [[server.endpoints]]) before
    // serving any of them: a config that cannot fully bind fails startup as
    // a whole instead of running with a partial endpoint set.
    let mut listeners = Vec::new();
    for (name, addr) in settings.listener_addrs() {
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .with_context(|| format!("failed to bind endpoint '{name}' on {addr}"))?;
        tracing::info!(endpoint = %name, address = %addr, "gateway listening");
        listeners.push((name, listener));
    }

    // First signal: axum stops accepting connections and drains in-flight
    // requests while the cleanup task cancels jobs and flushes the DB writers.
    // Both must finish before main returns; shutdown.rs enforces the deadline.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let inferio_manager = inferio_state
        .as_ref()
        .map(|state| Arc::clone(&state.manager));
    let cleanup = tokio::spawn(async move {
        shutdown::wait_for_signal().await;
        let _ = shutdown_tx.send(true);
        shutdown::run_cleanup(local_api, inferio_manager, ui_server).await;
    });
    // One server task per listener, all serving the same router; the only
    // difference is the ListenerEndpoint extension the policy layer reads.
    let mut servers = Vec::new();
    for (name, listener) in listeners {
        let app = app
            .clone()
            .layer(axum::Extension(policy::ListenerEndpoint(Arc::from(
                name.as_str(),
            ))));
        let mut shutdown_rx = shutdown_rx.clone();
        servers.push(tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.changed().await;
            })
            .await
        }));
    }
    drop(shutdown_rx);
    for server in servers {
        server.await??;
    }
    let _ = cleanup.await;
    tracing::info!("gateway stopped");
    Ok(())
}

/// Replaces the /api catch-all proxy when the API is served locally: the
/// proxy target would be this gateway itself (see router setup above).
async fn api_not_found(uri: axum::http::Uri) -> api_error::ApiError {
    api_error::ApiError::not_found(format!("Unknown API endpoint: {}", uri.path()))
}

/// `panoptikon-gateway inferio`: the standalone inference service (design
/// doc §3 "GPU lender" mode). Same config file, same policy layer (host
/// policies + rulesets apply; inference paths get DB params stripped), but
/// only `/api/inference/*` and `/health` are served — no proxy, local API,
/// jobs, cron, or migrations. `inference_local.enabled` is implied by the
/// subcommand; `[inference_local].port` overrides the listen port
/// (defaults to `server.port`).
async fn inferio_main(settings: Arc<config::Settings>) -> anyhow::Result<()> {
    let state = inferio::http::InferioState::from_settings(&settings)?;
    // Single listener: extra [[server.endpoints]] do not apply to the
    // standalone inference service. Its one listener is the primary.
    let app = inferio::http::standalone_router(Arc::clone(&state))
        .layer(TraceLayer::new_for_http())
        .layer(policy::PolicyLayer::new(Arc::clone(&settings)))
        .layer(axum::Extension(policy::ListenerEndpoint(Arc::from(
            config::PRIMARY_ENDPOINT,
        ))));

    let port = settings
        .inference_local
        .port
        .unwrap_or(settings.server.port);
    let listen_addr = format!("{}:{}", settings.server.host, port);
    let listener = tokio::net::TcpListener::bind(&listen_addr).await?;
    tracing::info!(address = %listen_addr, "inference service listening (inferio mode)");

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let manager = Arc::clone(&state.manager);
    let cleanup = tokio::spawn(async move {
        shutdown::wait_for_signal().await;
        let _ = shutdown_tx.send(());
        shutdown::run_inferio_cleanup(manager).await;
    });
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        let _ = shutdown_rx.await;
    })
    .await?;
    let _ = cleanup.await;
    tracing::info!("inference service stopped");
    Ok(())
}
