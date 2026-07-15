// rustc ≥1.94 overflows the default query depth computing the layout of
// the largest async fn bodies (api::jobs::update_config); the compiler's
// own suggestion is to raise the limit.
#![recursion_limit = "256"]

mod api;
mod api_error;
mod config;
mod db;
mod desktop;
mod env_template;
mod inferio;
mod inferio_client;
mod jobs;
mod logging;
mod media_tools;
mod openapi;
mod policy;
mod policy_token;
mod pql;
mod process_tree;
mod proxy;
mod resources;
mod setup;
mod shutdown;
#[cfg(test)]
mod test_utils;
mod ui;
mod update;

use crate::jobs::inference_pool::{InferencePool, JobInferenceContext, set_job_inference_context};
use anyhow::Context as _;
use axum::{
    Router,
    routing::{any, delete, get, post},
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
    name = "panoptikon",
    about = "Panoptikon media indexing and search server"
)]
struct Args {
    /// Config file path (global: also valid after the subcommand).
    #[arg(long, value_name = "PATH", global = true)]
    config: Option<PathBuf>,
    /// Root directory for all relative path resolution: data_folder,
    /// config, python sources, runtime/ (global: also valid after the
    /// subcommand). Default: the current working directory. Implemented as
    /// a chdir at startup before anything else runs, so every CWD-relative
    /// default resolves under it — .env auto-loading included.
    #[arg(long, value_name = "DIR", global = true)]
    root: Option<PathBuf>,
    /// Skip the best-effort startup check for a newer Panoptikon release.
    #[arg(long, global = true)]
    disable_update_check: bool,
    /// Internal: the process is a Panoptikon Desktop-owned sidecar. Enables
    /// stdin shutdown/EOF handling and identifies Desktop mode to clients.
    #[arg(long, global = true, hide = true)]
    desktop_managed: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Run ONLY the local inference service (`/api/inference/*` + `/health`):
    /// no proxy, API, jobs, cron, or migrations. For machines that just lend
    /// their GPU to other panoptikon instances (design doc §3).
    Inferio,
    /// Create or update the managed Python inference environment
    /// (python/.venv in a source checkout, runtime/venv for a bundled
    /// binary): find or download uv, detect the accelerator, and run a
    /// locked `uv sync`. Idempotent — re-running converges on the lockfile.
    Setup {
        /// Accelerator variant to install. Default: the config's
        /// `[inference_local.python_env] accelerator` (itself defaulting to
        /// auto-detection).
        #[arg(long, value_enum)]
        accelerator: Option<config::Accelerator>,
        /// Delete the managed venv first and recreate it from scratch.
        #[arg(long)]
        force: bool,
    },
    /// Download and install the latest release, replacing this executable.
    /// Checks GitHub every time (ignoring the startup-check throttle).
    Update {
        /// Skip the confirmation prompt and update immediately.
        #[arg(short = 'y', long)]
        yes: bool,
    },
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
    let args = Args::parse();
    // `--root` is the base for ALL relative path resolution (data_folder,
    // config, python, runtime). It is implemented as exactly that: a chdir
    // before anything else touches the filesystem, so every CWD-relative
    // default below — including the .env auto-load — resolves under it.
    if let Some(root) = &args.root {
        env::set_current_dir(root)
            .with_context(|| format!("failed to change to --root '{}'", root.display()))?;
    }
    desktop::set_managed(args.desktop_managed);
    env_template::capture_inherited_environment();
    // `.env` still auto-loads: it is how users populate the env vars that
    // config templating (`${VAR}` in TOML values) references, and children
    // (inference workers, the UI server) inherit it.
    dotenvy::dotenv().ok();

    let config_path = args
        .config
        .or_else(|| env::var(config::CONFIG_PATH_ENV).ok().map(PathBuf::from));
    // A serving process owns its complete root. This prevents a foreground
    // Server and Desktop sidecar (or two foreground Servers) from opening the
    // same SQLite databases. Setup/update/inferio retain their existing,
    // narrower concurrency behavior.
    let _root_lock = if args.command.is_none() {
        Some(desktop::RootLock::acquire(std::env::current_dir()?)?)
    } else {
        None
    };
    // Bundled builds materialize embedded resources on first run (write the
    // default configs when no config exists or was pointed at, extract the
    // Python source set when no dev tree is present); plain builds no-op.
    // This must precede Settings::load — it may create the very file that
    // load is about to read.
    // Desktop always names its managed config explicitly, but that file still
    // has to be materialized on a fresh Desktop-owned root. Treat managed
    // invocation as the bundled first-run case while preserving the ordinary
    // Server rule that an explicit config path is never synthesized.
    let first_run_messages =
        resources::materialize_first_run(config_path.is_some() && !args.desktop_managed)?;
    // Config must load before logging init (logging is configured by
    // [logging] now); a config-load error is reported on stderr by main.
    let settings = Arc::new(config::Settings::load(config_path)?);
    config::install_runtime(&settings);
    // The guard must stay alive for the whole process: dropping it flushes
    // buffered file-log output.
    let _log_guard = logging::init(&settings);
    // First-run actions went to stderr when they happened (pre-logging);
    // repeat them through tracing so they land in the log file too.
    for message in &first_run_messages {
        tracing::info!("{message}");
    }
    settings.log_warnings();

    // Policy-token HMAC key: random per boot unless [server]
    // policy_token_key pins it (policy_token.rs). Needed by the policy
    // layer (verify) and the UI proxy (mint) in every serving mode.
    let token_key = Arc::new(policy_token::TokenKey::from_settings(&settings)?);

    match args.command {
        Some(Command::Inferio) => return inferio_main(settings, token_key).await,
        Some(Command::Setup { accelerator, force }) => {
            // An explicit `panoptikon setup` always runs (never skipped by
            // the completion sentinel; a successful sync rewrites it).
            return setup::run(
                &settings,
                setup::SetupOptions {
                    accelerator,
                    force,
                    skip_if_converged: false,
                },
            )
            .await;
        }
        Some(Command::Update { yes }) => {
            return update::run_update_command(crate::resources::VERSION, yes).await;
        }
        None => {}
    }

    // Server path only (Setup/Inferio/Update returned above). Fire-and-forget a
    // best-effort, throttled check for a newer release; it prints a banner if
    // one exists.
    if !args.disable_update_check && settings.server.check_for_updates {
        crate::update::spawn_startup_check(crate::resources::VERSION);
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
    // Created here (not at serve time) because ProxyState carries a receiver:
    // proxied Upgrade bridges (WebSockets) select on it so they cannot
    // outlive graceful shutdown. First signal: axum stops accepting
    // connections and drains in-flight requests, bridges close, and the
    // cleanup task cancels jobs and flushes the DB writers.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let state = Arc::new(proxy::ProxyState::new(
        ui_upstream,
        api_upstream,
        inference_upstream,
        inference_client,
        settings.search.embedding_cache_size,
        Arc::clone(&settings),
        Arc::clone(&token_key),
        shutdown_rx.clone(),
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

    // Managed Python environment: when local inference is enabled with no
    // user-configured interpreter and the managed venv is missing, run
    // `panoptikon setup` now (blocking, before the orchestrator starts
    // serving). A failure logs and continues — the server comes up with
    // inference unavailable instead of dying.
    setup::maybe_auto_setup(&settings, settings.inference_local.enabled).await;

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
            .route("/api/db/create", post(api::db::db_create))
            // Always allowed regardless of ruleset (the policy layer
            // exempts GET on this path): clients discover their policy's
            // capabilities and [policies.client] settings here.
            .route("/api/client-config", get(api::client_config::client_config));
        app = app.route(
            "/api/relay/pairings/{relay_id}",
            get(api::relay::get_pairing)
                .put(api::relay::put_pairing)
                .delete(api::relay::delete_pairing),
        );
        if desktop::is_managed() {
            let desktop_routes = Router::new()
                .route("/api/desktop/setup-status", get(api::desktop::setup_status))
                .route(
                    "/api/desktop/setup-folders/validate",
                    post(api::desktop::validate_setup_folders),
                )
                .route(
                    "/api/desktop/setup-continuous/validate",
                    post(api::desktop::validate_setup_continuous_folders),
                )
                .route(
                    "/api/desktop/setup-schedule/preview",
                    post(api::desktop::preview_setup_schedule),
                )
                .route(
                    "/api/desktop/setup/complete",
                    post(api::desktop::complete_setup),
                )
                .route(
                    "/api/desktop/external-inputs",
                    get(api::desktop::external_inputs).put(api::desktop::update_external_inputs),
                )
                .route(
                    "/api/desktop/external-inputs/{variable}",
                    get(api::desktop::reveal_external_input),
                )
                .route(
                    "/api/desktop/update-status",
                    get(api::desktop::update_status),
                )
                .route(
                    "/api/desktop/update-window/open",
                    post(api::desktop::open_update_window),
                )
                .route(
                    "/api/desktop/update-ribbon/snooze",
                    post(api::desktop::snooze_update_ribbon),
                )
                .route(
                    "/api/desktop/update-ribbon/dismiss",
                    post(api::desktop::dismiss_update_ribbon),
                )
                .layer(axum::Extension(api::desktop::DesktopInferenceState(
                    inferio_state.clone(),
                )));
            app = app.merge(desktop_routes);
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
            .route(
                "/api/pinboards",
                get(api::pinboards::list_pinboards).post(api::pinboards::create_pinboard),
            )
            .route(
                "/api/pinboards/{pinboard_id}",
                get(api::pinboards::get_pinboard)
                    .patch(api::pinboards::update_pinboard)
                    .delete(api::pinboards::delete_pinboard),
            )
            .route(
                "/api/pinboards/{pinboard_id}/versions",
                get(api::pinboards::list_pinboard_versions)
                    .post(api::pinboards::save_pinboard_version),
            )
            .route(
                "/api/pinboards/{pinboard_id}/versions/{version_id}",
                delete(api::pinboards::delete_pinboard_version),
            )
            .route(
                "/api/pinboards/{pinboard_id}/versions/{version_id}/preview",
                get(api::pinboards::pinboard_version_preview),
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
            )
            .route(
                "/api/jobs/continuous/status",
                get(api::jobs::get_continuous_scan_status),
            );
    }

    let app = app
        .with_state(state)
        .layer(TraceLayer::new_for_http())
        .layer(policy::PolicyLayer::new(
            Arc::clone(&settings),
            Arc::clone(&token_key),
        ));

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

    // Cleanup task and HTTP drain both must finish before main returns;
    // shutdown.rs enforces the deadline.
    let inferio_manager = inferio_state
        .as_ref()
        .map(|state| Arc::clone(&state.manager));
    let cleanup = tokio::spawn(async move {
        shutdown::wait_for_signal(args.desktop_managed).await;
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

/// `panoptikon inferio`: the standalone inference service (design
/// doc §3 "GPU lender" mode). Same config file, same policy layer (host
/// policies + rulesets apply; inference paths get DB params stripped), but
/// only `/api/inference/*` and `/health` are served — no proxy, local API,
/// jobs, cron, or migrations. `inference_local.enabled` is implied by the
/// subcommand; `[inference_local].port` overrides the listen port
/// (defaults to `server.port`).
async fn inferio_main(
    settings: Arc<config::Settings>,
    token_key: Arc<policy_token::TokenKey>,
) -> anyhow::Result<()> {
    // Same managed-venv auto-setup as gateway mode: this subcommand spawns
    // the same Python workers (local inference is implied here, so the
    // config's `enabled` flag is not consulted).
    setup::maybe_auto_setup(&settings, true).await;
    let state = inferio::http::InferioState::from_settings(&settings)?;
    // Single listener: extra [[server.endpoints]] do not apply to the
    // standalone inference service. Its one listener is the primary.
    let app = inferio::http::standalone_router(Arc::clone(&state))
        .layer(TraceLayer::new_for_http())
        .layer(policy::PolicyLayer::new(Arc::clone(&settings), token_key))
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
        shutdown::wait_for_signal(false).await;
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
