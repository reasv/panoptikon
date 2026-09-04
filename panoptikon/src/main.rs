// rustc ≥1.94 overflows the default query depth computing the layout of
// the largest async fn bodies (api::jobs::update_config); the compiler's
// own suggestion is to raise the limit.
#![recursion_limit = "256"]

mod accelerator_env;
mod accelerator_report;
mod api;
mod api_error;
mod config;
mod db;
mod desktop;
mod env_template;
mod host_paths;
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
/// The process's open-file-descriptor budget: the startup raise of the soft
/// `RLIMIT_NOFILE` and the reader the extraction job's in-flight ceiling
/// clamps itself with.
mod rlimit;
mod setup;
mod shutdown;
#[cfg(test)]
mod test_utils;
mod ui;
mod update;
/// The stored-rendition ladder (`display`/`grid-m`/`grid-s`) shared by the
/// scan's generator, its backfill dispatcher and the thumbnail endpoint —
/// one place, because the three must agree exactly.
mod visual_tiers;

use crate::jobs::inference_pool::{InferencePool, JobInferenceContext, set_job_inference_context};
use anyhow::Context as _;
use axum::{
    Router,
    routing::{any, delete, get, post, put},
};
use clap::Parser;
use std::{env, path::PathBuf, sync::Arc};
use tower_http::trace::TraceLayer;
use utoipa::OpenApi;
use utoipa_redoc::Redoc;
use utoipa_redoc::Servable;
use utoipa_swagger_ui::SwaggerUi;

#[derive(Parser, Debug)]
#[command(
    name = "panoptikon",
    about = "Panoptikon media indexing and search server",
    version = crate::resources::VERSION,
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
        /// Skip when the managed venv is already complete (lockfile current
        /// and, with an explicit `--accelerator`, the installed variant
        /// matches). Ignored with `--force`.
        #[arg(long)]
        if_needed: bool,
    },
    /// Print the resolved inference accelerator (cpu/cuda/rocm/…) and any
    /// GPU names. CPU is reported without warning; GPU backends warn only
    /// when no device name can be detected.
    Accelerator,
    /// Download and install the latest release, replacing this executable.
    /// Checks GitHub every time (ignoring the startup-check throttle).
    Update {
        /// Skip the confirmation prompt and update immediately.
        #[arg(short = 'y', long)]
        yes: bool,
    },
}

/// Request-body ceiling for the pinboard routes.
///
/// Pinboard writes carry the board's composited preview image inline in the
/// JSON body as base64 (`POST /api/pinboards`, `POST …/versions`, and
/// `PUT …/versions/{id}/preview`), so axum's 2 MiB `DefaultBodyLimit` cuts
/// them off long before the handler's own caps apply: a dense board at the
/// 2048px preview master is a multi-megabyte payload, and a 413 at the
/// extractor makes the board permanently unsaveable. The worst case the
/// handlers actually accept is `MAX_PREVIEW_BYTES` (8 MiB) inflated ~4/3 by
/// base64 (~10.7 MiB) plus `MAX_LAYOUT_BYTES` (1 MiB) and JSON overhead, so
/// 16 MiB puts the limit just above what `api::pinboards` will accept and
/// keeps the handler the thing that rejects oversized uploads. Every other
/// route keeps the 2 MiB default.
const PINBOARD_BODY_LIMIT: usize = 16 * 1024 * 1024;

fn main() -> anyhow::Result<()> {
    // Raise the soft open-file-descriptor limit to the hard limit before
    // anything opens a descriptor, and before the runtime exists: rlimits are
    // per-process and inherited by every thread and child (inference workers,
    // the UI server), so this is the one place that fixes all of them. A
    // typical Linux shell and a containerd container both start the process
    // at soft 1024 / hard 524 288 while local inference costs two sockets per
    // in-flight predict (test protocol §8 G7, Phase 6 finding F6). Failure is
    // never fatal — `jobs::extraction` reads whatever limit survives and
    // bounds its in-flight window by it. The outcome is logged by
    // `async_main` once logging has been configured; up here there is nowhere
    // to log to yet.
    rlimit::raise_soft_limit_at_startup();

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
    // (inference workers, the UI server) inherit it. Malformed lines are
    // skipped and reported once logging is up, never fatal.
    let dotenv_diagnostics = env_template::load_process_dotenv();

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
    env_template::warn_dotenv_diagnostics(&dotenv_diagnostics);
    // Same shape as the first-run messages: the descriptor-limit raise
    // happened in `main`, long before there was a logger to say so.
    rlimit::log_startup_raise();
    settings.log_warnings();

    // Policy-token HMAC key: random per boot unless [server]
    // policy_token_key pins it (policy_token.rs). Needed by the policy
    // layer (verify) and the UI proxy (mint) in every serving mode.
    let token_key = Arc::new(policy_token::TokenKey::from_settings(&settings)?);

    match args.command {
        Some(Command::Inferio) => return inferio_main(settings, token_key).await,
        Some(Command::Setup {
            accelerator,
            force,
            if_needed,
        }) => {
            return setup::run(
                &settings,
                setup::SetupOptions {
                    accelerator,
                    force,
                    skip_if_converged: if_needed && !force,
                },
            )
            .await;
        }
        Some(Command::Accelerator) => {
            accelerator_report::print_report(&settings);
            return Ok(());
        }
        Some(Command::Update { yes }) => {
            return update::run_update_command(crate::resources::VERSION, yes).await;
        }
        None => {}
    }

    accelerator_report::log_report(&settings);

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

    // Startup values for the search result cache byte budget; the PUT
    // endpoint can adjust the budget at runtime (never above the ceiling)
    // without persisting.
    api::search_cache::set_budget_limit_mb(settings.search.cache_size_max_mb);
    api::search_cache::set_budget_mb(settings.search.cache_size_mb);

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
        // Vector-quant discrepancy check (crash/power-loss recovery and
        // first-post-upgrade convergence): metadata-only diffs are applied
        // synchronously, real data work enqueues a reconcile job. Runs in
        // the background — nobody waits; `auto` resolves to exact until
        // coverage is ready.
        tokio::spawn(jobs::vector_quants::check_all_at_startup());
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
    if let Some(state) = &inferio_state
        && settings.inference_local.prewarm.enabled
    {
        tokio::spawn(inferio::prewarm::run_eager_prewarm_loop(Arc::downgrade(
            &state.manager,
        )));
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
            get(api::relay::get_pairing).delete(api::relay::delete_pairing),
        );
        app = app.route(
            "/api/relay/pairing-operations/{relay_id}",
            get(api::relay::get_pairing_operation).post(api::relay::begin_pairing_operation),
        );
        app = app.route(
            "/api/relay/pairing-operations/{operation_id}/commit",
            axum::routing::put(api::relay::commit_pairing_operation),
        );
        app = app.route(
            "/api/relay/pairing-operations/{operation_id}/cancel",
            axum::routing::delete(api::relay::cancel_pairing_operation),
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
            // The pinboard surface is merged as its own router purely so the
            // raised body limit lands on these paths and nowhere else; the
            // routes are otherwise ordinary members of the app.
            .merge(
                Router::new()
                    .route(
                        "/api/pinboards",
                        get(api::pinboards::list_pinboards).post(api::pinboards::create_pinboard),
                    )
                    // Content search lives in the pinboard authz domain, not
                    // the search one: under /api/search/ it would inherit
                    // search-only ruleset grants (`path_prefix =
                    // "/api/search/"`) and leak board names, ids and
                    // timestamps to policies that deny pinboards. The handler
                    // still lives in api/search.rs.
                    .route(
                        "/api/pinboards/search",
                        post(api::search::search_pql_pinboards),
                    )
                    .route(
                        "/api/pinboards/{pinboard_id}",
                        get(api::pinboards::get_pinboard)
                            .patch(api::pinboards::update_pinboard)
                            .delete(api::pinboards::delete_pinboard),
                    )
                    .route(
                        "/api/pinboards/{pinboard_id}/databases",
                        put(api::pinboards::set_pinboard_databases),
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
                        get(api::pinboards::pinboard_version_preview)
                            .put(api::pinboards::update_pinboard_version_preview),
                    )
                    .layer(axum::extract::DefaultBodyLimit::max(PINBOARD_BODY_LIMIT)),
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
            // Ahead of the `{sha256}` capture below only for readability:
            // matchit prefers a literal segment over a parameter regardless
            // of registration order, so `.../clipboard/artifact` can never be
            // read as a file hash named "artifact".
            .route(
                "/api/open/clipboard/artifact",
                post(api::open::copy_artifact_to_clipboard_on_host),
            )
            .route(
                "/api/open/clipboard/{sha256}",
                post(api::open::copy_file_to_clipboard_on_host),
            )
            .route("/api/search/pql", post(api::search::search_pql))
            .route("/api/search/pql/build", post(api::search::search_pql_build))
            .route(
                "/api/search/embeddings/cache",
                get(api::search::get_search_cache).delete(api::search::clear_search_cache),
            )
            .route(
                "/api/search/cache",
                get(api::search_cache::get_result_cache)
                    .delete(api::search_cache::clear_result_cache)
                    .put(api::search_cache::resize_result_cache),
            )
            .route("/api/video/transcode", post(api::video::video_transcode))
            .route("/api/video/compose", post(api::video::video_compose))
            .route("/api/video/artifact", get(api::video::video_artifact))
            .route(
                "/api/video/jobs/{job_id}",
                get(api::video::video_job).delete(api::video::video_job_cancel),
            )
            .route(
                "/api/video/jobs/{job_id}/events",
                get(api::video::video_job_events),
            )
            .route("/api/video/presets", get(api::video::video_presets))
            .route(
                "/api/video/cache",
                get(api::video::get_transcode_cache)
                    .delete(api::video::clear_transcode_cache)
                    .put(api::video::resize_transcode_cache),
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
                "/api/jobs/maintenance",
                post(api::jobs::enqueue_maintenance),
            )
            .route(
                "/api/jobs/folders/history",
                get(api::jobs::get_scan_history),
            )
            .route(
                "/api/jobs/data/history",
                get(api::jobs::get_extraction_history).delete(api::jobs::delete_scan_data),
            )
            .route(
                "/api/jobs/data/failures",
                get(api::jobs::get_extraction_failures),
            )
            .route("/api/jobs/scan/failures", get(api::jobs::get_scan_failures))
            .route(
                "/api/jobs/config",
                get(api::jobs::get_config).put(api::jobs::update_config),
            )
            .route(
                "/api/jobs/data/setters/total",
                get(api::jobs::get_setter_data_count),
            )
            .route("/api/jobs/quants", get(api::jobs::get_vector_quants))
            .route(
                "/api/jobs/quants/reconcile",
                post(api::jobs::enqueue_vector_quant_reconcile),
            )
            .route(
                "/api/jobs/quants/rebuild",
                post(api::jobs::rebuild_vector_quant_pair),
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
    //
    // The serve loop is hyper-util's *auto* connection builder, which sniffs
    // the HTTP/2 client preface and serves either version on the same port —
    // so h2c with prior knowledge needs no upgrade handshake and no second
    // listener. It is gated on axum's `http2` feature, which `Cargo.toml`
    // therefore names explicitly; the inference client (`inferio_client.rs`)
    // probes for exactly this and falls back to HTTP/1.1 against a server
    // without it. `serve_with_stream_limit` drives that builder directly
    // rather than through `axum::serve` so the stream limit is ours; see
    // MAX_CONCURRENT_STREAMS.
    tracing::info!(
        max_concurrent_streams = MAX_CONCURRENT_STREAMS,
        "serving HTTP/1.1 and HTTP/2 cleartext"
    );
    let mut servers = Vec::new();
    for (name, listener) in listeners {
        let app = app
            .clone()
            .layer(axum::Extension(policy::ListenerEndpoint(Arc::from(
                name.as_str(),
            ))));
        let mut shutdown_rx = shutdown_rx.clone();
        servers.push(tokio::spawn(async move {
            serve_with_stream_limit(listener, app, async move {
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

/// Concurrent HTTP/2 streams this server admits **per connection**.
///
/// Every listener this binary opens serves `/api/inference` (the gateway
/// nests it; `panoptikon inferio` is only that), so this number is the
/// ceiling on concurrent predicts one peer connection can carry — which is
/// exactly why it is written down here instead of being inherited.
///
/// **Why it was not written down before, and what that cost.** `axum::serve`
/// builds hyper-util's auto connection builder and leaves its HTTP/2 config
/// alone, so the advertised `SETTINGS_MAX_CONCURRENT_STREAMS` was hyper's
/// default of **200**. Run2's `S2-wdvit` leg published a desired-in-flight
/// figure of 1 632 items, the client's log line claimed a concurrency of 256,
/// and the number that actually applied was 200 — named nowhere, logged
/// nowhere, absent from `/health`, and absent from the descriptor arithmetic
/// in `jobs::extraction::in_flight_unit_ceiling`. Window formation then
/// degenerated into the involution `W -> 200 - W`, and the calibration ramp
/// froze at an anchor of 136 units for the rest of the job. A ceiling no
/// layer can name is not a policy.
///
/// **The number.** 512 = 8 x `inferio_client::H2_STREAMS_PER_CONNECTION`
/// (64), which is the per-connection stream budget our own client offers a
/// peer. Eight times it, because the limit is per *connection* and the
/// clients on the other side are not all ours:
///
/// - our own gateway spreads its concurrency over independent connections and
///   never offers one of them more than 64 streams, so a single gateway is
///   covered eight times over;
/// - several gateways are covered without arithmetic: each brings its own
///   connections, each with its own budget of 512;
/// - a reverse proxy in front of the inference server — the NAS-to-GPU-box
///   deployment grows one easily — *does* fan several clients onto one
///   upstream connection, and that is the case the factor of eight is for;
/// - it stays under what a generous peer would consider hostile, and above
///   every common server default (nginx 128, Envoy 100, hyper 200), so this
///   server is never the tightest limit in a chain it is part of.
///
/// **The bound in the other direction.** A stream is not free: since
/// `7e96de62` the predict handler buffers the whole multipart body before
/// parsing it, so an open predict stream can hold up to
/// [`inferio::http::PREDICT_BODY_LIMIT`] of server memory. 512 is therefore
/// also the statement "one connection may pin at most 512 request bodies",
/// which is a number an operator can multiply; before this it was 200 and
/// nobody could have said so. Bounding the *aggregate* buffered predict
/// bytes across streams would need a byte admission budget on the handler,
/// which this does not add.
pub(crate) const MAX_CONCURRENT_STREAMS: u32 = 512;

/// Serve `app` on `listener` until `shutdown` resolves, then drain.
///
/// This is `axum::serve(...).with_graceful_shutdown(...)` re-implemented on
/// hyper-util's own auto builder, for one reason: `axum::serve` exposes no
/// hook onto that builder, and [`MAX_CONCURRENT_STREAMS`] has to be set on
/// it. Everything else here mirrors axum 0.8's loop deliberately — accept,
/// spawn per connection, `serve_connection_with_upgrades` (websockets),
/// `enable_connect_protocol` (HTTP/2 websockets), `graceful_shutdown` on the
/// signal, and a `watch` channel whose last receiver dropping is what "every
/// connection task has finished" means.
///
/// The one simplification: axum threads the peer address through a
/// `MakeService` so `ConnectInfo` can be extracted. There is exactly one
/// connect-info type in this binary (`SocketAddr`), so the extension is
/// inserted directly per request instead, which is what
/// `IntoMakeServiceWithConnectInfo` does at the end of its own chain.
pub(crate) async fn serve_with_stream_limit<F>(
    listener: tokio::net::TcpListener,
    app: Router,
    shutdown: F,
) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    serve_with_streams(listener, app, shutdown, MAX_CONCURRENT_STREAMS).await
}

/// [`serve_with_stream_limit`] with the limit as a parameter. Production
/// always passes [`MAX_CONCURRENT_STREAMS`]; the parameter exists so tests can
/// stand in for a peer — or a proxy in front of one — that advertises less,
/// which is the case a client cannot detect and must survive.
pub(crate) async fn serve_with_streams<F>(
    listener: tokio::net::TcpListener,
    app: Router,
    shutdown: F,
    max_concurrent_streams: u32,
) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder;
    use tower::ServiceExt as _;

    // Dropping the only receiver is the signal; `closed()` on the sender is
    // how every connection task learns about it. Same shape as axum's.
    let (signal_tx, signal_rx) = tokio::sync::watch::channel(());
    tokio::spawn(async move {
        shutdown.await;
        drop(signal_rx);
    });
    // Held by every live connection task; `closed()` on the sender is the
    // drain.
    let (close_tx, close_rx) = tokio::sync::watch::channel(());

    loop {
        let (stream, peer) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                // Per-connection errors (a peer that vanished between the
                // SYN and the accept, a momentary descriptor shortage) must
                // not take the listener down; the sleep keeps a persistent
                // one from spinning the CPU. This is what axum's `Listener`
                // impl for `TcpListener` does.
                Err(err) => {
                    tracing::debug!(error = %err, "failed to accept a connection");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue;
                }
            },
            _ = signal_tx.closed() => break,
        };

        let io = TokioIo::new(stream);
        let app = app.clone();
        let signal_tx = signal_tx.clone();
        let close_rx = close_rx.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(
                move |request: hyper::Request<hyper::body::Incoming>| {
                    let mut request = request.map(axum::body::Body::new);
                    request
                        .extensions_mut()
                        .insert(axum::extract::ConnectInfo(peer));
                    app.clone().oneshot(request)
                },
            );
            let mut builder = Builder::new(TokioExecutor::new());
            builder
                .http2()
                // The whole reason this function exists.
                .max_concurrent_streams(max_concurrent_streams)
                // CONNECT protocol, needed for HTTP/2 websockets (axum sets
                // it too; dropping it would be a silent regression).
                .enable_connect_protocol();
            let mut conn = std::pin::pin!(builder.serve_connection_with_upgrades(io, service));
            let mut draining = false;
            loop {
                if draining {
                    if let Err(err) = conn.as_mut().await {
                        tracing::trace!("failed to serve connection: {err:#}");
                    }
                    break;
                }
                tokio::select! {
                    result = conn.as_mut() => {
                        if let Err(err) = result {
                            tracing::trace!("failed to serve connection: {err:#}");
                        }
                        break;
                    }
                    _ = signal_tx.closed() => {
                        conn.as_mut().graceful_shutdown();
                        draining = true;
                    }
                }
            }
            drop(close_rx);
        });
    }

    drop(close_rx);
    drop(listener);
    close_tx.closed().await;
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
    accelerator_report::log_report(&settings);
    tracing::info!(address = %listen_addr, "inference service listening (inferio mode)");

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let manager = Arc::clone(&state.manager);
    let cleanup = tokio::spawn(async move {
        shutdown::wait_for_signal(false).await;
        let _ = shutdown_tx.send(());
        shutdown::run_inferio_cleanup(manager).await;
    });
    tracing::info!(
        max_concurrent_streams = MAX_CONCURRENT_STREAMS,
        "serving HTTP/1.1 and HTTP/2 cleartext"
    );
    serve_with_stream_limit(listener, app, async move {
        let _ = shutdown_rx.await;
    })
    .await?;
    let _ = cleanup.await;
    tracing::info!("inference service stopped");
    Ok(())
}

#[cfg(test)]
mod route_tests {
    use super::*;

    /// `serve_with_stream_limit` replaces `axum::serve` at both listener
    /// sites, so the three things `axum::serve` gave us for free are
    /// asserted here rather than assumed: it answers, the `ConnectInfo`
    /// extension the policy layer and the access log read is populated, and
    /// the graceful shutdown both stops accepting and lets the serve future
    /// return.
    ///
    /// The stream limit itself is measured end to end in
    /// `inferio_client::tests::the_server_and_the_pool_bound_concurrent_predicts`,
    /// which drives this same loop.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_serve_loop_answers_with_connect_info_and_then_drains() {
        use axum::extract::ConnectInfo;
        use std::net::SocketAddr;

        let app = Router::new().route(
            "/peer",
            get(|ConnectInfo(peer): ConnectInfo<SocketAddr>| async move { peer.to_string() }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(serve_with_stream_limit(listener, app, async move {
            let _ = stop_rx.await;
        }));

        // Over h2c with prior knowledge: the version the inference client
        // speaks, and the one the stream limit applies to.
        let client = reqwest::Client::builder()
            .http2_prior_knowledge()
            .build()
            .unwrap();
        let body = client
            .get(format!("http://{addr}/peer"))
            .send()
            .await
            .expect("the serve loop answers")
            .text()
            .await
            .unwrap();
        assert!(
            body.starts_with("127.0.0.1:"),
            "ConnectInfo must carry the peer address, not a default: {body}"
        );

        let _ = stop_tx.send(());
        let drained = tokio::time::timeout(std::time::Duration::from_secs(10), server)
            .await
            .expect("the serve future returns once the signal fires and connections drain");
        drained.expect("no panic").expect("clean shutdown");
        assert!(
            tokio::net::TcpStream::connect(addr).await.is_err(),
            "the listener must be closed once the serve future returns"
        );
    }

    #[test]
    fn relay_pairing_route_shapes_do_not_conflict() {
        let _: Router<Arc<proxy::ProxyState>> = Router::new()
            .route(
                "/api/relay/pairings/{relay_id}",
                get(api::relay::get_pairing).delete(api::relay::delete_pairing),
            )
            .route(
                "/api/relay/pairing-operations/{relay_id}",
                get(api::relay::get_pairing_operation).post(api::relay::begin_pairing_operation),
            )
            .route(
                "/api/relay/pairing-operations/{operation_id}/commit",
                axum::routing::put(api::relay::commit_pairing_operation),
            )
            .route(
                "/api/relay/pairing-operations/{operation_id}/cancel",
                axum::routing::delete(api::relay::cancel_pairing_operation),
            );
    }

    /// `/api/video/jobs/{job_id}` and its `/events` child are the one place
    /// in the video surface where a path parameter is followed by a literal
    /// segment. Both shapes must reach their own handler, and the job id must
    /// not swallow `events`.
    #[tokio::test]
    async fn video_job_routes_do_not_shadow_the_events_route() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let app: Router = Router::new()
            .route("/api/video/transcode", post(|| async { "submit" }))
            .route("/api/video/artifact", get(|| async { "artifact" }))
            .route(
                "/api/video/jobs/{job_id}",
                get(
                    |axum::extract::Path(id): axum::extract::Path<String>| async move {
                        format!("job {id}")
                    },
                )
                .delete(|| async { "cancel" }),
            )
            .route(
                "/api/video/jobs/{job_id}/events",
                get(
                    |axum::extract::Path(id): axum::extract::Path<String>| async move {
                        format!("events {id}")
                    },
                ),
            )
            .route("/api/video/presets", get(|| async { "presets" }));

        async fn call(app: &Router, method: &str, path: &str) -> (StatusCode, String) {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("route");
            let status = response.status();
            let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
                .await
                .expect("body");
            (status, String::from_utf8(body.to_vec()).expect("utf8"))
        }

        assert_eq!(
            call(&app, "GET", "/api/video/jobs/abc").await,
            (StatusCode::OK, "job abc".to_string())
        );
        assert_eq!(
            call(&app, "GET", "/api/video/jobs/abc/events").await,
            (StatusCode::OK, "events abc".to_string())
        );
        assert_eq!(
            call(&app, "DELETE", "/api/video/jobs/abc").await,
            (StatusCode::OK, "cancel".to_string())
        );
        assert_eq!(
            call(&app, "GET", "/api/video/presets").await,
            (StatusCode::OK, "presets".to_string())
        );
        // The artifact route is a sibling, not a job id.
        assert_eq!(
            call(&app, "GET", "/api/video/artifact").await,
            (StatusCode::OK, "artifact".to_string())
        );
    }

    /// The pinboard content search is a literal segment sitting where every
    /// other pinboard route has a `{pinboard_id}` path param. axum 0.8 gives
    /// the literal priority and still matches ids, but that is a routing
    /// subtlety worth pinning: this replicates the registered path set with
    /// stand-in handlers and asserts both shapes reach the right one.
    #[tokio::test]
    async fn pinboard_search_route_does_not_shadow_the_id_route() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let app: Router = Router::new()
            .route(
                "/api/pinboards",
                get(|| async { "list" }).post(|| async { "create" }),
            )
            .route("/api/pinboards/search", post(|| async { "search" }))
            .route(
                "/api/pinboards/{pinboard_id}",
                get(
                    |axum::extract::Path(id): axum::extract::Path<i64>| async move {
                        format!("board {id}")
                    },
                ),
            )
            .route(
                "/api/pinboards/{pinboard_id}/versions",
                get(|| async { "versions" }),
            );

        async fn call(app: &Router, method: &str, path: &str) -> (StatusCode, String) {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("route");
            let status = response.status();
            let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
                .await
                .expect("body");
            (status, String::from_utf8(body.to_vec()).expect("utf8"))
        }

        assert_eq!(
            call(&app, "POST", "/api/pinboards/search").await,
            (StatusCode::OK, "search".to_string())
        );
        assert_eq!(
            call(&app, "GET", "/api/pinboards/123").await,
            (StatusCode::OK, "board 123".to_string())
        );
        // The literal wins outright: axum does *not* fall back to the param
        // route for a method the literal route does not serve. Harmless,
        // because `{pinboard_id}` is an i64 — `/api/pinboards/search` was
        // never a reachable board URL — but worth pinning, since it is the
        // one behavior that would bite if ids ever became names.
        assert_eq!(
            call(&app, "GET", "/api/pinboards/search").await.0,
            StatusCode::METHOD_NOT_ALLOWED
        );
        assert_eq!(
            call(&app, "GET", "/api/pinboards/123/versions").await,
            (StatusCode::OK, "versions".to_string())
        );
    }
}
