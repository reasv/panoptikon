mod lifecycle;
mod paths;
mod relay;
mod settings;
mod supervisor;

use crate::{
    lifecycle::{ActivationIntent, LifecycleState, activation_intent},
    paths::DesktopPaths,
    relay::{PathMapping, RelayAction, RelayHandle, RelayState},
    settings::SettingsDocument,
    supervisor::{StatusSnapshot, Supervisor},
};
use serde::{Deserialize, Serialize};
use std::{
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tauri::{
    AppHandle, Emitter as _, Listener as _, Manager as _, WebviewUrl, WebviewWindow,
    WebviewWindowBuilder,
    menu::{CheckMenuItem, MenuBuilder, MenuItem},
    tray::TrayIconBuilder,
};
use tauri_plugin_autostart::ManagerExt as _;
use tauri_plugin_dialog::DialogExt;
use tauri_plugin_opener::OpenerExt as _;
use tauri_plugin_updater::UpdaterExt as _;
use tokio::sync::Mutex;
use uuid::Uuid;

const DEV_IDENTIFIER: &str = "app.panoptikon.desktop.dev";
const PROD_SERVER_CONFIG: &str = "config/server/desktop.toml";
const DEV_SERVER_CONFIG: &str = "config/server/desktop-dev.toml";
const PROD_SERVER_PORT: u16 = 6342;
const DEV_SERVER_PORT: u16 = 16342;
const SETUP_WINDOW_WIDTH: f64 = 1200.0;
const SETUP_WINDOW_HEIGHT: f64 = 800.0;

struct RuntimeState {
    relay_handle: Mutex<Option<RelayHandle>>,
    tray: Mutex<Option<TrayUi>>,
    startup_warnings: Vec<String>,
    pending_open: AtomicBool,
    interactive_startup_seen: AtomicBool,
    automated_setup_seen: AtomicBool,
    automated_setup_failed: AtomicBool,
    setup_start_notified: AtomicBool,
    setup_completion_notified: AtomicBool,
    startup_activity: Mutex<Option<String>>,
    setup_failure: Mutex<Option<String>>,
    _log_guard: tracing_appender::non_blocking::WorkerGuard,
}

struct TrayUi {
    status: MenuItem<tauri::Wry>,
    restart: MenuItem<tauri::Wry>,
    local: CheckMenuItem<tauri::Wry>,
    autostart: CheckMenuItem<tauri::Wry>,
}

#[derive(Debug, Serialize)]
struct UpdateInfo {
    version: String,
    current_version: String,
    notes: Option<String>,
    date: Option<String>,
}

pub fn run() {
    let builder = tauri::Builder::default()
        // Normative ordering: a secondary instance must exit before any
        // path, logging, tray, Relay, updater, or sidecar state is created.
        .plugin(tauri_plugin_single_instance::init(|app, args, _cwd| {
            route_activation(app.clone(), activation_intent(&args));
        }))
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_autostart::init(
            tauri_plugin_autostart::MacosLauncher::LaunchAgent,
            Some(vec!["--background"]),
        ))
        .plugin(tauri_plugin_updater::Builder::new().build())
        .invoke_handler(tauri::generate_handler![
            get_status, get_startup_warnings, open_action_command, open_setup_command, restart_server, set_local_server_enabled,
            set_start_at_login, open_known_folder, log_tail, choose_scan_folders, open_panoptikon_page,
            relay_status, relay_pending, relay_approve, relay_reject, relay_revoke,
            relay_set_mappings, set_relay_enabled, check_for_updates, install_update, quit_desktop
        ])
        .setup(|app| {
            let development = app.config().identifier == DEV_IDENTIFIER;
            let (server_config, default_port) = if development {
                (DEV_SERVER_CONFIG, DEV_SERVER_PORT)
            } else {
                (PROD_SERVER_CONFIG, PROD_SERVER_PORT)
            };
            let resolver = app.path();
            let paths = DesktopPaths::new(
                resolver.app_config_dir()?,
                resolver.app_local_data_dir()?,
                resolver.app_log_dir()?,
            );
            paths.ensure_shell_dirs()?;
            let log_guard = init_logging(&paths)?;
            let mut startup_warnings = Vec::new();
            let settings = match SettingsDocument::load(&paths) {
                Ok(settings) => settings,
                Err(error) => {
                    startup_warnings.push(error.to_string());
                    SettingsDocument::defaults(paths.desktop_settings.clone())?
                }
            };
            let relay_config = match relay::load_config(&paths.relay_settings, development) {
                Ok(config) => config,
                Err(error) => {
                    startup_warnings.push(error.to_string());
                    relay::RelayConfig::desktop_default(development)
                }
            };
            let supervisor = Arc::new(Supervisor::new(
                paths.clone(),
                settings,
                server_config,
                default_port,
            ));
            let opener = app.handle().clone();
            let action_handler = Arc::new(move |action: RelayAction, path: std::path::PathBuf| {
                match action {
                    RelayAction::OpenFile => opener.opener().open_path(path.display().to_string(), None::<&str>)?,
                    RelayAction::RevealInFolder => opener.opener().reveal_item_in_dir(path)?,
                }
                Ok(())
            });
            let relay_state = Arc::new(RelayState::new(relay_config, paths.relay_settings.clone(), action_handler));
            app.manage(supervisor.clone());
            app.manage(relay_state.clone());
            let has_startup_warnings = !startup_warnings.is_empty();
            app.manage(RuntimeState {
                relay_handle: Mutex::new(None),
                tray: Mutex::new(None),
                startup_warnings,
                pending_open: AtomicBool::new(false),
                interactive_startup_seen: AtomicBool::new(false),
                automated_setup_seen: AtomicBool::new(false),
                automated_setup_failed: AtomicBool::new(false),
                setup_start_notified: AtomicBool::new(false),
                setup_completion_notified: AtomicBool::new(false),
                startup_activity: Mutex::new(None),
                setup_failure: Mutex::new(None),
                _log_guard: log_guard,
            });
            let restart_app = app.handle().clone();
            app.listen("desktop-internal-restart", move |_| {
                let restart_app = restart_app.clone();
                tauri::async_runtime::spawn(async move {
                    if let Err(error) = Supervisor::start(restart_app.clone()).await {
                        restart_app.state::<Arc<Supervisor>>().set_state(&restart_app, LifecycleState::Failed(error.to_string())).await;
                        notify_startup_result(&restart_app, false).await;
                        if restart_app.state::<RuntimeState>().pending_open.load(Ordering::Acquire) {
                            let _ = show_launch_window(&restart_app, true);
                        }
                    }
                });
            });

            let tray_result = std::panic::catch_unwind(AssertUnwindSafe(|| create_tray(app.handle())));
            match tray_result {
                Ok(Ok(tray)) => {
                    let runtime = app.state::<RuntimeState>();
                    tauri::async_runtime::block_on(async { *runtime.tray.lock().await = Some(tray); });
                }
                Ok(Err(error)) => {
                    tracing::error!(%error, "tray initialization failed; using visible control fallback");
                    show_control_window(app.handle(), true)?;
                }
                Err(_) => {
                    tracing::error!("tray backend panicked; using visible control fallback");
                    show_control_window(app.handle(), true)?;
                }
            }
            if has_startup_warnings {
                show_control_window(app.handle(), true)?;
            }

            let relay_app = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                if relay_state.config().await.enabled {
                    match relay::start(relay_state).await {
                        Ok(handle) => *relay_app.state::<RuntimeState>().relay_handle.lock().await = Some(handle),
                        Err(error) => {
                            supervisor.set_state(&relay_app, LifecycleState::Failed(format!("Relay startup failed: {error}"))).await;
                            let _ = show_control_window(&relay_app, true);
                        }
                    }
                }
            });
            let start_app = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                if let Err(error) = Supervisor::start(start_app.clone()).await {
                    start_app.state::<Arc<Supervisor>>().set_state(&start_app, LifecycleState::Failed(error.to_string())).await;
                    notify_startup_result(&start_app, false).await;
                    if start_app.state::<RuntimeState>().pending_open.load(Ordering::Acquire) {
                        let _ = show_launch_window(&start_app, true);
                    }
                }
            });
            let update_app = app.handle().clone();
            tauri::async_runtime::spawn(async move { automatic_update_check(update_app).await; });
            let intent = activation_intent(&std::env::args().collect::<Vec<_>>());
            if intent == ActivationIntent::Open { route_activation(app.handle().clone(), intent); }
            Ok(())
        });

    let app = builder
        .build(tauri::generate_context!())
        .expect("error while building Panoptikon Desktop");
    app.run(|_app, event| {
        if let tauri::RunEvent::ExitRequested {
            code: None, api, ..
        } = event
        {
            // Closing the last webview must not terminate the tray process.
            // Explicit Quit and updater restart requests carry an exit code
            // and are therefore allowed through.
            api.prevent_exit();
        }
    });
}

fn init_logging(
    paths: &DesktopPaths,
) -> anyhow::Result<tracing_appender::non_blocking::WorkerGuard> {
    let appender = tracing_appender::rolling::daily(&paths.log_dir, "panoptikon-desktop.log");
    let (writer, guard) = tracing_appender::non_blocking(appender);
    use tracing_subscriber::prelude::*;
    let subscriber = tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(writer),
        );
    let _ = subscriber.try_init();
    Ok(guard)
}

fn create_tray(app: &AppHandle) -> tauri::Result<TrayUi> {
    let status = MenuItem::with_id(app, "status", "Status: Installing", false, None::<&str>)?;
    let restart = MenuItem::with_id(app, "restart", "Restart Panoptikon", true, None::<&str>)?;
    let local_enabled = tauri::async_runtime::block_on(async {
        app.state::<Arc<Supervisor>>()
            .settings
            .lock()
            .await
            .typed
            .local_server
            .enabled
    });
    let autostart_enabled = app.autolaunch().is_enabled().unwrap_or(false);
    let local = CheckMenuItem::with_id(
        app,
        "local",
        "Run Local Panoptikon",
        true,
        local_enabled,
        None::<&str>,
    )?;
    let autostart = CheckMenuItem::with_id(
        app,
        "autostart",
        "Start at Login",
        true,
        autostart_enabled,
        None::<&str>,
    )?;
    let menu = MenuBuilder::new(app)
        .text("open", "Open Panoptikon")
        .item(&status)
        .separator()
        .item(&restart)
        .item(&local)
        .text("relay", "Relay…")
        .text("settings", "Desktop Settings…")
        .text("logs", "View Logs")
        .text("data", "Open Data Folder")
        .text("updates", "Check for Updates…")
        .separator()
        .item(&autostart)
        .text("quit", "Quit Panoptikon")
        .build()?;
    let mut tray = TrayIconBuilder::with_id("panoptikon-desktop")
        .menu(&menu)
        .tooltip("Panoptikon Desktop");
    if let Some(icon) = app.default_window_icon().cloned() {
        tray = tray.icon(icon);
    }
    tray.on_menu_event(|app, event| match event.id().as_ref() {
        "open" => route_activation(app.clone(), ActivationIntent::Open),
        "restart" => {
            let app = app.clone();
            tauri::async_runtime::spawn(async move {
                if let Err(error) = Supervisor::restart(app.clone()).await {
                    app.state::<Arc<Supervisor>>()
                        .set_state(&app, LifecycleState::Failed(error.to_string()))
                        .await;
                    notify_startup_result(&app, false).await;
                }
            });
        }
        "local" => {
            let app = app.clone();
            tauri::async_runtime::spawn(async move {
                let was_enabled = app
                    .state::<Arc<Supervisor>>()
                    .settings
                    .lock()
                    .await
                    .typed
                    .local_server
                    .enabled;
                if was_enabled {
                    // The tray cannot present the same explicit confirmation
                    // as the bundled control UI. Restore its checkmark and
                    // route the user to the confirmed disable action there.
                    if let Some(tray) = app.state::<RuntimeState>().tray.lock().await.as_ref() {
                        let _ = tray.local.set_checked(true);
                    }
                    let _ = show_control_window(&app, true);
                } else {
                    let _ = set_local_enabled_inner(app, true, true).await;
                }
            });
        }
        "relay" | "settings" => {
            let _ = show_control_window(app, true);
        }
        "logs" => {
            let _ = open_folder(app, "logs");
        }
        "data" => {
            let _ = open_folder(app, "server_root");
        }
        "updates" => {
            let _ = show_control_window(app, true);
        }
        "autostart" => {
            let app = app.clone();
            tauri::async_runtime::spawn(async move {
                let enabled = !app.autolaunch().is_enabled().unwrap_or(false);
                let _ = set_autostart_inner(&app, enabled).await;
            });
        }
        "quit" => {
            let app = app.clone();
            tauri::async_runtime::spawn(async move {
                quit_inner(app).await;
            });
        }
        _ => {}
    })
    .build(app)?;
    Ok(TrayUi {
        status,
        restart,
        local,
        autostart,
    })
}

pub(crate) async fn update_tray(app: &AppHandle, state: &LifecycleState) {
    if let Some(tray) = app.state::<RuntimeState>().tray.lock().await.as_ref() {
        let _ = tray.status.set_text(format!("Status: {}", state.label()));
        let _ = tray.restart.set_enabled(!matches!(
            state,
            LifecycleState::LocalServerDisabled | LifecycleState::Stopping
        ));
    }
}

fn route_activation(app: AppHandle, intent: ActivationIntent) {
    if intent != ActivationIntent::Open {
        return;
    }
    tauri::async_runtime::spawn(async move {
        let _ = open_action_inner(&app).await;
    });
}

async fn open_action_inner(app: &AppHandle) -> Result<(), String> {
    let supervisor = app.state::<Arc<Supervisor>>().inner().clone();
    let snapshot = supervisor.snapshot().await;
    if !snapshot.local_server_enabled {
        return show_control_window(app, true).map_err(|e| e.to_string());
    }
    if snapshot.state != LifecycleState::Ready {
        if matches!(
            snapshot.state,
            LifecycleState::Installing
                | LifecycleState::Starting
                | LifecycleState::SettingUp
                | LifecycleState::Restarting
        ) {
            app.state::<RuntimeState>()
                .pending_open
                .store(true, Ordering::Release);
            return show_launch_window(app, true).map_err(|e| e.to_string());
        }
        if matches!(
            snapshot.state,
            LifecycleState::Degraded(_) | LifecycleState::Failed(_)
        ) {
            return show_launch_window(app, true).map_err(|e| e.to_string());
        }
        return show_control_window(app, true).map_err(|e| e.to_string());
    }
    if fetch_setup_status(snapshot.port).await?.ready {
        close_launch_window(app);
        app.opener()
            .open_url(local_browser_url(snapshot.port, "/search"), None::<&str>)
            .map_err(|e| e.to_string())
    } else {
        show_setup_window(app, snapshot.port, SetupMode::Onboarding).map_err(|e| e.to_string())
    }
}

#[derive(Serialize)]
struct LaunchView {
    kind: &'static str,
    activity: Option<String>,
    error: Option<String>,
    diagnostics: String,
}

#[derive(Debug, Deserialize)]
struct DesktopSetupStatus {
    ready: bool,
}

async fn fetch_setup_status(port: u16) -> Result<DesktopSetupStatus, String> {
    let response = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
        .map_err(|error| error.to_string())?
        .get(local_browser_url(port, "/api/desktop/setup-status"))
        .send()
        .await
        .map_err(|error| format!("failed to read Desktop setup status: {error}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "Desktop setup status returned HTTP {}",
            response.status()
        ));
    }
    response
        .json()
        .await
        .map_err(|error| format!("invalid Desktop setup status: {error}"))
}

fn lifecycle_kind(state: &LifecycleState) -> &'static str {
    match state {
        LifecycleState::Installing => "installing",
        LifecycleState::Starting => "starting",
        LifecycleState::SettingUp => "setting_up",
        LifecycleState::Ready => "ready",
        LifecycleState::Degraded(_) => "degraded",
        LifecycleState::LocalServerDisabled => "local_server_disabled",
        LifecycleState::Stopping => "stopping",
        LifecycleState::Failed(_) => "failed",
        LifecycleState::Restarting => "restarting",
        LifecycleState::Exited => "exited",
    }
}

pub(crate) async fn refresh_launch_window(app: &AppHandle) {
    let Some(window) = app.get_webview_window("launch") else {
        return;
    };
    if !window
        .url()
        .is_ok_and(|url| url.path().ends_with("/launch.html"))
    {
        return;
    }
    let supervisor = app.state::<Arc<Supervisor>>().inner().clone();
    let snapshot = supervisor.snapshot().await;
    let runtime = app.state::<RuntimeState>();
    let error = match &snapshot.state {
        LifecycleState::Failed(detail) | LifecycleState::Degraded(detail) => Some(detail.clone()),
        _ => runtime.setup_failure.lock().await.clone(),
    };
    let activity = if matches!(snapshot.state, LifecycleState::Installing) {
        runtime.startup_activity.lock().await.clone()
    } else {
        None
    };
    let logs = supervisor.tail(150).await;
    let diagnostics = format!(
        "Panoptikon Desktop {}\nOS: {} {}\nState: {}\nPort: {}\nServer root: {}\n\nRecent output:\n{}",
        app.package_info().version,
        std::env::consts::OS,
        std::env::consts::ARCH,
        snapshot.state_label,
        snapshot.port,
        snapshot.server_root,
        if logs.is_empty() {
            "No output yet.".into()
        } else {
            logs.join("\n")
        },
    );
    let view = LaunchView {
        kind: lifecycle_kind(&snapshot.state),
        activity,
        error,
        diagnostics,
    };
    if let Ok(json) = serde_json::to_string(&view) {
        let _ = window.eval(format!("window.updateLaunchState?.({json})"));
    }
}

pub(crate) fn show_launch_window(app: &AppHandle, focus: bool) -> tauri::Result<()> {
    app.state::<RuntimeState>()
        .interactive_startup_seen
        .store(true, Ordering::Release);
    let window = if let Some(window) = app.get_webview_window("launch") {
        if !window.url()?.path().ends_with("/launch.html") {
            window.destroy()?;
            return show_launch_window(app, focus);
        }
        window
    } else {
        WebviewWindowBuilder::new(app, "launch", WebviewUrl::App("launch.html".into()))
            .title("Starting Panoptikon")
            .inner_size(720.0, 720.0)
            .min_inner_size(540.0, 540.0)
            .on_page_load(|window, payload| {
                if payload.event() == tauri::webview::PageLoadEvent::Finished
                    && payload.url().path().ends_with("/launch.html")
                {
                    let app = window.app_handle().clone();
                    tauri::async_runtime::spawn(async move { refresh_launch_window(&app).await });
                }
            })
            .build()?
    };
    let close_app = app.clone();
    window.on_window_event(move |event| {
        if let tauri::WindowEvent::CloseRequested { .. } = event {
            close_app
                .state::<RuntimeState>()
                .pending_open
                .store(false, Ordering::Release);
        }
    });
    window.show()?;
    if focus {
        window.unminimize()?;
        window.set_focus()?;
    }
    let refresh_app = app.clone();
    tauri::async_runtime::spawn(async move { refresh_launch_window(&refresh_app).await });
    Ok(())
}

fn close_launch_window(app: &AppHandle) {
    if let Some(window) = app.get_webview_window("launch") {
        let _ = window.destroy();
    }
}

pub(crate) async fn open_pending_action(app: &AppHandle) {
    if app
        .state::<RuntimeState>()
        .pending_open
        .swap(false, Ordering::AcqRel)
    {
        if let Err(error) = open_action_inner(app).await {
            tracing::warn!(%error, "failed to fulfill pending Desktop open action");
            let _ = show_control_window(app, true);
        }
    }
}

pub(crate) fn show_control_window(app: &AppHandle, focus: bool) -> tauri::Result<()> {
    let window = if let Some(window) = app.get_webview_window("control") {
        window
    } else {
        let window =
            WebviewWindowBuilder::new(app, "control", WebviewUrl::App("index.html".into()))
                .title("Panoptikon Desktop")
                .inner_size(780.0, 680.0)
                .min_inner_size(560.0, 480.0)
                .build()?;
        let close_window = window.clone();
        window.on_window_event(move |event| {
            if let tauri::WindowEvent::CloseRequested { api, .. } = event {
                api.prevent_close();
                let _ = close_window.hide();
            }
        });
        window
    };
    window.show()?;
    if focus {
        window.unminimize()?;
        window.set_focus()?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum SetupMode {
    Onboarding,
    NewDatabase,
}

impl SetupMode {
    fn query(self) -> &'static str {
        match self {
            Self::Onboarding => "onboarding",
            Self::NewDatabase => "new-database",
        }
    }

    fn title(self) -> &'static str {
        match self {
            Self::Onboarding => "Set up Panoptikon",
            Self::NewDatabase => "New Panoptikon database",
        }
    }
}

fn show_setup_window(app: &AppHandle, port: u16, mode: SetupMode) -> tauri::Result<()> {
    let url = local_browser_url(port, &format!("/desktop/setup?mode={}", mode.query()))
        .parse()
        .map_err(|error| tauri::Error::InvalidUrl(error))?;
    let window = if let Some(window) = app.get_webview_window("launch") {
        window.navigate(url)?;
        window.set_title(mode.title())?;
        window.set_size(tauri::LogicalSize::new(
            SETUP_WINDOW_WIDTH,
            SETUP_WINDOW_HEIGHT,
        ))?;
        window
    } else {
        WebviewWindowBuilder::new(app, "launch", WebviewUrl::External(url))
            .title(mode.title())
            .inner_size(SETUP_WINDOW_WIDTH, SETUP_WINDOW_HEIGHT)
            .build()?
    };
    window.show()?;
    window.set_focus()
}

fn send_clickable_notification(app: &AppHandle, title: &str, body: &str) {
    let app_handle = app.clone();
    let title = title.to_owned();
    let body = body.to_owned();
    let identifier = app.config().identifier.clone();
    let product_name = app.package_info().name.clone();
    tauri::async_runtime::spawn_blocking(move || {
        let mut notification = notify_rust::Notification::new();
        notification
            .appname(&product_name)
            .app_id(&identifier)
            .summary(&title)
            .body(&body)
            .auto_icon()
            .action("open", "Open Panoptikon");
        match notification.show() {
            Ok(handle) => {
                let clicked = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
                let clicked_response = clicked.clone();
                if let Err(error) =
                    handle.wait_for_response(move |response: &notify_rust::NotificationResponse| {
                        clicked_response.store(
                            matches!(
                                response,
                                notify_rust::NotificationResponse::Default
                                    | notify_rust::NotificationResponse::Action(_)
                            ),
                            Ordering::Release,
                        );
                    })
                {
                    tracing::warn!(%error, "failed to wait for Desktop notification response");
                }
                if clicked.load(Ordering::Acquire) {
                    route_activation(app_handle, ActivationIntent::Open);
                }
            }
            Err(error) => tracing::warn!(%error, "failed to show Desktop notification"),
        }
    });
}

pub(crate) async fn observe_server_progress(app: &AppHandle, line: &str) {
    let runtime = app.state::<RuntimeState>();
    let lower = line.to_ascii_lowercase();
    let activity = if lower.contains("running setup automatically") {
        runtime.automated_setup_seen.store(true, Ordering::Release);
        if !runtime.setup_start_notified.swap(true, Ordering::AcqRel) {
            send_clickable_notification(
                app,
                "Panoptikon preparation has started",
                "First-time preparation is running in the background and may take a while. Click to view progress.",
            );
        }
        Some("Checking the local AI environment…")
    } else if lower.contains("accelerator selected") {
        Some("Selecting the best AI runtime for this computer…")
    } else if lower.contains("creating the managed venv") {
        Some("Creating the local AI environment…")
    } else if lower.contains("syncing the locked environment") {
        Some("Downloading and installing local AI components…")
    } else if lower.contains("python inference environment is ready") {
        Some("Local AI components are ready. Starting Panoptikon…")
    } else {
        None
    };
    if let Some(activity) = activity {
        *runtime.startup_activity.lock().await = Some(activity.into());
        if runtime.automated_setup_seen.load(Ordering::Acquire) {
            app.state::<Arc<Supervisor>>()
                .set_state(app, LifecycleState::Installing)
                .await;
        } else {
            refresh_launch_window(app).await;
        }
    }
    if lower.contains("automatic python environment setup failed") {
        runtime
            .automated_setup_failed
            .store(true, Ordering::Release);
        let detail = "The local AI environment could not be prepared. Panoptikon can start, but AI features will be unavailable until this is fixed.".to_owned();
        *runtime.setup_failure.lock().await = Some(detail.clone());
        app.state::<Arc<Supervisor>>()
            .set_state(app, LifecycleState::Degraded(detail))
            .await;
        notify_startup_result(app, false).await;
    }
}

pub(crate) async fn notify_startup_result(app: &AppHandle, ready: bool) {
    let runtime = app.state::<RuntimeState>();
    let automated_setup = runtime.automated_setup_seen.load(Ordering::Acquire);
    let interactive_startup = runtime.interactive_startup_seen.load(Ordering::Acquire);
    if (ready && !automated_setup && !interactive_startup)
        || runtime
            .setup_completion_notified
            .swap(true, Ordering::AcqRel)
    {
        return;
    }
    if ready && !runtime.automated_setup_failed.load(Ordering::Acquire) {
        let default_database_ready =
            match fetch_setup_status(app.state::<Arc<Supervisor>>().snapshot().await.port).await {
                Ok(status) => status.ready,
                Err(error) => {
                    tracing::warn!(%error, "could not determine default database readiness");
                    false
                }
            };
        send_clickable_notification(
            app,
            if automated_setup {
                "Panoptikon preparation is complete"
            } else {
                "Panoptikon is ready"
            },
            if default_database_ready {
                "Panoptikon is ready. Click to open Search."
            } else {
                "Panoptikon is ready. Click to set up your first database."
            },
        );
    } else {
        send_clickable_notification(
            app,
            if automated_setup {
                "Panoptikon preparation needs attention"
            } else {
                "Panoptikon could not start"
            },
            if automated_setup {
                "Automatic preparation did not complete. Click to view the error and diagnostics."
            } else {
                "Startup did not complete. Click to view the error and diagnostics."
            },
        );
    }
}

fn local_browser_url(port: u16, path: &str) -> String {
    format!("http://localhost:{port}{path}")
}

#[tauri::command]
async fn open_setup_command(window: WebviewWindow, app: AppHandle) -> Result<(), String> {
    validate_control(&window)?;
    let snapshot = app.state::<Arc<Supervisor>>().snapshot().await;
    if !snapshot.local_server_enabled || snapshot.state != LifecycleState::Ready {
        return Err("the local Server must be ready before setup can open".into());
    }
    let mode = if fetch_setup_status(snapshot.port).await?.ready {
        SetupMode::NewDatabase
    } else {
        SetupMode::Onboarding
    };
    show_setup_window(&app, snapshot.port, mode).map_err(|error| error.to_string())
}

fn validate_control(window: &WebviewWindow) -> Result<(), String> {
    if window.label() != "control" {
        Err("command is restricted to the bundled control window".into())
    } else {
        Ok(())
    }
}

fn validate_setup_window(window: &WebviewWindow) -> Result<(), String> {
    if window.label() != "launch" {
        return Err("command is restricted to the setup window".into());
    }
    let url = window.url().map_err(|error| error.to_string())?;
    if url.scheme() != "http"
        || url.host_str() != Some("localhost")
        || url.path() != "/desktop/setup"
    {
        return Err("command is restricted to the local setup page".into());
    }
    Ok(())
}

#[tauri::command]
async fn choose_scan_folders(window: WebviewWindow) -> Result<Vec<String>, String> {
    validate_setup_window(&window)?;
    let (send, receive) = tokio::sync::oneshot::channel();
    window
        .app_handle()
        .dialog()
        .file()
        .set_parent(&window)
        .set_title("Choose folders for Panoptikon")
        .pick_folders(move |folders| {
            let _ = send.send(folders);
        });
    let folders = receive
        .await
        .map_err(|_| "the folder picker closed unexpectedly".to_string())?
        .unwrap_or_default();
    Ok(folders
        .into_iter()
        .filter_map(|folder| folder.into_path().ok())
        .map(|path| path.to_string_lossy().into_owned())
        .collect())
}

#[tauri::command]
async fn open_panoptikon_page(
    window: WebviewWindow,
    app: AppHandle,
    page: String,
    index_db: String,
) -> Result<(), String> {
    validate_setup_window(&window)?;
    let path = match page.as_str() {
        "search" => "/search",
        "scan" => "/scan",
        _ => return Err("only the Search and Scan pages can be opened from setup".into()),
    };
    let snapshot = app.state::<Arc<Supervisor>>().snapshot().await;
    if !snapshot.local_server_enabled || snapshot.state != LifecycleState::Ready {
        return Err("the local Server is not ready".into());
    }
    let mut url = url::Url::parse(&local_browser_url(snapshot.port, path))
        .map_err(|error| error.to_string())?;
    url.query_pairs_mut().append_pair("index_db", &index_db);
    app.opener()
        .open_url(url.as_str(), None::<&str>)
        .map_err(|error| error.to_string())
}

#[tauri::command]
async fn get_status(window: WebviewWindow, app: AppHandle) -> Result<StatusSnapshot, String> {
    validate_control(&window)?;
    let mut snapshot = app.state::<Arc<Supervisor>>().snapshot().await;
    if snapshot.local_server_enabled && snapshot.state == LifecycleState::Ready {
        match fetch_setup_status(snapshot.port).await {
            Ok(status) => snapshot.default_database_ready = Some(status.ready),
            Err(error) => {
                tracing::warn!(%error, "could not refresh default database readiness");
            }
        }
    }
    Ok(snapshot)
}

#[tauri::command]
fn get_startup_warnings(window: WebviewWindow, app: AppHandle) -> Result<Vec<String>, String> {
    validate_control(&window)?;
    Ok(app.state::<RuntimeState>().startup_warnings.clone())
}

#[tauri::command]
async fn open_action_command(window: WebviewWindow, app: AppHandle) -> Result<(), String> {
    validate_control(&window)?;
    open_action_inner(&app).await
}

#[tauri::command]
async fn restart_server(window: WebviewWindow, app: AppHandle) -> Result<(), String> {
    validate_control(&window)?;
    if let Err(error) = Supervisor::restart(app.clone()).await {
        let detail = error.to_string();
        app.state::<Arc<Supervisor>>()
            .set_state(&app, LifecycleState::Failed(detail.clone()))
            .await;
        notify_startup_result(&app, false).await;
        return Err(detail);
    }
    Ok(())
}

#[tauri::command]
async fn set_local_server_enabled(
    window: WebviewWindow,
    app: AppHandle,
    enabled: bool,
    confirmed: bool,
) -> Result<(), String> {
    validate_control(&window)?;
    set_local_enabled_inner(app, enabled, confirmed).await
}

async fn set_local_enabled_inner(
    app: AppHandle,
    enabled: bool,
    confirmed: bool,
) -> Result<(), String> {
    let supervisor = app.state::<Arc<Supervisor>>().inner().clone();
    let was_enabled = supervisor.settings.lock().await.typed.local_server.enabled;
    if was_enabled && !enabled && !confirmed {
        return Err("disabling the local Server requires confirmation".into());
    }
    {
        let mut settings = supervisor.settings.lock().await;
        settings.typed.local_server.enabled = enabled;
        settings.save().map_err(|e| e.to_string())?;
    }
    if let Some(tray) = app.state::<RuntimeState>().tray.lock().await.as_ref() {
        let _ = tray.local.set_checked(enabled);
    }
    if enabled && !was_enabled {
        if let Err(error) = Supervisor::start(app.clone()).await {
            let detail = error.to_string();
            app.state::<Arc<Supervisor>>()
                .set_state(&app, LifecycleState::Failed(detail.clone()))
                .await;
            notify_startup_result(&app, false).await;
            return Err(detail);
        }
        Ok(())
    } else if !enabled && was_enabled {
        Supervisor::stop(&app, false)
            .await
            .map_err(|e| e.to_string())?;
        supervisor
            .set_state(&app, LifecycleState::LocalServerDisabled)
            .await;
        Ok(())
    } else {
        Ok(())
    }
}

#[tauri::command]
async fn set_start_at_login(
    window: WebviewWindow,
    app: AppHandle,
    enabled: bool,
) -> Result<(), String> {
    validate_control(&window)?;
    set_autostart_inner(&app, enabled).await
}

async fn set_autostart_inner(app: &AppHandle, enabled: bool) -> Result<(), String> {
    if enabled {
        app.autolaunch().enable()
    } else {
        app.autolaunch().disable()
    }
    .map_err(|e| e.to_string())?;
    let supervisor = app.state::<Arc<Supervisor>>().inner().clone();
    {
        let mut settings = supervisor.settings.lock().await;
        settings.typed.startup.start_at_login = enabled;
        settings.save().map_err(|e| e.to_string())?;
    }
    if let Some(tray) = app.state::<RuntimeState>().tray.lock().await.as_ref() {
        let _ = tray.autostart.set_checked(enabled);
    }
    Ok(())
}

#[tauri::command]
fn open_known_folder(window: WebviewWindow, app: AppHandle, kind: String) -> Result<(), String> {
    validate_control(&window)?;
    open_folder(&app, &kind)
}

fn open_folder(app: &AppHandle, kind: &str) -> Result<(), String> {
    let paths = &app.state::<Arc<Supervisor>>().paths;
    let path = match kind {
        "server_root" => &paths.server_root,
        "config" => &paths.config_dir,
        "logs" => &paths.log_dir,
        _ => return Err("unknown folder kind".into()),
    };
    if !paths.path_is_within_shell_roots(path) {
        return Err("folder is outside Desktop-owned roots".into());
    }
    app.opener()
        .open_path(path.display().to_string(), None::<&str>)
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn log_tail(
    window: WebviewWindow,
    app: AppHandle,
    lines: usize,
) -> Result<Vec<String>, String> {
    validate_control(&window)?;
    Ok(app.state::<Arc<Supervisor>>().tail(lines).await)
}

#[tauri::command]
async fn relay_status(
    window: WebviewWindow,
    app: AppHandle,
) -> Result<relay::RelayStatusView, String> {
    validate_control(&window)?;
    Ok(app.state::<Arc<RelayState>>().status().await)
}

#[tauri::command]
async fn set_relay_enabled(
    window: WebviewWindow,
    app: AppHandle,
    enabled: bool,
) -> Result<(), String> {
    validate_control(&window)?;
    let relay_state = app.state::<Arc<RelayState>>().inner().clone();
    relay_state
        .set_enabled(enabled)
        .await
        .map_err(|error| error.to_string())?;
    let runtime = app.state::<RuntimeState>();
    if enabled {
        let mut handle = runtime.relay_handle.lock().await;
        if handle.is_none() {
            *handle = Some(
                relay::start(relay_state)
                    .await
                    .map_err(|error| error.to_string())?,
            );
        }
    } else if let Some(handle) = runtime.relay_handle.lock().await.take() {
        handle.shutdown().await;
    }
    Ok(())
}
#[tauri::command]
async fn relay_pending(
    window: WebviewWindow,
    app: AppHandle,
) -> Result<Vec<relay::PendingPairingView>, String> {
    validate_control(&window)?;
    Ok(app.state::<Arc<RelayState>>().pending().await)
}
#[tauri::command]
async fn relay_approve(
    window: WebviewWindow,
    app: AppHandle,
    request_id: Uuid,
) -> Result<(), String> {
    validate_control(&window)?;
    app.state::<Arc<RelayState>>()
        .approve(request_id)
        .await
        .map_err(|e| e.to_string())
}
#[tauri::command]
async fn relay_reject(
    window: WebviewWindow,
    app: AppHandle,
    request_id: Uuid,
) -> Result<(), String> {
    validate_control(&window)?;
    app.state::<Arc<RelayState>>()
        .reject(request_id)
        .await
        .map_err(|e| e.to_string())
}
#[tauri::command]
async fn relay_revoke(
    window: WebviewWindow,
    app: AppHandle,
    instance_id: Uuid,
) -> Result<(), String> {
    validate_control(&window)?;
    app.state::<Arc<RelayState>>()
        .revoke(instance_id)
        .await
        .map_err(|e| e.to_string())
}
#[tauri::command]
async fn relay_set_mappings(
    window: WebviewWindow,
    app: AppHandle,
    instance_id: Uuid,
    mappings: Vec<PathMapping>,
) -> Result<(), String> {
    validate_control(&window)?;
    app.state::<Arc<RelayState>>()
        .replace_mappings(instance_id, mappings)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn check_for_updates(
    window: WebviewWindow,
    app: AppHandle,
) -> Result<Option<UpdateInfo>, String> {
    validate_control(&window)?;
    if cfg!(debug_assertions) {
        return Ok(None);
    }
    record_update_check(&app).await?;
    fetch_update_info(&app).await
}

async fn fetch_update_info(app: &AppHandle) -> Result<Option<UpdateInfo>, String> {
    let update = app
        .updater()
        .map_err(|e| e.to_string())?
        .check()
        .await
        .map_err(|e| e.to_string())?;
    Ok(update.map(|update| UpdateInfo {
        version: update.version,
        current_version: update.current_version,
        notes: update.body,
        date: update.date.map(|value| value.to_string()),
    }))
}

async fn record_update_check(app: &AppHandle) -> Result<(), String> {
    let supervisor = app.state::<Arc<Supervisor>>().inner().clone();
    let mut settings = supervisor.settings.lock().await;
    settings.typed.updates.last_checked_unix = Some(unix_timestamp());
    settings.save().map_err(|error| error.to_string())
}

async fn automatic_update_check(app: AppHandle) {
    if cfg!(debug_assertions) {
        return;
    }
    // Do not contend with initial sidecar setup. Check once the application is
    // usable (or Relay-only), and never more than once per 24 hours.
    for _ in 0..120 {
        let snapshot = app.state::<Arc<Supervisor>>().snapshot().await;
        if matches!(
            snapshot.state_label.as_str(),
            "Ready" | "Local Server Disabled"
        ) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    let due = {
        let supervisor = app.state::<Arc<Supervisor>>();
        let settings = supervisor.settings.lock().await;
        settings.typed.updates.check_automatically
            && settings
                .typed
                .updates
                .last_checked_unix
                .is_none_or(|last| unix_timestamp().saturating_sub(last) >= 86_400)
    };
    if !due {
        return;
    }
    if let Err(error) = record_update_check(&app).await {
        tracing::warn!(%error, "failed to persist automatic update-check time");
        return;
    }
    match fetch_update_info(&app).await {
        Ok(Some(update)) => {
            let _ = app.emit("desktop-update-available", &update);
            let _ = show_control_window(&app, false);
        }
        Ok(None) => {}
        Err(error) => tracing::warn!(%error, "automatic update check failed"),
    }
}

fn unix_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[tauri::command]
async fn install_update(window: WebviewWindow, app: AppHandle) -> Result<(), String> {
    validate_control(&window)?;
    if cfg!(debug_assertions) {
        return Err("updates are disabled in development builds".into());
    }
    let update = app
        .updater()
        .map_err(|e| e.to_string())?
        .check()
        .await
        .map_err(|e| e.to_string())?
        .ok_or("no update available")?;
    let restart_server_on_failure = app
        .state::<Arc<Supervisor>>()
        .snapshot()
        .await
        .local_server_enabled;
    Supervisor::stop(&app, false)
        .await
        .map_err(|e| e.to_string())?;
    let progress_app = app.clone();
    let finished_app = app.clone();
    let install_result = update
        .download_and_install(
            move |chunk, total| {
                let _ = progress_app.emit(
                    "desktop-update-progress",
                    serde_json::json!({ "chunk": chunk, "total": total }),
                );
            },
            move || {
                let _ = finished_app.emit(
                    "desktop-update-progress",
                    serde_json::json!({ "finished": true }),
                );
            },
        )
        .await;
    if let Err(error) = install_result {
        if restart_server_on_failure {
            if let Err(restart_error) = Supervisor::start(app.clone()).await {
                return Err(format!(
                    "update failed: {error}; the local Server also failed to restart: {restart_error}"
                ));
            }
            return Err(format!(
                "update failed: {error}; the local Server was restarted"
            ));
        }
        return Err(format!("update failed: {error}"));
    }
    app.restart();
}

#[tauri::command]
async fn quit_desktop(window: WebviewWindow, app: AppHandle) -> Result<(), String> {
    validate_control(&window)?;
    quit_inner(app).await;
    Ok(())
}

async fn quit_inner(app: AppHandle) {
    if let Some(handle) = app.state::<RuntimeState>().relay_handle.lock().await.take() {
        handle.shutdown().await;
    }
    let _ = Supervisor::stop(&app, false).await;
    app.exit(0);
}

#[cfg(test)]
mod tests {
    use super::local_browser_url;

    /// Production and development manifests stay product-separated, updater
    /// signing metadata is present only in production, and the Server is an
    /// explicit sidecar rather than an arbitrary shell permission.
    #[test]
    fn manifests_separate_products_and_updates() {
        let production: serde_json::Value =
            serde_json::from_str(include_str!("../tauri.conf.json")).unwrap();
        let development: serde_json::Value =
            serde_json::from_str(include_str!("../tauri.dev.conf.json")).unwrap();
        assert_eq!(production["identifier"], "app.panoptikon.desktop");
        assert_eq!(development["identifier"], "app.panoptikon.desktop.dev");
        assert_eq!(
            production["bundle"]["externalBin"][0],
            "binaries/panoptikon"
        );
        assert!(
            production["bundle"]["createUpdaterArtifacts"]
                .as_bool()
                .unwrap()
        );
        assert!(
            production["plugins"]["updater"]["pubkey"]
                .as_str()
                .is_some_and(|key| key.len() > 100)
        );
        assert!(
            development["plugins"]["updater"]["endpoints"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }

    /// The bundled control and external setup windows receive separate,
    /// explicitly enumerated application commands.
    #[test]
    fn desktop_capabilities_are_window_scoped_and_command_limited() {
        let capability: serde_json::Value =
            serde_json::from_str(include_str!("../capabilities/control.json")).unwrap();
        assert_eq!(capability["windows"], serde_json::json!(["control"]));
        assert_eq!(
            capability["permissions"],
            serde_json::json!(["core:default", "allow-control-commands"])
        );
        let control_permission: toml::Value =
            toml::from_str(include_str!("../permissions/control_commands.toml")).unwrap();
        let allowed_commands = control_permission["permission"][0]["commands"]["allow"]
            .as_array()
            .unwrap()
            .iter()
            .map(|command| command.as_str().unwrap())
            .collect::<std::collections::BTreeSet<_>>();
        let expected_commands = [
            "get_status",
            "get_startup_warnings",
            "open_action_command",
            "open_setup_command",
            "restart_server",
            "set_local_server_enabled",
            "set_start_at_login",
            "open_known_folder",
            "log_tail",
            "relay_status",
            "relay_pending",
            "relay_approve",
            "relay_reject",
            "relay_revoke",
            "relay_set_mappings",
            "set_relay_enabled",
            "check_for_updates",
            "install_update",
            "quit_desktop",
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(allowed_commands, expected_commands);
        let control_js = include_str!("../../dist/app.js");
        let invoked_commands = control_js
            .split("invoke('")
            .skip(1)
            .filter_map(|suffix| suffix.split('\'').next())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(
            invoked_commands.is_subset(&allowed_commands),
            "control frontend invokes commands absent from its capability: {:?}",
            invoked_commands
                .difference(&allowed_commands)
                .collect::<Vec<_>>()
        );

        let launch_html = include_str!("../../dist/launch.html");
        let launch_js = include_str!("../../dist/launch.js");
        assert!(launch_html.contains("spinner_text.svg"));
        assert!(!launch_js.contains("__TAURI__"));
        assert!(!launch_js.contains("invoke("));

        let setup: serde_json::Value =
            serde_json::from_str(include_str!("../capabilities/setup.json")).unwrap();
        assert_eq!(setup["windows"], serde_json::json!(["launch"]));
        assert_eq!(
            setup["remote"]["urls"],
            serde_json::json!(["http://localhost:*"])
        );
        assert_eq!(
            setup["permissions"],
            serde_json::json!([
                "core:default",
                "allow-choose-scan-folders",
                "allow-open-panoptikon-page"
            ])
        );
        assert_eq!(setup["local"], false);
        let picker_permission = include_str!("../permissions/choose_scan_folders.toml");
        assert!(picker_permission.contains("commands.allow = [\"choose_scan_folders\"]"));
        let browser_permission = include_str!("../permissions/open_panoptikon_page.toml");
        assert!(browser_permission.contains("commands.allow = [\"open_panoptikon_page\"]"));
    }

    #[test]
    fn browser_facing_desktop_urls_use_localhost() {
        assert_eq!(
            local_browser_url(16342, "/desktop/setup"),
            "http://localhost:16342/desktop/setup"
        );
    }
}
