mod lifecycle;
mod paths;
mod relay;
mod settings;
mod supervisor;

use crate::{
    lifecycle::{ActivationIntent, LifecycleState, activation_intent},
    paths::DesktopPaths,
    relay::{PathMapping, RelayAction, RelayHandle, RelayState},
    settings::{OnboardingState, SettingsDocument, sync_onboarding_marker},
    supervisor::{StatusSnapshot, Supervisor},
};
use serde::Serialize;
use std::{panic::AssertUnwindSafe, sync::Arc};
use tauri::{
    AppHandle, Emitter as _, Listener as _, Manager as _, WebviewUrl, WebviewWindow,
    WebviewWindowBuilder,
    menu::{CheckMenuItem, MenuBuilder, MenuItem},
    tray::TrayIconBuilder,
};
use tauri_plugin_autostart::ManagerExt as _;
use tauri_plugin_opener::OpenerExt as _;
use tauri_plugin_updater::UpdaterExt as _;
use tokio::sync::Mutex;
use uuid::Uuid;

struct RuntimeState {
    relay_handle: Mutex<Option<RelayHandle>>,
    tray: Mutex<Option<TrayUi>>,
    startup_warnings: Vec<String>,
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
            get_status, get_startup_warnings, open_action_command, restart_server, set_local_server_enabled,
            set_start_at_login, open_known_folder, log_tail, complete_onboarding,
            relay_status, relay_pending, relay_approve, relay_reject, relay_revoke,
            relay_set_mappings, set_relay_enabled, check_for_updates, install_update, quit_desktop
        ])
        .setup(|app| {
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
            let relay_config = match relay::load_config(&paths.relay_settings) {
                Ok(config) => config,
                Err(error) => {
                    startup_warnings.push(error.to_string());
                    relay::RelayConfig::desktop_default()
                }
            };
            let supervisor = Arc::new(Supervisor::new(paths.clone(), settings));
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
            app.manage(RuntimeState { relay_handle: Mutex::new(None), tray: Mutex::new(None), startup_warnings, _log_guard: log_guard });
            let restart_app = app.handle().clone();
            app.listen("desktop-internal-restart", move |_| {
                let restart_app = restart_app.clone();
                tauri::async_runtime::spawn(async move {
                    if let Err(error) = Supervisor::start(restart_app.clone()).await {
                        restart_app.state::<Arc<Supervisor>>().set_state(&restart_app, LifecycleState::Failed(error.to_string())).await;
                        let _ = show_control_window(&restart_app, true);
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
                    let _ = show_control_window(&start_app, true);
                }
            });
            let update_app = app.handle().clone();
            tauri::async_runtime::spawn(async move { automatic_update_check(update_app).await; });
            let intent = activation_intent(&std::env::args().collect::<Vec<_>>());
            if intent == ActivationIntent::Open { route_activation(app.handle().clone(), intent); }
            Ok(())
        });

    builder
        .run(tauri::generate_context!())
        .expect("error while running Panoptikon Desktop");
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
                    let _ = show_control_window(&app, true);
                }
            });
        }
        "local" => {
            let app = app.clone();
            tauri::async_runtime::spawn(async move {
                let enabled = !app
                    .state::<Arc<Supervisor>>()
                    .settings
                    .lock()
                    .await
                    .typed
                    .local_server
                    .enabled;
                let _ = set_local_enabled_inner(app, enabled, true).await;
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
        return show_control_window(app, true).map_err(|e| e.to_string());
    }
    let onboarding = {
        let mut settings = supervisor.settings.lock().await;
        sync_onboarding_marker(&supervisor.paths, &mut settings)
            .map_err(|error| error.to_string())?;
        settings.typed.onboarding.state
    };
    if matches!(
        onboarding,
        OnboardingState::Complete | OnboardingState::Skipped
    ) {
        app.opener()
            .open_url(
                format!("http://127.0.0.1:{}/search", snapshot.port),
                None::<&str>,
            )
            .map_err(|e| e.to_string())
    } else {
        show_setup_window(app, snapshot.port).map_err(|e| e.to_string())
    }
}

pub(crate) fn show_control_window(app: &AppHandle, focus: bool) -> tauri::Result<()> {
    let window = if let Some(window) = app.get_webview_window("control") {
        window
    } else {
        WebviewWindowBuilder::new(app, "control", WebviewUrl::App("index.html".into()))
            .title("Panoptikon Desktop")
            .inner_size(780.0, 680.0)
            .min_inner_size(560.0, 480.0)
            .build()?
    };
    window.show()?;
    if focus {
        window.unminimize()?;
        window.set_focus()?;
    }
    Ok(())
}

fn show_setup_window(app: &AppHandle, port: u16) -> tauri::Result<()> {
    let url = format!("http://127.0.0.1:{port}/desktop/setup")
        .parse()
        .map_err(|error| tauri::Error::InvalidUrl(error))?;
    let window = if let Some(window) = app.get_webview_window("setup") {
        window
    } else {
        WebviewWindowBuilder::new(app, "setup", WebviewUrl::External(url))
            .title("Set up Panoptikon")
            .inner_size(1000.0, 760.0)
            .build()?
    };
    window.show()?;
    window.set_focus()
}

fn validate_control(window: &WebviewWindow) -> Result<(), String> {
    if window.label() != "control" {
        Err("command is restricted to the bundled control window".into())
    } else {
        Ok(())
    }
}

#[tauri::command]
async fn get_status(window: WebviewWindow, app: AppHandle) -> Result<StatusSnapshot, String> {
    validate_control(&window)?;
    Ok(app.state::<Arc<Supervisor>>().snapshot().await)
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
    Supervisor::restart(app).await.map_err(|e| e.to_string())
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
        Supervisor::start(app).await.map_err(|e| e.to_string())
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
async fn complete_onboarding(
    window: WebviewWindow,
    app: AppHandle,
    skipped: bool,
) -> Result<(), String> {
    validate_control(&window)?;
    let supervisor = app.state::<Arc<Supervisor>>().inner().clone();
    let mut settings = supervisor.settings.lock().await;
    settings.typed.onboarding.state = if skipped {
        OnboardingState::Skipped
    } else {
        OnboardingState::Complete
    };
    settings.save().map_err(|e| e.to_string())
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
    Supervisor::stop(&app, false)
        .await
        .map_err(|e| e.to_string())?;
    let progress_app = app.clone();
    let finished_app = app.clone();
    update
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
        .await
        .map_err(|e| e.to_string())?;
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

    /// Only the bundled control window receives Tauri core capabilities; the
    /// external onboarding window receives none and no plugin grants exist.
    #[test]
    fn control_capability_is_window_scoped_and_core_only() {
        let capability: serde_json::Value =
            serde_json::from_str(include_str!("../capabilities/control.json")).unwrap();
        assert_eq!(capability["windows"], serde_json::json!(["control"]));
        assert_eq!(
            capability["permissions"],
            serde_json::json!(["core:default"])
        );
    }
}
