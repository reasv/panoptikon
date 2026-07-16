use crate::{
    lifecycle::{LifecycleState, RestartBudget},
    paths::DesktopPaths,
    settings::SettingsDocument,
};
use anyhow::{Context as _, bail};
use serde::Serialize;
use std::{
    collections::VecDeque,
    ffi::OsString,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tauri::{AppHandle, Emitter as _, Manager as _};
use tauri_plugin_shell::{
    ShellExt as _,
    process::{CommandChild, CommandEvent},
};
use tokio::sync::{Mutex, RwLock};

pub struct RunningSidecar {
    pub generation: u64,
    pub child: CommandChild,
    pub started: Instant,
}

pub struct Supervisor {
    pub paths: DesktopPaths,
    pub settings: Mutex<SettingsDocument>,
    /// Serializes every in-process edit of the user-owned Server TOML.
    pub config_write: Mutex<()>,
    pub state: RwLock<LifecycleState>,
    server_config: &'static str,
    default_port: u16,
    child: Mutex<Option<RunningSidecar>>,
    restart_budget: Mutex<RestartBudget>,
    intentional_stop: AtomicBool,
    generation: AtomicU64,
    active_port: AtomicU16,
    log_tail: Mutex<VecDeque<String>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusSnapshot {
    pub state: LifecycleState,
    pub state_label: String,
    pub local_server_enabled: bool,
    pub start_at_login: bool,
    pub port: u16,
    pub server_root: String,
    pub config_dir: String,
    pub log_dir: String,
    pub sidecar_pid: Option<u32>,
    pub default_database_ready: Option<bool>,
}

impl Supervisor {
    pub fn server_config_path(&self) -> std::path::PathBuf {
        self.paths.server_root.join(self.server_config)
    }
    pub fn new(
        paths: DesktopPaths,
        settings: SettingsDocument,
        server_config: &'static str,
        default_port: u16,
    ) -> Self {
        let initial = if settings.typed.local_server.enabled {
            LifecycleState::Installing
        } else {
            LifecycleState::LocalServerDisabled
        };
        Self {
            paths,
            settings: Mutex::new(settings),
            config_write: Mutex::new(()),
            state: RwLock::new(initial),
            server_config,
            default_port,
            child: Mutex::new(None),
            restart_budget: Mutex::new(RestartBudget::default()),
            intentional_stop: AtomicBool::new(false),
            generation: AtomicU64::new(0),
            active_port: AtomicU16::new(default_port),
            log_tail: Mutex::new(VecDeque::with_capacity(500)),
        }
    }

    pub async fn snapshot(&self) -> StatusSnapshot {
        let settings = self.settings.lock().await;
        let state = self.state.read().await.clone();
        let pid = self
            .child
            .lock()
            .await
            .as_ref()
            .map(|child| child.child.pid());
        StatusSnapshot {
            state_label: state.label().to_owned(),
            state,
            local_server_enabled: settings.typed.local_server.enabled,
            start_at_login: settings.typed.startup.start_at_login,
            port: self.active_port.load(Ordering::Acquire),
            server_root: self.paths.server_root.display().to_string(),
            config_dir: self.paths.config_dir.display().to_string(),
            log_dir: self.paths.log_dir.display().to_string(),
            sidecar_pid: pid,
            default_database_ready: None,
        }
    }

    pub async fn set_state(&self, app: &AppHandle, state: LifecycleState) {
        *self.state.write().await = state.clone();
        crate::update_tray(app, &state).await;
        let _ = app.emit("desktop-state", &state);
        crate::refresh_launch_window(app).await;
    }

    pub async fn start(app: AppHandle) -> anyhow::Result<()> {
        let supervisor = app.state::<Arc<Supervisor>>().inner().clone();
        let enabled = supervisor.settings.lock().await.typed.local_server.enabled;
        if !enabled {
            supervisor
                .set_state(&app, LifecycleState::LocalServerDisabled)
                .await;
            return Ok(());
        }
        if supervisor.child.lock().await.is_some() {
            return Ok(());
        }
        {
            let runtime = app.state::<crate::RuntimeState>();
            runtime.automated_setup_seen.store(false, Ordering::Release);
            runtime
                .automated_setup_failed
                .store(false, Ordering::Release);
            runtime.setup_start_notified.store(false, Ordering::Release);
            runtime
                .setup_completion_notified
                .store(false, Ordering::Release);
            runtime.interactive_startup_seen.store(
                app.get_webview_window("launch").is_some(),
                Ordering::Release,
            );
            *runtime.startup_activity.lock().await = None;
            *runtime.setup_failure.lock().await = None;
        }
        supervisor
            .paths
            .materialize_server_root()
            .context("failed to materialize the Desktop Server root")?;
        let port = effective_port(
            &supervisor.paths,
            supervisor.server_config,
            supervisor.default_port,
        )?;
        supervisor.active_port.store(port, Ordering::Release);
        supervisor.intentional_stop.store(false, Ordering::Release);
        supervisor.set_state(&app, LifecycleState::Starting).await;
        let generation = supervisor.generation.fetch_add(1, Ordering::AcqRel) + 1;
        let args = sidecar_arguments(&supervisor.paths, supervisor.server_config);
        let mut command = app
            .shell()
            .sidecar("panoptikon")
            .context("bundled Panoptikon Server sidecar is missing")?
            .args(args);
        if let Some((bridge_url, bridge_token)) = crate::updates::bridge_environment(&app) {
            command = command
                .env("PANOPTIKON_DESKTOP_BRIDGE_URL", bridge_url)
                .env("PANOPTIKON_DESKTOP_BRIDGE_TOKEN", bridge_token);
        }
        let (mut events, child) = command
            .spawn()
            .context("failed to start bundled Panoptikon Server sidecar")?;
        let pid = child.pid();
        supervisor
            .record(format!("started Panoptikon Server sidecar pid={pid}"))
            .await;
        *supervisor.child.lock().await = Some(RunningSidecar {
            generation,
            child,
            started: Instant::now(),
        });

        let event_app = app.clone();
        tauri::async_runtime::spawn(async move {
            while let Some(event) = events.recv().await {
                match event {
                    CommandEvent::Stdout(bytes) => {
                        let line = clean_terminal_output(&String::from_utf8_lossy(&bytes));
                        if !line.is_empty() {
                            event_app
                                .state::<Arc<Supervisor>>()
                                .record(format!("server: {line}"))
                                .await;
                            crate::observe_server_progress(&event_app, &line).await;
                        }
                    }
                    CommandEvent::Stderr(bytes) => {
                        let line = clean_terminal_output(&String::from_utf8_lossy(&bytes));
                        if !line.is_empty() {
                            event_app
                                .state::<Arc<Supervisor>>()
                                .record(format!("server stderr: {line}"))
                                .await;
                            crate::observe_server_progress(&event_app, &line).await;
                        }
                    }
                    CommandEvent::Error(error) => {
                        event_app
                            .state::<Arc<Supervisor>>()
                            .record(format!("sidecar stream error: {error}"))
                            .await
                    }
                    CommandEvent::Terminated(status) => {
                        handle_exit(event_app.clone(), generation, status.code).await;
                        break;
                    }
                    _ => {}
                }
            }
        });
        let ready_app = app.clone();
        tauri::async_runtime::spawn(async move {
            if let Err(error) = wait_for_readiness(&ready_app, generation, port).await {
                let supervisor = ready_app.state::<Arc<Supervisor>>().inner().clone();
                if supervisor.generation.load(Ordering::Acquire) == generation {
                    if matches!(*supervisor.state.read().await, LifecycleState::Restarting) {
                        return;
                    }
                    supervisor
                        .set_state(&ready_app, LifecycleState::Failed(error.to_string()))
                        .await;
                    crate::notify_startup_result(&ready_app, false).await;
                    let pending = ready_app
                        .state::<crate::RuntimeState>()
                        .pending_open
                        .load(Ordering::Acquire);
                    if pending {
                        let _ = crate::show_launch_window(&ready_app, true);
                    }
                }
            } else {
                let supervisor = ready_app.state::<Arc<Supervisor>>().inner().clone();
                if supervisor.generation.load(Ordering::Acquire) == generation {
                    let setup_failure = ready_app
                        .state::<crate::RuntimeState>()
                        .setup_failure
                        .lock()
                        .await
                        .clone();
                    if let Some(error) = setup_failure {
                        supervisor
                            .set_state(&ready_app, LifecycleState::Degraded(error))
                            .await;
                        crate::notify_startup_result(&ready_app, false).await;
                    } else {
                        supervisor
                            .set_state(&ready_app, LifecycleState::Ready)
                            .await;
                        crate::notify_startup_result(&ready_app, true).await;
                        crate::open_pending_action(&ready_app).await;
                    }
                }
            }
        });
        Ok(())
    }

    pub async fn stop(app: &AppHandle, for_restart: bool) -> anyhow::Result<()> {
        let supervisor = app.state::<Arc<Supervisor>>().inner().clone();
        supervisor.intentional_stop.store(true, Ordering::Release);
        supervisor.set_state(app, LifecycleState::Stopping).await;
        let target = {
            let mut child = supervisor.child.lock().await;
            if let Some(running) = child.as_mut() {
                let generation = running.generation;
                let pid = running.child.pid();
                running
                    .child
                    .write(b"shutdown\n")
                    .context("failed to send graceful shutdown to Server")?;
                Some((generation, pid))
            } else {
                None
            }
        };
        if let Some((generation, pid)) = target {
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let still_running = supervisor
                    .child
                    .lock()
                    .await
                    .as_ref()
                    .is_some_and(|running| running.generation == generation);
                if !still_running {
                    break;
                }
                if Instant::now() >= deadline {
                    let child = supervisor.child.lock().await.take();
                    if let Some(running) = child {
                        running
                            .child
                            .kill()
                            .context("failed to kill unresponsive Server sidecar")?;
                    }
                    supervisor
                        .record(format!(
                            "killed Server sidecar pid={pid} after shutdown deadline"
                        ))
                        .await;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        if for_restart {
            supervisor.set_state(app, LifecycleState::Restarting).await;
        } else {
            supervisor.set_state(app, LifecycleState::Exited).await;
        }
        Ok(())
    }

    pub async fn restart(app: AppHandle) -> anyhow::Result<()> {
        Self::stop(&app, true).await?;
        Self::start(app).await
    }

    pub async fn tail(&self, lines: usize) -> Vec<String> {
        let tail = self.log_tail.lock().await;
        tail.iter()
            .skip(tail.len().saturating_sub(lines.min(500)))
            .cloned()
            .collect()
    }

    async fn record(&self, line: String) {
        tracing::info!(target: "panoptikon_desktop", "{line}");
        let mut tail = self.log_tail.lock().await;
        if tail.len() == 500 {
            tail.pop_front();
        }
        tail.push_back(redact(&line));
    }
}

async fn handle_exit(app: AppHandle, generation: u64, code: Option<i32>) {
    let supervisor = app.state::<Arc<Supervisor>>().inner().clone();
    let run_duration = {
        let mut child = supervisor.child.lock().await;
        match child.as_ref() {
            Some(running) if running.generation == generation => {}
            _ => return,
        }
        child.take().unwrap().started.elapsed()
    };
    supervisor
        .record(format!("Server sidecar exited code={code:?}"))
        .await;
    if supervisor.intentional_stop.load(Ordering::Acquire) {
        return;
    }
    let delay = supervisor
        .restart_budget
        .lock()
        .await
        .record_exit(Instant::now(), run_duration);
    match delay {
        Some(delay) => {
            supervisor.set_state(&app, LifecycleState::Restarting).await;
            tokio::time::sleep(delay).await;
            // Type-erase the restart edge through the application event loop;
            // this also serializes it with the single-instance/UI lifecycle.
            let _ = app.emit("desktop-internal-restart", ());
        }
        None => {
            supervisor
                .set_state(
                    &app,
                    LifecycleState::Failed(
                        "Panoptikon Server exited repeatedly; automatic restart stopped".into(),
                    ),
                )
                .await;
            crate::notify_startup_result(&app, false).await;
            if app
                .state::<crate::RuntimeState>()
                .pending_open
                .load(Ordering::Acquire)
            {
                let _ = crate::show_launch_window(&app, true);
            }
        }
    }
}

fn effective_port(paths: &DesktopPaths, server_config: &str, fallback: u16) -> anyhow::Result<u16> {
    let config = paths.server_root.join(server_config);
    crate::server_config::effective_local_port(&paths.server_root, &config, fallback).with_context(
        || {
            format!(
                "failed to resolve Desktop Server port from '{}'",
                config.display()
            )
        },
    )
}

pub fn sidecar_arguments(paths: &DesktopPaths, server_config: &str) -> Vec<OsString> {
    vec![
        OsString::from("--root"),
        paths.server_root.as_os_str().to_owned(),
        OsString::from("--config"),
        OsString::from(server_config),
        OsString::from("--disable-update-check"),
        OsString::from("--desktop-managed"),
    ]
}

async fn wait_for_readiness(app: &AppHandle, generation: u64, port: u16) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()?;
    let deadline = Instant::now() + Duration::from_secs(180);
    let gateway = format!("http://127.0.0.1:{port}/api/client-config");
    let ui = format!("http://127.0.0.1:{port}/search");
    let mut delay = Duration::from_millis(150);
    let mut gateway_seen = false;
    loop {
        if Instant::now() >= deadline {
            bail!("Server readiness timed out on port {port}");
        }
        let child_is_running = app
            .state::<Arc<Supervisor>>()
            .child
            .lock()
            .await
            .as_ref()
            .is_some_and(|running| running.generation == generation);
        if !child_is_running {
            bail!("Server sidecar exited before readiness");
        }
        let gateway_ok = match client.get(&gateway).send().await {
            Ok(response) if response.status().is_success() => response
                .json::<serde_json::Value>()
                .await
                .ok()
                .is_some_and(|value| client_config_is_desktop_managed(&value)),
            _ => false,
        };
        if gateway_ok {
            if !gateway_seen {
                gateway_seen = true;
                let supervisor = app.state::<Arc<Supervisor>>().inner().clone();
                if supervisor.generation.load(Ordering::Acquire) == generation
                    && !matches!(*supervisor.state.read().await, LifecycleState::Degraded(_))
                {
                    supervisor.set_state(app, LifecycleState::SettingUp).await;
                }
            }
            if let Ok(response) = client.get(&ui).send().await {
                if response.status().is_success()
                    && response
                        .headers()
                        .get(reqwest::header::CONTENT_TYPE)
                        .and_then(|v| v.to_str().ok())
                        .is_some_and(|value| value.contains("text/html"))
                {
                    let child_is_still_running = app
                        .state::<Arc<Supervisor>>()
                        .child
                        .lock()
                        .await
                        .as_ref()
                        .is_some_and(|running| running.generation == generation);
                    if child_is_still_running {
                        return Ok(());
                    }
                    bail!("Server sidecar exited during readiness checks");
                }
            }
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(2));
    }
}

fn client_config_is_desktop_managed(value: &serde_json::Value) -> bool {
    value.get("desktop_managed").and_then(|v| v.as_bool()) == Some(true)
}

fn clean_terminal_output(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == 0x1b {
            index += 1;
            match bytes.get(index).copied() {
                Some(b'[') => {
                    index += 1;
                    while index < bytes.len() {
                        let byte = bytes[index];
                        index += 1;
                        if (0x40..=0x7e).contains(&byte) {
                            break;
                        }
                    }
                }
                Some(b']') => {
                    index += 1;
                    while index < bytes.len() {
                        if bytes[index] == 0x07 {
                            index += 1;
                            break;
                        }
                        if bytes[index] == 0x1b && bytes.get(index + 1) == Some(&b'\\') {
                            index += 2;
                            break;
                        }
                        index += 1;
                    }
                }
                Some(_) => index += 1,
                None => {}
            }
        } else {
            let byte = bytes[index];
            index += 1;
            if byte >= 0x20 || matches!(byte, b'\t' | b'\n') {
                output.push(byte);
            }
        }
    }
    String::from_utf8_lossy(&output)
        .trim_end_matches(['\r', '\n'])
        .to_owned()
}

fn redact(line: &str) -> String {
    let lower = line.to_ascii_lowercase();
    if [
        "credential",
        "authorization",
        "api_key",
        "password",
        "policy_token_key",
        "desktop_bridge_token",
        "tauri_signing_private_key",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
    {
        "[redacted sensitive diagnostic line]".into()
    } else {
        line.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Sidecar arguments remain a vector, preserving spaces and non-ASCII
    /// characters without shell quoting or interpolation.
    #[test]
    fn sidecar_arguments_preserve_complex_root() {
        let paths = DesktopPaths::new(
            "C:/cfg".into(),
            "C:/Données utilisateur".into(),
            "C:/logs".into(),
        );
        let args = sidecar_arguments(&paths, "config/server/desktop-dev.toml");
        assert_eq!(args[1], paths.server_root.as_os_str());
        assert_eq!(args[3], OsString::from("config/server/desktop-dev.toml"));
        assert_eq!(args[5], OsString::from("--desktop-managed"));
    }

    /// Diagnostics redact complete lines containing known secret markers.
    #[test]
    fn diagnostic_redaction_covers_secrets() {
        assert_eq!(
            redact("Authorization: Bearer abc"),
            "[redacted sensitive diagnostic line]"
        );
        assert_eq!(
            redact("PANOPTIKON_DESKTOP_BRIDGE_TOKEN=abc"),
            "[redacted sensitive diagnostic line]"
        );
        assert_eq!(redact("gateway ready"), "gateway ready");
    }

    #[test]
    fn diagnostic_output_strips_terminal_sequences_and_line_endings() {
        assert_eq!(
            clean_terminal_output("\u{1b}[2m2026-07-14\u{1b}[0m \u{1b}[32mINFO\u{1b}[0m\r\n"),
            "2026-07-14 INFO"
        );
    }

    #[test]
    fn effective_port_comes_from_the_user_owned_server_config() {
        let temp = tempfile::tempdir().unwrap();
        let paths = DesktopPaths::new(
            temp.path().join("config"),
            temp.path().join("data"),
            temp.path().join("logs"),
        );
        paths.materialize_server_root().unwrap();
        let config = paths.server_root.join("config/server/desktop.toml");
        std::fs::write(&config, "[server]\nport = 7123\n").unwrap();
        assert_eq!(
            effective_port(&paths, "config/server/desktop.toml", 6342).unwrap(),
            7123
        );
    }

    #[test]
    fn effective_port_resolves_the_managed_dotenv_binding() {
        let temp = tempfile::tempdir().unwrap();
        let paths = DesktopPaths::new(
            temp.path().join("config"),
            temp.path().join("data"),
            temp.path().join("logs"),
        );
        paths.materialize_server_root().unwrap();
        let config = paths.server_root.join("config/server/desktop.toml");
        std::fs::write(&config, "[server]\nport = \"${PORT:-6342}\"\n").unwrap();
        std::fs::write(paths.server_root.join(".env"), "PORT=7124\n").unwrap();
        assert_eq!(
            effective_port(&paths, "config/server/desktop.toml", 6342).unwrap(),
            7124
        );
    }

    #[test]
    fn readiness_rejects_an_ordinary_server_client_config() {
        assert!(client_config_is_desktop_managed(
            &serde_json::json!({"desktop_managed": true})
        ));
        assert!(!client_config_is_desktop_managed(
            &serde_json::json!({"desktop_managed": false})
        ));
        assert!(!client_config_is_desktop_managed(&serde_json::json!({})));
    }
}
