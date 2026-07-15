use crate::{
    settings::{UpdateSettings, atomic_write},
    supervisor::Supervisor,
};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use base64::Engine as _;
use rand::RngCore as _;
use semver::Version;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tauri::{
    AppHandle, Emitter as _, Manager as _, WebviewUrl, WebviewWindow, WebviewWindowBuilder,
};
use tauri_plugin_opener::OpenerExt as _;
use tauri_plugin_updater::{Update, UpdaterExt as _};
use tokio::sync::Mutex;

const CHANGELOG_URL: &str =
    "https://github.com/reasv/panoptikon/releases/latest/download/changelog.json";
const AUTOMATIC_WINDOW_SECS: i64 = 8 * 60 * 60;
const AUTOMATIC_MAX_ATTEMPTS: usize = 8;
const RUNTIME_INTERVAL_SECS: i64 = 8 * 60 * 60;
const FRESH_SECS: i64 = 10 * 60;
const MANUAL_WINDOW_SECS: i64 = 60;
const MANUAL_MAX_ATTEMPTS: usize = 10;
const MANUAL_MIN_GAP_SECS: i64 = 2;
const RIBBON_SNOOZE_SECS: i64 = 24 * 60 * 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckReason {
    Startup,
    Runtime,
    Manual,
    Freshness,
}

impl CheckReason {
    fn automatic(self) -> bool {
        matches!(self, Self::Startup | Self::Runtime)
    }

    fn label(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Runtime => "runtime",
            Self::Manual => "manual",
            Self::Freshness => "freshness",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReleaseNote {
    pub version: String,
    pub tag: String,
    pub date: String,
    pub notes_markdown: String,
    pub release_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChangelogFeed {
    schema_version: u32,
    releases: Vec<ReleaseNote>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UpdateView {
    pub current_version: String,
    pub available: bool,
    pub target_version: Option<String>,
    pub published_at: Option<String>,
    pub releases: Vec<ReleaseNote>,
    pub release_url: Option<String>,
    pub last_attempt_unix: Option<i64>,
    pub last_success_unix: Option<i64>,
    pub last_error: Option<String>,
    pub last_error_unix: Option<i64>,
    pub checking: bool,
    pub fresh: bool,
    pub can_install: bool,
    pub check_automatically: bool,
    pub ribbon_visible: bool,
    pub ribbon_snoozed_until_unix: Option<i64>,
    pub ribbon_dismissed_version: Option<String>,
    pub reminder_at_unix: Option<i64>,
    pub updates_disabled: bool,
    pub active_work: bool,
}

pub struct UpdateCoordinator {
    check_gate: Mutex<()>,
    pending: Mutex<Option<Update>>,
    manual_attempts: Mutex<VecDeque<i64>>,
    checking: AtomicBool,
    installing: AtomicBool,
}

pub struct UpdateBridge {
    url: String,
    token: String,
}

#[derive(Clone)]
struct BridgeServerState {
    app: AppHandle,
    token: String,
}

#[derive(Debug, Deserialize)]
struct BridgeDismissRequest {
    version: String,
}

#[derive(Serialize)]
struct BridgeUpdateStatus {
    available: bool,
    target_version: Option<String>,
    ribbon_visible: bool,
}

pub fn initialize_bridge(app: &mut tauri::App) -> tauri::Result<()> {
    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
    listener.set_nonblocking(true)?;
    let address = listener.local_addr()?;
    let mut token_bytes = [0_u8; 32];
    rand::rng().fill_bytes(&mut token_bytes);
    let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(token_bytes);
    app.manage(Arc::new(UpdateBridge {
        url: format!("http://{address}"),
        token: token.clone(),
    }));
    let bridge_app = app.handle().clone();
    tauri::async_runtime::spawn(async move {
        let listener = match tokio::net::TcpListener::from_std(listener) {
            Ok(listener) => listener,
            Err(error) => {
                tracing::error!(%error, "failed to initialize Desktop update bridge listener");
                return;
            }
        };
        let state = BridgeServerState {
            app: bridge_app,
            token,
        };
        let router = Router::new()
            .route("/status", get(bridge_status))
            .route("/open", post(bridge_open))
            .route("/snooze", post(bridge_snooze))
            .route("/dismiss", post(bridge_dismiss))
            .with_state(state);
        if let Err(error) = axum::serve(listener, router).await {
            tracing::error!(%error, "Desktop update bridge stopped");
        }
    });
    Ok(())
}

pub fn bridge_environment(app: &AppHandle) -> Option<(String, String)> {
    let bridge = app.try_state::<Arc<UpdateBridge>>()?;
    Some((bridge.url.clone(), bridge.token.clone()))
}

fn bridge_authorized(headers: &HeaderMap, state: &BridgeServerState) -> bool {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|value| value.as_bytes() == state.token.as_bytes())
}

async fn bridge_status(
    State(state): State<BridgeServerState>,
    headers: HeaderMap,
) -> Result<Json<BridgeUpdateStatus>, StatusCode> {
    if !bridge_authorized(&headers, &state) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let coordinator = state.app.state::<Arc<UpdateCoordinator>>();
    let view = coordinator.view(&state.app).await;
    Ok(Json(BridgeUpdateStatus {
        available: view.available,
        target_version: view.target_version,
        ribbon_visible: view.ribbon_visible,
    }))
}

async fn bridge_open(
    State(state): State<BridgeServerState>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    if !bridge_authorized(&headers, &state) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    show_update_window(&state.app, true).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn bridge_snooze(
    State(state): State<BridgeServerState>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    if !bridge_authorized(&headers, &state) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    set_ribbon_snooze(&state.app)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn bridge_dismiss(
    State(state): State<BridgeServerState>,
    headers: HeaderMap,
    Json(request): Json<BridgeDismissRequest>,
) -> Result<StatusCode, StatusCode> {
    if !bridge_authorized(&headers, &state) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    set_ribbon_dismissal(&state.app, request.version)
        .await
        .map_err(|_| StatusCode::CONFLICT)?;
    Ok(StatusCode::NO_CONTENT)
}

impl UpdateCoordinator {
    pub fn new() -> Self {
        Self {
            check_gate: Mutex::new(()),
            pending: Mutex::new(None),
            manual_attempts: Mutex::new(VecDeque::new()),
            checking: AtomicBool::new(false),
            installing: AtomicBool::new(false),
        }
    }

    pub async fn normalize_installed_version(&self, app: &AppHandle) -> Result<(), String> {
        let current = app.package_info().version.to_string();
        let supervisor = app.state::<Arc<Supervisor>>();
        let mut document = supervisor.settings.lock().await;
        let obsolete = document
            .typed
            .updates
            .latest_version
            .as_deref()
            .is_some_and(|latest| !version_is_newer(latest, &current));
        if obsolete {
            clear_availability(&mut document.typed.updates);
            document.save().map_err(|error| error.to_string())?;
            let _ = std::fs::remove_file(&supervisor.paths.update_changelog);
        }
        Ok(())
    }

    pub async fn view(&self, app: &AppHandle) -> UpdateView {
        let (settings, cache_path, port, local_enabled) = {
            let supervisor = app.state::<Arc<Supervisor>>();
            let settings = supervisor.settings.lock().await.typed.updates.clone();
            let snapshot = supervisor.snapshot().await;
            (
                settings,
                supervisor.paths.update_changelog.clone(),
                snapshot.port,
                snapshot.local_server_enabled,
            )
        };
        let current_version = app.package_info().version.to_string();
        let now = unix_timestamp();
        let available = settings
            .latest_version
            .as_deref()
            .is_some_and(|latest| version_is_newer(latest, &current_version));
        let releases = if available {
            read_releases(
                &cache_path,
                &current_version,
                settings.latest_version.as_deref(),
            )
            .unwrap_or_else(|| fallback_release(&settings))
        } else {
            Vec::new()
        };
        let fresh = settings
            .last_checked_unix
            .is_some_and(|last| timestamp_age(now, last).is_some_and(|age| age <= FRESH_SECS));
        let ribbon_visible = available
            && settings.ribbon_dismissed_version.as_deref() != settings.latest_version.as_deref()
            && settings
                .ribbon_snoozed_until_unix
                .is_none_or(|until| until <= now);
        UpdateView {
            current_version,
            available,
            target_version: settings.latest_version,
            published_at: settings.latest_published_at,
            releases,
            release_url: settings.latest_release_url,
            last_attempt_unix: settings.last_attempt_unix,
            last_success_unix: settings.last_checked_unix,
            last_error: settings.last_error,
            last_error_unix: settings.last_error_unix,
            checking: self.checking.load(Ordering::Acquire),
            fresh,
            can_install: available && fresh && !self.installing.load(Ordering::Acquire),
            check_automatically: settings.check_automatically,
            ribbon_visible,
            ribbon_snoozed_until_unix: settings.ribbon_snoozed_until_unix,
            ribbon_dismissed_version: settings.ribbon_dismissed_version,
            reminder_at_unix: settings.reminder_at_unix,
            updates_disabled: cfg!(debug_assertions),
            active_work: local_enabled && sidecar_has_active_work(port).await,
        }
    }

    pub async fn check(&self, app: &AppHandle, reason: CheckReason) -> Result<UpdateView, String> {
        if cfg!(debug_assertions) {
            return Ok(self.view(app).await);
        }

        let guard = match self.check_gate.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                let guard = self.check_gate.lock().await;
                drop(guard);
                return Ok(self.view(app).await);
            }
        };
        let now = unix_timestamp();
        if !self.allow_attempt(app, reason, now).await? {
            drop(guard);
            return Ok(self.view(app).await);
        }

        self.checking.store(true, Ordering::Release);
        if let Err(error) = self.record_attempt(app, reason, now).await {
            self.checking.store(false, Ordering::Release);
            drop(guard);
            emit_state(app, self).await;
            return Err(error);
        }
        emit_state(app, self).await;
        tracing::info!(source = reason.label(), "Desktop update check started");

        let result = match app.updater() {
            Ok(updater) => updater.check().await,
            Err(error) => Err(error),
        };
        let outcome = match result {
            Ok(update) => self.record_success(app, reason, update, now).await,
            Err(error) => {
                tracing::warn!(source = reason.label(), %error, "Desktop update check failed");
                self.record_failure(app, now).await?;
                Err(
                    "Unable to reach the update service. Check your connection and try again."
                        .into(),
                )
            }
        };
        self.checking.store(false, Ordering::Release);
        drop(guard);
        emit_state(app, self).await;
        outcome?;
        Ok(self.view(app).await)
    }

    async fn allow_attempt(
        &self,
        app: &AppHandle,
        reason: CheckReason,
        now: i64,
    ) -> Result<bool, String> {
        let supervisor = app.state::<Arc<Supervisor>>();
        let settings = supervisor.settings.lock().await;
        if reason.automatic() {
            if !settings.typed.updates.check_automatically {
                return Ok(false);
            }
            return Ok(automatic_attempts_in_window(
                &settings.typed.updates.automatic_attempts_unix,
                now,
            ) < AUTOMATIC_MAX_ATTEMPTS);
        }
        drop(settings);
        if reason == CheckReason::Freshness {
            return Ok(true);
        }
        let mut attempts = self.manual_attempts.lock().await;
        attempts.retain(|attempt| valid_age(now, *attempt, MANUAL_WINDOW_SECS));
        if attempts.len() >= MANUAL_MAX_ATTEMPTS {
            return Err("Too many update checks. Please wait a minute and try again.".into());
        }
        if attempts
            .back()
            .is_some_and(|attempt| valid_age(now, *attempt, MANUAL_MIN_GAP_SECS))
        {
            return Err("Please wait a moment before checking again.".into());
        }
        attempts.push_back(now);
        Ok(true)
    }

    async fn record_attempt(
        &self,
        app: &AppHandle,
        reason: CheckReason,
        now: i64,
    ) -> Result<(), String> {
        let supervisor = app.state::<Arc<Supervisor>>();
        let mut document = supervisor.settings.lock().await;
        document.typed.updates.last_attempt_unix = Some(now);
        if reason.automatic() {
            document
                .typed
                .updates
                .automatic_attempts_unix
                .retain(|attempt| valid_age(now, *attempt, AUTOMATIC_WINDOW_SECS));
            document.typed.updates.automatic_attempts_unix.push(now);
        }
        document.save().map_err(|error| error.to_string())
    }

    async fn record_success(
        &self,
        app: &AppHandle,
        reason: CheckReason,
        update: Option<Update>,
        now: i64,
    ) -> Result<(), String> {
        let mut notify_version = None;
        let supervisor = app.state::<Arc<Supervisor>>();
        let cache_path = supervisor.paths.update_changelog.clone();
        let current = app.package_info().version.to_string();
        match update {
            Some(update) => {
                let previous = {
                    let document = supervisor.settings.lock().await;
                    document.typed.updates.latest_version.clone()
                };
                let newly_discovered = previous.as_deref() != Some(update.version.as_str());
                let releases = fetch_changelog(&current, &update).await;
                if let Ok(releases) = &releases {
                    if let Ok(bytes) = serde_json::to_vec_pretty(releases)
                        && let Err(error) = atomic_write(&cache_path, &bytes)
                    {
                        tracing::warn!(%error, "failed to cache Desktop changelog");
                    }
                } else if let Err(error) = &releases {
                    tracing::warn!(%error, "failed to fetch structured Desktop changelog");
                }
                let mut document = supervisor.settings.lock().await;
                let settings = &mut document.typed.updates;
                record_success_fields(settings, now);
                settings.latest_version = Some(update.version.clone());
                settings.latest_published_at = update.date.map(|date| date.to_string());
                settings.latest_notes_markdown = update.body.clone();
                settings.latest_release_url = Some(format!(
                    "https://github.com/reasv/panoptikon/releases/tag/v{}",
                    update.version
                ));
                if newly_discovered {
                    settings.discovered_unix = Some(now);
                    settings.ribbon_snoozed_until_unix = None;
                    settings.ribbon_dismissed_version = None;
                    settings.reminder_version = None;
                    settings.reminder_at_unix = None;
                    if reason.automatic()
                        && settings.native_notified_version.as_deref()
                            != Some(update.version.as_str())
                    {
                        notify_version = Some(update.version.clone());
                    }
                }
                document.save().map_err(|error| error.to_string())?;
                *self.pending.lock().await = Some(update);
                tracing::info!(target = ?document.typed.updates.latest_version, newly_discovered, "Desktop update available");
            }
            None => {
                let mut document = supervisor.settings.lock().await;
                record_success_fields(&mut document.typed.updates, now);
                clear_availability(&mut document.typed.updates);
                document.save().map_err(|error| error.to_string())?;
                *self.pending.lock().await = None;
                let _ = std::fs::remove_file(cache_path);
                tracing::info!(current_version = %current, "Panoptikon Desktop is up to date");
            }
        }
        crate::update_update_tray(app).await;
        if let Some(version) = notify_version {
            let update_visible = app
                .get_webview_window("update")
                .and_then(|window| window.is_visible().ok())
                .unwrap_or(false);
            if !update_visible {
                notify_update(app.clone(), version).await;
            }
        }
        Ok(())
    }

    async fn record_failure(&self, app: &AppHandle, now: i64) -> Result<(), String> {
        let supervisor = app.state::<Arc<Supervisor>>();
        let mut document = supervisor.settings.lock().await;
        let settings = &mut document.typed.updates;
        settings.last_error = Some("Unable to reach the update service".into());
        settings.last_error_unix = Some(now);
        settings.consecutive_failures = settings.consecutive_failures.saturating_add(1);
        document.save().map_err(|error| error.to_string())
    }

    pub async fn install(
        &self,
        app: &AppHandle,
        expected_version: &str,
        confirm_active_work: bool,
    ) -> Result<(), String> {
        if cfg!(debug_assertions) {
            return Err("updates are disabled in development builds".into());
        }
        if self.installing.swap(true, Ordering::AcqRel) {
            return Err("an update installation is already in progress".into());
        }
        let result = self
            .install_inner(app, expected_version, confirm_active_work)
            .await;
        self.installing.store(false, Ordering::Release);
        emit_state(app, self).await;
        result
    }

    async fn install_inner(
        &self,
        app: &AppHandle,
        expected_version: &str,
        confirm_active_work: bool,
    ) -> Result<(), String> {
        let fresh = self.view(app).await.fresh;
        if !fresh {
            self.check(app, CheckReason::Freshness).await?;
        }
        let view = self.view(app).await;
        if view.target_version.as_deref() != Some(expected_version) {
            return Err("TARGET_CHANGED".into());
        }
        if !view.can_install {
            return Err("A fresh update check is required before installation.".into());
        }
        if view.active_work && !confirm_active_work {
            return Err("ACTIVE_WORK".into());
        }
        let update = self
            .pending
            .lock()
            .await
            .clone()
            .ok_or("The update must be checked again before installation.")?;
        if update.version != expected_version {
            return Err("TARGET_CHANGED".into());
        }

        emit_progress(app, "downloading", 0, None, false);
        let progress_app = app.clone();
        let finish_app = app.clone();
        let bytes = update
            .download(
                move |chunk, total| {
                    emit_progress(&progress_app, "downloading", chunk, total, false)
                },
                move || emit_progress(&finish_app, "verifying", 0, None, true),
            )
            .await
            .map_err(|error| {
                format!("Update download or signature verification failed: {error}")
            })?;

        let restart_server_on_failure = app
            .state::<Arc<Supervisor>>()
            .snapshot()
            .await
            .local_server_enabled;
        emit_progress(app, "stopping", 0, None, true);
        Supervisor::stop(app, false)
            .await
            .map_err(|error| format!("Panoptikon could not stop for the update: {error}"))?;
        emit_progress(app, "installing", 0, None, true);
        if let Err(error) = update.install(&bytes) {
            if restart_server_on_failure {
                if let Err(restart_error) = Supervisor::start(app.clone()).await {
                    return Err(format!(
                        "Update installation failed: {error}; Panoptikon also failed to restart: {restart_error}"
                    ));
                }
                return Err(format!(
                    "Update installation failed: {error}; Panoptikon was restarted"
                ));
            }
            return Err(format!("Update installation failed: {error}"));
        }
        emit_progress(app, "restarting", 0, None, true);
        #[cfg(not(windows))]
        app.restart();
        Ok(())
    }
}

fn record_success_fields(settings: &mut UpdateSettings, now: i64) {
    settings.last_checked_unix = Some(now);
    settings.last_error = None;
    settings.last_error_unix = None;
    settings.consecutive_failures = 0;
}

fn clear_availability(settings: &mut UpdateSettings) {
    settings.latest_version = None;
    settings.latest_published_at = None;
    settings.latest_notes_markdown = None;
    settings.latest_release_url = None;
    settings.discovered_unix = None;
    settings.ribbon_snoozed_until_unix = None;
    settings.ribbon_dismissed_version = None;
    settings.reminder_version = None;
    settings.reminder_at_unix = None;
}

pub fn start(app: AppHandle) {
    tauri::async_runtime::spawn(async move {
        let coordinator = app.state::<Arc<UpdateCoordinator>>().inner().clone();
        let runtime_jitter = rand::random_range(0..=15 * 60);
        if let Err(error) = coordinator.check(&app, CheckReason::Startup).await {
            tracing::debug!(%error, "startup update check did not complete");
        }
        loop {
            deliver_due_reminder(&app).await;
            if runtime_check_due(&app, runtime_jitter).await
                && let Err(error) = coordinator.check(&app, CheckReason::Runtime).await
            {
                tracing::debug!(%error, "runtime update check did not complete");
            }
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
    });
}

async fn runtime_check_due(app: &AppHandle, jitter_secs: i64) -> bool {
    let now = unix_timestamp();
    let supervisor = app.state::<Arc<Supervisor>>();
    let settings = supervisor.settings.lock().await.typed.updates.clone();
    if !settings.check_automatically {
        return false;
    }
    let since_attempt = settings
        .last_attempt_unix
        .and_then(|last| timestamp_age(now, last));
    if settings.consecutive_failures > 0 {
        let delay = match settings.consecutive_failures {
            1 => 15 * 60,
            2 => 60 * 60,
            _ => 4 * 60 * 60,
        };
        return since_attempt.is_none_or(|age| age >= delay);
    }
    settings
        .last_checked_unix
        .and_then(|last| timestamp_age(now, last))
        .is_none_or(|age| age >= RUNTIME_INTERVAL_SECS + jitter_secs)
}

async fn emit_state(app: &AppHandle, coordinator: &UpdateCoordinator) {
    crate::update_update_tray(app).await;
    let _ = app.emit("desktop-update-state", coordinator.view(app).await);
}

fn emit_progress(app: &AppHandle, stage: &str, chunk: usize, total: Option<u64>, finished: bool) {
    let _ = app.emit(
        "desktop-update-progress",
        serde_json::json!({
            "stage": stage,
            "chunk": chunk,
            "total": total,
            "finished": finished,
        }),
    );
}

async fn fetch_changelog(current: &str, update: &Update) -> Result<Vec<ReleaseNote>, String> {
    let response = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
        .map_err(|error| error.to_string())?
        .get(CHANGELOG_URL)
        .send()
        .await
        .map_err(|error| error.to_string())?
        .error_for_status()
        .map_err(|error| error.to_string())?;
    let feed: ChangelogFeed = response.json().await.map_err(|error| error.to_string())?;
    if feed.schema_version != 1 {
        return Err(format!(
            "unsupported changelog schema {}",
            feed.schema_version
        ));
    }
    let releases = filter_releases(feed.releases, current, &update.version);
    if releases.is_empty() {
        return Err("structured changelog has no notes for this update".into());
    }
    Ok(releases)
}

fn read_releases(
    path: &std::path::Path,
    current: &str,
    target: Option<&str>,
) -> Option<Vec<ReleaseNote>> {
    let target = target?;
    let bytes = std::fs::read(path).ok()?;
    let releases: Vec<ReleaseNote> = serde_json::from_slice(&bytes).ok()?;
    let filtered = filter_releases(releases, current, target);
    (!filtered.is_empty()).then_some(filtered)
}

fn fallback_release(settings: &UpdateSettings) -> Vec<ReleaseNote> {
    let Some(version) = settings.latest_version.clone() else {
        return Vec::new();
    };
    vec![ReleaseNote {
        tag: format!("v{version}"),
        version,
        date: settings.latest_published_at.clone().unwrap_or_default(),
        notes_markdown: settings
            .latest_notes_markdown
            .clone()
            .unwrap_or_else(|| "Release notes are temporarily unavailable.".into()),
        release_url: settings.latest_release_url.clone(),
    }]
}

fn filter_releases(
    mut releases: Vec<ReleaseNote>,
    current: &str,
    target: &str,
) -> Vec<ReleaseNote> {
    let (Ok(current), Ok(target)) = (Version::parse(current), Version::parse(target)) else {
        return Vec::new();
    };
    releases.retain(|release| {
        Version::parse(&release.version).is_ok_and(|version| version > current && version <= target)
    });
    releases.sort_by(|left, right| {
        Version::parse(&right.version)
            .ok()
            .cmp(&Version::parse(&left.version).ok())
    });
    releases
}

async fn sidecar_has_active_work(port: u16) -> bool {
    let url = format!("http://127.0.0.1:{port}/api/jobs/queue");
    let Ok(client) = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(700))
        .build()
    else {
        return false;
    };
    let Ok(response) = client.get(url).send().await else {
        return false;
    };
    let Ok(value) = response.json::<serde_json::Value>().await else {
        return false;
    };
    value
        .get("queue")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|queue| !queue.is_empty())
}

async fn notify_update(app: AppHandle, version: String) {
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let notification_app = app.clone();
    let title = format!("Panoptikon Desktop {version} is available");
    tauri::async_runtime::spawn_blocking(move || {
        let mut notification = notify_rust::Notification::new();
        notification
            .appname(&notification_app.package_info().name)
            .app_id(&notification_app.config().identifier)
            .summary(&title)
            .body("Review what is new and install when you are ready.")
            .auto_icon()
            .action("update", "View update");
        match notification.show() {
            Ok(handle) => {
                let _ = accepted_tx.send(true);
                let clicked = Arc::new(AtomicBool::new(false));
                let response_clicked = clicked.clone();
                if let Err(error) =
                    handle.wait_for_response(move |response: &notify_rust::NotificationResponse| {
                        response_clicked.store(
                            matches!(
                                response,
                                notify_rust::NotificationResponse::Default
                                    | notify_rust::NotificationResponse::Action(_)
                            ),
                            Ordering::Release,
                        );
                    })
                {
                    tracing::warn!(%error, "failed to wait for update notification response");
                }
                if clicked.load(Ordering::Acquire) {
                    let _ = show_update_window(&notification_app, true);
                }
            }
            Err(error) => {
                let _ = accepted_tx.send(false);
                tracing::warn!(%error, "failed to show update notification");
            }
        }
    });
    if accepted_rx.await.unwrap_or(false) {
        let supervisor = app.state::<Arc<Supervisor>>();
        let mut document = supervisor.settings.lock().await;
        document.typed.updates.native_notified_version = Some(version);
        if let Err(error) = document.save() {
            tracing::warn!(%error, "failed to persist update notification state");
        }
    }
}

async fn deliver_due_reminder(app: &AppHandle) {
    let now = unix_timestamp();
    let version = {
        let supervisor = app.state::<Arc<Supervisor>>();
        let mut document = supervisor.settings.lock().await;
        let settings = &mut document.typed.updates;
        let due = settings.reminder_at_unix.is_some_and(|at| at <= now)
            && settings.reminder_version == settings.latest_version;
        if !due {
            return;
        }
        let version = settings.reminder_version.take();
        settings.reminder_at_unix = None;
        if let Err(error) = document.save() {
            tracing::warn!(%error, "failed to clear delivered update reminder");
            return;
        }
        version
    };
    if let Some(version) = version {
        notify_update(app.clone(), version).await;
    }
}

pub fn show_update_window(app: &AppHandle, focus: bool) -> tauri::Result<()> {
    let window = if let Some(window) = app.get_webview_window("update") {
        window
    } else {
        let window =
            WebviewWindowBuilder::new(app, "update", WebviewUrl::App("update.html".into()))
                .title("Update Panoptikon")
                .inner_size(900.0, 760.0)
                .min_inner_size(620.0, 520.0)
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

#[tauri::command]
pub async fn get_update_state(window: WebviewWindow, app: AppHandle) -> Result<UpdateView, String> {
    validate_update_client(&window)?;
    Ok(app.state::<Arc<UpdateCoordinator>>().view(&app).await)
}

#[tauri::command]
pub async fn check_for_updates(
    window: WebviewWindow,
    app: AppHandle,
) -> Result<UpdateView, String> {
    validate_update_client(&window)?;
    let reason = if window.label() == "control" {
        CheckReason::Manual
    } else {
        CheckReason::Freshness
    };
    app.state::<Arc<UpdateCoordinator>>()
        .check(&app, reason)
        .await
}

#[tauri::command]
pub async fn open_update_window(window: WebviewWindow, app: AppHandle) -> Result<(), String> {
    if window.label() != "control" && window.label() != "update" {
        return Err("command is restricted to bundled Desktop windows".into());
    }
    show_update_window(&app, true).map_err(|error| error.to_string())
}

#[tauri::command]
pub fn close_update_window(window: WebviewWindow) -> Result<(), String> {
    validate_update_window(&window)?;
    window.hide().map_err(|error| error.to_string())
}

#[tauri::command]
pub fn open_update_link(window: WebviewWindow, app: AppHandle, url: String) -> Result<(), String> {
    validate_update_window(&window)?;
    let parsed = url::Url::parse(&url).map_err(|_| "invalid release-note link")?;
    if parsed.scheme() != "https" {
        return Err("release-note links must use HTTPS".into());
    }
    app.opener()
        .open_url(parsed.as_str(), None::<&str>)
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn set_automatic_update_checks(
    window: WebviewWindow,
    app: AppHandle,
    enabled: bool,
) -> Result<UpdateView, String> {
    validate_update_client(&window)?;
    let supervisor = app.state::<Arc<Supervisor>>();
    let mut document = supervisor.settings.lock().await;
    document.typed.updates.check_automatically = enabled;
    document.save().map_err(|error| error.to_string())?;
    drop(document);
    Ok(app.state::<Arc<UpdateCoordinator>>().view(&app).await)
}

#[tauri::command]
pub async fn schedule_update_reminder(
    window: WebviewWindow,
    app: AppHandle,
    preset: String,
) -> Result<(), String> {
    validate_update_window(&window)?;
    let delay = match preset.as_str() {
        "tomorrow" => 24 * 60 * 60,
        "three-days" => 3 * 24 * 60 * 60,
        "next-week" => 7 * 24 * 60 * 60,
        _ => return Err("unknown reminder preset".into()),
    };
    let supervisor = app.state::<Arc<Supervisor>>();
    let mut document = supervisor.settings.lock().await;
    let version = document
        .typed
        .updates
        .latest_version
        .clone()
        .ok_or("no update is available")?;
    document.typed.updates.reminder_version = Some(version);
    document.typed.updates.reminder_at_unix = Some(unix_timestamp() + delay);
    document.save().map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn snooze_update_ribbon(window: WebviewWindow, app: AppHandle) -> Result<(), String> {
    validate_update_client(&window)?;
    set_ribbon_snooze(&app).await
}

async fn set_ribbon_snooze(app: &AppHandle) -> Result<(), String> {
    let supervisor = app.state::<Arc<Supervisor>>();
    let mut document = supervisor.settings.lock().await;
    document.typed.updates.ribbon_snoozed_until_unix = Some(unix_timestamp() + RIBBON_SNOOZE_SECS);
    document.save().map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn dismiss_update_ribbon(
    window: WebviewWindow,
    app: AppHandle,
    version: String,
) -> Result<(), String> {
    validate_update_client(&window)?;
    set_ribbon_dismissal(&app, version).await
}

async fn set_ribbon_dismissal(app: &AppHandle, version: String) -> Result<(), String> {
    let supervisor = app.state::<Arc<Supervisor>>();
    let mut document = supervisor.settings.lock().await;
    if document.typed.updates.latest_version.as_deref() != Some(version.as_str()) {
        return Err("the available update changed; refresh and try again".into());
    }
    document.typed.updates.ribbon_dismissed_version = Some(version);
    document.save().map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn install_update(
    window: WebviewWindow,
    app: AppHandle,
    expected_version: String,
    confirm_active_work: bool,
) -> Result<(), String> {
    validate_update_window(&window)?;
    app.state::<Arc<UpdateCoordinator>>()
        .install(&app, &expected_version, confirm_active_work)
        .await
}

fn validate_update_window(window: &WebviewWindow) -> Result<(), String> {
    if window.label() == "update" {
        Ok(())
    } else {
        Err("command is restricted to the bundled update window".into())
    }
}

fn validate_update_client(window: &WebviewWindow) -> Result<(), String> {
    if matches!(window.label(), "control" | "update") {
        Ok(())
    } else {
        Err("command is restricted to bundled Desktop windows".into())
    }
}

fn version_is_newer(remote: &str, current: &str) -> bool {
    match (Version::parse(remote), Version::parse(current)) {
        (Ok(remote), Ok(current)) => remote > current,
        _ => false,
    }
}

fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn timestamp_age(now: i64, timestamp: i64) -> Option<i64> {
    (timestamp <= now + 5 * 60).then_some(now.saturating_sub(timestamp).max(0))
}

fn valid_age(now: i64, timestamp: i64, window: i64) -> bool {
    timestamp_age(now, timestamp).is_some_and(|age| age < window)
}

fn automatic_attempts_in_window(attempts: &[i64], now: i64) -> usize {
    attempts
        .iter()
        .filter(|attempt| valid_age(now, **attempt, AUTOMATIC_WINDOW_SECS))
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semantic_version_comparison_handles_prereleases() {
        assert!(version_is_newer("0.2.0", "0.1.9"));
        assert!(!version_is_newer("0.2.0-rc.1", "0.2.0"));
        assert!(!version_is_newer("garbage", "0.1.0"));
    }

    #[test]
    fn rolling_automatic_limit_ignores_old_and_future_timestamps() {
        let now = 100_000;
        let attempts = vec![
            now - AUTOMATIC_WINDOW_SECS,
            now - 10,
            now,
            now + 60,
            now + 10 * 60,
        ];
        assert_eq!(automatic_attempts_in_window(&attempts, now), 3);
    }

    #[test]
    fn release_filter_selects_missed_versions_newest_first() {
        let releases = ["0.2.0", "0.4.0", "0.3.0", "0.1.0"]
            .into_iter()
            .map(|version| ReleaseNote {
                version: version.into(),
                tag: format!("v{version}"),
                date: String::new(),
                notes_markdown: version.into(),
                release_url: None,
            })
            .collect();
        assert_eq!(
            filter_releases(releases, "0.1.5", "0.3.0")
                .into_iter()
                .map(|release| release.version)
                .collect::<Vec<_>>(),
            ["0.3.0", "0.2.0"]
        );
    }

    #[test]
    fn future_success_time_is_not_fresh() {
        assert_eq!(timestamp_age(1_000, 2_000), None);
        assert_eq!(timestamp_age(1_000, 1_200), Some(0));
    }
}
