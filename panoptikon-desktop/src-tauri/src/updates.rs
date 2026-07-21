use crate::{
    settings::{SettingsDocument, UpdateSettings, atomic_write},
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
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tauri::{
    AppHandle, Emitter as _, Manager as _, WebviewUrl, WebviewWindow, WebviewWindowBuilder,
};
use tauri_plugin_updater::{Update, UpdaterExt as _};
use tokio::sync::{Mutex, watch};

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
const NATIVE_NOTIFICATION_RETRY_SECS: i64 = 4 * 60 * 60;

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

    fn strength(self) -> u8 {
        match self {
            Self::Startup | Self::Runtime => 0,
            Self::Manual => 1,
            Self::Freshness => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CheckFlightOutcome {
    Attempted(Result<(), String>),
    NotAttempted {
        owner_reason: CheckReason,
        result: Result<(), String>,
    },
    Cancelled(String),
}

#[derive(Debug, PartialEq, Eq)]
enum CheckOutcomeDisposition {
    Retry,
    Complete(Result<(), String>),
}

fn check_outcome_disposition(
    requested_reason: CheckReason,
    outcome: CheckFlightOutcome,
) -> CheckOutcomeDisposition {
    match outcome {
        CheckFlightOutcome::Attempted(result) => CheckOutcomeDisposition::Complete(result),
        CheckFlightOutcome::NotAttempted {
            owner_reason,
            result: _,
        } if requested_reason.strength() > owner_reason.strength() => {
            CheckOutcomeDisposition::Retry
        }
        CheckFlightOutcome::NotAttempted { result, .. } => {
            CheckOutcomeDisposition::Complete(result)
        }
        CheckFlightOutcome::Cancelled(error) => CheckOutcomeDisposition::Complete(Err(error)),
    }
}

struct CheckSingleFlight {
    registry: StdMutex<CheckFlightRegistry>,
}

struct CheckFlightRegistry {
    next_id: u64,
    active: Option<ActiveCheckFlight>,
}

struct ActiveCheckFlight {
    id: u64,
    result: watch::Sender<Option<CheckFlightOutcome>>,
}

enum CheckFlightEntry<'a> {
    Owner(CheckFlightOwner<'a>),
    Joiner(watch::Receiver<Option<CheckFlightOutcome>>),
}

struct CheckFlightOwner<'a> {
    single_flight: &'a CheckSingleFlight,
    id: u64,
    completed: bool,
}

impl CheckSingleFlight {
    fn new() -> Self {
        Self {
            registry: StdMutex::new(CheckFlightRegistry {
                next_id: 0,
                active: None,
            }),
        }
    }

    fn enter(&self) -> CheckFlightEntry<'_> {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(active) = &registry.active {
            return CheckFlightEntry::Joiner(active.result.subscribe());
        }
        registry.next_id = registry.next_id.wrapping_add(1).max(1);
        let id = registry.next_id;
        let (sender, _receiver) = watch::channel(None);
        registry.active = Some(ActiveCheckFlight { id, result: sender });
        CheckFlightEntry::Owner(CheckFlightOwner {
            single_flight: self,
            id,
            completed: false,
        })
    }

    fn complete(&self, id: u64, outcome: CheckFlightOutcome) {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if registry
            .active
            .as_ref()
            .is_some_and(|active| active.id == id)
        {
            let active = registry
                .active
                .take()
                .expect("active check flight disappeared");
            active.result.send_replace(Some(outcome));
        }
    }
}

impl CheckFlightOwner<'_> {
    fn finish(mut self, outcome: CheckFlightOutcome) {
        self.single_flight.complete(self.id, outcome);
        self.completed = true;
    }
}

impl Drop for CheckFlightOwner<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.single_flight.complete(
                self.id,
                CheckFlightOutcome::Cancelled("the update check was cancelled".into()),
            );
        }
    }
}

async fn wait_for_check_flight(
    mut result: watch::Receiver<Option<CheckFlightOutcome>>,
) -> CheckFlightOutcome {
    loop {
        let outcome = result.borrow().clone();
        if let Some(outcome) = outcome {
            return outcome;
        }
        if result.changed().await.is_err() {
            return CheckFlightOutcome::Cancelled("the update check ended without a result".into());
        }
    }
}

struct AtomicBoolReset<'a>(&'a AtomicBool);

impl AtomicBoolReset<'_> {
    fn set(flag: &AtomicBool) -> AtomicBoolReset<'_> {
        flag.store(true, Ordering::Release);
        AtomicBoolReset(flag)
    }
}

impl Drop for AtomicBoolReset<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
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

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UpdatePresentationState {
    Available,
    Checking,
    Failed,
    Current,
    Unchecked,
    Disabled,
}

#[derive(Debug, Deserialize)]
struct ChangelogFeed {
    schema_version: u32,
    releases: Vec<ReleaseNote>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UpdateView {
    pub presentation_state: UpdatePresentationState,
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
    pub active_work_unknown: bool,
}

pub struct UpdateCoordinator {
    operation_gate: Mutex<()>,
    check_single_flight: CheckSingleFlight,
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
struct BridgeVersionRequest {
    version: String,
}

#[derive(Debug, thiserror::Error)]
enum RibbonActionError {
    #[error("the available update changed; refresh and try again")]
    TargetChanged,
    #[error("failed to save Desktop update settings: {0}")]
    Persistence(#[source] anyhow::Error),
}

impl RibbonActionError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::TargetChanged => StatusCode::CONFLICT,
            Self::Persistence(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
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
    Json(request): Json<BridgeVersionRequest>,
) -> Result<StatusCode, StatusCode> {
    if !bridge_authorized(&headers, &state) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    set_ribbon_snooze(&state.app, request.version)
        .await
        .map_err(|error| error.status_code())?;
    Ok(StatusCode::NO_CONTENT)
}

async fn bridge_dismiss(
    State(state): State<BridgeServerState>,
    headers: HeaderMap,
    Json(request): Json<BridgeVersionRequest>,
) -> Result<StatusCode, StatusCode> {
    if !bridge_authorized(&headers, &state) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    set_ribbon_dismissal(&state.app, request.version)
        .await
        .map_err(|error| error.status_code())?;
    Ok(StatusCode::NO_CONTENT)
}

impl UpdateCoordinator {
    pub fn new() -> Self {
        Self {
            operation_gate: Mutex::new(()),
            check_single_flight: CheckSingleFlight::new(),
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
        let (settings, cache_path, port, local_enabled, sidecar_running) = {
            let supervisor = app.state::<Arc<Supervisor>>();
            let settings = supervisor.settings.lock().await.typed.updates.clone();
            let snapshot = supervisor.snapshot().await;
            (
                settings,
                supervisor.paths.update_changelog.clone(),
                snapshot.port,
                snapshot.local_server_enabled,
                snapshot.sidecar_pid.is_some(),
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
        let updates_disabled = crate::updates_disabled(app);
        let ribbon_visible = !updates_disabled
            && available
            && settings.ribbon_dismissed_version.as_deref() != settings.latest_version.as_deref()
            && settings
                .ribbon_snoozed_until_unix
                .is_none_or(|until| until <= now);
        let checking = self.checking.load(Ordering::Acquire);
        let installing = self.installing.load(Ordering::Acquire);
        let presentation_state = update_presentation_state(
            updates_disabled,
            available,
            checking,
            settings.last_checked_unix,
            settings.last_error.as_deref(),
            settings.last_error_unix,
        );
        let pending_matches = self.pending.lock().await.as_ref().is_some_and(|update| {
            settings.latest_version.as_deref() == Some(update.version.as_str())
        });
        let active_work = if local_enabled && sidecar_running {
            match probe_sidecar_active_work(port).await {
                Ok(true) => ActiveWorkState::Active,
                Ok(false) => ActiveWorkState::Idle,
                Err(error) => {
                    tracing::debug!(%error, "could not determine whether the sidecar has active work");
                    ActiveWorkState::Unknown
                }
            }
        } else {
            ActiveWorkState::Idle
        };
        UpdateView {
            presentation_state,
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
            checking,
            fresh,
            can_install: can_advertise_install(
                available,
                fresh,
                pending_matches,
                checking,
                installing,
            ),
            check_automatically: settings.check_automatically,
            ribbon_visible,
            ribbon_snoozed_until_unix: settings.ribbon_snoozed_until_unix,
            ribbon_dismissed_version: settings.ribbon_dismissed_version,
            reminder_at_unix: settings.reminder_at_unix,
            updates_disabled,
            active_work: active_work == ActiveWorkState::Active,
            active_work_unknown: active_work == ActiveWorkState::Unknown,
        }
    }

    pub async fn check(&self, app: &AppHandle, reason: CheckReason) -> Result<UpdateView, String> {
        if crate::updates_disabled(app) {
            return Ok(self.view(app).await);
        }

        loop {
            if self.installing.load(Ordering::Acquire) {
                if reason.automatic() {
                    return Ok(self.view(app).await);
                }
                return Err("an update installation is in progress".into());
            }
            let outcome = match self.check_single_flight.enter() {
                CheckFlightEntry::Owner(owner) => {
                    let outcome = self.run_external_check_flight(app, reason).await;
                    owner.finish(outcome.clone());
                    outcome
                }
                CheckFlightEntry::Joiner(result) => wait_for_check_flight(result).await,
            };
            match check_outcome_disposition(reason, outcome) {
                CheckOutcomeDisposition::Retry => continue,
                CheckOutcomeDisposition::Complete(result) => {
                    result?;
                    return Ok(self.view(app).await);
                }
            }
        }
    }

    async fn run_external_check_flight(
        &self,
        app: &AppHandle,
        reason: CheckReason,
    ) -> CheckFlightOutcome {
        let _operation_guard = self.operation_gate.lock().await;
        if self.installing.load(Ordering::Acquire) {
            let result = if reason.automatic() {
                Ok(())
            } else {
                Err("an update installation is in progress".into())
            };
            return CheckFlightOutcome::NotAttempted {
                owner_reason: reason,
                result,
            };
        }
        self.run_check_locked(app, reason).await
    }

    async fn run_check_locked(&self, app: &AppHandle, reason: CheckReason) -> CheckFlightOutcome {
        let now = unix_timestamp();
        match self.allow_attempt(app, reason, now).await {
            Ok(true) => {}
            Ok(false) => {
                return CheckFlightOutcome::NotAttempted {
                    owner_reason: reason,
                    result: Ok(()),
                };
            }
            Err(error) => {
                return CheckFlightOutcome::NotAttempted {
                    owner_reason: reason,
                    result: Err(error),
                };
            }
        }

        let checking_guard = AtomicBoolReset::set(&self.checking);
        if let Err(error) = self.record_attempt(app, reason, now).await {
            drop(checking_guard);
            emit_state(app, self).await;
            return CheckFlightOutcome::NotAttempted {
                owner_reason: reason,
                result: Err(error),
            };
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
                match self.record_failure(app, now).await {
                    Ok(()) => Err(
                        "Unable to reach the update service. Check your connection and try again."
                            .into(),
                    ),
                    Err(error) => Err(error),
                }
            }
        };
        drop(checking_guard);
        emit_state(app, self).await;
        CheckFlightOutcome::Attempted(outcome)
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
                let update_visible = app
                    .get_webview_window("update")
                    .and_then(|window| window.is_visible().ok())
                    .unwrap_or(false);
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
                settings.latest_published_at = update.date.and_then(format_published_at);
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
                }
                // Manual/freshness results are presented directly, and an
                // already-visible update window has likewise informed the
                // user. Treat both as surfaced so a later automatic check
                // does not add a redundant native interruption.
                if !reason.automatic() || update_visible {
                    settings.native_surfaced_version = Some(update.version.clone());
                } else if native_notification_retry_due(settings, &update.version, now) {
                    settings.native_notification_attempt_version = Some(update.version.clone());
                    settings.native_notification_last_attempt_unix = Some(now);
                    notify_version = Some(update.version.clone());
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
            notify_update(app.clone(), version).await;
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

    async fn install(
        &self,
        app: &AppHandle,
        expected_version: &str,
        active_work_confirmation: Option<ActiveWorkConfirmation>,
    ) -> Result<(), String> {
        if crate::updates_disabled(app) {
            return Err("updates are disabled in development builds".into());
        }
        if self.installing.swap(true, Ordering::AcqRel) {
            return Err("an update installation is already in progress".into());
        }
        let installing_guard = AtomicBoolReset(&self.installing);
        let result = {
            let _operation_guard = self.operation_gate.lock().await;
            self.install_inner_locked(app, expected_version, active_work_confirmation)
                .await
        };
        drop(installing_guard);
        emit_state(app, self).await;
        result
    }

    async fn install_inner_locked(
        &self,
        app: &AppHandle,
        expected_version: &str,
        active_work_confirmation: Option<ActiveWorkConfirmation>,
    ) -> Result<(), String> {
        let initial_view = self.view(app).await;
        if initial_view.target_version.as_deref() != Some(expected_version) {
            return Err("TARGET_CHANGED".into());
        }
        let pending_matches = self
            .pending
            .lock()
            .await
            .as_ref()
            .is_some_and(|update| update.version == expected_version);
        if !initial_view.fresh || !pending_matches || initial_view.checking {
            let outcome = self.run_check_locked(app, CheckReason::Freshness).await;
            match check_outcome_disposition(CheckReason::Freshness, outcome) {
                CheckOutcomeDisposition::Complete(result) => result?,
                CheckOutcomeDisposition::Retry => {
                    return Err("the required update check did not run".into());
                }
            }
        }
        let view = self.view(app).await;
        if view.target_version.as_deref() != Some(expected_version) {
            return Err("TARGET_CHANGED".into());
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
        if !install_target_ready(view.available, view.fresh, true, view.checking) {
            return Err("A fresh update check is required before installation.".into());
        }
        let initial_work_state = if view.active_work_unknown {
            ActiveWorkState::Unknown
        } else {
            ActiveWorkState::from_active(view.active_work)
        };
        validate_active_work_state(initial_work_state, active_work_confirmation)?;

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

        // Work can begin while a large update is downloading. Query again as
        // the final operation before stopping the sidecar, and only accept an
        // explicit confirmation from the update dialog.
        self.validate_exact_install_target(app, expected_version)
            .await?;
        let restart_snapshot = app.state::<Arc<Supervisor>>().snapshot().await;
        let final_work_state = if restart_snapshot.local_server_enabled
            && restart_snapshot.sidecar_pid.is_some()
        {
            match probe_sidecar_active_work(restart_snapshot.port).await {
                Ok(active) => ActiveWorkState::from_active(active),
                Err(error) => {
                    tracing::warn!(%error, "aborting update because sidecar work state is unknown");
                    ActiveWorkState::Unknown
                }
            }
        } else {
            ActiveWorkState::Idle
        };
        validate_active_work_state(final_work_state, active_work_confirmation)?;
        let restart_server_on_failure = restart_snapshot.local_server_enabled;
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

    async fn validate_exact_install_target(
        &self,
        app: &AppHandle,
        expected_version: &str,
    ) -> Result<(), String> {
        let supervisor = app.state::<Arc<Supervisor>>();
        let latest_version = supervisor
            .settings
            .lock()
            .await
            .typed
            .updates
            .latest_version
            .clone();
        let pending_matches = self
            .pending
            .lock()
            .await
            .as_ref()
            .is_some_and(|update| update.version == expected_version);
        if latest_version.as_deref() == Some(expected_version) && pending_matches {
            Ok(())
        } else {
            Err("TARGET_CHANGED".into())
        }
    }
}

fn install_target_ready(
    available: bool,
    fresh: bool,
    pending_matches: bool,
    checking: bool,
) -> bool {
    available && fresh && pending_matches && !checking
}

fn can_advertise_install(
    available: bool,
    fresh: bool,
    pending_matches: bool,
    checking: bool,
    installing: bool,
) -> bool {
    install_target_ready(available, fresh, pending_matches, checking) && !installing
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveWorkState {
    Idle,
    Active,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveWorkConfirmation {
    Active,
    Unknown,
}

impl ActiveWorkState {
    fn from_active(active: bool) -> Self {
        if active { Self::Active } else { Self::Idle }
    }
}

fn validate_active_work_state(
    active_work: ActiveWorkState,
    confirmation: Option<ActiveWorkConfirmation>,
) -> Result<(), String> {
    match active_work {
        ActiveWorkState::Unknown => Err("ACTIVE_WORK_UNKNOWN".into()),
        ActiveWorkState::Active if confirmation != Some(ActiveWorkConfirmation::Active) => {
            Err("ACTIVE_WORK".into())
        }
        ActiveWorkState::Idle | ActiveWorkState::Active => Ok(()),
    }
}

fn parse_active_work_confirmation(
    value: Option<&str>,
) -> Result<Option<ActiveWorkConfirmation>, String> {
    match value {
        None => Ok(None),
        Some("active") => Ok(Some(ActiveWorkConfirmation::Active)),
        Some("unknown") => Ok(Some(ActiveWorkConfirmation::Unknown)),
        Some(_) => Err("invalid active-work confirmation state".into()),
    }
}

fn update_presentation_state(
    updates_disabled: bool,
    available: bool,
    checking: bool,
    last_success_unix: Option<i64>,
    last_error: Option<&str>,
    last_error_unix: Option<i64>,
) -> UpdatePresentationState {
    if updates_disabled {
        return UpdatePresentationState::Disabled;
    }
    if available {
        return UpdatePresentationState::Available;
    }
    if checking {
        return UpdatePresentationState::Checking;
    }
    let latest_attempt_failed = last_error.is_some()
        && match (last_error_unix, last_success_unix) {
            (Some(error), Some(success)) => error > success,
            (_, None) => true,
            (None, Some(_)) => true,
        };
    if latest_attempt_failed {
        UpdatePresentationState::Failed
    } else if last_success_unix.is_some() {
        UpdatePresentationState::Current
    } else {
        UpdatePresentationState::Unchecked
    }
}

fn native_notification_retry_due(settings: &UpdateSettings, version: &str, now: i64) -> bool {
    if settings.native_surfaced_version.as_deref() == Some(version)
        || settings.native_notified_version.as_deref() == Some(version)
    {
        return false;
    }
    if settings.native_notification_attempt_version.as_deref() != Some(version) {
        return true;
    }
    settings
        .native_notification_last_attempt_unix
        .and_then(|attempt| timestamp_age(now, attempt))
        .is_none_or(|age| age >= NATIVE_NOTIFICATION_RETRY_SECS)
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
    if crate::updates_disabled(app) {
        return false;
    }
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
    releases_through_target(feed.releases, current, &update.version)
        .ok_or_else(|| "structured changelog has no notes for the target update".into())
}

fn read_releases(
    path: &std::path::Path,
    current: &str,
    target: Option<&str>,
) -> Option<Vec<ReleaseNote>> {
    let target = target?;
    let bytes = std::fs::read(path).ok()?;
    let releases: Vec<ReleaseNote> = serde_json::from_slice(&bytes).ok()?;
    releases_through_target(releases, current, target)
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

fn format_published_at(date: time::OffsetDateTime) -> Option<String> {
    date.format(&time::format_description::well_known::Rfc3339)
        .ok()
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

fn releases_through_target(
    releases: Vec<ReleaseNote>,
    current: &str,
    target: &str,
) -> Option<Vec<ReleaseNote>> {
    let target_version = Version::parse(target).ok()?;
    let releases = filter_releases(releases, current, target);
    releases
        .iter()
        .any(|release| Version::parse(&release.version).ok().as_ref() == Some(&target_version))
        .then_some(releases)
}

async fn probe_sidecar_active_work(port: u16) -> Result<bool, String> {
    let url = format!("http://127.0.0.1:{port}/api/jobs/queue");
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(700))
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| format!("failed to create sidecar status client: {error}"))?;
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|error| format!("sidecar work-state request failed: {error}"))?;
    if response.status() != StatusCode::OK {
        return Err(format!(
            "sidecar work-state request returned HTTP {}",
            response.status()
        ));
    }
    let value = response
        .json::<serde_json::Value>()
        .await
        .map_err(|error| format!("sidecar work-state response was not valid JSON: {error}"))?;
    parse_active_work_response(&value)
}

fn parse_active_work_response(value: &serde_json::Value) -> Result<bool, String> {
    value
        .get("queue")
        .and_then(serde_json::Value::as_array)
        .map(|queue| !queue.is_empty())
        .ok_or_else(|| "sidecar work-state response did not contain a queue array".into())
}

async fn notify_update(app: AppHandle, version: String) {
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let notification_app = app.clone();
    let title = format!("Panoptikon Desktop {version} is available");
    tauri::async_runtime::spawn_blocking(move || {
        let mut notification = notify_rust::Notification::new();
        notification
            .appname(&notification_app.package_info().name)
            .summary(&title)
            .body("Review what is new and install when you are ready.")
            .auto_icon()
            .action("update", "View update");
        // app_id ties the toast to the installed shortcut; Windows-only API.
        #[cfg(windows)]
        notification.app_id(&notification_app.config().identifier);
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
        document.typed.updates.native_notified_version = Some(version.clone());
        document.typed.updates.native_surfaced_version = Some(version);
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
    crate::host_open::open_url(&app, parsed.as_str()).map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn set_automatic_update_checks(
    window: WebviewWindow,
    app: AppHandle,
    enabled: bool,
) -> Result<UpdateView, String> {
    validate_update_client(&window)?;
    if crate::updates_disabled(&app) {
        return Err("updates are disabled in development builds".into());
    }
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
pub async fn snooze_update_ribbon(
    window: WebviewWindow,
    app: AppHandle,
    version: String,
) -> Result<(), String> {
    validate_update_client(&window)?;
    set_ribbon_snooze(&app, version)
        .await
        .map_err(|error| error.to_string())
}

async fn set_ribbon_snooze(app: &AppHandle, version: String) -> Result<(), RibbonActionError> {
    let supervisor = app.state::<Arc<Supervisor>>();
    let mut document = supervisor.settings.lock().await;
    validate_known_target(&document.typed.updates, &version)?;
    persist_update_settings(&mut document, |settings| {
        settings.ribbon_snoozed_until_unix = Some(unix_timestamp() + RIBBON_SNOOZE_SECS);
    })
}

#[tauri::command]
pub async fn dismiss_update_ribbon(
    window: WebviewWindow,
    app: AppHandle,
    version: String,
) -> Result<(), String> {
    validate_update_client(&window)?;
    set_ribbon_dismissal(&app, version)
        .await
        .map_err(|error| error.to_string())
}

async fn set_ribbon_dismissal(app: &AppHandle, version: String) -> Result<(), RibbonActionError> {
    let supervisor = app.state::<Arc<Supervisor>>();
    let mut document = supervisor.settings.lock().await;
    validate_known_target(&document.typed.updates, &version)?;
    persist_update_settings(&mut document, move |settings| {
        settings.ribbon_dismissed_version = Some(version);
    })
}

fn persist_update_settings(
    document: &mut SettingsDocument,
    mutate: impl FnOnce(&mut UpdateSettings),
) -> Result<(), RibbonActionError> {
    persist_update_settings_with(document, mutate, SettingsDocument::save)
}

fn persist_update_settings_with(
    document: &mut SettingsDocument,
    mutate: impl FnOnce(&mut UpdateSettings),
    save: impl FnOnce(&mut SettingsDocument) -> anyhow::Result<()>,
) -> Result<(), RibbonActionError> {
    // SettingsDocument::save merges typed values into its raw TOML before the
    // filesystem write. Restore the whole document so a failed write changes
    // neither the live view nor a later save of an unrelated setting.
    let previous = document.clone();
    mutate(&mut document.typed.updates);
    if let Err(error) = save(document) {
        *document = previous;
        return Err(RibbonActionError::Persistence(error));
    }
    Ok(())
}

fn validate_known_target(
    settings: &UpdateSettings,
    expected_version: &str,
) -> Result<(), RibbonActionError> {
    if settings.latest_version.as_deref() == Some(expected_version) {
        Ok(())
    } else {
        Err(RibbonActionError::TargetChanged)
    }
}

#[tauri::command]
pub async fn install_update(
    window: WebviewWindow,
    app: AppHandle,
    expected_version: String,
    confirmed_work_state: Option<String>,
) -> Result<(), String> {
    validate_update_window(&window)?;
    let active_work_confirmation = parse_active_work_confirmation(confirmed_work_state.as_deref())?;
    app.state::<Arc<UpdateCoordinator>>()
        .install(&app, &expected_version, active_work_confirmation)
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

    async fn serve_queue_response(
        status: StatusCode,
        body: &'static str,
    ) -> (u16, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let router = Router::new().route(
            "/api/jobs/queue",
            axum::routing::get(move || async move { (status, body) }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        (port, server)
    }

    async fn serve_redirecting_queue_response() -> (u16, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let router = Router::new()
            .route(
                "/api/jobs/queue",
                axum::routing::get(|| async {
                    (
                        StatusCode::FOUND,
                        [(axum::http::header::LOCATION, "/redirected")],
                    )
                }),
            )
            .route(
                "/redirected",
                axum::routing::get(|| async { (StatusCode::OK, r#"{"queue":[]}"#) }),
            );
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        (port, server)
    }

    fn release(version: &str) -> ReleaseNote {
        ReleaseNote {
            version: version.into(),
            tag: format!("v{version}"),
            date: String::new(),
            notes_markdown: version.into(),
            release_url: None,
        }
    }

    #[test]
    fn semantic_version_comparison_handles_prereleases() {
        assert!(version_is_newer("0.2.0", "0.1.9"));
        assert!(!version_is_newer("0.2.0-rc.1", "0.2.0"));
        assert!(!version_is_newer("garbage", "0.1.0"));
    }

    #[test]
    fn presentation_state_never_calls_a_failed_or_missing_check_current() {
        assert_eq!(
            update_presentation_state(false, false, false, None, None, None),
            UpdatePresentationState::Unchecked
        );
        assert_eq!(
            update_presentation_state(false, false, false, None, Some("offline"), Some(10)),
            UpdatePresentationState::Failed
        );
        assert_eq!(
            update_presentation_state(false, false, false, Some(10), Some("offline"), Some(20)),
            UpdatePresentationState::Failed
        );
        assert_eq!(
            update_presentation_state(false, false, false, Some(20), None, None),
            UpdatePresentationState::Current
        );
        assert_eq!(
            update_presentation_state(true, true, true, Some(20), None, None),
            UpdatePresentationState::Disabled
        );
    }

    #[test]
    fn native_notification_failures_retry_after_a_version_scoped_cooldown() {
        let now = 100_000;
        let mut settings = UpdateSettings::default();
        assert!(native_notification_retry_due(&settings, "0.3.0", now));

        settings.native_notification_attempt_version = Some("0.3.0".into());
        settings.native_notification_last_attempt_unix = Some(now);
        assert!(!native_notification_retry_due(&settings, "0.3.0", now));
        assert!(native_notification_retry_due(
            &settings,
            "0.3.0",
            now + NATIVE_NOTIFICATION_RETRY_SECS
        ));
        assert!(native_notification_retry_due(&settings, "0.4.0", now));

        settings.native_surfaced_version = Some("0.3.0".into());
        assert!(!native_notification_retry_due(
            &settings,
            "0.3.0",
            now + NATIVE_NOTIFICATION_RETRY_SECS
        ));
        settings.native_surfaced_version = None;
        settings.native_notified_version = Some("0.3.0".into());
        assert!(!native_notification_retry_due(
            &settings,
            "0.3.0",
            now + NATIVE_NOTIFICATION_RETRY_SECS
        ));
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
            .map(release)
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
    fn publication_dates_are_exposed_as_browser_parseable_rfc3339() {
        let epoch = time::OffsetDateTime::from_unix_timestamp(0).unwrap();
        assert_eq!(
            format_published_at(epoch).as_deref(),
            Some("1970-01-01T00:00:00Z")
        );
    }

    #[test]
    fn structured_notes_must_include_the_exact_target_release() {
        assert!(
            releases_through_target(vec![release("0.2.0"), release("0.3.0")], "0.1.0", "0.3.0",)
                .is_some()
        );
        assert!(releases_through_target(vec![release("0.2.0")], "0.1.0", "0.3.0").is_none());
    }

    #[test]
    fn installability_requires_live_idle_coordinator_state() {
        assert!(can_advertise_install(true, true, true, false, false));
        assert!(!can_advertise_install(true, true, false, false, false));
        assert!(!can_advertise_install(true, true, true, true, false));
        assert!(!can_advertise_install(true, true, true, false, true));

        // The installation owns the `installing` flag, so its internal target
        // validation deliberately excludes that public presentation flag.
        assert!(install_target_ready(true, true, true, false));
    }

    #[test]
    fn active_work_validation_fails_closed() {
        assert_eq!(
            validate_active_work_state(ActiveWorkState::Active, None),
            Err("ACTIVE_WORK".into())
        );
        assert_eq!(
            validate_active_work_state(
                ActiveWorkState::Active,
                Some(ActiveWorkConfirmation::Active)
            ),
            Ok(())
        );
        assert_eq!(
            validate_active_work_state(
                ActiveWorkState::Unknown,
                Some(ActiveWorkConfirmation::Unknown)
            ),
            Err("ACTIVE_WORK_UNKNOWN".into())
        );
        assert_eq!(
            validate_active_work_state(ActiveWorkState::Idle, None),
            Ok(())
        );
        assert_eq!(
            validate_active_work_state(
                ActiveWorkState::Active,
                Some(ActiveWorkConfirmation::Unknown)
            ),
            Err("ACTIVE_WORK".into())
        );
        assert_eq!(
            parse_active_work_confirmation(Some("active")),
            Ok(Some(ActiveWorkConfirmation::Active))
        );
        assert!(parse_active_work_confirmation(Some("forged")).is_err());
    }

    #[test]
    fn active_work_response_requires_a_queue_array() {
        assert_eq!(
            parse_active_work_response(&serde_json::json!({ "queue": [] })),
            Ok(false)
        );
        assert_eq!(
            parse_active_work_response(&serde_json::json!({ "queue": [{}] })),
            Ok(true)
        );
        assert!(parse_active_work_response(&serde_json::json!({})).is_err());
        assert!(parse_active_work_response(&serde_json::json!({ "queue": null })).is_err());
    }

    #[tokio::test]
    async fn active_work_probe_rejects_http_and_json_failures() {
        let (error_port, error_server) =
            serve_queue_response(StatusCode::SERVICE_UNAVAILABLE, r#"{"queue":[]}"#).await;
        assert!(probe_sidecar_active_work(error_port).await.is_err());
        error_server.abort();

        let (json_port, json_server) = serve_queue_response(StatusCode::OK, "not json").await;
        assert!(probe_sidecar_active_work(json_port).await.is_err());
        json_server.abort();

        let (schema_port, schema_server) = serve_queue_response(StatusCode::OK, "{}").await;
        assert!(probe_sidecar_active_work(schema_port).await.is_err());
        schema_server.abort();

        let (redirect_port, redirect_server) = serve_redirecting_queue_response().await;
        assert!(probe_sidecar_active_work(redirect_port).await.is_err());
        redirect_server.abort();
    }

    #[tokio::test]
    async fn active_work_probe_accepts_only_valid_queue_state() {
        let (idle_port, idle_server) =
            serve_queue_response(StatusCode::OK, r#"{"queue":[]}"#).await;
        assert_eq!(probe_sidecar_active_work(idle_port).await, Ok(false));
        idle_server.abort();

        let (active_port, active_server) =
            serve_queue_response(StatusCode::OK, r#"{"queue":[{}]}"#).await;
        assert_eq!(probe_sidecar_active_work(active_port).await, Ok(true));
        active_server.abort();
    }

    #[test]
    fn ribbon_actions_reject_a_stale_target() {
        let mut settings = UpdateSettings {
            latest_version: Some("0.3.0".into()),
            ..UpdateSettings::default()
        };
        assert!(validate_known_target(&settings, "0.3.0").is_ok());
        assert!(matches!(
            validate_known_target(&settings, "0.2.0"),
            Err(RibbonActionError::TargetChanged)
        ));
        settings.latest_version = None;
        assert!(validate_known_target(&settings, "0.3.0").is_err());
    }

    #[test]
    fn ribbon_action_status_distinguishes_conflict_from_persistence_failure() {
        assert_eq!(
            RibbonActionError::TargetChanged.status_code(),
            StatusCode::CONFLICT
        );
        assert_eq!(
            RibbonActionError::Persistence(anyhow::anyhow!("disk full")).status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn ribbon_persistence_failure_restores_live_settings() {
        let path = std::path::PathBuf::from("unused-settings.toml");
        let mut document = SettingsDocument::defaults(path).unwrap();
        document.typed.updates.latest_version = Some("0.3.0".into());
        document.typed.updates.ribbon_snoozed_until_unix = Some(123);
        let previous = document.clone();

        let result = persist_update_settings_with(
            &mut document,
            |settings| settings.ribbon_snoozed_until_unix = Some(456),
            |_| Err(anyhow::anyhow!("disk full")),
        );

        assert!(matches!(result, Err(RibbonActionError::Persistence(_))));
        assert_eq!(document.typed, previous.typed);
    }

    #[tokio::test]
    async fn single_flight_joiners_remain_bound_to_their_generation() {
        let single_flight = CheckSingleFlight::new();
        let first_owner = match single_flight.enter() {
            CheckFlightEntry::Owner(owner) => owner,
            CheckFlightEntry::Joiner(_) => panic!("first caller did not own the flight"),
        };
        let first_joiner = match single_flight.enter() {
            CheckFlightEntry::Joiner(result) => result,
            CheckFlightEntry::Owner(_) => panic!("second caller did not join the flight"),
        };
        let first_outcome = CheckFlightOutcome::Attempted(Err("first failed".into()));
        first_owner.finish(first_outcome.clone());

        let second_owner = match single_flight.enter() {
            CheckFlightEntry::Owner(owner) => owner,
            CheckFlightEntry::Joiner(_) => panic!("new generation did not get a new owner"),
        };
        let second_joiner = match single_flight.enter() {
            CheckFlightEntry::Joiner(result) => result,
            CheckFlightEntry::Owner(_) => panic!("second generation was not joined"),
        };
        let second_outcome = CheckFlightOutcome::Attempted(Ok(()));
        second_owner.finish(second_outcome.clone());

        assert_eq!(wait_for_check_flight(first_joiner).await, first_outcome);
        assert_eq!(wait_for_check_flight(second_joiner).await, second_outcome);
    }

    #[tokio::test]
    async fn cancelled_check_flight_wakes_joiners_and_allows_a_new_owner() {
        let single_flight = CheckSingleFlight::new();
        let owner = match single_flight.enter() {
            CheckFlightEntry::Owner(owner) => owner,
            CheckFlightEntry::Joiner(_) => panic!("first caller did not own the flight"),
        };
        let joiner = match single_flight.enter() {
            CheckFlightEntry::Joiner(result) => result,
            CheckFlightEntry::Owner(_) => panic!("second caller did not join the flight"),
        };
        drop(owner);
        assert!(matches!(
            wait_for_check_flight(joiner).await,
            CheckFlightOutcome::Cancelled(_)
        ));
        assert!(matches!(single_flight.enter(), CheckFlightEntry::Owner(_)));
    }

    #[test]
    fn stronger_caller_retries_a_suppressed_weaker_flight() {
        let skipped = CheckFlightOutcome::NotAttempted {
            owner_reason: CheckReason::Startup,
            result: Ok(()),
        };
        assert_eq!(
            check_outcome_disposition(CheckReason::Manual, skipped.clone()),
            CheckOutcomeDisposition::Retry
        );
        assert_eq!(
            check_outcome_disposition(CheckReason::Freshness, skipped.clone()),
            CheckOutcomeDisposition::Retry
        );
        assert_eq!(
            check_outcome_disposition(CheckReason::Runtime, skipped),
            CheckOutcomeDisposition::Complete(Ok(()))
        );
    }

    #[tokio::test]
    async fn operation_gate_excludes_checks_during_installation() {
        let coordinator = Arc::new(UpdateCoordinator::new());
        let installation_guard = coordinator.operation_gate.lock().await;
        let waiting_coordinator = coordinator.clone();
        let (acquired_tx, mut acquired_rx) = tokio::sync::oneshot::channel();
        let waiting_check = tokio::spawn(async move {
            let _check_guard = waiting_coordinator.operation_gate.lock().await;
            let _ = acquired_tx.send(());
        });

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), &mut acquired_rx)
                .await
                .is_err()
        );
        drop(installation_guard);
        tokio::time::timeout(std::time::Duration::from_secs(1), acquired_rx)
            .await
            .expect("waiting check never acquired the operation gate")
            .expect("waiting check dropped its completion signal");
        waiting_check.await.unwrap();
    }

    #[test]
    fn operation_flag_guard_resets_on_early_drop() {
        let installing = AtomicBool::new(true);
        {
            let _guard = AtomicBoolReset(&installing);
            assert!(installing.load(Ordering::Acquire));
        }
        assert!(!installing.load(Ordering::Acquire));
    }

    #[test]
    fn future_success_time_is_not_fresh() {
        assert_eq!(timestamp_age(1_000, 2_000), None);
        assert_eq!(timestamp_age(1_000, 1_200), Some(0));
    }
}
