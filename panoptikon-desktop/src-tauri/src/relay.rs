//! Origin-bound loopback Relay v1. The HTTP surface is intentionally small:
//! discovery, expiring local-approval pairing, and two authenticated actions.

use crate::{settings::atomic_write, share_cache::ShareCache};
use anyhow::{Context as _, bail};
use argon2::{
    Argon2, PasswordHash, PasswordHasher as _, PasswordVerifier as _, password_hash::SaltString,
};
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path as AxumPath, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::Engine as _;
use http_body_util::BodyExt as _;
use rand::RngCore as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    panic::AssertUnwindSafe,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::AsyncWriteExt as _,
    sync::{Mutex, RwLock, oneshot},
};
use url::Url;
use uuid::Uuid;

const PAIRING_TTL: Duration = Duration::from_secs(5 * 60);
const RATE_WINDOW: Duration = Duration::from_secs(60);
const RATE_LIMIT: usize = 5;
const MAX_PENDING: usize = 10;
const MAX_ACTION_RECORDS: usize = 1024;
const ACTION_TTL_SECS: i64 = 10 * 60;
/// A record parked waiting for its bytes lives far longer than a finished one:
/// the browser may be fetching a multi-gigabyte original, and the user may take
/// their time. It is still bounded — an abandoned tab must not leave a record
/// behind forever.
const PENDING_BYTES_TTL_SECS: i64 = 60 * 60;
const PRODUCTION_DEFAULT_BIND: &str = "127.0.0.1:16341";
const DEVELOPMENT_DEFAULT_BIND: &str = "127.0.0.1:17601";
const LEGACY_DEFAULT_BIND: &str = "127.0.0.1:17600";
/// Filename of the action-state sidecar, written beside `relay.toml`.
const ACTIONS_FILE_NAME: &str = "relay-actions.toml";
/// Default share cache ceiling. A tunable: the key is omitted from `relay.toml`
/// while it holds this value, so an install that never changed it tracks the
/// code default and a later change to that default reaches it. Writing the key
/// out unconditionally would instead freeze today's number on disk forever.
const DEFAULT_SHARE_CACHE_MAX_BYTES: u64 = 5 * 1024 * 1024 * 1024;
/// Slack allowed above an upload's declared `size` before the relay hangs up.
const UPLOAD_SIZE_SLACK: u64 = 1024 * 1024;
/// Longest browser-supplied filename accepted with a share verb; 255 is the
/// per-component ceiling of every filesystem the relay writes its cache to.
const MAX_SHARE_FILENAME_LEN: usize = 255;
/// Optional relay capabilities, advertised by `GET /v1/health`. A relay that
/// omits the field entirely predates capability advertisement and must be
/// treated by clients as advertising nothing.
const RELAY_FEATURES: &[&str] = &["copy_to_clipboard"];
/// The single opaque message a failed local action reports to the origin and
/// stores in its record. The detailed error is logged locally only: the
/// clipboard backend formats `path.display()` into its errors, and a remote
/// origin has no business learning this host's filesystem layout.
const GENERIC_ACTION_FAILURE: &str = "The local action failed.";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayConfig {
    #[serde(default = "Uuid::new_v4")]
    pub relay_id: Uuid,
    #[serde(default = "relay_enabled_by_default")]
    pub enabled: bool,
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default)]
    pub instances: Vec<RelayInstance>,
    #[serde(default)]
    pub commands: FileActionCommands,
    #[serde(
        default = "default_share_cache_max_bytes",
        skip_serializing_if = "is_default_share_cache_max"
    )]
    pub share_cache_max_bytes: u64,
    #[serde(default)]
    pairing_operations: Vec<PairingOperation>,
    /// Live action records. Persisted to the `relay-actions.toml` sidecar, not
    /// to `relay.toml` — see [`ActionsFile`]. `#[serde(default)]` still reads
    /// them out of a legacy `relay.toml` once, so an upgrade absorbs rather
    /// than drops whatever was in flight.
    #[serde(default, skip_serializing)]
    actions: Vec<ActionRecord>,
}

/// The action-state sidecar.
///
/// Action records are the only part of the Relay's state whose *shape* grows
/// with the protocol: every new verb or record state adds an enum variant that
/// an older binary cannot deserialize. Keeping them in `relay.toml` would mean
/// that downgrading — or running an older build once from a second install —
/// quarantines the whole file and silently destroys every pairing. The sidecar
/// contains nothing but disposable state, so the worst case of a parse failure
/// is a handful of forgotten in-flight actions.
#[derive(Debug, Default, Serialize, Deserialize)]
struct ActionsFile {
    #[serde(default)]
    actions: Vec<ActionRecord>,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            relay_id: Uuid::new_v4(),
            enabled: relay_enabled_by_default(),
            bind: default_bind(),
            instances: Vec::new(),
            commands: FileActionCommands::default(),
            share_cache_max_bytes: default_share_cache_max_bytes(),
            pairing_operations: Vec::new(),
            actions: Vec::new(),
        }
    }
}

fn relay_enabled_by_default() -> bool {
    true
}

fn default_share_cache_max_bytes() -> u64 {
    DEFAULT_SHARE_CACHE_MAX_BYTES
}

fn is_default_share_cache_max(value: &u64) -> bool {
    *value == DEFAULT_SHARE_CACHE_MAX_BYTES
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FileActionCommands {
    #[serde(default)]
    pub open_file: CommandSpec,
    #[serde(default)]
    pub reveal_in_folder: CommandSpec,
    #[serde(default)]
    pub copy_to_clipboard: CommandSpec,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CommandSpec {
    #[serde(default)]
    pub mode: CommandMode,
    #[serde(default)]
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub shell_command: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CommandMode {
    #[default]
    SystemDefault,
    SpecificApplication,
    CustomDirect,
    CustomShell,
}

impl RelayConfig {
    pub fn desktop_default(development: bool) -> Self {
        Self {
            bind: if development {
                DEVELOPMENT_DEFAULT_BIND.into()
            } else {
                default_bind()
            },
            ..Self::default()
        }
    }
}

fn default_bind() -> String {
    PRODUCTION_DEFAULT_BIND.into()
}

/// Path of the action sidecar belonging to a `relay.toml`.
fn actions_path(config_path: &Path) -> PathBuf {
    config_path.with_file_name(ACTIONS_FILE_NAME)
}

/// A unique `.toml.invalid-*` sibling for quarantining an unparseable file.
/// The nanosecond component — plus a counter fallback — keeps two quarantines
/// in the same second (of one file, or of `relay.toml` and its sidecar) from
/// silently clobbering each other.
fn quarantine_path(path: &Path) -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let base = format!("toml.invalid-{}-{}", now.as_secs(), now.subsec_nanos());
    let mut candidate = path.with_extension(&base);
    let mut counter = 0u32;
    while candidate.exists() {
        counter += 1;
        candidate = path.with_extension(format!("{base}-{counter}"));
    }
    candidate
}

/// Reads the action sidecar, quarantining it — and only it — when it cannot be
/// parsed. Losing every pairing because an action record grew a field the
/// running binary does not know is exactly what the sidecar exists to prevent.
fn load_actions(path: &Path) -> Vec<ActionRecord> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    match toml::from_str::<ActionsFile>(&text) {
        Ok(file) => file.actions,
        Err(error) => {
            let quarantine = quarantine_path(path);
            match std::fs::rename(path, &quarantine) {
                Ok(()) => tracing::warn!(
                    %error,
                    quarantine = %quarantine.display(),
                    "Relay action state was invalid and was quarantined; in-flight actions were dropped"
                ),
                Err(rename_error) => tracing::warn!(
                    %error, %rename_error,
                    "Relay action state was invalid and could not be quarantined"
                ),
            }
            Vec::new()
        }
    }
}

/// A command that was running when the process died can never be observed
/// again, so leaving the record `Executing` would make a polling browser wait
/// forever against a state the TTL deliberately never prunes.
fn recover_interrupted_actions(actions: &mut [ActionRecord]) -> bool {
    let mut recovered = false;
    for action in actions {
        if matches!(action.state, ActionRecordState::Executing) {
            action.state = ActionRecordState::Failed {
                code: "interrupted".into(),
                message: "Interrupted by a Desktop restart".into(),
            };
            recovered = true;
        }
    }
    recovered
}

pub fn load_config(path: &Path, development: bool) -> anyhow::Result<RelayConfig> {
    let sidecar = actions_path(path);
    if !path.exists() {
        let mut config = RelayConfig::desktop_default(development);
        config.actions = load_actions(&sidecar);
        recover_interrupted_actions(&mut config.actions);
        return Ok(config);
    }
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read Relay settings '{}'", path.display()))?;
    match toml::from_str(&text) {
        Ok(config) => {
            let mut config: RelayConfig = config;
            let mut migrated = false;
            // A `relay.toml` written before the split may still carry actions.
            // The sidecar is authoritative once it exists; otherwise the
            // embedded records are absorbed and rewritten to their new home.
            if sidecar.exists() {
                config.actions = load_actions(&sidecar);
            } else if !config.actions.is_empty() {
                // Best effort: a sidecar write failure must never propagate out
                // of `load_config`, or the caller falls back to an empty config
                // and the next save overwrites `relay.toml`, destroying every
                // pairing. The in-memory absorption still stands; only its
                // persistence is skipped. Mirrors `persist_pruned`.
                if let Err(error) = save_actions(&sidecar, &config.actions) {
                    tracing::warn!(%error, path = %sidecar.display(), "failed to persist migrated Relay actions");
                }
                migrated = true;
            }
            if recover_interrupted_actions(&mut config.actions) {
                if let Err(error) = save_actions(&sidecar, &config.actions) {
                    tracing::warn!(%error, path = %sidecar.display(), "failed to persist recovered Relay actions");
                }
            }
            if config.bind == LEGACY_DEFAULT_BIND {
                config.bind = RelayConfig::desktop_default(development).bind;
                migrated = true;
            }
            for command in [
                &mut config.commands.open_file,
                &mut config.commands.reveal_in_folder,
                &mut config.commands.copy_to_clipboard,
            ] {
                if command.mode == CommandMode::SystemDefault {
                    if !command.shell_command.trim().is_empty() {
                        command.mode = CommandMode::CustomShell;
                        migrated = true;
                    } else if !command.program.trim().is_empty() {
                        command.mode = CommandMode::CustomDirect;
                        migrated = true;
                    }
                }
            }
            if migrated
                || !text
                    .lines()
                    .any(|line| line.trim_start().starts_with("relay_id"))
            {
                save_config(path, &config)?;
            }
            Ok(config)
        }
        Err(error) => {
            let quarantine = quarantine_path(path);
            std::fs::rename(path, &quarantine)?;
            bail!(
                "Relay settings '{}' are invalid and were quarantined as '{}': {error}",
                path.display(),
                quarantine.display()
            );
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayInstance {
    pub id: Uuid,
    pub name: String,
    pub server_url: String,
    pub origins: Vec<String>,
    pub credential_hash: String,
    #[serde(default)]
    pub mappings: Vec<PathMapping>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PathMapping {
    pub remote: String,
    pub local: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RelayStatusView {
    pub enabled: bool,
    pub bind: String,
    pub instances: Vec<RelayInstanceView>,
    pub commands: FileActionCommands,
    pub pending_actions: Vec<PendingActionView>,
    /// Current share-cache ceiling in bytes, so the control UI can populate the
    /// "Share cache limit" field (rendered in MB) on load.
    pub share_cache_max_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RelayInstanceView {
    pub id: Uuid,
    pub name: String,
    pub server_url: String,
    pub origins: Vec<String>,
    pub mappings: Vec<PathMapping>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PendingPairingView {
    pub id: Uuid,
    pub name: String,
    pub origin: String,
    pub server_url: String,
    pub roots: Vec<String>,
    pub expires_in_secs: u64,
    pub status: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct PairingProgressView {
    pub status: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PairingOperation {
    id: Uuid,
    name: String,
    origin: String,
    server_url: String,
    roots: Vec<String>,
    created_unix: i64,
    state: PairingOperationState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum PairingOperationState {
    Pending,
    Rejected,
    ApprovedUnconfirmed {
        instance_id: Uuid,
        credential: String,
    },
    Complete {
        instance_id: Uuid,
        completed_unix: i64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActionRecord {
    id: Uuid,
    instance_id: Uuid,
    action: RelayAction,
    remote_path: String,
    created_unix: i64,
    state: ActionRecordState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum ActionRecordState {
    PendingMapping,
    /// A share verb with no local copy of the bytes yet. Invisible to the
    /// mapping window by construction: only `PendingMapping` reaches it.
    PendingBytes {
        sha256: String,
        filename: String,
        size: u64,
    },
    Executing,
    Complete,
    Failed {
        code: String,
        message: String,
    },
}

type ActionHandler =
    Arc<dyn Fn(RelayAction, PathBuf, CommandSpec) -> anyhow::Result<()> + Send + Sync>;
type AttentionHandler = Arc<dyn Fn() + Send + Sync>;

/// How many recently handed-out cache paths are protected from eviction. One
/// per clipboard the user could plausibly still paste from; a small ring keeps
/// the protection from becoming a second, unbounded cache.
const MAX_HELD_CACHE_PATHS: usize = 8;

pub struct RelayState {
    config: RwLock<RelayConfig>,
    config_path: PathBuf,
    actions_path: PathBuf,
    share_cache: ShareCache,
    attempts: Mutex<HashMap<String, VecDeque<Instant>>>,
    /// Action ids with an upload currently streaming to disk. Guards against
    /// two concurrent uploads for one action and marks their temporary files
    /// as live for the cache sweep.
    uploads: std::sync::Mutex<HashSet<Uuid>>,
    /// Cache paths recently handed to a local command. One of them may be
    /// sitting on the system clipboard right now, where eviction would turn a
    /// later paste into a silent no-op.
    held_cache_paths: std::sync::Mutex<VecDeque<PathBuf>>,
    action_handler: ActionHandler,
    pairing_attention_handler: AttentionHandler,
    mapping_attention_handler: AttentionHandler,
}

/// Releases an upload claim on every exit path of `upload_file`, including
/// early returns and panics.
struct UploadClaim {
    state: Arc<RelayState>,
    action_id: Uuid,
}

impl Drop for UploadClaim {
    fn drop(&mut self) {
        self.state.uploads_lock().remove(&self.action_id);
    }
}

/// Location verbs (`OpenFile`, `RevealInFolder`) act on the real file at its
/// real path and prompt for a mapping when none resolves. Share verbs
/// (`CopyToClipboard`) act on the bytes: a mapping is a silent optimization
/// and its absence materializes a cache copy instead of prompting.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RelayAction {
    OpenFile,
    RevealInFolder,
    CopyToClipboard,
}

impl RelayAction {
    fn is_share_verb(self) -> bool {
        matches!(self, RelayAction::CopyToClipboard)
    }
}

#[derive(Debug, Deserialize)]
struct PairingRequest {
    operation_id: Uuid,
    name: String,
    origin: String,
    server_url: String,
    #[serde(default)]
    roots: Vec<String>,
}

/// `sha256`/`filename`/`size` describe the *bytes* and are required by share
/// verbs only; location verbs ignore them entirely.
#[derive(Debug, Deserialize)]
struct ActionRequest {
    action_id: Uuid,
    action: RelayAction,
    path: String,
    #[serde(default)]
    sha256: Option<String>,
    #[serde(default)]
    filename: Option<String>,
    #[serde(default)]
    size: Option<u64>,
}

/// Validated share metadata of a `copy_to_clipboard` request.
struct ShareMetadata {
    sha256: String,
    filename: String,
    size: u64,
}

fn share_metadata(request: &ActionRequest) -> Result<ShareMetadata, &'static str> {
    let sha256 = request.sha256.as_deref().unwrap_or_default();
    if sha256.len() != 64
        || !sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("sha256 must be 64 lowercase hexadecimal characters");
    }
    let filename = request.filename.as_deref().unwrap_or_default().trim();
    if filename.is_empty() || filename.len() > MAX_SHARE_FILENAME_LEN {
        return Err("filename must be non-empty and at most 255 bytes");
    }
    // Zero is a legitimate size: empty files exist, and every downstream rule
    // (the upload ceiling, the size match, the cache lookup) handles 0 without
    // a special case. Only a *missing* size is a protocol error, and serde
    // already defaulted that to zero — an absent field and an explicit `0` are
    // indistinguishable here and both mean "no bytes".
    let size = request.size.unwrap_or_default();
    Ok(ShareMetadata {
        sha256: sha256.to_owned(),
        filename: filename.to_owned(),
        size,
    })
}

#[derive(Debug, Clone, Serialize)]
pub struct PendingActionView {
    pub id: Uuid,
    pub instance_id: Uuid,
    pub action: RelayAction,
    pub remote_path: String,
    pub suggested_remote_root: String,
}

#[derive(Debug, Serialize)]
struct Health {
    protocol: &'static str,
    version: &'static str,
    pairing: bool,
    relay_id: Uuid,
    /// Optional capabilities beyond protocol v1. Clients treat an absent
    /// field (an older relay) as an empty list.
    features: &'static [&'static str],
}

impl RelayState {
    pub fn new(
        config: RelayConfig,
        config_path: PathBuf,
        share_cache_root: PathBuf,
        action_handler: ActionHandler,
        pairing_attention_handler: AttentionHandler,
        mapping_attention_handler: AttentionHandler,
    ) -> Self {
        Self {
            actions_path: actions_path(&config_path),
            config: RwLock::new(config),
            config_path,
            share_cache: ShareCache::new(share_cache_root),
            attempts: Mutex::new(HashMap::new()),
            uploads: std::sync::Mutex::new(HashSet::new()),
            held_cache_paths: std::sync::Mutex::new(VecDeque::new()),
            action_handler,
            pairing_attention_handler,
            mapping_attention_handler,
        }
    }

    pub async fn config(&self) -> RelayConfig {
        self.config.read().await.clone()
    }

    /// A poisoned claim set only means some upload task panicked; the set
    /// itself is a plain id collection and stays usable.
    fn uploads_lock(&self) -> std::sync::MutexGuard<'_, HashSet<Uuid>> {
        self.uploads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn in_flight_uploads(&self) -> HashSet<Uuid> {
        self.uploads_lock().clone()
    }

    /// Claims the upload slot for `action_id`, or `None` when another upload
    /// for the same action is already streaming.
    fn claim_upload(self: &Arc<Self>, action_id: Uuid) -> Option<UploadClaim> {
        self.uploads_lock().insert(action_id).then(|| UploadClaim {
            state: self.clone(),
            action_id,
        })
    }

    /// Records a cache path as live so eviction cannot pull it out from under
    /// a clipboard entry.
    fn hold_cache_path(&self, path: &Path) {
        let mut held = self
            .held_cache_paths
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        held.retain(|item| item != path);
        held.push_back(path.to_path_buf());
        while held.len() > MAX_HELD_CACHE_PATHS {
            held.pop_front();
        }
    }

    fn held_cache_paths(&self) -> Vec<PathBuf> {
        self.held_cache_paths
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .cloned()
            .collect()
    }

    /// Runs a configured local command, converting a panic in the handler into
    /// an ordinary failed action. Without this a panicking clipboard backend
    /// would leave the record `Executing` — a state the TTL never prunes — and
    /// the browser polling it forever.
    fn run_handler(
        &self,
        action: RelayAction,
        path: PathBuf,
        command: CommandSpec,
    ) -> anyhow::Result<()> {
        std::panic::catch_unwind(AssertUnwindSafe(|| {
            (self.action_handler)(action, path, command)
        }))
        .unwrap_or_else(|_| Err(anyhow::anyhow!("the local action crashed")))
    }

    fn persist_actions(&self, config: &RelayConfig) -> anyhow::Result<()> {
        save_actions(&self.actions_path, &config.actions)
    }

    /// Persists whatever a `prune_config` pass may have dropped. Pruning
    /// touches both files, and neither loss is worth failing a request over.
    fn persist_pruned(&self, config: &RelayConfig) {
        if let Err(error) = save_config(&self.config_path, config) {
            tracing::warn!(%error, "failed to persist Relay state garbage collection");
        }
        if let Err(error) = self.persist_actions(config) {
            tracing::warn!(%error, "failed to persist Relay action garbage collection");
        }
    }

    /// Return only fields safe to expose to the bundled control UI. In
    /// particular, credential hashes never cross the Rust command boundary.
    pub async fn status(&self) -> RelayStatusView {
        let mut config = self.config.write().await;
        if prune_config(&mut config) {
            self.persist_pruned(&config);
        }
        RelayStatusView {
            enabled: config.enabled,
            bind: config.bind.clone(),
            instances: config
                .instances
                .iter()
                .map(|item| RelayInstanceView {
                    id: item.id,
                    name: item.name.clone(),
                    server_url: item.server_url.clone(),
                    origins: item.origins.clone(),
                    mappings: item.mappings.clone(),
                })
                .collect(),
            commands: config.commands.clone(),
            pending_actions: config
                .actions
                .iter()
                .filter(|item| matches!(item.state, ActionRecordState::PendingMapping))
                .map(|item| PendingActionView {
                    id: item.id,
                    instance_id: item.instance_id,
                    action: item.action,
                    remote_path: item.remote_path.clone(),
                    suggested_remote_root: suggested_remote_root(
                        &item.remote_path,
                        &config
                            .instances
                            .iter()
                            .find(|instance| instance.id == item.instance_id)
                            .map(|instance| instance.mappings.as_slice())
                            .unwrap_or_default(),
                    ),
                })
                .collect(),
            share_cache_max_bytes: config.share_cache_max_bytes,
        }
    }

    pub async fn pending_actions(&self) -> Vec<PendingActionView> {
        self.status().await.pending_actions
    }

    pub async fn cancel_pending_actions(&self) -> anyhow::Result<()> {
        let mut config = self.config.write().await;
        for action in &mut config.actions {
            if matches!(action.state, ActionRecordState::PendingMapping) {
                action.state = ActionRecordState::Failed {
                    code: "mapping_cancelled".into(),
                    message: "Folder mapping was cancelled in Panoptikon Desktop".into(),
                };
            }
        }
        self.persist_actions(&config)
    }

    pub async fn set_enabled(&self, enabled: bool) -> anyhow::Result<()> {
        let mut config = self.config.write().await;
        config.enabled = enabled;
        save_config(&self.config_path, &config)
    }

    pub async fn set_share_cache_max_bytes(&self, max_bytes: u64) -> anyhow::Result<()> {
        let mut config = self.config.write().await;
        config.share_cache_max_bytes = max_bytes;
        save_config(&self.config_path, &config)
    }

    pub async fn set_commands(&self, commands: FileActionCommands) -> anyhow::Result<()> {
        for (command, is_clipboard) in [
            (&commands.open_file, false),
            (&commands.reveal_in_folder, false),
            (&commands.copy_to_clipboard, true),
        ] {
            let contains_placeholder = command
                .args
                .iter()
                .chain([&command.program, &command.shell_command])
                .any(|value| {
                    ["{path}", "{folder}", "{filename}"]
                        .iter()
                        .any(|placeholder| value.contains(placeholder))
                });
            match command.mode {
                CommandMode::SystemDefault => {
                    if !command.program.is_empty()
                        || !command.shell_command.is_empty()
                        || !command.args.is_empty()
                    {
                        bail!("system-default actions cannot include a command");
                    }
                }
                CommandMode::SpecificApplication | CommandMode::CustomDirect => {
                    if command.program.trim().is_empty() || !command.shell_command.trim().is_empty()
                    {
                        bail!("direct actions require one executable and no shell command");
                    }
                    if !contains_placeholder {
                        bail!("custom actions must include a path placeholder");
                    }
                    if command.mode == CommandMode::SpecificApplication
                        && !Path::new(&command.program).exists()
                    {
                        bail!("the selected application does not exist");
                    }
                }
                CommandMode::CustomShell => {
                    if command.shell_command.trim().is_empty() || !command.program.trim().is_empty()
                    {
                        bail!("shell actions require one shell command and no direct executable");
                    }
                    if !contains_placeholder {
                        bail!("custom actions must include a path placeholder");
                    }
                    // The clipboard verb's placeholder values are quoted
                    // automatically because its `{filename}` is remote-supplied
                    // (it rides in on the upload). A quote in the template
                    // itself would close that automatic quoting and hand the
                    // attacker-authored filename tail to the shell unquoted, so
                    // a correct clipboard template never quotes its own
                    // placeholders. Location verbs are Raw-substituted and quote
                    // their placeholders themselves, so this applies only here.
                    if is_clipboard && command.shell_command.contains(['"', '\'']) {
                        bail!(
                            "the clipboard command's placeholders are quoted automatically; \
                             remove the quotes from your shell command"
                        );
                    }
                }
            }
        }
        let mut config = self.config.write().await;
        config.commands = commands;
        save_config(&self.config_path, &config)
    }

    pub async fn pending(&self) -> Vec<PendingPairingView> {
        let mut config = self.config.write().await;
        if prune_config(&mut config) {
            self.persist_pruned(&config);
        }
        let now = unix_now();
        config
            .pairing_operations
            .iter()
            .filter(|item| {
                matches!(
                    item.state,
                    PairingOperationState::Pending
                        | PairingOperationState::ApprovedUnconfirmed { .. }
                )
            })
            .map(|item| PendingPairingView {
                id: item.id,
                name: item.name.clone(),
                origin: item.origin.clone(),
                server_url: item.server_url.clone(),
                roots: item.roots.clone(),
                expires_in_secs: (item.created_unix + PAIRING_TTL.as_secs() as i64 - now).max(0)
                    as u64,
                status: match item.state {
                    PairingOperationState::Pending => "pending",
                    PairingOperationState::ApprovedUnconfirmed { .. } => "finishing",
                    _ => unreachable!("filtered to incomplete pairing states"),
                },
            })
            .collect()
    }

    pub async fn pairing_progress(&self, request_id: Uuid) -> Option<PairingProgressView> {
        let config = self.config.read().await;
        config
            .pairing_operations
            .iter()
            .find(|item| item.id == request_id)
            .map(|item| {
                let status = match item.state {
                    PairingOperationState::Pending => "pending",
                    PairingOperationState::Rejected => "rejected",
                    PairingOperationState::ApprovedUnconfirmed { .. } => "finishing",
                    PairingOperationState::Complete { .. } => "complete",
                };
                PairingProgressView { status }
            })
    }

    /// Closing the dedicated pairing window is an explicit cancellation.
    /// Keep rejected tombstones long enough for polling browsers to observe
    /// them and cancel their matching durable Server operations.
    pub async fn cancel_incomplete_pairings(&self) -> anyhow::Result<()> {
        let mut config = self.config.write().await;
        let provisional_instances = config
            .pairing_operations
            .iter()
            .filter_map(|item| match item.state {
                PairingOperationState::ApprovedUnconfirmed { instance_id, .. } => Some(instance_id),
                _ => None,
            })
            .collect::<Vec<_>>();
        config
            .instances
            .retain(|item| !provisional_instances.contains(&item.id));
        for operation in &mut config.pairing_operations {
            if matches!(
                operation.state,
                PairingOperationState::Pending | PairingOperationState::ApprovedUnconfirmed { .. }
            ) {
                operation.state = PairingOperationState::Rejected;
            }
        }
        save_config(&self.config_path, &config)
    }

    #[cfg(test)]
    pub async fn approve(&self, request_id: Uuid) -> anyhow::Result<()> {
        self.approve_with_mappings(request_id, Vec::new()).await
    }

    pub async fn approve_with_mappings(
        &self,
        request_id: Uuid,
        mappings: Vec<PathMapping>,
    ) -> anyhow::Result<()> {
        let mut config = self.config.write().await;
        prune_config(&mut config);
        let Some(index) = config
            .pairing_operations
            .iter()
            .position(|item| item.id == request_id)
        else {
            bail!("pairing request not found or expired");
        };
        match config.pairing_operations[index].state {
            PairingOperationState::ApprovedUnconfirmed { .. }
            | PairingOperationState::Complete { .. } => return Ok(()),
            PairingOperationState::Rejected => bail!("pairing request was rejected"),
            PairingOperationState::Pending => {}
        }
        let name = config.pairing_operations[index].name.clone();
        let origin = config.pairing_operations[index].origin.clone();
        let server_url = config.pairing_operations[index].server_url.clone();
        for mapping in &mappings {
            // Supplied roots are usability hints, not authorization. The
            // user-approved mapping prefix is the actual Relay boundary and
            // may narrow, broaden, or replace the suggestion entirely.
            normalize_path(&mapping.remote)?;
            if !mapping.local.trim().is_empty() {
                normalize_path(&mapping.local)?;
            }
        }
        let mut secret = [0u8; 32];
        rand::rng().fill_bytes(&mut secret);
        let credential = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret);
        let salt = SaltString::encode_b64(&secret[..16])
            .map_err(|error| anyhow::anyhow!("failed to encode credential salt: {error}"))?;
        let credential_hash = Argon2::default()
            .hash_password(credential.as_bytes(), &salt)
            .map_err(|error| anyhow::anyhow!("failed to hash Relay credential: {error}"))?
            .to_string();
        let instance_id = Uuid::new_v4();
        // Explicit approval of a replacement rotates any earlier instance for
        // this origin, including an abandoned provisional pairing.
        config
            .instances
            .retain(|item| !item.origins.iter().any(|item| item == &origin));
        config
            .pairing_operations
            .retain(|item| item.id == request_id || item.origin != origin);
        let index = config
            .pairing_operations
            .iter()
            .position(|item| item.id == request_id)
            .context("pairing request disappeared")?;
        config.instances.push(RelayInstance {
            id: instance_id,
            name,
            server_url,
            origins: vec![origin],
            credential_hash,
            // A root left blank in the pairing window is intentionally
            // unmapped. Do not persist an empty local prefix: that would make
            // translation appear to succeed and bypass the first-use mapping
            // flow.
            mappings: mappings
                .into_iter()
                .filter_map(|mapping| {
                    let remote = mapping.remote.trim().to_owned();
                    let local = mapping.local.trim().to_owned();
                    (!local.is_empty()).then_some(PathMapping { remote, local })
                })
                .collect(),
        });
        config.pairing_operations[index].state = PairingOperationState::ApprovedUnconfirmed {
            instance_id,
            credential,
        };
        save_config(&self.config_path, &config)
    }

    pub async fn reject(&self, request_id: Uuid) -> anyhow::Result<()> {
        let mut config = self.config.write().await;
        let item = config
            .pairing_operations
            .iter_mut()
            .find(|item| item.id == request_id)
            .context("pairing request not found")?;
        match item.state {
            PairingOperationState::Pending => item.state = PairingOperationState::Rejected,
            PairingOperationState::Rejected => return Ok(()),
            _ => bail!("pairing request is already approved"),
        }
        save_config(&self.config_path, &config)
    }

    pub async fn revoke(&self, instance_id: Uuid) -> anyhow::Result<()> {
        let mut config = self.config.write().await;
        let old_len = config.instances.len();
        config.instances.retain(|item| item.id != instance_id);
        config.pairing_operations.retain(|item| match item.state {
            PairingOperationState::ApprovedUnconfirmed {
                instance_id: id, ..
            }
            | PairingOperationState::Complete {
                instance_id: id, ..
            } => id != instance_id,
            _ => true,
        });
        config
            .actions
            .retain(|item| item.instance_id != instance_id);
        if config.instances.len() == old_len {
            bail!("Relay instance not found");
        }
        // Revocation changes both halves of the split state: the instance list
        // in `relay.toml` and that instance's actions in the sidecar.
        save_config(&self.config_path, &config)?;
        self.persist_actions(&config)
    }

    pub async fn replace_mappings(
        &self,
        instance_id: Uuid,
        mappings: Vec<PathMapping>,
    ) -> anyhow::Result<()> {
        for mapping in &mappings {
            normalize_path(&mapping.remote)?;
            if !mapping.local.trim().is_empty() {
                normalize_path(&mapping.local)?;
            }
        }
        let mut config = self.config.write().await;
        let instance = config
            .instances
            .iter_mut()
            .find(|item| item.id == instance_id)
            .context("Relay instance not found")?;
        instance.mappings = mappings;
        save_config(&self.config_path, &config)?;
        drop(config);
        self.retry_pending_actions(instance_id).await
    }

    pub async fn resolve_mapping(
        &self,
        action_id: Uuid,
        remote: String,
        local: String,
    ) -> anyhow::Result<()> {
        normalize_path(&remote)?;
        normalize_path(&local)?;
        let mut config = self.config.write().await;
        let action = config
            .actions
            .iter()
            .find(|item| {
                item.id == action_id && matches!(item.state, ActionRecordState::PendingMapping)
            })
            .context("pending Relay action not found")?;
        let instance_id = action.instance_id;
        let translated = map_path(
            &action.remote_path,
            &[PathMapping {
                remote: remote.clone(),
                local: local.clone(),
            }],
        )?;
        if !translated.exists() {
            bail!("the translated path does not exist");
        }
        let instance = config
            .instances
            .iter_mut()
            .find(|item| item.id == instance_id)
            .context("Relay instance not found")?;
        instance.mappings.retain(|item| item.remote != remote);
        instance.mappings.push(PathMapping { remote, local });
        save_config(&self.config_path, &config)?;
        drop(config);
        self.retry_pending_actions(instance_id).await
    }

    pub async fn mapping_preview(
        &self,
        action_id: Uuid,
        remote: String,
        local: String,
    ) -> anyhow::Result<MappingPreview> {
        let config = self.config.read().await;
        let action = config
            .actions
            .iter()
            .find(|item| item.id == action_id)
            .context("pending Relay action not found")?;
        let translated = map_path(&action.remote_path, &[PathMapping { remote, local }])?;
        Ok(MappingPreview {
            translated_path: translated.to_string_lossy().into_owned(),
            exists: translated.exists(),
        })
    }

    async fn retry_pending_actions(&self, instance_id: Uuid) -> anyhow::Result<()> {
        let ids = {
            let config = self.config.read().await;
            config
                .actions
                .iter()
                .filter(|item| {
                    item.instance_id == instance_id
                        && matches!(item.state, ActionRecordState::PendingMapping)
                })
                .map(|item| item.id)
                .collect::<Vec<_>>()
        };
        for id in ids {
            let _ = self.execute_recorded_action(id).await;
        }
        Ok(())
    }

    async fn execute_recorded_action(&self, action_id: Uuid) -> anyhow::Result<()> {
        let (action, path, command) = {
            let mut config = self.config.write().await;
            let index = config
                .actions
                .iter()
                .position(|item| item.id == action_id)
                .context("Relay action not found")?;
            let record = config.actions[index].clone();
            let instance = config
                .instances
                .iter()
                .find(|item| item.id == record.instance_id)
                .context("Relay instance not found")?;
            let path = map_path(&record.remote_path, &instance.mappings)?;
            if !path.exists() {
                bail!("mapped path is unavailable");
            }
            config.actions[index].state = ActionRecordState::Executing;
            let command = command_for(&config.commands, record.action);
            // A persist failure here must not strand the record in `Executing`,
            // which the TTL never prunes: roll it back to a bounded `Failed`
            // before propagating the error.
            if let Err(error) = self.persist_actions(&config) {
                config.actions[index].state = ActionRecordState::Failed {
                    code: "command_failed".into(),
                    message: GENERIC_ACTION_FAILURE.into(),
                };
                return Err(error);
            }
            (record.action, path, command)
        };
        let result = self.run_handler(action, path, command);
        let mut config = self.config.write().await;
        if let Some(record) = config.actions.iter_mut().find(|item| item.id == action_id) {
            record.state = match &result {
                Ok(()) => ActionRecordState::Complete,
                Err(error) => {
                    // The detailed error stays in the local log; the origin and
                    // the persisted record get only the generic message.
                    tracing::warn!(%error, %action_id, "Relay action failed");
                    ActionRecordState::Failed {
                        code: "command_failed".into(),
                        message: GENERIC_ACTION_FAILURE.into(),
                    }
                }
            };
            self.persist_actions(&config)?;
        }
        result
    }

    /// Records the outcome of a handler invocation on an `Executing` record
    /// and answers the browser with the record's new state. Shared by the
    /// immediate action path and the upload path so both report failures
    /// identically.
    async fn complete_action(
        &self,
        action_id: Uuid,
        result: anyhow::Result<()>,
        origin: &str,
    ) -> Response {
        let mut config = self.config.write().await;
        if let Some(record) = config.actions.iter_mut().find(|item| item.id == action_id) {
            record.state = match &result {
                Ok(()) => ActionRecordState::Complete,
                Err(error) => {
                    // The detailed error — which may embed this host's local
                    // path — is logged locally only; the persisted record and
                    // the polled status endpoint see only the generic message.
                    tracing::warn!(%error, %action_id, "Relay action failed");
                    ActionRecordState::Failed {
                        code: "command_failed".into(),
                        message: GENERIC_ACTION_FAILURE.into(),
                    }
                }
            };
        }
        if let Err(error) = self.persist_actions(&config) {
            tracing::warn!(%error, "failed to persist the Relay action outcome");
        }
        drop(config);
        match result {
            Ok(()) => with_cors(StatusCode::NO_CONTENT.into_response(), origin),
            Err(error) => {
                tracing::warn!(%error, "Relay action failed");
                structured_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "command_failed",
                    "local action failed",
                    Some(origin),
                    serde_json::json!({"action_id": action_id}),
                )
            }
        }
    }
}

fn command_for(commands: &FileActionCommands, action: RelayAction) -> CommandSpec {
    match action {
        RelayAction::OpenFile => commands.open_file.clone(),
        RelayAction::RevealInFolder => commands.reveal_in_folder.clone(),
        RelayAction::CopyToClipboard => commands.copy_to_clipboard.clone(),
    }
}

#[derive(Debug, Serialize)]
pub struct MappingPreview {
    pub translated_path: String,
    pub exists: bool,
}

pub struct RelayHandle {
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl RelayHandle {
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = self.task.await;
    }
}

pub async fn start(state: Arc<RelayState>) -> anyhow::Result<RelayHandle> {
    let bind = state
        .config
        .read()
        .await
        .bind
        .parse::<std::net::SocketAddr>()
        .context("invalid Relay bind address")?;
    if !bind.ip().is_loopback() {
        bail!("Relay must bind to a loopback address, not {bind}");
    }
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("failed to bind Relay on {bind}"))?;
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router(state))
            .with_graceful_shutdown(async {
                let _ = rx.await;
            })
            .await
        {
            tracing::error!(%error, "Relay listener failed");
        }
    });
    Ok(RelayHandle {
        shutdown: Some(tx),
        task,
    })
}

pub fn router(state: Arc<RelayState>) -> Router {
    Router::new()
        .route("/v1/health", get(health))
        .route(
            "/v1/pairing/request",
            post(request_pairing).options(pairing_options),
        )
        .route(
            "/v1/pairing/{id}",
            get(pairing_status)
                .delete(cancel_pairing)
                .options(pairing_options),
        )
        .route(
            "/v1/pairing/{id}/ack",
            post(ack_pairing).options(pairing_options),
        )
        .route("/v1/auth/check", post(auth_check).options(auth_options))
        .route("/v1/actions", post(action).options(action_options))
        .route(
            "/v1/actions/{id}",
            get(action_status).options(action_options),
        )
        // Uploads are streamed to disk and bounded by the action's declared
        // size, so axum's 2 MiB default body limit — which the JSON routes
        // keep — must not apply here. Raw `Body` extraction already bypasses
        // it; disabling it explicitly keeps that independent of axum's
        // internals.
        .route(
            "/v1/files/{id}",
            post(upload_file)
                .options(action_options)
                .layer(DefaultBodyLimit::disable()),
        )
        .with_state(state)
}

async fn health(State(state): State<Arc<RelayState>>, headers: HeaderMap) -> Response {
    let relay_id = state.config.read().await.relay_id;
    let response = Json(Health {
        protocol: "panoptikon-relay-v1",
        version: env!("CARGO_PKG_VERSION"),
        pairing: true,
        relay_id,
        features: RELAY_FEATURES,
    })
    .into_response();
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Url::parse(value).ok().map(|url| (value, url)))
        .filter(|(value, url)| *value == serialized_origin(url));
    if let Some((origin, _)) = origin {
        with_cors(response, origin)
    } else {
        response
    }
}

async fn pairing_options(headers: HeaderMap) -> Response {
    let origin = match validated_origin(&headers, None) {
        Ok(origin) => origin,
        Err(response) => return response,
    };
    preflight(&origin, "GET, POST, DELETE, OPTIONS")
}

// Credential validation is also how a browser discovers that its pairing was
// revoked. Its preflight must remain reachable after the paired instance has
// been removed; the POST itself still requires and verifies the credential.
async fn auth_options(headers: HeaderMap) -> Response {
    let origin = match validated_origin(&headers, None) {
        Ok(origin) => origin,
        Err(response) => return response,
    };
    preflight(&origin, "POST, OPTIONS")
}

async fn action_options(State(state): State<Arc<RelayState>>, headers: HeaderMap) -> Response {
    let origin = match validated_origin(&headers, None) {
        Ok(origin) => origin,
        Err(response) => return response,
    };
    let allowed = state
        .config
        .read()
        .await
        .instances
        .iter()
        .any(|instance| instance.origins.iter().any(|item| item == &origin));
    if !allowed {
        return error(StatusCode::FORBIDDEN, "origin is not paired", Some(&origin));
    }
    preflight(&origin, "GET, POST, OPTIONS")
}

async fn request_pairing(
    State(state): State<Arc<RelayState>>,
    headers: HeaderMap,
    Json(request): Json<PairingRequest>,
) -> Response {
    let origin = match validated_origin(&headers, Some(&request.origin)) {
        Ok(origin) => origin,
        Err(response) => return response,
    };
    let server_url = match Url::parse(&request.server_url) {
        Ok(url) => url,
        Err(_) => return error(StatusCode::BAD_REQUEST, "invalid server URL", Some(&origin)),
    };
    if serialized_origin(&server_url) != origin {
        return error(
            StatusCode::BAD_REQUEST,
            "server URL does not match the requesting origin",
            Some(&origin),
        );
    }
    if request.name.trim().is_empty() || request.name.len() > 80 {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid instance name",
            Some(&origin),
        );
    }
    if request.roots.len() > 128 || request.roots.iter().any(|root| root.len() > 4096) {
        return error(StatusCode::BAD_REQUEST, "invalid root hints", Some(&origin));
    }

    // Retries of the same durable operation are reads, not new pairing
    // attempts. Check before rate limiting so a lost response can always be
    // recovered without eventually throttling its own idempotent retries.
    {
        let mut config = state.config.write().await;
        if prune_config(&mut config) {
            if let Err(save_error) = save_config(&state.config_path, &config) {
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("failed to garbage collect pairing requests: {save_error}"),
                    Some(&origin),
                );
            }
        }
        if let Some(existing) = config
            .pairing_operations
            .iter()
            .find(|item| item.id == request.operation_id)
        {
            if existing.origin != origin || existing.server_url != server_url.to_string() {
                return error(
                    StatusCode::CONFLICT,
                    "pairing operation conflicts with an existing request",
                    Some(&origin),
                );
            }
            if matches!(existing.state, PairingOperationState::Pending) {
                (state.pairing_attention_handler)();
            }
            return with_cors(
                (
                    StatusCode::ACCEPTED,
                    Json(serde_json::json!({
                        "operation_id": existing.id,
                        "expires_in_secs": PAIRING_TTL.as_secs()
                    })),
                )
                    .into_response(),
                &origin,
            );
        }
    }

    let now = Instant::now();
    {
        let mut attempts = state.attempts.lock().await;
        let values = attempts.entry(origin.clone()).or_default();
        while values
            .front()
            .is_some_and(|at| now.duration_since(*at) > RATE_WINDOW)
        {
            values.pop_front();
        }
        if values.len() >= RATE_LIMIT {
            return error(
                StatusCode::TOO_MANY_REQUESTS,
                "pairing requests are rate limited",
                Some(&origin),
            );
        }
        values.push_back(now);
    }
    let mut config = state.config.write().await;
    prune_config(&mut config);
    if let Some(existing) = config
        .pairing_operations
        .iter()
        .find(|item| item.id == request.operation_id)
    {
        if existing.origin != origin || existing.server_url != server_url.to_string() {
            return error(
                StatusCode::CONFLICT,
                "pairing operation conflicts with an existing request",
                Some(&origin),
            );
        }
        if matches!(existing.state, PairingOperationState::Pending) {
            (state.pairing_attention_handler)();
        }
        return with_cors(
            (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({
                    "operation_id": existing.id,
                    "expires_in_secs": PAIRING_TTL.as_secs()
                })),
            )
                .into_response(),
            &origin,
        );
    }
    let pending_count = config
        .pairing_operations
        .iter()
        .filter(|item| matches!(item.state, PairingOperationState::Pending))
        .count();
    if pending_count >= MAX_PENDING {
        return error(
            StatusCode::TOO_MANY_REQUESTS,
            "too many pending pairing requests",
            Some(&origin),
        );
    }
    config.pairing_operations.push(PairingOperation {
        id: request.operation_id,
        name: request.name.trim().to_owned(),
        origin: origin.clone(),
        server_url: server_url.to_string(),
        roots: request
            .roots
            .into_iter()
            .filter(|root| !root.trim().is_empty())
            .collect(),
        created_unix: unix_now(),
        state: PairingOperationState::Pending,
    });
    if let Err(save_error) = save_config(&state.config_path, &config) {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("failed to save pairing request: {save_error}"),
            Some(&origin),
        );
    }
    drop(config);
    (state.pairing_attention_handler)();
    with_cors(
        (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({
                "operation_id": request.operation_id,
                "expires_in_secs": PAIRING_TTL.as_secs(),
            })),
        )
            .into_response(),
        &origin,
    )
}

async fn pairing_status(
    State(state): State<Arc<RelayState>>,
    AxumPath(id): AxumPath<Uuid>,
    headers: HeaderMap,
) -> Response {
    let origin = match validated_origin(&headers, None) {
        Ok(origin) => origin,
        Err(response) => return response,
    };
    let mut config = state.config.write().await;
    if prune_config(&mut config) {
        let _ = save_config(&state.config_path, &config);
    }
    let Some(item) = config.pairing_operations.iter().find(|item| item.id == id) else {
        return error(
            StatusCode::NOT_FOUND,
            "pairing request not found",
            Some(&origin),
        );
    };
    if item.origin != origin {
        return error(
            StatusCode::FORBIDDEN,
            "origin is not authorized for this pairing request",
            Some(&origin),
        );
    }
    let status = match &item.state {
        PairingOperationState::Pending => serde_json::json!({"status":"pending"}),
        PairingOperationState::Rejected => serde_json::json!({"status":"rejected"}),
        PairingOperationState::ApprovedUnconfirmed {
            instance_id,
            credential,
        } => {
            serde_json::json!({"status":"approved_unconfirmed", "instance_id":instance_id, "credential":credential})
        }
        PairingOperationState::Complete { instance_id, .. } => {
            serde_json::json!({"status":"complete", "instance_id":instance_id})
        }
    };
    with_cors(Json(status).into_response(), &origin)
}

async fn ack_pairing(
    State(state): State<Arc<RelayState>>,
    AxumPath(id): AxumPath<Uuid>,
    headers: HeaderMap,
) -> Response {
    let origin = match validated_origin(&headers, None) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let credential = match bearer_credential(&headers) {
        Some(value) => value,
        None => {
            return structured_error(
                StatusCode::UNAUTHORIZED,
                "invalid_credential",
                "Relay credential is required",
                Some(&origin),
                serde_json::json!({}),
            );
        }
    };
    let mut config = state.config.write().await;
    let Some(index) = config
        .pairing_operations
        .iter()
        .position(|item| item.id == id && item.origin == origin)
    else {
        return error(
            StatusCode::NOT_FOUND,
            "pairing operation not found",
            Some(&origin),
        );
    };
    let instance_id = match config.pairing_operations[index].state.clone() {
        PairingOperationState::ApprovedUnconfirmed { instance_id, .. }
        | PairingOperationState::Complete { instance_id, .. } => instance_id,
        _ => {
            return error(
                StatusCode::CONFLICT,
                "pairing operation is not approved",
                Some(&origin),
            );
        }
    };
    let valid = config
        .instances
        .iter()
        .find(|item| item.id == instance_id)
        .is_some_and(|item| verify_credential(&item.credential_hash, credential));
    if !valid {
        return structured_error(
            StatusCode::UNAUTHORIZED,
            "invalid_credential",
            "Relay credential is invalid",
            Some(&origin),
            serde_json::json!({}),
        );
    }
    config.pairing_operations[index].state = PairingOperationState::Complete {
        instance_id,
        completed_unix: unix_now(),
    };
    if let Err(error_value) = save_config(&state.config_path, &config) {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &error_value.to_string(),
            Some(&origin),
        );
    }
    with_cors(StatusCode::NO_CONTENT.into_response(), &origin)
}

async fn cancel_pairing(
    State(state): State<Arc<RelayState>>,
    AxumPath(id): AxumPath<Uuid>,
    headers: HeaderMap,
) -> Response {
    let origin = match validated_origin(&headers, None) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let mut config = state.config.write().await;
    if let Some(operation) = config
        .pairing_operations
        .iter()
        .find(|item| item.id == id && item.origin == origin)
    {
        if let PairingOperationState::ApprovedUnconfirmed { instance_id, .. } = operation.state {
            config.instances.retain(|item| item.id != instance_id);
        }
    }
    config
        .pairing_operations
        .retain(|item| !(item.id == id && item.origin == origin));
    if let Err(error_value) = save_config(&state.config_path, &config) {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &error_value.to_string(),
            Some(&origin),
        );
    }
    with_cors(StatusCode::NO_CONTENT.into_response(), &origin)
}

async fn auth_check(State(state): State<Arc<RelayState>>, headers: HeaderMap) -> Response {
    let origin = match validated_origin(&headers, None) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let credential = match bearer_credential(&headers) {
        Some(value) => value,
        None => {
            return structured_error(
                StatusCode::UNAUTHORIZED,
                "invalid_credential",
                "Relay credential is required",
                Some(&origin),
                serde_json::json!({}),
            );
        }
    };
    // Argon2 verification is deliberately expensive. Never hold the Relay
    // configuration lock while doing it: local revocation and mapping edits
    // must remain immediately responsive while a browser validates its saved
    // credential.
    let credential_candidates = {
        let config = state.config.read().await;
        config
            .instances
            .iter()
            .filter(|item| item.origins.iter().any(|allowed| allowed == &origin))
            .map(|item| (item.id, item.credential_hash.clone()))
            .collect::<Vec<_>>()
    };
    let verified = credential_candidates
        .iter()
        .find(|(_, hash)| verify_credential(hash, credential));
    // Revocation may complete while Argon2 runs. Re-check the verified
    // instance under a short read lock so a pre-revocation snapshot cannot
    // authenticate after the revoke command has returned.
    let valid = if let Some((instance_id, credential_hash)) = verified {
        state.config.read().await.instances.iter().any(|item| {
            item.id == *instance_id
                && item.credential_hash == credential_hash.as_str()
                && item.origins.iter().any(|allowed| allowed == &origin)
        })
    } else {
        false
    };
    if valid {
        with_cors(StatusCode::NO_CONTENT.into_response(), &origin)
    } else {
        structured_error(
            StatusCode::UNAUTHORIZED,
            "invalid_credential",
            "Relay credential is invalid or revoked",
            Some(&origin),
            serde_json::json!({}),
        )
    }
}

async fn action(
    State(state): State<Arc<RelayState>>,
    headers: HeaderMap,
    Json(request): Json<ActionRequest>,
) -> Response {
    let origin = match validated_origin(&headers, None) {
        Ok(origin) => origin,
        Err(response) => return response,
    };
    if request.path.is_empty() || request.path.len() > 32 * 1024 {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid server path",
            Some(&origin),
        );
    }
    let share = if request.action.is_share_verb() {
        match share_metadata(&request) {
            Ok(share) => Some(share),
            Err(message) => {
                return structured_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_share_metadata",
                    message,
                    Some(&origin),
                    serde_json::json!({"action_id": request.action_id}),
                );
            }
        }
    } else {
        None
    };
    let credential = match bearer_credential(&headers) {
        Some(value) => value,
        _ => {
            return structured_error(
                StatusCode::UNAUTHORIZED,
                "invalid_credential",
                "Relay credential is required",
                Some(&origin),
                serde_json::json!({}),
            );
        }
    };
    let mut config = state.config.write().await;
    prune_config(&mut config);
    let instance = config.instances.iter().find(|item| {
        item.origins.iter().any(|allowed| allowed == &origin)
            && verify_credential(&item.credential_hash, credential)
    });
    let Some(instance) = instance else {
        return structured_error(
            StatusCode::UNAUTHORIZED,
            "invalid_credential",
            "Relay credential is invalid or revoked",
            Some(&origin),
            serde_json::json!({}),
        );
    };
    let instance_id = instance.id;
    let mappings = instance.mappings.clone();
    if let Some(existing) = config
        .actions
        .iter()
        .find(|item| item.id == request.action_id)
    {
        if existing.instance_id != instance_id
            || existing.action != request.action
            || existing.remote_path != request.path
        {
            return error(
                StatusCode::CONFLICT,
                "action ID conflicts with an existing action",
                Some(&origin),
            );
        }
        return action_record_response(existing, &origin);
    }
    if config.actions.len() >= MAX_ACTION_RECORDS && !evict_oldest_action(&mut config, &state) {
        // Only reachable when every retained record is executing or has an
        // upload streaming into it — i.e. a thousand simultaneous in-flight
        // actions. Refusing is correct there; refusing merely because a
        // thousand *finished* records accumulated would not be.
        return error(
            StatusCode::TOO_MANY_REQUESTS,
            "too many Relay actions are in flight",
            Some(&origin),
        );
    }
    // Share verbs move bytes, not locations: a resolved mapping is a silent
    // optimization and a miss materializes a cache copy. They never create a
    // `PendingMapping` record and never foreground the mapping window.
    if let Some(share) = share {
        let command = command_for(&config.commands, request.action);
        let max_bytes = config.share_cache_max_bytes;
        let local = map_path(&request.path, &mappings)
            .ok()
            .filter(|path| path.exists())
            .or_else(|| {
                state
                    .share_cache
                    .lookup(&share.sha256, &share.filename, share.size)
            });
        let Some(path) = local else {
            // Admission control, decided before a record exists: a file that
            // could never fit the cache must not park in `PendingBytes` and
            // then fail after the browser has uploaded gigabytes. The client
            // falls back to a plain download on this code. A resolved mapping
            // or an existing cache entry is exempt — the ceiling governs what
            // the Relay stores, and those paths store nothing.
            if share.size > max_bytes {
                return structured_error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "file_too_large",
                    "the file is larger than the Relay's share cache limit",
                    Some(&origin),
                    serde_json::json!({"action_id": request.action_id, "size": share.size, "max": max_bytes}),
                );
            }
            config.actions.push(ActionRecord {
                id: request.action_id,
                instance_id,
                action: request.action,
                remote_path: request.path.clone(),
                created_unix: unix_now(),
                state: ActionRecordState::PendingBytes {
                    sha256: share.sha256.clone(),
                    filename: share.filename.clone(),
                    size: share.size,
                },
            });
            if let Err(error_value) = state.persist_actions(&config) {
                tracing::warn!(%error_value, "failed to persist a parked Relay share action");
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "the Relay could not record this action",
                    Some(&origin),
                );
            }
            return bytes_required(request.action_id, &share.sha256, &share.filename, &origin);
        };
        config.actions.push(ActionRecord {
            id: request.action_id,
            instance_id,
            action: request.action,
            remote_path: request.path,
            created_unix: unix_now(),
            state: ActionRecordState::Executing,
        });
        if let Err(error_value) = state.persist_actions(&config) {
            tracing::warn!(%error_value, "failed to persist a Relay share action");
            // Drop the record just pushed: leaving it `Executing` would pin it
            // forever under the state the TTL never prunes.
            config.actions.retain(|item| item.id != request.action_id);
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "the Relay could not record this action",
                Some(&origin),
            );
        }
        drop(config);
        // A cache hit is about to become a clipboard entry; protect it from
        // eviction for as long as the ring holds it.
        if path.starts_with(state.share_cache.root()) {
            state.hold_cache_path(&path);
        }
        tracing::info!(%instance_id, action = ?request.action, "Relay share action authorized");
        let result = state.run_handler(request.action, path, command);
        return state
            .complete_action(request.action_id, result, &origin)
            .await;
    }
    let mapped = match map_path(&request.path, &mappings) {
        Ok(path) => path,
        Err(_) => {
            config.actions.push(ActionRecord {
                id: request.action_id,
                instance_id,
                action: request.action,
                remote_path: request.path.clone(),
                created_unix: unix_now(),
                state: ActionRecordState::PendingMapping,
            });
            if let Err(error_value) = state.persist_actions(&config) {
                tracing::warn!(%error_value, "failed to persist a parked Relay action");
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "the Relay could not record this action",
                    Some(&origin),
                );
            }
            drop(config);
            (state.mapping_attention_handler)();
            return structured_error(
                StatusCode::CONFLICT,
                "mapping_required",
                "Choose the local folder corresponding to this server path",
                Some(&origin),
                serde_json::json!({"path": request.path, "instance_id": instance_id, "action_id": request.action_id}),
            );
        }
    };
    if !mapped.exists() {
        return structured_error(
            StatusCode::NOT_FOUND,
            "mapped_path_unavailable",
            "mapped path is unavailable",
            Some(&origin),
            serde_json::json!({"path":request.path}),
        );
    }
    let command = command_for(&config.commands, request.action);
    config.actions.push(ActionRecord {
        id: request.action_id,
        instance_id,
        action: request.action,
        remote_path: request.path,
        created_unix: unix_now(),
        state: ActionRecordState::Executing,
    });
    if let Err(error_value) = state.persist_actions(&config) {
        tracing::warn!(%error_value, "failed to persist a Relay action");
        // Drop the record just pushed: leaving it `Executing` would pin it
        // forever under the state the TTL never prunes.
        config.actions.retain(|item| item.id != request.action_id);
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "the Relay could not record this action",
            Some(&origin),
        );
    }
    drop(config);
    tracing::info!(%instance_id, action = ?request.action, "Relay action authorized");
    let result = state.run_handler(request.action, mapped, command);
    state
        .complete_action(request.action_id, result, &origin)
        .await
}

/// Makes room for one more action record by dropping the oldest one that is
/// neither running a local command nor receiving an upload. Returns `false`
/// only when every retained record is one of those.
fn evict_oldest_action(config: &mut RelayConfig, state: &RelayState) -> bool {
    let in_flight = state.in_flight_uploads();
    let oldest = config
        .actions
        .iter()
        .enumerate()
        .filter(|(_, item)| {
            !matches!(item.state, ActionRecordState::Executing) && !in_flight.contains(&item.id)
        })
        .min_by_key(|(_, item)| item.created_unix)
        .map(|(index, _)| index);
    match oldest {
        Some(index) => {
            config.actions.remove(index);
            true
        }
        None => false,
    }
}

async fn action_status(
    State(state): State<Arc<RelayState>>,
    AxumPath(id): AxumPath<Uuid>,
    headers: HeaderMap,
) -> Response {
    let origin = match validated_origin(&headers, None) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let credential = match bearer_credential(&headers) {
        Some(value) => value,
        None => {
            return structured_error(
                StatusCode::UNAUTHORIZED,
                "invalid_credential",
                "Relay credential is required",
                Some(&origin),
                serde_json::json!({}),
            );
        }
    };
    let config = state.config.read().await;
    let Some(record) = config.actions.iter().find(|item| item.id == id) else {
        return error(
            StatusCode::NOT_FOUND,
            "Relay action not found",
            Some(&origin),
        );
    };
    let valid = config
        .instances
        .iter()
        .find(|item| item.id == record.instance_id)
        .is_some_and(|item| {
            item.origins.iter().any(|allowed| allowed == &origin)
                && verify_credential(&item.credential_hash, credential)
        });
    if !valid {
        return structured_error(
            StatusCode::UNAUTHORIZED,
            "invalid_credential",
            "Relay credential is invalid or revoked",
            Some(&origin),
            serde_json::json!({}),
        );
    }
    action_record_response(record, &origin)
}

enum UploadFailure {
    /// More bytes arrived than the action declared, plus slack.
    TooLarge,
    /// The browser's request body ended early or errored.
    Stream(String),
    /// The relay could not write its own cache. Carries the real error for
    /// the local log only — the origin is told nothing about this host's
    /// filesystem.
    Io(std::io::Error),
}

/// What arrived on the wire: the byte count and the hash of those exact bytes.
struct UploadDigest {
    written: u64,
    sha256: String,
}

/// Streams a request body to `temp`, refusing to write more than `ceiling` and
/// hashing every byte on the way past.
///
/// The file handle is always dropped before returning so the caller can
/// delete the temporary file on Windows, where an open handle blocks removal.
async fn stream_to_temp(
    mut body: Body,
    temp: &Path,
    ceiling: u64,
) -> Result<UploadDigest, UploadFailure> {
    // `create_new` rather than `create`: two writers must never share one
    // temporary file. The in-flight claim set makes that unreachable, and this
    // turns "unreachable" into "impossible".
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temp)
        .await
        .map_err(UploadFailure::Io)?;
    let mut written: u64 = 0;
    let mut hasher = Sha256::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| UploadFailure::Stream(error.to_string()))?;
        let Ok(chunk) = frame.into_data() else {
            continue;
        };
        written = written.saturating_add(chunk.len() as u64);
        if written > ceiling {
            return Err(UploadFailure::TooLarge);
        }
        hasher.update(&chunk);
        file.write_all(&chunk).await.map_err(UploadFailure::Io)?;
    }
    file.sync_all().await.map_err(UploadFailure::Io)?;
    Ok(UploadDigest {
        written,
        sha256: hex_digest(hasher.finalize().as_slice()),
    })
}

fn hex_digest(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// `POST /v1/files/{id}` — the browser-push leg of a share verb, reached only
/// after the action parked in `PendingBytes`.
///
/// Every failure leaves the record in `PendingBytes` and removes the partial
/// temporary file, so the browser can simply retry the upload.
async fn upload_file(
    State(state): State<Arc<RelayState>>,
    AxumPath(raw_id): AxumPath<String>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let origin = match validated_origin(&headers, None) {
        Ok(value) => value,
        Err(response) => return response,
    };
    // The id is extracted as a string and parsed here rather than through
    // `Path<Uuid>`: axum's own rejection is a bare 400 with no CORS header,
    // which a browser reports as an opaque network failure instead of a
    // readable error.
    let Ok(id) = Uuid::parse_str(&raw_id) else {
        return structured_error(
            StatusCode::BAD_REQUEST,
            "invalid_action_id",
            "the action id is not a UUID",
            Some(&origin),
            serde_json::json!({}),
        );
    };
    let credential = match bearer_credential(&headers) {
        Some(value) => value,
        None => {
            return structured_error(
                StatusCode::UNAUTHORIZED,
                "invalid_credential",
                "Relay credential is required",
                Some(&origin),
                serde_json::json!({}),
            );
        }
    };
    // Argon2 verification is deliberately expensive; the configuration lock is
    // released before it runs so that a revoke or a mapping edit is never
    // blocked behind an upload's authentication.
    let credential_hash = {
        let config = state.config.read().await;
        let Some(record) = config.actions.iter().find(|item| item.id == id) else {
            return error(
                StatusCode::NOT_FOUND,
                "Relay action not found",
                Some(&origin),
            );
        };
        config
            .instances
            .iter()
            .find(|item| {
                item.id == record.instance_id && item.origins.iter().any(|value| value == &origin)
            })
            .map(|item| item.credential_hash.clone())
    };
    let authorized = credential_hash
        .as_deref()
        .is_some_and(|hash| verify_credential(hash, credential));
    if !authorized {
        return structured_error(
            StatusCode::UNAUTHORIZED,
            "invalid_credential",
            "Relay credential is invalid or revoked",
            Some(&origin),
            serde_json::json!({}),
        );
    }
    let (sha256, filename, size, max_bytes) = {
        let config = state.config.read().await;
        let Some(record) = config.actions.iter().find(|item| item.id == id) else {
            return error(
                StatusCode::NOT_FOUND,
                "Relay action not found",
                Some(&origin),
            );
        };
        match &record.state {
            ActionRecordState::PendingBytes {
                sha256,
                filename,
                size,
            } => (
                sha256.clone(),
                filename.clone(),
                *size,
                config.share_cache_max_bytes,
            ),
            // Any other state answers with the record's own status, which is
            // also the idempotent answer to a retried upload.
            _ => return action_record_response(record, &origin),
        }
    };
    // Claimed after the state check so a retry against a finished action still
    // gets its idempotent answer rather than a spurious conflict. The guard
    // releases the claim on every exit path below.
    let Some(_claim) = state.claim_upload(id) else {
        return structured_error(
            StatusCode::CONFLICT,
            "upload_in_progress",
            "another upload for this action is already in progress",
            Some(&origin),
            serde_json::json!({"action_id": id}),
        );
    };

    let temp = state.share_cache.new_temp_path(id);
    if let Err(error_value) = tokio::fs::create_dir_all(state.share_cache.root()).await {
        tracing::warn!(error = %error_value, path = %state.share_cache.root().display(), "failed to create the Relay share cache directory");
        return upload_storage_failed(id, &origin);
    }
    let digest = match stream_to_temp(body, &temp, size.saturating_add(UPLOAD_SIZE_SLACK)).await {
        Ok(digest) => digest,
        Err(failure) => {
            let _ = tokio::fs::remove_file(&temp).await;
            return match failure {
                UploadFailure::TooLarge => structured_error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "upload_too_large",
                    "the upload is larger than the declared file size",
                    Some(&origin),
                    serde_json::json!({"action_id": id, "size": size}),
                ),
                UploadFailure::Stream(message) => structured_error(
                    StatusCode::BAD_REQUEST,
                    "upload_failed",
                    &message,
                    Some(&origin),
                    serde_json::json!({"action_id": id}),
                ),
                UploadFailure::Io(error_value) => {
                    tracing::warn!(error = %error_value, path = %temp.display(), "failed to write a Relay share upload");
                    upload_storage_failed(id, &origin)
                }
            };
        }
    };
    if digest.written != size {
        let _ = tokio::fs::remove_file(&temp).await;
        return structured_error(
            StatusCode::BAD_REQUEST,
            "size_mismatch",
            "the upload did not match the declared file size",
            Some(&origin),
            serde_json::json!({"action_id": id, "size": size, "received": digest.written}),
        );
    }
    // The cache is content-addressed and its entries are handed to local
    // commands by path. Trusting the origin's `sha256` without checking it
    // would let one action poison the entry every later action resolves to.
    if digest.sha256 != sha256 {
        let _ = tokio::fs::remove_file(&temp).await;
        return structured_error(
            StatusCode::BAD_REQUEST,
            "hash_mismatch",
            "the upload did not match the declared file hash",
            Some(&origin),
            serde_json::json!({"action_id": id, "sha256": sha256}),
        );
    }
    let path = match state.share_cache.insert(
        &temp,
        &sha256,
        &filename,
        max_bytes,
        &state.held_cache_paths(),
        &state.in_flight_uploads(),
    ) {
        Ok(path) => path,
        Err(error_value) => {
            let _ = tokio::fs::remove_file(&temp).await;
            tracing::warn!(error = %error_value, "failed to store a Relay share upload in the cache");
            return upload_storage_failed(id, &origin);
        }
    };
    state.hold_cache_path(&path);

    let (action, command) = {
        let mut config = state.config.write().await;
        let Some(record) = config.actions.iter_mut().find(|item| item.id == id) else {
            return error(
                StatusCode::NOT_FOUND,
                "Relay action not found",
                Some(&origin),
            );
        };
        record.state = ActionRecordState::Executing;
        // The long `PendingBytes` TTL ends here: from now on the record ages
        // out like any other finished action.
        record.created_unix = unix_now();
        let action = record.action;
        let command = command_for(&config.commands, action);
        if let Err(error_value) = state.persist_actions(&config) {
            tracing::warn!(%error_value, "failed to persist a Relay upload transition");
            // Roll the record back off `Executing` — the one state the TTL
            // never prunes — to a bounded `Failed` before returning.
            if let Some(record) = config.actions.iter_mut().find(|item| item.id == id) {
                record.state = ActionRecordState::Failed {
                    code: "command_failed".into(),
                    message: GENERIC_ACTION_FAILURE.into(),
                };
            }
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "the Relay could not record this action",
                Some(&origin),
            );
        }
        (action, command)
    };
    tracing::info!(action_id = %id, action = ?action, "Relay share upload complete");
    let result = state.run_handler(action, path, command);
    state.complete_action(id, result, &origin).await
}

/// One opaque answer for every way the Relay's own storage can fail. The real
/// `io::Error` and its path stay in the local log: a remote origin has no
/// business learning this machine's directory layout or disk state.
fn upload_storage_failed(action_id: Uuid, origin: &str) -> Response {
    structured_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "upload_failed",
        "the Relay could not store the upload",
        Some(origin),
        serde_json::json!({"action_id": action_id}),
    )
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn prune_config(config: &mut RelayConfig) -> bool {
    let now = unix_now();
    let old_operations = config.pairing_operations.len();
    config.pairing_operations.retain(|item| match item.state {
        PairingOperationState::Pending | PairingOperationState::Rejected => {
            item.created_unix + PAIRING_TTL.as_secs() as i64 > now
        }
        PairingOperationState::Complete { .. } => true,
        // An approved operation remains recoverable until Server persistence
        // is acknowledged or the user explicitly cancels/replaces it.
        PairingOperationState::ApprovedUnconfirmed { .. } => true,
    });
    let old_actions = config.actions.len();
    // A multi-gigabyte materialization can legitimately outlive the ordinary
    // action TTL, so a record waiting for its bytes gets a longer one of its
    // own rather than no expiry at all — an abandoned browser tab must not
    // pin a record forever. `created_unix` is refreshed when the upload
    // completes, so the finished record ages out normally.
    //
    // `Executing` is the one genuinely unbounded state: the local command's
    // runtime is not ours to predict, and dropping the record mid-command
    // would strand the browser polling it. Its recovery is not a TTL but
    // `recover_interrupted_actions`, which demotes anything still `Executing`
    // at startup.
    config.actions.retain(|item| match &item.state {
        ActionRecordState::Executing => true,
        ActionRecordState::PendingBytes { .. } => item.created_unix + PENDING_BYTES_TTL_SECS > now,
        _ => item.created_unix + ACTION_TTL_SECS > now,
    });
    old_operations != config.pairing_operations.len() || old_actions != config.actions.len()
}

fn suggested_remote_root(path: &str, mappings: &[PathMapping]) -> String {
    let input = match normalize_path(path) {
        Ok(value) => value,
        Err(_) => return path.to_owned(),
    };
    for mapping in mappings {
        if let Ok(remote) = normalize_path(&mapping.remote)
            && remote.windows == input.windows
            && component_eq(&remote.prefix, &input.prefix, input.windows)
            && remote.components.len() <= input.components.len()
            && remote
                .components
                .iter()
                .zip(&input.components)
                .all(|(a, b)| component_eq(a, b, input.windows))
        {
            return mapping.remote.clone();
        }
    }
    let mut parent = PathBuf::from(path.replace('\\', "/"));
    parent.pop();
    let value = parent.to_string_lossy().replace('\\', "/");
    if value.is_empty() {
        path.to_owned()
    } else {
        value
    }
}

fn bearer_credential(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| !value.is_empty())
}

/// The share-verb counterpart of `mapping_required`: the relay has no local
/// copy of these bytes and the browser must push them to
/// `POST /v1/files/{action_id}`.
fn bytes_required(action_id: Uuid, sha256: &str, filename: &str, origin: &str) -> Response {
    structured_error(
        StatusCode::CONFLICT,
        "bytes_required",
        "Upload the file to complete this action",
        Some(origin),
        serde_json::json!({"action_id": action_id, "sha256": sha256, "filename": filename}),
    )
}

fn action_record_response(record: &ActionRecord, origin: &str) -> Response {
    match &record.state {
        ActionRecordState::PendingMapping => with_cors((StatusCode::CONFLICT, Json(serde_json::json!({
            "error": { "code": "mapping_required", "message": "Choose the local folder corresponding to this server path", "details": { "path": record.remote_path, "instance_id": record.instance_id, "action_id": record.id } }
        }))).into_response(), origin),
        ActionRecordState::PendingBytes { sha256, filename, .. } => bytes_required(record.id, sha256, filename, origin),
        ActionRecordState::Executing => with_cors(
            (StatusCode::ACCEPTED, Json(serde_json::json!({"status":"executing"}))).into_response(),
            origin,
        ),
        ActionRecordState::Complete => with_cors(StatusCode::NO_CONTENT.into_response(), origin),
        ActionRecordState::Failed { code, message } => structured_error(StatusCode::INTERNAL_SERVER_ERROR, code, message, Some(origin), serde_json::json!({"action_id":record.id})),
    }
}

fn validated_origin(headers: &HeaderMap, body_origin: Option<&str>) -> Result<String, Response> {
    let header_origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "Origin header is required", None))?;
    let parsed = Url::parse(header_origin)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid Origin header", None))?;
    let origin = serialized_origin(&parsed);
    if origin != header_origin || body_origin.is_some_and(|value| value != origin) {
        return Err(error(StatusCode::BAD_REQUEST, "origin mismatch", None));
    }
    Ok(origin)
}

fn serialized_origin(url: &Url) -> String {
    let mut value = format!("{}://{}", url.scheme(), url.host_str().unwrap_or_default());
    if let Some(port) = url.port() {
        value.push_str(&format!(":{port}"));
    }
    value
}

fn with_cors(mut response: Response, origin: &str) -> Response {
    if let Ok(value) = HeaderValue::from_str(origin) {
        response
            .headers_mut()
            .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, value);
    }
    response
        .headers_mut()
        .insert(header::VARY, HeaderValue::from_static("Origin"));
    response
}

fn preflight(origin: &str, methods: &'static str) -> Response {
    let mut response = with_cors(StatusCode::NO_CONTENT.into_response(), origin);
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static(methods),
    );
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("Authorization, Content-Type"),
    );
    response
}

fn error(status: StatusCode, message: &str, origin: Option<&str>) -> Response {
    let response = (status, Json(serde_json::json!({"error": message}))).into_response();
    if let Some(origin) = origin {
        with_cors(response, origin)
    } else {
        response
    }
}

fn structured_error(
    status: StatusCode,
    code: &str,
    message: &str,
    origin: Option<&str>,
    details: serde_json::Value,
) -> Response {
    let response = (
        status,
        Json(serde_json::json!({
            "error": { "code": code, "message": message, "details": details }
        })),
    )
        .into_response();
    if let Some(origin) = origin {
        with_cors(response, origin)
    } else {
        response
    }
}

fn verify_credential(hash: &str, credential: &str) -> bool {
    PasswordHash::new(hash).ok().is_some_and(|parsed| {
        Argon2::default()
            .verify_password(credential.as_bytes(), &parsed)
            .is_ok()
    })
}

/// Writes `relay.toml`. Action records are excluded by
/// `#[serde(skip_serializing)]` and belong to [`save_actions`].
fn save_config(path: &Path, config: &RelayConfig) -> anyhow::Result<()> {
    atomic_write(path, toml::to_string_pretty(config)?.as_bytes())
}

/// Writes the action sidecar. Same atomic, owner-private write as
/// `relay.toml`, and the same directory.
fn save_actions(path: &Path, actions: &[ActionRecord]) -> anyhow::Result<()> {
    let file = ActionsFile {
        actions: actions.to_vec(),
    };
    atomic_write(path, toml::to_string_pretty(&file)?.as_bytes())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedPath {
    prefix: String,
    components: Vec<String>,
    windows: bool,
}

fn normalize_path(input: &str) -> anyhow::Result<NormalizedPath> {
    if input.trim().is_empty() || input.contains('\0') {
        bail!("invalid empty path");
    }
    let value = input.replace('\\', "/");
    let (prefix, rest, windows) = if value.starts_with("//") {
        let mut parts = value[2..].split('/').filter(|part| !part.is_empty());
        let server = parts.next().context("UNC path has no server")?;
        let share = parts.next().context("UNC path has no share")?;
        (
            format!("//{server}/{share}"),
            parts.collect::<Vec<_>>().join("/"),
            true,
        )
    } else if value.as_bytes().get(1) == Some(&b':')
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphabetic)
    {
        (
            value[..2].to_ascii_uppercase(),
            value[2..].trim_start_matches('/').to_owned(),
            true,
        )
    } else if value.starts_with('/') {
        ("/".into(), value[1..].to_owned(), false)
    } else {
        (String::new(), value, cfg!(windows))
    };
    let mut components = Vec::new();
    for component in rest
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
    {
        if component == ".." {
            if components.pop().is_none() {
                bail!("path traversal above root");
            }
        } else {
            components.push(component.to_owned());
        }
    }
    Ok(NormalizedPath {
        prefix,
        components,
        windows,
    })
}

fn component_eq(a: &str, b: &str, windows: bool) -> bool {
    if windows {
        a.eq_ignore_ascii_case(b)
    } else {
        a == b
    }
}

pub fn map_path(remote_path: &str, mappings: &[PathMapping]) -> anyhow::Result<PathBuf> {
    let input = normalize_path(remote_path)?;
    let mut selected: Option<(&PathMapping, NormalizedPath)> = None;
    for mapping in mappings {
        let remote = normalize_path(&mapping.remote)?;
        if remote.windows != input.windows
            || !component_eq(&remote.prefix, &input.prefix, input.windows)
            || remote.components.len() > input.components.len()
        {
            continue;
        }
        if remote
            .components
            .iter()
            .zip(&input.components)
            .all(|(a, b)| component_eq(a, b, input.windows))
            && selected
                .as_ref()
                .is_none_or(|(_, old)| remote.components.len() > old.components.len())
        {
            selected = Some((mapping, remote));
        }
    }
    let (mapping, remote) = selected.context("no Relay mapping covers the path")?;
    let local = normalize_path(&mapping.local)?;
    let mut output = if local.prefix == "/" {
        PathBuf::from("/")
    } else if local.prefix.is_empty() {
        PathBuf::new()
    } else if local.prefix.len() == 2 && local.prefix.ends_with(':') {
        PathBuf::from(format!("{}/", local.prefix))
    } else {
        PathBuf::from(&local.prefix)
    };
    for component in &local.components {
        output.push(component);
    }
    for component in &input.components[remote.components.len()..] {
        output.push(component);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tower::ServiceExt as _;

    fn test_state(temp: &tempfile::TempDir) -> Arc<RelayState> {
        Arc::new(RelayState::new(
            RelayConfig::desktop_default(false),
            temp.path().join("relay.toml"),
            temp.path().join("share-cache"),
            Arc::new(|_, _, _| Ok(())),
            Arc::new(|| {}),
            Arc::new(|| {}),
        ))
    }

    const TEST_CREDENTIAL: &str = "test-credential";
    const TEST_ORIGIN: &str = "https://remote.example";
    /// The bytes every share test uploads, and their real digest. The relay
    /// verifies the hash of what arrives, so a fixture whose declared sha256
    /// is decorative would only ever exercise the rejection path.
    const FIXTURE: &[u8] = b"fixture";
    const TEST_SHA: &str = "f16d05ec6b29248d2c61adb1e9263f78e4f7bace1b955014a2d17872cfe4064d";
    /// A syntactically valid digest that matches no fixture.
    const WRONG_SHA: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn the_fixture_digest_is_the_real_one() {
        assert_eq!(hex_digest(Sha256::digest(FIXTURE).as_slice()), TEST_SHA);
    }

    struct ShareFixture {
        state: Arc<RelayState>,
        /// Paths the action handler was invoked with, in order.
        calls: Arc<std::sync::Mutex<Vec<PathBuf>>>,
        /// Times the mapping window was asked for attention. A share verb
        /// must never raise it.
        mapping_attention: Arc<AtomicUsize>,
    }

    fn share_fixture(temp: &tempfile::TempDir, mappings: Vec<PathMapping>) -> ShareFixture {
        share_fixture_with(temp, mappings, default_share_cache_max_bytes(), None)
    }

    fn share_fixture_with(
        temp: &tempfile::TempDir,
        mappings: Vec<PathMapping>,
        share_cache_max_bytes: u64,
        handler: Option<ActionHandler>,
    ) -> ShareFixture {
        let salt = SaltString::encode_b64(b"0123456789abcdef").unwrap();
        let credential_hash = Argon2::default()
            .hash_password(TEST_CREDENTIAL.as_bytes(), &salt)
            .unwrap()
            .to_string();
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = calls.clone();
        let mapping_attention = Arc::new(AtomicUsize::new(0));
        let attention = mapping_attention.clone();
        let state = Arc::new(RelayState::new(
            RelayConfig {
                relay_id: Uuid::new_v4(),
                enabled: true,
                bind: default_bind(),
                instances: vec![RelayInstance {
                    id: Uuid::new_v4(),
                    name: "remote".into(),
                    server_url: TEST_ORIGIN.into(),
                    origins: vec![TEST_ORIGIN.into()],
                    credential_hash,
                    mappings,
                }],
                commands: FileActionCommands::default(),
                share_cache_max_bytes,
                pairing_operations: Vec::new(),
                actions: Vec::new(),
            },
            temp.path().join("relay.toml"),
            temp.path().join("share-cache"),
            handler.unwrap_or_else(|| {
                Arc::new(move |_, path: PathBuf, _| {
                    recorded.lock().unwrap().push(path);
                    Ok(())
                })
            }),
            Arc::new(|| {}),
            Arc::new(move || {
                attention.fetch_add(1, Ordering::Release);
            }),
        ));
        ShareFixture {
            state,
            calls,
            mapping_attention,
        }
    }

    fn authorized(method: &str, uri: String) -> axum::http::request::Builder {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(header::ORIGIN, TEST_ORIGIN)
            .header(header::AUTHORIZATION, format!("Bearer {TEST_CREDENTIAL}"))
    }

    fn copy_request(action_id: Uuid, path: &str, filename: &str, size: u64) -> Request<Body> {
        copy_request_with_sha(action_id, path, filename, size, TEST_SHA)
    }

    fn copy_request_with_sha(
        action_id: Uuid,
        path: &str,
        filename: &str,
        size: u64,
        sha256: &str,
    ) -> Request<Body> {
        authorized("POST", "/v1/actions".into())
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::json!({
                    "action_id": action_id,
                    "action": "copy_to_clipboard",
                    "path": path,
                    "sha256": sha256,
                    "filename": filename,
                    "size": size,
                })
                .to_string(),
            ))
            .unwrap()
    }

    fn upload_request(action_id: Uuid, bytes: Vec<u8>) -> Request<Body> {
        authorized("POST", format!("/v1/files/{action_id}"))
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(Body::from(bytes))
            .unwrap()
    }

    fn action_status_request(action_id: Uuid) -> Request<Body> {
        authorized("GET", format!("/v1/actions/{action_id}"))
            .body(Body::empty())
            .unwrap()
    }

    /// Temporary upload files still present in the cache root. Names are
    /// unique per attempt, so "the temp was cleaned up" is a statement about
    /// the directory rather than about one predictable path.
    fn leftover_temp_files(state: &RelayState) -> Vec<PathBuf> {
        std::fs::read_dir(state.share_cache.root())
            .into_iter()
            .flatten()
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with(".tmp-"))
            })
            .collect()
    }

    async fn json_body(response: Response) -> serde_json::Value {
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    fn pairing_request(origin: &str, name: &str) -> Request<Body> {
        pairing_request_with_id(origin, name, Uuid::new_v4())
    }

    fn pairing_request_with_id(origin: &str, name: &str, operation_id: Uuid) -> Request<Body> {
        pairing_request_with_roots(origin, name, operation_id, &[])
    }

    fn pairing_request_with_roots(
        origin: &str,
        name: &str,
        operation_id: Uuid,
        roots: &[&str],
    ) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/v1/pairing/request")
            .header(header::ORIGIN, origin)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::json!({
                    "operation_id": operation_id,
                    "name": name,
                    "origin": origin,
                    "server_url": format!("{origin}/search"),
                    "roots": roots,
                })
                .to_string(),
            ))
            .unwrap()
    }

    #[tokio::test]
    async fn retry_foregrounds_pending_pairing_and_window_close_rejects_it() {
        let temp = tempfile::tempdir().unwrap();
        let attention_count = Arc::new(AtomicUsize::new(0));
        let attention = attention_count.clone();
        let state = Arc::new(RelayState::new(
            RelayConfig::desktop_default(false),
            temp.path().join("relay.toml"),
            temp.path().join("share-cache"),
            Arc::new(|_, _, _| Ok(())),
            Arc::new(move || {
                attention.fetch_add(1, Ordering::Release);
            }),
            Arc::new(|| {}),
        ));
        let operation_id = Uuid::new_v4();
        for _ in 0..2 {
            let response = router(state.clone())
                .oneshot(pairing_request_with_id(
                    "https://remote.example",
                    "remote",
                    operation_id,
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::ACCEPTED);
        }
        assert_eq!(attention_count.load(Ordering::Acquire), 2);

        state.approve(operation_id).await.unwrap();
        assert_eq!(
            state.pairing_progress(operation_id).await.unwrap().status,
            "finishing"
        );
        assert_eq!(state.status().await.instances.len(), 1);

        state.cancel_incomplete_pairings().await.unwrap();
        assert_eq!(
            state.pairing_progress(operation_id).await.unwrap().status,
            "rejected"
        );
        assert!(state.status().await.instances.is_empty());
    }

    #[tokio::test]
    async fn pairing_saves_edited_roots_and_leaves_skipped_roots_unmapped() {
        let temp = tempfile::tempdir().unwrap();
        let state = test_state(&temp);
        let operation_id = Uuid::new_v4();
        let response = router(state.clone())
            .oneshot(pairing_request_with_roots(
                "https://remote.example",
                "remote",
                operation_id,
                &["/mapped", "/map-later"],
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        state
            .approve_with_mappings(
                operation_id,
                vec![
                    PathMapping {
                        remote: "/".into(),
                        local: temp.path().display().to_string(),
                    },
                    PathMapping {
                        remote: "/map-later".into(),
                        local: String::new(),
                    },
                ],
            )
            .await
            .unwrap();

        let status = state.status().await;
        assert_eq!(status.instances.len(), 1);
        assert_eq!(status.instances[0].mappings.len(), 1);
        assert_eq!(status.instances[0].mappings[0].remote, "/");
    }

    #[tokio::test]
    async fn auth_preflight_remains_available_after_revocation() {
        let temp = tempfile::tempdir().unwrap();
        let state = test_state(&temp);
        let request = Request::builder()
            .method("OPTIONS")
            .uri("/v1/auth/check")
            .header(header::ORIGIN, "https://remote.example")
            .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
            .header(header::ACCESS_CONTROL_REQUEST_HEADERS, "authorization")
            .body(Body::empty())
            .unwrap();
        let response = router(state.clone()).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://remote.example"
        );
        assert!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_HEADERS]
                .to_str()
                .unwrap()
                .contains("Authorization")
        );

        // Privileged action preflights remain restricted to paired origins.
        let action_request = Request::builder()
            .method("OPTIONS")
            .uri("/v1/actions")
            .header(header::ORIGIN, "https://remote.example")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            router(state)
                .oneshot(action_request)
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
    }

    #[test]
    fn relay_is_enabled_by_default() {
        let production = RelayConfig::desktop_default(false);
        let development = RelayConfig::desktop_default(true);

        assert!(RelayConfig::default().enabled);
        assert!(production.enabled);
        assert!(development.enabled);
        assert_eq!(production.bind, "127.0.0.1:16341");
        assert_eq!(development.bind, "127.0.0.1:17601");
    }

    #[test]
    fn missing_enabled_key_defaults_on_but_explicit_false_is_preserved() {
        let missing: RelayConfig = toml::from_str("bind = '127.0.0.1:16341'").unwrap();
        assert!(missing.enabled);

        let disabled: RelayConfig =
            toml::from_str("enabled = false\nbind = '127.0.0.1:16341'").unwrap();
        assert!(!disabled.enabled);
    }

    #[test]
    fn legacy_default_bind_is_migrated_but_custom_bind_is_preserved() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("relay.toml");
        std::fs::write(&path, "bind = '127.0.0.1:17600'\n").unwrap();

        let migrated = load_config(&path, false).unwrap();
        assert_eq!(migrated.bind, PRODUCTION_DEFAULT_BIND);
        assert!(
            std::fs::read_to_string(&path)
                .unwrap()
                .contains(PRODUCTION_DEFAULT_BIND)
        );

        std::fs::write(&path, "bind = '127.0.0.1:18000'\n").unwrap();
        let custom = load_config(&path, false).unwrap();
        assert_eq!(custom.bind, "127.0.0.1:18000");
    }

    /// Mapping is component-aware and the longest valid prefix wins.
    #[test]
    fn longest_component_prefix_wins() {
        let mappings = vec![
            PathMapping {
                remote: "/srv".into(),
                local: "/mnt/base".into(),
            },
            PathMapping {
                remote: "/srv/media".into(),
                local: "/mnt/media".into(),
            },
        ];
        assert_eq!(
            map_path("/srv/media/photos/a.jpg", &mappings).unwrap(),
            PathBuf::from("/mnt/media/photos/a.jpg")
        );
        assert!(map_path("/srv-media/a.jpg", &mappings).is_err());
    }

    /// Dot components normalize before matching while lexical traversal above
    /// the remote mapping prefix is rejected; mappings are not symlink sandboxes.
    #[test]
    fn traversal_cannot_escape_mapping() {
        let mappings = [PathMapping {
            remote: "/srv/media".into(),
            local: "/mnt/media".into(),
        }];
        assert_eq!(
            map_path("/srv/media/a/../b.jpg", &mappings).unwrap(),
            PathBuf::from("/mnt/media/b.jpg")
        );
        assert!(map_path("/srv/media/../../etc/passwd", &mappings).is_err());
    }

    /// Windows drive and UNC paths normalize separators and case without raw
    /// string-prefix confusion.
    #[test]
    fn windows_drive_and_unc_mapping() {
        let drive = [PathMapping {
            remote: "D:\\Archive".into(),
            local: "Z:\\Media".into(),
        }];
        assert_eq!(
            map_path("d:/archive/Set/file.jpg", &drive)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/"),
            "Z:/Media/Set/file.jpg"
        );
        let unc = [PathMapping {
            remote: "//nas/share/media".into(),
            local: "C:/cache".into(),
        }];
        assert_eq!(
            map_path("\\\\NAS\\share\\media\\x.png", &unc)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/"),
            "C:/cache/x.png"
        );
    }

    /// Credentials are unique salted password hashes and verification never
    /// relies on plaintext persistence.
    #[test]
    fn credential_hash_verification() {
        let salt = SaltString::encode_b64(b"0123456789abcdef").unwrap();
        let hash = Argon2::default()
            .hash_password(b"secret", &salt)
            .unwrap()
            .to_string();
        assert!(verify_credential(&hash, "secret"));
        assert!(!verify_credential(&hash, "wrong"));
    }

    /// Pairing reflects only a canonical matching Origin, adds CORS headers,
    /// and rejects the sixth request from one origin inside the rate window.
    #[tokio::test]
    async fn pairing_origin_cors_and_rate_limit() {
        let temp = tempfile::tempdir().unwrap();
        let state = test_state(&temp);
        for index in 0..RATE_LIMIT {
            let response = router(state.clone())
                .oneshot(pairing_request(
                    "https://remote.example",
                    &format!("remote-{index}"),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::ACCEPTED);
            assert_eq!(
                response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
                "https://remote.example"
            );
        }
        let limited = router(state.clone())
            .oneshot(pairing_request("https://remote.example", "limited"))
            .await
            .unwrap();
        assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);

        let mut mismatched = pairing_request("https://other.example", "wrong");
        *mismatched.body_mut() = Body::from(
            serde_json::json!({
                "operation_id": Uuid::new_v4(),
                "name": "wrong",
                "origin": "https://remote.example",
                "server_url": "https://remote.example/search"
            })
            .to_string(),
        );
        let response = router(state).oneshot(mismatched).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    /// An approved credential remains recoverable by its requesting origin
    /// for the request TTL; another origin cannot poll it and revocation persists.
    #[tokio::test]
    async fn approved_pairing_is_origin_bound_repeatable_and_revocable() {
        let temp = tempfile::tempdir().unwrap();
        let state = test_state(&temp);
        let response = router(state.clone())
            .oneshot(pairing_request("https://remote.example", "remote"))
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
        let requested: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let request_id = Uuid::parse_str(requested["operation_id"].as_str().unwrap()).unwrap();
        let (first, second) = tokio::join!(state.approve(request_id), state.approve(request_id));
        first.unwrap();
        second.unwrap();
        assert_eq!(state.status().await.instances.len(), 1);

        let wrong_origin = Request::builder()
            .uri(format!("/v1/pairing/{request_id}"))
            .header(header::ORIGIN, "https://other.example")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            router(state.clone())
                .oneshot(wrong_origin)
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );

        let poll = || {
            Request::builder()
                .uri(format!("/v1/pairing/{request_id}"))
                .header(header::ORIGIN, "https://remote.example")
                .body(Body::empty())
                .unwrap()
        };
        let approved = router(state.clone()).oneshot(poll()).await.unwrap();
        let body = to_bytes(approved.into_body(), 16 * 1024).await.unwrap();
        let approved: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(approved["status"], "approved_unconfirmed");
        assert!(approved["credential"].as_str().unwrap().len() >= 40);
        let repeated = router(state.clone()).oneshot(poll()).await.unwrap();
        assert_eq!(repeated.status(), StatusCode::OK);
        let repeated = to_bytes(repeated.into_body(), 16 * 1024).await.unwrap();
        let repeated: serde_json::Value = serde_json::from_slice(&repeated).unwrap();
        assert_eq!(repeated["credential"], approved["credential"]);

        let instance_id = Uuid::parse_str(approved["instance_id"].as_str().unwrap()).unwrap();
        let credential = approved["credential"].as_str().unwrap();
        let acknowledge = || {
            Request::builder()
                .method("POST")
                .uri(format!("/v1/pairing/{request_id}/ack"))
                .header(header::ORIGIN, "https://remote.example")
                .header(header::AUTHORIZATION, format!("Bearer {credential}"))
                .body(Body::empty())
                .unwrap()
        };
        assert_eq!(
            router(state.clone())
                .oneshot(acknowledge())
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            router(state.clone())
                .oneshot(acknowledge())
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );
        state.revoke(instance_id).await.unwrap();
        assert!(state.status().await.instances.is_empty());
    }

    /// Expired requests are garbage collected and no longer claimable.
    #[tokio::test]
    async fn expired_pairing_is_not_claimable() {
        let temp = tempfile::tempdir().unwrap();
        let state = test_state(&temp);
        let id = Uuid::new_v4();
        {
            let mut config = state.config.write().await;
            config.pairing_operations.push(PairingOperation {
                id,
                name: "expired".into(),
                origin: "https://remote.example".into(),
                server_url: "https://remote.example/search".into(),
                roots: Vec::new(),
                created_unix: unix_now() - PAIRING_TTL.as_secs() as i64 - 1,
                state: PairingOperationState::Pending,
            });
        }
        let request = Request::builder()
            .uri(format!("/v1/pairing/{id}"))
            .header(header::ORIGIN, "https://remote.example")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            router(state).oneshot(request).await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
    }

    /// Authenticated actions require both the paired origin and credential,
    /// use a mapped existing path, and fail immediately after revocation.
    #[tokio::test]
    async fn action_authentication_mapping_and_revocation() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("fixture.txt");
        std::fs::write(&file, "fixture").unwrap();
        let credential = "test-credential";
        let salt = SaltString::encode_b64(b"0123456789abcdef").unwrap();
        let hash = Argon2::default()
            .hash_password(credential.as_bytes(), &salt)
            .unwrap()
            .to_string();
        let instance_id = Uuid::new_v4();
        let invoked = Arc::new(AtomicBool::new(false));
        let invoked_for_action = invoked.clone();
        let state = Arc::new(RelayState::new(
            RelayConfig {
                relay_id: Uuid::new_v4(),
                enabled: true,
                bind: default_bind(),
                instances: vec![RelayInstance {
                    id: instance_id,
                    name: "remote".into(),
                    server_url: "https://remote.example/search".into(),
                    origins: vec!["https://remote.example".into()],
                    credential_hash: hash,
                    mappings: vec![PathMapping {
                        remote: "/remote".into(),
                        local: temp.path().display().to_string(),
                    }],
                }],
                commands: FileActionCommands::default(),
                share_cache_max_bytes: default_share_cache_max_bytes(),
                pairing_operations: Vec::new(),
                actions: Vec::new(),
            },
            temp.path().join("relay.toml"),
            temp.path().join("share-cache"),
            Arc::new(move |_, _, _| {
                invoked_for_action.store(true, Ordering::Release);
                Ok(())
            }),
            Arc::new(|| {}),
            Arc::new(|| {}),
        ));
        let action_id = Uuid::new_v4();
        let action = || {
            Request::builder()
                .method("POST")
                .uri("/v1/actions")
                .header(header::ORIGIN, "https://remote.example")
                .header(header::AUTHORIZATION, format!("Bearer {credential}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({"action_id":action_id,"action":"open_file","path":"/remote/fixture.txt"})
                        .to_string(),
                ))
                .unwrap()
        };
        let response = router(state.clone()).oneshot(action()).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(invoked.load(Ordering::Acquire));
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://remote.example"
        );
        state.revoke(instance_id).await.unwrap();
        assert_eq!(
            router(state).oneshot(action()).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }

    /// An action outside all root hints is retained, accepts a newly entered
    /// remote root, previews the translated file, and executes automatically
    /// after Desktop saves the mapping.
    #[tokio::test]
    async fn unknown_root_mapping_resumes_the_pending_action() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("fixture.txt"), "fixture").unwrap();
        let credential = "test-credential";
        let salt = SaltString::encode_b64(b"0123456789abcdef").unwrap();
        let hash = Argon2::default()
            .hash_password(credential.as_bytes(), &salt)
            .unwrap()
            .to_string();
        let instance_id = Uuid::new_v4();
        let invoked = Arc::new(AtomicBool::new(false));
        let invoked_for_action = invoked.clone();
        let state = Arc::new(RelayState::new(
            RelayConfig {
                relay_id: Uuid::new_v4(),
                enabled: true,
                bind: default_bind(),
                instances: vec![RelayInstance {
                    id: instance_id,
                    name: "remote".into(),
                    server_url: "https://remote.example".into(),
                    origins: vec!["https://remote.example".into()],
                    credential_hash: hash,
                    mappings: Vec::new(),
                }],
                commands: FileActionCommands::default(),
                share_cache_max_bytes: default_share_cache_max_bytes(),
                pairing_operations: Vec::new(),
                actions: Vec::new(),
            },
            temp.path().join("relay.toml"),
            temp.path().join("share-cache"),
            Arc::new(move |_, _, _| {
                invoked_for_action.store(true, Ordering::Release);
                Ok(())
            }),
            Arc::new(|| {}),
            Arc::new(|| {}),
        ));
        let action_id = Uuid::new_v4();
        let request = Request::builder()
            .method("POST")
            .uri("/v1/actions")
            .header(header::ORIGIN, "https://remote.example")
            .header(header::AUTHORIZATION, format!("Bearer {credential}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::json!({"action_id":action_id,"action":"open_file","path":"/unknown/fixture.txt"}).to_string(),
            ))
            .unwrap();
        assert_eq!(
            router(state.clone())
                .oneshot(request)
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
        assert_eq!(state.status().await.pending_actions.len(), 1);
        let preview = state
            .mapping_preview(
                action_id,
                "/unknown".into(),
                temp.path().display().to_string(),
            )
            .await
            .unwrap();
        assert!(preview.exists);
        state
            .resolve_mapping(
                action_id,
                "/unknown".into(),
                temp.path().display().to_string(),
            )
            .await
            .unwrap();
        assert!(invoked.load(Ordering::Acquire));
        assert!(state.status().await.pending_actions.is_empty());
    }

    /// Capability advertisement: a client that sees no `features` array is
    /// talking to a relay that predates share verbs.
    #[tokio::test]
    async fn health_advertises_the_clipboard_capability() {
        let temp = tempfile::tempdir().unwrap();
        let state = test_state(&temp);
        let request = Request::builder()
            .uri("/v1/health")
            .body(Body::empty())
            .unwrap();
        let health = json_body(router(state).oneshot(request).await.unwrap()).await;
        assert_eq!(health["features"], serde_json::json!(["copy_to_clipboard"]));
    }

    /// A share verb whose path maps to an existing local file is executed
    /// against the real file: nothing is cached and no bytes move.
    #[tokio::test]
    async fn share_verb_uses_a_resolved_mapping_without_caching() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("fixture.bin");
        std::fs::write(&file, b"fixture").unwrap();
        let fixture = share_fixture(
            &temp,
            vec![PathMapping {
                remote: "/remote".into(),
                local: temp.path().display().to_string(),
            }],
        );

        let response = router(fixture.state.clone())
            .oneshot(copy_request(
                Uuid::new_v4(),
                "/remote/fixture.bin",
                "fixture.bin",
                7,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            TEST_ORIGIN
        );
        assert_eq!(*fixture.calls.lock().unwrap(), vec![file]);
        assert!(
            !fixture.state.share_cache.root().exists(),
            "a mapped share verb must not materialize a cache copy"
        );
        assert_eq!(fixture.mapping_attention.load(Ordering::Acquire), 0);
    }

    /// Without a mapping and without cached bytes the action parks in
    /// `PendingBytes`: never a mapping prompt, invisible to the mapping
    /// window, and idempotent for a repeated action id.
    #[tokio::test]
    async fn share_verb_without_bytes_parks_and_repeats_the_same_conflict() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = share_fixture(&temp, Vec::new());
        let action_id = Uuid::new_v4();
        let request = || copy_request(action_id, "/unmapped/fixture.bin", "fixture.bin", 7);

        for _ in 0..2 {
            let response = router(fixture.state.clone())
                .oneshot(request())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::CONFLICT);
            let body = json_body(response).await;
            assert_eq!(body["error"]["code"], "bytes_required");
            assert_eq!(body["error"]["details"]["action_id"], action_id.to_string());
            assert_eq!(body["error"]["details"]["sha256"], TEST_SHA);
            assert_eq!(body["error"]["details"]["filename"], "fixture.bin");
        }
        assert!(fixture.calls.lock().unwrap().is_empty());
        assert_eq!(fixture.mapping_attention.load(Ordering::Acquire), 0);
        assert!(
            fixture.state.status().await.pending_actions.is_empty(),
            "PendingBytes must stay invisible to the mapping window"
        );

        // Share metadata is mandatory for the verb.
        let invalid = authorized("POST", "/v1/actions".into())
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::json!({"action_id": Uuid::new_v4(), "action": "copy_to_clipboard", "path": "/unmapped/fixture.bin"})
                    .to_string(),
            ))
            .unwrap();
        let response = router(fixture.state).oneshot(invalid).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            json_body(response).await["error"]["code"],
            "invalid_share_metadata"
        );
    }

    /// The upload completes the parked action against the cache copy, and the
    /// next action for the same content is served from that copy directly.
    #[tokio::test]
    async fn upload_completes_the_action_and_the_next_one_hits_the_cache() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = share_fixture(&temp, Vec::new());
        let action_id = Uuid::new_v4();
        assert_eq!(
            router(fixture.state.clone())
                .oneshot(copy_request(
                    action_id,
                    "/unmapped/fixture.bin",
                    "fixture.bin",
                    7
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );

        let response = router(fixture.state.clone())
            .oneshot(upload_request(action_id, FIXTURE.to_vec()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            TEST_ORIGIN
        );
        let cached = fixture
            .state
            .share_cache
            .entry_path(TEST_SHA, "fixture.bin");
        assert_eq!(std::fs::read(&cached).unwrap(), FIXTURE);
        assert_eq!(*fixture.calls.lock().unwrap(), vec![cached.clone()]);
        assert!(leftover_temp_files(&fixture.state).is_empty());
        assert_eq!(
            router(fixture.state.clone())
                .oneshot(action_status_request(action_id))
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );

        // Same content, new action: the cache answers immediately.
        let repeat = Uuid::new_v4();
        assert_eq!(
            router(fixture.state.clone())
                .oneshot(copy_request(
                    repeat,
                    "/unmapped/other/fixture.bin",
                    "fixture.bin",
                    7
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(*fixture.calls.lock().unwrap(), vec![cached.clone(), cached]);
    }

    /// An upload larger than the declared size plus slack is refused, the
    /// partial file is removed, and the record stays uploadable.
    #[tokio::test]
    async fn oversize_upload_is_refused_and_the_action_stays_pending() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = share_fixture(&temp, Vec::new());
        let action_id = Uuid::new_v4();
        assert_eq!(
            router(fixture.state.clone())
                .oneshot(copy_request(
                    action_id,
                    "/unmapped/fixture.bin",
                    "fixture.bin",
                    7
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );

        let oversize = vec![b'x'; (UPLOAD_SIZE_SLACK + 8) as usize];
        let response = router(fixture.state.clone())
            .oneshot(upload_request(action_id, oversize))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            json_body(response).await["error"]["code"],
            "upload_too_large"
        );
        assert!(leftover_temp_files(&fixture.state).is_empty());
        assert!(
            !fixture
                .state
                .share_cache
                .entry_path(TEST_SHA, "fixture.bin")
                .exists()
        );
        assert!(fixture.calls.lock().unwrap().is_empty());

        // Still `PendingBytes`, so the browser can retry the upload.
        let status = router(fixture.state.clone())
            .oneshot(action_status_request(action_id))
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::CONFLICT);
        assert_eq!(json_body(status).await["error"]["code"], "bytes_required");

        // A short body is equally rejected and equally retryable.
        let short = router(fixture.state.clone())
            .oneshot(upload_request(action_id, b"frag".to_vec()))
            .await
            .unwrap();
        assert_eq!(short.status(), StatusCode::BAD_REQUEST);
        assert_eq!(json_body(short).await["error"]["code"], "size_mismatch");
        assert!(leftover_temp_files(&fixture.state).is_empty());
    }

    /// Uploads are bound to the credential and to a `PendingBytes` record;
    /// a location verb awaiting a mapping still answers `mapping_required`.
    #[tokio::test]
    async fn upload_rejects_unknown_actions_bad_credentials_and_wrong_states() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = share_fixture(&temp, Vec::new());

        assert_eq!(
            router(fixture.state.clone())
                .oneshot(upload_request(Uuid::new_v4(), b"x".to_vec()))
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );

        // A malformed id is answered by us, not by axum's extractor: the
        // rejection must still carry CORS or the browser sees only an opaque
        // network failure.
        let malformed = authorized("POST", "/v1/files/not-a-uuid".into())
            .body(Body::from("x"))
            .unwrap();
        let response = router(fixture.state.clone())
            .oneshot(malformed)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            TEST_ORIGIN
        );
        assert_eq!(
            json_body(response).await["error"]["code"],
            "invalid_action_id"
        );

        // Location verbs are untouched by the share flow: still a mapping
        // prompt, still a `PendingMapping` record.
        let open_id = Uuid::new_v4();
        let open = authorized("POST", "/v1/actions".into())
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::json!({"action_id": open_id, "action": "open_file", "path": "/unmapped/fixture.bin"})
                    .to_string(),
            ))
            .unwrap();
        let response = router(fixture.state.clone()).oneshot(open).await.unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(
            json_body(response).await["error"]["code"],
            "mapping_required"
        );
        assert_eq!(fixture.mapping_attention.load(Ordering::Acquire), 1);
        assert_eq!(fixture.state.status().await.pending_actions.len(), 1);

        let wrong_state = router(fixture.state.clone())
            .oneshot(upload_request(open_id, b"x".to_vec()))
            .await
            .unwrap();
        assert_eq!(wrong_state.status(), StatusCode::CONFLICT);
        assert_eq!(
            json_body(wrong_state).await["error"]["code"],
            "mapping_required"
        );

        let unauthorized = Request::builder()
            .method("POST")
            .uri(format!("/v1/files/{open_id}"))
            .header(header::ORIGIN, TEST_ORIGIN)
            .header(header::AUTHORIZATION, "Bearer wrong-credential")
            .body(Body::from("x"))
            .unwrap();
        assert_eq!(
            router(fixture.state)
                .oneshot(unauthorized)
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }

    fn parked_record(created_unix: i64) -> ActionRecord {
        ActionRecord {
            id: Uuid::new_v4(),
            instance_id: Uuid::new_v4(),
            action: RelayAction::CopyToClipboard,
            remote_path: "/remote/fixture.bin".into(),
            created_unix,
            state: ActionRecordState::PendingBytes {
                sha256: TEST_SHA.into(),
                filename: "fixture.bin".into(),
                size: FIXTURE.len() as u64,
            },
        }
    }

    /// A materialization may legitimately outlive the ordinary action TTL, so
    /// a parked record gets a longer one of its own — but it does expire.
    /// Only `Executing` is unbounded, and startup recovery is what ends it.
    #[test]
    fn parked_records_get_a_longer_ttl_and_only_executing_is_unbounded() {
        let mut config = RelayConfig::default();
        let record = |state, created_unix| ActionRecord {
            created_unix,
            state,
            ..parked_record(0)
        };
        let past_action_ttl = unix_now() - ACTION_TTL_SECS - 1;
        let past_pending_ttl = unix_now() - PENDING_BYTES_TTL_SECS - 1;
        let parked = |created_unix| ActionRecordState::PendingBytes {
            sha256: TEST_SHA.into(),
            filename: "fixture.bin".into(),
            size: created_unix as u64,
        };
        config.actions = vec![
            // Survives the short TTL...
            record(parked(1), past_action_ttl),
            // ...but not its own.
            record(parked(2), past_pending_ttl),
            record(ActionRecordState::Executing, past_pending_ttl),
            record(ActionRecordState::Complete, past_action_ttl),
            record(ActionRecordState::PendingMapping, past_action_ttl),
        ];

        assert!(prune_config(&mut config));

        let states: Vec<&ActionRecordState> =
            config.actions.iter().map(|item| &item.state).collect();
        assert_eq!(states.len(), 2);
        assert!(matches!(
            states[0],
            ActionRecordState::PendingBytes { size: 1, .. }
        ));
        assert!(matches!(states[1], ActionRecordState::Executing));
    }

    /// An interrupted command can never report back, so a record left
    /// `Executing` by a crash is demoted at load rather than pinned forever in
    /// the one state the TTL does not touch.
    #[test]
    fn executing_records_are_demoted_to_interrupted_on_load() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("relay.toml");
        std::fs::write(&path, "bind = '127.0.0.1:16341'\n").unwrap();
        let mut executing = parked_record(unix_now());
        executing.state = ActionRecordState::Executing;
        save_actions(
            &actions_path(&path),
            &[executing, parked_record(unix_now())],
        )
        .unwrap();

        let loaded = load_config(&path, false).unwrap();

        assert_eq!(loaded.actions.len(), 2);
        assert!(matches!(
            &loaded.actions[0].state,
            ActionRecordState::Failed { code, .. } if code == "interrupted"
        ));
        assert!(matches!(
            loaded.actions[1].state,
            ActionRecordState::PendingBytes { .. }
        ));
        // The demotion is durable: a second crash must not resurrect it.
        let reloaded = load_config(&path, false).unwrap();
        assert!(matches!(
            &reloaded.actions[0].state,
            ActionRecordState::Failed { code, .. } if code == "interrupted"
        ));
    }

    /// Action state lives in its own file so that a record shape an older
    /// binary cannot parse costs at most the in-flight actions — never the
    /// pairings, which are what `relay.toml` exists to hold.
    #[test]
    fn actions_round_trip_through_the_sidecar_and_never_reach_relay_toml() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("relay.toml");
        let sidecar = actions_path(&path);
        assert_eq!(sidecar, temp.path().join("relay-actions.toml"));

        let mut config = RelayConfig::desktop_default(false);
        config.actions = vec![parked_record(unix_now())];
        save_config(&path, &config).unwrap();
        save_actions(&sidecar, &config.actions).unwrap();

        let written = std::fs::read_to_string(&path).unwrap();
        assert!(
            !written.contains("actions"),
            "relay.toml must not carry action state: {written}"
        );
        // The tunable ceiling is likewise absent while it holds its default,
        // so a later change to that default still reaches this install.
        assert!(!written.contains("share_cache_max_bytes"));

        let loaded = load_config(&path, false).unwrap();
        assert_eq!(loaded.actions.len(), 1);
        assert_eq!(loaded.actions[0].id, config.actions[0].id);

        // A changed ceiling *is* written, and read back.
        config.share_cache_max_bytes = 123;
        save_config(&path, &config).unwrap();
        assert!(std::fs::read_to_string(&path).unwrap().contains("123"));
        assert_eq!(
            load_config(&path, false).unwrap().share_cache_max_bytes,
            123
        );
    }

    /// The share-cache ceiling set through the control command is persisted to
    /// `relay.toml` and reflected back by `status()` (bytes), which is how the
    /// control UI populates its "Share cache limit" field on load.
    #[tokio::test]
    async fn set_share_cache_max_bytes_persists_and_surfaces_in_status() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = share_fixture(&temp, Vec::new());

        assert_eq!(
            fixture.state.status().await.share_cache_max_bytes,
            default_share_cache_max_bytes()
        );

        fixture
            .state
            .set_share_cache_max_bytes(256 * 1024 * 1024)
            .await
            .unwrap();

        assert_eq!(
            fixture.state.status().await.share_cache_max_bytes,
            256 * 1024 * 1024
        );
        assert_eq!(
            load_config(&temp.path().join("relay.toml"), false)
                .unwrap()
                .share_cache_max_bytes,
            256 * 1024 * 1024
        );
    }

    /// An unparseable sidecar is quarantined on its own. The pairings survive.
    #[test]
    fn a_corrupt_sidecar_costs_only_the_actions() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("relay.toml");
        let instance_id = Uuid::new_v4();
        let mut config = RelayConfig::desktop_default(false);
        config.instances.push(RelayInstance {
            id: instance_id,
            name: "remote".into(),
            server_url: TEST_ORIGIN.into(),
            origins: vec![TEST_ORIGIN.into()],
            credential_hash: "hash".into(),
            mappings: Vec::new(),
        });
        save_config(&path, &config).unwrap();
        std::fs::write(actions_path(&path), "actions = [ this is not toml").unwrap();

        let loaded = load_config(&path, false).unwrap();

        assert!(loaded.actions.is_empty());
        assert_eq!(loaded.instances.len(), 1);
        assert_eq!(loaded.instances[0].id, instance_id);
        assert!(!actions_path(&path).exists(), "the sidecar was quarantined");
        assert!(
            std::fs::read_dir(temp.path()).unwrap().any(|entry| {
                let name = entry.unwrap().file_name();
                name.to_string_lossy()
                    .starts_with("relay-actions.toml.invalid-")
            }),
            "the quarantined copy is kept"
        );
    }

    /// A `relay.toml` written before the split still carries its actions;
    /// loading absorbs them into the sidecar and strips them from the config.
    #[test]
    fn a_legacy_relay_toml_absorbs_its_embedded_actions() {
        /// `relay.toml` exactly as builds before the split wrote it.
        #[derive(Serialize)]
        struct LegacyConfig {
            relay_id: Uuid,
            bind: String,
            actions: Vec<ActionRecord>,
        }

        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("relay.toml");
        let legacy = parked_record(unix_now());
        let action_id = legacy.id;
        std::fs::write(
            &path,
            toml::to_string_pretty(&LegacyConfig {
                relay_id: Uuid::new_v4(),
                bind: default_bind(),
                actions: vec![legacy],
            })
            .unwrap(),
        )
        .unwrap();

        let loaded = load_config(&path, false).unwrap();

        assert_eq!(loaded.actions.len(), 1);
        assert_eq!(loaded.actions[0].id, action_id);
        assert!(actions_path(&path).exists(), "the sidecar was written");
        assert!(
            !std::fs::read_to_string(&path).unwrap().contains("actions"),
            "the legacy stanza was rewritten away"
        );
        // Re-loading now reads the sidecar and finds the same record.
        assert_eq!(load_config(&path, false).unwrap().actions[0].id, action_id);
    }

    /// The relay stores what it is told to store only if the bytes hash to the
    /// declared digest; otherwise the entry every later action resolves to
    /// would be whatever one origin decided to put there.
    #[tokio::test]
    async fn an_upload_whose_bytes_do_not_match_the_declared_hash_is_refused() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = share_fixture(&temp, Vec::new());
        let action_id = Uuid::new_v4();
        assert_eq!(
            router(fixture.state.clone())
                .oneshot(copy_request_with_sha(
                    action_id,
                    "/unmapped/fixture.bin",
                    "fixture.bin",
                    FIXTURE.len() as u64,
                    WRONG_SHA,
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );

        let response = router(fixture.state.clone())
            .oneshot(upload_request(action_id, FIXTURE.to_vec()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(json_body(response).await["error"]["code"], "hash_mismatch");
        assert!(fixture.calls.lock().unwrap().is_empty());
        assert!(
            !fixture
                .state
                .share_cache
                .entry_path(WRONG_SHA, "fixture.bin")
                .exists(),
            "nothing may be stored under a digest the bytes do not have"
        );
        assert!(
            std::fs::read_dir(fixture.state.share_cache.root())
                .map(|entries| entries.count())
                .unwrap_or_default()
                == 0,
            "the partial temporary file is removed"
        );

        // Still `PendingBytes`, so the correct bytes can still be pushed.
        let status = router(fixture.state.clone())
            .oneshot(action_status_request(action_id))
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::CONFLICT);
        assert_eq!(json_body(status).await["error"]["code"], "bytes_required");
    }

    /// Two uploads for one action would race over the same record and the same
    /// destination. The second is refused while the first is streaming.
    #[tokio::test]
    async fn a_second_concurrent_upload_is_refused() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = share_fixture(&temp, Vec::new());
        let action_id = Uuid::new_v4();
        assert_eq!(
            router(fixture.state.clone())
                .oneshot(copy_request(
                    action_id,
                    "/unmapped/fixture.bin",
                    "fixture.bin",
                    FIXTURE.len() as u64
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );

        // Stand in for an upload that is mid-stream.
        let claim = fixture.state.claim_upload(action_id).expect("first claim");
        let response = router(fixture.state.clone())
            .oneshot(upload_request(action_id, FIXTURE.to_vec()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            TEST_ORIGIN
        );
        assert_eq!(
            json_body(response).await["error"]["code"],
            "upload_in_progress"
        );

        // The claim is released on every exit path, so the retry succeeds.
        drop(claim);
        assert!(fixture.state.in_flight_uploads().is_empty());
        assert_eq!(
            router(fixture.state.clone())
                .oneshot(upload_request(action_id, FIXTURE.to_vec()))
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );
    }

    /// A file that could never fit the cache is refused before the browser
    /// uploads it — and before a record exists to upload against.
    #[tokio::test]
    async fn a_file_larger_than_the_cache_ceiling_is_refused_at_action_time() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = share_fixture_with(&temp, Vec::new(), 1024, None);
        let action_id = Uuid::new_v4();

        let response = router(fixture.state.clone())
            .oneshot(copy_request(
                action_id,
                "/unmapped/huge.bin",
                "huge.bin",
                4096,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            TEST_ORIGIN
        );
        let body = json_body(response).await;
        assert_eq!(body["error"]["code"], "file_too_large");
        assert_eq!(body["error"]["details"]["size"], 4096);
        assert_eq!(body["error"]["details"]["max"], 1024);
        // No record was created, so the id remains free for the fallback.
        assert_eq!(
            router(fixture.state.clone())
                .oneshot(action_status_request(action_id))
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );

        // A file that fits still parks normally, and a zero-byte file — no
        // longer a validation error — is an ordinary small file.
        for (name, size) in [("fits.bin", 512u64), ("empty.bin", 0)] {
            let response = router(fixture.state.clone())
                .oneshot(copy_request(Uuid::new_v4(), "/unmapped/x.bin", name, size))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::CONFLICT);
            assert_eq!(json_body(response).await["error"]["code"], "bytes_required");
        }
    }

    /// The record cap evicts the oldest finished record instead of refusing
    /// new work, and only refuses when everything retained is in flight.
    #[tokio::test]
    async fn the_record_cap_evicts_before_it_refuses() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = share_fixture(&temp, Vec::new());
        let now = unix_now();
        {
            let mut config = fixture.state.config.write().await;
            config.actions = (0..MAX_ACTION_RECORDS)
                .map(|index| ActionRecord {
                    // All fresh enough to survive pruning, so the cap — not
                    // the TTL — is what the request runs into. One is older
                    // than the rest and is the one that must go.
                    created_unix: now - if index == 0 { ACTION_TTL_SECS / 2 } else { 0 },
                    state: ActionRecordState::Complete,
                    ..parked_record(0)
                })
                .collect();
        }
        let oldest = fixture.state.config.read().await.actions[0].id;

        let action_id = Uuid::new_v4();
        assert_eq!(
            router(fixture.state.clone())
                .oneshot(copy_request(action_id, "/unmapped/a.bin", "a.bin", 7))
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
        let config = fixture.state.config.read().await;
        assert_eq!(config.actions.len(), MAX_ACTION_RECORDS);
        assert!(
            !config.actions.iter().any(|item| item.id == oldest),
            "the oldest evictable record made room"
        );
        assert!(config.actions.iter().any(|item| item.id == action_id));
        drop(config);

        // With every record executing there is nothing to evict, and the
        // relay says so rather than dropping live work.
        {
            let mut config = fixture.state.config.write().await;
            for action in &mut config.actions {
                action.state = ActionRecordState::Executing;
            }
        }
        assert_eq!(
            router(fixture.state.clone())
                .oneshot(copy_request(Uuid::new_v4(), "/unmapped/b.bin", "b.bin", 7))
                .await
                .unwrap()
                .status(),
            StatusCode::TOO_MANY_REQUESTS
        );
    }

    /// A panicking local command is an ordinary failed action, not a record
    /// stuck in the one state that never expires.
    #[tokio::test]
    async fn a_panicking_action_handler_fails_the_record() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = share_fixture_with(
            &temp,
            Vec::new(),
            default_share_cache_max_bytes(),
            Some(Arc::new(|_, _, _| panic!("the clipboard backend exploded"))),
        );
        let action_id = Uuid::new_v4();
        assert_eq!(
            router(fixture.state.clone())
                .oneshot(copy_request(
                    action_id,
                    "/unmapped/fixture.bin",
                    "fixture.bin",
                    FIXTURE.len() as u64
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );

        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let response = router(fixture.state.clone())
            .oneshot(upload_request(action_id, FIXTURE.to_vec()))
            .await
            .unwrap();
        std::panic::set_hook(previous);

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(json_body(response).await["error"]["code"], "command_failed");
        let config = fixture.state.config.read().await;
        let record = config
            .actions
            .iter()
            .find(|item| item.id == action_id)
            .unwrap();
        assert!(matches!(
            &record.state,
            // The stored message is the fixed generic string, never the
            // detailed error (which for the clipboard verb embeds a local path).
            ActionRecordState::Failed { code, message }
                if code == "command_failed" && message == GENERIC_ACTION_FAILURE
        ));
        assert!(fixture.state.in_flight_uploads().is_empty());
    }

    fn shell_spec(shell_command: &str) -> CommandSpec {
        CommandSpec {
            mode: CommandMode::CustomShell,
            shell_command: shell_command.into(),
            ..Default::default()
        }
    }

    /// The clipboard verb quotes its own (remote-authored) placeholder values,
    /// so a `CustomShell` template that adds quotes of its own would close that
    /// quoting and expose the filename tail to the shell. It is rejected — but
    /// only for the clipboard verb; location verbs quote their placeholders
    /// themselves and are Raw-substituted.
    #[tokio::test]
    async fn clipboard_custom_shell_rejects_its_own_quotes() {
        let temp = tempfile::tempdir().unwrap();
        let state = test_state(&temp);

        for template in ["myclip --name \"{path}\"", "mytool '{filename}'"] {
            let commands = FileActionCommands {
                copy_to_clipboard: shell_spec(template),
                ..Default::default()
            };
            assert!(
                state.set_commands(commands).await.is_err(),
                "a quoted clipboard template must be rejected: {template}"
            );
        }

        // The identical double-quoted template is accepted for a location verb.
        let location = FileActionCommands {
            open_file: shell_spec("xdg-open \"{path}\""),
            ..Default::default()
        };
        state.set_commands(location).await.unwrap();

        // A quote-free clipboard template is accepted.
        let clean = FileActionCommands {
            copy_to_clipboard: shell_spec("myclip --path {path}"),
            ..Default::default()
        };
        state.set_commands(clean).await.unwrap();
    }

    /// `evict_oldest_action` skips a record whose upload is streaming even when
    /// it is the oldest: dropping it would pull the destination out from under
    /// the in-flight write.
    #[tokio::test]
    async fn evict_refuses_an_in_flight_upload_even_when_it_is_oldest() {
        let temp = tempfile::tempdir().unwrap();
        let state = test_state(&temp);
        let uploading = Uuid::new_v4();
        let mut oldest = parked_record(1);
        oldest.id = uploading;
        let mut newer = parked_record(1_000_000);
        newer.state = ActionRecordState::Complete;
        let newer_id = newer.id;

        let mut config = RelayConfig::default();
        config.actions = vec![oldest, newer];

        // Stand in for an upload mid-stream against the oldest record.
        let _claim = state.claim_upload(uploading).expect("claim the upload");

        assert!(evict_oldest_action(&mut config, &state));
        assert!(
            config.actions.iter().any(|item| item.id == uploading),
            "the in-flight upload must survive eviction"
        );
        assert!(
            !config.actions.iter().any(|item| item.id == newer_id),
            "the newer evictable record is what made room"
        );
    }

    /// The share-cache ceiling governs only what the Relay *stores*. A file
    /// larger than the ceiling still copies when a mapping resolves it to a
    /// real local file: no 413, and nothing is cached.
    #[tokio::test]
    async fn an_oversize_file_still_copies_through_a_resolved_mapping() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("big.bin");
        let bytes = vec![b'x'; 4096];
        std::fs::write(&file, &bytes).unwrap();
        let sha = hex_digest(Sha256::digest(&bytes).as_slice());
        let fixture = share_fixture_with(
            &temp,
            vec![PathMapping {
                remote: "/remote".into(),
                local: temp.path().display().to_string(),
            }],
            1024,
            None,
        );
        let action_id = Uuid::new_v4();

        let response = router(fixture.state.clone())
            .oneshot(copy_request_with_sha(
                action_id,
                "/remote/big.bin",
                "big.bin",
                4096,
                &sha,
            ))
            .await
            .unwrap();

        // Executed, not refused — even though 4096 > the 1024 ceiling.
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(*fixture.calls.lock().unwrap(), vec![file]);
        assert!(
            !fixture.state.share_cache.root().exists(),
            "a mapped share verb caches nothing, so the ceiling never applies"
        );
        // The action completed (a Complete audit record, never a PendingBytes
        // one that would have needed an upload).
        assert_eq!(
            router(fixture.state.clone())
                .oneshot(action_status_request(action_id))
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );
    }

    /// An upload that fails on an early-return path (here `hash_mismatch`)
    /// releases its claim, so the corrected bytes are admitted rather than
    /// bounced with `upload_in_progress`.
    #[tokio::test]
    async fn a_failed_upload_releases_its_claim_for_the_retry() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = share_fixture(&temp, Vec::new());
        let action_id = Uuid::new_v4();
        assert_eq!(
            router(fixture.state.clone())
                .oneshot(copy_request(
                    action_id,
                    "/unmapped/fixture.bin",
                    "fixture.bin",
                    FIXTURE.len() as u64
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );

        // Same length as the fixture, different bytes: passes the size check,
        // fails the hash check — an early return between claim and release.
        let wrong = router(fixture.state.clone())
            .oneshot(upload_request(action_id, b"badbyte".to_vec()))
            .await
            .unwrap();
        assert_eq!(wrong.status(), StatusCode::BAD_REQUEST);
        assert_eq!(json_body(wrong).await["error"]["code"], "hash_mismatch");
        assert!(
            fixture.state.in_flight_uploads().is_empty(),
            "the claim must be released on the hash_mismatch return"
        );

        // The corrected bytes are admitted, not refused as a second upload.
        assert_eq!(
            router(fixture.state.clone())
                .oneshot(upload_request(action_id, FIXTURE.to_vec()))
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );
    }
}
