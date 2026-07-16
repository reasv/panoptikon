//! Origin-bound loopback Relay v1. The HTTP surface is intentionally small:
//! discovery, expiring local-approval pairing, and two authenticated actions.

use crate::settings::atomic_write;
use anyhow::{Context as _, bail};
use argon2::{
    Argon2, PasswordHash, PasswordHasher as _, PasswordVerifier as _, password_hash::SaltString,
};
use axum::{
    Json, Router,
    extract::{Path as AxumPath, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::Engine as _;
use rand::RngCore as _;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, RwLock, oneshot};
use url::Url;
use uuid::Uuid;

const PAIRING_TTL: Duration = Duration::from_secs(5 * 60);
const RATE_WINDOW: Duration = Duration::from_secs(60);
const RATE_LIMIT: usize = 5;
const MAX_PENDING: usize = 10;
const MAX_ACTION_RECORDS: usize = 1024;
const ACTION_TTL_SECS: i64 = 10 * 60;
const PRODUCTION_DEFAULT_BIND: &str = "127.0.0.1:16341";
const DEVELOPMENT_DEFAULT_BIND: &str = "127.0.0.1:17601";
const LEGACY_DEFAULT_BIND: &str = "127.0.0.1:17600";

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
    #[serde(default)]
    pairing_operations: Vec<PairingOperation>,
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
            pairing_operations: Vec::new(),
            actions: Vec::new(),
        }
    }
}

fn relay_enabled_by_default() -> bool {
    true
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FileActionCommands {
    #[serde(default)]
    pub open_file: CommandSpec,
    #[serde(default)]
    pub reveal_in_folder: CommandSpec,
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

pub fn load_config(path: &Path, development: bool) -> anyhow::Result<RelayConfig> {
    if !path.exists() {
        return Ok(RelayConfig::desktop_default(development));
    }
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read Relay settings '{}'", path.display()))?;
    match toml::from_str(&text) {
        Ok(config) => {
            let mut config: RelayConfig = config;
            let mut migrated = false;
            if config.bind == LEGACY_DEFAULT_BIND {
                config.bind = RelayConfig::desktop_default(development).bind;
                migrated = true;
            }
            for command in [
                &mut config.commands.open_file,
                &mut config.commands.reveal_in_folder,
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
            let stamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let quarantine = path.with_extension(format!("toml.invalid-{stamp}"));
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
    Executing,
    Complete,
    Failed { code: String, message: String },
}

type ActionHandler =
    Arc<dyn Fn(RelayAction, PathBuf, CommandSpec) -> anyhow::Result<()> + Send + Sync>;
type AttentionHandler = Arc<dyn Fn() + Send + Sync>;

pub struct RelayState {
    config: RwLock<RelayConfig>,
    config_path: PathBuf,
    attempts: Mutex<HashMap<String, VecDeque<Instant>>>,
    action_handler: ActionHandler,
    pairing_attention_handler: AttentionHandler,
    mapping_attention_handler: AttentionHandler,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RelayAction {
    OpenFile,
    RevealInFolder,
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

#[derive(Debug, Deserialize)]
struct ActionRequest {
    action_id: Uuid,
    action: RelayAction,
    path: String,
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
}

impl RelayState {
    pub fn new(
        config: RelayConfig,
        config_path: PathBuf,
        action_handler: ActionHandler,
        pairing_attention_handler: AttentionHandler,
        mapping_attention_handler: AttentionHandler,
    ) -> Self {
        Self {
            config: RwLock::new(config),
            config_path,
            attempts: Mutex::new(HashMap::new()),
            action_handler,
            pairing_attention_handler,
            mapping_attention_handler,
        }
    }

    pub async fn config(&self) -> RelayConfig {
        self.config.read().await.clone()
    }

    /// Return only fields safe to expose to the bundled control UI. In
    /// particular, credential hashes never cross the Rust command boundary.
    pub async fn status(&self) -> RelayStatusView {
        let mut config = self.config.write().await;
        if prune_config(&mut config) {
            if let Err(error) = save_config(&self.config_path, &config) {
                tracing::warn!(%error, "failed to persist Relay state garbage collection");
            }
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
        save_config(&self.config_path, &config)
    }

    pub async fn set_enabled(&self, enabled: bool) -> anyhow::Result<()> {
        let mut config = self.config.write().await;
        config.enabled = enabled;
        save_config(&self.config_path, &config)
    }

    pub async fn set_commands(&self, commands: FileActionCommands) -> anyhow::Result<()> {
        for command in [&commands.open_file, &commands.reveal_in_folder] {
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
            let _ = save_config(&self.config_path, &config);
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
        save_config(&self.config_path, &config)
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
            let command = match record.action {
                RelayAction::OpenFile => config.commands.open_file.clone(),
                RelayAction::RevealInFolder => config.commands.reveal_in_folder.clone(),
            };
            save_config(&self.config_path, &config)?;
            (record.action, path, command)
        };
        let result = (self.action_handler)(action, path, command);
        let mut config = self.config.write().await;
        if let Some(record) = config.actions.iter_mut().find(|item| item.id == action_id) {
            record.state = match &result {
                Ok(()) => ActionRecordState::Complete,
                Err(error) => ActionRecordState::Failed {
                    code: "command_failed".into(),
                    message: error.to_string(),
                },
            };
            save_config(&self.config_path, &config)?;
        }
        result
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
        .with_state(state)
}

async fn health(State(state): State<Arc<RelayState>>, headers: HeaderMap) -> Response {
    let relay_id = state.config.read().await.relay_id;
    let response = Json(Health {
        protocol: "panoptikon-relay-v1",
        version: env!("CARGO_PKG_VERSION"),
        pairing: true,
        relay_id,
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
    if config.actions.len() >= MAX_ACTION_RECORDS {
        return error(
            StatusCode::TOO_MANY_REQUESTS,
            "too many retained Relay actions",
            Some(&origin),
        );
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
            if let Err(error_value) = save_config(&state.config_path, &config) {
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &error_value.to_string(),
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
    let command = match request.action {
        RelayAction::OpenFile => config.commands.open_file.clone(),
        RelayAction::RevealInFolder => config.commands.reveal_in_folder.clone(),
    };
    config.actions.push(ActionRecord {
        id: request.action_id,
        instance_id,
        action: request.action,
        remote_path: request.path,
        created_unix: unix_now(),
        state: ActionRecordState::Executing,
    });
    if let Err(error_value) = save_config(&state.config_path, &config) {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &error_value.to_string(),
            Some(&origin),
        );
    }
    drop(config);
    tracing::info!(%instance_id, action = ?request.action, "Relay action authorized");
    match (state.action_handler)(request.action, mapped, command) {
        Ok(()) => {
            let mut config = state.config.write().await;
            if let Some(record) = config
                .actions
                .iter_mut()
                .find(|item| item.id == request.action_id)
            {
                record.state = ActionRecordState::Complete;
            }
            let _ = save_config(&state.config_path, &config);
            with_cors(StatusCode::NO_CONTENT.into_response(), &origin)
        }
        Err(error_value) => {
            tracing::warn!(%instance_id, error = %error_value, "Relay action failed");
            let mut config = state.config.write().await;
            if let Some(record) = config
                .actions
                .iter_mut()
                .find(|item| item.id == request.action_id)
            {
                record.state = ActionRecordState::Failed {
                    code: "command_failed".into(),
                    message: error_value.to_string(),
                };
            }
            let _ = save_config(&state.config_path, &config);
            structured_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "command_failed",
                "local action failed",
                Some(&origin),
                serde_json::json!({}),
            )
        }
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
    config
        .actions
        .retain(|item| item.created_unix + ACTION_TTL_SECS > now);
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

fn action_record_response(record: &ActionRecord, origin: &str) -> Response {
    match &record.state {
        ActionRecordState::PendingMapping => with_cors((StatusCode::CONFLICT, Json(serde_json::json!({
            "error": { "code": "mapping_required", "message": "Choose the local folder corresponding to this server path", "details": { "path": record.remote_path, "instance_id": record.instance_id, "action_id": record.id } }
        }))).into_response(), origin),
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

fn save_config(path: &Path, config: &RelayConfig) -> anyhow::Result<()> {
    atomic_write(path, toml::to_string_pretty(config)?.as_bytes())
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
            Arc::new(|_, _, _| Ok(())),
            Arc::new(|| {}),
            Arc::new(|| {}),
        ))
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
                pairing_operations: Vec::new(),
                actions: Vec::new(),
            },
            temp.path().join("relay.toml"),
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
                pairing_operations: Vec::new(),
                actions: Vec::new(),
            },
            temp.path().join("relay.toml"),
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
}
