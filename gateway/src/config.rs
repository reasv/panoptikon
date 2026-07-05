use anyhow::{Context, Result};
use axum::http::Method;
use serde::Deserialize;
use serde::de::{self, SeqAccess, Visitor};
use std::{collections::BTreeMap, env, fmt, path::PathBuf};

pub const MAX_DB_NAME_LEN: usize = 64;
pub const MAX_USERNAME_LEN: usize = 64;
pub const CONFIG_PATH_ENV: &str = "GATEWAY_CONFIG_PATH";

#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    pub server: ServerConfig,
    pub upstreams: UpstreamsConfig,
    #[serde(default)]
    pub search: SearchConfig,
    #[serde(default)]
    pub jobs: JobsConfig,
    #[serde(default)]
    pub rulesets: BTreeMap<String, RuleSetConfig>,
    #[serde(default)]
    pub policies: Vec<PolicyConfig>,
    #[serde(default)]
    pub inference_local: InferenceLocalConfig,
}

/// `[inference_local]`: the in-process inferio orchestrator (design doc §3).
/// When enabled the gateway serves `/api/inference/*` locally (spawning
/// Python worker processes on demand) instead of proxying to an upstream.
#[derive(Debug, Clone, Deserialize)]
pub struct InferenceLocalConfig {
    /// Serve inference locally instead of proxying. Default: false.
    #[serde(default)]
    pub enabled: bool,
    /// Python interpreter used to spawn workers. Default: auto-detect the
    /// repo venv (`.venv/Scripts/python.exe` on Windows, `.venv/bin/python`
    /// elsewhere) relative to the working directory.
    #[serde(default)]
    pub python: Option<PathBuf>,
    /// Directories searched (in order) for impl-class modules; forwarded to
    /// workers in the spawn handshake. Empty (default) means
    /// `["src/inferio/impl", "inferio_custom"]`.
    #[serde(default)]
    pub impl_dirs: Vec<PathBuf>,
    /// Registry TOML directories, built-in first. Empty (default) means the
    /// Python default resolution: `BASE_INFERENCE_CONFIG_FOLDER` env or
    /// `src/inferio/config`, then `INFERIO_CONFIG_DIR` env or
    /// `config/inference`.
    #[serde(default)]
    pub config_dirs: Vec<PathBuf>,
    /// Entries prepended to the workers' PYTHONPATH so the `inferio_worker`
    /// package resolves in the src/ layout. Empty (default) means `["src"]`.
    #[serde(default)]
    pub pythonpath: Vec<PathBuf>,
    /// Server-wide default batch cap when neither the request nor the
    /// registry expresses an opinion (Python's `MAX_COMBINED_BATCH`).
    #[serde(default = "default_inference_max_batch")]
    pub default_max_batch: u32,
    /// TTL sweeper period in seconds (Python: 10).
    #[serde(default = "default_inference_sweep_interval_secs")]
    pub sweep_interval_secs: u64,
    /// Optional worker lifecycle deadline overrides (protocol doc defaults:
    /// handshake 30 s, load 600 s, unload grace 10 s, terminate grace 5 s).
    #[serde(default)]
    pub handshake_secs: Option<u64>,
    #[serde(default)]
    pub load_secs: Option<u64>,
    #[serde(default)]
    pub unload_grace_secs: Option<u64>,
    #[serde(default)]
    pub terminate_grace_secs: Option<u64>,
    /// Listen port override for the `inferio` subcommand only (the full
    /// gateway always serves inference on its own `server.port`). Default:
    /// `server.port`.
    #[serde(default)]
    pub port: Option<u16>,
}

fn default_inference_max_batch() -> u32 {
    32
}

fn default_inference_sweep_interval_secs() -> u64 {
    10
}

impl Default for InferenceLocalConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            python: None,
            impl_dirs: Vec::new(),
            config_dirs: Vec::new(),
            pythonpath: Vec::new(),
            default_max_batch: default_inference_max_batch(),
            sweep_interval_secs: default_inference_sweep_interval_secs(),
            handshake_secs: None,
            load_secs: None,
            unload_grace_secs: None,
            terminate_grace_secs: None,
            port: None,
        }
    }
}

impl InferenceLocalConfig {
    /// The worker interpreter: configured path or the repo venv default.
    pub fn resolved_python(&self) -> PathBuf {
        self.python.clone().unwrap_or_else(|| {
            if cfg!(windows) {
                PathBuf::from(".venv/Scripts/python.exe")
            } else {
                PathBuf::from(".venv/bin/python")
            }
        })
    }

    /// Impl-class search dirs, defaulted and absolutized (the worker
    /// handshake forwards them verbatim; workers skip missing dirs).
    pub fn resolved_impl_dirs(&self) -> Vec<PathBuf> {
        let dirs = if self.impl_dirs.is_empty() {
            vec![
                PathBuf::from("src/inferio/impl"),
                PathBuf::from("inferio_custom"),
            ]
        } else {
            self.impl_dirs.clone()
        };
        dirs.into_iter().map(absolutize).collect()
    }

    /// PYTHONPATH prepends for workers, defaulted and absolutized.
    pub fn resolved_pythonpath(&self) -> Vec<PathBuf> {
        let dirs = if self.pythonpath.is_empty() {
            vec![PathBuf::from("src")]
        } else {
            self.pythonpath.clone()
        };
        dirs.into_iter().map(absolutize).collect()
    }
}

/// Best-effort absolutization against the current working directory (keeps
/// the path as-is if the CWD cannot be resolved).
fn absolutize(path: PathBuf) -> PathBuf {
    std::path::absolute(&path).unwrap_or(path)
}

/// Host-resource limits for extraction jobs. These describe the machine the
/// gateway runs on (unlike the per-index-DB SystemConfig, which describes
/// what to run): all index DBs share the same process memory and I/O.
#[derive(Debug, Clone, Deserialize)]
pub struct JobsConfig {
    /// Concurrent input loaders (decode processes and file reads). Bounds
    /// load-phase memory to roughly this many unmeasured items and doubles
    /// as an I/O-politeness cap for network mounts.
    #[serde(default = "default_loader_concurrency")]
    pub loader_concurrency: usize,
    /// Cap on loaded-but-unfinished intermediate input data (frames, audio,
    /// rendered pages) held in memory across in-flight items. A single item
    /// larger than the whole budget still runs, but alone.
    #[serde(default = "default_intermediate_data_budget_mb")]
    pub intermediate_data_budget_mb: u64,
}

fn default_loader_concurrency() -> usize {
    8
}

fn default_intermediate_data_budget_mb() -> u64 {
    1024
}

impl Default for JobsConfig {
    fn default() -> Self {
        Self {
            loader_concurrency: default_loader_concurrency(),
            intermediate_data_budget_mb: default_intermediate_data_budget_mb(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    #[serde(default)]
    pub trust_forwarded_headers: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamsConfig {
    pub ui: UpstreamConfig,
    pub api: UpstreamConfig,
    #[serde(default)]
    pub inference: Vec<InferenceEndpointConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamConfig {
    pub base_url: String,
    #[serde(default)]
    pub local: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InferenceEndpointConfig {
    pub base_url: String,
    #[serde(default = "default_inference_weight")]
    pub weight: f64,
    #[serde(default = "default_inference_use_for_jobs")]
    pub use_for_jobs: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SearchConfig {
    #[serde(default = "default_embedding_cache_size")]
    pub embedding_cache_size: usize,
}

fn default_embedding_cache_size() -> usize {
    16
}

fn default_inference_weight() -> f64 {
    1.0
}

fn default_inference_use_for_jobs() -> bool {
    true
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            embedding_cache_size: default_embedding_cache_size(),
        }
    }
}
#[derive(Debug, Clone, Deserialize)]
pub struct RuleSetConfig {
    #[serde(default)]
    pub allow_all: bool,
    #[serde(default)]
    pub allow: Vec<RuleConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RuleConfig {
    pub methods: MethodsSpec,
    pub path: Option<String>,
    pub path_prefix: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PolicyConfig {
    pub name: String,
    #[serde(default)]
    pub ruleset: Option<String>,
    #[serde(rename = "match")]
    pub match_rule: PolicyMatch,
    pub index_db: DbPolicy,
    pub user_data_db: DbPolicy,
    #[serde(default)]
    pub identity: Option<IdentityConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PolicyMatch {
    pub hosts: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IdentityConfig {
    pub user_header: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DbPolicy {
    pub default: String,
    pub allow: AllowList,
    #[serde(default)]
    pub tenant_default: Option<String>,
    #[serde(default)]
    pub tenant_prefix_template: Option<String>,
}

#[derive(Debug, Clone)]
pub enum AllowList {
    All,
    List(Vec<String>),
}

impl AllowList {
    pub fn is_all(&self) -> bool {
        matches!(self, AllowList::All)
    }

    pub fn allows(&self, value: &str) -> bool {
        match self {
            AllowList::All => true,
            AllowList::List(items) => items.iter().any(|item| item == value),
        }
    }
}

#[derive(Debug, Clone)]
pub enum MethodsSpec {
    All,
    List(Vec<Method>),
}

impl MethodsSpec {
    pub fn allows(&self, method: &Method) -> bool {
        match self {
            MethodsSpec::All => true,
            MethodsSpec::List(items) => items.iter().any(|item| item == method),
        }
    }
}

impl Settings {
    pub fn load(config_path: Option<PathBuf>) -> Result<Self> {
        let config_path = match config_path {
            Some(path) => path,
            None => default_config_path()?,
        };
        let builder = config::Config::builder()
            .set_default("server.host", "0.0.0.0")?
            .set_default("server.port", 8080)?
            .set_default("server.trust_forwarded_headers", false)?
            .set_default("upstreams.ui.base_url", "http://127.0.0.1:6339")?
            .set_default("upstreams.api.base_url", "http://127.0.0.1:6342")?
            .set_default(
                "search.embedding_cache_size",
                default_embedding_cache_size() as i64,
            )?
            .add_source(config::File::from(config_path).required(false))
            .add_source(config::Environment::with_prefix("GATEWAY").separator("__"));

        let mut settings: Settings = builder.build()?.try_deserialize()?;
        settings.apply_env_overrides()?;
        settings.apply_inference_default();
        settings.validate()?;
        Ok(settings)
    }

    pub fn listen_addr(&self) -> String {
        format!("{}:{}", self.server.host, self.server.port)
    }

    fn apply_env_overrides(&mut self) -> Result<()> {
        if let Ok(value) = env::var("GATEWAY__SERVER_HOST") {
            self.server.host = value;
        }
        if let Ok(value) = env::var("GATEWAY__SERVER_PORT") {
            self.server.port = value
                .parse()
                .context("GATEWAY__SERVER_PORT must be a valid u16")?;
        }
        if let Ok(value) = env::var("GATEWAY__SERVER_TRUST_FORWARDED_HEADERS") {
            self.server.trust_forwarded_headers = value
                .parse()
                .context("GATEWAY__SERVER_TRUST_FORWARDED_HEADERS must be a boolean")?;
        }
        if let Ok(value) = env::var("GATEWAY__UPSTREAM_UI") {
            self.upstreams.ui.base_url = value;
        }
        if let Ok(value) = env::var("GATEWAY__UPSTREAM_UI_LOCAL") {
            self.upstreams.ui.local = value
                .parse()
                .context("GATEWAY__UPSTREAM_UI_LOCAL must be a boolean")?;
        }
        if let Ok(value) = env::var("GATEWAY__UPSTREAM_API") {
            self.upstreams.api.base_url = value;
        }
        if let Ok(value) = env::var("GATEWAY__UPSTREAM_API_LOCAL") {
            self.upstreams.api.local = value
                .parse()
                .context("GATEWAY__UPSTREAM_API_LOCAL must be a boolean")?;
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        self.validate_rulesets()?;
        self.validate_policies()?;
        self.validate_inference_endpoints()?;
        self.warn_inference_local();
        Ok(())
    }

    /// Local inference spawns workers lazily, so a missing interpreter is a
    /// warning at load time, not an error — the first model load surfaces it.
    fn warn_inference_local(&self) {
        if !self.inference_local.enabled {
            return;
        }
        let python = self.inference_local.resolved_python();
        if !python.is_file() {
            tracing::warn!(
                python = %python.display(),
                "inference_local is enabled but the worker Python interpreter \
                 was not found; model loads will fail until it exists"
            );
        }
    }

    fn validate_rulesets(&self) -> Result<()> {
        for (name, ruleset) in &self.rulesets {
            for (idx, rule) in ruleset.allow.iter().enumerate() {
                let has_path = rule
                    .path
                    .as_ref()
                    .map(|value| !value.is_empty())
                    .unwrap_or(false);
                let has_prefix = rule
                    .path_prefix
                    .as_ref()
                    .map(|value| !value.is_empty())
                    .unwrap_or(false);
                if has_path == has_prefix {
                    anyhow::bail!(
                        "ruleset '{}' rule {} must specify exactly one of path or path_prefix",
                        name,
                        idx
                    );
                }
            }
        }
        Ok(())
    }

    fn validate_policies(&self) -> Result<()> {
        for policy in &self.policies {
            if policy.match_rule.hosts.is_empty() {
                anyhow::bail!("policy '{}' must list at least one host", policy.name);
            }
            if let Some(ruleset_name) = policy.ruleset.as_deref() {
                if ruleset_name != "allow_all" && !self.rulesets.contains_key(ruleset_name) {
                    anyhow::bail!(
                        "policy '{}' references unknown ruleset '{}'",
                        policy.name,
                        ruleset_name
                    );
                }
            }

            validate_db_policy("index_db", &policy.index_db)?;
            validate_db_policy("user_data_db", &policy.user_data_db)?;
        }
        Ok(())
    }

    fn validate_inference_endpoints(&self) -> Result<()> {
        if self.upstreams.inference.is_empty() {
            anyhow::bail!("upstreams.inference must include at least one endpoint");
        }
        for (idx, endpoint) in self.upstreams.inference.iter().enumerate() {
            if endpoint.base_url.trim().is_empty() {
                anyhow::bail!("upstreams.inference[{}] base_url must not be empty", idx);
            }
            if endpoint.weight.is_nan() || endpoint.weight < 0.0 {
                anyhow::bail!("upstreams.inference[{}] weight must be >= 0", idx);
            }
        }
        Ok(())
    }
}

impl Settings {
    /// Default inference upstream when `upstreams.inference` is empty.
    ///
    /// - `inference_local.enabled = true`: synthesize a loopback entry
    ///   pointing at the gateway itself, so jobs/PQL/preload (which all talk
    ///   to `upstreams.inference[0]`) work with zero config — the gateway
    ///   serves `/api/inference/*` locally anyway. When the user configured
    ///   endpoints explicitly, they are left untouched (mixing local +
    ///   remote is allowed; entry order decides who gets jobs/search).
    /// - otherwise: fall back to the API upstream, exactly as before.
    fn apply_inference_default(&mut self) {
        if self.upstreams.inference.is_empty() {
            let base_url = if self.inference_local.enabled {
                loopback_base_url(&self.server.host, self.server.port)
            } else {
                self.upstreams.api.base_url.clone()
            };
            self.upstreams.inference.push(InferenceEndpointConfig {
                base_url,
                weight: default_inference_weight(),
                use_for_jobs: default_inference_use_for_jobs(),
            });
        }
    }
}

/// A base URL that reaches this gateway's own listener: wildcard binds map
/// to 127.0.0.1, IPv6 hosts get bracketed.
pub fn loopback_base_url(host: &str, port: u16) -> String {
    let host = match host {
        "" | "0.0.0.0" | "::" | "[::]" => "127.0.0.1",
        other => other,
    };
    if host.contains(':') && !host.starts_with('[') {
        format!("http://[{host}]:{port}")
    } else {
        format!("http://{host}:{port}")
    }
}

fn default_config_path() -> Result<PathBuf> {
    let cwd = env::current_dir().context("failed to resolve current directory")?;
    Ok(cwd.join("config").join("gateway").join("default.toml"))
}

fn validate_db_policy(label: &str, policy: &DbPolicy) -> Result<()> {
    if !is_safe_identifier(&policy.default, MAX_DB_NAME_LEN) {
        anyhow::bail!("{} default '{}' is invalid", label, policy.default);
    }
    match &policy.allow {
        AllowList::All => {
            if policy.tenant_default.is_some() || policy.tenant_prefix_template.is_some() {
                anyhow::bail!(
                    "{} allow='*' cannot be combined with tenant templates",
                    label
                );
            }
        }
        AllowList::List(items) => {
            for entry in items {
                if !is_safe_identifier(entry, MAX_DB_NAME_LEN) {
                    anyhow::bail!("{} allow entry '{}' is invalid", label, entry);
                }
            }
        }
    }

    if let AllowList::List(items) = &policy.allow {
        if !items.iter().any(|entry| entry == &policy.default) {
            anyhow::bail!(
                "{} default '{}' must appear in allow list",
                label,
                policy.default
            );
        }
    }

    if let Some(tenant_default) = &policy.tenant_default {
        if !is_safe_identifier(tenant_default, MAX_DB_NAME_LEN) {
            anyhow::bail!("{} tenant_default '{}' is invalid", label, tenant_default);
        }
        if tenant_default == &policy.default {
            anyhow::bail!("{} tenant_default must not match the global default", label);
        }
        if policy.tenant_prefix_template.is_none() {
            anyhow::bail!("{} tenant_default requires tenant_prefix_template", label);
        }
    }
    if let Some(template) = &policy.tenant_prefix_template {
        validate_prefix_template(template, label)?;
    }

    Ok(())
}

fn validate_prefix_template(template: &str, label: &str) -> Result<()> {
    if template.contains("{db}") {
        anyhow::bail!("{} prefix template must not include {{db}}", label);
    }
    let rendered = template.replace("{username}", "user");
    if !is_safe_identifier(&rendered, MAX_DB_NAME_LEN) {
        anyhow::bail!("{} prefix template '{}' is invalid", label, template);
    }
    Ok(())
}

pub fn is_safe_identifier(value: &str, max_len: usize) -> bool {
    if value.is_empty() || value.len() > max_len {
        return false;
    }
    value.bytes().all(|byte| match byte {
        b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'.' | b'_' | b'-' => true,
        _ => false,
    })
}

impl<'de> Deserialize<'de> for AllowList {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct AllowVisitor;

        impl<'de> Visitor<'de> for AllowVisitor {
            type Value = AllowList;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("'*' or a list of strings")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value == "*" {
                    Ok(AllowList::All)
                } else {
                    Err(E::custom("allow must be '*' or list"))
                }
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut items = Vec::new();
                while let Some(value) = seq.next_element::<String>()? {
                    items.push(value);
                }
                Ok(AllowList::List(items))
            }
        }

        deserializer.deserialize_any(AllowVisitor)
    }
}

impl<'de> Deserialize<'de> for MethodsSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct MethodsVisitor;

        impl<'de> Visitor<'de> for MethodsVisitor {
            type Value = MethodsSpec;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("'*' or a list of HTTP methods")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value == "*" {
                    Ok(MethodsSpec::All)
                } else {
                    Err(E::custom("methods must be '*' or list"))
                }
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut methods = Vec::new();
                while let Some(value) = seq.next_element::<String>()? {
                    let method = Method::from_bytes(value.as_bytes())
                        .map_err(|_| de::Error::custom("invalid HTTP method"))?;
                    methods.push(method);
                }
                if methods.is_empty() {
                    return Err(de::Error::custom("methods list must not be empty"));
                }
                Ok(MethodsSpec::List(methods))
            }
        }

        deserializer.deserialize_any(MethodsVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// loopback_base_url maps wildcard binds to 127.0.0.1 and brackets bare
    /// IPv6 hosts; concrete hosts pass through unchanged. This is the URL
    /// synthesized for `upstreams.inference` when local inference is enabled
    /// with no configured endpoints.
    #[test]
    fn loopback_base_url_handles_wildcards_and_ipv6() {
        assert_eq!(loopback_base_url("0.0.0.0", 8080), "http://127.0.0.1:8080");
        assert_eq!(loopback_base_url("::", 8080), "http://127.0.0.1:8080");
        assert_eq!(loopback_base_url("", 1234), "http://127.0.0.1:1234");
        assert_eq!(
            loopback_base_url("127.0.0.1", 6342),
            "http://127.0.0.1:6342"
        );
        assert_eq!(loopback_base_url("myhost", 80), "http://myhost:80");
        assert_eq!(loopback_base_url("::1", 8080), "http://[::1]:8080");
        assert_eq!(loopback_base_url("[::1]", 8080), "http://[::1]:8080");
    }

    /// Config resolution rule for local inference (documented in AGENTS.md):
    /// `inference_local.enabled = true` with an empty `upstreams.inference`
    /// synthesizes a loopback self-entry (host/port of the gateway itself)
    /// so jobs/PQL/preload work with zero config; when endpoints are
    /// configured explicitly they are left exactly as-is; and when local
    /// inference is disabled the old default (API upstream) still applies.
    #[test]
    fn inference_local_synthesizes_loopback_upstream() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gw.toml");

        // Enabled + no inference endpoints -> loopback self entry.
        std::fs::write(
            &path,
            r#"
[server]
host = "127.0.0.1"
port = 9155

[upstreams.ui]
base_url = "http://127.0.0.1:6339"

[upstreams.api]
base_url = "http://127.0.0.1:6342"

[inference_local]
enabled = true
"#,
        )
        .unwrap();
        let settings = Settings::load(Some(path.clone())).unwrap();
        assert_eq!(settings.upstreams.inference.len(), 1);
        assert_eq!(
            settings.upstreams.inference[0].base_url,
            "http://127.0.0.1:9155"
        );
        assert!(settings.upstreams.inference[0].use_for_jobs);

        // Enabled + explicit endpoints -> left untouched (mix local+remote).
        std::fs::write(
            &path,
            r#"
[server]
host = "127.0.0.1"
port = 9155

[upstreams.ui]
base_url = "http://127.0.0.1:6339"

[upstreams.api]
base_url = "http://127.0.0.1:6342"

[inference_local]
enabled = true

[[upstreams.inference]]
base_url = "http://gpu-box:8080"
"#,
        )
        .unwrap();
        let settings = Settings::load(Some(path.clone())).unwrap();
        assert_eq!(settings.upstreams.inference.len(), 1);
        assert_eq!(
            settings.upstreams.inference[0].base_url,
            "http://gpu-box:8080"
        );

        // Disabled + no endpoints -> legacy default: the API upstream.
        std::fs::write(
            &path,
            r#"
[server]
host = "127.0.0.1"
port = 9155

[upstreams.ui]
base_url = "http://127.0.0.1:6339"

[upstreams.api]
base_url = "http://127.0.0.1:6342"
"#,
        )
        .unwrap();
        let settings = Settings::load(Some(path)).unwrap();
        assert_eq!(
            settings.upstreams.inference[0].base_url,
            "http://127.0.0.1:6342"
        );
        assert!(!settings.inference_local.enabled);
        assert_eq!(settings.inference_local.default_max_batch, 32);
        assert_eq!(settings.inference_local.sweep_interval_secs, 10);
    }
}
