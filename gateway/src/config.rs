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
    /// Prewarm pool policy (design §8: one parked, `prepare()`-warmed worker
    /// per impl class; no TTL by design).
    #[serde(default)]
    pub prewarm: PrewarmSettings,
}

/// `[inference_local.prewarm]` (design §8, policy decided 2026-07-05).
#[derive(Debug, Clone, Deserialize)]
pub struct PrewarmSettings {
    /// Master switch for the whole prewarm pool. Default: true.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Lazy warm: after a model of class C loads, keep one warm C worker for
    /// next time (respawn-on-claim), unless the request carried
    /// `prewarm=false`. Default: true.
    #[serde(default = "default_true")]
    pub lazy: bool,
    /// Impl classes warmed unconditionally at startup — the only eager
    /// mechanism available to the standalone `inferio` subcommand, which may
    /// have no index DBs. Default: empty.
    #[serde(default)]
    pub always_warm: Vec<String>,
}

fn default_true() -> bool {
    true
}

impl Default for PrewarmSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            lazy: true,
            always_warm: Vec::new(),
        }
    }
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
            prewarm: PrewarmSettings::default(),
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
    ///
    /// The default custom dir honors `INFERIO_CUSTOM_IMPL_PATH` like the
    /// Python server (utils.py: env or `./inferio_custom`). Explicitly
    /// configured `impl_dirs` win over the env var. Note there is no
    /// local-mode analogue of `INFERIO_ALLOW_BUILT_IN_OVERRIDE`: dirs are
    /// searched in order (built-ins first, customs later) and the first
    /// module providing a matching `name()` wins.
    pub fn resolved_impl_dirs(&self) -> Vec<PathBuf> {
        let custom_env = env::var_os("INFERIO_CUSTOM_IMPL_PATH");
        let dirs = if self.impl_dirs.is_empty() {
            let custom = custom_env
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("inferio_custom"));
            vec![PathBuf::from("src/inferio/impl"), custom]
        } else {
            if custom_env.is_some() {
                tracing::warn!(
                    "INFERIO_CUSTOM_IMPL_PATH is set but [inference_local].impl_dirs is \
                     configured explicitly; the environment variable is ignored"
                );
            }
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
    pub ui: UiUpstreamConfig,
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

/// `[upstreams.ui]`: where the Next.js frontend lives. With `local = true`
/// the gateway also *runs* it: npm install / `next build` when stale, then
/// a supervised `next start` bound to the host/port parsed from `base_url`
/// (single source of truth — the proxy keeps using `base_url` unchanged).
#[derive(Debug, Clone, Deserialize)]
pub struct UiUpstreamConfig {
    pub base_url: String,
    /// Spawn and supervise the production UI server. Default: false.
    #[serde(default)]
    pub local: bool,
    /// Path to the panoptikon-ui checkout (the user manages the clone).
    /// Required when `local = true`; relative paths resolve against CWD.
    #[serde(default)]
    pub dir: Option<PathBuf>,
    /// Explicit node binary. Default resolution: the repo venv's
    /// nodejs-wheel node, then `node` from PATH (see `ui::resolve_node`).
    #[serde(default)]
    pub node: Option<PathBuf>,
    /// When to run `next build`. Default: auto (build-staleness check).
    #[serde(default)]
    pub build: UiBuildPolicy,
}

/// `[upstreams.ui].build`: `next build` policy for local UI mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UiBuildPolicy {
    /// Build when `.next/BUILD_ID` is missing or older than the checkout's
    /// latest git commit (ported from the Python searchui router).
    #[default]
    Auto,
    /// Build on every startup.
    Always,
    /// Never build; `next start` fails visibly if there is no build.
    Never,
}

impl UiUpstreamConfig {
    /// The host/port the spawned `next start` binds to, parsed from
    /// `base_url`. Local mode requires a plain loopback `http://host:port`
    /// URL: the gateway proxies to that same URL, so a path/query or a
    /// non-loopback host means the config cannot describe a server this
    /// process spawns on this machine.
    pub fn local_bind_addr(&self) -> Result<(String, u16)> {
        let parsed = url::Url::parse(&self.base_url).with_context(|| {
            format!(
                "upstreams.ui.base_url '{}' is not a valid URL",
                self.base_url
            )
        })?;
        let describe = |problem: &str| {
            format!(
                "upstreams.ui.local = true requires base_url to be a plain loopback \
                 http://host:port URL (it doubles as the spawned UI server's bind \
                 address), but '{}' {problem}",
                self.base_url
            )
        };
        if parsed.scheme() != "http" {
            anyhow::bail!(describe("does not use the http scheme"));
        }
        if !matches!(parsed.path(), "" | "/") || parsed.query().is_some() {
            anyhow::bail!(describe("has a path or query"));
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            anyhow::bail!(describe("has userinfo"));
        }
        let host = match parsed.host() {
            Some(url::Host::Domain(domain)) => domain.to_string(),
            Some(url::Host::Ipv4(addr)) => addr.to_string(),
            Some(url::Host::Ipv6(addr)) => addr.to_string(),
            None => anyhow::bail!(describe("has no host")),
        };
        let loopback = host == "localhost"
            || host
                .parse::<std::net::IpAddr>()
                .map(|ip| ip.is_loopback())
                .unwrap_or(false);
        if !loopback {
            anyhow::bail!(describe("has a non-loopback host"));
        }
        let port = parsed
            .port_or_known_default()
            .expect("http URLs always have a known default port");
        Ok((host, port))
    }
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
        let loopback_synthesized = settings.apply_inference_default();
        settings.validate(loopback_synthesized)?;
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

    /// `loopback_synthesized` is true when `apply_inference_default` just
    /// synthesized a loopback self-call inference upstream, which must be
    /// checked against the policies (see
    /// [`Settings::validate_loopback_inference_policy`]).
    fn validate(&self, loopback_synthesized: bool) -> Result<()> {
        self.validate_rulesets()?;
        self.validate_policies()?;
        self.validate_inference_endpoints()?;
        self.validate_ui()?;
        if loopback_synthesized {
            self.validate_loopback_inference_policy()?;
        }
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

    /// When a loopback inference upstream was synthesized, the gateway's
    /// own inference clients (extraction jobs, PQL embedding search, cron
    /// preload) call back into this gateway's listener — through the policy
    /// layer. Verify at config-load time that a policy matches the
    /// synthesized host and that its ruleset admits the inference routes;
    /// otherwise every self-call would 403 silently at runtime, breaking
    /// all jobs/search/preload with nothing in the config to hint why.
    fn validate_loopback_inference_policy(&self) -> Result<()> {
        let host = crate::policy::normalize_host(loopback_host(&self.server.host));
        let base_url = &self.upstreams.inference[0].base_url;
        let Some(policy) = crate::policy::select_policy(self, Some(&host)) else {
            anyhow::bail!(
                "inference_local.enabled synthesized a loopback inference upstream ({base_url}) \
                 because upstreams.inference is empty, but no policy matches host '{host}' — \
                 every internal inference call (jobs, PQL search, preload) would be rejected \
                 with 403. Add a [[policies]] entry whose match.hosts includes '{host}', set \
                 [[upstreams.inference]] explicitly, or bind server.host to a policy-covered \
                 address"
            );
        };
        for (method, path, what) in [
            (
                Method::POST,
                "/api/inference/predict/group/id",
                "POST /api/inference/predict/*",
            ),
            (
                Method::PUT,
                "/api/inference/load/group/id",
                "PUT /api/inference/load/*",
            ),
            (Method::GET, "/api/inference/metadata", "GET /api/inference/metadata"),
        ] {
            if !crate::policy::ruleset_allows(self, policy, &method, path) {
                anyhow::bail!(
                    "inference_local.enabled synthesized a loopback inference upstream \
                     ({base_url}) for host '{host}', but policy '{}' (ruleset {:?}) denies \
                     {what}, which internal inference calls (jobs, PQL search, preload) \
                     require. Allow the /api/inference routes in that ruleset, set \
                     [[upstreams.inference]] explicitly, or add a dedicated policy for \
                     '{host}'",
                    policy.name,
                    policy.ruleset,
                );
            }
        }
        Ok(())
    }

    /// Local UI mode needs a checkout to run from and a bind address it can
    /// derive from `base_url`; both are config mistakes best caught at load.
    fn validate_ui(&self) -> Result<()> {
        if !self.upstreams.ui.local {
            return Ok(());
        }
        if self.upstreams.ui.dir.is_none() {
            anyhow::bail!(
                "upstreams.ui.local = true requires upstreams.ui.dir (path to the \
                 panoptikon-ui checkout)"
            );
        }
        self.upstreams.ui.local_bind_addr()?;
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
    ///
    /// Returns true when a loopback self entry was synthesized, so
    /// `validate` can verify the policies actually admit the self-calls.
    fn apply_inference_default(&mut self) -> bool {
        if self.upstreams.inference.is_empty() {
            let local = self.inference_local.enabled;
            let base_url = if local {
                loopback_base_url(&self.server.host, self.server.port)
            } else {
                self.upstreams.api.base_url.clone()
            };
            self.upstreams.inference.push(InferenceEndpointConfig {
                base_url,
                weight: default_inference_weight(),
                use_for_jobs: default_inference_use_for_jobs(),
            });
            return local;
        }
        false
    }
}

/// A base URL that reaches this gateway's own listener: wildcard binds map
/// to the matching loopback address, IPv6 hosts get bracketed.
pub fn loopback_base_url(host: &str, port: u16) -> String {
    let host = loopback_host(host);
    if host.contains(':') && !host.starts_with('[') {
        format!("http://[{host}]:{port}")
    } else {
        format!("http://{host}:{port}")
    }
}

/// The concrete host a synthesized loopback URL points at. IPv6 wildcards
/// map to `::1`, not 127.0.0.1: a `::` listener is IPV6_V6ONLY by default
/// on Windows, so an IPv4 loopback address would never reach it.
fn loopback_host(host: &str) -> &str {
    match host {
        "" | "0.0.0.0" => "127.0.0.1",
        "::" | "[::]" => "::1",
        other => other,
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

    /// loopback_base_url maps IPv4 wildcard binds to 127.0.0.1, IPv6
    /// wildcards to [::1] (an IPv6 wildcard listener is IPV6_V6ONLY by
    /// default on Windows, so 127.0.0.1 would never reach it), and brackets
    /// bare IPv6 hosts; concrete hosts pass through unchanged. This is the
    /// URL synthesized for `upstreams.inference` when local inference is
    /// enabled with no configured endpoints.
    #[test]
    fn loopback_base_url_handles_wildcards_and_ipv6() {
        assert_eq!(loopback_base_url("0.0.0.0", 8080), "http://127.0.0.1:8080");
        assert_eq!(loopback_base_url("::", 8080), "http://[::1]:8080");
        assert_eq!(loopback_base_url("[::]", 8080), "http://[::1]:8080");
        assert_eq!(loopback_base_url("", 1234), "http://127.0.0.1:1234");
        assert_eq!(
            loopback_base_url("127.0.0.1", 6342),
            "http://127.0.0.1:6342"
        );
        assert_eq!(loopback_base_url("myhost", 80), "http://myhost:80");
        assert_eq!(loopback_base_url("::1", 8080), "http://[::1]:8080");
        assert_eq!(loopback_base_url("[::1]", 8080), "http://[::1]:8080");
    }

    /// A minimal policy block matching the given hosts with no ruleset
    /// restriction, for tests that need config load to pass the loopback
    /// self-call policy validation.
    fn allow_all_policy_toml(hosts: &str) -> String {
        format!(
            r#"
[[policies]]
name = "test"

[policies.match]
hosts = [{hosts}]

[policies.index_db]
default = "default"
allow = "*"

[policies.user_data_db]
default = "default"
allow = "*"
"#
        )
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

        // Enabled + no inference endpoints -> loopback self entry (a policy
        // covering the loopback host is required — self-calls go through
        // the policy layer and are validated at config load).
        std::fs::write(
            &path,
            format!(
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
{}"#,
                allow_all_policy_toml(r#""localhost", "127.0.0.1""#)
            ),
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

    /// A synthesized loopback inference upstream must be reachable through
    /// the policy layer: binding a LAN address while only localhost-only
    /// policies exist fails config load with the offending host named
    /// (instead of silently 403ing every job/PQL/preload self-call at
    /// runtime), and a matching policy whose ruleset denies the inference
    /// routes fails too. Explicit upstreams skip the check entirely.
    #[test]
    fn synthesized_loopback_upstream_requires_matching_policy() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gw.toml");
        let base = r#"
[upstreams.ui]
base_url = "http://127.0.0.1:6339"

[upstreams.api]
base_url = "http://127.0.0.1:6342"

[inference_local]
enabled = true
"#;

        // LAN bind + localhost-only policy -> no policy matches the
        // synthesized host: config load fails and names the host.
        std::fs::write(
            &path,
            format!(
                "[server]\nhost = \"192.168.1.5\"\nport = 9155\n{base}{}",
                allow_all_policy_toml(r#""localhost", "127.0.0.1""#)
            ),
        )
        .unwrap();
        let err = Settings::load(Some(path.clone())).expect_err("no policy for the LAN host");
        let text = format!("{err:#}");
        assert!(
            text.contains("192.168.1.5"),
            "error names the unmatched host: {text}"
        );
        assert!(
            text.contains("[[policies]]") && text.contains("upstreams.inference"),
            "error explains the remedies: {text}"
        );

        // Matching policy, but its ruleset denies the inference routes ->
        // config load fails and names the policy.
        std::fs::write(
            &path,
            format!(
                r#"[server]
host = "127.0.0.1"
port = 9155
{base}
[rulesets.search_only]
allow = [{{ methods = ["GET"], path_prefix = "/api/search/" }}]

[[policies]]
name = "locked_down"
ruleset = "search_only"

[policies.match]
hosts = ["localhost", "127.0.0.1"]

[policies.index_db]
default = "default"
allow = "*"

[policies.user_data_db]
default = "default"
allow = "*"
"#
            ),
        )
        .unwrap();
        let err = Settings::load(Some(path.clone())).expect_err("ruleset denies inference routes");
        let text = format!("{err:#}");
        assert!(
            text.contains("locked_down") && text.contains("/api/inference"),
            "error names the policy and the denied surface: {text}"
        );

        // Same LAN bind + localhost-only policy, but an explicit inference
        // upstream: nothing is synthesized, so no check applies.
        std::fs::write(
            &path,
            format!(
                "[server]\nhost = \"192.168.1.5\"\nport = 9155\n{base}{}\n\
                 [[upstreams.inference]]\nbase_url = \"http://gpu-box:8080\"\n",
                allow_all_policy_toml(r#""localhost", "127.0.0.1""#)
            ),
        )
        .unwrap();
        Settings::load(Some(path)).expect("explicit upstreams skip the loopback policy check");
    }

    /// `[upstreams.ui]` local-mode keys: defaults are off/empty, the new
    /// keys parse, and `local = true` derives the spawned server's bind
    /// address from `base_url` (single source of truth).
    #[test]
    fn ui_local_config_parses() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gw.toml");
        std::fs::write(
            &path,
            r#"
[server]
host = "127.0.0.1"
port = 9155

[upstreams.ui]
base_url = "http://127.0.0.1:6339"
local = true
dir = "../panoptikon-ui"
node = "custom/node.exe"
build = "always"

[upstreams.api]
base_url = "http://127.0.0.1:6342"
"#,
        )
        .unwrap();
        let settings = Settings::load(Some(path.clone())).unwrap();
        let ui = &settings.upstreams.ui;
        assert!(ui.local);
        assert_eq!(ui.dir.as_deref(), Some(std::path::Path::new("../panoptikon-ui")));
        assert_eq!(ui.node.as_deref(), Some(std::path::Path::new("custom/node.exe")));
        assert_eq!(ui.build, UiBuildPolicy::Always);
        assert_eq!(
            ui.local_bind_addr().unwrap(),
            ("127.0.0.1".to_string(), 6339)
        );

        // Defaults: local off, auto build, no dir/node.
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
        let ui = &settings.upstreams.ui;
        assert!(!ui.local);
        assert!(ui.dir.is_none() && ui.node.is_none());
        assert_eq!(ui.build, UiBuildPolicy::Auto);
    }

    /// Local UI mode fails config load without a checkout dir, and with a
    /// `base_url` that cannot serve as the spawned server's bind address
    /// (non-loopback host, path/query, non-http scheme). IPv6 loopback and
    /// `localhost` pass, and the default port is implied.
    #[test]
    fn ui_local_validation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gw.toml");
        let write = |ui_block: &str| {
            std::fs::write(
                &path,
                format!(
                    r#"
[server]
host = "127.0.0.1"
port = 9155

[upstreams.ui]
{ui_block}

[upstreams.api]
base_url = "http://127.0.0.1:6342"
"#
                ),
            )
            .unwrap();
        };

        write(r#"base_url = "http://127.0.0.1:6339"
local = true"#);
        let err = Settings::load(Some(path.clone())).expect_err("dir is required");
        assert!(format!("{err:#}").contains("upstreams.ui.dir"), "{err:#}");

        for bad in [
            r#"base_url = "http://192.168.1.5:6339""#,
            r#"base_url = "http://127.0.0.1:6339/ui""#,
            r#"base_url = "https://127.0.0.1:6339""#,
        ] {
            write(&format!("{bad}\nlocal = true\ndir = \"ui\""));
            let err = Settings::load(Some(path.clone())).expect_err(bad);
            assert!(
                format!("{err:#}").contains("plain loopback"),
                "{bad}: {err:#}"
            );
        }

        // Same URLs are fine while local mode is off.
        write(r#"base_url = "http://192.168.1.5:6339/ui""#);
        Settings::load(Some(path.clone())).expect("proxy-only ui is unrestricted");

        write(r#"base_url = "http://[::1]:6339"
local = true
dir = "ui""#);
        let settings = Settings::load(Some(path.clone())).unwrap();
        assert_eq!(
            settings.upstreams.ui.local_bind_addr().unwrap(),
            ("::1".to_string(), 6339)
        );

        write(r#"base_url = "http://localhost"
local = true
dir = "ui""#);
        let settings = Settings::load(Some(path)).unwrap();
        assert_eq!(
            settings.upstreams.ui.local_bind_addr().unwrap(),
            ("localhost".to_string(), 80)
        );
    }

    /// impl_dirs defaulting honors INFERIO_CUSTOM_IMPL_PATH exactly like
    /// the Python server (utils.py: env var, default ./inferio_custom);
    /// explicitly configured impl_dirs win and the env var is ignored.
    #[test]
    fn impl_dirs_default_honors_custom_impl_path_env() {
        let mut local = InferenceLocalConfig::default();

        // Without the env var: the Python default pair.
        let dirs = local.resolved_impl_dirs();
        assert_eq!(dirs.len(), 2);
        assert!(dirs[0].ends_with("src/inferio/impl"), "got {dirs:?}");
        assert!(dirs[1].ends_with("inferio_custom"), "got {dirs:?}");

        // With the env var: it replaces the custom-dir default.
        unsafe { env::set_var("INFERIO_CUSTOM_IMPL_PATH", "my/custom_impls") };
        let dirs = local.resolved_impl_dirs();
        unsafe { env::remove_var("INFERIO_CUSTOM_IMPL_PATH") };
        assert_eq!(dirs.len(), 2);
        assert!(dirs[0].ends_with("src/inferio/impl"), "got {dirs:?}");
        assert!(dirs[1].ends_with("my/custom_impls"), "got {dirs:?}");

        // Explicit impl_dirs win over the env var (warned, not honored).
        local.impl_dirs = vec![PathBuf::from("explicit_dir")];
        unsafe { env::set_var("INFERIO_CUSTOM_IMPL_PATH", "my/custom_impls") };
        let dirs = local.resolved_impl_dirs();
        unsafe { env::remove_var("INFERIO_CUSTOM_IMPL_PATH") };
        assert_eq!(dirs.len(), 1);
        assert!(dirs[0].ends_with("explicit_dir"), "got {dirs:?}");
    }
}
