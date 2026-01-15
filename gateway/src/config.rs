use anyhow::{Context, Result};
use axum::http::Method;
use serde::de::{self, SeqAccess, Visitor};
use serde::Deserialize;
use std::{collections::BTreeMap, env, fmt, path::PathBuf};

pub const MAX_DB_NAME_LEN: usize = 64;
pub const MAX_USERNAME_LEN: usize = 64;
pub const CONFIG_PATH_ENV: &str = "GATEWAY_CONFIG_PATH";

#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    pub server: ServerConfig,
    pub upstreams: UpstreamsConfig,
    #[serde(default)]
    pub routes: Vec<RouteConfig>,
    #[serde(default)]
    pub rulesets: BTreeMap<String, RuleSetConfig>,
    #[serde(default)]
    pub policies: Vec<PolicyConfig>,
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
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamConfig {
    pub base_url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RouteConfig {
    pub name: String,
    pub path: Option<String>,
    pub path_prefix: Option<String>,
    pub upstream: String,
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
    pub defaults: DbDefaults,
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
pub struct DbDefaults {
    pub index_db: String,
    pub user_data_db: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DbPolicy {
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
            .add_source(config::File::from(config_path).required(false))
            .add_source(config::Environment::with_prefix("GATEWAY").separator("__"));

        let mut settings: Settings = builder.build()?.try_deserialize()?;
        settings.apply_env_overrides()?;
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
        if let Ok(value) = env::var("GATEWAY__UPSTREAM_API") {
            self.upstreams.api.base_url = value;
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        self.validate_rulesets()?;
        self.validate_policies()?;
        Ok(())
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

            validate_db_defaults(&policy.defaults, &policy.name)?;
            validate_db_policy("index_db", &policy.index_db, &policy.defaults.index_db)?;
            validate_db_policy(
                "user_data_db",
                &policy.user_data_db,
                &policy.defaults.user_data_db,
            )?;
        }
        Ok(())
    }
}

fn default_config_path() -> Result<PathBuf> {
    let cwd = env::current_dir().context("failed to resolve current directory")?;
    Ok(cwd.join("config").join("gateway").join("default.toml"))
}

fn validate_db_defaults(defaults: &DbDefaults, policy_name: &str) -> Result<()> {
    if !is_safe_identifier(&defaults.index_db, MAX_DB_NAME_LEN) {
        anyhow::bail!("policy '{}' default index_db is invalid", policy_name);
    }
    if !is_safe_identifier(&defaults.user_data_db, MAX_DB_NAME_LEN) {
        anyhow::bail!("policy '{}' default user_data_db is invalid", policy_name);
    }
    Ok(())
}

fn validate_db_policy(label: &str, policy: &DbPolicy, default_value: &str) -> Result<()> {
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
        if !items.iter().any(|entry| entry == default_value) {
            anyhow::bail!(
                "{} default '{}' must appear in allow list",
                label,
                default_value
            );
        }
    }

    if let Some(tenant_default) = &policy.tenant_default {
        if !is_safe_identifier(tenant_default, MAX_DB_NAME_LEN) {
            anyhow::bail!("{} tenant_default '{}' is invalid", label, tenant_default);
        }
        if tenant_default == default_value {
            anyhow::bail!(
                "{} tenant_default must not match the global default",
                label
            );
        }
        if policy.tenant_prefix_template.is_none() {
            anyhow::bail!(
                "{} tenant_default requires tenant_prefix_template",
                label
            );
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
