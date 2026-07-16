use anyhow::{Context as _, Result, bail};
use panoptikon_config::{DotenvDocument, TomlDocument, whole_value_env_binding};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    net::{Ipv4Addr, TcpListener},
    path::Path,
};

const LAN_ENDPOINT: &str = "lan";
const LAN_POLICY: &str = "desktop_lan";

#[derive(Debug, Clone, Serialize)]
pub struct ConfigField<T> {
    pub value: T,
    pub source: ConfigFieldSource,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConfigFieldSource {
    Toml,
    Environment { variable: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerConfigurationView {
    pub revision: String,
    pub local_port: ConfigField<u16>,
    pub lan: LanConfigurationView,
    pub performance: PerformanceConfigurationView,
    pub databases: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LanConfigurationView {
    pub mode: LanMode,
    pub port: u16,
    pub port_source: ConfigFieldSource,
    pub allowed_databases: Option<Vec<String>>,
    pub default_database: String,
    pub explanation: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LanMode {
    Disabled,
    Managed,
    Custom,
}

#[derive(Debug, Clone, Serialize)]
pub struct PerformanceConfigurationView {
    pub prewarm_enabled: ConfigField<bool>,
    pub prewarm_lazy: ConfigField<bool>,
    pub loader_concurrency: ConfigField<u64>,
    pub intermediate_data_budget_mb: ConfigField<u64>,
    pub embedding_cache_size: ConfigField<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfigurationUpdate {
    pub revision: String,
    pub local_port: u16,
    pub lan: LanConfigurationUpdate,
    pub performance: PerformanceConfigurationUpdate,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LanConfigurationUpdate {
    pub enabled: bool,
    pub port: u16,
    /// `None` means every database; `Some` is an explicit non-empty allowlist.
    pub allowed_databases: Option<Vec<String>>,
    pub default_database: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PerformanceConfigurationUpdate {
    pub prewarm_enabled: bool,
    pub prewarm_lazy: bool,
    pub loader_concurrency: u64,
    pub intermediate_data_budget_mb: u64,
    pub embedding_cache_size: u64,
}

pub fn load(server_root: &Path, config_path: &Path) -> Result<ServerConfigurationView> {
    let source = fs::read_to_string(config_path).with_context(|| {
        format!(
            "failed to read Server configuration '{}'",
            config_path.display()
        )
    })?;
    let value: toml::Value = toml::from_str(&source)
        .with_context(|| format!("invalid Server configuration '{}'", config_path.display()))?;
    let env_path = server_root.join(".env");
    let dotenv_source = fs::read_to_string(&env_path).unwrap_or_default();
    let environment = environment_values(&env_path)?;
    view_from_value(server_root, &source, &dotenv_source, &value, &environment)
}

/// Resolve the primary listener before the sidecar is reachable, including a
/// whole-value template backed by Desktop's managed `.env` file.
pub fn effective_local_port(server_root: &Path, config_path: &Path, fallback: u16) -> Result<u16> {
    if !config_path.exists() {
        return Ok(fallback);
    }
    let source = fs::read_to_string(config_path)?;
    let value: toml::Value = toml::from_str(&source)?;
    let Some(raw) = lookup(&value, &["server", "port"]) else {
        return Ok(fallback);
    };
    Ok(resolve_value(raw, &environment_values(&server_root.join(".env"))?)?.0)
}

pub fn save(
    server_root: &Path,
    config_path: &Path,
    active_local_port: Option<u16>,
    update: &ServerConfigurationUpdate,
) -> Result<ServerConfigurationView> {
    validate_update(update)?;
    let source = fs::read_to_string(config_path).with_context(|| {
        format!(
            "failed to read Server configuration '{}'",
            config_path.display()
        )
    })?;
    let before: toml::Value = toml::from_str(&source)
        .with_context(|| format!("invalid Server configuration '{}'", config_path.display()))?;
    let env_path = server_root.join(".env");
    let dotenv_source = fs::read_to_string(&env_path).unwrap_or_default();
    let environment = environment_values(&env_path)?;
    let current = view_from_value(server_root, &source, &dotenv_source, &before, &environment)?;
    if current.revision != update.revision {
        bail!("Server configuration changed on disk; reload it before saving");
    }
    if current.lan.mode == LanMode::Custom {
        let requested_change = update.lan.enabled
            || update.lan.port != current.lan.port
            || update.lan.allowed_databases != current.lan.allowed_databases
            || update.lan.default_database != current.lan.default_database;
        if requested_change {
            bail!(
                "the existing LAN endpoint or policy uses advanced settings; edit the TOML file directly"
            );
        }
    }

    preflight_ports(active_local_port, &current, update)?;

    let mut after = before.clone();
    let mut env_updates = BTreeMap::new();
    if update.local_port != current.local_port.value {
        set_env_aware(
            &mut after,
            &["server", "port"],
            toml::Value::Integer(update.local_port.into()),
            update.local_port.to_string(),
            &mut env_updates,
        )?;
    }
    if update.performance.prewarm_enabled != current.performance.prewarm_enabled.value {
        set_env_aware(
            &mut after,
            &["inference_local", "prewarm", "enabled"],
            toml::Value::Boolean(update.performance.prewarm_enabled),
            update.performance.prewarm_enabled.to_string(),
            &mut env_updates,
        )?;
    }
    if update.performance.prewarm_lazy != current.performance.prewarm_lazy.value {
        set_env_aware(
            &mut after,
            &["inference_local", "prewarm", "lazy"],
            toml::Value::Boolean(update.performance.prewarm_lazy),
            update.performance.prewarm_lazy.to_string(),
            &mut env_updates,
        )?;
    }
    if update.performance.loader_concurrency != current.performance.loader_concurrency.value {
        set_env_aware(
            &mut after,
            &["jobs", "loader_concurrency"],
            toml::Value::Integer(update.performance.loader_concurrency as i64),
            update.performance.loader_concurrency.to_string(),
            &mut env_updates,
        )?;
    }
    if update.performance.intermediate_data_budget_mb
        != current.performance.intermediate_data_budget_mb.value
    {
        set_env_aware(
            &mut after,
            &["jobs", "intermediate_data_budget_mb"],
            toml::Value::Integer(update.performance.intermediate_data_budget_mb as i64),
            update.performance.intermediate_data_budget_mb.to_string(),
            &mut env_updates,
        )?;
    }
    if update.performance.embedding_cache_size != current.performance.embedding_cache_size.value {
        set_env_aware(
            &mut after,
            &["search", "embedding_cache_size"],
            toml::Value::Integer(update.performance.embedding_cache_size as i64),
            update.performance.embedding_cache_size.to_string(),
            &mut env_updates,
        )?;
    }
    let lan_changed = match current.lan.mode {
        LanMode::Disabled => update.lan.enabled,
        LanMode::Managed => {
            !update.lan.enabled
                || update.lan.port != current.lan.port
                || update.lan.allowed_databases != current.lan.allowed_databases
                || update.lan.default_database != current.lan.default_database
        }
        LanMode::Custom => false,
    };
    if lan_changed {
        update_managed_lan(
            &mut after,
            &update.lan,
            (current.lan.mode == LanMode::Managed).then_some(current.lan.port),
            &mut env_updates,
        )?;
    }

    let mut document = TomlDocument::parse(&source)?;
    document.patch_values(&before, &after)?;
    let rendered = document.to_string();
    let mut dotenv = DotenvDocument::parse(&dotenv_source);
    dotenv.apply(&env_updates, &BTreeSet::new());
    let rendered_dotenv = dotenv.to_string();

    if rendered != source {
        panoptikon_config::atomic_write(config_path, rendered.as_bytes())?;
    }
    if rendered_dotenv != dotenv_source {
        if let Err(error) =
            panoptikon_config::atomic_write_private(&env_path, rendered_dotenv.as_bytes())
        {
            if rendered != source {
                let _ = panoptikon_config::atomic_write(config_path, source.as_bytes());
            }
            return Err(error).context("failed to commit environment-backed settings");
        }
    }
    load(server_root, config_path)
}

fn view_from_value(
    server_root: &Path,
    source: &str,
    dotenv_source: &str,
    value: &toml::Value,
    environment: &BTreeMap<String, String>,
) -> Result<ServerConfigurationView> {
    let local_port = resolved_field(value, &["server", "port"], 6342, environment)?;
    let lan = inspect_lan(value, environment)?;
    let performance = PerformanceConfigurationView {
        prewarm_enabled: resolved_field(
            value,
            &["inference_local", "prewarm", "enabled"],
            true,
            environment,
        )?,
        prewarm_lazy: resolved_field(
            value,
            &["inference_local", "prewarm", "lazy"],
            true,
            environment,
        )?,
        loader_concurrency: resolved_field(value, &["jobs", "loader_concurrency"], 8, environment)?,
        intermediate_data_budget_mb: resolved_field(
            value,
            &["jobs", "intermediate_data_budget_mb"],
            1024,
            environment,
        )?,
        embedding_cache_size: resolved_field(
            value,
            &["search", "embedding_cache_size"],
            1024,
            environment,
        )?,
    };
    let data_folder = resolved_string(value, &["data_folder"], "data", environment)?;
    let mut databases = database_names(&server_root.join(data_folder));
    if !databases.contains(&lan.default_database) {
        databases.push(lan.default_database.clone());
    }
    databases.sort();
    databases.dedup();
    Ok(ServerConfigurationView {
        revision: revision(source, dotenv_source),
        local_port,
        lan,
        performance,
        databases,
    })
}

fn validate_update(update: &ServerConfigurationUpdate) -> Result<()> {
    if update.local_port == 0 || (update.lan.enabled && update.lan.port == 0) {
        bail!("ports must be between 1 and 65535");
    }
    if update.lan.enabled && update.local_port == update.lan.port {
        bail!("the local and LAN listeners need different ports");
    }
    if !(1..=256).contains(&update.performance.loader_concurrency) {
        bail!("loader concurrency must be between 1 and 256");
    }
    if !(64..=1_048_576).contains(&update.performance.intermediate_data_budget_mb) {
        bail!("intermediate-data memory must be between 64 MiB and 1 TiB");
    }
    if update.performance.embedding_cache_size > 65_536 {
        bail!("embedding cache size must be at most 65536");
    }
    if update.lan.enabled {
        if let Some(databases) = &update.lan.allowed_databases {
            let databases = databases
                .iter()
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .collect::<BTreeSet<_>>();
            if databases.is_empty() {
                bail!("choose at least one LAN database, or allow all databases");
            }
            if !databases.contains(update.lan.default_database.trim()) {
                bail!("the default LAN database must be in the LAN allowlist");
            }
        }
    }
    Ok(())
}

fn preflight_ports(
    active_local_port: Option<u16>,
    current: &ServerConfigurationView,
    update: &ServerConfigurationUpdate,
) -> Result<()> {
    if active_local_port != Some(update.local_port) {
        TcpListener::bind((Ipv4Addr::LOCALHOST, update.local_port)).with_context(|| {
            format!(
                "local port {} is unavailable; choose another port",
                update.local_port
            )
        })?;
    }
    let current_lan_unchanged = current.lan.mode == LanMode::Managed
        && update.lan.enabled
        && current.lan.port == update.lan.port
        && active_local_port.is_some();
    if update.lan.enabled && !current_lan_unchanged {
        TcpListener::bind((Ipv4Addr::UNSPECIFIED, update.lan.port)).with_context(|| {
            format!(
                "LAN port {} is unavailable; choose another port",
                update.lan.port
            )
        })?;
    }
    Ok(())
}

fn inspect_lan(
    root: &toml::Value,
    environment: &BTreeMap<String, String>,
) -> Result<LanConfigurationView> {
    let endpoints = lookup(root, &["server", "endpoints"])
        .and_then(toml::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let policies = lookup(root, &["policies"])
        .and_then(toml::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let endpoint_position = endpoints
        .iter()
        .position(|item| table_name(item) == Some(LAN_ENDPOINT));
    let policy_position = policies
        .iter()
        .position(|item| table_name(item) == Some(LAN_POLICY));
    let endpoint = endpoint_position.and_then(|index| endpoints.get(index));
    let policy = policy_position.and_then(|index| policies.get(index));
    let conflicting_policy = policies.iter().any(|item| {
        table_name(item) != Some(LAN_POLICY)
            && lookup(item, &["match", "endpoints"])
                .and_then(toml::Value::as_array)
                .is_some_and(|items| items.iter().any(|item| item.as_str() == Some(LAN_ENDPOINT)))
    });
    if endpoint.is_none() && policy.is_none() && !conflicting_policy {
        return Ok(LanConfigurationView {
            mode: LanMode::Disabled,
            port: 6343,
            port_source: ConfigFieldSource::Toml,
            allowed_databases: None,
            default_database: "default".into(),
            explanation: None,
        });
    }
    let Some(endpoint) = endpoint else {
        return Ok(custom_lan(
            "A desktop_lan policy exists without the matching lan endpoint.",
        ));
    };
    let Some(policy) = policy else {
        return Ok(custom_lan(
            "A lan endpoint exists without the matching desktop_lan policy.",
        ));
    };
    let endpoint_host = lookup(endpoint, &["host"])
        .and_then(toml::Value::as_str)
        .unwrap_or("127.0.0.1");
    let matches_only_lan = lookup(policy, &["match", "endpoints"])
        .and_then(toml::Value::as_array)
        .is_some_and(|items| items.len() == 1 && items[0].as_str() == Some(LAN_ENDPOINT))
        && lookup(policy, &["match", "hosts"])
            .and_then(toml::Value::as_array)
            .is_none_or(Vec::is_empty);
    let safe_shape = endpoint_host == "0.0.0.0"
        && matches_only_lan
        && lookup(policy, &["ruleset"]).and_then(toml::Value::as_str) == Some("allow_all")
        && lookup(policy, &["identity"]).is_none()
        && !conflicting_policy
        && endpoint_position == endpoints.len().checked_sub(1)
        && policy_position == policies.len().checked_sub(1)
        && lookup(policy, &["client", "desktop"]).and_then(toml::Value::as_bool) != Some(true);
    if !safe_shape {
        return Ok(custom_lan(
            "The existing LAN endpoint or policy uses settings the simplified UI does not manage.",
        ));
    }
    let (port, port_variable) = resolve_value::<u16>(
        lookup(endpoint, &["port"]).context("the lan endpoint has no port")?,
        environment,
    )?;
    let index = lookup(policy, &["index_db"]).context("desktop_lan has no index_db policy")?;
    let user =
        lookup(policy, &["user_data_db"]).context("desktop_lan has no user_data_db policy")?;
    let index_allow = parse_allow(lookup(index, &["allow"]))?;
    let user_allow = parse_allow(lookup(user, &["allow"]))?;
    let index_default = lookup(index, &["default"])
        .and_then(toml::Value::as_str)
        .unwrap_or("default");
    let user_default = lookup(user, &["default"])
        .and_then(toml::Value::as_str)
        .unwrap_or("default");
    if index_allow != user_allow
        || index_default != user_default
        || lookup(index, &["tenant_default"]).is_some()
        || lookup(index, &["tenant_prefix_template"]).is_some()
        || lookup(user, &["tenant_default"]).is_some()
        || lookup(user, &["tenant_prefix_template"]).is_some()
    {
        return Ok(custom_lan(
            "Index and user-data access use different or tenant-aware policies.",
        ));
    }
    Ok(LanConfigurationView {
        mode: LanMode::Managed,
        port,
        port_source: port_variable.map_or(ConfigFieldSource::Toml, |variable| {
            ConfigFieldSource::Environment { variable }
        }),
        allowed_databases: index_allow,
        default_database: index_default.into(),
        explanation: None,
    })
}

fn custom_lan(explanation: &str) -> LanConfigurationView {
    LanConfigurationView {
        mode: LanMode::Custom,
        port: 6343,
        port_source: ConfigFieldSource::Toml,
        allowed_databases: None,
        default_database: "default".into(),
        explanation: Some(explanation.into()),
    }
}

fn update_managed_lan(
    root: &mut toml::Value,
    update: &LanConfigurationUpdate,
    current_port: Option<u16>,
    env_updates: &mut BTreeMap<String, String>,
) -> Result<()> {
    let table = root
        .as_table_mut()
        .context("Server configuration root is not a table")?;
    let endpoint_index = table
        .get("server")
        .and_then(|server| server.get("endpoints"))
        .and_then(toml::Value::as_array)
        .and_then(|endpoints| {
            endpoints
                .iter()
                .position(|item| table_name(item) == Some(LAN_ENDPOINT))
        });
    let existing_port = endpoint_index
        .and_then(|index| {
            table
                .get("server")?
                .get("endpoints")?
                .as_array()?
                .get(index)
        })
        .and_then(|endpoint| endpoint.get("port"))
        .cloned();
    let policy_index = table
        .get("policies")
        .and_then(toml::Value::as_array)
        .and_then(|policies| {
            policies
                .iter()
                .position(|item| table_name(item) == Some(LAN_POLICY))
        });
    {
        let server = ensure_table(table, "server")?;
        let _endpoints = server
            .entry("endpoints")
            .or_insert_with(|| toml::Value::Array(Vec::new()))
            .as_array_mut()
            .context("server.endpoints is not an array")?;
    }
    {
        let _policies = table
            .entry("policies")
            .or_insert_with(|| toml::Value::Array(Vec::new()))
            .as_array_mut()
            .context("policies is not an array")?;
    }
    if !update.enabled {
        if let Some(index) = endpoint_index {
            ensure_table(table, "server")?
                .get_mut("endpoints")
                .and_then(toml::Value::as_array_mut)
                .expect("validated above")
                .remove(index);
        }
        if let Some(index) = policy_index {
            table
                .get_mut("policies")
                .and_then(toml::Value::as_array_mut)
                .expect("validated above")
                .remove(index);
        }
        return Ok(());
    }

    let port_value = if let Some(raw @ toml::Value::String(_)) = existing_port {
        if let Some(binding) = raw.as_str().and_then(whole_value_env_binding) {
            if current_port != Some(update.port) {
                env_updates.insert(binding.variable.into(), update.port.to_string());
            }
            raw
        } else {
            toml::Value::Integer(update.port.into())
        }
    } else {
        toml::Value::Integer(update.port.into())
    };
    let endpoint = toml::Value::Table(toml::Table::from_iter([
        ("name".into(), toml::Value::String(LAN_ENDPOINT.into())),
        ("host".into(), toml::Value::String("0.0.0.0".into())),
        ("port".into(), port_value),
    ]));
    let endpoints = ensure_table(table, "server")?
        .get_mut("endpoints")
        .and_then(toml::Value::as_array_mut)
        .expect("validated above");
    if let Some(index) = endpoint_index {
        endpoints[index] = endpoint;
    } else {
        endpoints.push(endpoint);
    }
    let allow = match &update.allowed_databases {
        None => toml::Value::String("*".into()),
        Some(databases) => {
            let mut databases = databases
                .iter()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>();
            databases.sort();
            databases.dedup();
            toml::Value::Array(databases.into_iter().map(toml::Value::String).collect())
        }
    };
    let db_policy = || {
        toml::Value::Table(toml::Table::from_iter([
            (
                "default".into(),
                toml::Value::String(update.default_database.trim().into()),
            ),
            ("allow".into(), allow.clone()),
        ]))
    };
    let policy = toml::Value::Table(toml::Table::from_iter([
        ("name".into(), toml::Value::String(LAN_POLICY.into())),
        ("ruleset".into(), toml::Value::String("allow_all".into())),
        (
            "match".into(),
            toml::Value::Table(toml::Table::from_iter([(
                "endpoints".into(),
                toml::Value::Array(vec![toml::Value::String(LAN_ENDPOINT.into())]),
            )])),
        ),
        ("index_db".into(), db_policy()),
        ("user_data_db".into(), db_policy()),
        (
            "client".into(),
            toml::Value::Table(toml::Table::from_iter([
                (
                    "home_redirect".into(),
                    toml::Value::String("/search".into()),
                ),
                ("desktop".into(), toml::Value::Boolean(false)),
            ])),
        ),
    ]));
    let policies = table
        .get_mut("policies")
        .and_then(toml::Value::as_array_mut)
        .expect("validated above");
    if let Some(index) = policy_index {
        policies[index] = policy;
    } else {
        policies.push(policy);
    }
    Ok(())
}

fn set_env_aware(
    root: &mut toml::Value,
    path: &[&str],
    literal: toml::Value,
    environment_value: String,
    env_updates: &mut BTreeMap<String, String>,
) -> Result<()> {
    let binding = lookup(root, path)
        .and_then(toml::Value::as_str)
        .and_then(whole_value_env_binding)
        .map(|binding| binding.variable.to_owned());
    if let Some(variable) = binding {
        env_updates.insert(variable, environment_value);
        return Ok(());
    }
    insert_path(root, path, literal)
}

fn resolved_field<T>(
    root: &toml::Value,
    path: &[&str],
    default: T,
    environment: &BTreeMap<String, String>,
) -> Result<ConfigField<T>>
where
    T: Clone + std::str::FromStr + serde::de::DeserializeOwned,
    T::Err: std::fmt::Display,
{
    let Some(raw) = lookup(root, path) else {
        return Ok(ConfigField {
            value: default,
            source: ConfigFieldSource::Toml,
        });
    };
    let (value, variable) = resolve_value(raw, environment)?;
    Ok(ConfigField {
        value,
        source: variable.map_or(ConfigFieldSource::Toml, |variable| {
            ConfigFieldSource::Environment { variable }
        }),
    })
}

fn resolve_value<T>(
    raw: &toml::Value,
    environment: &BTreeMap<String, String>,
) -> Result<(T, Option<String>)>
where
    T: std::str::FromStr + serde::de::DeserializeOwned,
    T::Err: std::fmt::Display,
{
    if let Some(template) = raw.as_str() {
        let binding = whole_value_env_binding(template)
            .with_context(|| format!("'{template}' is not a whole-value environment template"))?;
        let configured = environment.get(binding.variable).map(String::as_str);
        let text = match configured {
            Some(value) if !(binding.fallback_on_empty && value.is_empty()) => value,
            _ => binding.fallback.with_context(|| {
                format!(
                    "environment variable {} is not configured",
                    binding.variable
                )
            })?,
        };
        let value = text
            .parse::<T>()
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        return Ok((value, Some(binding.variable.into())));
    }
    let value = raw
        .clone()
        .try_into()
        .context("configuration value has the wrong type")?;
    Ok((value, None))
}

fn resolved_string(
    root: &toml::Value,
    path: &[&str],
    default: &str,
    environment: &BTreeMap<String, String>,
) -> Result<String> {
    let Some(raw) = lookup(root, path) else {
        return Ok(default.into());
    };
    if let Some(value) = raw.as_str() {
        if let Some(binding) = whole_value_env_binding(value) {
            let configured = environment.get(binding.variable).map(String::as_str);
            return Ok(match configured {
                Some(value) if !(binding.fallback_on_empty && value.is_empty()) => value.into(),
                _ => binding
                    .fallback
                    .with_context(|| {
                        format!(
                            "environment variable {} is not configured",
                            binding.variable
                        )
                    })?
                    .into(),
            });
        }
        return Ok(value.into());
    }
    bail!("configuration path {} is not a string", path.join("."))
}

fn environment_values(path: &Path) -> Result<BTreeMap<String, String>> {
    let mut values = std::env::vars().collect::<BTreeMap<_, _>>();
    match fs::read_to_string(path) {
        Ok(source) => {
            let parsed = panoptikon_config::parse_dotenv(&source);
            for diagnostic in &parsed.diagnostics {
                tracing::warn!("{}: {diagnostic}", path.display());
            }
            values.extend(parsed.values);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read {}", path.display()));
        }
    }
    Ok(values)
}

fn parse_allow(value: Option<&toml::Value>) -> Result<Option<Vec<String>>> {
    match value {
        Some(toml::Value::String(value)) if value == "*" => Ok(None),
        Some(toml::Value::Array(values)) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .context("database allowlists must contain strings")
            })
            .collect::<Result<Vec<_>>>()
            .map(Some),
        _ => bail!("database allow must be '*' or an array"),
    }
}

fn database_names(data_folder: &Path) -> Vec<String> {
    fs::read_dir(data_folder.join("index"))
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .filter_map(|entry| entry.file_name().into_string().ok())
        .collect()
}

fn table_name(value: &toml::Value) -> Option<&str> {
    value.get("name").and_then(toml::Value::as_str)
}

fn lookup<'a>(root: &'a toml::Value, path: &[&str]) -> Option<&'a toml::Value> {
    path.iter().try_fold(root, |value, key| value.get(*key))
}

fn insert_path(root: &mut toml::Value, path: &[&str], value: toml::Value) -> Result<()> {
    let (last, parents) = path.split_last().context("empty configuration path")?;
    let mut table = root
        .as_table_mut()
        .context("Server configuration root is not a table")?;
    for key in parents {
        table = ensure_table(table, key)?;
    }
    table.insert((*last).into(), value);
    Ok(())
}

fn ensure_table<'a>(table: &'a mut toml::Table, key: &str) -> Result<&'a mut toml::Table> {
    table
        .entry(key)
        .or_insert_with(|| toml::Value::Table(toml::Table::new()))
        .as_table_mut()
        .with_context(|| format!("{key} is not a table"))
}

fn revision(source: &str, dotenv: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(source.as_bytes());
    hash.update([0]);
    hash.update(dotenv.as_bytes());
    format!("{:x}", hash.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> &'static str {
        include_str!("../../../config/server/desktop.toml")
    }

    fn unchanged_update(current: &ServerConfigurationView) -> ServerConfigurationUpdate {
        ServerConfigurationUpdate {
            revision: current.revision.clone(),
            local_port: current.local_port.value,
            lan: LanConfigurationUpdate {
                enabled: current.lan.mode == LanMode::Managed,
                port: current.lan.port,
                allowed_databases: current.lan.allowed_databases.clone(),
                default_database: current.lan.default_database.clone(),
            },
            performance: PerformanceConfigurationUpdate {
                prewarm_enabled: current.performance.prewarm_enabled.value,
                prewarm_lazy: current.performance.prewarm_lazy.value,
                loader_concurrency: current.performance.loader_concurrency.value,
                intermediate_data_budget_mb: current.performance.intermediate_data_budget_mb.value,
                embedding_cache_size: current.performance.embedding_cache_size.value,
            },
        }
    }

    #[test]
    fn managed_lan_round_trip_is_recognized() {
        let root = tempfile::tempdir().unwrap();
        let config = root.path().join("desktop.toml");
        fs::write(&config, fixture()).unwrap();
        let current = load(root.path(), &config).unwrap();
        assert_eq!(current.lan.mode, LanMode::Disabled);
        let update = ServerConfigurationUpdate {
            revision: current.revision,
            local_port: current.local_port.value,
            lan: LanConfigurationUpdate {
                enabled: true,
                port: 16433,
                allowed_databases: Some(vec!["private".into(), "default".into()]),
                default_database: "default".into(),
            },
            performance: PerformanceConfigurationUpdate {
                prewarm_enabled: true,
                prewarm_lazy: false,
                loader_concurrency: 4,
                intermediate_data_budget_mb: 512,
                embedding_cache_size: 8,
            },
        };
        let saved = save(root.path(), &config, Some(6342), &update).unwrap();
        assert_eq!(
            saved.lan.mode,
            LanMode::Managed,
            "{:?}",
            saved.lan.explanation
        );
        assert_eq!(saved.lan.allowed_databases.unwrap(), ["default", "private"]);
        let text = fs::read_to_string(config).unwrap();
        assert!(text.starts_with("# Panoptikon Server configuration"));
    }

    #[test]
    fn environment_bound_values_update_dotenv_without_replacing_template() {
        let root = tempfile::tempdir().unwrap();
        let config = root.path().join("desktop.toml");
        let source = fixture().replace("port = 6342", "port = \"${PORT:-6342}\"");
        fs::write(&config, &source).unwrap();
        let current = load(root.path(), &config).unwrap();
        let update = ServerConfigurationUpdate {
            revision: current.revision,
            local_port: 16434,
            lan: LanConfigurationUpdate {
                enabled: false,
                port: 16435,
                allowed_databases: None,
                default_database: "default".into(),
            },
            performance: PerformanceConfigurationUpdate {
                prewarm_enabled: current.performance.prewarm_enabled.value,
                prewarm_lazy: current.performance.prewarm_lazy.value,
                loader_concurrency: current.performance.loader_concurrency.value,
                intermediate_data_budget_mb: current.performance.intermediate_data_budget_mb.value,
                embedding_cache_size: current.performance.embedding_cache_size.value,
            },
        };
        save(root.path(), &config, Some(6342), &update).unwrap();
        assert_eq!(fs::read_to_string(&config).unwrap(), source);
        assert_eq!(
            fs::read_to_string(root.path().join(".env")).unwrap(),
            "PORT=\"16434\"\n"
        );
    }

    #[test]
    fn custom_lan_policy_is_read_only_in_simplified_editor() {
        let root = tempfile::tempdir().unwrap();
        let config = root.path().join("desktop.toml");
        let source = format!(
            "{}\n[[server.endpoints]]\nname='lan'\nhost='0.0.0.0'\nport=16436\n",
            fixture()
        );
        fs::write(&config, source).unwrap();
        let current = load(root.path(), &config).unwrap();
        assert_eq!(current.lan.mode, LanMode::Custom);
    }

    #[test]
    fn stale_revision_and_occupied_recovery_port_are_rejected_before_writing() {
        let root = tempfile::tempdir().unwrap();
        let config = root.path().join("desktop.toml");
        fs::write(&config, fixture()).unwrap();
        let current = load(root.path(), &config).unwrap();
        let update = unchanged_update(&current);
        fs::write(&config, format!("{}\n# external edit\n", fixture())).unwrap();
        let error = save(root.path(), &config, Some(6342), &update)
            .unwrap_err()
            .to_string();
        assert!(error.contains("changed on disk"), "{error}");

        fs::write(&config, fixture()).unwrap();
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let occupied = listener.local_addr().unwrap().port();
        let current = load(root.path(), &config).unwrap();
        let mut update = unchanged_update(&current);
        update.local_port = occupied;
        let error = save(root.path(), &config, None, &update)
            .unwrap_err()
            .to_string();
        assert!(error.contains("unavailable"), "{error}");
        assert_eq!(fs::read_to_string(config).unwrap(), fixture());
    }

    #[test]
    fn environment_bound_lan_port_keeps_its_reference() {
        let root = tempfile::tempdir().unwrap();
        let config = root.path().join("desktop.toml");
        fs::write(&config, fixture()).unwrap();
        let current = load(root.path(), &config).unwrap();
        let mut update = unchanged_update(&current);
        update.lan = LanConfigurationUpdate {
            enabled: true,
            port: 16437,
            allowed_databases: None,
            default_database: "default".into(),
        };
        save(root.path(), &config, Some(6342), &update).unwrap();
        let source = fs::read_to_string(&config)
            .unwrap()
            .replace("port = 16437", "port = \"${LAN_PORT:-16437}\"");
        fs::write(&config, &source).unwrap();
        let current = load(root.path(), &config).unwrap();
        let mut update = unchanged_update(&current);
        update.lan.port = 16438;
        save(root.path(), &config, Some(6342), &update).unwrap();
        assert_eq!(fs::read_to_string(&config).unwrap(), source);
        assert_eq!(
            fs::read_to_string(root.path().join(".env")).unwrap(),
            "LAN_PORT=\"16438\"\n"
        );
    }
}
