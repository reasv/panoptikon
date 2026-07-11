//! Model registry: parses the inferio inference TOML registry and resolves
//! per-inference-id spawn specs (impl class + constructor kwargs).
//!
//! This is a faithful port of the legacy Python `inferio/config.py`
//! (python-legacy branch). The semantics that
//! matter (verified against the Python source, cited by line):
//!
//! - Config folders are scanned for `*.toml` in alphabetical order, built-in
//!   folder first, then the user folder (`load_config`, config.py:100-101).
//!   A missing folder is skipped with a warning (config.py:29-31).
//! - Any error in any file — unparseable TOML, duplicate inference_id —
//!   fails the whole load; Python logs and re-raises (config.py:77-79),
//!   nothing is skipped.
//! - `allow_override` is a *per-file* flag (config.py:39): a file may
//!   redefine an inference_id that an earlier file already defined only when
//!   the *later* file sets `allow_override = true`; otherwise the load fails
//!   with a duplicate-id error (config.py:56-63). Group-level `config` /
//!   `metadata` tables always merge across files (later file wins per key,
//!   config.py:44-51) regardless of `allow_override`.
//! - Group config is merged under inference-id config *eagerly at the point
//!   the id is defined* (config.py:66-69): group config added by a later
//!   file does not retroactively apply to ids defined in earlier files.
//! - The resolved id entry stores the merged config and the id's own
//!   metadata (config.py:71-76); redefinition (when allowed) replaces both.
//! - `/metadata` returns, per group, `group_metadata` and a map of id ->
//!   id-level metadata only (`list_inference_ids`, config.py:105-118).
//!   `impl_class`/`config` never leak into the metadata output.
//! - Reload is mtime-triggered: reuse the cached snapshot only when the max
//!   mtime over all `*.toml` files is <= the recorded mtime (config.py:90).
//!   Python's check is `if config and mtime and latest <= mtime`, so an
//!   *empty* registry or a 0.0 mtime (no files at all) is never treated as
//!   a valid cache and reloads on every call — replicated here.
//!
//! Spawn-spec kwargs replicate `process_model.py::_model_process`
//! (process_model.py:208-212): the merged config minus `impl_class` and
//! `ray_config` is exactly what Python passes to `impl_class(**kwargs)`
//! (`clean_dict` is a no-op for TOML-derived plain data). `ray_config` is
//! therefore *not* forwarded to workers, matching Python.
//!
//! Rust-orchestrator extension (design §8, Phase 3): `config.replicas` and
//! `config.devices` configure the per-model WorkerSet. Both are stripped
//! from spawn kwargs exactly like `ray_config` (they are orchestrator
//! directives, not impl constructor arguments) and, being ordinary config
//! keys, inherit from group config like everything else. Resolution
//! (`resolve_device_pins`):
//! - `devices = ["3", "7"]` -> 2 replicas, replica i pinned
//!   `CUDA_VISIBLE_DEVICES=devices[i]`;
//! - `replicas = N` alone -> N replicas pinned `"0"`..`"N-1"`;
//! - neither -> 1 replica, no pin (today's behavior);
//! - both with mismatched lengths, a non-positive/non-integer `replicas`,
//!   or a non-array-of-strings/empty `devices` -> **registry load error**
//!   (explicit beats silent), validated per id at load time against the
//!   merged config so the error names the offending inference id.
//!
//! JSON object key order IS semantic here: Python dicts preserve insertion
//! order, FastAPI serializes `/metadata` in that order, and the web UI
//! renders model-group tabs and model rows in that key order (the registry
//! TOML deliberately lists the recommended models first). So this module
//! uses `IndexMap` for groups/ids and relies on the `preserve_order`
//! features of `toml` and `serde_json` so file order survives parsing and
//! serialization, matching Python end to end.

use anyhow::{Context, Result, bail};
use indexmap::IndexMap;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

/// Ordered list of directories scanned for `*.toml` registry files.
/// Built-in (repo) config first, user config after, so user files can extend
/// groups and (with `allow_override`) redefine inference ids.
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    pub config_dirs: Vec<PathBuf>,
}

impl RegistryConfig {
    /// Default folder resolution when `[inference_local].config_dirs` is
    /// empty (the config key is the only override — the old
    /// `BASE_INFERENCE_CONFIG_FOLDER`/`INFERIO_CONFIG_DIR` env fallbacks are
    /// gone):
    ///
    /// - Base folder: the active mode's built-in registry dir — dev:
    ///   `python/inferio/config` (Python parity: legacy config.py:168-171),
    ///   extracted bundled mode: `runtime/pysrc/<version>/inferio/config`.
    ///   Must exist. Python resolved this relative to the `inferio`
    ///   package's `__file__`; Rust has no equivalent, so the default is
    ///   relative to the current working directory — the server is run from
    ///   the repo root in the dev layout, where the two agree. Windows
    ///   nuance: relative paths resolve against the drive+dir of the
    ///   process CWD like on Unix; no special handling is needed.
    /// - User folder: `config/inference` (legacy config.py:88), never
    ///   existence-checked (a missing folder is skipped with a warning at
    ///   load time).
    pub fn default_dirs() -> Result<Self> {
        let base = crate::resources::builtin_registry_dir(crate::resources::py_source_mode());
        if !base.is_dir() {
            bail!("Base configuration folder not found at: {}", base.display());
        }
        Ok(Self {
            config_dirs: vec![base, PathBuf::from("config/inference")],
        })
    }
}

/// One inference id inside a group: the fully merged config (group config
/// overlaid with id config, id wins) and the id's own metadata.
#[derive(Debug, Clone, Default)]
pub struct InferenceIdEntry {
    /// Merged config, still including `impl_class` / `ray_config`; use
    /// [`Registry::spawn_spec`] for the kwargs actually passed to workers.
    pub config: JsonMap<String, JsonValue>,
    pub metadata: JsonMap<String, JsonValue>,
}

/// A model group: accumulated group-level config/metadata plus its ids.
#[derive(Debug, Clone, Default)]
pub struct GroupEntry {
    pub group_config: JsonMap<String, JsonValue>,
    pub group_metadata: JsonMap<String, JsonValue>,
    pub inference_ids: IndexMap<String, InferenceIdEntry>,
}

/// Immutable loaded snapshot of the whole registry.
#[derive(Debug, Clone, Default)]
pub struct Registry {
    pub groups: IndexMap<String, GroupEntry>,
}

/// What a worker needs to instantiate a model: the impl class name and the
/// exact kwargs Python passes to `impl_class(**kwargs)`.
#[derive(Debug, Clone)]
pub struct SpawnSpec {
    pub impl_class: String,
    /// Always a JSON object; merged config minus `impl_class`/`ray_config`
    /// and the orchestrator-only `replicas`/`devices` keys
    /// (process_model.py:209-211 for the Python-parity part).
    pub config_kwargs: JsonValue,
    /// Per-replica `CUDA_VISIBLE_DEVICES` pins (design §8): one entry per
    /// replica to spawn, `None` = no pin (inherit the parent env). Always
    /// non-empty; `vec![None]` is the single-replica default.
    pub device_pins: Vec<Option<String>>,
}

impl Registry {
    /// Load a fresh snapshot from the configured directories, in order.
    /// Any file error fails the whole load (Python re-raises, config.py:79).
    pub fn load(config: &RegistryConfig) -> Result<Self> {
        let mut groups: IndexMap<String, GroupEntry> = IndexMap::new();
        for dir in &config.config_dirs {
            load_folder(dir, &mut groups)?;
        }
        Ok(Self { groups })
    }

    /// Build the `/metadata` response body, matching Python's
    /// `list_inference_ids` (config.py:105-118) exactly:
    /// `{group: {"group_metadata": {...}, "inference_ids": {id: metadata}}}`.
    /// Group/id config and `impl_class` are never included.
    pub fn metadata_json(&self) -> JsonValue {
        let mut root = JsonMap::new();
        for (group_name, group) in &self.groups {
            let mut ids = JsonMap::new();
            for (id, entry) in &group.inference_ids {
                ids.insert(id.clone(), JsonValue::Object(entry.metadata.clone()));
            }
            let mut group_obj = JsonMap::new();
            group_obj.insert(
                "group_metadata".to_string(),
                JsonValue::Object(group.group_metadata.clone()),
            );
            group_obj.insert("inference_ids".to_string(), JsonValue::Object(ids));
            root.insert(group_name.clone(), JsonValue::Object(group_obj));
        }
        JsonValue::Object(root)
    }

    /// Metadata for one inference id, matching Python's `get_metadata`
    /// (config.py:120-136): `{"group_metadata": ..., "inference_id_metadata":
    /// ...}`, or None when the group or id is unknown.
    // Not exposed over HTTP (router.py has no such endpoint); kept for the
    // API-side consumers that use Python's get_metadata when they port over.
    #[allow(dead_code)]
    pub fn inference_id_metadata(&self, group_name: &str, inference_id: &str) -> Option<JsonValue> {
        let group = self.groups.get(group_name)?;
        let entry = group.inference_ids.get(inference_id)?;
        let mut obj = JsonMap::new();
        obj.insert(
            "group_metadata".to_string(),
            JsonValue::Object(group.group_metadata.clone()),
        );
        obj.insert(
            "inference_id_metadata".to_string(),
            JsonValue::Object(entry.metadata.clone()),
        );
        Some(JsonValue::Object(obj))
    }

    /// Resolve the spawn spec for `group/name`, replicating
    /// `get_model_config` (config.py:138-155) + the kwarg stripping in
    /// `_model_process` (process_model.py:208-212): kwargs are the merged
    /// config minus `impl_class` and `ray_config`.
    ///
    /// Python defers a missing/non-string `impl_class` to the worker, which
    /// fails its impl-class lookup at load time; we fail here instead — the
    /// same user-visible outcome (model load errors), just earlier.
    pub fn spawn_spec(&self, full_inference_id: &str) -> Result<SpawnSpec> {
        let (group_name, inference_id) = full_inference_id.split_once('/').with_context(|| {
            format!("Inference ID '{full_inference_id}' must be in 'group/name' form")
        })?;
        let group = self
            .groups
            .get(group_name)
            .with_context(|| format!("Group '{group_name}' not found in registry"))?;
        let entry = group.inference_ids.get(inference_id).with_context(|| {
            format!("Inference ID '{inference_id}' not found in group '{group_name}'")
        })?;

        let mut kwargs = entry.config.clone();
        let impl_class = kwargs.remove("impl_class");
        // ray_config rode along in the merged config for the dropped Ray
        // mode; Python strips it before instantiation (process_model.py:211)
        // and it is NOT forwarded to workers.
        kwargs.remove("ray_config");
        // replicas/devices are orchestrator directives (WorkerSet shape,
        // design §8), stripped from kwargs exactly like ray_config. Load
        // already validated them; this re-resolution can only fail if the
        // registry was constructed without going through load().
        let device_pins = resolve_device_pins(&kwargs).with_context(|| {
            format!("invalid replicas/devices config for inference id '{full_inference_id}'")
        })?;
        kwargs.remove("replicas");
        kwargs.remove("devices");
        let impl_class = match impl_class {
            Some(JsonValue::String(name)) => name,
            other => bail!(
                "Model class {:?} not found in impl_classes (inference id '{}' has no valid impl_class)",
                other,
                full_inference_id
            ),
        };
        Ok(SpawnSpec {
            impl_class,
            config_kwargs: JsonValue::Object(kwargs),
            device_pins,
        })
    }
}

/// Resolve the WorkerSet shape from a merged id config (design §8; see the
/// module docs for the rules). Returns one entry per replica: the
/// `CUDA_VISIBLE_DEVICES` value to pin at spawn, or `None` for no pin.
fn resolve_device_pins(config: &JsonMap<String, JsonValue>) -> Result<Vec<Option<String>>> {
    // Hard ceiling on the WorkerSet size: each replica is a full Python
    // process, so anything past this is a config typo, and the pin vector is
    // materialized eagerly at registry load (an unbounded value would OOM at
    // boot instead of producing a load error).
    const MAX_REPLICAS: usize = 64;
    let replicas = match config.get("replicas") {
        None => None,
        Some(value) => match value.as_i64() {
            Some(n) if n >= 1 && (n as u64) <= MAX_REPLICAS as u64 => Some(n as usize),
            Some(n) if n >= 1 => bail!(
                "'replicas' ({n}) exceeds the maximum of {MAX_REPLICAS} worker processes"
            ),
            _ => bail!("'replicas' must be an integer >= 1, got {value}"),
        },
    };
    let devices = match config.get("devices") {
        None => None,
        Some(value) => {
            let items = value
                .as_array()
                .with_context(|| format!("'devices' must be an array of strings, got {value}"))?;
            let devices: Vec<String> = items
                .iter()
                .map(|item| {
                    item.as_str().map(str::to_owned).with_context(|| {
                        format!("'devices' entries must be strings, got {item}")
                    })
                })
                .collect::<Result<_>>()?;
            if devices.is_empty() {
                bail!("'devices' must not be empty (omit it for the single-replica default)");
            }
            if devices.len() > MAX_REPLICAS {
                bail!(
                    "'devices' lists {} entries, exceeding the maximum of {MAX_REPLICAS} \
                     worker processes",
                    devices.len()
                );
            }
            Some(devices)
        }
    };
    match (replicas, devices) {
        // Both given: allowed only when consistent (explicit beats silent —
        // a mismatch is a config contradiction, not something to guess at).
        (Some(replicas), Some(devices)) if replicas != devices.len() => bail!(
            "'replicas' ({replicas}) contradicts 'devices' length ({}); drop one or make them match",
            devices.len()
        ),
        (_, Some(devices)) => Ok(devices.into_iter().map(Some).collect()),
        (Some(replicas), None) => Ok((0..replicas).map(|i| Some(i.to_string())).collect()),
        (None, None) => Ok(vec![None]),
    }
}

/// Mtime-gated registry cache with the same trigger semantics as Python's
/// `load_config(config, mtime)` on `/metadata` (config.py:82-103): re-parse
/// when any scanned file's mtime exceeds the recorded one; keep the previous
/// snapshot untouched if a reload fails (the caller sees the error, the next
/// call retries — Python leaves its globals unchanged when the reload
/// raises, router.py:247-250).
///
/// Not internally synchronized; wrap in a `Mutex` (or own it in an actor)
/// for shared use.
#[derive(Debug)]
pub struct RegistryCache {
    config: RegistryConfig,
    cached: Option<CachedSnapshot>,
}

#[derive(Debug)]
struct CachedSnapshot {
    registry: Arc<Registry>,
    /// Max mtime observed over all scanned files *before* the load, exactly
    /// like Python records `latest_time` computed up front (config.py:89).
    mtime: SystemTime,
}

impl RegistryCache {
    pub fn new(config: RegistryConfig) -> Self {
        Self {
            config,
            cached: None,
        }
    }

    /// Return the current snapshot, reloading if any file changed.
    pub fn get(&mut self) -> Result<Arc<Registry>> {
        let latest = latest_config_mtime(&self.config.config_dirs)?;
        if let (Some(cached), Some(latest)) = (&self.cached, latest) {
            // Python: `if config and mtime and latest_time <= mtime`
            // (config.py:90) — an empty registry (falsy dict) or a missing
            // mtime (0.0) never counts as a valid cache.
            if !cached.registry.groups.is_empty() && latest <= cached.mtime {
                return Ok(cached.registry.clone());
            }
        }
        let registry = Arc::new(Registry::load(&self.config)?);
        self.cached = latest.map(|mtime| CachedSnapshot {
            registry: registry.clone(),
            mtime,
        });
        Ok(registry)
    }
}

/// All `*.toml` files directly inside `dir`, sorted by file name — the same
/// order as Python's `sorted(folder.glob("*.toml"))` within one directory
/// (config.py:32-34). The extension match is ASCII-case-insensitive: Python's
/// `Path.glob` is case-insensitive on Windows (the current deployment
/// platform) and the registry only ships lowercase `.toml` anyway, so this
/// keeps behavior identical across platforms.
fn toml_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = fs::read_dir(dir)
        .with_context(|| format!("failed to read config folder {}", dir.display()))?
        .collect::<std::io::Result<Vec<_>>>()
        .with_context(|| format!("failed to read config folder {}", dir.display()))?
        .into_iter()
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("toml"))
                .unwrap_or(false)
        })
        .collect();
    files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
    Ok(files)
}

/// Max mtime over every scanned file, or None when there are no files —
/// Python's `get_config_mtime` returning 0.0 (config.py:11-22). Missing
/// folders contribute nothing (Python's glob on a missing dir is empty).
fn latest_config_mtime(dirs: &[PathBuf]) -> Result<Option<SystemTime>> {
    let mut latest: Option<SystemTime> = None;
    for dir in dirs {
        if !dir.is_dir() {
            continue;
        }
        for file in toml_files(dir)? {
            let modified = fs::metadata(&file)
                .and_then(|meta| meta.modified())
                .with_context(|| format!("failed to stat {}", file.display()))?;
            if latest.map_or(true, |current| modified > current) {
                latest = Some(modified);
            }
        }
    }
    Ok(latest)
}

/// Load every TOML file in `folder` into `groups`, mirroring
/// `load_config_folder` (config.py:24-80). A missing folder logs a warning
/// and is skipped; any file error aborts the whole load.
fn load_folder(folder: &Path, groups: &mut IndexMap<String, GroupEntry>) -> Result<()> {
    if !folder.is_dir() {
        tracing::warn!(
            folder = %folder.display(),
            "config folder does not exist or is not a directory"
        );
        return Ok(());
    }
    for file in toml_files(folder)? {
        load_file(&file, groups)
            .with_context(|| format!("Error loading TOML file {}", file.display()))?;
    }
    Ok(())
}

fn load_file(file: &Path, groups: &mut IndexMap<String, GroupEntry>) -> Result<()> {
    let text = fs::read_to_string(file).context("failed to read file")?;
    let mut doc: toml::Value = toml::from_str(&text).context("failed to parse TOML")?;
    // Env templating (`${VAR}` / `${VAR:-default}` in string values): this is
    // how secrets and URLs reach inference impls — a registry file can set
    // `config.api_key = "${SOME_API_KEY}"` and the substituted value flows
    // into the impl constructor kwargs. Applied on every (re)load, so the
    // mtime-gated reload path re-substitutes too. A substitution error fails
    // the whole load like any other file error.
    crate::env_template::substitute_toml_value(&mut doc, file)?;

    // Python evaluates the flag's *truthiness* (`data.get("allow_override",
    // False)` used in a boolean context, config.py:39,59), so non-bool
    // values follow Python truthiness rules.
    let allow_inference_id_overrides = toml_truthy(doc.get("allow_override"));

    let Some(groups_value) = doc.get("group") else {
        // No [group.*] tables: nothing to merge (data.get("group", {})).
        return Ok(());
    };
    let group_tables = groups_value
        .as_table()
        .context("'group' must be a table of group tables")?;

    for (group_name, group_data) in group_tables {
        let group_data = group_data
            .as_table()
            .with_context(|| format!("group '{group_name}' must be a table"))?;
        let entry = groups.entry(group_name.clone()).or_default();

        // Merge group-level config and metadata, later file wins per key
        // (config.py:44-51). Always allowed, independent of allow_override.
        if let Some(config) = group_data.get("config") {
            let table = config
                .as_table()
                .with_context(|| format!("group '{group_name}' config must be a table"))?;
            merge_table_into(&mut entry.group_config, table);
        }
        if let Some(metadata) = group_data.get("metadata") {
            let table = metadata
                .as_table()
                .with_context(|| format!("group '{group_name}' metadata must be a table"))?;
            merge_table_into(&mut entry.group_metadata, table);
        }

        let Some(ids_value) = group_data.get("inference_ids") else {
            continue;
        };
        let id_tables = ids_value
            .as_table()
            .with_context(|| format!("group '{group_name}' inference_ids must be a table"))?;
        for (inference_id, inf_data) in id_tables {
            if entry.inference_ids.contains_key(inference_id) && !allow_inference_id_overrides {
                // Same failure mode as Python (config.py:61-63): the error
                // propagates and the entire load fails.
                bail!(
                    "Duplicate inference_id '{}/{}' found in {}",
                    group_name,
                    inference_id,
                    file.display()
                );
            }
            let inf_data = inf_data.as_table().with_context(|| {
                format!("inference id '{group_name}/{inference_id}' must be a table")
            })?;

            // Merge group config under id config, id wins — computed *now*,
            // against the group config accumulated so far (config.py:66-69):
            // group config added by later files does not retroactively
            // change ids defined earlier.
            let mut merged_config = entry.group_config.clone();
            if let Some(config) = inf_data.get("config") {
                let table = config.as_table().with_context(|| {
                    format!("inference id '{group_name}/{inference_id}' config must be a table")
                })?;
                merge_table_into(&mut merged_config, table);
            }
            // Validate the WorkerSet shape now so a bad replicas/devices
            // combination is a *registry load* error naming the id, not a
            // spawn-time surprise (design §8; explicit beats silent). The
            // merged config is what spawn_spec will resolve, so validating
            // it here covers group-level inheritance too.
            resolve_device_pins(&merged_config).with_context(|| {
                format!("invalid replicas/devices config for inference id '{group_name}/{inference_id}'")
            })?;
            let metadata = match inf_data.get("metadata") {
                Some(metadata) => {
                    let table = metadata.as_table().with_context(|| {
                        format!(
                            "inference id '{group_name}/{inference_id}' metadata must be a table"
                        )
                    })?;
                    table_to_json_map(table)
                }
                None => JsonMap::new(),
            };
            // Redefinition (when allowed) fully replaces the entry
            // (config.py:71-76): config re-merged, metadata replaced.
            entry.inference_ids.insert(
                inference_id.clone(),
                InferenceIdEntry {
                    config: merged_config,
                    metadata,
                },
            );
        }
    }
    Ok(())
}

/// Shallow merge of a TOML table into a JSON map (`dict.update` semantics:
/// existing keys are replaced wholesale, not deep-merged).
fn merge_table_into(target: &mut JsonMap<String, JsonValue>, table: &toml::Table) {
    for (key, value) in table {
        target.insert(key.clone(), toml_to_json(value));
    }
}

fn table_to_json_map(table: &toml::Table) -> JsonMap<String, JsonValue> {
    let mut map = JsonMap::new();
    merge_table_into(&mut map, table);
    map
}

fn toml_to_json(value: &toml::Value) -> JsonValue {
    match value {
        toml::Value::String(s) => JsonValue::String(s.clone()),
        toml::Value::Integer(i) => JsonValue::from(*i),
        // Non-finite floats have no JSON representation; TOML permits them
        // but the registry never uses them. Null is the serde_json fallback.
        toml::Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        toml::Value::Boolean(b) => JsonValue::Bool(*b),
        // tomli yields datetime objects which FastAPI would serialize as ISO
        // strings; the TOML text form is that ISO string.
        toml::Value::Datetime(dt) => JsonValue::String(dt.to_string()),
        toml::Value::Array(items) => JsonValue::Array(items.iter().map(toml_to_json).collect()),
        toml::Value::Table(table) => JsonValue::Object(table_to_json_map(table)),
    }
}

/// Python truthiness for a TOML value, for the `allow_override` flag.
fn toml_truthy(value: Option<&toml::Value>) -> bool {
    match value {
        None => false,
        Some(toml::Value::Boolean(b)) => *b,
        Some(toml::Value::String(s)) => !s.is_empty(),
        Some(toml::Value::Integer(i)) => *i != 0,
        Some(toml::Value::Float(f)) => *f != 0.0,
        Some(toml::Value::Array(items)) => !items.is_empty(),
        Some(toml::Value::Table(table)) => !table.is_empty(),
        Some(toml::Value::Datetime(_)) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Duration;

    fn write_file(dir: &Path, name: &str, content: &str) -> PathBuf {
        let path = dir.join(name);
        fs::write(&path, content).expect("write fixture file");
        path
    }

    fn registry_for(dirs: &[&Path]) -> Result<Registry> {
        Registry::load(&RegistryConfig {
            config_dirs: dirs.iter().map(|d| d.to_path_buf()).collect(),
        })
    }

    fn set_mtime(path: &Path, mtime: SystemTime) {
        let file = fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open fixture file");
        file.set_modified(mtime).expect("set fixture mtime");
    }

    /// Loading the real built-in registry (python/inferio/config) must succeed
    /// and contain the known `tags` group with the `wd-swinv2-tagger-v3`
    /// inference id. Its spawn spec resolves impl_class from the group
    /// config ("wd_tagger") and passes exactly the id-level config as kwargs
    /// (model_repo only) — with `impl_class` stripped, mirroring
    /// process_model.py:208-212.
    #[test]
    fn loads_real_builtin_registry() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../python/inferio/config");
        let registry = registry_for(&[&dir]).expect("built-in registry loads");

        let tags = registry.groups.get("tags").expect("tags group exists");
        assert!(tags.inference_ids.contains_key("wd-swinv2-tagger-v3"));
        assert_eq!(
            tags.group_metadata.get("name"),
            Some(&json!("Tags")),
            "group metadata carries the display name"
        );

        let spec = registry
            .spawn_spec("tags/wd-swinv2-tagger-v3")
            .expect("spawn spec resolves");
        assert_eq!(spec.impl_class, "wd_tagger");
        assert_eq!(
            spec.config_kwargs,
            json!({"model_repo": "SmilingWolf/wd-swinv2-tagger-v3"}),
            "kwargs are exactly what Python passes to __init__ — no impl_class"
        );
        assert_eq!(
            spec.device_pins,
            vec![None],
            "no replicas/devices in the shipped registry -> single unpinned replica"
        );
    }

    /// The `tagmatch` group carries a group-level `ray_config` table; Python
    /// strips both `impl_class` and `ray_config` before instantiating
    /// (process_model.py:211), so the danbooru spawn kwargs must be an empty
    /// object even though the merged config contains both keys.
    #[test]
    fn real_registry_strips_ray_config_from_spawn_kwargs() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../python/inferio/config");
        let registry = registry_for(&[&dir]).expect("built-in registry loads");

        let entry = &registry.groups["tagmatch"].inference_ids["danbooru"];
        assert!(
            entry.config.contains_key("ray_config"),
            "merged config keeps it"
        );

        let spec = registry.spawn_spec("tagmatch/danbooru").expect("resolves");
        assert_eq!(spec.impl_class, "danbooru_tagger");
        assert_eq!(spec.config_kwargs, json!({}));
    }

    /// The /metadata payload for the real registry must expose only
    /// metadata: group_metadata + per-id metadata, never config or
    /// impl_class (config.py:105-118).
    #[test]
    fn real_registry_metadata_excludes_config() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../python/inferio/config");
        let registry = registry_for(&[&dir]).expect("built-in registry loads");

        let metadata = registry.metadata_json();
        let tags = &metadata["tags"];
        assert_eq!(tags["group_metadata"]["name"], json!("Tags"));
        let id_meta = &tags["inference_ids"]["wd-swinv2-tagger-v3"];
        assert!(id_meta["description"].is_string());
        assert!(id_meta.get("config").is_none(), "config must not leak");
        assert!(
            id_meta.get("impl_class").is_none(),
            "impl_class must not leak"
        );
        assert!(
            tags.get("group_config").is_none(),
            "group config must not leak"
        );
    }

    /// The web UI renders model-group tabs and model rows in JSON key order,
    /// and Python serves /metadata in TOML declaration order (tomli + dicts
    /// + FastAPI). Groups and inference ids must keep file order — not sort
    /// alphabetically — including across files (first mention pins the
    /// position, later files append).
    #[test]
    fn metadata_preserves_toml_declaration_order() {
        let dir = tempfile::tempdir().unwrap();
        write_file(
            dir.path(),
            "a.toml",
            r#"
[group.zgroup]
config.impl_class = "cls"
[group.zgroup.inference_ids.second_id]
[group.zgroup.inference_ids.first_id]
[group.agroup]
config.impl_class = "cls"
[group.agroup.inference_ids.x]
"#,
        );
        write_file(
            dir.path(),
            "b.toml",
            r#"
[group.zgroup.inference_ids.third_id]
[group.bgroup]
config.impl_class = "cls"
[group.bgroup.inference_ids.y]
"#,
        );
        let registry = registry_for(&[dir.path()]).unwrap();

        let groups: Vec<&String> = registry.groups.keys().collect();
        assert_eq!(groups, ["zgroup", "agroup", "bgroup"]);
        let ids: Vec<&String> = registry.groups["zgroup"].inference_ids.keys().collect();
        assert_eq!(ids, ["second_id", "first_id", "third_id"]);

        // The order must survive serialization of the /metadata body
        // (serde_json preserve_order).
        let body = serde_json::to_string(&registry.metadata_json()).unwrap();
        let pos = |needle: &str| body.find(needle).expect(needle);
        assert!(pos("zgroup") < pos("agroup"));
        assert!(pos("agroup") < pos("bgroup"));
        assert!(pos("second_id") < pos("first_id"));
        assert!(pos("first_id") < pos("third_id"));
    }

    /// Group-level config is inherited by every inference id, and id-level
    /// config wins on key conflicts (config.py:66-69): `b` is overridden by
    /// the id, `a` flows through from the group.
    #[test]
    fn group_config_inherited_and_overridden_by_id() {
        let dir = tempfile::tempdir().unwrap();
        write_file(
            dir.path(),
            "a.toml",
            r#"
[group.g]
config.impl_class = "cls"
config.a = 1
config.b = 2

[group.g.inference_ids.override_id]
config.b = 3

[group.g.inference_ids.plain_id]
"#,
        );
        let registry = registry_for(&[dir.path()]).expect("fixture loads");

        let spec = registry.spawn_spec("g/override_id").expect("resolves");
        assert_eq!(spec.config_kwargs, json!({"a": 1, "b": 3}));

        let spec = registry.spawn_spec("g/plain_id").expect("resolves");
        assert_eq!(spec.config_kwargs, json!({"a": 1, "b": 2}));
    }

    /// The group config → id config merge happens eagerly when the id is
    /// defined (config.py:66-69): group config added by a *later* file
    /// applies only to ids defined from that point on, never retroactively.
    #[test]
    fn later_group_config_does_not_retroactively_apply() {
        let dir = tempfile::tempdir().unwrap();
        write_file(
            dir.path(),
            "a.toml",
            r#"
[group.g]
config.impl_class = "cls"
config.a = 1

[group.g.inference_ids.early]
"#,
        );
        write_file(
            dir.path(),
            "b.toml",
            r#"
[group.g]
config.b = 2

[group.g.inference_ids.late]
"#,
        );
        let registry = registry_for(&[dir.path()]).expect("fixture loads");

        // Defined before b.toml's group config existed: no `b`.
        let early = registry.spawn_spec("g/early").expect("resolves");
        assert_eq!(early.config_kwargs, json!({"a": 1}));

        // Defined after the merge: sees both keys.
        let late = registry.spawn_spec("g/late").expect("resolves");
        assert_eq!(late.config_kwargs, json!({"a": 1, "b": 2}));
    }

    /// A later file redefining an existing inference id without setting
    /// `allow_override = true` is a hard error that fails the entire load
    /// (config.py:56-63,77-79) — nothing is skipped or partially loaded.
    #[test]
    fn duplicate_id_without_allow_override_fails_whole_load() {
        let base = tempfile::tempdir().unwrap();
        let user = tempfile::tempdir().unwrap();
        write_file(
            base.path(),
            "base.toml",
            r#"
[group.g.inference_ids.x]
config.v = 1
"#,
        );
        write_file(
            user.path(),
            "user.toml",
            r#"
[group.g.inference_ids.x]
config.v = 2
"#,
        );
        let err =
            registry_for(&[base.path(), user.path()]).expect_err("duplicate id must fail the load");
        assert!(
            format!("{err:#}").contains("Duplicate inference_id 'g/x'"),
            "unexpected error: {err:#}"
        );
    }

    /// With `allow_override = true` in the *later* file, redefinition is
    /// allowed and fully replaces the entry (config.py:39,56-76): new config
    /// re-merged against the current group config, new metadata replacing
    /// the old. Group metadata still merges across files regardless.
    #[test]
    fn allow_override_lets_later_file_redefine_id() {
        let base = tempfile::tempdir().unwrap();
        let user = tempfile::tempdir().unwrap();
        write_file(
            base.path(),
            "base.toml",
            r#"
[group.g]
config.impl_class = "cls"
metadata.name = "G"

[group.g.inference_ids.x]
config.v = 1
metadata.description = "old"
"#,
        );
        write_file(
            user.path(),
            "user.toml",
            r#"
allow_override = true

[group.g]
metadata.extra = "added"

[group.g.inference_ids.x]
config.v = 2
"#,
        );
        let registry = registry_for(&[base.path(), user.path()]).expect("override load succeeds");

        let spec = registry.spawn_spec("g/x").expect("resolves");
        assert_eq!(spec.config_kwargs, json!({"v": 2}), "later definition wins");

        let group = &registry.groups["g"];
        // Metadata was replaced (not merged) by the redefinition, which had
        // no metadata table -> empty.
        assert_eq!(group.inference_ids["x"].metadata, JsonMap::new());
        // Group metadata merged across both files.
        assert_eq!(group.group_metadata.get("name"), Some(&json!("G")));
        assert_eq!(group.group_metadata.get("extra"), Some(&json!("added")));
    }

    /// metadata_json() must produce exactly the Python /metadata shape
    /// (config.py:105-118): per group a "group_metadata" object and an
    /// "inference_ids" object mapping id -> id metadata only. Verified
    /// against a hand-written literal.
    #[test]
    fn metadata_json_matches_python_shape() {
        let dir = tempfile::tempdir().unwrap();
        write_file(
            dir.path(),
            "a.toml",
            r#"
[group.g]
config.impl_class = "cls"

[group.g.metadata]
name = "G"
default_batch_size = 8

[group.g.inference_ids.i]
config.x = 1
metadata.description = "d"

[group.g.inference_ids.bare]
"#,
        );
        let registry = registry_for(&[dir.path()]).expect("fixture loads");

        assert_eq!(
            registry.metadata_json(),
            json!({
                "g": {
                    "group_metadata": {"name": "G", "default_batch_size": 8},
                    "inference_ids": {
                        "i": {"description": "d"},
                        "bare": {}
                    }
                }
            })
        );
    }

    /// Unparseable TOML in any scanned file fails the entire load — Python
    /// logs and re-raises (config.py:77-79); it does not skip the bad file.
    #[test]
    fn unparseable_toml_fails_whole_load() {
        let dir = tempfile::tempdir().unwrap();
        write_file(
            dir.path(),
            "good.toml",
            r#"
[group.g.inference_ids.x]
config.v = 1
"#,
        );
        write_file(dir.path(), "z_bad.toml", "this is [not valid toml");
        let err = registry_for(&[dir.path()]).expect_err("bad TOML must fail the load");
        assert!(
            format!("{err:#}").contains("z_bad.toml"),
            "error should name the offending file: {err:#}"
        );
    }

    /// A missing config directory is skipped with a warning, not an error
    /// (config.py:29-31): loading proceeds with the remaining directories.
    #[test]
    fn missing_directory_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        write_file(
            dir.path(),
            "a.toml",
            r#"
[group.g]
config.impl_class = "cls"
[group.g.inference_ids.x]
"#,
        );
        let missing = dir.path().join("does-not-exist");
        let registry = registry_for(&[dir.path(), &missing]).expect("missing dir is not fatal");
        assert!(registry.groups["g"].inference_ids.contains_key("x"));
    }

    /// The cache reloads only when a scanned file's mtime advances past the
    /// recorded one (config.py:90): content changes with an unchanged mtime
    /// keep the old snapshot; an mtime bump triggers a re-parse that
    /// reflects the new content.
    #[test]
    fn cache_reloads_on_mtime_bump_only() {
        let dir = tempfile::tempdir().unwrap();
        let file = write_file(
            dir.path(),
            "a.toml",
            r#"
[group.g]
config.impl_class = "cls"
[group.g.inference_ids.x]
config.v = 1
"#,
        );
        let mut cache = RegistryCache::new(RegistryConfig {
            config_dirs: vec![dir.path().to_path_buf()],
        });

        let first = cache.get().expect("initial load");
        let second = cache.get().expect("cached load");
        assert!(
            Arc::ptr_eq(&first, &second),
            "unchanged mtime returns the same snapshot"
        );

        // Rewrite the content but pin the mtime back: must stay cached,
        // proving the trigger is mtime, not content.
        let original_mtime = fs::metadata(&file).unwrap().modified().unwrap();
        fs::write(
            &file,
            r#"
[group.g]
config.impl_class = "cls"
[group.g.inference_ids.x]
config.v = 2
"#,
        )
        .unwrap();
        set_mtime(&file, original_mtime);
        let third = cache.get().expect("still cached");
        assert!(
            Arc::ptr_eq(&first, &third),
            "same mtime keeps the old snapshot"
        );

        // Bump the mtime forward: reload picks up the new content.
        set_mtime(&file, original_mtime + Duration::from_secs(2));
        let fourth = cache.get().expect("reloaded");
        assert!(!Arc::ptr_eq(&first, &fourth), "mtime bump reloads");
        let spec = fourth.spawn_spec("g/x").expect("resolves");
        assert_eq!(spec.config_kwargs, json!({"v": 2}));
    }

    /// Python's cache-validity check is `if config and mtime and ...`
    /// (config.py:90): an empty registry (no groups → falsy dict) is never
    /// treated as a valid cache, so every call re-parses. Replicated here:
    /// two gets over an empty directory return distinct snapshots.
    #[test]
    fn cache_never_reuses_empty_registry() {
        let dir = tempfile::tempdir().unwrap();
        let mut cache = RegistryCache::new(RegistryConfig {
            config_dirs: vec![dir.path().to_path_buf()],
        });
        let first = cache.get().expect("empty load succeeds");
        assert!(first.groups.is_empty());
        let second = cache.get().expect("empty load succeeds again");
        assert!(
            !Arc::ptr_eq(&first, &second),
            "empty registry is reloaded on every call, matching Python"
        );
    }

    /// spawn_spec error paths mirror get_model_config (config.py:143-150):
    /// unknown group and unknown inference id are distinct errors, and an id
    /// whose merged config lacks impl_class fails resolution (Python fails
    /// the equivalent lookup worker-side, process_model.py:215-217).
    #[test]
    fn spawn_spec_error_paths() {
        let dir = tempfile::tempdir().unwrap();
        write_file(
            dir.path(),
            "a.toml",
            r#"
[group.g.inference_ids.noimpl]
config.v = 1
"#,
        );
        let registry = registry_for(&[dir.path()]).expect("fixture loads");

        let err = registry.spawn_spec("nope/x").expect_err("unknown group");
        assert!(format!("{err:#}").contains("Group 'nope' not found"));

        let err = registry.spawn_spec("g/nope").expect_err("unknown id");
        assert!(format!("{err:#}").contains("Inference ID 'nope' not found in group 'g'"));

        let err = registry
            .spawn_spec("g/noimpl")
            .expect_err("missing impl_class");
        assert!(format!("{err:#}").contains("impl_class"));

        let err = registry.spawn_spec("no-slash").expect_err("malformed id");
        assert!(format!("{err:#}").contains("group/name"));
    }

    /// WorkerSet resolution (design §8): `devices` alone sets the replica
    /// count to its length with replica i pinned to devices[i]; `replicas`
    /// alone pins "0".."N-1"; neither yields today's single unpinned
    /// replica; both given with *matching* lengths is allowed (devices win,
    /// same shape). In every case both keys are stripped from the spawn
    /// kwargs exactly like ray_config — impl constructors never see them.
    #[test]
    fn replicas_and_devices_resolve_to_device_pins() {
        let dir = tempfile::tempdir().unwrap();
        write_file(
            dir.path(),
            "a.toml",
            r#"
[group.g]
config.impl_class = "cls"

[group.g.inference_ids.devs]
config.devices = ["3", "7"]
config.x = 1

[group.g.inference_ids.reps]
config.replicas = 3

[group.g.inference_ids.plain]

[group.g.inference_ids.both]
config.replicas = 2
config.devices = ["a", "b"]
"#,
        );
        let registry = registry_for(&[dir.path()]).expect("fixture loads");

        let spec = registry.spawn_spec("g/devs").expect("resolves");
        assert_eq!(
            spec.device_pins,
            vec![Some("3".to_string()), Some("7".to_string())],
            "devices give the count and per-replica pins"
        );
        assert_eq!(
            spec.config_kwargs,
            json!({"x": 1}),
            "devices stripped from kwargs like ray_config"
        );

        let spec = registry.spawn_spec("g/reps").expect("resolves");
        assert_eq!(
            spec.device_pins,
            vec![
                Some("0".to_string()),
                Some("1".to_string()),
                Some("2".to_string())
            ],
            "replicas=N alone pins \"0\"..\"N-1\""
        );
        assert_eq!(
            spec.config_kwargs,
            json!({}),
            "replicas stripped from kwargs"
        );

        let spec = registry.spawn_spec("g/plain").expect("resolves");
        assert_eq!(
            spec.device_pins,
            vec![None],
            "neither key -> one replica, no pin (Phase 1 behavior)"
        );

        let spec = registry.spawn_spec("g/both").expect("resolves");
        assert_eq!(
            spec.device_pins,
            vec![Some("a".to_string()), Some("b".to_string())],
            "consistent replicas+devices resolve to the device pins"
        );
        assert_eq!(spec.config_kwargs, json!({}));
    }

    /// `replicas` and `devices` are ordinary config keys, so they inherit
    /// from group config like everything else, and id-level config
    /// overrides them per key.
    #[test]
    fn replicas_devices_inherit_from_group_config() {
        let dir = tempfile::tempdir().unwrap();
        write_file(
            dir.path(),
            "a.toml",
            r#"
[group.g]
config.impl_class = "cls"
config.devices = ["0", "1"]

[group.g.inference_ids.inherits]

[group.g.inference_ids.overrides]
config.devices = ["5"]
"#,
        );
        let registry = registry_for(&[dir.path()]).expect("fixture loads");

        let spec = registry.spawn_spec("g/inherits").expect("resolves");
        assert_eq!(
            spec.device_pins,
            vec![Some("0".to_string()), Some("1".to_string())],
            "group-level devices inherited by the id"
        );

        let spec = registry.spawn_spec("g/overrides").expect("resolves");
        assert_eq!(
            spec.device_pins,
            vec![Some("5".to_string())],
            "id-level devices override the group's"
        );
    }

    /// replicas contradicting devices (mismatched lengths) is a *registry
    /// load* error naming the offending inference id — explicit beats
    /// silent, and nothing is partially loaded (same policy as duplicate
    /// ids and unparseable TOML).
    #[test]
    fn replicas_devices_mismatch_fails_registry_load() {
        let dir = tempfile::tempdir().unwrap();
        write_file(
            dir.path(),
            "a.toml",
            r#"
[group.g]
config.impl_class = "cls"

[group.g.inference_ids.bad]
config.replicas = 3
config.devices = ["0", "1"]
"#,
        );
        let err = registry_for(&[dir.path()]).expect_err("mismatch must fail the load");
        let text = format!("{err:#}");
        assert!(
            text.contains("g/bad") && text.contains("contradicts"),
            "error names the id and the contradiction: {text}"
        );
    }

    /// Malformed replicas/devices values fail the registry load: replicas
    /// must be an integer >= 1, devices must be a non-empty array of
    /// strings. The group-inheritance path is covered too — a bad
    /// group-level value fails at the first id that inherits it.
    #[test]
    fn invalid_replicas_or_devices_fail_registry_load() {
        let cases = [
            ("config.replicas = 0", "replicas"),
            ("config.replicas = -2", "replicas"),
            ("config.replicas = 1.5", "replicas"),
            ("config.replicas = \"two\"", "replicas"),
            // Above the 64-process ceiling: a typo'd huge value must be a
            // load error, not an eager multi-billion-entry pin allocation.
            ("config.replicas = 65", "replicas"),
            ("config.replicas = 30000000000", "replicas"),
            ("config.devices = []", "devices"),
            ("config.devices = [3, 7]", "devices"),
            ("config.devices = \"3\"", "devices"),
        ];
        for (line, key) in cases {
            let dir = tempfile::tempdir().unwrap();
            write_file(
                dir.path(),
                "a.toml",
                &format!(
                    r#"
[group.g]
config.impl_class = "cls"
{line}

[group.g.inference_ids.x]
"#
                ),
            );
            let err = match registry_for(&[dir.path()]) {
                Ok(_) => panic!("`{line}` must fail the registry load"),
                Err(err) => err,
            };
            let text = format!("{err:#}");
            assert!(
                text.contains(key) && text.contains("g/x"),
                "error for `{line}` names '{key}' and the id: {text}"
            );
        }
    }

    /// Env templating applies to registry files: `${VAR}` in `config.*`
    /// values reaches the merged config and the spawn kwargs (the mechanism
    /// for feeding secrets/URLs to impl constructors), `${VAR:-default}`
    /// falls back when unset, and an unset `${VAR}` without a default fails
    /// the whole load naming the file and the variable.
    #[test]
    fn registry_values_are_env_templated() {
        let dir = tempfile::tempdir().unwrap();
        write_file(
            dir.path(),
            "a.toml",
            r#"
[group.g]
config.impl_class = "cls"

[group.g.inference_ids.x]
config.api_key = "${REGISTRY_TEST_KEY}"
config.endpoint = "${REGISTRY_TEST_ENDPOINT:-https://default.example}"
"#,
        );

        // Unset required var: the load fails, naming file and variable.
        let err = registry_for(&[dir.path()]).expect_err("unset ${VAR} fails the load");
        let text = format!("{err:#}");
        assert!(text.contains("REGISTRY_TEST_KEY"), "{text}");
        assert!(text.contains("a.toml"), "{text}");

        unsafe { std::env::set_var("REGISTRY_TEST_KEY", "sekrit") };
        let registry = registry_for(&[dir.path()]);
        unsafe { std::env::remove_var("REGISTRY_TEST_KEY") };
        let registry = registry.expect("loads once the variable is set");

        let spec = registry.spawn_spec("g/x").expect("resolves");
        assert_eq!(
            spec.config_kwargs,
            json!({"api_key": "sekrit", "endpoint": "https://default.example"}),
            "substituted values reach the impl constructor kwargs"
        );
    }

    /// The mtime-gated reload re-substitutes: after the env value changes,
    /// a reload (triggered by an mtime bump) reflects the new value.
    #[test]
    fn cache_reload_resubstitutes_env() {
        let dir = tempfile::tempdir().unwrap();
        let file = write_file(
            dir.path(),
            "a.toml",
            r#"
[group.g]
config.impl_class = "cls"
[group.g.inference_ids.x]
config.token = "${REGISTRY_RELOAD_TOKEN:-none}"
"#,
        );
        let mut cache = RegistryCache::new(RegistryConfig {
            config_dirs: vec![dir.path().to_path_buf()],
        });

        let first = cache.get().expect("initial load");
        assert_eq!(
            first.spawn_spec("g/x").unwrap().config_kwargs,
            json!({"token": "none"})
        );

        unsafe { std::env::set_var("REGISTRY_RELOAD_TOKEN", "fresh") };
        let mtime = fs::metadata(&file).unwrap().modified().unwrap();
        set_mtime(&file, mtime + Duration::from_secs(2));
        let reloaded = cache.get();
        unsafe { std::env::remove_var("REGISTRY_RELOAD_TOKEN") };
        let reloaded = reloaded.expect("reload succeeds");
        assert_eq!(
            reloaded.spawn_spec("g/x").unwrap().config_kwargs,
            json!({"token": "fresh"}),
            "reload re-runs substitution with the current environment"
        );
    }

    /// inference_id_metadata mirrors Python get_metadata (config.py:120-136):
    /// group metadata + id metadata under the exact Python key names, None
    /// for unknown group or id.
    #[test]
    fn inference_id_metadata_shape() {
        let dir = tempfile::tempdir().unwrap();
        write_file(
            dir.path(),
            "a.toml",
            r#"
[group.g.metadata]
name = "G"
[group.g.inference_ids.i]
metadata.description = "d"
"#,
        );
        let registry = registry_for(&[dir.path()]).expect("fixture loads");
        assert_eq!(
            registry.inference_id_metadata("g", "i"),
            Some(json!({
                "group_metadata": {"name": "G"},
                "inference_id_metadata": {"description": "d"}
            }))
        );
        assert_eq!(registry.inference_id_metadata("g", "missing"), None);
        assert_eq!(registry.inference_id_metadata("missing", "i"), None);
    }
}
