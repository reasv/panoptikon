use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{
    collections::BTreeMap,
    env, fs,
    path::{Component, Path, PathBuf},
};
use utoipa::ToSchema;

use crate::api_error::ApiError;
use crate::pql::model::{JobFilter, Match};

type ApiResult<T> = std::result::Result<T, ApiError>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub(crate) struct CronJob {
    pub inference_id: String,
    #[serde(default)]
    pub batch_size: Option<i64>,
    #[serde(default)]
    pub threshold: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub(crate) struct JobSettings {
    pub group_name: String,
    #[serde(default)]
    pub inference_id: Option<String>,
    #[serde(default)]
    pub default_batch_size: Option<i64>,
    #[serde(default)]
    pub default_threshold: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct SystemConfig {
    #[serde(default = "default_true")]
    pub remove_unavailable_files: bool,
    #[serde(default = "default_true")]
    pub scan_images: bool,
    #[serde(default = "default_true")]
    pub scan_video: bool,
    #[serde(default)]
    pub scan_audio: bool,
    #[serde(default)]
    pub scan_html: bool,
    #[serde(default)]
    pub scan_pdf: bool,
    #[serde(default)]
    pub enable_cron_job: bool,
    #[serde(default = "default_cron_schedule")]
    pub cron_schedule: String,
    #[serde(default)]
    pub cron_jobs: Vec<CronJob>,
    #[serde(default)]
    pub job_settings: Vec<JobSettings>,
    #[serde(default)]
    pub included_folders: Vec<String>,
    #[serde(default)]
    pub excluded_folders: Vec<String>,
    #[serde(default)]
    pub preload_embedding_models: bool,
    #[serde(default)]
    pub continuous_filescan: ContinuousFilescanConfig,

    /// PQL job filters (parsed).
    #[serde(default)]
    pub job_filters: Vec<JobFilter>,
    /// PQL filter applied during file scans (parsed).
    #[serde(default)]
    pub filescan_filter: Option<Match>,

    /// Unknown keys are preserved to keep forward/backward compatibility.
    #[serde(flatten)]
    #[schema(value_type = std::collections::BTreeMap<String, JsonValue>)]
    pub extra: BTreeMap<String, toml::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
pub(crate) struct ContinuousFilescanConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub poll_interval_secs: Option<u64>,
    #[serde(default)]
    pub included_folders: Vec<String>,
}

fn default_true() -> bool {
    true
}

fn default_cron_schedule() -> String {
    "0 3 * * *".to_string()
}

impl Default for SystemConfig {
    fn default() -> Self {
        Self {
            remove_unavailable_files: true,
            scan_images: true,
            scan_video: true,
            scan_audio: false,
            scan_html: false,
            scan_pdf: false,
            enable_cron_job: false,
            cron_schedule: default_cron_schedule(),
            cron_jobs: Vec::new(),
            job_settings: Vec::new(),
            included_folders: Vec::new(),
            excluded_folders: Vec::new(),
            preload_embedding_models: false,
            continuous_filescan: ContinuousFilescanConfig {
                enabled: false,
                poll_interval_secs: None,
                included_folders: Vec::new(),
            },
            job_filters: Vec::new(),
            filescan_filter: None,
            extra: BTreeMap::new(),
        }
    }
}

pub(crate) struct SystemConfigStore {
    data_dir: PathBuf,
}

impl SystemConfigStore {
    pub(crate) fn from_env() -> Self {
        let data_dir = env::var("DATA_FOLDER").unwrap_or_else(|_| "data".to_string());
        Self {
            data_dir: PathBuf::from(data_dir),
        }
    }

    pub(crate) fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    pub(crate) fn config_path(&self, index_db: &str) -> PathBuf {
        self.data_dir
            .join("index")
            .join(index_db)
            .join("config.toml")
    }

    pub(crate) fn load(&self, index_db: &str) -> ApiResult<SystemConfig> {
        let config_path = self.config_path(index_db);
        if !config_path.exists() {
            let config = SystemConfig::default();
            self.save(index_db, &config)?;
            return Ok(config);
        }

        let raw = fs::read_to_string(&config_path).map_err(|err| {
            tracing::error!(error = %err, path = %config_path.display(), "failed to read system config");
            ApiError::internal("Failed to read system configuration")
        })?;
        let mut config: SystemConfig = toml::from_str(&raw).map_err(|err| {
            tracing::error!(error = %err, path = %config_path.display(), "failed to parse system config");
            ApiError::internal("Failed to parse system configuration")
        })?;

        normalize_folder_lists(&mut config);
        Ok(config)
    }

    pub(crate) fn save(&self, index_db: &str, config: &SystemConfig) -> ApiResult<()> {
        let config_path = self.config_path(index_db);
        if let Some(parent) = config_path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                tracing::error!(error = %err, path = %parent.display(), "failed to create config dir");
                ApiError::internal("Failed to prepare config directory")
            })?;
        }

        let mut normalized = config.clone();
        normalize_folder_lists(&mut normalized);

        let serialized = toml::to_string(&normalized).map_err(|err| {
            tracing::error!(error = %err, "failed to serialize system config");
            ApiError::internal("Failed to serialize system configuration")
        })?;
        fs::write(&config_path, serialized).map_err(|err| {
            tracing::error!(error = %err, path = %config_path.display(), "failed to write system config");
            ApiError::internal("Failed to write system configuration")
        })?;
        Ok(())
    }
}

fn normalize_folder_lists(config: &mut SystemConfig) {
    if !config.included_folders.is_empty() {
        config.included_folders = clean_folder_list(&config.included_folders);
    }
    if !config.excluded_folders.is_empty() {
        config.excluded_folders = clean_folder_list(&config.excluded_folders);
    }
    if !config.continuous_filescan.included_folders.is_empty() {
        config.continuous_filescan.included_folders =
            clean_folder_list(&config.continuous_filescan.included_folders);
    }
}

fn clean_folder_list(folder_list: &[String]) -> Vec<String> {
    folder_list
        .iter()
        .filter_map(|entry| {
            let trimmed = entry.trim();
            if trimmed.is_empty() {
                return None;
            }
            Some(normalize_path(trimmed))
        })
        .collect()
}

fn normalize_path(path: &str) -> String {
    let trimmed = path.trim();
    let mut buf = PathBuf::from(trimmed);
    if !buf.is_absolute() {
        if let Ok(cwd) = env::current_dir() {
            buf = cwd.join(buf);
        }
    }

    let mut normalized = PathBuf::new();
    for component in buf.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                let _ = normalized.pop();
            }
            Component::Prefix(_) | Component::RootDir => normalized.push(component.as_os_str()),
            Component::Normal(part) => normalized.push(part),
        }
    }

    ensure_trailing_separator(&normalized)
}

fn ensure_trailing_separator(path: &Path) -> String {
    let mut out = path.to_string_lossy().to_string();
    if !out.ends_with(['\\', '/']) {
        out.push(std::path::MAIN_SEPARATOR);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // Ensures missing config files are created with defaults.
    #[test]
    fn load_creates_default_config_when_missing() {
        let tmp = TempDir::new().unwrap();
        let store = SystemConfigStore::new(tmp.path().to_path_buf());
        let config = store.load("default").unwrap();

        let default = SystemConfig::default();
        assert_eq!(
            config.remove_unavailable_files,
            default.remove_unavailable_files
        );
        assert_eq!(config.scan_images, default.scan_images);
        assert_eq!(config.scan_video, default.scan_video);
        assert!(config.job_filters.is_empty());
        assert!(config.filescan_filter.is_none());
        assert!(store.config_path("default").exists());
    }

    // Ensures job_filters and filescan_filter parse as PQL and round-trip with extra fields preserved.
    #[test]
    fn pql_fields_round_trip() {
        let tmp = TempDir::new().unwrap();
        let store = SystemConfigStore::new(tmp.path().to_path_buf());

        let raw = r#"
scan_images = true
scan_video = false

job_filters = [
  { setter_names = ["file_scan"], pql_query = { match = { gt = { size = 10 } } } }
]

filescan_filter = { match = { or = [
  { contains = { path = "png" } },
  { eq = { type = "image/png" } }
] } }

some_future_key = { a = 1, b = [1, 2, 3] }
"#;

        let path = store.config_path("default");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, raw).unwrap();

        let loaded = store.load("default").unwrap();
        assert_eq!(loaded.job_filters.len(), 1);
        assert!(loaded.filescan_filter.is_some());
        assert!(loaded.extra.contains_key("some_future_key"));

        store.save("default", &loaded).unwrap();

        let reloaded = store.load("default").unwrap();
        assert_eq!(reloaded.job_filters.len(), loaded.job_filters.len());
        assert!(reloaded.filescan_filter.is_some());
        assert!(reloaded.extra.contains_key("some_future_key"));
    }
}
