use crate::paths::DesktopPaths;
use anyhow::{Context as _, bail};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DesktopSettings {
    #[serde(default)]
    pub local_server: LocalServerSettings,
    #[serde(default)]
    pub startup: StartupSettings,
    #[serde(default)]
    pub updates: UpdateSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalServerSettings {
    #[serde(default = "yes")]
    pub enabled: bool,
    #[serde(default = "default_port")]
    pub port: u16,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct StartupSettings {
    #[serde(default)]
    pub start_at_login: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateSettings {
    #[serde(default = "yes")]
    pub check_automatically: bool,
    #[serde(default)]
    pub last_checked_unix: Option<i64>,
}

fn yes() -> bool {
    true
}
fn default_port() -> u16 {
    6342
}

impl Default for LocalServerSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            port: default_port(),
        }
    }
}
impl Default for UpdateSettings {
    fn default() -> Self {
        Self {
            check_automatically: true,
            last_checked_unix: None,
        }
    }
}
impl Default for DesktopSettings {
    fn default() -> Self {
        Self {
            local_server: LocalServerSettings::default(),
            startup: StartupSettings::default(),
            updates: UpdateSettings::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SettingsDocument {
    pub typed: DesktopSettings,
    raw: toml::Table,
    path: PathBuf,
}

impl SettingsDocument {
    pub fn defaults(path: PathBuf) -> anyhow::Result<Self> {
        let typed = DesktopSettings::default();
        let raw = toml::Value::try_from(&typed)?
            .as_table()
            .cloned()
            .unwrap_or_default();
        Ok(Self { typed, raw, path })
    }

    pub fn load(paths: &DesktopPaths) -> anyhow::Result<Self> {
        Self::load_path(&paths.desktop_settings)
    }

    pub fn load_path(path: &Path) -> anyhow::Result<Self> {
        if !path.exists() {
            return Self::defaults(path.to_path_buf());
        }
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read '{}'", path.display()))?;
        let raw: toml::Table = match toml::from_str(&content) {
            Ok(raw) => raw,
            Err(error) => {
                let quarantine = quarantine_path(path);
                std::fs::rename(path, &quarantine).with_context(|| {
                    format!(
                        "Desktop settings are invalid and could not be quarantined as '{}'",
                        quarantine.display()
                    )
                })?;
                bail!(
                    "Desktop settings '{}' are invalid and were quarantined as '{}': {error}",
                    path.display(),
                    quarantine.display()
                );
            }
        };
        let typed = match toml::Value::Table(raw.clone()).try_into() {
            Ok(typed) => typed,
            Err(error) => {
                let quarantine = quarantine_path(path);
                std::fs::rename(path, &quarantine)?;
                bail!(
                    "Desktop settings '{}' have an invalid schema and were quarantined as '{}': {error}",
                    path.display(),
                    quarantine.display()
                );
            }
        };
        Ok(Self {
            typed,
            raw,
            path: path.to_path_buf(),
        })
    }

    pub fn save(&mut self) -> anyhow::Result<()> {
        merge_known(&mut self.raw, &self.typed)?;
        let body = toml::to_string_pretty(&self.raw)?;
        atomic_write(&self.path, body.as_bytes())
    }
}

fn merge_known(raw: &mut toml::Table, settings: &DesktopSettings) -> anyhow::Result<()> {
    let known = toml::Value::try_from(settings)?
        .as_table()
        .cloned()
        .unwrap_or_default();
    for (section, value) in known {
        match (raw.get_mut(&section), value) {
            (Some(toml::Value::Table(existing)), toml::Value::Table(update)) => {
                for (key, value) in update {
                    existing.insert(key, value);
                }
            }
            (_, value) => {
                raw.insert(section, value);
            }
        }
    }
    Ok(())
}

pub fn atomic_write(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let parent = path.parent().context("settings path has no parent")?;
    std::fs::create_dir_all(parent)?;
    let tmp = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name().unwrap_or_default().to_string_lossy(),
        std::process::id()
    ));
    std::fs::write(&tmp, bytes)
        .with_context(|| format!("failed to write temporary settings '{}'", tmp.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    std::fs::rename(&tmp, path)
        .with_context(|| format!("failed to commit settings '{}'", path.display()))
}

fn quarantine_path(path: &Path) -> PathBuf {
    let stamp = time::OffsetDateTime::now_utc().unix_timestamp();
    path.with_extension(format!("toml.invalid-{stamp}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rewriting known settings preserves unknown future sections and keys.
    #[test]
    fn save_preserves_unknown_settings() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("desktop.toml");
        std::fs::write(
            &path,
            "[local_server]\nenabled=true\nfuture=42\n[future_section]\nflag='keep'\n",
        )
        .unwrap();
        let mut doc = SettingsDocument::load_path(&path).unwrap();
        doc.typed.local_server.enabled = false;
        doc.save().unwrap();
        let raw: toml::Value = toml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(raw["local_server"]["future"].as_integer(), Some(42));
        assert_eq!(raw["future_section"]["flag"].as_str(), Some("keep"));
        assert!(!raw["local_server"]["enabled"].as_bool().unwrap());
    }

    /// Corrupt settings are retained under a quarantine name and never
    /// silently overwritten with defaults.
    #[test]
    fn invalid_settings_are_quarantined() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("desktop.toml");
        std::fs::write(&path, "not [ valid").unwrap();
        let error = SettingsDocument::load_path(&path).unwrap_err().to_string();
        assert!(error.contains("quarantined"), "{error}");
        assert!(!path.exists());
        assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);
    }
}
