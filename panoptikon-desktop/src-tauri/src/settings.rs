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
    /// Last authoritative successful check. The serialized name is retained
    /// for compatibility with the initial Desktop updater.
    #[serde(default)]
    pub last_checked_unix: Option<i64>,
    #[serde(default)]
    pub last_attempt_unix: Option<i64>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub last_error_unix: Option<i64>,
    #[serde(default)]
    pub consecutive_failures: u32,
    #[serde(default)]
    pub automatic_attempts_unix: Vec<i64>,
    #[serde(default)]
    pub latest_version: Option<String>,
    #[serde(default)]
    pub latest_published_at: Option<String>,
    #[serde(default)]
    pub latest_notes_markdown: Option<String>,
    #[serde(default)]
    pub latest_release_url: Option<String>,
    #[serde(default)]
    pub discovered_unix: Option<i64>,
    #[serde(default)]
    pub native_notified_version: Option<String>,
    /// Version already surfaced either through an accepted native
    /// notification or through an intentional foreground update flow.
    #[serde(default)]
    pub native_surfaced_version: Option<String>,
    /// Target associated with the most recent native-notification attempt.
    #[serde(default)]
    pub native_notification_attempt_version: Option<String>,
    #[serde(default)]
    pub native_notification_last_attempt_unix: Option<i64>,
    #[serde(default)]
    pub ribbon_snoozed_until_unix: Option<i64>,
    #[serde(default)]
    pub ribbon_dismissed_version: Option<String>,
    #[serde(default)]
    pub reminder_version: Option<String>,
    #[serde(default)]
    pub reminder_at_unix: Option<i64>,
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
            last_attempt_unix: None,
            last_error: None,
            last_error_unix: None,
            consecutive_failures: 0,
            automatic_attempts_unix: Vec::new(),
            latest_version: None,
            latest_published_at: None,
            latest_notes_markdown: None,
            latest_release_url: None,
            discovered_unix: None,
            native_notified_version: None,
            native_surfaced_version: None,
            native_notification_attempt_version: None,
            native_notification_last_attempt_unix: None,
            ribbon_snoozed_until_unix: None,
            ribbon_dismissed_version: None,
            reminder_version: None,
            reminder_at_unix: None,
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
                // TOML has no null value, so serde omits `None` fields. Remove
                // the optional keys owned by the current schema before merging
                // the serialized values; otherwise an old `Some` value in the
                // raw document would survive after the typed value was cleared.
                if section == "updates" {
                    for key in OPTIONAL_UPDATE_KEYS {
                        existing.remove(*key);
                    }
                }
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

const OPTIONAL_UPDATE_KEYS: &[&str] = &[
    "last_checked_unix",
    "last_attempt_unix",
    "last_error",
    "last_error_unix",
    "latest_version",
    "latest_published_at",
    "latest_notes_markdown",
    "latest_release_url",
    "discovered_unix",
    "native_notified_version",
    "native_surfaced_version",
    "native_notification_attempt_version",
    "native_notification_last_attempt_unix",
    "ribbon_snoozed_until_unix",
    "ribbon_dismissed_version",
    "reminder_version",
    "reminder_at_unix",
];

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

    #[test]
    fn update_awareness_state_round_trips() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("desktop.toml");
        let mut document = SettingsDocument::defaults(path.clone()).unwrap();
        document.typed.updates.last_attempt_unix = Some(100);
        document.typed.updates.last_checked_unix = Some(90);
        document.typed.updates.latest_version = Some("0.2.0".into());
        document.typed.updates.ribbon_dismissed_version = Some("0.2.0".into());
        document.typed.updates.native_surfaced_version = Some("0.2.0".into());
        document.typed.updates.native_notification_attempt_version = Some("0.2.0".into());
        document.typed.updates.native_notification_last_attempt_unix = Some(100);
        document.typed.updates.automatic_attempts_unix = vec![80, 100];
        document.save().unwrap();

        let restored = SettingsDocument::load_path(&path).unwrap();
        assert_eq!(restored.typed.updates.last_attempt_unix, Some(100));
        assert_eq!(
            restored.typed.updates.latest_version.as_deref(),
            Some("0.2.0")
        );
        assert_eq!(
            restored.typed.updates.ribbon_dismissed_version.as_deref(),
            Some("0.2.0")
        );
        assert_eq!(restored.typed.updates.automatic_attempts_unix, [80, 100]);
        assert_eq!(
            restored.typed.updates.native_surfaced_version.as_deref(),
            Some("0.2.0")
        );
        assert_eq!(
            restored.typed.updates.native_notification_last_attempt_unix,
            Some(100)
        );
    }

    /// Clearing typed optional values removes their old TOML keys without
    /// discarding settings written by a newer Desktop version.
    #[test]
    fn save_removes_cleared_known_values_and_preserves_unknown_values() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("desktop.toml");
        std::fs::write(
            &path,
            concat!(
                "[updates]\n",
                "latest_version = '0.2.0'\n",
                "last_error = 'offline'\n",
                "ribbon_snoozed_until_unix = 1234\n",
                "reminder_version = '0.2.0'\n",
                "future_update_setting = 'keep'\n",
                "[future_section]\n",
                "flag = true\n",
            ),
        )
        .unwrap();

        let mut document = SettingsDocument::load_path(&path).unwrap();
        document.typed.updates.latest_version = None;
        document.typed.updates.last_error = None;
        document.typed.updates.ribbon_snoozed_until_unix = None;
        document.typed.updates.reminder_version = None;
        document.save().unwrap();

        let raw: toml::Value = toml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        let updates = raw["updates"].as_table().unwrap();
        assert!(!updates.contains_key("latest_version"));
        assert!(!updates.contains_key("last_error"));
        assert!(!updates.contains_key("ribbon_snoozed_until_unix"));
        assert!(!updates.contains_key("reminder_version"));
        assert_eq!(updates["future_update_setting"].as_str(), Some("keep"));
        assert_eq!(raw["future_section"]["flag"].as_bool(), Some(true));

        let restored = SettingsDocument::load_path(&path).unwrap();
        assert_eq!(restored.typed.updates.latest_version, None);
        assert_eq!(restored.typed.updates.last_error, None);
        assert_eq!(restored.typed.updates.ribbon_snoozed_until_unix, None);
        assert_eq!(restored.typed.updates.reminder_version, None);
    }

    /// Keep the removal list exhaustive when the persisted update schema
    /// gains another optional field.
    #[test]
    fn optional_update_key_list_matches_the_serialized_schema() {
        let populated = UpdateSettings {
            check_automatically: true,
            last_checked_unix: Some(1),
            last_attempt_unix: Some(2),
            last_error: Some("offline".into()),
            last_error_unix: Some(3),
            consecutive_failures: 1,
            automatic_attempts_unix: vec![2],
            latest_version: Some("0.2.0".into()),
            latest_published_at: Some("2026-01-01".into()),
            latest_notes_markdown: Some("notes".into()),
            latest_release_url: Some("https://example.invalid/release".into()),
            discovered_unix: Some(4),
            native_notified_version: Some("0.2.0".into()),
            native_surfaced_version: Some("0.2.0".into()),
            native_notification_attempt_version: Some("0.2.0".into()),
            native_notification_last_attempt_unix: Some(4),
            ribbon_snoozed_until_unix: Some(5),
            ribbon_dismissed_version: Some("0.2.0".into()),
            reminder_version: Some("0.2.0".into()),
            reminder_at_unix: Some(6),
        };
        let populated = toml::Value::try_from(&populated).unwrap();
        let defaults = toml::Value::try_from(UpdateSettings::default()).unwrap();
        let populated = populated.as_table().unwrap();
        let defaults = defaults.as_table().unwrap();
        let serialized_optional = populated
            .keys()
            .filter(|key| !defaults.contains_key(*key))
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>();
        let declared_optional = OPTIONAL_UPDATE_KEYS
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(serialized_optional, declared_optional);
    }
}
