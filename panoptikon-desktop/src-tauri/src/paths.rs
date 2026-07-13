use serde::Serialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DesktopPaths {
    pub config_dir: PathBuf,
    pub local_data_dir: PathBuf,
    pub log_dir: PathBuf,
    pub desktop_settings: PathBuf,
    pub relay_settings: PathBuf,
    pub relay_secrets: PathBuf,
    pub server_root: PathBuf,
    pub desktop_log: PathBuf,
    pub bootstrap_log: PathBuf,
}

impl DesktopPaths {
    pub fn new(config_dir: PathBuf, local_data_dir: PathBuf, log_dir: PathBuf) -> Self {
        Self {
            desktop_settings: config_dir.join("desktop.toml"),
            relay_settings: config_dir.join("relay.toml"),
            relay_secrets: config_dir.join("relay-secrets.toml"),
            server_root: local_data_dir.join("server"),
            desktop_log: log_dir.join("panoptikon-desktop.log"),
            bootstrap_log: log_dir.join("bootstrap.log"),
            config_dir,
            local_data_dir,
            log_dir,
        }
    }

    pub fn ensure_shell_dirs(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.config_dir)?;
        std::fs::create_dir_all(&self.local_data_dir)?;
        std::fs::create_dir_all(&self.log_dir)
    }

    pub fn materialize_server_root(&self) -> std::io::Result<()> {
        for relative in [
            "config/server",
            "config/inference",
            "data",
            "inferio_custom",
            "runtime",
        ] {
            std::fs::create_dir_all(self.server_root.join(relative))?;
        }
        Ok(())
    }

    pub fn instance_id_path(&self) -> PathBuf {
        self.server_root.join("runtime/desktop-instance-id")
    }

    pub fn path_is_within_shell_roots(&self, path: &Path) -> bool {
        path.starts_with(&self.config_dir)
            || path.starts_with(&self.local_data_dir)
            || path.starts_with(&self.log_dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Desktop paths are derived only from resolved platform homes and never
    /// from the launch working directory.
    #[test]
    fn roots_are_stable_and_separated() {
        let paths = DesktopPaths::new(
            "C:/cfg/Panoptikon".into(),
            "D:/data/Panoptikon".into(),
            "E:/logs/Panoptikon".into(),
        );
        assert_eq!(
            paths.server_root,
            PathBuf::from("D:/data/Panoptikon/server")
        );
        assert_eq!(
            paths.desktop_settings,
            PathBuf::from("C:/cfg/Panoptikon/desktop.toml")
        );
        assert!(!paths.server_root.starts_with(&paths.config_dir));
    }

    /// Relay-only initialization creates shell directories but never touches
    /// the local Server root until explicitly materialized.
    #[test]
    fn relay_only_does_not_touch_server_root() {
        let temp = tempfile::tempdir().unwrap();
        let paths = DesktopPaths::new(
            temp.path().join("cfg"),
            temp.path().join("data"),
            temp.path().join("log"),
        );
        paths.ensure_shell_dirs().unwrap();
        assert!(!paths.server_root.exists());
        paths.materialize_server_root().unwrap();
        assert!(paths.server_root.join("runtime").is_dir());
    }
}
