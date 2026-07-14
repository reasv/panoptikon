//! Server-side half of the private Panoptikon Desktop lifecycle contract.

use anyhow::{Context, bail};
use fs2::FileExt as _;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

static DESKTOP_MANAGED: AtomicBool = AtomicBool::new(false);

pub(crate) fn set_managed(value: bool) {
    DESKTOP_MANAGED.store(value, Ordering::Release);
}

pub(crate) fn is_managed() -> bool {
    DESKTOP_MANAGED.load(Ordering::Acquire)
}

/// Held for the lifetime of a serving process. File locking is advisory and
/// automatically released by the OS on crash or normal process exit.
pub(crate) struct RootLock {
    _file: File,
    #[allow(dead_code)]
    path: PathBuf,
}

impl RootLock {
    pub(crate) fn acquire(root: PathBuf) -> anyhow::Result<Self> {
        let runtime = root.join("runtime");
        std::fs::create_dir_all(&runtime).with_context(|| {
            format!(
                "failed to create Server runtime directory '{}'",
                runtime.display()
            )
        })?;
        let path = runtime.join("server.lock");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("failed to open root lock '{}'", path.display()))?;
        if let Err(error) = file.try_lock_exclusive() {
            bail!(
                "Panoptikon Server root '{}' is already owned by another process (lock '{}'): {error}. Stop the other Server or Panoptikon Desktop instance before using this root.",
                root.display(),
                path.display()
            );
        }
        file.set_len(0).ok();
        use std::io::Write as _;
        writeln!(&file, "pid={}", std::process::id()).ok();
        Ok(Self { _file: file, path })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A second process handle cannot acquire a root while the first lock is
    /// alive; dropping the owner releases it for recovery/restart.
    #[test]
    fn root_lock_is_exclusive_and_released_on_drop() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        let first = RootLock::acquire(root.clone()).unwrap();
        let error = RootLock::acquire(root.clone()).err().unwrap().to_string();
        assert!(error.contains("already owned"), "{error}");
        assert!(error.contains(&root.display().to_string()), "{error}");
        drop(first);
        RootLock::acquire(root).unwrap();
    }

    /// The Desktop marker is process-global diagnostics state and can be
    /// toggled deterministically without changing API behavior.
    #[test]
    fn managed_marker_round_trips() {
        set_managed(true);
        assert!(is_managed());
        set_managed(false);
        assert!(!is_managed());
    }
}
