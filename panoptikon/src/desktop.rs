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
        std::fs::create_dir_all(&runtime)
            .with_context(|| {
                format!(
                    "failed to create Server runtime directory '{}'",
                    runtime.display()
                )
            })
            .map_err(|err| crate::ownership::explain(err, &root, std::slice::from_ref(&root)))?;
        let path = runtime.join("server.lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("failed to open root lock '{}'", path.display()))
            .map_err(|err| {
                crate::ownership::explain(err, &runtime, &[runtime.clone(), path.clone()])
            })?;
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

    /// A root, then only its `runtime/`, that another user owns: both
    /// failures name the folder and its owner above the original error.
    #[cfg(unix)]
    #[test]
    fn a_root_another_user_owns_is_named() {
        use crate::ownership::tests::{foreign_folder, owned_by_another_user};
        let Some((folder, owner)) = foreign_folder(false) else {
            return;
        };
        let error = RootLock::acquire(folder.to_path_buf()).err().unwrap();
        let expected = owned_by_another_user(folder, owner, folder);
        assert!(format!("{error:#}").starts_with(&expected), "{error:#}");

        let temp = tempfile::tempdir().unwrap();
        let runtime = temp.path().join("runtime");
        std::os::unix::fs::symlink(folder, &runtime).unwrap();
        let error = RootLock::acquire(temp.path().to_path_buf()).err().unwrap();
        let expected = owned_by_another_user(&runtime, owner, folder);
        assert!(format!("{error:#}").starts_with(&expected), "{error:#}");
        assert!(
            format!("{error:#}").contains("failed to open root lock"),
            "{error:#}"
        );
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
