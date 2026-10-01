//! Names the file another user owns when stored state cannot be written
//! (a root left behind by a run as root, a container started under a
//! different uid).

use std::path::Path;

/// Fails when another user owns a database file or folder the current user
/// cannot write. Such a server would start and then fail on its first write.
pub(crate) fn check_databases(data_folder: &Path) -> anyhow::Result<()> {
    match reason(data_folder, &["index", "user_data"]) {
        Some(reason) => Err(anyhow::anyhow!(reason)),
        None => Ok(()),
    }
}

/// Adds to `err` the first entry under `dir` (itself included) that another
/// user owns and the current user cannot write; `err` unchanged without one.
pub(crate) fn explain(err: anyhow::Error, dir: &Path) -> anyhow::Error {
    match reason(dir, &[""]) {
        Some(reason) => err.context(reason),
        None => err,
    }
}

/// The first such entry under `root`'s `subdirs` (`""` is `root` itself), as
/// a message. Always `None` on non-Unix.
fn reason(root: &Path, subdirs: &[&str]) -> Option<String> {
    #[cfg(unix)]
    {
        let root = std::path::absolute(root).unwrap_or_else(|_| root.to_path_buf());
        let (path, owner) = subdirs.iter().find_map(|sub| {
            let dir = if sub.is_empty() {
                root.clone()
            } else {
                root.join(sub)
            };
            unix::first_foreign_unwritable(&dir)
        })?;
        // SAFETY: geteuid has no preconditions and cannot fail.
        let uid = unsafe { libc::geteuid() };
        Some(format!(
            "'{}' is owned by uid {owner} and is not writable by the current user \
             (uid {uid}); run as uid {owner}, or change the owner of '{}' to uid {uid}",
            path.display(),
            root.display()
        ))
    }
    #[cfg(not(unix))]
    {
        let _ = (root, subdirs);
        None
    }
}

#[cfg(unix)]
mod unix {
    use std::os::unix::ffi::OsStrExt as _;
    use std::os::unix::fs::MetadataExt as _;
    use std::path::{Path, PathBuf};

    /// Depth-first; symlinks are not followed.
    pub(super) fn first_foreign_unwritable(path: &Path) -> Option<(PathBuf, u32)> {
        let meta = path.symlink_metadata().ok()?;
        if meta.file_type().is_symlink() {
            return None;
        }
        // SAFETY: geteuid has no preconditions and cannot fail.
        if meta.uid() != unsafe { libc::geteuid() } && !writable(path) {
            return Some((path.to_path_buf(), meta.uid()));
        }
        if !meta.is_dir() {
            return None;
        }
        std::fs::read_dir(path)
            .ok()?
            .flatten()
            .find_map(|entry| first_foreign_unwritable(&entry.path()))
    }

    /// Write permission for the effective user, as the kernel decides it.
    pub(super) fn writable(path: &Path) -> bool {
        let Ok(path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
            return true;
        };
        // SAFETY: `path` is a valid NUL-terminated string for the call.
        unsafe { libc::faccessat(libc::AT_FDCWD, path.as_ptr(), libc::W_OK, libc::AT_EACCESS) == 0 }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt as _;

    /// Everything the current user owns passes, read-only or not, and so does
    /// a data folder with no databases yet.
    #[test]
    fn own_files_are_never_named() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        check_databases(dir.path()).unwrap();
        let file = dir.path().join("index/default/index.db");
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, b"").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o444)).unwrap();
        check_databases(dir.path()).unwrap();
        let err = explain(anyhow::anyhow!("open failed"), dir.path());
        assert_eq!(format!("{err:#}"), "open failed");
    }

    /// `/usr` belongs to root: any other user is told so, with both uids, and
    /// `explain` keeps the original error as the cause. Skipped where the
    /// current user owns or can write it (root, a sandbox).
    #[test]
    fn another_users_unwritable_directory_is_named() {
        // SAFETY: geteuid has no preconditions and cannot fail.
        let uid = unsafe { libc::geteuid() };
        let Ok(meta) = Path::new("/usr").symlink_metadata() else {
            return;
        };
        let owner = meta.uid();
        if !meta.is_dir() || owner == uid || unix::writable(Path::new("/usr")) {
            return;
        }
        let named = |root: &str| {
            format!(
                "'/usr' is owned by uid {owner} and is not writable by the current user \
                 (uid {uid}); run as uid {owner}, or change the owner of '{root}' to uid {uid}"
            )
        };
        assert_eq!(
            reason(Path::new("/"), &["no-such-dir", "usr"]),
            Some(named("/"))
        );
        let err = explain(anyhow::anyhow!("open failed"), Path::new("/usr"));
        assert_eq!(
            format!("{err:#}"),
            format!("{}: open failed", named("/usr"))
        );
    }
}
