//! Says why stored state cannot be written: another user owns it (a root
//! left behind by a run as root, a container started under a different uid),
//! or its filesystem is read-only.

use std::path::{Path, PathBuf};

/// Fails when another user owns one of [`database_paths`] and the current
/// user cannot write it. Whether such a server starts depends on what SQLite
/// left behind; it cannot write either way.
pub(crate) fn check_databases(data_folder: &Path, index_db: &str) -> anyhow::Result<()> {
    let data_folder = absolute(data_folder);
    match reason(&data_folder, &database_paths(&data_folder, index_db), false) {
        Some(reason) => Err(anyhow::anyhow!(reason)),
        None => Ok(()),
    }
}

/// [`explain`] for a failed database open or migration, over [`database_paths`].
pub(crate) fn explain_databases(
    err: anyhow::Error,
    data_folder: &Path,
    index_db: &str,
) -> anyhow::Error {
    let data_folder = absolute(data_folder);
    explain(err, &data_folder, &database_paths(&data_folder, index_db))
}

/// Adds to `err` the first of `paths` the current user cannot write because
/// another user owns it or because its filesystem is read-only; `err`
/// unchanged without one. `tree` is the folder to hand over in the first case.
pub(crate) fn explain(err: anyhow::Error, tree: &Path, paths: &[PathBuf]) -> anyhow::Error {
    match reason(tree, paths, true) {
        Some(reason) => err.context(reason),
        None => err,
    }
}

/// Everything under `data_folder` the server writes in place: each database
/// with its `-wal` and `-shm`, and the folders it creates files in. Files it
/// replaces by rename (`config.toml`) and anything else kept there are not
/// listed. Reads two folder listings, never deeper.
fn database_paths(data_folder: &Path, index_db: &str) -> Vec<PathBuf> {
    let index = data_folder.join("index");
    let user_data = data_folder.join("user_data");
    let default = index.join(index_db);
    let mut paths = Vec::new();
    // A missing folder is created in its parent.
    if !index.is_dir() || !user_data.is_dir() {
        paths.push(data_folder.to_path_buf());
    }
    if !default.is_dir() {
        paths.push(index.clone());
    }
    let databases = |folder: &Path| [folder.join("index.db"), folder.join("storage.db")];
    let others = entries(&index)
        .into_iter()
        .filter(|folder| *folder != default && databases(folder).iter().any(|db| db.is_file()));
    for folder in std::iter::once(default.clone()).chain(others) {
        let databases = databases(&folder);
        paths.push(folder);
        databases
            .into_iter()
            .for_each(|db| push_database(&mut paths, db));
    }
    paths.push(user_data.clone());
    for file in entries(&user_data) {
        let extension = file.extension().and_then(|extension| extension.to_str());
        if extension.is_some_and(|extension| extension.eq_ignore_ascii_case("db")) {
            push_database(&mut paths, file);
        }
    }
    paths
}

fn entries(folder: &Path) -> Vec<PathBuf> {
    let entries = std::fs::read_dir(folder).into_iter().flatten().flatten();
    entries.map(|entry| entry.path()).collect()
}

/// A database and the two files SQLite keeps beside it.
fn push_database(paths: &mut Vec<PathBuf>, db: PathBuf) {
    for suffix in ["", "-wal", "-shm"] {
        let mut name = db.clone().into_os_string();
        name.push(suffix);
        paths.push(name.into());
    }
}

fn absolute(path: &Path) -> PathBuf {
    std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf())
}

/// The first of `paths` with a problem, as a message; a read-only filesystem
/// counts only with `read_only_too`. Always `None` on non-Unix.
fn reason(tree: &Path, paths: &[PathBuf], read_only_too: bool) -> Option<String> {
    #[cfg(unix)]
    {
        // SAFETY: geteuid has no preconditions and cannot fail.
        let uid = unsafe { libc::geteuid() };
        unix::reason(tree, paths, read_only_too, uid, unix::access)
    }
    #[cfg(not(unix))]
    {
        let _ = (tree, paths, read_only_too);
        None
    }
}

#[cfg(unix)]
mod unix {
    use std::os::unix::ffi::OsStrExt as _;
    use std::os::unix::fs::MetadataExt as _;
    use std::path::{Path, PathBuf};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Access {
        Writable,
        Denied { owner: u32 },
        ReadOnly,
    }

    pub(super) fn reason(
        tree: &Path,
        paths: &[PathBuf],
        read_only_too: bool,
        uid: u32,
        access: impl Fn(&Path) -> Access,
    ) -> Option<String> {
        paths.iter().find_map(|path| match access(path) {
            Access::Denied { owner } if owner != uid => Some(format!(
                "'{}' is owned by uid {owner} and is not writable by the current user \
                 (uid {uid}); run as uid {owner}, or change the owner of '{}' and \
                 everything in it to uid {uid}",
                path.display(),
                tree.display()
            )),
            Access::ReadOnly if read_only_too => {
                Some(format!("'{}' is on a read-only filesystem", path.display()))
            }
            _ => None,
        })
    }

    /// The kernel's answer for a write by the effective user, symlinks
    /// followed. A missing path, like any other answer, counts as writable.
    pub(super) fn access(path: &Path) -> Access {
        let Ok(c_path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
            return Access::Writable;
        };
        // SAFETY: `c_path` is a valid NUL-terminated string for the call.
        let granted = unsafe {
            libc::faccessat(
                libc::AT_FDCWD,
                c_path.as_ptr(),
                libc::W_OK,
                libc::AT_EACCESS,
            ) == 0
        };
        if granted {
            return Access::Writable;
        }
        match std::io::Error::last_os_error().raw_os_error() {
            Some(libc::EROFS) => Access::ReadOnly,
            Some(libc::EACCES) => match path.metadata() {
                Ok(meta) => Access::Denied { owner: meta.uid() },
                Err(_) => Access::Writable,
            },
            _ => Access::Writable,
        }
    }
}

#[cfg(all(test, unix))]
pub(crate) mod tests {
    use super::unix::Access;
    use super::*;
    use std::os::unix::fs::MetadataExt as _;

    /// A folder another user owns, with its owner, where creating a file
    /// really succeeds (`writable`) or really fails. `None` where the host
    /// has no such folder (tests run as root, a sandbox).
    pub(crate) fn foreign_folder(writable: bool) -> Option<(&'static Path, u32)> {
        let folder = Path::new(if writable { "/tmp" } else { "/usr" });
        let owner = folder.metadata().ok()?.uid();
        // SAFETY: geteuid has no preconditions and cannot fail.
        let foreign = owner != unsafe { libc::geteuid() };
        (foreign && tempfile::tempfile_in(folder).is_ok() == writable).then_some((folder, owner))
    }

    pub(crate) fn owned_by_another_user(path: &Path, owner: u32, tree: &Path) -> String {
        // SAFETY: geteuid has no preconditions and cannot fail.
        let uid = unsafe { libc::geteuid() };
        format!(
            "'{}' is owned by uid {owner} and is not writable by the current user (uid {uid}); \
             run as uid {owner}, or change the owner of '{}' and everything in it to uid {uid}",
            path.display(),
            tree.display()
        )
    }

    /// Two index databases, a user-data database, and what else can sit
    /// beside them.
    fn data_folder() -> tempfile::TempDir {
        let data = tempfile::tempdir().unwrap();
        for file in [
            "index/default/index.db",
            "index/default/index.db-wal",
            "index/default/storage.db",
            "index/default/config.toml",
            "index/default/index.db.bak",
            "index/second/index.db",
            "index/second/config.toml",
            "index/lost+found/file",
            "index/notes.txt",
            "user_data/default.db",
            "user_data/other.DB",
            "user_data/notes.txt",
        ] {
            let file = data.path().join(file);
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, b"").unwrap();
        }
        data
    }

    /// The startup check's message for uid 1000 when exactly `owned` belongs
    /// to root and is not writable.
    fn refusal(data: &Path, owned: &Path) -> Option<String> {
        let access = |path: &Path| {
            if path == owned {
                Access::Denied { owner: 0 }
            } else {
                Access::Writable
            }
        };
        unix::reason(data, &database_paths(data, "default"), false, 1000, access)
    }

    #[test]
    fn the_list_is_the_databases_their_wal_files_and_their_folders() {
        let data = data_folder();
        let mut listed: Vec<_> = database_paths(data.path(), "default")
            .into_iter()
            .map(|path| path.strip_prefix(data.path()).unwrap().to_owned())
            .collect();
        listed.sort();
        let expected = [
            "index/default",
            "index/default/index.db",
            "index/default/index.db-shm",
            "index/default/index.db-wal",
            "index/default/storage.db",
            "index/default/storage.db-shm",
            "index/default/storage.db-wal",
            "index/second",
            "index/second/index.db",
            "index/second/index.db-shm",
            "index/second/index.db-wal",
            "index/second/storage.db",
            "index/second/storage.db-shm",
            "index/second/storage.db-wal",
            "user_data",
            "user_data/default.db",
            "user_data/default.db-shm",
            "user_data/default.db-wal",
            "user_data/other.DB",
            "user_data/other.DB-shm",
            "user_data/other.DB-wal",
        ];
        assert_eq!(listed, expected.map(PathBuf::from));
    }

    /// Root's `config.toml` (replaced by rename), a backup copy, `lost+found`
    /// and `index/` itself do not stop a server that can write its databases.
    #[test]
    fn what_the_server_never_writes_in_place_does_not_refuse() {
        let data = data_folder();
        for owned in [
            "index/default/config.toml",
            "index/second/config.toml",
            "index/default/index.db.bak",
            "index/lost+found",
            "index",
        ] {
            let owned = data.path().join(owned);
            assert_eq!(refusal(data.path(), &owned), None, "{}", owned.display());
        }
        assert_eq!(refusal(data.path(), data.path()), None, "the data folder");
    }

    #[test]
    fn a_database_its_wal_files_or_its_folder_owned_by_root_refuses() {
        let data = data_folder();
        for owned in [
            "index/default",
            "index/default/index.db",
            "index/default/storage.db-shm",
            "index/second",
            "index/second/index.db",
            "user_data",
            "user_data/default.db",
            "user_data/other.DB-wal",
        ] {
            let owned = data.path().join(owned);
            let expected = format!(
                "'{}' is owned by uid 0 and is not writable by the current user (uid 1000); \
                 run as uid 0, or change the owner of '{}' and everything in it to uid 1000",
                owned.display(),
                data.path().display()
            );
            assert_eq!(refusal(data.path(), &owned), Some(expected));
        }
    }

    /// An empty data folder root owns (a bind mount Docker created), then
    /// `index/` without the default database's folder.
    #[test]
    fn a_folder_the_server_must_create_in_refuses() {
        let data = tempfile::tempdir().unwrap();
        let data = data.path();
        assert!(refusal(data, data).is_some());
        std::fs::create_dir(data.join("index")).unwrap();
        assert!(
            refusal(data, data).is_some(),
            "user_data is still to create"
        );
        std::fs::create_dir(data.join("user_data")).unwrap();
        assert_eq!(refusal(data, data), None);
        assert!(refusal(data, &data.join("index")).is_some());
    }

    #[test]
    fn a_symlinked_database_folder_is_listed() {
        let (data, elsewhere) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        std::fs::write(elsewhere.path().join("index.db"), b"").unwrap();
        let index = data.path().join("index");
        std::fs::create_dir(&index).unwrap();
        std::os::unix::fs::symlink(elsewhere.path(), index.join("linked")).unwrap();
        let listed = database_paths(data.path(), "default");
        assert!(listed.contains(&index.join("linked")), "{listed:?}");
        assert!(
            listed.contains(&index.join("linked/index.db")),
            "{listed:?}"
        );
    }

    /// The startup check lets a read-only filesystem through (a database on
    /// it may only ever be read); a failure is then explained by it, without
    /// a word about owners.
    #[test]
    fn a_read_only_filesystem_is_named_only_after_a_failure() {
        let data = data_folder();
        let folder = data.path().join("index/default");
        let paths = database_paths(data.path(), "default");
        let access = |path: &Path| {
            if path == folder {
                Access::ReadOnly
            } else {
                Access::Writable
            }
        };
        assert_eq!(unix::reason(data.path(), &paths, false, 0, access), None);
        assert_eq!(
            unix::reason(data.path(), &paths, true, 0, access),
            Some(format!(
                "'{}' is on a read-only filesystem",
                folder.display()
            ))
        );
    }

    #[test]
    fn the_kernel_decides_what_is_writable() {
        use std::os::unix::fs::PermissionsExt as _;
        let own = tempfile::tempdir().unwrap();
        // SAFETY: geteuid has no preconditions and cannot fail.
        let uid = unsafe { libc::geteuid() };
        assert_eq!(unix::access(own.path()), Access::Writable);
        assert_eq!(unix::access(&own.path().join("missing")), Access::Writable);
        if uid != 0 {
            let file = own.path().join("index.db");
            std::fs::write(&file, b"").unwrap();
            std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o444)).unwrap();
            assert_eq!(unix::access(&file), Access::Denied { owner: uid });
            assert_eq!(reason(own.path(), &[file], true), None, "own file");
        }
        if let Some((folder, _)) = foreign_folder(true) {
            assert_eq!(
                unix::access(folder),
                Access::Writable,
                "{}",
                folder.display()
            );
        }
        if let Some((folder, owner)) = foreign_folder(false) {
            assert_eq!(unix::access(folder), Access::Denied { owner });
            let link = own.path().join("link");
            std::os::unix::fs::symlink(folder, &link).unwrap();
            assert_eq!(
                unix::access(&link),
                Access::Denied { owner },
                "symlinks are followed"
            );
        }
    }

    /// Both entry points on a real folder of another user: the default
    /// database's folder is a symlink to it.
    #[test]
    fn another_users_database_folder_is_refused_and_explained() {
        let Some((folder, owner)) = foreign_folder(false) else {
            return;
        };
        let data = tempfile::tempdir().unwrap();
        let default = data.path().join("index/default");
        std::fs::create_dir_all(data.path().join("user_data")).unwrap();
        std::fs::create_dir(default.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(folder, &default).unwrap();
        let expected = owned_by_another_user(&default, owner, data.path());
        let refused = check_databases(data.path(), "default").unwrap_err();
        assert_eq!(refused.to_string(), expected);
        let explained = explain_databases(anyhow::anyhow!("open failed"), data.path(), "default");
        assert_eq!(format!("{explained:#}"), format!("{expected}: open failed"));
    }
}
