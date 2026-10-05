//! Says why stored state cannot be written: another user owns it (a root
//! left behind by a run as root, a container started under a different uid),
//! or its filesystem is read-only.

use crate::db::migrations::FailedDatabase;
use std::path::{Path, PathBuf};

/// Fails when another user owns one of [`database_paths`] and the current
/// user cannot write it. Whether such a server starts depends on what SQLite
/// left behind; it cannot write either way.
pub(crate) fn check_databases(data_folder: &Path, index_db: &str) -> anyhow::Result<()> {
    let data_folder = absolute(data_folder);
    let paths = database_paths(&data_folder, index_db);
    match refusal(&data_folder, &paths, |path| {
        reason(&data_folder, std::slice::from_ref(path), false)
    }) {
        Some(refusal) => Err(anyhow::anyhow!(refusal)),
        None => Ok(()),
    }
}

/// The first of `paths` with a `problem`, as a message. An index folder
/// holding a database, or one the current user cannot look into, or a
/// user-data database file, can instead be moved out of the data folder: the
/// folder only as root, unless it is a symlink in a writable `index/`; the
/// file with its `-wal` and `-shm`, unless it is a symlink.
fn refusal(
    data_folder: &Path,
    paths: &[PathBuf],
    problem: impl Fn(&PathBuf) -> Option<String>,
) -> Option<String> {
    let (path, reason) = paths
        .iter()
        .find_map(|path| problem(path).map(|reason| (path, reason)))?;
    let (index, user_data) = (data_folder.join("index"), data_folder.join("user_data"));
    let extension = path.extension().and_then(|extension| extension.to_str());
    let link = path.is_symlink();
    let what = if path.parent() == Some(index.as_path())
        && ["index.db", "storage.db"]
            .iter()
            .any(|db| path.join(db).try_exists().unwrap_or(true))
    {
        if link && problem(&index).is_none() {
            "move it"
        } else {
            "as root, move it"
        }
    } else if path.parent() == Some(user_data.as_path())
        && extension.is_some_and(|extension| extension.eq_ignore_ascii_case("db"))
    {
        if link {
            "move it"
        } else {
            "move it with its -wal and -shm"
        }
    } else {
        return Some(reason);
    };
    Some(format!(
        "{reason}; to keep that database as it is, {what} out of '{}'",
        data_folder.display()
    ))
}

/// [`explain`] for a failed migration, over the database it failed on.
pub(crate) fn explain_migration(err: anyhow::Error, data_folder: &Path) -> anyhow::Error {
    let paths = migration_paths(&err);
    explain(err, &absolute(data_folder), &paths)
}

/// The database a migration error names, with what [`push_database`] lists
/// and the folder it is kept in; empty when the error names none.
fn migration_paths(err: &anyhow::Error) -> Vec<PathBuf> {
    let Some(FailedDatabase(db)) = err.downcast_ref() else {
        return Vec::new();
    };
    let db = absolute(db);
    let mut paths: Vec<_> = db.parent().map(Path::to_path_buf).into_iter().collect();
    push_database(&mut paths, db);
    paths
}

/// Adds to `err` the first of `paths` the current user cannot write because
/// another user owns it or because its filesystem is read-only; `err`
/// unchanged without one. In the first case the folder whose owner to change
/// is `tree` (the data folder for a migration), or the target of a symlink
/// at or below it (`chown_target`); a path outside `tree`, or a folder to
/// change that is `/`, names none.
pub(crate) fn explain(err: anyhow::Error, tree: &Path, paths: &[PathBuf]) -> anyhow::Error {
    #[cfg(unix)]
    {
        // SAFETY: geteuid has no preconditions and cannot fail.
        let uid = unsafe { libc::geteuid() };
        unix::explain(err, tree, paths, uid, unix::access)
    }
    #[cfg(not(unix))]
    {
        let _ = (tree, paths);
        err
    }
}

/// Why creating databases failed, if another user owns or a read-only
/// filesystem holds what it writes: the database a migration failed on, or
/// else the folder a missing `index/<index_db>` or `user_data/` is made in.
pub(crate) fn create_databases_problem(
    err: &anyhow::Error,
    data_folder: &Path,
    index_db: &str,
) -> Option<String> {
    let data_folder = absolute(data_folder);
    let paths = create_databases_paths(err, &data_folder, index_db);
    reason(&data_folder, &paths, true)
}

fn create_databases_paths(err: &anyhow::Error, data_folder: &Path, index_db: &str) -> Vec<PathBuf> {
    let paths = migration_paths(err);
    if !paths.is_empty() {
        return paths;
    }
    let index = data_folder.join("index").join(index_db);
    [index, data_folder.join("user_data")]
        .iter()
        .filter(|folder| !folder.is_dir())
        .filter_map(|folder| folder.ancestors().find(|folder| folder.is_dir()))
        .map(Path::to_path_buf)
        .collect()
}

/// [`explain`] for a failure to create or replace `path`: what
/// [`create_problem`] says.
pub(crate) fn explain_create(err: anyhow::Error, tree: &Path, path: &Path) -> anyhow::Error {
    match create_problem(tree, path) {
        Some(reason) => err.context(reason),
        None => err,
    }
}

/// Why `path` cannot be created or replaced, if it cannot: what [`explain`]
/// would add over `path` and the folder it is created in, its nearest
/// existing ancestor.
pub(crate) fn create_problem(tree: &Path, path: &Path) -> Option<String> {
    let paths = create_paths(&absolute(path))?;
    reason(&absolute(tree), &paths, true)
}

fn create_paths(path: &Path) -> Option<Vec<PathBuf>> {
    let folder = path.ancestors().skip(1).find(|folder| folder.is_dir())?;
    Some(vec![path.to_path_buf(), folder.to_path_buf()])
}

/// Why a database kept in a `folder` of its own cannot be written, if it
/// cannot: what [`explain`] would add, over the folder, the database and what
/// [`push_database`] lists (the folder's parent while the folder does not
/// exist).
pub(crate) fn database_problem(folder: &Path, file_name: &str) -> Option<String> {
    let mut paths = Vec::new();
    let tree = if folder.is_dir() {
        paths.push(folder.to_path_buf());
        push_database(&mut paths, folder.join(file_name));
        folder
    } else {
        paths.extend(folder.parent().map(Path::to_path_buf));
        folder.parent()?
    };
    reason(tree, &paths, true)
}

/// Everything the server writes in place for the databases in `data_folder`:
/// each database with its `-wal` and `-shm`, and the folders it creates files
/// in. Files it replaces by rename (`config.toml`) and anything else kept
/// there are not listed. Reads two folder listings, never deeper.
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
        if extension.is_some_and(|extension| extension.eq_ignore_ascii_case("db")) && file.is_file()
        {
            push_database(&mut paths, file);
        }
    }
    paths
}

fn entries(folder: &Path) -> Vec<PathBuf> {
    let entries = std::fs::read_dir(folder).into_iter().flatten().flatten();
    entries.map(|entry| entry.path()).collect()
}

/// A database and the two files SQLite keeps beside it. SQLite opens a
/// symlink's target, so for a symlink they are beside the target, and the
/// target's folder, where SQLite creates them, is listed too.
fn push_database(paths: &mut Vec<PathBuf>, db: PathBuf) {
    paths.push(db.clone());
    let mut beside = db;
    if beside.is_symlink()
        && let Ok(target) = std::fs::canonicalize(&beside)
    {
        paths.extend(target.parent().map(Path::to_path_buf));
        beside = target;
    }
    for suffix in ["-wal", "-shm"] {
        let mut name = beside.clone().into_os_string();
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
            Access::Denied { owner } if owner != uid => {
                let fact = format!(
                    "'{}' is owned by uid {owner} and is not writable by the current user \
                     (uid {uid})",
                    path.display()
                );
                let target = chown_target(tree, path);
                // Outside `tree` there is no folder known to hand over, and
                // `/` is never one.
                if !path.starts_with(tree) || tree.parent().is_none() || target.parent().is_none() {
                    return Some(fact);
                }
                Some(format!(
                    "{fact}; run as uid {owner}, or change the owner of '{}' and \
                     everything in it to uid {uid}",
                    target.display()
                ))
            }
            Access::ReadOnly if read_only_too => {
                Some(format!("'{}' is on a read-only filesystem", path.display()))
            }
            _ => None,
        })
    }

    /// The folder whose owner, changed with everything in it, reaches `path`:
    /// `tree`, or the target of the deepest symlink from `tree` down to
    /// `path`, since a recursive change of owner follows no symlink.
    fn chown_target(tree: &Path, path: &Path) -> PathBuf {
        let mut ancestors = path.ancestors().take_while(|path| path.starts_with(tree));
        match ancestors.find(|path| path.is_symlink()) {
            Some(link) => std::fs::canonicalize(link).unwrap_or_else(|_| link.to_path_buf()),
            None => tree.to_path_buf(),
        }
    }

    pub(super) fn explain(
        err: anyhow::Error,
        tree: &Path,
        paths: &[PathBuf],
        uid: u32,
        access: impl Fn(&Path) -> Access,
    ) -> anyhow::Error {
        match reason(tree, paths, true, uid, access) {
            Some(reason) => err.context(reason),
            None => err,
        }
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
        let foreign = owner != unsafe { libc::geteuid() }
            && (writable || super::unix::access(folder) == Access::Denied { owner });
        (foreign && tempfile::tempfile_in(folder).is_ok() == writable).then_some((folder, owner))
    }

    pub(crate) fn owned_by_another_user(path: &Path, owner: u32, tree: &Path) -> String {
        // SAFETY: geteuid has no preconditions and cannot fail.
        owned_by(path, owner, unsafe { libc::geteuid() }, tree)
    }

    /// [`not_writable`] for the current user, with no folder to hand over.
    pub(crate) fn not_writable_by_current_user(path: &Path, owner: u32) -> String {
        // SAFETY: geteuid has no preconditions and cannot fail.
        not_writable(path, owner, unsafe { libc::geteuid() })
    }

    /// The message for `path`, owned by `owner` and not writable by `uid`.
    fn not_writable(path: &Path, owner: u32, uid: u32) -> String {
        format!(
            "'{}' is owned by uid {owner} and is not writable by the current user (uid {uid})",
            path.display()
        )
    }

    /// [`not_writable`], with `tree` as the folder whose owner to change.
    fn owned_by(path: &Path, owner: u32, uid: u32, tree: &Path) -> String {
        format!(
            "{}; run as uid {owner}, or change the owner of '{}' and everything in it to uid {uid}",
            not_writable(path, owner, uid),
            tree.display()
        )
    }

    /// `plain`, with the advice to `what` out of `data` when there is one.
    fn refusal_text(plain: String, what: Option<&str>, data: &Path) -> String {
        match what {
            Some(what) => format!(
                "{plain}; to keep that database as it is, {what} out of '{}'",
                data.display()
            ),
            None => plain,
        }
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
            "index/second/storage.db",
            "index/second/config.toml",
            "index/lost+found/file",
            "index/notes.txt",
            "user_data/default.db",
            "user_data/other.DB",
            "user_data/notes.txt",
            "user_data/archive.db/notes.txt",
        ] {
            let file = data.path().join(file);
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, b"").unwrap();
        }
        data
    }

    /// The startup check's message for uid 1000 when root owns exactly the
    /// `owned` paths and they are not writable.
    fn refusal_for(data: &Path, owned: &[&Path]) -> Option<String> {
        let access = |path: &Path| {
            if owned.contains(&path) {
                Access::Denied { owner: 0 }
            } else {
                Access::Writable
            }
        };
        let paths = database_paths(data, "default");
        refusal(data, &paths, |path| {
            unix::reason(data, std::slice::from_ref(path), false, 1000, access)
        })
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

    /// Root's `config.toml` (replaced by rename), a backup copy, `lost+found`,
    /// a folder named like a database and `index/` itself do not stop a
    /// server that can write its databases.
    #[test]
    fn what_the_server_never_writes_in_place_does_not_refuse() {
        let data = data_folder();
        for owned in [
            "index/default/config.toml",
            "index/second/config.toml",
            "index/default/index.db.bak",
            "index/lost+found",
            "index",
            "user_data/archive.db",
        ] {
            let owned = data.path().join(owned);
            assert_eq!(
                refusal_for(data.path(), &[&owned]),
                None,
                "{}",
                owned.display()
            );
        }
        assert_eq!(
            refusal_for(data.path(), &[data.path()]),
            None,
            "the data folder"
        );
    }

    /// A user-data database can instead be moved out of the data folder with
    /// its `-wal` and `-shm`, and a real index database folder as root (it
    /// cannot be moved without write access on it), also when its contents
    /// cannot be seen; a file inside it, a `-wal` or `-shm`, and a folder the
    /// server creates databases in cannot.
    #[test]
    fn a_database_its_wal_files_or_its_folder_owned_by_root_refuses() {
        let data = data_folder();
        let (as_root, with_wal) = (
            Some("as root, move it"),
            Some("move it with its -wal and -shm"),
        );
        for (owned, what) in [
            ("index/default", as_root),
            ("index/default/index.db", None),
            ("index/default/storage.db-shm", None),
            ("index/second", as_root),
            ("index/second/index.db", None),
            ("user_data", None),
            ("user_data/default.db", with_wal),
            ("user_data/other.DB", with_wal),
            ("user_data/other.DB-wal", None),
        ] {
            let owned = data.path().join(owned);
            let plain = owned_by(&owned, 0, 1000, data.path());
            let expected = refusal_text(plain, what, data.path());
            assert_eq!(refusal_for(data.path(), &[&owned]), Some(expected));
        }
        use std::os::unix::fs::PermissionsExt as _;
        let default = data.path().join("index/default");
        std::fs::set_permissions(&default, std::fs::Permissions::from_mode(0o000)).unwrap();
        let refused = refusal_for(data.path(), &[&default]);
        std::fs::set_permissions(&default, std::fs::Permissions::from_mode(0o755)).unwrap();
        let plain = owned_by(&default, 0, 1000, data.path());
        assert_eq!(refused, Some(refusal_text(plain, as_root, data.path())));
    }

    /// The startup check offers moving out a user-data database linked to a
    /// file another user owns.
    #[test]
    fn the_startup_check_offers_moving_out_a_linked_database() {
        let Some((folder, owner)) = foreign_folder(false) else {
            return;
        };
        let Ok(target) = folder.join("bin/env").canonicalize() else {
            return;
        };
        if super::unix::access(&target) != (Access::Denied { owner }) {
            return;
        }
        let data = data_folder();
        assert!(check_databases(data.path(), "default").is_ok());
        let link = data.path().join("user_data/linked.db");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let error = check_databases(data.path(), "default").unwrap_err();
        let plain = owned_by_another_user(&link, owner, &target);
        let expected = refusal_text(plain, Some("move it"), data.path());
        assert_eq!(error.to_string(), expected);
    }

    /// An empty data folder root owns (a bind mount Docker created), then
    /// `index/` without the default database's folder, then an empty default
    /// folder.
    #[test]
    fn a_folder_the_server_must_create_in_refuses() {
        let data = tempfile::tempdir().unwrap();
        let (data, index) = (data.path(), data.path().join("index"));
        let plain = owned_by(data, 0, 1000, data);
        assert_eq!(refusal_for(data, &[data]), Some(plain.clone()));
        std::fs::create_dir(&index).unwrap();
        assert_eq!(
            refusal_for(data, &[data]),
            Some(plain),
            "user_data is still to create"
        );
        std::fs::create_dir(data.join("user_data")).unwrap();
        assert_eq!(refusal_for(data, &[data]), None);
        let plain = owned_by(&index, 0, 1000, data);
        assert_eq!(refusal_for(data, &[&index]), Some(plain));
        let default = index.join("default");
        std::fs::create_dir(&default).unwrap();
        let plain = owned_by(&default, 0, 1000, data);
        assert_eq!(refusal_for(data, &[&default]), Some(plain));
    }

    /// A recursive change of owner of the data folder does not follow the
    /// link, so the deepest link's target is the folder whose owner to
    /// change; a link above the data folder is ignored. A linked database
    /// folder can be moved out while `index/` is writable, and a linked
    /// database's `-wal` and `-shm` are beside its target, outside the tree.
    #[test]
    fn a_symlinked_database_folder_is_listed_and_its_target_named() {
        // <root>/alias -> real; data/index -> x; x/linked -> y;
        // data/user_data/user.db -> ../../../z/user.db.
        let root = tempfile::tempdir().unwrap();
        let [real, x, y, z] = ["real", "x", "y", "z"].map(|name| root.path().join(name));
        std::fs::create_dir_all(real.join("data/user_data")).unwrap();
        std::fs::create_dir_all(x.join("default")).unwrap();
        std::fs::create_dir(&y).unwrap();
        std::fs::create_dir(&z).unwrap();
        std::fs::write(y.join("index.db"), b"").unwrap();
        std::fs::write(x.join("default/index.db"), b"").unwrap();
        std::fs::write(z.join("user.db"), b"").unwrap();
        std::os::unix::fs::symlink(&real, root.path().join("alias")).unwrap();
        let data = root.path().join("alias/data");
        std::os::unix::fs::symlink(&x, data.join("index")).unwrap();
        std::os::unix::fs::symlink(&y, x.join("linked")).unwrap();
        std::os::unix::fs::symlink("../../../z/user.db", data.join("user_data/user.db")).unwrap();
        let listed = database_paths(&data, "default");
        for path in ["index/linked", "index/linked/index.db"] {
            assert!(listed.contains(&data.join(path)), "{listed:?}");
        }
        let [x, y, z] = [x, y, z].map(|folder| folder.canonicalize().unwrap());
        let (link, as_root) = (Some("move it"), Some("as root, move it"));
        for (owned, target, what) in [
            ("index/linked", &y, link),
            ("index/linked/index.db-wal", &y, None),
            ("index/default", &x, as_root),
            ("user_data", &data, None),
            ("user_data/user.db", &z.join("user.db"), link),
        ] {
            let owned = data.join(owned);
            let expected = refusal_text(owned_by(&owned, 0, 1000, target), what, &data);
            assert_eq!(refusal_for(&data, &[&owned]), Some(expected));
        }
        for owned in [z.join("user.db-wal"), z.clone()] {
            let expected = not_writable(&owned, 0, 1000);
            assert_eq!(refusal_for(&data, &[&owned]), Some(expected));
        }
        let (index, linked) = (data.join("index"), data.join("index/linked"));
        let plain = owned_by(&linked, 0, 1000, &y);
        let expected = refusal_text(plain, as_root, &data);
        assert_eq!(refusal_for(&data, &[&index, &linked]), Some(expected));
    }

    /// A failed migration is explained by the database it failed on (its
    /// folder, the file, its `-wal` and `-shm`), never by another one. The
    /// startup check lets a read-only filesystem through; only a failure
    /// names it.
    #[test]
    fn a_failed_migration_is_explained_by_its_own_database_only() {
        let data = data_folder();
        let read_only = ["index/default", "user_data/default.db-shm"];
        let access = |path: &Path| {
            if read_only.iter().any(|read_only| path.ends_with(read_only)) {
                Access::ReadOnly
            } else {
                Access::Writable
            }
        };
        let paths = database_paths(data.path(), "default");
        assert_eq!(unix::reason(data.path(), &paths, false, 1000, access), None);
        for (db, named) in [
            ("index/default/storage.db", vec!["index/default"]),
            ("index/second/index.db", vec![]),
            ("user_data/default.db", vec!["user_data/default.db-shm"]),
        ] {
            let failed = FailedDatabase(data.path().join(db));
            let err = anyhow::anyhow!("disk full").context(failed);
            let paths = migration_paths(&err);
            let explained = unix::explain(err, data.path(), &paths, 1000, access);
            let explained = format!("{explained:#}");
            let quoted: Vec<_> = read_only
                .into_iter()
                .filter(|path| {
                    explained.contains(&format!("'{}'", data.path().join(path).display()))
                })
                .collect();
            assert_eq!(quoted, named, "{explained}");
        }
        assert!(migration_paths(&anyhow::anyhow!("disk full")).is_empty());
    }

    /// A failed database create is explained by the database a migration
    /// failed on, or else by the folder a missing database folder is made in.
    #[test]
    fn a_failed_create_is_explained_by_its_database_or_the_folder_it_is_made_in() {
        let data = data_folder();
        let index = data.path().join("index");
        let access = |path: &Path| {
            if path == index || path == index.join("second") {
                Access::Denied { owner: 0 }
            } else {
                Access::Writable
            }
        };
        let explained = |err: &anyhow::Error, index_db: &str| {
            let paths = create_databases_paths(err, data.path(), index_db);
            unix::reason(data.path(), &paths, true, 1000, access)
        };
        let denied = anyhow::anyhow!("permission denied");
        let named = Some(owned_by(&index, 0, 1000, data.path()));
        assert_eq!(explained(&denied, "new"), named);
        assert_eq!(explained(&denied, "second"), None, "nothing to create");
        let failed = FailedDatabase(index.join("second/index.db"));
        let second = Some(owned_by(&index.join("second"), 0, 1000, data.path()));
        assert_eq!(explained(&denied.context(failed), "second"), second);
    }

    /// A file that cannot be created or replaced is explained by itself or
    /// by the nearest folder that exists. Outside the tree, with `/` as the
    /// tree, or through a link to `/`, only the fact is given.
    #[test]
    fn a_file_is_explained_by_itself_or_the_folder_it_is_created_in() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path();
        std::fs::create_dir_all(root.join("conf/old")).unwrap();
        std::fs::write(root.join("conf/old/a.toml"), b"").unwrap();
        std::os::unix::fs::symlink("/", root.join("up")).unwrap();
        std::os::unix::fs::symlink(root.join("conf"), root.join("link")).unwrap();
        let new = root.join("conf/new");
        // `advice`: `None` for no reason, else whether a folder to hand over
        // is named.
        for (tree, path, owned, advice) in [
            (root, "conf/old/a.toml", "conf/old/a.toml", Some(true)),
            (root, "conf/old/b.toml", "conf/old", Some(true)),
            (root, "conf/old/b.toml", "conf", None),
            (root, "conf/old", "conf", Some(true)),
            (root, "conf/new/b.toml", "conf", Some(true)),
            (&new, "conf/new/b.toml", "conf", Some(false)),
            (Path::new("/"), "link/b.toml", "link", Some(false)),
            (root, "up/b.toml", "up/b.toml", Some(false)),
        ] {
            let owned = root.join(owned);
            let access = |path: &Path| {
                if path == owned {
                    Access::Denied { owner: 0 }
                } else {
                    Access::Writable
                }
            };
            let expected = advice.map(|advice| {
                if advice {
                    owned_by(&owned, 0, 1000, root)
                } else {
                    not_writable(&owned, 0, 1000)
                }
            });
            let paths = create_paths(&root.join(path)).unwrap();
            let reason = unix::reason(tree, &paths, true, 1000, access);
            assert_eq!(reason, expected, "{path}");
        }
    }

    /// The transcode cache's shape: a folder holding one database.
    #[test]
    fn a_database_in_its_own_folder_names_the_folder_or_its_parent() {
        let own = tempfile::tempdir().unwrap();
        std::fs::write(own.path().join("cache.db"), b"").unwrap();
        assert_eq!(database_problem(own.path(), "cache.db"), None);
        assert_eq!(database_problem(&own.path().join("new"), "cache.db"), None);
        let Some((folder, owner)) = foreign_folder(false) else {
            return;
        };
        let link = own.path().join("link");
        std::os::unix::fs::symlink(folder, &link).unwrap();
        assert_eq!(
            database_problem(&link, "cache.db"),
            Some(owned_by_another_user(&link, owner, folder))
        );
        assert_eq!(
            database_problem(&folder.join("no-such-folder"), "cache.db"),
            Some(owned_by_another_user(folder, owner, folder))
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
            let explained = explain(anyhow::anyhow!("open failed"), own.path(), &[file]);
            assert_eq!(format!("{explained:#}"), "open failed", "own file");
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
}
