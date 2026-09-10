use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};
use tempfile::{TempDir, TempPath};

pub(crate) struct TestDataGuard {
    _lock: MutexGuard<'static, ()>,
    root: &'static std::path::Path,
}

impl TestDataGuard {
    pub(crate) fn path(&self) -> &std::path::Path {
        self.root
    }
}

/// The per-process temp root every test's data folder points at. Shared by
/// [`test_data_dir`] and the `cfg(test)` default of `config::runtime()`, so
/// tests never touch a real `./data` regardless of which path initializes
/// the process-global runtime config first.
///
/// A static is never dropped, so the directory is removed by an `atexit`
/// hook instead: libtest returns from `main` or calls `process::exit`, and
/// both run the hooks. A killed process leaves `panoptikon-tests-*` behind.
pub(crate) fn test_data_root() -> &'static std::path::Path {
    static ROOT: OnceLock<TempDir> = OnceLock::new();
    extern "C" fn remove_root() {
        if let Some(root) = ROOT.get() {
            let _ = std::fs::remove_dir_all(root.path());
        }
    }
    ROOT.get_or_init(|| {
        let root = tempfile::Builder::new()
            .prefix("panoptikon-tests-")
            .tempdir()
            .unwrap();
        // SAFETY: `remove_root` is a plain `extern "C"` function with no
        // arguments, which is what `atexit` requires.
        unsafe { libc::atexit(remove_root) };
        root
    })
    .path()
}

/// A unique file path under the system temp dir, removed when the value
/// drops (a panic included). The file is not created; the path may stay
/// absent when a test needs a missing file.
pub(crate) fn temp_path(label: &str) -> TempPath {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
    let name = format!("panoptikon_{label}_{}_{unique}", std::process::id());
    TempPath::from_path(std::env::temp_dir().join(name))
}

/// Serializes tests that read or mutate process-global environment variables
/// consumed by `Settings::load` (templated variables like LOGLEVEL).
/// Every test that calls `Settings::load` *or*
/// sets such variables must hold this lock, otherwise parallel tests can
/// observe each other's overrides.
pub(crate) fn env_lock() -> MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|err| err.into_inner())
}

/// Serialized access to the shared test data folder (replaces the old
/// DATA_FOLDER env var: the process-global runtime config points at the
/// shared temp root instead).
pub(crate) fn test_data_dir() -> TestDataGuard {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let lock = LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    let root = test_data_root();
    // Install (or confirm) the runtime config pointing at the test root.
    // runtime()'s cfg(test) default installs the same root if it runs
    // first, so this is idempotent either way.
    let installed = crate::config::install_runtime_for_tests(crate::config::RuntimeConfig {
        data_folder: root.to_path_buf(),
        ..crate::config::RuntimeConfig::default()
    });
    assert_eq!(
        installed.data_folder, root,
        "test runtime config must use the shared test data root"
    );
    TestDataGuard { _lock: lock, root }
}

/// A directory that does not exist, spelled as a native absolute path:
/// `C:\gone` on Windows, `/gone` elsewhere. Fixtures that need a file row the
/// handler must never stat put it below this root, so what `Path` makes of
/// the string — the file name, the stem — is the platform's own answer. A
/// Windows spelling on Unix is one long file name, backslashes included.
pub(crate) fn absent_root() -> &'static str {
    if cfg!(windows) { r"C:\gone" } else { "/gone" }
}

/// `name` placed directly under [`absent_root`], joined with the native
/// separator.
pub(crate) fn absent_path(name: &str) -> String {
    format!("{}{}{name}", absent_root(), std::path::MAIN_SEPARATOR)
}

/// Writes a per-database `config.toml` carrying only `detect_outros`, at the
/// path `SystemConfigStore::from_env()` resolves for `index_db`.
///
/// The API-side outro gate reads its config through `from_env`, so a test
/// that wants the toggle off has to place the file where that store will
/// look. Every other key stays absent and therefore at its serde default.
/// Call while holding [`test_data_dir`] and use a database name no other
/// test uses.
pub(crate) fn write_detect_outros_config(index_db: &str, detect_outros: bool) {
    let path = crate::db::system_config::SystemConfigStore::from_env().config_path(index_db);
    std::fs::create_dir_all(path.parent().expect("config path has a parent")).unwrap();
    std::fs::write(&path, format!("detect_outros = {detect_outros}\n")).unwrap();
}
