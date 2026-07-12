use std::sync::{Mutex, MutexGuard, OnceLock};
use tempfile::TempDir;

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
pub(crate) fn test_data_root() -> &'static std::path::Path {
    static ROOT: OnceLock<TempDir> = OnceLock::new();
    ROOT.get_or_init(|| TempDir::new().unwrap()).path()
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
