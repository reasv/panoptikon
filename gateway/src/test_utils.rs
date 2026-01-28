use std::sync::{Mutex, MutexGuard, OnceLock};
use tempfile::TempDir;

pub(crate) struct TestDataGuard {
    _lock: MutexGuard<'static, ()>,
    root: &'static TempDir,
}

impl TestDataGuard {
    pub(crate) fn path(&self) -> &std::path::Path {
        self.root.path()
    }
}

pub(crate) fn test_data_dir() -> TestDataGuard {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    static ROOT: OnceLock<TempDir> = OnceLock::new();
    let lock = LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    let root = ROOT.get_or_init(|| TempDir::new().unwrap());
    unsafe {
        std::env::set_var("DATA_FOLDER", root.path());
    }
    TestDataGuard { _lock: lock, root }
}
