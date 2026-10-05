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
/// Removed by an `atexit` hook (a static is never dropped); a killed process
/// leaves `panoptikon-tests-*` behind.
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

/// A unique path under the system temp dir, removed on drop (panics included).
/// The file is not created.
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

/// How long a test waits on a real ffmpeg run before calling it hung. A hang
/// detector, not a speed bound: a fixture encode that takes 0.4 s on a quiet
/// host has taken over a minute on a loaded one.
pub(crate) const FFMPEG_HANG_DEADLINE: std::time::Duration = std::time::Duration::from_secs(300);

/// A port nothing in this process can be listening on. Binding and dropping
/// an *ephemeral* port is not sound: it goes straight back to the pool this
/// binary's other tests bind from, so the "closed" port is occasionally a
/// neighbour's stub. Port 1 is below `ip_local_port_range`.
pub(crate) async fn closed_port() -> std::net::SocketAddr {
    let addr: std::net::SocketAddr = "127.0.0.1:1".parse().unwrap();
    assert!(
        tokio::net::TcpStream::connect(addr).await.is_err(),
        "premise: {addr} refuses connections; something here is listening"
    );
    addr
}

/// Replaces `path` with a new file (a new inode, so a descriptor some child
/// inherited on the old one does not matter) holding `contents`, executable.
/// A child process writes it, so this process never holds a write descriptor
/// on it: one open while another test thread forks is inherited by that child
/// until its exec, and executing the file meanwhile fails with ETXTBSY.
#[cfg(unix)]
pub(crate) fn write_executable(path: &std::path::Path, contents: &str) {
    let script = r#"rm -f "$2" && printf '%s' "$1" > "$2" && chmod 755 "$2""#;
    let status = std::process::Command::new("/bin/sh")
        .args(["-c", script, "sh", contents])
        .arg(path)
        .status()
        .unwrap();
    assert!(status.success(), "could not write {}", path.display());
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

/// Installs, before any test thread starts, a global subscriber that drops
/// every event but answers `sometimes` for every callsite. tracing caches a
/// callsite's interest process-wide at its first hit; without this, a thread
/// with no capture can cache `never` and so drop the event for a thread
/// capturing it. With it, every event asks the emitting thread's own
/// subscriber.
// SAFETY: runs before `main`; it allocates, takes tracing's own locks and
// sets its global dispatcher, none of which needs anything `main` sets up.
#[ctor::ctor]
unsafe fn install_ask_every_event() {
    use tracing_subscriber::layer::SubscriberExt;
    let subscriber = tracing_subscriber::registry().with(AskEveryEvent);
    tracing::subscriber::set_global_default(subscriber).expect("no global subscriber yet");
}

struct AskEveryEvent;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for AskEveryEvent {
    fn register_callsite(
        &self,
        _metadata: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::sometimes()
    }

    fn enabled(
        &self,
        _metadata: &tracing::Metadata<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        false
    }
}

#[test]
fn the_global_subscriber_answers_sometimes() {
    let callsite = tracing::info_span!("probe").metadata().unwrap();
    assert!(tracing::dispatcher::get_default(|d| d
        .register_callsite(callsite)
        .is_sometimes()));
}

/// A script replacing a file this process still holds open for writing runs.
#[cfg(unix)]
#[test]
fn a_written_executable_replaces_a_file_open_for_writing() {
    use std::process::Command;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("script");
    let _old = std::fs::File::create(&path).unwrap();
    write_executable(&path, "#!/bin/sh\n");
    assert!(Command::new(&path).status().unwrap().success());
}
