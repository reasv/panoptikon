//! `panoptikon setup`: create or update the managed Python inference
//! environment (docs/architecture.md "Python environment management").
//!
//! The flow is: take the exclusive setup lock (`runtime/setup.lock` — the
//! gateway and the `inferio` subcommand may auto-trigger concurrently),
//! locate `uv` (PATH, then a previously downloaded managed copy, then
//! download the pinned standalone build — checksum-verified against
//! [`UV_ASSET_SHA256`]), pick the accelerator variant (config/CLI, `auto`
//! probes for NVIDIA/ROCm), then create the managed venv if missing and run
//! a locked `uv sync --extra <variant>` against the active Python project
//! dir's `pyproject.toml` + `uv.lock`. The project dir and venv follow
//! `resources::py_source_mode`: `python/` + `python/.venv` in the dev
//! layout, `runtime/pysrc/<version>/` + `runtime/venv` when a `bundled`
//! build runs from its extracted source set (see [`ManagedPython`]). The
//! lock is authoritative (`--locked`): syncing can never resolve versions
//! the repository does not pin, and re-running converges (idempotent). A
//! successful sync writes a completion sentinel ([`SETUP_SENTINEL`])
//! recording the extra and the uv.lock hash; the startup auto-trigger keys
//! on it, so an interrupted first sync (which leaves an interpreter but no
//! sentinel) or a changed lock (a git pull in dev, a re-extracted source
//! set after a version bump in bundled mode) re-arms it.
//!
//! Safety: this module refuses to operate on any venv other than the
//! active mode's managed venv — a user-configured
//! `[inference_local].python` interpreter is never created, deleted, or
//! synced (see [`guard_managed_venv`], enforced on every mutating step, and
//! `UV_PROJECT_ENVIRONMENT`, pinned on every child so ambient uv
//! configuration cannot redirect the sync).

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, bail};
use futures_util::StreamExt as _;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt as _, BufReader};
use tokio::process::Command;

use crate::config::{Accelerator, Settings};
use crate::process_tree::{JobGuard, detach_from_console, die_with_parent};

/// Pinned standalone uv release (astral-sh/uv GitHub release tag) downloaded
/// when no usable `uv` is on PATH. Downloads land in
/// `runtime/uv/<version>/`, keyed by version so a pin bump re-downloads.
pub const UV_VERSION: &str = "0.11.28";

/// Official SHA-256 checksums for the [`UV_VERSION`] release assets,
/// verified after download and before extraction. To refresh when bumping
/// `UV_VERSION`: every asset on the GitHub release has a companion
/// `<asset>.sha256` file — fetch
/// `https://github.com/astral-sh/uv/releases/download/<version>/<asset>.sha256`
/// for each asset listed in [`uv_asset_name`] and copy the hex digest here.
const UV_ASSET_SHA256: &[(&str, &str)] = &[
    (
        "uv-x86_64-pc-windows-msvc.zip",
        "0a23463216d09c6a72ff80ef5dc5a795f07dc1575cb84d24596c2f124a441b7b",
    ),
    (
        "uv-x86_64-unknown-linux-gnu.tar.gz",
        "e490a6464492183c5d4534a5527fb4440f7f2bb2f228162ad7e4afe076dc0224",
    ),
    (
        "uv-aarch64-unknown-linux-gnu.tar.gz",
        "03e9fe0a81b0718d0bc84625de3885df6cc3f89a8b6af6121d6b9f6113fb6533",
    ),
    (
        "uv-aarch64-apple-darwin.tar.gz",
        "33540eb7c883ab857eff79bd5ac2aa31fe27b595abecb4a9c003a2c998447232",
    ),
    (
        "uv-x86_64-apple-darwin.tar.gz",
        "2ad79983127ffca7d77b77ce6a24278d7e4f7b817a1acf72fea5f8124b4aac5e",
    ),
];

/// Oldest PATH uv accepted — the empirically verified floor: 0.6.13 was
/// tested to read, re-resolve (`uv lock --check`), and sync the current
/// revision-3 lockfile with its `[tool.uv] conflicts` + per-extra sources.
/// Anything older falls through to the pinned download instead of failing
/// mid-sync.
const UV_MIN_VERSION: (u64, u64, u64) = (0, 6, 13);

/// Where managed uv downloads live, relative to the CWD.
const UV_RUNTIME_DIR: &str = "runtime/uv";

/// Python version for `uv venv` (uv auto-fetches a managed CPython when the
/// host has none). Must stay in sync with `requires-python` in
/// python/pyproject.toml.
const PYTHON_VERSION: &str = "3.12";

/// Download progress is logged every this many bytes.
const DOWNLOAD_LOG_STEP: u64 = 8 * 1024 * 1024;

/// stderr lines kept for error reporting when a uv child fails.
const STDERR_TAIL_LINES: usize = 20;

/// Completion sentinel written inside the managed venv after every
/// successful sync. `uv venv` creates the interpreter *before* the multi-GB
/// sync, so interpreter existence alone cannot distinguish a complete
/// environment from an interrupted first setup — the sentinel (recording
/// the synced extra and the uv.lock hash) does. Deleted along with the venv
/// by `--force`.
const SETUP_SENTINEL: &str = ".panoptikon-setup-complete";

/// The legacy pre-restructure venv at the repo root. Setup NEVER touches
/// it; its existence only suppresses the fresh-install auto-trigger (dev
/// layout only — extracted bundled mode has no legacy installs).
const LEGACY_VENV: &str = ".venv";

/// The paths setup manages, resolved from the active Python source mode
/// (docs/architecture.md "Self-contained releases"): the project dir `uv`
/// runs in (pyproject.toml + uv.lock), the ONLY venv this module may touch,
/// and whether a legacy root `.venv` suppresses the auto-trigger. All
/// relative to the CWD like every other path in the config.
struct ManagedPython {
    /// `python` (dev) or `runtime/pysrc/<version>` (extracted).
    project_dir: PathBuf,
    /// `python/.venv` (dev) or `runtime/venv` (extracted — outside the
    /// version-keyed dir so version bumps keep the venv; the sentinel's
    /// lock hash triggers the re-sync instead).
    venv: PathBuf,
    /// `<project_dir>/uv.lock`: the lockfile the sentinel hash records.
    uv_lock: PathBuf,
    /// Dev layout only: pre-restructure root `.venv` installs keep working
    /// untouched. Meaningless in extracted mode.
    legacy_suppresses: bool,
}

impl ManagedPython {
    fn for_mode(mode: crate::resources::PySourceMode) -> Self {
        let project_dir = crate::resources::python_project_dir(mode);
        Self {
            venv: crate::resources::managed_venv_dir(mode),
            uv_lock: project_dir.join("uv.lock"),
            legacy_suppresses: mode == crate::resources::PySourceMode::Dev,
            project_dir,
        }
    }

    /// The paths for the active mode.
    fn active() -> Self {
        Self::for_mode(crate::resources::py_source_mode())
    }
}

/// Advisory lock file serializing concurrent setup runs (gateway and the
/// `inferio` subcommand starting together), relative to the CWD.
const SETUP_LOCK_PATH: &str = "runtime/setup.lock";

/// Options for one setup run (`panoptikon setup [--accelerator ...]
/// [--force]`, or the startup auto-trigger with defaults).
pub struct SetupOptions {
    /// CLI override; `None` falls back to
    /// `[inference_local.python_env] accelerator`.
    pub accelerator: Option<Accelerator>,
    /// Delete the managed venv and recreate it from scratch.
    pub force: bool,
    /// Auto-trigger mode: after acquiring the setup lock, skip the run if
    /// the environment converged in the meantime (a concurrent process —
    /// gateway vs `inferio` — finished the same setup while we waited).
    /// The CLI passes false: an explicit `panoptikon setup` always runs.
    pub skip_if_converged: bool,
}

/// Run the whole setup flow. Blocking by design: callers (the CLI
/// subcommand and the startup auto-trigger) want the environment ready —
/// or a definitive error — before continuing.
pub async fn run(settings: &Settings, options: SetupOptions) -> Result<()> {
    if cfg!(target_os = "macos") && std::env::consts::ARCH == "x86_64" {
        bail!(
            "unsupported platform: macOS x86_64 (PyTorch no longer publishes \
             wheels for it, so the lock excludes it; only Apple Silicon \
             macOS is supported)"
        );
    }
    let managed = ManagedPython::active();
    let python_dir = std::path::absolute(&managed.project_dir)
        .with_context(|| format!("failed to resolve '{}'", managed.project_dir.display()))?;
    if !python_dir.join("pyproject.toml").is_file() {
        bail!(
            "'{}' has no pyproject.toml — run setup from the panoptikon root \
             (the managed environment lives at '{}'; a bundled binary \
             extracts its Python source set on startup first)",
            python_dir.display(),
            managed.venv.display()
        );
    }
    let venv = std::path::absolute(&managed.venv)
        .with_context(|| format!("failed to resolve '{}'", managed.venv.display()))?;
    guard_managed_venv(&venv)?;

    // Serialize concurrent setups (gateway + `inferio` starting together):
    // held for the whole run, released when dropped at return.
    let _setup_lock = SetupLock::acquire().await?;
    if options.skip_if_converged && auto_setup_needed().is_none() {
        tracing::info!(
            "the environment converged while waiting for the setup lock \
             (another panoptikon process finished setup); nothing to do"
        );
        return Ok(());
    }

    let uv = locate_uv().await?;
    tracing::info!(uv = %uv.path.display(), version = %uv.version, "using uv");

    let requested = options
        .accelerator
        .unwrap_or(settings.inference_local.python_env.accelerator);
    let (accelerator, evidence) = resolve_accelerator(requested)?;
    let extra = accelerator_extra(accelerator);
    tracing::info!(
        requested = ?requested,
        accelerator = ?accelerator,
        extra,
        evidence,
        "accelerator selected"
    );

    if options.force && venv.exists() {
        guard_managed_venv(&venv)?;
        tracing::info!(venv = %venv.display(), "--force: deleting the managed venv");
        tokio::fs::remove_dir_all(&venv)
            .await
            .with_context(|| format!("failed to delete '{}'", venv.display()))?;
    }

    if !venv.join(python_relpath()).is_file() {
        tracing::info!(venv = %venv.display(), python = PYTHON_VERSION, "creating the managed venv (uv venv)");
        run_uv_logged(&uv.path, &uv_venv_args(&venv), &python_dir, &venv, "uv venv").await?;
    }

    tracing::info!(
        extra,
        "syncing the locked environment (uv sync); the first run downloads \
         several GB of packages and can take a while"
    );
    let sync_args = uv_sync_args(extra);
    run_uv_logged(&uv.path, &sync_args, &python_dir, &venv, "uv sync").await?;

    let interpreter = venv.join(python_relpath());
    if !interpreter.is_file() {
        bail!(
            "uv sync succeeded but the interpreter '{}' does not exist",
            interpreter.display()
        );
    }
    write_sentinel(&venv, extra, &managed.uv_lock)?;
    tracing::info!(
        interpreter = %interpreter.display(),
        extra,
        "Python inference environment is ready"
    );
    Ok(())
}

/// Write the completion sentinel after a successful sync: the synced extra
/// plus the hash of the uv.lock it was synced from. Its absence marks an
/// interrupted setup; a hash mismatch marks a changed lock — both re-arm
/// the startup auto-trigger.
fn write_sentinel(venv: &Path, extra: &str, uv_lock: &Path) -> Result<()> {
    guard_managed_venv(venv)?;
    let hash = current_lock_hash(uv_lock).with_context(|| {
        format!(
            "failed to hash '{}' for the setup sentinel",
            uv_lock.display()
        )
    })?;
    let path = venv.join(SETUP_SENTINEL);
    std::fs::write(&path, format!("extra={extra}\nuv_lock_sha256={hash}\n"))
        .with_context(|| format!("failed to write '{}'", path.display()))?;
    Ok(())
}

/// SHA-256 (hex) of the active uv.lock, as recorded in the sentinel.
fn current_lock_hash(uv_lock: &Path) -> Result<String> {
    let bytes = std::fs::read(uv_lock)
        .with_context(|| format!("failed to read '{}'", uv_lock.display()))?;
    Ok(sha256_hex_of(&bytes))
}

fn sha256_hex_of(bytes: &[u8]) -> String {
    use sha2::Digest as _;
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    hex_string(&hasher.finalize())
}

fn hex_string(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Sentinel state relative to the current lockfile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SentinelStatus {
    /// Present and recorded for the current uv.lock.
    Valid,
    /// Absent (or unreadable/malformed): setup never completed.
    Missing,
    /// Present but recorded for a different uv.lock (e.g. after a pull).
    Stale,
}

/// Classify sentinel content against the current lock hash (pure function
/// for the decision-table tests).
fn sentinel_status_from(content: Option<&str>, current_lock_hash: Option<&str>) -> SentinelStatus {
    let Some(content) = content else {
        return SentinelStatus::Missing;
    };
    let recorded = content
        .lines()
        .find_map(|line| line.trim().strip_prefix("uv_lock_sha256="));
    match (recorded, current_lock_hash) {
        (None, _) => SentinelStatus::Missing,
        (Some(recorded), Some(current)) if recorded == current => SentinelStatus::Valid,
        // Unreadable current lock also counts as stale: something is off
        // enough that a re-run (which will surface the real error) is right.
        (Some(_), _) => SentinelStatus::Stale,
    }
}

/// Whether the startup auto-trigger should fire, and why (`None` = the
/// environment needs nothing). Callers gate on config themselves
/// (`auto_setup`, explicit `python`); this only inspects the filesystem.
pub(crate) fn auto_setup_needed() -> Option<String> {
    let managed = ManagedPython::active();
    let managed_interpreter = managed.venv.join(python_relpath()).is_file();
    let legacy_interpreter = managed.legacy_suppresses
        && Path::new(LEGACY_VENV).join(python_relpath()).is_file();
    let sentinel = sentinel_status_from(
        std::fs::read_to_string(managed.venv.join(SETUP_SENTINEL))
            .ok()
            .as_deref(),
        current_lock_hash(&managed.uv_lock).ok().as_deref(),
    );
    auto_setup_decision(managed_interpreter, legacy_interpreter, sentinel)
}

/// The auto-trigger decision table. A managed interpreter is judged by its
/// sentinel (missing → interrupted setup, stale → lock changed); without
/// one, a legacy root `.venv` (pre-restructure install, dev layout only)
/// suppresses the trigger — that environment keeps working and is never
/// auto-managed.
fn auto_setup_decision(
    managed_interpreter: bool,
    legacy_interpreter: bool,
    sentinel: SentinelStatus,
) -> Option<String> {
    if managed_interpreter {
        return match sentinel {
            SentinelStatus::Valid => None,
            SentinelStatus::Missing => Some(
                "the managed venv exists but no setup ever completed for it \
                 (interrupted sync?)"
                    .into(),
            ),
            SentinelStatus::Stale => {
                Some("uv.lock changed since setup last completed".into())
            }
        };
    }
    if legacy_interpreter {
        return None;
    }
    Some("no Python inference environment exists yet".into())
}

/// The interpreter path inside a venv, relative to the venv root.
fn python_relpath() -> &'static Path {
    crate::resources::venv_python_relpath()
}

/// Refuse to operate on anything but the active mode's managed venv
/// (`python/.venv` in dev, `runtime/venv` in extracted bundled mode, both
/// resolved against the CWD). Defense in depth for the "never touch a
/// user-managed environment" rule: every create/delete/sync call re-checks.
fn guard_managed_venv(venv: &Path) -> Result<()> {
    let active = ManagedPython::active().venv;
    let managed = std::path::absolute(&active)
        .with_context(|| format!("failed to resolve '{}'", active.display()))?;
    if venv != managed {
        bail!(
            "refusing to operate on venv '{}': setup only manages '{}'",
            venv.display(),
            managed.display()
        );
    }
    Ok(())
}

/// `uv venv` argument construction (separated for tests). The venv path is
/// passed positionally: `UV_PROJECT_ENVIRONMENT` (pinned by the runner) is
/// honored by `uv sync` but not reliably by `uv venv`, and in extracted
/// bundled mode the managed venv (`runtime/venv`) is NOT the project-dir
/// default. Both spellings come from the same guarded absolute path, so
/// they cannot disagree.
fn uv_venv_args(venv: &Path) -> Vec<String> {
    vec![
        "venv".into(),
        venv.display().to_string(),
        "--python".into(),
        PYTHON_VERSION.into(),
    ]
}

/// `uv sync` argument construction (separated for tests): a locked sync of
/// the default groups (the inference set) plus exactly one accelerator
/// extra. `--locked` makes the committed lock authoritative — setup never
/// re-resolves, so it can never drift from the repository state.
fn uv_sync_args(extra: &str) -> Vec<String> {
    vec![
        "sync".into(),
        "--locked".into(),
        "--extra".into(),
        extra.into(),
    ]
}

/// The pyproject extra for each resolved accelerator.
/// [`Accelerator::Auto`] must be resolved first (see
/// [`resolve_accelerator`]).
fn accelerator_extra(accelerator: Accelerator) -> &'static str {
    match accelerator {
        // On macOS the source markers route every extra to default PyPI
        // wheels, so `cpu` doubles as the macOS/MPS selection.
        Accelerator::Cpu => "cpu",
        Accelerator::Cuda => "cu128",
        Accelerator::Rocm => "rocm",
        Accelerator::Auto => unreachable!("auto is resolved before extra mapping"),
    }
}

/// Resolve an accelerator request into a concrete choice plus the evidence
/// for logging. Explicit choices are validated (ROCm is Linux-only);
/// `auto` runs the platform probes.
fn resolve_accelerator(requested: Accelerator) -> Result<(Accelerator, String)> {
    match requested {
        Accelerator::Auto => Ok(decide_accelerator(&DetectionProbes::gather())),
        Accelerator::Rocm if !cfg!(target_os = "linux") => {
            bail!("accelerator 'rocm' is only supported on Linux (PyTorch publishes no ROCm wheels elsewhere)")
        }
        Accelerator::Cuda if cfg!(target_os = "macos") => {
            tracing::warn!(
                "accelerator 'cuda' requested on macOS, where no CUDA wheels \
                 exist; the default PyPI wheels (MPS) will be installed"
            );
            Ok((Accelerator::Cuda, "explicitly configured".into()))
        }
        explicit => Ok((explicit, "explicitly configured".into())),
    }
}

/// Everything the auto-detection decision looks at, gathered up front so
/// the decision itself is a pure function (tested against the full table).
struct DetectionProbes {
    os: &'static str,
    /// `nvidia-smi` on PATH (any platform).
    nvidia_smi_on_path: bool,
    /// Windows: `%SystemRoot%\System32\nvidia-smi.exe` (driver installs put
    /// it there without touching PATH).
    system32_nvidia_smi: bool,
    /// Linux: `/proc/driver/nvidia` (kernel driver loaded).
    proc_driver_nvidia: bool,
    /// Linux: `/opt/rocm` exists.
    rocm_dir: bool,
    /// `rocm-smi` on PATH.
    rocm_smi_on_path: bool,
}

impl DetectionProbes {
    fn gather() -> Self {
        let system32_nvidia_smi = cfg!(windows)
            && std::env::var_os("SystemRoot")
                .map(|root| Path::new(&root).join("System32/nvidia-smi.exe").is_file())
                .unwrap_or(false);
        Self {
            os: std::env::consts::OS,
            nvidia_smi_on_path: on_path("nvidia-smi").is_some(),
            system32_nvidia_smi,
            proc_driver_nvidia: cfg!(target_os = "linux")
                && Path::new("/proc/driver/nvidia").exists(),
            rocm_dir: cfg!(target_os = "linux") && Path::new("/opt/rocm").is_dir(),
            rocm_smi_on_path: on_path("rocm-smi").is_some(),
        }
    }
}

/// The auto-detection decision table: macOS always takes the default PyPI
/// wheels (spelled `cpu`; they include MPS on Apple Silicon), NVIDIA
/// evidence wins over ROCm evidence, ROCm only exists on Linux, and no
/// evidence means CPU.
fn decide_accelerator(probes: &DetectionProbes) -> (Accelerator, String) {
    if probes.os == "macos" {
        return (
            Accelerator::Cpu,
            "macOS: default PyPI wheels (MPS on Apple Silicon); no CUDA/ROCm builds exist".into(),
        );
    }
    let nvidia = [
        (probes.nvidia_smi_on_path, "nvidia-smi on PATH"),
        (
            probes.system32_nvidia_smi,
            r"System32\nvidia-smi.exe exists",
        ),
        (probes.proc_driver_nvidia, "/proc/driver/nvidia exists"),
    ];
    if let Some((_, evidence)) = nvidia.iter().find(|(hit, _)| *hit) {
        return (Accelerator::Cuda, (*evidence).into());
    }
    if probes.os == "linux" {
        let rocm = [
            (probes.rocm_dir, "/opt/rocm exists"),
            (probes.rocm_smi_on_path, "rocm-smi on PATH"),
        ];
        if let Some((_, evidence)) = rocm.iter().find(|(hit, _)| *hit) {
            return (Accelerator::Rocm, (*evidence).into());
        }
    }
    (
        Accelerator::Cpu,
        "no NVIDIA or ROCm evidence found".into(),
    )
}

/// Exclusive advisory file lock (`runtime/setup.lock`) serializing
/// concurrent setup runs — the gateway and the `inferio` subcommand can
/// start together and would otherwise race `uv venv`/`uv sync` against the
/// same environment. Closing the file (drop) releases the lock on every
/// platform.
struct SetupLock {
    _file: std::fs::File,
}

impl SetupLock {
    /// Acquire by polling `try_lock` (a blocked OS-level wait would pin a
    /// runtime thread for however long the winner's multi-GB sync takes).
    /// Logs once when it starts waiting.
    async fn acquire() -> Result<Self> {
        let path = std::path::absolute(SETUP_LOCK_PATH)
            .with_context(|| format!("failed to resolve '{SETUP_LOCK_PATH}'"))?;
        let dir = path.parent().expect("setup lock path has a parent");
        tokio::fs::create_dir_all(dir)
            .await
            .with_context(|| format!("failed to create '{}'", dir.display()))?;
        let file = std::fs::File::options()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .with_context(|| format!("failed to open '{}'", path.display()))?;
        let mut waiting = false;
        loop {
            match try_lock_exclusive(&file)
                .with_context(|| format!("failed to lock '{}'", path.display()))?
            {
                true => {
                    if waiting {
                        tracing::info!("setup lock acquired; continuing");
                    }
                    return Ok(Self { _file: file });
                }
                false => {
                    if !waiting {
                        waiting = true;
                        tracing::info!(
                            lock = %path.display(),
                            "another panoptikon process is running setup; waiting..."
                        );
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
        }
    }
}

/// Non-blocking exclusive lock attempt: `Ok(true)` = acquired, `Ok(false)`
/// = held by another process.
#[cfg(windows)]
fn try_lock_exclusive(file: &std::fs::File) -> std::io::Result<bool> {
    use std::os::windows::io::AsRawHandle as _;
    use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;
    use windows_sys::Win32::Storage::FileSystem::{
        LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY, LockFileEx,
    };
    let mut overlapped: windows_sys::Win32::System::IO::OVERLAPPED =
        unsafe { std::mem::zeroed() };
    let ok = unsafe {
        LockFileEx(
            file.as_raw_handle(),
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,
            1,
            0,
            &mut overlapped,
        )
    };
    if ok != 0 {
        return Ok(true);
    }
    let err = std::io::Error::last_os_error();
    if err.raw_os_error() == Some(ERROR_LOCK_VIOLATION as i32) {
        return Ok(false);
    }
    Err(err)
}

/// Non-blocking exclusive lock attempt: `Ok(true)` = acquired, `Ok(false)`
/// = held by another process.
#[cfg(unix)]
fn try_lock_exclusive(file: &std::fs::File) -> std::io::Result<bool> {
    use std::os::unix::io::AsRawFd as _;
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        return Ok(true);
    }
    let err = std::io::Error::last_os_error();
    if err.kind() == std::io::ErrorKind::WouldBlock {
        return Ok(false);
    }
    Err(err)
}

/// Locate an executable on PATH (Windows: with `.exe` appended too).
fn on_path(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        if dir.as_os_str().is_empty() {
            continue;
        }
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
        if cfg!(windows) {
            let exe = dir.join(format!("{name}.exe"));
            if exe.is_file() {
                return Some(exe);
            }
        }
    }
    None
}

/// A validated uv binary.
struct UvBinary {
    path: PathBuf,
    version: String,
}

/// uv discovery: PATH (validated with `uv --version` and the
/// [`UV_MIN_VERSION`] floor), then a previously downloaded managed copy at
/// `runtime/uv/<UV_VERSION>/`, then a fresh download of the pinned
/// standalone build.
async fn locate_uv() -> Result<UvBinary> {
    if let Some(path) = on_path("uv") {
        match validate_uv(&path).await {
            Ok(version) if version_at_least(&version, UV_MIN_VERSION) => {
                return Ok(UvBinary { path, version });
            }
            Ok(version) => tracing::warn!(
                uv = %path.display(),
                version = %version,
                minimum = format!(
                    "{}.{}.{}",
                    UV_MIN_VERSION.0, UV_MIN_VERSION.1, UV_MIN_VERSION.2
                ),
                "uv on PATH is too old; falling back to the managed download"
            ),
            Err(err) => tracing::warn!(
                uv = %path.display(),
                error = format!("{err:#}"),
                "uv on PATH failed validation; falling back to the managed download"
            ),
        }
    }

    let managed = std::path::absolute(managed_uv_path())
        .context("failed to resolve the managed uv path")?;
    if managed.is_file() {
        match validate_uv(&managed).await {
            Ok(version) => return Ok(UvBinary { path: managed, version }),
            Err(err) => tracing::warn!(
                uv = %managed.display(),
                error = format!("{err:#}"),
                "previously downloaded uv failed validation; re-downloading"
            ),
        }
    }

    download_uv(&managed).await?;
    let version = validate_uv(&managed)
        .await
        .context("downloaded uv failed validation")?;
    Ok(UvBinary {
        path: managed,
        version,
    })
}

/// `runtime/uv/<UV_VERSION>/uv(.exe)`, relative to the CWD.
fn managed_uv_path() -> PathBuf {
    Path::new(UV_RUNTIME_DIR)
        .join(UV_VERSION)
        .join(if cfg!(windows) { "uv.exe" } else { "uv" })
}

/// Run `<uv> --version` and return the parsed version string (e.g.
/// "0.11.28").
async fn validate_uv(uv: &Path) -> Result<String> {
    let output = Command::new(uv)
        .arg("--version")
        .stdin(Stdio::null())
        .output()
        .await
        .with_context(|| format!("failed to run '{} --version'", uv.display()))?;
    if !output.status.success() {
        bail!(
            "'{} --version' failed with {}",
            uv.display(),
            output.status
        );
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_uv_version(&stdout)
        .with_context(|| format!("unexpected '{} --version' output: {stdout:?}", uv.display()))
}

/// Parse "uv X.Y.Z (…)" into "X.Y.Z".
fn parse_uv_version(output: &str) -> Option<String> {
    let version = output.trim().strip_prefix("uv ")?.split_whitespace().next()?;
    version_triple(version).map(|_| version.to_string())
}

fn version_triple(version: &str) -> Option<(u64, u64, u64)> {
    let mut parts = version.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    // Allow suffixes like "1rc1" by taking leading digits of the patch.
    let patch_part = parts.next()?;
    let digits: String = patch_part.chars().take_while(char::is_ascii_digit).collect();
    let patch = digits.parse().ok()?;
    Some((major, minor, patch))
}

fn version_at_least(version: &str, minimum: (u64, u64, u64)) -> bool {
    version_triple(version).is_some_and(|v| v >= minimum)
}

/// The standalone-build asset name for the current platform.
fn uv_asset_name() -> Result<&'static str> {
    Ok(match (std::env::consts::OS, std::env::consts::ARCH) {
        ("windows", "x86_64") => "uv-x86_64-pc-windows-msvc.zip",
        ("linux", "x86_64") => "uv-x86_64-unknown-linux-gnu.tar.gz",
        ("linux", "aarch64") => "uv-aarch64-unknown-linux-gnu.tar.gz",
        ("macos", "aarch64") => "uv-aarch64-apple-darwin.tar.gz",
        ("macos", "x86_64") => "uv-x86_64-apple-darwin.tar.gz",
        (os, arch) => bail!(
            "no pinned uv build for {os}/{arch}; install uv on PATH \
             (https://docs.astral.sh/uv/) and re-run setup"
        ),
    })
}

/// Download the pinned standalone uv release and extract it next to
/// `target` (the managed uv binary path). Progress is streamed to tracing.
async fn download_uv(target: &Path) -> Result<()> {
    let asset = uv_asset_name()?;
    let expected_sha256 = uv_asset_sha256(asset)?;
    let url =
        format!("https://github.com/astral-sh/uv/releases/download/{UV_VERSION}/{asset}");
    let dir = target
        .parent()
        .context("managed uv path has no parent directory")?;
    tokio::fs::create_dir_all(dir)
        .await
        .with_context(|| format!("failed to create '{}'", dir.display()))?;
    tracing::info!(url = %url, "downloading uv {UV_VERSION}");

    let response = reqwest::get(&url)
        .await
        .and_then(reqwest::Response::error_for_status)
        .with_context(|| format!("failed to download {url}"))?;
    let total = response.content_length();
    // Unique temp name (pid suffix) so a concurrent writer — another
    // process not serialized by our setup lock — can never interleave into
    // the same file; renamed into place only after the checksum verifies.
    let temp = dir.join(format!("{asset}.{}.part", std::process::id()));
    let mut file = tokio::fs::File::create(&temp)
        .await
        .with_context(|| format!("failed to create '{}'", temp.display()))?;
    let mut stream = response.bytes_stream();
    let mut hasher = {
        use sha2::Digest as _;
        sha2::Sha256::new()
    };
    let mut downloaded: u64 = 0;
    let mut next_log = DOWNLOAD_LOG_STEP;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.with_context(|| format!("download of {url} failed mid-stream"))?;
        file.write_all(&chunk)
            .await
            .with_context(|| format!("failed to write '{}'", temp.display()))?;
        {
            use sha2::Digest as _;
            hasher.update(&chunk);
        }
        downloaded += chunk.len() as u64;
        if downloaded >= next_log {
            next_log = downloaded + DOWNLOAD_LOG_STEP;
            match total {
                Some(total) => tracing::info!(
                    "downloading uv: {} / {} MiB",
                    downloaded / (1024 * 1024),
                    total / (1024 * 1024)
                ),
                None => tracing::info!(
                    "downloading uv: {} MiB",
                    downloaded / (1024 * 1024)
                ),
            }
        }
    }
    file.flush().await.ok();
    drop(file);

    let actual_sha256 = {
        use sha2::Digest as _;
        hex_string(&hasher.finalize())
    };
    if actual_sha256 != expected_sha256 {
        tokio::fs::remove_file(&temp).await.ok();
        bail!(
            "checksum mismatch for {url}: expected sha256 {expected_sha256}, \
             got {actual_sha256} — the download is corrupted or tampered \
             with; refusing to extract"
        );
    }
    tracing::info!(
        size_mib = downloaded / (1024 * 1024),
        sha256 = %actual_sha256,
        "download complete, checksum verified; extracting"
    );

    // Prefer the canonical archive name; if the rename loses a race with
    // another (non-locked) process, extract from our verified temp file.
    let canonical = dir.join(asset);
    let archive = match std::fs::rename(&temp, &canonical) {
        Ok(()) => canonical,
        Err(_) => temp,
    };
    let archive_for_task = archive.clone();
    let dir_for_task = dir.to_path_buf();
    tokio::task::spawn_blocking(move || extract_uv_archive(&archive_for_task, &dir_for_task))
        .await
        .context("uv extraction task panicked")?
        .with_context(|| format!("failed to extract '{}'", archive.display()))?;
    tokio::fs::remove_file(&archive).await.ok();

    if !target.is_file() {
        bail!(
            "extracted uv archive did not contain '{}'",
            target.display()
        );
    }
    tracing::info!(uv = %target.display(), "uv {UV_VERSION} installed");
    Ok(())
}

/// The pinned checksum for a release asset ([`UV_ASSET_SHA256`]).
fn uv_asset_sha256(asset: &str) -> Result<&'static str> {
    UV_ASSET_SHA256
        .iter()
        .find(|(name, _)| *name == asset)
        .map(|(_, sha)| *sha)
        .with_context(|| format!("no pinned sha256 for uv asset '{asset}' (UV_ASSET_SHA256)"))
}

/// Windows zip: uv.exe/uvx.exe/uvw.exe sit at the archive root. Only file
/// basenames are used (zip-slip guard) and every regular file is extracted
/// flat into `dest`.
#[cfg(windows)]
fn extract_uv_archive(archive: &Path, dest: &Path) -> Result<()> {
    let file = std::fs::File::open(archive)?;
    let mut zip = zip::ZipArchive::new(file)?;
    for index in 0..zip.len() {
        let mut entry = zip.by_index(index)?;
        if !entry.is_file() {
            continue;
        }
        let Some(name) = Path::new(entry.name()).file_name().map(PathBuf::from) else {
            continue;
        };
        let mut out = std::fs::File::create(dest.join(name))?;
        std::io::copy(&mut entry, &mut out)?;
    }
    Ok(())
}

/// Unix tar.gz: entries live under a `uv-<triple>/` directory. The leading
/// directory is stripped (basenames only — also the path-traversal guard)
/// and the executable bit is set.
#[cfg(unix)]
fn extract_uv_archive(archive: &Path, dest: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    let file = std::fs::File::open(archive)?;
    let mut tar = tar::Archive::new(flate2::read::GzDecoder::new(file));
    for entry in tar.entries()? {
        let mut entry = entry?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let Some(name) = entry.path()?.file_name().map(PathBuf::from) else {
            continue;
        };
        let out = dest.join(name);
        entry.unpack(&out)?;
        std::fs::set_permissions(&out, std::fs::Permissions::from_mode(0o755))?;
    }
    Ok(())
}

/// Spawn a uv child in `cwd` with stdout/stderr streamed line-by-line into
/// tracing (mirrors ui.rs `run_logged`; uv writes its progress to stderr).
/// `UV_PROJECT_ENVIRONMENT` is pinned to the guarded managed venv so no
/// ambient uv configuration (env vars, user-level uv.toml) can redirect the
/// operation to another environment. On failure the error carries the exit
/// status and the last stderr lines.
async fn run_uv_logged(
    uv: &Path,
    args: &[String],
    cwd: &Path,
    venv: &Path,
    what: &'static str,
) -> Result<()> {
    guard_managed_venv(venv)?;
    let mut command = Command::new(uv);
    command
        .args(args)
        .current_dir(cwd)
        .env("UV_PROJECT_ENVIRONMENT", venv)
        // A different active venv (e.g. the repo root's) would make uv warn
        // or bail; the managed environment is explicit, so drop it.
        .env_remove("VIRTUAL_ENV")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    detach_from_console(&mut command);
    die_with_parent(&mut command);
    let mut child = command
        .spawn()
        .with_context(|| format!("failed to spawn {what} ('{}')", uv.display()))?;
    let job_guard = JobGuard::assign_tokio(&child);
    let stdout = child.stdout.take().expect("stdout is piped");
    let stderr = child.stderr.take().expect("stderr is piped");
    let tail = Arc::new(Mutex::new(VecDeque::with_capacity(STDERR_TAIL_LINES)));
    let stdout_task = tokio::spawn(forward_lines(stdout, what, "stdout", None));
    let stderr_task = tokio::spawn(forward_lines(
        stderr,
        what,
        "stderr",
        Some(Arc::clone(&tail)),
    ));
    let status = child
        .wait()
        .await
        .with_context(|| format!("failed to wait for {what}"))?;
    drop(job_guard);
    let _ = stdout_task.await;
    let _ = stderr_task.await;
    if !status.success() {
        let tail = tail.lock().expect("stderr tail lock");
        let tail = tail.iter().cloned().collect::<Vec<_>>().join("\n");
        bail!("{what} failed with {status}; last output:\n{tail}");
    }
    Ok(())
}

/// Forward one child stream to tracing, optionally keeping the last
/// [`STDERR_TAIL_LINES`] lines for error reporting.
async fn forward_lines(
    stream: impl AsyncRead + Unpin,
    what: &'static str,
    name: &'static str,
    tail: Option<Arc<Mutex<VecDeque<String>>>>,
) {
    let mut lines = BufReader::new(stream).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                let line = line.trim_end();
                if line.is_empty() {
                    continue;
                }
                tracing::info!(setup = what, stream = name, "{line}");
                if let Some(tail) = &tail {
                    let mut tail = tail.lock().expect("stderr tail lock");
                    if tail.len() == STDERR_TAIL_LINES {
                        tail.pop_front();
                    }
                    tail.push_back(line.to_string());
                }
            }
            Ok(None) => break,
            Err(err) => {
                tracing::debug!(setup = what, stream = name, "stream read failed: {err}");
                break;
            }
        }
    }
}

/// Startup auto-trigger (gateway and `inferio` modes): run setup before the
/// orchestrator starts when local inference is enabled, `auto_setup` is on,
/// no explicit interpreter is configured (a user-specified interpreter is
/// never auto-managed), and [`auto_setup_needed`] says the managed
/// environment is missing, incomplete (interrupted first sync — the
/// completion sentinel is absent), or stale (uv.lock changed). A legacy
/// root `.venv` without a managed venv suppresses the trigger
/// (pre-restructure installs keep working untouched). Failures are logged,
/// not fatal: the server comes up with inference unavailable rather than
/// dying.
///
/// `inference_enabled` is passed by the caller because the `inferio`
/// subcommand implies local inference regardless of the config flag.
pub async fn maybe_auto_setup(settings: &Settings, inference_enabled: bool) {
    let local = &settings.inference_local;
    if !inference_enabled || !local.python_env.auto_setup || local.python.is_some() {
        return;
    }
    let Some(reason) = auto_setup_needed() else {
        return;
    };
    tracing::info!(
        reason,
        "running setup automatically (disable with \
         [inference_local.python_env] auto_setup = false)"
    );
    match run(
        settings,
        SetupOptions {
            accelerator: None,
            force: false,
            skip_if_converged: true,
        },
    )
    .await
    {
        Ok(()) => tracing::info!("automatic Python environment setup finished"),
        Err(err) => tracing::error!(
            error = format!("{err:#}"),
            "automatic Python environment setup failed; local inference will \
             be unavailable until `panoptikon setup` succeeds"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probes(os: &'static str) -> DetectionProbes {
        DetectionProbes {
            os,
            nvidia_smi_on_path: false,
            system32_nvidia_smi: false,
            proc_driver_nvidia: false,
            rocm_dir: false,
            rocm_smi_on_path: false,
        }
    }

    /// The auto-detection decision table: macOS is always the PyPI/MPS path
    /// (even with stray GPU evidence), any NVIDIA probe wins CUDA, ROCm
    /// evidence only counts on Linux, and no evidence means CPU.
    #[test]
    fn accelerator_decision_table() {
        // macOS: always PyPI wheels, even if probes claim GPUs.
        let mut mac = probes("macos");
        mac.nvidia_smi_on_path = true;
        mac.rocm_smi_on_path = true;
        assert_eq!(decide_accelerator(&mac).0, Accelerator::Cpu);

        // Windows: System32 nvidia-smi or PATH nvidia-smi → CUDA.
        let mut win = probes("windows");
        assert_eq!(decide_accelerator(&win).0, Accelerator::Cpu);
        win.system32_nvidia_smi = true;
        assert_eq!(decide_accelerator(&win).0, Accelerator::Cuda);
        let mut win = probes("windows");
        win.nvidia_smi_on_path = true;
        assert_eq!(decide_accelerator(&win).0, Accelerator::Cuda);
        // ROCm evidence is ignored off Linux.
        let mut win = probes("windows");
        win.rocm_smi_on_path = true;
        assert_eq!(decide_accelerator(&win).0, Accelerator::Cpu);

        // Linux: /proc/driver/nvidia or nvidia-smi → CUDA, which beats ROCm.
        let mut linux = probes("linux");
        assert_eq!(decide_accelerator(&linux).0, Accelerator::Cpu);
        linux.proc_driver_nvidia = true;
        assert_eq!(decide_accelerator(&linux).0, Accelerator::Cuda);
        linux.rocm_dir = true;
        assert_eq!(decide_accelerator(&linux).0, Accelerator::Cuda);
        // ROCm without NVIDIA: /opt/rocm or rocm-smi → ROCm.
        let mut linux = probes("linux");
        linux.rocm_dir = true;
        assert_eq!(decide_accelerator(&linux).0, Accelerator::Rocm);
        let mut linux = probes("linux");
        linux.rocm_smi_on_path = true;
        assert_eq!(decide_accelerator(&linux).0, Accelerator::Rocm);
    }

    /// Accelerator → pyproject extra mapping, and command construction for
    /// both uv operations (`--locked` is what makes the lock authoritative).
    #[test]
    fn uv_command_construction() {
        assert_eq!(accelerator_extra(Accelerator::Cuda), "cu128");
        assert_eq!(accelerator_extra(Accelerator::Rocm), "rocm");
        assert_eq!(accelerator_extra(Accelerator::Cpu), "cpu");
        assert_eq!(
            uv_sync_args("cu128"),
            ["sync", "--locked", "--extra", "cu128"]
        );
        let venv = std::env::temp_dir().join("pan-venv");
        assert_eq!(
            uv_venv_args(&venv),
            [
                "venv".to_string(),
                venv.display().to_string(),
                "--python".into(),
                PYTHON_VERSION.into()
            ]
        );
    }

    /// The per-mode managed paths (docs/architecture.md): the dev layout is
    /// the pre-bundling one; extracted mode keys the project dir by version
    /// but keeps the venv outside it (version bumps re-extract sources, the
    /// sentinel's lock hash drives the re-sync).
    #[test]
    fn managed_python_paths_per_mode() {
        use crate::resources::PySourceMode::*;
        let dev = ManagedPython::for_mode(Dev);
        assert_eq!(dev.project_dir, PathBuf::from("python"));
        assert_eq!(dev.venv, PathBuf::from("python/.venv"));
        assert_eq!(dev.uv_lock, PathBuf::from("python/uv.lock"));
        assert!(dev.legacy_suppresses);

        let extracted = ManagedPython::for_mode(Extracted);
        assert_eq!(
            extracted.project_dir,
            Path::new("runtime/pysrc").join(crate::resources::VERSION)
        );
        assert_eq!(extracted.venv, PathBuf::from("runtime/venv"));
        assert_eq!(
            extracted.uv_lock,
            Path::new("runtime/pysrc")
                .join(crate::resources::VERSION)
                .join("uv.lock")
        );
        assert!(!extracted.legacy_suppresses);
    }

    /// The safety guard: only the CWD-resolved managed venv of the ACTIVE
    /// mode passes; the repo-root legacy venv, sibling paths, the other
    /// mode's venv, and arbitrary paths are all refused.
    #[test]
    fn venv_guard_refuses_everything_but_the_managed_venv() {
        use crate::resources::PySourceMode::*;
        let active_mode = crate::resources::py_source_mode();
        let active = ManagedPython::for_mode(active_mode).venv;
        let managed = std::path::absolute(&active).unwrap();
        assert!(guard_managed_venv(&managed).is_ok());

        let other_mode = match active_mode {
            Dev => Extracted,
            Extracted => Dev,
        };
        for bad in [
            std::path::absolute(".venv").unwrap(),
            std::path::absolute("python/.venv2").unwrap(),
            std::path::absolute("python").unwrap(),
            // The OTHER mode's managed venv is refused too: the guard is
            // strict about the active mode.
            std::path::absolute(ManagedPython::for_mode(other_mode).venv).unwrap(),
            active.clone(), // relative spelling is refused too
            std::env::temp_dir().join("some-venv"),
        ] {
            let err = guard_managed_venv(&bad).unwrap_err();
            assert!(
                err.to_string().contains("refusing to operate"),
                "path {bad:?} produced: {err}"
            );
        }
    }

    /// `uv --version` output parsing and the PATH-uv minimum floor.
    #[test]
    fn uv_version_parsing() {
        assert_eq!(
            parse_uv_version("uv 0.11.28 (ebf0f43d7 2026-07-07)").as_deref(),
            Some("0.11.28")
        );
        assert_eq!(parse_uv_version("uv 0.6.13\n").as_deref(), Some("0.6.13"));
        assert_eq!(parse_uv_version("not uv"), None);
        assert_eq!(parse_uv_version(""), None);
        assert!(version_at_least("0.6.13", UV_MIN_VERSION));
        assert!(version_at_least("1.0.0", UV_MIN_VERSION));
        assert!(!version_at_least("0.6.12", UV_MIN_VERSION));
        assert!(!version_at_least("0.5.9", UV_MIN_VERSION));
    }

    /// Every platform asset has a pinned checksum, and the lookup rejects
    /// unknown assets.
    #[test]
    fn uv_asset_checksums_pinned() {
        for asset in [
            "uv-x86_64-pc-windows-msvc.zip",
            "uv-x86_64-unknown-linux-gnu.tar.gz",
            "uv-aarch64-unknown-linux-gnu.tar.gz",
            "uv-aarch64-apple-darwin.tar.gz",
            "uv-x86_64-apple-darwin.tar.gz",
        ] {
            let sha = uv_asset_sha256(asset).unwrap();
            assert_eq!(sha.len(), 64, "{asset}: not a sha256 hex digest");
            assert!(sha.chars().all(|c| c.is_ascii_hexdigit()));
        }
        assert!(uv_asset_sha256("uv-unknown.zip").is_err());
        // The asset for the current platform (when supported) is pinned.
        if let Ok(asset) = uv_asset_name() {
            assert!(uv_asset_sha256(asset).is_ok());
        }
    }

    /// Sentinel classification: absent/malformed = Missing (setup never
    /// completed), matching hash = Valid, anything else = Stale.
    #[test]
    fn sentinel_status_classification() {
        let current = Some("abc123");
        assert_eq!(sentinel_status_from(None, current), SentinelStatus::Missing);
        assert_eq!(
            sentinel_status_from(Some("garbage without the key"), current),
            SentinelStatus::Missing
        );
        assert_eq!(
            sentinel_status_from(Some("extra=cu128\nuv_lock_sha256=abc123\n"), current),
            SentinelStatus::Valid
        );
        assert_eq!(
            sentinel_status_from(Some("extra=cu128\nuv_lock_sha256=OLD\n"), current),
            SentinelStatus::Stale
        );
        // Unreadable current lock: present sentinel counts as stale (re-run
        // surfaces the real problem), absent stays missing.
        assert_eq!(
            sentinel_status_from(Some("uv_lock_sha256=abc123"), None),
            SentinelStatus::Stale
        );
        assert_eq!(sentinel_status_from(None, None), SentinelStatus::Missing);
    }

    /// The auto-trigger decision table: a managed interpreter is judged by
    /// its sentinel; without one, the legacy root .venv suppresses the
    /// trigger (pre-restructure installs untouched); fresh installs fire.
    #[test]
    fn auto_setup_decision_table() {
        use SentinelStatus::*;
        // Managed venv complete: never fires, legacy irrelevant.
        assert_eq!(auto_setup_decision(true, false, Valid), None);
        assert_eq!(auto_setup_decision(true, true, Valid), None);
        // Managed venv interrupted or stale: fires even with a legacy venv.
        assert!(auto_setup_decision(true, false, Missing).is_some());
        assert!(auto_setup_decision(true, true, Missing).is_some());
        assert!(auto_setup_decision(true, false, Stale).is_some());
        assert!(auto_setup_decision(true, true, Stale).is_some());
        // No managed venv: legacy suppresses, fresh install fires.
        assert_eq!(auto_setup_decision(false, true, Missing), None);
        assert!(auto_setup_decision(false, false, Missing).is_some());
    }

    /// Sentinel write/read round-trip against the real managed venv path
    /// requires the venv to exist; instead verify the writer's guard: it
    /// refuses any path but the managed venv.
    #[test]
    fn write_sentinel_is_guarded() {
        let dir = tempfile::tempdir().unwrap();
        let err =
            write_sentinel(dir.path(), "cpu", Path::new("python/uv.lock")).unwrap_err();
        assert!(err.to_string().contains("refusing to operate"), "{err}");
    }
}
