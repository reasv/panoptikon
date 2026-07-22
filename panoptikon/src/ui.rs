//! Production UI server (`[upstreams.ui] local = true`).
//!
//! Port of the legacy Python `searchui/router.py` (python-legacy branch)
//! minus the git clone/pull: the checkout
//! at `upstreams.ui.dir` is managed by the user. A background task installs
//! dependencies and builds when stale, then supervises `next start` bound to
//! the host/port parsed from `base_url` — the proxy keeps forwarding to that
//! same URL, returning 502 until the server is up. Children run under a
//! kill-on-close Job Object on Windows (`next start` forks its own workers)
//! plus tokio `kill_on_drop`, so no drop path leaks a node tree.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::Command;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::config::{Settings, UiBuildPolicy};
use crate::process_tree::{JobGuard, detach_from_console, die_with_parent};

/// Restart backoff for unexpected exits and failed install/build steps:
/// 1s doubling to 30s, reset once a start stays up for [`STABLE_UPTIME`].
const RESTART_BACKOFF_MIN: Duration = Duration::from_secs(1);
const RESTART_BACKOFF_MAX: Duration = Duration::from_secs(30);
const STABLE_UPTIME: Duration = Duration::from_secs(60);

/// Cadence and give-up horizon for the "UI is up" readiness log line. Only
/// the logging gives up — the server (and restart loop) keep running.
const PORT_POLL_INTERVAL: Duration = Duration::from_millis(500);
const PORT_POLL_GIVE_UP: Duration = Duration::from_secs(300);

/// The Next.js CLI entrypoint, run directly under node (relative to the
/// checkout). Avoids the platform-dependent `.bin/next` shims the Python
/// implementation had to shell out for.
const NEXT_BIN: &str = "node_modules/next/dist/bin/next";

/// Stamp touched after each successful `npm install` (relative to the
/// checkout). npm does not reliably bump `node_modules`' own mtime, so the
/// staleness check compares `package.json` against this when present.
const INSTALL_STAMP: &str = "node_modules/.gateway-install-stamp";

/// Stamp holding the HEAD hash the last gateway-run `next build` was built
/// from (relative to the checkout). The auto policy rebuilds on any HEAD
/// change; comparing BUILD_ID's mtime against the commit *time* went stale
/// after build-then-pull, where the pulled commit predates the build.
const BUILT_COMMIT_STAMP: &str = ".next/.gateway-built-commit";

/// Handle to the supervision task. Dropping it without [`shutdown`] leaves
/// the task running detached (the Job Object still reaps children when the
/// gateway process exits).
///
/// [`shutdown`]: UiServerHandle::shutdown
pub(crate) struct UiServerHandle {
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl UiServerHandle {
    /// Stops the restart loop and kills the running child tree. The UI is
    /// stateless, so there is no graceful ladder — straight to kill.
    pub(crate) async fn shutdown(self) {
        let _ = self.stop.send(true);
        let _ = self.task.await;
    }
}

/// Everything the supervision task needs, resolved once at startup.
struct UiPlan {
    dir: PathBuf,
    host: String,
    port: u16,
    base_url: String,
    api_url: String,
    node_override: Option<PathBuf>,
    build: UiBuildPolicy,
}

/// Start the install → build → `next start` sequence in a background task
/// (gateway startup and readiness are not blocked). Config errors (missing
/// dir, bad base_url) fail fast here; runtime failures are logged and
/// retried with backoff inside the task.
pub(crate) fn start(settings: &Settings) -> Result<UiServerHandle> {
    let ui = &settings.upstreams.ui;
    let dir = ui
        .dir
        .clone()
        .context("upstreams.ui.local = true requires upstreams.ui.dir")?;
    let dir = std::path::absolute(&dir)
        .with_context(|| format!("failed to resolve upstreams.ui.dir '{}'", dir.display()))?;
    let (host, port) = ui.local_bind_addr()?;
    let plan = UiPlan {
        dir,
        host,
        port,
        base_url: ui.base_url.clone(),
        api_url: crate::config::loopback_base_url(&settings.server.host, settings.server.port),
        node_override: ui.node.clone(),
        build: ui.build,
    };
    let (stop, stop_rx) = watch::channel(false);
    let task = tokio::spawn(run(plan, stop_rx));
    Ok(UiServerHandle { stop, task })
}

/// Supervision loop: run the whole sequence, restart on unexpected exit or
/// failed step with capped backoff. Staleness is re-checked every attempt,
/// so a retry after a successful build skips straight to `next start`.
async fn run(plan: UiPlan, mut stop: watch::Receiver<bool>) {
    let mut backoff = RESTART_BACKOFF_MIN;
    let mut announcer_started = false;
    loop {
        if *stop.borrow() {
            return;
        }
        match run_once(&plan, &mut stop, &mut announcer_started).await {
            Ok(RunOutcome::Stopped) => return,
            // Retrying cannot help and the cause was already logged.
            Ok(RunOutcome::Fatal) => return,
            Ok(RunOutcome::Exited { status, uptime }) => {
                if uptime >= STABLE_UPTIME {
                    backoff = RESTART_BACKOFF_MIN;
                }
                tracing::warn!(
                    %status,
                    uptime_secs = uptime.as_secs(),
                    retry_in_secs = backoff.as_secs(),
                    "UI server exited unexpectedly; restarting"
                );
            }
            Err(err) => {
                tracing::error!(
                    error = format!("{err:#}"),
                    retry_in_secs = backoff.as_secs(),
                    "UI startup step failed; retrying"
                );
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = stopped(&mut stop) => return,
        }
        backoff = (backoff * 2).min(RESTART_BACKOFF_MAX);
    }
}

enum RunOutcome {
    /// Shutdown was requested; the child (if any) has been killed.
    Stopped,
    /// An unrecoverable configuration problem (already logged): the
    /// supervisor stops instead of restarting into the same failure.
    Fatal,
    /// `next start` exited on its own.
    Exited {
        status: std::process::ExitStatus,
        uptime: Duration,
    },
}

async fn run_once(
    plan: &UiPlan,
    stop: &mut watch::Receiver<bool>,
    announcer_started: &mut bool,
) -> Result<RunOutcome> {
    if !plan.dir.is_dir() {
        // Resolution order (docs/architecture.md): a dev checkout at the
        // configured dir always wins; only when it is absent does a
        // bundled-ui build fall back to the embedded bundle.
        return run_once_without_checkout(plan, stop, announcer_started).await;
    }
    let node = resolve_node(plan.node_override.as_deref(), Path::new("."));

    if needs_install(&plan.dir) {
        let npm = resolve_npm(&node);
        tracing::info!(dir = %plan.dir.display(), npm = %npm, "installing UI dependencies");
        let mut command = npm.command();
        command
            .args(["install", "--include=dev"])
            .current_dir(&plan.dir);
        match run_logged(command, "npm install", stop).await? {
            CommandEnd::Stopped => return Ok(RunOutcome::Stopped),
            CommandEnd::Exited(status) if !status.success() => {
                bail!("npm install failed with {status}");
            }
            CommandEnd::Exited(_) => {
                let stamp = plan.dir.join(INSTALL_STAMP);
                if let Err(err) = std::fs::write(&stamp, b"") {
                    tracing::warn!(path = %stamp.display(), "failed to write install stamp: {err}");
                }
            }
        }
    }

    if !plan.dir.join(NEXT_BIN).is_file() {
        bail!(
            "'{}' is missing under '{}' even after npm install — is the dir a \
             panoptikon-ui checkout?",
            NEXT_BIN,
            plan.dir.display()
        );
    }

    match build_decision(&plan.dir, plan.build).await {
        BuildDecision::MissingBuildForbidden => {
            tracing::error!(
                dir = %plan.dir.display(),
                "no production build exists (.next/BUILD_ID missing) and build = \"never\" \
                 prevents creating one; stopping the UI supervisor — run `next build` in \
                 the checkout or set build = \"auto\""
            );
            return Ok(RunOutcome::Fatal);
        }
        BuildDecision::Build => {
            tracing::info!(dir = %plan.dir.display(), "building the UI (next build); this can take minutes");
            let mut command = Command::new(&node);
            command
                .arg(NEXT_BIN)
                .arg("build")
                .current_dir(&plan.dir)
                .env("PANOPTIKON_API_URL", &plan.api_url);
            match run_logged(command, "next build", stop).await? {
                CommandEnd::Stopped => return Ok(RunOutcome::Stopped),
                CommandEnd::Exited(status) if !status.success() => {
                    bail!("next build failed with {status}");
                }
                CommandEnd::Exited(_) => {
                    stamp_built_commit(&plan.dir).await;
                    tracing::info!("UI build finished");
                }
            }
        }
        BuildDecision::Skip => {}
    }

    warn_if_port_taken(plan, "next start").await;

    tracing::info!(url = %plan.base_url, node = %node.display(), "starting UI server (next start)");
    let mut command = Command::new(&node);
    command
        .arg(NEXT_BIN)
        .args(["start", "-p", &plan.port.to_string(), "-H", &plan.host])
        .current_dir(&plan.dir)
        .env("PANOPTIKON_API_URL", &plan.api_url);
    start_announcer_once(plan, stop, announcer_started);
    let started = Instant::now();
    match run_logged(command, "next start", stop).await? {
        CommandEnd::Stopped => Ok(RunOutcome::Stopped),
        CommandEnd::Exited(status) => Ok(RunOutcome::Exited {
            status,
            uptime: started.elapsed(),
        }),
    }
}

/// The configured checkout dir does not exist and this build embeds no UI
/// bundle: an unrecoverable setup problem, reported as before.
#[cfg(not(all(feature = "bundled-ui", ui_bundle_present)))]
async fn run_once_without_checkout(
    plan: &UiPlan,
    _stop: &mut watch::Receiver<bool>,
    _announcer_started: &mut bool,
) -> Result<RunOutcome> {
    bail!(
        "upstreams.ui.dir '{}' does not exist; clone panoptikon-ui there \
         (the gateway does not manage the checkout)",
        plan.dir.display()
    );
}

/// Embedded-bundle mode (`bundled-ui`, docs/architecture.md): the
/// configured checkout dir is absent, so extract the embedded Next.js
/// standalone bundle to `runtime/ui/<version>` (no-op when already there)
/// and supervise `node server.js` bound via the PORT/HOSTNAME env vars —
/// the standalone server is NOT `next start`. Install/build staleness steps
/// are skipped entirely: the bundle is immutable.
#[cfg(all(feature = "bundled-ui", ui_bundle_present))]
async fn run_once_without_checkout(
    plan: &UiPlan,
    stop: &mut watch::Receiver<bool>,
    announcer_started: &mut bool,
) -> Result<RunOutcome> {
    let bundle_dir = crate::resources::ensure_ui_bundle_extracted()?;
    let server_js = bundle_dir.join("server.js");
    if !server_js.is_file() {
        bail!(
            "extracted UI bundle at '{}' has no server.js — delete the directory \
             and restart to re-extract",
            bundle_dir.display()
        );
    }
    let node = resolve_node(plan.node_override.as_deref(), Path::new("."));

    warn_if_port_taken(plan, "node server.js").await;

    tracing::info!(
        url = %plan.base_url,
        node = %node.display(),
        dir = %bundle_dir.display(),
        "starting embedded UI server (node server.js)"
    );
    let mut command = Command::new(&node);
    command
        .arg(&server_js)
        .current_dir(&bundle_dir)
        // The Next standalone server takes its bind address from the
        // environment, not CLI flags.
        .env("PORT", plan.port.to_string())
        .env("HOSTNAME", &plan.host)
        .env("PANOPTIKON_API_URL", &plan.api_url)
        .env("NODE_ENV", "production");
    start_announcer_once(plan, stop, announcer_started);
    let started = Instant::now();
    match run_logged(command, "ui server", stop).await? {
        CommandEnd::Stopped => Ok(RunOutcome::Stopped),
        CommandEnd::Exited(status) => Ok(RunOutcome::Exited {
            status,
            uptime: started.elapsed(),
        }),
    }
}

/// A foreign process already answering on the UI port means the spawn will
/// fail to bind — and the readiness announcer would credit the wrong
/// server. The Python-managed UI is the usual suspect.
async fn warn_if_port_taken(plan: &UiPlan, what: &str) {
    if tokio::net::TcpStream::connect((plan.host.as_str(), plan.port))
        .await
        .is_ok()
    {
        tracing::warn!(
            host = %plan.host,
            port = plan.port,
            "UI port is already accepting connections before {what}; another \
             process (e.g. a Python-managed UI) appears to hold it"
        );
    }
}

/// Start the "UI is up" readiness announcer with the first spawn attempt;
/// it survives restarts, so later attempts must not start a second one.
fn start_announcer_once(plan: &UiPlan, stop: &watch::Receiver<bool>, announcer_started: &mut bool) {
    if !*announcer_started {
        *announcer_started = true;
        tokio::spawn(announce_when_up(
            plan.host.clone(),
            plan.port,
            plan.base_url.clone(),
            stop.clone(),
        ));
    }
}

enum CommandEnd {
    /// Shutdown was requested mid-run; the child tree has been killed.
    Stopped,
    Exited(std::process::ExitStatus),
}

/// Spawn a child with stdout/stderr streamed line-by-line into tracing
/// (info level, labelled with the step name), wait for it — no timeout;
/// builds take minutes — and kill it if shutdown arrives first. The child
/// sits under a kill-on-close Job Object on Windows so `next start`'s own
/// children cannot outlive the gateway.
async fn run_logged(
    mut command: Command,
    what: &'static str,
    stop: &mut watch::Receiver<bool>,
) -> Result<CommandEnd> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    detach_from_console(&mut command);
    die_with_parent(&mut command);
    let mut child = command
        .spawn()
        .with_context(|| format!("failed to spawn {what}"))?;
    let job_guard = JobGuard::assign_tokio(&child);
    let stdout = child.stdout.take().expect("stdout is piped");
    let stderr = child.stderr.take().expect("stderr is piped");
    let stdout_task = tokio::spawn(forward_lines(stdout, what, "stdout"));
    let stderr_task = tokio::spawn(forward_lines(stderr, what, "stderr"));

    let status = tokio::select! {
        status = child.wait() => status.with_context(|| format!("failed to wait for {what}"))?,
        _ = stopped(stop) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            // Dropping the guard closes the job object, reaping any
            // grandchildren start_kill did not reach.
            drop(job_guard);
            return Ok(CommandEnd::Stopped);
        }
    };
    drop(job_guard);
    // The forwarders end on pipe EOF; join so trailing output lands in the
    // log before the exit status does.
    let _ = stdout_task.await;
    let _ = stderr_task.await;
    Ok(CommandEnd::Exited(status))
}

/// Resolves when shutdown is requested. A closed channel (handle dropped
/// without shutdown) counts as a stop so the loop cannot run unsupervised.
async fn stopped(stop: &mut watch::Receiver<bool>) {
    while !*stop.borrow_and_update() {
        if stop.changed().await.is_err() {
            return;
        }
    }
}

/// Forward one child stream to tracing, one line per event, tagged with the
/// step (`next start`, `next build`, `npm install`) and stream name.
async fn forward_lines(stream: impl AsyncRead + Unpin, what: &'static str, name: &'static str) {
    let mut lines = BufReader::new(stream).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                let line = line.trim_end();
                if !line.is_empty() {
                    tracing::info!(ui = what, stream = name, "{line}");
                }
            }
            Ok(None) => break,
            Err(err) => {
                tracing::debug!(ui = what, stream = name, "stream read failed: {err}");
                break;
            }
        }
    }
}

/// Poll the UI port and log the final URL once it accepts TCP connections.
/// Started with the first `next start` and survives restarts; gives up
/// logging after [`PORT_POLL_GIVE_UP`] without touching the server.
async fn announce_when_up(
    host: String,
    port: u16,
    base_url: String,
    mut stop: watch::Receiver<bool>,
) {
    let deadline = Instant::now() + PORT_POLL_GIVE_UP;
    loop {
        if tokio::net::TcpStream::connect((host.as_str(), port))
            .await
            .is_ok()
        {
            tracing::info!(url = %base_url, "UI server is up");
            return;
        }
        if Instant::now() >= deadline {
            tracing::warn!(
                url = %base_url,
                waited_secs = PORT_POLL_GIVE_UP.as_secs(),
                "UI server has not accepted connections yet; will keep \
                 supervising but stop polling"
            );
            return;
        }
        tokio::select! {
            _ = tokio::time::sleep(PORT_POLL_INTERVAL) => {}
            _ = stopped(&mut stop) => return,
        }
    }
}

/// `npm install` is needed when `node_modules` is missing or `package.json`
/// is newer (mtime) than the last install. The reference for "last install"
/// is the [`INSTALL_STAMP`] when present (written after every successful
/// gateway-run install), falling back to `node_modules`' own mtime for
/// checkouts installed by other means.
fn needs_install(dir: &Path) -> bool {
    let Ok(node_modules) = std::fs::metadata(dir.join("node_modules")) else {
        return true;
    };
    let reference = std::fs::metadata(dir.join(INSTALL_STAMP))
        .unwrap_or(node_modules)
        .modified();
    let package_json = std::fs::metadata(dir.join("package.json")).and_then(|m| m.modified());
    match (package_json, reference) {
        (Ok(package_json), Ok(reference)) => package_json > reference,
        // Unreadable mtimes: assume the install is current rather than
        // reinstalling on every restart.
        _ => false,
    }
}

#[derive(Debug, PartialEq, Eq)]
enum BuildDecision {
    Build,
    Skip,
    /// `build = "never"` with no existing production build: retrying cannot
    /// create one, so the supervisor must stop with a clear error.
    MissingBuildForbidden,
}

async fn build_decision(dir: &Path, policy: UiBuildPolicy) -> BuildDecision {
    let build_id_exists = dir.join(".next").join("BUILD_ID").is_file();
    match policy {
        UiBuildPolicy::Always => BuildDecision::Build,
        UiBuildPolicy::Never if build_id_exists => BuildDecision::Skip,
        UiBuildPolicy::Never => BuildDecision::MissingBuildForbidden,
        UiBuildPolicy::Auto => {
            let stamped = std::fs::read_to_string(dir.join(BUILT_COMMIT_STAMP))
                .ok()
                .map(|hash| hash.trim().to_owned());
            let head = head_commit(dir).await;
            if needs_build_auto(build_id_exists, head.as_deref(), stamped.as_deref()) {
                BuildDecision::Build
            } else {
                BuildDecision::Skip
            }
        }
    }
}

/// Auto policy: build when there is no previous build, or when HEAD differs
/// from the commit stamped by the last gateway-run build (a missing stamp —
/// external build, older gateway — counts as differing). An unknown HEAD
/// (git missing, not a repo) only builds when BUILD_ID is missing.
fn needs_build_auto(build_id_exists: bool, head: Option<&str>, stamped: Option<&str>) -> bool {
    if !build_id_exists {
        return true;
    }
    match head {
        None => false,
        Some(head) => stamped != Some(head),
    }
}

/// Record HEAD alongside a fresh build so the auto policy can detect
/// checkout changes exactly. Best-effort: with git unavailable the stamp is
/// cleared instead, and auto degrades to rebuild-only-when-BUILD_ID-missing.
async fn stamp_built_commit(dir: &Path) {
    let stamp = dir.join(BUILT_COMMIT_STAMP);
    match head_commit(dir).await {
        Some(head) => {
            if let Err(err) = std::fs::write(&stamp, head) {
                tracing::warn!(path = %stamp.display(), "failed to write UI build stamp: {err}");
            }
        }
        None => {
            let _ = std::fs::remove_file(&stamp);
        }
    }
}

/// `git -C <dir> log -1 --format=%H`; any failure is "unknown".
async fn head_commit(dir: &Path) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["log", "-1", "--format=%H"])
        .output()
        .await;
    let output = match output {
        Ok(output) if output.status.success() => output,
        Ok(output) => {
            tracing::warn!(
                dir = %dir.display(),
                status = %output.status,
                stderr = %String::from_utf8_lossy(&output.stderr).trim(),
                "git log failed; treating the UI checkout's HEAD as unknown"
            );
            return None;
        }
        Err(err) => {
            tracing::warn!(
                dir = %dir.display(),
                "failed to run git ({err}); treating the UI checkout's HEAD as unknown"
            );
            return None;
        }
    };
    let head = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    (!head.is_empty()).then_some(head)
}

/// Best-effort absolutization against the CWD (matches config.rs). Every
/// resolved node/npm-cli path must be absolute: the children run with
/// `current_dir` set to the UI checkout, which would silently re-anchor a
/// relative program or script-argument path there.
fn absolutize(path: PathBuf) -> PathBuf {
    std::path::absolute(&path).unwrap_or(path)
}

/// Config → runnable venv nodejs-wheel → PATH. Skips stub-ld venv ELFs.
fn resolve_node(explicit: Option<&Path>, base: &Path) -> PathBuf {
    if let Some(node) = explicit {
        return if node.parent().is_some_and(|dir| !dir.as_os_str().is_empty()) {
            absolutize(node.to_path_buf())
        } else {
            node.to_path_buf()
        };
    }
    venv_node_candidates(base)
        .into_iter()
        .find(|candidate| {
            candidate.is_file() && crate::host_paths::can_spawn(candidate, &["-v"])
        })
        .map(absolutize)
        .unwrap_or_else(|| PathBuf::from("node"))
}

fn venv_node_candidates(base: &Path) -> Vec<PathBuf> {
    let venvs = match crate::resources::py_source_mode() {
        crate::resources::PySourceMode::Dev => {
            vec![base.join("python/.venv"), base.join(".venv")]
        }
        crate::resources::PySourceMode::Extracted => vec![base.join("runtime/venv")],
    };
    let mut candidates = Vec::new();
    for venv in &venvs {
        if cfg!(windows) {
            candidates.push(venv.join("Lib/site-packages/nodejs_wheel/node.exe"));
            candidates.push(venv.join("Scripts/node.exe"));
        } else {
            // The site-packages path embeds the Python version
            // (lib/pythonX.Y/...), so scan for it.
            if let Ok(entries) = std::fs::read_dir(venv.join("lib")) {
                for entry in entries.flatten() {
                    candidates.push(entry.path().join("site-packages/nodejs_wheel/bin/node"));
                }
            }
            candidates.push(venv.join("bin/node"));
        }
    }
    candidates
}

/// How to invoke npm. `npm` on PATH is a `.cmd` shim on Windows and a shell
/// script elsewhere; running `npm-cli.js` under the already-resolved node is
/// the one spelling that works everywhere, so it is preferred whenever the
/// script can be found near the node binary.
enum NpmInvocation {
    NodeCli { node: PathBuf, cli: PathBuf },
    PathNpm(&'static str),
}

impl NpmInvocation {
    fn command(&self) -> Command {
        match self {
            NpmInvocation::NodeCli { node, cli } => {
                let mut command = Command::new(node);
                command.arg(cli);
                command
            }
            NpmInvocation::PathNpm(name) => Command::new(name),
        }
    }
}

impl std::fmt::Display for NpmInvocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NpmInvocation::NodeCli { node, cli } => {
                write!(f, "{} {}", node.display(), cli.display())
            }
            NpmInvocation::PathNpm(name) => f.write_str(name),
        }
    }
}

/// Look for `npm-cli.js` in the layouts that ship npm next to node,
/// relative to the node binary's directory:
/// - `node_modules/npm/bin/npm-cli.js` — standard Windows node install
/// - `lib/node_modules/npm/bin/npm-cli.js` — nodejs_wheel on Windows
///   (node.exe at the package root, npm under `lib/`)
/// - `../lib/node_modules/npm/bin/npm-cli.js` — standard Unix prefix and
///   nodejs_wheel on Unix (node under `bin/`)
///
/// Falls back to npm on PATH (`npm.cmd` on Windows: bare `npm` there is a
/// cmd shim that cannot be exec'd without a shell).
fn resolve_npm(node: &Path) -> NpmInvocation {
    const NPM_CLI: &str = "npm/bin/npm-cli.js";
    if let Some(node_dir) = node.parent().filter(|dir| !dir.as_os_str().is_empty()) {
        for candidate in [
            node_dir.join("node_modules").join(NPM_CLI),
            node_dir.join("lib/node_modules").join(NPM_CLI),
            node_dir.join("../lib/node_modules").join(NPM_CLI),
        ] {
            if candidate.is_file() {
                return NpmInvocation::NodeCli {
                    node: node.to_path_buf(),
                    // Absolute: node resolves a relative script argument
                    // against its cwd, which is the UI checkout.
                    cli: absolutize(candidate),
                };
            }
        }
    }
    NpmInvocation::PathNpm(if cfg!(windows) { "npm.cmd" } else { "npm" })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    /// The auto build-staleness decision: no BUILD_ID always builds; with
    /// one, any HEAD/stamp mismatch (including a missing stamp) builds; an
    /// unknown HEAD never rebuilds an existing build.
    #[test]
    fn needs_build_auto_decision() {
        assert!(needs_build_auto(false, None, None));
        assert!(needs_build_auto(false, Some("a"), Some("a")));
        assert!(needs_build_auto(true, Some("a"), None), "stamp missing");
        assert!(needs_build_auto(true, Some("a"), Some("b")), "HEAD moved");
        assert!(!needs_build_auto(true, Some("a"), Some("a")), "up to date");
        assert!(!needs_build_auto(true, None, None), "HEAD unknown");
        assert!(
            !needs_build_auto(true, None, Some("b")),
            "HEAD unknown ignores a stale stamp"
        );
    }

    /// Build policy wrapper: always builds unconditionally; never skips an
    /// existing build but is a fatal misconfiguration without one; auto
    /// consults the checkout (a tempdir is not a git repo, so HEAD is
    /// unknown: build only when .next/BUILD_ID is missing).
    #[tokio::test]
    async fn build_decision_policies() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            build_decision(dir.path(), UiBuildPolicy::Always).await,
            BuildDecision::Build
        );
        assert_eq!(
            build_decision(dir.path(), UiBuildPolicy::Never).await,
            BuildDecision::MissingBuildForbidden
        );
        assert_eq!(
            build_decision(dir.path(), UiBuildPolicy::Auto).await,
            BuildDecision::Build
        );

        std::fs::create_dir(dir.path().join(".next")).unwrap();
        std::fs::write(dir.path().join(".next/BUILD_ID"), "abc").unwrap();
        assert_eq!(
            build_decision(dir.path(), UiBuildPolicy::Never).await,
            BuildDecision::Skip
        );
        assert_eq!(
            build_decision(dir.path(), UiBuildPolicy::Auto).await,
            BuildDecision::Skip
        );
        assert_eq!(
            build_decision(dir.path(), UiBuildPolicy::Always).await,
            BuildDecision::Build
        );
    }

    /// npm install is needed when node_modules is missing or package.json
    /// is newer than the last install; the install stamp, when present,
    /// beats the node_modules dir mtime as the "last install" reference.
    #[test]
    fn needs_install_checks_node_modules_staleness() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("package.json"), "{}").unwrap();
        assert!(needs_install(dir.path()), "node_modules missing");

        std::fs::create_dir(dir.path().join("node_modules")).unwrap();
        assert!(
            !needs_install(dir.path()),
            "node_modules at least as new as package.json"
        );

        // Bump package.json's mtime past node_modules'.
        let package_json = std::fs::File::options()
            .write(true)
            .open(dir.path().join("package.json"))
            .unwrap();
        package_json
            .set_modified(SystemTime::now() + Duration::from_secs(60))
            .unwrap();
        assert!(needs_install(dir.path()), "package.json newer");

        // An install stamp newer than package.json overrides the dir mtime.
        std::fs::write(dir.path().join(INSTALL_STAMP), b"").unwrap();
        let stamp = std::fs::File::options()
            .write(true)
            .open(dir.path().join(INSTALL_STAMP))
            .unwrap();
        stamp
            .set_modified(SystemTime::now() + Duration::from_secs(120))
            .unwrap();
        assert!(!needs_install(dir.path()), "stamp newer than package.json");

        package_json
            .set_modified(SystemTime::now() + Duration::from_secs(180))
            .unwrap();
        assert!(needs_install(dir.path()), "package.json newer than stamp");
    }

    #[test]
    fn resolve_node_order() {
        let base = tempfile::tempdir().unwrap();
        assert_eq!(resolve_node(None, base.path()), PathBuf::from("node"));

        let venv_node = venv_node_candidates(base.path())
            .last()
            .cloned()
            .expect("candidate list is never empty");
        std::fs::create_dir_all(venv_node.parent().unwrap()).unwrap();
        std::fs::write(&venv_node, "").unwrap();
        assert_eq!(resolve_node(None, base.path()), PathBuf::from("node"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::write(&venv_node, "#!/bin/sh\necho v0.0.0-test\n").unwrap();
            let mut perms = std::fs::metadata(&venv_node).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&venv_node, perms).unwrap();
            assert_eq!(resolve_node(None, base.path()), venv_node);
        }

        let explicit = resolve_node(Some(Path::new("custom/node")), base.path());
        assert!(explicit.is_absolute(), "{explicit:?}");
        assert!(explicit.ends_with("custom/node"), "{explicit:?}");
        assert_eq!(
            resolve_node(Some(Path::new("node18")), base.path()),
            PathBuf::from("node18")
        );

        #[cfg(windows)]
        {
            let candidates = venv_node_candidates(base.path());
            if candidates.len() > 1 {
                let preferred = &candidates[0];
                std::fs::create_dir_all(preferred.parent().unwrap()).unwrap();
                std::fs::write(preferred, "").unwrap();
                assert_eq!(resolve_node(None, base.path()), PathBuf::from("node"));
            }
        }
    }

    /// npm resolution: npm-cli.js found near the node binary is run under
    /// that node; otherwise fall back to npm on PATH.
    #[test]
    fn resolve_npm_prefers_npm_cli_near_node() {
        let dir = tempfile::tempdir().unwrap();
        let node = dir.path().join("node.exe");
        std::fs::write(&node, "").unwrap();

        match resolve_npm(&node) {
            NpmInvocation::PathNpm(name) => {
                assert_eq!(name, if cfg!(windows) { "npm.cmd" } else { "npm" });
            }
            NpmInvocation::NodeCli { .. } => panic!("no npm-cli.js exists yet"),
        }

        // nodejs_wheel Windows layout: lib/node_modules next to node.
        let cli = dir.path().join("lib/node_modules/npm/bin/npm-cli.js");
        std::fs::create_dir_all(cli.parent().unwrap()).unwrap();
        std::fs::write(&cli, "").unwrap();
        match resolve_npm(&node) {
            NpmInvocation::NodeCli {
                node: used_node,
                cli: used_cli,
            } => {
                assert_eq!(used_node, node);
                assert_eq!(used_cli, cli);
            }
            NpmInvocation::PathNpm(_) => panic!("npm-cli.js should be found"),
        }

        // Standard Windows layout wins when it exists too (checked first).
        let sibling = dir.path().join("node_modules/npm/bin/npm-cli.js");
        std::fs::create_dir_all(sibling.parent().unwrap()).unwrap();
        std::fs::write(&sibling, "").unwrap();
        match resolve_npm(&node) {
            NpmInvocation::NodeCli { cli: used_cli, .. } => assert_eq!(used_cli, sibling),
            NpmInvocation::PathNpm(_) => panic!("npm-cli.js should be found"),
        }
    }
}
