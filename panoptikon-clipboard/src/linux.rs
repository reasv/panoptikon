//! Linux and the BSDs: a file selection handed to an external clipboard tool —
//! `wl-copy` under Wayland, `xclip` under X11 — mirroring the external-tool
//! probe chain the open/reveal verbs already use.
//!
//! Each tool advertises exactly one MIME type per invocation, and Panoptikon
//! always picks `text/uri-list` (see `payload.rs`). Chat clients, browsers,
//! web upload forms and non-GNOME file managers all read that type; GNOME's
//! file-manager family (Nautilus, Nemo, Caja) reads only
//! `x-special/gnome-copied-files` and will therefore paste nothing from this
//! clipboard. Serving both would need an in-process selection owner offering
//! two targets at once; the custom clipboard command is the escape hatch
//! until then.
//!
//! Selection ownership must be held by a live process, so the tool is spawned
//! and left running; it is reaped in the background once the selection is lost.

use std::env;
use std::ffi::OsStr;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{ChildStderr, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail};

use crate::payload::{URI_LIST_MIME, uri_list_payload};
use crate::uri::file_uri;

const HINT: &str = "Install wl-clipboard (wl-copy) or xclip, or set a custom clipboard command in Panoptikon's settings. Panoptikon publishes text/uri-list, which chat clients, browsers and most file managers accept — GNOME's Nautilus, Nemo and Caja do not.";

/// How long a tool is given to fail before the write is reported as a success.
///
/// Both tools fork and exit within milliseconds in every healthy case, so a
/// process still alive at the deadline is ambiguous: it may be a slow fork on a
/// loaded machine, or it may be wedged. We resolve the ambiguity optimistically
/// — killing it at the deadline would destroy a selection owner that is very
/// probably live and serving the paste we were asked for.
const SETTLE_TIMEOUT: Duration = Duration::from_millis(400);
const SETTLE_POLL: Duration = Duration::from_millis(20);

/// Ceiling on the stderr text kept for a diagnostic, and how long a failing
/// call waits for the drain thread to hand it over.
const STDERR_CAP: usize = 8 * 1024;
const STDERR_DEADLINE: Duration = Duration::from_millis(200);
/// Cap on the tool text spliced into a message: these render verbatim in
/// browser toasts and HTTP 500 bodies.
const DETAIL_MAX_CHARS: usize = 200;

#[derive(Clone, Copy, Debug)]
struct Tool {
    /// Display name, used in messages; never the thing that gets spawned.
    program: &'static str,
    /// Args that precede the MIME type; the last one is the flag taking it.
    args: &'static [&'static str],
}

const WL_COPY: Tool = Tool {
    program: "wl-copy",
    args: &["--type"],
};
const XCLIP: Tool = Tool {
    program: "xclip",
    args: &["-selection", "clipboard", "-t"],
};

impl Tool {
    fn args_for(self, mime: &str) -> Vec<&str> {
        let mut args: Vec<&str> = self.args.to_vec();
        args.push(mime);
        args
    }
}

/// A chosen tool together with the absolute path that will be executed.
///
/// The path must travel with the choice: spawning the bare `program` name
/// sends the lookup back through `execvp`, which searches `PATH` only — and
/// the profile fallback in `search_dirs` exists precisely for hosts where the
/// tool is installed somewhere `PATH` never mentions.
#[derive(Debug)]
struct Selected {
    tool: Tool,
    path: PathBuf,
}

pub(crate) fn copy_files(paths: &[&Path]) -> anyhow::Result<()> {
    let selected = select_tool().map_err(|reason| anyhow!("{reason}"))?;
    let uris: Vec<String> = paths.iter().map(|path| file_uri(path)).collect();

    run(&selected, uri_list_payload(&uris).as_bytes())
}

pub(crate) fn available() -> Result<(), String> {
    select_tool().map(drop)
}

fn select_tool() -> Result<Selected, String> {
    select_from(
        has_display("WAYLAND_DISPLAY"),
        has_display("DISPLAY"),
        &search_dirs(),
    )
}

fn select_from(wayland: bool, x11: bool, dirs: &[PathBuf]) -> Result<Selected, String> {
    if !wayland && !x11 {
        return Err(
            "No graphical session was detected (neither WAYLAND_DISPLAY nor DISPLAY is set), \
             so there is no clipboard to copy to."
                .to_owned(),
        );
    }

    let mut missing = Vec::new();
    for (enabled, tool) in [(wayland, WL_COPY), (x11, XCLIP)] {
        if !enabled {
            continue;
        }
        if let Some(path) = which_in(dirs, tool.program) {
            return Ok(Selected { tool, path });
        }
        missing.push(tool.program);
    }

    Err(format!(
        "Cannot copy files to the clipboard: {} not found on PATH. {HINT}",
        missing.join(", ")
    ))
}

fn has_display(var: &str) -> bool {
    env::var_os(var).is_some_and(|value| !value.is_empty())
}

/// `PATH` entries first, then the profile directories a desktop-launched
/// process frequently does not inherit.
fn search_dirs() -> Vec<PathBuf> {
    let path = env::var_os("PATH");
    let mut dirs = path_dirs(path.as_deref());
    dirs.extend(profile_bin_dirs());
    dirs
}

/// Splits a `PATH` value, dropping empty components: `split_paths` yields one
/// for `"a::b"`, and joining a program onto it would probe the process's
/// current directory.
fn path_dirs(path: Option<&OsStr>) -> Vec<PathBuf> {
    let Some(path) = path else {
        return Vec::new();
    };
    env::split_paths(path)
        .filter(|dir| !dir.as_os_str().is_empty())
        .collect()
}

fn which_in(dirs: &[PathBuf], program: &str) -> Option<PathBuf> {
    dirs.iter().find_map(|dir| runnable(dir, program))
}

fn runnable(dir: &Path, program: &str) -> Option<PathBuf> {
    use std::os::unix::fs::PermissionsExt;

    let candidate = dir.join(program);
    let meta = std::fs::metadata(&candidate).ok()?;
    (meta.is_file() && meta.permissions().mode() & 0o111 != 0).then_some(candidate)
}

/// Nix profiles are frequently absent from the PATH a desktop-launched process
/// inherits. Same fallback set as `profile_bin_dirs()` in
/// `panoptikon/src/host_paths.rs`; kept duplicated because that crate depends
/// on this one, so depending back would be circular.
fn profile_bin_dirs() -> Vec<PathBuf> {
    let mut dirs = vec![
        PathBuf::from("/run/current-system/sw/bin"),
        PathBuf::from("/nix/var/nix/profiles/default/bin"),
    ];
    if let Some(home) = env::var_os("HOME") {
        let home = PathBuf::from(home);
        dirs.push(home.join(".nix-profile/bin"));
        dirs.push(home.join(".local/state/nix/profile/bin"));
    }
    if let Ok(user) = env::var("USER") {
        dirs.push(PathBuf::from(format!("/etc/profiles/per-user/{user}/bin")));
    }
    dirs
}

fn run(selected: &Selected, payload: &[u8]) -> anyhow::Result<()> {
    let program = selected.tool.program;
    let mut child = match Command::new(&selected.path)
        .args(selected.tool.args_for(URI_LIST_MIME))
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(err) => bail!("Could not run {program} ({err}). {HINT}"),
    };

    let stderr = StderrDrain::spawn(child.stderr.take());

    let Some(mut stdin) = child.stdin.take() else {
        let _ = child.kill();
        let _ = child.wait();
        bail!("Could not write to {program}.");
    };
    let write = stdin.write_all(payload).and_then(|()| stdin.flush());
    drop(stdin);
    if let Err(cause) = write {
        // A write failure means the tool is already gone; its own diagnostic
        // is the only thing that explains why, so carry it into the message.
        let _ = child.kill();
        let _ = child.wait();
        let detail = stderr.collect();
        let mut message = format!("Could not hand the file list to {program} ({cause}).");
        if !detail.is_empty() {
            message.push(' ');
            message.push_str(&detail);
        }
        bail!("{message}");
    }

    let deadline = Instant::now() + SETTLE_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if status.success() {
                    return Ok(());
                }
                let detail = stderr.collect();
                if detail.is_empty() {
                    bail!("{program} failed to set the clipboard ({status}).");
                }
                bail!("{program} failed to set the clipboard: {detail}");
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    thread::spawn(move || {
                        let _ = child.wait();
                    });
                    return Ok(());
                }
                thread::sleep(SETTLE_POLL);
            }
            Err(err) => {
                // The child's state is unknown, so neither block on it here nor
                // kill what may well be a live selection owner: hand it to the
                // same background reaper the settle path uses, so the failure
                // does not leave a zombie behind for the process's lifetime.
                thread::spawn(move || {
                    let _ = child.wait();
                });
                bail!("Could not wait for {program}: {err}");
            }
        }
    }
}

/// Reads a child's stderr to EOF on a thread of its own, into a bounded buffer.
///
/// Both tools daemonize, and the surviving process inherits our pipe's write
/// end, so EOF may never arrive: reading on the calling thread (a blocking
/// worker serving an HTTP request) could block forever. Closing our read end
/// instead is worse — Rust restores `SIGPIPE` to its default in children, so
/// the next diagnostic the selection owner writes would kill it and silently
/// empty the clipboard we just reported as set.
struct StderrDrain {
    collected: Arc<Mutex<Vec<u8>>>,
    finished: Receiver<()>,
}

impl StderrDrain {
    fn spawn(stderr: Option<ChildStderr>) -> Self {
        let collected = Arc::new(Mutex::new(Vec::new()));
        let (tx, finished) = mpsc::channel();
        if let Some(mut pipe) = stderr {
            let sink = Arc::clone(&collected);
            thread::spawn(move || {
                let mut chunk = [0u8; 1024];
                loop {
                    match pipe.read(&mut chunk) {
                        Ok(0) | Err(_) => break,
                        Ok(read) => {
                            if let Ok(mut sink) = sink.lock() {
                                let room = STDERR_CAP.saturating_sub(sink.len());
                                sink.extend_from_slice(&chunk[..read.min(room)]);
                            }
                        }
                    }
                }
                let _ = tx.send(());
            });
        }
        Self {
            collected,
            finished,
        }
    }

    /// What the tool wrote, condensed for a user-facing message.
    ///
    /// Waits briefly for EOF so a tool that has just exited gets its last
    /// words in, then reports whatever arrived — a tool that daemonized may
    /// never close the pipe, and what it wrote before that is the diagnostic.
    fn collect(&self) -> String {
        let _ = self.finished.recv_timeout(STDERR_DEADLINE);
        match self.collected.lock() {
            Ok(sink) => condense(&String::from_utf8_lossy(&sink)),
            Err(_) => String::new(),
        }
    }
}

/// Collapses a tool's stderr onto one line and caps its length.
fn condense(detail: &str) -> String {
    let mut out = String::new();
    for word in detail.split_whitespace() {
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(word);
    }
    if out.chars().count() > DETAIL_MAX_CHARS {
        let cut = out
            .char_indices()
            .nth(DETAIL_MAX_CHARS - 1)
            .map_or(out.len(), |(index, _)| index);
        out.truncate(cut);
        out.push('…');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    fn install(dir: &Path, name: &str, script: &str) -> PathBuf {
        let path = dir.join(name);
        fs::write(&path, script).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    fn dirs_of(paths: &[&Path]) -> Vec<PathBuf> {
        paths.iter().map(|path| path.to_path_buf()).collect()
    }

    #[test]
    fn no_display_reports_a_headless_session() {
        let err = select_from(false, false, &[]).unwrap_err();
        assert!(err.contains("No graphical session"), "{err}");
    }

    #[test]
    fn a_missing_tool_names_it_and_the_escape_hatch() {
        let err = select_from(true, false, &[]).unwrap_err();
        assert!(err.contains("wl-copy"), "{err}");
        assert!(err.contains("custom clipboard command"), "{err}");
    }

    #[test]
    fn selection_carries_the_absolute_resolved_path_not_the_bare_name() {
        let dir = tempfile::tempdir().unwrap();
        let installed = install(dir.path(), "wl-copy", "#!/bin/sh\nexit 0\n");

        let selected = select_from(true, false, &dirs_of(&[dir.path()])).unwrap();

        assert_eq!(selected.tool.program, "wl-copy");
        assert!(selected.path.is_absolute(), "{}", selected.path.display());
        assert_eq!(selected.path, installed);
    }

    /// The regression test for a resolved path that never reached the spawn:
    /// the fake tool lives in a directory that is *not* on this process's
    /// `PATH`, so a bare-name spawn could not have found it.
    #[test]
    fn run_spawns_exactly_what_selection_resolved() {
        let dir = tempfile::tempdir().unwrap();
        install(
            dir.path(),
            "wl-copy",
            "#!/bin/sh\n{ printf '%s\\n' \"$0\" \"$@\"; cat; } > \"$(dirname \"$0\")/record\"\n",
        );

        let selected = select_from(true, false, &dirs_of(&[dir.path()])).unwrap();
        run(&selected, b"file:///tmp/x.bin\r\n").unwrap();

        let record = fs::read_to_string(dir.path().join("record")).unwrap();
        assert_eq!(
            record,
            format!(
                "{}\n--type\ntext/uri-list\nfile:///tmp/x.bin\r\n",
                dir.path().join("wl-copy").display()
            )
        );
    }

    #[test]
    fn a_failing_tool_reports_its_own_diagnostic() {
        let dir = tempfile::tempdir().unwrap();
        install(
            dir.path(),
            "wl-copy",
            "#!/bin/sh\ncat > /dev/null\necho 'no wayland compositor' >&2\nexit 1\n",
        );

        let selected = select_from(true, false, &dirs_of(&[dir.path()])).unwrap();
        let err = run(&selected, b"file:///tmp/x.bin\r\n")
            .unwrap_err()
            .to_string();

        assert!(err.contains("no wayland compositor"), "{err}");
    }

    /// The regression test for a blocking `read_to_string` on the child's
    /// stderr: this fake leaves a background process holding the write end, so
    /// EOF never arrives and reading to EOF would hang the calling thread —
    /// which, in the server, is a worker serving an HTTP request.
    #[test]
    fn a_failing_tool_that_left_a_child_behind_still_answers_promptly() {
        let dir = tempfile::tempdir().unwrap();
        install(
            dir.path(),
            "wl-copy",
            "#!/bin/sh\ncat > /dev/null\nsleep 30 &\necho 'compositor went away' >&2\nexit 1\n",
        );

        let selected = select_from(true, false, &dirs_of(&[dir.path()])).unwrap();
        let started = Instant::now();
        let err = run(&selected, b"file:///tmp/x.bin\r\n")
            .unwrap_err()
            .to_string();

        assert!(err.contains("compositor went away"), "{err}");
        assert!(started.elapsed() < Duration::from_secs(5), "{err}");
    }

    #[test]
    fn which_in_finds_a_tool_in_a_path_directory() {
        let dir = tempfile::tempdir().unwrap();
        let installed = install(dir.path(), "xclip", "#!/bin/sh\n");
        let dirs = path_dirs(Some(dir.path().as_os_str()));

        assert_eq!(which_in(&dirs, "xclip"), Some(installed));
    }

    #[test]
    fn which_in_falls_back_past_path_to_a_profile_directory() {
        // The Nix case: nothing on PATH, the tool only in a profile bin dir.
        let on_path = tempfile::tempdir().unwrap();
        let profile = tempfile::tempdir().unwrap();
        let installed = install(profile.path(), "wl-copy", "#!/bin/sh\n");

        let mut dirs = path_dirs(Some(on_path.path().as_os_str()));
        dirs.push(profile.path().to_path_buf());

        assert_eq!(which_in(&dirs, "wl-copy"), Some(installed));
    }

    #[test]
    fn which_in_ignores_a_non_executable_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wl-copy");
        fs::write(&path, "#!/bin/sh\n").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();

        assert_eq!(which_in(&dirs_of(&[dir.path()]), "wl-copy"), None);
    }

    #[test]
    fn which_in_reports_a_missing_tool() {
        let dir = tempfile::tempdir().unwrap();

        assert_eq!(which_in(&dirs_of(&[dir.path()]), "wl-copy"), None);
    }

    #[test]
    fn path_dirs_skips_empty_components() {
        assert_eq!(
            path_dirs(Some(OsStr::new("/a::/b:"))),
            vec![PathBuf::from("/a"), PathBuf::from("/b")]
        );
        assert!(path_dirs(None).is_empty());
    }

    #[test]
    fn condense_collapses_whitespace() {
        assert_eq!(
            condense("  wl-copy:\n  no  compositor \n"),
            "wl-copy: no compositor"
        );
        assert_eq!(condense("   "), "");
    }

    #[test]
    fn condense_truncates_long_output() {
        let condensed = condense(&"x".repeat(5_000));

        assert_eq!(condensed.chars().count(), DETAIL_MAX_CHARS);
        assert!(condensed.ends_with('…'), "{condensed}");
    }

    #[test]
    fn condense_truncates_on_a_character_boundary() {
        let condensed = condense(&"é".repeat(5_000));

        assert_eq!(condensed.chars().count(), DETAIL_MAX_CHARS);
        assert!(condensed.ends_with('…'), "{condensed}");
    }
}
