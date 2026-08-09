//! Linux and the BSDs: a file selection handed to an external clipboard tool —
//! `wl-copy` under Wayland, `xclip` under X11 — mirroring the external-tool
//! probe chain the open/reveal verbs already use.
//!
//! Each tool advertises exactly one MIME type per invocation, so the flavour is
//! chosen from `XDG_CURRENT_DESKTOP`: GNOME-family desktops get
//! `x-special/gnome-copied-files`, everything else `text/uri-list` (see
//! `payload.rs`). Desktops that want some third flavour are served by the
//! custom clipboard command escape hatch.
//!
//! Selection ownership must be held by a live process, so the tool is spawned
//! and left running; it is reaped in the background once the selection is lost.

use std::env;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow, bail};

use crate::payload::{Flavour, flavour_for_desktop};
use crate::uri::file_uri;

const HINT: &str = "Install wl-clipboard (wl-copy) or xclip, or configure a custom clipboard command in Panoptikon's settings.";

/// How long a tool is given to fail before the write is reported as a success.
///
/// Both tools fork and exit within milliseconds in every healthy case, so a
/// process still alive at the deadline is ambiguous: it may be a slow fork on a
/// loaded machine, or it may be wedged. We resolve the ambiguity optimistically
/// — killing it at the deadline would destroy a selection owner that is very
/// probably live and serving the paste we were asked for.
const SETTLE_TIMEOUT: Duration = Duration::from_millis(400);
const SETTLE_POLL: Duration = Duration::from_millis(20);

#[derive(Clone, Copy)]
struct Tool {
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

pub(crate) fn copy_files(paths: &[&Path]) -> anyhow::Result<()> {
    let tool = select_tool().map_err(|reason| anyhow!("{reason}"))?;

    let flavour = flavour_for_desktop(
        env::var_os("XDG_CURRENT_DESKTOP")
            .as_deref()
            .and_then(std::ffi::OsStr::to_str),
    );
    let uris: Vec<String> = paths.iter().map(|path| file_uri(path)).collect();

    run(tool, flavour, flavour.payload(&uris).as_bytes())
}

pub(crate) fn available() -> Result<(), String> {
    select_tool().map(drop)
}

fn select_tool() -> Result<Tool, String> {
    let wayland = has_display("WAYLAND_DISPLAY");
    let x11 = has_display("DISPLAY");

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
        if which(tool.program).is_some() {
            return Ok(tool);
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

fn which(program: &str) -> Option<PathBuf> {
    if let Some(path) = env::var_os("PATH")
        && let Some(found) = env::split_paths(&path).find_map(|dir| runnable(&dir, program))
    {
        return Some(found);
    }
    // Nix profiles are frequently absent from the PATH a desktop-launched
    // process inherits. Same fallback set as `profile_bin_dirs()` in
    // `panoptikon/src/host_paths.rs`; kept duplicated because that crate
    // depends on this one, so depending back would be circular.
    profile_bin_dirs()
        .iter()
        .find_map(|dir| runnable(dir, program))
}

fn runnable(dir: &Path, program: &str) -> Option<PathBuf> {
    use std::os::unix::fs::PermissionsExt;

    let candidate = dir.join(program);
    let meta = std::fs::metadata(&candidate).ok()?;
    (meta.is_file() && meta.permissions().mode() & 0o111 != 0).then_some(candidate)
}

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

fn run(tool: Tool, flavour: Flavour, payload: &[u8]) -> anyhow::Result<()> {
    let mut child = Command::new(tool.program)
        .args(tool.args_for(flavour.mime()))
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("Could not run {}. {HINT}", tool.program))?;

    let Some(mut stdin) = child.stdin.take() else {
        let _ = child.kill();
        let _ = child.wait();
        bail!("Could not write to {}.", tool.program);
    };
    let write = stdin.write_all(payload).and_then(|()| stdin.flush());
    drop(stdin);
    if let Err(cause) = write {
        // A write failure means the tool is already gone; its own diagnostic
        // is the only thing that explains why, so carry it into the message.
        let _ = child.kill();
        let _ = child.wait();
        let detail = drain_stderr(&mut child);
        let mut message = format!(
            "Could not hand the file list to {} ({cause}).",
            tool.program
        );
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
                let detail = drain_stderr(&mut child);
                if detail.is_empty() {
                    bail!("{} failed to set the clipboard ({status}).", tool.program);
                }
                bail!("{} failed to set the clipboard: {detail}", tool.program);
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    // Nobody will drain stderr from here on: closing our read
                    // end turns a full-pipe block into EPIPE, so a chatty or
                    // stuck tool cannot wedge the reaper's `wait()` forever.
                    drop(child.stderr.take());
                    thread::spawn(move || {
                        let _ = child.wait();
                    });
                    return Ok(());
                }
                thread::sleep(SETTLE_POLL);
            }
            Err(err) => bail!("Could not wait for {}: {err}", tool.program),
        }
    }
}

/// Reads whatever the (already exited) child wrote to stderr, trimmed.
fn drain_stderr(child: &mut Child) -> String {
    let mut buffer = String::new();
    if let Some(mut pipe) = child.stderr.take() {
        let _ = pipe.read_to_string(&mut buffer);
    }
    buffer.trim().to_owned()
}
