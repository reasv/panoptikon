//! Handing URLs and paths to the host desktop environment.
//!
//! Everywhere except Linux this is `tauri-plugin-opener` unchanged. On Linux
//! the plugin cannot be used, for two reasons that only show up in the
//! AppImage:
//!
//! * The launcher it spawns inherits the AppImage environment and dies. That
//!   part is [`crate::host_env`]'s problem, and the launchers below are built
//!   through it.
//! * `open::that_detached` reports success as soon as the launcher *forks*, so
//!   a launcher that then exits non-zero is silently swallowed. Every "Open in
//!   browser" affordance appeared to do nothing at all, with no log line. So we
//!   run the launcher chain ourselves and watch the child long enough to notice
//!   an immediate failure and move on to the next launcher.

use tauri::AppHandle;
#[cfg(not(target_os = "linux"))]
use tauri_plugin_opener::OpenerExt as _;

/// Open a URL in the host's default browser.
pub(crate) fn open_url(app: &AppHandle, url: &str) -> anyhow::Result<()> {
    #[cfg(target_os = "linux")]
    {
        let _ = app;
        imp::launch(url)
    }
    #[cfg(not(target_os = "linux"))]
    {
        app.opener().open_url(url, None::<&str>)?;
        Ok(())
    }
}

/// Open a path with the host's default application for it.
pub(crate) fn open_path(app: &AppHandle, path: &std::path::Path) -> anyhow::Result<()> {
    #[cfg(target_os = "linux")]
    {
        let _ = app;
        // The plugin stats the path first so that a missing file is an error
        // rather than a launcher no-op; keep that.
        let _ = path.metadata()?;
        imp::launch(&path.to_string_lossy())
    }
    #[cfg(not(target_os = "linux"))]
    {
        app.opener()
            .open_path(path.display().to_string(), None::<&str>)?;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
mod imp {
    use crate::host_env;
    use std::{
        io::Read as _,
        process::{Child, Command, Stdio},
        time::{Duration, Instant},
    };

    /// How long a launcher gets to fail. `xdg-open` execs the handler and
    /// returns promptly, so a child still alive after this is a handler that
    /// stayed in the foreground — a success, not a hang.
    const PROBE: Duration = Duration::from_millis(400);

    /// `xdg-open` first, then the same fallbacks the `open` crate uses.
    fn launchers(target: &str) -> Vec<Command> {
        // A target that starts with `-` would be read as an option by the
        // launchers that have no `--` marker.
        let safe = if target.starts_with('-') {
            format!("./{target}")
        } else {
            target.to_string()
        };
        let mut xdg = host_env::command("xdg-open");
        xdg.arg(&safe);
        let mut gio = host_env::command("gio");
        gio.args(["open", &safe]);
        let mut gnome = host_env::command("gnome-open");
        gnome.arg(&safe);
        let mut kde = host_env::command("kde-open");
        kde.args(["--", target]);
        vec![xdg, gio, gnome, kde]
    }

    pub(super) fn launch(target: &str) -> anyhow::Result<()> {
        let mut failures = Vec::new();
        for mut command in launchers(target) {
            let program = command.get_program().to_string_lossy().into_owned();
            command
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::piped());
            let child = match command.spawn() {
                Ok(child) => child,
                Err(error) => {
                    failures.push(format!("{program}: {error}"));
                    continue;
                }
            };
            match probe(child) {
                Ok(()) => {
                    tracing::debug!(launcher = %program, "opened in the host desktop");
                    return Ok(());
                }
                Err(detail) => failures.push(format!("{program}: {detail}")),
            }
        }
        Err(anyhow::anyhow!(
            "no desktop launcher could open {target} ({})",
            failures.join("; ")
        ))
    }

    /// Watch a freshly spawned launcher for [`PROBE`]. An immediate non-zero
    /// exit is the failure we are here to catch; anything still running is
    /// handed to a thread that reaps it and logs whatever it wrote.
    fn probe(mut child: Child) -> Result<(), String> {
        let deadline = Instant::now() + PROBE;
        loop {
            match child.try_wait() {
                Ok(Some(status)) if status.success() => return Ok(()),
                Ok(Some(status)) => {
                    let mut stderr = String::new();
                    if let Some(mut pipe) = child.stderr.take() {
                        let _ = pipe.read_to_string(&mut stderr);
                    }
                    let stderr = stderr.trim();
                    return Err(if stderr.is_empty() {
                        format!("exited with {status}")
                    } else {
                        format!("exited with {status}: {stderr}")
                    });
                }
                Ok(None) => {}
                Err(error) => return Err(error.to_string()),
            }
            if Instant::now() >= deadline {
                detach(child);
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Drain the launcher's stderr and reap it, so that neither a full pipe
    /// buffer nor a zombie outlives the handler.
    fn detach(mut child: Child) {
        std::thread::spawn(move || {
            let mut stderr = String::new();
            if let Some(mut pipe) = child.stderr.take() {
                let _ = pipe.read_to_string(&mut stderr);
            }
            match child.wait() {
                Ok(status) if !status.success() => {
                    tracing::warn!(%status, stderr = %stderr.trim(), "desktop launcher failed");
                }
                Ok(_) => {}
                Err(error) => tracing::debug!(%error, "could not reap the desktop launcher"),
            }
        });
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn launchers_do_not_let_a_target_look_like_an_option() {
            let commands = launchers("--load-modules=/tmp/evil.so");
            let args: Vec<_> = commands[0].get_args().collect();
            assert_eq!(args, ["./--load-modules=/tmp/evil.so"]);
        }
    }
}
