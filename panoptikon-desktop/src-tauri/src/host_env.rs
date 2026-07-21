//! The environment child processes inherit.
//!
//! There is exactly one source of the problem this module exists for: the
//! AppImage `AppRun` script. It exports `LD_LIBRARY_PATH`, `XDG_DATA_DIRS`,
//! `GIO_MODULE_DIR`, `PYTHONHOME`, `PATH`, and friends pointing into the
//! transient `/tmp/.mount_*`, and exactly one of our processes ever sees them:
//! Desktop. Everything else — the Server sidecar, its uv and Python children,
//! ffmpeg, the browser, the user's file-action commands — is downstream of a
//! process *we* spawn, and so inherits the poison only because we hand it over.
//!
//! Past fixes chased individual variables at individual downstream sites
//! (`PYTHONHOME` in the venv bootstrap, then in the inferio worker, then in the
//! static-ffmpeg prefetch). That is unbounded work: every new variable and
//! every new spawn site is another recurrence. This module is the boundary
//! instead. Desktop keeps its own poisoned environment — WebKitGTK's
//! subprocesses need the bundled paths, so it cannot be scrubbed process-wide —
//! and hands every child a host environment.
//!
//! Nothing downstream of Desktop needs to know any of this, and the guard test
//! at the bottom of this file keeps new spawn sites from bypassing it.

/// `:`-separated search paths: keep the entries the user set, drop the ones
/// rooted in the mount.
const SEARCH_PATHS: &[&str] = &[
    "PATH",
    "LD_LIBRARY_PATH",
    "XDG_DATA_DIRS",
    "XDG_CONFIG_DIRS",
    "GI_TYPELIB_PATH",
    "GTK_PATH",
    "QT_PLUGIN_PATH",
    "GST_PLUGIN_SYSTEM_PATH",
    "GST_PLUGIN_SYSTEM_PATH_1_0",
    "PERLLIB",
    "PYTHONPATH",
];

/// Single-value variables: drop them outright when they point at the mount.
const SINGLE_VALUES: &[&str] = &[
    "LD_PRELOAD",
    "PYTHONHOME",
    "GIO_MODULE_DIR",
    "GDK_PIXBUF_MODULEDIR",
    "GDK_PIXBUF_MODULE_FILE",
    "GSETTINGS_SCHEMA_DIR",
    "GTK_IM_MODULE_FILE",
    "GTK_DATA_PREFIX",
    "GTK_EXE_PREFIX",
];

/// Removed from every child unconditionally: they describe a mount that only
/// Desktop is entitled to know about, and a child that cannot see them cannot
/// grow a dependency on them.
const MOUNT_MARKERS: &[&str] = &["APPDIR", "APPIMAGE", "ARGV0", "OWD"];

/// A `std::process::Command` that will not hand the AppImage mount to its
/// child. Desktop MUST create every child process through this or through
/// [`child_environment`]; see the guard test in this module.
pub(crate) fn command(program: impl AsRef<std::ffi::OsStr>) -> std::process::Command {
    let mut command = std::process::Command::new(program);
    scrub(&mut command);
    command
}

/// Apply the scrub to a command built elsewhere (a `tokio` or plugin builder
/// has no `std::process::Command` to hand out).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn scrub(command: &mut std::process::Command) {
    for (name, value) in child_overrides() {
        match value {
            Some(value) => command.env(name, value),
            None => command.env_remove(name),
        };
    }
}

/// The full environment a child should get, for spawners that take a map
/// rather than a `Command` (the Tauri sidecar builder). Pair it with the
/// builder's `env_clear`.
///
/// `None` when the current environment needs nothing changed, which is every
/// platform but an AppImage: rebuilding an environment can only lose detail
/// that plain inheritance keeps, so outside the mount we do not touch it at
/// all. Values stay `OsString` for the same reason — an environment is bytes
/// on Unix, and a lossy round-trip through `String` would drop variables the
/// child is entitled to.
pub(crate) fn child_environment() -> Option<Vec<(std::ffi::OsString, std::ffi::OsString)>> {
    let overrides = child_overrides();
    if overrides.is_empty() {
        return None;
    }
    let mut env: std::collections::BTreeMap<std::ffi::OsString, std::ffi::OsString> =
        std::env::vars_os().collect();
    for (name, value) in overrides {
        match value {
            Some(value) => env.insert(name.into(), value.into()),
            None => env.remove(std::ffi::OsStr::new(name)),
        };
    }
    Some(env.into_iter().collect())
}

/// What the current environment needs changed for a child: `Some` to replace,
/// `None` to remove. Empty outside the AppImage, where `APPDIR` is unset.
///
/// Every rule here is scoped by *value*, not by name: a search path keeps the
/// entries the user set and loses only the mount-rooted ones, and a
/// single-value variable is dropped only when it points into the mount. So
/// `LD_LIBRARY_PATH=/opt/mine ./Panoptikon.AppImage`, which `AppRun` rewrites
/// to `$APPDIR/usr/lib:/opt/mine`, reaches the child as `/opt/mine`, and
/// variables the launcher never touched are passed through untouched.
fn child_overrides() -> Vec<(&'static str, Option<String>)> {
    let Some(appdir) = std::env::var_os("APPDIR") else {
        return Vec::new();
    };
    let appdir = std::path::PathBuf::from(appdir);
    let mut overrides = Vec::new();
    for name in SEARCH_PATHS {
        if let Ok(value) = std::env::var(name)
            && let Some(scrubbed) = without_appdir(&value, &appdir)
        {
            overrides.push((*name, scrubbed));
        }
    }
    for name in SINGLE_VALUES {
        if let Ok(value) = std::env::var(name)
            && std::path::Path::new(&value).starts_with(&appdir)
        {
            overrides.push((*name, None));
        }
    }
    for name in MOUNT_MARKERS {
        overrides.push((*name, None));
    }
    overrides
}

/// A `:`-separated search path with its mount-rooted entries dropped: `None`
/// when there are none (leave the variable alone), `Some(None)` when nothing
/// survives (remove it), `Some(Some(v))` otherwise. A poisoned value also sheds
/// its empty segments — they mean "the current directory" to both the loader
/// and Python, and they come from the launcher's blind `:$VAR` concatenation
/// over an unset variable, not from the user.
fn without_appdir(value: &str, appdir: &std::path::Path) -> Option<Option<String>> {
    let (dropped, kept): (Vec<&str>, Vec<&str>) = value
        .split(':')
        .partition(|entry| std::path::Path::new(entry).starts_with(appdir));
    if dropped.is_empty() {
        return None;
    }
    let kept: Vec<&str> = kept.into_iter().filter(|entry| !entry.is_empty()).collect();
    if kept.is_empty() {
        Some(None)
    } else {
        Some(Some(kept.join(":")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn search_paths_keep_host_entries() {
        let appdir = Path::new("/tmp/.mount_abc");
        assert_eq!(
            without_appdir("/tmp/.mount_abc/usr/lib:/usr/lib", appdir),
            Some(Some("/usr/lib".into()))
        );
        assert_eq!(
            without_appdir("/tmp/.mount_abc/usr/lib::", appdir),
            Some(None)
        );
        assert_eq!(without_appdir("/usr/lib:/lib", appdir), None);
    }

    /// Launching the AppImage with a custom search path must still reach the
    /// child: `AppRun` prepends the mount to whatever the user exported, and
    /// only that prefix is ours to remove.
    #[test]
    fn a_custom_launch_environment_survives() {
        let appdir = Path::new("/tmp/.mount_abc");
        assert_eq!(
            without_appdir("/tmp/.mount_abc/usr/lib:/opt/mine:/usr/lib", appdir),
            Some(Some("/opt/mine:/usr/lib".into()))
        );
        // A value the launcher never wrote is not ours to touch.
        assert_eq!(without_appdir("/opt/mine", appdir), None);
    }

    /// The guard: this is the fourth fix for the AppImage environment leaking
    /// into a child, and every previous one was defeated by a *new* spawn site
    /// that did not know it had to care. A spawn site is only allowed to exist
    /// here.
    #[test]
    fn every_spawn_site_goes_through_this_module() {
        /// How far from the program name the scrub may sit.
        const WINDOW: usize = 6;

        let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut offenders = Vec::new();
        let mut stack = vec![src.clone()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("the crate source tree is readable") {
                let path = entry.expect("a readable directory entry").path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().is_none_or(|ext| ext != "rs")
                    || path.file_name().is_some_and(|name| name == "host_env.rs")
                {
                    continue;
                }
                let text = std::fs::read_to_string(&path).expect("a readable source file");
                let lines: Vec<&str> = text.lines().collect();
                for (index, line) in lines.iter().enumerate() {
                    if !(line.contains("process::Command::new") || line.contains(".sidecar(")) {
                        continue;
                    }
                    // A builder spells its scrub out a few lines from the
                    // program name; anything further away is not obviously
                    // scrubbed, which is the point.
                    let window = index.saturating_sub(WINDOW)..lines.len().min(index + WINDOW + 1);
                    if lines[window].iter().any(|line| line.contains("host_env")) {
                        continue;
                    }
                    offenders.push(format!(
                        "{}:{}",
                        path.strip_prefix(&src).unwrap_or(&path).display(),
                        index + 1
                    ));
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "spawn sites must build their child through host_env::command (or \
             env_clear + host_env::child_environment) so the AppImage mount \
             cannot leak into the child: {}",
            offenders.join(", ")
        );
    }
}
