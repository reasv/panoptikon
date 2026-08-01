//! ffmpeg/ffprobe executable resolution for scan and extraction jobs
//! (video metadata and frames, thumbnails, audio decoding).
//!
//! Per tool: explicit `[jobs] ffmpeg`/`ffprobe` config path → the managed
//! venv's `static-ffmpeg` package → the bare name, left to PATH lookup at
//! spawn time. static-ffmpeg ships platform binaries for every release
//! target (including ffprobe, which imageio-ffmpeg lacks) but downloads
//! them on first use — `panoptikon setup` prefetches so that download does
//! not land in the middle of the first video scan.
//!
//! Resolution runs once per process, on first use, and is cached: the
//! callers are blocking job helpers, so the python probe (and a possible
//! first-use download) never blocks the async runtime.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// Python snippet printing the ffmpeg and ffprobe paths on two lines,
/// downloading the binaries first if needed. Shared with the setup
/// prefetch so both always agree on the API used.
pub(crate) const STATIC_FFMPEG_PROBE: &str = "from static_ffmpeg import run\n\
paths = run.get_or_fetch_platform_executables_else_raise()\n\
print(paths[0])\n\
print(paths[1])\n";

/// The ffmpeg executable to spawn. Cached after the first call.
pub(crate) fn ffmpeg() -> &'static OsStr {
    resolved().0.as_os_str()
}

/// The ffprobe executable to spawn. Cached after the first call.
pub(crate) fn ffprobe() -> &'static OsStr {
    resolved().1.as_os_str()
}

/// Classifies a failure to *start* ffmpeg/ffprobe, which is never a verdict
/// on the media (docs/failed-media-retry-design.md: "Spawn errors are
/// `blocked`, never `input`"). Only `NotFound` means the toolchain is
/// missing; a permission or resource failure is this machine's problem and
/// stays transient, so the item is retried untouched.
///
/// The blocker is derived from `tool` rather than assumed: a `blocked` row
/// names the dependency the auto-heal will probe, so a third tool routed
/// through here and silently recorded as `ffmpeg` would be healed by an
/// ffmpeg that has nothing to do with it. An unknown tool falls back to
/// transient, which costs a re-attempt and never a wrong verdict.
pub(crate) fn spawn_error(tool: &str, err: &std::io::Error) -> crate::api_error::ApiError {
    if err.kind() == std::io::ErrorKind::NotFound {
        match tool {
            "ffmpeg" | "ffprobe" => {
                return crate::api_error::ApiError::blocked(
                    crate::api_error::Blocker::Ffmpeg,
                    format!("{tool} is not installed: {err}"),
                );
            }
            other => {
                debug_assert!(false, "spawn_error has no blocker for {other}");
                tracing::warn!(
                    tool = other,
                    "spawn failure for a tool with no known blocker"
                );
            }
        }
    }
    crate::api_error::ApiError::internal(format!("{tool} failed to start: {err}"))
}

/// Whether the resolved toolchain can actually be started, for the ledger's
/// blocked auto-heal. `host_paths::can_spawn` insists on a real file, but the
/// resolver legitimately hands back a bare name for PATH lookup at spawn
/// time, so the probe has to be a spawn.
///
/// *Both* executables are probed: they are resolved independently (config
/// override, venv, PATH) and both classification sites record the same
/// `Blocker::Ffmpeg`. Healing on ffmpeg alone would clear rows that ffprobe
/// is still blocking, and every job would re-heal, re-fail, and bump the
/// search-cache epoch for nothing.
pub(crate) fn ffmpeg_available() -> bool {
    can_run(ffmpeg()) && can_run(ffprobe())
}

fn can_run(exe: &OsStr) -> bool {
    std::process::Command::new(exe)
        .arg("-version")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn resolved() -> &'static (PathBuf, PathBuf) {
    static RESOLVED: OnceLock<(PathBuf, PathBuf)> = OnceLock::new();
    RESOLVED.get_or_init(|| {
        let runtime = crate::config::runtime();
        let pair = resolve(
            runtime.ffmpeg.as_deref(),
            runtime.ffprobe.as_deref(),
            &runtime.venv_python,
        );
        tracing::info!(
            ffmpeg = %pair.0.display(),
            ffprobe = %pair.1.display(),
            "media tools resolved"
        );
        pair
    })
}

fn resolve(
    ffmpeg_override: Option<&Path>,
    ffprobe_override: Option<&Path>,
    venv_python: &Path,
) -> (PathBuf, PathBuf) {
    // Only probe the venv when a tool is not explicitly overridden.
    let venv_pair = if ffmpeg_override.is_none() || ffprobe_override.is_none() {
        venv_static_ffmpeg(venv_python)
    } else {
        None
    };
    let ffmpeg = ffmpeg_override
        .map(Path::to_path_buf)
        .or_else(|| venv_pair.as_ref().map(|pair| pair.0.clone()))
        .unwrap_or_else(|| PathBuf::from("ffmpeg"));
    let ffprobe = ffprobe_override
        .map(Path::to_path_buf)
        .or_else(|| venv_pair.as_ref().map(|pair| pair.1.clone()))
        .unwrap_or_else(|| PathBuf::from("ffprobe"));
    (ffmpeg, ffprobe)
}

/// Ask the venv's static-ffmpeg package for its executables. Any failure
/// (no venv, package not installed, download error) falls back to PATH;
/// the debug log says why.
fn venv_static_ffmpeg(python: &Path) -> Option<(PathBuf, PathBuf)> {
    if !python.is_file() {
        tracing::debug!(
            python = %python.display(),
            "no venv interpreter; using ffmpeg/ffprobe from PATH"
        );
        return None;
    }
    let output = match std::process::Command::new(python)
        .args(["-c", STATIC_FFMPEG_PROBE])
        .output()
    {
        Ok(output) => output,
        Err(err) => {
            tracing::debug!(
                error = %err,
                python = %python.display(),
                "static-ffmpeg probe did not run; using ffmpeg/ffprobe from PATH"
            );
            return None;
        }
    };
    if !output.status.success() {
        tracing::debug!(
            stderr = %crate::jobs::files::stderr_tail(&output.stderr),
            "static-ffmpeg probe failed (package missing from the venv?); \
             using ffmpeg/ffprobe from PATH"
        );
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut lines = stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty());
    let ffmpeg = PathBuf::from(lines.next()?);
    let ffprobe = PathBuf::from(lines.next()?);
    if !ffmpeg.is_file() || !ffprobe.is_file() {
        tracing::debug!(
            ffmpeg = %ffmpeg.display(),
            ffprobe = %ffprobe.display(),
            "static-ffmpeg reported paths that do not exist; \
             using ffmpeg/ffprobe from PATH"
        );
        return None;
    }
    if !crate::host_paths::can_spawn(&ffmpeg, &["-version"])
        || !crate::host_paths::can_spawn(&ffprobe, &["-version"])
    {
        tracing::debug!(
            ffmpeg = %ffmpeg.display(),
            ffprobe = %ffprobe.display(),
            "static-ffmpeg not runnable; using ffmpeg/ffprobe from PATH"
        );
        return None;
    }
    Some((ffmpeg, ffprobe))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_names_are_the_fallback_without_venv_or_overrides() {
        let missing = Path::new("does-not-exist/python");
        let (ffmpeg, ffprobe) = resolve(None, None, missing);
        assert_eq!(ffmpeg, PathBuf::from("ffmpeg"));
        assert_eq!(ffprobe, PathBuf::from("ffprobe"));
    }

    #[test]
    fn explicit_overrides_win_per_tool() {
        let missing = Path::new("does-not-exist/python");
        let (ffmpeg, ffprobe) = resolve(Some(Path::new("C:/tools/ffmpeg.exe")), None, missing);
        assert_eq!(ffmpeg, PathBuf::from("C:/tools/ffmpeg.exe"));
        assert_eq!(ffprobe, PathBuf::from("ffprobe"));

        let (ffmpeg, ffprobe) = resolve(None, Some(Path::new("/opt/ffprobe")), missing);
        assert_eq!(ffmpeg, PathBuf::from("ffmpeg"));
        assert_eq!(ffprobe, PathBuf::from("/opt/ffprobe"));
    }
}
