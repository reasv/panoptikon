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

pub(crate) mod outro;
/// Driver for the outro-detector equivalence harness of design §12
/// (`tools/outro-equivalence`). `cfg(test)` so the shipped binary gains no
/// surface — and the only form with access to `outro`'s `pub(crate)` items,
/// this crate having no library target.
#[cfg(test)]
mod outro_equivalence;
pub(crate) mod transcode;

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// The marker ffmpeg prints when it could not open an input **at all**, as
/// distinct from failing partway through one. It is what gates the `cache:`
/// retry below: an error of any other shape means the input was readable,
/// and a different io path cannot change the verdict.
pub(crate) const FFMPEG_INPUT_OPEN_FAILURE: &str = "Error opening input";

/// The argument vector with every `-i` operand rewritten through ffmpeg's
/// `cache:` protocol, which interposes a lazily-populated temp-file cache
/// between the demuxer and the real file — rebuilding the whole io stack
/// underneath the same demuxer.
///
/// This exists for one empirically-mapped bug. On Windows SMB mounts, the
/// gyan.dev ffmpeg builds (7.1 and 8.0.1; static_ffmpeg ships gyan) fail to
/// open faststart mp4s — "moov atom not found" after exactly one 32 KiB read
/// with zero seeks, on files whose moov is at the *front* and whose bytes
/// read back bit-exact through the same binary's rawvideo demuxer — whenever
/// the command line also carries one of two unrelated-looking triggers: a
/// `-progress pipe:N` destination, or ANY input-side time option (`-ss`,
/// `-t`, `-itsoffset` — even the no-op `-itsoffset 0`). Local copies of the
/// same bytes are immune, moov-at-end files are immune, output-side time
/// options are immune, and fftools sets the input open up identically either
/// way, so the upstream mechanism is unknown.
///
/// Where a trigger is incidental it is simply avoided (the transcoder writes
/// `-progress` to a file). Where it is load-bearing — the trim fast seek, the
/// outro decode clamp — the trigger stays and this is the escape hatch. The
/// cost (temp-file writes of whatever ranges are read) is paid only on a
/// retry, never on a healthy path.
pub(crate) fn cache_wrapped_args<S: AsRef<OsStr>>(args: &[S]) -> Vec<OsString> {
    let mut wrapped: Vec<OsString> = Vec::with_capacity(args.len());
    let mut prefix_next = false;
    for arg in args {
        let arg = arg.as_ref();
        if prefix_next {
            let mut prefixed = OsString::from("cache:");
            prefixed.push(arg);
            wrapped.push(prefixed);
        } else {
            wrapped.push(arg.to_os_string());
        }
        prefix_next = arg == "-i";
    }
    wrapped
}

/// Whether a `cache:` retry could possibly help: every `-i` operand has to
/// name a file that is really there. The bug is specific to file io, so an
/// input that is a filter graph (`-f lavfi -i testsrc=...`) or a url is out
/// of scope, and an input that does not exist is already answered.
///
/// This is a litter control as much as a shortcut. ffmpeg's `cache:`
/// protocol creates its backing temp file *before* it finds out the inner
/// url will not open, and on Windows it cannot unlink a file it still holds
/// open — so retrying a missing input leaves an empty `ffcache*` file in the
/// process's working directory, which for the gateway is the install
/// directory. Not retrying what cannot be helped avoids that entirely.
pub(crate) fn ffmpeg_inputs_all_exist<S: AsRef<OsStr>>(args: &[S]) -> bool {
    let mut inputs = 0usize;
    let mut is_input = false;
    for arg in args {
        let arg = arg.as_ref();
        if is_input {
            inputs += 1;
            if !Path::new(arg).is_file() {
                return false;
            }
        }
        is_input = arg == "-i";
    }
    inputs > 0
}

/// Runs ffmpeg to completion with its output captured, retrying once with
/// [`cache_wrapped_args`] if the first attempt could not open an input that
/// [`ffmpeg_inputs_all_exist`] says is really there.
///
/// `configure` applies whatever stdio and spawn policy the call site needs,
/// and is applied to both attempts so the retry is the same command by every
/// measure except the io path to its inputs. A file that exists but is
/// genuinely unreadable fails the retry too and keeps its verdict; that
/// doomed second spawn costs milliseconds.
pub(crate) fn ffmpeg_output_with_input_retry(
    args: &[OsString],
    configure: impl Fn(&mut std::process::Command),
) -> std::io::Result<std::process::Output> {
    let run = |args: &[OsString]| {
        let mut command = std::process::Command::new(ffmpeg());
        command.args(args);
        configure(&mut command);
        command.output()
    };
    let first = run(args)?;
    if first.status.success()
        || !String::from_utf8_lossy(&first.stderr).contains(FFMPEG_INPUT_OPEN_FAILURE)
        || !ffmpeg_inputs_all_exist(args)
    {
        return Ok(first);
    }
    tracing::warn!("ffmpeg could not open an input; retrying through the cache: protocol");
    run(&cache_wrapped_args(args))
}

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

    /// The `cache:` rewrite touches exactly the operand after each `-i` —
    /// every input of a multi-input vector, and nothing else. An output path
    /// (which follows no `-i`) must never be wrapped: `cache:` is a
    /// read-only protocol, so wrapping one would fail the mux outright.
    #[test]
    fn cache_wrap_prefixes_every_input_and_nothing_else() {
        let args: Vec<OsString> = [
            "-nostdin",
            "-ss",
            "1.00",
            "-i",
            r"Z:\a.mp4",
            "-i",
            "b.mp4",
            "-map",
            "0:v:0",
            "-y",
            "out.tmp",
        ]
        .into_iter()
        .map(OsString::from)
        .collect();
        let wrapped: Vec<String> = cache_wrapped_args(&args)
            .into_iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            wrapped,
            [
                "-nostdin",
                "-ss",
                "1.00",
                "-i",
                r"cache:Z:\a.mp4",
                "-i",
                "cache:b.mp4",
                "-map",
                "0:v:0",
                "-y",
                "out.tmp",
            ]
        );

        // A vector with no inputs at all is returned unchanged.
        let args: Vec<OsString> = ["-version"].into_iter().map(OsString::from).collect();
        assert_eq!(cache_wrapped_args(&args), args);
    }

    /// The retry gate matches what ffmpeg actually prints when an input
    /// cannot be opened — both the SMB "moov atom not found" shape and an
    /// ordinary missing file — and nothing that failed later in the run.
    #[test]
    fn the_retry_gate_matches_open_failures_only() {
        for opened in [
            "[in#0 @ 0x1] Error opening input: Invalid data found when processing input",
            "Error opening input files: Invalid data found when processing input",
        ] {
            assert!(opened.contains(FFMPEG_INPUT_OPEN_FAILURE), "{opened}");
        }
        for other in [
            "[vost#0:0 @ 0x1] Unknown encoder 'libsvtav1'",
            "Error opening output file out.mp4.",
            "Conversion failed!",
        ] {
            assert!(!other.contains(FFMPEG_INPUT_OPEN_FAILURE), "{other}");
        }
    }

    /// The retry only fires for inputs that are really files. A missing one
    /// is already answered, and retrying it would leave an empty `ffcache*`
    /// file in the working directory for nothing.
    #[test]
    fn only_real_files_are_worth_a_cache_retry() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("here.mp4");
        std::fs::write(&real, b"not really a video").unwrap();
        let missing = dir.path().join("gone.mp4");

        let args = |input: &Path| -> Vec<OsString> {
            vec![
                OsString::from("-ss"),
                OsString::from("1.00"),
                OsString::from("-i"),
                input.into(),
                OsString::from("out.mp4"),
            ]
        };
        assert!(ffmpeg_inputs_all_exist(&args(&real)));
        assert!(!ffmpeg_inputs_all_exist(&args(&missing)));

        // Every input must exist, not just one of them.
        let mut both = args(&real);
        both.insert(4, OsString::from("-i"));
        both.insert(5, missing.clone().into());
        assert!(!ffmpeg_inputs_all_exist(&both));

        // A directory is not an input, and a generated source is not a file
        // at all — neither is in scope for a file-io bug.
        let dir_arg: Vec<OsString> = vec![OsString::from("-i"), dir.path().into()];
        assert!(!ffmpeg_inputs_all_exist(&dir_arg));
        let lavfi: Vec<OsString> = ["-f", "lavfi", "-i", "testsrc=size=8x8:rate=1"]
            .into_iter()
            .map(OsString::from)
            .collect();
        assert!(!ffmpeg_inputs_all_exist(&lavfi));

        // And a vector with no inputs never qualifies.
        let none: Vec<OsString> = vec![OsString::from("-version")];
        assert!(!ffmpeg_inputs_all_exist(&none));
    }
}
