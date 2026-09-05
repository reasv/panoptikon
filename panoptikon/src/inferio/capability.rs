//! Host GPU compute-capability probe and per-model availability overlay.
//!
//! `nvidia-smi --query-gpu=compute_cap` is the source: no torch import,
//! independent of venv state, and any failure degrades to "unknown", which
//! never filters anything. ROCm, MPS and CPU hosts are unknown by design —
//! the only floors shipped today are CUDA-specific, and the Python impls
//! carry their own load-time backstop. HIP in particular has no
//! compute-capability analogue at all, so every ROCm row's `compute_cap` is
//! `None` and the `/metadata` overlay stays absent
//! (docs/rocm-batch-calibration-parity.md D7).
//!
//! The probe itself lives in `gpu.rs`, where capabilities and GPU identities
//! come out of one `nvidia-smi --query-gpu` call, positionally matched. This
//! module owns the type, the floor comparison and the overlay.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde_json::Value as JsonValue;

/// Compute capabilities of the visible NVIDIA GPUs; `None` = unknown host.
#[derive(Debug, Clone)]
pub struct HostComputeCaps(Option<Vec<(u32, u32)>>);

impl HostComputeCaps {
    /// The state every non-CUDA host is in, and what any probe failure
    /// degrades to. Never filters anything.
    pub fn unknown() -> Self {
        Self(None)
    }

    /// Build from the capabilities the merged probe found. Empty is
    /// indistinguishable from unknown: nothing readable cannot filter.
    pub fn from_caps(caps: Vec<(u32, u32)>) -> Self {
        if caps.is_empty() {
            Self(None)
        } else {
            tracing::info!(compute_caps = %join_caps(&caps), "detected GPU compute capabilities");
            Self(Some(caps))
        }
    }

    #[cfg(test)]
    pub fn known(caps: Vec<(u32, u32)>) -> Self {
        Self::from_caps(caps)
    }

    /// Whether ANY visible device meets `floor` (e.g. `8.0`); `None` when
    /// the host is unknown. Tenths-integer compare, no float equality.
    pub fn meets_floor(&self, floor: f64) -> Option<bool> {
        let caps = self.0.as_ref()?;
        let floor_tenths = (floor * 10.0).round() as i64;
        Some(
            caps.iter()
                .any(|(major, minor)| i64::from(major * 10 + minor) >= floor_tenths),
        )
    }

    fn describe(&self) -> String {
        match &self.0 {
            Some(caps) => join_caps(caps),
            None => "unknown".to_string(),
        }
    }
}

/// Inject `unavailable: true` + `unavailable_reason` into every inference id
/// whose numeric `min_compute_capability` this host provably fails. Unknown
/// hosts and satisfied floors leave the body untouched, and floors are read
/// from per-id metadata only, never group metadata.
pub fn overlay_metadata(root: &mut JsonValue, caps: &HostComputeCaps) {
    let Some(groups) = root.as_object_mut() else {
        return;
    };
    for group in groups.values_mut() {
        let Some(ids) = group
            .get_mut("inference_ids")
            .and_then(JsonValue::as_object_mut)
        else {
            continue;
        };
        for meta in ids.values_mut() {
            let Some(obj) = meta.as_object_mut() else {
                continue;
            };
            let Some(floor) = obj
                .get("min_compute_capability")
                .and_then(JsonValue::as_f64)
            else {
                continue;
            };
            if caps.meets_floor(floor) == Some(false) {
                let tenths = (floor * 10.0).round() as i64;
                obj.insert("unavailable".to_string(), JsonValue::Bool(true));
                obj.insert(
                    "unavailable_reason".to_string(),
                    JsonValue::String(format!(
                        "Requires an NVIDIA GPU with compute capability >= \
                         {}.{} (detected: {})",
                        tenths / 10,
                        tenths % 10,
                        caps.describe(),
                    )),
                );
            }
        }
    }
}

/// One `major.minor` capability field as nvidia-smi prints it; `None` for
/// anything else, which makes the whole probe unknown in `gpu.rs`.
pub(super) fn parse_compute_cap(field: &str) -> Option<(u32, u32)> {
    let (major, minor) = field.trim().split_once('.')?;
    Some((
        major.trim().parse::<u32>().ok()?,
        minor.trim().parse::<u32>().ok()?,
    ))
}

fn join_caps(caps: &[(u32, u32)]) -> String {
    caps.iter()
        .map(|(major, minor)| format!("{major}.{minor}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// PATH, plus the Windows driver install location that never touches it.
/// Shared with `gpu.rs`, which probes GPU identities the same way.
pub(super) fn find_nvidia_smi() -> Option<PathBuf> {
    let path = std::env::var_os("PATH");
    if let Some(path) = path {
        for dir in std::env::split_paths(&path) {
            if dir.as_os_str().is_empty() {
                continue;
            }
            let name = if cfg!(windows) {
                "nvidia-smi.exe"
            } else {
                "nvidia-smi"
            };
            let candidate = dir.join(name);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    if cfg!(windows)
        && let Some(root) = std::env::var_os("SystemRoot")
    {
        let candidate = Path::new(&root).join("System32/nvidia-smi.exe");
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

/// How often the wait below looks at the child: small enough to add nothing
/// measurable to a healthy probe, large enough that waiting out the give-up
/// costs a thousand wakeups rather than a million.
const PROBE_POLL_INTERVAL: Duration = Duration::from_millis(5);

/// Run to completion or give up after `timeout`, **killing the child** if it
/// is still running when we do — so at most one probe process exists at a
/// time, however slow the binary is.
///
/// The kill is a **process-group** kill, which is why the probe is spawned
/// into a group of its own: a wrapper script's `sleep` inherits the pipes,
/// so killing only the direct child leaves the readers blocked on a write
/// end nobody closed.
///
/// Output is drained on two threads while the child runs, so a child that
/// fills a pipe cannot deadlock the wait. On the give-up path those threads
/// are not joined: every writer has just been killed, and not joining means
/// an escaped descendant can never wedge this boot-path caller.
pub(super) fn output_with_timeout(
    mut cmd: Command,
    timeout: Duration,
) -> Option<std::process::Output> {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // Its own process group (its own console-signal group on Windows), so
    // the give-up below can take the whole probe down and not this process.
    crate::process_tree::detach_from_console(&mut cmd);
    let mut child = cmd.spawn().ok()?;
    let stdout = drain(child.stdout.take());
    let stderr = drain(child.stderr.take());
    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) => {}
            // Unwaitable is as good as gone; fall through to the kill.
            Err(_) => break None,
        }
        if Instant::now() >= deadline {
            break None;
        }
        std::thread::sleep(PROBE_POLL_INTERVAL);
    };
    let Some(status) = status else {
        // Group first, then the child, then reap it: no process of this
        // attempt outlives the call.
        crate::process_tree::kill_process_group_pid(Some(child.id()));
        let _ = child.kill();
        let _ = child.wait();
        drop((stdout, stderr));
        return None;
    };
    Some(std::process::Output {
        status,
        stdout: drained(stdout),
        stderr: drained(stderr),
    })
}

/// Read one of the child's pipes to EOF on a thread of its own, so a child
/// that fills a pipe cannot deadlock the wait. No pipe (already taken) reads
/// as empty, as does a read that failed: the caller decides on the status.
fn drain<R: Read + Send + 'static>(pipe: Option<R>) -> JoinHandle<Vec<u8>> {
    std::thread::spawn(move || {
        let mut buf = Vec::new();
        if let Some(mut pipe) = pipe {
            let _ = pipe.read_to_end(&mut buf);
        }
        buf
    })
}

/// What a finished [`drain`] read. A thread that **panicked** costs the stream
/// it was reading and nothing else: the child answered, so this is a probe
/// with one empty stream, not an unknown host. (It used to be `join().ok()?`,
/// which turned a successful probe into `None`.)
fn drained(pipe: JoinHandle<Vec<u8>>) -> Vec<u8> {
    pipe.join().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The rewrite still has to be a plain `output()` when the child
    /// answers in time: status, stdout and stderr, all three.
    #[cfg(unix)]
    #[test]
    fn a_probe_that_answers_returns_its_output() {
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg("printf out; printf err >&2");
        let output = output_with_timeout(cmd, Duration::from_secs(5)).expect("the probe answered");
        assert!(output.status.success());
        assert_eq!(output.stdout, b"out");
        assert_eq!(output.stderr, b"err");
    }

    /// A drain thread that panicked costs only the stream it was reading:
    /// the child answered, so the probe stands with that stream empty. It
    /// used to be `join().ok()?`, which reported the whole probe — and so
    /// the host's capabilities — as unknown.
    #[test]
    fn a_panicking_drain_thread_costs_only_its_own_stream() {
        struct PanicsOnRead;
        impl Read for PanicsOnRead {
            fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
                panic!("the drain thread died");
            }
        }
        // The panic is the point; keep it off the test log.
        let hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let handle = drain(Some(PanicsOnRead));
        let read = drained(handle);
        std::panic::set_hook(hook);
        assert!(read.is_empty(), "the stream is empty, and the probe stands");
    }

    /// F13: giving up on a probe must *end* the probe. An abandoned child
    /// keeps running (1.04 s of overlap measured against a deliberately slow
    /// nvidia-smi shim), so a binary slower than the caller's 10 s failure
    /// backoff would accumulate one process and one reader thread per
    /// attempt. The child here would create a marker one second in; the
    /// timeout is 200 ms, and the marker must never appear.
    #[cfg(unix)]
    #[test]
    fn a_timed_out_probe_child_is_killed_rather_than_abandoned() {
        let marker = std::env::temp_dir().join(format!("panoptikon-f13-{}", std::process::id()));
        let _ = std::fs::remove_file(&marker);
        let mut cmd = Command::new("sh");
        cmd.arg("-c")
            .arg(format!("sleep 1; : > '{}'", marker.display()));

        let started = Instant::now();
        assert!(
            output_with_timeout(cmd, Duration::from_millis(200)).is_none(),
            "the probe did not answer within its timeout"
        );
        assert!(
            started.elapsed() < Duration::from_millis(900),
            "and it gave up at the timeout rather than at the child's pace: {:?}",
            started.elapsed()
        );

        // Well past the point the child would have written it.
        std::thread::sleep(Duration::from_millis(1_500));
        assert!(
            !marker.exists(),
            "the timed-out child kept running: {}",
            marker.display()
        );
        let _ = std::fs::remove_file(&marker);
    }

    #[test]
    fn parses_a_capability_field() {
        assert_eq!(parse_compute_cap("8.6"), Some((8, 6)));
        assert_eq!(parse_compute_cap(" 12.0 "), Some((12, 0)));
    }

    #[test]
    fn garbage_or_na_field_is_unknown() {
        assert_eq!(parse_compute_cap(""), None);
        assert_eq!(parse_compute_cap("N/A"), None);
        assert_eq!(parse_compute_cap("8"), None);
        assert_eq!(
            parse_compute_cap("Failed to initialize NVML: Driver error"),
            None
        );
    }

    #[test]
    fn meets_floor_boundaries() {
        let caps = HostComputeCaps::known(vec![(7, 5)]);
        assert_eq!(caps.meets_floor(7.5), Some(true));
        assert_eq!(caps.meets_floor(8.0), Some(false));
        // ANY device qualifying is enough.
        let mixed = HostComputeCaps::known(vec![(6, 1), (8, 6)]);
        assert_eq!(mixed.meets_floor(8.0), Some(true));
        assert_eq!(HostComputeCaps::unknown().meets_floor(8.0), None);
        // 10.x majors compare above 9.x, not lexicographically.
        let blackwell = HostComputeCaps::known(vec![(12, 0)]);
        assert_eq!(blackwell.meets_floor(8.0), Some(true));
    }

    #[test]
    fn overlay_marks_only_failing_ids() {
        let mut body = json!({
            "doctr": {
                "group_metadata": {"name": "OCR"},
                "inference_ids": {
                    "dots_ocr": {
                        "description": "gated",
                        "min_compute_capability": 8.0
                    },
                    "doctr|db_resnet50": {"description": "open"}
                }
            }
        });
        let caps = HostComputeCaps::known(vec![(6, 1)]);
        overlay_metadata(&mut body, &caps);
        let gated = &body["doctr"]["inference_ids"]["dots_ocr"];
        assert_eq!(gated["unavailable"], json!(true));
        let reason = gated["unavailable_reason"].as_str().unwrap();
        assert!(reason.contains(">= 8.0"), "reason: {reason}");
        assert!(reason.contains("6.1"), "reason: {reason}");
        let open = &body["doctr"]["inference_ids"]["doctr|db_resnet50"];
        assert!(open.get("unavailable").is_none());
    }

    #[test]
    fn overlay_untouched_when_satisfied_or_unknown() {
        let template = json!({
            "doctr": {
                "group_metadata": {},
                "inference_ids": {
                    "dots_ocr": {"min_compute_capability": 8.0}
                }
            }
        });
        let mut satisfied = template.clone();
        overlay_metadata(&mut satisfied, &HostComputeCaps::known(vec![(8, 9)]));
        assert_eq!(satisfied, template);

        let mut unknown = template.clone();
        overlay_metadata(&mut unknown, &HostComputeCaps::unknown());
        assert_eq!(unknown, template);
    }

    #[test]
    fn overlay_ignores_non_numeric_floor() {
        let template = json!({
            "g": {
                "group_metadata": {},
                "inference_ids": {
                    "id": {"min_compute_capability": "high"}
                }
            }
        });
        let mut body = template.clone();
        overlay_metadata(&mut body, &HostComputeCaps::known(vec![(6, 1)]));
        assert_eq!(body, template);
    }
}
