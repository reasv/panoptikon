//! Host GPU compute-capability probe and per-model availability overlay.
//!
//! `nvidia-smi --query-gpu=compute_cap` (available since driver R470) is
//! the source: no torch import (~100 ms vs seconds), independent of venv
//! state, and any failure degrades to "unknown", which never filters
//! anything. ROCm/MPS/CPU hosts are not queried and are unknown by
//! design — the only capability floors shipped today are
//! CUDA-specific (bf16 + FlashAttention 2 want sm_80+), and the Python
//! impls carry their own load-time backstop guard. A model that loads on
//! some backends only lists them in its `accelerators` metadata.
//! The query itself runs in `gpu.rs`, together with the GPU identity probe.

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
    /// Unknown capabilities, which never filter anything.
    pub fn unknown() -> Self {
        Self(None)
    }

    /// Build from probed capabilities; empty means unknown.
    pub fn from_caps(caps: Vec<(u32, u32)>) -> Self {
        if caps.is_empty() {
            Self(None)
        } else {
            tracing::info!(compute_caps = %join_caps(&caps), "detected GPU compute capabilities");
            Self(Some(caps))
        }
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

/// Inject `unavailable: true` + `unavailable_reason` into every inference
/// id whose `accelerators` metadata (backends it loads on: `cuda`, `rocm`,
/// `mps`, `cpu`) leaves out `backend`, the one this host runs models on, or
/// whose numeric `min_compute_capability` this host provably fails. Unknown
/// hosts and satisfied floors leave the body untouched. Both are read from
/// per-id metadata only (where the shipped registry sets them), not group
/// metadata.
pub fn overlay_metadata(root: &mut JsonValue, caps: &HostComputeCaps, backend: &str) {
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
            if let Some(accelerators) = obj.get("accelerators").and_then(JsonValue::as_array)
                && !accelerators
                    .iter()
                    .any(|name| name.as_str() == Some(backend))
            {
                let names: Vec<&str> = accelerators.iter().filter_map(JsonValue::as_str).collect();
                let reason = format!(
                    "Runs only on {} (this host runs models on {backend})",
                    names.join(", ")
                );
                mark_unavailable(obj, reason);
                continue;
            }
            let Some(floor) = obj
                .get("min_compute_capability")
                .and_then(JsonValue::as_f64)
            else {
                continue;
            };
            if caps.meets_floor(floor) == Some(false) {
                let tenths = (floor * 10.0).round() as i64;
                let reason = format!(
                    "Requires an NVIDIA GPU with compute capability >= {}.{} (detected: {})",
                    tenths / 10,
                    tenths % 10,
                    caps.describe(),
                );
                mark_unavailable(obj, reason);
            }
        }
    }
}

fn mark_unavailable(obj: &mut serde_json::Map<String, JsonValue>, reason: String) {
    obj.insert("unavailable".to_string(), JsonValue::Bool(true));
    obj.insert("unavailable_reason".to_string(), JsonValue::String(reason));
}

/// One `major.minor` capability field as nvidia-smi prints it, else `None`.
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

/// Same locations the setup accelerator probes use: PATH, plus the
/// Windows driver install location that never touches PATH.
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

/// Poll interval while waiting for the probe child.
const PROBE_POLL_INTERVAL: Duration = Duration::from_millis(5);

/// Run to completion or give up after `timeout`, killing the child's whole
/// process group (a wrapper script's children hold the pipes too). Output is
/// drained on two threads so a full pipe cannot deadlock the wait; on
/// timeout they are not joined, so an escaped descendant cannot block us.
pub(super) fn output_with_timeout(
    mut cmd: Command,
    timeout: Duration,
) -> Option<std::process::Output> {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // Own process group (console-signal group on Windows), for the kill below.
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
        // Group, then the child, then reap it.
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

/// Read one of the child's pipes to EOF on its own thread. A missing pipe or
/// a failed read yields empty output.
fn drain<R: Read + Send + 'static>(pipe: Option<R>) -> JoinHandle<Vec<u8>> {
    std::thread::spawn(move || {
        let mut buf = Vec::new();
        if let Some(mut pipe) = pipe {
            let _ = pipe.read_to_end(&mut buf);
        }
        buf
    })
}

/// What a finished [`drain`] read; empty if the thread panicked.
fn drained(pipe: JoinHandle<Vec<u8>>) -> Vec<u8> {
    pipe.join().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A child that answers in time yields what a plain `output()` would:
    /// status, stdout and stderr.
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
    /// the child answered, so the probe stands with that stream empty.
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

    /// Giving up on a probe must end the probe: an abandoned child keeps
    /// running, so a binary slower than the caller's retry backoff would pile
    /// up processes. The child here would create a marker one second in; the
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
        let caps = HostComputeCaps::from_caps(vec![(7, 5)]);
        assert_eq!(caps.meets_floor(7.5), Some(true));
        assert_eq!(caps.meets_floor(8.0), Some(false));
        // ANY device qualifying is enough.
        let mixed = HostComputeCaps::from_caps(vec![(6, 1), (8, 6)]);
        assert_eq!(mixed.meets_floor(8.0), Some(true));
        assert_eq!(HostComputeCaps::unknown().meets_floor(8.0), None);
        // 10.x majors compare above 9.x, not lexicographically.
        let blackwell = HostComputeCaps::from_caps(vec![(12, 0)]);
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
        let caps = HostComputeCaps::from_caps(vec![(6, 1)]);
        overlay_metadata(&mut body, &caps, "cuda");
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
        overlay_metadata(
            &mut satisfied,
            &HostComputeCaps::from_caps(vec![(8, 9)]),
            "cuda",
        );
        assert_eq!(satisfied, template);

        let mut unknown = template.clone();
        overlay_metadata(&mut unknown, &HostComputeCaps::unknown(), "cuda");
        assert_eq!(unknown, template);
    }

    /// A model that lists the backends it loads on is unavailable on any
    /// other, whatever the capability probe knows.
    #[test]
    fn overlay_marks_ids_whose_accelerators_leave_out_the_host_backend() {
        let template = json!({
            "doctr": {
                "group_metadata": {},
                "inference_ids": {
                    "dots_ocr": {"accelerators": ["cuda"], "min_compute_capability": 8.0},
                    "doctr|db_resnet50": {"description": "open"}
                }
            }
        });
        for backend in ["rocm", "mps", "cpu"] {
            let mut body = template.clone();
            overlay_metadata(&mut body, &HostComputeCaps::unknown(), backend);
            let ids = &body["doctr"]["inference_ids"];
            assert_eq!(ids["dots_ocr"]["unavailable"], json!(true), "{backend}");
            assert!(ids["doctr|db_resnet50"].get("unavailable").is_none());
        }
        let mut cuda = template.clone();
        overlay_metadata(&mut cuda, &HostComputeCaps::unknown(), "cuda");
        assert_eq!(cuda, template);
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
        overlay_metadata(&mut body, &HostComputeCaps::from_caps(vec![(6, 1)]), "cuda");
        assert_eq!(body, template);
    }
}
