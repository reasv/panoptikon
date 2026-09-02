//! Host GPU compute-capability probe and per-model availability overlay.
//!
//! `nvidia-smi --query-gpu=compute_cap` (available since driver R470) is
//! the source: no torch import (~100 ms vs seconds), independent of venv
//! state, and any failure degrades to "unknown", which never filters
//! anything. ROCm/MPS/CPU hosts have no nvidia-smi and are likewise
//! unknown by design — the only capability floors shipped today are
//! CUDA-specific (bf16 + FlashAttention 2 want sm_80+), and the Python
//! impls carry their own load-time backstop guard.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use serde_json::Value as JsonValue;

/// Compute capabilities of the visible NVIDIA GPUs; `None` = unknown host.
#[derive(Debug, Clone)]
pub struct HostComputeCaps(Option<Vec<(u32, u32)>>);

impl HostComputeCaps {
    /// Only tests construct states without probing; production goes
    /// through [`Self::probe`].
    #[cfg(test)]
    pub fn unknown() -> Self {
        Self(None)
    }

    #[cfg(test)]
    pub fn known(caps: Vec<(u32, u32)>) -> Self {
        if caps.is_empty() {
            Self(None)
        } else {
            Self(Some(caps))
        }
    }

    /// Probe once at startup. Never fails: no nvidia-smi, a timeout, or
    /// unparseable output all yield "unknown".
    pub fn probe() -> Self {
        let Some(smi) = find_nvidia_smi() else {
            return Self(None);
        };
        let mut cmd = Command::new(smi);
        cmd.args(["--query-gpu=compute_cap", "--format=csv,noheader"]);
        let Some(output) = output_with_timeout(cmd, Duration::from_secs(5)) else {
            tracing::warn!(
                "nvidia-smi compute_cap probe failed or timed out; \
                 model availability will not be capability-filtered"
            );
            return Self(None);
        };
        if !output.status.success() {
            return Self(None);
        }
        let caps = parse_compute_caps(&String::from_utf8_lossy(&output.stdout));
        if let Some(caps) = &caps {
            tracing::info!(compute_caps = %join_caps(caps), "detected GPU compute capabilities");
        }
        Self(caps)
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
/// id whose numeric `min_compute_capability` metadata this host provably
/// fails. Unknown hosts and satisfied floors leave the body untouched.
/// Floors are read from per-id metadata only (where the shipped registry
/// sets them), not group metadata.
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

/// One capability per line, `major.minor` (`--format=csv,noheader`). Any
/// unparseable non-empty line (e.g. `N/A`, driver error text) makes the
/// whole probe unknown — a partial picture must not filter models.
fn parse_compute_caps(stdout: &str) -> Option<Vec<(u32, u32)>> {
    let mut caps = Vec::new();
    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let (major, minor) = line.split_once('.')?;
        caps.push((
            major.trim().parse::<u32>().ok()?,
            minor.trim().parse::<u32>().ok()?,
        ));
    }
    if caps.is_empty() { None } else { Some(caps) }
}

fn join_caps(caps: &[(u32, u32)]) -> String {
    caps.iter()
        .map(|(major, minor)| format!("{major}.{minor}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Same locations the setup accelerator probes use: PATH, plus the
/// Windows driver install location that never touches PATH.
fn find_nvidia_smi() -> Option<PathBuf> {
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
    if cfg!(windows) {
        if let Some(root) = std::env::var_os("SystemRoot") {
            let candidate = Path::new(&root).join("System32/nvidia-smi.exe");
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

/// Run to completion or give up after `timeout`. On timeout the child is
/// left to finish on its own (nvidia-smi is short-lived); only the boot
/// path must not stall behind a wedged driver.
fn output_with_timeout(mut cmd: Command, timeout: Duration) -> Option<std::process::Output> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(cmd.output());
    });
    rx.recv_timeout(timeout).ok()?.ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_single_and_multi_gpu_output() {
        assert_eq!(parse_compute_caps("8.6\n"), Some(vec![(8, 6)]));
        assert_eq!(
            parse_compute_caps("12.0\n6.1\n"),
            Some(vec![(12, 0), (6, 1)])
        );
    }

    #[test]
    fn garbage_or_na_output_is_unknown() {
        assert_eq!(parse_compute_caps(""), None);
        assert_eq!(parse_compute_caps("N/A\n"), None);
        assert_eq!(parse_compute_caps("8.6\nN/A\n"), None);
        assert_eq!(
            parse_compute_caps("Failed to initialize NVML: Driver error\n"),
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
