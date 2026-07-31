//! Host GPU compute-capability probe and per-model availability overlay.
//!
//! `nvidia-smi --query-gpu=compute_cap` (available since driver R470) is
//! the source: no torch import (~100 ms vs seconds), independent of venv
//! state, and any failure degrades to "unknown", which never filters
//! anything. ROCm/MPS/CPU hosts are likewise unknown by design — the only
//! capability floors shipped today are CUDA-specific (bf16 +
//! FlashAttention 2 want sm_80+), and the Python impls carry their own
//! load-time backstop guard. On ROCm that is a decision, not an accident of
//! tooling: the sysfs probe enumerates boards perfectly well but HIP has no
//! compute-capability analogue, so every row's `compute_cap` is `None`, the
//! host view collapses to unknown, and the `/metadata` overlay stays absent
//! (docs/rocm-batch-calibration-parity.md D7 — the rows do carry
//! `gfx_target_version` for a future gfx-arch allowlist).
//!
//! The probe itself lives in `gpu.rs`: on CUDA, capabilities and board
//! identities come out of **one** `nvidia-smi --query-gpu` call,
//! positionally matched, so the two views can never disagree about which
//! board is which. This module owns the type, the floor comparison and the
//! `/metadata` overlay.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use serde_json::Value as JsonValue;

/// Compute capabilities of the visible NVIDIA GPUs; `None` = unknown host.
#[derive(Debug, Clone)]
pub struct HostComputeCaps(Option<Vec<(u32, u32)>>);

impl HostComputeCaps {
    /// The state every non-CUDA host is in, and what any probe failure or
    /// unparseable row degrades to. Never filters anything.
    pub fn unknown() -> Self {
        Self(None)
    }

    /// Build from the capabilities of the boards the merged probe found (or
    /// tests' fixtures). Empty is indistinguishable from unknown: a host
    /// with no readable board cannot filter.
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

/// One `major.minor` capability field as nvidia-smi prints it. `None` for
/// anything else (`N/A`, driver error text, a changed column shape), which
/// makes the whole probe unknown in `gpu.rs` — a partial picture must not
/// filter models.
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
/// Windows driver install location that never touches PATH. Shared with
/// `gpu.rs`, which probes board identities the same way.
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
pub(super) fn output_with_timeout(
    mut cmd: Command,
    timeout: Duration,
) -> Option<std::process::Output> {
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
