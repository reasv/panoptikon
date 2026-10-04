//! Runtime accelerator / GPU diagnostics for logs and `panoptikon accelerator`.
//!
//! Two independent facts:
//!
//! 1. **Backend** — which inference stack we use (`cpu` / `cuda` / `rocm`, …).
//!    Always resolvable. Priority: managed-venv sentinel → config / `auto`
//!    host probes. There is deliberately no separate env-var resolution path:
//!    `PANOPTIKON_ACCELERATOR` (Nix wrap, packagers) reaches the config value
//!    through the `${PANOPTIKON_ACCELERATOR:-auto}` template line in the
//!    shipped TOML, like every other env-bridged setting.
//! 2. **Devices** — optional marketing names (plus compute capability where
//!    the vendor tool reports it) from pluggable **stack probes** (NVIDIA,
//!    AMD/ROCm today). Append to [`GPU_STACK_PROBES`] for new stacks (e.g.
//!    Intel XPU); add an [`Accelerator`] variant when the managed venv gains
//!    a matching extra.
//!
//! **Warnings:** only when a backend with a driver stack to probe is selected
//! and no device name is found. **CPU and MPS are never a warning.**

use std::path::PathBuf;
use std::process::Command;

use crate::config::{Accelerator, Settings};
use crate::setup::{installed_accelerator, resolve_accelerator};

/// One named GPU/accelerator device from a hardware stack probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GpuDevice {
    /// Stack id (`nvidia`, `amd-rocm`, future `intel-xpu`, …).
    pub stack: &'static str,
    pub name: String,
    /// CUDA compute capability (e.g. `12.0`) where the vendor tool reports
    /// it; `None` for stacks without the concept or on old drivers.
    pub compute_cap: Option<String>,
}

impl GpuDevice {
    /// Name plus compute capability when known, for text and log output.
    fn label(&self) -> String {
        match &self.compute_cap {
            Some(cc) => format!("{} (CC {cc})", self.name),
            None => self.name.clone(),
        }
    }
}

/// Presence of one GPU software stack on the host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GpuStackPresence {
    pub stack: &'static str,
    /// Backend this stack typically drives (never `auto`).
    pub backend: Accelerator,
    pub devices: Vec<GpuDevice>,
    pub evidence: String,
}

/// Where the resolved backend came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendSource {
    /// Setup sentinel `extra=` (installed torch wheels).
    InstalledVenv,
    /// Config (env bridges already expanded), including `auto` host probes.
    ConfigOrProbe { evidence: String },
}

impl BackendSource {
    pub fn label(&self) -> String {
        match self {
            Self::InstalledVenv => "managed venv (setup sentinel)".into(),
            Self::ConfigOrProbe { evidence } => evidence.clone(),
        }
    }
}

/// Full diagnostic snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceleratorReport {
    /// Resolved backend; never [`Accelerator::Auto`] after resolve.
    pub backend: Accelerator,
    pub backend_source: BackendSource,
    pub stacks: Vec<GpuStackPresence>,
    pub warnings: Vec<String>,
}

impl AcceleratorReport {
    /// Devices belonging to the stack that matches the selected GPU backend.
    pub fn selected_devices(&self) -> Vec<&GpuDevice> {
        let Some(stack_id) = stack_id_for_backend(self.backend) else {
            return Vec::new();
        };
        self.stacks
            .iter()
            .filter(|s| s.stack == stack_id)
            .flat_map(|s| s.devices.iter())
            .collect()
    }

    /// Multi-line human text for CLI / package tests.
    pub fn format_text(&self) -> String {
        let mut lines = vec![format!(
            "accelerator backend: {} ({})",
            accelerator_slug(self.backend),
            self.backend_source.label()
        )];

        if is_gpu_backend(self.backend) {
            let devices = self.selected_devices();
            if !devices.is_empty() {
                lines.push("GPU devices:".into());
                for d in devices {
                    lines.push(format!("  - [{}] {}", d.stack, d.label()));
                }
            } else if stack_id_for_backend(self.backend).is_none() {
                // Metal is part of macOS; no vendor tool names the device.
                lines.push("GPU device: the one this OS provides".into());
            } else {
                lines.push("GPU devices: (none detected)".into());
            }
        } else {
            // CPU is a normal outcome — never a warning.
            lines.push("using CPU (no GPU accelerator selected)".into());
            let other: Vec<&GpuDevice> =
                self.stacks.iter().flat_map(|s| s.devices.iter()).collect();
            if !other.is_empty() {
                lines.push("GPU devices present on host (not selected):".into());
                for d in other {
                    lines.push(format!("  - [{}] {}", d.stack, d.label()));
                }
            }
        }

        for w in &self.warnings {
            lines.push(format!("warning: {w}"));
        }
        lines.join("\n")
    }
}

/// Whether this backend runs the model on a GPU (MPS included, though it has
/// no driver stack to probe: [`stack_id_for_backend`]).
pub fn is_gpu_backend(a: Accelerator) -> bool {
    matches!(a, Accelerator::Cuda | Accelerator::Rocm | Accelerator::Mps)
    // | Accelerator::Xpu
}

/// Stack probe id for a GPU backend (`nvidia` ↔ cuda, `amd-rocm` ↔ rocm).
pub fn stack_id_for_backend(a: Accelerator) -> Option<&'static str> {
    match a {
        Accelerator::Cuda => Some("nvidia"),
        Accelerator::Rocm => Some("amd-rocm"),
        // Accelerator::Xpu => Some("intel-xpu"),
        // MPS has no driver stack to probe: Metal is part of the OS.
        Accelerator::Cpu | Accelerator::Mps | Accelerator::Auto => None,
    }
}

/// Canonical lowercase slug for logs, wrap env, and tests.
pub fn accelerator_slug(a: Accelerator) -> &'static str {
    match a {
        Accelerator::Auto => "auto",
        Accelerator::Cuda => "cuda",
        Accelerator::Rocm => "rocm",
        Accelerator::Cpu => "cpu",
        Accelerator::Mps => "mps",
        // Accelerator::Xpu => "xpu",
    }
}

/// Resolve which backend we use (always concrete). Reads process state.
pub fn resolve_backend(requested: Accelerator) -> (Accelerator, BackendSource) {
    resolve_backend_from(requested, installed_accelerator())
}

/// Pure resolver (unit-tested).
///
/// Priority: installed venv → config/`auto` probes. `requested` is the
/// config value with env bridges (`${PANOPTIKON_ACCELERATOR:-auto}`) already
/// expanded — the env var has no resolution path of its own.
pub fn resolve_backend_from(
    requested: Accelerator,
    installed: Option<Accelerator>,
) -> (Accelerator, BackendSource) {
    if let Some(installed) = installed {
        return (installed, BackendSource::InstalledVenv);
    }
    match resolve_accelerator(requested) {
        Ok((backend, evidence)) => (backend, BackendSource::ConfigOrProbe { evidence }),
        Err(_) => (
            match requested {
                Accelerator::Auto => Accelerator::Cpu,
                other => other,
            },
            BackendSource::ConfigOrProbe {
                evidence: "fallback after resolve error".into(),
            },
        ),
    }
}

/// Build a report from live config + host probes.
pub fn build_report(settings: &Settings) -> AcceleratorReport {
    let (backend, backend_source) =
        resolve_backend(settings.inference_local.python_env.accelerator);
    assemble_report(backend, backend_source, probe_gpu_stacks())
}

/// Pure assembly of warnings + device list (unit-tested).
pub fn assemble_report(
    backend: Accelerator,
    backend_source: BackendSource,
    stacks: Vec<GpuStackPresence>,
) -> AcceleratorReport {
    let mut warnings = Vec::new();

    if let Some(stack_id) = stack_id_for_backend(backend) {
        let named = stacks
            .iter()
            .filter(|s| s.stack == stack_id)
            .any(|s| !s.devices.is_empty());
        if !named {
            let slug = accelerator_slug(backend);
            warnings.push(format!(
                "backend is {slug} but no GPU name could be detected for stack \
                 '{stack_id}' (is the vendor tool on PATH and a device visible?)"
            ));
        }
    }

    AcceleratorReport {
        backend,
        backend_source,
        stacks,
        warnings,
    }
}

/// Log via tracing (server / inferio startup).
pub fn log_report(settings: &Settings) {
    let report = build_report(settings);
    let devices: Vec<String> = if is_gpu_backend(report.backend) {
        report
            .selected_devices()
            .iter()
            .map(|d| format!("{}:{}", d.stack, d.label()))
            .collect()
    } else {
        report
            .stacks
            .iter()
            .flat_map(|s| s.devices.iter())
            .map(|d| format!("{}:{}", d.stack, d.label()))
            .collect()
    };
    let slug = accelerator_slug(report.backend);
    if is_gpu_backend(report.backend) {
        tracing::info!(
            backend = slug,
            backend_source = %report.backend_source.label(),
            devices = ?devices,
            "accelerator backend"
        );
    } else {
        tracing::info!(
            backend = slug,
            backend_source = %report.backend_source.label(),
            devices = ?devices,
            "accelerator backend: using CPU"
        );
    }
    for w in &report.warnings {
        tracing::warn!("{w}");
    }
}

/// Print to stdout (`panoptikon accelerator`).
pub fn print_report(settings: &Settings) {
    println!("{}", build_report(settings).format_text());
}

// --- GPU stack probes (append new stacks to the list) -------------------------

type StackProbeFn = fn() -> Option<GpuStackPresence>;

const GPU_STACK_PROBES: &[StackProbeFn] = &[probe_nvidia_stack, probe_amd_rocm_stack];

fn probe_gpu_stacks() -> Vec<GpuStackPresence> {
    GPU_STACK_PROBES.iter().filter_map(|p| p()).collect()
}

fn probe_nvidia_stack() -> Option<GpuStackPresence> {
    let mut evidence = Vec::new();
    if which("nvidia-smi").is_some() {
        evidence.push("nvidia-smi on PATH");
    }
    if cfg!(target_os = "linux") && std::path::Path::new("/proc/driver/nvidia").exists() {
        evidence.push("/proc/driver/nvidia exists");
    }
    if cfg!(windows)
        && std::env::var_os("SystemRoot")
            .map(|root| {
                PathBuf::from(root)
                    .join("System32/nvidia-smi.exe")
                    .is_file()
            })
            .unwrap_or(false)
    {
        evidence.push(r"System32\nvidia-smi.exe exists");
    }
    if evidence.is_empty() {
        return None;
    }
    Some(GpuStackPresence {
        stack: "nvidia",
        backend: Accelerator::Cuda,
        devices: nvidia_devices(),
        evidence: evidence.join("; "),
    })
}

fn probe_amd_rocm_stack() -> Option<GpuStackPresence> {
    if !cfg!(target_os = "linux") {
        return None;
    }
    let kfd_gpus = crate::inferio::gpu::rocm_topology_gpus();
    let mut evidence = Vec::new();
    if std::path::Path::new("/opt/rocm").is_dir() {
        evidence.push("/opt/rocm exists");
    }
    if which("rocm-smi").is_some() {
        evidence.push("rocm-smi on PATH");
    }
    if which("rocminfo").is_some() {
        evidence.push("rocminfo on PATH");
    }
    if !kfd_gpus.is_empty() {
        evidence.push("KFD topology lists a GPU");
    }
    if evidence.is_empty() {
        return None;
    }
    Some(GpuStackPresence {
        stack: "amd-rocm",
        backend: Accelerator::Rocm,
        devices: amd_devices(&kfd_gpus),
        evidence: evidence.join("; "),
    })
}

/// One device per KFD GPU node, named by ISA as the GPU inventory names it;
/// a GPU this process cannot open, which the inventory leaves out, says so.
fn amd_devices(kfd_gpus: &[(String, bool)]) -> Vec<GpuDevice> {
    kfd_gpus
        .iter()
        .map(|(gfx, openable)| GpuDevice {
            stack: "amd-rocm",
            name: if *openable {
                format!("AMD {gfx}")
            } else {
                format!("AMD {gfx} (not openable by this process)")
            },
            compute_cap: None,
        })
        .collect()
}

fn nvidia_devices() -> Vec<GpuDevice> {
    let Some(bin) = which("nvidia-smi") else {
        return Vec::new();
    };
    // `compute_cap` needs a reasonably recent driver (R470+); fall back to a
    // name-only query rather than losing the device list on old drivers.
    for fields in ["name,compute_cap", "name"] {
        let query = format!("--query-gpu={fields}");
        let output = match Command::new(&bin)
            .args([query.as_str(), "--format=csv,noheader"])
            .output()
        {
            Ok(o) if o.status.success() => o,
            _ => continue,
        };
        let devices = parse_nvidia_query_lines(&String::from_utf8_lossy(&output.stdout));
        if !devices.is_empty() {
            return devices;
        }
    }
    Vec::new()
}

/// Parse `nvidia-smi --query-gpu=name[,compute_cap] --format=csv,noheader`
/// lines. The capability field is kept only when it looks numeric —
/// nvidia-smi reports `[N/A]` / `[Not Supported]` shapes on odd setups.
fn parse_nvidia_query_lines(text: &str) -> Vec<GpuDevice> {
    text.lines()
        .filter_map(|line| {
            let (name, cap) = match line.rsplit_once(',') {
                Some((n, c)) => (n.trim(), c.trim()),
                None => (line.trim(), ""),
            };
            if name.is_empty() {
                return None;
            }
            let compute_cap = cap
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_digit())
                .then(|| cap.to_string());
            Some(GpuDevice {
                stack: "nvidia",
                name: name.to_string(),
                compute_cap,
            })
        })
        .collect()
}

fn which(name: &str) -> Option<PathBuf> {
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths).find_map(|dir| {
            let candidate = dir.join(name);
            if candidate.is_file() {
                return Some(candidate);
            }
            let with_exe = dir.join(format!("{name}.exe"));
            with_exe.is_file().then_some(with_exe)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_stacks() -> Vec<GpuStackPresence> {
        Vec::new()
    }

    fn nvidia_named() -> Vec<GpuStackPresence> {
        vec![GpuStackPresence {
            stack: "nvidia",
            backend: Accelerator::Cuda,
            devices: vec![GpuDevice {
                stack: "nvidia",
                name: "Test GPU".into(),
                compute_cap: Some("8.6".into()),
            }],
            evidence: "test".into(),
        }]
    }

    #[test]
    fn slugs_are_stable() {
        assert_eq!(accelerator_slug(Accelerator::Cpu), "cpu");
        assert_eq!(accelerator_slug(Accelerator::Cuda), "cuda");
        assert_eq!(accelerator_slug(Accelerator::Rocm), "rocm");
        assert_eq!(accelerator_slug(Accelerator::Mps), "mps");
        assert_eq!(accelerator_slug(Accelerator::Auto), "auto");
    }

    #[test]
    fn format_text_cpu_no_devices() {
        let report = assemble_report(
            Accelerator::Cpu,
            BackendSource::ConfigOrProbe {
                evidence: "no NVIDIA or ROCm evidence found".into(),
            },
            empty_stacks(),
        );
        let text = report.format_text();
        assert!(text.contains("accelerator backend: cpu"), "{text}");
        assert!(
            text.contains("using CPU (no GPU accelerator selected)"),
            "{text}"
        );
        assert!(!text.contains("warning:"), "{text}");
        assert!(!text.contains("none detected"), "{text}");
        assert!(report.warnings.is_empty());
    }

    #[test]
    fn format_text_cuda_with_device() {
        let report = assemble_report(
            Accelerator::Cuda,
            BackendSource::ConfigOrProbe {
                evidence: "explicitly configured".into(),
            },
            nvidia_named(),
        );
        let text = report.format_text();
        assert!(text.contains("backend: cuda"), "{text}");
        assert!(text.contains("[nvidia] Test GPU (CC 8.6)"), "{text}");
        assert!(report.warnings.is_empty());
    }

    #[test]
    fn format_text_rocm_lists_only_selected_stack_devices() {
        let stacks = vec![
            GpuStackPresence {
                stack: "amd-rocm",
                backend: Accelerator::Rocm,
                devices: amd_devices(&[("gfx1100".into(), true), ("gfx1030".into(), false)]),
                evidence: "test".into(),
            },
            // Unrelated stack should not appear under selected ROCm devices.
            GpuStackPresence {
                stack: "nvidia",
                backend: Accelerator::Cuda,
                devices: vec![GpuDevice {
                    stack: "nvidia",
                    name: "Should Not Appear".into(),
                    compute_cap: None,
                }],
                evidence: "test".into(),
            },
        ];
        let report = assemble_report(Accelerator::Rocm, BackendSource::InstalledVenv, stacks);
        let text = report.format_text();
        assert!(text.contains("backend: rocm"), "{text}");
        assert!(text.contains("[amd-rocm] AMD gfx1100\n"), "{text}");
        assert!(
            text.contains("[amd-rocm] AMD gfx1030 (not openable by this process)"),
            "{text}"
        );
        assert!(!text.contains("Should Not Appear"), "{text}");
        assert!(!text.contains("using CPU"), "{text}");
        assert!(report.warnings.is_empty());
    }

    #[test]
    fn cuda_without_devices_warns() {
        let report = assemble_report(
            Accelerator::Cuda,
            BackendSource::InstalledVenv,
            empty_stacks(),
        );
        assert_eq!(report.backend, Accelerator::Cuda);
        assert_eq!(report.warnings.len(), 1);
        assert!(report.warnings[0].contains("cuda"));
        assert!(report.format_text().contains("warning:"));
    }

    #[test]
    fn rocm_without_devices_warns() {
        let report = assemble_report(
            Accelerator::Rocm,
            BackendSource::InstalledVenv,
            empty_stacks(),
        );
        assert_eq!(report.backend, Accelerator::Rocm);
        assert!(report.warnings.iter().any(|w| w.contains("rocm")));
    }

    #[test]
    fn cpu_with_host_gpu_is_not_a_warning() {
        let report = assemble_report(
            Accelerator::Cpu,
            BackendSource::ConfigOrProbe {
                evidence: "explicitly configured".into(),
            },
            nvidia_named(),
        );
        assert!(report.warnings.is_empty(), "{:?}", report.warnings);
        let text = report.format_text();
        assert!(text.contains("using CPU"), "{text}");
        assert!(text.contains("not selected"), "{text}");
        assert!(text.contains("Test GPU"), "{text}");
        assert!(!text.contains("warning:"), "{text}");
    }

    #[test]
    fn installed_venv_wins_over_config() {
        let (backend, source) = resolve_backend_from(Accelerator::Cuda, Some(Accelerator::Rocm));
        assert_eq!(backend, Accelerator::Rocm);
        assert_eq!(source, BackendSource::InstalledVenv);
    }

    #[test]
    fn explicit_config_used_when_no_sentinel() {
        let (backend, source) = resolve_backend_from(Accelerator::Cpu, None);
        assert_eq!(backend, Accelerator::Cpu);
        assert!(matches!(source, BackendSource::ConfigOrProbe { .. }));
    }

    #[test]
    fn nvidia_query_parses_name_and_compute_cap() {
        let devices = parse_nvidia_query_lines(
            "NVIDIA GeForce RTX 5090, 12.0\nNVIDIA GeForce GTX 1080, [N/A]\n",
        );
        assert_eq!(devices.len(), 2);
        assert_eq!(devices[0].name, "NVIDIA GeForce RTX 5090");
        assert_eq!(devices[0].compute_cap.as_deref(), Some("12.0"));
        assert_eq!(devices[0].label(), "NVIDIA GeForce RTX 5090 (CC 12.0)");
        assert_eq!(devices[1].name, "NVIDIA GeForce GTX 1080");
        assert_eq!(devices[1].compute_cap, None);
        assert_eq!(devices[1].label(), "NVIDIA GeForce GTX 1080");
    }

    #[test]
    fn nvidia_query_parses_name_only_fallback() {
        let devices = parse_nvidia_query_lines("NVIDIA GeForce RTX 3060\n\n");
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0].name, "NVIDIA GeForce RTX 3060");
        assert_eq!(devices[0].compute_cap, None);
    }

    #[test]
    fn stack_id_mapping() {
        assert_eq!(stack_id_for_backend(Accelerator::Cuda), Some("nvidia"));
        assert_eq!(stack_id_for_backend(Accelerator::Rocm), Some("amd-rocm"));
        assert_eq!(stack_id_for_backend(Accelerator::Cpu), None);
        assert_eq!(stack_id_for_backend(Accelerator::Mps), None);
        assert!(!is_gpu_backend(Accelerator::Cpu));
        assert!(is_gpu_backend(Accelerator::Cuda));
        assert!(
            is_gpu_backend(Accelerator::Mps),
            "a GPU with no stack to probe"
        );
    }

    /// An Apple Silicon host is not a CPU host, and the absence of a vendor
    /// tool that could name its device is not a missing driver.
    #[test]
    fn format_text_mps_is_not_reported_as_cpu() {
        let report = assemble_report(
            Accelerator::Mps,
            BackendSource::InstalledVenv,
            empty_stacks(),
        );
        let text = report.format_text();
        assert!(text.contains("accelerator backend: mps"), "{text}");
        assert!(
            text.contains("GPU device: the one this OS provides"),
            "{text}"
        );
        assert!(!text.contains("using CPU"), "{text}");
        assert!(!text.contains("none detected"), "{text}");
        assert!(report.warnings.is_empty(), "{:?}", report.warnings);
    }
}
