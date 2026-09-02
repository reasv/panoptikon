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
//! **Warnings:** only when a *GPU* backend is selected but no device name is
//! found. **CPU is never a warning** — it is reported as using CPU.

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
            if devices.is_empty() {
                lines.push("GPU devices: (none detected)".into());
            } else {
                lines.push("GPU devices:".into());
                for d in devices {
                    lines.push(format!("  - [{}] {}", d.stack, d.label()));
                }
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

/// Whether this backend is a GPU stack (may warn if devices are missing).
pub fn is_gpu_backend(a: Accelerator) -> bool {
    matches!(a, Accelerator::Cuda | Accelerator::Rocm)
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
    if evidence.is_empty() {
        return None;
    }
    Some(GpuStackPresence {
        stack: "amd-rocm",
        backend: Accelerator::Rocm,
        devices: amd_device_names()
            .into_iter()
            .map(|name| GpuDevice {
                stack: "amd-rocm",
                name,
                compute_cap: None,
            })
            .collect(),
        evidence: evidence.join("; "),
    })
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

fn amd_device_names() -> Vec<String> {
    if let Some(names) = rocm_smi_product_names()
        && !names.is_empty()
    {
        return names;
    }
    rocminfo_marketing_names()
}

fn rocm_smi_product_names() -> Option<Vec<String>> {
    let bin = which("rocm-smi")?;
    let output = Command::new(bin)
        .args(["--showproductname"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let mut names = Vec::new();
    for line in text.lines() {
        let lower = line.to_ascii_lowercase();
        for key in ["card series:", "card model:"] {
            if let Some(idx) = lower.find(key) {
                let v = line[idx + key.len()..].trim();
                if !v.is_empty() {
                    names.push(v.to_string());
                }
            }
        }
    }
    Some(names)
}

fn rocminfo_marketing_names() -> Vec<String> {
    let Some(bin) = which("rocminfo") else {
        return Vec::new();
    };
    let output = match Command::new(bin).output() {
        Ok(o) if o.status.success() => o,
        _ => return Vec::new(),
    };
    parse_rocminfo_gpu_marketing_names(&String::from_utf8_lossy(&output.stdout))
}

/// Collect marketing names only for **GPU** agents.
///
/// `rocminfo` prints a `Marketing Name` for every HSA agent, including the
/// host CPU (usually listed first). Naively taking every Marketing Name line
/// reports "CPU then GPU" under the ROCm stack even when only the GPU is used
/// for inference. Fields may appear in either order within an agent block;
/// agent blocks are separated by lines of asterisks.
fn parse_rocminfo_gpu_marketing_names(text: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut marketing: Option<String> = None;
    let mut device_type: Option<String> = None;

    let flush = |marketing: &mut Option<String>,
                 device_type: &mut Option<String>,
                 names: &mut Vec<String>| {
        let name = marketing.take();
        let dtype = device_type.take();
        if let (Some(name), Some(dtype)) = (name, dtype)
            && dtype.eq_ignore_ascii_case("GPU")
            && !name.is_empty()
            && !name.eq_ignore_ascii_case("N/A")
        {
            names.push(name);
        }
    };

    for line in text.lines() {
        let t = line.trim();
        // Agent separator: "*******" (rocminfo) between Agent blocks.
        if !t.is_empty() && t.chars().all(|c| c == '*') {
            flush(&mut marketing, &mut device_type, &mut names);
            continue;
        }
        if let Some(rest) = t.strip_prefix("Marketing Name:") {
            marketing = Some(rest.trim().to_string());
            continue;
        }
        if let Some(rest) = t.strip_prefix("Device Type:") {
            device_type = Some(rest.trim().to_string());
            continue;
        }
    }
    flush(&mut marketing, &mut device_type, &mut names);
    names
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
                devices: vec![GpuDevice {
                    stack: "amd-rocm",
                    name: "Radeon RX 7900 XTX".into(),
                    compute_cap: None,
                }],
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
        assert!(text.contains("[amd-rocm] Radeon RX 7900 XTX"), "{text}");
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
        assert!(!is_gpu_backend(Accelerator::Mps));
        assert!(!is_gpu_backend(Accelerator::Cpu));
        assert!(is_gpu_backend(Accelerator::Cuda));
    }

    /// Minimal rocminfo-shaped output: CPU agent first, then GPU (real tools
    /// list both Marketing Names; we must keep only Device Type GPU).
    #[test]
    fn rocminfo_skips_cpu_agent_marketing_name() {
        let sample = r#"
ROCm System Management Interface
===============================
*******                      
Agent 1                      
*******                      
  Name:                    AMD Ryzen 9 7950X 16-Core Processor
  Uuid:                    CPU-XX                             
  Marketing Name:          AMD Ryzen 9 7950X 16-Core Processor
  Vendor Name:             CPU                                
  Feature:                 None specified                     
  Profile:                 FULL_PROFILE                       
  Float Round Mode:        NEAR                               
  Max Queue Number:         0(0x0)                             
  Queue Min Size:           0(0x0)                             
  Queue Max Size:           0(0x0)                             
  Queue Type:              MULTI                              
  Node:                    0                                  
  Device Type:             CPU                                
*******                      
Agent 2                      
*******                      
  Name:                    gfx1100                            
  Uuid:                    GPU-XX                             
  Marketing Name:          Radeon RX 7900 XTX                 
  Vendor Name:             AMD                                
  Feature:                 KERNEL_DISPATCH                    
  Profile:                 BASE_PROFILE                       
  Float Round Mode:        NEAR                               
  Max Queue Number:         128(0x80)                          
  Queue Min Size:           64(0x40)                           
  Queue Max Size:           131072(0x20000)                    
  Queue Type:              MULTI                              
  Node:                    1                                  
  Device Type:             GPU                                
"#;
        let names = parse_rocminfo_gpu_marketing_names(sample);
        assert_eq!(names, vec!["Radeon RX 7900 XTX".to_string()]);
    }

    /// Device Type may appear before Marketing Name within an agent block.
    #[test]
    fn rocminfo_accepts_device_type_before_marketing_name() {
        let sample = r#"
*******
Agent 1
*******
  Device Type:             GPU
  Marketing Name:          Radeon RX 6800 XT
*******
Agent 2
*******
  Device Type:             CPU
  Marketing Name:          Some CPU
"#;
        let names = parse_rocminfo_gpu_marketing_names(sample);
        assert_eq!(names, vec!["Radeon RX 6800 XT".to_string()]);
    }

    #[test]
    fn rocminfo_drops_na_and_empty_gpu_names() {
        let sample = r#"
*******
  Device Type:             GPU
  Marketing Name:          N/A
*******
  Marketing Name:          
  Device Type:             GPU
*******
  Marketing Name:          Real GPU
  Device Type:             GPU
"#;
        let names = parse_rocminfo_gpu_marketing_names(sample);
        assert_eq!(names, vec!["Real GPU".to_string()]);
    }
}
