//! GPU identity enumeration and worker→GPU pin resolution.
//!
//! Budgets are keyed by GPU UUID (`GPU-…`), never by a device index, which
//! moves across reboots and `CUDA_VISIBLE_DEVICES` changes. [`probe`]
//! dispatches on the resolved accelerator: `Rocm` to `rocm.rs`, `Mps` and
//! `Cpu` to one synthetic device each, `Cuda`/`Auto` to nvidia-smi.
//! See docs/batch-calibration-design.md "Two keyspaces".
//!
//! On CUDA, one `--query-gpu` call yields both identities and capabilities
//! (`capability.rs`), matched by row, so the two views always agree. Any
//! unparseable identity makes the whole result unknown, and unknown leaves
//! pins untouched.

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::capability::{HostComputeCaps, find_nvidia_smi, output_with_timeout, parse_compute_cap};
use super::cpu;
use super::mps;
use super::rocm;
use super::worker::WorkerSpawnConfig;
use crate::config::Accelerator;

/// CUDA's device filter (and HIP's alias for its own). Takes a `GPU-…` UUID.
pub const CUDA_PIN_ENV_VAR: &str = "CUDA_VISIBLE_DEVICES";

/// HIP's device filter. Takes a device index, never a UUID; composes with an
/// ambient `ROCR_VISIBLE_DEVICES`, which filters below it.
pub const HIP_PIN_ENV_VAR: &str = "HIP_VISIBLE_DEVICES";

/// Set on a worker pinned to a unified ROCm GPU so its memory arithmetic
/// includes GTT. The value is the GPU's PCI address, which the worker checks
/// against the GPU it resolved, falling back to discrete arithmetic on a
/// mismatch.
pub const UNIFIED_GPU_ENV_VAR: &str = "PANOPTIKON_UNIFIED_GPU";

/// Set on a worker on an NVIDIA GPU: `1` when a full allocation there spills
/// to system RAM ([`GpuInventory::spill_verdict`]), `0` when it fails.
pub const SPILLS_TO_RAM_ENV_VAR: &str = "PANOPTIKON_SPILLS_TO_RAM";

/// Written next to the visibility variable with the same pin, so the worker
/// can tell our pin from an operator's ambient one
/// (`memory.py::pinned_device_missing`).
pub const DEVICE_PIN_MARKER_ENV_VAR: &str = "PANOPTIKON_DEVICE_PIN";

/// The one variable a resolved pin is written to, chosen by the resolved
/// accelerator (a ROCm host with no GPUs found is still a HIP host).
pub fn pin_env_var(accelerator: Accelerator) -> &'static str {
    match accelerator {
        Accelerator::Rocm => HIP_PIN_ENV_VAR,
        // `Mps`/`Cpu` never yield a pin.
        Accelerator::Cuda | Accelerator::Cpu | Accelerator::Mps | Accelerator::Auto => {
            CUDA_PIN_ENV_VAR
        }
    }
}

/// One visible GPU, from nvidia-smi (CUDA) or KFD topology (ROCm).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct GpuInfo {
    /// nvidia-smi or HIP device index, for `devices = ["3"]` pins; not unique.
    pub index: u32,
    /// GPU UUID (`GPU-…`; on ROCm possibly a synthetic `GPU-BDF-…`), the ledger key.
    pub uuid: String,
    /// Marketing name, e.g. `NVIDIA GeForce RTX 5090`; `AMD gfx…` on ROCm.
    pub name: String,
    pub total_mb: u64,
    /// Compute capability as `major.minor` (`"12.0"`); `None` if unreported or ROCm.
    pub compute_cap: Option<String>,
    /// PCI address `dddd:bb:dd.f`, the key into amdgpu sysfs. ROCm only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bdf: Option<String>,
    /// KFD's packed ISA target (`110000` = gfx1100). ROCm only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gfx_target_version: Option<u32>,
    /// Host RAM in MiB behind a unified GPU; `Some` exactly on unified GPUs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unified_ram_mb: Option<u64>,
    /// Device-local (non-GTT) VRAM of a unified ROCm GPU, in MiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vram_carveout_mb: Option<u64>,
}

impl GpuInfo {
    /// Whether this GPU's memory is host RAM rather than private VRAM.
    pub fn unified(&self) -> bool {
        self.unified_ram_mb.is_some()
    }

    /// Capacity used to rank GPUs for default placement: `max(carve-out,
    /// total / 8)` on a unified ROCm GPU, `total_mb` otherwise. Not used for
    /// pricing. See docs/unified-memory-admission.md "Backend B".
    pub fn placement_total_mb(&self) -> u64 {
        match self.vram_carveout_mb {
            Some(carveout) => carveout.max(self.total_mb / 8),
            None => self.total_mb,
        }
    }

    /// `major * 10 + minor`, or `None` when unknown (never 0, so unknown is
    /// unranked rather than slowest).
    fn cap_tenths(&self) -> Option<u32> {
        parse_compute_cap(self.compute_cap.as_deref()?).map(|(major, minor)| major * 10 + minor)
    }

    /// Architecture, the calibration profile key: `sm_<major><minor>` on CUDA,
    /// `gfx…` on ROCm, spelled as the worker derives it from torch. `None` on
    /// MPS, CPU and when the driver reports no compute capability; the ledger
    /// then takes it from the first load report.
    pub fn arch(&self) -> Option<String> {
        if let Some(target) = self.gfx_target_version {
            return super::rocm::gfx_name(target);
        }
        let (major, minor) = parse_compute_cap(self.compute_cap.as_deref()?)?;
        Some(format!("sm_{major}{minor}"))
    }
}

/// The visible GPUs, or `None` when unknown (no nvidia-smi, probe failure,
/// unparseable output), plus the backend they were read through. Cheap to
/// clone.
#[derive(Debug, Clone, Default)]
pub struct GpuInventory {
    gpus: Option<Arc<[GpuInfo]>>,
    /// The rows nvidia-smi reported when an ambient mask we cannot map left
    /// `gpus` unknown. The ledger admits one only when a worker's load report
    /// names its UUID.
    adoptable: Option<Arc<[GpuInfo]>>,
    /// Adoptable rows the ledger has admitted, shared by every clone.
    adopted: Arc<Mutex<Vec<GpuInfo>>>,
    /// The accelerators' backend; the CPU device uses `cpu_roots` instead.
    backend: MemoryBackend,
    /// CPU device RAM statistics roots; `Some` iff there is a CPU device.
    cpu_roots: Option<cpu::MemRoots>,
    /// The visibility variable is set and empty (`CUDA_VISIBLE_DEVICES=`): no
    /// GPU is visible, no pin may be written, everything runs on the CPU.
    blank_mask: bool,
}

/// Which interface answers live-memory queries and which pin vocabulary
/// applies, set from the resolved accelerator.
#[derive(Debug, Clone)]
enum MemoryBackend {
    NvidiaSmi {
        /// The GPUs (by UUID) that move memory to system RAM when full.
        spilling: Arc<[String]>,
    },
    RocmSysfs {
        /// The PCI device root the probe read, reused by the refresh.
        pci_devices: PathBuf,
        /// `/proc/meminfo`; read only for unified GPUs, which clamp unclaimed
        /// GTT to `MemAvailable`.
        meminfo: PathBuf,
        /// A HIP-layer visibility variable (`HIP_VISIBLE_DEVICES`,
        /// `CUDA_VISIBLE_DEVICES`, `GPU_DEVICE_ORDINAL`) was set at probe
        /// time, so no pin of ours is written. `ROCR_VISIBLE_DEVICES` is not.
        ambient_hip_restriction: bool,
    },
    /// Apple Silicon: one synthetic unified-memory device (`mps.rs`). No pins.
    Mps,
    /// No accelerator; only the CPU device. No pins.
    Cpu,
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::NvidiaSmi {
            spilling: Arc::default(),
        }
    }
}

/// Everything one `nvidia-smi` call tells us about this host's GPUs.
pub struct HostGpus {
    /// Compute-capability floors for `/metadata` availability filtering.
    pub caps: HostComputeCaps,
    /// GPU identities for worker→GPU pinning and the per-GPU ledger.
    pub inventory: GpuInventory,
}

/// ISA names of the GPUs in the KFD topology (`gfx1100`), for the startup
/// accelerator report; empty without amdgpu.
pub fn rocm_topology_gfx_names() -> Vec<String> {
    rocm::topology_gfx_names(&rocm::SysfsRoots::default().kfd_nodes)
}

/// Probe once at startup; never fails. `accelerator` must be the resolved
/// one. The CPU device is added on every host whose RAM can be read.
pub fn probe(accelerator: Accelerator) -> HostGpus {
    let host = match accelerator {
        Accelerator::Rocm => probe_rocm(),
        Accelerator::Mps => probe_mps(),
        Accelerator::Cpu => probe_cpu(),
        Accelerator::Cuda | Accelerator::Auto => {
            // nvidia-smi ignores CUDA_VISIBLE_DEVICES, so it is applied here.
            let visible = std::env::var("CUDA_VISIBLE_DEVICES").ok();
            let mut host = build(query(accelerator).as_deref(), visible.as_deref());
            let platform = DriverPlatform::current(Path::new(WSL_GPU_DEVICE));
            let models = (platform == DriverPlatform::Windows)
                .then(query_driver_models)
                .flatten();
            host.inventory.set_spilling(platform, models.as_deref());
            if !host.inventory.spilling_gpus().is_empty() {
                tracing::warn!(
                    "with the NVIDIA driver's default \"CUDA - Sysmem Fallback Policy\", a GPU \
                     that runs out of memory silently uses system RAM instead of failing, and \
                     batch-size calibration can briefly exceed GPU memory, so inference can run \
                     several times slower; on the Windows host, set it to \"Prefer No Sysmem \
                     Fallback\" in NVIDIA Control Panel > Manage 3D Settings"
                );
            }
            host
        }
    };
    with_cpu_device(host)
}

/// The GPU device WSL2 and Docker Desktop expose: the Windows display driver.
const WSL_GPU_DEVICE: &str = "/dev/dxg";

/// Where the NVIDIA driver runs, for [`spills`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DriverPlatform {
    /// Native Windows: each GPU has its own driver model.
    Windows,
    /// Linux under WSL2 or Docker Desktop: every GPU goes through the Windows
    /// display driver.
    Wsl,
    /// Any other host.
    Other,
}

impl DriverPlatform {
    /// This host's platform; on Linux `dxg` is the WSL GPU device to look for.
    fn current(dxg: &Path) -> Self {
        if cfg!(windows) {
            Self::Windows
        } else if cfg!(target_os = "linux") && dxg.exists() {
            Self::Wsl
        } else {
            Self::Other
        }
    }
}

/// Whether a full NVIDIA GPU moves memory to system RAM instead of failing
/// the allocation. The Windows display driver model (WDDM) does; TCC and
/// MCDM, the compute-only models, fail it. A driver model nvidia-smi did not
/// report counts as WDDM, the model every display GPU runs.
fn spills(platform: DriverPlatform, driver_model: Option<&str>) -> bool {
    match platform {
        DriverPlatform::Other => false,
        DriverPlatform::Wsl => true,
        DriverPlatform::Windows => !driver_model.is_some_and(|model| {
            let model = model.trim();
            model.eq_ignore_ascii_case("TCC") || model.eq_ignore_ascii_case("MCDM")
        }),
    }
}

/// Each GPU's current driver model (native Windows only). `None` on any
/// failure, which [`spills`] reads as WDDM.
fn query_driver_models() -> Option<String> {
    let mut cmd = Command::new(find_nvidia_smi()?);
    cmd.args([
        "--query-gpu=uuid,driver_model.current",
        "--format=csv,noheader",
    ]);
    let output = output_with_timeout(cmd, Duration::from_secs(5))?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).into_owned())
}

/// `uuid, driver model` rows; unparseable rows are skipped.
fn parse_driver_models(stdout: &str) -> HashMap<&str, &str> {
    stdout
        .lines()
        .filter_map(|line| line.split_once(','))
        .map(|(uuid, model)| (uuid.trim(), model.trim()))
        .collect()
}

/// Append the CPU device after the accelerators, so a CPU worker on any host
/// is priced against RAM (`cpu.rs`).
fn with_cpu_device(mut host: HostGpus) -> HostGpus {
    let roots = cpu::MemRoots::default();
    let Some(ram_mb) = cpu::probe(&roots) else {
        tracing::warn!(
            "this host's total RAM could not be read, so it gets no CPU \
             device: a worker that runs on the CPU gets no memory ledger, no \
             grants and no calibration — dispatch takes the unpriced path \
             (your cap, then the registry default, then default_max_batch)"
        );
        return host;
    };
    let gpu = cpu::gpu(ram_mb);
    tracing::info!(
        uuid = %gpu.uuid,
        name = %gpu.name,
        total_mb = gpu.total_mb,
        // A configured `cap_fraction` overrides this later, in the ledger.
        default_cap_fraction = cpu::DEFAULT_CAP_FRACTION,
        accelerators = host.inventory.gpus().map_or(0, <[GpuInfo]>::len),
        "admitting batches from workers that run on the CPU against system \
         RAM (running out of it is an OS process kill rather than a catchable \
         allocation failure, so the device ships with a default ceiling)"
    );
    host.inventory = host.inventory.with_cpu(ram_mb, roots);
    host
}

/// KFD topology + amdgpu sysfs (`rocm.rs`). Capabilities are always unknown;
/// off Linux there are no GPUs. The backend is `RocmSysfs` on every path.
fn probe_rocm() -> HostGpus {
    let roots = rocm::SysfsRoots::default();
    let blank = if cfg!(target_os = "linux") {
        let ambient = rocm::VISIBILITY_VARS.map(|var| std::env::var(var).ok());
        rocm::blank_visibility_var(ambient.each_ref().map(Option::as_deref))
    } else {
        None
    };
    if let Some(var) = blank {
        tracing::info!(
            variable = var,
            "{var} is set and names no device, which is how the runtime is \
             told to expose no GPU at all — every worker spawned here inherits \
             it, so this host has no GPU devices and its models run on the CPU \
             device and are priced against RAM"
        );
        return HostGpus {
            caps: HostComputeCaps::unknown(),
            inventory: GpuInventory {
                gpus: Some(Vec::new().into()),
                adoptable: None,
                adopted: Arc::default(),
                backend: MemoryBackend::RocmSysfs {
                    pci_devices: roots.pci_devices.clone(),
                    meminfo: roots.meminfo.clone(),
                    ambient_hip_restriction: true,
                },
                cpu_roots: None,
                blank_mask: true,
            },
        };
    }
    let (inventory, ambient_hip_restriction) = if cfg!(target_os = "linux") {
        let ambient = rocm::VISIBILITY_VARS.map(|var| std::env::var(var).ok());
        let ambient = ambient.each_ref().map(Option::as_deref);
        (
            Some(rocm::build(&roots, ambient)),
            rocm::ambient_hip_restriction(ambient),
        )
    } else {
        (None, false)
    };
    let backend = MemoryBackend::RocmSysfs {
        pci_devices: roots.pci_devices.clone(),
        meminfo: roots.meminfo.clone(),
        ambient_hip_restriction,
    };
    let host = |gpus: Option<Arc<[GpuInfo]>>| HostGpus {
        caps: HostComputeCaps::unknown(),
        inventory: GpuInventory {
            gpus,
            adoptable: None,
            adopted: Arc::default(),
            backend: backend.clone(),
            cpu_roots: None,
            blank_mask: false,
        },
    };
    let gpus = match inventory {
        Some(Ok(gpus)) => gpus,
        Some(Err(failure)) => {
            if DriverPlatform::current(Path::new(WSL_GPU_DEVICE)) == DriverPlatform::Wsl {
                tracing::warn!(
                    "ROCm under WSL2 runs through the Windows display driver, which \
                     exposes none of the amdgpu memory counters this host reads: models \
                     on the GPU run without a memory ledger or batch-size calibration, \
                     and a GPU that runs out of memory may move it to system RAM and \
                     slow down instead of failing"
                );
            } else {
                failure.log();
            }
            return host(None);
        }
        None => return host(None),
    };
    for gpu in &gpus {
        tracing::info!(
            index = gpu.index,
            uuid = %gpu.uuid,
            name = %gpu.name,
            total_mb = gpu.total_mb,
            bdf = gpu.bdf.as_deref().unwrap_or("unknown"),
            unified = gpu.unified(),
            vram_carveout_mb = ?gpu.vram_carveout_mb,
            "detected GPU"
        );
    }
    host(Some(gpus.into()))
}

/// One synthetic unified-memory device from macOS sysctls (`mps.rs`). With
/// no answer there are no GPUs, but the backend is still `Mps`.
fn probe_mps() -> HostGpus {
    let inventory = |gpus: Option<Arc<[GpuInfo]>>| HostGpus {
        caps: HostComputeCaps::unknown(),
        inventory: GpuInventory {
            gpus,
            adoptable: None,
            adopted: Arc::default(),
            backend: MemoryBackend::Mps,
            cpu_roots: None,
            blank_mask: false,
        },
    };
    let Some(facts) = mps::probe() else {
        if cfg!(target_os = "macos") {
            tracing::warn!(
                "this host is configured for MPS but the chip and memory size \
                 could not be read from sysctl, so it gets no VRAM ledger, no \
                 grants and no calibration — dispatch takes the unpriced path \
                 (your cap, then the registry default, then default_max_batch)"
            );
        }
        return inventory(None);
    };
    let gpu = mps::gpu(&facts);
    tracing::info!(
        index = gpu.index,
        uuid = %gpu.uuid,
        name = %gpu.name,
        total_mb = gpu.total_mb,
        ram_mb = facts.ram_bytes / (1024 * 1024),
        unified = gpu.unified(),
        "detected GPU (unified memory; the total is the 75% seed until a \
         worker reports the exact recommended-max figure)"
    );
    inventory(Some(vec![gpu].into()))
}

/// A host with no accelerator: only the CPU device [`with_cpu_device`] adds.
fn probe_cpu() -> HostGpus {
    HostGpus {
        caps: HostComputeCaps::unknown(),
        inventory: GpuInventory {
            gpus: None,
            adoptable: None,
            adopted: Arc::default(),
            backend: MemoryBackend::Cpu,
            cpu_roots: None,
            blank_mask: false,
        },
    }
}

/// Run the single query. `None` on any failure, each logged.
fn query(accelerator: Accelerator) -> Option<String> {
    let Some(smi) = find_nvidia_smi() else {
        // `cpu` and `auto` hosts legitimately have no nvidia-smi.
        if accelerator == Accelerator::Cuda {
            tracing::warn!(
                "this host is configured for CUDA but nvidia-smi was not \
                 found on PATH{}; workers will not be pinned, batch sizes \
                 will not be calibrated and model availability will not be \
                 capability-filtered",
                if cfg!(windows) { " or in System32" } else { "" }
            );
        }
        return None;
    };
    let mut cmd = Command::new(smi);
    cmd.args([
        "--query-gpu=index,uuid,name,memory.total,compute_cap",
        "--format=csv,noheader,nounits",
    ]);
    let Some(output) = output_with_timeout(cmd, Duration::from_secs(5)) else {
        tracing::warn!(
            "nvidia-smi GPU probe failed or timed out; workers will not be \
             pinned to a specific GPU, batch sizes will not be calibrated \
             and model availability will not be capability-filtered"
        );
        return None;
    };
    if !output.status.success() {
        tracing::warn!(
            status = %output.status,
            stderr = %String::from_utf8_lossy(&output.stderr).trim(),
            "nvidia-smi exited nonzero; leaving the GPU inventory unknown \
             (workers will not be pinned and batch sizes will not be \
             calibrated)"
        );
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// One GPU's live memory occupancy, from the ledger's staleness refresh.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GpuMemory {
    pub uuid: String,
    pub total_mb: u64,
    pub free_mb: u64,
}

/// How this host's live free/total memory is read. Cheap to clone.
#[derive(Debug, Clone)]
pub(super) enum MemoryQuery {
    /// One `nvidia-smi --query-gpu` call covering every visible GPU.
    NvidiaSmi,
    /// amdgpu's `mem_info_vram_{total,used}` per GPU, plus
    /// `mem_info_gtt_{total,used}` and `MemAvailable` for a unified GPU.
    RocmSysfs {
        pci_devices: PathBuf,
        meminfo: PathBuf,
        /// Every GPU's key, address and unified flag, in inventory order.
        gpus: Arc<[rocm::GpuRef]>,
    },
    /// macOS RAM statistics for the one unified-memory device (`mps.rs`).
    Mps {
        key: String,
        /// Physical RAM in MiB; bounds the reading, not the admission total.
        ram_mb: u64,
    },
    /// Host RAM statistics for the CPU device (`cpu.rs`).
    Cpu {
        key: String,
        /// Physical RAM in MiB, also the device total.
        ram_mb: u64,
        roots: cpu::MemRoots,
    },
    /// No refresh: [`Self::run`] returns `None` and the ledger keeps its
    /// readings (never a partial refresh).
    Unavailable,
}

impl MemoryQuery {
    /// Live free/total memory for every GPU the ledger knows. `None` on any
    /// failure. Blocking: callers run it under `spawn_blocking`.
    pub fn run(&self) -> Option<Vec<GpuMemory>> {
        match self {
            Self::NvidiaSmi => query_memory_nvidia_smi(),
            Self::RocmSysfs {
                pci_devices,
                meminfo,
                gpus,
            } => rocm::query_memory(pci_devices, meminfo, gpus),
            Self::Mps { key, ram_mb } => mps::query_memory(key, *ram_mb),
            Self::Cpu { key, ram_mb, roots } => cpu::query_memory(key, *ram_mb, roots),
            Self::Unavailable => None,
        }
    }

    /// The source label the ledger records readings under. All are
    /// device-wide; `"mps"` and `"ram"` must match the worker's own labels.
    pub fn free_source(&self) -> &'static str {
        match self {
            Self::NvidiaSmi => "nvidia-smi",
            Self::Mps { .. } => "mps",
            Self::Cpu { .. } => "ram",
            Self::RocmSysfs { .. } | Self::Unavailable => "amdgpu-sysfs",
        }
    }
}

/// One `nvidia-smi` call for every GPU; `None` on any failure.
fn query_memory_nvidia_smi() -> Option<Vec<GpuMemory>> {
    let smi = find_nvidia_smi()?;
    let mut cmd = Command::new(smi);
    cmd.args([
        "--query-gpu=uuid,memory.total,memory.free",
        "--format=csv,noheader,nounits",
    ]);
    let output = output_with_timeout(cmd, Duration::from_secs(5))?;
    if !output.status.success() {
        return None;
    }
    parse_memory(&String::from_utf8_lossy(&output.stdout))
}

/// One GPU per line, `uuid, total, free`. Any unparseable row makes the whole
/// reading unknown, since a missing GPU would read as fully free.
fn parse_memory(stdout: &str) -> Option<Vec<GpuMemory>> {
    let mut gpus = Vec::new();
    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split(',');
        let uuid = fields.next()?.trim().to_owned();
        let total_mb = fields.next()?.trim().parse::<u64>().ok()?;
        let free_mb = fields.next()?.trim().parse::<u64>().ok()?;
        if fields.next().is_some() || !is_uuid_pin(&uuid) {
            return None;
        }
        gpus.push(GpuMemory {
            uuid,
            total_mb,
            free_mb,
        });
    }
    if gpus.is_empty() { None } else { Some(gpus) }
}

/// Turn probe output plus the ambient `CUDA_VISIBLE_DEVICES` into both views.
/// A mask we cannot map leaves the inventory unknown (with the reported rows
/// adoptable) but keeps the capability view, so capability gates still
/// apply; a mask that resolves narrows both.
fn build(stdout: Option<&str>, visible: Option<&str>) -> HostGpus {
    let Some(gpus) = stdout.and_then(parse_inventory) else {
        return HostGpus {
            caps: HostComputeCaps::unknown(),
            inventory: GpuInventory::default(),
        };
    };
    let all_caps = caps_of(&gpus);
    let gpus = match restrict_to_visible(gpus, visible) {
        Visible::Resolved(gpus) => gpus,
        // Known empty, not unknown.
        Visible::Blank => {
            return HostGpus {
                caps: HostComputeCaps::unknown(),
                inventory: GpuInventory {
                    gpus: Some(Vec::new().into()),
                    adoptable: None,
                    adopted: Arc::default(),
                    backend: MemoryBackend::default(),
                    cpu_roots: None,
                    blank_mask: true,
                },
            };
        }
        Visible::Unmapped(reported) => {
            return HostGpus {
                caps: HostComputeCaps::from_caps(all_caps),
                inventory: GpuInventory {
                    gpus: None,
                    adoptable: Some(reported.into()),
                    adopted: Arc::default(),
                    backend: MemoryBackend::default(),
                    cpu_roots: None,
                    blank_mask: false,
                },
            };
        }
    };
    for gpu in &gpus {
        tracing::info!(
            index = gpu.index,
            uuid = %gpu.uuid,
            name = %gpu.name,
            total_mb = gpu.total_mb,
            compute_cap = gpu.compute_cap.as_deref().unwrap_or("unknown"),
            "detected GPU"
        );
    }
    HostGpus {
        caps: HostComputeCaps::from_caps(caps_of(&gpus)),
        inventory: GpuInventory {
            gpus: Some(gpus.into()),
            adoptable: None,
            adopted: Arc::default(),
            backend: MemoryBackend::default(),
            cpu_roots: None,
            blank_mask: false,
        },
    }
}

/// The capabilities of the GPUs that reported one (empty means unknown).
fn caps_of(gpus: &[GpuInfo]) -> Vec<(u32, u32)> {
    gpus.iter()
        .filter_map(|gpu| parse_compute_cap(gpu.compute_cap.as_deref()?))
        .collect()
}

/// What the ambient mask did to the rows nvidia-smi reported.
enum Visible {
    /// The mask is set and names no device (`CUDA_VISIBLE_DEVICES=`): no GPUs.
    Blank,
    /// The mask resolved (or there was none): these are the visible GPUs.
    Resolved(Vec<GpuInfo>),
    /// The mask cannot be mapped; these rows are adoptable. Empty when an
    /// all-UUID mask matched no row.
    Unmapped(Vec<GpuInfo>),
}

/// Apply the ambient `CUDA_VISIBLE_DEVICES` to nvidia-smi's rows. All-UUID
/// entries keep those GPUs; any index entry is unmappable, because CUDA's
/// index order is not nvidia-smi's.
fn restrict_to_visible(gpus: Vec<GpuInfo>, visible: Option<&str>) -> Visible {
    let entries: Vec<&str> = visible
        .unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .collect();
    if entries.is_empty() {
        if visible.is_none() {
            return Visible::Resolved(gpus);
        }
        tracing::info!(
            gpus = gpus.len(),
            "CUDA_VISIBLE_DEVICES is set and names no device, which is how \
             CUDA is told to expose no GPU at all — every worker spawned here \
             inherits it, so this host has no GPU devices and its models run \
             on the CPU device and are priced against RAM"
        );
        return Visible::Blank;
    }
    if !entries.iter().all(|entry| is_uuid_pin(entry)) {
        tracing::info!(
            visible_devices = %visible.unwrap_or(""),
            gpus = gpus.len(),
            "CUDA_VISIBLE_DEVICES names devices by index, which is in CUDA \
             order and cannot be mapped to nvidia-smi's rows; workers inherit \
             the restriction as-is and the ledger adopts the GPU each one \
             reports by UUID"
        );
        return Visible::Unmapped(gpus);
    }
    let matched = |gpu: &GpuInfo| {
        entries.iter().any(|entry| {
            let entry = entry.to_ascii_uppercase();
            gpu.uuid.to_ascii_uppercase().starts_with(&entry)
        })
    };
    if !gpus.iter().any(matched) {
        tracing::warn!(
            visible_devices = %visible.unwrap_or(""),
            gpus = gpus.len(),
            "CUDA_VISIBLE_DEVICES names no GPU nvidia-smi reports; leaving \
             the GPU inventory unknown"
        );
        // Nothing adoptable: the mask hides every reported GPU.
        return Visible::Unmapped(Vec::new());
    }
    let restricted: Vec<GpuInfo> = gpus.into_iter().filter(matched).collect();
    tracing::info!(
        visible_devices = %visible.unwrap_or(""),
        gpus = restricted.len(),
        "restricting the GPU inventory to the ambient CUDA_VISIBLE_DEVICES"
    );
    Visible::Resolved(restricted)
}

/// Whether a registry `devices` entry names the CPU device (case ignored).
pub(super) fn is_cpu_request(requested: Option<&str>) -> bool {
    requested.is_some_and(|entry| entry.trim().eq_ignore_ascii_case(cpu::DEVICE_KEY))
}

/// The devices before the CPU device, which is always last.
fn accelerators_of(gpus: &[GpuInfo]) -> &[GpuInfo] {
    let end = gpus
        .iter()
        .position(|gpu| gpu.uuid == cpu::DEVICE_KEY)
        .unwrap_or(gpus.len());
    &gpus[..end]
}

/// Where an unpinned replica lands: the highest compute capability, ties
/// broken by [`GpuInfo::placement_total_mb`] and then the lowest index.
fn default_gpu(gpus: &[GpuInfo]) -> Option<&GpuInfo> {
    gpus.iter().min_by_key(|gpu| {
        (
            std::cmp::Reverse(gpu.cap_tenths()),
            std::cmp::Reverse(gpu.placement_total_mb()),
            gpu.index,
        )
    })
}

impl GpuInventory {
    /// Explicitly-unknown inventory (tests only; production probes).
    #[cfg(test)]
    pub fn unknown() -> Self {
        Self::default()
    }

    /// Construct a known CUDA inventory (tests only).
    #[cfg(test)]
    pub fn known(gpus: Vec<GpuInfo>) -> Self {
        Self {
            gpus: (!gpus.is_empty()).then(|| gpus.into()),
            adoptable: None,
            adopted: Arc::default(),
            backend: MemoryBackend::default(),
            cpu_roots: None,
            blank_mask: false,
        }
    }

    /// A CPU-only inventory with `ram_mb` MiB of RAM (tests only).
    #[cfg(test)]
    pub fn known_cpu(ram_mb: u64) -> Self {
        Self {
            gpus: Some(vec![cpu::gpu(ram_mb)].into()),
            adoptable: None,
            adopted: Arc::default(),
            backend: MemoryBackend::Cpu,
            cpu_roots: Some(cpu::MemRoots::default()),
            blank_mask: false,
        }
    }

    /// An MPS inventory plus the CPU device sharing its RAM (tests only).
    #[cfg(test)]
    pub fn known_mps(ram_mb: u64) -> Self {
        let facts = mps::HostFacts {
            chip: "Apple M3 Max".to_owned(),
            ram_bytes: ram_mb * 1024 * 1024,
        };
        Self {
            gpus: Some(vec![mps::gpu(&facts)].into()),
            adoptable: None,
            adopted: Arc::default(),
            backend: MemoryBackend::Mps,
            cpu_roots: None,
            blank_mask: false,
        }
        .with_cpu(ram_mb, cpu::MemRoots::default())
    }

    /// A ROCm inventory with the production sysfs roots (tests only).
    #[cfg(test)]
    pub fn known_rocm(gpus: Vec<GpuInfo>) -> Self {
        Self {
            gpus: (!gpus.is_empty()).then(|| gpus.into()),
            adoptable: None,
            adopted: Arc::default(),
            backend: MemoryBackend::RocmSysfs {
                pci_devices: rocm::SysfsRoots::default().pci_devices,
                meminfo: rocm::SysfsRoots::default().meminfo,
                ambient_hip_restriction: false,
            },
            cpu_roots: None,
            blank_mask: false,
        }
    }

    /// This inventory with the CPU device appended after the accelerators.
    pub(super) fn with_cpu(mut self, ram_mb: u64, roots: cpu::MemRoots) -> Self {
        let mut gpus = self.gpus().unwrap_or(&[]).to_vec();
        gpus.push(cpu::gpu(ram_mb));
        self.gpus = Some(gpus.into());
        self.cpu_roots = Some(roots);
        self
    }

    /// Every device this host prices (accelerators, then the CPU device), or
    /// `None` when unknown.
    pub fn gpus(&self) -> Option<&[GpuInfo]> {
        self.gpus.as_deref()
    }

    /// The accelerators alone, without the CPU device; `None` if there are none.
    fn accelerators(&self) -> Option<&[GpuInfo]> {
        Some(accelerators_of(self.gpus()?)).filter(|gpus| !gpus.is_empty())
    }

    /// The devices default placement ranks: the accelerators, or the CPU
    /// device on a host known to have none. Never both.
    fn rankable<'a>(&self, gpus: &'a [GpuInfo]) -> &'a [GpuInfo] {
        match accelerators_of(gpus) {
            [] if self.blank_mask || matches!(self.backend, MemoryBackend::Cpu) => gpus,
            accelerators => accelerators,
        }
    }

    /// The device kind (`"cuda"`, `"rocm"`, `"mps"`, `"cpu"`) a key names.
    pub(super) fn device_kind(&self, key: &str) -> &'static str {
        if key == cpu::DEVICE_KEY {
            return "cpu";
        }
        match self.backend {
            MemoryBackend::NvidiaSmi { .. } => "cuda",
            MemoryBackend::RocmSysfs { .. } => "rocm",
            MemoryBackend::Mps => "mps",
            MemoryBackend::Cpu => "cpu",
        }
    }

    /// The NVIDIA GPUs, visible or adoptable, that move memory to system RAM
    /// instead of failing a full allocation ([`spills`]).
    fn set_spilling(&mut self, platform: DriverPlatform, driver_models: Option<&str>) {
        let models = driver_models.map(parse_driver_models).unwrap_or_default();
        let gpus = self.gpus().unwrap_or(&[]).iter().chain(self.adoptable());
        let verdicts: Arc<[String]> = gpus
            .filter(|gpu| spills(platform, models.get(gpu.uuid.as_str()).copied()))
            .map(|gpu| gpu.uuid.clone())
            .collect();
        if let MemoryBackend::NvidiaSmi { spilling } = &mut self.backend {
            *spilling = verdicts;
        }
    }

    /// The GPUs (by UUID) a full allocation spills to system RAM on.
    pub(super) fn spilling_gpus(&self) -> &[String] {
        match &self.backend {
            MemoryBackend::NvidiaSmi { spilling } => spilling,
            _ => &[],
        }
    }

    /// What a worker on this device is told about spilling
    /// ([`SPILLS_TO_RAM_ENV_VAR`]): `None` off NVIDIA or for an unknown device.
    pub(super) fn spill_verdict(&self, key: Option<&str>) -> Option<bool> {
        let key = key.filter(|_| matches!(self.backend, MemoryBackend::NvidiaSmi { .. }))?;
        Some(self.spilling_gpus().iter().any(|uuid| uuid == key))
    }

    /// The spawn config of a replica on device `key`: the CPU device's, or
    /// `spawn` with a unified GPU's address and an NVIDIA GPU's spill verdict.
    pub(super) fn spawn_config<'a>(
        &self,
        spawn: &'a WorkerSpawnConfig,
        key: Option<&str>,
    ) -> Cow<'a, WorkerSpawnConfig> {
        if key == Some(cpu::DEVICE_KEY) {
            return Cow::Owned(spawn.for_cpu_device());
        }
        let bdf = key.and_then(|key| self.unified_pin_bdf(Some(key)));
        spawn.for_gpu(bdf.as_deref(), self.spill_verdict(key))
    }

    /// The GPUs an unmappable ambient mask hid, candidates for adoption.
    pub(super) fn adoptable(&self) -> &[GpuInfo] {
        self.adoptable.as_deref().unwrap_or(&[])
    }

    /// Admit the adoptable row with this UUID. Idempotent; shared by clones.
    pub(super) fn adopt(&self, uuid: &str) {
        let Some(gpu) = self
            .adoptable()
            .iter()
            .find(|gpu| gpu.uuid.eq_ignore_ascii_case(uuid))
        else {
            return;
        };
        let mut adopted = self.adopted();
        if !adopted.iter().any(|row| row.uuid == gpu.uuid) {
            adopted.push(gpu.clone());
        }
    }

    fn adopted(&self) -> std::sync::MutexGuard<'_, Vec<GpuInfo>> {
        // A poisoned list is still valid.
        match self.adopted.lock() {
            Ok(adopted) => adopted,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// The inventory plus any adopted rows; `None` while the host is unknown.
    pub(super) fn priced_gpus(&self) -> Option<Vec<GpuInfo>> {
        let adopted = self.adopted().clone();
        match (self.gpus(), adopted.is_empty()) {
            (None, true) => None,
            (None, false) => Some(adopted),
            (Some(gpus), true) => Some(gpus.to_vec()),
            // Adopted GPUs first, keeping the CPU device last.
            (Some(gpus), false) => Some(adopted.into_iter().chain(gpus.iter().cloned()).collect()),
        }
    }

    /// An unknown inventory with every row adoptable (tests only).
    #[cfg(test)]
    pub fn masked(gpus: Vec<GpuInfo>) -> Self {
        Self {
            gpus: None,
            adoptable: (!gpus.is_empty()).then(|| gpus.into()),
            adopted: Arc::default(),
            backend: MemoryBackend::default(),
            cpu_roots: None,
            blank_mask: false,
        }
    }

    /// The first accelerator's key and unified RAM figure, if it has one.
    fn first_unified_ram_mb(&self) -> Option<(String, u64)> {
        let gpu = self.accelerators().and_then(<[GpuInfo]>::first)?;
        Some((gpu.uuid.clone(), gpu.unified_ram_mb?))
    }

    /// The live-memory interface for the accelerators. On ROCm every GPU
    /// must have a PCI address, or there is no refresh at all.
    pub(super) fn memory_query(&self) -> MemoryQuery {
        if matches!(self.backend, MemoryBackend::Cpu) {
            // The CPU device is served by `cpu_memory_query`.
            return MemoryQuery::Unavailable;
        }
        if matches!(self.backend, MemoryBackend::Mps) {
            return match self.first_unified_ram_mb() {
                Some((key, ram_mb)) => MemoryQuery::Mps { key, ram_mb },
                None => MemoryQuery::Unavailable,
            };
        }
        let MemoryBackend::RocmSysfs {
            pci_devices,
            meminfo,
            ..
        } = &self.backend
        else {
            return MemoryQuery::NvidiaSmi;
        };
        let Some(gpus) = self.accelerators() else {
            return MemoryQuery::Unavailable;
        };
        let mut keyed = Vec::with_capacity(gpus.len());
        for gpu in gpus {
            let Some(bdf) = gpu.bdf.clone() else {
                tracing::warn!(
                    uuid = %gpu.uuid,
                    name = %gpu.name,
                    "a ROCm inventory GPU has no PCI address; its live VRAM \
                     counters cannot be located, so this host gets no external \
                     memory refresh at all (a partial one would price the \
                     remaining GPUs off stale readings)"
                );
                return MemoryQuery::Unavailable;
            };
            keyed.push(rocm::GpuRef {
                key: gpu.uuid.clone(),
                bdf,
                unified: gpu.unified(),
            });
        }
        MemoryQuery::RocmSysfs {
            pci_devices: pci_devices.clone(),
            meminfo: meminfo.clone(),
            gpus: keyed.into(),
        }
    }

    /// The CPU device's live-memory interface (host RAM statistics).
    pub(super) fn cpu_memory_query(&self) -> MemoryQuery {
        let Some(roots) = self.cpu_roots.clone() else {
            return MemoryQuery::Unavailable;
        };
        let Some(gpu) = self
            .gpus()
            .and_then(|gpus| gpus.iter().find(|gpu| gpu.uuid == cpu::DEVICE_KEY))
        else {
            return MemoryQuery::Unavailable;
        };
        MemoryQuery::Cpu {
            key: gpu.uuid.clone(),
            ram_mb: gpu.total_mb,
            roots,
        }
    }

    /// The pin of the default GPU ([`default_gpu`]): its UUID on CUDA, its
    /// index on ROCm, none on MPS/CPU.
    pub fn default_pin(&self) -> Option<String> {
        if self.pins_are_absent() {
            return None;
        }
        let gpu = self.default_gpu()?;
        Some(if self.pins_are_indices() {
            gpu.index.to_string()
        } else {
            gpu.uuid.clone()
        })
    }

    /// Whether pins are HIP device indices rather than CUDA GPU UUIDs.
    fn pins_are_indices(&self) -> bool {
        matches!(self.backend, MemoryBackend::RocmSysfs { .. })
    }

    /// Whether this host has no pins at all (MPS, CPU).
    fn pins_are_absent(&self) -> bool {
        matches!(self.backend, MemoryBackend::Mps | MemoryBackend::Cpu)
    }

    /// Whether a worker's total-memory report replaces the device total (MPS
    /// only: `recommendedMaxWorkingSetSize` is readable only in the worker).
    pub(super) fn adopts_worker_total(&self) -> bool {
        matches!(self.backend, MemoryBackend::Mps)
    }

    /// Whether device allocations go through Metal's allocator (the ledger's
    /// pool-margin ceiling is per allocator).
    pub(super) fn metal_allocator(&self) -> bool {
        matches!(self.backend, MemoryBackend::Mps)
    }

    /// Whether a HIP-layer visibility restriction was set at probe time.
    fn ambient_hip_restriction(&self) -> bool {
        matches!(
            self.backend,
            MemoryBackend::RocmSysfs {
                ambient_hip_restriction: true,
                ..
            }
        )
    }

    /// The default GPU's model name, or `None` on an unknown host.
    pub fn default_gpu_name(&self) -> Option<String> {
        Some(
            default_gpu(self.rankable(&self.priced_gpus()?))?
                .name
                .clone(),
        )
    }

    /// The default GPU's architecture ([`GpuInfo::arch`]).
    pub fn default_gpu_arch(&self) -> Option<String> {
        default_gpu(self.rankable(&self.priced_gpus()?))?.arch()
    }

    fn default_gpu(&self) -> Option<&GpuInfo> {
        default_gpu(self.accelerators()?)
    }

    /// Resolve a replica's registry pin into the value written to
    /// [`pin_env_var`].
    ///
    /// CUDA: no request gives the default GPU; a UUID or index naming a
    /// visible GPU gives the inventory's spelling of its UUID (pins are
    /// compared byte-wise); anything else passes through verbatim. MPS/CPU:
    /// `None`. ROCm: indices only, anything else is dropped. An ambient
    /// HIP-layer restriction drops every pin. A `cpu` request returns the
    /// empty pin on every host, MPS and CPU included. See
    /// docs/rocm-batch-calibration-parity.md "D2 (G2) — Pinning".
    pub fn resolve_pin(&self, requested: Option<&str>) -> Option<String> {
        // `cpu` on any host: the empty value hides every accelerator. It also
        // differs from `default_pin()`, so the replica cannot share a pooled
        // worker spawned for the default device.
        if is_cpu_request(requested) {
            return Some(String::new());
        }
        if self.pins_are_absent() {
            if let Some(requested) = requested.map(str::trim).filter(|pin| !pin.is_empty()) {
                tracing::warn!(
                    pin = %requested,
                    "ignoring this device pin: this host has no device to \
                     select — an Apple Silicon host has exactly one Metal \
                     device and no visibility variable that names it, and a \
                     CPU host has none at all — so the model runs where it was \
                     always going to and is priced against that"
                );
            }
            return None;
        }
        // A pin could only re-expose a GPU the operator hid.
        if self.blank_mask {
            if let Some(requested) = requested.map(str::trim).filter(|pin| !pin.is_empty()) {
                tracing::warn!(
                    pin = %requested,
                    "ignoring this device pin: this host's ambient visibility \
                     variable is set to a value that names no device, so no \
                     GPU is visible to a worker at all and this model runs on \
                     the CPU device, priced against RAM"
                );
            }
            return None;
        }
        // The operator's HIP-layer restriction outranks every arm below.
        if self.ambient_hip_restriction() {
            if let Some(requested) = requested {
                tracing::warn!(
                    pin = %requested.trim(),
                    "ignoring this device pin: a HIP-layer visibility restriction \
                     (HIP_VISIBLE_DEVICES / CUDA_VISIBLE_DEVICES / \
                     GPU_DEVICE_ORDINAL) is already set in this gateway's own \
                     environment, and writing our own would override it and hand \
                     the worker GPUs the operator deliberately hid — the \
                     operator's restriction wins, and the worker inherits it \
                     as-is"
                );
            }
            return None;
        }
        let Some(gpus) = self.accelerators() else {
            if self.pins_are_indices() {
                return self.resolve_hip_pin_uninventoried(requested);
            }
            return requested.map(str::to_owned);
        };
        if self.pins_are_indices() {
            return self.resolve_hip_pin(gpus, requested);
        }
        let Some(requested) = requested else {
            return self.default_pin();
        };
        let trimmed = requested.trim();
        if is_uuid_pin(trimmed) {
            // Canonical spelling: pins are compared byte-wise elsewhere.
            if let Some(gpu) = gpus
                .iter()
                .find(|gpu| gpu.uuid.eq_ignore_ascii_case(trimmed))
            {
                return Some(gpu.uuid.clone());
            }
            let wanted = trimmed.to_ascii_uppercase();
            let mut matches = gpus
                .iter()
                .filter(|gpu| gpu.uuid.to_ascii_uppercase().starts_with(&wanted));
            if let Some(first) = matches.next()
                && matches.next().is_none()
            {
                return Some(first.uuid.clone());
            }
            // Ambiguous or not visible (e.g. `MIG-…`): left to CUDA.
            return Some(trimmed.to_owned());
        }
        if let Ok(index) = trimmed.parse::<u32>()
            && let Some(gpu) = gpus.iter().find(|gpu| gpu.index == index)
        {
            return Some(gpu.uuid.clone());
        }
        tracing::warn!(
            pin = %requested,
            "device pin does not name a visible GPU; passing it to \
             CUDA_VISIBLE_DEVICES unchanged"
        );
        Some(requested.to_owned())
    }

    /// Resolve a registry `devices` entry into the ledger device key (the
    /// GPU's `uuid`), which differs from the pin on ROCm. Use this, never the
    /// pin, to key the ledger.
    ///
    /// No request gives the default GPU; a full key or an index gives that
    /// row; on CUDA an unambiguous UUID prefix also resolves. Anything else is
    /// `None` without a warning (`resolve_pin` already warned).
    pub fn resolve_device_key(&self, requested: Option<&str>) -> Option<String> {
        let gpus = &self.priced_gpus()?;
        let Some(requested) = requested else {
            return Some(default_gpu(self.rankable(gpus))?.uuid.clone());
        };
        let trimmed = requested.trim();
        if let Some(gpu) = gpus
            .iter()
            .find(|gpu| gpu.uuid.eq_ignore_ascii_case(trimmed))
        {
            return Some(gpu.uuid.clone());
        }
        if let Ok(index) = trimmed.parse::<u32>() {
            // Indices name accelerators only; the CPU device also has index 0.
            return accelerators_of(gpus)
                .iter()
                .find(|gpu| gpu.index == index)
                .map(|gpu| gpu.uuid.clone());
        }
        if self.pins_are_indices() || !is_uuid_pin(trimmed) {
            return None;
        }
        let wanted = trimmed.to_ascii_uppercase();
        let mut matches = gpus
            .iter()
            .filter(|gpu| gpu.uuid.to_ascii_uppercase().starts_with(&wanted));
        let first = matches.next()?;
        matches.next().is_none().then(|| first.uuid.clone())
    }

    /// The PCI address for [`UNIFIED_GPU_ENV_VAR`] when the entry names a
    /// unified ROCm GPU, else `None`.
    pub fn unified_pin_bdf(&self, requested: Option<&str>) -> Option<String> {
        if !self.pins_are_indices() {
            return None;
        }
        let key = self.resolve_device_key(requested)?;
        self.accelerators()?
            .iter()
            .find(|gpu| gpu.uuid == key && gpu.unified())
            .and_then(|gpu| gpu.bdf.clone())
    }

    /// The ROCm arm of [`Self::resolve_pin`]: a device key translates to its
    /// index, and an unresolvable non-numeric string is dropped.
    fn resolve_hip_pin(&self, gpus: &[GpuInfo], requested: Option<&str>) -> Option<String> {
        let Some(requested) = requested else {
            return self.default_pin();
        };
        let trimmed = requested.trim();
        if let Some(gpu) = gpus
            .iter()
            .find(|gpu| gpu.uuid.eq_ignore_ascii_case(trimmed))
        {
            return Some(gpu.index.to_string());
        }
        if let Ok(index) = trimmed.parse::<u32>() {
            if !gpus.iter().any(|gpu| gpu.index == index) {
                tracing::warn!(
                    pin = %trimmed,
                    gpus = gpus.len(),
                    "device pin names no GPU in this host's HIP enumeration; \
                     writing the index to HIP_VISIBLE_DEVICES anyway (HIP \
                     takes indices, so the operator's intent survives — but a \
                     worker pinned out of range falls back to the CPU)"
                );
            }
            return Some(index.to_string());
        }
        if let Some(list) = canonical_index_list(trimmed) {
            tracing::warn!(
                pin = %trimmed,
                "device pin is a HIP device list; writing it to \
                 HIP_VISIBLE_DEVICES as asked — a worker left with more than \
                 one visible GPU is not something the per-GPU ledger can \
                 price"
            );
            return Some(list);
        }
        tracing::warn!(
            pin = %trimmed,
            "device pin is neither a HIP device index nor a device key this \
             host reports; dropping it rather than writing it to \
             HIP_VISIBLE_DEVICES, where it would match no device, hide every \
             GPU and silently run the worker on the CPU"
        );
        None
    }

    /// The ROCm arm of [`Self::resolve_pin`] for a host with no GPUs found:
    /// only an index list passes.
    fn resolve_hip_pin_uninventoried(&self, requested: Option<&str>) -> Option<String> {
        let trimmed = requested?.trim();
        if let Some(pin) = canonical_index_list(trimmed) {
            return Some(pin);
        }
        tracing::warn!(
            pin = %trimmed,
            "this ROCm host reports no GPUs to resolve device pins \
             against, and this pin is not a HIP device index either; \
             dropping it rather than writing it to HIP_VISIBLE_DEVICES, \
             where it would match no device, hide every GPU and silently \
             run the worker on the CPU"
        );
        None
    }
}

/// A comma-separated list of device indices (empty entries ignored, as HIP
/// does), re-rendered canonically because `prewarm.rs` compares pins
/// byte-wise. `None` if the list is empty or any entry is not an index.
fn canonical_index_list(value: &str) -> Option<String> {
    let mut canonical = String::new();
    for entry in value.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let index = entry.parse::<u32>().ok()?;
        if !canonical.is_empty() {
            canonical.push(',');
        }
        canonical.push_str(&index.to_string());
    }
    (!canonical.is_empty()).then_some(canonical)
}

/// The forms CUDA accepts in `CUDA_VISIBLE_DEVICES` verbatim.
fn is_uuid_pin(value: &str) -> bool {
    let upper = value.to_ascii_uppercase();
    upper.starts_with("GPU-") || upper.starts_with("MIG-")
}

/// One GPU per line, `index, uuid, name, total, compute_cap`. Any row whose
/// identity columns do not parse makes the whole probe unknown; the
/// capability column is optional per row.
fn parse_inventory(stdout: &str) -> Option<Vec<GpuInfo>> {
    let mut gpus = Vec::new();
    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some(gpu) = parse_row(line) else {
            tracing::warn!(
                row = %line,
                "unparseable nvidia-smi row; leaving the whole GPU inventory \
                 unknown (workers will not be pinned and batch sizes will \
                 not be calibrated)"
            );
            return None;
        };
        gpus.push(gpu);
    }
    if gpus.is_empty() {
        tracing::warn!(
            "nvidia-smi reported no GPUs; workers will not be pinned and \
             batch sizes will not be calibrated"
        );
        None
    } else {
        Some(gpus)
    }
}

/// One inventory row; `None` if any identity column does not parse.
fn parse_row(line: &str) -> Option<GpuInfo> {
    let mut fields = line.split(',');
    let index = fields.next()?.trim().parse::<u32>().ok()?;
    let uuid = fields.next()?.trim().to_owned();
    let name = fields.next()?.trim().to_owned();
    let total_mb = fields.next()?.trim().parse::<u64>().ok()?;
    let cap_field = fields.next()?.trim().to_owned();
    if fields.next().is_some() || !is_uuid_pin(&uuid) || name.is_empty() {
        return None;
    }
    let compute_cap = if parse_compute_cap(&cap_field).is_some() {
        Some(cap_field)
    } else {
        tracing::info!(
            uuid = %uuid,
            compute_cap = %cap_field,
            "nvidia-smi did not report this GPU's compute capability; it \
             stays pinnable but is not used for default placement and \
             cannot satisfy a model's capability floor"
        );
        None
    };
    Some(GpuInfo {
        index,
        uuid,
        name,
        total_mb,
        compute_cap,
        bdf: None,
        gfx_target_version: None,
        unified_ram_mb: None,
        vram_carveout_mb: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gpu(index: u32, uuid: &str, cap: &str) -> GpuInfo {
        sized_gpu(index, uuid, cap, 32607)
    }

    fn sized_gpu(index: u32, uuid: &str, cap: &str, total_mb: u64) -> GpuInfo {
        GpuInfo {
            index,
            uuid: uuid.into(),
            name: "NVIDIA GeForce RTX 5090".into(),
            total_mb,
            compute_cap: (!cap.is_empty()).then(|| cap.to_owned()),
            bdf: None,
            gfx_target_version: None,
            unified_ram_mb: None,
            vram_carveout_mb: None,
        }
    }

    fn inventory() -> GpuInventory {
        GpuInventory::known(vec![gpu(0, "GPU-1111", "12.0"), gpu(3, "GPU-3333", "12.0")])
    }

    /// Only the Windows display driver spills a full GPU to system RAM: on
    /// native Windows per GPU by its driver model, under WSL every GPU.
    #[test]
    fn a_gpu_spills_under_the_windows_display_driver_only() {
        use DriverPlatform::{Other, Windows, Wsl};
        for (platform, model, spilled) in [
            (Other, None, false),
            (Wsl, None, true),
            (Wsl, Some("TCC"), true),
            (Windows, Some("WDDM"), true),
            (Windows, Some(" tcc "), false),
            (Windows, Some("MCDM"), false),
            (Windows, Some("[N/A]"), true),
            (Windows, None, true),
        ] {
            assert_eq!(spills(platform, model), spilled, "{platform:?} {model:?}");
        }
        #[cfg(target_os = "linux")]
        {
            let dir = tempfile::tempdir().expect("tempdir");
            let dxg = dir.path().join("dxg");
            assert_eq!(DriverPlatform::current(&dxg), Other);
            std::fs::write(&dxg, "").expect("writes");
            assert_eq!(DriverPlatform::current(&dxg), Wsl);
        }
        // The verdict is per GPU, adoptable rows included, joined by UUID.
        let rows = "0, GPU-1111, A, 24576, 8.9\n1, GPU-2222, B, 97887, 12.0\n";
        let models = "GPU-1111, WDDM\nGPU-2222, TCC\nunparseable\n";
        for visible in [None, Some("1")] {
            let mut inventory = build(Some(rows), visible).inventory;
            inventory.set_spilling(Windows, Some(models));
            assert_eq!(inventory.spilling_gpus(), ["GPU-1111"], "{visible:?}");
            assert_eq!(inventory.spill_verdict(Some("GPU-1111")), Some(true));
            assert_eq!(inventory.spill_verdict(Some("GPU-2222")), Some(false));
            assert_eq!(inventory.spill_verdict(None), None, "an unknown device");
            inventory.set_spilling(Windows, None);
            assert_eq!(inventory.spilling_gpus().len(), 2, "no models read: WDDM");
            inventory.set_spilling(Other, Some(models));
            assert!(inventory.spilling_gpus().is_empty());
        }
        // Each replica's spawn config carries its own GPU's verdict.
        let mut inventory = build(Some(rows), None).inventory;
        inventory.set_spilling(Windows, Some(models));
        let spawn = super::super::worker::testing::test_spawn_config();
        let told = |key| {
            let config = inventory.spawn_config(&spawn, Some(key));
            config
                .env
                .iter()
                .find(|(name, _)| name == SPILLS_TO_RAM_ENV_VAR)
                .map(|(_, value)| value.clone())
        };
        assert_eq!(told("GPU-1111").as_deref(), Some("1"));
        assert_eq!(told("GPU-2222").as_deref(), Some("0"));
        let rocm = GpuInventory::known_rocm(vec![gpu(0, "GPU-1111", "")]);
        assert_eq!(
            rocm.spill_verdict(Some("GPU-1111")),
            None,
            "not an NVIDIA GPU"
        );
    }

    /// The calibration keyspace the host derives for itself: the compute
    /// capability on CUDA, KFD's ISA target on ROCm, nothing where only a
    /// loaded worker can answer.
    #[test]
    fn the_architecture_key_comes_from_the_capability_or_the_gfx_target() {
        for (cap, want) in [
            ("12.0", Some("sm_120")),
            ("8.6", Some("sm_86")),
            ("7.5", Some("sm_75")),
            ("", None),
            ("N/A", None),
        ] {
            assert_eq!(gpu(0, "GPU-1111", cap).arch().as_deref(), want, "{cap}");
        }
        // The ROCm target wins outright: no ROCm row carries a capability, and
        // the two vocabularies must never be mixed.
        // 90_010 is gfx90a, not gfx901: the minor and stepping render as
        // single hex digits.
        for (target, want) in [
            (110_000u32, Some("gfx1100")),
            (90_010, Some("gfx90a")),
            (0, None),
        ] {
            let mut row = amd_gpu(0, "0000:03:00.0", 24_576);
            row.gfx_target_version = Some(target);
            assert_eq!(row.arch().as_deref(), want, "{target}");
        }
        // MPS and CPU have neither, and learn the key from a load report.
        assert_eq!(
            super::mps::gpu(&super::mps::HostFacts {
                chip: "Apple M3 Max".into(),
                ram_bytes: 128 * 1024 * 1024 * 1024,
            })
            .arch(),
            None
        );
        assert_eq!(
            inventory().default_gpu_arch().as_deref(),
            Some("sm_120"),
            "the default GPU answers for the /metadata overlay"
        );
        assert_eq!(GpuInventory::unknown().default_gpu_arch(), None);
    }

    fn amd_gpu(index: u32, bdf: &str, total_mb: u64) -> GpuInfo {
        GpuInfo {
            index,
            uuid: format!("GPU-BDF-{bdf}"),
            name: "AMD gfx1100 (24 GB)".into(),
            total_mb,
            compute_cap: None,
            bdf: Some(bdf.to_owned()),
            gfx_target_version: Some(110000),
            unified_ram_mb: None,
            vram_carveout_mb: None,
        }
    }

    fn rocm_inventory(pci_devices: PathBuf, gpus: Vec<GpuInfo>) -> GpuInventory {
        rocm_inventory_with(pci_devices, rocm::SysfsRoots::default().meminfo, gpus)
    }

    /// The same, with `/proc/meminfo` — only the unified refresh reads it.
    fn rocm_inventory_with(
        pci_devices: PathBuf,
        meminfo: PathBuf,
        gpus: Vec<GpuInfo>,
    ) -> GpuInventory {
        GpuInventory {
            gpus: Some(gpus.into()),
            adoptable: None,
            adopted: Arc::default(),
            backend: MemoryBackend::RocmSysfs {
                pci_devices,
                meminfo,
                // A knowable inventory is proof of no ambient restriction:
                // the probe blanks it otherwise.
                ambient_hip_restriction: false,
            },
            cpu_roots: None,
            blank_mask: false,
        }
    }

    fn mps_inventory(ram_gib: u64) -> GpuInventory {
        let facts = super::mps::HostFacts {
            chip: "Apple M3 Max".into(),
            ram_bytes: ram_gib * 1024 * 1024 * 1024,
        };
        GpuInventory {
            gpus: Some(vec![super::mps::gpu(&facts)].into()),
            adoptable: None,
            adopted: Arc::default(),
            backend: MemoryBackend::Mps,
            cpu_roots: None,
            blank_mask: false,
        }
    }

    fn amd_apu(index: u32, bdf: &str, carveout_mb: u64, gtt_mb: u64, ram_mb: u64) -> GpuInfo {
        GpuInfo {
            index,
            uuid: format!("GPU-BDF-{bdf}"),
            name: format!("AMD gfx1151 APU ({} GB)", ram_mb / 1024),
            total_mb: carveout_mb + gtt_mb,
            compute_cap: None,
            bdf: Some(bdf.to_owned()),
            gfx_target_version: Some(110_501),
            unified_ram_mb: Some(ram_mb),
            vram_carveout_mb: Some(carveout_mb),
        }
    }

    fn uninventoried_rocm(ambient_hip_restriction: bool) -> GpuInventory {
        GpuInventory {
            gpus: None,
            adoptable: None,
            adopted: Arc::default(),
            backend: MemoryBackend::RocmSysfs {
                pci_devices: PathBuf::from("/sys/bus/pci/devices"),
                meminfo: PathBuf::from("/proc/meminfo"),
                ambient_hip_restriction,
            },
            cpu_roots: None,
            blank_mask: false,
        }
    }

    const TWO_GPUS: &str = "0, GPU-1a2b, NVIDIA GeForce RTX 5090, 32607, 12.0\n\
                              1, GPU-3c4d, NVIDIA RTX A2000, 6138, 8.6\n";

    /// One coherent snapshot: a single unparseable row makes the whole
    /// reading unknown rather than pricing a GPU's external usage as zero.
    #[test]
    fn parses_a_memory_snapshot() {
        let gpus = parse_memory("GPU-1a2b, 32607, 21000\nGPU-3c4d, 6138, 512\n").expect("parses");
        let read: Vec<_> = gpus
            .into_iter()
            .map(|m| (m.uuid, m.total_mb, m.free_mb))
            .collect();
        assert_eq!(
            read,
            vec![
                ("GPU-1a2b".to_owned(), 32607, 21000),
                ("GPU-3c4d".to_owned(), 6138, 512),
            ]
        );
        // Empty, unparseable, one bad row among good ones, a missing column,
        // and a non-UUID identity (which could not key a ledger) all make the
        // whole snapshot unknown.
        #[rustfmt::skip]
        let unreadable = [
            "", "N/A, N/A, N/A\n", "GPU-1a2b, 32607, 21000\nGPU-3c4d, [N/A], 512\n",
            "GPU-1a2b, 32607\n", "0, 32607, 21000\n",
        ];
        for stdout in unreadable {
            assert!(parse_memory(stdout).is_none(), "{stdout:?}");
        }
    }

    /// Both views come from the same rows, so an inventory index and a
    /// capability always describe the same physical GPU.
    #[test]
    fn one_probe_builds_both_views() {
        let gpus = parse_inventory(TWO_GPUS).expect("parses");
        assert_eq!(gpus.len(), 2);
        assert_eq!(gpus[0].uuid, "GPU-1a2b");
        assert_eq!(gpus[0].name, "NVIDIA GeForce RTX 5090");
        assert_eq!(gpus[0].total_mb, 32607);
        assert_eq!(gpus[0].compute_cap.as_deref(), Some("12.0"));
        assert_eq!(gpus[1].index, 1);

        let host = build(Some(TWO_GPUS), None);
        assert_eq!(
            host.inventory
                .gpus()
                .expect("known")
                .iter()
                .map(|gpu| gpu.uuid.as_str())
                .collect::<Vec<_>>(),
            vec!["GPU-1a2b", "GPU-3c4d"]
        );
        // A floor is met when *any* device meets it.
        for (floor, expected) in [(8.0, true), (9.0, true), (12.1, false)] {
            assert_eq!(host.caps.meets_floor(floor), Some(expected), "{floor}");
        }
    }

    /// Any unparseable **identity** column makes the whole probe unknown: a
    /// partial picture must not drive pinning or filter models.
    #[test]
    fn garbage_in_the_identity_columns_makes_both_views_unknown() {
        // Empty, unparseable, driver-error text (which must not become a
        // GPU), one bad line among good ones, a non-UUID identity column, and
        // a column count that is not five.
        #[rustfmt::skip]
        let unreadable = [
            "", "N/A\n", "Failed to initialize NVML: Driver error\n",
            "0, GPU-1a2b, RTX, 32607, 8.6\nN/A, N/A, N/A, N/A, N/A\n",
            "0, 0, RTX, 32607, 8.6\n", "0, GPU-1a2b, RTX, 32607\n",
            "0, GPU-1a2b, RTX, 32607, 8.6, extra\n",
        ];
        for stdout in unreadable {
            assert!(parse_inventory(stdout).is_none(), "{stdout:?}");
        }
        for stdout in [Some("N/A\n"), None] {
            let host = build(stdout, None);
            assert!(host.inventory.gpus().is_none());
            let caps = host.caps.meets_floor(8.0);
            assert_eq!(caps, None, "the capability view goes with it");
        }
    }

    /// The capability column is the one separably-useless field: dropping a
    /// row for it would cost the host pinning and the ledger.
    #[test]
    fn an_unreported_capability_keeps_the_gpu_identity() {
        let host = build(
            Some(
                "0, GPU-1a2b, NVIDIA A100-SXM4-40GB MIG 1g.5gb, 4864, [N/A]\n\
                 1, GPU-3c4d, NVIDIA RTX A2000, 6138, 8.6\n",
            ),
            None,
        );
        let gpus = host.inventory.gpus().expect("identities are all good");
        assert_eq!(gpus.len(), 2);
        assert_eq!(gpus[0].compute_cap, None);
        assert_eq!(gpus[0].uuid, "GPU-1a2b", "still a pinnable ledger identity");
        let pin = host.inventory.resolve_pin(Some("0"));
        assert_eq!(pin.as_deref(), Some("GPU-1a2b"), "index pins still resolve");
        // Capabilities come from the GPUs that reported one, and unknown is
        // not slow: the capless GPU is unranked, not compute capability 0.
        assert_eq!(host.caps.meets_floor(8.0), Some(true));
        assert_eq!(host.caps.meets_floor(9.0), Some(false));
        let pin = host.inventory.default_pin();
        assert_eq!(pin.as_deref(), Some("GPU-3c4d"), "unknown must not win");

        // No GPU reports one: identities stay, the capability view is unknown
        // (and filters nothing), and placement falls to the lowest index.
        let capless = build(
            Some(
                "1, GPU-3c4d, NVIDIA RTX A2000, 6138, [N/A]\n\
                 0, GPU-1a2b, NVIDIA RTX A2000, 6138, N/A\n",
            ),
            None,
        );
        assert_eq!(capless.inventory.gpus().map(<[GpuInfo]>::len), Some(2));
        assert_eq!(capless.caps.meets_floor(8.0), None);
        assert_eq!(capless.inventory.default_pin().as_deref(), Some("GPU-1a2b"));
    }

    /// nvidia-smi ignores `CUDA_VISIBLE_DEVICES`, so the ambient restriction
    /// is applied here. A UUID form that resolves narrows both views;
    /// anything unmappable blanks the **inventory only**, since taking the
    /// capability view with it would un-gate every capability-floored model.
    #[test]
    fn ambient_visible_devices_restricts_the_inventory() {
        // UUID form: keep exactly the named GPUs, in nvidia-smi order.
        let host = build(Some(TWO_GPUS), Some("GPU-3c4d"));
        let gpus = host.inventory.gpus().expect("known");
        assert_eq!(gpus.len(), 1);
        assert_eq!(gpus[0].uuid, "GPU-3c4d");
        let pin = host.inventory.default_pin();
        assert_eq!(pin.as_deref(), Some("GPU-3c4d"), "only visible GPUs");
        assert_eq!(
            host.caps.meets_floor(12.0),
            Some(false),
            "the hidden GPU's capability must not filter models either"
        );
        // Abbreviated UUIDs are legal for CUDA, so they are honoured here.
        let abbrev = build(Some(TWO_GPUS), Some("GPU-1a")).inventory;
        assert_eq!(abbrev.default_pin().as_deref(), Some("GPU-1a2b"));
        // Only an **unset** variable means "no restriction"; set and naming
        // nothing means no GPU at all (see
        // `every_mask_form_is_resolved_or_unmapped`).
        assert_eq!(
            build(Some(TWO_GPUS), None)
                .inventory
                .gpus()
                .map(<[GpuInfo]>::len),
            Some(2)
        );
        for visible in ["", " , "] {
            let host = build(Some(TWO_GPUS), Some(visible));
            assert_eq!(
                host.inventory.gpus().map(<[GpuInfo]>::len),
                Some(0),
                "{visible:?}"
            );
        }

        // The unmappable forms: an index (CUDA order is not nvidia-smi
        // order) and a mixed list. The inventory is unknown, but the rows
        // nvidia-smi did report stay adoptable: the ledger takes the one a
        // worker names by UUID, which is the mapping no static rule can make.
        for visible in ["1", "GPU-1a2b,1"] {
            let host = build(Some(TWO_GPUS), Some(visible));
            assert!(host.inventory.gpus().is_none(), "{visible}");
            assert_eq!(host.inventory.resolve_pin(None), None, "{visible}: no pin");
            assert_eq!(
                host.inventory
                    .adoptable()
                    .iter()
                    .map(|gpu| gpu.uuid.as_str())
                    .collect::<Vec<_>>(),
                vec!["GPU-1a2b", "GPU-3c4d"],
                "{visible}: both rows stay adoptable"
            );
            assert_eq!(
                host.caps.meets_floor(12.0),
                Some(true),
                "{visible}: model availability is still capability-filtered"
            );
            assert_eq!(host.caps.meets_floor(12.1), Some(false), "{visible}");
        }
        // A mask that resolved adopts nothing: the operator excluded those
        // cards, and a worker on one must not be priced against it.
        for visible in [None, Some(""), Some("GPU-3c4d")] {
            let host = build(Some(TWO_GPUS), visible);
            assert!(host.inventory.adoptable().is_empty(), "{visible:?}");
        }
        // No probe output at all leaves nothing to adopt either.
        assert!(build(None, Some("1")).inventory.adoptable().is_empty());
    }

    /// Every mask form, and which of the two answers it lands on: resolved
    /// (the pre-mask inventory, narrowed, adopting nothing) or unmapped (the
    /// inventory unknown, every reported row adoptable). Includes CUDA's
    /// "no devices" spellings.
    #[test]
    fn every_mask_form_is_resolved_or_unmapped() {
        let uuids = |host: &HostGpus, take: fn(&GpuInventory) -> Vec<String>| take(&host.inventory);
        let visible = |inv: &GpuInventory| {
            inv.gpus()
                .unwrap_or(&[])
                .iter()
                .map(|gpu| gpu.uuid.clone())
                .collect::<Vec<_>>()
        };
        let adoptable = |inv: &GpuInventory| {
            inv.adoptable()
                .iter()
                .map(|gpu| gpu.uuid.clone())
                .collect::<Vec<_>>()
        };
        let both = ["GPU-1a2b".to_owned(), "GPU-3c4d".to_owned()];
        // (mask, visible, adoptable)
        let cases: Vec<(Option<&str>, Vec<String>, Vec<String>)> = vec![
            // Resolved: narrowed as before, and adopting nothing. Only an
            // **unset** variable is "no restriction"; set and naming nothing
            // is CUDA's "no GPU at all" and is the case below.
            (None, both.to_vec(), vec![]),
            (Some("GPU-3c4d"), vec!["GPU-3c4d".to_owned()], vec![]),
            (
                Some("gpu-3c4d,GPU-9999"),
                vec!["GPU-3c4d".to_owned()],
                vec![],
            ),
            // A UUID or MIG mask matching *no* row resolved too: the operator
            // excluded every card, so there is nothing to adopt back.
            (Some("MIG-abcd"), vec![], vec![]),
            (Some("GPU-9999"), vec![], vec![]),
            // Unmapped: the inventory is unknown exactly as before, and now
            // every reported row is adoptable.
            (Some("1"), vec![], both.to_vec()),
            (Some("0,1"), vec![], both.to_vec()),
            (Some("GPU-1a2b,1"), vec![], both.to_vec()),
            // Not a number and not a UUID: CUDA stops at the first invalid
            // entry, we call it unmappable. Same for a negative index and for
            // `-1`, CUDA's "no devices at all" — after which no worker can
            // report a GPU, so nothing is ever adopted in practice.
            (Some("abc"), vec![], both.to_vec()),
            (Some("-1"), vec![], both.to_vec()),
            (Some("0,-1"), vec![], both.to_vec()),
        ];
        for (mask, want_visible, want_adoptable) in cases {
            let host = build(Some(TWO_GPUS), mask);
            assert_eq!(uuids(&host, visible), want_visible, "visible: {mask:?}");
            assert_eq!(
                uuids(&host, adoptable),
                want_adoptable,
                "adoptable: {mask:?}"
            );
            // The capability view never blanks, whichever answer it was.
            assert_eq!(host.caps.meets_floor(8.6), Some(true), "{mask:?}");
            assert!(!host.inventory.blank_mask, "{mask:?}");
        }

        // Set and naming no device — `CUDA_VISIBLE_DEVICES=`, or a value of
        // nothing but separators — is how CUDA is told to expose no GPU at
        // all, and every worker spawned here inherits it. The inventory is
        // **known empty** rather than unknown: nothing to adopt, nothing to
        // pin, no capability to gate a model on, and the models run on the
        // CPU device.
        for mask in [Some(""), Some(" , , "), Some("  ")] {
            let host = build(Some(TWO_GPUS), mask);
            assert_eq!(uuids(&host, visible), Vec::<String>::new(), "{mask:?}");
            assert!(uuids(&host, adoptable).is_empty(), "{mask:?}");
            assert!(host.inventory.blank_mask, "{mask:?}");
            assert_eq!(host.caps.meets_floor(8.6), None, "{mask:?}");
            assert_eq!(host.inventory.accelerators(), None, "{mask:?}");
            // No pin in any form, so no worker is handed a GPU back.
            for requested in [None, Some("0"), Some("GPU-1a2b")] {
                assert_eq!(host.inventory.resolve_pin(requested), None, "{mask:?}");
            }
            // Except `cpu`, which this mask already grants: it is honoured
            // rather than warned about, and the empty pin is not the
            // `default_pin()` a pooled worker is claimable for.
            assert_eq!(
                host.inventory.resolve_pin(Some("cpu")).as_deref(),
                Some(""),
                "{mask:?}"
            );
            assert_eq!(host.inventory.default_pin(), None, "{mask:?}");
            // And the device every model on this host now runs on, which is
            // also the calibration keyspace `/metadata` reports.
            let host = host.inventory.with_cpu(64 * 1024, cpu::MemRoots::default());
            assert_eq!(host.resolve_device_key(None).as_deref(), Some("CPU"));
            assert_eq!(host.resolve_device_key(Some("0")), None);
            assert_eq!(host.default_gpu_name().as_deref(), Some("CPU (64 GB)"));
        }

        // A host whose accelerators are merely **unknown** answers nothing:
        // its workers do run on a GPU, and naming the CPU device there would
        // key the /metadata overlay to the wrong silicon.
        {
            let unknown = GpuInventory::unknown().with_cpu(64 * 1024, cpu::MemRoots::default());
            assert_eq!(unknown.default_gpu_name(), None);
            assert_eq!(unknown.default_gpu_arch(), None);
            assert_eq!(unknown.resolve_device_key(None), None);
        }
    }

    /// A resolved mask is the only arm that can narrow the inventory, and it
    /// fills no adoptable set — so no resolved mask can ever hand the ledger
    /// a card the operator excluded.
    #[test]
    fn a_resolved_mask_never_offers_the_excluded_card() {
        let host = build(Some(TWO_GPUS), Some("GPU-3c4d"));
        assert_eq!(host.inventory.gpus().map(<[GpuInfo]>::len), Some(1));
        assert!(host.inventory.adoptable().is_empty());
        assert_eq!(
            host.inventory.resolve_pin(Some("GPU-1a2b")).as_deref(),
            Some("GPU-1a2b"),
            "an excluded card is still passed through verbatim, as before"
        );
        // And nothing can adopt it in: `adopt` only ever moves an adoptable
        // row, so the priced set stays the resolved one.
        host.inventory.adopt("GPU-1a2b");
        assert_eq!(host.inventory.priced_gpus().map(|gpus| gpus.len()), Some(1));
    }

    /// Default placement: highest compute capability, ties broken by
    /// [`GpuInfo::placement_total_mb`] and then the lowest index. The
    /// capacity tie-break is load-bearing on ROCm, where every GPU is
    /// capless.
    #[test]
    fn default_placement_ranks_by_capability_then_capacity_then_index() {
        // Two rows, `GPU-a` then `GPU-b`, each (index, cap, total MiB); then
        // the key placement picks and why.
        #[rustfmt::skip]
        let cases = [
            (0, "8.6", 32607, 1, "12.0", 32607, "GPU-b", "fastest, not first"),
            (0, "9.0", 32607, 1, "12.0", 32607, "GPU-b", "10.x is above 9.x"),
            (0, "12.0", 32607, 3, "12.0", 32607, "GPU-a", "ties: lowest index"),
            (3, "12.0", 8192, 0, "12.0", 8192, "GPU-b", "in any row order"),
            (0, "12.0", 8192, 1, "12.0", 32607, "GPU-b", "ties break on capacity"),
            (0, "8.6", 49152, 1, "12.0", 8192, "GPU-b", "capability outranks it"),
            (0, "", 2048, 1, "", 24576, "GPU-b", "the all-capless ROCm shape"),
        ];
        for (ai, acap, amb, bi, bcap, bmb, expected, label) in cases {
            let host = GpuInventory::known(vec![
                sized_gpu(ai, "GPU-a", acap, amb),
                sized_gpu(bi, "GPU-b", bcap, bmb),
            ]);
            assert_eq!(host.default_pin().as_deref(), Some(expected), "{label}");
        }
    }

    /// Default placement on a dGPU+APU host compares carve-outs, not
    /// budgets, with an eighth-of-budget floor.
    /// See docs/unified-memory-admission.md "Backend B: AMD APUs (ROCm)".
    #[test]
    fn default_placement_compares_an_apus_carve_out_not_its_budget() {
        const DGPU: &str = "AMD gfx1100 (24 GB)";
        const APU: &str = "AMD gfx1151 APU (128 GB)";
        const GTT: u64 = 64 * 1024;
        const RAM: u64 = 128 * 1024;
        // (APU carve-out and GTT, the card's VRAM) -> the GPU placement picks.
        #[rustfmt::skip]
        let cases = [
            (512, GTT, 24_576, DGPU, "1", "a 64.5 GB budget loses to 24 GB VRAM"),
            (96 * 1024, 16 * 1024, 24_576, APU, "0", "a real carve-out wins"),
            (512, GTT, 2048, APU, "0", "an eighth still beats a token card"),
        ];
        for (carveout, gtt, dgpu_mb, name, pin, label) in cases {
            let host = GpuInventory::known_rocm(vec![
                amd_apu(0, "0000:03:00.0", carveout, gtt, RAM),
                amd_gpu(1, "0000:0c:00.0", dgpu_mb),
            ]);
            assert_eq!(host.default_gpu_name().as_deref(), Some(name), "{label}");
            assert_eq!(host.default_pin().as_deref(), Some(pin), "{label}");
        }
    }

    /// The refresh interface follows the inventory, so a ROCm host never asks
    /// nvidia-smi about an AMD GPU — with no GPUs either. The query carries
    /// each row's unified flag, since GTT and `MemAvailable` are read only
    /// for those rows.
    #[test]
    fn the_memory_query_follows_the_inventory_backend() {
        assert_eq!(inventory().memory_query().free_source(), "nvidia-smi");
        let unknown = GpuInventory::unknown().memory_query();
        assert_eq!(unknown.free_source(), "nvidia-smi", "nothing to refresh");
        let host = rocm_inventory_with(
            PathBuf::from("/sys/bus/pci/devices"),
            PathBuf::from("/proc/meminfo"),
            vec![
                amd_apu(0, "0000:03:00.0", 512, 64 * 1024, 128 * 1024),
                amd_gpu(1, "0000:0c:00.0", 24_576),
            ],
        );
        let query = host.memory_query();
        assert_eq!(
            query.free_source(),
            "amdgpu-sysfs",
            "the driver, not the filesystem: a future generic \"sysfs\" \
             reporter must not inherit authority by string collision"
        );
        match query {
            MemoryQuery::RocmSysfs { gpus, meminfo, .. } => {
                let rows: Vec<_> = gpus
                    .iter()
                    .map(|g| (g.key.as_str(), g.bdf.as_str(), g.unified))
                    .collect();
                assert_eq!(
                    rows,
                    vec![
                        ("GPU-BDF-0000:03:00.0", "0000:03:00.0", true),
                        ("GPU-BDF-0000:0c:00.0", "0000:0c:00.0", false),
                    ]
                );
                assert_eq!(meminfo, PathBuf::from("/proc/meminfo"));
            }
            other => panic!("expected the sysfs query, got {other:?}"),
        }

        // No refresh at all, for either reason: no GPUs, or a row with no
        // PCI address to locate its counters by — refreshing the rest would
        // leave the ledger pricing that one off a stale reading.
        let mut no_address = amd_gpu(1, "0000:0c:00.0", 24576);
        no_address.bdf = None;
        for host in [
            uninventoried_rocm(false),
            uninventoried_rocm(true),
            rocm_inventory(
                PathBuf::from("/sys/bus/pci/devices"),
                vec![amd_gpu(0, "0000:03:00.0", 24576), no_address],
            ),
        ] {
            let query = host.memory_query();
            assert!(matches!(query, MemoryQuery::Unavailable), "{query:?}");
            assert!(query.run().is_none());
            assert_eq!(
                query.free_source(),
                "amdgpu-sysfs",
                "still a ROCm host; it just never records anything"
            );
        }
    }

    /// The unified-GPU resolver: the **address** of the GPU a registry entry
    /// names, when that GPU is unified — from the same request the pin and the
    /// key are, so the worker can check the claim against where it came up.
    #[test]
    fn a_unified_pin_resolves_to_its_gpus_address() {
        const APU_BDF: &str = "0000:03:00.0";
        let apu = || amd_apu(0, APU_BDF, 512, 64 * 1024, 128 * 1024);
        let host = GpuInventory::known_rocm(vec![apu(), amd_gpu(1, "0000:0c:00.0", 24_576)]);
        for requested in ["0", "GPU-BDF-0000:03:00.0"] {
            let got = host.unified_pin_bdf(Some(requested));
            assert_eq!(got.as_deref(), Some(APU_BDF), "{requested:?} is the APU");
        }
        // The dGPU, an unpinned replica (which lands on it), and a pin naming
        // nothing we enumerated all resolve to no claim at all.
        for requested in [Some("1"), None, Some("7"), Some("GPU-1a2b")] {
            assert_eq!(host.unified_pin_bdf(requested), None, "{requested:?}");
        }
        // An APU-only host: the default GPU *is* the unified one. Never on
        // the other backends — a CUDA GPU is not unified, and an MPS worker's
        // tiers are unified by construction and read no flag.
        let apu_only = GpuInventory::known_rocm(vec![apu()]);
        assert_eq!(apu_only.unified_pin_bdf(None).as_deref(), Some(APU_BDF));
        assert_eq!(inventory().unified_pin_bdf(None), None);
        assert_eq!(mps_inventory(128).unified_pin_bdf(None), None);
        assert_eq!(uninventoried_rocm(false).unified_pin_bdf(Some("0")), None);
    }

    /// The whole refresh end to end against a fixture PCI tree, which the
    /// inventory carries — so this is the production path, not a copy of it.
    #[test]
    fn the_rocm_refresh_reads_live_memory_from_the_probed_roots() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pci = dir.path().join("pci");
        let write = |bdf: &str, total: u64, used: u64| {
            let gpu = super::rocm::pci_device_dir(&pci, bdf);
            std::fs::create_dir_all(&gpu).unwrap();
            std::fs::write(gpu.join("mem_info_vram_total"), format!("{total}\n")).unwrap();
            std::fs::write(gpu.join("mem_info_vram_used"), format!("{used}\n")).unwrap();
        };
        const GB: u64 = 1024 * 1024 * 1024;
        write("0000:03:00.0", 24 * GB, 4 * GB);
        write("0000:0c:00.0", 16 * GB, 0);
        let host = rocm_inventory(
            pci.clone(),
            vec![
                amd_gpu(0, "0000:03:00.0", 24 * 1024),
                amd_gpu(1, "0000:0c:00.0", 16 * 1024),
            ],
        );
        let read: Vec<_> = host
            .memory_query()
            .run()
            .expect("both GPUs read")
            .into_iter()
            .map(|m| (m.uuid, m.total_mb, m.free_mb))
            .collect();
        assert_eq!(
            read,
            vec![
                ("GPU-BDF-0000:03:00.0".to_owned(), 24 * 1024, 20 * 1024),
                ("GPU-BDF-0000:0c:00.0".to_owned(), 16 * 1024, 16 * 1024),
            ]
        );
        // All-or-nothing: one GPU whose counters are gone makes the whole
        // snapshot unknown rather than pricing its external usage as zero.
        let partial = rocm_inventory(
            pci,
            vec![
                amd_gpu(0, "0000:03:00.0", 24 * 1024),
                amd_gpu(1, "0000:ff:00.0", 16 * 1024),
            ],
        );
        assert!(partial.memory_query().run().is_none());
    }

    /// The dispatch itself: each accelerator gets its own backend, whatever
    /// the host running the test has installed. ROCm off Linux and MPS off
    /// macOS are unknown-but-still-themselves.
    #[test]
    fn the_probe_dispatches_on_the_resolved_accelerator() {
        #[cfg(not(target_os = "linux"))]
        {
            let rocm = probe(Accelerator::Rocm).inventory;
            assert!(rocm.accelerators().is_none(), "no KFD topology off Linux");
            let query = rocm.memory_query();
            assert!(matches!(query, MemoryQuery::Unavailable), "{query:?}");
        }
        for accelerator in [Accelerator::Cuda, Accelerator::Auto] {
            let host = probe(accelerator);
            assert!(
                matches!(host.inventory.memory_query(), MemoryQuery::NvidiaSmi),
                "{accelerator:?} must keep the nvidia-smi path"
            );
            assert!(
                host.inventory
                    .gpus()
                    .unwrap_or(&[])
                    .iter()
                    .all(|gpu| gpu.bdf.is_none() && gpu.gfx_target_version.is_none()),
                "{accelerator:?} must not have gone through the ROCm parser"
            );
        }

        // MPS: its own backend on `Mps` and nothing else, unknown-but-still-
        // MPS off macOS, and no capability analogue to filter with.
        let mps = probe(Accelerator::Mps);
        assert!(matches!(mps.inventory.backend, MemoryBackend::Mps));
        assert_eq!(mps.caps.meets_floor(8.0), None);
        assert_eq!(mps.inventory.resolve_pin(Some("0")), None);
        #[cfg(not(target_os = "macos"))]
        {
            assert!(
                mps.inventory.accelerators().is_none(),
                "no sysctl off macOS"
            );
            assert!(matches!(
                mps.inventory.memory_query(),
                MemoryQuery::Unavailable
            ));
        }
        #[cfg(target_os = "macos")]
        {
            let gpu = mps.inventory.gpus().expect("Apple Silicon")[0].clone();
            assert_eq!(gpu.uuid, "GPU-MPS");
            assert!(gpu.unified() && gpu.total_mb > 0);
        }

        // CPU: `Cpu` is the host with no accelerator at all, and the only one
        // whose accelerator list is empty. The CPU *device* is on every host,
        // which the loop below holds.
        let cpu = probe(Accelerator::Cpu);
        assert!(cpu.inventory.accelerators().is_none());
        assert_eq!(
            cpu.caps.meets_floor(8.0),
            None,
            "a CPU host filters no model by a GPU capability: its workers are \
             pinned to the CPU device, and the impls' own load-time guard is \
             the backstop"
        );
        assert_eq!(cpu.inventory.resolve_pin(Some("0")), None);
        // Every host carries the CPU device, whatever its accelerator is: a
        // worker may run on the CPU on any of them and is priced against RAM
        // there. Every platform this ships to has a RAM reader, so the device
        // here is real rather than fixture-shaped.
        #[cfg(any(target_os = "linux", target_os = "windows", target_os = "macos"))]
        for accelerator in [
            Accelerator::Cpu,
            Accelerator::Cuda,
            Accelerator::Rocm,
            Accelerator::Mps,
            Accelerator::Auto,
        ] {
            let host = probe(accelerator).inventory;
            let gpu = host
                .gpus()
                .expect("a host with RAM")
                .last()
                .expect("the CPU device is appended last")
                .clone();
            assert_eq!(gpu.uuid, "CPU", "{accelerator:?}");
            if accelerator == Accelerator::Cpu {
                assert_eq!(
                    host.gpus().expect("a host with RAM").len(),
                    1,
                    "a host with no accelerator has exactly the CPU device"
                );
            }
            assert!(gpu.unified() && gpu.total_mb > 0);
            assert!(gpu.name.starts_with("CPU ("), "name: {}", gpu.name);
            assert_eq!(host.cpu_memory_query().free_source(), "ram");
            assert_eq!(host.device_kind("CPU"), "cpu", "{accelerator:?}");
            assert!(
                !host
                    .accelerators()
                    .unwrap_or(&[])
                    .iter()
                    .any(|gpu| gpu.uuid == "CPU"),
                "{accelerator:?}: the CPU device is never one of the \
                 accelerators the GPU rules read"
            );
        }
        for accelerator in [
            Accelerator::Cuda,
            Accelerator::Rocm,
            Accelerator::Mps,
            Accelerator::Auto,
        ] {
            let host = probe(accelerator).inventory;
            assert!(
                !matches!(host.backend, MemoryBackend::Mps) || accelerator == Accelerator::Mps,
                "{accelerator:?} has its own backend and must not borrow MPS's"
            );
        }
    }

    /// The two pinless backends: one constant-keyed device each, no pin in
    /// any vocabulary, and a device key that still resolves — so
    /// reservations, budgets and the ledger work as on a pinned host. The
    /// refresh reads RAM statistics under the worker's own label, never
    /// nvidia-smi, including when no device could be built at all.
    #[test]
    fn a_pinless_backend_has_a_device_key_but_never_a_pin() {
        for (host, key, name, source, ram_mb) in [
            (
                mps_inventory(128),
                "GPU-MPS",
                "Apple M3 Max (128 GB)",
                "mps",
                128 * 1024,
            ),
            (
                GpuInventory::known_cpu(64 * 1024),
                "CPU",
                "CPU (64 GB)",
                "ram",
                64 * 1024,
            ),
        ] {
            let gpus = host.gpus().expect("known");
            assert_eq!(gpus.len(), 1);
            assert_eq!(gpus[0].uuid, key);
            assert!(gpus[0].unified());
            let keyspace = host.default_gpu_name();
            assert_eq!(keyspace.as_deref(), Some(name), "the calibration key");
            assert_eq!(host.default_pin(), None);
            assert_eq!(host.unified_pin_bdf(None), None, "no address to verify");
            for requested in [None, Some(key), Some("0"), Some(""), Some("GPU-1a2b")] {
                // Except the one request that is honoured everywhere, which
                // on the CPU host is spelled with its own key
                // ([`a_cpu_pin_is_honoured_where_there_is_no_pin_vocabulary`]).
                if is_cpu_request(requested) {
                    continue;
                }
                let pin = host.resolve_pin(requested);
                assert_eq!(pin, None, "{requested:?} must reach no variable");
            }
            // The ledger vocabulary is unaffected.
            assert_eq!(host.resolve_device_key(None).as_deref(), Some(key));
            let lower = key.to_ascii_lowercase();
            assert_eq!(host.resolve_device_key(Some(&lower)).as_deref(), Some(key));
            assert_eq!(host.resolve_device_key(Some("GPU-1a2b")), None);
            // The refresh: RAM statistics, bounded by physical RAM.
            let query = if key == "CPU" {
                host.cpu_memory_query()
            } else {
                host.memory_query()
            };
            assert_eq!(query.free_source(), source);
            match &query {
                MemoryQuery::Mps { key: k, ram_mb: mb }
                | MemoryQuery::Cpu {
                    key: k, ram_mb: mb, ..
                } => {
                    assert_eq!(k, key);
                    assert_eq!(*mb, ram_mb, "physical RAM, not the budget");
                }
                other => panic!("expected a pinless query, got {other:?}"),
            }
        }
        assert!(
            !GpuInventory::known_cpu(64 * 1024).adopts_worker_total(),
            "a CPU device's total is physical RAM, known at probe time: there \
             is nothing for a worker to adopt it from (only MPS adopts one)"
        );

        // No device at all (off-platform, or a reader that said nothing): the
        // backend is still set, so nothing falls back to nvidia-smi.
        let unprobed_cpu = GpuInventory {
            gpus: None,
            adoptable: None,
            adopted: Arc::default(),
            backend: MemoryBackend::Cpu,
            cpu_roots: None,
            blank_mask: false,
        };
        assert!(
            matches!(unprobed_cpu.cpu_memory_query(), MemoryQuery::Unavailable),
            "a host whose RAM could not be read has no CPU device to refresh"
        );
        for unprobed in [
            GpuInventory {
                gpus: None,
                adoptable: None,
                adopted: Arc::default(),
                backend: MemoryBackend::Mps,
                cpu_roots: None,
                blank_mask: false,
            },
            unprobed_cpu,
        ] {
            let query = unprobed.memory_query();
            assert!(matches!(query, MemoryQuery::Unavailable), "{query:?}");
            assert_eq!(unprobed.resolve_pin(Some("0")), None);
            assert_eq!(unprobed.default_pin(), None);
            assert_eq!(unprobed.resolve_device_key(None), None);
        }
    }

    /// ROCm pin resolution, by request form: a device key translates to its
    /// row index (in full, never by prefix), numeric forms pass through
    /// canonicalised so `prewarm.rs` keeps matching `default_pin`, and
    /// anything HIP could not read as an index is dropped rather than
    /// written — it would hide every device and drop the worker to the CPU.
    #[test]
    fn rocm_pins_resolve_by_request_form() {
        let mut fused = amd_gpu(0, "0000:03:00.0", 24576);
        fused.uuid = "GPU-0123456789abcdef".to_owned();
        let host = rocm_inventory(
            PathBuf::from("/sys/bus/pci/devices"),
            vec![fused, amd_gpu(1, "0000:0c:00.0", 24576)],
        );
        assert_eq!(host.default_pin().as_deref(), Some("0"));
        assert_eq!(host.resolve_pin(None).as_deref(), Some("0"), "no pin");
        #[rustfmt::skip]
        let translated = [
            ("GPU-0123456789abcdef", "0", "fused KFD unique_id"),
            ("GPU-BDF-0000:0c:00.0", "1", "synthetic BDF form"),
            ("  gpu-bdf-0000:0C:00.0  ", "1", "case-insensitive, trimmed"),
            ("1", "1", "an index"),
            (" 0 ", "0", "trimmed"),
            ("7", "7", "an unreported index is still HIP-legal"),
            ("0,1", "0,1", "a list the ledger cannot price"),
            (" 1 , 2 ", "1,2", "canonicalised"),
            ("0,", "0", "a trailing separator, as HIP reads it"),
        ];
        for (requested, expected, label) in translated {
            let got = host.resolve_pin(Some(requested));
            assert_eq!(got.as_deref(), Some(expected), "{requested:?}: {label}");
        }
        // Dropped rather than written: a truncated key (no prefix arm on
        // ROCm), an index past u32, a CUDA UUID, a key we do not have, a
        // template, a stray word, a mixed list — and the empty forms, which a
        // failed expansion produces and which must not silently mean `no pin`
        // (that would pin the replica to the default GPU nobody named).
        #[rustfmt::skip]
        let dropped = [
            "GPU-BDF-0000:0c", "4294967296", "GPU-1a2b", "GPU-BDF-0000:ff:00.0",
            "${DEVICE}", "0,GPU-BDF-0000:03:00.0", "", "   ", ",",
        ];
        for requested in dropped {
            assert_eq!(host.resolve_pin(Some(requested)), None, "{requested:?}");
        }
        // Every spelling of the default GPU has to render identically to
        // `default_pin` or the prewarm pool stops claiming. A leading `+`,
        // which `u32::from_str` accepts, is normalised away rather than
        // forwarded, so HIP never sees it.
        for spelling in ["00", " 0 ", "+0", "0000"] {
            assert_eq!(
                host.resolve_pin(Some(spelling)),
                host.default_pin(),
                "{spelling:?} must render like the default pin"
            );
        }
        // Placement ranks by VRAM (every `compute_cap` is `None`) and answers
        // in HIP's vocabulary: the row index, never the key.
        let mixed = rocm_inventory(
            PathBuf::from("/sys/bus/pci/devices"),
            vec![
                amd_gpu(0, "0000:03:00.0", 2048),
                amd_gpu(1, "0000:0c:00.0", 24576),
            ],
        );
        assert_eq!(mixed.default_pin().as_deref(), Some("1"));
        assert_eq!(mixed.resolve_pin(None).as_deref(), Some("1"));

        // A ROCm host that found no GPUs has nothing to translate a key
        // against, but HIP's grammar still applies: an index survives
        // canonicalised, and everything else is dropped rather than passed
        // through the way an unknown *CUDA* host would pass it.
        let blank = uninventoried_rocm(false);
        assert!(blank.gpus().is_none());
        for (requested, expected) in [("0", "0"), ("0,1", "0,1"), (" 1 , 2 ", "1,2"), ("00", "0")] {
            let got = blank.resolve_pin(Some(requested));
            assert_eq!(got.as_deref(), Some(expected), "{requested:?} with no GPUs");
        }
        for requested in ["GPU-1a2b", "${DEVICE}", "", "4294967296"] {
            assert_eq!(blank.resolve_pin(Some(requested)), None, "{requested:?}");
        }
        assert_eq!(
            blank.resolve_pin(Some("cpu")).as_deref(),
            Some(""),
            "a cpu pin hides every GPU, inventory or not"
        );
        assert_eq!(blank.resolve_pin(None), None, "no GPUs is no default GPU");
        assert_eq!(blank.default_pin(), None);
        // The GPU *name* is still available — /metadata's calibration overlay
        // needs it — and it never reaches a worker's environment.
        assert_eq!(
            mixed.default_gpu_name().as_deref(),
            Some("AMD gfx1100 (24 GB)")
        );
    }

    /// The pin *vocabulary* and the pin *variable* are one decision:
    /// `pins_are_indices` is the single source of the first, and it and
    /// [`pin_env_var`] must never disagree, because a GPU UUID in
    /// `HIP_VISIBLE_DEVICES` (or an index in `CUDA_VISIBLE_DEVICES`) hides
    /// every GPU from the worker. Asserted against the real `probe`, where
    /// the two are wired together.
    #[test]
    fn the_pin_vocabulary_and_the_pin_variable_agree() {
        // ROCm, including on this box — the probe finds no AMD GPUs off
        // Linux, and that must not change the answer.
        assert_eq!(pin_env_var(Accelerator::Rocm), HIP_PIN_ENV_VAR);
        assert!(
            probe(Accelerator::Rocm).inventory.pins_are_indices(),
            "a ROCm host pins by index whether or not its probe found GPUs"
        );
        let known_rocm = rocm_inventory(
            PathBuf::from("/sys/bus/pci/devices"),
            vec![amd_gpu(0, "0000:03:00.0", 24576)],
        );
        assert!(known_rocm.pins_are_indices());
        assert_eq!(
            known_rocm.default_pin().as_deref(),
            Some("0"),
            "an index — never the GPU-BDF-… key the ledger is keyed by"
        );
        assert_eq!(
            known_rocm.gpus().expect("known")[0].uuid,
            "GPU-BDF-0000:03:00.0",
            "and the key is still there, for everything but the pin"
        );
        // CUDA, and every accelerator that is not ROCm.
        for accelerator in [Accelerator::Cuda, Accelerator::Cpu, Accelerator::Auto] {
            assert_eq!(pin_env_var(accelerator), CUDA_PIN_ENV_VAR);
        }
        assert!(!probe(Accelerator::Cuda).inventory.pins_are_indices());
        assert_eq!(
            inventory().default_pin().as_deref(),
            Some("GPU-1111"),
            "a UUID, which is the only unambiguous form CUDA takes"
        );

        // An unknown non-ROCm inventory passes the request through verbatim:
        // nothing filtered, nothing normalised, because CUDA is the one that
        // reads it and an unresolvable string there is the operator's to
        // explain. It has no ledger row to key against either.
        let unknown = GpuInventory::unknown();
        assert!(unknown.gpus().is_none());
        for requested in ["1", "GPU-1a2b", "${DEVICE}", " 0 ", ""] {
            let pin = unknown.resolve_pin(Some(requested));
            assert_eq!(pin.as_deref(), Some(requested), "{requested:?} verbatim");
            assert_eq!(unknown.resolve_device_key(Some(requested)), None);
        }
        assert_eq!(unknown.resolve_pin(None), None);
        assert_eq!(unknown.default_pin(), None);
        assert_eq!(unknown.resolve_device_key(None), None);
    }

    /// When the operator's own ambient restriction is at HIP's layer, it
    /// wins outright: we write nothing, not even the index we would
    /// otherwise be allowed to write. Ours would overwrite theirs (same
    /// variable) or outrank it (the alias), handing the worker GPUs they
    /// deliberately hid.
    ///
    /// An ambient `ROCR_VISIBLE_DEVICES` alone is the other case and does
    /// **not** set the flag: it filters below HIP, so a HIP index counts
    /// into the operator's set instead of escaping it.
    #[test]
    fn an_ambient_hip_restriction_outranks_a_registry_pin() {
        // The guard sits at the top of `resolve_pin`, before the inventory is
        // consulted, so it cannot be bypassed by a GPU list. The probe never
        // produces that combination today (any HIP-layer variable also blanks
        // the inventory), which is why the guard has to be positional rather
        // than rely on that invariant holding forever.
        let with_gpus = GpuInventory {
            gpus: Some(vec![amd_gpu(0, "0000:03:00.0", 24576)].into()),
            adoptable: None,
            adopted: Arc::default(),
            backend: MemoryBackend::RocmSysfs {
                pci_devices: PathBuf::from("/sys/bus/pci/devices"),
                meminfo: PathBuf::from("/proc/meminfo"),
                ambient_hip_restriction: true,
            },
            cpu_roots: None,
            blank_mask: false,
        };
        for host in [uninventoried_rocm(true), with_gpus] {
            for requested in [
                None,
                Some("0"),
                Some("0,1"),
                Some("GPU-BDF-0000:03:00.0"),
                Some("GPU-1a2b"),
                Some(""),
            ] {
                let pin = host.resolve_pin(requested);
                assert_eq!(pin, None, "{requested:?} over their restriction");
            }
            // Except a `cpu` pin, which asks for no GPU at all: hiding every
            // one of them cannot hand the worker a GPU they hid.
            assert_eq!(host.resolve_pin(Some("cpu")).as_deref(), Some(""));
        }
        // The flag is what distinguishes the two cases, and comes from the
        // same positional array the probe reads the environment into.
        use super::rocm::{VISIBILITY_VARS, ambient_hip_restriction};
        let one = |set: &str| {
            ambient_hip_restriction(VISIBILITY_VARS.map(|var| (var == set).then_some("0")))
        };
        #[rustfmt::skip]
        let cases = [
            ("ROCR_VISIBLE_DEVICES", false, "composes with a HIP index"),
            ("HIP_VISIBLE_DEVICES", true, "the variable we write"),
            ("CUDA_VISIBLE_DEVICES", true, "the alias we outrank"),
            ("GPU_DEVICE_ORDINAL", true, "the same layer"),
            ("NOTHING_SET_AT_ALL", false, "nothing set"),
        ];
        for (var, expected, label) in cases {
            assert_eq!(one(var), expected, "{var}: {label}");
        }
        // Both set: the scan must not stop at ROCR, which comes first.
        assert!(ambient_hip_restriction(VISIBILITY_VARS.map(|var| {
            (var == "ROCR_VISIBLE_DEVICES" || var == "HIP_VISIBLE_DEVICES").then_some("0")
        })));
        // Whitespace/comma-only values are "not configured", as everywhere.
        assert!(!ambient_hip_restriction(
            VISIBILITY_VARS.map(|var| (var == "HIP_VISIBLE_DEVICES").then_some(" , "))
        ));
    }

    /// A `cpu` pin on the hosts with no pin vocabulary (MPS and CPU) resolves
    /// to the empty pin, not to `None`: `None` equals
    /// [`GpuInventory::default_pin`] there, and would let the replica claim a
    /// pooled worker spawned for the Metal device (`prewarm.rs`).
    #[test]
    fn a_cpu_pin_is_honoured_where_there_is_no_pin_vocabulary() {
        for inventory in [
            GpuInventory::known_mps(128 * 1024),
            GpuInventory::known_cpu(64 * 1024),
        ] {
            assert_eq!(inventory.default_pin(), None);
            assert_eq!(inventory.resolve_pin(None), None);
            assert_eq!(inventory.resolve_pin(Some("0")), None);
            for spelling in ["cpu", "CPU", " cpu "] {
                assert_eq!(
                    inventory.resolve_pin(Some(spelling)).as_deref(),
                    Some(""),
                    "{spelling:?}"
                );
                assert_eq!(
                    inventory.resolve_device_key(Some(spelling)).as_deref(),
                    Some(cpu::DEVICE_KEY),
                    "{spelling:?}"
                );
            }
        }
        // On MPS it is the one request that does not land where every other
        // one does.
        let mps = GpuInventory::known_mps(128 * 1024);
        assert_eq!(
            mps.resolve_device_key(None).as_deref(),
            Some(mps::DEVICE_KEY)
        );
    }

    /// CUDA pin resolution, by request form. A request naming a visible GPU
    /// comes back in the **inventory's** spelling, because `prewarm.rs`
    /// compares pin strings byte-wise; anything else reaches
    /// `CUDA_VISIBLE_DEVICES` unchanged, since resolving it is CUDA's job.
    #[test]
    fn cuda_pins_resolve_by_request_form() {
        let inventory = inventory();
        assert_eq!(
            inventory.resolve_pin(None).as_deref(),
            Some("GPU-1111"),
            "no pin is the default GPU"
        );
        #[rustfmt::skip]
        let cases = [
            ("3", "GPU-3333", "an index names a row"),
            (" 0 ", "GPU-1111", "trimmed"),
            ("GPU-9999", "GPU-9999", "a UUID we cannot see"),
            ("MIG-abc", "MIG-abc", "a MIG instance"),
            ("7", "7", "an unreported index"),
            ("0,3", "0,3", "a device list"),
            ("zzz", "zzz", "a non-numeric string"),
            // The CPU device is the one string that is not passed through:
            // it names a device this host has, and hiding every GPU is how
            // CUDA is told so.
            ("cpu", "", "the CPU device"),
        ];
        for (requested, expected, label) in cases {
            let got = inventory.resolve_pin(Some(requested));
            assert_eq!(got.as_deref(), Some(expected), "{requested:?}: {label}");
        }

        const FFFF: &str = "GPU-ffff0000-0000-0000-0000-000000000000";
        let abbrev = GpuInventory::known(vec![
            gpu(0, "GPU-1a2b0000-0000-0000-0000-000000000000", "12.0"),
            gpu(1, "GPU-1a2b9999-0000-0000-0000-000000000000", "12.0"),
            gpu(2, FFFF, "12.0"),
        ]);
        for (requested, expected, label) in [
            (
                "gpu-FFFF0000-0000-0000-0000-000000000000",
                Some(FFFF),
                "case",
            ),
            ("  GPU-ffff  ", Some(FFFF), "unambiguous abbreviation"),
            ("GPU-1a2b", Some("GPU-1a2b"), "shared prefix: verbatim"),
            ("GPU-deadbeef", Some("GPU-deadbeef"), "a GPU we cannot see"),
            ("MIG-abc", Some("MIG-abc"), "outside the enumeration"),
        ] {
            let got = abbrev.resolve_pin(Some(requested));
            assert_eq!(got.as_deref(), expected, "{requested:?}: {label}");
        }
        // Pin and ledger key agree for every spelling that names a GPU, which
        // is what the pool compares.
        for spelling in ["GPU-ffff", "gpu-FFFF0000", FFFF, "2"] {
            assert_eq!(
                abbrev.resolve_pin(Some(spelling)),
                abbrev.resolve_device_key(Some(spelling)),
                "{spelling:?} must resolve to one string on both sides"
            );
        }
    }

    /// The ledger vocabulary of the same registry entry, on both backends —
    /// pin and key are resolved as a pair from one request, and on ROCm they
    /// are never the same string. CUDA resolves abbreviated UUIDs itself, so
    /// the ledger must too; an ambiguous one resolves to nothing, since
    /// reserving against the wrong GPU is worse than not reserving.
    #[test]
    fn device_keys_resolve_by_request_form() {
        let cuda = inventory();
        assert_eq!(
            cuda.resolve_device_key(None).as_deref(),
            Some("GPU-1111"),
            "no request is the default GPU, as for the pin"
        );
        assert_eq!(cuda.resolve_pin(None), cuda.resolve_device_key(None));
        for (requested, expected) in [("3", "GPU-3333"), (" gpu-3333 ", "GPU-3333")] {
            let got = cuda.resolve_device_key(Some(requested));
            assert_eq!(got.as_deref(), Some(expected), "{requested:?}");
        }
        // An unreported index, a list, a bare string and an unseen UUID. A
        // `cpu` request answers `None` here too *on this fixture*, which has
        // no CPU device; over a probed inventory it resolves to that device.
        for requested in ["7", "0,3", "cpu", "GPU-9999"] {
            assert_eq!(
                cuda.resolve_device_key(Some(requested)),
                None,
                "{requested:?}"
            );
        }

        const FFFF: &str = "GPU-ffff0000-0000-0000-0000-000000000000";
        let abbrev = GpuInventory::known(vec![
            gpu(0, "GPU-1a2b0000-0000-0000-0000-000000000000", "12.0"),
            gpu(1, "GPU-1a2b9999-0000-0000-0000-000000000000", "12.0"),
            gpu(2, FFFF, "12.0"),
        ]);
        for requested in ["GPU-ffff", "gpu-FFFF0000"] {
            let got = abbrev.resolve_device_key(Some(requested));
            assert_eq!(got.as_deref(), Some(FFFF), "{requested:?} is unambiguous");
        }
        // A shared prefix (`GPU-` included) and a MIG instance outside the
        // enumeration name no row: refuse rather than guess.
        for requested in ["GPU-1a2b", "GPU-", "MIG-unknown"] {
            assert_eq!(
                abbrev.resolve_device_key(Some(requested)),
                None,
                "{requested:?}"
            );
        }
        // On a single-GPU host that degenerate prefix *is* unambiguous and
        // resolves, as CUDA itself does, so the reservation lands on the GPU
        // the pin will select.
        let only = GpuInventory::known(vec![gpu(0, FFFF, "12.0")]);
        assert_eq!(only.resolve_device_key(Some("GPU-")).as_deref(), Some(FFFF));

        let rocm = rocm_inventory(
            PathBuf::from("/sys/bus/pci/devices"),
            vec![
                amd_gpu(0, "0000:03:00.0", 24576),
                amd_gpu(1, "0000:0c:00.0", 24576),
            ],
        );
        const KEY0: &str = "GPU-BDF-0000:03:00.0";
        const KEY1: &str = "GPU-BDF-0000:0c:00.0";
        assert_eq!(
            rocm.resolve_device_key(None).as_deref(),
            Some(KEY0),
            "the default GPU, whose pin for the same request is `0`"
        );
        for (requested, expected) in [("1", KEY1), ("GPU-BDF-0000:0C:00.0", KEY1)] {
            let got = rocm.resolve_device_key(Some(requested));
            assert_eq!(got.as_deref(), Some(expected), "{requested:?}");
        }
        // No prefix arm on ROCm: a prefix could name two GPUs on one bus, and
        // these keys never reach HIP.
        for requested in ["GPU-BDF-0000:0c", "9", "0,1"] {
            assert_eq!(
                rocm.resolve_device_key(Some(requested)),
                None,
                "{requested:?}"
            );
        }
        // The pair: HIP gets the index, the ledger gets the key.
        assert_eq!(rocm.resolve_pin(None).as_deref(), Some("0"));
        assert_eq!(rocm.resolve_pin(Some("1")).as_deref(), Some("1"));
    }
}
