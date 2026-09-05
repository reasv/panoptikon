//! GPU identity enumeration and worker→GPU pin resolution.
//!
//! Budgets are keyed by GPU *instance* (the `GPU-…` UUID), never by a device
//! index, which moves across reboots and `CUDA_VISIBLE_DEVICES` changes. This
//! module is the source of those identities and the one place that turns a
//! registry `devices` entry into the pin a worker is spawned with — plus, in
//! [`pin_env_var`], the variable it is written to: a GPU UUID belongs in
//! `CUDA_VISIBLE_DEVICES` and only an index in `HIP_VISIBLE_DEVICES`, and
//! crossing the two hides every GPU from the worker.
//! See docs/batch-calibration-design.md "Two keyspaces".
//!
//! [`probe`] takes the **resolved** accelerator and dispatches four ways:
//! `Rocm` to `rocm.rs`, `Mps` and `Cpu` to one synthetic device each
//! (`mps.rs`, `cpu.rs`), `Cuda`/`Auto` to the nvidia-smi path below. Only the
//! CUDA path has a capability view; the other three have no
//! compute-capability analogue at all. See
//! docs/rocm-batch-calibration-parity.md (D1/D7) and
//! docs/unified-memory-admission.md (backends A and C).
//!
//! The CUDA path is one `--query-gpu` call for both hardware facts the server
//! needs — identities here, capabilities in `capability.rs` — with rows
//! matched positionally, so the two views can never disagree about which GPU
//! is which. Any unparseable *identity* makes the whole result unknown, and
//! unknown never changes behaviour: pins pass through untouched.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::capability::{HostComputeCaps, find_nvidia_smi, output_with_timeout, parse_compute_cap};
use super::cpu;
use super::mps;
use super::rocm;
use crate::config::Accelerator;

/// The variable CUDA filters devices with, and HIP's compatibility alias for
/// its own. Written with a `GPU-…` GPU UUID on CUDA hosts.
pub const CUDA_PIN_ENV_VAR: &str = "CUDA_VISIBLE_DEVICES";

/// HIP's own device filter, written with a **device index**, never a key. It
/// composes with an ambient `ROCR_VISIBLE_DEVICES`, which filters below it.
/// See docs/rocm-batch-calibration-parity.md "D2 (G2) — Pinning".
pub const HIP_PIN_ENV_VAR: &str = "HIP_VISIBLE_DEVICES";

/// Set on a worker pinned to a **unified** GPU so its own memory arithmetic
/// includes GTT; unified ROCm GPUs only. The value is that GPU's **PCI
/// address**, not a flag, so the worker can check the claim against the GPU
/// it resolved for itself and fall back to the discrete arithmetic on a
/// mismatch. See docs/unified-memory-admission.md "Backend B: AMD APUs
/// (ROCm)" (DP-5).
pub const UNIFIED_GPU_ENV_VAR: &str = "PANOPTIKON_UNIFIED_GPU";

/// Written alongside the backend's visibility variable with the same resolved
/// pin: *we* placed this replica, and on this device. An operator's ambient
/// visibility variable is indistinguishable from ours, so the worker's
/// pinned-but-invisible tripwire (`memory.py::pinned_device_missing`) keys
/// off this marker instead.
pub const DEVICE_PIN_MARKER_ENV_VAR: &str = "PANOPTIKON_DEVICE_PIN";

/// Which variable a resolved pin is written to, decided by the **resolved
/// accelerator** rather than by the inventory: a ROCm host with a blank
/// inventory is still a HIP host. Only one variable is ever set.
/// See docs/rocm-batch-calibration-parity.md "D2 (G2) — Pinning".
pub fn pin_env_var(accelerator: Accelerator) -> &'static str {
    match accelerator {
        Accelerator::Rocm => HIP_PIN_ENV_VAR,
        // `Mps`/`Cpu` never yield a pin at all, and `Auto` only reaches here
        // from a caller with no sentinel to resolve with, where the CUDA form
        // is what those hosts already wrote.
        Accelerator::Cuda | Accelerator::Cpu | Accelerator::Mps | Accelerator::Auto => {
            CUDA_PIN_ENV_VAR
        }
    }
}

/// One visible GPU, from nvidia-smi (CUDA) or KFD topology (ROCm).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct GpuInfo {
    /// Enumeration index: nvidia-smi's on CUDA, the position within the
    /// openable KFD-node set on ROCm (which is the HIP device index). Only
    /// for resolving registry `devices = ["3"]` pins; never an identity.
    pub index: u32,
    /// GPU UUID (`GPU-…`), the budget/ledger key and the pin form CUDA
    /// accepts directly. On ROCm it is the fused KFD `unique_id` or a
    /// synthetic `GPU-BDF-…` — an identity only, since HIP takes indices.
    pub uuid: String,
    /// Marketing name, e.g. `NVIDIA GeForce RTX 5090`; the cost-profile key.
    /// On ROCm, the deterministic `AMD gfx…` form `rocm.rs` derives.
    pub name: String,
    pub total_mb: u64,
    /// Compute capability as `major.minor` (`"12.0"`), per GPU because
    /// default placement picks the fastest one. `None` when nvidia-smi did
    /// not report it, and always `None` on ROCm: the GPU stays pinnable but
    /// cannot be ranked or unlock a capability-gated model.
    pub compute_cap: Option<String>,
    /// PCI address `dddd:bb:dd.f`. ROCm only: the key into amdgpu's per-GPU
    /// sysfs counters and the one vocabulary a worker can report about
    /// itself. `None` on CUDA, where the UUID serves both.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bdf: Option<String>,
    /// KFD's packed ISA target (`110000` = gfx1100). ROCm only; recorded so a
    /// future gfx-arch allowlist needs no second probe. `None` on CUDA.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gfx_target_version: Option<u32>,
    /// Host RAM this GPU's memory is carved out of, in MiB — present exactly
    /// on **unified** GPUs, so its presence *is* the unified flag
    /// ([`GpuInfo::unified`]). It bounds the authoritative total a worker may
    /// report, and gates the synthetic negative the ledger records for a
    /// mid-window replica death (docs/unified-memory-admission.md, DP-2/4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unified_ram_mb: Option<u64>,
    /// The device-local VRAM carve-out of a unified **ROCm** GPU, in MiB —
    /// the part of [`Self::total_mb`] that is not GTT, and the placement rank
    /// ([`Self::placement_total_mb`]). `None` on every other GPU. The
    /// registration cross-check accepts it *or* the carve+GTT sum, since HIP
    /// may report either.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vram_carveout_mb: Option<u64>,
}

impl GpuInfo {
    /// Whether this GPU's memory is the host's RAM rather than private
    /// VRAM (see [`Self::unified_ram_mb`]).
    pub fn unified(&self) -> bool {
        self.unified_ram_mb.is_some()
    }

    /// The capacity figure default placement ranks GPUs by:
    /// `max(carve-out, total / 8)` on a unified ROCm GPU, [`Self::total_mb`]
    /// on every other, because carve-out + GTT is not like-for-like against a
    /// dGPU's private VRAM. Placement only — pricing is unaffected.
    /// See docs/unified-memory-admission.md "Backend B: AMD APUs (ROCm)".
    pub fn placement_total_mb(&self) -> u64 {
        match self.vram_carveout_mb {
            Some(carveout) => carveout.max(self.total_mb / 8),
            None => self.total_mb,
        }
    }

    /// `major * 10 + minor`, the comparable form, or `None` for a GPU whose
    /// capability nvidia-smi did not report. Never `0`: unknown must rank as
    /// unranked, not as the slowest GPU.
    fn cap_tenths(&self) -> Option<u32> {
        parse_compute_cap(self.compute_cap.as_deref()?).map(|(major, minor)| major * 10 + minor)
    }
}

/// The visible GPUs, or `None` for "unknown host" (no nvidia-smi, probe
/// failure, or any unparseable output). Cheap to clone. It carries the
/// interface its GPUs were read through, so the ledger's staleness refresh
/// cannot end up asking nvidia-smi about AMD GPUs.
#[derive(Debug, Clone, Default)]
pub struct GpuInventory {
    gpus: Option<Arc<[GpuInfo]>>,
    backend: MemoryBackend,
}

/// Which kernel/driver interface answers this host's live-memory questions —
/// and, by the same token, which vocabulary its pins are written in
/// ([`GpuInventory::pins_are_indices`]). Set from the **resolved
/// accelerator**, not from whether any GPU was found: a ROCm host whose probe
/// came back empty is still a ROCm host.
#[derive(Debug, Clone, Default)]
enum MemoryBackend {
    #[default]
    NvidiaSmi,
    RocmSysfs {
        /// The PCI device root the inventory was **probed** through, so the
        /// staleness refresh reads the same tree (and a fixture drives it).
        pci_devices: PathBuf,
        /// `/proc/meminfo`, from the same probe roots. Read only for
        /// **unified** GPUs, which clamp unclaimed GTT to `MemAvailable`.
        meminfo: PathBuf,
        /// Whether an ambient restriction at HIP's own layer
        /// (`HIP_VISIBLE_DEVICES`, its `CUDA_VISIBLE_DEVICES` alias, or
        /// `GPU_DEVICE_ORDINAL`) was in force when the probe ran; only ever
        /// true alongside a blanked GPU list, and it suppresses our own pin
        /// entirely. `ROCR_VISIBLE_DEVICES` does **not** set it: HIP indexes
        /// into the ROCr-filtered set, so a pin composes with it.
        ambient_hip_restriction: bool,
    },
    /// Apple Silicon: one synthetic unified-memory device (`mps.rs`), whose
    /// live free reading is the host's RAM statistics. No pin vocabulary —
    /// one device, and no variable that selects it.
    Mps,
    /// A host with no accelerator at all: one synthetic device over the
    /// machine's own RAM (`cpu.rs`, docs/unified-memory-admission.md backend
    /// C). No pin vocabulary either — there is no device to select.
    Cpu {
        /// `/proc/meminfo`, from the same roots the probe read the total
        /// through, so the refresh reads the same file.
        meminfo: PathBuf,
    },
}

/// Everything one `nvidia-smi` call tells us about this host's GPUs.
pub struct HostGpus {
    /// Compute-capability floors for `/metadata` availability filtering.
    pub caps: HostComputeCaps,
    /// GPU identities for worker→GPU pinning and the per-GPU ledger.
    pub inventory: GpuInventory,
}

/// Probe once at startup; never fails. `accelerator` is the **resolved** one
/// and is the whole of the dispatch. A CPU device exists exactly when it is
/// [`Accelerator::Cpu`] — never on a *broken* CUDA host, which keeps the
/// unknown-inventory behaviour instead (unpriced, plus the WARN in
/// [`query`]). See docs/unified-memory-admission.md "Backend C: CPU".
pub fn probe(accelerator: Accelerator) -> HostGpus {
    match accelerator {
        Accelerator::Rocm => return probe_rocm(),
        Accelerator::Mps => return probe_mps(),
        Accelerator::Cpu => return probe_cpu(),
        // `Auto` only reaches here from a caller that could not resolve at
        // all, so this arm is effectively `Cuda`.
        Accelerator::Cuda | Accelerator::Auto => {}
    }
    // nvidia-smi *ignores* CUDA_VISIBLE_DEVICES and reports every GPU, so the
    // ambient value has to be applied by hand (see `restrict_to_visible`).
    let visible = std::env::var("CUDA_VISIBLE_DEVICES").ok();
    build(query(accelerator).as_deref(), visible.as_deref())
}

/// KFD topology + amdgpu sysfs (`rocm.rs`); the capability view is always
/// unknown, and off Linux there are no GPUs at all. "Unknown" means *no
/// GPUs*, never *not a ROCm host*: the backend stays
/// [`MemoryBackend::RocmSysfs`] on every path out.
fn probe_rocm() -> HostGpus {
    let roots = rocm::SysfsRoots::default();
    let (inventory, ambient_hip_restriction) = if cfg!(target_os = "linux") {
        let ambient = rocm::VISIBILITY_VARS.map(|var| std::env::var(var).ok());
        let ambient = ambient.each_ref().map(Option::as_deref);
        (
            Some(rocm::build(&roots, ambient)),
            rocm::ambient_hip_restriction(ambient),
        )
    } else {
        // Nothing was read, so nothing is known about the ambient
        // environment either. `None` rather than a failure: there is nothing
        // to diagnose, the paths do not exist on this platform.
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
            backend: backend.clone(),
        },
    };
    let gpus = match inventory {
        Some(Ok(gpus)) => gpus,
        // Unpriced is safe but indistinguishable from "the feature is not
        // working", so the failure is named: `ProbeFailure::log` emits one
        // WARN unless the deciding site already logged the detail.
        Some(Err(failure)) => {
            failure.log();
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

/// One synthetic unified-memory device from macOS kernel facts (`mps.rs`);
/// the capability view is always unknown. Off macOS, and on a macOS whose
/// sysctls did not answer, there are no GPUs — but the backend stays
/// [`MemoryBackend::Mps`], so such a host never inherits CUDA's rules.
fn probe_mps() -> HostGpus {
    let inventory = |gpus: Option<Arc<[GpuInfo]>>| HostGpus {
        caps: HostComputeCaps::unknown(),
        inventory: GpuInventory {
            gpus,
            backend: MemoryBackend::Mps,
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

/// One synthetic device over the host's own RAM (`cpu.rs`); the capability
/// view is always unknown, and a model's floor is left to the Python impl's
/// load-time guard. As on MPS, a host whose RAM statistics could not be read
/// has no devices but keeps [`MemoryBackend::Cpu`].
fn probe_cpu() -> HostGpus {
    let roots = cpu::MemRoots::default();
    let inventory = |gpus: Option<Arc<[GpuInfo]>>| HostGpus {
        caps: HostComputeCaps::unknown(),
        inventory: GpuInventory {
            gpus,
            backend: MemoryBackend::Cpu {
                meminfo: roots.meminfo.clone(),
            },
        },
    };
    let Some(ram_mb) = cpu::probe(&roots) else {
        tracing::warn!(
            "this host has no accelerator and its total RAM could not be \
             read, so it gets no memory ledger, no grants and no calibration \
             — dispatch takes the unpriced path (your cap, then the registry \
             default, then default_max_batch)"
        );
        return inventory(None);
    };
    let gpu = cpu::gpu(ram_mb);
    tracing::info!(
        index = gpu.index,
        uuid = %gpu.uuid,
        name = %gpu.name,
        total_mb = gpu.total_mb,
        // The *shipped* default, not necessarily what binds: a configured
        // `cap_fraction` replaces it later, in the ledger. `/health` prints
        // the figure actually in force.
        default_cap_fraction = cpu::DEFAULT_CAP_FRACTION,
        unified = gpu.unified(),
        "no accelerator on this host; admitting batches against system RAM \
         (running out of it is an OS process kill rather than a catchable \
         allocation failure, so the GPU ships with a default ceiling)"
    );
    inventory(Some(vec![gpu].into()))
}

/// Run the single query. `None` on any failure, each logged — at WARN when
/// the host is positively configured for CUDA, so an empty ledger is never
/// silent.
fn query(accelerator: Accelerator) -> Option<String> {
    let Some(smi) = find_nvidia_smi() else {
        // Only a WARN on a CUDA host: `cpu` and `auto` boxes legitimately
        // have no nvidia-smi.
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

/// How this host's live free/total memory is read, resolved once from the
/// inventory so the ledger never has to know which accelerator it is on.
/// Cheap to clone; the ROCm variant carries the GPU→BDF list because the
/// sysfs counters are per-GPU files with no enumeration of their own.
#[derive(Debug, Clone)]
pub(super) enum MemoryQuery {
    /// One `nvidia-smi --query-gpu` call covering every visible GPU.
    NvidiaSmi,
    /// amdgpu's `mem_info_vram_{total,used}`, one file pair per GPU — plus
    /// the `mem_info_gtt_{total,used}` pair and `MemAvailable` for a GPU the
    /// probe flagged unified, whose budget is carve-out + GTT.
    RocmSysfs {
        pci_devices: PathBuf,
        meminfo: PathBuf,
        /// Every GPU's key, address and unified flag, in inventory order.
        gpus: Arc<[rocm::GpuRef]>,
    },
    /// macOS RAM statistics for the one unified-memory device (`mps.rs`):
    /// what the OS says it could deliver right now, which is what external
    /// pressure looks like on a unified device.
    Mps {
        key: String,
        /// Physical RAM in MiB — a bound on the reading, not the admission
        /// total (see `mps::query_memory`).
        ram_mb: u64,
    },
    /// The host's own RAM statistics for the one CPU device (`cpu.rs`). With
    /// no accelerator there is no pool counter to intersect with, so `free`
    /// is `ram_available` alone.
    Cpu {
        key: String,
        /// Physical RAM in MiB — here both the bound on the reading and the
        /// device total, which on this backend are one fact.
        ram_mb: u64,
        meminfo: PathBuf,
    },
    /// No refresh at all: [`Self::run`] answers `None` and the ledger keeps
    /// what it had. This rules out a **partial** refresh, which would price
    /// the dropped GPUs off stale readings the ledger believes are fresh. A
    /// ROCm, MPS or CPU host with no GPU list lands here too, so it cannot
    /// fall through to [`Self::NvidiaSmi`].
    Unavailable,
}

impl MemoryQuery {
    /// Live free/total memory for every GPU the ledger knows, used when the
    /// freshest worker-reported sample has aged past its threshold. `None` on
    /// any failure, and the ledger then keeps the stale reading.
    ///
    /// **Blocking**: both probe paths run it under `spawn_blocking`.
    pub fn run(&self) -> Option<Vec<GpuMemory>> {
        match self {
            Self::NvidiaSmi => query_memory_nvidia_smi(),
            Self::RocmSysfs {
                pci_devices,
                meminfo,
                gpus,
            } => rocm::query_memory(pci_devices, meminfo, gpus),
            Self::Mps { key, ram_mb } => mps::query_memory(key, *ram_mb),
            Self::Cpu {
                key,
                ram_mb,
                meminfo,
            } => cpu::query_memory(
                key,
                *ram_mb,
                &cpu::MemRoots {
                    meminfo: meminfo.clone(),
                },
            ),
            Self::Unavailable => None,
        }
    }

    /// The provenance label the ledger records readings under; every value
    /// here is authoritative (device-wide, not process-local), unlike a
    /// worker-reported `"torch"`. `"mps"` and `"ram"` are byte-identical to
    /// the worker's own labels for the same readings, which is what keeps the
    /// ledger's free-source consistency rule true across the two sides.
    pub fn free_source(&self) -> &'static str {
        match self {
            Self::NvidiaSmi => "nvidia-smi",
            Self::Mps { .. } => "mps",
            Self::Cpu { .. } => "ram",
            // Including `Unavailable`, which never records anything anyway.
            Self::RocmSysfs { .. } | Self::Unavailable => "amdgpu-sysfs",
        }
    }
}

/// One `nvidia-smi` call, so per-GPU readings can never be stitched from
/// different moments. `None` on any failure.
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
/// reading unknown: a partial picture would price some GPUs' external usage
/// as zero, which is phantom headroom.
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
/// Pure, so tests drive it without a GPU or a mutated environment. The two
/// degrade independently: a restriction we cannot map to GPUs blanks the
/// **inventory** alone, since blanking the capability view would un-gate
/// every capability-floored model; one that *resolves* narrows both.
fn build(stdout: Option<&str>, visible: Option<&str>) -> HostGpus {
    let Some(gpus) = stdout.and_then(parse_inventory) else {
        return HostGpus {
            caps: HostComputeCaps::unknown(),
            inventory: GpuInventory::default(),
        };
    };
    let all_caps = caps_of(&gpus);
    let Some(gpus) = restrict_to_visible(gpus, visible) else {
        return HostGpus {
            caps: HostComputeCaps::from_caps(all_caps),
            inventory: GpuInventory::default(),
        };
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
            backend: MemoryBackend::NvidiaSmi,
        },
    }
}

/// The capabilities of the GPUs that reported one; an all-capless set yields
/// an empty vec, which `HostComputeCaps::from_caps` turns into "unknown".
/// Kept free of `HostComputeCaps` construction so `build` pays (and logs)
/// exactly one capability view per call.
fn caps_of(gpus: &[GpuInfo]) -> Vec<(u32, u32)> {
    gpus.iter()
        .filter_map(|gpu| parse_compute_cap(gpu.compute_cap.as_deref()?))
        .collect()
}

/// Apply the operator's ambient `CUDA_VISIBLE_DEVICES` to the GPU list
/// nvidia-smi reported (it ignores the variable entirely). Unset or empty is
/// no restriction; all-UUID entries keep exactly those GPUs in nvidia-smi
/// order; **any** index entry answers `None` — ambient indices are in CUDA
/// order and cannot be mapped to rows, so workers inherit the restriction.
fn restrict_to_visible(gpus: Vec<GpuInfo>, visible: Option<&str>) -> Option<Vec<GpuInfo>> {
    let entries: Vec<&str> = visible
        .unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .collect();
    if entries.is_empty() {
        return Some(gpus);
    }
    if !entries.iter().all(|entry| is_uuid_pin(entry)) {
        tracing::info!(
            visible_devices = %visible.unwrap_or(""),
            "CUDA_VISIBLE_DEVICES names devices by index; leaving the GPU \
             inventory unknown (indices are in CUDA order and cannot be \
             mapped to GPUs) — workers inherit the restriction as-is"
        );
        return None;
    }
    let restricted: Vec<GpuInfo> = gpus
        .into_iter()
        .filter(|gpu| {
            entries.iter().any(|entry| {
                let entry = entry.to_ascii_uppercase();
                gpu.uuid.to_ascii_uppercase().starts_with(&entry)
            })
        })
        .collect();
    if restricted.is_empty() {
        tracing::warn!(
            visible_devices = %visible.unwrap_or(""),
            "CUDA_VISIBLE_DEVICES names no GPU nvidia-smi reports; leaving \
             the GPU inventory unknown"
        );
        return None;
    }
    tracing::info!(
        visible_devices = %visible.unwrap_or(""),
        gpus = restricted.len(),
        "restricting the GPU inventory to the ambient CUDA_VISIBLE_DEVICES"
    );
    Some(restricted)
}

impl GpuInventory {
    /// Explicitly-unknown inventory (tests only; production probes).
    #[cfg(test)]
    pub fn unknown() -> Self {
        Self::default()
    }

    /// Construct a known CUDA inventory (tests only; the probe path builds
    /// it directly).
    #[cfg(test)]
    pub fn known(gpus: Vec<GpuInfo>) -> Self {
        Self {
            gpus: (!gpus.is_empty()).then(|| gpus.into()),
            backend: MemoryBackend::NvidiaSmi,
        }
    }

    /// [`Self::known`]'s CPU twin: the one synthetic RAM device, no pins, the
    /// `"ram"` backend. Takes RAM in MiB, so a test cannot build a device
    /// shape production never produces.
    #[cfg(test)]
    pub fn known_cpu(ram_mb: u64) -> Self {
        Self {
            gpus: Some(vec![cpu::gpu(ram_mb)].into()),
            backend: MemoryBackend::Cpu {
                meminfo: cpu::MemRoots::default().meminfo,
            },
        }
    }

    /// [`Self::known`]'s ROCm twin: index pins, amdgpu backend, no ambient
    /// restriction. The PCI root is the production one because callers read
    /// no file; refresh tests build theirs around a fixture tree.
    #[cfg(test)]
    pub fn known_rocm(gpus: Vec<GpuInfo>) -> Self {
        Self {
            gpus: (!gpus.is_empty()).then(|| gpus.into()),
            backend: MemoryBackend::RocmSysfs {
                pci_devices: rocm::SysfsRoots::default().pci_devices,
                meminfo: rocm::SysfsRoots::default().meminfo,
                ambient_hip_restriction: false,
            },
        }
    }

    /// The GPUs, or `None` when the host is unknown.
    pub fn gpus(&self) -> Option<&[GpuInfo]> {
        self.gpus.as_deref()
    }

    /// The one unified-memory device's key and RAM figure, for the two
    /// backends whose whole inventory is that device. The RAM comes off the
    /// GPU itself — the same fact that flags it unified — so the refresh and
    /// the flag can never disagree about which memory this GPU is made of.
    /// `None` where there is no GPU at all (off macOS, or a reader that said
    /// nothing) or no RAM figure: nothing to refresh either way.
    fn first_unified_ram_mb(&self) -> Option<(String, u64)> {
        let gpu = self.gpus().and_then(<[GpuInfo]>::first)?;
        Some((gpu.uuid.clone(), gpu.unified_ram_mb?))
    }

    /// The live-memory interface for these GPUs, resolved once so the
    /// ledger's refresh can never ask nvidia-smi about an AMD GPU. The ROCm
    /// arm is **total or nothing**: every row must carry the PCI address the
    /// counters are keyed by, or the refresh is withdrawn entirely.
    pub(super) fn memory_query(&self) -> MemoryQuery {
        if let MemoryBackend::Cpu { meminfo } = &self.backend {
            return match self.first_unified_ram_mb() {
                Some((key, ram_mb)) => MemoryQuery::Cpu {
                    key,
                    ram_mb,
                    meminfo: meminfo.clone(),
                },
                None => MemoryQuery::Unavailable,
            };
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
        let Some(gpus) = self.gpus() else {
            // A ROCm host with no inventory: nothing to refresh, and not
            // nvidia-smi's business (see `MemoryQuery::Unavailable`).
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

    /// Default placement: the **highest-compute-capability** GPU, ties broken
    /// by the largest [`GpuInfo::placement_total_mb`] and then the lowest
    /// enumeration index. A GPU with no reported capability ranks *last*, not
    /// lowest — unknown is not slow — and the capacity tie-break is what keeps
    /// a first-enumerated iGPU from out-ranking the dGPU on ROCm, where every
    /// `compute_cap` is `None`. The pin itself is the GPU's row index on ROCm
    /// and absent on MPS/CPU (docs/rocm-batch-calibration-parity.md, D2).
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

    /// Whether this host's pin vocabulary is HIP's (device indices) rather
    /// than CUDA's (GPU UUIDs) — the single source of that answer, for
    /// [`Self::default_pin`], [`Self::resolve_pin`] and (through the same
    /// accelerator) [`pin_env_var`]. A host whose inventory is unknown still
    /// answers truthfully.
    fn pins_are_indices(&self) -> bool {
        matches!(self.backend, MemoryBackend::RocmSysfs { .. })
    }

    /// Whether this host has no pin vocabulary at all — MPS, with one device
    /// and nothing to name it with, and CPU, with no device. Every pin
    /// request is dropped; device keys keep resolving as everywhere else.
    fn pins_are_absent(&self) -> bool {
        matches!(self.backend, MemoryBackend::Mps | MemoryBackend::Cpu { .. })
    }

    /// Whether this host's devices are priced against system RAM because it
    /// has no accelerator at all. True on every path out of [`probe_cpu`].
    /// See docs/unified-memory-admission.md "Backend C: CPU".
    pub(super) fn prices_host_ram(&self) -> bool {
        matches!(self.backend, MemoryBackend::Cpu { .. })
    }

    /// Whether a worker's own total-memory report may **replace** this host's
    /// device total: MPS and nothing else, because Metal's
    /// `recommendedMaxWorkingSetSize` is the one total nothing but the worker
    /// can read. Every other backend reads its total from the kernel or the
    /// driver (docs/unified-memory-admission.md, DP-4).
    pub(super) fn adopts_worker_total(&self) -> bool {
        matches!(self.backend, MemoryBackend::Mps)
    }

    /// Whether the operator had a HIP-layer visibility restriction in force
    /// when this inventory was probed; always false on CUDA, where the
    /// ambient value is *composed with* rather than fought over.
    fn ambient_hip_restriction(&self) -> bool {
        matches!(
            self.backend,
            MemoryBackend::RocmSysfs {
                ambient_hip_restriction: true,
                ..
            }
        )
    }

    /// The default GPU's **model name** — the calibration keyspace, which is
    /// per silicon rather than per instance. `None` on an unknown host, whose
    /// `/metadata` calibration overlay is omitted entirely.
    pub fn default_gpu_name(&self) -> Option<String> {
        self.default_gpu().map(|gpu| gpu.name.clone())
    }

    fn default_gpu(&self) -> Option<&GpuInfo> {
        self.gpus.as_deref()?.iter().min_by_key(|gpu| {
            (
                std::cmp::Reverse(gpu.cap_tenths()),
                std::cmp::Reverse(gpu.placement_total_mb()),
                gpu.index,
            )
        })
    }

    /// Resolve one replica's registry pin into the value it is spawned with,
    /// in the vocabulary of the variable it will be written to
    /// ([`pin_env_var`]).
    ///
    /// **CUDA** resolves into `CUDA_VISIBLE_DEVICES`, in GPU UUIDs: no
    /// request → the default GPU; a `GPU-…`/`MIG-…` or index request naming a
    /// visible GPU → **the inventory's own spelling** of that UUID, so the
    /// byte-wise pin comparison in `prewarm.rs` keeps matching; anything else
    /// → verbatim, which preserves what the operator meant. **MPS and CPU**
    /// have no pin in any vocabulary, so every request resolves to `None`
    /// (the device *key* still resolves). **ROCm** takes indices only and
    /// drops anything it cannot render as one, rather than hiding every
    /// device from the worker; an ambient HIP-layer restriction drops
    /// everything, checked first because it is a fact about the gateway's own
    /// environment. See docs/rocm-batch-calibration-parity.md "D2 (G2) —
    /// Pinning" for the arm-by-arm table.
    pub fn resolve_pin(&self, requested: Option<&str>) -> Option<String> {
        // Before anything else, because it is a fact about the host rather
        // than about which GPUs were found.
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
        // Then, unconditionally: the operator's own HIP-layer restriction
        // outranks every arm below, including the ones allowed to write an
        // index. Checked here rather than in the uninventoried arm alone so
        // it cannot be bypassed if a HIP-restricted host ever carries a
        // non-empty inventory.
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
        let Some(gpus) = self.gpus.as_deref() else {
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
            // Canonicalised against the inventory when it names a GPU we can
            // see: the rest of the system compares pin strings byte-wise, so
            // the pool and the ledger would otherwise disagree about whether
            // two replicas are on one GPU.
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
            // Ambiguous, or a GPU this host cannot see (a `MIG-…` instance, a
            // UUID from another machine): verbatim — resolving it is CUDA's
            // business, not ours.
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

    /// Resolve the same registry `devices` entry [`Self::resolve_pin`] takes
    /// into the **ledger device key** — the GPU's `uuid`, whatever vocabulary
    /// the pin is written in. The two are a pair, resolved together at every
    /// call site that needs both, because keying the ledger by the pin
    /// instead loses the load reservation wherever pin ≠ key.
    /// See docs/rocm-batch-calibration-parity.md "D3 (G3) — Worker identity".
    ///
    /// No request → the default GPU's key; a device key (case-insensitive,
    /// **in full**) or an index naming a row → that row's key; on CUDA only,
    /// an unambiguous `GPU-`/`MIG-` prefix, the abbreviation CUDA itself
    /// resolves. Everything else answers `None` — a reservation on the wrong
    /// GPU is worse than none — and silently, since `resolve_pin` has already
    /// warned about each of these strings.
    pub fn resolve_device_key(&self, requested: Option<&str>) -> Option<String> {
        let gpus = self.gpus.as_deref()?;
        let Some(requested) = requested else {
            return self.default_gpu().map(|gpu| gpu.uuid.clone());
        };
        let trimmed = requested.trim();
        if let Some(gpu) = gpus
            .iter()
            .find(|gpu| gpu.uuid.eq_ignore_ascii_case(trimmed))
        {
            return Some(gpu.uuid.clone());
        }
        if let Ok(index) = trimmed.parse::<u32>() {
            return gpus
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

    /// The **PCI address** of the GPU a registry `devices` entry names, when
    /// that GPU is a unified one whose worker needs the GTT-inclusive
    /// arithmetic ([`UNIFIED_GPU_ENV_VAR`]); `None` otherwise, including for
    /// anything unresolvable — the discrete arithmetic never over-counts.
    /// Resolved through the same resolver as the pin and the device key, so
    /// the three can never disagree about where a replica should land.
    pub fn unified_pin_bdf(&self, requested: Option<&str>) -> Option<String> {
        if !self.pins_are_indices() {
            return None;
        }
        let key = self.resolve_device_key(requested)?;
        self.gpus()?
            .iter()
            .find(|gpu| gpu.uuid == key && gpu.unified())
            .and_then(|gpu| gpu.bdf.clone())
    }

    /// The ROCm arm of [`Self::resolve_pin`] (see its docs for the
    /// vocabulary). Split out because HIP's rules diverge at every branch: a
    /// device key translates, and an unresolvable non-numeric string is
    /// dropped.
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

    /// The ROCm arm of [`Self::resolve_pin`] for a host with **no GPUs**:
    /// nothing to translate a key against and no index to range-check, so all
    /// that is left is HIP's grammar. The ambient restriction was already
    /// handled at the top of [`Self::resolve_pin`].
    fn resolve_hip_pin_uninventoried(&self, requested: Option<&str>) -> Option<String> {
        // No request is no pin here: with no GPUs there is no default either.
        let trimmed = requested?.trim();
        if let Some(pin) = canonical_hip_pin(trimmed) {
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

/// The HIP-legal pin forms, canonicalised: one device index, or a list of
/// them. `None` for anything HIP cannot read as an index, which on ROCm means
/// "write no pin at all".
fn canonical_hip_pin(value: &str) -> Option<String> {
    if let Ok(index) = value.parse::<u32>() {
        return Some(index.to_string());
    }
    canonical_index_list(value)
}

/// A HIP-shaped multi-device pin: at least one entry, every entry a device
/// index; trailing and empty entries are ignored, as HIP's own parser does.
/// Returns the **canonical** rendering — entries re-parsed and re-joined with
/// `,` — because `prewarm.rs` claims a parked worker only when the two pin
/// strings are byte-equal, so `" 0 "` and `"00"` would defeat pooling.
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

/// One GPU per line, `index, uuid, name, total, compute_cap`
/// (`--format=csv,noheader,nounits`). Any line whose **identity** columns do
/// not parse — or whose column count is not five — makes the whole probe
/// unknown; the capability column alone is per-row optional.
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

/// One row of the inventory query, or `None` if any identity column does not
/// parse. Every `None` funnels through `parse_inventory`'s single WARN.
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
        // nvidia-smi rows need neither: the UUID is both identity and pin
        // form, and there is no gfx target.
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
            backend: MemoryBackend::RocmSysfs {
                pci_devices,
                meminfo,
                // A knowable inventory is proof of no ambient restriction:
                // the probe blanks it otherwise.
                ambient_hip_restriction: false,
            },
        }
    }

    fn mps_inventory(ram_gib: u64) -> GpuInventory {
        let facts = super::mps::HostFacts {
            chip: "Apple M3 Max".into(),
            ram_bytes: ram_gib * 1024 * 1024 * 1024,
        };
        GpuInventory {
            gpus: Some(vec![super::mps::gpu(&facts)].into()),
            backend: MemoryBackend::Mps,
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
            backend: MemoryBackend::RocmSysfs {
                pci_devices: PathBuf::from("/sys/bus/pci/devices"),
                meminfo: PathBuf::from("/proc/meminfo"),
                ambient_hip_restriction,
            },
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
        // Unset, empty and separator-only all mean "no restriction".
        for visible in ["", " , "] {
            let host = build(Some(TWO_GPUS), Some(visible));
            assert_eq!(host.inventory.gpus().map(<[GpuInfo]>::len), Some(2));
        }

        // The unmappable forms: an index (CUDA order is not nvidia-smi
        // order), a mixed list, and a UUID naming nothing we listed — a
        // legitimate `MIG-…` pin never appears among these rows, so that is
        // "cannot map", not "no GPUs".
        for visible in ["1", "GPU-1a2b,1", "MIG-abcd"] {
            let host = build(Some(TWO_GPUS), Some(visible));
            assert!(host.inventory.gpus().is_none(), "{visible}");
            assert_eq!(host.inventory.resolve_pin(None), None, "{visible}: no pin");
            assert_eq!(
                host.caps.meets_floor(12.0),
                Some(true),
                "{visible}: model availability is still capability-filtered"
            );
            assert_eq!(host.caps.meets_floor(12.1), Some(false), "{visible}");
        }
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

    /// DP-5's resolver: the **address** of the GPU a registry entry names,
    /// when that GPU is unified — from the same request the pin and the key
    /// are, so the worker can check the claim against where it came up.
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
            assert!(rocm.gpus().is_none(), "no KFD topology off Linux");
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
            assert!(mps.inventory.gpus().is_none(), "no sysctl off macOS");
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

        // CPU: priced against system RAM on `Cpu` and no other resolved
        // accelerator — the negative half is load-bearing, and holds whatever
        // this host has installed. Every platform this ships to has a RAM
        // reader, so the device here is real rather than fixture-shaped.
        let cpu = probe(Accelerator::Cpu);
        assert!(cpu.inventory.prices_host_ram());
        assert_eq!(
            cpu.caps.meets_floor(8.0),
            None,
            "a CPU host filters no model by a GPU capability: its workers are \
             pinned to the CPU device, and the impls' own load-time guard is \
             the backstop"
        );
        assert_eq!(cpu.inventory.resolve_pin(Some("0")), None);
        #[cfg(any(target_os = "linux", target_os = "windows", target_os = "macos"))]
        {
            let gpu = cpu.inventory.gpus().expect("a host with RAM")[0].clone();
            assert_eq!(gpu.uuid, "CPU");
            assert!(gpu.unified() && gpu.total_mb > 0);
            assert!(gpu.name.starts_with("CPU ("), "name: {}", gpu.name);
            assert_eq!(cpu.inventory.memory_query().free_source(), "ram");
        }
        for accelerator in [
            Accelerator::Cuda,
            Accelerator::Rocm,
            Accelerator::Mps,
            Accelerator::Auto,
        ] {
            let host = probe(accelerator).inventory;
            assert!(
                !host.prices_host_ram(),
                "{accelerator:?} must never be priced against system RAM — an \
                 accelerator host whose probe came back unknown stays unknown"
            );
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
                let pin = host.resolve_pin(requested);
                assert_eq!(pin, None, "{requested:?} must reach no variable");
            }
            // The ledger vocabulary is unaffected.
            assert_eq!(host.resolve_device_key(None).as_deref(), Some(key));
            let lower = key.to_ascii_lowercase();
            assert_eq!(host.resolve_device_key(Some(&lower)).as_deref(), Some(key));
            assert_eq!(host.resolve_device_key(Some("GPU-1a2b")), None);
            // The refresh: RAM statistics, bounded by physical RAM.
            let query = host.memory_query();
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
        assert!(GpuInventory::known_cpu(64 * 1024).prices_host_ram());
        assert!(
            !GpuInventory::known_cpu(64 * 1024).adopts_worker_total(),
            "a CPU device's total is physical RAM, known at probe time: there \
             is nothing for a worker to adopt it from (DP-4 is MPS-only)"
        );

        // No device at all (off-platform, or a reader that said nothing): the
        // backend is still set, so nothing falls back to nvidia-smi.
        let unprobed_cpu = GpuInventory {
            gpus: None,
            backend: MemoryBackend::Cpu {
                meminfo: super::cpu::MemRoots::default().meminfo,
            },
        };
        assert!(
            unprobed_cpu.prices_host_ram(),
            "still a CPU host: the backend is set on every path out of the probe"
        );
        for unprobed in [
            GpuInventory {
                gpus: None,
                backend: MemoryBackend::Mps,
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
            "${DEVICE}", "cpu", "0,GPU-BDF-0000:03:00.0", "", "   ", ",",
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
        for requested in ["GPU-1a2b", "${DEVICE}", "cpu", "", "4294967296"] {
            assert_eq!(blank.resolve_pin(Some(requested)), None, "{requested:?}");
        }
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
        for requested in ["1", "GPU-1a2b", "${DEVICE}", "cpu", " 0 ", ""] {
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
            backend: MemoryBackend::RocmSysfs {
                pci_devices: PathBuf::from("/sys/bus/pci/devices"),
                meminfo: PathBuf::from("/proc/meminfo"),
                ambient_hip_restriction: true,
            },
        };
        for host in [uninventoried_rocm(true), with_gpus] {
            for requested in [
                None,
                Some("0"),
                Some("0,1"),
                Some("GPU-BDF-0000:03:00.0"),
                Some("GPU-1a2b"),
                Some("cpu"),
                Some(""),
            ] {
                let pin = host.resolve_pin(requested);
                assert_eq!(pin, None, "{requested:?} over their restriction");
            }
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
            ("cpu", "cpu", "a non-numeric string"),
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
        // An unreported index, a list, a bare string and an unseen UUID.
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
