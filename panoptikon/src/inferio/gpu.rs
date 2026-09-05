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
    let gpus = match inventory {
        Some(Ok(gpus)) => gpus,
        // Unpriced is safe but indistinguishable from "the feature is not
        // working", so the failure is named: `ProbeFailure::log` emits one
        // WARN unless the deciding site already logged the detail.
        Some(Err(failure)) => {
            failure.log();
            return HostGpus {
                caps: HostComputeCaps::unknown(),
                inventory: GpuInventory {
                    gpus: None,
                    backend,
                },
            };
        }
        None => {
            return HostGpus {
                caps: HostComputeCaps::unknown(),
                inventory: GpuInventory {
                    gpus: None,
                    backend,
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
            bdf = gpu.bdf.as_deref().unwrap_or("unknown"),
            unified = gpu.unified(),
            vram_carveout_mb = ?gpu.vram_carveout_mb,
            "detected GPU"
        );
    }
    HostGpus {
        caps: HostComputeCaps::unknown(),
        inventory: GpuInventory {
            gpus: Some(gpus.into()),
            backend,
        },
    }
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

    /// The live-memory interface for these GPUs, resolved once so the
    /// ledger's refresh can never ask nvidia-smi about an AMD GPU. The ROCm
    /// arm is **total or nothing**: every row must carry the PCI address the
    /// counters are keyed by, or the refresh is withdrawn entirely.
    pub(super) fn memory_query(&self) -> MemoryQuery {
        if let MemoryBackend::Cpu { meminfo } = &self.backend {
            // As on MPS, the RAM figure comes off the GPU itself — the same
            // fact that flags it unified — so the refresh and the flag can
            // never disagree about which memory this GPU is made of.
            return match self.gpus().and_then(<[GpuInfo]>::first) {
                Some(gpu) => match gpu.unified_ram_mb {
                    Some(ram_mb) => MemoryQuery::Cpu {
                        key: gpu.uuid.clone(),
                        ram_mb,
                        meminfo: meminfo.clone(),
                    },
                    None => MemoryQuery::Unavailable,
                },
                None => MemoryQuery::Unavailable,
            };
        }
        if matches!(self.backend, MemoryBackend::Mps) {
            return match self.gpus().and_then(<[GpuInfo]>::first) {
                Some(gpu) => match gpu.unified_ram_mb {
                    Some(ram_mb) => MemoryQuery::Mps {
                        key: gpu.uuid.clone(),
                        ram_mb,
                    },
                    None => MemoryQuery::Unavailable,
                },
                // No GPU (off macOS, or sysctl said nothing): nothing to
                // refresh, and not nvidia-smi's business.
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

    /// A ROCm-shaped GPU: no compute capability, a PCI address, a gfx
    /// target, and the deterministic `AMD gfx…` name `rocm.rs` derives.
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

    /// A known ROCm inventory reading from `pci_devices` — a fixture tree in
    /// tests, `/sys/bus/pci/devices` in production.
    fn rocm_inventory(pci_devices: PathBuf, gpus: Vec<GpuInfo>) -> GpuInventory {
        rocm_inventory_with(pci_devices, rocm::SysfsRoots::default().meminfo, gpus)
    }

    /// The same, with `/proc/meminfo` injected too — the unified refresh is
    /// the only reader of it, and the only test that needs a fixture there.
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
                // A knowable inventory is proof there was no ambient
                // restriction of any layer: the probe blanks it otherwise.
                ambient_hip_restriction: false,
            },
        }
    }

    /// The MPS host: one synthetic unified-memory device, built by the same
    /// `mps::gpu` the probe uses so the fixture cannot drift from it.
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

    /// A ROCm-shaped **APU** row, as `rocm.rs` builds one: unified, budgeted
    /// against carve-out + GTT, named by the machine's RAM, and carrying the
    /// carve-out separately.
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

    /// A ROCm host with **no** GPUs — the ambient-restricted, probe-failed
    /// and non-Linux shape. It is still a ROCm host: the backend, and with it
    /// the pin vocabulary and the memory interface, is what `probe_rocm`
    /// leaves behind on every one of those paths.
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

    /// The ledger's staleness refresh reads one coherent snapshot; a single
    /// unparseable row makes the whole reading unknown rather than pricing
    /// some GPU's external usage as zero.
    #[test]
    fn parses_a_memory_snapshot() {
        let gpus = parse_memory("GPU-1a2b, 32607, 21000\nGPU-3c4d, 6138, 512\n").expect("parses");
        assert_eq!(
            gpus,
            vec![
                GpuMemory {
                    uuid: "GPU-1a2b".into(),
                    total_mb: 32607,
                    free_mb: 21000,
                },
                GpuMemory {
                    uuid: "GPU-3c4d".into(),
                    total_mb: 6138,
                    free_mb: 512,
                },
            ]
        );
        assert!(parse_memory("").is_none());
        assert!(parse_memory("N/A, N/A, N/A\n").is_none());
        assert!(
            parse_memory("GPU-1a2b, 32607, 21000\nGPU-3c4d, [N/A], 512\n").is_none(),
            "one bad row makes the whole snapshot unknown"
        );
        assert!(
            parse_memory("GPU-1a2b, 32607\n").is_none(),
            "missing column"
        );
        assert!(
            parse_memory("0, 32607, 21000\n").is_none(),
            "a non-UUID identity cannot key a ledger"
        );
    }

    #[test]
    fn parses_nvidia_smi_inventory() {
        let gpus = parse_inventory(TWO_GPUS).expect("parses");
        assert_eq!(gpus.len(), 2);
        assert_eq!(gpus[0].uuid, "GPU-1a2b");
        assert_eq!(gpus[0].name, "NVIDIA GeForce RTX 5090");
        assert_eq!(gpus[0].total_mb, 32607);
        assert_eq!(gpus[0].compute_cap.as_deref(), Some("12.0"));
        assert_eq!(gpus[1].index, 1);
    }

    /// The merged probe feeds both views from the same rows, so an index in
    /// the inventory and a capability in the filter always describe the same
    /// physical GPU.
    #[test]
    fn one_probe_builds_both_views() {
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
        // 12.0 satisfies an sm_80 floor, 8.6 does too; a 9.0 floor is met by
        // exactly one GPU, which is what "ANY device" means.
        assert_eq!(host.caps.meets_floor(8.0), Some(true));
        assert_eq!(host.caps.meets_floor(9.0), Some(true));
        assert_eq!(host.caps.meets_floor(12.1), Some(false));
    }

    #[test]
    fn garbage_in_the_identity_columns_makes_both_views_unknown() {
        assert!(parse_inventory("").is_none());
        assert!(parse_inventory("N/A\n").is_none());
        assert!(
            parse_inventory("Failed to initialize NVML: Driver error\n").is_none(),
            "driver error text must not become a GPU"
        );
        // One good line plus one bad line: the whole probe is unknown.
        assert!(
            parse_inventory("0, GPU-1a2b, RTX, 32607, 8.6\nN/A, N/A, N/A, N/A, N/A\n").is_none()
        );
        // A non-UUID identity column is not something we can key a ledger
        // by, so it is unknown rather than half-trusted.
        assert!(parse_inventory("0, 0, RTX, 32607, 8.6\n").is_none());
        // Missing/extra columns mean the query changed shape under us.
        assert!(parse_inventory("0, GPU-1a2b, RTX, 32607\n").is_none());
        assert!(parse_inventory("0, GPU-1a2b, RTX, 32607, 8.6, extra\n").is_none());

        let host = build(Some("N/A\n"), None);
        assert!(host.inventory.gpus().is_none());
        assert_eq!(host.caps.meets_floor(8.0), None, "capabilities go with it");
        let missing_smi = build(None, None);
        assert!(missing_smi.inventory.gpus().is_none());
        assert_eq!(missing_smi.caps.meets_floor(8.0), None);
    }

    /// The capability column is the one separably-useless field: vGPU slices
    /// and some datacenter SKUs print `[N/A]` there with every identity
    /// column intact. Dropping the row would cost the host pinning and the
    /// per-GPU ledger over a field only placement and gating read.
    #[test]
    fn an_unreported_capability_keeps_the_gpu_identity() {
        let mixed = "0, GPU-1a2b, NVIDIA A100-SXM4-40GB MIG 1g.5gb, 4864, [N/A]\n\
                     1, GPU-3c4d, NVIDIA RTX A2000, 6138, 8.6\n";
        let host = build(Some(mixed), None);
        let gpus = host.inventory.gpus().expect("identities are all good");
        assert_eq!(gpus.len(), 2);
        assert_eq!(gpus[0].compute_cap, None);
        assert_eq!(gpus[0].uuid, "GPU-1a2b", "still a pinnable ledger identity");
        assert_eq!(
            host.inventory.resolve_pin(Some("0")),
            Some("GPU-1a2b".to_string()),
            "index pins still resolve through it"
        );
        // Capabilities come from the GPUs that reported one.
        assert_eq!(host.caps.meets_floor(8.0), Some(true));
        assert_eq!(host.caps.meets_floor(9.0), Some(false));
        // Unknown is not slow: the capless GPU is unranked, not preferred
        // and not treated as compute capability 0.
        assert_eq!(
            host.inventory.default_pin(),
            Some("GPU-3c4d".to_string()),
            "a GPU of unknown speed must not win default placement"
        );

        // No GPU reports one: identities stay, the capability view is
        // unknown (and so filters nothing), and placement still has to pick
        // something — the lowest index, as any tie does.
        let capless = build(
            Some(
                "1, GPU-3c4d, NVIDIA RTX A2000, 6138, [N/A]\n\
                 0, GPU-1a2b, NVIDIA RTX A2000, 6138, N/A\n",
            ),
            None,
        );
        assert_eq!(capless.inventory.gpus().map(<[GpuInfo]>::len), Some(2));
        assert_eq!(capless.caps.meets_floor(8.0), None);
        assert_eq!(
            capless.inventory.default_pin(),
            Some("GPU-1a2b".to_string())
        );
    }

    /// nvidia-smi ignores `CUDA_VISIBLE_DEVICES`, so an operator's ambient
    /// restriction has to be applied here — otherwise pin resolution would
    /// hand a worker a GPU the operator deliberately hid, and the worker
    /// (which *does* honour the variable) would fail or land elsewhere.
    #[test]
    fn ambient_visible_devices_restricts_the_inventory() {
        // UUID form: keep exactly the named GPUs, in nvidia-smi order.
        let host = build(Some(TWO_GPUS), Some("GPU-3c4d"));
        let gpus = host.inventory.gpus().expect("known");
        assert_eq!(gpus.len(), 1);
        assert_eq!(gpus[0].uuid, "GPU-3c4d");
        assert_eq!(
            host.inventory.default_pin(),
            Some("GPU-3c4d".to_string()),
            "placement can only choose among visible GPUs"
        );
        assert_eq!(
            host.caps.meets_floor(12.0),
            Some(false),
            "the hidden GPU's capability must not filter models either"
        );

        // Abbreviated UUIDs are legal for CUDA, so they are honoured here.
        let abbreviated = build(Some(TWO_GPUS), Some("GPU-1a"));
        assert_eq!(
            abbreviated.inventory.default_pin(),
            Some("GPU-1a2b".to_string())
        );

        // Unset and empty both mean "no restriction".
        assert_eq!(
            build(Some(TWO_GPUS), Some(""))
                .inventory
                .gpus()
                .map(<[GpuInfo]>::len),
            Some(2)
        );
        assert_eq!(
            build(Some(TWO_GPUS), Some(" , "))
                .inventory
                .gpus()
                .map(<[GpuInfo]>::len),
            Some(2)
        );
    }

    /// An unmappable ambient restriction blanks the **inventory only**. The
    /// capability view keeps every GPU nvidia-smi reported, which is what
    /// Package 1's availability gate saw before this module existed (it never
    /// looked at `CUDA_VISIBLE_DEVICES`); taking it down with the inventory
    /// would silently un-gate every capability-floored model on any host that
    /// merely restricts by index.
    #[test]
    fn an_unmappable_restriction_blanks_only_the_inventory() {
        // Index form: unmappable (CUDA order != nvidia-smi order), so the
        // inventory goes unknown and workers inherit the restriction as-is.
        let indexed = build(Some(TWO_GPUS), Some("1"));
        assert!(indexed.inventory.gpus().is_none());
        assert_eq!(indexed.inventory.resolve_pin(None), None, "no pinning");
        assert_eq!(
            indexed.caps.meets_floor(12.0),
            Some(true),
            "model availability is still capability-filtered"
        );
        assert_eq!(indexed.caps.meets_floor(12.1), Some(false));
        // Mixed forms are index-form as far as safety goes.
        let mixed = build(Some(TWO_GPUS), Some("GPU-1a2b,1"));
        assert!(mixed.inventory.gpus().is_none());
        assert_eq!(mixed.caps.meets_floor(12.0), Some(true));
        // A UUID restriction naming nothing we listed is unknown, not empty:
        // a legitimate `MIG-…` pin never appears among these rows, so this is
        // "cannot map", not "no GPUs", and the physical capabilities stand.
        let nothing = build(Some(TWO_GPUS), Some("MIG-abcd"));
        assert!(nothing.inventory.gpus().is_none());
        assert_eq!(nothing.caps.meets_floor(12.0), Some(true));
    }

    /// Default placement is the fastest GPU (parity with CUDA's
    /// FASTEST_FIRST ordering, which is what unpinned workers saw), not the
    /// lowest index.
    #[test]
    fn default_pin_is_the_fastest_gpu() {
        let mixed =
            GpuInventory::known(vec![gpu(0, "GPU-slow", "8.6"), gpu(1, "GPU-fast", "12.0")]);
        assert_eq!(mixed.default_pin(), Some("GPU-fast".to_string()));
        // 10.x is above 9.x, not lexicographically below it.
        let blackwell = GpuInventory::known(vec![gpu(0, "GPU-9", "9.0"), gpu(1, "GPU-12", "12.0")]);
        assert_eq!(blackwell.default_pin(), Some("GPU-12".to_string()));
        // Ties (the common homogeneous host) go to the lowest index.
        assert_eq!(inventory().default_pin(), Some("GPU-1111".to_string()));
    }

    /// Equal capability (or, on ROCm, none at all) is broken by VRAM before
    /// index: a first-enumerated iGPU must not out-rank the dGPU behind it.
    #[test]
    fn equal_capability_ties_break_on_vram() {
        let mixed = GpuInventory::known(vec![
            sized_gpu(0, "GPU-small", "12.0", 8192),
            sized_gpu(1, "GPU-big", "12.0", 32607),
        ]);
        assert_eq!(mixed.default_pin(), Some("GPU-big".to_string()));
        // Capability still outranks VRAM: a big slow GPU does not win.
        let slow_and_big = GpuInventory::known(vec![
            sized_gpu(0, "GPU-slow-big", "8.6", 49152),
            sized_gpu(1, "GPU-fast-small", "12.0", 8192),
        ]);
        assert_eq!(
            slow_and_big.default_pin(),
            Some("GPU-fast-small".to_string())
        );
        // The all-capless (ROCm-shaped) host, where this is load-bearing.
        let rocm_shaped = GpuInventory::known(vec![
            sized_gpu(0, "GPU-igpu", "", 2048),
            sized_gpu(1, "GPU-dgpu", "", 24576),
        ]);
        assert_eq!(rocm_shaped.default_pin(), Some("GPU-dgpu".to_string()));
        // Equal on both: the lowest index, as before.
        let identical = GpuInventory::known(vec![
            sized_gpu(3, "GPU-3333", "12.0", 8192),
            sized_gpu(0, "GPU-0000", "12.0", 8192),
        ]);
        assert_eq!(identical.default_pin(), Some("GPU-0000".to_string()));
    }

    /// The refresh interface follows the inventory, so a ROCm host can
    /// never end up asking nvidia-smi about an AMD GPU.
    #[test]
    fn the_memory_query_follows_the_inventory_backend() {
        assert_eq!(inventory().memory_query().free_source(), "nvidia-smi");
        assert_eq!(
            GpuInventory::unknown().memory_query().free_source(),
            "nvidia-smi",
            "an unknown host has no GPUs to refresh either way"
        );
        let rocm_host = rocm_inventory(
            PathBuf::from("/sys/bus/pci/devices"),
            vec![amd_gpu(0, "0000:03:00.0", 24576)],
        );
        let query = rocm_host.memory_query();
        assert_eq!(
            query.free_source(),
            "amdgpu-sysfs",
            "the driver, not the filesystem: a future generic \"sysfs\" \
             reporter must not inherit authority by string collision"
        );
        match query {
            MemoryQuery::RocmSysfs { gpus, .. } => assert_eq!(
                &*gpus,
                &[rocm::GpuRef {
                    key: "GPU-BDF-0000:03:00.0".to_owned(),
                    bdf: "0000:03:00.0".to_owned(),
                    unified: false,
                }]
            ),
            other => panic!("expected the sysfs query, got {other:?}"),
        }
        // And a ROCm host with no GPUs at all (ambient restriction, probe
        // failure, non-Linux) must not fall back to CUDA's interface: there
        // is nothing to refresh, but nvidia-smi is not the thing that would
        // have refreshed it.
        for ambient_hip_restriction in [false, true] {
            let query = uninventoried_rocm(ambient_hip_restriction).memory_query();
            assert!(
                matches!(query, MemoryQuery::Unavailable),
                "expected no refresh at all, got {query:?}"
            );
            assert_eq!(query.free_source(), "amdgpu-sysfs");
            assert!(query.run().is_none());
        }
    }

    /// The refresh carries each GPU's unified flag, because the extra
    /// files (GTT, `MemAvailable`) are read for those rows and only those:
    /// `mem_info_gtt_*` exists for discrete GPUs too, so its presence
    /// could never be the test.
    #[test]
    fn the_rocm_memory_query_carries_the_unified_flag() {
        let host = rocm_inventory_with(
            PathBuf::from("/sys/bus/pci/devices"),
            PathBuf::from("/proc/meminfo"),
            vec![
                amd_apu(0, "0000:03:00.0", 512, 64 * 1024, 128 * 1024),
                amd_gpu(1, "0000:0c:00.0", 24_576),
            ],
        );
        match host.memory_query() {
            MemoryQuery::RocmSysfs { gpus, meminfo, .. } => {
                assert_eq!(
                    gpus.iter().map(|b| b.unified).collect::<Vec<_>>(),
                    vec![true, false]
                );
                assert_eq!(meminfo, PathBuf::from("/proc/meminfo"));
            }
            other => panic!("expected the sysfs query, got {other:?}"),
        }
        // The label is unchanged: both kinds of row are amdgpu's own
        // counters, and the worker reports the same string for the same
        // reading (GTT-inclusive on its side too, under the DP-5 flag).
        assert_eq!(host.memory_query().free_source(), "amdgpu-sysfs");
    }

    /// Default placement on a dGPU+APU host. The APU's *budget* dwarfs the
    /// card's VRAM — that is what makes it worth pricing — but the two are
    /// unlike quantities, and ranking by it would put every unpinned model
    /// on the slower GPU. The comparison is by carve-out, so the dGPU wins
    /// unless the operator gave the iGPU that memory outright in the BIOS.
    #[test]
    fn default_placement_prefers_a_dgpu_over_an_apu_of_larger_budget() {
        let dgpu_wins = GpuInventory::known_rocm(vec![
            amd_apu(0, "0000:03:00.0", 512, 64 * 1024, 128 * 1024),
            amd_gpu(1, "0000:0c:00.0", 24_576),
        ]);
        assert_eq!(
            dgpu_wins.default_gpu_name().as_deref(),
            Some("AMD gfx1100 (24 GB)"),
            "a 64.5 GB nominal APU budget must not out-rank 24 GB of VRAM"
        );
        assert_eq!(dgpu_wins.default_pin().as_deref(), Some("1"));
        // …unless the APU genuinely owns more memory than the card does,
        // which on an APU means someone set it that way.
        let apu_wins = GpuInventory::known_rocm(vec![
            amd_apu(0, "0000:03:00.0", 96 * 1024, 16 * 1024, 128 * 1024),
            amd_gpu(1, "0000:0c:00.0", 24_576),
        ]);
        assert_eq!(
            apu_wins.default_gpu_name().as_deref(),
            Some("AMD gfx1151 APU (128 GB)")
        );
        assert_eq!(apu_wins.default_pin().as_deref(), Some("0"));
        // …and the carve-out alone is not the whole rank either. A 2 GB
        // display card next to a 128 GB Strix Halo left at its BIOS default
        // is not a GPU anyone wants a model on: an eighth of the unified
        // budget (a deliberately pessimistic reading of memory shared with
        // the whole OS) is what the APU is credited with, which beats the
        // token card and still loses to any real one.
        let token_card = GpuInventory::known_rocm(vec![
            amd_apu(0, "0000:03:00.0", 512, 64 * 1024, 128 * 1024),
            amd_gpu(1, "0000:0c:00.0", 2048),
        ]);
        assert_eq!(
            token_card.default_gpu_name().as_deref(),
            Some("AMD gfx1151 APU (128 GB)")
        );
        assert_eq!(token_card.default_pin().as_deref(), Some("0"));
        // Nothing about the ranking changed for discrete GPUs.
        assert_eq!(
            GpuInventory::known(vec![
                sized_gpu(0, "GPU-small", "12.0", 8192),
                sized_gpu(1, "GPU-big", "12.0", 24_576),
            ])
            .default_pin()
            .as_deref(),
            Some("GPU-big")
        );
    }

    /// DP-5's resolver: the **address** of the GPU a registry entry names,
    /// when that GPU is unified — answered from the same request the pin
    /// and the device key are, and an address rather than a flag so the
    /// worker can check the claim against the GPU it actually came up on.
    #[test]
    fn a_unified_pin_resolves_to_its_gpus_address() {
        let host = GpuInventory::known_rocm(vec![
            amd_apu(0, "0000:03:00.0", 512, 64 * 1024, 128 * 1024),
            amd_gpu(1, "0000:0c:00.0", 24_576),
        ]);
        let apu = Some("0000:03:00.0".to_owned());
        assert_eq!(host.unified_pin_bdf(Some("0")), apu);
        assert_eq!(host.unified_pin_bdf(Some("GPU-BDF-0000:03:00.0")), apu);
        assert_eq!(host.unified_pin_bdf(Some("1")), None);
        assert_eq!(
            host.unified_pin_bdf(None),
            None,
            "an unpinned replica lands on the default GPU, which is the dGPU"
        );
        // A pin naming nothing this host enumerated: the discrete
        // arithmetic is the reading that never over-counts, so unknown
        // resolves to nothing rather than to a claim.
        assert_eq!(host.unified_pin_bdf(Some("7")), None);
        assert_eq!(host.unified_pin_bdf(Some("GPU-1a2b")), None);
        // An APU-only host: the default GPU *is* the unified one.
        let apu_only =
            GpuInventory::known_rocm(vec![amd_apu(0, "0000:03:00.0", 512, 64 * 1024, 128 * 1024)]);
        assert_eq!(apu_only.unified_pin_bdf(None), apu);
        // Never on the other backends: a CUDA GPU is not unified, and an
        // MPS worker's tiers are unified by construction and read no flag.
        assert_eq!(inventory().unified_pin_bdf(None), None);
        assert_eq!(mps_inventory(128).unified_pin_bdf(None), None);
        assert_eq!(uninventoried_rocm(false).unified_pin_bdf(Some("0")), None);
    }

    /// The refresh is total or withdrawn. A row without a PCI address
    /// cannot be located in sysfs; refreshing the others would leave the
    /// ledger pricing this one off a stale reading it believes is fresh.
    #[test]
    fn a_rocm_gpu_without_a_pci_address_withdraws_the_whole_refresh() {
        let mut broken = amd_gpu(1, "0000:0c:00.0", 24576);
        broken.bdf = None;
        let host = rocm_inventory(
            PathBuf::from("/sys/bus/pci/devices"),
            vec![amd_gpu(0, "0000:03:00.0", 24576), broken],
        );
        let query = host.memory_query();
        assert!(
            matches!(query, MemoryQuery::Unavailable),
            "expected no refresh at all, got {query:?}"
        );
        assert!(query.run().is_none());
        assert_eq!(
            query.free_source(),
            "amdgpu-sysfs",
            "still a ROCm host; it just never records anything"
        );
    }

    /// The whole D5 refresh, end to end, against a fixture PCI tree: the
    /// inventory carries the roots it was probed through, so the production
    /// path — not a re-implementation of it — is what runs here.
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
        assert_eq!(
            host.memory_query().run(),
            Some(vec![
                GpuMemory {
                    uuid: "GPU-BDF-0000:03:00.0".to_owned(),
                    total_mb: 24 * 1024,
                    free_mb: 20 * 1024,
                },
                GpuMemory {
                    uuid: "GPU-BDF-0000:0c:00.0".to_owned(),
                    total_mb: 16 * 1024,
                    free_mb: 16 * 1024,
                },
            ])
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

    /// The dispatch itself. ROCm off Linux is unconditionally unknown (the
    /// `rocm` torch extra is Linux-only and the sysfs roots do not exist),
    /// and the CUDA arm keeps the nvidia-smi path whatever this host happens
    /// to have installed — it can never produce a sysfs-backed inventory.
    ///
    /// `Accelerator::Cpu` used to be on this list, on the rule that "an
    /// explicit `cpu` host with an NVIDIA card must keep nvidia-smi's
    /// capability filtering". Backend C supersedes that: such a host is now
    /// priced against system RAM *and* has its workers pinned to the CPU
    /// device (`accelerator_env::worker_env`), so filtering models by a GPU
    /// its CPU-only torch cannot address was gating on a device nothing would
    /// have used — see `only_a_resolved_cpu_accelerator_gets_the_cpu_device`.
    #[test]
    fn the_probe_dispatches_on_the_resolved_accelerator() {
        #[cfg(not(target_os = "linux"))]
        {
            let rocm = probe(Accelerator::Rocm);
            assert!(rocm.inventory.gpus().is_none(), "no KFD topology off Linux");
            assert!(
                matches!(rocm.inventory.memory_query(), MemoryQuery::Unavailable),
                "an unknown ROCm host has nothing to refresh, but it is still \
                 a ROCm host — falling back to nvidia-smi would ask an \
                 NVIDIA binary about AMD GPUs"
            );
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
    }

    /// The MPS inventory: one constant-keyed unified-memory device, no pin in any
    /// vocabulary, and a device key that still resolves — the pin and the key
    /// are separate answers, and only the pin is missing here.
    #[test]
    fn an_mps_inventory_has_a_device_key_but_never_a_pin() {
        let host = mps_inventory(128);
        let gpus = host.gpus().expect("known");
        assert_eq!(gpus.len(), 1);
        assert_eq!(gpus[0].uuid, "GPU-MPS");
        assert!(gpus[0].unified());
        assert_eq!(
            host.default_gpu_name().as_deref(),
            Some("Apple M3 Max (128 GB)"),
            "the calibration keyspace"
        );
        // No pin, whatever the registry says — including the pin forms that
        // are legal on the other two backends.
        assert_eq!(host.default_pin(), None);
        for requested in [
            None,
            Some("GPU-MPS"),
            Some("0"),
            Some("mps"),
            Some("GPU-1a2b"),
            Some(""),
        ] {
            assert_eq!(
                host.resolve_pin(requested),
                None,
                "{requested:?} must not reach a visibility variable on a host \
                 whose only device is not selected by one"
            );
        }
        // The ledger vocabulary is unaffected: a load reservation is taken
        // against this GPU like any other.
        assert_eq!(
            host.resolve_device_key(None),
            Some("GPU-MPS".to_string()),
            "universal placement still names the GPU"
        );
        assert_eq!(
            host.resolve_device_key(Some("gpu-mps")),
            Some("GPU-MPS".to_string())
        );
        assert_eq!(host.resolve_device_key(Some("GPU-1a2b")), None);
    }

    /// The refresh follows the backend here too: an MPS host reads RAM
    /// statistics, never nvidia-smi, and an MPS host with no GPU (off
    /// macOS, or a sysctl that said nothing) has no refresh at all rather
    /// than CUDA's.
    #[test]
    fn the_mps_memory_query_follows_the_inventory_backend() {
        let query = mps_inventory(128).memory_query();
        assert_eq!(query.free_source(), "mps");
        match &query {
            MemoryQuery::Mps { key, ram_mb } => {
                assert_eq!(key, "GPU-MPS");
                assert_eq!(*ram_mb, 128 * 1024, "physical RAM, not the budget");
            }
            other => panic!("expected the MPS query, got {other:?}"),
        }
        // Only macOS can answer it; elsewhere the reading is simply unknown
        // and the ledger keeps whatever it had.
        #[cfg(not(target_os = "macos"))]
        assert!(query.run().is_none());

        let unprobed = GpuInventory {
            gpus: None,
            backend: MemoryBackend::Mps,
        };
        assert!(
            matches!(unprobed.memory_query(), MemoryQuery::Unavailable),
            "an MPS host with no GPU must not fall back to nvidia-smi"
        );
        assert_eq!(unprobed.resolve_pin(Some("0")), None);
        assert_eq!(unprobed.default_pin(), None);
        assert_eq!(unprobed.resolve_device_key(None), None);
    }

    /// The probe dispatches to the MPS path on `Accelerator::Mps` and to
    /// nowhere else — and off macOS that path is unknown-but-still-MPS, the
    /// same shape ROCm has off Linux.
    #[test]
    fn the_probe_dispatches_to_the_mps_backend() {
        let host = probe(Accelerator::Mps);
        assert!(matches!(host.inventory.backend, MemoryBackend::Mps));
        assert_eq!(
            host.caps.meets_floor(8.0),
            None,
            "Metal has no compute-capability analogue to filter with"
        );
        assert_eq!(host.inventory.resolve_pin(Some("0")), None);
        #[cfg(not(target_os = "macos"))]
        {
            assert!(host.inventory.gpus().is_none(), "no sysctl off macOS");
            assert!(matches!(
                host.inventory.memory_query(),
                MemoryQuery::Unavailable
            ));
        }
        #[cfg(target_os = "macos")]
        {
            let gpu = host.inventory.gpus().expect("Apple Silicon")[0].clone();
            assert_eq!(gpu.uuid, "GPU-MPS");
            assert!(gpu.unified() && gpu.total_mb > 0);
        }
        for accelerator in [Accelerator::Cuda, Accelerator::Cpu] {
            assert!(
                !matches!(probe(accelerator).inventory.backend, MemoryBackend::Mps),
                "{accelerator:?} has its own backend and must not borrow MPS's"
            );
        }
    }

    // ------------------------------------------------------------------
    // CPU-only hosts (docs/unified-memory-admission.md, backend C)
    // ------------------------------------------------------------------

    /// A CPU inventory: one constant-keyed device over the machine's RAM, no
    /// pin in any vocabulary, and a device key that still resolves — the same
    /// pin/key split MPS has, for the stronger reason that there is no device
    /// here at all.
    #[test]
    fn a_cpu_inventory_has_a_device_key_but_never_a_pin() {
        let host = GpuInventory::known_cpu(64 * 1024 - 700);
        let gpus = host.gpus().expect("known");
        assert_eq!(gpus.len(), 1);
        assert_eq!(gpus[0].uuid, "CPU");
        assert!(gpus[0].unified());
        assert!(host.prices_host_ram());
        assert!(
            !host.adopts_worker_total(),
            "a CPU device's total is physical RAM, read at probe time — there \
             is nothing for a worker to adopt it from (DP-4 is MPS-only)"
        );
        assert_eq!(
            host.default_gpu_name().as_deref(),
            Some("CPU (64 GB)"),
            "the calibration keyspace"
        );
        assert_eq!(host.default_pin(), None);
        for requested in [None, Some("CPU"), Some("0"), Some("cpu"), Some("")] {
            assert_eq!(
                host.resolve_pin(requested),
                None,
                "{requested:?} must not reach a visibility variable on a host \
                 with no device to hide"
            );
        }
        assert_eq!(
            host.resolve_device_key(None),
            Some("CPU".to_string()),
            "universal placement still names the GPU"
        );
        assert_eq!(
            host.resolve_device_key(Some("cpu")),
            Some("CPU".to_string())
        );
        assert_eq!(host.resolve_device_key(Some("GPU-1a2b")), None);
        assert_eq!(host.unified_pin_bdf(None), None, "no address to verify");
    }

    /// The refresh follows the backend here too: a CPU host reads the
    /// machine's RAM statistics under the `"ram"` label the worker's own
    /// psutil tier uses, never nvidia-smi — including when no GPU was built
    /// at all.
    #[test]
    fn the_cpu_memory_query_follows_the_inventory_backend() {
        let host = GpuInventory::known_cpu(64 * 1024);
        let query = host.memory_query();
        assert_eq!(query.free_source(), "ram");
        match &query {
            MemoryQuery::Cpu { key, ram_mb, .. } => {
                assert_eq!(key, "CPU");
                assert_eq!(*ram_mb, 64 * 1024);
            }
            other => panic!("expected the CPU query, got {other:?}"),
        }

        let unprobed = GpuInventory {
            gpus: None,
            backend: MemoryBackend::Cpu {
                meminfo: super::cpu::MemRoots::default().meminfo,
            },
        };
        assert!(
            matches!(unprobed.memory_query(), MemoryQuery::Unavailable),
            "a CPU host with no readable RAM must not fall back to nvidia-smi"
        );
        assert!(
            unprobed.prices_host_ram(),
            "it is still a CPU host: the backend is set on every path out of \
             the probe, exactly as ROCm's and MPS's are"
        );
        assert_eq!(unprobed.resolve_pin(Some("0")), None);
        assert_eq!(unprobed.default_pin(), None);
        assert_eq!(unprobed.resolve_device_key(None), None);
    }

    /// The existence rule (docs/unified-memory-admission.md, backend C): the
    /// CPU device appears on `Accelerator::Cpu` and on no other resolved
    /// accelerator.
    ///
    /// The negative half is the load-bearing one. `Cuda` reaching the probe
    /// means a CUDA torch is what is installed, so a host whose nvidia-smi is
    /// missing, wedged or unparseable stays *unknown* — unpriced, plus the
    /// startup WARN — rather than becoming CPU-priced, because its workers do
    /// run on the GPU and budgeting them against RAM would price the wrong
    /// memory entirely. This holds whatever the machine running the test has
    /// installed, which is why it is asserted as "no CPU device" rather than
    /// against a particular inventory.
    #[test]
    fn only_a_resolved_cpu_accelerator_gets_the_cpu_device() {
        let host = probe(Accelerator::Cpu);
        assert!(host.inventory.prices_host_ram());
        assert_eq!(
            host.caps.meets_floor(8.0),
            None,
            "a CPU host filters no model by a GPU capability: its workers are \
             pinned to the CPU device, and the impls' own load-time guard is \
             the backstop"
        );
        assert_eq!(host.inventory.resolve_pin(Some("0")), None);
        // Every platform this ships to has a RAM reader, so the GPU is
        // real here rather than fixture-shaped.
        #[cfg(any(target_os = "linux", target_os = "windows", target_os = "macos"))]
        {
            let gpu = host.inventory.gpus().expect("a host with RAM")[0].clone();
            assert_eq!(gpu.uuid, "CPU");
            assert!(gpu.unified() && gpu.total_mb > 0);
            assert!(gpu.name.starts_with("CPU ("), "name: {}", gpu.name);
            assert_eq!(host.inventory.memory_query().free_source(), "ram");
        }

        for accelerator in [
            Accelerator::Cuda,
            Accelerator::Rocm,
            Accelerator::Mps,
            Accelerator::Auto,
        ] {
            assert!(
                !probe(accelerator).inventory.prices_host_ram(),
                "{accelerator:?} must never be priced against system RAM — an \
                 accelerator host whose probe came back unknown stays unknown"
            );
        }
    }

    #[test]
    fn unpinned_replica_resolves_to_the_default_gpu() {
        let inventory = inventory();
        assert_eq!(
            inventory.resolve_pin(None),
            Some("GPU-1111".to_string()),
            "universal pinning: no pin means the default GPU's UUID"
        );
    }

    /// A known ROCm inventory speaks HIP's vocabulary: every pin it emits is
    /// a device index, never a `GPU-…` string. Written into
    /// `HIP_VISIBLE_DEVICES` a device key matches nothing, hides every
    /// device, and drops the worker to CPU in silence, so it must never get
    /// there — the key stays the *ledger's* identity and the index is the
    /// pin (D2).
    #[test]
    fn a_rocm_inventory_never_emits_a_device_key_as_a_pin() {
        let host = rocm_inventory(
            PathBuf::from("/sys/bus/pci/devices"),
            vec![
                amd_gpu(0, "0000:03:00.0", 24576),
                amd_gpu(1, "0000:0c:00.0", 24576),
            ],
        );
        // Universal pinning, in indices: the fastest GPU is the tie-break
        // winner (equal VRAM here, so the lowest index).
        assert_eq!(host.default_pin(), Some("0".to_string()));
        assert_eq!(host.resolve_pin(None), Some("0".to_string()));
        // The property, over every shape a registry can hand this host —
        // resolvable, unresolvable, hostile, empty. Whatever comes back is
        // HIP-readable or nothing; a `None` is a passing outcome, so the
        // drop cases belong in the same sweep rather than in a list of
        // hand-checked equalities.
        for requested in [
            None,
            Some("1"),
            Some("GPU-BDF-0000:0c:00.0"),
            Some("gpu-bdf-0000:0C:00.0"),
            Some("7"),
            Some("0,1"),
            Some(" 1 , 2 "),
            Some("00"),
            Some("cpu"),
            Some("${DEVICE}"),
            Some("GPU-1a2b"),
            Some("0,GPU-BDF-0000:03:00.0"),
            Some("4294967296"),
            Some(""),
            Some("  "),
        ] {
            let Some(pin) = host.resolve_pin(requested) else {
                continue;
            };
            assert!(
                !pin.is_empty() && pin.split(',').all(|entry| entry.parse::<u32>().is_ok()),
                "{requested:?} resolved to {pin:?}, which HIP cannot read as a \
                 device index"
            );
        }
        // The GPU name is still available: the /metadata calibration
        // overlay needs it, and it never reaches a worker's environment.
        assert_eq!(
            host.default_gpu_name().as_deref(),
            Some("AMD gfx1100 (24 GB)")
        );
    }

    /// Default placement on ROCm ranks by VRAM (every GPU's `compute_cap`
    /// is `None`) and answers in HIP's vocabulary — the row's position in
    /// the openable KFD-node order, which is the HIP device index.
    #[test]
    fn the_rocm_default_pin_is_the_default_gpus_index() {
        let host = rocm_inventory(
            PathBuf::from("/sys/bus/pci/devices"),
            vec![
                // An APU-shaped first row: enumerated first, far smaller.
                amd_gpu(0, "0000:03:00.0", 2048),
                amd_gpu(1, "0000:0c:00.0", 24576),
            ],
        );
        assert_eq!(
            host.default_pin(),
            Some("1".to_string()),
            "the dGPU wins on VRAM, and its pin is its index — not its key"
        );
        assert_eq!(host.resolve_pin(None), Some("1".to_string()));
    }

    /// The device key an operator writes in `devices` (the same string the
    /// ledger and the per-GPU VRAM overrides use) is translated to the
    /// row's HIP index here, once. Both key forms, either case, with
    /// whatever whitespace the TOML carried.
    #[test]
    fn rocm_device_keys_resolve_to_their_row_index() {
        let mut fused = amd_gpu(0, "0000:03:00.0", 24576);
        fused.uuid = "GPU-0123456789abcdef".to_owned();
        let host = rocm_inventory(
            PathBuf::from("/sys/bus/pci/devices"),
            vec![fused, amd_gpu(1, "0000:0c:00.0", 16384)],
        );
        assert_eq!(
            host.resolve_pin(Some("GPU-0123456789abcdef")),
            Some("0".to_string()),
            "the fused KFD unique_id form"
        );
        assert_eq!(
            host.resolve_pin(Some("GPU-BDF-0000:0c:00.0")),
            Some("1".to_string()),
            "the synthetic BDF form"
        );
        assert_eq!(
            host.resolve_pin(Some("  gpu-bdf-0000:0C:00.0  ")),
            Some("1".to_string()),
            "case-insensitive and trimmed, like the CUDA UUID handling"
        );
        // Exact, not prefix: CUDA's abbreviated UUIDs are a runtime feature
        // of a string we hand to CUDA, but these keys never reach HIP, and a
        // prefix could name two GPUs on the same bus.
        assert_eq!(
            host.resolve_pin(Some("GPU-BDF-0000:0c")),
            None,
            "a truncated key is not a key"
        );
    }

    /// HIP takes indices, so a numeric pin survives even when it names no
    /// row we can see (the mirror of the CUDA arm's unresolvable-index
    /// passthrough), and so does an all-numeric list. Both warn; the
    /// operator's intent is the thing being preserved.
    ///
    /// What survives is the *canonical* rendering, not the operator's
    /// spelling: `prewarm.rs` claims a parked worker only when its recorded
    /// pin string equals the replica's resolved one, so `"00"` and `" 0 "`
    /// have to converge on the `"0"` that `default_pin` renders or pooling
    /// quietly stops matching on this host.
    #[test]
    fn rocm_numeric_pins_pass_through_canonicalised() {
        let host = rocm_inventory(
            PathBuf::from("/sys/bus/pci/devices"),
            vec![
                amd_gpu(0, "0000:03:00.0", 24576),
                amd_gpu(1, "0000:0c:00.0", 24576),
            ],
        );
        assert_eq!(host.resolve_pin(Some("1")), Some("1".to_string()));
        assert_eq!(host.resolve_pin(Some(" 0 ")), Some("0".to_string()));
        assert_eq!(
            host.resolve_pin(Some("7")),
            Some("7".to_string()),
            "an index beyond this host's GPUs is still the operator's call"
        );
        assert_eq!(
            host.resolve_pin(Some("0,1")),
            Some("0,1".to_string()),
            "a multi-device list is HIP-legal; the ledger simply cannot price it"
        );
        // Canonical forms. `u32::from_str` accepts a leading `+`, and that is
        // fine here precisely because the value is normalised away rather
        // than forwarded: HIP never sees the `+`.
        assert_eq!(host.default_pin(), Some("0".to_string()));
        for spelling in ["00", " 0 ", "+0", "0000"] {
            assert_eq!(
                host.resolve_pin(Some(spelling)),
                host.default_pin(),
                "{spelling:?} names the default GPU and must render \
                 identically to it, or the prewarm pool stops claiming"
            );
        }
        assert_eq!(host.resolve_pin(Some(" 1 , 2 ")), Some("1,2".to_string()));
        assert_eq!(
            host.resolve_pin(Some("0,")),
            Some("0".to_string()),
            "a trailing separator is how HIP's own parser reads a one-device \
             list"
        );
        assert_eq!(
            host.resolve_pin(Some("4294967296")),
            None,
            "numeric but past u32: not an index HIP could act on, so it is \
             dropped like any other unreadable string"
        );
    }

    /// The one place ROCm refuses to pass a pin through: a non-numeric
    /// string that matches no device key. In `HIP_VISIBLE_DEVICES` it would
    /// match no device, hide the whole GPU set and drop the worker to the
    /// CPU — strictly worse than the no-pin behaviour dropping it preserves.
    #[test]
    fn rocm_drops_a_pin_hip_could_not_read() {
        let host = rocm_inventory(
            PathBuf::from("/sys/bus/pci/devices"),
            vec![amd_gpu(0, "0000:03:00.0", 24576)],
        );
        // A CUDA config's GPU UUID, carried over to an AMD host.
        assert_eq!(host.resolve_pin(Some("GPU-1a2b")), None);
        // A device key for a GPU this host does not have.
        assert_eq!(host.resolve_pin(Some("GPU-BDF-0000:ff:00.0")), None);
        // An unexpanded template and a stray word.
        assert_eq!(host.resolve_pin(Some("${DEVICE}")), None);
        assert_eq!(host.resolve_pin(Some("cpu")), None);
        // A mixed list is not an index list.
        assert_eq!(host.resolve_pin(Some("0,GPU-BDF-0000:03:00.0")), None);
        // The empty string, which a templated config expands to more often
        // than anything else here. It is *not* "no pin": no pin means the
        // default GPU, and silently promoting an expansion failure to
        // universal pinning would put a worker on a GPU nobody named.
        assert_eq!(host.resolve_pin(Some("")), None);
        assert_eq!(host.resolve_pin(Some("   ")), None);
        assert_eq!(host.resolve_pin(Some(",")), None);
        assert_eq!(
            host.resolve_pin(None),
            Some("0".to_string()),
            "and *that* is what no pin means"
        );
    }

    /// An unknown inventory keeps today's passthrough on the CUDA and
    /// no-accelerator backends — which is why the *variable* is chosen by
    /// the resolved accelerator and not by the inventory.
    #[test]
    fn the_pin_variable_follows_the_resolved_accelerator() {
        assert_eq!(pin_env_var(Accelerator::Rocm), "HIP_VISIBLE_DEVICES");
        for accelerator in [Accelerator::Cuda, Accelerator::Cpu, Accelerator::Auto] {
            assert_eq!(
                pin_env_var(accelerator),
                "CUDA_VISIBLE_DEVICES",
                "{accelerator:?} keeps the pre-ROCm variable"
            );
        }
        let unknown = GpuInventory::unknown();
        assert_eq!(unknown.resolve_pin(Some("1")), Some("1".to_string()));
        // Verbatim means verbatim on that backend: nothing is filtered,
        // nothing is normalised, because CUDA is the one that reads it and
        // an unresolvable string there is the operator's to explain.
        for requested in ["GPU-1a2b", "${DEVICE}", "cpu", " 0 ", ""] {
            assert_eq!(
                unknown.resolve_pin(Some(requested)),
                Some(requested.to_string()),
                "the non-ROCm unknown-inventory arm must not have changed"
            );
        }
        assert_eq!(unknown.resolve_pin(None), None);
    }

    /// A ROCm host that found no GPUs (ambient restriction, probe failure,
    /// non-Linux) does not thereby forget it is a ROCm host. It has nothing
    /// to translate a device key against, but HIP's grammar still applies —
    /// so an index passes and a `GPU-…` string, which would hide every GPU
    /// and drop the worker to the CPU, still does not.
    #[test]
    fn an_unknown_rocm_inventory_keeps_hips_vocabulary() {
        let host = uninventoried_rocm(false);
        assert!(host.gpus().is_none());
        // HIP-legal, so it survives — canonicalised, exactly as it would be
        // on a host whose GPUs we could see.
        assert_eq!(host.resolve_pin(Some("0")), Some("0".to_string()));
        assert_eq!(host.resolve_pin(Some("0,1")), Some("0,1".to_string()));
        assert_eq!(host.resolve_pin(Some(" 1 , 2 ")), Some("1,2".to_string()));
        assert_eq!(host.resolve_pin(Some("00")), Some("0".to_string()));
        // Not HIP-legal, so it is dropped rather than passed through the way
        // an unknown *CUDA* host would pass it.
        assert_eq!(host.resolve_pin(Some("GPU-1a2b")), None);
        assert_eq!(host.resolve_pin(Some("${DEVICE}")), None);
        assert_eq!(host.resolve_pin(Some("cpu")), None);
        assert_eq!(host.resolve_pin(Some("")), None);
        assert_eq!(host.resolve_pin(Some("4294967296")), None);
        // And with no GPUs there is no default GPU either.
        assert_eq!(host.resolve_pin(None), None);
        assert_eq!(host.default_pin(), None);
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
        let restricted = uninventoried_rocm(true);
        for requested in ["0", "0,1", "GPU-1a2b", "cpu", ""] {
            assert_eq!(
                restricted.resolve_pin(Some(requested)),
                None,
                "{requested:?} must not be written over the operator's own \
                 HIP-layer restriction"
            );
        }
        assert_eq!(restricted.resolve_pin(None), None);
        // The guard sits at the top of `resolve_pin`, before the inventory is
        // consulted at all, so it cannot be bypassed by a GPU list. Today
        // the probe never produces this combination (any HIP-layer variable
        // also blanks the inventory), which is exactly why the guard has to
        // be positional rather than rely on that invariant holding forever.
        let restricted_with_gpus = GpuInventory {
            gpus: Some(vec![amd_gpu(0, "0000:03:00.0", 24576)].into()),
            backend: MemoryBackend::RocmSysfs {
                pci_devices: PathBuf::from("/sys/bus/pci/devices"),
                meminfo: PathBuf::from("/proc/meminfo"),
                ambient_hip_restriction: true,
            },
        };
        for requested in [None, Some("0"), Some("GPU-BDF-0000:03:00.0")] {
            assert_eq!(
                restricted_with_gpus.resolve_pin(requested),
                None,
                "{requested:?} must not be written over the operator's own \
                 restriction, inventory or no inventory"
            );
        }
        // The flag is what distinguishes them, and it comes from the same
        // positional array the probe reads the environment into.
        use super::rocm::{VISIBILITY_VARS, ambient_hip_restriction};
        let ambient = |set: &str| {
            let values = VISIBILITY_VARS.map(|var| (var == set).then_some("0"));
            ambient_hip_restriction(values)
        };
        assert!(
            !ambient("ROCR_VISIBLE_DEVICES"),
            "composes with a HIP index"
        );
        assert!(ambient("HIP_VISIBLE_DEVICES"), "the variable we write");
        assert!(ambient("CUDA_VISIBLE_DEVICES"), "the alias we outrank");
        assert!(ambient("GPU_DEVICE_ORDINAL"), "the same layer");
        assert!(!ambient("NOTHING_SET_AT_ALL"));
        // Both set: the scan must not stop at ROCR, which comes first.
        assert!(ambient_hip_restriction(VISIBILITY_VARS.map(|var| {
            (var == "ROCR_VISIBLE_DEVICES" || var == "HIP_VISIBLE_DEVICES").then_some("0")
        })));
        // Whitespace/comma-only values are "not configured", as everywhere.
        assert!(!ambient_hip_restriction(
            VISIBILITY_VARS.map(|var| (var == "HIP_VISIBLE_DEVICES").then_some(" , "))
        ));
    }

    /// The pin *vocabulary* and the pin *variable* are one decision.
    /// `pins_are_indices()` is the single source of the first — it is what
    /// `default_pin` and `resolve_pin` branch on — and it and `pin_env_var`
    /// must never disagree, because a GPU UUID in `HIP_VISIBLE_DEVICES` or
    /// an index in `CUDA_VISIBLE_DEVICES` hides every GPU from the worker.
    ///
    /// Asserted against the real `probe`, which is where the two are
    /// actually wired together (the backend and the variable both come from
    /// the resolved accelerator), and against the known-inventory fixtures
    /// for the vocabulary each one then emits.
    #[test]
    fn the_pin_vocabulary_and_the_pin_variable_agree() {
        // ROCm, including on this box — the probe finds no AMD GPUs off
        // Linux, and that must not change the answer.
        let probed = probe(Accelerator::Rocm).inventory;
        assert!(
            probed.pins_are_indices(),
            "a ROCm host pins by index whether or not its probe found GPUs"
        );
        assert_eq!(pin_env_var(Accelerator::Rocm), HIP_PIN_ENV_VAR);
        let known_rocm = rocm_inventory(
            PathBuf::from("/sys/bus/pci/devices"),
            vec![amd_gpu(0, "0000:03:00.0", 24576)],
        );
        assert!(known_rocm.pins_are_indices());
        assert_eq!(
            known_rocm.default_pin(),
            Some("0".to_string()),
            "an index — never the GPU-BDF-… key the ledger is keyed by"
        );
        assert_eq!(
            known_rocm.gpus().expect("known")[0].uuid,
            "GPU-BDF-0000:03:00.0",
            "and the key is still there, for everything that is not a pin"
        );
        // CUDA, and every accelerator that is not ROCm.
        for accelerator in [Accelerator::Cuda, Accelerator::Cpu, Accelerator::Auto] {
            assert_eq!(pin_env_var(accelerator), CUDA_PIN_ENV_VAR);
        }
        assert!(!probe(Accelerator::Cuda).inventory.pins_are_indices());
        let known_cuda = inventory();
        assert!(!known_cuda.pins_are_indices());
        assert_eq!(
            known_cuda.default_pin(),
            Some("GPU-1111".to_string()),
            "a UUID, which is the only unambiguous form CUDA takes"
        );
    }

    #[test]
    fn index_pins_map_to_uuids() {
        let inventory = inventory();
        assert_eq!(
            inventory.resolve_pin(Some("3")),
            Some("GPU-3333".to_string())
        );
        assert_eq!(
            inventory.resolve_pin(Some(" 0 ")),
            Some("GPU-1111".to_string())
        );
    }

    #[test]
    fn uuid_pins_pass_through_verbatim() {
        let inventory = inventory();
        assert_eq!(
            inventory.resolve_pin(Some("GPU-9999")),
            Some("GPU-9999".to_string()),
            "an explicit UUID is accepted even for a GPU we cannot see"
        );
        assert_eq!(
            inventory.resolve_pin(Some("MIG-abc")),
            Some("MIG-abc".to_string())
        );
    }

    /// A CUDA UUID pin that names a GPU we *can* see comes back in the
    /// **inventory's** spelling, not the operator's. CUDA accepts every
    /// spelling — either case, any unambiguous abbreviation — but the pin
    /// string is compared byte-wise elsewhere: `prewarm.rs` claims a parked
    /// worker only when its recorded pin equals the replica's resolved one,
    /// and `resolve_device_key` already canonicalises for the ledger. Two
    /// spellings of one GPU therefore have to converge here, or the pool
    /// and the ledger disagree about which replicas share a card.
    #[test]
    fn cuda_uuid_pins_are_canonicalised_against_the_inventory() {
        let inventory = GpuInventory::known(vec![
            gpu(0, "GPU-1a2b0000-0000-0000-0000-000000000000", "12.0"),
            gpu(1, "GPU-1a2b9999-0000-0000-0000-000000000000", "12.0"),
            gpu(2, "GPU-ffff0000-0000-0000-0000-000000000000", "12.0"),
        ]);
        assert_eq!(
            inventory.resolve_pin(Some("gpu-FFFF0000-0000-0000-0000-000000000000")),
            Some("GPU-ffff0000-0000-0000-0000-000000000000".to_string()),
            "an exact match differing only in case takes the inventory's form"
        );
        assert_eq!(
            inventory.resolve_pin(Some("  GPU-ffff  ")),
            Some("GPU-ffff0000-0000-0000-0000-000000000000".to_string()),
            "an unambiguous abbreviation resolves to the full UUID, which \
             CUDA accepts everywhere the abbreviation was legal"
        );
        assert_eq!(
            inventory.resolve_pin(Some("GPU-1a2b")),
            Some("GPU-1a2b".to_string()),
            "two GPUs share the prefix: verbatim, as before — resolving it \
             is CUDA's business, and guessing a GPU would be worse"
        );
        assert_eq!(
            inventory.resolve_pin(Some("GPU-deadbeef")),
            Some("GPU-deadbeef".to_string()),
            "a UUID this host cannot see is unchanged"
        );
        assert_eq!(
            inventory.resolve_pin(Some("MIG-abc")),
            Some("MIG-abc".to_string()),
            "and so is a MIG instance outside the enumeration"
        );
        // The point of the change: the pin and the ledger key now agree for
        // every spelling that names a GPU, which is what the pool compares.
        for spelling in [
            "GPU-ffff",
            "gpu-FFFF0000",
            "GPU-ffff0000-0000-0000-0000-000000000000",
            "2",
        ] {
            assert_eq!(
                inventory.resolve_pin(Some(spelling)),
                inventory.resolve_device_key(Some(spelling)),
                "{spelling:?} must resolve to one string on both sides"
            );
        }
    }

    #[test]
    fn unresolvable_pins_pass_through() {
        let inventory = inventory();
        // Index nobody reported, a multi-device list, and a non-numeric
        // string all reach CUDA_VISIBLE_DEVICES unchanged.
        assert_eq!(inventory.resolve_pin(Some("7")), Some("7".to_string()));
        assert_eq!(inventory.resolve_pin(Some("0,3")), Some("0,3".to_string()));
        assert_eq!(inventory.resolve_pin(Some("cpu")), Some("cpu".to_string()));
    }

    /// The ledger vocabulary of the same registry entry: a device key, on
    /// both backends, for every form a pin can take. This is what closes
    /// D2's load-reservation gap — the pin and the key are resolved as a
    /// pair from one request, and on ROCm they are never the same string.
    #[test]
    fn device_keys_resolve_in_both_vocabularies() {
        let cuda = inventory();
        assert_eq!(
            cuda.resolve_device_key(None),
            Some("GPU-1111".to_string()),
            "no request is the default GPU, the one universal pinning uses"
        );
        assert_eq!(cuda.resolve_pin(None), cuda.resolve_device_key(None));
        assert_eq!(
            cuda.resolve_device_key(Some("3")),
            Some("GPU-3333".to_string()),
            "an index names a row, whose key is what the ledger holds"
        );
        assert_eq!(
            cuda.resolve_device_key(Some(" gpu-3333 ")),
            Some("GPU-3333".to_string()),
            "the key comes back in the inventory's spelling, not the operator's"
        );
        assert_eq!(
            cuda.resolve_device_key(Some("7")),
            None,
            "an index nobody reported names no ledger row (the pin still \
             passes through to CUDA)"
        );
        assert_eq!(cuda.resolve_device_key(Some("0,3")), None, "a device list");
        assert_eq!(cuda.resolve_device_key(Some("cpu")), None);
        assert_eq!(
            cuda.resolve_device_key(Some("GPU-9999")),
            None,
            "a UUID for a GPU this host cannot see"
        );

        let rocm = rocm_inventory(
            PathBuf::from("/sys/bus/pci/devices"),
            vec![
                amd_gpu(0, "0000:03:00.0", 24576),
                amd_gpu(1, "0000:0c:00.0", 24576),
            ],
        );
        assert_eq!(
            (
                rocm.resolve_pin(Some("1")),
                rocm.resolve_device_key(Some("1"))
            ),
            (
                Some("1".to_string()),
                Some("GPU-BDF-0000:0c:00.0".to_string())
            ),
            "the pair: HIP gets the index, the ledger gets the key"
        );
        assert_eq!(
            rocm.resolve_device_key(Some("GPU-BDF-0000:0C:00.0")),
            Some("GPU-BDF-0000:0c:00.0".to_string()),
            "a device key resolves to itself, case-insensitively"
        );
        assert_eq!(
            rocm.resolve_device_key(None),
            Some("GPU-BDF-0000:03:00.0".to_string()),
            "while the pin for the same request is the index 0"
        );
        assert_eq!(rocm.resolve_pin(None), Some("0".to_string()));
        assert_eq!(
            rocm.resolve_device_key(Some("GPU-BDF-0000:0c")),
            None,
            "no prefix matching on ROCm: a prefix could name two GPUs on \
             one bus, and these keys never reach HIP"
        );
        assert_eq!(rocm.resolve_device_key(Some("9")), None);
        assert_eq!(rocm.resolve_device_key(Some("0,1")), None);
    }

    /// CUDA resolves abbreviated UUIDs itself, so `resolve_pin` hands them
    /// to it verbatim — which means the ledger has to resolve them too, or
    /// an operator who wrote `GPU-1a2b` silently gets no load reservation.
    /// An *ambiguous* abbreviation resolves to nothing: reserving against
    /// the wrong GPU is worse than not reserving.
    #[test]
    fn abbreviated_cuda_uuids_resolve_to_a_device_key_when_unambiguous() {
        let inventory = GpuInventory::known(vec![
            gpu(0, "GPU-1a2b0000-0000-0000-0000-000000000000", "12.0"),
            gpu(1, "GPU-1a2b9999-0000-0000-0000-000000000000", "12.0"),
            gpu(2, "GPU-ffff0000-0000-0000-0000-000000000000", "12.0"),
        ]);
        assert_eq!(
            inventory.resolve_device_key(Some("GPU-ffff")),
            Some("GPU-ffff0000-0000-0000-0000-000000000000".to_string())
        );
        assert_eq!(
            inventory.resolve_device_key(Some("gpu-FFFF0000")),
            Some("GPU-ffff0000-0000-0000-0000-000000000000".to_string()),
            "case-insensitive, as CUDA is"
        );
        assert_eq!(
            inventory.resolve_device_key(Some("GPU-1a2b")),
            None,
            "two GPUs share the prefix: refuse rather than guess"
        );
        assert_eq!(
            inventory.resolve_device_key(Some("GPU-")),
            None,
            "the degenerate prefix matches everything"
        );
        assert_eq!(
            inventory.resolve_device_key(Some("MIG-unknown")),
            None,
            "a MIG instance outside the enumeration has no ledger GPU"
        );
        // On a single-GPU host the same degenerate prefix is unambiguous
        // and resolves — which is exactly what CUDA does with it, so the
        // reservation lands on the GPU the pin will select. Asserted
        // because it is a behaviour, not an accident of the ambiguity rule.
        let only = GpuInventory::known(vec![gpu(
            0,
            "GPU-ffff0000-0000-0000-0000-000000000000",
            "12.0",
        )]);
        assert_eq!(
            only.resolve_device_key(Some("GPU-")),
            Some("GPU-ffff0000-0000-0000-0000-000000000000".to_string())
        );
    }

    #[test]
    fn unknown_inventory_changes_nothing() {
        let unknown = GpuInventory::unknown();
        assert_eq!(
            unknown.resolve_device_key(Some("3")),
            None,
            "no inventory is no ledger GPU to key against either"
        );
        assert_eq!(unknown.resolve_device_key(None), None);
        assert!(unknown.gpus().is_none());
        assert_eq!(unknown.default_pin(), None);
        assert_eq!(unknown.resolve_pin(None), None, "no pin, as before");
        assert_eq!(
            unknown.resolve_pin(Some("3")),
            Some("3".to_string()),
            "raw index passthrough, as before"
        );
    }
}
