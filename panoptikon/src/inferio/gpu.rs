//! GPU identity enumeration and worker→GPU pin resolution.
//!
//! Batch calibration keys budgets by GPU *instance* — the board UUID
//! (`GPU-…`) NVML/nvidia-smi report — because a CUDA device index is not an
//! identity: it moves across reboots and with every `CUDA_VISIBLE_DEVICES`
//! change (docs/batch-calibration-design.md, "Two keyspaces"). This module
//! is the source of those identities, and the one place that turns a
//! registry `devices` entry (or the absence of one) into the concrete
//! pin value a worker is spawned with — plus, in [`pin_env_var`], the
//! variable that value is written to, because the two are one decision: a
//! board UUID belongs in `CUDA_VISIBLE_DEVICES` and only a device index
//! belongs in `HIP_VISIBLE_DEVICES`, and writing either into the other's
//! variable hides every board from the worker.
//!
//! Probing reuses the Package-1 philosophy from `capability.rs`: one short
//! `nvidia-smi` call with a timeout, and any unparseable *identity* makes
//! the whole result unknown. Unknown never changes behaviour — pins pass
//! through exactly as they did before this existed (raw index strings, or
//! no pin at all), which is what keeps CPU/MPS hosts and hosts without
//! nvidia-smi on today's code path. (ROCm hosts take the sysfs probe
//! below instead of falling through here, and Apple Silicon takes `mps.rs`.)
//!
//! The `compute_cap` column is the one exception, because it is the one
//! field that is *separably* useless: vGPU slices and a few datacenter SKUs
//! print `[N/A]` there while every identity column is perfectly good, and
//! discarding those rows wholesale would take pinning, the ledger and the
//! board list down with them. Such a board keeps its identity and is simply
//! never chosen by capability-ranked default placement.
//!
//! On CUDA there is exactly **one** probe for both hardware facts the
//! server needs (board identities here, compute capabilities in
//! `capability.rs`): they
//! come from one `--query-gpu` invocation, so the two views can never
//! disagree about which board is which, and boot pays one subprocess
//! instead of two. Rows are matched positionally, so an inventory index and
//! a capability always describe the same physical board.
//!
//! [`probe`] takes the **resolved** accelerator (the setup sentinel's, not
//! a re-probe of the hardware) and dispatches on it: ROCm hosts get the
//! kernel-sysfs inventory in `rocm.rs` instead, with an always-unknown
//! capability view because HIP has no compute-capability analogue
//! (docs/rocm-batch-calibration-parity.md, D1/D7), and Apple Silicon gets
//! the single synthetic unified-memory board in `mps.rs`
//! (docs/unified-memory-admission.md). Every other accelerator
//! — including a `cpu` host that happens to have an NVIDIA card — keeps the
//! nvidia-smi path exactly as it was, so capability filtering never depends
//! on which wheels are installed.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::capability::{
    HostComputeCaps, find_nvidia_smi, output_with_timeout, parse_compute_cap,
};
use super::mps;
use super::rocm;
use crate::config::Accelerator;

/// The variable CUDA filters devices with, and HIP's compatibility alias for
/// its own. Written with a `GPU-…` board UUID on CUDA hosts.
pub const CUDA_PIN_ENV_VAR: &str = "CUDA_VISIBLE_DEVICES";

/// HIP's own device filter, written with a **device index** (D2).
///
/// The two AMD variables do not compete, they *compose*:
/// `ROCR_VISIBLE_DEVICES` filters below, at the ROCr/KFD layer, and a HIP
/// index counts into whatever survived that filter. We choose the HIP form
/// because AMD documents it as the application-scoped mechanism, because it
/// is the layer our own indices are enumerated in (`rocm.rs` reconstructs
/// ROCr's agent order), and — decisively — because torch < 2.6 crashes at
/// init when the ROCR form is set, which a user-managed venv can still be.
pub const HIP_PIN_ENV_VAR: &str = "HIP_VISIBLE_DEVICES";

/// Set on a worker pinned to a **unified** board, so its own memory
/// arithmetic includes GTT (docs/unified-memory-admission.md, DP-5). The
/// value is that board's **PCI address**, not a flag.
///
/// The address is what makes this self-validating rather than a belief. The
/// spawner knows which board the pin *named*; it does not know which board
/// the replica came up on, and the one load-bearing unverifiable of the ROCm
/// design is precisely that the inventory's row order is HIP's device order
/// (docs/rocm-batch-calibration-parity.md, D2). A bare flag on a
/// mis-enumerated host would tell a worker that landed on a **dGPU** to add
/// GTT to its free reading and report the sum under the authoritative
/// `"amdgpu-sysfs"` label — phantom headroom, the one error direction the
/// ledger cannot absorb — and would tell a worker that landed on the *APU*
/// to price it at its 512 MB carve-out, collapsing it to batch-1. With the
/// address, the worker compares it against the board it independently
/// resolved (`memory.py::_identity_bdf`) and only counts GTT when the two
/// agree; a mismatch, or a value it cannot parse, falls back to the discrete
/// arithmetic, which is conservative in both directions.
///
/// Written for unified **ROCm** boards only. MPS's tiers are unified by
/// construction (there is no other kind of board on a Mac, and the worker's
/// own `torch.mps` tier is the only one that can answer there), so setting it
/// on those workers would be inert noise in the one place a reader could
/// mistake it for a signal.
pub const UNIFIED_GPU_ENV_VAR: &str = "PANOPTIKON_UNIFIED_GPU";

/// Written alongside the backend's visibility variable with the same
/// resolved pin: *we* placed this replica, and on this device.
///
/// The visibility variables cannot say that. An operator's ambient
/// `CUDA_VISIBLE_DEVICES` is indistinguishable from one of ours in the
/// child's environment, and the two mean opposite things to the worker's
/// pinned-but-invisible tripwire (`memory.py::pinned_device_missing`): a
/// replica *we* pinned to a device the runtime does not enumerate has
/// silently fallen back to the CPU and must fail its load, while a host
/// whose operator hid every device meant exactly that and must keep
/// working. So the tripwire keys off this marker instead, and fires only
/// where the orchestrator made the placement.
pub const DEVICE_PIN_MARKER_ENV_VAR: &str = "PANOPTIKON_DEVICE_PIN";

/// Which variable a resolved pin is written to, decided by the **resolved
/// accelerator** rather than by the inventory (docs/rocm-batch-calibration-parity.md,
/// D2).
///
/// Deciding by the accelerator is what makes the *unknown-inventory* ROCm
/// host behave: an ambient visibility restriction or a probe failure blanks
/// the inventory, and `resolve_pin` still lets a HIP-legal registry pin
/// through — an index, the only form HIP accepts, has to reach HIP to mean
/// anything. Deciding by the *inventory* would send it to
/// `CUDA_VISIBLE_DEVICES` instead, which HIP only consults when
/// `HIP_VISIBLE_DEVICES` is unset — so the pin would land in the weaker of
/// the two aliases on exactly the hosts that are hardest to reason about.
///
/// `CUDA_VISIBLE_DEVICES` is deliberately **not** also set on ROCm: it is a
/// HIP alias, and AMD documents setting both as unintended-behaviour
/// territory. The accelerator sentinel's HSA/MIOpen worker env
/// (`accelerator_env::worker_env`) composes with this untouched — it never
/// sets a visibility variable.
pub fn pin_env_var(accelerator: Accelerator) -> &'static str {
    match accelerator {
        Accelerator::Rocm => HIP_PIN_ENV_VAR,
        // Including `Auto`, which only reaches here unresolved from a caller
        // that has no sentinel to resolve with — the CUDA form is what those
        // hosts wrote before this dispatch existed. And including `Mps`,
        // where the answer is arbitrary because there is nothing to write:
        // one Metal device per host, no visibility variable that selects it,
        // and `GpuInventory::resolve_pin` therefore never yields a pin at all
        // (docs/unified-memory-admission.md, backend A).
        Accelerator::Cuda | Accelerator::Cpu | Accelerator::Mps | Accelerator::Auto => {
            CUDA_PIN_ENV_VAR
        }
    }
}

/// One visible board, from nvidia-smi (CUDA) or KFD topology (ROCm).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct GpuInfo {
    /// Enumeration index: nvidia-smi's on CUDA, the position within the
    /// openable KFD-node set on ROCm (which is the HIP device index). Useful
    /// only for resolving registry `devices = ["3"]` pins; never an identity.
    pub index: u32,
    /// Board UUID (`GPU-…`), the budget/ledger key and the pin form CUDA
    /// accepts directly in `CUDA_VISIBLE_DEVICES`. On ROCm it is the fused
    /// KFD `unique_id` or a synthetic `GPU-BDF-…` (see `rocm.rs`) — an
    /// identity, not a pin form, because HIP only accepts indices.
    pub uuid: String,
    /// Marketing name, e.g. `NVIDIA GeForce RTX 5090`; the cost-profile key.
    /// On ROCm, the deterministic `AMD gfx…` form `rocm.rs` derives.
    pub name: String,
    pub total_mb: u64,
    /// Compute capability as `major.minor` (`"12.0"`), the same value
    /// `HostComputeCaps` filters models with — per board here, because
    /// default placement picks the fastest one. `None` when nvidia-smi could
    /// not report it for this board (`[N/A]` on vGPU slices and some
    /// datacenter SKUs): the board is still a usable, pinnable identity, it
    /// just cannot be ranked or used to unlock a capability-gated model.
    /// Always `None` on ROCm — HIP has no analogue at all.
    pub compute_cap: Option<String>,
    /// PCI address `dddd:bb:dd.f`. ROCm only: it is the key into amdgpu's
    /// per-board sysfs counters (the memory refresh, D5) and the one
    /// vocabulary a worker can independently report about itself (D3).
    /// `None` on CUDA, where the UUID already serves both purposes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bdf: Option<String>,
    /// KFD's packed ISA target (`110000` = gfx1100). ROCm only; recorded so
    /// a future gfx-arch allowlist has its datum without another probe
    /// (D7). `None` on CUDA.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gfx_target_version: Option<u32>,
    /// Host RAM this board's memory is carved out of, in MiB — present
    /// exactly on **unified** boards (Apple Silicon today; AMD APUs when
    /// backend B lands), absent on a discrete board with private VRAM. Its
    /// presence *is* the unified flag ([`GpuInfo::unified`]), because the two
    /// facts are one: a board is unified precisely when its memory is the
    /// host's.
    ///
    /// Two things downstream read it. The ledger records a synthetic negative
    /// sample when a replica dies mid-window on such a board (DP-2: on a
    /// dGPU a mid-window death has too many non-memory causes, on a unified
    /// board it is overwhelmingly the OS memory killer), and it is the only
    /// sanity bound on the authoritative total a worker reports back (DP-4)
    /// — the board's own `total_mb` is a *policy* number there, tunable by
    /// the user, so it cannot bound anything.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unified_ram_mb: Option<u64>,
    /// The device-local VRAM carve-out of a unified **ROCm** board (an APU's
    /// `mem_info_vram_total`), in MiB — the part of [`Self::total_mb`] that
    /// is not GTT. `None` on every other board, including MPS, where no such
    /// split exists.
    ///
    /// It is carried because the carve-out is a figure other components
    /// legitimately mean by "this board's memory", and they must not be
    /// refused for it. HIP's `total_memory` on an APU may report the
    /// carve-out, the carve+GTT sum, or something else again — unverified
    /// until a BC-250 field pass — so the registration cross-check accepts
    /// **either**. It is also the placement rank
    /// ([`Self::placement_total_mb`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vram_carveout_mb: Option<u64>,
}

impl GpuInfo {
    /// Whether this board's memory is the host's RAM rather than private
    /// VRAM (see [`Self::unified_ram_mb`]).
    pub fn unified(&self) -> bool {
        self.unified_ram_mb.is_some()
    }

    /// The capacity figure default placement ranks boards by: the private
    /// VRAM carve-out on a unified ROCm board, [`Self::total_mb`] on every
    /// other.
    ///
    /// The two are unlike quantities and the tie-break has to compare like
    /// with like. A dGPU's total is memory it owns outright; an APU's total
    /// is carve-out + GTT, and the GTT half is borrowed from the RAM the OS
    /// and every other process are using — nominally most of the machine, in
    /// practice the first thing external pressure takes away. Ranking by it
    /// would hand default placement to the *slower* board on essentially
    /// every dGPU+APU host, since a Strix Halo's nominal budget dwarfs any
    /// consumer card's VRAM, and an unpinned model would quietly run on the
    /// iGPU.
    ///
    /// Ranking by the carve-out keeps the discrete board the default unless
    /// the APU is *genuinely* bigger — which on an APU means the operator
    /// went into the BIOS and gave the iGPU that memory outright, an explicit
    /// statement about where they want work to land. Note this is placement
    /// only: an APU that loses the tie-break is still fully priced, still
    /// budgeted against carve+GTT, and still selectable with a `devices` pin.
    ///
    /// The carve-out alone is too harsh at the other end, though: a
    /// 128 GB Strix Halo left at its 512 MB BIOS default would lose to a
    /// 2 GB display card, which is not a board anyone wants a model on. So a
    /// unified board ranks by `max(carve-out, total / 8)` — an eighth of the
    /// unified budget is a deliberately pessimistic reading of memory that is
    /// shared with the whole OS, and it is still enough to beat a token dGPU
    /// while staying under any real one (a 64 GB budget ranks as 8 GB, below
    /// every discrete card worth defaulting to).
    pub fn placement_total_mb(&self) -> u64 {
        match self.vram_carveout_mb {
            Some(carveout) => carveout.max(self.total_mb / 8),
            None => self.total_mb,
        }
    }

    /// `major * 10 + minor`, the comparable form, or `None` for a board
    /// whose capability nvidia-smi did not report. Every stored string
    /// parsed when it was accepted, so `None` here means "not reported",
    /// never "unreadable" — and it must never collapse to `0`, which would
    /// make an unknown board look like the slowest one rather than an
    /// unranked one.
    fn cap_tenths(&self) -> Option<u32> {
        parse_compute_cap(self.compute_cap.as_deref()?).map(|(major, minor)| major * 10 + minor)
    }
}

/// The visible GPUs, or `None` for "unknown host" (no nvidia-smi, probe
/// failure, or any unparseable output). Cheap to clone.
///
/// The inventory carries the interface its boards were read through, so the
/// ledger's staleness refresh cannot end up asking nvidia-smi about AMD
/// boards. Tests construct CUDA inventories, which is the default.
#[derive(Debug, Clone, Default)]
pub struct GpuInventory {
    boards: Option<Arc<[GpuInfo]>>,
    backend: MemoryBackend,
}

/// Which kernel/driver interface answers this host's live-memory questions
/// — and, by the same token, which vocabulary its pins are written in
/// ([`GpuInventory::pins_are_indices`]). The two travel together because
/// they are the same fact about the host: an amdgpu-sysfs host is a HIP
/// host.
///
/// Set from the **resolved accelerator**, not from whether any board was
/// found: a ROCm host whose probe came back empty is still a ROCm host, and
/// forgetting that would send its pins into CUDA's variable and its memory
/// refresh to nvidia-smi.
#[derive(Debug, Clone, Default)]
enum MemoryBackend {
    #[default]
    NvidiaSmi,
    RocmSysfs {
        /// The PCI device root the inventory was **probed** through, kept so
        /// the staleness refresh reads the same tree the boards were
        /// identified from instead of re-deriving the production default —
        /// which is what makes the refresh path testable from a fixture.
        pci_devices: PathBuf,
        /// `/proc/meminfo`, from the same probe roots and kept for the same
        /// reason. Read only for **unified** boards, whose free reading
        /// clamps unclaimed GTT to `MemAvailable`.
        meminfo: PathBuf,
        /// Whether an ambient restriction at HIP's own layer
        /// (`HIP_VISIBLE_DEVICES`, its `CUDA_VISIBLE_DEVICES` alias, or
        /// `GPU_DEVICE_ORDINAL`) was in force when the probe ran
        /// (`rocm::ambient_hip_restriction`). Only ever true alongside a
        /// blanked board list, since any such variable also makes the
        /// inventory unknown.
        ///
        /// It decides what happens to a registry pin on that blanked host:
        /// an index we write would clobber or override the operator's
        /// restriction and widen what they narrowed, so nothing is written
        /// at all. An ambient `ROCR_VISIBLE_DEVICES` does **not** set this
        /// — HIP indexes into the ROCr-filtered set, so an index pin
        /// selects within the operator's set instead of escaping it.
        ambient_hip_restriction: bool,
    },
    /// Apple Silicon: one synthetic unified-memory board (`mps.rs`), whose
    /// live free reading is the host's RAM statistics rather than any
    /// accelerator counter. No pin vocabulary at all — there is one device
    /// and no variable that selects it.
    Mps,
}

/// Everything one `nvidia-smi` call tells us about this host's boards.
pub struct HostGpus {
    /// Compute-capability floors for `/metadata` availability filtering.
    pub caps: HostComputeCaps,
    /// Board identities for worker→GPU pinning and the per-GPU ledger.
    pub inventory: GpuInventory,
}

/// Probe once at startup; never fails. There is no state in which we trust a
/// row's capability but not its identity — but the reverse exists twice over:
/// a board can report an identity and no capability (see [`GpuInfo`]), and an
/// ambient index-form `CUDA_VISIBLE_DEVICES` blanks the inventory while
/// leaving the capabilities perfectly knowable (see [`build`]).
///
/// `accelerator` is the **resolved** one (the setup sentinel's answer, which
/// `http.rs` computes before building the spawn config). Only `Rocm` takes
/// the sysfs path: an explicit `cpu` host with an NVIDIA card must keep
/// nvidia-smi's capability filtering, which is what it had before this
/// dispatch existed.
pub fn probe(accelerator: Accelerator) -> HostGpus {
    if accelerator == Accelerator::Rocm {
        return probe_rocm();
    }
    if accelerator == Accelerator::Mps {
        return probe_mps();
    }
    // The ambient value matters: nvidia-smi *ignores* CUDA_VISIBLE_DEVICES
    // and reports every board, so an operator who launched the gateway with
    // a restriction would otherwise see us pin workers to boards they
    // deliberately hid (see `restrict_to_visible`).
    let visible = std::env::var("CUDA_VISIBLE_DEVICES").ok();
    build(query(accelerator).as_deref(), visible.as_deref())
}

/// KFD topology + amdgpu sysfs (`rocm.rs`). The capability view is always
/// unknown: HIP exposes no compute capability, so there is nothing to
/// filter with and every shipped floor is CUDA-specific anyway (D7).
///
/// Off Linux this is unconditionally unknown — the `rocm` torch extra
/// carries a `sys_platform == 'linux'` marker, so no supported install has
/// ROCm wheels anywhere else, and the sysfs paths do not exist.
///
/// "Unknown" here means *no boards*, never *not a ROCm host*: the backend is
/// [`MemoryBackend::RocmSysfs`] on every path out of this function, including
/// the ambient-restricted, probe-failed and non-Linux ones. A
/// `GpuInventory::default()` there would silently hand the host CUDA's
/// vocabulary — nvidia-smi for the memory refresh, and a registry pin
/// written verbatim into `CUDA_VISIBLE_DEVICES` — on a machine that has
/// neither.
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
        // environment either; the legality filter in `resolve_pin` still
        // applies, it just has no restriction to defer to. `None` rather
        // than a failure because there is nothing to diagnose: the paths do
        // not exist here and were never going to.
        (None, false)
    };
    let backend = MemoryBackend::RocmSysfs {
        pci_devices: roots.pci_devices.clone(),
        meminfo: roots.meminfo.clone(),
        ambient_hip_restriction,
    };
    let gpus = match inventory {
        Some(Ok(gpus)) => gpus,
        // Silently-unpriced is the failure mode this arm exists to prevent:
        // the host behaves exactly as it did before the ROCm probe existed,
        // which is safe but indistinguishable from "the feature is not
        // working". `ProbeFailure::log` emits one WARN naming what was seen,
        // and stays quiet when the deciding site already named the node,
        // address or board it tripped on.
        Some(Err(failure)) => {
            failure.log();
            return HostGpus {
                caps: HostComputeCaps::unknown(),
                inventory: GpuInventory {
                    boards: None,
                    backend,
                },
            };
        }
        None => {
            return HostGpus {
                caps: HostComputeCaps::unknown(),
                inventory: GpuInventory {
                    boards: None,
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
            boards: Some(gpus.into()),
            backend,
        },
    }
}

/// One synthetic unified-memory board from macOS kernel facts (`mps.rs`).
/// The capability view is always unknown, as on ROCm: Metal exposes no
/// compute-capability analogue and every shipped floor is CUDA-specific.
///
/// Off macOS — and on a macOS whose sysctls did not answer — this is
/// unknown, and "unknown" again means *no boards*, never *not an MPS host*:
/// the backend stays [`MemoryBackend::Mps`] on every path out of here, so
/// such a host keeps its own (empty) memory refresh and its own no-pin rule
/// instead of silently inheriting CUDA's.
fn probe_mps() -> HostGpus {
    let inventory = |boards: Option<Arc<[GpuInfo]>>| HostGpus {
        caps: HostComputeCaps::unknown(),
        inventory: GpuInventory {
            boards,
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
    let board = mps::board(&facts);
    tracing::info!(
        index = board.index,
        uuid = %board.uuid,
        name = %board.name,
        total_mb = board.total_mb,
        ram_mb = facts.ram_bytes / (1024 * 1024),
        unified = board.unified(),
        "detected GPU (unified memory; the total is the 75% seed until a \
         worker reports the exact recommended-max figure)"
    );
    inventory(Some(vec![board].into()))
}

/// Run the single query. `None` on any failure — no nvidia-smi, timeout,
/// non-zero exit — and every failure is logged, at WARN when the host is
/// positively configured for CUDA. The ROCm probe already names every path
/// to an unknown inventory; without these the CUDA side's empty ledger was
/// indistinguishable from "the feature is not working".
fn query(accelerator: Accelerator) -> Option<String> {
    let Some(smi) = find_nvidia_smi() else {
        // Only a WARN on a CUDA host: `cpu` boxes (and `auto`, which means
        // accelerator resolution itself failed) legitimately have no
        // nvidia-smi, and a startup warning on every such machine is noise.
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

/// One board's live memory occupancy, from the ledger's staleness refresh.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GpuMemory {
    pub uuid: String,
    pub total_mb: u64,
    pub free_mb: u64,
}

/// How this host's live free/total memory is read, resolved once from the
/// inventory so the ledger never has to know which accelerator it is on.
///
/// Cheap to clone; the ROCm variant carries the board→BDF list because the
/// sysfs counters are per-board files with no enumeration of their own.
#[derive(Debug, Clone)]
pub(super) enum MemoryQuery {
    /// One `nvidia-smi --query-gpu` call covering every visible board.
    NvidiaSmi,
    /// amdgpu's `mem_info_vram_{total,used}`, one file pair per board — plus
    /// the `mem_info_gtt_{total,used}` pair and `MemAvailable` for a board
    /// the probe flagged unified (an APU, whose budget is carve-out + GTT).
    RocmSysfs {
        pci_devices: PathBuf,
        meminfo: PathBuf,
        /// Every board's key, address and unified flag, in inventory order.
        boards: Arc<[rocm::BoardRef]>,
    },
    /// macOS RAM statistics for the one unified board (`mps.rs`): what the
    /// OS says it could deliver right now, which on a unified board is what
    /// external pressure actually looks like.
    Mps {
        key: String,
        /// Physical RAM in MiB — a bound on the reading, not the board's
        /// admission total (see `mps::query_memory`).
        ram_mb: u64,
    },
    /// The inventory cannot be turned into a total set of per-board reads,
    /// so this host has no refresh at all: [`Self::run`] always answers
    /// `None` and the ledger keeps whatever it already had.
    ///
    /// A *partial* board list is the thing being ruled out here. The ROCm
    /// query is keyed by PCI address; a row without one could only be
    /// dropped, and the ledger — which sees a successful snapshot covering
    /// the boards it was handed — would then keep pricing that board off a
    /// stale reading while believing it was just refreshed. Refusing the
    /// whole refresh is the honest answer, and matches the all-or-nothing
    /// rule both backends' parsers already follow.
    ///
    /// A ROCm or MPS host with **no** board list at all (ambient
    /// restriction, probe failure, wrong platform) lands here too. Its ledger
    /// is empty and would never refresh anything anyway; the point is that it
    /// must not fall through to [`Self::NvidiaSmi`] and start shelling out to
    /// a binary that is not on this machine and could not describe its boards
    /// if it were.
    Unavailable,
}

impl MemoryQuery {
    /// Live free/total memory for every board the ledger knows.
    ///
    /// Used when the freshest worker-reported sample has aged past its
    /// threshold (docs/batch-calibration-design.md: samples arrive only on
    /// response frames, so an idle board's picture goes stale). `None` on
    /// any failure, in which case the ledger keeps the stale reading, which
    /// the worker's per-batch shrink clamp makes safe. Runs off the hot path
    /// (`spawn_blocking`), never inline.
    pub fn run(&self) -> Option<Vec<GpuMemory>> {
        match self {
            Self::NvidiaSmi => query_memory_nvidia_smi(),
            Self::RocmSysfs {
                pci_devices,
                meminfo,
                boards,
            } => rocm::query_memory(pci_devices, meminfo, boards),
            Self::Mps { key, ram_mb } => mps::query_memory(key, *ram_mb),
            Self::Unavailable => None,
        }
    }

    /// The provenance label the ledger records readings under. Both live
    /// backends are authoritative (device-wide, not process-local); the
    /// distinction matters because worker-reported `"torch"` readings are
    /// not. `"amdgpu-sysfs"` names the *driver* rather than the filesystem
    /// deliberately: a bare `"sysfs"` would let any future generic reporter
    /// inherit authority by string collision.
    pub fn free_source(&self) -> &'static str {
        match self {
            Self::NvidiaSmi => "nvidia-smi",
            // Byte-identical to the worker's own label for the same reading
            // (`inferio_worker/memory.py`), which is what keeps the ledger's
            // free-source consistency rule true across the two components on
            // a unified board: both sides read the OS's RAM statistics —
            // free + inactive pages, the same terms psutil's macOS
            // `available` sums — and both call that `"mps"`.
            Self::Mps { .. } => "mps",
            // Including `Unavailable`, which never records anything anyway
            // (it is what a backend with no boards resolves to, on either of
            // the two that can be in that state).
            Self::RocmSysfs { .. } | Self::Unavailable => "amdgpu-sysfs",
        }
    }
}

/// One `nvidia-smi` call, so per-board readings can never be stitched from
/// different moments. `None` on any failure — no nvidia-smi, timeout,
/// unparseable row.
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

/// One board per line, `uuid, total, free`. Any unparseable row makes the
/// whole reading unknown: a partial memory picture would silently price some
/// boards' external usage as zero, which is exactly the phantom headroom the
/// ledger's clamps exist to prevent.
fn parse_memory(stdout: &str) -> Option<Vec<GpuMemory>> {
    let mut boards = Vec::new();
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
        boards.push(GpuMemory {
            uuid,
            total_mb,
            free_mb,
        });
    }
    if boards.is_empty() { None } else { Some(boards) }
}

/// Turn probe output plus the ambient `CUDA_VISIBLE_DEVICES` into both
/// views. Pure, so tests drive it without a GPU and without mutating the
/// process environment.
///
/// The two views degrade independently in exactly one direction. A
/// restriction we cannot map to boards (index form) blanks the **inventory**
/// only — nobody gets pinned, as before pinning existed — while the
/// capability view keeps every row nvidia-smi reported, which is what
/// Package 1's availability gate did before this module existed (it ignored
/// `CUDA_VISIBLE_DEVICES` outright). Blanking both would silently un-gate
/// every capability-floored model on hosts that merely set an index
/// restriction. A UUID-form restriction that *resolves* does narrow both:
/// there we know exactly which boards are hidden, and a hidden board must
/// not unlock a gated model it will never run. One that resolves to nothing
/// is the unmappable case again rather than "no boards" — a `MIG-…` pin is
/// legitimate and never appears among these rows, so its physical board's
/// capability is still the right answer for gating.
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
            boards: Some(gpus.into()),
            backend: MemoryBackend::NvidiaSmi,
        },
    }
}

/// The capabilities of the boards that reported one. An all-capless (or
/// empty) row set yields an empty vec, which `HostComputeCaps::from_caps`
/// turns into "unknown" — a host with nothing readable cannot filter. Kept
/// free of `HostComputeCaps` construction so `build` pays (and logs) exactly
/// one capability view per call.
fn caps_of(gpus: &[GpuInfo]) -> Vec<(u32, u32)> {
    gpus.iter()
        .filter_map(|gpu| parse_compute_cap(gpu.compute_cap.as_deref()?))
        .collect()
}

/// Apply the operator's ambient `CUDA_VISIBLE_DEVICES` to the board list
/// nvidia-smi reported (it ignores the variable entirely).
///
/// - unset, or set to the empty string → no restriction (the empty value is
///   treated as "not configured" rather than "hide everything");
/// - all entries in UUID form (`GPU-…`/`MIG-…`, abbreviated forms included,
///   which CUDA accepts) → keep exactly those boards, in nvidia-smi order;
/// - **any** index-form entry → `None`, i.e. the inventory becomes unknown
///   (the capability view does not — see [`build`]). Ambient indices are in
///   *CUDA* order (affected by `CUDA_DEVICE_ORDER`), which we cannot map to
///   nvidia-smi rows, and pinning a worker to the wrong board is worse than
///   not pinning: with an unknown inventory workers simply inherit the
///   ambient restriction, which is what they did before pinning existed.
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
             mapped to boards) — workers inherit the restriction as-is"
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
            "CUDA_VISIBLE_DEVICES names no board nvidia-smi reports; leaving \
             the GPU inventory unknown"
        );
        return None;
    }
    tracing::info!(
        visible_devices = %visible.unwrap_or(""),
        boards = restricted.len(),
        "restricting the GPU inventory to the ambient CUDA_VISIBLE_DEVICES"
    );
    Some(restricted)
}

impl GpuInventory {
    /// Explicitly-unknown inventory: the state every non-CUDA host is in.
    /// Only tests construct one without probing; production goes through
    /// [`probe`] (same convention as `HostComputeCaps`).
    #[cfg(test)]
    pub fn unknown() -> Self {
        Self::default()
    }

    /// Construct a known CUDA inventory (tests only; the probe path builds
    /// it directly).
    #[cfg(test)]
    pub fn known(gpus: Vec<GpuInfo>) -> Self {
        Self {
            boards: (!gpus.is_empty()).then(|| gpus.into()),
            backend: MemoryBackend::NvidiaSmi,
        }
    }

    /// [`Self::known`]'s ROCm twin: index pins, amdgpu memory backend, no
    /// ambient restriction. The PCI root is the production one because the
    /// callers of this are pin/key/ledger tests that never read a file; the
    /// refresh tests build their inventory around a fixture tree instead.
    #[cfg(test)]
    pub fn known_rocm(gpus: Vec<GpuInfo>) -> Self {
        Self {
            boards: (!gpus.is_empty()).then(|| gpus.into()),
            backend: MemoryBackend::RocmSysfs {
                pci_devices: rocm::SysfsRoots::default().pci_devices,
                meminfo: rocm::SysfsRoots::default().meminfo,
                ambient_hip_restriction: false,
            },
        }
    }

    /// The boards, or `None` when the host is unknown.
    pub fn gpus(&self) -> Option<&[GpuInfo]> {
        self.boards.as_deref()
    }

    /// The live-memory interface for these boards, resolved once so the
    /// ledger's refresh never has to re-derive it (and can never ask
    /// nvidia-smi about an AMD board).
    ///
    /// The ROCm arm is **total or nothing**: every row must carry the PCI
    /// address the sysfs counters are keyed by. The probe guarantees that
    /// today (a row without a BDF makes the whole inventory unknown), so a
    /// missing one means that invariant broke — and quietly refreshing the
    /// rows that survived would leave the ledger pricing the rest off stale
    /// readings it believes are fresh. That is phantom headroom, so the
    /// whole refresh is withdrawn instead ([`MemoryQuery::Unavailable`]).
    pub(super) fn memory_query(&self) -> MemoryQuery {
        if matches!(self.backend, MemoryBackend::Mps) {
            // The RAM figure comes off the board itself: it is the same fact
            // that flags the board unified, so the refresh and the flag can
            // never disagree about which memory this board is made of.
            return match self.gpus().and_then(<[GpuInfo]>::first) {
                Some(board) => match board.unified_ram_mb {
                    Some(ram_mb) => MemoryQuery::Mps {
                        key: board.uuid.clone(),
                        ram_mb,
                    },
                    None => MemoryQuery::Unavailable,
                },
                // No board (off macOS, or sysctl said nothing): nothing to
                // refresh — and above all not nvidia-smi's business.
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
        let Some(boards) = self.gpus() else {
            // A ROCm host with no inventory: nothing to refresh, and above
            // all not nvidia-smi's business (see `MemoryQuery::Unavailable`).
            return MemoryQuery::Unavailable;
        };
        let mut keyed = Vec::with_capacity(boards.len());
        for gpu in boards {
            let Some(bdf) = gpu.bdf.clone() else {
                tracing::warn!(
                    uuid = %gpu.uuid,
                    name = %gpu.name,
                    "a ROCm inventory board has no PCI address; its live VRAM \
                     counters cannot be located, so this host gets no external \
                     memory refresh at all (a partial one would price the \
                     remaining boards off stale readings)"
                );
                return MemoryQuery::Unavailable;
            };
            keyed.push(rocm::BoardRef {
                key: gpu.uuid.clone(),
                bdf,
                unified: gpu.unified(),
            });
        }
        MemoryQuery::RocmSysfs {
            pci_devices: pci_devices.clone(),
            meminfo: meminfo.clone(),
            boards: keyed.into(),
        }
    }

    /// Default placement: the **highest-compute-capability** board, ties
    /// broken by the **largest VRAM total** and then the lowest enumeration
    /// index. Boards whose capability nvidia-smi did not report rank *last*
    /// rather than lowest — unknown is not slow, and a host where nothing
    /// reported a capability still needs a default.
    ///
    /// The VRAM tie-break exists for ROCm, where `compute_cap` is `None` on
    /// every board: without it a first-enumerated iGPU would out-rank the
    /// dGPU on any APU-plus-card host. On CUDA it only reorders equal-cap,
    /// unequal-VRAM hosts, where the bigger board is the strictly better
    /// default anyway. The figure compared is
    /// [`GpuInfo::placement_total_mb`], which on a unified board is the
    /// private carve-out rather than the carve+GTT budget — see there for
    /// why an APU's nominal capacity must not win this comparison by
    /// default.
    ///
    /// This is rough parity with what an unpinned worker got before pinning
    /// existed: torch's default device order is CUDA's `FASTEST_FIRST`, so
    /// "no pin, impls run on `devices[0]`" already meant the fastest visible
    /// board, not the lowest one on the bus. Ordering by nvidia-smi index
    /// instead would silently move every unpinned model onto the slow card
    /// of a mixed host. Headroom-based placement is a follow-up once the
    /// ledgers exist (docs/batch-calibration-design.md).
    ///
    /// One behavioural nuance remains: "usable" is a torch-build property
    /// (kernel coverage per compute capability) that this side cannot see,
    /// so on a host whose fastest board has no kernels the impl-side filter
    /// falls back to CPU instead of silently using another board.
    ///
    /// # ROCm: the default board's HIP device index
    ///
    /// Universal pinning holds on ROCm too, in HIP's vocabulary: the pin is
    /// the board's **row index**, which is its position in the openable
    /// KFD-node order `rocm.rs` enumerates — the order ROCr builds its agent
    /// list in, hence the order HIP indexes devices in (D1/D2). A `GPU-…`
    /// board key would match nothing in `HIP_VISIBLE_DEVICES`, hide every
    /// device, and drop the worker to CPU in silence; the key stays what the
    /// *ledger* is keyed by, and the index is only ever a pin.
    ///
    /// The row-order-is-HIP-order assumption is this design's one
    /// unverifiable, and D3's registration cross-check is its guard — a
    /// *warning*, not a refusal. Registration is order-independent and
    /// self-correcting: it resolves the board from what the worker itself
    /// reports (its PCI address), so a replica that came up on a different
    /// board than the pin named is still admitted, under the board it is
    /// physically on, which is where its memory has to be priced. What the
    /// mis-order costs is the *load reservation*: that is taken before the
    /// worker exists and stays keyed by the board the pin believed, so it
    /// protected the wrong card for the duration of the load. The divergence
    /// warning (`ledger::BoardLog::PinDiverged`) is the field diagnostic for
    /// exactly that, and it stays that way until a report from real
    /// multi-board ROCm hardware says the order needs fixing.
    ///
    /// # MPS: there is no pin
    ///
    /// Apple Silicon has one Metal device, no visibility variable that could
    /// select it and no eGPU shapes to disambiguate, so the answer is always
    /// `None` and the worker inherits its environment. The board *key* is
    /// unaffected — [`Self::resolve_board_key`] still resolves `GPU-MPS`, so
    /// load reservations, budgets and the ledger all work exactly as on a
    /// pinned host.
    pub fn default_pin(&self) -> Option<String> {
        if self.pins_are_absent() {
            return None;
        }
        let gpu = self.default_board()?;
        Some(if self.pins_are_indices() {
            gpu.index.to_string()
        } else {
            gpu.uuid.clone()
        })
    }

    /// Whether this host's pin vocabulary is HIP's (device indices) rather
    /// than CUDA's (board UUIDs). The distinction is the whole of D2: the
    /// same board is named by its key in the ledger and by its index in the
    /// worker's environment, and the two must never be swapped.
    ///
    /// This is the **single source** of that answer: it is what
    /// [`Self::default_pin`] and [`Self::resolve_pin`] choose their
    /// vocabulary by, and — through the same `Accelerator::Rocm` that set
    /// the backend — what [`pin_env_var`] chooses the variable by. A host
    /// whose inventory is unknown still answers truthfully, which is what
    /// keeps an ambient-restricted ROCm host from being handed CUDA's rules
    /// by default.
    fn pins_are_indices(&self) -> bool {
        matches!(self.backend, MemoryBackend::RocmSysfs { .. })
    }

    /// Whether this host has no pin vocabulary at all — MPS, where there is
    /// one device and nothing to name it with. Every pin request is dropped
    /// (a value written into either visibility variable could only *hide*
    /// the device), while board keys keep resolving as everywhere else.
    fn pins_are_absent(&self) -> bool {
        matches!(self.backend, MemoryBackend::Mps)
    }

    /// Whether the operator had a HIP-layer visibility restriction in force
    /// when this inventory was probed (`rocm::ambient_hip_restriction`);
    /// always false on CUDA, where the ambient value is *composed with*
    /// rather than fought over.
    fn ambient_hip_restriction(&self) -> bool {
        matches!(
            self.backend,
            MemoryBackend::RocmSysfs {
                ambient_hip_restriction: true,
                ..
            }
        )
    }

    /// The default board's **model name** — the calibration keyspace, which
    /// is per silicon rather than per instance. `None` on an unknown host,
    /// where the `/metadata` calibration overlay is omitted entirely rather
    /// than answering for a board it cannot name.
    pub fn default_board_name(&self) -> Option<String> {
        self.default_board().map(|gpu| gpu.name.clone())
    }

    fn default_board(&self) -> Option<&GpuInfo> {
        self.boards.as_deref()?.iter().min_by_key(|gpu| {
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
    /// On CUDA (and on every non-ROCm host with no inventory), that variable
    /// is `CUDA_VISIBLE_DEVICES` and the vocabulary is board UUIDs:
    ///
    /// - unknown inventory → the request verbatim (`None` stays `None`):
    ///   exactly today's behaviour, which is what CPU/MPS hosts and CUDA
    ///   hosts with an ambient restriction need. ROCm's unknown-inventory
    ///   arm is *not* this one — see the last section;
    /// - no request → the default board's UUID (universal pinning);
    /// - a UUID request (`GPU-…`/`MIG-…`) naming a board this host reports,
    ///   exactly (case-insensitively) or as an unambiguous abbreviation →
    ///   **the inventory's own spelling of that board's UUID**. CUDA accepts
    ///   every spelling, but pin strings are compared byte-wise elsewhere
    ///   (`prewarm.rs`'s pool claim) and [`Self::resolve_board_key`] already
    ///   canonicalises, so the two have to agree about board equality;
    /// - a UUID request that is ambiguous or names no board we can see (a
    ///   `MIG-…` instance, another machine's UUID) → verbatim; resolving it
    ///   is CUDA's business;
    /// - an index request → that board's UUID, so the ledger key is stable
    ///   even though the index is not;
    /// - anything else (an index we cannot see, a comma-separated list, a
    ///   templated leftover) → verbatim with a warning. Passing it through
    ///   preserves whatever the operator meant; guessing would not.
    ///
    /// # MPS: there is no pin, in any vocabulary
    ///
    /// Apple Silicon has exactly one Metal device and no visibility variable
    /// that names it, so every request — including one that names the board
    /// key — resolves to `None` and the worker inherits its environment. The
    /// board *key* still resolves ([`Self::resolve_board_key`]), so load
    /// reservations, per-board budget overrides and the ledger work exactly
    /// as they do on a pinned host; the pin is the only thing missing, and
    /// there is nothing for it to say.
    ///
    /// # ROCm: the vocabulary is HIP device indices
    ///
    /// A known ROCm inventory resolves into `HIP_VISIBLE_DEVICES`, which
    /// accepts **indices only** — the row index of D1's openable-KFD-node
    /// enumeration (see [`Self::default_pin`]):
    ///
    /// - no request → the default board's index (universal pinning, as on
    ///   CUDA);
    /// - a **board key** (`GPU-<16hex>` or `GPU-BDF-…`, the row's `uuid`,
    ///   matched case-insensitively) → that row's index. This is the whole
    ///   point of D2: an operator writes the same stable identity in
    ///   `devices` that the ledger and the per-board VRAM overrides use, and
    ///   the translation to HIP's index happens here, once. The match is
    ///   **exact**, unlike CUDA's abbreviated-UUID prefixes: CUDA's
    ///   abbreviation is a runtime feature of the string we hand it, whereas
    ///   these keys never reach HIP at all, and a prefix could name two
    ///   `GPU-BDF-…` boards on the same bus and silently pin the wrong one;
    /// - a **numeric** request → that index, even when it names no row we
    ///   can see, with a warning in that case. HIP takes indices, so the
    ///   operator's intent survives — the mirror of the CUDA arm's
    ///   unresolvable-index passthrough;
    /// - an **all-numeric comma list** → that list, with a warning. Multiple
    ///   visible devices per worker is not something this layer arranges,
    ///   but HIP accepts the form and it is the operator's business;
    ///
    /// - anything **non-numeric that matches no board key** (a `GPU-…`
    ///   leftover from a CUDA config, an unexpanded template) → **no pin at
    ///   all**, with a warning. Written into a HIP visibility variable such
    ///   a string matches no device, hides the entire board set, and drops
    ///   the worker to the CPU without a word; no pin is strictly better,
    ///   and the warning says so.
    ///
    /// Both numeric forms come back **canonicalised** (`"00"` → `"0"`,
    /// `" 1 , 2 "` → `"1,2"`), not as the operator spelled them: pins are
    /// compared as strings elsewhere — `prewarm.rs` hands a parked worker to
    /// a replica only when the two pin strings are equal — so two spellings
    /// of one device have to converge here or the pool silently stops
    /// matching (see [`canonical_index_list`]).
    ///
    /// # ROCm with an unknown inventory: the vocabulary still holds
    ///
    /// A ROCm host that found no boards (ambient restriction, probe failure,
    /// non-Linux) has nothing to translate against, but it has not stopped
    /// being a HIP host, so the CUDA passthrough is *not* what it gets:
    ///
    /// - a HIP-legal request (an index or an index list) → canonicalised and
    ///   passed through, since HIP can read it without an inventory;
    /// - anything else → dropped with a warning, for the same reason a known
    ///   ROCm host drops it — the harm (every board hidden, silent CPU) does
    ///   not depend on whether we could enumerate the boards;
    /// - **anything at all, including an index, when the operator's own
    ///   ambient restriction is at HIP's layer** → dropped with a warning.
    ///   Our value would overwrite `HIP_VISIBLE_DEVICES` outright, or take
    ///   precedence over their `CUDA_VISIBLE_DEVICES`, widening a set they
    ///   deliberately narrowed. An ambient `ROCR_VISIBLE_DEVICES` is a
    ///   different matter and does not trigger this: HIP indexes into the
    ///   ROCr-filtered set, so the pin composes with the restriction instead
    ///   of escaping it. That check is the **first** thing this function
    ///   does, before the inventory is even looked at: it is a fact about
    ///   the gateway's environment, not about which boards we found.
    pub fn resolve_pin(&self, requested: Option<&str>) -> Option<String> {
        // MPS first, and before anything else for the same positional reason
        // the HIP guard below is where it is: it is a fact about the host,
        // not about which boards were found. One device, no variable that
        // selects it — so a registry `devices` entry has nothing to resolve
        // to, and writing it into either visibility variable could only hide
        // the device from the worker.
        if self.pins_are_absent() {
            if let Some(requested) = requested.map(str::trim).filter(|pin| !pin.is_empty()) {
                tracing::warn!(
                    pin = %requested,
                    "ignoring this device pin: an Apple Silicon host has exactly \
                     one Metal device and no visibility variable that selects \
                     it, so there is nothing to pin to — the model runs on that \
                     device either way, and is priced against it"
                );
            }
            return None;
        }
        // Then, unconditionally: the operator's own HIP-layer restriction
        // outranks everything below, including the arms that would otherwise
        // have been allowed to write an index. Checked here rather than in
        // the uninventoried arm alone so the guard cannot be bypassed if a
        // future change ever lets a HIP-restricted host carry a non-empty
        // inventory. Today it cannot — the flag is only ever set alongside a
        // blanked board list — so this changes no behaviour.
        if self.ambient_hip_restriction() {
            if let Some(requested) = requested {
                tracing::warn!(
                    pin = %requested.trim(),
                    "ignoring this device pin: a HIP-layer visibility restriction \
                     (HIP_VISIBLE_DEVICES / CUDA_VISIBLE_DEVICES / \
                     GPU_DEVICE_ORDINAL) is already set in this gateway's own \
                     environment, and writing our own would override it and hand \
                     the worker boards the operator deliberately hid — the \
                     operator's restriction wins, and the worker inherits it \
                     as-is"
                );
            }
            return None;
        }
        let Some(gpus) = self.boards.as_deref() else {
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
            // Canonicalised against the inventory when it names a board we
            // can see. CUDA accepts every spelling here — a full UUID in
            // either case, and any unambiguous abbreviation of one — but the
            // rest of the system compares pin strings **byte-wise**:
            // `prewarm.rs` claims a parked worker only when its recorded pin
            // equals the replica's resolved one, and `resolve_board_key`
            // (which the ledger and telemetry key by) already canonicalises.
            // Leaving the operator's spelling through meant the pool and the
            // ledger disagreed about whether two replicas were on one board.
            // A full UUID is legal everywhere an abbreviation was, so this
            // never narrows what CUDA will accept.
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
            if let Some(first) = matches.next() {
                if matches.next().is_none() {
                    return Some(first.uuid.clone());
                }
            }
            // Ambiguous, or a board this host cannot see (a `MIG-…`
            // instance, a UUID from another machine): verbatim, as before —
            // resolving it is CUDA's business, not ours.
            return Some(trimmed.to_owned());
        }
        if let Ok(index) = trimmed.parse::<u32>() {
            if let Some(gpu) = gpus.iter().find(|gpu| gpu.index == index) {
                return Some(gpu.uuid.clone());
            }
        }
        tracing::warn!(
            pin = %requested,
            "device pin does not name a visible GPU; passing it to \
             CUDA_VISIBLE_DEVICES unchanged"
        );
        Some(requested.to_owned())
    }

    /// Resolve the same registry `devices` entry [`Self::resolve_pin`] takes
    /// into the **ledger board key** — the board's `uuid`, whatever
    /// vocabulary the pin itself is written in.
    ///
    /// The two are a pair and are resolved together at every call site that
    /// needs both: the pin goes into the worker's environment, the key goes
    /// to the ledger (`VramLedger::reserve_load`, whose board map is keyed by
    /// `uuid`). Before this existed the resolved *pin* was handed to the
    /// ledger, which silently missed for every form where pin ≠ key: every
    /// ROCm pin (an index), and on CUDA an abbreviated-UUID or unresolvable
    /// pin. The miss costs the load reservation — the in-flight load is not
    /// charged until the worker registers — so a second model loading
    /// concurrently could be granted a window against memory the first load
    /// was about to take (docs/rocm-batch-calibration-parity.md, D2's known
    /// gap, closed by D3).
    ///
    /// The arms, in order, on **both** backends:
    ///
    /// - unknown inventory → `None`. There is no board map to key into
    ///   either, so this is not a lost reservation, it is a host that has no
    ///   ledger at all;
    /// - no request → the default board's key (universal pinning puts the
    ///   worker there);
    /// - a board key, matched **case-insensitively but in full** → that
    ///   board's key, in the inventory's own spelling (the ledger is keyed by
    ///   that exact string);
    /// - an index naming a row → that row's key;
    /// - **CUDA only**, a `GPU-`/`MIG-` request that is an unambiguous
    ///   *prefix* of exactly one board's UUID → that board's key. This is the
    ///   abbreviation CUDA itself resolves (and which `resolve_pin` passes
    ///   through verbatim for exactly that reason), so the ledger has to
    ///   resolve it too or an operator writing `GPU-1a2b` gets no
    ///   reservation. Two boards sharing the prefix answer `None`: a
    ///   reservation on the wrong board is worse than none. The degenerate
    ///   prefix `GPU-` is not special-cased: on a multi-board host it is
    ///   ambiguous and answers `None` like any other shared prefix, and on a
    ///   single-board host it resolves to that board — which is exactly what
    ///   CUDA itself does with the same string, so the reservation and the
    ///   pin agree. The prefix arm is deliberately absent on ROCm, mirroring
    ///   `resolve_pin`'s exactness rule there — a prefix could name two
    ///   `GPU-BDF-…` boards on one bus, and these keys never reach HIP, so
    ///   there is no runtime abbreviation to be compatible with;
    /// - anything else (an index we cannot see, a device *list*, a template)
    ///   → `None`. A worker with several visible boards has no single board
    ///   to charge, and an invisible index names no ledger row.
    ///
    /// No warning is logged on the `None` arms: `resolve_pin` has already
    /// warned about every one of these strings, and a second line per replica
    /// saying the same thing twice is noise.
    pub fn resolve_board_key(&self, requested: Option<&str>) -> Option<String> {
        let gpus = self.boards.as_deref()?;
        let Some(requested) = requested else {
            return self.default_board().map(|gpu| gpu.uuid.clone());
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

    /// The **PCI address** of the board a registry `devices` entry names,
    /// when that board is a unified one whose worker needs the GTT-inclusive
    /// arithmetic (DP-5); `None` otherwise.
    ///
    /// Resolved from the same request and through the same resolver as the
    /// pin and the board key, so the three can never disagree about which
    /// board a replica is *meant* to land on — and the address is what lets
    /// the worker check whether it actually did ([`UNIFIED_GPU_ENV_VAR`]).
    ///
    /// `None` for anything unresolvable, which is the safe direction: an
    /// unrecognised pin means we do not know what this replica will land on,
    /// and the discrete arithmetic is the reading that never over-counts.
    /// `None` on MPS too, and on a unified board with no address — there
    /// would be nothing for the worker to check the claim against.
    pub fn unified_pin_bdf(&self, requested: Option<&str>) -> Option<String> {
        if !self.pins_are_indices() {
            return None;
        }
        let key = self.resolve_board_key(requested)?;
        self.gpus()?
            .iter()
            .find(|gpu| gpu.uuid == key && gpu.unified())
            .and_then(|gpu| gpu.bdf.clone())
    }

    /// The ROCm arm of [`Self::resolve_pin`] (see its docs for the full
    /// vocabulary and the reasoning). Split out because HIP's rules diverge
    /// from CUDA's at every branch: the board key translates instead of
    /// passing through, and an unresolvable non-numeric string is dropped
    /// instead of passed through.
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
                    boards = gpus.len(),
                    "device pin names no board in this host's HIP enumeration; \
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
                 one visible board is not something the per-GPU ledger can \
                 price"
            );
            return Some(list);
        }
        tracing::warn!(
            pin = %trimmed,
            "device pin is neither a HIP device index nor a board key this \
             host reports; dropping it rather than writing it to \
             HIP_VISIBLE_DEVICES, where it would match no device, hide every \
             board and silently run the worker on the CPU"
        );
        None
    }

    /// The ROCm arm of [`Self::resolve_pin`] for a host with **no boards**
    /// (see its docs). There is nothing to translate a board key against and
    /// no index to check for range, so all that is left is HIP's grammar.
    /// The operator's own HIP-layer restriction was already handled by the
    /// guard at the top of [`Self::resolve_pin`], which no arm reaches past.
    fn resolve_hip_pin_uninventoried(&self, requested: Option<&str>) -> Option<String> {
        // No request is no pin here regardless: with no boards there is no
        // default board to pin to either.
        let trimmed = requested?.trim();
        if let Some(pin) = canonical_hip_pin(trimmed) {
            return Some(pin);
        }
        tracing::warn!(
            pin = %trimmed,
            "this ROCm host reports no boards to resolve device pins \
             against, and this pin is not a HIP device index either; \
             dropping it rather than writing it to HIP_VISIBLE_DEVICES, \
             where it would match no device, hide every board and silently \
             run the worker on the CPU"
        );
        None
    }
}

/// The HIP-legal pin forms, canonicalised: one device index, or a list of
/// them. `None` for anything HIP cannot read as an index — which on ROCm
/// means "write no pin at all", never "write it and hope".
fn canonical_hip_pin(value: &str) -> Option<String> {
    if let Ok(index) = value.parse::<u32>() {
        return Some(index.to_string());
    }
    canonical_index_list(value)
}

/// A HIP-shaped multi-device pin: at least one entry, every entry a device
/// index. Trailing/empty entries are ignored (`"0,"` is the operator asking
/// for device 0), which is how HIP's own parser reads the list.
///
/// Returns the **canonical** rendering — entries re-parsed and re-joined
/// with `,` — rather than the operator's spelling. Every index this module
/// emits has to be byte-comparable with every other one: `prewarm.rs` claims
/// a parked worker only when its recorded pin string *equals* the replica's
/// resolved pin, so `" 0 "` and `"00"` reaching that comparison as
/// themselves would silently defeat pooling against a `default_pin()` that
/// renders `0`.
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

/// `GPU-…` (and MIG's `MIG-…`) are the forms CUDA accepts in
/// `CUDA_VISIBLE_DEVICES` verbatim.
fn is_uuid_pin(value: &str) -> bool {
    let upper = value.to_ascii_uppercase();
    upper.starts_with("GPU-") || upper.starts_with("MIG-")
}

/// One GPU per line, `index, uuid, name, total, compute_cap`
/// (`--format=csv,noheader,nounits`). Any line whose **identity** columns
/// (index, uuid, name, total) do not parse — or whose column count is not
/// five — makes the whole probe unknown: a partial picture must not drive
/// pinning or filter models, exactly as before the two probes were merged.
///
/// The capability column alone is per-row optional (`[N/A]` on vGPU slices
/// and some datacenter SKUs). Dropping such a host's whole inventory would
/// cost it pinning and the ledger over a field only default placement and
/// model gating use, so the row keeps its identity with no capability.
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

/// One row of the inventory query, or `None` if any identity column does
/// not parse. All `None`s funnel through `parse_inventory`'s single WARN,
/// which names the row.
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
            "nvidia-smi did not report this board's compute capability; it \
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
        // nvidia-smi rows need neither: the UUID is both the identity
        // and the pin form, and there is no gfx target.
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

    /// A ROCm-shaped board: no compute capability, a PCI address, a gfx
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
            boards: Some(gpus.into()),
            backend: MemoryBackend::RocmSysfs {
                pci_devices,
                meminfo,
                // A knowable inventory is proof there was no ambient
                // restriction of any layer: the probe blanks it otherwise.
                ambient_hip_restriction: false,
            },
        }
    }

    /// The MPS host: one synthetic unified board, built by the same
    /// `mps::board` the probe uses so the fixture cannot drift from it.
    fn mps_inventory(ram_gib: u64) -> GpuInventory {
        let facts = super::mps::HostFacts {
            chip: "Apple M3 Max".into(),
            ram_bytes: ram_gib * 1024 * 1024 * 1024,
        };
        GpuInventory {
            boards: Some(vec![super::mps::board(&facts)].into()),
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

    /// A ROCm host with **no** boards — the ambient-restricted, probe-failed
    /// and non-Linux shape. It is still a ROCm host: the backend, and with it
    /// the pin vocabulary and the memory interface, is what `probe_rocm`
    /// leaves behind on every one of those paths.
    fn uninventoried_rocm(ambient_hip_restriction: bool) -> GpuInventory {
        GpuInventory {
            boards: None,
            backend: MemoryBackend::RocmSysfs {
                pci_devices: PathBuf::from("/sys/bus/pci/devices"),
                meminfo: PathBuf::from("/proc/meminfo"),
                ambient_hip_restriction,
            },
        }
    }

    const TWO_BOARDS: &str = "0, GPU-1a2b, NVIDIA GeForce RTX 5090, 32607, 12.0\n\
                              1, GPU-3c4d, NVIDIA RTX A2000, 6138, 8.6\n";

    /// The ledger's staleness refresh reads one coherent snapshot; a single
    /// unparseable row makes the whole reading unknown rather than pricing
    /// some board's external usage as zero.
    #[test]
    fn parses_a_memory_snapshot() {
        let boards = parse_memory(
            "GPU-1a2b, 32607, 21000\nGPU-3c4d, 6138, 512\n",
        )
        .expect("parses");
        assert_eq!(
            boards,
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
        assert!(parse_memory("GPU-1a2b, 32607\n").is_none(), "missing column");
        assert!(
            parse_memory("0, 32607, 21000\n").is_none(),
            "a non-UUID identity cannot key a ledger"
        );
    }

    #[test]
    fn parses_nvidia_smi_inventory() {
        let gpus = parse_inventory(TWO_BOARDS).expect("parses");
        assert_eq!(gpus.len(), 2);
        assert_eq!(gpus[0].uuid, "GPU-1a2b");
        assert_eq!(gpus[0].name, "NVIDIA GeForce RTX 5090");
        assert_eq!(gpus[0].total_mb, 32607);
        assert_eq!(gpus[0].compute_cap.as_deref(), Some("12.0"));
        assert_eq!(gpus[1].index, 1);
    }

    /// The merged probe feeds both views from the same rows, so an index in
    /// the inventory and a capability in the filter always describe the same
    /// physical board.
    #[test]
    fn one_probe_builds_both_views() {
        let host = build(Some(TWO_BOARDS), None);
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
        // exactly one board, which is what "ANY device" means.
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
        assert!(parse_inventory("0, GPU-1a2b, RTX, 32607, 8.6\nN/A, N/A, N/A, N/A, N/A\n").is_none());
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
    fn an_unreported_capability_keeps_the_board_identity() {
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
        // Capabilities come from the boards that reported one.
        assert_eq!(host.caps.meets_floor(8.0), Some(true));
        assert_eq!(host.caps.meets_floor(9.0), Some(false));
        // Unknown is not slow: the capless board is unranked, not preferred
        // and not treated as compute capability 0.
        assert_eq!(
            host.inventory.default_pin(),
            Some("GPU-3c4d".to_string()),
            "a board of unknown speed must not win default placement"
        );

        // No board reports one: identities stay, the capability view is
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
        assert_eq!(capless.inventory.default_pin(), Some("GPU-1a2b".to_string()));
    }

    /// nvidia-smi ignores `CUDA_VISIBLE_DEVICES`, so an operator's ambient
    /// restriction has to be applied here — otherwise pin resolution would
    /// hand a worker a board the operator deliberately hid, and the worker
    /// (which *does* honour the variable) would fail or land elsewhere.
    #[test]
    fn ambient_visible_devices_restricts_the_inventory() {
        // UUID form: keep exactly the named boards, in nvidia-smi order.
        let host = build(Some(TWO_BOARDS), Some("GPU-3c4d"));
        let gpus = host.inventory.gpus().expect("known");
        assert_eq!(gpus.len(), 1);
        assert_eq!(gpus[0].uuid, "GPU-3c4d");
        assert_eq!(
            host.inventory.default_pin(),
            Some("GPU-3c4d".to_string()),
            "placement can only choose among visible boards"
        );
        assert_eq!(
            host.caps.meets_floor(12.0),
            Some(false),
            "the hidden board's capability must not filter models either"
        );

        // Abbreviated UUIDs are legal for CUDA, so they are honoured here.
        let abbreviated = build(Some(TWO_BOARDS), Some("GPU-1a"));
        assert_eq!(
            abbreviated.inventory.default_pin(),
            Some("GPU-1a2b".to_string())
        );

        // Unset and empty both mean "no restriction".
        assert_eq!(build(Some(TWO_BOARDS), Some("")).inventory.gpus().map(<[GpuInfo]>::len), Some(2));
        assert_eq!(build(Some(TWO_BOARDS), Some(" , ")).inventory.gpus().map(<[GpuInfo]>::len), Some(2));
    }

    /// An unmappable ambient restriction blanks the **inventory only**. The
    /// capability view keeps every board nvidia-smi reported, which is what
    /// Package 1's availability gate saw before this module existed (it never
    /// looked at `CUDA_VISIBLE_DEVICES`); taking it down with the inventory
    /// would silently un-gate every capability-floored model on any host that
    /// merely restricts by index.
    #[test]
    fn an_unmappable_restriction_blanks_only_the_inventory() {
        // Index form: unmappable (CUDA order != nvidia-smi order), so the
        // inventory goes unknown and workers inherit the restriction as-is.
        let indexed = build(Some(TWO_BOARDS), Some("1"));
        assert!(indexed.inventory.gpus().is_none());
        assert_eq!(indexed.inventory.resolve_pin(None), None, "no pinning");
        assert_eq!(
            indexed.caps.meets_floor(12.0),
            Some(true),
            "model availability is still capability-filtered"
        );
        assert_eq!(indexed.caps.meets_floor(12.1), Some(false));
        // Mixed forms are index-form as far as safety goes.
        let mixed = build(Some(TWO_BOARDS), Some("GPU-1a2b,1"));
        assert!(mixed.inventory.gpus().is_none());
        assert_eq!(mixed.caps.meets_floor(12.0), Some(true));
        // A UUID restriction naming nothing we listed is unknown, not empty:
        // a legitimate `MIG-…` pin never appears among these rows, so this is
        // "cannot map", not "no boards", and the physical capabilities stand.
        let nothing = build(Some(TWO_BOARDS), Some("MIG-abcd"));
        assert!(nothing.inventory.gpus().is_none());
        assert_eq!(nothing.caps.meets_floor(12.0), Some(true));
    }

    /// Default placement is the fastest board (parity with CUDA's
    /// FASTEST_FIRST ordering, which is what unpinned workers saw), not the
    /// lowest index.
    #[test]
    fn default_pin_is_the_fastest_board() {
        let mixed = GpuInventory::known(vec![gpu(0, "GPU-slow", "8.6"), gpu(1, "GPU-fast", "12.0")]);
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
        // Capability still outranks VRAM: a big slow board does not win.
        let slow_and_big = GpuInventory::known(vec![
            sized_gpu(0, "GPU-slow-big", "8.6", 49152),
            sized_gpu(1, "GPU-fast-small", "12.0", 8192),
        ]);
        assert_eq!(slow_and_big.default_pin(), Some("GPU-fast-small".to_string()));
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
    /// never end up asking nvidia-smi about an AMD board.
    #[test]
    fn the_memory_query_follows_the_inventory_backend() {
        assert_eq!(inventory().memory_query().free_source(), "nvidia-smi");
        assert_eq!(
            GpuInventory::unknown().memory_query().free_source(),
            "nvidia-smi",
            "an unknown host has no boards to refresh either way"
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
            MemoryQuery::RocmSysfs { boards, .. } => assert_eq!(
                &*boards,
                &[rocm::BoardRef {
                    key: "GPU-BDF-0000:03:00.0".to_owned(),
                    bdf: "0000:03:00.0".to_owned(),
                    unified: false,
                }]
            ),
            other => panic!("expected the sysfs query, got {other:?}"),
        }
        // And a ROCm host with no boards at all (ambient restriction, probe
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

    /// The refresh carries each board's unified flag, because the extra
    /// files (GTT, `MemAvailable`) are read for those rows and only those:
    /// `mem_info_gtt_*` exists for discrete boards too, so its presence
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
            MemoryQuery::RocmSysfs {
                boards, meminfo, ..
            } => {
                assert_eq!(
                    boards.iter().map(|b| b.unified).collect::<Vec<_>>(),
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
    /// on the slower board. The comparison is by carve-out, so the dGPU wins
    /// unless the operator gave the iGPU that memory outright in the BIOS.
    #[test]
    fn default_placement_prefers_a_dgpu_over_an_apu_of_larger_budget() {
        let dgpu_wins = GpuInventory::known_rocm(vec![
            amd_apu(0, "0000:03:00.0", 512, 64 * 1024, 128 * 1024),
            amd_gpu(1, "0000:0c:00.0", 24_576),
        ]);
        assert_eq!(
            dgpu_wins.default_board_name().as_deref(),
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
            apu_wins.default_board_name().as_deref(),
            Some("AMD gfx1151 APU (128 GB)")
        );
        assert_eq!(apu_wins.default_pin().as_deref(), Some("0"));
        // …and the carve-out alone is not the whole rank either. A 2 GB
        // display card next to a 128 GB Strix Halo left at its BIOS default
        // is not a board anyone wants a model on: an eighth of the unified
        // budget (a deliberately pessimistic reading of memory shared with
        // the whole OS) is what the APU is credited with, which beats the
        // token card and still loses to any real one.
        let token_card = GpuInventory::known_rocm(vec![
            amd_apu(0, "0000:03:00.0", 512, 64 * 1024, 128 * 1024),
            amd_gpu(1, "0000:0c:00.0", 2048),
        ]);
        assert_eq!(
            token_card.default_board_name().as_deref(),
            Some("AMD gfx1151 APU (128 GB)")
        );
        assert_eq!(token_card.default_pin().as_deref(), Some("0"));
        // Nothing about the ranking changed for discrete boards.
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

    /// DP-5's resolver: the **address** of the board a registry entry names,
    /// when that board is unified — answered from the same request the pin
    /// and the board key are, and an address rather than a flag so the
    /// worker can check the claim against the board it actually came up on.
    #[test]
    fn a_unified_pin_resolves_to_its_boards_address() {
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
            "an unpinned replica lands on the default board, which is the dGPU"
        );
        // A pin naming nothing this host enumerated: the discrete
        // arithmetic is the reading that never over-counts, so unknown
        // resolves to nothing rather than to a claim.
        assert_eq!(host.unified_pin_bdf(Some("7")), None);
        assert_eq!(host.unified_pin_bdf(Some("GPU-1a2b")), None);
        // An APU-only host: the default board *is* the unified one.
        let apu_only =
            GpuInventory::known_rocm(vec![amd_apu(0, "0000:03:00.0", 512, 64 * 1024, 128 * 1024)]);
        assert_eq!(apu_only.unified_pin_bdf(None), apu);
        // Never on the other backends: a CUDA board is not unified, and an
        // MPS worker's tiers are unified by construction and read no flag.
        assert_eq!(inventory().unified_pin_bdf(None), None);
        assert_eq!(mps_inventory(128).unified_pin_bdf(None), None);
        assert_eq!(uninventoried_rocm(false).unified_pin_bdf(Some("0")), None);
    }

    /// The refresh is total or withdrawn. A row without a PCI address
    /// cannot be located in sysfs; refreshing the others would leave the
    /// ledger pricing this one off a stale reading it believes is fresh.
    #[test]
    fn a_rocm_board_without_a_pci_address_withdraws_the_whole_refresh() {
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
            let board = super::rocm::pci_device_dir(&pci, bdf);
            std::fs::create_dir_all(&board).unwrap();
            std::fs::write(board.join("mem_info_vram_total"), format!("{total}\n")).unwrap();
            std::fs::write(board.join("mem_info_vram_used"), format!("{used}\n")).unwrap();
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
        // All-or-nothing: one board whose counters are gone makes the whole
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
    /// and every other accelerator keeps the nvidia-smi path whatever this
    /// host happens to have installed — the CUDA arm can never produce a
    /// sysfs-backed inventory.
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
                 NVIDIA binary about AMD boards"
            );
        }
        for accelerator in [Accelerator::Cuda, Accelerator::Cpu] {
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

    /// The MPS inventory: one constant-keyed unified board, no pin in any
    /// vocabulary, and a board key that still resolves — the pin and the key
    /// are separate answers, and only the pin is missing here.
    #[test]
    fn an_mps_inventory_has_a_board_key_but_never_a_pin() {
        let host = mps_inventory(128);
        let boards = host.gpus().expect("known");
        assert_eq!(boards.len(), 1);
        assert_eq!(boards[0].uuid, "GPU-MPS");
        assert!(boards[0].unified());
        assert_eq!(
            host.default_board_name().as_deref(),
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
        // against this board like any other.
        assert_eq!(
            host.resolve_board_key(None),
            Some("GPU-MPS".to_string()),
            "universal placement still names the board"
        );
        assert_eq!(
            host.resolve_board_key(Some("gpu-mps")),
            Some("GPU-MPS".to_string())
        );
        assert_eq!(host.resolve_board_key(Some("GPU-1a2b")), None);
    }

    /// The refresh follows the backend here too: an MPS host reads RAM
    /// statistics, never nvidia-smi, and an MPS host with no board (off
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
            boards: None,
            backend: MemoryBackend::Mps,
        };
        assert!(
            matches!(unprobed.memory_query(), MemoryQuery::Unavailable),
            "an MPS host with no board must not fall back to nvidia-smi"
        );
        assert_eq!(unprobed.resolve_pin(Some("0")), None);
        assert_eq!(unprobed.default_pin(), None);
        assert_eq!(unprobed.resolve_board_key(None), None);
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
            let board = host.inventory.gpus().expect("Apple Silicon")[0].clone();
            assert_eq!(board.uuid, "GPU-MPS");
            assert!(board.unified() && board.total_mb > 0);
        }
        for accelerator in [Accelerator::Cuda, Accelerator::Cpu] {
            assert!(
                !matches!(probe(accelerator).inventory.backend, MemoryBackend::Mps),
                "{accelerator:?} must keep the nvidia-smi path"
            );
        }
    }

    #[test]
    fn unpinned_replica_resolves_to_the_default_board() {
        let inventory = inventory();
        assert_eq!(
            inventory.resolve_pin(None),
            Some("GPU-1111".to_string()),
            "universal pinning: no pin means the default board's UUID"
        );
    }

    /// A known ROCm inventory speaks HIP's vocabulary: every pin it emits is
    /// a device index, never a `GPU-…` string. Written into
    /// `HIP_VISIBLE_DEVICES` a board key matches nothing, hides every
    /// device, and drops the worker to CPU in silence, so it must never get
    /// there — the key stays the *ledger's* identity and the index is the
    /// pin (D2).
    #[test]
    fn a_rocm_inventory_never_emits_a_board_key_as_a_pin() {
        let host = rocm_inventory(
            PathBuf::from("/sys/bus/pci/devices"),
            vec![
                amd_gpu(0, "0000:03:00.0", 24576),
                amd_gpu(1, "0000:0c:00.0", 24576),
            ],
        );
        // Universal pinning, in indices: the fastest board is the tie-break
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
        // The board name is still available: the /metadata calibration
        // overlay needs it, and it never reaches a worker's environment.
        assert_eq!(
            host.default_board_name().as_deref(),
            Some("AMD gfx1100 (24 GB)")
        );
    }

    /// Default placement on ROCm ranks by VRAM (every board's `compute_cap`
    /// is `None`) and answers in HIP's vocabulary — the row's position in
    /// the openable KFD-node order, which is the HIP device index.
    #[test]
    fn the_rocm_default_pin_is_the_default_boards_index() {
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

    /// The board key an operator writes in `devices` (the same string the
    /// ledger and the per-board VRAM overrides use) is translated to the
    /// row's HIP index here, once. Both key forms, either case, with
    /// whatever whitespace the TOML carried.
    #[test]
    fn rocm_board_keys_resolve_to_their_row_index() {
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
        // prefix could name two boards on the same bus.
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
            "an index beyond this host's boards is still the operator's call"
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
                "{spelling:?} names the default board and must render \
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
    /// string that matches no board key. In `HIP_VISIBLE_DEVICES` it would
    /// match no device, hide the whole board set and drop the worker to the
    /// CPU — strictly worse than the no-pin behaviour dropping it preserves.
    #[test]
    fn rocm_drops_a_pin_hip_could_not_read() {
        let host = rocm_inventory(
            PathBuf::from("/sys/bus/pci/devices"),
            vec![amd_gpu(0, "0000:03:00.0", 24576)],
        );
        // A CUDA config's board UUID, carried over to an AMD host.
        assert_eq!(host.resolve_pin(Some("GPU-1a2b")), None);
        // A board key for a board this host does not have.
        assert_eq!(host.resolve_pin(Some("GPU-BDF-0000:ff:00.0")), None);
        // An unexpanded template and a stray word.
        assert_eq!(host.resolve_pin(Some("${DEVICE}")), None);
        assert_eq!(host.resolve_pin(Some("cpu")), None);
        // A mixed list is not an index list.
        assert_eq!(host.resolve_pin(Some("0,GPU-BDF-0000:03:00.0")), None);
        // The empty string, which a templated config expands to more often
        // than anything else here. It is *not* "no pin": no pin means the
        // default board, and silently promoting an expansion failure to
        // universal pinning would put a worker on a board nobody named.
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

    /// A ROCm host that found no boards (ambient restriction, probe failure,
    /// non-Linux) does not thereby forget it is a ROCm host. It has nothing
    /// to translate a board key against, but HIP's grammar still applies —
    /// so an index passes and a `GPU-…` string, which would hide every board
    /// and drop the worker to the CPU, still does not.
    #[test]
    fn an_unknown_rocm_inventory_keeps_hips_vocabulary() {
        let host = uninventoried_rocm(false);
        assert!(host.gpus().is_none());
        // HIP-legal, so it survives — canonicalised, exactly as it would be
        // on a host whose boards we could see.
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
        // And with no boards there is no default board either.
        assert_eq!(host.resolve_pin(None), None);
        assert_eq!(host.default_pin(), None);
    }

    /// When the operator's own ambient restriction is at HIP's layer, it
    /// wins outright: we write nothing, not even the index we would
    /// otherwise be allowed to write. Ours would overwrite theirs (same
    /// variable) or outrank it (the alias), handing the worker boards they
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
        // consulted at all, so it cannot be bypassed by a board list. Today
        // the probe never produces this combination (any HIP-layer variable
        // also blanks the inventory), which is exactly why the guard has to
        // be positional rather than rely on that invariant holding forever.
        let restricted_with_boards = GpuInventory {
            boards: Some(vec![amd_gpu(0, "0000:03:00.0", 24576)].into()),
            backend: MemoryBackend::RocmSysfs {
                pci_devices: PathBuf::from("/sys/bus/pci/devices"),
                meminfo: PathBuf::from("/proc/meminfo"),
                ambient_hip_restriction: true,
            },
        };
        for requested in [None, Some("0"), Some("GPU-BDF-0000:03:00.0")] {
            assert_eq!(
                restricted_with_boards.resolve_pin(requested),
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
        assert!(!ambient("ROCR_VISIBLE_DEVICES"), "composes with a HIP index");
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
    /// must never disagree, because a board UUID in `HIP_VISIBLE_DEVICES` or
    /// an index in `CUDA_VISIBLE_DEVICES` hides every board from the worker.
    ///
    /// Asserted against the real `probe`, which is where the two are
    /// actually wired together (the backend and the variable both come from
    /// the resolved accelerator), and against the known-inventory fixtures
    /// for the vocabulary each one then emits.
    #[test]
    fn the_pin_vocabulary_and_the_pin_variable_agree() {
        // ROCm, including on this box — the probe finds no AMD boards off
        // Linux, and that must not change the answer.
        let probed = probe(Accelerator::Rocm).inventory;
        assert!(
            probed.pins_are_indices(),
            "a ROCm host pins by index whether or not its probe found boards"
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
        assert_eq!(inventory.resolve_pin(Some("3")), Some("GPU-3333".to_string()));
        assert_eq!(inventory.resolve_pin(Some(" 0 ")), Some("GPU-1111".to_string()));
    }

    #[test]
    fn uuid_pins_pass_through_verbatim() {
        let inventory = inventory();
        assert_eq!(
            inventory.resolve_pin(Some("GPU-9999")),
            Some("GPU-9999".to_string()),
            "an explicit UUID is accepted even for a board we cannot see"
        );
        assert_eq!(
            inventory.resolve_pin(Some("MIG-abc")),
            Some("MIG-abc".to_string())
        );
    }

    /// A CUDA UUID pin that names a board we *can* see comes back in the
    /// **inventory's** spelling, not the operator's. CUDA accepts every
    /// spelling — either case, any unambiguous abbreviation — but the pin
    /// string is compared byte-wise elsewhere: `prewarm.rs` claims a parked
    /// worker only when its recorded pin equals the replica's resolved one,
    /// and `resolve_board_key` already canonicalises for the ledger. Two
    /// spellings of one board therefore have to converge here, or the pool
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
            "two boards share the prefix: verbatim, as before — resolving it \
             is CUDA's business, and guessing a board would be worse"
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
        // every spelling that names a board, which is what the pool compares.
        for spelling in [
            "GPU-ffff",
            "gpu-FFFF0000",
            "GPU-ffff0000-0000-0000-0000-000000000000",
            "2",
        ] {
            assert_eq!(
                inventory.resolve_pin(Some(spelling)),
                inventory.resolve_board_key(Some(spelling)),
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

    /// The ledger vocabulary of the same registry entry: a board key, on
    /// both backends, for every form a pin can take. This is what closes
    /// D2's load-reservation gap — the pin and the key are resolved as a
    /// pair from one request, and on ROCm they are never the same string.
    #[test]
    fn board_keys_resolve_in_both_vocabularies() {
        let cuda = inventory();
        assert_eq!(
            cuda.resolve_board_key(None),
            Some("GPU-1111".to_string()),
            "no request is the default board, the one universal pinning uses"
        );
        assert_eq!(cuda.resolve_pin(None), cuda.resolve_board_key(None));
        assert_eq!(
            cuda.resolve_board_key(Some("3")),
            Some("GPU-3333".to_string()),
            "an index names a row, whose key is what the ledger holds"
        );
        assert_eq!(
            cuda.resolve_board_key(Some(" gpu-3333 ")),
            Some("GPU-3333".to_string()),
            "the key comes back in the inventory's spelling, not the operator's"
        );
        assert_eq!(
            cuda.resolve_board_key(Some("7")),
            None,
            "an index nobody reported names no ledger row (the pin still \
             passes through to CUDA)"
        );
        assert_eq!(cuda.resolve_board_key(Some("0,3")), None, "a device list");
        assert_eq!(cuda.resolve_board_key(Some("cpu")), None);
        assert_eq!(
            cuda.resolve_board_key(Some("GPU-9999")),
            None,
            "a UUID for a board this host cannot see"
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
                rocm.resolve_board_key(Some("1"))
            ),
            (
                Some("1".to_string()),
                Some("GPU-BDF-0000:0c:00.0".to_string())
            ),
            "the pair: HIP gets the index, the ledger gets the key"
        );
        assert_eq!(
            rocm.resolve_board_key(Some("GPU-BDF-0000:0C:00.0")),
            Some("GPU-BDF-0000:0c:00.0".to_string()),
            "a board key resolves to itself, case-insensitively"
        );
        assert_eq!(
            rocm.resolve_board_key(None),
            Some("GPU-BDF-0000:03:00.0".to_string()),
            "while the pin for the same request is the index 0"
        );
        assert_eq!(rocm.resolve_pin(None), Some("0".to_string()));
        assert_eq!(
            rocm.resolve_board_key(Some("GPU-BDF-0000:0c")),
            None,
            "no prefix matching on ROCm: a prefix could name two boards on \
             one bus, and these keys never reach HIP"
        );
        assert_eq!(rocm.resolve_board_key(Some("9")), None);
        assert_eq!(rocm.resolve_board_key(Some("0,1")), None);
    }

    /// CUDA resolves abbreviated UUIDs itself, so `resolve_pin` hands them
    /// to it verbatim — which means the ledger has to resolve them too, or
    /// an operator who wrote `GPU-1a2b` silently gets no load reservation.
    /// An *ambiguous* abbreviation resolves to nothing: reserving against
    /// the wrong board is worse than not reserving.
    #[test]
    fn abbreviated_cuda_uuids_resolve_to_a_board_key_when_unambiguous() {
        let inventory = GpuInventory::known(vec![
            gpu(0, "GPU-1a2b0000-0000-0000-0000-000000000000", "12.0"),
            gpu(1, "GPU-1a2b9999-0000-0000-0000-000000000000", "12.0"),
            gpu(2, "GPU-ffff0000-0000-0000-0000-000000000000", "12.0"),
        ]);
        assert_eq!(
            inventory.resolve_board_key(Some("GPU-ffff")),
            Some("GPU-ffff0000-0000-0000-0000-000000000000".to_string())
        );
        assert_eq!(
            inventory.resolve_board_key(Some("gpu-FFFF0000")),
            Some("GPU-ffff0000-0000-0000-0000-000000000000".to_string()),
            "case-insensitive, as CUDA is"
        );
        assert_eq!(
            inventory.resolve_board_key(Some("GPU-1a2b")),
            None,
            "two boards share the prefix: refuse rather than guess"
        );
        assert_eq!(
            inventory.resolve_board_key(Some("GPU-")),
            None,
            "the degenerate prefix matches everything"
        );
        assert_eq!(
            inventory.resolve_board_key(Some("MIG-unknown")),
            None,
            "a MIG instance outside the enumeration has no ledger board"
        );
        // On a single-board host the same degenerate prefix is unambiguous
        // and resolves — which is exactly what CUDA does with it, so the
        // reservation lands on the board the pin will select. Asserted
        // because it is a behaviour, not an accident of the ambiguity rule.
        let only = GpuInventory::known(vec![gpu(
            0,
            "GPU-ffff0000-0000-0000-0000-000000000000",
            "12.0",
        )]);
        assert_eq!(
            only.resolve_board_key(Some("GPU-")),
            Some("GPU-ffff0000-0000-0000-0000-000000000000".to_string())
        );
    }

    #[test]
    fn unknown_inventory_changes_nothing() {
        let unknown = GpuInventory::unknown();
        assert_eq!(
            unknown.resolve_board_key(Some("3")),
            None,
            "no inventory is no ledger board to key against either"
        );
        assert_eq!(unknown.resolve_board_key(None), None);
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
