//! GPU identity enumeration and worker→GPU pin resolution.
//!
//! Batch calibration keys budgets by GPU *instance* — the board UUID
//! (`GPU-…`) NVML/nvidia-smi report — because a CUDA device index is not an
//! identity: it moves across reboots and with every `CUDA_VISIBLE_DEVICES`
//! change (docs/batch-calibration-design.md, "Two keyspaces"). This module
//! is the source of those identities, and the one place that turns a
//! registry `devices` entry (or the absence of one) into the concrete
//! `CUDA_VISIBLE_DEVICES` value a worker is spawned with.
//!
//! Probing reuses the Package-1 philosophy from `capability.rs`: one short
//! `nvidia-smi` call with a timeout, and any unparseable *identity* makes
//! the whole result unknown. Unknown never changes behaviour — pins pass
//! through exactly as they did before this existed (raw index strings, or
//! no pin at all), which is what keeps CPU/MPS/ROCm hosts and hosts without
//! nvidia-smi on today's code path.
//!
//! The `compute_cap` column is the one exception, because it is the one
//! field that is *separably* useless: vGPU slices and a few datacenter SKUs
//! print `[N/A]` there while every identity column is perfectly good, and
//! discarding those rows wholesale would take pinning, the ledger and the
//! board list down with them. Such a board keeps its identity and is simply
//! never chosen by capability-ranked default placement.
//!
//! There is exactly **one** probe for both hardware facts the server needs
//! (board identities here, compute capabilities in `capability.rs`): they
//! come from one `--query-gpu` invocation, so the two views can never
//! disagree about which board is which, and boot pays one subprocess
//! instead of two. Rows are matched positionally, so an inventory index and
//! a capability always describe the same physical board.
//!
//! [`probe`] takes the **resolved** accelerator (the setup sentinel's, not
//! a re-probe of the hardware) and dispatches on it: ROCm hosts get the
//! kernel-sysfs inventory in `rocm.rs` instead, with an always-unknown
//! capability view because HIP has no compute-capability analogue
//! (docs/rocm-batch-calibration-parity.md, D1/D7). Every other accelerator
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
use super::rocm;
use crate::config::Accelerator;

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
}

impl GpuInfo {
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

/// Which kernel/driver interface answers this host's live-memory questions.
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
    },
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
    // The ambient value matters: nvidia-smi *ignores* CUDA_VISIBLE_DEVICES
    // and reports every board, so an operator who launched the gateway with
    // a restriction would otherwise see us pin workers to boards they
    // deliberately hid (see `restrict_to_visible`).
    let visible = std::env::var("CUDA_VISIBLE_DEVICES").ok();
    build(query().as_deref(), visible.as_deref())
}

/// KFD topology + amdgpu sysfs (`rocm.rs`). The capability view is always
/// unknown: HIP exposes no compute capability, so there is nothing to
/// filter with and every shipped floor is CUDA-specific anyway (D7).
///
/// Off Linux this is unconditionally unknown — the `rocm` torch extra
/// carries a `sys_platform == 'linux'` marker, so no supported install has
/// ROCm wheels anywhere else, and the sysfs paths do not exist.
fn probe_rocm() -> HostGpus {
    let roots = rocm::SysfsRoots::default();
    let inventory = if cfg!(target_os = "linux") {
        let ambient = rocm::VISIBILITY_VARS.map(|var| std::env::var(var).ok());
        let ambient = ambient.each_ref().map(Option::as_deref);
        rocm::build(&roots, ambient)
    } else {
        None
    };
    let Some(gpus) = inventory else {
        return HostGpus {
            caps: HostComputeCaps::unknown(),
            inventory: GpuInventory::default(),
        };
    };
    for gpu in &gpus {
        tracing::info!(
            index = gpu.index,
            uuid = %gpu.uuid,
            name = %gpu.name,
            total_mb = gpu.total_mb,
            bdf = gpu.bdf.as_deref().unwrap_or("unknown"),
            "detected GPU"
        );
    }
    HostGpus {
        caps: HostComputeCaps::unknown(),
        inventory: GpuInventory {
            boards: Some(gpus.into()),
            backend: MemoryBackend::RocmSysfs {
                pci_devices: roots.pci_devices,
            },
        },
    }
}

/// Run the single query. `None` on any failure — no nvidia-smi, timeout,
/// non-zero exit.
fn query() -> Option<String> {
    let smi = find_nvidia_smi()?;
    let mut cmd = Command::new(smi);
    cmd.args([
        "--query-gpu=index,uuid,name,memory.total,compute_cap",
        "--format=csv,noheader,nounits",
    ]);
    let Some(output) = output_with_timeout(cmd, Duration::from_secs(5)) else {
        tracing::warn!(
            "nvidia-smi GPU probe failed or timed out; workers will not be \
             pinned to a specific GPU and model availability will not be \
             capability-filtered"
        );
        return None;
    };
    if !output.status.success() {
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
pub enum MemoryQuery {
    /// One `nvidia-smi --query-gpu` call covering every visible board.
    NvidiaSmi,
    /// amdgpu's `mem_info_vram_{total,used}`, one file pair per board.
    RocmSysfs {
        pci_devices: PathBuf,
        /// `(board key, PCI address)`, in inventory order.
        boards: Arc<[(String, String)]>,
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
                boards,
            } => rocm::query_memory(pci_devices, boards),
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
            // Including `Unavailable`, which is only ever reached from a
            // ROCm inventory and never records anything anyway.
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
    pub fn memory_query(&self) -> MemoryQuery {
        let MemoryBackend::RocmSysfs { pci_devices } = &self.backend else {
            return MemoryQuery::NvidiaSmi;
        };
        let boards = self.gpus().unwrap_or(&[]);
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
            keyed.push((gpu.uuid.clone(), bdf));
        }
        MemoryQuery::RocmSysfs {
            pci_devices: pci_devices.clone(),
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
    /// default anyway.
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
    /// # ROCm: always `None` — Step-1 interim contract
    ///
    /// **This is temporary and must be revisited when D2 lands.** The spawn
    /// layer writes whatever comes back here into `CUDA_VISIBLE_DEVICES`,
    /// which on a HIP host is an alias for `HIP_VISIBLE_DEVICES` and
    /// accepts **only device indices**. A `GPU-…` board key there matches
    /// nothing, hides every device from the worker, and sends it silently
    /// to CPU — strictly worse than not pinning at all. Until D2 plumbs
    /// `HIP_VISIBLE_DEVICES=<row index>` through the spawn config, a ROCm
    /// host therefore gets no universal pinning: the boards are still
    /// inventoried (the ledger, the `/metadata` calibration overlay and
    /// [`Self::default_board_name`] all work), they are just not pinned.
    /// See docs/rocm-batch-calibration-parity.md, D2.
    pub fn default_pin(&self) -> Option<String> {
        if self.pins_are_indices() {
            return None;
        }
        self.default_board().map(|gpu| gpu.uuid.clone())
    }

    /// Whether this host's pin vocabulary is HIP's (bare indices) rather
    /// than CUDA's (board UUIDs). See [`Self::default_pin`] for why Step 1
    /// answers "then emit no pin" rather than translating.
    fn pins_are_indices(&self) -> bool {
        matches!(self.backend, MemoryBackend::RocmSysfs { .. })
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
                std::cmp::Reverse(gpu.total_mb),
                gpu.index,
            )
        })
    }

    /// Resolve one replica's registry pin into the `CUDA_VISIBLE_DEVICES`
    /// value it is spawned with:
    ///
    /// - unknown inventory → the request verbatim (`None` stays `None`):
    ///   exactly today's behaviour, which is what CPU/MPS/ROCm hosts need;
    /// - no request → the default board's UUID (universal pinning);
    /// - a UUID request (`GPU-…`/`MIG-…`) → verbatim; CUDA accepts it;
    /// - an index request → that board's UUID, so the ledger key is stable
    ///   even though the index is not;
    /// - anything else (an index we cannot see, a comma-separated list, a
    ///   templated leftover) → verbatim with a warning. Passing it through
    ///   preserves whatever the operator meant; guessing would not.
    ///
    /// # ROCm: indices pass, everything else is dropped — Step-1 interim
    ///
    /// **Temporary; D2 replaces this.** On a known ROCm inventory the only
    /// safe value to write into the worker's `CUDA_VISIBLE_DEVICES` (a HIP
    /// alias) is a **plain numeric index**, because that is the only form
    /// HIP understands. So a numeric request survives verbatim — the
    /// operator's intent is expressible — and *everything else* resolves to
    /// no pin at all: a board key or a `GPU-…` leftover written there would
    /// match no device, hide the whole board set, and drop the worker to
    /// CPU without a word. No pin is worse than a correct pin and far
    /// better than a device-hiding one. The board-key → row-index
    /// translation this should eventually do arrives with
    /// `HIP_VISIBLE_DEVICES` plumbing (docs/rocm-batch-calibration-parity.md,
    /// D2).
    pub fn resolve_pin(&self, requested: Option<&str>) -> Option<String> {
        let Some(gpus) = self.boards.as_deref() else {
            return requested.map(str::to_owned);
        };
        if self.pins_are_indices() {
            let index = requested?.trim();
            if index.parse::<u32>().is_ok() {
                return Some(index.to_owned());
            }
            tracing::warn!(
                pin = %index,
                "this ROCm host pins by HIP device index; dropping a device \
                 pin that is not one rather than hiding every board from the \
                 worker (which would silently run it on the CPU)"
            );
            return None;
        }
        let Some(requested) = requested else {
            return self.default_pin();
        };
        let trimmed = requested.trim();
        if is_uuid_pin(trimmed) {
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
        gpus.push(GpuInfo {
            index,
            uuid,
            name,
            total_mb,
            compute_cap,
            // nvidia-smi rows need neither: the UUID is both the identity
            // and the pin form, and there is no gfx target.
            bdf: None,
            gfx_target_version: None,
        });
    }
    if gpus.is_empty() {
        None
    } else {
        Some(gpus)
    }
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
        }
    }

    /// A known ROCm inventory reading from `pci_devices` — a fixture tree in
    /// tests, `/sys/bus/pci/devices` in production.
    fn rocm_inventory(pci_devices: PathBuf, gpus: Vec<GpuInfo>) -> GpuInventory {
        GpuInventory {
            boards: Some(gpus.into()),
            backend: MemoryBackend::RocmSysfs { pci_devices },
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
                &[("GPU-BDF-0000:03:00.0".to_owned(), "0000:03:00.0".to_owned())]
            ),
            other => panic!("expected the sysfs query, got {other:?}"),
        }
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
                matches!(rocm.inventory.memory_query(), MemoryQuery::NvidiaSmi),
                "an unknown host stays on the default backend rather than \
                 promising sysfs reads it cannot do"
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

    #[test]
    fn unpinned_replica_resolves_to_the_default_board() {
        let inventory = inventory();
        assert_eq!(
            inventory.resolve_pin(None),
            Some("GPU-1111".to_string()),
            "universal pinning: no pin means the default board's UUID"
        );
    }

    /// Step-1 interim contract (D2 replaces it): the spawn layer writes
    /// whatever `resolve_pin` returns into `CUDA_VISIBLE_DEVICES`, which on
    /// a HIP host aliases `HIP_VISIBLE_DEVICES` and accepts only device
    /// indices. A board key there would match nothing, hide every device,
    /// and drop the worker to CPU in silence — so a known ROCm inventory
    /// emits an index or no pin at all, never a `GPU-…` string.
    #[test]
    fn a_rocm_inventory_never_emits_a_board_key_as_a_pin() {
        let host = rocm_inventory(
            PathBuf::from("/sys/bus/pci/devices"),
            vec![
                amd_gpu(0, "0000:03:00.0", 24576),
                amd_gpu(1, "0000:0c:00.0", 24576),
            ],
        );
        assert_eq!(host.default_pin(), None, "no universal pinning yet");
        assert_eq!(host.resolve_pin(None), None);
        assert_eq!(
            host.resolve_pin(Some("1")),
            Some("1".to_string()),
            "HIP understands indices, so the operator's intent survives"
        );
        assert_eq!(host.resolve_pin(Some("GPU-BDF-0000:03:00.0")), None);
        assert_eq!(host.resolve_pin(Some("cpu")), None);
        // The board name is still available: the /metadata calibration
        // overlay needs it, and it never reaches a worker's environment.
        assert_eq!(
            host.default_board_name().as_deref(),
            Some("AMD gfx1100 (24 GB)")
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

    #[test]
    fn unresolvable_pins_pass_through() {
        let inventory = inventory();
        // Index nobody reported, a multi-device list, and a non-numeric
        // string all reach CUDA_VISIBLE_DEVICES unchanged.
        assert_eq!(inventory.resolve_pin(Some("7")), Some("7".to_string()));
        assert_eq!(inventory.resolve_pin(Some("0,3")), Some("0,3".to_string()));
        assert_eq!(inventory.resolve_pin(Some("cpu")), Some("cpu".to_string()));
    }

    #[test]
    fn unknown_inventory_changes_nothing() {
        let unknown = GpuInventory::unknown();
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
