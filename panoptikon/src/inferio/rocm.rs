//! ROCm GPU inventory and live VRAM, read from kernel sysfs.
//!
//! The CUDA side of `gpu.rs` shells out to nvidia-smi. There is no
//! equivalent here on purpose (docs/rocm-batch-calibration-parity.md, D1):
//! amd-smi/rocm-smi changed JSON shape at ROCm 6.1 and keep drifting, print
//! human text on error paths while still exiting 0, are absent from bare
//! installs, and — decisively — **enumerate in PCI-BDF order while HIP
//! enumerates in KFD topology-node order**, so any ordinal we learned from
//! them would name a different GPU than the one a pin selects
//! (pytorch#131901). Everything below comes from the interfaces the ROCr
//! runtime itself is built on, which therefore cannot disagree with it:
//!
//! - `/sys/class/kfd/kfd/topology/nodes/<n>/properties` — the KFD topology
//!   ROCr reads to build its agent list, in the same ascending node order;
//! - `/dev/dri/renderD<minor>` — the node ROCr must open to use a GPU, so
//!   "can I open it" is the same admission test the runtime applies;
//! - `/sys/bus/pci/devices/<bdf>/mem_info_vram_{total,used}` — amdgpu's own
//!   per-GPU counters, and the *same files* the worker's free/total tier
//!   reads (D4), which is what makes the ledger's one-memory-vocabulary
//!   rule hold by construction rather than by matching two drivers. On an
//!   **APU** the `mem_info_gtt_{total,used}` pair beside them is read too,
//!   because that is where ROCm allocations actually land once the BIOS
//!   carve-out fills (docs/unified-memory-admission.md, backend B);
//! - `/proc/meminfo` — `MemTotal` names an APU GPU (its capacity is the
//!   machine's), and `MemAvailable` clamps the GTT half of its free reading
//!   to RAM that exists right now, which is what makes external pressure on
//!   a unified-memory device visible at all.
//!
//! Everything is a pure function of four injectable roots so the whole
//! probe is testable from fixture directory trees on any platform; only
//! `SysfsRoots::default()` and the caller's `cfg!(target_os = "linux")`
//! gate know that these are Linux paths.
//!
//! Identity is all-or-nothing per host, mirroring the nvidia-smi parser: a
//! GPU we can open but cannot name or size makes the whole inventory
//! unknown. A *partial* inventory would be worse than none, because a row's
//! index is the `HIP_VISIBLE_DEVICES` value D2 pins with — it only means
//! anything if the rows cover the entire openable set.

use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};

use super::gpu::{GpuInfo, GpuMemory};

/// Every env var that can restrict which GPUs a HIP process sees.
///
/// `ROCR_VISIBLE_DEVICES` filters at the ROCr/KFD layer;
/// `HIP_VISIBLE_DEVICES`, its CUDA-compat alias `CUDA_VISIBLE_DEVICES` and
/// `GPU_DEVICE_ORDINAL` filter at the HIP layer, indexing *into* the
/// ROCr-filtered set. Any of them set non-empty blanks the inventory
/// (see [`build`]).
pub(super) const VISIBILITY_VARS: [&str; 4] = [
    "ROCR_VISIBLE_DEVICES",
    "HIP_VISIBLE_DEVICES",
    "CUDA_VISIBLE_DEVICES",
    "GPU_DEVICE_ORDINAL",
];

/// The HIP-layer subset of [`VISIBILITY_VARS`] — the ones a pin of ours
/// would collide with rather than compose with. `HIP_VISIBLE_DEVICES` is the
/// variable D2 writes, so we would overwrite the operator's value outright;
/// `CUDA_VISIBLE_DEVICES` is its alias, which the one we write takes
/// precedence over; `GPU_DEVICE_ORDINAL` filters at the same layer, so our
/// value and theirs contend. `ROCR_VISIBLE_DEVICES` is deliberately absent:
/// it filters *below* HIP, and a HIP index counts into the ROCr-filtered
/// set, so the two compose correctly (see [`ambient_hip_restriction`]).
const HIP_LAYER_VISIBILITY_VARS: [&str; 3] = [
    "HIP_VISIBLE_DEVICES",
    "CUDA_VISIBLE_DEVICES",
    "GPU_DEVICE_ORDINAL",
];

/// The four filesystem roots the probe reads. Injectable so the parse and
/// build logic is exercised against fixture trees rather than hardware
/// (and so those tests run on non-Linux dev machines).
#[derive(Debug, Clone)]
pub(super) struct SysfsRoots {
    /// KFD topology nodes, one numeric subdirectory per node.
    pub kfd_nodes: PathBuf,
    /// PCI device directories, named by lower-case BDF.
    pub pci_devices: PathBuf,
    /// DRM device nodes; `renderD<minor>` is the per-GPU render node.
    pub dev_dri: PathBuf,
    /// `/proc/meminfo`: `MemTotal` for an APU GPU's identity, `MemAvailable`
    /// for the GTT clamp in its live free reading.
    pub meminfo: PathBuf,
}

impl Default for SysfsRoots {
    fn default() -> Self {
        Self {
            kfd_nodes: PathBuf::from("/sys/class/kfd/kfd/topology/nodes"),
            pci_devices: PathBuf::from("/sys/bus/pci/devices"),
            dev_dri: PathBuf::from("/dev/dri"),
            meminfo: PathBuf::from("/proc/meminfo"),
        }
    }
}

/// One GPU the live-memory refresh reads: where its counters are, and
/// whether it is the unified kind whose total includes GTT.
///
/// The flag rides here rather than being re-derived from the filesystem
/// because `mem_info_gtt_*` exists for **discrete** GPUs too (GTT is
/// system memory any amdgpu GPU can map), so its presence is not the
/// question — whether this GPU's *budget* is carve-out + GTT is, and only
/// the probe's KFD classification answers that.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct GpuRef {
    /// The ledger's device key.
    pub key: String,
    /// The PCI address amdgpu names this GPU's sysfs directory with.
    pub bdf: String,
    /// An APU: total and free include GTT (backend B).
    pub unified: bool,
}

/// Why a probe produced no inventory, carried out of [`build`] so the
/// caller can say what was seen rather than leaving a ROCm host silently
/// unpriced.
///
/// A host that finds no GPUs behaves exactly as it did before this module
/// existed — which is the safe outcome, and also an *invisible* one: the
/// operator sees no ledger, no grants and no explanation. The bucket plus
/// the two counts are the whole diagnosis, and they are what a field report
/// needs to distinguish "this is not a ROCm host at all" from "the container
/// was granted no render nodes" from "the GPU is partitioned".
///
/// [`Self::log`] is deliberately silent when the deciding site already
/// warned: those lines name the specific node, address or GPU that
/// tripped, which is strictly more informative than this summary, and two
/// WARNs per boot saying the same thing is noise.
pub(super) struct ProbeFailure {
    bucket: &'static str,
    gpu_nodes: usize,
    openable: usize,
    already_logged: bool,
}

impl ProbeFailure {
    /// A failure whose deciding site said nothing; [`Self::log`] speaks.
    fn undiagnosed(bucket: &'static str, gpu_nodes: usize, openable: usize) -> Self {
        Self {
            bucket,
            gpu_nodes,
            openable,
            already_logged: false,
        }
    }

    /// A failure whose deciding site already emitted its own WARN (or, for
    /// the ambient restriction, its own INFO).
    fn logged(bucket: &'static str, gpu_nodes: usize, openable: usize) -> Self {
        Self {
            bucket,
            gpu_nodes,
            openable,
            already_logged: true,
        }
    }

    /// One WARN naming what the probe saw, unless the deciding site already
    /// spoke for itself.
    pub(super) fn log(&self) {
        if self.already_logged {
            return;
        }
        tracing::warn!(
            reason = self.bucket,
            gpu_nodes = self.gpu_nodes,
            openable_nodes = self.openable,
            "this host is configured for ROCm but no GPU inventory could be \
             built, so it gets no VRAM ledger, no grants and no calibration — \
             dispatch takes the unpriced path (your cap, then the registry \
             default, then default_max_batch)"
        );
    }
}

/// Build the GPU inventory, or a [`ProbeFailure`] for "unknown host" —
/// which leaves ROCm hosts on exactly the unpriced dispatch path they were
/// on before this existed.
///
/// `ambient` is the value of each [`VISIBILITY_VARS`] entry, positionally.
/// The caller reads the environment once and passes the values in, so the
/// tests never mutate process env (same convention as `gpu.rs::build`). The
/// parameter is a **fixed-size array** rather than a slice so the compiler
/// enforces that positional contract: a caller that collects one value too
/// few — silently making the last variable unreadable — does not compile.
///
/// The ambient rule is deliberately **stricter than CUDA's**: there, a
/// UUID-form restriction is honoured because CUDA pins are absolute UUIDs,
/// so composing ours on top of the operator's is well defined. ROCm pins
/// are *relative indices* into the ROCr-filtered set, so composing an index
/// pin on top of an ambient filter is precisely the ordinal-correlation
/// mistake that makes cross-vocabulary AMD device matching unsound. Cost:
/// scheduler-managed hosts (Slurm sets `ROCR_VISIBLE_DEVICES`) stay
/// unpriced, which is safe and today's behaviour.
///
/// Blanking the inventory withdraws the pins *we* derive. It does not by
/// itself decide what happens to a pin the operator wrote in the registry;
/// that depends on which layer the ambient restriction sits in, which is
/// what [`ambient_hip_restriction`] records for `gpu.rs`.
pub(super) fn build(
    roots: &SysfsRoots,
    ambient: [Option<&str>; VISIBILITY_VARS.len()],
) -> Result<Vec<GpuInfo>, ProbeFailure> {
    if let Some(var) = ambient_restriction(ambient) {
        tracing::info!(
            variable = var,
            hip_layer = ambient_hip_restriction(ambient),
            "an ambient GPU visibility restriction is set; leaving the ROCm \
             GPU inventory unknown (HIP device indices count the filtered \
             set, so our pins cannot compose with it) — workers inherit the \
             restriction as-is: a HIP-layer restriction suppresses our \
             pinning entirely, and under a ROCR-only one a registry index \
             pin from the registry is still written and selects *within* the \
             operator's filtered set, not the host's own GPU order. That \
             last point is the diagnostic for \"the model ran on a different \
             card than devices = [N] names\": with a ROCR filter in force, \
             index N counts the GPUs the operator left visible"
        );
        return Err(ProbeFailure::logged("ambient visibility restriction", 0, 0));
    }
    let openable = openable_gpu_nodes(roots)?;
    let gpu_nodes = openable.gpu_nodes;
    let count = openable.nodes.len();
    if count == 0 {
        return Err(ProbeFailure::undiagnosed(
            if gpu_nodes == 0 {
                "no KFD GPU nodes (this host has no amdgpu topology)"
            } else {
                "no openable render node"
            },
            gpu_nodes,
            count,
        ));
    }
    let mut rows = Vec::with_capacity(count);
    for (index, (node, props)) in openable.nodes.iter().enumerate() {
        let Ok(index) = u32::try_from(index) else {
            return Err(ProbeFailure::undiagnosed(
                "more openable GPUs than a device index can name",
                gpu_nodes,
                count,
            ));
        };
        let Some(row) = identify(roots, *node, props, index) else {
            return Err(ProbeFailure::logged(
                "identity read failed",
                gpu_nodes,
                count,
            ));
        };
        rows.push(row);
    }
    let rows = demote_duplicate_ids(rows)
        .ok_or_else(|| ProbeFailure::logged("duplicate device keys", gpu_nodes, count))?;
    reject_partitioned_gpus(rows)
        .ok_or_else(|| ProbeFailure::logged("partitioned GPU", gpu_nodes, count))
}

/// Live free/total for every GPU, all-or-nothing. One unreadable GPU
/// makes the whole reading unknown rather than pricing that GPU's
/// external usage as zero — the same rule the nvidia-smi snapshot parser
/// enforces, for the same reason (phantom headroom).
///
/// `free = total - used` ignores firmware/kernel reserved carve-outs that
/// nvidia-smi's `memory.free` excludes, so ROCm free readings run a few
/// hundred MB optimistic; the ledger's default margin absorbs it.
///
/// # Unified-memory devices (APUs)
///
/// A GPU the probe flagged unified is budgeted against carve-out **plus**
/// GTT, because GTT is where its allocations land once the carve-out fills,
/// and its free reading is
/// `(vram_total − vram_used) + min(gtt_total − gtt_used, ram_available)`.
/// The clamp is the whole point: unclaimed GTT is an address-space figure,
/// and the pages behind it have to come out of RAM that exists *now*, so
/// without it a machine whose browser is holding 40 GB would look idle
/// (docs/unified-memory-admission.md, backend B). Discrete rows read exactly
/// the two files they always read.
pub(super) fn query_memory(
    pci_devices: &Path,
    meminfo: &Path,
    gpus: &[GpuRef],
) -> Option<Vec<GpuMemory>> {
    if gpus.is_empty() {
        return None;
    }
    // Read once for the whole pass, and only if a unified-memory device asks: every
    // row in one snapshot must see the same instant, and a host of discrete
    // GPUs must not acquire a dependency on /proc/meminfo being readable.
    let mut ram_available_mb: Option<u64> = None;
    let mut out = Vec::with_capacity(gpus.len());
    for gpu in gpus {
        let dir = pci_device_dir(pci_devices, &gpu.bdf);
        let vram_total_mb = read_mb(&dir.join("mem_info_vram_total"))?;
        let vram_free_mb = vram_total_mb.saturating_sub(read_mb(&dir.join("mem_info_vram_used"))?);
        if !gpu.unified {
            out.push(GpuMemory {
                uuid: gpu.key.clone(),
                total_mb: vram_total_mb,
                free_mb: vram_free_mb,
            });
            continue;
        }
        let gtt_total_mb = read_mb(&dir.join("mem_info_gtt_total"))?;
        let gtt_free_mb = gtt_total_mb.saturating_sub(read_mb(&dir.join("mem_info_gtt_used"))?);
        let available_mb = match ram_available_mb {
            Some(mb) => mb,
            None => *ram_available_mb.insert(meminfo_mb(meminfo, "MemAvailable")?),
        };
        out.push(GpuMemory {
            uuid: gpu.key.clone(),
            total_mb: vram_total_mb + gtt_total_mb,
            free_mb: vram_free_mb + gtt_free_mb.min(available_mb),
        });
    }
    Some(out)
}

/// The first visibility variable set to something non-empty, if any.
/// Whitespace- and comma-only values count as unset, matching how the CUDA
/// path treats an empty `CUDA_VISIBLE_DEVICES` as "not configured".
fn ambient_restriction(ambient: [Option<&str>; VISIBILITY_VARS.len()]) -> Option<&'static str> {
    VISIBILITY_VARS
        .iter()
        .zip(ambient)
        .find(|(_, value)| is_set(*value))
        .map(|(var, _)| *var)
}

/// Whether any [`HIP_LAYER_VISIBILITY_VARS`] entry is set — i.e. whether the
/// operator's ambient restriction lives in the *same* layer the pin would be
/// written to.
///
/// This is not the same question as [`ambient_restriction`], which answers
/// "is the inventory knowable" and is satisfied by any of the four. It is
/// the pin question: a HIP index composes correctly with an ambient
/// `ROCR_VISIBLE_DEVICES` (HIP indexes into the ROCr-filtered set, so the
/// operator's filter still holds and ours selects within it), but there is
/// no composing with a HIP-layer restriction — writing one clobbers or
/// overrides it, quietly widening what the operator deliberately narrowed.
/// So the *kind* is recorded here and `gpu.rs` refuses to pin at all in that
/// case, rather than only refusing to pin what it cannot name.
///
/// Scans every variable rather than stopping at the first set one:
/// `ROCR_VISIBLE_DEVICES` comes first positionally, and a host with both
/// would otherwise be read as ROCR-only.
pub(super) fn ambient_hip_restriction(ambient: [Option<&str>; VISIBILITY_VARS.len()]) -> bool {
    VISIBILITY_VARS
        .iter()
        .zip(ambient)
        .any(|(var, value)| HIP_LAYER_VISIBILITY_VARS.contains(var) && is_set(value))
}

/// A visibility variable counts as set only when it names at least one
/// entry: whitespace- and comma-only values are "not configured", matching
/// how the CUDA path treats an empty `CUDA_VISIBLE_DEVICES`.
fn is_set(value: Option<&str>) -> bool {
    value.is_some_and(|value| {
        value
            .split(',')
            .map(str::trim)
            .any(|entry| !entry.is_empty())
    })
}

/// The openable GPU nodes, plus how many GPU nodes the topology listed at
/// all — the two numbers a [`ProbeFailure`] needs to say whether this host
/// has no amdgpu topology or merely no render nodes this process may open.
struct OpenableNodes {
    /// Every KFD node with SIMDs, whether or not it survived the filter.
    gpu_nodes: usize,
    /// The survivors, in ascending KFD node order.
    nodes: Vec<(u32, HashMap<String, u64>)>,
}

/// GPU nodes whose render node this process can actually open, in ascending
/// KFD node order — which is the order ROCr enumerates agents in, hence the
/// order HIP indexes them in. An `Err` makes the whole probe unknown; see
/// [`node_hidden_from_this_process`] for which read failures do that.
///
/// A container granted a `/dev/dri` subset still sees the *whole* host
/// topology through KFD, so without this filter its row indices would name
/// GPUs it cannot touch and every pin would land out of range (the worker
/// then silently falls back to CPU). Cgroup-hidden and unopenable nodes are
/// dropped rather than failing the probe: such a node is one ROCr will not
/// offer either, so excluding it *reconstructs* the runtime's enumeration.
fn openable_gpu_nodes(roots: &SysfsRoots) -> Result<OpenableNodes, ProbeFailure> {
    let mut nodes = node_dirs(&roots.kfd_nodes);
    // Numerically, never lexicographically: node 10 comes after node 9, and
    // a string sort would put it between 1 and 2 — silently renumbering
    // every row on a host with ten or more KFD nodes, and those row numbers
    // are the HIP device indices D2 pins with.
    nodes.sort_by_key(|(node, _)| *node);
    let mut gpu_nodes = 0usize;
    let mut out = Vec::new();
    for (node, dir) in nodes {
        let text = match fs::read_to_string(dir.join("properties")) {
            Ok(text) => text,
            Err(err) if node_hidden_from_this_process(&err) => {
                tracing::info!(
                    node,
                    error = %err,
                    "KFD denied this node's properties; a device cgroup hides \
                     the GPU from this process, so ROCr will not enumerate \
                     it either — excluding it from the ROCm inventory"
                );
                continue;
            }
            Err(err) => {
                tracing::warn!(
                    node,
                    error = %err,
                    "cannot read this KFD node's properties; leaving the ROCm \
                     GPU inventory unknown (dropping the node would shift \
                     every later row's index, and those indices are the HIP \
                     device numbers a pin selects with)"
                );
                return Err(ProbeFailure::logged(
                    "node properties unreadable",
                    gpu_nodes,
                    out.len(),
                ));
            }
        };
        let props = parse_properties(&text);
        // The KFD topology lists CPU nodes too; only GPU nodes have SIMDs.
        // An **absent** `simd_count` is not a CPU node, it is a properties
        // file we do not understand — and reading it as a CPU node would
        // silently drop a GPU and shift every later row's index. Only an
        // explicit 0 skips, exactly as with the other required keys.
        let Some(simd_count) = props.get("simd_count").copied() else {
            tracing::warn!(
                node,
                "this KFD node's properties carry no simd_count, so whether \
                 it is a GPU cannot be decided; leaving the ROCm GPU \
                 inventory unknown (treating it as a CPU node would drop it \
                 and shift every later row's HIP device index)"
            );
            return Err(ProbeFailure::logged(
                "node reports no simd_count",
                gpu_nodes,
                out.len(),
            ));
        };
        if simd_count == 0 {
            continue;
        }
        gpu_nodes += 1;
        let Some(minor) = props.get("drm_render_minor").copied().filter(|m| *m > 0) else {
            tracing::info!(
                node,
                "KFD GPU node reports no DRM render minor; excluding it from \
                 the ROCm inventory"
            );
            continue;
        };
        // Opening for **read+write** is the test, because that is the
        // access KFD itself demands: its device-cgroup admission check asks
        // for `DEVCG_ACC_READ | DEVCG_ACC_WRITE` on the render node before
        // it will bind the process to the device. A cgroup can grant `r`
        // without `w`, and a read-only open would then succeed here while
        // ROCr still refuses the GPU — exactly the phantom row this
        // filter exists to prevent.
        let render = roots.dev_dri.join(format!("renderD{minor}"));
        if let Err(err) = OpenOptions::new().read(true).write(true).open(&render) {
            tracing::info!(
                node,
                render_minor = minor,
                error = %err,
                "KFD GPU node's render node cannot be opened read-write; \
                 excluding it from the ROCm inventory"
            );
            continue;
        }
        out.push((node, props));
    }
    Ok(OpenableNodes {
        gpu_nodes,
        nodes: out,
    })
}

/// Whether an error reading a KFD node's `properties` means "this node is
/// hidden from this process" (skip it) rather than "sysfs is not answering
/// the way we understand" (fail the whole probe).
///
/// Only `PermissionDenied` is a skip: KFD returns `-EPERM` for a node a
/// device cgroup hides from the container, and ROCr — which reads the same
/// file — will not enumerate that node either, so skipping reconstructs the
/// runtime's view. Everything else, including `NotFound` (a hot-unplug race
/// between listing the directory and reading it), fails the probe: a
/// dropped node shifts every row index after it, and those indices are the
/// HIP device numbers D2 pins with. Silently answering with a *shifted*
/// inventory is the single failure this whole module exists to avoid.
fn node_hidden_from_this_process(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::PermissionDenied
}

/// Numerically-named subdirectories of the topology root. `None`-ish (an
/// empty vec) when the root does not exist, which is every non-ROCm host.
fn node_dirs(root: &Path) -> Vec<(u32, PathBuf)> {
    let Ok(entries) = fs::read_dir(root) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| {
            let node = entry.file_name().to_str()?.parse::<u32>().ok()?;
            Some((node, entry.path()))
        })
        .collect()
}

/// Turn one openable GPU node into a GPU row, or `None` to make the whole
/// probe unknown. Device key, name and VRAM total are all identity: a row
/// missing any of them could not key the ledger, the calibration profile
/// or the config, and half-identified GPUs must never reach either.
///
/// # APUs
///
/// KFD models an integrated part as one node carrying both SIMDs **and** CPU
/// cores, and that combination is the only positive signal there is. Such a
/// node used to make the whole probe unknown, because amdgpu publishes
/// `mem_info_vram_total` for iGPUs too — the BIOS UMA carve-out, commonly
/// 512 MB — so admitting it on the ordinary rules would have budgeted the
/// host against a few hundred MB and collapsed every grant to batch-1. It is
/// now priced as a **unified-memory device** instead
/// (docs/unified-memory-admission.md, backend B): the total is carve-out +
/// GTT, the name carries the machine's RAM rather than the BIOS-configurable
/// carve-out (DP-6), and `unified_ram_mb` flags it for the ledger. The extra
/// facts that pricing needs are part of the identity like every other one —
/// a GTT total or a `MemTotal` we cannot read fails the probe rather than
/// silently falling back to the carve-out, which is the exact mis-pricing
/// the old decline existed to prevent.
fn identify(
    roots: &SysfsRoots,
    node: u32,
    props: &HashMap<String, u64>,
    index: u32,
) -> Option<GpuInfo> {
    let bdf = props
        .get("location_id")
        .copied()
        .zip(props.get("domain").copied())
        .and_then(|(location_id, domain)| format_bdf(domain, location_id));
    let Some(bdf) = bdf else {
        tracing::warn!(
            node,
            "KFD GPU node has no usable location_id/domain; leaving the ROCm \
             GPU inventory unknown"
        );
        return None;
    };
    let device = pci_device_dir(&roots.pci_devices, &bdf);
    // A zero total is as unusable as an absent one *here*, in the identity
    // pass: it would name the GPU `… (1 GB)` (the profile keyspace), give
    // the ledger a GPU with no capacity, and make every grant on it a
    // division by a fiction. The live `query_memory` path stays tolerant of
    // zero on purpose — there it is a reading, not an identity.
    let Some(vram_total_mb) = read_mb(&device.join("mem_info_vram_total")).filter(|mb| *mb > 0)
    else {
        tracing::warn!(
            node,
            bdf = %bdf,
            "cannot read a nonzero mem_info_vram_total for this GPU (a \
             non-amdgpu node, or a container with a partial /sys, would look \
             like this; on an APU this file is the BIOS carve-out, which is \
             only part of the total — see below); leaving the ROCm GPU \
             inventory unknown"
        );
        return None;
    };
    let target = props
        .get("gfx_target_version")
        .copied()
        .and_then(|v| u32::try_from(v).ok())
        .unwrap_or(0);
    let Some(gfx) = gfx_name(target) else {
        tracing::warn!(
            node,
            bdf = %bdf,
            gfx_target_version = target,
            "KFD GPU node reports no decodable gfx_target_version; leaving \
             the ROCm GPU inventory unknown (the GPU's name is the \
             calibration keyspace and must not be a placeholder)"
        );
        return None;
    };
    // An APU, per KFD's only positive signal: one node with both SIMDs and
    // CPU cores. Discrete GPUs report an explicit 0 or omit the key.
    let unified = props.get("cpu_cores_count").copied().unwrap_or(0) > 0;
    let unified = match unified {
        false => None,
        true => Some(unified_facts(roots, node, &bdf, &device, vram_total_mb)?),
    };
    let unique_id = props.get("unique_id").copied().filter(|id| *id != 0);
    let (total_mb, name) = match &unified {
        Some(facts) => (facts.total_mb, apu_device_name(&gfx, facts.ram_mb)),
        None => (vram_total_mb, gpu_name(&gfx, vram_total_mb)),
    };
    Some(GpuInfo {
        index,
        // Provisional: `demote_duplicate_ids` rewrites this if two GPUs
        // fused the same serial.
        uuid: device_key(unique_id, &bdf),
        name,
        total_mb,
        // HIP has no compute-capability analogue (D7). The per-row Option
        // already tolerates it; the host's capability view is unknown, so
        // nothing is capability-filtered on ROCm.
        compute_cap: None,
        bdf: Some(bdf),
        gfx_target_version: Some(target),
        unified_ram_mb: unified.as_ref().map(|facts| facts.ram_mb),
        vram_carveout_mb: unified.as_ref().map(|_| vram_total_mb),
    })
}

/// The three extra numbers an APU row needs, or `None` — which fails the
/// whole probe — when any of them cannot be read.
struct UnifiedFacts {
    /// Carve-out + GTT: the GPU's admission budget.
    total_mb: u64,
    /// Physical RAM, which is what the GPU's name carries (DP-6).
    ram_mb: u64,
}

/// Read them, warning about whichever one was missing.
///
/// Both reads are **required**, and that is the design decision rather than
/// an implementation detail: falling back to the carve-out alone is exactly
/// the batch-1 collapse the old all-or-nothing APU decline existed to
/// prevent, and falling back to `MemTotal` alone would name the GPU by a
/// figure that moves with the BIOS.
fn unified_facts(
    roots: &SysfsRoots,
    node: u32,
    bdf: &str,
    device: &Path,
    vram_total_mb: u64,
) -> Option<UnifiedFacts> {
    let Some(gtt_total_mb) = read_mb(&device.join("mem_info_gtt_total")).filter(|mb| *mb > 0)
    else {
        tracing::warn!(
            node,
            bdf = %bdf,
            vram_total_mb,
            "this KFD node reports both SIMDs and CPU cores, i.e. an APU, but \
             its mem_info_gtt_total is missing or zero; amdgpu publishes only \
             the BIOS UMA carve-out as such a GPU's VRAM total, so pricing \
             it on that alone would budget every grant against a few hundred \
             MB — leaving the ROCm GPU inventory unknown instead"
        );
        return None;
    };
    let Some(mem_total_mb) = meminfo_mb(&roots.meminfo, "MemTotal") else {
        tracing::warn!(
            node,
            bdf = %bdf,
            meminfo = %roots.meminfo.display(),
            "this KFD node is an APU but MemTotal could not be read; the \
             machine's RAM is that GPU's capacity and its calibration name, \
             so there is nothing to name it with — leaving the ROCm GPU \
             inventory unknown"
        );
        return None;
    };
    Some(UnifiedFacts {
        total_mb: vram_total_mb + gtt_total_mb,
        // The carve-out is reserved by firmware *before* the kernel counts
        // memory, so it is missing from `MemTotal` and adding it back is what
        // makes this figure the machine's RAM rather than a reading of the
        // BIOS setting. DP-6 keeps the carve-out out of the calibration name
        // precisely so that changing it in the BIOS does not split a
        // machine's profiles; taking `MemTotal` raw would embed it
        // negatively and do exactly that. The result still lands a little
        // under the sticker capacity (the kernel reserves a percent or so for
        // itself before reporting `MemTotal`), which costs nothing: the name
        // has to be *deterministic per machine*, not accurate to marketing.
        ram_mb: mem_total_mb + vram_total_mb,
    })
}

/// One `/proc/meminfo` row in whole MiB, or `None`.
///
/// Every memory row there is `"<Key>: <value> kB"` with the value in
/// **kibibytes** despite the spelling (`fs/proc/meminfo.c` renders
/// `pages << (PAGE_SHIFT - 10)`). A row without that unit is not a row this
/// understands, and the honest answer to a file we do not understand is no
/// reading at all.
/// Shared with `cpu.rs`, whose whole GPU is `MemTotal`/`MemAvailable`: the
/// two backends must read the same rows the same way, or one host's free
/// figure would be in a different currency depending on which module produced
/// it.
pub(super) fn meminfo_mb(path: &Path, key: &str) -> Option<u64> {
    let text = fs::read_to_string(path).ok()?;
    text.lines().find_map(|line| {
        let (name, rest) = line.split_once(':')?;
        if name.trim() != key {
            return None;
        }
        let mut fields = rest.split_whitespace();
        let value = fields.next()?.parse::<u64>().ok()?;
        (fields.next() == Some("kB")).then_some(value / 1024)
    })
}

/// `GPU-<16 lower hex>` from a fused `unique_id` — the same string ROCR
/// accepts and rocminfo prints — else the synthetic `GPU-BDF-<bdf>`, which
/// is stable across reboots by bus location. Both satisfy the `GPU-` prefix
/// convention the rest of the system keys by.
fn device_key(unique_id: Option<u64>, bdf: &str) -> String {
    match unique_id {
        Some(id) => format!("GPU-{id:016x}"),
        None => format!("GPU-BDF-{bdf}"),
    }
}

/// Consumer GPUs without a fused serial share a `unique_id` (the kernel
/// only fills it on GFX9+, and not universally). Two GPUs keyed alike
/// would merge into one ledger GPU and mis-price both, so a duplicate
/// demotes **both** carriers to the BDF form rather than picking a winner.
///
/// `None` if a duplicate carrier has no BDF to fall back to. [`identify`]
/// makes that unreachable (a row without a BDF fails the probe there), but
/// the alternative — skipping the row — would leave two GPUs sharing a
/// key, which is precisely the silent ledger merge this function exists to
/// prevent, so the impossible case fails the probe rather than rotting into
/// one.
fn demote_duplicate_ids(mut rows: Vec<GpuInfo>) -> Option<Vec<GpuInfo>> {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for row in &rows {
        *counts.entry(row.uuid.as_str()).or_default() += 1;
    }
    let duplicated: Vec<String> = counts
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .map(|(uuid, _)| uuid.to_owned())
        .collect();
    for row in &mut rows {
        if !duplicated.iter().any(|uuid| uuid == &row.uuid) {
            continue;
        }
        let Some(bdf) = row.bdf.as_deref() else {
            tracing::warn!(
                uuid = %row.uuid,
                "two GPUs report the same KFD unique_id and one has no PCI \
                 address to fall back to; leaving the ROCm GPU inventory \
                 unknown rather than merging them into one ledger GPU"
            );
            return None;
        };
        tracing::warn!(
            uuid = %row.uuid,
            bdf = %bdf,
            "two GPUs report the same KFD unique_id; keying both by PCI \
             address instead"
        );
        row.uuid = format!("GPU-BDF-{bdf}");
    }
    Some(rows)
}

/// Reject a host whose rows do not map one-to-one onto PCI devices.
///
/// MI300-class GPUs can be *partitioned*: one PCI device publishes
/// several KFD nodes (CPX/NPS modes), which the openability filter happily
/// admits as several rows sharing one BDF. amdgpu's VRAM counters are
/// per-**device** — there is no per-partition `mem_info_vram_*` — so every
/// such row would report, and the ledger would budget, the *whole GPU's*
/// memory: an N-way partitioned GPU would be over-admitted N-fold, with
/// each partition's grants invisible to the others. The refresh (D5) has
/// the same problem from the other side, since the shared BDF makes the
/// per-GPU reading ambiguous.
///
/// Partition-aware pricing is a real design (it needs per-partition
/// capacity from `properties`, not from the PCI counters); until it exists,
/// an unpriced host is the correct answer, so the whole probe goes unknown.
fn reject_partitioned_gpus(rows: Vec<GpuInfo>) -> Option<Vec<GpuInfo>> {
    let mut nodes_per_bdf: HashMap<&str, usize> = HashMap::new();
    for row in &rows {
        let bdf = row.bdf.as_deref()?;
        *nodes_per_bdf.entry(bdf).or_default() += 1;
    }
    if let Some((bdf, nodes)) = nodes_per_bdf.into_iter().find(|(_, nodes)| *nodes > 1) {
        tracing::warn!(
            bdf,
            nodes,
            "this PCI device publishes several KFD nodes (a partitioned \
             MI300-class GPU); amdgpu only reports whole-GPU VRAM, so \
             every partition would claim the entire GPU's memory — \
             leaving the ROCm GPU inventory unknown"
        );
        return None;
    }
    Some(rows)
}

/// The deterministic display *and* calibration-profile name (D1.6).
///
/// Derived only from sysfs facts, so it is byte-identical on every host
/// with the same silicon and can never appear, disappear or change with
/// PATH, packaging or an SMI schema bump — which would orphan every local
/// profile, ratchet anchor and knee keyed by it. VRAM separates the
/// gfx-sharing SKUs that matter (a 16 GB and a 24 GB gfx1100 do not price
/// alike). A marketing-name join may be added later as display-only
/// metadata; it must never become the key.
///
/// The VRAM figure rounds to the **nearest** GiB. The direction is a
/// profile-key stability tradeoff — a GPU whose reported total drifted
/// across a `.5` boundary between driver versions would silently start a
/// new profile — but real totals sit hundreds of MB clear of any boundary
/// (24560 → 24, 16368 → 16, 196608 → 192, 8175 → 8), and rounding to
/// nearest keeps the displayed size honest rather than naming a 24 GB
/// GPU 23 GB. Revisit only on field evidence of an actual boundary flip.
fn gpu_name(gfx: &str, total_mb: u64) -> String {
    format!("AMD {gfx} ({} GB)", whole_gb(total_mb))
}

/// The same name for a **unified** GPU: `AMD gfx1151 APU (128 GB)`.
///
/// Three deliberate differences from the discrete form. The literal `APU`,
/// because the same gfx target can appear on both shapes and they do not
/// price alike. The capacity is the **machine's RAM**, never the BIOS UMA
/// carve-out (DP-6): the carve-out is a setting — a user who moves it from
/// 512 MB to 96 GB has the same silicon and the same admission behaviour,
/// and would otherwise orphan every profile, ratchet anchor and knee the
/// machine had written. And the rounding is **up to the nearest 4 GiB**
/// rather than to the nearest GiB.
///
/// That last one is not cosmetic. Adding the carve-out back to `MemTotal`
/// cancels the BIOS knob, but not the *other* reservations the kernel takes
/// before reporting it — firmware/ACPI maps, the crashkernel region, kernel
/// text — which come to something like 1.5–2 GiB on a real machine and are
/// not constant across kernels, boot parameters or a `crashkernel=` change.
/// Rounded to nearest, a 128 GiB box would name itself `(126 GB)` and could
/// silently become `(127 GB)` after a kernel update, orphaning its profiles
/// for the same reason DP-6 keeps the carve-out out. Rounding up to a 4 GiB
/// grid swallows a couple of gigabytes of drift and lands on the capacity
/// the machine is actually sold with, while still separating the sizes that
/// matter (32 vs 64 vs 128 GB). Budgets are untouched — this is the name,
/// i.e. the calibration key, and nothing else.
fn apu_device_name(gfx: &str, ram_mb: u64) -> String {
    format!("AMD {gfx} APU ({} GB)", capacity_gb_up_4(ram_mb))
}

/// MiB to whole GiB, rounded to nearest and never to zero.
fn whole_gb(mb: u64) -> u64 {
    ((mb + 512) / 1024).max(1)
}

/// MiB to GiB, rounded **up** to the next multiple of 4 and never to zero.
///
/// Shared with `cpu.rs`, which names its GPU by the same rule for the same
/// reason (see [`apu_device_name`]): what the kernel counts as total RAM sits
/// a gigabyte or two under the sticker capacity and moves with kernel
/// reservations, so a finer grid would split one machine's calibration
/// profiles across a kernel update.
pub(super) fn capacity_gb_up_4(mb: u64) -> u64 {
    const GRID_MB: u64 = 4 * 1024;
    (mb.div_ceil(GRID_MB) * 4).max(4)
}

/// Decode `gfx_target_version` into the canonical ISA name.
///
/// The kernel packs the compiler target as **`major * 10000 + minor * 100 +
/// stepping`**, all decimal, and the canonical name renders major in
/// decimal followed by minor and stepping as single **hex** digits — which
/// is why `gfx90a` (stepping 10) and `gfx942` look inconsistent as strings
/// but are not. Verified against the kernel's own table in
/// `drivers/gpu/drm/amd/amdkfd/kfd_device.c`
/// (<https://github.com/torvalds/linux/blob/master/drivers/gpu/drm/amd/amdkfd/kfd_device.c>):
/// IP 9.4.2/Aldebaran → 90010 → gfx90a, IP 9.4.3/MI300 → 90402 → gfx942,
/// IP 11.0.0/Navi31 → 110000 → gfx1100, IP 10.3.0 → 100300 → gfx1030.
/// The value is written to sysfs by `kfd_topology.c` as a plain `%u`.
///
/// `None` for 0 (KFD's "unsupported ASIC" value) or anything whose minor or
/// stepping does not fit a hex digit, which would mean the packing changed
/// under us.
fn gfx_name(target: u32) -> Option<String> {
    let major = target / 10000;
    let minor = (target / 100) % 100;
    let stepping = target % 100;
    if major == 0 || minor > 0xf || stepping > 0xf {
        return None;
    }
    Some(format!("gfx{major}{minor:x}{stepping:x}"))
}

/// Derive the PCI address `dddd:bb:dd.f` amdgpu names its sysfs directory
/// with, from the KFD node's `domain` and `location_id`.
///
/// `kfd_topology.c` sets `location_id = pci_dev_id(pdev)`, and
/// `include/linux/pci.h` defines `PCI_DEVID(bus, devfn) = (bus << 8) |
/// devfn` with `devfn = (slot << 3) | func` — so bits 15..8 are the bus,
/// 7..3 the device and 2..0 the function
/// (<https://github.com/torvalds/linux/blob/master/drivers/gpu/drm/amd/amdkfd/kfd_topology.c>,
/// `kfd_topology_add_device`; <https://github.com/torvalds/linux/blob/master/include/linux/pci.h>).
/// `domain` is `pci_domain_nr(pdev->bus)`.
///
/// The function digit is deliberately **not** taken from those low bits.
/// The same kernel line ORs the KFD node id into `location_id` when a
/// device exposes several nodes (`if (gpu->kfd->num_nodes > 1)
/// location_id |= dev->gpu->node_id`), which lands exactly on the function
/// field for partitioned MI300-class GPUs. The amdgpu GPU function is
/// always **.0** — the HDMI/DP audio controller is function **.1 of the
/// same device**, never the GPU's own function — and D3 formats the
/// worker-reported BDF as `.0` for the same reason, so forcing 0 both
/// survives partitioning and keeps the two sides joinable. (An SR-IOV
/// virtual function does live at a nonzero function; forcing 0 there would
/// fabricate an address whose PCI directory does not exist, the VRAM total
/// read fails, and the probe goes unknown — unpriced, which is the safe
/// answer for a passthrough VF anyway.)
///
/// `None` when either field exceeds the width the kernel writes, which
/// would mean the encoding changed and every derived address is suspect.
fn format_bdf(domain: u64, location_id: u64) -> Option<String> {
    if domain > 0xffff || location_id > 0xffff {
        return None;
    }
    let bus = (location_id >> 8) & 0xff;
    let device = (location_id >> 3) & 0x1f;
    Some(format!("{domain:04x}:{bus:02x}:{device:02x}.0"))
}

/// One `key value` pair per whitespace-separated line, as
/// `sysfs_show_32bit_prop`/`sysfs_show_64bit_prop` emit them (`"%s %u\n"` /
/// `"%s %llu\n"`). Unparseable lines are dropped; a *required* key's
/// absence is what decides the outcome, not the line count.
fn parse_properties(text: &str) -> HashMap<String, u64> {
    let mut out = HashMap::new();
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        let (Some(key), Some(value)) = (fields.next(), fields.next()) else {
            continue;
        };
        if let Ok(value) = value.parse::<u64>() {
            out.insert(key.to_owned(), value);
        }
    }
    out
}

/// amdgpu's per-GPU directory under the PCI device root.
///
/// On Linux — the only platform this probe ever runs on, since the `rocm`
/// torch extra carries a `sys_platform == 'linux'` marker — this is plainly
/// `<root>/<bdf>`. A PCI address contains colons, which Windows forbids in
/// a path component, and the fixture tests *do* run there, so the mapping
/// exists purely to keep those fixtures buildable. Be honest about what
/// that buys: on Windows the tests exercise the `':'`→`'-'` branch, which
/// never runs in production; the Linux branch they do *not* exercise is a
/// bare `join`. Both probe and fixtures call this one function, so the two
/// sides can never disagree about where a GPU's directory is.
pub(super) fn pci_device_dir(pci_devices: &Path, bdf: &str) -> PathBuf {
    if cfg!(windows) {
        pci_devices.join(bdf.replace(':', "-"))
    } else {
        pci_devices.join(bdf)
    }
}

/// An amdgpu VRAM counter (bytes, `"%llu\n"`) as whole MiB.
fn read_mb(path: &Path) -> Option<u64> {
    let text = fs::read_to_string(path).ok()?;
    Some(text.trim().parse::<u64>().ok()? / (1024 * 1024))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fixture host: fake KFD nodes, fake `/dev/dri` render nodes (plain
    /// files — on a real host the open is the access test, here it is only
    /// existence + readability) and fake amdgpu PCI directories.
    struct Fixture {
        _dir: tempfile::TempDir,
        roots: SysfsRoots,
    }

    impl Fixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().expect("tempdir");
            let roots = SysfsRoots {
                kfd_nodes: dir.path().join("kfd/nodes"),
                pci_devices: dir.path().join("pci"),
                dev_dri: dir.path().join("dri"),
                meminfo: dir.path().join("meminfo"),
            };
            fs::create_dir_all(&roots.kfd_nodes).unwrap();
            fs::create_dir_all(&roots.pci_devices).unwrap();
            fs::create_dir_all(&roots.dev_dri).unwrap();
            let fixture = Self { _dir: dir, roots };
            // A 128 GiB machine whose 512 MiB carve-out firmware already took
            // out of MemTotal — the shape every APU case here starts from.
            // Written by default so that a *discrete* host proves it never
            // reads the file (its content is irrelevant there); the cases
            // that care overwrite it.
            fixture.meminfo(MEM_TOTAL_KB, MEM_TOTAL_KB / 2)
        }

        /// `/proc/meminfo`, in the kernel's own shape. Only two rows matter,
        /// and the neighbours are here so the parser is exercised against a
        /// file it has to search rather than a single line it could take on
        /// trust.
        fn meminfo(self, total_kb: u64, available_kb: u64) -> Self {
            let body = format!(
                "MemTotal:       {total_kb} kB\nMemFree:         1234567 kB\n\
                 MemAvailable:   {available_kb} kB\nBuffers:           98765 kB\n"
            );
            fs::write(&self.roots.meminfo, body).unwrap();
            self
        }

        fn node(&self, node: u32, props: &[(&str, u64)]) -> &Self {
            let dir = self.roots.kfd_nodes.join(node.to_string());
            fs::create_dir_all(&dir).unwrap();
            let body: String = props
                .iter()
                .map(|(key, value)| format!("{key} {value}\n"))
                .collect();
            fs::write(dir.join("properties"), body).unwrap();
            self
        }

        /// A node whose `properties` cannot be read for a reason that is
        /// *not* "a cgroup hides this GPU" — the case that must fail the
        /// whole probe rather than shift the surviving rows' indices.
        ///
        /// Undecodable bytes are how this is simulated portably. The
        /// obvious alternative — making `properties` a directory — is not:
        /// Windows reports opening a directory as `ERROR_ACCESS_DENIED`,
        /// which `std` maps to `PermissionDenied`, i.e. exactly the *skip*
        /// case, so on this project's dev boxes it would assert the
        /// opposite of what it looks like it asserts. `InvalidData` from
        /// UTF-8 validation is raised by `std` itself and is identical on
        /// every platform.
        fn unreadable_node(&self, node: u32) -> &Self {
            let dir = self.roots.kfd_nodes.join(node.to_string());
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join("properties"), [0x66, 0x6f, 0x6f, 0xff, 0xfe]).unwrap();
            self
        }

        fn render(&self, minor: u64) -> &Self {
            fs::write(self.roots.dev_dri.join(format!("renderD{minor}")), b"").unwrap();
            self
        }

        fn pci(&self, bdf: &str, total_bytes: u64, used_bytes: u64) -> &Self {
            let dir = pci_device_dir(&self.roots.pci_devices, bdf);
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join("mem_info_vram_total"), format!("{total_bytes}\n")).unwrap();
            fs::write(dir.join("mem_info_vram_used"), format!("{used_bytes}\n")).unwrap();
            self
        }

        /// The GTT counters beside them. amdgpu publishes these for discrete
        /// GPUs too — they are only ever *read* for a GPU KFD called an
        /// APU, which is why the fixture may write them anywhere.
        fn gtt(&self, bdf: &str, total_bytes: u64, used_bytes: u64) -> &Self {
            let dir = pci_device_dir(&self.roots.pci_devices, bdf);
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join("mem_info_gtt_total"), format!("{total_bytes}\n")).unwrap();
            fs::write(dir.join("mem_info_gtt_used"), format!("{used_bytes}\n")).unwrap();
            self
        }

        fn build(&self) -> Option<Vec<GpuInfo>> {
            build(&self.roots, [None; VISIBILITY_VARS.len()]).ok()
        }

        /// The failure bucket, for the cases where *why* the host went
        /// unpriced is the thing under test.
        fn bucket(&self) -> Option<&'static str> {
            build(&self.roots, [None; VISIBILITY_VARS.len()])
                .err()
                .map(|failure| failure.bucket)
        }
    }

    const GB24: u64 = 24 * 1024 * 1024 * 1024;
    const GB16: u64 = 16 * 1024 * 1024 * 1024;
    /// A common BIOS UMA carve-out: what amdgpu publishes as an APU's whole
    /// VRAM total, and the number the old decline existed to keep out of the
    /// ledger.
    const CARVE_512M: u64 = 512 * 1024 * 1024;
    /// `MemTotal` on a 128 GiB machine with that carve-out, in kB: firmware
    /// reserves the carve-out before the kernel counts memory, so it is
    /// *missing* from this figure — which is why the probe adds it back
    /// (a real machine also loses a little more to kernel reservations; the
    /// fixture idealizes that away, and the naming rule only needs to be
    /// deterministic per machine, not accurate to the sticker).
    const MEM_TOTAL_KB: u64 = (128 * 1024 - 512) * 1024;
    /// The RAM figure both spellings of that machine must name.
    const RAM_128_MB: u64 = 128 * 1024;

    /// `location_id` from a real gfx1100 node: bus 0x03, device 0x00,
    /// function 0 → `PCI_DEVID(3, 0) = 0x0300`.
    const LOC_03_00: u64 = 0x0300;
    /// bus 0x0c, device 0x00 → `0x0c00`.
    const LOC_0C_00: u64 = 0x0c00;

    fn gpu_props(
        location_id: u64,
        minor: u64,
        unique_id: u64,
        target: u64,
    ) -> Vec<(&'static str, u64)> {
        let mut props: Vec<(&'static str, u64)> = vec![
            ("cpu_cores_count", 0),
            ("simd_count", 192),
            ("gfx_target_version", target),
            ("location_id", location_id),
            ("domain", 0),
            ("drm_render_minor", minor),
        ];
        if unique_id != 0 {
            props.push(("unique_id", unique_id));
        }
        props
    }

    /// The APU shape: the same node, but KFD reports the host's CPU cores on
    /// it, which is the only signal an integrated part has.
    fn apu_props(location_id: u64, minor: u64, target: u64) -> Vec<(&'static str, u64)> {
        let mut props = gpu_props(location_id, minor, 0, target);
        props[0] = ("cpu_cores_count", 16);
        props
    }

    /// The happy path: two dGPUs, both openable, keyed by their fused
    /// serials, indexed in ascending KFD node order.
    #[test]
    fn builds_a_two_gpu_inventory() {
        let fixture = Fixture::new();
        fixture
            .node(0, &[("cpu_cores_count", 32), ("simd_count", 0)])
            .node(1, &gpu_props(LOC_03_00, 128, 0x1122_3344_5566_7788, 110000))
            .node(2, &gpu_props(LOC_0C_00, 129, 0x8877_6655_4433_2211, 90402))
            .render(128)
            .render(129)
            .pci("0000:03:00.0", GB24, 0)
            .pci("0000:0c:00.0", GB16, 0);
        let rows = fixture.build().expect("known");
        assert_eq!(rows.len(), 2, "the CPU node is not a GPU");
        assert_eq!(rows[0].index, 0);
        assert_eq!(rows[0].uuid, "GPU-1122334455667788");
        assert_eq!(rows[0].name, "AMD gfx1100 (24 GB)");
        assert_eq!(rows[0].total_mb, 24 * 1024);
        assert_eq!(rows[0].bdf.as_deref(), Some("0000:03:00.0"));
        assert_eq!(rows[0].gfx_target_version, Some(110000));
        assert_eq!(rows[0].compute_cap, None, "HIP has no compute_cap");
        assert_eq!(rows[1].index, 1);
        assert_eq!(rows[1].uuid, "GPU-8877665544332211");
        assert_eq!(rows[1].name, "AMD gfx942 (16 GB)");
    }

    /// Consumer GPUs have no fused serial (the kernel fills `unique_id`
    /// on GFX9+ only, and not universally), so the key falls back to the
    /// bus location, which is stable across reboots.
    #[test]
    fn absent_or_zero_unique_id_keys_by_pci_address() {
        let fixture = Fixture::new();
        fixture
            .node(1, &gpu_props(LOC_03_00, 128, 0, 110000))
            .node(2, &{
                let mut props = gpu_props(LOC_0C_00, 129, 0, 110000);
                props.push(("unique_id", 0));
                props
            })
            .render(128)
            .render(129)
            .pci("0000:03:00.0", GB24, 0)
            .pci("0000:0c:00.0", GB24, 0);
        let rows = fixture.build().expect("known");
        assert_eq!(rows[0].uuid, "GPU-BDF-0000:03:00.0", "key absent");
        assert_eq!(rows[1].uuid, "GPU-BDF-0000:0c:00.0", "key present but 0");
    }

    /// Same-model cards can report the same `unique_id`. Two ledger GPUs
    /// keyed alike would merge and mis-price both, so both are demoted —
    /// picking a winner would be arbitrary and still wrong for the loser.
    #[test]
    fn duplicate_unique_ids_demote_both_gpus() {
        let fixture = Fixture::new();
        fixture
            .node(1, &gpu_props(LOC_03_00, 128, 0xdead_beef_dead_beef, 110000))
            .node(2, &gpu_props(LOC_0C_00, 129, 0xdead_beef_dead_beef, 110000))
            .render(128)
            .render(129)
            .pci("0000:03:00.0", GB24, 0)
            .pci("0000:0c:00.0", GB24, 0);
        let rows = fixture.build().expect("known");
        assert_eq!(rows[0].uuid, "GPU-BDF-0000:03:00.0");
        assert_eq!(rows[1].uuid, "GPU-BDF-0000:0c:00.0");
    }

    /// A partitioned MI300-class GPU: several KFD nodes behind one PCI
    /// device (the kernel ORs the node id into `location_id`'s function
    /// bits, which is why both rows derive the same `.0` address). amdgpu
    /// publishes only whole-GPU VRAM counters, so pricing each partition
    /// as a GPU would admit the card's memory N times over.
    #[test]
    fn partitions_sharing_one_pci_device_make_the_probe_unknown() {
        let fixture = Fixture::new();
        fixture
            .node(1, &gpu_props(LOC_03_00, 128, 0x1111_1111_1111_1111, 90402))
            .node(
                2,
                &gpu_props(LOC_03_00 | 1, 129, 0x2222_2222_2222_2222, 90402),
            )
            .render(128)
            .render(129)
            .pci("0000:03:00.0", GB24, 0);
        assert!(
            fixture.build().is_none(),
            "two KFD nodes on one PCI device cannot be priced separately"
        );
    }

    /// APU-shaped node: KFD reports SIMDs and an openable render node, but
    /// the amdgpu VRAM counter is missing. All-or-nothing — a hybrid host
    /// loses the whole ledger rather than getting row indices that do not
    /// cover the openable set (and so mean nothing to HIP).
    #[test]
    fn a_gpu_without_a_vram_total_makes_the_probe_unknown() {
        let fixture = Fixture::new();
        fixture
            .node(1, &gpu_props(LOC_03_00, 128, 0, 110000))
            .node(2, &gpu_props(LOC_0C_00, 129, 0, 90012))
            .render(128)
            .render(129)
            .pci("0000:03:00.0", GB24, 0);
        assert!(fixture.build().is_none());
    }

    /// A VRAM total that reads as zero is not an identity: it would name
    /// the GPU `(1 GB)` — the calibration keyspace — and hand the ledger
    /// a GPU with no capacity to divide grants by.
    #[test]
    fn a_zero_vram_total_makes_the_probe_unknown() {
        let fixture = Fixture::new();
        fixture
            .node(1, &gpu_props(LOC_03_00, 128, 0, 110000))
            .render(128)
            .pci("0000:03:00.0", 0, 0);
        assert!(fixture.build().is_none());
    }

    /// A `properties` file we cannot read for any reason other than "a
    /// device cgroup hides this GPU" fails the whole probe: skipping the
    /// node would shift every later row's index, and those indices are the
    /// HIP device numbers a pin selects with.
    #[test]
    fn an_unreadable_properties_file_makes_the_probe_unknown() {
        let fixture = Fixture::new();
        fixture
            .node(1, &gpu_props(LOC_03_00, 128, 0, 110000))
            .unreadable_node(2)
            .node(3, &gpu_props(LOC_0C_00, 129, 0, 110000))
            .render(128)
            .render(129)
            .pci("0000:03:00.0", GB24, 0)
            .pci("0000:0c:00.0", GB24, 0);
        assert!(fixture.build().is_none());
    }

    /// The read-failure discrimination itself. `PermissionDenied` is the
    /// cgroup-hidden case ROCr also cannot see, and is the *only* skip;
    /// everything else — `NotFound` from a hot-unplug race included — fails
    /// the probe. Simulated directly because a `PermissionDenied` read is
    /// not portably constructible on Windows, where these tests run.
    #[test]
    fn only_permission_denied_skips_a_node() {
        assert!(node_hidden_from_this_process(&io::Error::from(
            io::ErrorKind::PermissionDenied
        )));
        for kind in [
            io::ErrorKind::NotFound,
            io::ErrorKind::InvalidData,
            io::ErrorKind::Other,
        ] {
            assert!(
                !node_hidden_from_this_process(&io::Error::from(kind)),
                "{kind:?} must fail the probe, not shift the row indices"
            );
        }
    }

    /// A node that reports no decodable gfx target cannot be *named*, and
    /// the name is the calibration keyspace — a placeholder there would
    /// pollute every profile the host writes.
    #[test]
    fn an_undecodable_gfx_target_makes_the_probe_unknown() {
        let fixture = Fixture::new();
        fixture
            .node(1, &gpu_props(LOC_03_00, 128, 0, 0))
            .render(128)
            .pci("0000:03:00.0", GB24, 0);
        assert!(fixture.build().is_none());
    }

    /// An APU node — KFD's only positive signal being SIMDs *and* CPU cores
    /// on one node — is a **priced unified-memory device**, not a poison pill: total
    /// = carve-out + GTT, name from the machine's RAM, and the unified flag
    /// the ledger's DP-2/DP-5 machinery keys on
    /// (docs/unified-memory-admission.md, backend B).
    ///
    /// The carve-out is what makes this worth a test of its own: amdgpu
    /// publishes it as `mem_info_vram_total` for an iGPU, so a GPU admitted
    /// on the discrete rules would be budgeted against 512 MB and collapse
    /// every grant to batch-1 — which is why this used to fail the whole
    /// probe instead.
    #[test]
    fn an_apu_node_is_a_priced_unified_device() {
        let fixture = Fixture::new();
        fixture
            .node(1, &apu_props(LOC_03_00, 128, 110_501))
            .render(128)
            // The carve-out, and the GTT window the rest of the machine's
            // memory is reachable through: 64 GiB.
            .pci("0000:03:00.0", CARVE_512M, 0)
            .gtt("0000:03:00.0", 64 * 1024 * 1024 * 1024, 0);
        let rows = fixture.build().expect("an APU host is priced now");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].uuid, "GPU-BDF-0000:03:00.0");
        assert_eq!(
            rows[0].total_mb,
            512 + 64 * 1024,
            "the budget is the carve-out plus GTT, not the carve-out"
        );
        assert_eq!(
            rows[0].name, "AMD gfx1151 APU (128 GB)",
            "the machine's RAM, never the BIOS-configurable carve-out (DP-6)"
        );
        assert_eq!(
            rows[0].unified_ram_mb,
            Some(RAM_128_MB),
            "MemTotal plus the carve-out firmware took out of it"
        );
        assert!(rows[0].unified());
        assert_eq!(
            rows[0].vram_carveout_mb,
            Some(512),
            "kept for the either-of registration cross-check"
        );

        // The same machine with the carve-out set to 96 GiB in the BIOS —
        // the tuned Strix Halo shape. `MemTotal` collapses to 32 GiB because
        // firmware took the rest, and the name must not move: DP-6 keeps the
        // carve-out out of the calibration key precisely so that changing it
        // does not orphan every profile, anchor and knee the machine wrote.
        let tuned = Fixture::new().meminfo(32 * 1024 * 1024, 8 * 1024 * 1024);
        tuned
            .node(1, &apu_props(LOC_03_00, 128, 110_501))
            .render(128)
            .pci("0000:03:00.0", 96 * 1024 * 1024 * 1024, 0)
            .gtt("0000:03:00.0", 16 * 1024 * 1024 * 1024, 0);
        let tuned = tuned.build().expect("priced");
        assert_eq!(tuned[0].name, "AMD gfx1151 APU (128 GB)");
        assert_eq!(tuned[0].vram_carveout_mb, Some(96 * 1024));
        assert_eq!(tuned[0].total_mb, 112 * 1024, "the carve-out plus GTT");

        // Neither extra fact is optional. Without GTT the GPU would be
        // priced at its carve-out — the batch-1 collapse — and without
        // MemTotal it could not be named at all; both fail the whole probe,
        // exactly as a missing VRAM total does on a discrete GPU.
        let no_gtt = Fixture::new();
        no_gtt
            .node(1, &apu_props(LOC_03_00, 128, 110_501))
            .render(128)
            .pci("0000:03:00.0", CARVE_512M, 0);
        assert!(no_gtt.build().is_none(), "no GTT total");
        let zero_gtt = Fixture::new();
        zero_gtt
            .node(1, &apu_props(LOC_03_00, 128, 110_501))
            .render(128)
            .pci("0000:03:00.0", CARVE_512M, 0)
            .gtt("0000:03:00.0", 0, 0);
        assert!(zero_gtt.build().is_none(), "a zero GTT total is no GTT");
        let no_meminfo = Fixture::new();
        no_meminfo
            .node(1, &apu_props(LOC_03_00, 128, 110_501))
            .render(128)
            .pci("0000:03:00.0", CARVE_512M, 0)
            .gtt("0000:03:00.0", 64 * 1024 * 1024 * 1024, 0);
        fs::remove_file(&no_meminfo.roots.meminfo).unwrap();
        assert!(no_meminfo.build().is_none(), "no MemTotal");

        // And the normal dGPU shape is untouched, in both spellings: an
        // explicit `cpu_cores_count 0`, and the key absent altogether.
        let explicit_zero = Fixture::new();
        explicit_zero
            .node(1, &gpu_props(LOC_03_00, 128, 0, 110000))
            .render(128)
            .pci("0000:03:00.0", GB24, 0);
        assert!(explicit_zero.build().is_some(), "cpu_cores_count 0");
        let absent = Fixture::new();
        absent
            .node(1, &{
                let mut props = gpu_props(LOC_03_00, 128, 0, 110000);
                props.retain(|(key, _)| *key != "cpu_cores_count");
                props
            })
            .render(128)
            .pci("0000:03:00.0", GB24, 0);
        assert!(
            absent.build().is_some(),
            "an absent cpu_cores_count is the ordinary dGPU shape"
        );
    }

    /// An **absent** `simd_count` is not a CPU node, it is a properties file
    /// we do not understand — and skipping it as one would drop a GPU and
    /// shift every later row's HIP device index. Only an explicit 0 skips.
    #[test]
    fn an_absent_simd_count_makes_the_probe_unknown() {
        let fixture = Fixture::new();
        fixture
            .node(1, &gpu_props(LOC_03_00, 128, 0, 110000))
            .node(2, &{
                let mut props = gpu_props(LOC_0C_00, 129, 0, 110000);
                props.retain(|(key, _)| *key != "simd_count");
                props
            })
            .render(128)
            .render(129)
            .pci("0000:03:00.0", GB24, 0)
            .pci("0000:0c:00.0", GB24, 0);
        assert!(fixture.build().is_none());
        assert_eq!(fixture.bucket(), Some("node reports no simd_count"));
    }

    /// KFD node numbers are sorted **numerically**. A lexicographic sort
    /// puts node 10 between 1 and 2, which renumbers every row — and those
    /// row numbers are the HIP device indices a pin selects with.
    #[test]
    fn node_order_is_numeric_not_lexicographic() {
        let fixture = Fixture::new();
        fixture
            .node(2, &gpu_props(LOC_03_00, 128, 0, 110000))
            .node(9, &gpu_props(LOC_0C_00, 129, 0, 110000))
            .node(10, &gpu_props(0x1000, 130, 0, 110000))
            .render(128)
            .render(129)
            .render(130)
            .pci("0000:03:00.0", GB24, 0)
            .pci("0000:0c:00.0", GB24, 0)
            .pci("0000:10:00.0", GB24, 0);
        let rows = fixture.build().expect("known");
        assert_eq!(
            rows.iter()
                .map(|row| (row.index, row.bdf.as_deref().unwrap_or("")))
                .collect::<Vec<_>>(),
            vec![
                (0, "0000:03:00.0"),
                (1, "0000:0c:00.0"),
                (2, "0000:10:00.0"),
            ],
            "nodes 2, 9, 10 index as 0, 1, 2 — not 10, 2, 9"
        );
    }

    /// The cgroup-hidden skip, end to end through `openable_gpu_nodes`
    /// rather than through the predicate alone: a node whose `properties`
    /// read fails with `PermissionDenied` is excluded and its siblings keep
    /// their positions.
    ///
    /// **Windows only, deliberately.** Reading a *directory* as a file is
    /// `ERROR_ACCESS_DENIED` there, which `std` maps to `PermissionDenied`
    /// — a portable way to produce that exact error kind with no privileges
    /// and no mode bits. The Unix equivalent (a `chmod 000` properties file)
    /// is not portable in the other direction: root ignores the mode, and
    /// these tests run as root in containers. The predicate itself is
    /// covered on every platform by `only_permission_denied_skips_a_node`.
    #[cfg(windows)]
    #[test]
    fn a_cgroup_hidden_node_is_skipped_and_its_siblings_keep_their_positions() {
        let fixture = Fixture::new();
        fixture
            .node(1, &gpu_props(LOC_03_00, 128, 0, 110000))
            .node(3, &gpu_props(LOC_0C_00, 129, 0, 110000))
            .render(128)
            .render(129)
            .pci("0000:03:00.0", GB24, 0)
            .pci("0000:0c:00.0", GB24, 0);
        // Node 2's `properties` is a directory: the read fails with
        // PermissionDenied, i.e. the cgroup-hidden shape.
        let hidden = fixture.roots.kfd_nodes.join("2");
        fs::create_dir_all(hidden.join("properties")).unwrap();
        assert_eq!(
            fs::read_to_string(hidden.join("properties"))
                .expect_err("a directory is not readable as a file")
                .kind(),
            io::ErrorKind::PermissionDenied,
            "the premise of this test"
        );
        let rows = fixture
            .build()
            .expect("the hidden node is skipped, not fatal");
        assert_eq!(
            rows.iter()
                .map(|row| (row.index, row.bdf.as_deref().unwrap_or("")))
                .collect::<Vec<_>>(),
            vec![(0, "0000:03:00.0"), (1, "0000:0c:00.0")]
        );
    }

    /// The openability test is an open for **read+write**, because that is
    /// the access KFD's own device-cgroup check demands. A render node we
    /// can read but not write is one ROCr will refuse, so admitting it would
    /// put a phantom row in the middle of the index space.
    #[test]
    fn a_read_only_render_node_is_excluded() {
        let fixture = Fixture::new();
        fixture
            .node(1, &gpu_props(LOC_03_00, 128, 0, 110000))
            .node(2, &gpu_props(LOC_0C_00, 129, 0, 110000))
            .render(128)
            .render(129)
            .pci("0000:03:00.0", GB24, 0)
            .pci("0000:0c:00.0", GB24, 0);
        let read_only = fixture.roots.dev_dri.join("renderD128");
        let mut perms = fs::metadata(&read_only).unwrap().permissions();
        perms.set_readonly(true);
        fs::set_permissions(&read_only, perms).unwrap();
        // Running with privileges that ignore the mode bits (root in a CI
        // container) defeats the fixture, and there is then nothing to
        // assert — the premise, not the behaviour, is what fails.
        if OpenOptions::new()
            .read(true)
            .write(true)
            .open(&read_only)
            .is_ok()
        {
            return;
        }
        let rows = fixture.build().expect("the writable sibling survives");
        // Restored before the assertions so a failure still leaves a
        // deletable tree: `TempDir`'s drop cannot remove a read-only file on
        // Windows and swallows the error, leaking the fixture.
        let mut perms = fs::metadata(&read_only).unwrap().permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        perms.set_readonly(false);
        fs::set_permissions(&read_only, perms).unwrap();
        assert_eq!(rows.len(), 1, "the read-only node is not an openable GPU");
        assert_eq!(rows[0].index, 0);
        assert_eq!(rows[0].bdf.as_deref(), Some("0000:0c:00.0"));
    }

    /// A container granted a `/dev/dri` subset still sees the whole host
    /// topology. Row indices must be positions within the *openable*
    /// subset, because that is what ROCr enumerates and what HIP indexes.
    #[test]
    fn indices_are_positions_within_the_openable_subset() {
        let fixture = Fixture::new();
        fixture
            .node(1, &gpu_props(LOC_03_00, 128, 0, 110000))
            .node(2, &gpu_props(LOC_0C_00, 129, 0, 110000))
            .node(3, &gpu_props(0x1000, 130, 0, 110000))
            // Only nodes 2 and 3 were granted to this container.
            .render(129)
            .render(130)
            .pci("0000:03:00.0", GB24, 0)
            .pci("0000:0c:00.0", GB24, 0)
            .pci("0000:10:00.0", GB24, 0);
        let rows = fixture.build().expect("known");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].index, 0);
        assert_eq!(rows[0].bdf.as_deref(), Some("0000:0c:00.0"));
        assert_eq!(rows[1].index, 1);
        assert_eq!(rows[1].bdf.as_deref(), Some("0000:10:00.0"));
    }

    #[test]
    fn no_openable_gpu_node_is_unknown() {
        let fixture = Fixture::new();
        fixture
            .node(0, &[("cpu_cores_count", 32), ("simd_count", 0)])
            .node(1, &gpu_props(LOC_03_00, 128, 0, 110000))
            .pci("0000:03:00.0", GB24, 0);
        assert!(fixture.build().is_none(), "render node absent");
        assert_eq!(fixture.bucket(), Some("no openable render node"));
        // A topology with no GPU nodes at all — and, on every non-ROCm host,
        // no topology root to read — is the same answer, under the bucket
        // that tells an operator this is not an amdgpu host at all.
        let empty = Fixture::new();
        assert!(empty.build().is_none());
        assert_eq!(
            empty.bucket(),
            Some("no KFD GPU nodes (this host has no amdgpu topology)")
        );
        assert!(
            build(
                &SysfsRoots {
                    kfd_nodes: empty.roots.kfd_nodes.join("absent"),
                    ..empty.roots.clone()
                },
                [None; VISIBILITY_VARS.len()]
            )
            .is_err()
        );
    }

    /// Any of the four visibility vars blanks the inventory; empty and
    /// comma/whitespace-only values are "not configured", as on CUDA.
    #[test]
    fn ambient_visibility_blanks_the_inventory() {
        let fixture = Fixture::new();
        fixture
            .node(1, &gpu_props(LOC_03_00, 128, 0, 110000))
            .render(128)
            .pci("0000:03:00.0", GB24, 0);
        assert!(fixture.build().is_some(), "baseline is a known host");
        for position in 0..VISIBILITY_VARS.len() {
            let mut ambient = [None; VISIBILITY_VARS.len()];
            ambient[position] = Some("0");
            assert!(
                build(&fixture.roots, ambient).is_err(),
                "{} must blank the inventory",
                VISIBILITY_VARS[position]
            );
            ambient[position] = Some("");
            assert!(build(&fixture.roots, ambient).is_ok());
            ambient[position] = Some(" , ");
            assert!(build(&fixture.roots, ambient).is_ok());
        }
    }

    /// `location_id` is `PCI_DEVID(bus, devfn)`; the function digit is
    /// forced to 0 because the kernel ORs the KFD node id into those bits
    /// on multi-node (partitioned) devices.
    #[test]
    fn derives_the_pci_address_from_location_id_and_domain() {
        assert_eq!(format_bdf(0, 0x0300).as_deref(), Some("0000:03:00.0"));
        assert_eq!(format_bdf(0, 0xc308).as_deref(), Some("0000:c3:01.0"));
        assert_eq!(format_bdf(0x10, 0x0300).as_deref(), Some("0010:03:00.0"));
        // Partitioned MI300: node id 3 ORed into the function bits.
        assert_eq!(format_bdf(0, 0x0300 | 3).as_deref(), Some("0000:03:00.0"));
        // Wider than the kernel writes: the encoding changed under us.
        assert_eq!(format_bdf(0, 0x1_0000), None);
        assert_eq!(format_bdf(0x1_0000, 0x0300), None);
    }

    /// major*10000 + minor*100 + stepping, rendered major-decimal then
    /// minor and stepping as hex digits.
    #[test]
    fn decodes_gfx_target_versions() {
        assert_eq!(gfx_name(110000).as_deref(), Some("gfx1100"));
        assert_eq!(gfx_name(90010).as_deref(), Some("gfx90a"));
        assert_eq!(gfx_name(90402).as_deref(), Some("gfx942"));
        assert_eq!(gfx_name(100300).as_deref(), Some("gfx1030"));
        assert_eq!(gfx_name(90012).as_deref(), Some("gfx90c"));
        assert_eq!(gfx_name(120500).as_deref(), Some("gfx1250"));
        assert_eq!(gfx_name(0), None, "KFD's unsupported-ASIC value");
        assert_eq!(gfx_name(90099), None, "stepping outside a hex digit");
        // The other half of the guard: a minor that no longer fits a hex
        // digit means the packing changed under us, and rendering it anyway
        // would mint a plausible-looking name for the wrong silicon — which
        // is the calibration profile keyspace.
        assert_eq!(gfx_name(92000), None, "minor outside a hex digit");
        assert_eq!(gfx_name(91600), None, "minor 16, the first value past 0xf");
        assert_eq!(gfx_name(91500).as_deref(), Some("gfx9f0"), "minor 15 fits");
    }

    #[test]
    fn gpu_names_round_vram_to_whole_gibibytes() {
        assert_eq!(gpu_name("gfx1100", 24 * 1024), "AMD gfx1100 (24 GB)");
        // 7900 GRE-shaped: a few MB shaved off by carve-outs still names 16.
        assert_eq!(gpu_name("gfx1100", 16368), "AMD gfx1100 (16 GB)");
        assert_eq!(gpu_name("gfx90c", 512), "AMD gfx90c (1 GB)");
        assert_eq!(gpu_name("gfx90c", 0), "AMD gfx90c (1 GB)", "never 0 GB");
    }

    #[test]
    fn parses_kfd_properties() {
        let props = parse_properties(
            "cpu_cores_count 0\nsimd_count 192\nunique_id 18446744073709551615\n\
             garbage\nname_only \nnot_a_number abc\n",
        );
        assert_eq!(props.get("simd_count"), Some(&192));
        assert_eq!(props.get("unique_id"), Some(&u64::MAX));
        assert_eq!(props.get("garbage"), None);
        assert_eq!(props.get("not_a_number"), None);
    }

    /// The staleness refresh reads the same files the worker's free/total
    /// tier does, keyed by the inventory's device key.
    #[test]
    fn reads_live_memory_for_every_gpu() {
        let fixture = Fixture::new();
        fixture
            .pci("0000:03:00.0", GB24, 4 * 1024 * 1024 * 1024)
            .pci("0000:0c:00.0", GB16, 0);
        let gpus = vec![
            discrete("GPU-a", "0000:03:00.0"),
            discrete("GPU-b", "0000:0c:00.0"),
        ];
        let readings =
            query_memory(&fixture.roots.pci_devices, &fixture.roots.meminfo, &gpus).expect("read");
        assert_eq!(
            readings,
            vec![
                GpuMemory {
                    uuid: "GPU-a".to_owned(),
                    total_mb: 24 * 1024,
                    free_mb: 20 * 1024,
                },
                GpuMemory {
                    uuid: "GPU-b".to_owned(),
                    total_mb: 16 * 1024,
                    free_mb: 16 * 1024,
                },
            ]
        );
        // One unreadable GPU makes the whole snapshot unknown, or its
        // external usage would silently price as zero.
        let partial = vec![
            discrete("GPU-a", "0000:03:00.0"),
            discrete("GPU-c", "0000:ff:00.0"),
        ];
        let roots = &fixture.roots;
        assert!(query_memory(&roots.pci_devices, &roots.meminfo, &partial).is_none());
        assert!(query_memory(&roots.pci_devices, &roots.meminfo, &[]).is_none());
    }

    /// A discrete GPU the refresh reads today's two files for.
    fn discrete(key: &str, bdf: &str) -> GpuRef {
        GpuRef {
            key: key.to_owned(),
            bdf: bdf.to_owned(),
            unified: false,
        }
    }

    /// The unified refresh: total is carve-out + GTT, and free is the
    /// carve-out's own free memory plus **as much GTT as RAM can actually
    /// deliver right now**. The clamp is the whole reason `/proc/meminfo` is
    /// read at all — unclaimed GTT is address space, and the pages behind it
    /// have to come from somewhere.
    #[test]
    fn a_unified_devices_reading_clamps_gtt_by_available_ram() {
        let gib = 1024 * 1024 * 1024;
        let gpu = |unified| GpuRef {
            key: "GPU-apu".to_owned(),
            bdf: "0000:03:00.0".to_owned(),
            unified,
        };
        // 512 MiB carve-out (half used), a 64 GiB GTT window with 4 GiB of it
        // taken, and 8 GiB of RAM the OS says it could deliver.
        let fixture = Fixture::new().meminfo(MEM_TOTAL_KB, 8 * 1024 * 1024);
        fixture.pci("0000:03:00.0", CARVE_512M, CARVE_512M / 2).gtt(
            "0000:03:00.0",
            64 * gib,
            4 * gib,
        );
        let roots = &fixture.roots;
        let read = |gpu: GpuRef| {
            query_memory(&roots.pci_devices, &roots.meminfo, &[gpu]).map(|mut r| r.remove(0))
        };
        assert_eq!(
            read(gpu(true)),
            Some(GpuMemory {
                uuid: "GPU-apu".to_owned(),
                total_mb: 512 + 64 * 1024,
                // 256 MiB of carve-out, plus 60 GiB of unclaimed GTT clamped
                // to the 8 GiB the machine has.
                free_mb: 256 + 8 * 1024,
            })
        );
        // Plenty of RAM: the GTT term is the driver's own figure again.
        let roomy = Fixture::new().meminfo(MEM_TOTAL_KB, 100 * 1024 * 1024);
        roomy.pci("0000:03:00.0", CARVE_512M, CARVE_512M / 2).gtt(
            "0000:03:00.0",
            64 * gib,
            4 * gib,
        );
        assert_eq!(
            query_memory(&roomy.roots.pci_devices, &roomy.roots.meminfo, &[gpu(true)])
                .map(|mut r| r.remove(0))
                .map(|reading| reading.free_mb),
            Some(256 + 60 * 1024)
        );
        // The same GPU read as discrete is byte-identical to today: the
        // GTT files are there and are not consulted.
        assert_eq!(
            read(gpu(false)),
            Some(GpuMemory {
                uuid: "GPU-apu".to_owned(),
                total_mb: 512,
                free_mb: 256,
            })
        );
        // All-or-nothing extends to the new files: a unified-memory device whose GTT
        // counters or whose MemAvailable cannot be read makes the whole
        // snapshot unknown rather than reporting the carve-out as the GPU.
        let no_gtt = Fixture::new();
        no_gtt.pci("0000:03:00.0", CARVE_512M, 0);
        assert!(
            query_memory(
                &no_gtt.roots.pci_devices,
                &no_gtt.roots.meminfo,
                &[gpu(true)]
            )
            .is_none()
        );
        let no_meminfo = Fixture::new();
        no_meminfo
            .pci("0000:03:00.0", CARVE_512M, 0)
            .gtt("0000:03:00.0", 64 * gib, 0);
        fs::remove_file(&no_meminfo.roots.meminfo).unwrap();
        assert!(
            query_memory(
                &no_meminfo.roots.pci_devices,
                &no_meminfo.roots.meminfo,
                &[gpu(true)]
            )
            .is_none()
        );
        // …and a *discrete* host never acquires that dependency: no
        // /proc/meminfo, no problem.
        assert!(
            query_memory(
                &no_meminfo.roots.pci_devices,
                &no_meminfo.roots.meminfo,
                &[gpu(false)]
            )
            .is_some()
        );
    }

    /// Only two rows of `/proc/meminfo` are ever read, and only in the shape
    /// the kernel writes them: a value without the `kB` unit is a file we do
    /// not understand, and the honest answer to that is no reading.
    #[test]
    fn parses_the_two_meminfo_rows() {
        let fixture = Fixture::new().meminfo(MEM_TOTAL_KB, 8 * 1024 * 1024);
        let path = &fixture.roots.meminfo;
        assert_eq!(meminfo_mb(path, "MemTotal"), Some(128 * 1024 - 512));
        assert_eq!(meminfo_mb(path, "MemAvailable"), Some(8 * 1024));
        assert_eq!(meminfo_mb(path, "MemShrubbery"), None);
        fs::write(path, "MemTotal:       131928204\nMemAvailable: 8 MB\n").unwrap();
        assert_eq!(meminfo_mb(path, "MemTotal"), None, "no unit");
        assert_eq!(meminfo_mb(path, "MemAvailable"), None, "wrong unit");
        assert_eq!(
            meminfo_mb(&path.join("absent"), "MemTotal"),
            None,
            "no file at all"
        );
    }

    /// A dGPU **and** an APU on one host: two rows, indices covering the
    /// whole openable set (which is what makes them HIP device indices), and
    /// each priced in its own currency. This is the shape the old
    /// all-or-nothing decline sank entirely.
    #[test]
    fn a_dgpu_and_an_apu_are_both_priced() {
        let fixture = Fixture::new();
        fixture
            // KFD lists the APU's node first on such a machine, which is
            // exactly why the decline could not simply skip it.
            .node(1, &apu_props(LOC_03_00, 128, 110_501))
            .node(2, &gpu_props(LOC_0C_00, 129, 0, 110_000))
            .render(128)
            .render(129)
            .pci("0000:03:00.0", CARVE_512M, 0)
            .gtt("0000:03:00.0", 64 * 1024 * 1024 * 1024, 0)
            .pci("0000:0c:00.0", GB24, 0);
        let rows = fixture.build().expect("both GPUs are priced");
        assert_eq!(rows.len(), 2);
        assert_eq!((rows[0].index, rows[1].index), (0, 1));
        assert!(rows[0].unified(), "the APU is row 0, as KFD listed it");
        assert_eq!(rows[0].name, "AMD gfx1151 APU (128 GB)");
        assert_eq!(rows[0].total_mb, 512 + 64 * 1024);
        assert!(!rows[1].unified());
        assert_eq!(rows[1].name, "AMD gfx1100 (24 GB)");
        assert_eq!(rows[1].total_mb, 24 * 1024);
        assert_eq!(rows[1].vram_carveout_mb, None);
    }

    /// The names, side by side: an APU carries the literal `APU` and the
    /// machine's RAM on a 4 GiB grid, a discrete GPU carries its VRAM
    /// rounded to the nearest GiB.
    #[test]
    fn apu_names_carry_the_machines_ram() {
        assert_eq!(
            apu_device_name("gfx1151", 128 * 1024),
            "AMD gfx1151 APU (128 GB)"
        );
        assert_eq!(
            apu_device_name("gfx1013", 16 * 1024),
            "AMD gfx1013 APU (16 GB)"
        );
        // Rounds **up** to the 4 GiB grid, and never to zero: the grid is
        // what absorbs the kernel's own reservations, which are neither
        // small nor constant across kernels and boot parameters.
        assert_eq!(
            apu_device_name("gfx90c", 32 * 1024 - 8),
            "AMD gfx90c APU (32 GB)"
        );
        assert_eq!(
            apu_device_name("gfx90c", 30 * 1024),
            "AMD gfx90c APU (32 GB)"
        );
        assert_eq!(apu_device_name("gfx90c", 0), "AMD gfx90c APU (4 GB)");
        // Sizes that price differently still separate.
        assert_eq!(capacity_gb_up_4(64 * 1024 - 1800), 64);
        assert_eq!(capacity_gb_up_4(32 * 1024 - 1800), 32);
    }

    /// The rounding, end to end and for the reason it exists: a machine's
    /// name must not move when its BIOS carve-out changes (DP-6) **or** when
    /// the kernel's own reservations do. `MemTotal` misses both — firmware
    /// takes the carve-out before the kernel counts memory, and the kernel
    /// then holds back another 1.5–2 GiB for ACPI maps, the crashkernel
    /// region and itself — so the probe adds the carve-out back and rounds
    /// up to a 4 GiB grid.
    #[test]
    fn the_apu_name_survives_carve_out_and_kernel_reservation_changes() {
        // A 128 GiB machine losing 1800 MiB to the kernel, at three BIOS
        // carve-out settings covering the practical range.
        let named = |carve_mb: u64, reserved_mb: u64| {
            let mem_total_mb = 128 * 1024 - carve_mb - reserved_mb;
            let fixture = Fixture::new().meminfo(mem_total_mb * 1024, 8 * 1024 * 1024);
            fixture
                .node(1, &apu_props(LOC_03_00, 128, 110_501))
                .render(128)
                .pci("0000:03:00.0", carve_mb * 1024 * 1024, 0)
                .gtt("0000:03:00.0", 16 * 1024 * 1024 * 1024, 0);
            fixture.build().expect("priced")[0].name.clone()
        };
        for carve_mb in [512, 4 * 1024, 8 * 1024] {
            assert_eq!(
                named(carve_mb, 1800),
                "AMD gfx1151 APU (128 GB)",
                "carve-out {carve_mb} MiB must not change the calibration key"
            );
            // A `crashkernel=1G` added on the next boot moves `MemTotal` by a
            // gigabyte and must not orphan the machine's profiles either.
            assert_eq!(named(carve_mb, 1800 + 1024), "AMD gfx1151 APU (128 GB)");
        }
    }

    /// `/proc/meminfo` is read for **unified** GPUs only. A discrete host
    /// must not acquire a dependency on it — a container with a partial
    /// `/proc`, or any future platform where it is absent, still gets its
    /// full inventory.
    #[test]
    fn a_discrete_host_needs_no_meminfo() {
        let fixture = Fixture::new();
        fixture
            .node(1, &gpu_props(LOC_03_00, 128, 0, 110_000))
            .node(2, &gpu_props(LOC_0C_00, 129, 0, 90402))
            .render(128)
            .render(129)
            .pci("0000:03:00.0", GB24, 0)
            .pci("0000:0c:00.0", GB16, 0);
        fs::remove_file(&fixture.roots.meminfo).unwrap();
        let rows = fixture.build().expect("no meminfo, no problem");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].name, "AMD gfx1100 (24 GB)");
        assert!(rows.iter().all(|row| !row.unified()));
    }
}
