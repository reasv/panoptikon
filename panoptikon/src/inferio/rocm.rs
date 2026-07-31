//! ROCm board inventory and live VRAM, read from kernel sysfs.
//!
//! The CUDA side of `gpu.rs` shells out to nvidia-smi. There is no
//! equivalent here on purpose (docs/rocm-batch-calibration-parity.md, D1):
//! amd-smi/rocm-smi changed JSON shape at ROCm 6.1 and keep drifting, print
//! human text on error paths while still exiting 0, are absent from bare
//! installs, and — decisively — **enumerate in PCI-BDF order while HIP
//! enumerates in KFD topology-node order**, so any ordinal we learned from
//! them would name a different board than the one a pin selects
//! (pytorch#131901). Everything below comes from the interfaces the ROCr
//! runtime itself is built on, which therefore cannot disagree with it:
//!
//! - `/sys/class/kfd/kfd/topology/nodes/<n>/properties` — the KFD topology
//!   ROCr reads to build its agent list, in the same ascending node order;
//! - `/dev/dri/renderD<minor>` — the node ROCr must open to use a board, so
//!   "can I open it" is the same admission test the runtime applies;
//! - `/sys/bus/pci/devices/<bdf>/mem_info_vram_{total,used}` — amdgpu's own
//!   per-board counters, and the *same files* the worker's free/total tier
//!   reads (D4), which is what makes the ledger's one-memory-vocabulary
//!   rule hold by construction rather than by matching two drivers.
//!
//! Everything is a pure function of three injectable roots so the whole
//! probe is testable from fixture directory trees on any platform; only
//! `SysfsRoots::default()` and the caller's `cfg!(target_os = "linux")`
//! gate know that these are Linux paths.
//!
//! Identity is all-or-nothing per host, mirroring the nvidia-smi parser: a
//! board we can open but cannot name or size makes the whole inventory
//! unknown. A *partial* inventory would be worse than none, because a row's
//! index is the `HIP_VISIBLE_DEVICES` value D2 pins with — it only means
//! anything if the rows cover the entire openable set.

use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};

use super::gpu::{GpuInfo, GpuMemory};

/// Every env var that can restrict which boards a HIP process sees.
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

/// The three filesystem roots the probe reads. Injectable so the parse and
/// build logic is exercised against fixture trees rather than hardware
/// (and so those tests run on non-Linux dev machines).
#[derive(Debug, Clone)]
pub(super) struct SysfsRoots {
    /// KFD topology nodes, one numeric subdirectory per node.
    pub kfd_nodes: PathBuf,
    /// PCI device directories, named by lower-case BDF.
    pub pci_devices: PathBuf,
    /// DRM device nodes; `renderD<minor>` is the per-board render node.
    pub dev_dri: PathBuf,
}

impl Default for SysfsRoots {
    fn default() -> Self {
        Self {
            kfd_nodes: PathBuf::from("/sys/class/kfd/kfd/topology/nodes"),
            pci_devices: PathBuf::from("/sys/bus/pci/devices"),
            dev_dri: PathBuf::from("/dev/dri"),
        }
    }
}

/// Why a probe produced no inventory, carried out of [`build`] so the
/// caller can say what was seen rather than leaving a ROCm host silently
/// unpriced.
///
/// A host that finds no boards behaves exactly as it did before this module
/// existed — which is the safe outcome, and also an *invisible* one: the
/// operator sees no ledger, no grants and no explanation. The bucket plus
/// the two counts are the whole diagnosis, and they are what a field report
/// needs to distinguish "this is not a ROCm host at all" from "the container
/// was granted no render nodes" from "the board is partitioned".
///
/// [`Self::log`] is deliberately silent when the deciding site already
/// warned: those lines name the specific node, address or board that
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

/// Build the board inventory, or a [`ProbeFailure`] for "unknown host" —
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
             operator's filtered set, not the host's own board order. That \
             last point is the diagnostic for \"the model ran on a different \
             card than devices = [N] names\": with a ROCR filter in force, \
             index N counts the boards the operator left visible"
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
                "more openable boards than a device index can name",
                gpu_nodes,
                count,
            ));
        };
        let Some(row) = identify(roots, *node, props, index) else {
            return Err(ProbeFailure::logged("identity read failed", gpu_nodes, count));
        };
        rows.push(row);
    }
    let rows = demote_duplicate_ids(rows)
        .ok_or_else(|| ProbeFailure::logged("duplicate board keys", gpu_nodes, count))?;
    reject_partitioned_boards(rows)
        .ok_or_else(|| ProbeFailure::logged("partitioned board", gpu_nodes, count))
}

/// Live free/total for every board, all-or-nothing. One unreadable board
/// makes the whole reading unknown rather than pricing that board's
/// external usage as zero — the same rule the nvidia-smi snapshot parser
/// enforces, for the same reason (phantom headroom).
///
/// `free = total - used` ignores firmware/kernel reserved carve-outs that
/// nvidia-smi's `memory.free` excludes, so ROCm free readings run a few
/// hundred MB optimistic; the ledger's default margin absorbs it.
pub(super) fn query_memory(
    pci_devices: &Path,
    boards: &[(String, String)],
) -> Option<Vec<GpuMemory>> {
    if boards.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(boards.len());
    for (uuid, bdf) in boards {
        let dir = pci_device_dir(pci_devices, bdf);
        let total_mb = read_mb(&dir.join("mem_info_vram_total"))?;
        let used_mb = read_mb(&dir.join("mem_info_vram_used"))?;
        out.push(GpuMemory {
            uuid: uuid.clone(),
            total_mb,
            free_mb: total_mb.saturating_sub(used_mb),
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
/// boards it cannot touch and every pin would land out of range (the worker
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
                     the board from this process, so ROCr will not enumerate \
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
        // silently drop a board and shift every later row's index. Only an
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
        // ROCr still refuses the board — exactly the phantom row this
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
        // An **APU**: KFD models an integrated part as one node carrying
        // both SIMDs and CPU cores, and that combination is the only
        // positive signal there is. It has to be tested here, after the
        // openability filter, because a cgroup-hidden APU is not one ROCr
        // will offer either.
        //
        // The all-or-nothing VRAM rule in `identify` does *not* catch these:
        // amdgpu registers `mem_info_vram_total` for iGPUs too, reporting
        // the BIOS's UMA carve-out (512 MB is a common default). So the host
        // would be priced against the carve-out rather than against the
        // memory an APU can actually reach, every grant would collapse to
        // batch-1, and nothing would say why. Declining to price the host is
        // the design's promised outcome for an APU, so take it explicitly.
        //
        // The node is not *skipped*: HIP still enumerates it, so excluding
        // one row would shift every later row's device index. The whole
        // probe goes unknown instead.
        if props.get("cpu_cores_count").copied().unwrap_or(0) > 0 {
            tracing::warn!(
                node,
                gfx_target_version = props.get("gfx_target_version").copied().unwrap_or(0),
                "this KFD node reports both SIMDs and CPU cores, i.e. an APU; \
                 amdgpu publishes only the BIOS UMA carve-out as its VRAM \
                 total, so pricing this host would budget every grant against \
                 a few hundred MB — leaving the ROCm GPU inventory unknown \
                 instead (an APU host is unpriced by design, not mis-priced)"
            );
            return Err(ProbeFailure::logged("APU node", gpu_nodes, out.len()));
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

/// Turn one openable GPU node into a board row, or `None` to make the whole
/// probe unknown. Board key, name and VRAM total are all identity: a row
/// missing any of them could not key the ledger, the calibration profile
/// or the config, and half-identified boards must never reach either.
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
    let vram_total = pci_device_dir(&roots.pci_devices, &bdf).join("mem_info_vram_total");
    // A zero total is as unusable as an absent one *here*, in the identity
    // pass: it would name the board `… (1 GB)` (the profile keyspace), give
    // the ledger a board with no capacity, and make every grant on it a
    // division by a fiction. The live `query_memory` path stays tolerant of
    // zero on purpose — there it is a reading, not an identity.
    let Some(total_mb) = read_mb(&vram_total).filter(|mb| *mb > 0) else {
        tracing::warn!(
            node,
            bdf = %bdf,
            "cannot read a nonzero mem_info_vram_total for this board (a \
             non-amdgpu node, or a container with a partial /sys, would look \
             like this — an APU is caught earlier, by its own signal); \
             leaving the ROCm GPU inventory unknown"
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
             the ROCm GPU inventory unknown (the board's name is the \
             calibration keyspace and must not be a placeholder)"
        );
        return None;
    };
    let unique_id = props.get("unique_id").copied().filter(|id| *id != 0);
    Some(GpuInfo {
        index,
        // Provisional: `demote_duplicate_ids` rewrites this if two boards
        // fused the same serial.
        uuid: board_key(unique_id, &bdf),
        name: board_name(&gfx, total_mb),
        total_mb,
        // HIP has no compute-capability analogue (D7). The per-row Option
        // already tolerates it; the host's capability view is unknown, so
        // nothing is capability-filtered on ROCm.
        compute_cap: None,
        bdf: Some(bdf),
        gfx_target_version: Some(target),
    })
}

/// `GPU-<16 lower hex>` from a fused `unique_id` — the same string ROCR
/// accepts and rocminfo prints — else the synthetic `GPU-BDF-<bdf>`, which
/// is stable across reboots by bus location. Both satisfy the `GPU-` prefix
/// convention the rest of the system keys by.
fn board_key(unique_id: Option<u64>, bdf: &str) -> String {
    match unique_id {
        Some(id) => format!("GPU-{id:016x}"),
        None => format!("GPU-BDF-{bdf}"),
    }
}

/// Consumer boards without a fused serial share a `unique_id` (the kernel
/// only fills it on GFX9+, and not universally). Two boards keyed alike
/// would merge into one ledger board and mis-price both, so a duplicate
/// demotes **both** carriers to the BDF form rather than picking a winner.
///
/// `None` if a duplicate carrier has no BDF to fall back to. [`identify`]
/// makes that unreachable (a row without a BDF fails the probe there), but
/// the alternative — skipping the row — would leave two boards sharing a
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
                "two boards report the same KFD unique_id and one has no PCI \
                 address to fall back to; leaving the ROCm GPU inventory \
                 unknown rather than merging them into one ledger board"
            );
            return None;
        };
        tracing::warn!(
            uuid = %row.uuid,
            bdf = %bdf,
            "two boards report the same KFD unique_id; keying both by PCI \
             address instead"
        );
        row.uuid = format!("GPU-BDF-{bdf}");
    }
    Some(rows)
}

/// Reject a host whose rows do not map one-to-one onto PCI devices.
///
/// MI300-class boards can be *partitioned*: one PCI device publishes
/// several KFD nodes (CPX/NPS modes), which the openability filter happily
/// admits as several rows sharing one BDF. amdgpu's VRAM counters are
/// per-**device** — there is no per-partition `mem_info_vram_*` — so every
/// such row would report, and the ledger would budget, the *whole board's*
/// memory: an N-way partitioned board would be over-admitted N-fold, with
/// each partition's grants invisible to the others. The refresh (D5) has
/// the same problem from the other side, since the shared BDF makes the
/// per-board reading ambiguous.
///
/// Partition-aware pricing is a real design (it needs per-partition
/// capacity from `properties`, not from the PCI counters); until it exists,
/// an unpriced host is the correct answer, so the whole probe goes unknown.
fn reject_partitioned_boards(rows: Vec<GpuInfo>) -> Option<Vec<GpuInfo>> {
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
             MI300-class board); amdgpu only reports whole-board VRAM, so \
             every partition would claim the entire board's memory — \
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
/// profile-key stability tradeoff — a board whose reported total drifted
/// across a `.5` boundary between driver versions would silently start a
/// new profile — but real totals sit hundreds of MB clear of any boundary
/// (24560 → 24, 16368 → 16, 196608 → 192, 8175 → 8), and rounding to
/// nearest keeps the displayed size honest rather than naming a 24 GB
/// board 23 GB. Revisit only on field evidence of an actual boundary flip.
fn board_name(gfx: &str, total_mb: u64) -> String {
    let gb = ((total_mb + 512) / 1024).max(1);
    format!("AMD {gfx} ({gb} GB)")
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
/// field for partitioned MI300-class boards. The amdgpu GPU function is
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

/// amdgpu's per-board directory under the PCI device root.
///
/// On Linux — the only platform this probe ever runs on, since the `rocm`
/// torch extra carries a `sys_platform == 'linux'` marker — this is plainly
/// `<root>/<bdf>`. A PCI address contains colons, which Windows forbids in
/// a path component, and the fixture tests *do* run there, so the mapping
/// exists purely to keep those fixtures buildable. Be honest about what
/// that buys: on Windows the tests exercise the `':'`→`'-'` branch, which
/// never runs in production; the Linux branch they do *not* exercise is a
/// bare `join`. Both probe and fixtures call this one function, so the two
/// sides can never disagree about where a board's directory is.
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
            };
            fs::create_dir_all(&roots.kfd_nodes).unwrap();
            fs::create_dir_all(&roots.pci_devices).unwrap();
            fs::create_dir_all(&roots.dev_dri).unwrap();
            Self { _dir: dir, roots }
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
        /// *not* "a cgroup hides this board" — the case that must fail the
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

    /// The happy path: two dGPUs, both openable, keyed by their fused
    /// serials, indexed in ascending KFD node order.
    #[test]
    fn builds_a_two_board_inventory() {
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

    /// Consumer boards have no fused serial (the kernel fills `unique_id`
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

    /// Same-model cards can report the same `unique_id`. Two ledger boards
    /// keyed alike would merge and mis-price both, so both are demoted —
    /// picking a winner would be arbitrary and still wrong for the loser.
    #[test]
    fn duplicate_unique_ids_demote_both_boards() {
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

    /// A partitioned MI300-class board: several KFD nodes behind one PCI
    /// device (the kernel ORs the node id into `location_id`'s function
    /// bits, which is why both rows derive the same `.0` address). amdgpu
    /// publishes only whole-board VRAM counters, so pricing each partition
    /// as a board would admit the card's memory N times over.
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
    fn a_board_without_a_vram_total_makes_the_probe_unknown() {
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
    /// the board `(1 GB)` — the calibration keyspace — and hand the ledger
    /// a board with no capacity to divide grants by.
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
    /// device cgroup hides this board" fails the whole probe: skipping the
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

    /// A node reporting both SIMDs and CPU cores is how KFD models an
    /// **APU**, and that combination is the only positive signal there is.
    /// The all-or-nothing VRAM rule does not catch it: amdgpu publishes a
    /// `mem_info_vram_total` for iGPUs too — the BIOS UMA carve-out — so
    /// without this the host would be priced against a few hundred MB and
    /// every grant would collapse to batch-1. Unpriced is the promised
    /// outcome for an APU, so it is taken deliberately.
    #[test]
    fn an_apu_node_makes_the_probe_unknown() {
        let fixture = Fixture::new();
        fixture
            .node(1, &{
                let mut props = gpu_props(LOC_03_00, 128, 0, 90012);
                // KFD reports the host's cores on the APU's own node.
                props[0] = ("cpu_cores_count", 16);
                props
            })
            .render(128)
            // The carve-out: openable, identifiable, and a lie about
            // capacity — which is exactly why the VRAM rule cannot see it.
            .pci("0000:03:00.0", 512 * 1024 * 1024, 0);
        assert!(
            fixture.build().is_none(),
            "an APU's UMA carve-out must not become this host's VRAM budget"
        );
        assert_eq!(fixture.bucket(), Some("APU node"));

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
    /// we do not understand — and skipping it as one would drop a board and
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
        let rows = fixture.build().expect("the hidden node is skipped, not fatal");
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
        assert_eq!(rows.len(), 1, "the read-only node is not an openable board");
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
    fn board_names_round_vram_to_whole_gibibytes() {
        assert_eq!(board_name("gfx1100", 24 * 1024), "AMD gfx1100 (24 GB)");
        // 7900 GRE-shaped: a few MB shaved off by carve-outs still names 16.
        assert_eq!(board_name("gfx1100", 16368), "AMD gfx1100 (16 GB)");
        assert_eq!(board_name("gfx90c", 512), "AMD gfx90c (1 GB)");
        assert_eq!(board_name("gfx90c", 0), "AMD gfx90c (1 GB)", "never 0 GB");
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
    /// tier does, keyed by the inventory's board key.
    #[test]
    fn reads_live_memory_for_every_board() {
        let fixture = Fixture::new();
        fixture
            .pci("0000:03:00.0", GB24, 4 * 1024 * 1024 * 1024)
            .pci("0000:0c:00.0", GB16, 0);
        let boards = vec![
            ("GPU-a".to_owned(), "0000:03:00.0".to_owned()),
            ("GPU-b".to_owned(), "0000:0c:00.0".to_owned()),
        ];
        let readings = query_memory(&fixture.roots.pci_devices, &boards).expect("read");
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
        // One unreadable board makes the whole snapshot unknown, or its
        // external usage would silently price as zero.
        let partial = vec![
            ("GPU-a".to_owned(), "0000:03:00.0".to_owned()),
            ("GPU-c".to_owned(), "0000:ff:00.0".to_owned()),
        ];
        assert!(query_memory(&fixture.roots.pci_devices, &partial).is_none());
        assert!(query_memory(&fixture.roots.pci_devices, &[]).is_none());
    }
}
