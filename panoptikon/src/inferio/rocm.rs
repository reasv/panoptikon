//! ROCm GPU inventory and live VRAM, read from kernel sysfs.
//!
//! Assumes (untested on AMD hardware): ascending KFD topology node order is
//! ROCr's agent order and therefore HIP's device index; a GPU is usable iff
//! its `/dev/dri/renderD<minor>` opens read-write. amd-smi/rocm-smi are not
//! used because they enumerate in PCI order, which is not HIP's. Live memory
//! comes from amdgpu's `mem_info_vram_{total,used}` (plus `gtt` on an APU)
//! and `/proc/meminfo`, bounded by the cgroup limit as for the CPU device.
//!
//! Identity is all-or-nothing per host: a row's index is the
//! `HIP_VISIBLE_DEVICES` value a pin selects with, so it only means anything
//! if the rows cover the whole openable set. See
//! docs/rocm-batch-calibration-parity.md "D1 (G1) — Inventory probe" and
//! docs/unified-memory-admission.md "Backend B: AMD APUs (ROCm)".

use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};

use super::cpu;
use super::gpu::{GpuInfo, GpuMemory};

/// Every env var that can restrict which GPUs a HIP process sees.
/// `ROCR_VISIBLE_DEVICES` filters at the ROCr/KFD layer; the other three at
/// the HIP layer, indexing into that filtered set.
pub(super) const VISIBILITY_VARS: [&str; 4] = [
    "ROCR_VISIBLE_DEVICES",
    "HIP_VISIBLE_DEVICES",
    "CUDA_VISIBLE_DEVICES",
    "GPU_DEVICE_ORDINAL",
];

/// The HIP-layer subset of [`VISIBILITY_VARS`], which a pin of ours would
/// collide with rather than compose with.
const HIP_LAYER_VISIBILITY_VARS: [&str; 3] = [
    "HIP_VISIBLE_DEVICES",
    "CUDA_VISIBLE_DEVICES",
    "GPU_DEVICE_ORDINAL",
];

/// The filesystem paths the probe reads, and how it asks amdgpu whether a
/// GPU is an APU, injectable for fixture trees.
#[derive(Debug, Clone)]
pub(super) struct SysfsRoots {
    /// KFD topology nodes, one numeric subdirectory per node.
    pub kfd_nodes: PathBuf,
    /// PCI device directories, named by lower-case BDF.
    pub pci_devices: PathBuf,
    /// DRM nodes; `renderD<minor>` is the per-GPU render node.
    pub dev_dri: PathBuf,
    /// The KFD device ROCr opens read-write; without it no GPU is usable.
    pub kfd: PathBuf,
    /// Host RAM statistics: `MemTotal` for an APU's identity, and
    /// deliverable RAM for its GTT clamp.
    pub ram: cpu::MemRoots,
    /// [`amdgpu_fusion`], read on each openable render node.
    pub fusion: fn(&fs::File) -> Option<bool>,
}

impl Default for SysfsRoots {
    fn default() -> Self {
        Self {
            kfd_nodes: PathBuf::from("/sys/class/kfd/kfd/topology/nodes"),
            pci_devices: PathBuf::from("/sys/bus/pci/devices"),
            dev_dri: PathBuf::from("/dev/dri"),
            kfd: PathBuf::from("/dev/kfd"),
            ram: cpu::MemRoots::default(),
            fusion: amdgpu_fusion,
        }
    }
}

/// One GPU the live-memory refresh reads. `unified` comes from the probe
/// because `mem_info_gtt_*` exists on discrete GPUs too.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct GpuRef {
    /// The ledger's device key.
    pub key: String,
    /// The PCI address amdgpu names this GPU's sysfs directory with.
    pub bdf: String,
    /// An APU: total and free include GTT.
    pub unified: bool,
}

/// Why a probe produced no inventory, logged so a ROCm host is never
/// silently unpriced.
pub(super) struct ProbeFailure {
    bucket: &'static str,
    gpu_nodes: usize,
    openable: usize,
    already_logged: bool,
}

impl ProbeFailure {
    /// A failure whose deciding site logged nothing.
    fn undiagnosed(bucket: &'static str, gpu_nodes: usize, openable: usize) -> Self {
        Self {
            bucket,
            gpu_nodes,
            openable,
            already_logged: false,
        }
    }

    /// A failure whose deciding site already logged its own line.
    fn logged(bucket: &'static str, gpu_nodes: usize, openable: usize) -> Self {
        Self {
            bucket,
            gpu_nodes,
            openable,
            already_logged: true,
        }
    }

    /// One WARN naming what the probe saw, unless already logged.
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

/// Build the GPU inventory, or a [`ProbeFailure`] for an unknown host.
/// `ambient` holds each [`VISIBILITY_VARS`] value, by position. Any ambient
/// restriction leaves the inventory unknown, since HIP indices count the
/// filtered set and cannot be mapped to KFD nodes. Empty when this process
/// can use no GPU, so ROCr enumerates none and workers run on the CPU.
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
        if gpu_nodes + openable.hidden == 0 {
            tracing::warn!(
                "this host is configured for ROCm but its KFD topology lists no \
                 GPU; models run on the CPU device, priced against RAM"
            );
        } else {
            tracing::warn!(
                gpu_nodes,
                hidden_nodes = openable.hidden,
                kfd_openable = openable.kfd_openable,
                "this process can open none of this host's AMD GPUs, so ROCm \
                 enumerates none and models run on the CPU device, priced \
                 against RAM; to use them, add the user running panoptikon to \
                 the group that owns /dev/kfd and /dev/dri/renderD*, usually \
                 render (in Docker, pass /dev/kfd and /dev/dri, and set \
                 RENDER_GID in deploy/docker-compose.rocm.yml)"
            );
        }
        return Ok(Vec::new());
    }
    let mut rows = Vec::with_capacity(count);
    for (index, (node, props, fusion)) in openable.nodes.iter().enumerate() {
        let Ok(index) = u32::try_from(index) else {
            return Err(ProbeFailure::undiagnosed(
                "more openable GPUs than a device index can name",
                gpu_nodes,
                count,
            ));
        };
        let Some(row) = identify(roots, *node, props, *fusion, index) else {
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

/// Live free/total for every GPU, all-or-nothing: one unreadable GPU makes
/// the whole reading unknown. `free = total - used` does not subtract the
/// driver reservations nvidia-smi's `memory.free` does, so it reads slightly
/// high; the ledger's margin covers that.
///
/// A unified GPU's total is carve-out plus GTT, and its free is
/// `(vram_total − vram_used) + min(gtt_total − gtt_used, deliverable RAM)`,
/// because unclaimed GTT is backed by RAM that may not be free. Deliverable
/// RAM is the CPU device's free reading ([`cpu::ram_available_mb`]): within
/// the cgroup limit too.
pub(super) fn query_memory(
    pci_devices: &Path,
    ram: &cpu::MemRoots,
    gpus: &[GpuRef],
) -> Option<Vec<GpuMemory>> {
    if gpus.is_empty() {
        return None;
    }
    // Read at most once per pass, and only for a unified GPU.
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
            None => *ram_available_mb.insert(cpu::ram_available_mb(ram)?),
        };
        out.push(GpuMemory {
            uuid: gpu.key.clone(),
            total_mb: vram_total_mb + gtt_total_mb,
            free_mb: vram_free_mb + gtt_free_mb.min(available_mb),
        });
    }
    Some(out)
}

/// The first visibility variable that [`is_set`], if any.
fn ambient_restriction(ambient: [Option<&str>; VISIBILITY_VARS.len()]) -> Option<&'static str> {
    VISIBILITY_VARS
        .iter()
        .zip(ambient)
        .find(|(_, value)| is_set(*value))
        .map(|(var, _)| *var)
}

/// Whether any HIP-layer visibility variable is set. A HIP pin composes with
/// `ROCR_VISIBLE_DEVICES` but would override these, so `gpu.rs` writes no pin.
pub(super) fn ambient_hip_restriction(ambient: [Option<&str>; VISIBILITY_VARS.len()]) -> bool {
    VISIBILITY_VARS
        .iter()
        .zip(ambient)
        .any(|(var, value)| HIP_LAYER_VISIBILITY_VARS.contains(var) && is_set(value))
}

/// The first visibility variable that is set but names no device
/// (`HIP_VISIBLE_DEVICES=`), which exposes no GPU at all.
pub(super) fn blank_visibility_var(
    ambient: [Option<&str>; VISIBILITY_VARS.len()],
) -> Option<&'static str> {
    VISIBILITY_VARS
        .iter()
        .zip(ambient)
        .find(|(_, value)| value.is_some() && !is_set(*value))
        .map(|(var, _)| *var)
}

/// Whether the value names at least one entry.
fn is_set(value: Option<&str>) -> bool {
    value.is_some_and(|value| {
        value
            .split(',')
            .map(str::trim)
            .any(|entry| !entry.is_empty())
    })
}

/// The openable GPU nodes, plus how many the topology listed at all.
struct OpenableNodes {
    /// Every KFD node with SIMDs, whether or not it survived the filter.
    gpu_nodes: usize,
    /// Nodes skipped because a device cgroup hides them; not in `gpu_nodes`.
    hidden: usize,
    /// Whether `/dev/kfd` opens read-write; if not, no node survives.
    kfd_openable: bool,
    /// The survivors, in ascending KFD node order, with [`amdgpu_fusion`].
    nodes: Vec<(u32, HashMap<String, u64>, Option<bool>)>,
}

/// GPU nodes this process can open, in ascending KFD node order (assumed to
/// be HIP's order). A container granted a `/dev/dri` subset still lists the
/// whole host topology, but its device cgroup also hides the other GPUs from
/// KFD, so ROCr enumerates exactly the nodes kept here. A render node this
/// process cannot open while KFD still exposes the GPU is different: on ROCm
/// 7.2, with that restriction emulated, ROCr enumerated no GPU at all, which
/// the worker's pin check refuses.
fn openable_gpu_nodes(roots: &SysfsRoots) -> Result<OpenableNodes, ProbeFailure> {
    let mut nodes = node_dirs(&roots.kfd_nodes);
    // Numeric order: a string sort would put node 10 before node 2.
    nodes.sort_by_key(|(node, _)| *node);
    let kfd_openable = opens_read_write(&roots.kfd);
    let mut gpu_nodes = 0usize;
    let mut hidden = 0usize;
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
                hidden += 1;
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
        // CPU nodes report `simd_count 0`; an absent key fails the probe.
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
        if !kfd_openable {
            continue;
        }
        let Some(minor) = props.get("drm_render_minor").copied().filter(|m| *m > 0) else {
            tracing::info!(
                node,
                "KFD GPU node reports no DRM render minor; excluding it from \
                 the ROCm inventory"
            );
            continue;
        };
        let render = match open_render_node(&roots.dev_dri, minor) {
            Ok(render) => render,
            Err(err) => {
                tracing::info!(
                    node,
                    render_minor = minor,
                    error = %err,
                    "KFD GPU node's render node cannot be opened read-write; \
                     excluding it from the ROCm inventory"
                );
                continue;
            }
        };
        let fusion = (roots.fusion)(&render);
        out.push((node, props, fusion));
    }
    Ok(OpenableNodes {
        gpu_nodes,
        hidden,
        kfd_openable,
        nodes: out,
    })
}

/// Open a GPU's render node read-write, as KFD's device-cgroup check
/// requires: a cgroup can grant `r` without `w`, and ROCr then refuses the GPU.
fn open_render_node(dev_dri: &Path, minor: u64) -> io::Result<fs::File> {
    let render = dev_dri.join(format!("renderD{minor}"));
    OpenOptions::new().read(true).write(true).open(render)
}

/// Whether `path` opens read-write, as ROCr opens `/dev/kfd`.
fn opens_read_write(path: &Path) -> bool {
    OpenOptions::new().read(true).write(true).open(path).is_ok()
}

/// Whether amdgpu flags this GPU as an APU: `AMDGPU_IDS_FLAGS_FUSION` in the
/// `AMDGPU_INFO_DEV_INFO` answer on its render node, the flag ROCr reports as
/// integrated. `None` if the query fails.
#[cfg(target_os = "linux")]
fn amdgpu_fusion(render: &fs::File) -> Option<bool> {
    use std::os::fd::AsRawFd;
    /// `DRM_IOW(DRM_COMMAND_BASE + DRM_AMDGPU_INFO, struct drm_amdgpu_info)`
    /// in the generic ioctl encoding (x86_64, aarch64).
    const DRM_IOCTL_AMDGPU_INFO: u32 = 0x4020_6445;
    const AMDGPU_INFO_DEV_INFO: u32 = 0x16;
    const AMDGPU_IDS_FLAGS_FUSION: u64 = 0x1;
    /// `ids_flags` is at byte 136 of `struct drm_amdgpu_info_device`; the
    /// kernel copies at most the size asked for.
    const IDS_FLAGS_WORD: usize = 17;
    /// `struct drm_amdgpu_info`: 32 bytes, the query arguments unused here.
    #[repr(C)]
    struct DrmAmdgpuInfo {
        return_pointer: u64,
        return_size: u32,
        query: u32,
        arguments: [u32; 4],
    }
    const _: () = assert!(size_of::<DrmAmdgpuInfo>() == 32);
    let mut device = [0u64; IDS_FLAGS_WORD + 1];
    let mut request = DrmAmdgpuInfo {
        return_pointer: device.as_mut_ptr() as u64,
        return_size: std::mem::size_of_val(&device) as u32,
        query: AMDGPU_INFO_DEV_INFO,
        arguments: [0; 4],
    };
    // SAFETY: the kernel writes at most `return_size` bytes to
    // `return_pointer`, which `device` provides for the whole call.
    let rc = unsafe {
        libc::ioctl(
            render.as_raw_fd(),
            DRM_IOCTL_AMDGPU_INFO as libc::Ioctl,
            &mut request,
        )
    };
    (rc == 0).then(|| device[IDS_FLAGS_WORD] & AMDGPU_IDS_FLAGS_FUSION != 0)
}

/// No amdgpu off Linux.
#[cfg(not(target_os = "linux"))]
fn amdgpu_fusion(_render: &fs::File) -> Option<bool> {
    None
}

/// Whether a `properties` read error means the node is hidden from this
/// process (skip it) rather than a failure. KFD returns `-EPERM` for a node
/// a device cgroup hides, which ROCr does not enumerate either.
fn node_hidden_from_this_process(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::PermissionDenied
}

/// Numerically-named subdirectories of the topology root; empty if absent.
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

/// Every GPU node in the KFD topology that this process can see, in node
/// order: its ISA name (`gfx1100`) and whether `/dev/kfd` and its render node
/// open read-write, which is what [`build`] admits it by. Quiet and best-effort.
/// Without `check_access` nothing is opened and every GPU reports `false`.
pub(super) fn topology_gpus(roots: &SysfsRoots, check_access: bool) -> Vec<(String, bool)> {
    let mut nodes = node_dirs(&roots.kfd_nodes);
    nodes.sort_by_key(|(node, _)| *node);
    let kfd_openable = check_access && opens_read_write(&roots.kfd);
    nodes
        .into_iter()
        .filter_map(|(_, dir)| {
            let props = parse_properties(&fs::read_to_string(dir.join("properties")).ok()?);
            if props.get("simd_count").copied().unwrap_or(0) == 0 {
                return None;
            }
            let gfx = gfx_name(u32::try_from(*props.get("gfx_target_version")?).ok()?)?;
            let openable = kfd_openable
                && props.get("drm_render_minor").is_some_and(|minor| {
                    *minor > 0 && open_render_node(&roots.dev_dri, *minor).is_ok()
                });
            Some((gfx, openable))
        })
        .collect()
}

/// Turn one openable GPU node into a GPU row, or `None` to make the whole
/// probe unknown. An APU is a unified GPU: total is carve-out plus GTT, and it
/// is named by host RAM. `fusion` is [`amdgpu_fusion`] for its render node.
fn identify(
    roots: &SysfsRoots,
    node: u32,
    props: &HashMap<String, u64>,
    fusion: Option<bool>,
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
    // Zero is rejected here (it names the GPU), unlike in `query_memory`.
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
    let Some(fusion) = fusion else {
        tracing::warn!(
            node,
            bdf = %bdf,
            "amdgpu did not answer whether this GPU is an APU; leaving the \
             ROCm GPU inventory unknown (an APU priced as a discrete GPU \
             would be budgeted against its BIOS carve-out alone)"
        );
        return None;
    };
    // HIP's `integrated`: amdgpu's APU flag, or CPU cores on the GPU's own
    // KFD node (an ACPI CRAT APU). Kernels that build the topology from a
    // virtual CRAT, every GPU since 6.7, put an APU's CPU cores on a node of
    // their own.
    let unified = fusion || props.get("cpu_cores_count").copied().unwrap_or(0) > 0;
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
        // May be rewritten by `demote_duplicate_ids`.
        uuid: device_key(unique_id, &bdf),
        name,
        total_mb,
        // HIP has no compute-capability analogue.
        compute_cap: None,
        bdf: Some(bdf),
        gfx_target_version: Some(target),
        unified_ram_mb: unified.as_ref().map(|facts| facts.ram_mb),
        vram_carveout_mb: unified.as_ref().map(|_| vram_total_mb),
    })
}

/// The extra numbers an APU row needs; `None` fails the whole probe.
struct UnifiedFacts {
    /// Carve-out + GTT.
    total_mb: u64,
    /// Physical RAM, used in the GPU's name.
    ram_mb: u64,
}

/// Read the APU facts, warning about whichever is missing. Both are required.
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
            "this GPU is an APU, but its mem_info_gtt_total is missing or \
             zero; amdgpu publishes only the BIOS UMA carve-out as such a \
             GPU's VRAM total, so pricing it on that alone would budget every \
             grant against a few hundred MB — leaving the ROCm GPU inventory \
             unknown instead"
        );
        return None;
    };
    let Some(mem_total_mb) = meminfo_mb(&roots.ram.meminfo, "MemTotal") else {
        tracing::warn!(
            node,
            bdf = %bdf,
            meminfo = %roots.ram.meminfo.display(),
            "this KFD node is an APU but MemTotal could not be read; the \
             machine's RAM is that GPU's capacity and its calibration name, \
             so there is nothing to name it with — leaving the ROCm GPU \
             inventory unknown"
        );
        return None;
    };
    Some(UnifiedFacts {
        total_mb: vram_total_mb + gtt_total_mb,
        // `MemTotal` excludes the firmware carve-out; adding it back keeps
        // the name stable when the BIOS setting changes.
        ram_mb: mem_total_mb + vram_total_mb,
    })
}

/// RAM the kernel could deliver now, in MiB: `MemAvailable` less
/// `SReclaimable`, slab it counts as available but may not free before it
/// kills a process. `None` without a `MemAvailable` row.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(super) fn ram_deliverable_mb(meminfo: &Path) -> Option<u64> {
    let available = meminfo_mb(meminfo, "MemAvailable")?;
    Some(available.saturating_sub(meminfo_mb(meminfo, "SReclaimable").unwrap_or(0)))
}

/// One `/proc/meminfo` row in whole MiB, or `None`. Rows are
/// `"<Key>: <value> kB"`, where `kB` means KiB; any other unit is rejected.
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

/// `GPU-<16 lower hex>` from a fused `unique_id` (the form rocminfo prints),
/// else the synthetic `GPU-BDF-<bdf>`.
fn device_key(unique_id: Option<u64>, bdf: &str) -> String {
    match unique_id {
        Some(id) => format!("GPU-{id:016x}"),
        None => format!("GPU-BDF-{bdf}"),
    }
}

/// Consumer GPUs without a fused serial can share a `unique_id`, so every
/// duplicate is re-keyed by BDF. `None` if one has no BDF.
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

/// Reject a host whose rows do not map one-to-one onto PCI devices. A
/// partitioned MI300-class GPU publishes several KFD nodes behind one PCI
/// device, while amdgpu's VRAM counters are per device.
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

/// The GPU name, from sysfs facts alone so it is stable: `AMD gfx1100
/// (24 GB)`, VRAM rounded to the nearest GiB.
fn gpu_name(gfx: &str, total_mb: u64) -> String {
    format!("AMD {gfx} ({} GB)", whole_gb(total_mb))
}

/// The name for a unified GPU: `AMD gfx1151 APU (128 GB)`, host RAM rounded
/// up to 4 GiB so kernel reservation changes cannot rename it.
fn apu_device_name(gfx: &str, ram_mb: u64) -> String {
    format!("AMD {gfx} APU ({} GB)", capacity_gb_up_4(ram_mb))
}

/// MiB to whole GiB, rounded to nearest and never to zero.
fn whole_gb(mb: u64) -> u64 {
    ((mb + 512) / 1024).max(1)
}

/// MiB to GiB, rounded up to the next multiple of 4 and never to zero.
pub(super) fn capacity_gb_up_4(mb: u64) -> u64 {
    const GRID_MB: u64 = 4 * 1024;
    (mb.div_ceil(GRID_MB) * 4).max(4)
}

/// Decode `gfx_target_version` (`major * 10000 + minor * 100 + stepping`)
/// into the ISA name: major in decimal, minor and stepping as one hex digit
/// each (`90010` is `gfx90a`). `None` for 0 or a minor/stepping above 15.
pub(super) fn gfx_name(target: u32) -> Option<String> {
    let major = target / 10000;
    let minor = (target / 100) % 100;
    let stepping = target % 100;
    if major == 0 || minor > 0xf || stepping > 0xf {
        return None;
    }
    Some(format!("gfx{major}{minor:x}{stepping:x}"))
}

/// Derive the PCI address `dddd:bb:dd.f` of amdgpu's sysfs directory.
/// `kfd_topology.c` sets `location_id = PCI_DEVID(bus, devfn)`: bits 15..8
/// are the bus, 7..3 the device. The function is written as `.0`: an amdgpu
/// GPU is assumed to be PCI function 0, and bits 2..0 are unusable because
/// the kernel ORs the node id into them on a partitioned device. The worker
/// formats its BDF the same way.
fn format_bdf(domain: u64, location_id: u64) -> Option<String> {
    if domain > 0xffff || location_id > 0xffff {
        return None;
    }
    let bus = (location_id >> 8) & 0xff;
    let device = (location_id >> 3) & 0x1f;
    Some(format!("{domain:04x}:{bus:02x}:{device:02x}.0"))
}

/// One `key value` pair per line, as `sysfs_show_{32,64}bit_prop` emit them.
/// Unparseable lines are dropped.
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

/// amdgpu's per-GPU directory, `<root>/<bdf>`. On Windows (fixture tests
/// only) colons become `-`.
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

    /// A fixture host: fake KFD nodes, fake `/dev/dri` render nodes and fake
    /// amdgpu PCI directories.
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
                kfd: dir.path().join("kfd/device"),
                ram: cpu::MemRoots {
                    meminfo: dir.path().join("meminfo"),
                    cgroup: dir.path().join("cgroup"),
                },
                fusion: fixture_fusion,
            };
            for root in [&roots.kfd_nodes, &roots.pci_devices, &roots.dev_dri] {
                fs::create_dir_all(root).unwrap();
            }
            fs::write(&roots.kfd, "").unwrap();
            // A 128 GiB machine whose 512 MiB carve-out firmware already took
            // out of MemTotal, which the cases that care overwrite.
            Self { _dir: dir, roots }.meminfo(MEM_TOTAL_KB, MEM_TOTAL_KB / 2)
        }

        /// `/proc/meminfo`, in the kernel's own shape: the neighbours are
        /// here so the parser has to search rather than take a line on
        /// trust.
        fn meminfo(self, total_kb: u64, available_kb: u64) -> Self {
            let body = format!(
                "MemTotal:       {total_kb} kB\nMemFree:         1234567 kB\n\
                 MemAvailable:   {available_kb} kB\nBuffers:           98765 kB\n"
            );
            fs::write(&self.roots.ram.meminfo, body).unwrap();
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
        /// whole probe. Undecodable bytes simulate it portably: making
        /// `properties` a directory would be `PermissionDenied` on Windows,
        /// i.e. exactly the *skip* case.
        fn unreadable_node(&self, node: u32) -> &Self {
            let dir = self.roots.kfd_nodes.join(node.to_string());
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join("properties"), [0x66, 0x6f, 0x6f, 0xff, 0xfe]).unwrap();
            self
        }

        fn render(&self, minor: u64) -> &Self {
            self.render_answering(minor, "")
        }

        /// A render node whose amdgpu query answers per [`fixture_fusion`].
        fn render_answering(&self, minor: u64, answer: &str) -> &Self {
            fs::write(self.roots.dev_dri.join(format!("renderD{minor}")), answer).unwrap();
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
        /// GPUs too, so the fixture may write them anywhere.
        fn gtt(&self, bdf: &str, total_bytes: u64, used_bytes: u64) -> &Self {
            let dir = pci_device_dir(&self.roots.pci_devices, bdf);
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join("mem_info_gtt_total"), format!("{total_bytes}\n")).unwrap();
            fs::write(dir.join("mem_info_gtt_used"), format!("{used_bytes}\n")).unwrap();
            self
        }

        /// A discrete GPU in one call: KFD node, render node, VRAM counters.
        fn dgpu(&self, node: u32, location_id: u64, minor: u64, total_bytes: u64) -> &Self {
            let bdf = format_bdf(0, location_id).expect("a fixture address");
            self.node(node, &gpu_props(location_id, minor, 0, 110000))
                .render(minor)
                .pci(&bdf, total_bytes, 0)
        }

        /// An APU in one call, as a virtual-CRAT kernel lists it: a GPU node
        /// with no CPU cores whose amdgpu query reports the APU flag, plus its
        /// GTT window.
        fn apu(&self, node: u32, location_id: u64, minor: u64, carve: u64, gtt: u64) -> &Self {
            let bdf = format_bdf(0, location_id).expect("a fixture address");
            self.node(node, &gpu_props(location_id, minor, 0, 110_501))
                .render_answering(minor, "fusion")
                .pci(&bdf, carve, 0)
                .gtt(&bdf, gtt, 0)
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

    /// The amdgpu APU query on a fixture render node: its content is
    /// `fusion` for an APU, `unanswered` for a failed query.
    fn fixture_fusion(render: &fs::File) -> Option<bool> {
        let mut answer = String::new();
        io::Read::read_to_string(&mut &*render, &mut answer).unwrap();
        (answer != "unanswered").then_some(answer == "fusion")
    }

    fn set_readonly(path: &Path, value: bool) {
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_readonly(value);
        fs::set_permissions(path, perms).unwrap();
    }

    const GB24: u64 = 24 * 1024 * 1024 * 1024;
    const GB16: u64 = 16 * 1024 * 1024 * 1024;
    /// A common BIOS UMA carve-out: what amdgpu publishes as an APU's whole
    /// VRAM total.
    const CARVE_512M: u64 = 512 * 1024 * 1024;
    /// `MemTotal` on a 128 GiB machine with that carve-out, in kB: firmware
    /// takes the carve-out before the kernel counts memory, so it is
    /// *missing* here and the probe adds it back.
    const MEM_TOTAL_KB: u64 = (128 * 1024 - 512) * 1024;
    /// The RAM figure both spellings of that machine must name.
    const RAM_128_MB: u64 = 128 * 1024;

    /// `location_id` from a real gfx1100 node: bus 0x03, device 0x00,
    /// function 0 → `PCI_DEVID(3, 0) = 0x0300`.
    const LOC_03_00: u64 = 0x0300;
    /// bus 0x0c, device 0x00 → `0x0c00`.
    const LOC_0C_00: u64 = 0x0c00;
    const LOC_10_00: u64 = 0x1000;
    const BDF_03: &str = "0000:03:00.0";
    const BDF_0C: &str = "0000:0c:00.0";
    const KEY_03: &str = "GPU-BDF-0000:03:00.0";
    const KEY_0C: &str = "GPU-BDF-0000:0c:00.0";
    const GIB: u64 = 1024 * 1024 * 1024;
    const GTT_64G: u64 = 64 * GIB;
    const APU_128: &str = "AMD gfx1151 APU (128 GB)";

    #[rustfmt::skip]
    fn gpu_props(loc: u64, minor: u64, id: u64, target: u64) -> Vec<(&'static str, u64)> {
        let mut props = vec![
            ("cpu_cores_count", 0), ("simd_count", 192),
            ("gfx_target_version", target), ("location_id", loc),
            ("domain", 0), ("drm_render_minor", minor),
        ];
        if id != 0 {
            props.push(("unique_id", id));
        }
        props
    }

    /// The ACPI CRAT APU shape: the same node, but KFD reports the host's CPU
    /// cores on it.
    fn apu_props(location_id: u64, minor: u64, target: u64) -> Vec<(&'static str, u64)> {
        let mut props = gpu_props(location_id, minor, 0, target);
        props[0] = ("cpu_cores_count", 16);
        props
    }

    /// The happy path plus the keying rules: two dGPUs indexed in ascending
    /// KFD node order and keyed by their fused serials. A GPU with no serial
    /// keys by bus location instead, and a *duplicate* serial demotes
    /// **both** carriers.
    #[test]
    fn builds_a_two_gpu_inventory_keyed_by_serial_or_address() {
        let fixture = Fixture::new();
        fixture
            .node(0, &[("cpu_cores_count", 32), ("simd_count", 0)])
            .node(1, &gpu_props(LOC_03_00, 128, 0x1122_3344_5566_7788, 110000))
            .node(2, &gpu_props(LOC_0C_00, 129, 0x8877_6655_4433_2211, 90402))
            .render(128)
            .render(129)
            .pci(BDF_03, GB24, 0)
            .pci(BDF_0C, GB16, 0);
        let rows = fixture.build().expect("known");
        assert_eq!(rows.len(), 2, "the CPU node is not a GPU");
        assert_eq!(rows[0].bdf.as_deref(), Some(BDF_03));
        assert_eq!(rows[0].total_mb, 24 * 1024);
        assert_eq!(rows[0].gfx_target_version, Some(110000));
        assert_eq!(rows[0].compute_cap, None, "HIP has no compute_cap");
        #[rustfmt::skip]
        let identities = [
            (0u32, "GPU-1122334455667788", "AMD gfx1100 (24 GB)"),
            (1, "GPU-8877665544332211", "AMD gfx942 (16 GB)"),
        ];
        for (row, (index, uuid, name)) in rows.iter().zip(identities) {
            assert_eq!(
                (row.index, row.uuid.as_str(), row.name.as_str()),
                (index, uuid, name)
            );
        }

        // (serial on node 1, serial on node 2); `Some(0)` is the explicit
        // zero spelling, `None` the absent one.
        for (first, second, label) in [
            (None, Some(0), "absent, then an explicit 0"),
            (
                Some(0xdead_beef_dead_beef),
                Some(0xdead_beef_dead_beef),
                "duplicated",
            ),
        ] {
            let props = |location_id, minor, serial: Option<u64>| {
                let mut props = gpu_props(location_id, minor, serial.unwrap_or(0), 110000);
                if serial == Some(0) {
                    props.push(("unique_id", 0));
                }
                props
            };
            let fixture = Fixture::new();
            fixture
                .node(1, &props(LOC_03_00, 128, first))
                .node(2, &props(LOC_0C_00, 129, second))
                .render(128)
                .render(129)
                .pci(BDF_03, GB24, 0)
                .pci(BDF_0C, GB24, 0);
            let rows = fixture.build().expect("known");
            assert_eq!(rows[0].uuid, KEY_03, "{label}");
            assert_eq!(rows[1].uuid, KEY_0C, "{label}");
        }
    }

    /// Identity is all-or-nothing per host: anything that leaves one openable
    /// node unidentifiable blanks the whole inventory, because a row's index
    /// is the HIP device number a pin selects with. A partitioned MI300-class
    /// GPU is here for a second reason — several KFD nodes behind one PCI
    /// device would each claim the whole card's VRAM.
    #[test]
    fn an_unidentifiable_node_makes_the_whole_probe_unknown() {
        // Each case builds on one good node 1 (24 GB gfx1100 at 03:00.0).
        let without = |key: &'static str| {
            let mut props = gpu_props(LOC_0C_00, 129, 0, 110000);
            props.retain(|(name, _)| *name != key);
            props
        };
        /// One way of breaking the fixture host, and what to call it.
        type Case<'a> = (&'a str, &'a dyn Fn(&Fixture));
        #[rustfmt::skip]
        let cases: [Case; 6] = [
            ("no VRAM counters", &|f: &Fixture| {
                f.node(2, &gpu_props(LOC_0C_00, 129, 0, 90012)).render(129);
            }),
            ("a VRAM total that reads as zero", &|f: &Fixture| { f.dgpu(2, LOC_0C_00, 129, 0); }),
            ("an undecodable gfx target", &|f: &Fixture| {
                f.node(2, &gpu_props(LOC_0C_00, 129, 0, 0)).render(129).pci(BDF_0C, GB24, 0);
            }),
            ("unreadable properties", &|f: &Fixture| { f.unreadable_node(2); }),
            ("an absent simd_count", &|f: &Fixture| {
                f.node(2, &without("simd_count")).render(129).pci(BDF_0C, GB24, 0);
            }),
            ("a partitioned GPU on one PCI device", &|f: &Fixture| {
                f.node(2, &gpu_props(LOC_03_00 | 1, 129, 0, 90402)).render(129);
            }),
        ];
        for (label, add) in cases {
            let fixture = Fixture::new();
            fixture.dgpu(1, LOC_03_00, 128, GB24);
            assert!(fixture.build().is_some(), "{label}: baseline is known");
            add(&fixture);
            assert!(fixture.build().is_none(), "{label}");
        }
        // An absent `simd_count` is a file we do not understand, not a CPU
        // node, and the bucket says so.
        let absent_simd = Fixture::new();
        absent_simd
            .node(1, &without("simd_count"))
            .render(129)
            .pci(BDF_0C, GB24, 0);
        assert_eq!(absent_simd.bucket(), Some("node reports no simd_count"));
    }

    /// `PermissionDenied` is the cgroup-hidden case ROCr also cannot see and
    /// is the *only* skip; everything else, `NotFound` included, fails the
    /// probe. Tested on the predicate because the read is not portably
    /// constructible.
    #[test]
    fn only_permission_denied_skips_a_node() {
        use io::ErrorKind::{InvalidData, NotFound, Other, PermissionDenied};
        for (kind, skipped) in [
            (PermissionDenied, true),
            (NotFound, false),
            (InvalidData, false),
            (Other, false),
        ] {
            assert_eq!(
                node_hidden_from_this_process(&io::Error::from(kind)),
                skipped,
                "{kind:?}: everything but PermissionDenied fails the probe"
            );
        }
    }

    /// An APU is a **priced unified device**: total is carve-out + GTT,
    /// named from the machine's RAM. amdgpu publishes the carve-out as
    /// `mem_info_vram_total`, so the discrete rules would budget it against
    /// 512 MB and collapse every grant to batch-1
    /// (docs/unified-memory-admission.md, backend B). An APU is what HIP
    /// calls integrated: amdgpu's APU flag on a GPU node beside a separate
    /// CPU node (virtual CRAT, every kernel since 6.7), or one node with both
    /// SIMDs and CPU cores (ACPI CRAT).
    #[test]
    fn an_apu_node_is_a_priced_unified_device() {
        let fixture = Fixture::new();
        fixture
            .node(0, &[("cpu_cores_count", 16), ("simd_count", 0)])
            .apu(1, LOC_03_00, 128, CARVE_512M, GTT_64G);
        let rows = fixture.build().expect("an APU host is priced now");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].uuid, KEY_03);
        assert!(rows[0].unified());
        assert_eq!(rows[0].unified_ram_mb, Some(RAM_128_MB));
        let budget = 512 + 64 * 1024;
        assert_eq!(rows[0].total_mb, budget, "the budget is carve-out + GTT");
        assert_eq!(
            rows[0].name, APU_128,
            "the machine's RAM, never the BIOS-configurable carve-out"
        );
        assert_eq!(
            rows[0].vram_carveout_mb,
            Some(512),
            "kept for the either-of registration cross-check"
        );

        // The same machine with a 96 GiB BIOS carve-out, where `MemTotal`
        // collapses to 32 GiB because firmware took the rest.
        let tuned = Fixture::new().meminfo(32 * 1024 * 1024, 8 * 1024 * 1024);
        tuned.apu(1, LOC_03_00, 128, 96 * GIB, 16 * GIB);
        let tuned = tuned.build().expect("priced");
        assert_eq!(tuned[0].name, APU_128);
        assert_eq!(
            (tuned[0].vram_carveout_mb, tuned[0].total_mb),
            (Some(96 * 1024), 112 * 1024)
        );

        // The ACPI CRAT shape, with no APU flag, is the same device.
        let single_node = Fixture::new();
        single_node
            .node(1, &apu_props(LOC_03_00, 128, 110_501))
            .render(128)
            .pci(BDF_03, CARVE_512M, 0)
            .gtt(BDF_03, GTT_64G, 0);
        assert_eq!(single_node.build(), Some(rows));

        // Neither extra fact is optional: without GTT the GPU is priced at
        // its carve-out, and without MemTotal it cannot be named. Nor is the
        // APU flag: unanswered, an APU and a discrete GPU look alike.
        #[rustfmt::skip]
        let missing = [
            (None, false, "no GTT total"), (Some(0), false, "a zero GTT total"),
            (Some(GTT_64G), true, "no MemTotal"),
        ];
        for (gtt_bytes, drop_meminfo, label) in missing {
            let fixture = Fixture::new();
            fixture.apu(1, LOC_03_00, 128, CARVE_512M, gtt_bytes.unwrap_or(0));
            let gtt_total =
                pci_device_dir(&fixture.roots.pci_devices, BDF_03).join("mem_info_gtt_total");
            if gtt_bytes.is_none() {
                fs::remove_file(gtt_total).unwrap();
            }
            if drop_meminfo {
                fs::remove_file(&fixture.roots.ram.meminfo).unwrap();
            }
            assert!(fixture.build().is_none(), "{label}");
        }
        let unanswered = Fixture::new();
        unanswered
            .apu(1, LOC_03_00, 128, CARVE_512M, GTT_64G)
            .render_answering(128, "unanswered");
        assert!(unanswered.build().is_none(), "no APU flag");

        // A dGPU **and** an APU on one host. KFD lists the APU's node first
        // on such a machine, so the old decline could not simply skip it;
        // both are priced, each in its own currency.
        let mixed = Fixture::new();
        mixed
            .apu(1, LOC_03_00, 128, CARVE_512M, GTT_64G)
            .dgpu(2, LOC_0C_00, 129, GB24);
        let rows = mixed.build().expect("both GPUs are priced");
        assert_eq!((rows.len(), rows[0].index, rows[1].index), (2, 0, 1));
        assert!(rows[0].unified(), "the APU is row 0, as KFD listed it");
        assert_eq!(
            (rows[0].total_mb, rows[1].total_mb),
            (512 + 64 * 1024, 24 * 1024)
        );
        assert!(!rows[1].unified() && rows[1].vram_carveout_mb.is_none());

        // The dGPU shape is untouched in both spellings: an explicit
        // `cpu_cores_count 0`, and the key absent altogether.
        for absent in [false, true] {
            let mut props = gpu_props(LOC_03_00, 128, 0, 110000);
            props.retain(|(key, _)| !absent || *key != "cpu_cores_count");
            let fixture = Fixture::new();
            fixture.node(1, &props).render(128).pci(BDF_03, GB24, 0);
            assert!(fixture.build().is_some(), "absent: {absent}");
        }
    }

    /// Row indices are positions within the **openable** subset, in ascending
    /// *numeric* node order — what ROCr enumerates and HIP indexes. A
    /// lexicographic sort would put node 10 between 1 and 2, and a container
    /// granted part of `/dev/dri` still sees the whole topology.
    #[test]
    fn indices_are_numeric_positions_within_the_openable_subset() {
        let fixture = Fixture::new();
        fixture
            .dgpu(2, LOC_03_00, 128, GB24)
            .dgpu(9, LOC_0C_00, 129, GB24)
            .dgpu(10, LOC_10_00, 130, GB24);
        let indexed = |rows: Vec<GpuInfo>| {
            rows.iter()
                .map(|row| (row.index, row.bdf.clone().unwrap_or_default()))
                .collect::<Vec<_>>()
        };
        let at = |index, bdf: &str| (index, bdf.to_owned());
        assert_eq!(
            indexed(fixture.build().expect("known")),
            vec![at(0, BDF_03), at(1, BDF_0C), at(2, "0000:10:00.0")],
            "nodes 2, 9, 10 index as 0, 1, 2 — not 10, 2, 9"
        );

        // Only two of the three render nodes granted to this container.
        let subset = Fixture::new();
        subset
            .node(1, &gpu_props(LOC_03_00, 128, 0, 110000))
            .pci(BDF_03, GB24, 0)
            .dgpu(2, LOC_0C_00, 129, GB24)
            .dgpu(3, LOC_10_00, 130, GB24);
        assert_eq!(
            indexed(subset.build().expect("known")),
            vec![at(0, BDF_0C), at(1, "0000:10:00.0")]
        );
    }

    /// The two exclusion paths of the openability filter, end to end.
    ///
    /// A node whose `properties` read fails with `PermissionDenied` is the
    /// cgroup-hidden shape: it is skipped and its siblings keep their
    /// positions. **Windows only**, because reading a directory as a file is
    /// the one portable way to produce that error kind with no mode bits; the
    /// predicate itself is covered everywhere by
    /// `only_permission_denied_skips_a_node`.
    ///
    /// A render node we can read but not write is one ROCr will refuse, since
    /// read+write is what KFD's own device-cgroup check demands; admitting it
    /// would put a phantom row in the middle of the index space.
    #[test]
    fn an_unopenable_node_is_excluded_and_the_rest_keep_their_positions() {
        let indexed = |rows: Vec<GpuInfo>| {
            rows.iter()
                .map(|row| (row.index, row.bdf.clone().unwrap_or_default()))
                .collect::<Vec<_>>()
        };
        let at = |index, bdf: &str| (index, bdf.to_owned());

        #[cfg(windows)]
        {
            let fixture = Fixture::new();
            fixture
                .dgpu(1, LOC_03_00, 128, GB24)
                .dgpu(3, LOC_0C_00, 129, GB24);
            // Node 2's `properties` is a directory, so the read fails with
            // PermissionDenied — the cgroup-hidden shape.
            let hidden = fixture.roots.kfd_nodes.join("2/properties");
            fs::create_dir_all(&hidden).unwrap();
            assert_eq!(
                fs::read_to_string(&hidden)
                    .expect_err("not readable")
                    .kind(),
                io::ErrorKind::PermissionDenied,
                "the premise of this test"
            );
            let rows = fixture.build().expect("the hidden node is skipped");
            assert_eq!(indexed(rows), vec![at(0, BDF_03), at(1, BDF_0C)]);
        }

        let fixture = Fixture::new();
        fixture
            .dgpu(1, LOC_03_00, 128, GB24)
            .dgpu(2, LOC_0C_00, 129, GB24);
        let read_only = fixture.roots.dev_dri.join("renderD128");
        set_readonly(&read_only, true);
        // Privileges that ignore the mode bits (root in a container) defeat
        // the fixture: the premise fails, not the behaviour.
        let writable = OpenOptions::new().read(true).write(true).open(&read_only);
        if writable.is_ok() {
            set_readonly(&read_only, false);
            return;
        }
        let rows = fixture.build().expect("the writable sibling survives");
        // Restored before the assertions so a failure still leaves a
        // deletable tree: `TempDir`'s drop leaks a read-only file.
        set_readonly(&read_only, false);
        assert_eq!(indexed(rows), vec![at(0, BDF_0C)], "one openable GPU");
    }

    /// GPU nodes this process cannot open (no render node, or no read-write
    /// `/dev/kfd`), no GPU nodes, no topology, and every GPU node hidden by a
    /// device cgroup: ROCr enumerates nothing, so the inventory is known empty.
    #[test]
    fn no_usable_gpu_node_is_an_empty_inventory() {
        let fixture = Fixture::new();
        fixture
            .node(0, &[("cpu_cores_count", 32), ("simd_count", 0)])
            .node(1, &gpu_props(LOC_03_00, 128, 0, 110000))
            .pci(BDF_03, GB24, 0);
        assert_eq!(fixture.build(), Some(Vec::new()));
        let no_kfd = Fixture::new();
        no_kfd.dgpu(1, LOC_03_00, 128, GB24);
        fs::remove_file(&no_kfd.roots.kfd).unwrap();
        assert_eq!(no_kfd.build(), Some(Vec::new()));
        assert_eq!(
            topology_gpus(&no_kfd.roots, true),
            [("gfx1100".to_owned(), false)]
        );
        let read_only_kfd = Fixture::new();
        read_only_kfd.dgpu(1, LOC_03_00, 128, GB24);
        let kfd = &read_only_kfd.roots.kfd;
        set_readonly(kfd, true);
        // Skipped where privileges ignore the mode bits (root).
        if OpenOptions::new().read(true).write(true).open(kfd).is_err() {
            let rows = read_only_kfd.build();
            let topology = topology_gpus(&read_only_kfd.roots, true);
            set_readonly(kfd, false);
            assert_eq!(rows, Some(Vec::new()));
            assert_eq!(topology, [("gfx1100".to_owned(), false)]);
        } else {
            set_readonly(kfd, false);
        }
        let empty = Fixture::new();
        assert_eq!(empty.build(), Some(Vec::new()));
        let rootless = SysfsRoots {
            kfd_nodes: empty.roots.kfd_nodes.join("absent"),
            ..empty.roots.clone()
        };
        assert_eq!(
            build(&rootless, [None; VISIBILITY_VARS.len()]).ok(),
            Some(Vec::new())
        );
        // Mode bits produce the denial unless privileges ignore them.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let hidden = Fixture::new();
            hidden
                .node(0, &[("cpu_cores_count", 32), ("simd_count", 0)])
                .dgpu(1, LOC_03_00, 128, GB24);
            let props = hidden.roots.kfd_nodes.join("1/properties");
            fs::set_permissions(&props, fs::Permissions::from_mode(0o000)).unwrap();
            if fs::read_to_string(&props).is_err() {
                assert_eq!(hidden.build(), Some(Vec::new()));
            }
        }
    }

    /// Any of the four visibility vars blanks the inventory; empty and
    /// comma/whitespace-only values are "not configured", as on CUDA.
    #[test]
    fn ambient_visibility_blanks_the_inventory() {
        let fixture = Fixture::new();
        fixture.dgpu(1, LOC_03_00, 128, GB24);
        assert!(fixture.build().is_some(), "baseline is a known host");
        for position in 0..VISIBILITY_VARS.len() {
            let var = VISIBILITY_VARS[position];
            let with = |value| {
                let mut ambient = [None; VISIBILITY_VARS.len()];
                ambient[position] = Some(value);
                build(&fixture.roots, ambient)
            };
            assert!(with("0").is_err(), "{var} must blank the inventory");
            assert!(with("").is_ok(), "{var} empty is not configured");
            assert!(with(" , ").is_ok(), "{var} comma-only is not configured");
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

    /// GPU nodes only, in numeric node order, each marked by whether its
    /// render node opens, with no VRAM counter needed; no topology lists
    /// nothing.
    #[test]
    fn topology_lists_every_gpu_node_in_node_order() {
        let fixture = Fixture::new();
        fixture
            .node(0, &[("cpu_cores_count", 32), ("simd_count", 0)])
            .node(10, &gpu_props(LOC_0C_00, 129, 0, 120001))
            .node(2, &gpu_props(LOC_03_00, 128, 0, 110000))
            .render(129);
        assert_eq!(
            topology_gpus(&fixture.roots, true),
            [("gfx1100".to_owned(), false), ("gfx1201".to_owned(), true)]
        );
        assert_eq!(
            topology_gpus(&fixture.roots, false),
            [("gfx1100".to_owned(), false), ("gfx1201".to_owned(), false)]
        );
        let rootless = SysfsRoots {
            kfd_nodes: fixture.roots.kfd_nodes.join("absent"),
            ..fixture.roots.clone()
        };
        assert!(topology_gpus(&rootless, true).is_empty());
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
        // A minor that no longer fits a hex digit means the packing changed,
        // and rendering it anyway would name the wrong silicon.
        assert_eq!(gfx_name(92000), None, "minor outside a hex digit");
        assert_eq!(gfx_name(91600), None, "minor 16, the first value past 0xf");
        assert_eq!(gfx_name(91500).as_deref(), Some("gfx9f0"), "minor 15 fits");
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

    /// A discrete GPU the refresh reads today's two files for.
    fn discrete(key: &str, bdf: &str) -> GpuRef {
        let (key, bdf) = (key.to_owned(), bdf.to_owned());
        GpuRef {
            key,
            bdf,
            unified: false,
        }
    }

    /// The staleness refresh reads the same files the worker's free/total
    /// tier does, keyed by the inventory's device key, and is all-or-nothing.
    /// On a **unified** GPU total is carve-out + GTT and free adds as much
    /// GTT as RAM can deliver right now, within the cgroup limit — that
    /// clamp is the whole reason `/proc/meminfo` is read, and why a discrete
    /// host must not depend on it.
    #[test]
    fn the_refresh_reads_every_gpu_or_none_of_them() {
        let fixture = Fixture::new();
        fixture.pci(BDF_03, GB24, 4 * GIB).pci(BDF_0C, GB16, 0);
        let roots = &fixture.roots;
        let read = |gpus: &[GpuRef]| query_memory(&roots.pci_devices, &roots.ram, gpus);
        let tuple = |r: GpuMemory| (r.uuid, r.total_mb, r.free_mb);
        let seen = |gpus: &[GpuRef]| {
            read(gpus).map(|rows| rows.into_iter().map(tuple).collect::<Vec<_>>())
        };
        assert_eq!(
            seen(&[discrete("GPU-a", BDF_03), discrete("GPU-b", BDF_0C)]),
            Some(vec![
                ("GPU-a".to_owned(), 24 * 1024, 20 * 1024),
                ("GPU-b".to_owned(), 16 * 1024, 16 * 1024),
            ])
        );
        assert!(
            read(&[discrete("GPU-a", BDF_03), discrete("GPU-c", "0000:ff:00.0")]).is_none(),
            "one unreadable GPU makes the whole snapshot unknown"
        );
        assert!(read(&[]).is_none());

        // A 512 MiB carve-out (half used) and a 64 GiB GTT window with 4 GiB
        // taken, on a machine with 8 GiB of RAM available.
        let apu = |unified| GpuRef {
            key: "GPU-apu".to_owned(),
            bdf: BDF_03.to_owned(),
            unified,
        };
        let read_from = |f: &Fixture, gpu: GpuRef| {
            query_memory(&f.roots.pci_devices, &f.roots.ram, &[gpu])
                .map(|mut r| r.remove(0))
                .map(|r| (r.total_mb, r.free_mb))
        };
        let host = |available_kb| {
            let f = Fixture::new().meminfo(MEM_TOTAL_KB, available_kb);
            f.pci(BDF_03, CARVE_512M, CARVE_512M / 2)
                .gtt(BDF_03, GTT_64G, 4 * GIB);
            f
        };
        // 256 MiB of carve-out, plus 60 GiB of unclaimed GTT clamped to the
        // 8 GiB this machine has.
        let tight = host(8 * 1024 * 1024);
        let budget = 512 + 64 * 1024;
        assert_eq!(read_from(&tight, apu(true)), Some((budget, 256 + 8 * 1024)));
        assert_eq!(
            read_from(&tight, apu(false)),
            Some((512, 256)),
            "the same GPU read as discrete never consults the GTT files"
        );
        // 2 GiB of that RAM is reclaimable slab, which is not deliverable.
        let slab = host(8 * 1024 * 1024);
        let mut rows = fs::read_to_string(&slab.roots.ram.meminfo).unwrap();
        rows.push_str("SReclaimable:    2097152 kB\n");
        fs::write(&slab.roots.ram.meminfo, rows).unwrap();
        assert_eq!(read_from(&slab, apu(true)), Some((budget, 256 + 6 * 1024)));
        assert_eq!(ram_deliverable_mb(&slab.roots.ram.meminfo), Some(6 * 1024));
        // Plenty of RAM: the GTT term is the driver's own figure again.
        let roomy = host(100 * 1024 * 1024);
        let seen = read_from(&roomy, apu(true));
        assert_eq!(seen, Some((budget, 256 + 60 * 1024)));
        // In a container limited to 16 GiB with 10 GiB used, 6 GiB is left.
        fs::create_dir_all(&roomy.roots.ram.cgroup).unwrap();
        fs::write(
            roomy.roots.ram.cgroup.join("memory.max"),
            format!("{}\n", 16 * GIB),
        )
        .unwrap();
        fs::write(
            roomy.roots.ram.cgroup.join("memory.current"),
            format!("{}\n", 10 * GIB),
        )
        .unwrap();
        assert_eq!(read_from(&roomy, apu(true)), Some((budget, 256 + 6 * 1024)));

        // All-or-nothing extends to the new files, and only to unified rows.
        let no_gtt = Fixture::new();
        no_gtt.pci(BDF_03, CARVE_512M, 0);
        assert!(read_from(&no_gtt, apu(true)).is_none(), "no GTT counters");
        let no_meminfo = Fixture::new();
        no_meminfo
            .pci(BDF_03, CARVE_512M, 0)
            .gtt(BDF_03, GTT_64G, 0);
        fs::remove_file(&no_meminfo.roots.ram.meminfo).unwrap();
        assert!(
            read_from(&no_meminfo, apu(true)).is_none(),
            "no MemAvailable"
        );
        assert!(
            read_from(&no_meminfo, apu(false)).is_some(),
            "a discrete host never acquires the /proc/meminfo dependency"
        );
    }

    /// Only two rows of `/proc/meminfo` are read, and only in the shape the
    /// kernel writes them: a value without the `kB` unit is no reading.
    #[test]
    fn parses_the_two_meminfo_rows() {
        let fixture = Fixture::new().meminfo(MEM_TOTAL_KB, 8 * 1024 * 1024);
        let path = &fixture.roots.ram.meminfo;
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

    /// A discrete GPU names its VRAM rounded to the nearest GiB, an APU the
    /// literal `APU` and the machine's RAM on a 4 GiB grid; never 0 GB.
    #[test]
    fn derived_names_round_capacity_to_a_stable_grid() {
        for (mb, expected) in [(24 * 1024, "24 GB"), (16368, "16 GB")] {
            assert_eq!(gpu_name("gfx1100", mb), format!("AMD gfx1100 ({expected})"));
        }
        assert_eq!(gpu_name("gfx90c", 512), "AMD gfx90c (1 GB)");
        assert_eq!(gpu_name("gfx90c", 0), "AMD gfx90c (1 GB)", "never 0 GB");
        #[rustfmt::skip]
        let apu_sizes = [(128 * 1024, "128 GB"), (32 * 1024 - 8, "32 GB"),
                         (30 * 1024, "32 GB"), (0, "4 GB")];
        for (mb, expected) in apu_sizes {
            let name = apu_device_name("gfx1151", mb);
            assert_eq!(name, format!("AMD gfx1151 APU ({expected})"));
        }
        // Sizes that price differently still separate.
        assert_eq!(capacity_gb_up_4(64 * 1024 - 1800), 64);
        assert_eq!(capacity_gb_up_4(32 * 1024 - 1800), 32);
    }

    /// The rounding end to end: a machine's name must not move when its BIOS
    /// carve-out changes or when the kernel's reservations do.
    #[test]
    fn the_apu_name_survives_carve_out_and_kernel_reservation_changes() {
        let named = |carve_mb: u64, reserved_mb: u64| {
            let mem_total_mb = 128 * 1024 - carve_mb - reserved_mb;
            let fixture = Fixture::new().meminfo(mem_total_mb * 1024, 8 * 1024 * 1024);
            fixture.apu(1, LOC_03_00, 128, carve_mb * 1024 * 1024, 16 * GIB);
            fixture.build().expect("priced")[0].name.clone()
        };
        // Three BIOS carve-outs covering the practical range, each also with
        // a `crashkernel=1G` added on the next boot.
        for carve_mb in [512, 4 * 1024, 8 * 1024] {
            for reserved_mb in [1800, 1800 + 1024] {
                let name = named(carve_mb, reserved_mb);
                assert_eq!(name, APU_128, "carve {carve_mb}, reserved {reserved_mb}");
            }
        }
    }

    /// `/proc/meminfo` is read for **unified** GPUs only, so a container
    /// with a partial `/proc` still gets its full discrete inventory.
    #[test]
    fn a_discrete_host_needs_no_meminfo() {
        let fixture = Fixture::new();
        fixture
            .dgpu(1, LOC_03_00, 128, GB24)
            .dgpu(2, LOC_0C_00, 129, GB16);
        fs::remove_file(&fixture.roots.ram.meminfo).unwrap();
        let rows = fixture.build().expect("no meminfo, no problem");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].name, "AMD gfx1100 (24 GB)");
        assert!(rows.iter().all(|row| !row.unified()));
    }
}
