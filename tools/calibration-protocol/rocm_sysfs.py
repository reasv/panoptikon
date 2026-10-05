"""rocm_sysfs.py - amdgpu and KFD readings shared by the calibration tools.

Mirrors the gateway's probe (`panoptikon/src/inferio/rocm.rs`): the same
openable-node order (the HIP device index), BDF decoding, device keys and
free/total arithmetic, so a reading here joins `/health` by key. Per-process
VRAM comes from KFD's own counter (`/sys/class/kfd/kfd/proc/<pid>/vram_<gpu_id>`)
or, where that cannot be used, from DRM fdinfo parsed as
`python/inferio_worker/memory.py` parses it. Stdlib only.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

MIB = 1024 * 1024

# `/proc/self/ns/pid` in the initial PID namespace (PROC_PID_INIT_INO). KFD
# names `proc/<pid>` by initial-namespace PID, so only there do they match ours.
INIT_PID_NS = "pid:[4026531836]"


@dataclass(frozen=True)
class Roots:
    """The filesystem roots read here, injectable for fixture trees."""
    kfd: str = "/sys/class/kfd/kfd"
    pci_devices: str = "/sys/bus/pci/devices"
    dev_dri: str = "/dev/dri"
    proc: str = "/proc"


@dataclass(frozen=True)
class Gpu:
    index: int              # HIP device index: position among openable nodes
    key: str                # the ledger's device key
    bdf: str
    gpu_id: Optional[int]   # suffix of KFD's `proc/<pid>/vram_<gpu_id>`
    unified: bool           # an APU: memory includes GTT


def _read(path: str) -> Optional[str]:
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            return handle.read()
    except OSError:
        return None


def read_int(path: str) -> Optional[int]:
    text = _read(path)
    try:
        return int(text.strip()) if text is not None else None
    except ValueError:
        return None


def parse_properties(text: str) -> Dict[str, int]:
    """`key value` lines of a KFD `properties` file; unparseable lines dropped."""
    out: Dict[str, int] = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) >= 2 and fields[1].isdigit():
            out[fields[0]] = int(fields[1])
    return out


def format_bdf(domain: int, location_id: int) -> Optional[str]:
    """`dddd:bb:dd.0` from KFD's `domain` and `location_id` (`rocm.rs::format_bdf`)."""
    if domain > 0xFFFF or location_id > 0xFFFF:
        return None
    bus, device = (location_id >> 8) & 0xFF, (location_id >> 3) & 0x1F
    return f"{domain:04x}:{bus:02x}:{device:02x}.0"


def gpu_nodes(roots: Roots = Roots()) -> List[Dict[str, Any]]:
    """Every readable KFD GPU node in node order, with whether this process can
    open its render node read-write (the test `rocm.rs` filters by)."""
    base = os.path.join(roots.kfd, "topology", "nodes")
    try:
        names = sorted((int(name) for name in os.listdir(base) if name.isdigit()))
    except OSError:
        return []
    out: List[Dict[str, Any]] = []
    for node in names:
        text = _read(os.path.join(base, str(node), "properties"))
        props = parse_properties(text) if text is not None else {}
        if not props.get("simd_count"):
            continue
        minor = props.get("drm_render_minor", 0)
        openable = minor > 0
        try:
            os.close(os.open(os.path.join(roots.dev_dri, f"renderD{minor}"), os.O_RDWR))
        except OSError:
            openable = False
        out.append({"node": node, "properties": props, "openable": openable,
                    "gpu_id": read_int(os.path.join(base, str(node), "gpu_id"))})
    return out


def inventory(roots: Roots = Roots()) -> List[Gpu]:
    """The openable GPUs, indexed and keyed as `rocm.rs::build` does. A node
    with no usable address is dropped but still takes its index."""
    rows: List[Gpu] = []
    for index, entry in enumerate(n for n in gpu_nodes(roots) if n["openable"]):
        props = entry["properties"]
        bdf = None
        if "location_id" in props and "domain" in props:
            bdf = format_bdf(props["domain"], props["location_id"])
        if bdf is None:
            continue
        unique_id = props.get("unique_id") or 0
        key = f"GPU-{unique_id:016x}" if unique_id else f"GPU-BDF-{bdf}"
        rows.append(Gpu(index, key, bdf, entry["gpu_id"],
                        props.get("cpu_cores_count", 0) > 0))
    # GPUs sharing a `unique_id` are keyed by address instead.
    keys = [row.key for row in rows]
    return [replace(row, key=f"GPU-BDF-{row.bdf}") if keys.count(row.key) > 1
            else row for row in rows]


def pinned_gpu(device: int, environ: Dict[str, str],
               roots: Roots = Roots()) -> Optional[Gpu]:
    """The GPU a process pinned to HIP device `device` uses. A single index
    already in `HIP_VISIBLE_DEVICES` is kept and names the device; any other
    visibility variable leaves the process unpinned (None)."""
    hip = (environ.get("HIP_VISIBLE_DEVICES") or "").strip()
    others = ("ROCR_VISIBLE_DEVICES", "CUDA_VISIBLE_DEVICES", "GPU_DEVICE_ORDINAL")
    if any((environ.get(name) or "").strip() for name in others) or (
            hip and not hip.isdigit()):
        return None
    return next((gpu for gpu in inventory(roots)
                 if gpu.index == (int(hip) if hip else device)), None)


def pin_env(gpu: Gpu) -> Dict[str, str]:
    """The variables the spawner gives a worker on `gpu`: `HIP_VISIBLE_DEVICES`
    and `PANOPTIKON_DEVICE_PIN`, plus `PANOPTIKON_UNIFIED_GPU` on a unified
    GPU."""
    out = {"HIP_VISIBLE_DEVICES": str(gpu.index),
           "PANOPTIKON_DEVICE_PIN": str(gpu.index)}
    if gpu.unified:
        out["PANOPTIKON_UNIFIED_GPU"] = gpu.bdf
    return out


def meminfo_mb(roots: Roots, key: str) -> Optional[int]:
    """One `/proc/meminfo` row in MiB (`rocm.rs::meminfo_mb`)."""
    for line in (_read(os.path.join(roots.proc, "meminfo")) or "").splitlines():
        name, _, rest = line.partition(":")
        fields = rest.split()
        if name.strip() == key and len(fields) == 2 and fields[1] == "kB":
            return int(fields[0]) // 1024
    return None


def memory_mb(roots: Roots, gpu: Gpu) -> Optional[Tuple[int, int]]:
    """`(total_mb, free_mb)` as `rocm.rs::query_memory` computes it: on a
    unified GPU, carve-out plus GTT, with free GTT clamped by the RAM the
    kernel could deliver, `MemAvailable` less `SReclaimable`."""
    device = os.path.join(roots.pci_devices, gpu.bdf)

    def mb(name: str) -> Optional[int]:
        value = read_int(os.path.join(device, name))
        return None if value is None else value // MIB

    total, used = mb("mem_info_vram_total"), mb("mem_info_vram_used")
    if total is None or used is None:
        return None
    if not gpu.unified:
        return total, max(0, total - used)
    gtt_total, gtt_used = mb("mem_info_gtt_total"), mb("mem_info_gtt_used")
    available = meminfo_mb(roots, "MemAvailable")
    if gtt_total is None or gtt_used is None or available is None:
        return None
    available = max(0, available - (meminfo_mb(roots, "SReclaimable") or 0))
    return (total + gtt_total,
            max(0, total - used) + min(max(0, gtt_total - gtt_used), available))


def parse_fdinfo(text: str, regions: Tuple[str, ...]
                 ) -> Optional[Tuple[str, Tuple[str, int], int]]:
    """`(pdev, client, bytes)` of one DRM fdinfo file, as the worker's
    `parse_drm_fdinfo`: client `("drm-client-id", id)`, else `("pasid", id)`;
    `drm-resident-*` preferred over `drm-memory-*`."""
    fields = {}
    for line in text.splitlines():
        key, sep, value = line.partition(":")
        key = key.strip().lower()
        if sep and (key.startswith("drm-") or key == "pasid"):
            fields[key] = value.split()
    kind = "drm-client-id" if "drm-client-id" in fields else "pasid"
    pdev, client = fields.get("drm-pdev"), fields.get(kind)
    if not pdev or not client or not client[0].isdigit():
        return None
    resident = any(f"drm-resident-{region}" in fields for region in regions)
    prefix = "drm-resident-" if resident else "drm-memory-"
    total = 0
    for region in regions:
        value = fields.get(prefix + region)
        if not value:
            continue
        unit = value[1].upper() if len(value) > 1 else ""
        scale = {"": 1, "KIB": 1024, "MIB": MIB}.get(unit)
        if scale is None or not value[0].isdigit():
            return None
        total += int(value[0]) * scale
    return pdev[0].lower(), (kind, int(client[0])), total


def _numbered(root: str) -> List[int]:
    """Numeric entries of a directory (PIDs, or descriptor numbers)."""
    try:
        return [int(name) for name in os.listdir(root) if name.isdigit()]
    except OSError:
        return []


def _drm_fdinfo(roots: Roots, pid: int) -> Optional[List[str]]:
    """The fdinfo text of every `/dev/dri/*` descriptor this PID holds, or
    None when its descriptors may not be read (another user's process, without
    CAP_SYS_PTRACE). A PID that exits meanwhile holds nothing."""
    base = os.path.join(roots.proc, str(pid))
    texts = []
    try:
        for fd in os.listdir(os.path.join(base, "fd")):
            try:
                if not os.readlink(os.path.join(base, "fd", fd)).startswith("/dev/dri/"):
                    continue
                with open(os.path.join(base, "fdinfo", fd), encoding="utf-8",
                          errors="replace") as handle:
                    texts.append(handle.read())
            except FileNotFoundError:
                continue
    except PermissionError:
        return None
    except OSError:
        return []
    return texts


def _pasid(text: str) -> Optional[int]:
    """The `pasid:` line of an amdgpu fdinfo file."""
    for line in text.splitlines():
        key, _, value = line.partition(":")
        if key.strip() == "pasid" and value.strip().isdigit():
            return int(value.strip())
    return None


def _fdinfo_held_mb(texts: Dict[int, Optional[List[str]]], gpu: Gpu
                    ) -> Dict[int, int]:
    """MiB on `gpu` per PID over its DRM clients. Each client counts once: a
    descriptor inherited across fork names the same client in both PIDs and
    is credited to the lower PID."""
    regions = ("vram", "gtt") if gpu.unified else ("vram",)
    seen, held = set(), {}
    for pid in sorted(texts):
        total = 0
        for text in texts[pid] or []:
            record = parse_fdinfo(text, regions)
            if record is None or record[0] != gpu.bdf or record[1] in seen:
                continue
            seen.add(record[1])
            total += record[2]
        if total:
            held[pid] = total // MIB
    return held


def _in_initial_pid_ns(roots: Roots) -> bool:
    try:
        return os.readlink(os.path.join(roots.proc, "self", "ns", "pid")) == INIT_PID_NS
    except OSError:
        return False


class Reading(NamedTuple):
    source: str             # "kfd" or "fdinfo"
    held: Dict[int, int]    # {pid: MiB}; a PID holding nothing is left out
    unreadable: List[int]   # PIDs whose descriptors could not be read


def process_vram_mb(roots: Roots, gpus: List[Gpu], pids: Optional[List[int]] = None
                    ) -> Dict[str, Reading]:
    """Per GPU key, what every PID (or only `pids`) holds on it.

    KFD's counter where a PID can be tied to its KFD entry: by PID in the
    initial PID namespace, else by the `pasid:` of the PID's DRM fdinfo, which
    KFD sets to its own PASID for that process. A unified GPU is read from
    fdinfo (KFD counts VRAM only, not GTT). When PIDs are matched by PASID, so
    is a GPU where a PID holding memory has no KFD entry, and each PID takes
    its first PASID whose KFD entry no lower PID took, so an entry two PIDs
    reach (a descriptor inherited across fork carries the parent's PASID) is
    credited once, to the lower PID.
    """
    kfd_root = os.path.join(roots.kfd, "proc")
    kfd_present = os.path.isdir(kfd_root)
    by_pid = _in_initial_pid_ns(roots) and kfd_present
    # Read before the descriptors: a PASID freed and reused in between then
    # names a directory KFD has already removed, so its PID is left out
    # instead of credited with another process's memory.
    by_pasid = {} if by_pid else {
        read_int(os.path.join(kfd_root, str(entry), "pasid")):
        os.path.join(kfd_root, str(entry)) for entry in _numbered(kfd_root)}
    texts: Dict[int, Optional[List[str]]] = {}
    if not by_pid or any(gpu.unified or gpu.gpu_id is None for gpu in gpus):
        texts = {pid: _drm_fdinfo(roots, pid)
                 for pid in (pids if pids is not None else _numbered(roots.proc))}
    if by_pid:
        entries = {pid: os.path.join(kfd_root, str(pid))
                   for pid in (pids if pids is not None else _numbered(kfd_root))}
    else:
        entries = {}
        for pid in sorted(texts):
            match = next((by_pasid[pasid] for pasid in map(_pasid, texts[pid] or [])
                          if pasid and pasid in by_pasid
                          and by_pasid[pasid] not in entries.values()), None)
            if match:
                entries[pid] = match
    unreadable = sorted(pid for pid, pid_texts in texts.items() if pid_texts is None)
    out: Dict[str, Reading] = {}
    for gpu in gpus:
        fdinfo = _fdinfo_held_mb(texts, gpu)
        if (gpu.unified or gpu.gpu_id is None or not kfd_present
                or not (by_pid or (entries and set(fdinfo) <= set(entries)))):
            out[gpu.key] = Reading("fdinfo", fdinfo, unreadable)
            continue
        held = {}
        for pid, directory in entries.items():
            value = read_int(os.path.join(directory, f"vram_{gpu.gpu_id}"))
            if value:
                held[pid] = value // MIB
        out[gpu.key] = Reading("kfd", held, [] if by_pid else unreadable)
    return out
