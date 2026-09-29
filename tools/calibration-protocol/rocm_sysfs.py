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
from typing import Any, Dict, List, Optional, Tuple

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
    unified GPU, carve-out plus GTT, with free GTT clamped by `MemAvailable`."""
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
    return (total + gtt_total,
            max(0, total - used) + min(max(0, gtt_total - gtt_used), available))


def parse_fdinfo(text: str, regions: Tuple[str, ...]) -> Optional[Tuple[str, int, int]]:
    """`(pdev, client_id, bytes)` of one DRM fdinfo file, as the worker's
    `parse_drm_fdinfo`: `drm-resident-*` preferred over `drm-memory-*`."""
    fields = {}
    for line in text.splitlines():
        key, sep, value = line.partition(":")
        if sep and key.strip().lower().startswith("drm-"):
            fields[key.strip().lower()] = value.split()
    pdev, client = fields.get("drm-pdev"), fields.get("drm-client-id")
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
    return pdev[0].lower(), int(client[0]), total


def _numbered(root: str) -> List[int]:
    """Numeric entries of a directory (PIDs, or descriptor numbers)."""
    try:
        return [int(name) for name in os.listdir(root) if name.isdigit()]
    except OSError:
        return []


def _drm_fdinfo(roots: Roots, pid: int) -> List[str]:
    """The fdinfo text of every `/dev/dri/*` descriptor this PID holds."""
    base = os.path.join(roots.proc, str(pid))
    texts = []
    for fd in _numbered(os.path.join(base, "fd")):
        try:
            if not os.readlink(os.path.join(base, "fd", str(fd))).startswith("/dev/dri/"):
                continue
        except OSError:
            continue
        text = _read(os.path.join(base, "fdinfo", str(fd)))
        if text:
            texts.append(text)
    return texts


def _fdinfo_bytes(texts: List[str], gpu: Gpu) -> int:
    """Bytes on `gpu` over one PID's DRM clients, each client counted once."""
    regions = ("vram", "gtt") if gpu.unified else ("vram",)
    seen, total = set(), 0
    for text in texts:
        record = parse_fdinfo(text, regions)
        if record is None or record[0] != gpu.bdf or record[1] in seen:
            continue
        seen.add(record[1])
        total += record[2]
    return total


def process_source(roots: Roots, gpu: Gpu) -> str:
    """`kfd` where KFD's per-process counter can be read for our PIDs, else
    `fdinfo`. A unified GPU always uses fdinfo: KFD counts VRAM only, not GTT."""
    try:
        link = os.readlink(os.path.join(roots.proc, "self", "ns", "pid"))
        same_ns = link == INIT_PID_NS
    except OSError:
        same_ns = False
    usable = same_ns and os.path.isdir(os.path.join(roots.kfd, "proc"))
    return "kfd" if usable and gpu.gpu_id is not None and not gpu.unified else "fdinfo"


def process_vram_mb(roots: Roots, gpus: List[Gpu], pids: Optional[List[int]] = None
                    ) -> Dict[str, Tuple[str, Dict[int, int]]]:
    """Per GPU key, `(source, {pid: MiB})` over every PID holding memory on it
    (or only `pids`). A PID holding nothing is left out."""
    out: Dict[str, Tuple[str, Dict[int, int]]] = {}
    texts: Optional[Dict[int, List[str]]] = None
    for gpu in gpus:
        source = process_source(roots, gpu)
        held: Dict[int, int] = {}
        if source == "kfd":
            root = os.path.join(roots.kfd, "proc")
            for pid in pids if pids is not None else _numbered(root):
                value = read_int(os.path.join(root, str(pid), f"vram_{gpu.gpu_id}"))
                if value:
                    held[pid] = value // MIB
        else:
            if texts is None:
                texts = {pid: _drm_fdinfo(roots, pid)
                         for pid in (pids if pids is not None else _numbered(roots.proc))}
            for pid, pid_texts in texts.items():
                value = _fdinfo_bytes(pid_texts, gpu)
                if value:
                    held[pid] = value // MIB
        out[gpu.key] = (source, held)
    return out
