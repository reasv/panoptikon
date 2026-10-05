"""Device-memory sensing for the worker: the worker senses, the orchestrator
decides. Every helper returns `None` rather than raising. See
docs/inferio-worker-protocol.md "Memory sensing (optional response fields)".

Never initialize CUDA: reading the device creates a 300-600 MB context, so every
torch path requires `is_initialized()` first. A process that never allocated on
the device reports no `base_mb` rather than 0. Imports are stdlib only.
"""

from __future__ import annotations

import logging
import os
import re
import sys
import threading
import time
from collections import deque
from collections.abc import Callable, Iterable
from functools import lru_cache
from types import ModuleType
from typing import Any, NamedTuple

logger = logging.getLogger("inferio_worker.memory")

_MIB = 1024 * 1024

# Accelerator-context allowance when this process could not measure its own.
CONTEXT_ESTIMATE_MB = 500

# Plausible band (MiB) for a measured context; outside it the estimate is used.
CONTEXT_MIN_MB = 64
CONTEXT_MAX_MB = 2048

# Context probe poll interval and deadline (the orchestrator's `load_secs`).
_CONTEXT_POLL_SECONDS = 0.005
_CONTEXT_PROBE_MAX_SECONDS = 600

# How often the probe re-reads its pre-initialisation baseline while it waits.
_CONTEXT_BASELINE_SECONDS = 0.25

# Allowance above the context for memory held outside the caching allocator
# (cuDNN/cuBLAS workspaces, driver bookkeeping); caps the free delta.
IMPLAUSIBLE_SLACK_MB = 2048

# How far below our own allocator pool an fdinfo VRAM reading may sit before it
# is judged an under-report.
FDINFO_UNDERREPORT_SLACK_MB = 256

# NVML memoization: the module is one-shot, the handle is retried (see `_nvml`).
_nvml_state: dict[str, Any] = {"module_tried": False, "module": None, "handle": None}

# One-shot log flags (per worker process).
_logged: dict[str, bool] = {
    "nvml_pid_missing": False,
    "nvml_gpu_unidentified": False,
    "hip_uuid_suppressed": False,
    "fdinfo_identity": False,
    "fdinfo_under_reported": False,
}

# This worker's GPU PCI address, memoized on success only.
_bdf_state: dict[str, Any] = {"bdf": None}

# This process's DRM clients (Linux only).
FDINFO_ROOT = "/proc/self/fdinfo"

# amdgpu per-GPU VRAM counters (`<root>/<bdf>/mem_info_vram_{total,used}`), the
# same files the orchestrator reads.
PCI_DEVICES_ROOT = "/sys/bus/pci/devices"

# The memory limit the kernel enforces on this process (psutil answers for the
# whole machine in a container). `cpu.rs` reads the same files; the two must
# agree or registration refuses the worker.
CGROUP_ROOT = "/sys/fs/cgroup"

# DRM usage-stats memory units: `<uint> [KiB|MiB]`, absent meaning bytes
# (<https://docs.kernel.org/gpu/drm-usage-stats.html>).
_DRM_UNITS = {
    "": 1,
    "KIB": 1024,
    "MIB": 1024 * 1024,
}

# A PCI address as the kernel writes it: `dddd:bb:dd.f`, lower-case hex.
_BDF_RE = re.compile(r"[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-7]")

# The device the orchestrator priced this worker against; `cpu` means host RAM.
DEVICE_ENV_VAR = "INFERIO_DEVICE"

# `device_kind` and calibration architecture key of a CPU worker.
DEVICE_KIND_CPU = "cpu"

# The MPS allocator's ceiling as a fraction of the recommended maximum, which
# the spawner pins to 1.0 (`accelerator_env.rs`). Read, never written, here.
MPS_WATERMARK_ENV_VAR = "PYTORCH_MPS_HIGH_WATERMARK_RATIO"

# Peak sampler interval, and the lifetime of a sampler nobody stopped.
MPS_SAMPLE_SECONDS = 0.02
MPS_SAMPLE_MAX_SECONDS = 900
_MPS_SAMPLE_JOIN_SECONDS = 1.0

# Where Linux publishes this process's peak resident set and its anonymous
# and swapped memory.
PROC_STATUS = "/proc/self/status"

# Where Linux publishes the machine's memory statistics.
PROC_MEMINFO = "/proc/meminfo"


# --- torch, only if the impl already brought it *and* already used the GPU ---


def _torch() -> Any | None:
    """The already-imported torch module, or None. Touches nothing."""
    return sys.modules.get("torch")


def _torch_cuda() -> Any | None:
    """The already-imported torch iff its CUDA device is already live."""
    torch = _torch()
    if torch is None:
        return None
    try:
        if not torch.cuda.is_available():
            return None
        if not torch.cuda.is_initialized():
            return None
    except Exception:
        return None
    return torch


def _mb(value: Any) -> int | None:
    try:
        mib = int(value) // _MIB
    except Exception:
        return None
    return max(mib, 0)


# --- NVML: per-process footprint and a torch-free memory reading ---


def _nvml() -> tuple[Any, Any] | None:
    """`(pynvml, handle)` for the GPU this worker is pinned to, else None.

    NVML ignores `CUDA_VISIBLE_DEVICES`, so the pin is resolved explicitly;
    the lookup is retried on every call, since the first call precedes CUDA
    init. Refused on a ROCm worker: `nvmlInit` succeeds wherever an NVIDIA
    driver is loaded, and a hybrid box would mix two GPUs' readings.
    """
    if _is_hip(_torch()) or _hip_pinned():
        return None
    pynvml = _nvml_module()
    if pynvml is None:
        return None
    handle = _nvml_state["handle"]
    if handle is None:
        try:
            handle = _nvml_handle(pynvml)
        except Exception as exc:
            logger.debug("NVML device lookup failed (%s)", exc)
            return None
        if handle is None:
            return None
        _nvml_state["handle"] = handle
    return (pynvml, handle)


def _nvml_module() -> Any | None:
    """The initialized `pynvml` module, or None. A failure is permanent."""
    if _nvml_state["module_tried"]:
        return _nvml_state["module"]
    _nvml_state["module_tried"] = True
    try:
        import pynvml
    except Exception as exc:
        logger.debug("NVML unavailable (%s); per-process base measurement off", exc)
        return None
    try:
        pynvml.nvmlInit()
    except Exception as exc:
        logger.debug("NVML init failed (%s)", exc)
        return None
    _nvml_state["module"] = pynvml
    return pynvml


def _nvml_handle(pynvml: Any) -> Any | None:
    pin = (os.environ.get("CUDA_VISIBLE_DEVICES") or "").strip()
    named_uuid = False
    if pin.upper().startswith(("GPU-", "MIG-")):
        named_uuid = True
        handle = _nvml_handle_by_uuid(pynvml, pin)
        if handle is not None:
            return handle
    # A MIG pin stops here: torch reports the parent board's UUID for a slice.
    if not pin.upper().startswith("MIG-"):
        # Ask torch for the device's UUID (only once CUDA is initialized).
        uuid, _ = device_identity()
        if uuid is not None:
            handle = _nvml_handle_by_uuid(pynvml, uuid)
            if handle is not None:
                return handle
    # Last resort: a single-GPU host whose pin named no UUID (for a MIG pin the
    # one board is the parent). An index pin is never mapped to an NVML index:
    # the orderings differ under CUDA_DEVICE_ORDER.
    try:
        if not named_uuid and pynvml.nvmlDeviceGetCount() == 1:
            return pynvml.nvmlDeviceGetHandleByIndex(0)
    except Exception:
        pass
    # Logged once: `_nvml` retries on every call.
    if not _logged["nvml_gpu_unidentified"]:
        _logged["nvml_gpu_unidentified"] = True
        logger.debug("cannot identify this worker's GPU in NVML; skipping NVML paths")
    return None


def _nvml_handle_by_uuid(pynvml: Any, uuid: str) -> Any | None:
    """Handle for `uuid`, accepting a unique prefix as CUDA does."""
    exact = uuid.strip()
    try:
        return pynvml.nvmlDeviceGetHandleByUUID(exact.encode())
    except Exception:
        pass
    wanted = exact.upper()
    matches: list[Any] = []
    try:
        for index in range(pynvml.nvmlDeviceGetCount()):
            handle = pynvml.nvmlDeviceGetHandleByIndex(index)
            found = pynvml.nvmlDeviceGetUUID(handle)
            if isinstance(found, bytes):
                found = found.decode("utf-8", "replace")
            if str(found).strip().upper().startswith(wanted):
                matches.append(handle)
    except Exception:
        return None
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        logger.debug(
            "%s is an abbreviated UUID matching %d GPUs in NVML; refusing to "
            "guess which one this worker is on",
            uuid,
            len(matches),
        )
    return None


def _nvml_memory() -> tuple[int | None, int | None]:
    """`(free_mb, total_mb)` from NVML, or `(None, None)`."""
    nvml = _nvml()
    if nvml is None:
        return (None, None)
    pynvml, handle = nvml
    try:
        info = pynvml.nvmlDeviceGetMemoryInfo(handle)
    except Exception:
        return (None, None)
    return (_mb(info.free), _mb(info.total))


def _nvml_own_process_mb(holding_mb: int | None = None) -> int | None:
    """This process's device footprint per NVML, or None. Unavailable under WDDM
    and in a PID namespace (NVML lists host pids); a reading at or above the
    GPU's capacity is rejected.
    """
    nvml = _nvml()
    if nvml is None:
        return None
    pynvml, handle = nvml
    try:
        procs = pynvml.nvmlDeviceGetComputeRunningProcesses(handle)
    except Exception:
        return None
    pid = os.getpid()
    for proc in procs:
        if getattr(proc, "pid", None) != pid:
            continue
        used = getattr(proc, "usedGpuMemory", None)
        if used is None:
            return None
        used_mb = _mb(used)
        if used_mb is None:
            return None
        _, total_mb = _nvml_memory()
        if total_mb is not None and total_mb > 0 and used_mb >= total_mb:
            logger.debug(
                "NVML reports this process holding %d MiB of a %d MiB GPU; "
                "rejecting the reading and falling back to the memory deltas",
                used_mb,
                total_mb,
            )
            return None
        return used_mb
    if (holding_mb or 0) > 0 and not _logged["nvml_pid_missing"]:
        _logged["nvml_pid_missing"] = True
        logger.info(
            "NVML lists no process with pid %d on this GPU although this worker "
            "holds %d MiB; per-process base measurement is unavailable and the "
            "free-memory delta is used instead. NVML reports host pids, so this "
            "is expected in a container started without --pid=host.",
            pid,
            holding_mb,
        )
    return None


# --- GPU identity (part of the calibration profile key) ---


def _is_hip(torch: Any) -> bool:
    """Whether this torch is a ROCm build (`torch.version.hip` is set)."""
    try:
        return bool(getattr(getattr(torch, "version", None), "hip", None))
    except Exception:
        return False


def _hip_pinned() -> bool:
    """Whether the spawner pinned this worker to a HIP device (it sets
    `HIP_VISIBLE_DEVICES` only for ROCm workers). Needs no torch import.
    """
    value = os.environ.get("HIP_VISIBLE_DEVICES") or ""
    return any(entry.strip() for entry in value.split(","))


def _unified_gpu() -> bool:
    """Whether this worker is on a unified-memory device. The spawner's
    `PANOPTIKON_UNIFIED_GPU=<pci address>` is checked against `_identity_bdf`.
    """
    claimed = (os.environ.get("PANOPTIKON_UNIFIED_GPU") or "").strip().lower()
    if not _BDF_RE.fullmatch(claimed):
        return False
    return claimed == _identity_bdf()


def _memory_regions() -> tuple[str, ...]:
    """DRM regions this worker's usage is summed over: VRAM, plus GTT on a
    unified-memory device (an APU spills into GTT once the carve-out fills).
    """
    return ("vram", "gtt") if _unified_gpu() else ("vram",)


def pinned_device_missing() -> str | None:
    """An actionable message when this worker was pinned
    (`PANOPTIKON_DEVICE_PIN`) to a device its runtime does not enumerate, or
    `None`. Otherwise the model would silently run on the CPU.
    """
    pin = (os.environ.get("PANOPTIKON_DEVICE_PIN") or "").strip()
    if not pin:
        return None
    torch = _torch()
    if torch is None:
        return None
    # A CPU-only torch build has no devices to lose; must not fail the load.
    try:
        version = getattr(torch, "version", None)
        accelerated = bool(getattr(version, "cuda", None)) or bool(
            getattr(version, "hip", None)
        )
    except Exception:
        return None
    if not accelerated:
        return None
    try:
        count = int(torch.cuda.device_count())
    except Exception:
        return None
    if count != 0:
        return None
    return (
        f"this worker was pinned to GPU device '{pin}' but the torch "
        "runtime enumerates no devices at all, so the model would have run on "
        "the CPU while being priced against a GPU. The likely causes are a "
        "GPU the ROCm userspace does not enumerate (an unsupported gfx "
        "target — an integrated GPU alongside a discrete one is the common "
        "case, and HSA_OVERRIDE_GFX_VERSION is how such a part is usually "
        "made usable), a render node this process cannot open "
        "(`/dev/dri/renderD*` permissions or the render group), or a device "
        "index that does not exist in this process's visible set. Pin this "
        "model to a GPU that works (inference_local `devices`), or make the "
        "pinned one enumerable"
    )


def _device_props() -> Any | None:
    """`get_device_properties(0)` for the pinned device, or None. Gated on
    `_torch_cuda`, since the call creates a context on a process that has none.
    """
    torch = _torch_cuda()
    if torch is None:
        return None
    try:
        return torch.cuda.get_device_properties(0)
    except Exception:
        return None


def _prop(props: Any, field: str) -> Any | None:
    """One field of a device-properties struct (which may be None), or None.
    Every read goes through here: the pybind getters can be missing or raise.
    """
    try:
        return getattr(props, field, None)
    except Exception:
        return None


def device_identity() -> tuple[str | None, str | None]:
    """`(uuid, name)` of this worker's CUDA device 0, UUID in NVML form. No
    UUID on ROCm, where it repeats across same-model cards (`device_bdf` keys).
    """
    # Checked before torch: a RAM-priced report must not carry a GPU UUID.
    if _ram_currency():
        return (None, ram_gpu_name())
    props = _device_props()
    if props is None:
    # MPS: no UUID, one device per host.
        return (None, mps_gpu_name() if _torch_mps() is not None else None)
    uuid = _prop(props, "uuid")
    name = _prop(props, "name")
    if uuid is not None and _is_hip(_torch()):
        uuid = None
        if not _logged["hip_uuid_suppressed"]:
            _logged["hip_uuid_suppressed"] = True
            logger.debug(
                "this is a ROCm torch build; not reporting its rendered GPU "
                "UUID (a third identity vocabulary that matches neither KFD's "
                "nor amd-smi's and repeats across same-model GPUs). The PCI "
                "address is reported instead"
            )
    return (
        f"GPU-{uuid}" if uuid is not None else None,
        name if isinstance(name, str) and name else None,
    )


# `Apple M3 Max` -> `M3`: the chip family; the variant only scales core counts.
_APPLE_CHIP_RE = re.compile(r"^Apple\s+(M\d+)\b", re.IGNORECASE)


def device_arch() -> str | None:
    """This worker's device architecture, the calibration profile key: memory
    per item follows the kernels, which follow the architecture, not the SKU.

    `sm_<major><minor>` on CUDA, the `gfx` target (without `:` feature
    suffixes) on ROCm, `apple-m<n>` on MPS, `cpu` on a RAM-priced worker, or
    None when nothing answers.
    """
    kind = device_kind()
    if kind == DEVICE_KIND_CPU:
        return DEVICE_KIND_CPU
    if kind == "mps":
        return _mps_arch()
    torch = _torch_cuda()
    if torch is None:
        # A CUDA device used outside torch (CTranslate2): ask NVML.
        return _nvml_arch()
    if _is_hip(torch):
        gfx = _prop(_device_props(), "gcnArchName")
        if not isinstance(gfx, str):
            return None
        gfx = gfx.split(":", 1)[0].strip()
        return gfx or None
    try:
        major, minor = torch.cuda.get_device_capability(0)
        return f"sm_{int(major)}{int(minor)}"
    except Exception:
        return None


def _nvml_arch() -> str | None:
    """`sm_<major><minor>` from NVML, for an impl that allocates outside torch
    (faster-whisper/CTranslate2); None when NVML cannot answer.
    """
    nvml = _nvml()
    if nvml is None:
        return None
    pynvml, handle = nvml
    try:
        major, minor = pynvml.nvmlDeviceGetCudaComputeCapability(handle)
        return f"sm_{int(major)}{int(minor)}"
    except Exception:
        return None


def _mps_arch() -> str | None:
    """`Apple M3 Max` -> `apple-m3`; None off Apple Silicon."""
    chip = _sysctl_string("machdep.cpu.brand_string")
    if not chip:
        return None
    match = _APPLE_CHIP_RE.match(chip)
    return f"apple-{match.group(1).lower()}" if match else None


def device_kind() -> str | None:
    """Which device torch put this model on (`cpu`, `cuda`, `rocm`, `mps`); the
    host prices the replica by it. None when torch was never imported, or when
    an accelerator is reachable but the load has not touched it yet.
    """
    if _forced_cpu():
        return DEVICE_KIND_CPU
    torch = _torch()
    if torch is None:
        return None
    if _is_hip(torch) or _hip_pinned():
        return "rocm"
    if _torch_cuda() is not None:
        return "cuda"
    if _torch_mps() is not None:
        return "mps"
    return None if _accelerator_present(torch) else DEVICE_KIND_CPU


def _accelerator_present(torch: Any) -> bool:
    """Whether this torch build can reach an accelerator. Unreadable counts as
    present: calling a GPU load `cpu` would price it against RAM."""
    try:
        if torch.cuda.is_available():
            return True
        return bool(torch.backends.mps.is_available())
    except Exception:
        return True


def device_label() -> str:
    """The device the memory figures describe, for `oom_class.device`."""
    try:
        kind = device_kind() or "unknown"
        uuid, _ = device_identity()
        return f"{kind}:{uuid}" if uuid else kind
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("device label unavailable: %s", exc)
        return "unknown"


def device_bdf() -> str | None:
    """This worker's GPU as a PCI address (`dddd:bb:dd.0`), or None; the ROCm
    identity key. Read from `get_device_properties(0)`; a ROCm build without
    the PCI fields falls back to the DRM client holding the most VRAM.
    """
    if _ram_currency():
        return None
    props = _device_props()
    if props is None:
        return None
    bdf = _props_bdf(props)
    if bdf is not None:
        return bdf
    if not _is_hip(_torch()):
        # CUDA without the PCI fields: the identity is the UUID.
        return None
    bdf = dominant_vram_pdev()
    # Lifted verbatim from a `drm-pdev` line, so shape-checked.
    if bdf is not None and not _BDF_RE.fullmatch(bdf):
        return None
    if bdf is not None and not _logged["fdinfo_identity"]:
        _logged["fdinfo_identity"] = True
        logger.debug(
            "this torch build exposes no PCI fields on get_device_properties; "
            "identifying this worker's GPU as %s, the DRM client holding the "
            "most VRAM in this process",
            bdf,
        )
    return bdf


def _props_bdf(props: Any) -> str | None:
    """The BDF from torch's PCI fields, or None unless all three are present."""
    domain = _prop(props, "pci_domain_id")
    bus = _prop(props, "pci_bus_id")
    device = _prop(props, "pci_device_id")
    if domain is None or bus is None or device is None:
        return None
    try:
        domain, bus, device = int(domain), int(bus), int(device)
    except Exception:
        return None
    if not (0 <= domain <= 0xFFFF and 0 <= bus <= 0xFF and 0 <= device <= 0x1F):
        return None
    return f"{domain:04x}:{bus:02x}:{device:02x}.0"


def gpu_total_mb() -> int | None:
    """Total device memory in MiB per torch, or None; the orchestrator
    cross-checks it against the driver. On MPS it is `recommended_max_memory()`.
    """
    if _ram_currency():
        _, total_mb = ram_free_total_mb()
        return total_mb
    props = _device_props()
    if props is None:
        return _mb(_mps_call("recommended_max_memory"))
    return _mb(_prop(props, "total_memory"))


# --- DRM fdinfo (per-process, per-GPU VRAM) ---


def parse_drm_fdinfo(
    text: str, regions: tuple[str, ...] = ("vram",)
) -> tuple[str, tuple[str, int], int] | None:
    """`(pdev, client, bytes)` for one fdinfo file, or None.

    Requires `drm-pdev` and a client identity that deduplicates fds of one
    client: `("drm-client-id", id)`, else amdgpu's `("pasid", id)` (older
    amdgpu drivers print no `drm-client-id`). One DRM file has one PASID, but
    a PASID identifies one client only per GPU: KFD puts its per-process PASID
    on every GPU's render node, so callers key on `(pdev, client)`.
    `drm-resident-*` is preferred over the deprecated `drm-memory-*`.
    A missing memory key counts as 0; an unparseable one makes the record None.
    """
    fields: dict[str, str] = {}
    for line in text.splitlines():
        key, separator, value = line.partition(":")
        if not separator:
            continue
        key = key.strip().lower()
        if key.startswith("drm-") or key == "pasid":
            fields[key] = value.strip()
    pdev = fields.get("drm-pdev")
    kind = "drm-client-id" if "drm-client-id" in fields else "pasid"
    if not pdev or kind not in fields:
        return None
    try:
        client = (kind, int(fields[kind]))
    except ValueError:
        return None
    prefix = (
        "drm-resident-"
        if any(f"drm-resident-{region}" in fields for region in regions)
        else "drm-memory-"
    )
    total = 0
    for region in regions:
        raw = fields.get(f"{prefix}{region}")
        if raw is None:
            continue
        amount = _parse_drm_bytes(raw)
        if amount is None:
            return None
        total += amount
    return (pdev.lower(), client, total)


def _parse_drm_bytes(value: str | None) -> int | None:
    """`<uint> [KiB|MiB]` in bytes, or None when it is not that."""
    if value is None:
        return None
    parts = value.split()
    if not parts:
        return None
    try:
        amount = int(parts[0])
    except ValueError:
        return None
    if amount < 0:
        return None
    scale = _DRM_UNITS.get(parts[1].upper() if len(parts) > 1 else "")
    if scale is None:
        return None
    return amount * scale


def fdinfo_vram_by_pdev(
    texts: Iterable[str], regions: tuple[str, ...] = ("vram",)
) -> dict[str, int]:
    """Per-GPU VRAM this process holds in bytes, keyed by PCI address."""
    seen: set[tuple[str, tuple[str, int]]] = set()
    totals: dict[str, int] = {}
    for text in texts:
        record = parse_drm_fdinfo(text, regions)
        if record is None:
            continue
        pdev, client, vram = record
        if (pdev, client) in seen:
            continue
        seen.add((pdev, client))
        totals[pdev] = totals.get(pdev, 0) + vram
    return totals


def dominant_vram_pdev(root: str | None = None) -> str | None:
    """The PCI address holding most of this process's VRAM; None on a tie."""
    totals = fdinfo_vram_by_pdev(
        _fdinfo_texts(FDINFO_ROOT if root is None else root)
    )
    if not totals:
        return None
    ranked = sorted(totals.items(), key=lambda entry: entry[1], reverse=True)
    if len(ranked) > 1 and ranked[0][1] == ranked[1][1]:
        return None
    pdev, vram = ranked[0]
    return pdev if vram > 0 else None


def _fdinfo_texts(root: str) -> list[str]:
    """The contents of every readable fdinfo file."""
    texts: list[str] = []
    try:
        names = os.listdir(root)
    except Exception:
        return texts
    for name in names:
        try:
            with open(os.path.join(root, name), encoding="utf-8", errors="replace") as fd:
                texts.append(fd.read())
        except Exception:
            continue
    return texts


def _identity_bdf() -> str | None:
    """`device_bdf`, memoized once resolved, or None (retried on every call)."""
    cached = _bdf_state["bdf"]
    if cached is not None:
        return cached
    bdf = device_bdf()
    if bdf is not None:
        _bdf_state["bdf"] = bdf
    return bdf


def fdinfo_own_vram_mb(root: str | None = None) -> int | None:
    """This process's VRAM on its own GPU in MiB (via DRM fdinfo), or None."""
    bdf = _identity_bdf()
    if bdf is None:
        return None
    texts = _fdinfo_texts(FDINFO_ROOT if root is None else root)
    vram = fdinfo_vram_by_pdev(texts, _memory_regions()).get(bdf)
    if vram is None:
        return None
    own_mb = _mb(vram)
    return own_mb if own_mb else None


def _fdinfo_base_mb(
    reserved_mb: int | None,
    reserved_delta: int | None,
    root: str | None = None,
) -> int | None:
    """`fdinfo_own_vram_mb` if plausible, ROCm only. Older kernels under-report
    compute memory, so the reading must not fall below our own absolute
    post-load allocator pool (`reserved_mb`).
    """
    if not _is_hip(_torch()):
        return None
    own = fdinfo_own_vram_mb(root)
    if own is None:
        return None
    # On a unified GPU HIP may report only the carve-out as `total_memory`,
    # while the reading includes GTT.
    total_mb = amdgpu_device_total_mb() if _unified_gpu() else gpu_total_mb()
    if total_mb is not None and total_mb > 0 and own >= total_mb:
        logger.debug(
            "DRM fdinfo reports this process holding %d MiB of a %d MiB GPU; "
            "rejecting the reading and falling back to the memory deltas",
            own,
            total_mb,
        )
        return None
    pool = reserved_mb if reserved_mb is not None else reserved_delta
    floor = (pool or 0) - FDINFO_UNDERREPORT_SLACK_MB
    if own < floor:
        if not _logged["fdinfo_under_reported"]:
            _logged["fdinfo_under_reported"] = True
            logger.debug(
                "DRM fdinfo reports this process holding %d MiB of VRAM while "
                "our own allocator pool is %d MiB (-%d MiB tolerance); "
                "rejecting the reading as an under-report (fdinfo memory stats "
                "for compute allocations need a recent kernel) and falling "
                "back to the memory deltas",
                own,
                pool or 0,
                FDINFO_UNDERREPORT_SLACK_MB,
            )
        return None
    return own


# --- amdgpu sysfs (device-wide free/total for this worker's GPU) ---


def amdgpu_free_total_mb(root: str | None = None) -> tuple[int | None, int | None]:
    """Device-wide `(free_mb, total_mb)` for this worker's GPU from amdgpu sysfs
    (the files the orchestrator reads), or `(None, None)`. On a unified-memory
    device GTT is added, its free part clamped by available RAM (less
    reclaimable slab, as `rocm.rs` reads it).
    """
    bdf = _identity_bdf()
    if bdf is None:
        return (None, None)
    device = _pci_device_dir(PCI_DEVICES_ROOT if root is None else root, bdf)
    total = _sysfs_bytes(os.path.join(device, "mem_info_vram_total"))
    used = _sysfs_bytes(os.path.join(device, "mem_info_vram_used"))
    if total is None or used is None:
        return (None, None)
    if _unified_gpu():
        gtt_total = _sysfs_bytes(os.path.join(device, "mem_info_gtt_total"))
        gtt_used = _sysfs_bytes(os.path.join(device, "mem_info_gtt_used"))
        available = _ram_available_bytes()
        if gtt_total is None or gtt_used is None or available is None:
            return (None, None)
        free = max(total - used, 0) + min(max(gtt_total - gtt_used, 0), available)
        return (_mb(free), _mb(total + gtt_total))
    return (_mb(total - used), _mb(total))


def amdgpu_device_total_mb(root: str | None = None) -> int | None:
    """`amdgpu_free_total_mb`'s total alone, which does not need psutil."""
    bdf = _identity_bdf()
    if bdf is None:
        return None
    device = _pci_device_dir(PCI_DEVICES_ROOT if root is None else root, bdf)
    total = _sysfs_bytes(os.path.join(device, "mem_info_vram_total"))
    if total is None:
        return None
    if _unified_gpu():
        gtt_total = _sysfs_bytes(os.path.join(device, "mem_info_gtt_total"))
        if gtt_total is None:
            return None
        total += gtt_total
    return _mb(total)


def _pci_device_dir(base: str, bdf: str) -> str:
    """`<base>/<bdf>`, colons as dashes on Windows (for test fixtures), as in
    `rocm.rs::pci_device_dir`.
    """
    return os.path.join(base, bdf.replace(":", "-") if os.name == "nt" else bdf)


def _sysfs_bytes(path: str) -> int | None:
    """A sysfs file holding one non-negative decimal integer, or None."""
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            raw = handle.read(64).strip()
    except Exception:
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value >= 0 else None


# --- MPS (Apple Silicon): a unified-memory device ---


def _torch_mps() -> Any | None:
    """The already-imported torch iff MPS is this worker's device (CUDA wins).
    MPS has no context to create, so no initialization check is needed.
    """
    if _torch_cuda() is not None:
        return None
    torch = _torch()
    if torch is None:
        return None
    try:
        backends = getattr(torch, "backends", None)
        mps = getattr(backends, "mps", None)
        available = getattr(mps, "is_available", None)
        if available is None or not available():
            return None
        if getattr(torch, "mps", None) is None:
            return None
    except Exception:
        return None
    return torch


def _mps_call(name: str) -> int | None:
    """One `torch.mps.<name>()` byte count, or None if it cannot be read."""
    torch = _torch_mps()
    if torch is None:
        return None
    try:
        call = getattr(torch.mps, name, None)
        if call is None:
            return None
        return int(call())
    except Exception:
        return None


def mps_pool_mb() -> tuple[int | None, int | None]:
    """`(reserved_mb, allocated_mb)` for the MPS allocator, or `(None, None)`.
    torch.mps has no peak API, so batch peaks are these figures read afterwards.
    """
    return (
        _mb(_mps_call("driver_allocated_memory")),
        _mb(_mps_call("current_allocated_memory")),
    )


def mps_headroom_mb() -> int | None:
    """The MPS allocator's ceiling (`recommended_max_memory()` times the high
    watermark ratio) minus the pool it holds; None off MPS or without a ceiling.
    """
    total = _mps_call("recommended_max_memory")
    if not total:
        return None
    driver = _mps_call("driver_allocated_memory")
    if driver is None:
        return None
    # An unreadable ratio is taken as the pinned 1.0.
    try:
        watermark = float(os.environ.get(MPS_WATERMARK_ENV_VAR) or 1.0)
    except ValueError:
        watermark = 1.0
    if watermark <= 0:
        return None
    return _mb(max(0, int(total * watermark) - driver))


def free_at_failure_mb() -> int | None:
    """The free reading an out-of-memory report is weighed against
    (`oom_class.free_mb_at_failure`), or None. On MPS this is the allocator's
    headroom, not free RAM: the watermark ceiling is what refuses allocations.
    """
    if not _ram_currency():
        headroom = mps_headroom_mb()
        if headroom is not None:
            return headroom
    free_mb, _, _ = free_total_mb()
    return free_mb


def _mps_free_with_basis() -> tuple[int | None, int | None, int | None, int | None]:
    """`(free_mb, total_mb, ram_total_mb, ram_available_mb)` for a unified-memory
    device from one read of the kernel counters, or all-None. `total` is
    `recommended_max_memory()`; `free` is `min(total, ram_available)`.
    """
    total = _mps_call("recommended_max_memory")
    facts = _mac_memory_counters()
    if not total or facts is None:
        return (None, None, None, None)
    available = _mac_available(facts)
    return (_mb(min(total, available)), _mb(total), _mb(facts[0]), _mb(available))


def mps_free_total_mb() -> tuple[int | None, int | None]:
    """`(free_mb, total_mb)` for a unified-memory device."""
    free_mb, total_mb, _, _ = _mps_free_with_basis()
    return (free_mb, total_mb)


def mps_ram_basis_mb() -> tuple[int | None, int | None]:
    """`(ram_total_mb, ram_available_mb)` behind an MPS free reading."""
    _, _, ram_total_mb, ram_available_mb = _mps_free_with_basis()
    return (ram_total_mb, ram_available_mb)


def mac_available_bytes() -> int | None:
    """RAM a new allocation could get on macOS, or None off it
    (`_mac_available`). psutil's `available` does not track MPS allocations.
    """
    facts = _mac_memory_counters()
    if facts is None:
        return None
    return _mac_available(facts)


def _mac_available(facts: tuple[int, int, int, int, int, int]) -> int:
    """Total RAM minus wired, compressed and anonymous pages; 0 at critical
    memory pressure, and at warning while the kernel is paging
    (`_mac_paging`): the file cache this formula counts as available is not
    free then. Warning without paging only means memory is held compressed.
    Same as `mps.rs::available_bytes`.
    """
    ram, wired, compressed, anonymous, pressure, swapouts = facts
    paging = _mac_paging(swapouts)
    if pressure >= MAC_PRESSURE_CRITICAL:
        return 0
    if pressure >= MAC_PRESSURE_WARNING and paging:
        return 0
    return max(0, ram - wired - compressed - anonymous)


def _mac_paging(swapouts: int) -> bool:
    """Whether macOS swapped pages out between the previous reading and this
    one, or within `MAC_PAGING_SECONDS` before it. A first reading, or one
    whose predecessor is older than `MAC_PAGING_STALE_SECONDS`, has nothing
    to compare with and is not paging.
    """
    now = time.monotonic()
    previous, read_at = _swapouts["count"], _swapouts["read_at"]
    _swapouts["count"], _swapouts["read_at"] = swapouts, now
    if (
        previous is not None
        and swapouts > previous
        and now - read_at <= MAC_PAGING_STALE_SECONDS
    ):
        _swapouts["rose_at"] = now
    rose_at = _swapouts["rose_at"]
    return rose_at is not None and now - rose_at <= MAC_PAGING_SECONDS


# `vm_statistics64_data_t` (<mach/vm_statistics.h>) layout and the flavour
# that fills it. Indexes: `wire_count`, `swapouts`, `compressor_page_count`,
# `internal_page_count` (pageable anonymous pages, so wired are not counted).
_VM_STATISTICS64 = "@4I9Q2I4Q4IQ"
_VM_WIRE, _VM_SWAPOUTS, _VM_COMPRESSOR, _VM_INTERNAL = 3, 18, 19, 22
_HOST_VM_INFO64 = 4

# `kern.memorystatus_vm_pressure_level` values.
MAC_PRESSURE_NORMAL, MAC_PRESSURE_WARNING, MAC_PRESSURE_CRITICAL = 1, 2, 4

# How long after the swap-out counter last rose the kernel counts as paging.
# Must match `mps.rs::PAGING_WINDOW`.
MAC_PAGING_SECONDS = 10.0

# The oldest previous reading a rise is still judged against: an older one
# cannot say when the counter rose. Longer than `MAC_PAGING_SECONDS` so that
# batches longer than that still see the kernel paging. Must match
# `mps.rs::PAGING_STALE`.
MAC_PAGING_STALE_SECONDS = 60.0

# The swap-out counter at the previous reading, when that was, and when the
# counter was last seen to rise.
_swapouts: dict[str, Any] = {"count": None, "read_at": None, "rose_at": None}


def _mac_pressure_level() -> int:
    """macOS's memory pressure level; unreadable counts as normal."""
    level = _sysctl_u32("kern.memorystatus_vm_pressure_level")
    return level or MAC_PRESSURE_NORMAL


def _mac_memory_counters() -> tuple[int, int, int, int, int, int] | None:
    """`(ram, wired, compressed, anonymous)` bytes, the memory pressure level
    and the pages swapped out since boot, from macOS; or None.
    """
    if sys.platform != "darwin":
        return None
    ram = _sysctl_u64("hw.memsize")
    if not ram:
        return None
    pressure = _mac_pressure_level()
    try:
        import ctypes
        import ctypes.util
        import struct

        libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.dylib", use_errno=True)
        size = struct.calcsize(_VM_STATISTICS64)
        buffer = ctypes.create_string_buffer(size)
        count = ctypes.c_uint(size // 4)
        failed = libc.host_statistics64(
            libc.mach_host_self(),
            ctypes.c_int(_HOST_VM_INFO64),
            buffer,
            ctypes.byref(count),
        )
        if failed:
            return None
        stats = struct.unpack(_VM_STATISTICS64, buffer.raw[:size])
        page = os.sysconf("SC_PAGE_SIZE")
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("macOS memory counters unreadable: %s", exc)
        return None
    if not isinstance(page, int) or page <= 0:
        return None
    return (
        ram,
        stats[_VM_WIRE] * page,
        stats[_VM_COMPRESSOR] * page,
        stats[_VM_INTERNAL] * page,
        pressure,
        stats[_VM_SWAPOUTS],
    )


def _virtual_memory() -> Any | None:
    """psutil's whole-machine memory statistics, or None."""
    try:
        import psutil
    except Exception:
        return None
    try:
        return psutil.virtual_memory()
    except Exception:
        return None


def _ram_available_bytes() -> int | None:
    """psutil's `virtual_memory().available` in bytes, on Linux less
    reclaimable slab (`_reclaimable_slab_bytes`), or None."""
    memory = _virtual_memory()
    if memory is None:
        return None
    try:
        return max(int(memory.available) - _reclaimable_slab_bytes(), 0)
    except Exception:
        return None


def mps_gpu_name() -> str | None:
    """`Apple M3 Max (128 GB)`, or None. Must match `mps.rs::gpu_name`."""
    chip = _sysctl_string("machdep.cpu.brand_string")
    ram = _sysctl_u64("hw.memsize")
    if chip is None or not ram:
        return None
    gib = 1024 * 1024 * 1024
    return f"{chip} ({max((ram + gib // 2) // gib, 1)} GB)"


def _sysctl(name: str, size: int) -> bytes | None:
    """`sysctlbyname(name)` as raw bytes, or None off macOS / on any error."""
    if sys.platform != "darwin":
        return None
    try:
        import ctypes
        import ctypes.util

        libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.dylib", use_errno=True)
        buffer = ctypes.create_string_buffer(size)
        length = ctypes.c_size_t(size)
        if libc.sysctlbyname(
            name.encode("ascii"),
            buffer,
            ctypes.byref(length),
            None,
            ctypes.c_size_t(0),
        ) != 0:
            return None
        return buffer.raw[: length.value]
    except Exception:
        return None


def _sysctl_string(name: str) -> str | None:
    raw = _sysctl(name, 512)
    if not raw:
        return None
    text = raw.split(b"\0", 1)[0].decode("utf-8", "replace").strip()
    return text or None


def _sysctl_u64(name: str) -> int | None:
    raw = _sysctl(name, 8)
    if raw is None or len(raw) != 8:
        return None
    return int.from_bytes(raw, sys.byteorder)


def _sysctl_u32(name: str) -> int | None:
    raw = _sysctl(name, 4)
    if raw is None or len(raw) != 4:
        return None
    return int.from_bytes(raw, sys.byteorder)


# --- CPU-only hosts: host RAM as the memory currency ---


def _ram_currency() -> bool:
    """Whether this worker's memory is host RAM: `INFERIO_DEVICE=cpu`, or torch
    is imported and reaches no accelerator. Never inferred from torch being
    absent (a remote-API worker).
    """
    return device_kind() == DEVICE_KIND_CPU


def _books_host_ram() -> bool:
    """Whether the orchestrator books this worker's host RAM
    (`_ram_side_bytes`) beside its GPU memory: a CUDA or ROCm worker. MPS
    memory is RAM already.
    """
    return device_kind() in ("cuda", "rocm")


def _forced_cpu() -> bool:
    """Whether the orchestrator pinned this replica to the CPU."""
    return (os.environ.get(DEVICE_ENV_VAR) or "").strip().lower() == "cpu"


def cgroup_limit_used_bytes(root: str | None = None) -> tuple[int | None, int]:
    """`(limit, used)` for this process's cgroup in bytes, or `(None, 0)` with
    no limit: cgroup v2 `memory.max`/`memory.current`, else v1. Same files and
    order as `cpu.rs::cgroup_limit_mb`. `used` excludes the reclaimable file
    cache (`active_file + inactive_file`), as `MemAvailable` does on the host.
    """
    base = CGROUP_ROOT if root is None else root
    limit = _sysfs_bytes(os.path.join(base, "memory.max"))
    if limit is not None:
        used = _sysfs_bytes(os.path.join(base, "memory.current")) or 0
        cache = _cgroup_file_lru(
            os.path.join(base, "memory.stat"), ("active_file", "inactive_file")
        )
        return (limit, max(used - cache, 0))
    limit = _sysfs_bytes(os.path.join(base, "memory", "memory.limit_in_bytes"))
    if limit is None:
        return (None, 0)
    usage = os.path.join(base, "memory", "memory.usage_in_bytes")
    used = _sysfs_bytes(usage) or 0
    cache = _cgroup_file_lru(
        os.path.join(base, "memory", "memory.stat"),
        ("total_active_file", "total_inactive_file"),
    )
    return (limit, max(used - cache, 0))


def _cgroup_file_lru(path: str, keys: tuple[str, ...]) -> int:
    """The named rows of a `memory.stat` summed in bytes; missing counts 0."""
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            text = handle.read(65536)
    except Exception:
        return 0
    total = 0
    for line in text.splitlines():
        name, _, value = line.partition(" ")
        if name in keys:
            try:
                total += int(value.strip())
            except ValueError:
                pass
    return total


def _reclaimable_slab_bytes() -> int:
    """`SReclaimable` from `/proc/meminfo` in bytes; 0 off Linux or if unread.
    `MemAvailable` counts it, but the kernel may not free it before it kills a
    process, so the free reading leaves it out, as `cpu.rs` does.
    """
    if not sys.platform.startswith("linux"):
        return 0
    try:
        with open(PROC_MEMINFO, encoding="utf-8", errors="replace") as meminfo:
            for line in meminfo:
                key, _, rest = line.partition(":")
                if key == "SReclaimable":
                    value, unit = rest.split()
                    return int(value) * 1024 if unit == "kB" else 0
    except (OSError, ValueError):
        pass
    return 0


def _ram_bounds_bytes(root: str | None = None) -> tuple[int | None, int | None]:
    """`(total, available)` in bytes for a CPU-priced host, or `(None, None)`:
    psutil (macOS available from `mac_available_bytes`; Linux less
    `SReclaimable`), bounded by the cgroup limit. Must match `cpu.rs`.
    """
    memory = _virtual_memory()
    if memory is None:
        return (None, None)
    try:
        total = int(memory.total)
        available = int(memory.available)
    except Exception:
        return (None, None)
    if total <= 0:
        return (None, None)
    mac_available = mac_available_bytes()
    if mac_available is not None:
        available = mac_available
    available = max(available - _reclaimable_slab_bytes(), 0)
    limit, used = cgroup_limit_used_bytes(root)
    if limit is not None:
        total = min(total, limit)
        available = min(available, max(limit - used, 0))
    return (total, available)


def ram_free_total_mb() -> tuple[int | None, int | None]:
    """`(free_mb, total_mb)` for a CPU-priced host, or `(None, None)`; free is
    the available RAM. See docs/unified-memory-admission.md.
    """
    total, available = _ram_bounds_bytes()
    if total is None or available is None:
        return (None, None)
    return (_mb(min(total, available)), _mb(total))


def ram_gpu_name() -> str | None:
    """`CPU (64 GB)`, or None. Must match `cpu.rs::gpu_name`."""
    total, _ = _ram_bounds_bytes()
    if total is None:
        return None
    total_mb = total // _MIB
    if total_mb <= 0:
        return None
    # Rounded up to 4 GiB: the OS's total RAM moves with kernel and BIOS.
    grid = 4 * 1024
    return f"CPU ({max(-(-total_mb // grid) * 4, 4)} GB)"


@lru_cache(maxsize=1)
def _psutil_process(pid: int) -> Any:
    """`psutil.Process` for `pid`, cached: construction costs 3x the read, and
    the RSS sampler reads every 20 ms.
    """
    import psutil

    return psutil.Process(pid)


def _rss_bytes() -> int | None:
    """This process's resident set right now in bytes, or None."""
    try:
        return int(_psutil_process(os.getpid()).memory_info().rss)
    except Exception:
        return None


def parse_ram_side(text: str) -> int | None:
    """`RssAnon + VmSwap` from `/proc/<pid>/status` in bytes, or None without a
    `RssAnon` row in `kB`; a missing `VmSwap` row counts as 0.
    """
    rows: dict[str, int] = {}
    for line in text.splitlines():
        key, separator, rest = line.partition(":")
        fields = rest.split()
        if separator and key.strip() in ("RssAnon", "VmSwap") and len(fields) == 2:
            if fields[1] != "kB" or not fields[0].isdigit():
                return None
            rows[key.strip()] = int(fields[0]) * 1024
    if "RssAnon" not in rows:
        return None
    return rows["RssAnon"] + rows.get("VmSwap", 0)


def _ram_side_bytes() -> int | None:
    """A CUDA or ROCm worker's host RAM in bytes, or None: anonymous plus
    swapped memory on Linux, private commit on Windows, the resident set
    elsewhere. Reclaimed file pages leave it unchanged, so a fall is memory
    the worker released. The orchestrator reads the same figure for this
    process (`cpu.rs`).
    """
    if sys.platform.startswith("linux"):
        try:
            with open(PROC_STATUS, encoding="utf-8", errors="replace") as status:
                return parse_ram_side(status.read())
        except Exception:
            return None
    if sys.platform == "win32":
        try:
            return int(_psutil_process(os.getpid()).memory_info().private)
        except Exception:
            return None
    return _rss_bytes()


def parse_vm_high_water(text: str) -> int | None:
    """`VmHWM` from `/proc/self/status` in bytes (`kB` means KiB), or None."""
    for line in text.splitlines():
        key, separator, rest = line.partition(":")
        if not separator or key.strip() != "VmHWM":
            continue
        fields = rest.split()
        if len(fields) != 2 or fields[1] != "kB":
            return None
        try:
            return int(fields[0]) * 1024
        except ValueError:
            return None
    return None


def _rusage_peak_bytes() -> int | None:
    """`ru_maxrss` in bytes, or None: bytes on macOS, KiB on other Unixes."""
    try:
        import resource
    except Exception:
        return None
    try:
        peak = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    except Exception:
        return None
    if peak <= 0:
        return None
    return peak if sys.platform == "darwin" else peak * 1024


def _peak_rss_bytes() -> int | None:
    """The OS's lifetime high-water mark for this process's resident set, or
    None. Not resettable, so it is reported as the pool ("reserved")."""
    peak: int | None = None
    if sys.platform.startswith("linux"):
        try:
            with open(PROC_STATUS, encoding="utf-8", errors="replace") as status:
                peak = parse_vm_high_water(status.read())
        except Exception:
            peak = None
    elif sys.platform == "win32":
        try:
            import psutil

            peak = int(getattr(psutil.Process().memory_info(), "peak_wset", 0)) or None
        except Exception:
            peak = None
    else:
        peak = _rusage_peak_bytes()
    rss = _rss_bytes()
    if peak is None:
        return rss
    return peak if rss is None else max(peak, rss)


@lru_cache(maxsize=1)
def _malloc_trim() -> Any | None:
    """glibc's `malloc_trim`, or None where the C library has none."""
    if not sys.platform.startswith("linux"):
        return None
    try:
        import ctypes

        return ctypes.CDLL(None).malloc_trim
    except (OSError, AttributeError):
        return None


def return_freed_memory() -> None:
    """Hand the C heap's free pages back to the OS (glibc `malloc_trim(0)`),
    so the resident set holds only live memory. A no-op without glibc."""
    trim = _malloc_trim()
    if trim is None:
        return
    try:
        trim(0)
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("malloc_trim failed: %s", exc)


def ram_pool_mb() -> tuple[int | None, int | None]:
    """`(pool_mb, resident_mb)` for a CPU-priced host: peak RSS stands in for
    the allocator pool, live RSS for `allocated`.
    """
    return (_mb(_peak_rss_bytes()), _mb(_rss_bytes()))


def torch_version() -> str | None:
    """`torch.__version__`, or None. Part of the calibration profile key."""
    torch = _torch()
    version = getattr(torch, "__version__", None) if torch is not None else None
    return str(version) if version is not None else None


# --- Samples ---


def device_memory_sample() -> dict[str, Any] | None:
    """A memory sample for this worker's GPU, or None if nothing is known. Wire
    shape: docs/inferio-worker-protocol.md "Memory sensing"."""
    reserved_mb, allocated_mb, _, _ = _allocator_stats()
    reading = _free_total_reading()
    sample: dict[str, Any] = {
        "free_mb": reading.free_mb,
        "total_mb": reading.total_mb,
        "free_source": reading.source,
        "reserved_mb": reserved_mb,
        "allocated_mb": allocated_mb,
    }
    if reading.source == "mps":
        sample["ram_total_mb"] = reading.ram_total_mb
        sample["ram_available_mb"] = reading.ram_available_mb
    if all(value is None for value in sample.values()):
        return None
    return sample


def pool_stats_mb() -> tuple[int | None, int | None]:
    """`(reserved_mb, allocated_mb)` for our allocator, or `(None, None)`,
    without a driver query.
    """
    reserved, allocated, _, _ = _allocator_stats()
    return (reserved, allocated)


def unreturnable_split_mb() -> int | None:
    """Free pool MiB that `empty_cache()` cannot return: the rest of segments
    split by a live block (`inactive_split_bytes.all.current`). CUDA only; None
    elsewhere, including MPS, which has no such statistic.
    """
    if _ram_currency():
        return None
    torch = _torch_cuda()
    if torch is None:
        return None
    try:
        stats = torch.cuda.memory_stats()
        return _mb(stats["inactive_split_bytes.all.current"])
    except Exception:
        return None


# Allocated MiB when the first load finished; None until then.
_allocated_at_load_mb: int | None = None


def held_since_load_mb() -> int:
    """MiB allocated now above the level at load: what earlier batches left
    allocated. 0 on a RAM-priced worker, or when either figure is unknown.
    """
    if _ram_currency() or _allocated_at_load_mb is None:
        return 0
    _, allocated = pool_stats_mb()
    if allocated is None:
        return 0
    return max(0, allocated - _allocated_at_load_mb)


def releasable_pool_mb() -> int | None:
    """Pool this process holds that a batch can spend without a new device
    allocation: `reserved - allocated`. None on a RAM-priced worker, where
    freed pages are already in the free reading.
    """
    if _ram_currency():
        return None
    reserved, allocated = pool_stats_mb()
    if reserved is None or allocated is None:
        return None
    return max(0, reserved - allocated)


# Who asked for a pool release; reported with the next batch's re-grow.
TRIM_RELEASE = "trim"
SHRINK_RELEASE = "shrink"
IMPL_RELEASE = "impl"
GROWTH_RELEASE = "growth"
SPILL_RELEASE = "spill"

# The last release, whether the next batch's pool growth is its re-grow, and
# the largest batch (in units) run since it; None until a batch runs.
_release_state: dict[str, Any] = {
    "armed": False,
    "released_mb": None,
    "release_ms": None,
    "trigger": None,
    "largest_units": None,
}

# The WSL2 GPU device: CUDA through the Windows display driver.
DXG_DEVICE = "/dev/dxg"


def spill_capable() -> bool:
    """Whether a full GPU moves this worker's memory to system RAM instead of
    failing the allocation: CUDA under the Windows display driver (native
    Windows, or WSL2 and Docker Desktop through `/dev/dxg`)."""
    if device_kind() != "cuda":
        return False
    return sys.platform == "win32" or os.path.exists(DXG_DEVICE)


def outgrows_pool(units: int) -> bool:
    """Whether a batch of `units` is larger than every batch run since the
    pool was last released. False when none has run since."""
    largest = _release_state["largest_units"]
    return largest is not None and units > largest


def note_batch_units(units: int) -> None:
    """Record a batch that ran, for `outgrows_pool`."""
    _release_state["largest_units"] = max(_release_state["largest_units"] or 0, units)


def _note_release(
    released_mb: int | None, elapsed_ms: float, trigger: str, arm: bool = True
) -> None:
    """Record a completed release and arm the next batch's re-grow report."""
    _release_state["armed"] = arm
    _release_state["released_mb"] = released_mb
    _release_state["release_ms"] = round(elapsed_ms, 3)
    _release_state["trigger"] = trigger
    _release_state["largest_units"] = None
    logger.debug(
        "released the allocator pool (%s): handed back %s MiB in %.1f ms%s",
        trigger,
        "?" if released_mb is None else released_mb,
        elapsed_ms,
        "; the next batch pays the re-grow" if arm else "",
    )


def last_release() -> tuple[int | None, float | None]:
    """`(released_mb, release_ms)` of the most recent release, for the `trim`
    reply; `(None, None)` before any release ran."""
    return (_release_state["released_mb"], _release_state["release_ms"])


def empty_cache(trigger: str = TRIM_RELEASE, arm: bool = True) -> bool:
    """Release the caching allocator's unused pool; returns whether it ran.
    The only place the pool is released, so it sizes, times and logs the
    release and arms the next batch's re-grow report. A no-op on a CPU-priced
    host. `IMPL_RELEASE` passes `arm=False`: it runs inside `predict`.
    """
    if _ram_currency():
        return False
    torch = _torch_cuda()
    if torch is None:
        return _mps_empty_cache(trigger, arm)
    before, _ = pool_stats_mb()
    started = time.perf_counter()
    try:
        torch.cuda.empty_cache()
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("empty_cache failed: %s", exc)
        return False
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    after, _ = pool_stats_mb()
    released = None if (before is None or after is None) else max(before - after, 0)
    _note_release(released, elapsed_ms, trigger, arm)
    return True


def _mps_empty_cache(trigger: str, arm: bool = True) -> bool:
    """The MPS arm of `empty_cache`."""
    torch = _torch_mps()
    if torch is None:
        return False
    before, _ = pool_stats_mb()
    started = time.perf_counter()
    try:
        release = getattr(torch.mps, "empty_cache", None)
        if release is None:
            return False
        release()
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("mps empty_cache failed: %s", exc)
        return False
    after, _ = pool_stats_mb()
    released = None if (before is None or after is None) else max(before - after, 0)
    _note_release(released, (time.perf_counter() - started) * 1000.0, trigger, arm)
    return True


def alloc_retries() -> int | None:
    """`num_alloc_retries`: times the CUDA caching allocator freed cached blocks
    and retried a `cudaMalloc`; a rising count means the device is full. CUDA
    only, None elsewhere.
    """
    if _ram_currency():
        return None
    torch = _torch_cuda()
    if torch is None:
        return None
    try:
        return int(torch.cuda.memory_stats()["num_alloc_retries"])
    except Exception:
        return None


def _reset_peaks() -> None:
    torch = _torch_cuda()
    if torch is None:
        return
    try:
        torch.cuda.reset_peak_memory_stats()
    except Exception:
        pass


def _allocator_stats() -> tuple[int | None, int | None, int | None, int | None]:
    """`(reserved, allocated, peak_reserved, peak_allocated)` in MiB. On MPS and
    CPU the peak slots hold the live figures, so `peak > before` still means
    "this batch grew the pool" (see `mps_pool_mb`, `ram_pool_mb`)."""
    if _ram_currency():
        pool, resident = ram_pool_mb()
        return (pool, resident, pool, resident)
    torch = _torch_cuda()
    if torch is None:
        reserved, allocated = mps_pool_mb()
        return (reserved, allocated, reserved, allocated)
    try:
        return (
            _mb(torch.cuda.memory_reserved()),
            _mb(torch.cuda.memory_allocated()),
            _mb(torch.cuda.max_memory_reserved()),
            _mb(torch.cuda.max_memory_allocated()),
        )
    except Exception:
        return (None, None, None, None)


class FreeReading(NamedTuple):
    """One free-memory reading, its source and, on MPS, its RAM basis."""

    free_mb: int | None
    total_mb: int | None
    source: str | None
    ram_total_mb: int | None = None
    ram_available_mb: int | None = None


def _free_total_reading(source: str | None = None) -> FreeReading:
    """`(free_mb, total_mb, source)` and its basis: `ram`|`nvml`|`amdgpu-sysfs`|`mps`|`torch`.

    The one place free/total memory is read. Sources are tried in that order;
    `torch` (`mem_get_info`) is last and non-authoritative (process-local on
    some HIP versions). A delta is only meaningful within one source.
    """
    if source in (None, "ram") and _ram_currency():
        # Source labels must match the orchestrator's (`gpu.rs::free_source`).
        free, total = ram_free_total_mb()
        if free is not None:
            return FreeReading(free, total, "ram")
        return FreeReading(None, None, None)
    if source in (None, "nvml"):
        free, total = _nvml_memory()
        if free is not None:
            return FreeReading(free, total, "nvml")
    if source in (None, "amdgpu-sysfs"):
        free, total = amdgpu_free_total_mb()
        if free is not None:
            return FreeReading(free, total, "amdgpu-sysfs")
    if source in (None, "mps"):
        free, total, ram_total, ram_available = _mps_free_with_basis()
        if free is not None:
            return FreeReading(free, total, "mps", ram_total, ram_available)
    if source in (None, "torch"):
        torch = _torch_cuda()
        if torch is not None:
            try:
                free, total = torch.cuda.mem_get_info()
                free_mb, total_mb = _mb(free), _mb(total)
            except Exception:
                free_mb = total_mb = None
            if free_mb is not None:
                return FreeReading(free_mb, total_mb, "torch")
    return FreeReading(None, None, None)


def _free_total_mb(
    source: str | None = None,
) -> tuple[int | None, int | None, str | None]:
    """`(free_mb, total_mb, source)`, without the RAM basis."""
    reading = _free_total_reading(source)
    return (reading.free_mb, reading.total_mb, reading.source)


def _free_mb(source: str | None = None) -> tuple[int | None, str | None]:
    """`(free_mb, source)`."""
    free_mb, _, resolved = _free_total_mb(source)
    return (free_mb, resolved)


def free_total_mb() -> tuple[int | None, int | None, str | None]:
    """Live `(free_mb, total_mb, source)` reading for this worker's GPU."""
    return _free_total_mb()


def free_total_reading() -> FreeReading:
    """The same reading with its RAM basis attached."""
    return _free_total_reading()


# --- Accelerator context: measured once per process ---

# The context this process measured for itself (MiB) and the running probe.
_context_state: dict[str, Any] = {
    "measured_mb": None,
    "logged": False,
    "probe": None,
}


class _ContextProbe:
    """Measures the accelerator context as the free-memory delta across this
    process's first CUDA initialisation, from a daemon thread polling
    `is_initialized()`. See docs/inferio-worker-protocol.md "The accelerator
    context probe".
    """

    def __init__(
        self,
        free_before: int | None,
        free_source: str | None,
        torch_reader: Any = None,
        free_reader: Any = None,
        reserved_reader: Any = None,
    ) -> None:
        self._free_before = free_before
        self._torch = torch_reader or _torch
        self._read_free = free_reader or (lambda: _free_mb(free_source)[0])
        self._read_reserved = reserved_reader or _reserved_mb_unguarded
        self._free_at_init: int | None = None
        self._reserved_at_init: int = 0
        self._baseline_at = time.monotonic()
        self._done = False
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def poll(self) -> bool:
        """One observation; True once the probe has its answer or gave up."""
        if self._done:
            return True
        torch = self._torch()
        if torch is None:
            return False
        try:
            live = torch.cuda.is_initialized()
        except Exception:
            self._done = True
            return True
        if not live:
            self._refresh_baseline(torch)
            return False
        try:
            # Pool before free, so a racing allocation over-states the context.
            self._reserved_at_init = self._read_reserved() or 0
            self._free_at_init = self._read_free()
        except Exception:  # pragma: no cover - defensive
            self._free_at_init = None
        self._done = True
        return True

    def _refresh_baseline(self, torch: Any) -> None:
        """Re-read the pre-initialisation baseline, so external memory changes
        during a long load stay out of the delta. A reading that raced
        initialisation is discarded.
        """
        now = time.monotonic()
        if now - self._baseline_at < _CONTEXT_BASELINE_SECONDS:
            return
        self._baseline_at = now
        try:
            candidate = self._read_free()
            if torch.cuda.is_initialized():
                return
        except Exception:  # pragma: no cover - defensive
            return
        if candidate is not None:
            self._free_before = candidate

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._watch, name="inferio-context-probe", daemon=True
        )
        self._thread.start()

    def _watch(self) -> None:
        deadline = time.monotonic() + _CONTEXT_PROBE_MAX_SECONDS
        while not self._stop.is_set() and time.monotonic() < deadline:
            try:
                if self.poll():
                    return
            except Exception:  # pragma: no cover - defensive
                return
            self._stop.wait(_CONTEXT_POLL_SECONDS)

    def result(self) -> int | None:
        """Stop watching; the measured context in MiB, or None if unmeasured or
        outside `CONTEXT_MIN_MB`..`CONTEXT_MAX_MB`.
        """
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=1.0)
        if self._free_before is None or self._free_at_init is None:
            return None
        measured = self._free_before - self._free_at_init - self._reserved_at_init
        if measured < CONTEXT_MIN_MB or measured > CONTEXT_MAX_MB:
            logger.debug(
                "discarding a %d MiB context measurement: outside the "
                "%d-%d MiB band a context can plausibly occupy",
                measured,
                CONTEXT_MIN_MB,
                CONTEXT_MAX_MB,
            )
            return None
        return measured


def _reserved_mb_unguarded() -> int | None:
    """The allocator pool size in MiB, for the probe thread."""
    torch = _torch_cuda()
    if torch is None:
        return None
    try:
        return _mb(torch.cuda.memory_reserved())
    except Exception:  # pragma: no cover - defensive
        return None


def _start_context_probe(
    free_mb: int | None, free_source: str | None
) -> "_ContextProbe | None":
    """Start the context probe, or None when no measurement is possible: no
    driver-level reading, a RAM-priced process, CUDA already initialised, or a
    context already measured. A leftover probe is collected first."""
    _collect_context_probe(announce=False)
    if free_mb is None or free_source not in ("nvml", "amdgpu-sysfs"):
        return None
    if _ram_currency() or _context_state["measured_mb"] is not None:
        return None
    torch = _torch()
    if torch is not None:
        try:
            if torch.cuda.is_initialized():
                return None
        except Exception:
            return None
    probe = _ContextProbe(free_mb, free_source)
    probe.start()
    _context_state["probe"] = probe
    return probe


def _collect_context_probe(
    probe: "_ContextProbe | None" = None, announce: bool = True
) -> None:
    """Stop any running context probe and keep its result."""
    running = _context_state.get("probe")
    _context_state["probe"] = None
    seen: list[Any] = []
    for candidate in (probe, running):
        if candidate is None or any(candidate is other for other in seen):
            continue
        seen.append(candidate)
        try:
            measured = candidate.result()
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("could not collect the context probe: %s", exc)
            continue
        if measured is not None or announce:
            _remember_context_mb(measured)


def abort_load(before: dict[str, Any]) -> None:
    """Release what `begin_load` started, for a load that raised; keeps the
    probe's measurement. Never raises.
    """
    try:
        probe = before.get("context_probe") if isinstance(before, dict) else None
        _collect_context_probe(probe, announce=False)
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("post-failure memory cleanup failed: %s", exc)


def context_allowance_mb() -> tuple[int, str]:
    """`(mb, "measured"|"estimate")`: the device memory this process holds
    outside the caching allocator.
    """
    measured = _context_state["measured_mb"]
    if measured is not None:
        return (int(measured), "measured")
    return (CONTEXT_ESTIMATE_MB, "estimate")


def _remember_context_mb(measured: int | None) -> None:
    """Cache a context measurement and log once which figure is used."""
    if measured is not None and _context_state["measured_mb"] is None:
        _context_state["measured_mb"] = measured
    if _context_state["logged"]:
        return
    _context_state["logged"] = True
    allowance, source = context_allowance_mb()
    if source == "measured":
        logger.info(
            "measured this process's accelerator context at %d MiB across its "
            "first CUDA initialisation; using it instead of the %d MiB estimate",
            allowance,
            CONTEXT_ESTIMATE_MB,
        )
    else:
        logger.info(
            "could not measure this process's accelerator context; using the "
            "%d MiB estimate",
            CONTEXT_ESTIMATE_MB,
        )


def begin_load() -> dict[str, Any]:
    """Snapshot what `finish_load` needs to price the load. Never raises. The
    free reading comes before any torch.cuda call, so the context the load
    creates falls inside the measured window."""
    try:
        free_mb, free_source = _free_mb()
        # Before the peak reset: the baseline must predate the CUDA context.
        probe = _start_context_probe(free_mb, free_source)
        _reset_peaks()
        reserved, allocated, _, _ = _allocator_stats()
        return {
            "free_mb": free_mb,
            "free_source": free_source,
            "reserved_mb": reserved,
            "allocated_mb": allocated,
            "context_probe": probe,
        }
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("pre-load memory snapshot failed: %s", exc)
        return {}


def finish_load(before: dict[str, Any], instance: Any) -> dict[str, Any]:
    """Load-response payload: base, provenance, pool size, dtype, sample.
    Unmeasured keys are omitted.
    """
    try:
        return _finish_load(before, instance)
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("base measurement failed: %s", exc)
        return {}


def _finish_load(before: dict[str, Any], instance: Any) -> dict[str, Any]:
    # First: everything below may use the context figure.
    _collect_context_probe(before.get("context_probe"))
    # The resident figures below then hold the loaded model and nothing the
    # load freed.
    return_freed_memory()
    reserved, allocated, _, peak_allocated = _allocator_stats()
    free_after, _ = _free_mb(before.get("free_source"))

    allocated_delta = _delta(allocated, before.get("allocated_mb"))
    reserved_delta = _delta(reserved, before.get("reserved_mb"))
    # The allocator's peak delta is the floor under every other tier.
    alloc_floor = _delta(peak_allocated, before.get("allocated_mb"))
    if alloc_floor is None:
        alloc_floor = allocated_delta
    # Whether this process itself allocated on the device.
    touched_gpu = (allocated_delta or 0) > 0 or (reserved_delta or 0) > 0

    base_mb, method = _resolve_base(
        before=before,
        free_after=free_after,
        reserved_mb=reserved,
        reserved_delta=reserved_delta,
        alloc_floor=alloc_floor,
        touched_gpu=touched_gpu,
    )

    payload: dict[str, Any] = {}
    if base_mb is not None and method is not None:
        payload["base_mb"] = base_mb
        payload["base_method"] = method
    if reserved is not None:
        payload["reserved_at_load_mb"] = reserved
    if allocated is not None:
        payload["allocated_at_load_mb"] = allocated
        global _allocated_at_load_mb
        if _allocated_at_load_mb is None:
            _allocated_at_load_mb = allocated
    dtype, dtype_method = resolved_dtype(instance)
    # The unstated sentinel is only useful with a footprint to key.
    if dtype != DTYPE_UNSTATED or "base_mb" in payload:
        payload["dtype"] = dtype
        payload["dtype_method"] = dtype_method
    uuid, name = device_identity()
    if uuid is not None:
        payload["gpu_uuid"] = uuid
    if name is not None:
        payload["gpu_name"] = name
    arch = device_arch()
    if arch is not None:
        payload["gpu_arch"] = arch
    # The memoized value, so the wire field matches the amdgpu readings' filter.
    bdf = _identity_bdf()
    if bdf is not None:
        payload["gpu_bdf"] = bdf
    total_mb = gpu_total_mb()
    if total_mb is not None:
        payload["gpu_total_mb"] = total_mb
    version = torch_version()
    if version is not None:
        payload["torch_version"] = version
    kind = device_kind()
    if kind is not None:
        payload["device_kind"] = kind
    if _books_host_ram():
        rss = _mb(_ram_side_bytes())
        if rss is not None:
            payload["rss_at_load_mb"] = rss
            payload["pid"] = os.getpid()
    sample = device_memory_sample()
    if sample is not None:
        payload["memory"] = sample
    # Keep load peaks out of the first batch's measurement.
    _reset_peaks()
    return payload


def _resolve_base(
    before: dict[str, Any],
    free_after: int | None,
    reserved_mb: int | None,
    reserved_delta: int | None,
    alloc_floor: int | None,
    touched_gpu: bool,
) -> tuple[int | None, str | None]:
    """`(base_mb, base_method)`, or `(None, None)`.

    1. A process that did not allocate through torch reports nothing.
    2. A per-process figure wins: NVML, fdinfo (ROCm), or MPS
       `driver_allocated_memory()`.
    3. Otherwise the free-memory delta, if positive and not implausibly larger
       than the pool's growth (`reserved_delta`) plus the context and slack.
    4. A free delta below the allocator floor loses to the floor.
    """
    if not touched_gpu:
        return (None, None)
    # CPU-priced: RSS growth across the load window (`alloc_floor` is RSS here).
    if _ram_currency():
        return (alloc_floor, "rss") if alloc_floor else (None, None)
    own = _nvml_own_process_mb(holding_mb=reserved_mb)
    if own is not None and own > 0:
        return (own, "nvml")
    own = _fdinfo_base_mb(reserved_mb, reserved_delta)
    if own is not None and own > 0:
        return (own, "fdinfo")
    # On MPS each process owns its heap, so this is its whole footprint.
    own = _mb(_mps_call("driver_allocated_memory"))
    if own is not None and own > 0:
        return (own, "mps")

    floor = alloc_floor or 0
    context_mb, context_source = context_allowance_mb()
    # A stored profile records whether its context was measured or assumed.
    alloc_method = (
        "alloc_delta_measured" if context_source == "measured" else "alloc_delta"
    )
    free_delta = _free_delta(before.get("free_mb"), free_after)
    ceiling = (reserved_delta or 0) + context_mb + IMPLAUSIBLE_SLACK_MB
    if free_delta is None or free_delta <= 0 or free_delta > ceiling:
        if free_delta is not None and free_delta > ceiling:
            logger.debug(
                "free-memory delta %d MiB implausible against %d MiB reserved "
                "(+%d MiB %s context, +%d MiB workspace allowance); using the "
                "allocator delta plus the context allowance",
                free_delta,
                reserved_delta or 0,
                context_mb,
                context_source,
                IMPLAUSIBLE_SLACK_MB,
            )
        return (floor + context_mb, alloc_method)
    if free_delta >= floor:
        return (free_delta, "free_delta")
    return (floor + context_mb, alloc_method)


def _delta(after: int | None, before: int | None) -> int | None:
    """Growth of an allocator counter, clamped at 0 (`None` before counts 0)."""
    if after is None:
        return None
    return max(after - (before or 0), 0)


def _free_delta(before_mb: int | None, after_mb: int | None) -> int | None:
    """Free memory lost across the load window, unclamped; None without both."""
    if before_mb is None or after_mb is None:
        return None
    return before_mb - after_mb


# --- dtype provenance ---

_DTYPE_NAMES = {
    "torch.float16": "fp16",
    "torch.bfloat16": "bf16",
    "torch.float32": "fp32",
    "float16": "fp16",
    "bfloat16": "bf16",
    "float32": "fp32",
    "fp16": "fp16",
    "bf16": "bf16",
    "fp32": "fp32",
    "half": "fp16",
    "float": "fp32",
}


# Reported when nothing states a precision; a value, not an omission, since
# `dtype` is part of the calibration profile key.
DTYPE_UNSTATED = "unstated"

# `dtype_method`: stated by the impl, a `torch.dtype` attribute, read off the
# loaded weights, or unknown.
DTYPE_METHOD_SELECTED = "selected"
DTYPE_METHOD_ATTRIBUTE = "attribute"
DTYPE_METHOD_INFERRED = "inferred"
DTYPE_METHOD_UNSTATED = "unstated"

# Bounds on the search for a `torch.nn.Module` inside the impl instance.
_WALK_DEPTH = 2
_WALK_BUDGET = 256

# Elements of one container the walk unpacks.
_WALK_FANOUT = 16

# Attribute names tried first; the walk reports the first module it finds.
_MODEL_ATTRS = ("model", "_model", "module", "net", "pipeline", "reader")


def _dtype_name(value: Any) -> str | None:
    if value is None:
        return None
    return _DTYPE_NAMES.get(str(value).strip().lower())


def _is_torch_dtype(value: Any) -> bool:
    """Whether `value` is an actual `torch.dtype` (not a config string)."""
    torch = _torch()
    dtype_type = getattr(torch, "dtype", None) if torch is not None else None
    if not isinstance(dtype_type, type):
        return False
    return isinstance(value, dtype_type)


def resolved_dtype_name(instance: Any) -> str | None:
    """The precision the impl stated, or None. In order:
    `instance.resolved_dtype`, the last `inferio.impl.utils.select_dtype`
    decision, and a `dtype`/`_dtype` attribute holding a real `torch.dtype`.
    """
    return _stated_dtype(instance)[0]


def _stated_dtype(instance: Any) -> tuple[str | None, str | None]:
    """`(name, method)` from the three sources that state a precision."""
    name = _dtype_name(getattr(instance, "resolved_dtype", None))
    if name is not None:
        return name, DTYPE_METHOD_SELECTED
    utils = sys.modules.get("inferio.impl.utils")
    getter = getattr(utils, "last_selected_dtype", None) if utils else None
    if getter is not None:
        try:
            name = _dtype_name(getter())
        except Exception:
            name = None
        if name is not None:
            return name, DTYPE_METHOD_SELECTED
    for attribute in ("dtype", "_dtype"):
        value = getattr(instance, attribute, None)
        if _is_torch_dtype(value):
            name = _dtype_name(value)
            if name is not None:
                return name, DTYPE_METHOD_ATTRIBUTE
    return None, None


def _walk_children(value: Any) -> list[Any]:
    """The objects one level inside `value`. Reads `__dict__`, never `getattr`
    on properties, which could load or move a model."""
    if isinstance(value, (str, bytes, bytearray)):
        return []
    # A Python module is never a `torch.nn.Module`.
    if isinstance(value, ModuleType):
        return []
    if isinstance(value, (list, tuple, set, frozenset)):
        return list(value)[:_WALK_FANOUT]
    if isinstance(value, dict):
        return list(value.values())[:_WALK_FANOUT]
    try:
        namespace = getattr(value, "__dict__", None)
    except Exception:
        return []
    if isinstance(namespace, dict):
        named = [namespace[name] for name in _MODEL_ATTRS if name in namespace]
        rest = [
            child
            for name, child in namespace.items()
            if name not in _MODEL_ATTRS
        ]
        return named + rest
    return []


def _module_dtype_name(module: Any) -> str | None:
    """The first float parameter's (else buffer's) dtype name."""
    for accessor in ("parameters", "buffers"):
        getter = getattr(module, accessor, None)
        if not callable(getter):
            continue
        try:
            for tensor in getter():
                name = _dtype_name(getattr(tensor, "dtype", None))
                if name is not None:
                    return name
        except Exception:
            continue
    return None


def _inferred_dtype_name(instance: Any) -> str | None:
    """The dtype of the loaded weights: a bounded breadth-first walk finds the
    `torch.nn.Module` the instance holds and reads its first float parameter.
    """
    torch = _torch()
    nn = getattr(torch, "nn", None) if torch is not None else None
    module_type = getattr(nn, "Module", None)
    if not isinstance(module_type, type):
        return None
    seen: set[int] = set()
    queue: deque[tuple[Any, int]] = deque([(instance, 0)])
    budget = _WALK_BUDGET
    while queue and budget > 0:
        obj, depth = queue.popleft()
        if id(obj) in seen:
            continue
        seen.add(id(obj))
        budget -= 1
        if isinstance(obj, module_type):
            name = _module_dtype_name(obj)
            if name is not None:
                return name
            # `parameters()` already recursed into its submodules.
            continue
        if depth >= _WALK_DEPTH:
            continue
        for child in _walk_children(obj):
            queue.append((child, depth + 1))
    return None


def resolved_dtype(instance: Any) -> tuple[str, str]:
    """`(dtype, dtype_method)` for the load response, never absent: the stated
    dtype, else the loaded weights' dtype, else the unstated sentinel.
    """
    name, method = _stated_dtype(instance)
    if name is not None and method is not None:
        return name, method
    try:
        name = _inferred_dtype_name(instance)
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("dtype inference failed: %s", exc)
        name = None
    if name is not None:
        return name, DTYPE_METHOD_INFERRED
    return DTYPE_UNSTATED, DTYPE_METHOD_UNSTATED


# --- Per-batch measurement ---


class _PeakSampler:
    """A daemon thread keeping the largest reading `observe` took during a
    batch, until stopped or its deadline. Subclasses set their counters before
    calling this constructor, which starts the thread.
    """

    _thread_name = "inferio-peak"

    def __init__(self, interval: float = MPS_SAMPLE_SECONDS) -> None:
        self._interval = interval
        self._deadline = time.monotonic() + MPS_SAMPLE_MAX_SECONDS
        self._stopped = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name=self._thread_name, daemon=True
        )
        self._thread.start()

    def _run(self) -> None:
        while not self._stopped.wait(self._interval):
            self.observe()
            if time.monotonic() >= self._deadline:  # pragma: no cover - timing
                return

    def observe(self) -> None:
        """One reading of every counter, kept if it is the largest so far."""
        raise NotImplementedError

    def _finish(self) -> None:
        self._stopped.set()
        self._thread.join(timeout=_MPS_SAMPLE_JOIN_SECONDS)
        # The batch's last state, which the loop may have missed.
        self.observe()


class _MpsPeakSampler(_PeakSampler):
    """The highest reading of both MPS counters seen while a batch runs.

    MPS has no peak counter, and the allocator frees cached buffers near its
    ceiling, so a post-batch reading under-states the peak. The in-batch
    maximum of `current_allocated_memory` is the allocated peak; whether the
    pool grew is answered by `reserved_after_mb`, not by these peaks.
    """

    _thread_name = "inferio-mps-peak"

    def __init__(self, interval: float = MPS_SAMPLE_SECONDS) -> None:
        self._peak = 0
        self._peak_allocated = 0
        super().__init__(interval)

    def observe(self) -> None:
        """One reading of each counter, kept if it is the largest so far."""
        driver = _mps_call("driver_allocated_memory")
        if driver is not None and driver > self._peak:
            self._peak = driver
        allocated = _mps_call("current_allocated_memory")
        if allocated is not None and allocated > self._peak_allocated:
            self._peak_allocated = allocated

    def stop(self) -> tuple[int | None, int | None]:
        """`(pool_mb, allocated_mb)` peaks, either None if never readable."""
        self._finish()
        return (
            _mb(self._peak) if self._peak else None,
            _mb(self._peak_allocated) if self._peak_allocated else None,
        )


class _RssPeakSampler(_PeakSampler):
    """The highest reading of `read` seen while a batch runs: the RSS on a
    RAM-priced worker, `_ram_side_bytes` on a GPU worker. The OS high-water
    mark cannot be reset, so it would hide the batch's cost behind the load's
    own peak.
    """

    _thread_name = "inferio-rss-peak"

    def __init__(
        self, read: Callable[[], int | None], interval: float = MPS_SAMPLE_SECONDS
    ) -> None:
        self._read = read
        self._peak = read() or 0
        super().__init__(interval)

    def observe(self) -> None:
        rss = self._read()
        if rss is not None and rss > self._peak:
            self._peak = rss

    def stop(self) -> int | None:
        """The in-batch maximum in MiB, None if it was never readable."""
        self._finish()
        return _mb(self._peak) if self._peak else None


def _mps_peak_sampler() -> _MpsPeakSampler | None:
    """A running sampler on an MPS worker, None anywhere else."""
    if _ram_currency() or _torch_mps() is None:
        return None
    try:
        return _MpsPeakSampler()
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("the MPS peak sampler did not start: %s", exc)
        return None


def _rss_peak_sampler() -> _RssPeakSampler | None:
    """A running sampler on a CPU-priced or GPU worker, None anywhere else."""
    if not (_ram_currency() or _books_host_ram()):
        return None
    try:
        return _RssPeakSampler(_ram_side_bytes if _books_host_ram() else _rss_bytes)
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("the RSS peak sampler did not start: %s", exc)
        return None


def _mps_peak_mb(state: dict[str, Any]) -> tuple[int | None, int | None]:
    """Stop this batch's sampler and take its `(pool, allocated)` peaks."""
    sampler = state.pop("mps_sampler", None)
    if sampler is None:
        return (None, None)
    try:
        return sampler.stop()
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("the MPS peak sampler did not stop cleanly: %s", exc)
        return (None, None)


def _rss_peak_mb(state: dict[str, Any]) -> int | None:
    """Stop this batch's RSS sampler and take its peak."""
    sampler = state.pop("rss_sampler", None)
    if sampler is None:
        return None
    try:
        return sampler.stop()
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("the RSS peak sampler did not stop cleanly: %s", exc)
        return None


def begin_batch() -> dict[str, Any]:
    """Reset the peak counters and snapshot the pre-batch state."""
    try:
        _reset_peaks()
        reserved, allocated, _, _ = _allocator_stats()
    except Exception:  # pragma: no cover - defensive
        reserved = allocated = None
    # Only the first batch after a release measures a re-grow.
    regrow = bool(_release_state["armed"])
    released_mb = _release_state["released_mb"] if regrow else None
    release_trigger = _release_state["trigger"] if regrow else None
    _release_state["armed"] = False
    return {
        "reserved_before_mb": reserved,
        "allocated_before_mb": allocated,
        "alloc_retries_before": alloc_retries(),
        "regrow": regrow,
        "released_mb": released_mb,
        "release_trigger": release_trigger,
        "started": time.perf_counter(),
        "mps_sampler": _mps_peak_sampler(),
        "rss_sampler": _rss_peak_sampler(),
        "host_ram": _books_host_ram(),
    }


def abandon_batch(state: dict[str, Any]) -> None:
    """Stop this batch's samplers if nothing measured it. Idempotent, so it is
    safe in a `finally` beside `measure_batch` or `finish_batch`.
    """
    _mps_peak_mb(state)
    _rss_peak_mb(state)


def measure_batch(
    state: dict[str, Any],
    items: int,
    units: int | None = None,
    oom: bool = False,
    oom_class: dict[str, Any] | None = None,
    free_mb: int | None = None,
    free_source: str | None = None,
    ram_mb: tuple[int | None, int | None] | None = None,
    clamped: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """One measurement map for the batch bracketed by `state` (never raises).

    Wire shape: docs/inferio-worker-protocol.md "Memory sensing". `units` is the
    batch size in the model's cost dimension; `duration_ms` covers
    `instance.predict(batch)` alone; `free_mb`/`free_source` are the pre-batch
    reading the clamp took.
    """
    sampled_pool, sampled_allocated = _mps_peak_mb(state)
    sampled_rss = _rss_peak_mb(state)
    try:
        reserved_after, _, peak_reserved, peak_allocated = _allocator_stats()
        if sampled_pool is not None:
            peak_reserved = max(peak_reserved or 0, sampled_pool)
        if sampled_allocated is not None:
            peak_allocated = max(peak_allocated or 0, sampled_allocated)
        # The RSS is the allocated figure on a RAM-priced worker only.
        if sampled_rss is not None and not state.get("host_ram"):
            peak_allocated = max(peak_allocated or 0, sampled_rss)
    except Exception as exc:  # pragma: no cover - defensive
        # Keep the rest of the measurement (an OOM, a live reading).
        logger.debug("batch measurement failed: %s", exc)
        reserved_after = peak_reserved = peak_allocated = None
    started = state.get("started")
    duration_ms = (
        round((time.perf_counter() - started) * 1000.0, 3)
        if isinstance(started, float)
        else None
    )
    # After the timed section: the resident readings below exclude what the
    # batch freed.
    return_freed_memory()
    measurement: dict[str, Any] = {
        "items": items,
        "reserved_before_mb": state.get("reserved_before_mb"),
        "reserved_after_mb": reserved_after,
        "peak_reserved_mb": peak_reserved,
        "allocated_before_mb": state.get("allocated_before_mb"),
        "peak_allocated_mb": peak_allocated,
        "duration_ms": duration_ms,
    }
    # Per-batch delta; both ends must be readable.
    retries_before = state.get("alloc_retries_before")
    retries_after = alloc_retries()
    if retries_before is not None and retries_after is not None:
        measurement["alloc_retries"] = max(retries_after - retries_before, 0)
    if state.get("regrow"):
        regrow_mb = _delta(peak_reserved, state.get("reserved_before_mb"))
        if regrow_mb is not None:
            measurement["regrow_mb"] = regrow_mb
            measurement["regrow_after"] = state.get("release_trigger")
            logger.debug(
                "the batch after a %s release that handed back %s MiB re-grew "
                "the pool by %d MiB and took %s ms in all",
                state.get("release_trigger"),
                state.get("released_mb"),
                regrow_mb,
                duration_ms,
            )
    if free_mb is not None:
        measurement["free_mb"] = free_mb
        measurement["free_source"] = free_source
        if ram_mb is not None:
            # The RAM basis of this same reading (MPS).
            measurement["ram_total_mb"], measurement["ram_available_mb"] = ram_mb
    if clamped:
        measurement["clamped"] = clamped
    if state.get("host_ram") and sampled_rss is not None:
        measurement["peak_rss_mb"] = sampled_rss
    # The level the batch left: a GPU worker's host RAM, and a RAM-priced
    # worker's footprint (its `reserved` is a peak that never falls).
    if state.get("host_ram") or _ram_currency():
        rss_after = _mb(_ram_side_bytes() if state.get("host_ram") else _rss_bytes())
        if rss_after is not None:
            measurement["rss_after_mb"] = rss_after
    if units is not None:
        measurement["units"] = units
    if oom:
        measurement["oom"] = True
        if oom_class:
            measurement["oom_class"] = oom_class
    return measurement


def finish_batch(state: dict[str, Any], items: int) -> dict[str, Any]:
    """Predict-response payload for a grantless window: a fresh sample plus one
    measurement covering the whole `instance.predict` call, without `units`.
    """
    try:
        payload: dict[str, Any] = {"measurements": [measure_batch(state, items)]}
        sample = device_memory_sample()
        if sample is not None:
            payload["memory"] = sample
        return payload
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("batch measurement failed: %s", exc)
        return {}
