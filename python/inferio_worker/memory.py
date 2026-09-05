"""Device-memory sensing for the worker.

The worker is the only component that can see the allocator statistics and the
driver's free memory for the GPU it is pinned to, so it senses and the
orchestrator decides. Every helper degrades to `None` rather than raising, and
a caller that gets `None` reports nothing on the wire.
See docs/inferio-worker-protocol.md "Memory sensing (optional response fields)".

Two rules the rest of this module is built on. **Never initialize CUDA**: the
torch calls that read the device also *create* a 300-600 MB context on it when
none exists, so every torch path here requires `is_initialized()` first.
**Never invent a footprint**: a process that never allocated on the device
reports no `base_mb` at all rather than 0. Imports are stdlib only.
"""

from __future__ import annotations

import logging
import os
import re
import sys
import threading
import time
from collections import deque
from collections.abc import Iterable
from types import ModuleType
from typing import Any

logger = logging.getLogger("inferio_worker.memory")

_MIB = 1024 * 1024

# Fixed accelerator-context allowance, used when this process could not
# measure its own ([`_ContextProbe`]).
CONTEXT_ESTIMATE_MB = 500

# Plausibility band for a *measured* context, in MiB; a measurement outside
# it is discarded and the estimate is used. See
# docs/inferio-worker-protocol.md "The accelerator context probe".
CONTEXT_MIN_MB = 64
CONTEXT_MAX_MB = 2048

# Context probe: how often it checks whether CUDA has come up, and its
# deadline (the orchestrator's own `load_secs` default).
_CONTEXT_POLL_SECONDS = 0.005
_CONTEXT_PROBE_MAX_SECONDS = 600

# How often the probe re-reads its pre-initialisation baseline while it waits.
_CONTEXT_BASELINE_SECONDS = 0.25

# Allowance, above the context, for memory this process legitimately holds
# outside the caching allocator (cuDNN/cuBLAS workspaces, NCCL buffers,
# driver bookkeeping). It sets the free-delta ceiling in [`_resolve_base`].
IMPLAUSIBLE_SLACK_MB = 2048

# How far *below* our own allocator pool an fdinfo per-process VRAM reading may
# sit before it is judged an under-report. See docs/inferio-worker-protocol.md
# "The accelerator context probe".
FDINFO_UNDERREPORT_SLACK_MB = 256

# NVML memoization. The *module* half is one-shot (an import or `nvmlInit`
# failure is permanent); the *handle* half deliberately is not — see `_nvml`.
_nvml_state: dict[str, Any] = {"module_tried": False, "module": None, "handle": None}

# One-shot log flags (per worker process).
_logged: dict[str, bool] = {
    "nvml_pid_missing": False,
    "nvml_gpu_unidentified": False,
    "hip_uuid_suppressed": False,
    "fdinfo_identity": False,
    "fdinfo_under_reported": False,
}

# This worker's own GPU address, memoized. Resolution only: no negative
# caching, for the same reason as the NVML handle (see `_nvml`).
_bdf_state: dict[str, Any] = {"bdf": None}

# Where the kernel exposes this process's own DRM clients. Linux-only, as is
# amdgpu.
FDINFO_ROOT = "/proc/self/fdinfo"

# Where amdgpu exposes each GPU's VRAM counters
# (`<root>/<bdf>/mem_info_vram_{total,used}`) — the same files the
# orchestrator's staleness refresh reads, so both sides speak one vocabulary.
PCI_DEVICES_ROOT = "/sys/bus/pci/devices"

# The unit suffixes a DRM usage-stats memory line may carry: exactly the
# documented grammar `<uint> [KiB|MiB]`, absent meaning bytes
# (<https://docs.kernel.org/gpu/drm-usage-stats.html>), and nothing else.
_DRM_UNITS = {
    "": 1,
    "KIB": 1024,
    "MIB": 1024 * 1024,
}

# A PCI address as the kernel writes it: `dddd:bb:dd.f`, lower-case hex.
# Validates the one BDF this module does not build itself (see `device_bdf`).
_BDF_RE = re.compile(r"[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-7]")

# The device the orchestrator priced this worker against. `cpu` means this
# replica's memory currency is host RAM ([`_ram_currency`]).
DEVICE_ENV_VAR = "INFERIO_DEVICE"

# Where Linux publishes this process's peak resident set.
PROC_STATUS = "/proc/self/status"


# --- torch, only if the impl already brought it *and* already used the GPU ---


def _torch() -> Any | None:
    """The already-imported torch module, or None. Touches nothing."""
    return sys.modules.get("torch")


def _torch_cuda() -> Any | None:
    """The already-imported torch module iff its CUDA device is live. The
    `is_initialized` requirement is what keeps this harness from creating the
    context it is measuring; every torch-backed reading goes through here.
    """
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


# --- NVML: per-process footprint (tier 1) and a torch-free memory reading ---


def _nvml() -> tuple[Any, Any] | None:
    """`(pynvml, handle)` for the GPU this worker is pinned to, else None.

    NVML ignores `CUDA_VISIBLE_DEVICES`, so the pin is resolved explicitly, and
    retried on **every** call: the first is pre-load, before anything touched
    torch, and caching that failure would kill tier 1 for the worker's life.
    **Refused outright on a ROCm worker**, since `nvmlInit` succeeds wherever an
    NVIDIA driver is loaded and a hybrid box would then report one GPU's
    identity beside another's free/total.
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
    """The initialized `pynvml` module, or None. One-shot: an import or
    `nvmlInit` failure is permanent, so it is paid and logged exactly once.
    """
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
    # `CUDA_VISIBLE_DEVICES` only: a ROCm pin is a HIP device index, never a
    # UUID, and `_nvml` refuses a ROCm worker outright before reaching here.
    pin = (os.environ.get("CUDA_VISIBLE_DEVICES") or "").strip()
    if pin.upper().startswith(("GPU-", "MIG-")):
        handle = _nvml_handle_by_uuid(pynvml, pin)
        if handle is not None:
            return handle
    # No usable UUID pin: ask torch for the visible device's UUID, but only
    # once the impl has initialized CUDA; before that this falls through.
    uuid, _ = device_identity()
    if uuid is not None:
        handle = _nvml_handle_by_uuid(pynvml, uuid)
        if handle is not None:
            return handle
    # Last resort: unambiguous only on a single-GPU host. An index pin is
    # deliberately NOT mapped to an NVML index — the two orderings differ
    # under CUDA_DEVICE_ORDER, and a wrong GPU is worse than no reading.
    try:
        if pynvml.nvmlDeviceGetCount() == 1:
            return pynvml.nvmlDeviceGetHandleByIndex(0)
    except Exception:
        pass
    # One-shot: `_nvml` retries every call, so on a multi-GPU host with an
    # index pin this would otherwise repeat for every batch.
    if not _logged["nvml_gpu_unidentified"]:
        _logged["nvml_gpu_unidentified"] = True
        logger.debug("cannot identify this worker's GPU in NVML; skipping NVML paths")
    return None


def _nvml_handle_by_uuid(pynvml: Any, uuid: str) -> Any | None:
    """Handle for `uuid`, tolerating the abbreviations CUDA accepts: a failed
    exact lookup enumerates and prefix-matches, since CUDA resolves prefixes
    itself. An ambiguous prefix resolves to *nothing*.
    """
    exact = uuid.strip()
    try:
        return pynvml.nvmlDeviceGetHandleByUUID(exact.encode())
    except Exception:
        pass
    # The prefix compare is case-folded; the exact lookup above is not, since
    # NVML matches the string it printed.
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
    """This process's device footprint per NVML, or None. `usedGpuMemory` is
    N/A under Windows' WDDM, and NVML reports *host* pids, so in a PID namespace
    our pid is never listed — a silent degradation, hence the one-shot line. A
    reading of at least the GPU's capacity is sentinel garbage.
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
    """Whether this torch is a ROCm build (`torch.version.hip` is set).

    The one positive signal: `torch.cuda.*` is hipified and reveals nothing.
    """
    try:
        return bool(getattr(getattr(torch, "version", None), "hip", None))
    except Exception:
        return False


def _hip_pinned() -> bool:
    """Whether the orchestrator pinned this worker to a HIP device: the
    pre-torch-import half of [`_is_hip`], since our spawner writes
    `HIP_VISIBLE_DEVICES` on every pinned ROCm worker and no other kind.
    """
    value = os.environ.get("HIP_VISIBLE_DEVICES") or ""
    return any(entry.strip() for entry in value.split(","))


def _unified_gpu() -> bool:
    """Whether this worker is **on** a unified-memory device (DP-5). The spawner
    names it (`PANOPTIKON_UNIFIED_GPU=<pci address>`), and the address is
    **checked against [`_identity_bdf`], not trusted**: believing it wrongly
    either reports GTT as free VRAM under an authoritative label or prices a
    64 GB device at its 512 MB carve-out.
    """
    claimed = (os.environ.get("PANOPTIKON_UNIFIED_GPU") or "").strip().lower()
    if not _BDF_RE.fullmatch(claimed):
        return False
    return claimed == _identity_bdf()


def _memory_regions() -> tuple[str, ...]:
    """The DRM/amdgpu memory regions this worker's own usage is summed over:
    VRAM **plus GTT** on a unified-memory device, since an APU's allocations
    land in GTT once the carve-out fills and a VRAM-only figure under-measures.
    """
    return ("vram", "gtt") if _unified_gpu() else ("vram",)


def pinned_device_missing() -> str | None:
    """An actionable message when this worker was pinned to a device its own
    runtime does not enumerate, or `None`. Without it the impl quietly runs the
    model on the **CPU** while being priced against a GPU
    (docs/rocm-batch-calibration-parity.md, unsupported gfx target). It reads
    our own `PANOPTIKON_DEVICE_PIN` marker, not the visibility variable.
    """
    pin = (os.environ.get("PANOPTIKON_DEVICE_PIN") or "").strip()
    if not pin:
        return None
    torch = _torch()
    if torch is None:
        return None
    # A **CPU-only torch build** never enumerated a device to lose, so its empty
    # device list must not fail a load; both version fields are `None` there.
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
        "made usable) or a device index that does not exist in this "
        "process's visible set. Pin this model to a GPU that works "
        "(inference_local `devices`), or make the pinned one enumerable"
    )


def _device_props() -> Any | None:
    """`get_device_properties(0)` for the pinned device, or None. Gated on
    [`_torch_cuda`]: the call *creates* a context on a process that has none.
    """
    torch = _torch_cuda()
    if torch is None:
        return None
    try:
        return torch.cuda.get_device_properties(0)
    except Exception:
        return None


def _prop(props: Any, field: str) -> Any | None:
    """One field of a device-properties struct, or None if it cannot be read.
    **Every** read of that struct goes through here: the fields are pybind
    getters that can be missing or raise, and an escaping exception would take
    down the whole load report.
    """
    try:
        return getattr(props, field, None)
    except Exception:
        return None


def device_identity() -> tuple[str | None, str | None]:
    """`(uuid, name)` of the GPU this worker's CUDA device 0 resolved to, in
    NVML form so the ledger keys on what the worker actually got. **Suppressed
    entirely on a ROCm build**, where the rendered UUID repeats across cards of
    a model; those replicas key on [`device_bdf`].
    """
    # A CPU-priced host is answered **before** torch is consulted: a live CUDA
    # context must not put a GPU UUID on a report whose figures are all RAM.
    if _ram_currency():
        return (None, ram_gpu_name())
    props = _device_props()
    if props is None:
    # MPS has no UUID and no GPU struct — one device per host, keyed by a
    # constant. The name comes from the same sysctls the orchestrator uses.
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


def device_label() -> str:
    """Which device the memory figures reported beside this label describe, for
    `oom_class.device`: a free reading taken at a failure is only interpretable
    against the GPU it was taken on. ROCm is tested before CUDA because
    `torch.cuda` is hipified.
    """
    try:
        if _ram_currency():
            return "cpu"
        torch = _torch()
        if (torch is not None and _is_hip(torch)) or _hip_pinned():
            kind = "rocm"
        elif _torch_cuda() is not None:
            kind = "cuda"
        elif _torch_mps() is not None:
            kind = "mps"
        else:
            kind = "unknown"
        uuid, _ = device_identity()
        return f"{kind}:{uuid}" if uuid else kind
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("device label unavailable: %s", exc)
        return "unknown"


def device_bdf() -> str | None:
    """This worker's GPU as a PCI address (`dddd:bb:dd.0`), or None: the one
    identity vocabulary the kernel, amdgpu and HIP all speak, and so the ROCm
    ledger join. The **device-0-scoped** PCI fields of
    `get_device_properties(0)` describe exactly the GPU the pin selected; only a
    ROCm build too old for them falls back to the dominant-VRAM DRM client.
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
        # A CUDA host without the PCI fields has no BDF to report; its
        # identity is the UUID.
        return None
    bdf = dominant_vram_pdev()
    # Shape-checked before it is believed, unlike the address this module
    # formats itself: this one is lifted verbatim out of a `drm-pdev` line.
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
    """The BDF from torch's PCI fields, or None when they are absent. Read
    through [`_prop`]; a partial triple is not an address.
    """
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
    """Total VRAM of this worker's GPU in MiB, per torch, or None: an
    **independent** second source for a number the orchestrator also reads from
    the driver, so a BDF-matched registration can be cross-checked. On MPS it is
    `recommended_max_memory()` and authoritative instead.
    """
    # First, and before torch is asked anything, for the reason
    # [`device_identity`] documents.
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
) -> tuple[str, int, int] | None:
    """`(pdev, client_id, bytes)` for one fdinfo file, or None.

    `drm-pdev` and `drm-client-id` are both required: the address is what the
    reading is *about*, and the client id makes two fds of one client countable
    once. Both memory spellings are accepted, `drm-memory-<region>` being the
    docs' amdgpu-only deprecated alias, and `drm-resident-*` wins for the whole
    sum. **Absent and unreadable are different answers**: a client with neither
    key has no VRAM, while an unparseable one makes the record `None` rather
    than hand dominance to another GPU.
    """
    fields: dict[str, str] = {}
    for line in text.splitlines():
        key, separator, value = line.partition(":")
        if not separator:
            continue
        key = key.strip().lower()
        if key.startswith("drm-"):
            fields[key] = value.strip()
    pdev = fields.get("drm-pdev")
    client_id = fields.get("drm-client-id")
    if not pdev or client_id is None:
        return None
    try:
        client = int(client_id)
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
    """`<uint> [KiB|MiB]` in bytes, or None when it is not that: "not a
    reading", which the caller turns into a discarded record, not a zero.
    """
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
    """Per-GPU VRAM this process holds, keyed by PCI address, in bytes. A *map*
    because the per-process tier filters it by the identity BDF while the
    identity fallback takes its argmax; deduplicated by DRM client id.
    """
    seen: set[tuple[str, int]] = set()
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
    """The PCI address this process holds the most VRAM on, or None: the
    identity fallback of [`device_bdf`]. A *strict* maximum is required, so a
    tie answers None rather than price one model against another GPU's ledger.
    """
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
    """The contents of every readable fdinfo file. Best-effort throughout:
    fds come and go while the directory is being walked, so an entry that
    vanishes mid-scan is skipped rather than fatal."""
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
    """[`device_bdf`], resolved once and then remembered, or None. Both amdgpu
    tiers are about this worker's GPU, so they read nothing until the identity
    is known; retried every call, as the NVML handle is.
    """
    cached = _bdf_state["bdf"]
    if cached is not None:
        return cached
    bdf = device_bdf()
    if bdf is not None:
        _bdf_state["bdf"] = bdf
    return bdf


def fdinfo_own_vram_mb(root: str | None = None) -> int | None:
    """This process's VRAM on **its own** GPU, in MiB, or None: the ROCm twin of
    NVML's own-pid figure, read without root, amdsmi or a PID-namespace caveat.
    Filtered by the identity address rather than summed, since HIP filters
    *above* ROCr and the other GPUs' clients are not ours to charge.
    """
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
    """[`fdinfo_own_vram_mb`] once it has passed the plausibility floor.

    fdinfo's compute memory stats need a recent kernel, and an under-measured
    base is phantom headroom, so the reading is floored against the allocator
    pool we measured ourselves, which it is expected to exceed. The comparand is
    the **absolute** post-load `reserved_mb`; a windowed one would wave an
    under-report through on every reload. **HIP-gated**.
    """
    if not _is_hip(_torch()):
        return None
    own = fdinfo_own_vram_mb(root)
    if own is None:
        return None
    # On a **unified** GPU the same rule takes a different comparand: HIP may
    # report an APU's `total_memory` as the carve-out alone while the reading
    # includes GTT. Read without the free half, so the bound cannot inherit
    # psutil and vanish silently.
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
    """`(free_mb, total_mb)` for this worker's GPU from amdgpu sysfs.

    `mem_info_vram_total - mem_info_vram_used`: **device-wide**, and the same
    files the orchestrator's refresh reads, so both sides speak one memory
    vocabulary by construction. `(None, None)` on any host that is not an amdgpu
    Linux box, so the tier needs no platform test. **On a verified
    unified-memory device** the GTT neighbours join in, clamped by
    `ram_available`, and every term is required.
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
    """This worker's GPU **capacity** from amdgpu sysfs, or None:
    [`amdgpu_free_total_mb`]'s total without the free half, so capacity stays
    knowable on a machine without psutil, which that formula needs.
    """
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
    """`<base>/<bdf>`, with the colons swapped for dashes on Windows: a fixture
    affordance, and the twin of `rocm.rs::pci_device_dir`. In production on
    Linux this is always the plain join.
    """
    return os.path.join(base, bdf.replace(":", "-") if os.name == "nt" else bdf)


def _sysfs_bytes(path: str) -> int | None:
    """A sysfs file holding one non-negative decimal integer, or None. A missing
    file, a permission error, a changed format and a negative number are all
    "no reading", which every caller treats as unknown.
    """
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
    """The already-imported torch module iff **MPS** is this worker's device;
    `_torch_cuda` is checked first and wins when both answer. No
    `is_initialized()` analogue is needed — MPS has no context to create.
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
    torch.mps has **no peak or reset APIs**, so the peaks reported per batch are
    these figures read afterwards; the pool is monotone absent an
    `empty_cache()`.
    """
    return (
        _mb(_mps_call("driver_allocated_memory")),
        _mb(_mps_call("current_allocated_memory")),
    )


def mps_free_total_mb() -> tuple[int | None, int | None]:
    """`(free_mb, total_mb)` for a unified-memory device, or `(None, None)`.
    `total` is `recommended_max_memory()`, the figure allocations are judged
    against and the one the orchestrator adopts; it moves with the GPU wired
    limit. `free` is `max(0, min(total, ram_available))`, whose RAM term is what
    makes external pressure visible here at all.
    """
    total = _mps_call("recommended_max_memory")
    if not total:
        return (None, None)
    available = _ram_available_bytes()
    if available is None:
        return (None, None)
    return (_mb(min(total, available)), _mb(total))


def _virtual_memory() -> Any | None:
    """psutil's whole-machine memory statistics, or None. Imported lazily,
    like everything else here (stdlib only at import time).
    """
    try:
        import psutil
    except Exception:
        return None
    try:
        return psutil.virtual_memory()
    except Exception:
        return None


def _ram_available_bytes() -> int | None:
    """RAM the OS says it could deliver right now, per psutil, or None.
    `virtual_memory().available` is the same question the orchestrator asks.
    """
    memory = _virtual_memory()
    if memory is None:
        return None
    try:
        return int(memory.available)
    except Exception:
        return None


def mps_gpu_name() -> str | None:
    """`Apple M3 Max (128 GB)` — this Mac's chip and capacity, or None.
    **Diagnostic only**, but byte-identical to `mps.rs::gpu_name` for the same
    host — a free cross-check on two derivations that could drift apart.
    """
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


# --- CPU-only hosts: host RAM as the memory currency ---


def _ram_currency() -> bool:
    """Whether this worker's memory is the machine's RAM (backend C): exactly
    when the orchestrator **said so** (`INFERIO_DEVICE=cpu`), never inferred
    from the absence of accelerator facts, which a remote-API worker on a CUDA
    host matches perfectly.
    """
    return (os.environ.get(DEVICE_ENV_VAR) or "").strip().lower() == "cpu"


def ram_free_total_mb() -> tuple[int | None, int | None]:
    """`(free_mb, total_mb)` for a CPU-priced host, or `(None, None)`: the
    degenerate unified-memory device (docs/unified-memory-admission.md, backend
    C), where the free formula collapses to `ram_available`. Both figures come
    from `psutil.virtual_memory()`, the sources `cpu.rs` reads.
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
    return (_mb(min(total, available)), _mb(total))


def ram_gpu_name() -> str | None:
    """`CPU (64 GB)` — this machine's capacity, or None. **Diagnostic only**,
    like [`mps_gpu_name`], and byte-identical to `cpu.rs::gpu_name`.
    """
    memory = _virtual_memory()
    if memory is None:
        return None
    try:
        total_mb = int(memory.total) // _MIB
    except Exception:
        return None
    if total_mb <= 0:
        return None
    # Up to the next multiple of 4 GiB, never below it: what an OS calls
    # "total RAM" moves with a kernel update or a BIOS setting.
    grid = 4 * 1024
    return f"CPU ({max(-(-total_mb // grid) * 4, 4)} GB)"


def _rss_bytes() -> int | None:
    """This process's resident set right now, or None: the CPU analogue of
    `memory_allocated`, and *not* monotone, which is why the peak below is a
    separate reading rather than a max of this one.
    """
    try:
        import psutil
    except Exception:
        return None
    try:
        return int(psutil.Process().memory_info().rss)
    except Exception:
        return None


def parse_vm_high_water(text: str) -> int | None:
    """`VmHWM` out of `/proc/self/status`, in **bytes**, or None. Linux renders
    it as `VmHWM:\t   12345 kB` — kibibytes despite the spelling, and a row
    without that unit is not one this understands.
    """
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
    """`ru_maxrss` in bytes, or None. **The unit is platform-specific and
    getting it wrong is a factor of 1024**: macOS reports bytes, every other
    Unix kibibytes.
    """
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
    """The OS's high-water mark for this process's resident set, or None. A
    **real** peak but not resettable on any platform, so it is the process's
    lifetime high-water and is reported as the pool ("reserved"), which is
    exactly the CUDA allocator pool's own shape."""
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


def ram_pool_mb() -> tuple[int | None, int | None]:
    """`(pool_mb, resident_mb)` for a CPU-priced host, or `(None, None)`: the
    `mps_pool_mb` shape for backend C, the OS high-water standing in for the
    allocator pool (see [`_peak_rss_bytes`]) and the live RSS for `allocated`.
    """
    return (_mb(_peak_rss_bytes()), _mb(_rss_bytes()))


def torch_version() -> str | None:
    """`torch.__version__`, or None when the impl never imported torch. Part of
    the calibration profile key and knowable only here.
    """
    torch = _torch()
    version = getattr(torch, "__version__", None) if torch is not None else None
    return str(version) if version is not None else None


# --- Samples ---


def device_memory_sample() -> dict[str, Any] | None:
    """A memory sample for this worker's GPU, or None if nothing is known. Wire
    shape: docs/inferio-worker-protocol.md "Memory sensing". `free_mb` and
    `total_mb` come from the single-source helper the base measurement uses;
    the sources otherwise disagree by 3.4 GB in one field."""
    reserved_mb, allocated_mb, _, _ = _allocator_stats()
    free_mb, total_mb, free_source = _free_total_mb()
    sample: dict[str, Any] = {
        "free_mb": free_mb,
        "total_mb": total_mb,
        "free_source": free_source,
        "reserved_mb": reserved_mb,
        "allocated_mb": allocated_mb,
    }
    if all(value is None for value in sample.values()):
        return None
    return sample


def pool_stats_mb() -> tuple[int | None, int | None]:
    """`(reserved_mb, allocated_mb)` for our allocator, or `(None, None)`, and
    without the driver query [`device_memory_sample`] pays for. Both numbers,
    because an `empty_cache()` can only hand back `reserved - allocated`.
    """
    reserved, allocated, _, _ = _allocator_stats()
    return (reserved, allocated)


def empty_cache() -> bool:
    """Release the caching allocator's unused pool. Returns whether it ran.
    Freeing tensors gives nothing back to the driver, so this is the only way
    our process returns VRAM short of exiting. Gated on a live CUDA context, so
    False means "nothing of ours is on the device". **On a CPU-priced host it is
    a no-op returning False** (docs/unified-memory-admission.md, "Trim").
    """
    if _ram_currency():
        return False
    torch = _torch_cuda()
    if torch is None:
        return _mps_empty_cache()
    try:
        torch.cuda.empty_cache()
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("empty_cache failed: %s", exc)
        return False
    return True


def _mps_empty_cache() -> bool:
    """The MPS arm of [`empty_cache`] — `torch.mps.empty_cache()`, and the one
    place the MPS pool can ever be released.
    """
    torch = _torch_mps()
    if torch is None:
        return False
    try:
        release = getattr(torch.mps, "empty_cache", None)
        if release is None:
            return False
        release()
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("mps empty_cache failed: %s", exc)
        return False
    return True


def _reset_peaks() -> None:
    torch = _torch_cuda()
    if torch is None:
        return
    try:
        torch.cuda.reset_peak_memory_stats()
    except Exception:
        pass


def _allocator_stats() -> tuple[int | None, int | None, int | None, int | None]:
    """`(reserved, allocated, peak_reserved, peak_allocated)` in MiB. On MPS the
    two "peaks" are the live figures; on a CPU-priced host the "pool" is the OS
    high-water and "allocated" the live RSS, which keeps `peak > before` meaning
    "this batch grew the envelope" ([`mps_pool_mb`], [`ram_pool_mb`])."""
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


def _free_total_mb(
    source: str | None = None,
) -> tuple[int | None, int | None, str | None]:
    """`(free_mb, total_mb, source)`: `ram`|`nvml`|`amdgpu-sysfs`|`mps`|`torch`.

    The one place free/total memory is read, so every consumer sees the same
    currency. The chain is tried in that order on every host with no platform
    test here, because each tier's own availability is one; `mem_get_info` is
    last on both backends — on HIP its "free" was historically process-local
    (ROCm/hip#348) — which is why the ledger treats `"torch"` as
    non-authoritative. The sources do not agree, so a delta is only meaningful
    between two readings of the *same* source.
    """
    if source in (None, "ram") and _ram_currency():
        # Byte-identical to the orchestrator's label for the same reading
        # (`gpu.rs::free_source`).
        free, total = ram_free_total_mb()
        if free is not None:
            return (free, total, "ram")
        return (None, None, None)
    if source in (None, "nvml"):
        free, total = _nvml_memory()
        if free is not None:
            return (free, total, "nvml")
    if source in (None, "amdgpu-sysfs"):
        # Byte-identical to the Rust `MemoryQuery`'s label for the same files,
        # and it names the *driver*, not the filesystem, so no later
        # sysfs-derived reporter inherits its authority by string collision.
        free, total = amdgpu_free_total_mb()
        if free is not None:
            return (free, total, "amdgpu-sysfs")
    if source in (None, "mps"):
        # Byte-identical to the orchestrator's label for the same reading;
        # availability is the platform test again, since `torch.backends.mps` is
        # unavailable everywhere else.
        free, total = mps_free_total_mb()
        if free is not None:
            return (free, total, "mps")
    if source in (None, "torch"):
        torch = _torch_cuda()
        if torch is not None:
            try:
                free, total = torch.cuda.mem_get_info()
                free_mb, total_mb = _mb(free), _mb(total)
            except Exception:
                free_mb = total_mb = None
            if free_mb is not None:
                return (free_mb, total_mb, "torch")
    return (None, None, None)


def _free_mb(source: str | None = None) -> tuple[int | None, str | None]:
    """`(free_mb, source)` — [`_free_total_mb`] for callers wanting only free."""
    free_mb, _, resolved = _free_total_mb(source)
    return (free_mb, resolved)


def free_total_mb() -> tuple[int | None, int | None, str | None]:
    """Public `(free_mb, total_mb, source)` reading for this worker's GPU. The
    packing harness's clamp needs a *live* reading before every GPU batch, from
    the same source everything else uses or the comparison is meaningless.
    """
    return _free_total_mb()


# --- Accelerator context: measured once per process, never assumed twice ---

# The context this process measured for itself, in MiB, or None. A dict so
# tests can isolate it the way they isolate `_nvml_state`.
_context_state: dict[str, Any] = {
    "measured_mb": None,
    "logged": False,
    # The probe this process has running, so it can also be collected from
    # the load-failure path ([`abort_load`]).
    "probe": None,
}


class _ContextProbe:
    """Measures the accelerator context: the GPU free-memory delta across this
    process's **first CUDA initialisation**. A watcher rather than a call,
    because this module may not create a context itself: a daemon thread polls
    `is_initialized()` and reads the moment it flips. See
    docs/inferio-worker-protocol.md "The accelerator context probe".
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
        """One observation. True once the probe has its answer, or has given
        up; False while CUDA is still not live."""
        if self._done:
            return True
        torch = self._torch()
        if torch is None:
            return False
        try:
            live = torch.cuda.is_initialized()
        except Exception:
            # A torch that cannot answer will not answer later either.
            self._done = True
            return True
        if not live:
            self._refresh_baseline(torch)
            return False
        try:
            # The pool first, the free memory second: an allocation landing
            # between the two reads must over-state the context, never under.
            self._reserved_at_init = self._read_reserved() or 0
            self._free_at_init = self._read_free()
        except Exception:  # pragma: no cover - defensive
            self._free_at_init = None
        self._done = True
        return True

    def _refresh_baseline(self, torch: Any) -> None:
        """Re-read the pre-initialisation baseline, at most once per
        [`_CONTEXT_BASELINE_SECONDS`], so an external process moving memory
        during a long load cannot land in the delta. A reading that raced the
        flip already contains the context and is discarded.
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
        """Stop watching and return the measured context in MiB, or None: when
        CUDA never came up, a reading was missing, or the delta fell outside
        [`CONTEXT_MIN_MB`]..[`CONTEXT_MAX_MB`].
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
    """The allocator pool size, for the probe thread. Separate from
    [`_allocator_stats`] because the probe needs only this one number and
    reads it at an instant when the peak counters are meaningless."""
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
    """Start the context probe if this load could possibly answer, else None.
    Every gate states that a measurement is *impossible*: no driver-level
    pre-load reading, a RAM-priced process, CUDA already initialised, or a
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
    """Stop this process's context probe, if one is running, and keep whatever
    it measured. Both the probe handed through the `begin_load` state and the
    one this module recorded are collected, so no route leaves a thread polling.
    """
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
    """Release what `begin_load` started, for a load that **raised**: without it
    a retried load accumulates one polling thread per attempt. Whatever the
    probe measured is *kept*, and this never raises.
    """
    try:
        probe = before.get("context_probe") if isinstance(before, dict) else None
        _collect_context_probe(probe, announce=False)
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("post-failure memory cleanup failed: %s", exc)


def context_allowance_mb() -> tuple[int, str]:
    """`(mb, "measured"|"estimate")`: the device memory this process holds
    outside the caching allocator — the term `base` needs when the allocator's
    own delta is all it has, and the term the free-delta ceiling is built from.
    """
    measured = _context_state["measured_mb"]
    if measured is not None:
        return (int(measured), "measured")
    return (CONTEXT_ESTIMATE_MB, "estimate")


def _remember_context_mb(measured: int | None) -> None:
    """Cache a context measurement for the life of the process, and say once
    which figure everything downstream is using."""
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
    free reading is taken **before any torch.cuda call**, so nothing here can
    initialize CUDA and the context the load is about to create falls inside the
    measured window; the peak reset is skipped while CUDA is not live."""
    try:
        free_mb, free_source = _free_mb()
        # Started here, from the same reading, and *before* the peak reset:
        # the probe's baseline must be the last free reading taken while this
        # process still had no CUDA context.
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
    """Load-response payload: base, provenance, pool size, dtype, sample. Keys
    whose value could not be measured are omitted entirely, so a worker with no
    torch and no NVML replies with a plain `ok`.
    """
    try:
        return _finish_load(before, instance)
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("base measurement failed: %s", exc)
        return {}


def _finish_load(before: dict[str, Any], instance: Any) -> dict[str, Any]:
    # Collect the context probe first: it holds a live thread, and everything
    # below may consult the figure it produced.
    _collect_context_probe(before.get("context_probe"))
    reserved, allocated, _, peak_allocated = _allocator_stats()
    free_after, _ = _free_mb(before.get("free_source"))

    allocated_delta = _delta(allocated, before.get("allocated_mb"))
    reserved_delta = _delta(reserved, before.get("reserved_mb"))
    # Tier 3 is always computed: the allocator's own peak delta is the floor
    # under whatever the coarser tiers report.
    alloc_floor = _delta(peak_allocated, before.get("allocated_mb"))
    if alloc_floor is None:
        alloc_floor = allocated_delta
    # Evidence that *this* process put something on the device, not that torch
    # is importable: a worker that never allocates has no footprint of its own.
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
    dtype, dtype_method = resolved_dtype(instance)
    # The sentinel is reported only for a process that has a footprint to key;
    # without one nothing can be persisted. A known dtype goes either way.
    if dtype != DTYPE_UNSTATED or "base_mb" in payload:
        payload["dtype"] = dtype
        payload["dtype_method"] = dtype_method
    uuid, name = device_identity()
    if uuid is not None:
        payload["gpu_uuid"] = uuid
    if name is not None:
        payload["gpu_name"] = name
    # The ROCm ledger join and its cross-check, through the memoizing accessor:
    # one value serves both the wire field and the amdgpu tiers' filter, or a
    # GPU resolving mid-load would be attributed to one GPU and measured on
    # another.
    bdf = _identity_bdf()
    if bdf is not None:
        payload["gpu_bdf"] = bdf
    total_mb = gpu_total_mb()
    if total_mb is not None:
        payload["gpu_total_mb"] = total_mb
    version = torch_version()
    if version is not None:
        payload["torch_version"] = version
    sample = device_memory_sample()
    if sample is not None:
        payload["memory"] = sample
    # Peaks recorded during load must not leak into the first batch's
    # measurement.
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
    """`(base_mb, base_method)` per the tiers in the design, or `(None, None)`.

    1. A process that did not demonstrably allocate on the device reports
       nothing at all. This excludes engines whose VRAM never passes through
       torch's allocator: a footprint that appears on Linux and vanishes on
       Windows is worse than one consistently in the external-usage term.
    2. NVML's own-pid figure wins outright — absolute and pollution-free. Its
       ROCm twin `"fdinfo"` and MPS's `driver_allocated_memory()` rank with it.
    3. Otherwise the driver's free-memory delta, if present, positive and not
       implausibly larger than what we could hold outside the allocator. That
       test is against **reserved**: the caching allocator legitimately
       overshoots live tensors during `from_pretrained`.
    4. A usable free delta below the allocator floor loses to the floor.
    """
    if not touched_gpu:
        return (None, None)
    # Backend C's tier, and the only one that can apply on a CPU-priced host:
    # the growth of this process's resident set across the load window.
    # `alloc_floor` already *is* that number ([`_allocator_stats`] returns the
    # live RSS in both slots). A **window** delta, not growth since process
    # start, which would charge a second load the first model's residency.
    if _ram_currency():
        return (alloc_floor, "rss") if alloc_floor else (None, None)
    own = _nvml_own_process_mb(holding_mb=reserved_mb)
    if own is not None and own > 0:
        return (own, "nvml")
    own = _fdinfo_base_mb(reserved_mb, reserved_delta)
    if own is not None and own > 0:
        return (own, "fdinfo")
    # MPS's own tier-1: `driver_allocated_memory()` after the load is this
    # process's whole Metal footprint **by construction** — each process owns
    # its heap — so it ranks with NVML's own-pid figure, without a pid lookup.
    own = _mb(_mps_call("driver_allocated_memory"))
    if own is not None and own > 0:
        return (own, "mps")

    floor = alloc_floor or 0
    context_mb, context_source = context_allowance_mb()
    # Two formulas, not one with a footnote: a stored profile has to say
    # whether the context in its base was measured on that machine or assumed.
    alloc_method = (
        "alloc_delta_measured" if context_source == "measured" else "alloc_delta"
    )
    free_delta = _free_delta(before.get("free_mb"), free_after)
    # The ceiling uses the same allowance. Not circular: the context was
    # measured across the initialisation window alone.
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
    """Growth of a monotonic-ish allocator counter, clamped at 0. A `None`
    "before" is treated as 0, which is right for a context that did not exist.
    """
    if after is None:
        return None
    return max(after - (before or 0), 0)


def _free_delta(before_mb: int | None, after_mb: int | None) -> int | None:
    """How much free memory the driver lost across the load window. Both
    readings are required; returned unclamped, since a non-positive delta is a
    signal the caller acts on.
    """
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


# What the worker reports when nothing states a precision. A **value**, not an
# omission: `dtype` is part of the calibration profile key, and an absent
# component makes the entry unkeyable. See the protocol doc's `dtype` field.
DTYPE_UNSTATED = "unstated"

# How the reported dtype was arrived at, reported beside it as `dtype_method`:
# the impl stated it, it was read off a `torch.dtype` attribute, it was read off
# the loaded weights, or nothing could answer.
DTYPE_METHOD_SELECTED = "selected"
DTYPE_METHOD_ATTRIBUTE = "attribute"
DTYPE_METHOD_INFERRED = "inferred"
# Renamed with the dtype sentinel above, and for the same reason.
DTYPE_METHOD_UNSTATED = "unstated"

# Bounds on the hunt for a `torch.nn.Module` inside the impl instance. Depth 2
# reaches `self.model` and `self.model.<part>`; the budget is the backstop.
_WALK_DEPTH = 2
_WALK_BUDGET = 256

# How many elements of one *container* the walk unpacks: an impl attribute can
# be a list of ten thousand tag strings as easily as a pair of towers.
_WALK_FANOUT = 16

# Attribute names looked at first at each level of that walk: the walk reports
# the first module it finds, so the conventional names for "the model" go first.
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
    """The precision the impl *stated*, or None if it stated none. Three sources
    in order of authority: `instance.resolved_dtype`; the last decision
    `inferio.impl.utils.select_dtype` recorded, read through `sys.modules` so
    this module never imports `inferio`; and an instance `dtype`/`_dtype`
    attribute, **only** when it holds a real `torch.dtype`.
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
    """The objects one level inside `value`, for the module hunt. Instance
    attributes come from `__dict__` and never from `dir()` + `getattr`: an
    impl's properties can load, download or move a model, and a measurement
    harness must not trigger that."""
    if isinstance(value, (str, bytes, bytearray)):
        return []
    # An imported *Python* module is never a `torch.nn.Module`, and its
    # `__dict__` holds thousands of names; the budget bounds the work, not the
    # queue.
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
    """The first floating-point parameter's (else buffer's) dtype name.
    [`_DTYPE_NAMES`] maps the three float dtypes and nothing else, so an int8
    weight is skipped by construction. Parameters before buffers.
    """
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
    """The dtype of the weights actually loaded, read off the model itself: a
    breadth-first walk finds the `torch.nn.Module` the instance holds, directly
    or one level in, and the first float dtype among its parameters is the
    precision. Bounded on every axis — it runs on every load.
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
            # A module whose own parameters said nothing has submodules, and
            # `parameters()` already recursed through every one of them.
            continue
        if depth >= _WALK_DEPTH:
            continue
        for child in _walk_children(obj):
            queue.append((child, depth + 1))
    return None


def resolved_dtype(instance: Any) -> tuple[str, str]:
    """`(dtype, dtype_method)` for the load response — never absent. The store
    key is `(… torch, dtype)`, so a load reporting no dtype writes no profile,
    ever, silently: the stated sources answer for the four impls that call
    `select_dtype`, the weights for other torch models, the sentinel otherwise.
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


def begin_batch() -> dict[str, Any]:
    """Reset the peak counters and snapshot the pre-batch state."""
    try:
        _reset_peaks()
        reserved, allocated, _, _ = _allocator_stats()
    except Exception:  # pragma: no cover - defensive
        reserved = allocated = None
    return {
        "reserved_before_mb": reserved,
        "allocated_before_mb": allocated,
        "started": time.perf_counter(),
    }


def measure_batch(
    state: dict[str, Any],
    items: int,
    units: int | None = None,
    oom: bool = False,
    oom_class: dict[str, Any] | None = None,
    free_mb: int | None = None,
    free_source: str | None = None,
    clamped: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """One measurement map for the batch bracketed by `state` (never raises).

    Wire shape: docs/inferio-worker-protocol.md "Memory sensing". `units` is the
    batch priced in the model's declared cost dimension, which only the packing
    harness can know. `duration_ms` covers `instance.predict(batch)` alone, so
    unit pricing stays outside it and the throughput-collapse comparator sees
    GPU throughput, not decode noise. `free_mb`/`free_source` are the pre-batch
    reading the clamp already took.
    """
    try:
        _, _, peak_reserved, peak_allocated = _allocator_stats()
    except Exception as exc:  # pragma: no cover - defensive
        # The peaks are the only reading here that can fail; everything else was
        # decided by the caller, and dropping it would discard an OOM or a live
        # reading.
        logger.debug("batch measurement failed: %s", exc)
        peak_reserved = peak_allocated = None
    started = state.get("started")
    duration_ms = (
        round((time.perf_counter() - started) * 1000.0, 3)
        if isinstance(started, float)
        else None
    )
    measurement: dict[str, Any] = {
        "items": items,
        "reserved_before_mb": state.get("reserved_before_mb"),
        "peak_reserved_mb": peak_reserved,
        "allocated_before_mb": state.get("allocated_before_mb"),
        "peak_allocated_mb": peak_allocated,
        "duration_ms": duration_ms,
    }
    if free_mb is not None:
        measurement["free_mb"] = free_mb
        measurement["free_source"] = free_source
    if clamped:
        measurement["clamped"] = clamped
    if units is not None:
        measurement["units"] = units
    if oom:
        measurement["oom"] = True
        if oom_class:
            measurement["oom_class"] = oom_class
    return measurement


def finish_batch(state: dict[str, Any], items: int) -> dict[str, Any]:
    """Predict-response payload for a **grantless** window: a fresh sample plus
    one measurement covering the whole `instance.predict` call. On that
    compatibility path the window is the GPU batch, so there is one measurement
    and no `units`.
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
