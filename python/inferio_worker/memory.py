"""Device-memory sensing for the worker (batch-calibration step 1a).

The worker is the only component that can see the allocator statistics and
the driver's free memory for the GPU it is pinned to, so it senses and the
orchestrator decides (docs/batch-calibration-design.md, "Where each piece
runs"). Everything here is best-effort: every helper degrades to `None`
rather than raising, and a caller that gets `None` reports nothing on the
wire (docs/inferio-worker-protocol.md, "Memory sensing").

Two hard rules, both of them things this module can get wrong in ways that
are invisible until the ledger is built on top:

- **Never initialize CUDA.** torch's `reset_peak_memory_stats`,
  `mem_get_info` and `get_device_properties` all *create* a CUDA context
  when none exists — 300–600 MB on the very device we are trying to
  measure, in a process (CPU impl, remote API, CTranslate2 engine) that was
  never going to touch the GPU. So every torch path here additionally
  requires `torch.cuda.is_initialized()`: if the impl never initialized
  CUDA there is nothing of *ours* to measure and the honest answer is
  "unknown".
- **Never invent a footprint.** A process that demonstrably never allocated
  on the device reports no `base_mb` at all rather than 0 — the driver's
  free-memory delta would otherwise attribute another process's
  allocations to this model. Engines with VRAM outside torch's allocator
  (faster-whisper/CTranslate2) land in the ledger's *external usage* term
  by design, which is the correct place for them.

Import rules (docs/inferio-rust-orchestrator-design.md §4): this module is
imported by the worker entry point, whose import footprint must stay
strictly known, so it imports **stdlib only** at module level. In
particular it never imports torch: it uses torch only when the impl has
*already* imported it (`sys.modules`), because importing it here would
charge every CPU-only and remote-API worker seconds of startup and hundreds
of MB of RAM for nothing. `pynvml` (nvidia-ml-py) is a pure-python ctypes
wrapper and is imported on demand.
"""

from __future__ import annotations

import logging
import os
import re
import sys
import time
from collections.abc import Iterable
from typing import Any

logger = logging.getLogger("inferio_worker.memory")

_MIB = 1024 * 1024

# Fixed CUDA-context allowance used when the driver's free-memory delta is
# unusable. Contexts are 300-600 MB in practice; the design names ~500 MB.
CONTEXT_ESTIMATE_MB = 500

# Extra allowance, on top of the context estimate, for memory our process
# legitimately holds outside the caching allocator: cuDNN/cuBLAS workspaces,
# NCCL buffers, driver-side bookkeeping. A free-memory delta beyond
# `reserved_delta + CONTEXT_ESTIMATE_MB + this` is judged contaminated by
# another process rather than ours.
IMPLAUSIBLE_SLACK_MB = 2048

# The mirror of `IMPLAUSIBLE_SLACK_MB`, pointed the other way, for the fdinfo
# tier: how far *below* the allocator pool we are holding a per-process VRAM
# reading may sit before it is judged an under-report rather than a measurement
# (docs/rocm-batch-calibration-parity.md, D4/F6). fdinfo's KFD/compute figures
# are VM-walk-based and comparatively recent, and an older kernel can report a
# fraction of what we actually hold — which would be phantom headroom, the one
# error direction the ledger cannot absorb.
#
# The reading is expected to be **larger** than that pool (the HIP context and
# every non-torch allocation ride on top of it), so only a shortfall is
# suspicious, and only the two innocent shortfalls need covering:
# MiB truncation on both sides, and pages the driver has evicted since we
# committed them (`drm-resident-vram` counts *resident* pages, not reserved
# ones). 256 MiB covers both with room to spare while staying well under
# `CONTEXT_ESTIMATE_MB`, so a reading that missed a whole HIP context — the
# actual failure mode this guards — can never slip through as jitter.
FDINFO_UNDERREPORT_SLACK_MB = 256

# NVML memoization. The *module* half is one-shot: importing pynvml and
# calling nvmlInit either works for the life of the process or never does, so
# a failure is paid (and logged) exactly once. The *handle* half deliberately
# is not: resolving which board this worker sits on can fail before the impl
# has initialized CUDA and succeed afterwards (see `_nvml`).
_nvml_state: dict[str, Any] = {"module_tried": False, "module": None, "handle": None}

# One-shot log flags (per worker process).
_logged: dict[str, bool] = {
    "nvml_pid_missing": False,
    "nvml_board_unidentified": False,
    "hip_uuid_suppressed": False,
    "fdinfo_identity": False,
    "fdinfo_under_reported": False,
}

# This worker's own board address, memoized. The *resolution* half only —
# deliberately no negative caching, exactly as with the NVML handle (see
# `_nvml`): the first call is always pre-load, before the impl has a device,
# so a `None` there is "not yet", never "not ever".
_bdf_state: dict[str, Any] = {"bdf": None}

# Where the kernel exposes this process's own DRM clients. Absent on every
# platform but Linux, which is exactly the platforms that cannot have an
# amdgpu board (the `rocm` torch extra is Linux-only).
FDINFO_ROOT = "/proc/self/fdinfo"

# Where the amdgpu driver exposes each board's VRAM counters
# (`<root>/<bdf>/mem_info_vram_{total,used}`). Same Linux-only reasoning as
# `FDINFO_ROOT`, and the *same files the orchestrator's staleness refresh
# reads* (docs/rocm-batch-calibration-parity.md, D5) — which is what makes the
# design's "one memory vocabulary per host" rule hold by construction here
# rather than by two drivers happening to agree.
PCI_DEVICES_ROOT = "/sys/bus/pci/devices"

# The unit suffixes the DRM usage-stats format allows on a memory line
# (`drm-resident-vram: 12345 KiB`). Exactly the kernel-documented grammar,
# `<uint> [KiB|MiB]` with the suffix optional and its absence meaning plain
# bytes, per <https://docs.kernel.org/gpu/drm-usage-stats.html>. Deliberately
# not a superset: accepting spellings the format does not define (a bare `B`,
# a `GiB`) would be guessing at the meaning of a line we do not understand,
# and the safe reading of a line we do not understand is "no reading at all"
# — see `parse_drm_fdinfo`.
_DRM_UNITS = {
    "": 1,
    "KIB": 1024,
    "MIB": 1024 * 1024,
}

# A PCI address as the kernel writes it and as both sides of this design
# format it: `dddd:bb:dd.f`, lower-case hex, function 0-7. Used to validate
# the one BDF this module does *not* build itself — the one lifted out of a
# `drm-pdev` line (see `device_bdf`).
_BDF_RE = re.compile(r"[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-7]")


# ---------------------------------------------------------------------------
# torch, but only if the impl already brought it *and* already used the GPU
# ---------------------------------------------------------------------------


def _torch() -> Any | None:
    """The already-imported torch module, or None. Touches nothing."""
    return sys.modules.get("torch")


def _torch_cuda() -> Any | None:
    """The already-imported torch module iff its CUDA device is live.

    Returns None when torch was never imported, has no CUDA/ROCm device
    (CPU/MPS hosts), has one but never initialized it, or errors while
    answering. The `is_initialized` requirement is what keeps this harness
    from lazily creating the context it is meant to be measuring (module
    docstring); every torch-backed reading below goes through here.
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


# ---------------------------------------------------------------------------
# NVML: per-process footprint (tier 1) and a torch-free memory reading
# ---------------------------------------------------------------------------


def _nvml() -> tuple[Any, Any] | None:
    """`(pynvml, handle)` for the GPU this worker is pinned to, else None.

    NVML enumerates physical boards and ignores `CUDA_VISIBLE_DEVICES`, so
    the pin has to be resolved explicitly. The orchestrator writes CUDA
    pins in UUID form precisely so this lookup is unambiguous
    (batch-calibration design, "Every worker is pinned to exactly one
    GPU"); the torch and single-GPU paths cover hosts where it could not.

    Handle resolution is retried on **every** call, and that is load-bearing:
    the first call is always pre-load (`begin_load` reads free memory before
    anything touches torch), so on a host whose pin is not in UUID form there
    is no CUDA context yet and `_nvml_handle` cannot identify the board.
    Caching that first failure would kill tier 1 *and* NVML free readings for
    the worker's whole life, even though the same lookup succeeds the moment
    the impl initializes CUDA. Retrying costs one env read plus, at worst, a
    handful of NVML calls — nothing compared to what it buys.

    **Refused outright on a ROCm worker**, which is not the same thing as
    NVML being unavailable there. `pynvml` is an unconditional base
    dependency and `nvmlInit` succeeds on *any* host with an NVIDIA driver
    loaded — including a hybrid box with an AMD board doing the compute. On
    such a host the D3 UUID suppression removes the very thing that would
    have disambiguated the lookup, so the single-GPU last-resort arm below
    could hand back a handle to the NVIDIA board, and one load report would
    then describe two different pieces of silicon: base and identity from
    the AMD board, free/total from the NVIDIA one. Nothing downstream can
    detect that — the registration cross-check compares the worker's *total*
    against the inventory, and the NVIDIA total is a perfectly plausible
    number for some board. So the gate is here, ahead of every NVML path.
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
    `nvmlInit` failure is permanent (no driver, no library, no permission),
    so it is paid and logged exactly once per worker.
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
    # `CUDA_VISIBLE_DEVICES` deliberately, with no HIP_VISIBLE_DEVICES
    # fallback: a ROCm pin is a HIP device index, never a UUID, so reading it
    # here could not produce a handle — and a ROCm worker never reaches this
    # function at all, because `_nvml` refuses one outright. (`nvmlInit` does
    # fail once and permanently on a *pure* ROCm host, but a hybrid host has
    # an NVIDIA driver and would initialize happily, which is exactly why
    # that gate is explicit rather than incidental.) ROCm gets its own memory
    # tiers from amdgpu sysfs (docs/rocm-batch-calibration-parity.md, D4).
    pin = (os.environ.get("CUDA_VISIBLE_DEVICES") or "").strip()
    if pin.upper().startswith(("GPU-", "MIG-")):
        handle = _nvml_handle_by_uuid(pynvml, pin)
        if handle is not None:
            return handle
    # No usable UUID pin: ask torch for the visible device's UUID (torch >=
    # 2.1 exposes it), which stays correct whatever the pin form was. Only
    # possible once the impl has initialized CUDA; before that this falls
    # through rather than initializing it (and `_nvml` retries later).
    uuid, _ = device_identity()
    if uuid is not None:
        handle = _nvml_handle_by_uuid(pynvml, uuid)
        if handle is not None:
            return handle
    # Last resort: unambiguous only on a single-GPU host. An index pin is
    # deliberately NOT mapped to an NVML index — the two orderings differ
    # under CUDA_DEVICE_ORDER, and a wrong board is worse than no reading.
    try:
        if pynvml.nvmlDeviceGetCount() == 1:
            return pynvml.nvmlDeviceGetHandleByIndex(0)
    except Exception:
        pass
    # Retried every call (see `_nvml`), so this line is one-shot: on a
    # multi-GPU host with an index pin and a CPU-only impl it would otherwise
    # repeat for every batch.
    if not _logged["nvml_board_unidentified"]:
        _logged["nvml_board_unidentified"] = True
        logger.debug("cannot identify this worker's GPU in NVML; skipping NVML paths")
    return None


def _nvml_handle_by_uuid(pynvml: Any, uuid: str) -> Any | None:
    """Handle for `uuid`, tolerating the abbreviations CUDA accepts.

    `nvmlDeviceGetHandleByUUID` wants the full `GPU-<36 hex chars>` string,
    but the orchestrator's `resolve_pin` passes an operator's abbreviated
    `GPU-1a2b` through verbatim precisely because CUDA resolves prefixes
    itself. So a failed exact lookup falls back to enumerating boards and
    prefix-matching. An ambiguous prefix (two boards match) resolves to
    *nothing*: a reading from the wrong board is worse than none.
    """
    exact = uuid.strip()
    try:
        return pynvml.nvmlDeviceGetHandleByUUID(exact.encode())
    except Exception:
        pass
    # NVML renders UUIDs in lower-case hex; the pin may be in either case, so
    # the prefix compare is case-folded (the exact lookup above is not — NVML
    # matches the string it printed).
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
            "%s is an abbreviated UUID matching %d boards in NVML; refusing to "
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
    """This process's device footprint per NVML, or None.

    `usedGpuMemory` is N/A under Windows' WDDM driver model, which is what
    the free-delta tier exists for. The PID list is another degradation
    path: NVML reports *host* PIDs, so inside a PID namespace (a container
    without `--pid=host`) our PID is simply never in it. That is silent —
    the tier just drops — so when we know we hold device memory
    (`holding_mb`) and still cannot find ourselves, say so once.

    A reading of at least the board's whole capacity is rejected as sentinel
    garbage: some driver/NVML combinations answer with a filled-in `-1`
    (`ULLONG_MAX`) rather than None, and an absolute footprint tier that
    accepts it would charge the ledger a nonsense number with the most
    authoritative provenance we have.
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
                "NVML reports this process holding %d MiB of a %d MiB board; "
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


# ---------------------------------------------------------------------------
# Board identity (part of the calibration profile key)
# ---------------------------------------------------------------------------


def _is_hip(torch: Any) -> bool:
    """Whether this torch is a ROCm build (`torch.version.hip` is set).

    `torch.version.cuda` is `None` there and `torch.__version__` reads like
    `"2.11.0+rocm7.2"`, but `version.hip` is the one positive signal, and it
    is what the hipified `torch.cuda.*` namespace never reveals on its own.
    """
    try:
        return bool(getattr(getattr(torch, "version", None), "hip", None))
    except Exception:
        return False


def _hip_pinned() -> bool:
    """Whether the orchestrator pinned this worker to a HIP device.

    The pre-torch-import half of [`_is_hip`]: the very first memory reading a
    worker takes (`begin_load`) happens before any impl has imported torch,
    so `torch.version.hip` cannot answer yet — but our own spawner writes
    `HIP_VISIBLE_DEVICES` on every pinned ROCm worker and on no other kind
    (`gpu.rs::pin_env_var`), so a non-empty value is proof of the backend
    without importing anything.

    Whitespace/comma-only counts as unset, matching how both sides treat an
    empty visibility variable.
    """
    value = os.environ.get("HIP_VISIBLE_DEVICES") or ""
    return any(entry.strip() for entry in value.split(","))


def _unified_gpu() -> bool:
    """Whether this worker is **on** a unified-memory board (DP-5).

    The worker has no inventory and no way to ask the driver what kind of
    board it landed on — an APU's KFD node is the only thing that says so,
    and reading KFD topology here would be a second, divergent probe of the
    orchestrator's own question. So the spawner, which resolved the board to
    write the pin, names it:
    `PANOPTIKON_UNIFIED_GPU=<that board's PCI address>`
    (`gpu.rs::UNIFIED_GPU_ENV_VAR`).

    **The address is checked, not trusted.** The spawner knows which board
    the pin *named*; whether the replica came up on it is the one
    load-bearing unverifiable of the ROCm design (KFD row order = HIP device
    order). A bare flag would therefore be a belief, and a wrong belief here
    is expensive in both directions: a worker that landed on a **dGPU** would
    add GTT to its free reading and report the sum under the authoritative
    `"amdgpu-sysfs"` label — phantom headroom, the one error the ledger
    cannot absorb — while a worker that landed on the **APU** without the
    flag prices a 64 GB board at its 512 MB carve-out and collapses to
    batch-1. So this answers true only when the named address is the one
    [`_identity_bdf`] independently resolved for this process.

    Everything else — a mismatch, an unparseable value, a legacy bare `1`,
    or an identity that is not known yet (the pre-load reading, before any
    impl has touched torch) — is the discrete arithmetic, which is
    conservative in both directions.

    Read on every call rather than memoized: it costs an environment lookup
    plus the already-memoized identity, and the answer legitimately changes
    from false to true the moment the device resolves.
    """
    claimed = (os.environ.get("PANOPTIKON_UNIFIED_GPU") or "").strip().lower()
    if not _BDF_RE.fullmatch(claimed):
        return False
    return claimed == _identity_bdf()


def _memory_regions() -> tuple[str, ...]:
    """The DRM/amdgpu memory regions this worker's own usage is summed over.

    On a unified board that is VRAM **plus GTT**: once the BIOS carve-out
    fills, an APU's allocations land in GTT, so a VRAM-only figure would
    report a multi-gigabyte model as holding a few hundred MB — an
    under-measured base, which is headroom the ledger hands out twice.
    """
    return ("vram", "gtt") if _unified_gpu() else ("vram",)


def pinned_device_missing() -> str | None:
    """An actionable message when this worker was pinned to a device its own
    runtime does not enumerate, or `None` when there is nothing to report.

    The tripwire for the shape backend B newly puts on the pinned path
    (docs/unified-memory-admission.md, backend B): a desktop with an AMD
    dGPU *and* a Raphael/G-series iGPU used to be declined wholesale by the
    probe and ran unpinned, and is now two ledger boards with
    `HIP_VISIBLE_DEVICES=<row index>` pins. Those indices are positions in
    the kernel's KFD topology, and if the ROCm userspace does not enumerate
    the iGPU — an unsupported gfx target, a missing
    `HSA_OVERRIDE_GFX_VERSION` — the index names nothing, torch sees no
    device at all, and the impl quietly runs the model on the **CPU**: no
    error, no admission, and a job that takes twenty times as long for
    reasons nothing in the logs explains.

    So it is made loud instead. Called after the impl's `load()`, because
    torch is only in `sys.modules` once the impl has imported it, and it
    answers `None` for everything it cannot positively call wrong: no pin, no
    torch, or a runtime that would not say how many devices it has.

    The pin it reads is `PANOPTIKON_DEVICE_PIN`, which the spawner writes
    beside the visibility variable, and **not** the visibility variable
    itself. The two are indistinguishable in this process's environment and
    mean opposite things here: a device the *orchestrator* placed us on and
    the runtime cannot see is the silent CPU fallback above, while
    `CUDA_VISIBLE_DEVICES=-1` (or an empty value, or a scheduler's ambient
    restriction) is an operator deliberately hiding every device — a host
    that worked yesterday and must keep working. Keying off our own marker
    fires exactly where we made the placement.
    """
    pin = (os.environ.get("PANOPTIKON_DEVICE_PIN") or "").strip()
    if not pin:
        return None
    torch = _torch()
    if torch is None:
        return None
    # A **CPU-only torch build** never enumerated a device to lose, so its
    # empty device list is not a fault and must not fail a load. This is not
    # a corner case: pinning is universal, so every replica on a priced host
    # carries the marker, and `gpu::probe` prices `accelerator = "cpu"` and
    # `auto` hosts through nvidia-smi *by design* (capability filtering must
    # not depend on which wheels are installed). A box with an NVIDIA card,
    # `accelerator = "cpu"` and the CPU wheels is therefore pinned, sees no
    # devices, and is working exactly as configured. Both version fields are
    # `None` on such a build and exactly one is set on any accelerated one.
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
        "board the ROCm userspace does not enumerate (an unsupported gfx "
        "target — an integrated GPU alongside a discrete one is the common "
        "case, and HSA_OVERRIDE_GFX_VERSION is how such a part is usually "
        "made usable) or a device index that does not exist in this "
        "process's visible set. Pin this model to a board that works "
        "(inference_local `devices`), or make the pinned one enumerable"
    )


def _device_props() -> Any | None:
    """`get_device_properties(0)` for the pinned device, or None.

    Gated on [`_torch_cuda`] like every torch path here — the call *creates*
    a context on a process that has none. One helper because three public
    readings (identity, PCI address, total memory) come off the same struct
    and torch caches it, so asking three times costs nothing.
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

    **Every** read of that struct in this module goes through here, and the
    module's never-raise rule is the whole reason. The fields are pybind
    getters, not plain attributes: an older build simply does not define some
    of them (an `AttributeError`, which the `getattr` default absorbs), but a
    getter is arbitrary C++ and can raise anything at all — and an exception
    escaping here would take down `finish_load` and with it the *entire* load
    report, losing the measured base and the negotiated dtype over an
    unreadable identity field. A field that cannot be read is `None`, which
    every consumer already treats as "unknown".
    """
    try:
        return getattr(props, field, None)
    except Exception:
        return None


def device_identity() -> tuple[str | None, str | None]:
    """`(uuid, name)` of the board this worker's CUDA device 0 resolved to.

    The UUID is rendered in nvidia-smi/NVML form (`GPU-<uuid>`), which is
    byte-identical to what the orchestrator's inventory holds, so the
    ledger can key on what the worker *actually* got rather than on the
    device-pin string it was spawned with. `(None, None)` whenever CUDA is
    not live (see `_torch_cuda`).

    **On a ROCm build the UUID is suppressed entirely** (returned as `None`)
    even though torch >= 2.5 renders one: it is a *third* vocabulary,
    derived from the ASIC serial and unrelated to both the KFD `GPU-<16hex>`
    form the inventory may use and the amd-smi 8-4-4-4-12 form, and on
    consumer boards without a fused serial it degenerates to the same string
    for every card of a model. A value that can neither match nor be trusted
    to differ is worse than no value: the orchestrator keys ROCm replicas on
    [`device_bdf`] instead (docs/rocm-batch-calibration-parity.md, D3/F5).
    The *name* is kept — it is informational only; the profile keyspace is
    the orchestrator's own inventory name.
    """
    props = _device_props()
    if props is None:
        # MPS has no UUID and no board struct — one device per host, keyed by
        # a constant. The name is still worth reporting, and is derived from
        # the same two sysctls (and the same rounding) the orchestrator uses,
        # so the two spellings of this host cannot drift apart.
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
                "nor amd-smi's and repeats across same-model boards). The PCI "
                "address is reported instead"
            )
    return (
        f"GPU-{uuid}" if uuid is not None else None,
        name if isinstance(name, str) and name else None,
    )


def device_bdf() -> str | None:
    """This worker's board as a PCI address (`dddd:bb:dd.0`), or None.

    The PCI BDF is the one identity vocabulary the kernel, the amdgpu driver
    and the HIP runtime all speak, which is what makes it the ROCm ledger
    join (docs/rocm-batch-calibration-parity.md, D3). It is reported on CUDA
    hosts too — it costs nothing, and registration keys on the UUID there
    first — so nothing downstream has to know which backend it is reading.

    Two sources, in order:

    1. `get_device_properties(0)`'s `pci_domain_id`/`pci_bus_id`/
       `pci_device_id` (`hipDeviceProp_t`, also present on CUDA). These are
       **device-0-scoped**: they describe exactly the board the pin
       selected, which no scan of this process's open files could establish
       — HIP filters *above* ROCr, so a pinned worker still holds render
       nodes for every ROCR-visible board.
    2. Only on a ROCm build whose torch is too old to expose those fields:
       the dominant-VRAM DRM client in `/proc/self/fdinfo`
       ([`dominant_vram_pdev`]). Ambiguous by construction on a multi-GPU
       host, hence the fallback rather than the source — and, being a string
       copied out of a kernel file rather than one this module formats, it
       must match the `dddd:bb:dd.f` shape before it is believed.

    The function digit is forced to `.0`: the amdgpu GPU function is always
    0 (the HDMI/DP audio controller is function .1 of the *same device*, not
    the GPU's own function), which is also how the orchestrator's KFD probe
    renders it (`rocm.rs::format_bdf`), so the two sides stay joinable. An
    SR-IOV virtual function does sit at a nonzero function; forcing 0 there
    fabricates an address whose PCI directory does not exist, the
    orchestrator's VRAM read fails and the host goes unpriced — the safe
    answer for a passthrough VF.

    Which source answers is a fact about the *pin*, not the platform, today:
    the `cpu`/`cu128` extras pin torch 2.7.1 and `_CudaDeviceProperties` grew
    the PCI fields in 2.8, so on the shipped CUDA build source 1 is absent,
    source 2 is HIP-only, and no `gpu_bdf` is emitted there at all — this
    becomes live on CUDA when that pin moves to >= 2.8. The `rocm` extra pins
    2.11, so the identity chain this feeds is load-bearing on ROCm alone.
    """
    props = _device_props()
    if props is None:
        return None
    bdf = _props_bdf(props)
    if bdf is not None:
        return bdf
    if not _is_hip(_torch()):
        # A CUDA host without the PCI fields simply has no BDF to report;
        # its identity is the UUID and the fdinfo tree holds nvidia
        # character devices, not DRM clients.
        return None
    bdf = dominant_vram_pdev()
    # Shape-checked before it is believed, unlike the torch-derived address
    # above, which this module *formats* itself from three integers. This one
    # is a string lifted verbatim out of a `drm-pdev` line: the parser only
    # requires the key to be present and non-empty, so a driver that ever
    # writes something else there — or a non-amdgpu DRM client that spells the
    # field differently — would otherwise become this worker's identity, be
    # sent on the wire as `gpu_bdf`, and be joined against the orchestrator's
    # inventory and used to build a `/sys/bus/pci/devices/<bdf>` path.
    if bdf is not None and not _BDF_RE.fullmatch(bdf):
        return None
    if bdf is not None and not _logged["fdinfo_identity"]:
        _logged["fdinfo_identity"] = True
        logger.debug(
            "this torch build exposes no PCI fields on get_device_properties; "
            "identifying this worker's board as %s, the DRM client holding the "
            "most VRAM in this process",
            bdf,
        )
    return bdf


def _props_bdf(props: Any) -> str | None:
    """The BDF from torch's PCI fields, or None when they are absent.

    Read through [`_prop`] like every other field of that struct: older torch
    builds (including the 2.7.1 the CUDA extras pin) simply do not carry
    them, and a partial triple is not an address.
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
    """Total VRAM of this worker's board in MiB, per torch, or None.

    Reported on the load response so the orchestrator can cross-check a
    BDF-matched registration against an **independent** source: the
    inventory's total came from amdgpu's `mem_info_vram_total` sysfs file,
    this one from HIP, so agreement is evidence rather than a file compared
    with itself (docs/rocm-batch-calibration-parity.md, D3/F4).

    On MPS it is `recommended_max_memory()` and it is not a cross-check at
    all but the **authoritative** figure: the orchestrator's own total there
    is a seeded fraction of RAM, while this is the number the allocator's
    ceiling is actually set from (docs/unified-memory-admission.md, DP-4).
    """
    props = _device_props()
    if props is None:
        return _mb(_mps_call("recommended_max_memory"))
    return _mb(_prop(props, "total_memory"))


# ---------------------------------------------------------------------------
# DRM fdinfo (per-process, per-board VRAM)
# ---------------------------------------------------------------------------


def parse_drm_fdinfo(
    text: str, regions: tuple[str, ...] = ("vram",)
) -> tuple[str, int, int] | None:
    """`(pdev, client_id, bytes)` for one fdinfo file, or None.

    The DRM usage-stats format is one `key: value` per line
    (<https://docs.kernel.org/gpu/drm-usage-stats.html>); non-DRM fds carry
    none of these keys and parse to None. `drm-pdev` and `drm-client-id` are
    both required: the address is what the reading is *about*, and the
    client id is what makes two fds of one client countable once (an fd
    duplicated by `dup()`/fork shows the same client id, and summing both
    would double the process's VRAM).

    Both memory spellings are accepted. `drm-memory-<region>` is the kernel
    docs' deprecated alias for `drm-resident-<region>` and is "only printed
    by amdgpu" — exactly the driver this exists for — so a parser that knew
    only the modern spelling would read every AMD client as zero.
    `drm-resident-*` wins when a kernel prints both, and it wins **for the
    whole sum**: if any requested region has a resident line, every region is
    read in that spelling and a deprecated line is ignored even where it is
    the only one present. The two spellings are different vintages of the
    same accounting, and mixing them within one figure would add a modern
    region to a legacy one — a number that is not a reading of anything.

    **Absent and unreadable are different answers.** A DRM client with
    neither key is a real record with **no VRAM** — a board this process has
    open but has not allocated on, which is precisely what the dominance rule
    needs to see. A key that is *present* but does not parse (a unit outside
    the documented grammar, a number that is not one) makes the whole record
    `None` instead: reading it as 0 would be inventing an observation, and
    the observation it would invent is the one that hands dominance — and
    with it this worker's board identity — to a different card.

    `regions` is which memory regions the byte count sums. The default is
    VRAM alone, which is every discrete board and the identity fallback's
    dominance rule. A **unified** board adds `gtt`, because that is where an
    APU's allocations land once the carve-out fills (DP-5); the same
    spelling pair applies to it (`drm-resident-gtt`, and amdgpu's deprecated
    `drm-memory-gtt` alias), so nothing about the parse changes but the set
    of keys it looks for.

    The pdev is lower-cased so it compares directly against the addresses
    [`device_bdf`] and the orchestrator's inventory render.
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
    """`<uint> [KiB|MiB]` in bytes, or None when it is not that.

    The whole documented grammar and nothing else: the suffix is optional and
    bare means bytes. `None` is "this line is not a reading", which the
    caller turns into a discarded record rather than a zero.
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
    """Per-board VRAM this process holds, keyed by PCI address, in bytes.

    Pure over an iterable of fdinfo file contents so it is testable without
    `/proc`, and general (a *map*, not one winner) because D4's per-process
    memory tier consumes the same parse filtered by the identity BDF while
    the identity fallback here consumes its argmax.

    Deduplicated by DRM client id: several fds of one client are one client.
    Records [`parse_drm_fdinfo`] rejected — a non-DRM fd, or a memory line it
    could not read — contribute nothing at all, not a zero.

    `regions` is forwarded verbatim: VRAM alone by default, VRAM + GTT for a
    unified board's own footprint (see [`parse_drm_fdinfo`]).
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
    """The PCI address this process holds the most VRAM on, or None.

    The identity fallback of [`device_bdf`], and only that: a *strict*
    maximum is required, so a tie — including the all-zero tie of a process
    that has opened render nodes but allocated nothing — answers None rather
    than picking a board. Guessing wrong here does not degrade a reading, it
    prices one model's memory against another board's ledger.

    Linux-only by nature: `/proc/self/fdinfo` does not exist elsewhere, and
    the read then yields nothing.

    `root` defaults to `None` and resolves to [`FDINFO_ROOT`] *inside* the
    call, exactly as [`fdinfo_own_vram_mb`] does. A `root=FDINFO_ROOT`
    parameter default would bind the module global at import time, so the two
    readers of the same tree would answer from different roots the moment one
    is redirected.
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
    """[`device_bdf`], resolved once and then remembered, or None.

    Both amdgpu tiers below are *about one board* — this worker's — so they
    cannot read anything until the identity is known. Resolution is retried on
    **every** call until it succeeds, for the same reason the NVML handle is
    (see `_nvml`): the first call of a worker's life comes from `begin_load`,
    which reads free memory before anything has touched torch, so there is no
    device to ask yet. Caching that first `None` would silence both tiers for
    the process's whole life over a question that answers itself moments later.
    The retry costs one cached `get_device_properties` read; on an older ROCm
    torch that falls through to [`dominant_vram_pdev`] it costs one scan of
    `/proc/self/fdinfo`, which is bounded by this process's own fd count and
    stops the moment the scan identifies a board.
    """
    cached = _bdf_state["bdf"]
    if cached is not None:
        return cached
    bdf = device_bdf()
    if bdf is not None:
        _bdf_state["bdf"] = bdf
    return bdf


def fdinfo_own_vram_mb(root: str | None = None) -> int | None:
    """This process's VRAM on **its own** board, in MiB, or None.

    The ROCm twin of NVML's own-PID figure (`_nvml_own_process_mb`): an
    absolute, pollution-free whole-process footprint, which is exactly what
    `base_mb` is defined as. It reads *ourselves* — no root, no amdsmi (which
    is not on PyPI), and no PID-namespace caveat, since a container's
    `/proc/self` is its own.

    Filtered by the identity address rather than summed across boards: HIP
    filters *above* ROCr, so this process holds render nodes for every
    ROCR-visible board and the other boards' clients are not ours to charge.
    A board absent from the map, or present holding nothing, is no reading at
    all (None) rather than a zero — the never-invent-a-footprint rule.

    On a **unified** board the sum is VRAM + GTT (DP-5, [`_memory_regions`]):
    an APU's allocations spill into GTT as soon as the BIOS carve-out fills,
    which on a 512 MB carve-out is immediately.
    """
    bdf = _identity_bdf()
    if bdf is None:
        return None
    texts = _fdinfo_texts(FDINFO_ROOT if root is None else root)
    vram = fdinfo_vram_by_pdev(texts, _memory_regions()).get(bdf)
    # `vram <= 0` is redundant with the trailing falsy check (`_mb` clamps at
    # 0, so a non-positive byte count can only become 0, which is discarded
    # below) — deliberately kept: the two guards say different things. This
    # one is the never-invent-a-footprint rule at the point the *reading* is
    # taken; the other is the same rule at the point the MiB figure is
    # reported. Collapsing either into the other makes the rule implicit.
    if vram is None or vram <= 0:
        return None
    own_mb = _mb(vram)
    return own_mb if own_mb else None


def _fdinfo_base_mb(
    reserved_mb: int | None,
    reserved_delta: int | None,
    root: str | None = None,
) -> int | None:
    """[`fdinfo_own_vram_mb`] once it has passed the plausibility floor.

    fdinfo's KFD/compute memory stats are VM-walk-based and recent (~kernel
    6.x); an older kernel can under-report them, and an under-measured base is
    phantom headroom — the ledger hands out memory that is already spent. So
    the reading is checked against the one quantity we measured ourselves: the
    allocator pool this process is holding.

    The comparand is the **absolute** post-load `reserved_mb`, not the load
    window's delta, because fdinfo reports absolute whole-process VRAM: the two
    quantities only coincide on a process's *first* load, and the ledger
    explicitly anticipates repeat loads into one worker (a model reloaded after
    a trim, a replica that loads a second model). Differencing an absolute
    reading against a windowed one would compare the whole process against one
    window's growth and pass an under-report the moment the process held
    anything from before. `reserved_delta` is the fallback for the case where
    the allocator could not be read *after* the load at all, which is the only
    reading that ever leaves `reserved_mb` unknown.

    fdinfo *above* the pool is expected (the HIP context and any non-torch
    allocation sit on top of it); fdinfo materially below it means the walk did
    not see our allocations, and the tier loses to the coarser ones instead of
    reporting the shortfall. A reading at or above the board's whole capacity
    is rejected for the mirror-image reason — see the sentinel guard in
    [`_nvml_own_process_mb`], which this follows: a per-process figure that
    equals or exceeds the *device* is not a footprint, it is a parse or a
    kernel accounting artefact, and an absolute tier that accepted it would
    charge the ledger a nonsense number under the most authoritative
    provenance ROCm has.

    Structurally the mirror of the free-delta implausibility guard in
    [`_resolve_base`]: same measured quantities, same one-shot debug line, the
    inequality pointed the other way.

    **HIP-gated**, unlike the sysfs free/total tier, which needs no gate
    because `mem_info_vram_*` exists under no other driver's PCI directory.
    Recent nvidia-drm *does* publish DRM fdinfo memory stats, and they are a
    different quantity wearing the same key: GEM/DRM allocations, not the CUDA
    context and the caching allocator this base must account for. The
    plausibility floor alone would not catch that — a small model's pool is
    below the tolerance, so any reading at all passes it — and the
    result would be a base of a few MiB for a process holding a 600 MB
    context, which is the one error direction the ledger cannot absorb. On
    CUDA the per-process tier is NVML's, and its absence is what the deltas
    are for.
    """
    if not _is_hip(_torch()):
        return None
    own = fdinfo_own_vram_mb(root)
    if own is None:
        return None
    # …and on a **unified** board the bound is the same rule against a
    # different comparand, not a dropped rule. What HIP reports as an APU's
    # `total_memory` may be the BIOS carve-out alone — 512 MB is a common
    # default — while the reading legitimately includes GTT, so measuring
    # against it would reject every model worth measuring. The board's real
    # capacity there is carve-out + GTT, read from the same sysfs files but
    # **without** the free half: a bound derived from the free reading would
    # have inherited its psutil dependency and vanished silently on a machine
    # without it — the one way a missing dependency could produce an
    # over-reported footprint rather than a missing one.
    total_mb = amdgpu_board_total_mb() if _unified_gpu() else gpu_total_mb()
    if total_mb is not None and total_mb > 0 and own >= total_mb:
        logger.debug(
            "DRM fdinfo reports this process holding %d MiB of a %d MiB board; "
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


# ---------------------------------------------------------------------------
# amdgpu sysfs (device-wide free/total for this worker's board)
# ---------------------------------------------------------------------------


def amdgpu_free_total_mb(root: str | None = None) -> tuple[int | None, int | None]:
    """`(free_mb, total_mb)` for this worker's board from amdgpu sysfs.

    `mem_info_vram_total - mem_info_vram_used` on
    `/sys/bus/pci/devices/<bdf>/`, which is **device-wide** (every process's
    allocations, not just ours) and is the same pair of files the
    orchestrator's own refresh reads — so the two sides of the ledger speak
    one vocabulary by construction (design principle "one memory vocabulary
    per host"), instead of NVML's and torch's disagreement being replaced by
    amdgpu's and HIP's.

    `(None, None)` on every host that is not an amdgpu Linux box: the paths
    simply do not exist, which is also true of an NVIDIA board's PCI
    directory, so the tier needs no platform test of its own. Both files are
    required — a total without a used figure is not a free reading — and the
    subtraction saturates at 0 rather than going negative if the driver
    reports a used figure above the total mid-update.

    One known optimism, accepted and documented in D5: `total - used` ignores
    the firmware/kernel carve-outs nvidia-smi's `memory.free` excludes, so
    these readings run a few hundred MB high. The ledger's margin absorbs it.

    **On a verified unified board** (`PANOPTIKON_UNIFIED_GPU` naming the
    address this worker resolved for itself, DP-5) the same files'
    GTT neighbours join in, because an APU is budgeted against carve-out +
    GTT: `total = vram_total + gtt_total`, and
    `free = (vram_total - vram_used) + min(gtt_total - gtt_used,
    ram_available)`. The RAM clamp is the load-bearing part — unclaimed GTT
    is address space, and the pages behind it come out of the same RAM every
    other process is using, so without it a machine under real memory
    pressure would read as idle. The orchestrator's own refresh computes the
    identical formula from the identical files
    (`rocm.rs::query_memory`), which is what keeps the one-vocabulary rule
    true on this backend too; every term is required, so a board whose GTT
    counters or whose `MemAvailable` cannot be read is no reading at all
    rather than a VRAM-only one under a label that now means something else.
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
    # The `max(..., 0)` is redundant with `_mb`'s own clamp and is kept on
    # purpose: it states the *semantic* saturation (the driver updates the two
    # counters independently, so `used > total` is a real mid-update reading
    # and the honest answer is "full board"), where `_mb`'s clamp is a
    # defensive floor on an arbitrary value. Removing it would leave the
    # subtraction looking like it can go negative.
    return (_mb(max(total - used, 0)), _mb(total))


def amdgpu_board_total_mb(root: str | None = None) -> int | None:
    """This worker's board **capacity** from amdgpu sysfs, or None.

    The same figure [`amdgpu_free_total_mb`] reports as its total — VRAM
    alone, plus GTT on a verified unified board — computed without the free
    half. It exists so the capacity is knowable when the free reading is not:
    the unified free formula needs `ram_available`, i.e. psutil, and psutil is
    a dependency the worker's venv is *expected* to have but does not
    *require* for a sanity bound to hold. Deriving the fdinfo tier's upper
    bound from the free reading would have made that bound quietly vanish on
    a machine without psutil, which is the one place a missing dependency
    could turn into an over-reported footprint instead of a missing one.
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
    """`<base>/<bdf>`, with the colons swapped for dashes on Windows.

    A fixture affordance, and the exact twin of the orchestrator's
    `rocm.rs::pci_device_dir` (the two must agree or a test could pass on one
    side of the wire and not the other). A colon cannot appear in a Windows
    path component — it opens an NTFS alternate data stream — so a fixture
    tree for a board named `0000:03:00.0` is unwritable there.

    Be precise about which branch runs where. **In production on Linux — the
    only platform the amdgpu tier can exist on — this is always the plain
    join**, and the mapping branch is dead. The mapping branch does run
    outside tests, on a Windows host with a torch new enough to expose the
    PCI fields (>= 2.8): the address resolves, this builds a dash-spelled
    path, and nothing is there, which is the same "no reading" a Linux CUDA
    box gets from the plain join. It is harmless because no Windows host has
    amdgpu sysfs at all, and no Windows host can have a ROCm torch (the
    `rocm` extra carries a `sys_platform == 'linux'` marker).
    """
    return os.path.join(base, bdf.replace(":", "-") if os.name == "nt" else bdf)


def _sysfs_bytes(path: str) -> int | None:
    """A sysfs file holding one non-negative decimal integer, or None.

    Never raises (the module rule) and never guesses: a missing file, a
    permission error, a driver that changed the format and a negative number
    are all "no reading", which every caller already treats as unknown.
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


# ---------------------------------------------------------------------------
# MPS (Apple Silicon): a unified-memory board
# ---------------------------------------------------------------------------


def _torch_mps() -> Any | None:
    """The already-imported torch module iff **MPS** is this worker's device.

    The same contract `_torch_cuda` has, for the other backend: torch must be
    in `sys.modules` already (this module never imports it) and its Metal
    backend must be available. `_torch_cuda` is checked first and wins — a
    torch build can report both on a machine that has both, and everything
    downstream keys one worker to one device.

    Every attribute is `getattr`-guarded: `torch.mps` and
    `torch.backends.mps` do not exist on every build we might be running
    under (nor on the fakes the tests inject), and an `AttributeError` here
    would take down the whole load report.

    There is no `is_initialized()` analogue to gate on, and none is needed:
    the MPS allocator has no context to create — `is_available()` answers
    from the OS and the build, and the driver-allocated figure is 0 until
    something allocates. So the "never initialize the device" rule this
    module lives by is satisfied by construction rather than by a guard.
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

    `driver_allocated_memory()` is the reserved-pool analogue — total GPU
    memory allocated by the Metal driver on our behalf, which is what an
    `empty_cache()` shrinks — and `current_allocated_memory()` is the live
    tensor bytes.

    torch.mps has **no peak or reset APIs at all**, so the peaks reported per
    batch are these same figures read afterwards. That is a real
    approximation and it is accepted deliberately: the pool is monotone
    absent an `empty_cache()`, exactly as the CUDA caching allocator's is, so
    post-batch `driver_allocated` *is* the window's high-water reserved size
    unless something released the pool mid-batch — which nothing does
    (docs/inferio-worker-protocol.md, "Memory sensing").
    """
    return (
        _mb(_mps_call("driver_allocated_memory")),
        _mb(_mps_call("current_allocated_memory")),
    )


def mps_free_total_mb() -> tuple[int | None, int | None]:
    """`(free_mb, total_mb)` for a unified-memory board, or `(None, None)`.

    `total` is `recommended_max_memory()` — Metal's
    `recommendedMaxWorkingSetSize`, the figure our allocations are actually
    judged against and the one the orchestrator adopts as this board's
    authoritative total (docs/unified-memory-admission.md, DP-4). It is
    ≈75 % of RAM by default but moves with the GPU wired limit, so it is read
    rather than assumed.

    `free` is the unified formula: `max(0, min(total, ram_available))`. The
    RAM term is what makes external pressure visible at all here — the memory
    is the machine's, so a browser eating 40 GB has to show up the way a game
    eating VRAM shows up on a dGPU — and there is no accelerator-level
    counter that would ever say so.

    Without a RAM reading there is no sample: a bare `total` with no free
    figure would tell the ledger a board exists and nothing about its
    pressure, and `free_total_mb`'s contract is that both numbers come from
    the same source or neither does.
    """
    total = _mps_call("recommended_max_memory")
    if not total:
        return (None, None)
    available = _ram_available_bytes()
    if available is None:
        return (None, None)
    return (_mb(min(total, available)), _mb(total))


def _ram_available_bytes() -> int | None:
    """RAM the OS says it could deliver right now, per psutil, or None.

    psutil is a base dependency (not a torch-sized import), and
    `virtual_memory().available` is the figure that already accounts for
    reclaimable pages on each platform — the same question the
    orchestrator's own refresh asks `host_statistics64`.
    """
    try:
        import psutil
    except Exception:
        return None
    try:
        return int(psutil.virtual_memory().available)
    except Exception:
        return None


def mps_gpu_name() -> str | None:
    """`Apple M3 Max (128 GB)` — this Mac's chip and capacity, or None.

    **Diagnostic only.** Nothing downstream keys on it: the registration
    join is backend + single-board inventory, and the calibration profile
    key is the name the orchestrator's own probe derived, never this field
    (docs/inferio-worker-protocol.md, `gpu_name`). Dropping it would cost a
    log line and nothing else.

    It is still derived rather than left blank because it is byte-identical
    to the orchestrator's name for the same host
    (`panoptikon/src/inferio/mps.rs::board_name`), from the same two sysctls
    and the same rounding — which makes it a free cross-check on two
    derivations that would otherwise be able to drift apart unnoticed.

    ctypes rather than a subprocess: the worker must not fork `sysctl` on a
    load path, and `platform` exposes neither of these values.
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


def torch_version() -> str | None:
    """`torch.__version__`, or None when the impl never imported torch.

    Part of the calibration profile key and knowable only here: the
    orchestrator does not know which torch the worker's venv holds. Needs
    no CUDA, so it is answered even for a CPU-only load.
    """
    torch = _torch()
    version = getattr(torch, "__version__", None) if torch is not None else None
    return str(version) if version is not None else None


# ---------------------------------------------------------------------------
# Samples
# ---------------------------------------------------------------------------


def device_memory_sample() -> dict[str, Any] | None:
    """A memory sample for this worker's GPU, or None if nothing is known.

    Wire shape: docs/inferio-worker-protocol.md "Memory sensing". Keys are
    always present when a sample is produced at all; individual values are
    None where the platform cannot answer.

    `free_mb`/`total_mb` come from the same single-source helper the base
    measurement uses (NVML preferred), and the sample says which one answered.
    Preferring torch here and NVML there would put readings 3.4 GB apart in
    the same field on the same host, and step 1b derives *other processes'*
    usage from this field — the skew would land straight in its margin.
    """
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
    """`(reserved_mb, allocated_mb)` for our allocator, or `(None, None)`.

    Separate from [`device_memory_sample`] because the reactive shrink needs
    only these two numbers once per window: the sample additionally reads
    free/total memory, which on an NVML host is a driver query, and there is
    no reason to pay for one to answer a question about our own allocator.

    Both numbers, not just the pool size, because the only thing an
    `empty_cache()` can actually hand back is `reserved - allocated`: the
    blocks no live tensor sits in. Weights are `allocated`, so a decision made
    on `reserved` alone would read a model's own weights as releasable slack.
    """
    reserved, allocated, _, _ = _allocator_stats()
    return (reserved, allocated)


def empty_cache() -> bool:
    """Release the caching allocator's unused pool. Returns whether it ran.

    Freeing tensors gives nothing back to the driver — torch's caching
    allocator keeps the blocks — so this is the *only* way our process returns
    VRAM to the board short of exiting. Both step-2 paths end here: the
    worker's own reactive shrink between batches, and the orchestrator's
    `trim` request to an idle resident (docs/batch-calibration-design.md,
    "Reactive shrink" and "Trim for idle residents").

    Gated on a live CUDA context exactly like every other torch path in this
    module: `torch.cuda.empty_cache()` on a process that never initialized CUDA
    would create the 300-600 MB context this module exists to avoid creating.
    False therefore means "nothing of ours is on the device", which is also the
    correct answer to "was there a pool to release".
    """
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
    """The MPS arm of [`empty_cache`] — `torch.mps.empty_cache()`.

    The trim path is the orchestrator's hygiene message *and* the worker's
    own reactive shrink, and both end here, so this is the one place the MPS
    pool can ever be released. (`inferio.impl.utils.clear_cache` has known
    about MPS since long before this, but nothing on the worker's trim path
    goes through it.)
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
    """`(reserved, allocated, peak_reserved, peak_allocated)` in MiB.

    On MPS the two "peaks" are the live figures: torch.mps exposes no peak
    counters, and the pool is monotone between `empty_cache()` calls, so the
    post-batch reading is the batch's high-water mark (see [`mps_pool_mb`]).
    """
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
    """`(free_mb, total_mb, source)`: `nvml`|`amdgpu-sysfs`|`mps`|`torch`.

    The one place free/total memory is read, so every consumer (the base
    measurement's deltas and the wire sample) sees the same currency.

    The chain is tried in that order on **every** host, with no platform test
    of its own here, because each tier's own availability already is one. On a
    *pure* ROCm host `nvmlInit` fails once and permanently (and is memoized, so
    the cost is one failed import for the process's life); on a **hybrid**
    AMD+NVIDIA host it would succeed, which is why the NVIDIA-ness test lives
    inside `_nvml` itself rather than being left to the driver — see its
    docstring. Downwards the availability rule holds unaided: amdgpu's
    `mem_info_vram_*` files exist only under an amdgpu board's PCI directory,
    so a CUDA host falls through that tier by its absence. Effective order:
    `nvml → torch` on CUDA, `amdgpu-sysfs → torch` on ROCm.

    Both driver-level tiers see the whole board; `mem_get_info` is last-resort
    on both backends (on HIP doubly so — its "free" was historically
    process-local, ROCm/hip#348 — which is why the ledger treats `"torch"` as
    non-authoritative). The sources do not agree (NVML and `mem_get_info`
    measured 3.4 GB apart on the dev box), so a delta across the load window is
    only meaningful between two readings of the *same* source. Pass the source
    the "before" reading came from to pin it; `None` picks the best available.
    Both numbers always come from whichever source answered — never one from
    each, and a pinned source that cannot answer yields nothing rather than
    silently sliding down the chain.
    """
    if source in (None, "nvml"):
        free, total = _nvml_memory()
        if free is not None:
            return (free, total, "nvml")
        # Belt and braces: a pinned source that cannot answer already falls
        # out of the `source in (None, X)` guards below and reaches the final
        # `(None, None, None)`. Kept because it states the pinning contract
        # *at the tier that failed* — "this pin does not slide down the
        # chain" — rather than leaving it to be inferred from three guards
        # further down. Same for the `amdgpu-sysfs` early return below.
        if source == "nvml":
            return (None, None, None)
    if source in (None, "amdgpu-sysfs"):
        # Byte-identical to the Rust `MemoryQuery`'s label for the same files
        # (`gpu.rs::free_source`), which is what makes the orchestrator's
        # free-source consistency rule hold across the two components — and it
        # names the *driver*, not the filesystem, so no future generic
        # sysfs-derived reporter inherits its authority by string collision.
        free, total = amdgpu_free_total_mb()
        if free is not None:
            return (free, total, "amdgpu-sysfs")
        # Belt and braces, as above: unreachable as behaviour, load-bearing
        # as a statement of the contract.
        if source == "amdgpu-sysfs":
            return (None, None, None)
    if source in (None, "mps"):
        # Byte-identical to the orchestrator's label for the same reading
        # (`gpu.rs::free_source`), which both sides derive from the OS's RAM
        # statistics — the only whole-machine view a unified board has.
        # Availability is the platform test again: `torch.backends.mps` is
        # unavailable everywhere else.
        free, total = mps_free_total_mb()
        if free is not None:
            return (free, total, "mps")
        if source == "mps":
            return (None, None, None)
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
    """Public `(free_mb, total_mb, source)` reading for this worker's GPU.

    The packing harness's defensive clamp needs a *live* reading before every
    GPU batch (docs/batch-calibration-design.md: freshness is per-batch in the
    shrink direction), and it must come from the same single source everything
    else here uses or the comparison against the grant is meaningless.
    """
    return _free_total_mb()


# ---------------------------------------------------------------------------
# Base measurement (tiered; design "Base measurement")
# ---------------------------------------------------------------------------


def begin_load() -> dict[str, Any]:
    """Snapshot what `finish_load` needs to price the load. Never raises.

    Order matters twice over. The free-memory reading is taken **before any
    torch.cuda call**, so (a) nothing here can initialize CUDA on a process
    that has not, and (b) on a process that has not, the context the load is
    about to create is inside the measured window — the whole point of
    measuring base in driver currency. The peak reset is skipped when CUDA
    is not live yet; `finish_load` re-reads the allocator afterwards, so an
    impl that initializes CUDA during `load()` still produces a reading
    (with `before` = 0, which is exactly right for a fresh context).
    """
    try:
        free_mb, free_source = _free_mb()
        _reset_peaks()
        reserved, allocated, _, _ = _allocator_stats()
        return {
            "free_mb": free_mb,
            "free_source": free_source,
            "reserved_mb": reserved,
            "allocated_mb": allocated,
        }
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("pre-load memory snapshot failed: %s", exc)
        return {}


def finish_load(before: dict[str, Any], instance: Any) -> dict[str, Any]:
    """Load-response payload: base, provenance, pool size, dtype, sample.

    Keys whose value could not be measured are omitted entirely, so a
    worker with no torch and no NVML replies with a plain `ok` exactly as
    before this existed.
    """
    try:
        return _finish_load(before, instance)
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("base measurement failed: %s", exc)
        return {}


def _finish_load(before: dict[str, Any], instance: Any) -> dict[str, Any]:
    reserved, allocated, _, peak_allocated = _allocator_stats()
    free_after, _ = _free_mb(before.get("free_source"))

    allocated_delta = _delta(allocated, before.get("allocated_mb"))
    reserved_delta = _delta(reserved, before.get("reserved_mb"))
    # Tier 3 is always computed: the allocator's own peak delta is the floor
    # under whatever the coarser tiers report.
    alloc_floor = _delta(peak_allocated, before.get("allocated_mb"))
    if alloc_floor is None:
        alloc_floor = allocated_delta
    # Evidence that *this* process put something on the device. Not "torch is
    # importable" and not "CUDA is available": a torch-importing worker that
    # never allocates (CTranslate2/faster-whisper, CPU-fallback impls, remote
    # APIs) has no footprint of its own to report, and the free-memory delta
    # would hand it someone else's.
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
    dtype = resolved_dtype_name(instance)
    if dtype is not None:
        payload["dtype"] = dtype
    uuid, name = device_identity()
    if uuid is not None:
        payload["gpu_uuid"] = uuid
    if name is not None:
        payload["gpu_name"] = name
    # The BDF and the board's total memory: the ROCm ledger join and the
    # independent cross-check that guards it (D3). Emitted on CUDA too — the
    # keys are additive and the orchestrator keys on the UUID first there.
    # Through the memoizing accessor, not `device_bdf` directly: the wire
    # field is the ledger's join key and the amdgpu tiers' filter, and one
    # value has to serve both or a board whose address became resolvable
    # mid-load would be *attributed* to one board while its memory was
    # *measured* on another.
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

    One decision, not a ladder of special cases:

    1. A process that did not demonstrably allocate on the device reports
       nothing at all — no tier applies to it. This deliberately excludes
       engines whose VRAM never passes through torch's allocator
       (CTranslate2/faster-whisper): on Linux NVML *could* price them, but
       the ledger accounts for them in its external-usage term instead, and
       a footprint that appears on Linux and vanishes on Windows would be
       worse than one that is consistently external.
    2. NVML's own-PID figure then wins outright — absolute, pollution-free,
       and already the whole-process footprint `base` is defined as. Its ROCm
       twin, DRM fdinfo's per-process VRAM on this worker's own board
       (`base_method: "fdinfo"`), is the same kind of reading and takes the
       same rank: NVML is asked first and dies naturally on a ROCm host, so on
       any one host exactly one of the two can answer. MPS's
       `driver_allocated_memory()` (`base_method: "mps"`) joins them at that
       rank for the same reason — it is per-process by construction, so it
       needs neither a PID lookup nor a plausibility floor.
    3. Otherwise the driver's free-memory delta is used *if usable*:
       present, positive, and not implausibly larger than what we could
       plausibly hold outside the allocator (reserved delta + context +
       workspace allowance). Note the plausibility test is against
       **reserved**, not allocated: the caching allocator legitimately
       overshoots live tensors during `from_pretrained`, and comparing to
       allocated would reject perfectly good readings.
    4. A usable free delta below the allocator floor means the driver saw
       less move than our own allocator did, so the floor wins instead.

    Both routes to `alloc_delta` — an unusable free delta, and a usable one
    the allocator floor beats — report the same formula: the allocator delta
    **plus** the fixed context allowance. The context is real whether or not
    the driver reading could show it, and one `base_method` value must name
    exactly one formula or a stored profile cannot be interpreted at all.

    `base_method` names whichever term actually produced the number, so a
    profile never claims driver-currency provenance for an allocator-derived
    figure.
    """
    if not touched_gpu:
        return (None, None)
    own = _nvml_own_process_mb(holding_mb=reserved_mb)
    if own is not None and own > 0:
        return (own, "nvml")
    own = _fdinfo_base_mb(reserved_mb, reserved_delta)
    if own is not None and own > 0:
        return (own, "fdinfo")
    # MPS's own tier-1: `driver_allocated_memory()` after the load is this
    # process's whole Metal footprint **by construction** — each process owns
    # its heap and the figure counts what the driver allocated for us, not
    # just what our allocator handed out — so it is the same quality of
    # reading NVML's own-PID figure is, without a PID lookup to get wrong.
    own = _mb(_mps_call("driver_allocated_memory"))
    if own is not None and own > 0:
        return (own, "mps")

    floor = alloc_floor or 0
    free_delta = _free_delta(before.get("free_mb"), free_after)
    ceiling = (reserved_delta or 0) + CONTEXT_ESTIMATE_MB + IMPLAUSIBLE_SLACK_MB
    if free_delta is None or free_delta <= 0 or free_delta > ceiling:
        if free_delta is not None and free_delta > ceiling:
            logger.debug(
                "free-memory delta %d MiB implausible against %d MiB reserved "
                "(+%d MiB context, +%d MiB workspace allowance); using the "
                "allocator delta plus the context estimate",
                free_delta,
                reserved_delta or 0,
                CONTEXT_ESTIMATE_MB,
                IMPLAUSIBLE_SLACK_MB,
            )
        return (floor + CONTEXT_ESTIMATE_MB, "alloc_delta")
    if free_delta >= floor:
        return (free_delta, "free_delta")
    return (floor + CONTEXT_ESTIMATE_MB, "alloc_delta")


def _delta(after: int | None, before: int | None) -> int | None:
    """Growth of a monotonic-ish allocator counter, clamped at 0.

    A `None` "before" is treated as 0: an impl that initialized CUDA inside
    `load()` has no pre-load allocator reading, and 0 is the correct baseline
    for a context that did not exist yet.
    """
    if after is None:
        return None
    return max(after - (before or 0), 0)


def _free_delta(before_mb: int | None, after_mb: int | None) -> int | None:
    """How much free memory the driver lost across the load window.

    Both readings are required and neither may substitute 0 for the other
    (missing "after" would otherwise read as "the whole board went to us").
    Returned unclamped: a non-positive delta is a signal — another process
    released memory during our window — that the caller acts on.
    """
    if before_mb is None or after_mb is None:
        return None
    return before_mb - after_mb


# ---------------------------------------------------------------------------
# dtype provenance
# ---------------------------------------------------------------------------

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
    """The load precision actually in use: `"fp16"`/`"bf16"`/`"fp32"`, else None.

    Three sources, in order of authority:

    1. `instance.resolved_dtype` — the forward-looking convention for an
       impl that wants to state its outcome, in either form;
    2. the last decision `inferio.impl.utils.select_dtype` recorded — read
       through `sys.modules` so this module never imports the `inferio`
       package (the harness must not depend on it), only observes it when
       the impl already did;
    3. an instance `dtype`/`_dtype` attribute, **only** when it holds a real
       `torch.dtype` object. Those names are ambiguous in today's impls: a
       few store the negotiated dtype, but dots_ocr stores the *requested*
       precision string, which is a config value and can differ from what
       was actually loaded (`select_dtype` downgrades bf16 on pre-Ampere).
       Preferring a string here is how a profile ends up keyed on a
       precision the model is not running in.

    None whenever nothing negotiated a precision (CPU impls, remote APIs).
    """
    name = _dtype_name(getattr(instance, "resolved_dtype", None))
    if name is not None:
        return name
    utils = sys.modules.get("inferio.impl.utils")
    getter = getattr(utils, "last_selected_dtype", None) if utils else None
    if getter is not None:
        try:
            name = _dtype_name(getter())
        except Exception:
            name = None
        if name is not None:
            return name
    for attribute in ("dtype", "_dtype"):
        value = getattr(instance, attribute, None)
        if _is_torch_dtype(value):
            name = _dtype_name(value)
            if name is not None:
                return name
    return None


# ---------------------------------------------------------------------------
# Per-batch measurement
# ---------------------------------------------------------------------------


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
    throughput_collapse: bool = False,
) -> dict[str, Any]:
    """One measurement map for the batch bracketed by `state` (never raises).

    `items` is a plain input count; `units` is the same batch priced in the
    model's declared cost dimension, which only the packing harness can know
    (it is the side that sees decoded inputs) and which is what the
    orchestrator's cost fit regresses against. `oom` and
    `throughput_collapse` are the negative-sample flags — see the protocol
    doc's "Memory sensing".

    `duration_ms` covers only what the caller bracketed. The harness brackets
    `instance.predict(batch)` alone, deliberately: unit pricing (image-header
    reads, byte counts) happens before the bracket so the throughput-collapse
    comparator sees GPU throughput rather than CPU decode noise.
    """
    try:
        _, _, peak_reserved, peak_allocated = _allocator_stats()
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
        if units is not None:
            measurement["units"] = units
        if oom:
            measurement["oom"] = True
        if throughput_collapse:
            measurement["throughput_collapse"] = True
        return measurement
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("batch measurement failed: %s", exc)
        # The peaks are what failed to read; the flags were already decided by
        # the caller and are the negative samples the orchestrator's deflation
        # path runs on. Dropping them here would silently discard an OOM
        # because an allocator query happened to raise.
        minimal: dict[str, Any] = {"items": items}
        if oom:
            minimal["oom"] = True
        if throughput_collapse:
            minimal["throughput_collapse"] = True
        return minimal


def finish_batch(state: dict[str, Any], items: int) -> dict[str, Any]:
    """Predict-response payload for a **grantless** window: a fresh sample
    plus one measurement covering the whole `instance.predict` call.

    This is the compatibility path (`none`-class models, CPU/MPS hosts, hosts
    with no GPU inventory, an orchestrator that sends no grant): the window is
    the GPU batch, so there is exactly one measurement and no `units` — with
    no grant there is no declared cost dimension to price in. The packing
    harness reports one entry per GPU batch instead, which is why the wire
    field is an array in both cases.
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
