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
import sys
import time
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
}


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
    the pin has to be resolved explicitly. The orchestrator writes the pin
    in UUID form precisely so this lookup is unambiguous
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
    """
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


def device_identity() -> tuple[str | None, str | None]:
    """`(uuid, name)` of the board this worker's CUDA device 0 resolved to.

    The UUID is rendered in nvidia-smi/NVML form (`GPU-<uuid>`), which is
    byte-identical to what the orchestrator's inventory holds, so the
    ledger can key on what the worker *actually* got rather than on the
    `CUDA_VISIBLE_DEVICES` string it was spawned with. `(None, None)`
    whenever CUDA is not live (see `_torch_cuda`).
    """
    torch = _torch_cuda()
    if torch is None:
        return (None, None)
    try:
        props = torch.cuda.get_device_properties(0)
    except Exception:
        return (None, None)
    uuid = getattr(props, "uuid", None)
    name = getattr(props, "name", None)
    return (
        f"GPU-{uuid}" if uuid is not None else None,
        name if isinstance(name, str) and name else None,
    )


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
    reserved_mb = allocated_mb = None
    torch = _torch_cuda()
    if torch is not None:
        try:
            reserved_mb = _mb(torch.cuda.memory_reserved())
            allocated_mb = _mb(torch.cuda.memory_allocated())
        except Exception:
            pass
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


def _reset_peaks() -> None:
    torch = _torch_cuda()
    if torch is None:
        return
    try:
        torch.cuda.reset_peak_memory_stats()
    except Exception:
        pass


def _allocator_stats() -> tuple[int | None, int | None, int | None, int | None]:
    """`(reserved, allocated, peak_reserved, peak_allocated)` in MiB."""
    torch = _torch_cuda()
    if torch is None:
        return (None, None, None, None)
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
    """`(free_mb, total_mb, source)` for the worker's GPU, source nvml|torch.

    The one place free/total memory is read, so every consumer (the base
    measurement's deltas and the wire sample) sees the same currency.

    NVML is preferred: it answers before this process has a CUDA context,
    whereas `mem_get_info` would *create* one. The two sources do not agree
    (measured 3.4 GB apart on the dev box — NVML sees the whole board,
    `mem_get_info` the current context's view), so a delta across the load
    window is only meaningful between two readings of the *same* source.
    Pass the source the "before" reading came from to pin it; `None` picks
    the best available. Both numbers always come from whichever source
    answered — never one from each.
    """
    if source in (None, "nvml"):
        free, total = _nvml_memory()
        if free is not None:
            return (free, total, "nvml")
        if source == "nvml":
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
       and already the whole-process footprint `base` is defined as.
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


def finish_batch(state: dict[str, Any], items: int) -> dict[str, Any]:
    """Predict-response payload: a fresh sample plus one measurement entry.

    Step 1a measures the whole `predict` call as one batch; the field is an
    array because the packing harness (step 1b) reports one entry per GPU
    batch. `items` is a plain input count, not cost-dimension units — only
    the packing harness decodes inputs, so a dimension-priced `units` key
    joins the measurement then (protocol doc, "Memory sensing").
    """
    try:
        _, _, peak_reserved, peak_allocated = _allocator_stats()
        started = state.get("started")
        duration_ms = (
            round((time.perf_counter() - started) * 1000.0, 3)
            if isinstance(started, float)
            else None
        )
        measurement = {
            "items": items,
            "reserved_before_mb": state.get("reserved_before_mb"),
            "peak_reserved_mb": peak_reserved,
            "allocated_before_mb": state.get("allocated_before_mb"),
            "peak_allocated_mb": peak_allocated,
            "duration_ms": duration_ms,
        }
        payload: dict[str, Any] = {"measurements": [measurement]}
        sample = device_memory_sample()
        if sample is not None:
            payload["memory"] = sample
        return payload
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("batch measurement failed: %s", exc)
        return {}
