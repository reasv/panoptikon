import io
import sys
import os
import logging
import json
import re
import struct
from typing import List, Optional, Sequence
import numpy as np
from PIL import Image
import PIL.Image
from io import BytesIO
from typing import Optional

# The device the orchestrator priced this worker against, written by the
# spawner on a host whose admission board is system RAM
# (`panoptikon/src/accelerator_env.rs`, docs/unified-memory-admission.md
# backend C). `cpu` is the only value defined today.
DEVICE_ENV_VAR = "INFERIO_DEVICE"
_FORCED_DEVICES = frozenset({"cpu"})


def forced_device() -> Optional[str]:
    """The device the orchestrator requires, or None when it said nothing.

    This exists because the probe below and the orchestrator answer different
    questions about the same host. `get_device` asks the *machine* what it
    has; the orchestrator's pricing follows which torch wheels are actually
    installed and what the user configured. On a host where those diverge —
    an `accelerator = "cpu"` Mac, or a box with an NVIDIA card whose venv
    holds the CPU wheels — the model would otherwise run on a device nothing
    budgeted a batch against, which is the incoherence the design's
    device-coherence item closes.

    An unrecognised value is treated as unset, with a warning: a newer
    orchestrator naming a device this worker does not know must degrade to
    probing rather than fail every load.
    """
    value = (os.environ.get(DEVICE_ENV_VAR) or "").strip().lower()
    if not value:
        return None
    if value not in _FORCED_DEVICES:
        logging.getLogger(__name__).warning(
            "%s=%r is not a device this worker understands (expected one of "
            "%s); falling back to probing the hardware",
            DEVICE_ENV_VAR,
            value,
            ", ".join(sorted(_FORCED_DEVICES)),
        )
        return None
    return value


def get_device():
    import torch

    """
    Returns the appropriate torch device based on the available hardware.
    Supports CUDA, ROCm, MPS (Apple Silicon), and CPU.

    The orchestrator's own answer wins when it gave one (`INFERIO_DEVICE`,
    see `forced_device`); only then does this probe the hardware.
    """
    forced = forced_device()
    if forced is not None:
        return [torch.device(forced)]
    if torch.cuda.is_available():  # This covers both CUDA and ROCm
        num_gpus = torch.cuda.device_count()
        if num_gpus > 1:
            devices = [torch.device(f"cuda:{i}") for i in range(num_gpus)]
        else:
            devices = [torch.device("cuda")]
        devices = _drop_uncovered_cuda_devices(devices)
        if devices:
            return devices
        return [torch.device("cpu")]
    elif torch.backends.mps.is_available():  # Apple Silicon (M1/M2)
        return [torch.device("mps")]
    else:
        return [torch.device("cpu")]


def _drop_uncovered_cuda_devices(devices: list) -> list:
    """Filter out GPUs this torch build has no kernels for.

    Cubins are forward-compatible within a major version (an sm_60 kernel
    runs on sm_61), so a device (major, minor) is covered when the build's
    arch list has an sm_ entry with the same major and minor <= the
    device's. Skipped under ROCm (arch entries are gfx strings) and when
    the arch list is empty. A no-op on cu128 (kernels reach sm_50); this
    is the safety net for the cu13x transition, which drops old majors,
    and the correct guard for the genuine "no kernel image" failure class.
    """
    import torch

    if getattr(torch.version, "hip", None):
        return devices
    floors: dict = {}
    for arch in torch.cuda.get_arch_list():
        match = re.fullmatch(r"sm_(\d{2,3})[a-z]?", arch)
        if not match:
            continue  # compute_* PTX entries and anything unrecognised
        num = match.group(1)
        major, minor = int(num[:-1]), int(num[-1])
        floors[major] = min(minor, floors.get(major, minor))
    if not floors:
        return devices

    log = logging.getLogger(__name__)
    kept = []
    for dev in devices:
        major, minor = torch.cuda.get_device_capability(dev)
        if major in floors and floors[major] <= minor:
            kept.append(dev)
        else:
            log.warning(
                "Dropping GPU %s: compute capability %d.%d has no kernels "
                "in this torch build (archs: %s).",
                dev,
                major,
                minor,
                ", ".join(torch.cuda.get_arch_list()),
            )
    if not kept:
        log.error(
            "No installed CUDA kernels cover any available GPU; "
            "falling back to CPU."
        )
    return kept


def clear_cache() -> None:
    """
    Clears the torch memory cache if applicable:
    - CUDA (NVIDIA and ROCm): uses torch.cuda.empty_cache()
    - MPS (Apple Silicon): uses torch.mps.empty_cache()
    """
    import torch

    if torch.cuda.is_available():
        torch.cuda.empty_cache()
    elif hasattr(torch, "mps") and torch.backends.mps.is_available():
        torch.mps.empty_cache()


def cuda_capability(device) -> "tuple[int, int] | None":
    """CUDA compute capability (major, minor) of `device`.

    None for CPU/MPS, and for ROCm/HIP builds, where the reported numbers
    are HIP versions and must not be compared against CUDA capabilities.
    """
    import torch

    if getattr(device, "type", None) != "cuda" or getattr(
        torch.version, "hip", None
    ):
        return None
    return torch.cuda.get_device_capability(device)


_PRECISION_NAMES = {
    "bf16": "bf16",
    "bfloat16": "bf16",
    "fp16": "fp16",
    "float16": "fp16",
    "fp32": "fp32",
    "float32": "fp32",
}


def _precision_to_dtype(name: str):
    import torch

    canonical = _PRECISION_NAMES.get(str(name).lower())
    if canonical is None:
        raise ValueError(
            f"Unknown precision {name!r}; expected one of "
            f"{sorted(set(_PRECISION_NAMES))}"
        )
    return {
        "bf16": torch.bfloat16,
        "fp16": torch.float16,
        "fp32": torch.float32,
    }[canonical]


_last_selected_dtype = None


def last_selected_dtype():
    """The dtype the most recent `select_dtype` call returned, or None.

    Calibration profiles are keyed by the *negotiated* dtype, and the
    worker harness has to report it on the load response
    (docs/inferio-worker-protocol.md, "Memory sensing") without knowing
    which impl attribute — if any — holds it. Each worker process loads
    exactly one model, so "the last decision in this process" is that
    model's dtype. Read through `sys.modules` by the harness, which must
    not import this package.
    """
    return _last_selected_dtype


def select_dtype(
    device,
    preferred: str,
    explicit: str | None = None,
    logger: logging.Logger | None = None,
) -> "torch.dtype":
    """Negotiate a load dtype for `device`.

    `preferred` is the model's best-case precision ("bf16"/"fp16"/"fp32");
    `explicit` is a user-configured override that wins verbatim. bf16 wants
    tensor-core support (sm_80+); below that the fallback is fp32, never
    fp16 — fp16 lacks bf16's exponent range, so a silent step down can
    produce inf/NaN in bf16-trained weights. fp16 runs on every CUDA arch
    we ship kernels for, so it is honoured as-is.
    """
    dtype = _select_dtype(device, preferred, explicit, logger)
    global _last_selected_dtype
    _last_selected_dtype = dtype
    return dtype


def _select_dtype(
    device,
    preferred: str,
    explicit: str | None = None,
    logger: logging.Logger | None = None,
) -> "torch.dtype":
    import torch

    log = logger or logging.getLogger(__name__)
    if explicit is not None:
        dtype = _precision_to_dtype(explicit)
        cap = cuda_capability(device)
        if dtype is torch.bfloat16 and cap is not None and cap < (8, 0):
            log.warning(
                "Precision %r configured but GPU capability %d.%d is below "
                "8.0; bf16 has no tensor-core path here and may fail per-op.",
                explicit,
                *cap,
            )
        return dtype

    want = _precision_to_dtype(preferred)
    if getattr(device, "type", None) != "cuda":
        if want is not torch.float32:
            log.info(
                "Precision %r requested but device is %s; using fp32.",
                preferred,
                getattr(device, "type", device),
            )
        return torch.float32
    if want is torch.bfloat16:
        if getattr(torch.version, "hip", None):
            if torch.cuda.is_bf16_supported():
                return torch.bfloat16
            log.info("bf16 unsupported on this ROCm device; using fp32.")
            return torch.float32
        cap = cuda_capability(device)
        if cap is not None and cap < (8, 0):
            log.info(
                "bf16 requested but GPU capability %d.%d is below 8.0; "
                "using fp32 (not fp16: narrower exponent range).",
                *cap,
            )
            return torch.float32
    return want


def select_ct2_compute_type(
    preferred: str = "float16",
    explicit: str | None = None,
    logger: logging.Logger | None = None,
    device_kind: str | None = None,
) -> str:
    """Pick a CTranslate2 compute type the device actually supports.

    Queries ctranslate2.get_supported_compute_types instead of hardcoding a
    compute-capability threshold, because CT2 raises rather than degrades
    when an explicitly named type is unsupported (float16 needs CC >= 7.0).
    Any probe failure falls back to float32, which is always supported —
    this also covers ROCm, where torch reports CUDA available but CT2 has
    no HIP backend.

    `device_kind` (`"cuda"`/`"cpu"`) is the device the *caller* resolved, and
    it wins when given. Probing `torch.cuda.is_available()` here asks about
    the machine, which is the wrong question on a host the orchestrator priced
    against system RAM (`INFERIO_DEVICE=cpu`): the model runs on the CPU, so
    the compute type has to be one the CPU supports
    (docs/unified-memory-admission.md, backend C). Omitted, the probe is kept
    for callers that have not resolved a device of their own.
    """
    log = logger or logging.getLogger(__name__)
    if explicit is not None:
        return explicit
    try:
        import ctranslate2
        import torch

        if device_kind is not None:
            kind = "cuda" if device_kind == "cuda" else "cpu"
        else:
            kind = "cuda" if torch.cuda.is_available() else "cpu"
        supported = set(ctranslate2.get_supported_compute_types(kind))
    except Exception as err:
        log.warning(
            "CT2 compute-type probe failed (%s); using float32.", err
        )
        return "float32"
    for candidate in (preferred, "float16", "float32"):
        if candidate in supported:
            if candidate != preferred:
                log.info(
                    "CT2 compute type %r unsupported on %s; using %r.",
                    preferred,
                    kind,
                    candidate,
                )
            return candidate
    log.warning(
        "No supported CT2 compute type among %r on %s; using float32.",
        (preferred, "float16", "float32"),
        kind,
    )
    return "float32"

OOM_BATCH1_PREFIX = "INFERENCE_OOM_BATCH_SIZE_1:"


class InferenceOOMError(RuntimeError):
    """Out of GPU memory on a single input after cache-clearing retries.

    str() starts with OOM_BATCH1_PREFIX: the worker's error frame carries
    only the message string, so the prefix is what the orchestrator (and
    future dispatch-side classification) can recognise the condition by.
    """


_oom_retry_generation = 0
_last_oom_retry: "tuple[int, int, int] | None" = None
_total_oom_halvings = 0


def total_oom_halvings() -> int:
    """Halvings this process has performed across *every* call, monotonically.

    `last_oom_retry` describes the most recent call only, and several impls
    call `run_with_oom_retry` more than once per `predict` (CLIP and
    nemotron-embed-vl run a text pass and an image pass). Reading only the last
    call's record loses an out-of-memory condition absorbed by an earlier one —
    exactly the sample the orchestrator's deflation path exists for. The worker
    harness diffs this counter across the whole `predict` call instead, so any
    halving anywhere inside it flags the batch
    (docs/inferio-worker-protocol.md, "Memory sensing").

    Monotonic and never reset: a diff across a bracketed call is what callers
    want, and resetting it would race with anything else reading it.
    """
    return _total_oom_halvings


def last_oom_retry():
    """What the most recent `run_with_oom_retry` call actually executed.

    `(generation, largest_chunk_executed, halvings_performed)`, or None when
    no call has recorded anything yet in this process.

    The worker's packing harness reads this through `sys.modules` (it must
    not import this package) to answer one question about the batch it just
    handed to `predict`: *did the impl run the batch it was given?* If not,
    the batch is unpriceable — the allocator peaks describe a fraction of the
    packed units, and reporting the packed figure would bias the fitted slope
    low, i.e. towards over-admission
    (docs/inferio-worker-protocol.md, "Memory sensing").

    `generation` counts calls, and is what makes the reading unambiguous: an
    impl that sub-batches on one GPU batch and not the next would otherwise
    leave the harness reading a stale record as if it described the new
    batch. The harness compares the generation before and after `predict` and
    trusts the record only when it moved.

    One worker process serves exactly one model, so "the last call in this
    process" is that model's behaviour.
    """
    return _last_oom_retry


def looks_like_oom(exc: BaseException) -> bool:
    """Whether an exception is an out-of-memory condition by its *text*.

    The backstop for backends whose out-of-memory error is not a type we can
    name (docs/unified-memory-admission.md, "Negative signals"): MPS raises
    `RuntimeError("MPS backend out of memory (…)")` and CPU torch raises
    `RuntimeError("… DefaultCPUAllocator: can't allocate memory …")`, neither
    of which is a `torch.cuda.OutOfMemoryError` and only one of which even
    says "out of memory".

    Deliberately conservative: a generic exception is treated as an OOM only
    when one of these forms matches, so an assertion or a bad input still
    propagates untouched instead of being retried at half the batch size.

    **This is the retry backstop, not the calibration signal, and since
    run2's R3 the two are deliberately different classifiers.** The worker
    harness's `packing.classify_oom` decides whether the orchestrator is told
    a batch hit an out-of-memory condition, and a false positive there
    deflates a healthy model's batch size for minutes (run1 report §4,
    Q1/B11), so it refuses a bare `out of memory` substring and reports which
    tier matched. This function decides whether *we* retry the same work at
    half the size; a false positive costs one wasted attempt and a false
    negative costs the backstop entirely, so it stays deliberately broad. The
    two must **not** be kept in sync: they answer different questions, in
    opposite error directions. Anything this one recognises that
    `classify_oom` does not is, by construction, a batch we retried without
    telling the orchestrator it was a memory event — which is the safe half of
    the asymmetry.
    """
    for error in (exc, exc.__cause__, exc.__context__):
        if error is None:
            continue
        text = str(error)
        lowered = text.lower()
        if "out of memory" in lowered or "INFERENCE_OOM" in text:
            return True
        if "defaultcpuallocator" in lowered and "allocate memory" in lowered:
            return True
    return False


# Message fragments, lower-cased, of a kernel refusing a tensor because its
# element count does not fit the *index arithmetic* the kernel is compiled
# with — a hard shape ceiling that has nothing to do with how much memory the
# board has free.
#
# `integer out of range` is `at::native::safe_downcast<dest_t, src_t>`
# (`ATen/native/Pool.h`), which every pooling kernel runs over the figures it
# has to hand to a 32-bit launcher — including, on CUDA,
# `safe_downcast<int32_t, int64_t>(output.numel())` in
# `DilatedMaxPool2d.cu`'s `max_pool2d_with_indices` forward. That is the one
# run2's probes met: easyOCR's CRAFT detector at batch 29 of 2560-bounded
# pages (`run2-probes-report.md`, S1).
#
# `canuse32bitindexmath` is the same ceiling stated by the other family of
# kernels that assert on it rather than downcasting.
INDEX_LIMIT_MARKERS = ("integer out of range", "canuse32bitindexmath")

_total_index_limit_events = 0


def total_index_limit_events() -> int:
    """Kernel-index-ceiling events this process has seen, monotonically.

    An *event* is one batch that could not be executed at the size it was
    formed at because a kernel's 32-bit element index would have overflowed:
    either [`run_with_oom_retry`] halving on such a failure, or an impl
    capping a batch up front from its own ceiling formula
    ([`note_index_limit_event`]).

    Read by the worker's packing harness and by `ceiling_probe.py` the same
    way [`total_oom_halvings`] is — diffed across one `predict` call, through
    `sys.modules`, never imported. It is deliberately a **separate** counter:
    an index ceiling is size-dependent, so it halves like an out-of-memory
    condition, but it is not one, and reporting it as one would deflate a
    model that has plenty of memory (protocol doc, "Memory sensing").

    Monotonic and never reset, for the same reason `total_oom_halvings` is.
    """
    return _total_index_limit_events


def note_index_limit_event() -> None:
    """Record that a batch was shrunk by a kernel's index ceiling.

    Called by [`run_with_oom_retry`] when it halves on such a failure, and by
    an impl that computes the ceiling in advance and caps its own batch
    rather than letting the kernel raise. Both are the same fact for the
    orchestrator: this batch did not execute at the size it was formed at,
    for a reason that is not memory.
    """
    global _total_index_limit_events
    _total_index_limit_events += 1


def looks_like_index_limit(exc: BaseException) -> bool:
    """Whether an exception is a kernel's 32-bit index ceiling, by its text.

    Scans the exception and its `__cause__`/`__context__`, as
    [`looks_like_oom`] does, against [`INDEX_LIMIT_MARKERS`].

    Deliberately narrow where `looks_like_oom` is deliberately broad. Both
    decide "retry this at half the size", and both err in the direction that
    costs one wasted attempt rather than a lost backstop — but this one also
    decides that a failure is **not** a memory event, and a false positive
    there would hide a genuine out-of-memory condition from the
    orchestrator's deflation path. So it matches only strings torch emits for
    an index overflow and nothing that merely sounds like one, and
    [`run_with_oom_retry`] consults it only *after* every out-of-memory test
    has already declined the exception.
    """
    for error in (exc, exc.__cause__, exc.__context__):
        if error is None:
            continue
        lowered = str(error).lower()
        if any(marker in lowered for marker in INDEX_LIMIT_MARKERS):
            return True
    return False


def run_with_oom_retry(
    process_chunk,
    items,
    *,
    initial_chunk_size: int | None = None,
    oom_exceptions=None,
    logger: logging.Logger | None = None,
) -> list:
    """Run `process_chunk` over `items`, halving the chunk size on OOM.

    `process_chunk(chunk)` must return exactly len(chunk) results; results
    are concatenated in input order. On OOM the torch cache is cleared and
    the same position is retried at half the size — never re-grown within
    a call, since the dispatcher forms fresh full batches on the next
    request anyway. An OOM with a single item raises InferenceOOMError;
    any other exception propagates untouched.

    **What counts as an OOM** is three things, not one: the CUDA/HIP
    exception type (`torch.cuda.OutOfMemoryError`, or whatever
    `oom_exceptions` overrides it with), a plain `MemoryError` — host RAM
    exhaustion, which is a *builtin* and so no type torch could hand us —
    and any exception whose text [`looks_like_oom`] recognises, which is how
    MPS's and CPU torch's untyped `RuntimeError`s are caught. Before this
    widened, a too-big batch on any backend but CUDA/HIP was an uncaught
    error or a dead worker rather than a halved retry, on exactly the
    platforms where the halving is the only backstop there is
    (docs/unified-memory-admission.md).

    Two consequences of that widening are worth stating, because both are
    deliberate and neither is obvious:

    - a **nested** `InferenceOOMError` — an inner `run_with_oom_retry`
      giving up at a single item, inside an impl whose outer chunk is still
      several items — is now *absorbed* and the outer chunk retried at half
      size, where it used to propagate on the first hit. Its
      `INFERENCE_OOM` text is what [`looks_like_oom`] matches. This is the
      right direction (the outer loop has smaller batches left to try, and
      the prefix still reaches the orchestrator if the halving runs out),
      but it does mean the batch-1 error is no longer terminal for callers
      that nest;
    - the scan covers `__cause__` and `__context__` as well as the
      exception itself, so an OOM re-raised inside an `except` block is
      still recognised. That deliberately **errs toward** classifying as
      OOM: a chained non-memory error that merely happened during an OOM
      cleanup is retried at half size, costing one wasted attempt, which is
      cheaper than the alternative of a missed backstop on the backends
      that have no other one.

    **A fourth condition halves without being an OOM at all**: a kernel
    whose 32-bit element index cannot address the tensor the chunk builds
    ([`looks_like_index_limit`]). It is size-dependent, so the same halving
    is the right response; it is not a memory condition, so it increments
    [`total_index_limit_events`] and deliberately **not** the halving counter
    the worker reads as the `oom` flag — reporting it as an out-of-memory
    negative would deflate a model on a board with tens of GB free. At a
    single item it propagates untouched rather than becoming an
    `InferenceOOMError`, because a batch of one that overflows an index is
    not something a smaller batch can fix.

    Every call records what it actually executed for `last_oom_retry` — the
    largest chunk that ran and how many halvings it took to get there. The
    record is reset at entry so a failed or empty call cannot leave the
    previous one's numbers standing. Halvings additionally accumulate in the
    process-total `total_oom_halvings`, which is what survives a second call
    within the same `predict`.

    `oom_exceptions` overrides the caught types (used by torch-free tests).
    """
    global _oom_retry_generation, _last_oom_retry, _total_oom_halvings

    log = logger or logging.getLogger(__name__)
    _oom_retry_generation += 1
    generation = _oom_retry_generation
    largest = 0
    halvings = 0
    _last_oom_retry = (generation, largest, halvings)
    if oom_exceptions is None:
        import torch

        # Canonical spelling: torch.cuda.OutOfMemoryError; it is the same
        # class as torch.OutOfMemoryError in both shipped torch generations.
        oom_exceptions = (torch.cuda.OutOfMemoryError,)
    elif not isinstance(oom_exceptions, tuple):
        oom_exceptions = (oom_exceptions,)

    items = list(items)
    if not items:
        return []
    chunk_size = max(1, min(initial_chunk_size or len(items), len(items)))
    results: list = []
    pos = 0
    while pos < len(items):
        chunk = items[pos : pos + chunk_size]
        try:
            out = list(process_chunk(chunk))
        except Exception as err:
            # `MemoryError` is tested outside `oom_exceptions` on purpose: it
            # is a builtin rather than a torch type, it is the *only* form a
            # host-RAM exhaustion takes, and it is unambiguous — so it must
            # hold even where a caller narrowed the device exception type.
            if not (
                isinstance(err, oom_exceptions)
                or isinstance(err, MemoryError)
                or looks_like_oom(err)
            ):
                if not looks_like_index_limit(err):
                    # Not an out-of-memory condition and not a shape ceiling:
                    # an assertion, a bad input, a processor that rejected
                    # something. Halving would run it again at half the size
                    # and fail again, hiding the real error behind a retry
                    # loop.
                    raise
                # A kernel's 32-bit element index overflowed. Size-dependent,
                # so halving is exactly the right response — but it is *not*
                # a memory event, so it must not touch `halvings`, which is
                # what the harness reads as the `oom` flag. No `clear_cache()`
                # either: nothing here is short of memory, and emptying the
                # allocator would cost a synchronise for no reason.
                if len(chunk) == 1:
                    # One input alone exceeds the kernel's index range. There
                    # is no smaller batch to fall back to, and it is not an
                    # out-of-memory condition, so it must not be re-raised as
                    # `InferenceOOMError`: the caller's own fallback (or the
                    # orchestrator) has to see it for what it is.
                    raise
                chunk_size = max(1, len(chunk) // 2)
                note_index_limit_event()
                log.warning(
                    "a kernel's 32-bit element index overflowed on a chunk of "
                    "%d inputs; retrying at %d. This is a shape ceiling, not "
                    "an out-of-memory condition, and is not reported as one.",
                    len(chunk),
                    chunk_size,
                    exc_info=True,
                )
                continue
            clear_cache()
            if len(chunk) == 1:
                raise InferenceOOMError(
                    f"{OOM_BATCH1_PREFIX} out of GPU memory on a single "
                    f"input: {err}"
                ) from err
            chunk_size = max(1, len(chunk) // 2)
            halvings += 1
            _total_oom_halvings += 1
            _last_oom_retry = (generation, largest, halvings)
            log.warning(
                "GPU OOM on a chunk of %d inputs; retrying at %d.",
                len(chunk),
                chunk_size,
            )
            continue
        if len(out) != len(chunk):
            raise RuntimeError(
                f"process_chunk returned {len(out)} results for "
                f"{len(chunk)} inputs"
            )
        largest = max(largest, len(chunk))
        _last_oom_retry = (generation, largest, halvings)
        results.extend(out)
        pos += len(chunk)
    return results


def mcut_threshold(probs: np.ndarray) -> float:
    """
    Maximum Cut Thresholding (MCut)
    Largeron, C., Moulin, C., & Gery, M. (2012). MCut: A Thresholding Strategy
     for Multi-label Classification. In 11th International Symposium, IDA 2012
     (pp. 172-183).
    """
    sorted_probs = probs[probs.argsort()[::-1]]
    difs = sorted_probs[:-1] - sorted_probs[1:]
    t = difs.argmax()
    thresh = (sorted_probs[t] + sorted_probs[t + 1]) / 2
    return thresh


def pil_pad_square(image: Image.Image) -> Image.Image:
    w, h = image.size
    # get the largest dimension so we can pad to a square
    px = max(image.size)
    # pad to square with white background
    canvas = Image.new("RGB", (px, px), (255, 255, 255))
    canvas.paste(image, ((px - w) // 2, (px - h) // 2))
    return canvas


def pil_ensure_rgb(image: Image.Image) -> Image.Image:
    # convert to RGB/RGBA if not already (deals with palette images etc.)
    if image.mode not in ["RGB", "RGBA"]:
        image = (
            image.convert("RGBA")
            if "transparency" in image.info
            else image.convert("RGB")
        )
    # convert RGBA to RGB with white background
    if image.mode == "RGBA":
        canvas = Image.new("RGBA", image.size, (255, 255, 255))
        canvas.alpha_composite(image)
        image = canvas.convert("RGB")
    return image


def serialize_array(array: np.ndarray) -> bytes:
    buffer = io.BytesIO()
    np.save(buffer, array)
    buffer.seek(0)
    return buffer.read()


def deserialize_array(buffer: bytes) -> np.ndarray:
    bio = io.BytesIO(buffer)
    bio.seek(0)
    return np.load(bio, allow_pickle=False)



def extract_partial_json_array(json_str: str) -> Optional[List[str]]:
    """
    Attempts to extract a partial JSON array from a potentially truncated string.
    Returns the longest valid prefix of the array that can be parsed.
    """
    # Find the first opening bracket to start parsing
    start_idx = json_str.find('[')
    if start_idx == -1:
        return None  # No array found
    
    # Extract from first [ to end
    partial_str = json_str[start_idx:]
    
    # First try parsing the complete JSON
    try:
        return json.loads(partial_str)
    except json.JSONDecodeError:
        pass  # We'll handle this below
    
    # If we're here, the JSON is incomplete. We'll try to find the longest valid prefix.
    # We'll work backwards from the end, removing characters until we get valid JSON
    for end_idx in range(len(partial_str), start_idx + 1, -1):
        test_str = partial_str[:end_idx] + ']'  # Close the array
        try:
            result = json.loads(test_str)
            if isinstance(result, list):
                # Verify all elements are strings (as per your requirement)
                if all(isinstance(item, str) for item in result):
                    return result
        except (json.JSONDecodeError, TypeError):
            continue
    
    # Try one more approach - extract individual elements
    # This handles cases where the array is cut off in the middle of an element
    elements = []
    current_pos = start_idx + 1  # position after '['
    while current_pos < len(partial_str):
        # Try to parse from current position to end
        try:
            # Attempt to parse a complete JSON string
            end_of_str = current_pos
            while True:
                next_quote = partial_str.find('"', end_of_str)
                if next_quote == -1:
                    break  # No closing quote found
                
                # Check if this is an unescaped quote
                if partial_str[next_quote-1] != '\\':
                    # Try to parse from current_pos to next_quote+1
                    test_str = '[' + partial_str[current_pos:next_quote+1] + ']'
                    try:
                        element = json.loads(test_str)[0]
                        elements.append(element)
                        current_pos = next_quote + 2  # move past quote and comma/whitespace
                        break
                    except json.JSONDecodeError:
                        pass
                end_of_str = next_quote + 1
            else:
                break
        except (IndexError, json.JSONDecodeError):
            break
    
    return elements if elements else None

def clean_whitespace(input_string: str) -> str:
    # Replace three or more consecutive whitespaces with just two
    cleaned_string = re.sub(r"(\s)\1{2,}", r"\1\1", input_string)

    return cleaned_string


def print_resource_usage(logger: logging.Logger | None = None):
    """
    Logs process resource usage.
    - On Unix (Linux/macOS): tries to use `resource` for max RSS.
    - On all platforms: uses `psutil` (if available) for RSS, VMS, threads, and CPU.
    - Falls back to `print()` if no logger is given.
    """
    def log(msg):
        if logger is not None:
            logger.info(msg)
        else:
            print(msg)

    log(f"Resource usage for PID {os.getpid()}:")

    # Try using resource for max RSS on Unix platforms
    if sys.platform != 'win32':
        try:
            import resource
            maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            # On Mac, this is bytes; on Linux, it's kilobytes
            if sys.platform == "darwin":
                maxrss_mb = maxrss / (1024*1024)
                log(f"  [resource] Max Resident Set Size (Mac): {maxrss_mb:.2f} MB")
            else:
                maxrss_mb = maxrss / 1024
                log(f"  [resource] Max Resident Set Size (Linux): {maxrss_mb:.2f} MB")
        except Exception as e:
            log(f"  [resource] Unable to get max RSS via resource module: {e}")

    # Universal: try psutil for more detail
    try:
        import psutil
        proc = psutil.Process(os.getpid())
        rss = proc.memory_info().rss / (1024 ** 2)  # MB
        vms = proc.memory_info().vms / (1024 ** 2)  # MB
        threads = proc.num_threads()
        cpu = proc.cpu_percent(interval=0.1)
        log(f"  [psutil] Resident RAM (RSS):  {rss:.2f} MB")
        log(f"  [psutil] Virtual Memory (VMS): {vms:.2f} MB")
        log(f"  [psutil] Num Threads:          {threads}")
        log(f"  [psutil] CPU usage:            {cpu:.1f}%")
    except ImportError:
        log("  [psutil] psutil not installed. Install with `pip install psutil` for more details.")

# --------------------------------------------------------------------------
# Per-item error slots (docs/inferio-worker-protocol.md).
#
# An output slot may carry a typed failure instead of a payload, so one
# undecodable input no longer takes its healthy batch-mates down with it. Only
# a decode failure of *this input's own bytes* may become a slot: everything
# else (OOM, a broken model, a missing dependency) keeps failing the whole
# batch, which is what the orchestrator retries.
# --------------------------------------------------------------------------

ERROR_SLOT_KEY = "__error__"
ERROR_CLASS_INPUT = "input"


class ImageDecodeError(ValueError):
    """The image payload itself is undecodable by every enabled backend.

    A ValueError subclass, so callers that never caught it see no change; the
    distinct type is what lets the per-item seam tell a bad payload apart from
    a broken environment ("Pillow is not installed"), which must never be
    blamed on the item.
    """


def input_error_slot(message: str) -> dict:
    """One output slot reporting that this input's own payload was rejected."""
    return {
        ERROR_SLOT_KEY: {"class": ERROR_CLASS_INPUT, "message": str(message)}
    }


def load_image_or_slot(
    buf: bytes, *, logger: logging.Logger | None = None, **kwargs
) -> "tuple[PIL.Image.Image | None, dict | None]":
    """Decode one image payload, per item, before the batch is assembled.

    Returns `(image, None)` on success and `(None, slot)` when these bytes are
    undecodable. Only `ImageDecodeError` becomes a slot, and
    `load_image_from_buffer` raises it only for a genuine decode failure of
    this payload; everything else (MemoryError, a decompression-bomb ceiling,
    a broken cv2, a RecursionError) propagates and fails the whole batch,
    which the orchestrator retries.
    """
    try:
        return load_image_from_buffer(buf, **kwargs), None
    except ImageDecodeError as err:
        (logger or logging.getLogger(__name__)).warning(
            "Excluding an undecodable image input from the batch: %s", err
        )
        return None, input_error_slot(str(err))


def decode_image_inputs(
    inputs, *, what: str, logger: logging.Logger | None = None
) -> "tuple[list, list[int], list[tuple[int, dict]]]":
    """Decode the image payload of every input of an image-only model.

    Returns `(images, kept, slots)`: `images` are the decoded payloads of the
    inputs whose positions are listed in `kept` (both in input order), and
    `slots` pairs each excluded position with its error slot. An input with no
    file at all is a caller error, not an input error, and still raises —
    `what` names the model in that message, as before.
    """
    images: list = []
    kept: List[int] = []
    slots: List[tuple] = []
    for idx, input_item in enumerate(inputs):
        if not input_item.file:
            raise ValueError(f"{what} requires image inputs.")
        image, slot = load_image_or_slot(input_item.file, logger=logger)
        if slot is not None:
            slots.append((idx, slot))
            continue
        images.append(image)
        kept.append(idx)
    return images, kept, slots


def assemble_slots(
    total: int, kept: List[int], results: Sequence, slots: List[tuple]
) -> list:
    """Rebuild the full, input-ordered output list around the error slots.

    `kept` are the input positions the batch actually ran and `results` their
    outputs (same order and length); `slots` are the excluded positions. Every
    position must end up filled — a hole means the impl lost an output, which
    the protocol treats as a fatal count mismatch anyway, so it is raised here
    where the cause is still visible.
    """
    outputs: list = [None] * total
    if len(kept) != len(results):
        raise RuntimeError(
            f"predict produced {len(results)} results for {len(kept)} inputs"
        )
    for index, result in zip(kept, results):
        outputs[index] = result
    for index, slot in slots:
        outputs[index] = slot
    missing = [i for i, out in enumerate(outputs) if out is None]
    if missing:
        raise RuntimeError(f"predict produced no output for inputs {missing}")
    return outputs


def load_image_from_buffer(
    buf: bytes,
    *,
    accept_truncated: bool = True,
    try_fix_jpeg: bool = True,
    fallback_opencv: bool = True,
) -> "PIL.Image.Image":
    """
    Load an image from a raw byte buffer and return a Pillow Image in RGB
    mode, display-oriented (the EXIF orientation applied, as a browser would).

    Parameters
    ----------
    buf : bytes
        Raw image data.
    accept_truncated : bool, default True
        If True, sets PIL.ImageFile.LOAD_TRUNCATED_IMAGES so Pillow will
        return partially‑decoded images instead of raising OSError.
    try_fix_jpeg : bool, default True
        If True, appends the JPEG end‑of‑image marker 0xFF 0xD9 if it is missing.
    fallback_opencv : bool, default True
        If Pillow still cannot decode, fall back to OpenCV and convert the
        result back to Pillow. OpenCV is optional: when it is not installed
        the Pillow verdict simply stands.

    Raises
    ------
    ImageDecodeError
        If the image is unreadable by all enabled back‑ends (a ValueError
        subclass, so existing callers are unaffected; the per-item seam
        `load_image_or_slot` catches exactly this one). This is the only
        exception this function ever mints, and the only one that can become
        a persisted `input` verdict.
    Exception
        Anything that is *not* a decode failure of these bytes propagates
        untouched — a missing Pillow, a cv2 that is installed but fails to
        import, a MemoryError, a DecompressionBombError, a RecursionError.
        Those are facts about the machine or its configuration, never about
        the item, so they must fail the whole predict (which the orchestrator
        retries) instead of being recorded against the file forever.
    """
    # Only decoder-shaped failures may fall through to the next backend and
    # end as an `input` verdict. Pillow raises UnidentifiedImageError (an
    # OSError) for "not an image", OSError for a broken/truncated stream,
    # ValueError/SyntaxError/EOFError/struct.error from the individual format
    # plugins. Catching bare Exception here (as this did) quietly turned
    # MemoryError, DecompressionBombError and a failed `import cv2` into
    # "undecodable file", which the ledger then persists.
    decode_errors = (OSError, ValueError, SyntaxError, EOFError, struct.error)

    # ––––– 1.  Pillow first –––––
    try:
        from PIL import Image as PILImage
        from PIL import ImageFile, ImageOps

        if accept_truncated:
            ImageFile.LOAD_TRUNCATED_IMAGES = True

        raw = buf
        if try_fix_jpeg and not raw.endswith(b"\xFF\xD9"):  # add missing EOI
            raw += b"\xFF\xD9"

        with PILImage.open(BytesIO(raw)) as im:
            im.load()                   # force decoding now
            # The picture, not the pixels as stored: browsers, ffmpeg's
            # video decoder and the indexed dimensions all apply the file's
            # EXIF orientation (docs/display-dimensions-design.md §5), so
            # the models must see the same frame. This is the one decode
            # every image payload passes through — indexed files sent whole,
            # and query uploads for search-by-image, which keeps a query
            # embedding comparable with the index it searches. Re-encoded
            # payloads (video frames, slices, PDF pages) carry no EXIF, so
            # this is a no-op for them.
            #
            # A broken EXIF chunk is never a verdict on pixels that decoded
            # fine: a decoder-shaped failure here degrades to the
            # un-oriented image (what every consumer saw before orientation
            # was read at all) instead of falling through to the ledger as
            # "undecodable". Anything else propagates, per this function's
            # taxonomy.
            try:
                im = ImageOps.exif_transpose(im)
            except decode_errors:
                pass
            return im.convert("RGB")

    except (ModuleNotFoundError, ImportError):
        raise ValueError("Pillow is not installed") from None
    except PIL.Image.DecompressionBombError:
        # Deliberately NOT a decode failure: the image decoded fine, it is
        # merely larger than the configurable PIL.Image.MAX_IMAGE_PIXELS
        # ceiling. That is a machine/config limit, so raising the limit
        # changes the answer — exactly the kind of verdict that must never be
        # persisted against the file. (The softer DecompressionBombWarning is
        # a warning, not an exception, unless the deployment turns warnings
        # into errors; if it does, it arrives here as a Warning subclass and
        # likewise propagates.)
        raise
    except decode_errors as err:
        # A decode failure of these bytes: fall through to the optional
        # fallback backend, and to ImageDecodeError if that fails too.
        last_err: Optional[Exception] = err

    # ––––– 2.  OpenCV fallback –––––
    if fallback_opencv:
        # The import is outside the decode try/except on purpose. cv2 is an
        # optional dependency, so "not installed" only means the fallback is
        # unavailable and the Pillow verdict stands — but a cv2 that *is*
        # installed and fails to import (broken build, missing shared
        # library) is an environment problem and must propagate rather than
        # be folded into `last_err` and blamed on the payload.
        try:
            import cv2
        except ModuleNotFoundError:
            cv2 = None  # type: ignore[assignment]
    else:
        cv2 = None  # type: ignore[assignment]

    if cv2 is not None:
        try:
            # `imdecode` ignores EXIF orientation (only `imread` applies it),
            # so this fallback yields the un-oriented pixels. Deliberately
            # left that way: it only runs for a file Pillow could not decode
            # at all, where sideways pixels beat none.
            arr = np.frombuffer(buf, dtype=np.uint8)
            img_cv = cv2.imdecode(arr, cv2.IMREAD_UNCHANGED)
            if img_cv is None:
                raise ValueError("OpenCV could not decode image")
            # BGR ➜ RGB and back to Pillow
            img_cv = cv2.cvtColor(img_cv, cv2.COLOR_BGR2RGB)
            from PIL import Image as PILImage
            return PILImage.fromarray(img_cv)
        except decode_errors as err:
            last_err = err

    # ––––– 3.  Give up –––––
    raise ImageDecodeError(f"Unreadable image: {last_err}") from last_err
