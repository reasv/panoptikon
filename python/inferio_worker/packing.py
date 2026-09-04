"""Worker-side packing harness (batch-calibration step 1b).

The orchestrator decides *how much* memory a window may use; this module is
the mechanism that spends it. Given a `predict` request that carries a
**grant** (docs/inferio-worker-protocol.md, "Memory grants"), it:

1. prices every input in the model's declared cost dimension — the worker is
   the only side that can, because it is the only side with the decoded
   inputs (or at least their headers);
2. packs the window into GPU batches within the grant's unit budget,
   *bucketing* `max-times-count` models so one 8000×6000 scan does not tax 63
   thumbnails;
3. applies the **defensive clamp** before every batch — shrink-only, never
   above the grant — so a window granted seconds ago cannot run past a world
   that moved;
4. measures every batch (`memory.measure_batch`) and flags out-of-memory and
   throughput-collapse batches, which is what the orchestrator's deflation
   path consumes — and, since run2's R3, says *why* it called a failure an
   out-of-memory condition ([`classify_oom`]), so the orchestrator never has
   to re-derive that from a message it can only pattern-match;
5. restores the original input order before replying, since bucketed packing
   reorders items and the dispatcher splits outputs back by position.

Nothing here decides *safety*: the grant does. A mis-priced input only
mis-packs a batch, and `run_with_oom_retry` inside the impl stays the
backstop, untouched.

**No grant, no harness.** A request without one goes to a single
`instance.predict` call exactly as before this module existed — that is the
permanent compatibility path for `none`-class models, CPU/MPS hosts, hosts
with no GPU inventory, and any older orchestrator.

Import rules (docs/inferio-rust-orchestrator-design.md §4): stdlib only at
module level. PIL is imported lazily inside the pixel pricer, so a
text-only or CPU-only worker never pays for it.
"""

from __future__ import annotations

import logging
import sys
import time
from typing import Any, Iterable, Sequence

from inferio_worker import memory

logger = logging.getLogger("inferio_worker.packing")

# Prefix on the error message of a whole-batch (more than one item) OOM the
# impl's own halving loop did not absorb. The orchestrator classifies OOM
# from the message — an error frame carries no structured outcome — and
# `INFERENCE_OOM_BATCH_SIZE_1:` (from inferio.impl.utils) already covers the
# single-item case.
OOM_WINDOW_PREFIX = "INFERENCE_OOM_WINDOW:"

# The substring every one of our own out-of-memory markers contains
# (`INFERENCE_OOM_BATCH_SIZE_1:` from inferio.impl.utils, and
# [`OOM_WINDOW_PREFIX`] above). Case-sensitive on purpose: it is a token we
# emit, not prose a driver wrote.
OOM_MARKER = "INFERENCE_OOM"

# `oom_class.exception` when the classification came from the impl helper's
# halving counter rather than from an exception — a batch that *succeeded*
# after the impl absorbed an out-of-memory condition internally has no
# exception to name (docs/inferio-worker-protocol.md, "Memory sensing").
OOM_HALVING_WITNESS = "run_with_oom_retry"

# `oom_class.source` values, in descending order of how much the orchestrator
# can conclude from one. The wire vocabulary is fixed by the protocol doc.
OOM_SOURCE_TYPED = "typed_exception"
OOM_SOURCE_MARKER = "marker"
OOM_SOURCE_PATTERN = "message_pattern"

# Driver-shaped message fragments, lower-cased, for the fallback tier of the
# classifier. Every entry names an allocator, a driver or a CUDA/HIP API that
# only ever emits it for an allocation failure. A bare `out of memory` is
# deliberately absent: run1 measured it deflating a healthy model 15 times on
# a board with 96 GB free, from an impl that worded an unrelated failure as
# "out of memory slots" (batch-calibration run1 report §4, Q1/B11).
OOM_MESSAGE_PATTERNS = (
    "cuda out of memory",
    "cuda error: out of memory",
    "hip out of memory",
    "hip error: out of memory",
    "mps backend out of memory",
    "enforce fail at alloc_cpu.cpp",
    "cublas_status_alloc_failed",
    "cudnn_status_alloc_failed",
    "cudaerrormemoryallocation",
)

# Two-part patterns: both fragments must appear in the same message. CPU
# torch's classic allocator failure ("DefaultCPUAllocator: can't allocate
# memory: you tried to allocate …") is one string in practice, but the middle
# of it varies by torch version, and neither half alone is specific enough to
# match on.
OOM_MESSAGE_PAIRS = (("defaultcpuallocator", "allocate memory"),)

# Units/sec ratio below which a pool-growing batch is judged to have spilled
# to system RAM rather than run. On Windows' WDDM the driver's sysmem
# fallback (default on since ~536) lets an over-budget allocation succeed by
# spilling, so over-admission shows up as a silent throughput collapse and
# never as an exception. 0.4 is deliberately far below any plausible
# batch-size efficiency change: real batching curves flatten, they do not
# lose 60% of their throughput in one doubling.
COLLAPSE_RATIO = 0.4

# Units charged to a `pixel` input whose header cannot be read at all. It
# must never be zero (a free item would be packed without limit), and the
# largest input already seen in this window is a better local estimate than
# any constant, so the constant is only the first-input fallback: ~2 MP, the
# pixel unit class's own seed.
UNREADABLE_PIXEL_UNITS = 2_000_000

# Flat per-input allowance for `audio-second` pricing: reading a clip's real
# duration needs a decoder, and nothing in the shipped registry is priced
# this way yet (CLAP pads to a fixed window and is `item`/`count`).
AUDIO_FALLBACK_SECONDS = 30

# Bytes per token, matching the dispatcher's estimate. The worker *could*
# tokenize, but only through the impl's own tokenizer, which is not part of
# the `InferenceModel` contract — so this stays an estimate on both sides,
# which the design permits for tokens specifically.
BYTES_PER_TOKEN = 4

# Consecutive non-comparable batches (smaller, or not pool-growing, or
# unpriceable) after which the throughput comparator is discarded. A collapsed
# batch deliberately does not become the new comparator — that would make a
# spill the new normal — but a comparator kept forever would eventually be
# comparing against a rate the model no longer runs at (different input mix,
# a driver change, a trimmed pool), so it ages out instead.
COMPARATOR_MAX_AGE = 8

# The last *comparable* pool-growing batch, as `(units, units_per_sec)`, for
# the WDDM comparator. Pre-fit this is exactly "the previous ramp step", which
# is the comparison the design asks for: the ramp is the riskiest phase and is
# precisely the window a fitted-curve comparator could not cover. One model per
# worker process, so a module-level value is that model's history.
#
# Storing the units alongside the rate is what makes the comparison sound: a
# collapse claim is only meaningful for an **upward-or-equal step** in units. A
# window's small tail batch pays the same fixed per-call overhead over less
# work, so its units/sec is legitimately lower — flagging that as a memory
# spill would deflate a healthy worker once per window, forever.
_last_growth: "tuple[int, float] | None" = None

# How many consecutive non-comparable batches have been seen since
# `_last_growth` was set.
_non_comparable_streak = 0

# Reactive shrink (docs/batch-calibration-design.md, "Reactive shrink").
#
# Grants shrink as external usage rises, but freeing our tensors gives nothing
# back — the caching allocator holds the pool — so a worker whose grant has
# fallen well below the **releasable slack** it is sitting on is squeezing its
# neighbours for memory it is not using. `empty_cache()` between batches is the
# only way to return it.
#
# The comparison is against `reserved - allocated`, never `reserved` itself.
# `reserved` includes the model's weights, which no `empty_cache()` can hand
# back: comparing an *incremental* activation grant against the whole pool is
# comparing two different quantities, and on any calibrated model the grant is
# smaller essentially always — so the trigger would fire every other window,
# tear down a pool that had nothing spare in it, and (through `note_trimmed`)
# permanently reset the WDDM throughput comparator that depends on consecutive
# comparable batches. Against slack the rule is also self-limiting: after a
# release slack is ~0, so the very next window cannot re-trigger.
#
# Both constants are tunable ("Exact thresholds: implementation detail, tune
# empirically"). The ratio is the design's own example: a window needing less
# than 80% of the slack lying around is a material gap, not rounding. The
# window count is the hysteresis — one window below the line can be a transient
# (a small tail window, a momentary external spike), and paying a full pool
# teardown for it would cost more throughput than the freed memory buys anyone.
SHRINK_RATIO = 0.8
SHRINK_WINDOWS = 2

# Consecutive granted windows whose grant was below `SHRINK_RATIO` × the
# releasable slack. Module-level like the comparator: one model per worker
# process, so this is that model's history.
_under_grant_windows = 0


class WindowFailure(Exception):
    """A packed batch failed. Carries what ran before it.

    The window still fails as a whole (the protocol has no per-input error),
    so the dispatcher's per-request fallback and the caller's error handling
    are unchanged. What this adds is the measurements of the batches that did
    run — including the failing one, flagged — so the orchestrator can record
    the negative sample instead of inferring it.
    """

    def __init__(
        self,
        message: str,
        measurements: list[dict[str, Any]],
        cause: BaseException,
    ) -> None:
        super().__init__(message)
        self.measurements = measurements
        self.cause = cause


def reset_comparator() -> None:
    """Forget the cross-window throughput comparator.

    Called by the tests, and by both `empty_cache()` paths (the reactive shrink
    below and the orchestrator's `trim`): after an `empty_cache()` the pool
    regrows from nothing, so the next pool-growing batch's units/sec is not
    comparable to anything measured against a warm pool. Comparing across the
    event would flag a perfectly healthy regrowth batch as a WDDM memory spill
    and deflate the worker for it.
    """
    global _last_growth, _non_comparable_streak
    _last_growth = None
    _non_comparable_streak = 0


def reset_shrink_state() -> None:
    """Forget the reactive-shrink hysteresis. For tests, and after a trim.

    An orchestrator-initiated trim has already released the pool, so the
    consecutive-window count that was building towards doing it ourselves is
    stale evidence about a pool that no longer exists.
    """
    global _under_grant_windows
    _under_grant_windows = 0


def note_trimmed() -> None:
    """Everything a completed `empty_cache()` invalidates, in one place.

    Called by the worker's `trim` arm and by the reactive shrink below, so the
    two paths can never drift apart on what an `empty_cache()` means.
    """
    reset_comparator()
    reset_shrink_state()


def maybe_shrink(grant_mb: int | None) -> bool:
    """Release the pool when the grant is well below its **releasable slack**.

    Called once per granted window, **before its first batch** — which is
    "between batches" in the only sense the design's rule can mean for a
    worker: it is the one moment in a window's life when nothing is in flight,
    the grant for the work about to run is known, and the pool can be torn down
    without racing an allocation. Doing it mid-window instead would mean
    tearing down a pool the very next batch is about to rebuild.

    Slack is `memory_reserved() - memory_allocated()`: the blocks the caching
    allocator is holding that no live tensor sits in, which is exactly and only
    what an `empty_cache()` can give back. Comparing the grant against
    `memory_reserved()` instead would compare an incremental activation
    reservation against a total that includes the weights — a mismatch that is
    true nearly always and would fire the trigger on healthy workers (see the
    module constants above).

    Two hysteresis conditions: there must be slack at all, and the grant must
    be below [`SHRINK_RATIO`] of it for [`SHRINK_WINDOWS`] consecutive windows.
    A window that does not meet the second resets the count — recovery is
    immediate, because the whole point is to react to a world that moved and it
    can move back.

    Returns whether `empty_cache()` actually ran, which the caller reports as
    `trimmed` on the window's first measurement.
    """
    global _under_grant_windows
    if not grant_mb or grant_mb <= 0:
        # No MB reservation to compare against (a pre-1b orchestrator, or a
        # contention share that rounded to nothing). Not evidence of a squeeze.
        _under_grant_windows = 0
        return False
    reserved_mb, allocated_mb = memory.pool_stats_mb()
    if reserved_mb is None or allocated_mb is None:
        # No live CUDA of ours: nothing to measure and nothing to release.
        _under_grant_windows = 0
        return False
    slack_mb = max(0, reserved_mb - allocated_mb)
    if slack_mb <= 0:
        # The pool is fully occupied by live tensors. An `empty_cache()` would
        # return nothing at all, so a grant "below the pool" says nothing here.
        _under_grant_windows = 0
        return False
    if grant_mb >= SHRINK_RATIO * slack_mb:
        _under_grant_windows = 0
        return False
    _under_grant_windows += 1
    if _under_grant_windows < SHRINK_WINDOWS:
        logger.debug(
            "this window's %d MiB grant is below %.0f%% of the %d MiB of "
            "releasable slack in the %d MiB pool (%d/%d consecutive windows "
            "before releasing it)",
            grant_mb,
            SHRINK_RATIO * 100,
            slack_mb,
            reserved_mb,
            _under_grant_windows,
            SHRINK_WINDOWS,
        )
        return False
    if not memory.empty_cache():
        # Nothing of ours on the device after all; do not keep counting.
        _under_grant_windows = 0
        return False
    logger.info(
        "grant fell to %d MiB against %d MiB of releasable slack (a %d MiB "
        "allocator pool) for %d consecutive windows; released the pool "
        "(empty_cache) so the memory returns to the board",
        grant_mb,
        slack_mb,
        reserved_mb,
        _under_grant_windows,
    )
    # The pool regrows from here, which makes the next batches high-water
    # batches — fresh calibration material — and makes the previous window's
    # throughput an invalid comparator.
    note_trimmed()
    return True


# ---------------------------------------------------------------------------
# Pricing
# ---------------------------------------------------------------------------


def _image_source(value: Any) -> Any | None:
    """Something PIL can open, from a `PredictionInput.file`-shaped value.

    `file` is raw bytes in every shipped impl, but a path is a legal shape
    (and cheaper to price), so both are accepted.
    """
    if isinstance(value, (bytes, bytearray, memoryview)):
        import io

        return io.BytesIO(bytes(value))
    if isinstance(value, str) and value:
        return value
    path_like = getattr(value, "__fspath__", None)
    if path_like is not None:
        return value
    return None


def _pixels(value: Any) -> int | None:
    """Decoded pixel count from an image header, or None.

    `Image.open` is lazy — it parses the header and reads `size` without
    decoding pixel data, which is the whole point: pricing must not cost what
    the batch itself will cost.
    """
    source = _image_source(value)
    if source is None:
        return None
    try:
        from PIL import Image

        with Image.open(source) as image:
            width, height = image.size
    except Exception as exc:
        logger.debug("could not read image dimensions for pricing: %s", exc)
        return None
    if width <= 0 or height <= 0:
        return None
    return int(width) * int(height)


def _text_bytes(data: Any) -> int:
    if data is None:
        return 0
    if isinstance(data, str):
        return len(data.encode("utf-8", "ignore"))
    if isinstance(data, (bytes, bytearray, memoryview)):
        return len(bytes(data))
    return len(repr(data))


def price_inputs(inputs: Sequence[Any], unit: str) -> list[int]:
    """Per-input units in the model's cost dimension. Never zero, never raises.

    An unreadable `pixel` input is charged the largest input seen so far in
    this window (falling back to [`UNREADABLE_PIXEL_UNITS`]) rather than
    failing the window: one corrupt file must not cost 63 good ones their
    batch, and over-charging it only makes its batch smaller.
    """
    units: list[int] = []
    largest = 0
    if unit == "pixel":
        for entry in inputs:
            priced = _pixels(getattr(entry, "file", None))
            units.append(priced if priced is not None else 0)
            if priced:
                largest = max(largest, priced)
        fallback = largest or UNREADABLE_PIXEL_UNITS
        return [priced or fallback for priced in units]
    if unit == "token":
        for entry in inputs:
            total = _text_bytes(getattr(entry, "file", None)) + _text_bytes(
                getattr(entry, "data", None)
            )
            units.append(max(1, total // BYTES_PER_TOKEN))
        return units
    if unit == "audio-second":
        return [AUDIO_FALLBACK_SECONDS for _ in inputs]
    # `item` and anything unrecognised: one unit each. An unknown unit string
    # from a newer orchestrator degrades to per-item packing, which is worse
    # packing and never a crash.
    return [1 for _ in inputs]


def batch_units(indices: Iterable[int], units: Sequence[int], aggregation: str) -> int:
    """What a batch of these indices costs, per the model's aggregation."""
    picked = [units[index] for index in indices]
    if not picked:
        return 0
    if aggregation == "sum":
        return sum(picked)
    if aggregation == "max-times-count":
        return max(picked) * len(picked)
    # `count`: one unit per item, whatever the per-item pricing says.
    return len(picked)


# ---------------------------------------------------------------------------
# Packing
# ---------------------------------------------------------------------------


def plan_batches(
    units: Sequence[int],
    aggregation: str,
    unit_budget: int,
    cap_items: int | None = None,
) -> list[list[int]]:
    """Split input indices into GPU batches within `unit_budget`.

    - `count` — the budget *is* an item count;
    - `sum` — greedy running total in FIFO order;
    - `max-times-count` — **bucketed**: indices are visited largest-first, so
      each batch's price is set by its first (largest) member and its count
      grows until `max × count` would exceed the budget. That is what packs
      similarly-sized neighbours together and what finally retires easyOCR's
      `enable_batching = false` stopgap. Safety never depends on it —
      `max × count` prices a mixed batch conservatively either way — it is
      purely the throughput win.

    A batch is never smaller than one item: a single item over budget goes
    through alone and the impl's OOM backstop catches it if it truly cannot
    run (Package 1 already decided batch-1 OOM = item fails, job continues).
    `cap_items` is an additional item-count bound, never converted to units.
    """
    budget = max(1, int(unit_budget))
    cap = cap_items if cap_items and cap_items > 0 else None
    order = list(range(len(units)))
    if aggregation == "max-times-count":
        # Stable descending sort: the largest item sets each batch's price.
        order.sort(key=lambda index: units[index], reverse=True)

    batches: list[list[int]] = []
    current: list[int] = []
    for index in order:
        if not current:
            current = [index]
            if cap == 1:
                batches.append(current)
                current = []
            continue
        if cap is not None and len(current) >= cap:
            batches.append(current)
            current = [index]
            continue
        if batch_units(current + [index], units, aggregation) > budget:
            batches.append(current)
            current = [index]
            continue
        current.append(index)
    if current:
        batches.append(current)
    return batches


def clamp_to_live_memory(unit_budget: int, grant_mb: int | None) -> int:
    """Shrink the budget if free memory has fallen below what it assumed.

    Shrink-only, and never above the grant. The rule compares live free
    memory against the grant's own MB reservation, which works pre-fit as
    well as post-fit: pre-fit the grant has no slope to convert units with,
    but its MB figure is still the contention share it was carved from, so
    the *ratio* is the honest scaling factor either way.

    Returns `unit_budget` unchanged when nothing can be read (no CUDA, no
    NVML, no torch) — the orchestrator's own staleness refresh and the impl's
    OOM backstop cover that case.
    """
    if not grant_mb or grant_mb <= 0:
        return unit_budget
    free_mb, _, _ = memory.free_total_mb()
    if free_mb is None or free_mb >= grant_mb:
        return unit_budget
    shrunk = max(1, int(unit_budget * free_mb / grant_mb))
    if shrunk < unit_budget:
        logger.info(
            "free memory fell to %d MiB against a %d MiB grant; shrinking this "
            "batch's budget from %d to %d units",
            free_mb,
            grant_mb,
            unit_budget,
            shrunk,
        )
    return shrunk


# ---------------------------------------------------------------------------
# Running a window
# ---------------------------------------------------------------------------


def _qualified_name(cls: type) -> str:
    """`"torch.OutOfMemoryError"` for a library type, `"MemoryError"` for a
    builtin. The name the orchestrator sees in `oom_class.exception`, and the
    reason it is qualified: `OutOfMemoryError` alone is a name three
    libraries could plausibly have used."""
    module = getattr(cls, "__module__", "") or ""
    # `__name__`, not `__qualname__`: every type this can name is defined at
    # module level, and a qualname would only add an enclosing-scope prefix
    # for something defined inside a function — noise on a wire field a human
    # reads out of a log.
    name = getattr(cls, "__name__", None) or repr(cls)
    if not module or module in ("builtins", "__main__"):
        return name
    return f"{module}.{name}"


def _typed_oom(error: BaseException) -> str | None:
    """The exception's name when it is an allocator failure by **type**.

    Two types, and only two. `torch.OutOfMemoryError` is the one CUDA raises
    and — the same class object — the one a HIP build raises, so ROCm needs no
    entry of its own; it is looked up through `sys.modules` rather than
    imported, because this module must never import torch (module docstring),
    and an exception that *is* one guarantees torch is already there. The
    interpreter's own `MemoryError` is host-RAM exhaustion, a builtin no
    library could hand us, and the only form the CPU allocator's hard failure
    takes.

    The name-on-the-MRO fallback exists for the case where torch is absent
    from `sys.modules` but an `OutOfMemoryError` still arrives (a fake in a
    test, an exception rebuilt across a process boundary). It is a *type*
    test, not a message test: nothing about it depends on what the exception
    says.
    """
    if isinstance(error, MemoryError):
        return _qualified_name(type(error))
    torch = sys.modules.get("torch")
    oom_type = getattr(torch, "OutOfMemoryError", None) if torch is not None else None
    if isinstance(oom_type, type) and isinstance(error, oom_type):
        return _qualified_name(type(error))
    for cls in type(error).__mro__:
        if cls.__name__ == "OutOfMemoryError":
            return _qualified_name(type(error))
    return None


def _marker_oom(error: BaseException) -> str | None:
    """The exception's name when it carries one of *our own* OOM markers.

    `inferio.impl.utils.run_with_oom_retry` classifies a device failure one
    frame below the impl and re-raises it as `InferenceOOMError`, whose text
    starts `INFERENCE_OOM_BATCH_SIZE_1:`; the harness's own whole-window
    wrapper uses `INFERENCE_OOM_WINDOW:`. Both are our code stating a
    classification it already made from a typed exception, so they are
    structural evidence and not a message pattern — the string is a marker we
    emit, not a driver's prose. Matched by type name as well as by text, so a
    marker whose message an impl reworded is still recognised.
    """
    if type(error).__name__ == "InferenceOOMError":
        return _qualified_name(type(error))
    if OOM_MARKER in str(error):
        return _qualified_name(type(error))
    return None


def _pattern_oom(error: BaseException) -> str | None:
    """The exception's name when its text is **driver-shaped**.

    The last resort, and the only tier that reads prose. Every entry names an
    allocator or a driver explicitly, because run1 measured what happens when
    the test is looser: a bare `out of memory` substring deflated a healthy
    model 15 times on a board with 96 GB free, purely because an impl worded
    an unrelated failure as "out of memory slots" (report §4, Q1/B11). That
    substring is deliberately **not** here.

    MPS is why the tier exists at all: an MPS allocation failure is a plain
    `RuntimeError("MPS backend out of memory (…)")` and there is no typed
    form of it to catch (docs/unified-memory-admission.md, "Negative
    signals"). CPU torch's `DefaultCPUAllocator` message is the same shape —
    it does not even contain the words "out of memory" — and the
    `alloc_cpu.cpp` enforce-fail is the newer spelling of it. The CUBLAS,
    CUDNN and cudaError entries are allocation failures the driver reports
    without ever saying "out of memory".

    Extending this tuple is how a new backend's spelling is learned; widening
    it into a generic substring is how a healthy model gets deflated.
    """
    lowered = str(error).lower()
    for pattern in OOM_MESSAGE_PATTERNS:
        if pattern in lowered:
            return _qualified_name(type(error))
    for first, second in OOM_MESSAGE_PAIRS:
        if first in lowered and second in lowered:
            return _qualified_name(type(error))
    return None


def _chain(exc: BaseException | None) -> tuple[BaseException, ...]:
    """The exception and the two links Python attaches to it.

    `__cause__` and `__context__` are scanned because an out-of-memory error
    re-raised inside an `except` block — which is exactly what
    `run_with_oom_retry` does — would otherwise be invisible.
    """
    return tuple(
        error
        for error in (exc, getattr(exc, "__cause__", None), getattr(exc, "__context__", None))
        if error is not None
    )


def classify_oom(
    exc: BaseException | None, absorbed: int = 0
) -> dict[str, Any] | None:
    """`oom_class` for a batch, or `None` when nothing says out of memory.

    The structural signal run2's R3 asks for
    (docs/inferio-worker-protocol.md, "Memory sensing"): the worker decides
    *and says how it decided*, so the orchestrator can act on a typed
    exception outright and corroborate a textual one against
    `free_mb_at_failure` before it deflates anything.

    The three tiers are tried over the whole exception chain in strength
    order, not in chain order, so the strongest available evidence wins
    wherever it sits: an `InferenceOOMError` raised `from` a
    `torch.OutOfMemoryError` classifies as `typed_exception`, because that is
    what it actually is. A batch that *succeeded* while the impl's halving
    loop absorbed an out-of-memory condition has no exception at all and is
    classified from the halving counter (`absorbed`), as a `marker`.

    Returning `None` is a positive statement, not an absence of information:
    this batch's failure was **not** an out-of-memory condition, and the
    orchestrator must not deflate on it.

    Never raises: a classifier that threw would turn a failed batch into a
    dead worker.
    """
    try:
        chain = _chain(exc)
        found: tuple[str, str] | None = None
        for source, probe in (
            (OOM_SOURCE_TYPED, _typed_oom),
            (OOM_SOURCE_MARKER, _marker_oom),
            (OOM_SOURCE_PATTERN, _pattern_oom),
        ):
            for error in chain:
                name = probe(error)
                if name is not None:
                    found = (source, name)
                    break
            if found is not None:
                break
        if found is None and absorbed > 0:
            # No exception to name: the batch ran, and the only witness is the
            # impl helper's halving counter. Naming the helper rather than
            # inventing an exception keeps `exception` honest about what was
            # actually observed.
            found = (OOM_SOURCE_MARKER, OOM_HALVING_WITNESS)
        if found is None:
            return None
        # One live reading, taken now, on the failure path only. It is the
        # corroboration a `message_pattern` verdict needs: an out-of-memory
        # claim made while the board has tens of GB free is a wording, not a
        # condition.
        free_mb, _, _ = memory.free_total_mb()
        return {
            "source": found[0],
            "exception": found[1],
            "free_mb_at_failure": free_mb,
            "device": memory.device_label(),
        }
    except Exception as exc_inner:  # pragma: no cover - defensive
        logger.debug("out-of-memory classification failed: %s", exc_inner)
        return None


def batching_disabled(instance: Any) -> bool:
    """Whether this impl has switched its own GPU batching off.

    `enable_batching` / `enable_batch` are the shipped registry's knobs for
    "run one item at a time inside `predict`" (easyOCR's OOM stopgap is the
    live example). An impl in that mode decides its own batch shape regardless
    of what it is handed, so a grant would buy nothing and the reported `units`
    would describe a batch the allocator never saw. Such a worker takes the
    grantless compatibility path and is `none`-class for calibration until
    batching is re-enabled (docs/batch-calibration-design.md, "Measurement").

    Only an attribute that is *present and falsy* disables: an impl that never
    heard of the knob is batched normally.
    """
    for attribute in ("enable_batching", "enable_batch"):
        if not hasattr(instance, attribute):
            continue
        try:
            if not getattr(instance, attribute):
                return True
        except Exception:  # pragma: no cover - a property that raises
            continue
    return False


def _oom_retry_record() -> tuple[int, int, int] | None:
    """`inferio.impl.utils.last_oom_retry()`, or None.

    Read through `sys.modules` — the harness must never import the `inferio`
    package (module docstring), it only observes it when the impl already
    brought it. An impl that does not route through `run_with_oom_retry` at
    all leaves this at None, which is "no information", not "ran the whole
    batch".
    """
    utils = sys.modules.get("inferio.impl.utils")
    reader = getattr(utils, "last_oom_retry", None) if utils is not None else None
    if reader is None:
        return None
    try:
        record = reader()
    except Exception:  # pragma: no cover - defensive
        return None
    if not isinstance(record, tuple) or len(record) != 3:
        return None
    try:
        return (int(record[0]), int(record[1]), int(record[2]))
    except Exception:  # pragma: no cover - defensive
        return None


def _oom_halvings_total() -> int:
    """`inferio.impl.utils.total_oom_halvings()`, or 0 when unavailable.

    Diffed across the whole `predict` call: an impl that calls
    `run_with_oom_retry` twice per `predict` (clip, nemotron-embed-vl) leaves
    only the *last* call's halvings in the per-call record, so an out-of-memory
    condition absorbed by the first would otherwise be lost entirely.
    """
    utils = sys.modules.get("inferio.impl.utils")
    reader = getattr(utils, "total_oom_halvings", None) if utils is not None else None
    if reader is None:
        return 0
    try:
        return int(reader())
    except Exception:  # pragma: no cover - defensive
        return 0


def _executed_shape(
    before: tuple[int, int, int] | None, planned: int
) -> tuple[int | None, int]:
    """`(largest_chunk_executed, halvings_performed)` for the batch just run.

    `largest_chunk_executed` is None when nothing is known — the impl does not
    route through `run_with_oom_retry`, or it did not call it for this batch.
    The generation counter is what distinguishes "did not call it" from "called
    it and got the same numbers as last time"; without it a stale record would
    be read as a fresh one and an internally-halved batch could be priced as
    if it ran whole.
    """
    after = _oom_retry_record()
    if after is None:
        return (None, 0)
    if before is not None and after[0] == before[0]:
        # The impl did not consult the retry helper for this batch.
        return (None, 0)
    _, largest, halvings = after
    if largest <= 0:
        # The record *moved* for this batch and still says nothing ran through
        # the helper: the impl consulted it, executed zero items there, and did
        # the work by another route (easyOCR's `readtext` fallback, which calls
        # the helper with an empty list and then loops per image). That is
        # "executed nothing here", NOT "ran the whole batch" — reporting 0 of
        # `planned` makes the batch unpriceable, which is the honest answer.
        return (0, halvings)
    return (min(largest, planned), halvings)


def _batch_shape(
    before: tuple[int, int, int] | None, planned: int, halvings_before: int
) -> tuple[int | None, int]:
    """`(largest_chunk_executed, absorbed_ooms)` for the batch just run.

    The absorbed-OOM count comes from the **process total** diffed across the
    whole `predict` call, so a halving in any of several helper calls counts;
    the per-call record is the fallback for a process where the total counter is
    not available (an older `inferio.impl.utils`).
    """
    executed, halvings = _executed_shape(before, planned)
    across_call = max(_oom_halvings_total() - halvings_before, 0)
    return (executed, max(across_call, halvings))


def _note_throughput(
    measurement: dict[str, Any],
    priced: int | None,
    elapsed: float,
    items: int,
    unit: str,
) -> None:
    """Apply the WDDM synthetic-negative rule to one measurement, in place.

    On Windows the driver's sysmem fallback lets an over-budget allocation
    succeed by spilling to system RAM, so over-admission shows up as a silent
    throughput collapse rather than an exception. The signal is only sound
    between two **pool-growing** batches (a warm-pool repeat says nothing about
    admission) where the second is an **upward-or-equal step** in units (a
    smaller batch amortizes fixed per-call overhead over less work and is
    legitimately slower per unit). Anything else is non-comparable, and a run of
    [`COMPARATOR_MAX_AGE`] non-comparable batches retires the comparator rather
    than letting a stale rate flag forever.
    """
    global _last_growth, _non_comparable_streak

    grew = (measurement.get("peak_reserved_mb") or 0) > (
        measurement.get("reserved_before_mb") or 0
    )
    rate = (priced / elapsed) if (priced and elapsed > 0) else None
    previous = _last_growth
    comparable = (
        grew
        and rate is not None
        and priced is not None
        and (previous is None or priced >= previous[0])
    )
    if not comparable:
        if previous is not None:
            _non_comparable_streak += 1
            if _non_comparable_streak >= COMPARATOR_MAX_AGE:
                logger.debug(
                    "retiring the throughput comparator after %d non-comparable "
                    "batches",
                    _non_comparable_streak,
                )
                reset_comparator()
        return
    _non_comparable_streak = 0
    if previous is not None and rate < COLLAPSE_RATIO * previous[1]:
        measurement["throughput_collapse"] = True
        logger.warning(
            "batch of %d inputs (%d %s units) ran at %.0f units/sec against "
            "%.0f for the previous growing batch of %d units; treating it as a "
            "memory spill (driver sysmem fallback)",
            items,
            priced,
            unit,
            rate,
            previous[1],
            previous[0],
        )
        # A collapsed batch is NOT the new comparator: letting a spill set the
        # bar would make it the new normal and hide a persistent one.
        return
    _last_growth = (priced, rate)


def run_window(instance: Any, inputs: Sequence[Any], grant: dict[str, Any]) -> dict[str, Any]:
    """Run one granted window and build the `predict` `ok` payload.

    Returns `{"outputs": [...], "measurements": [...], "memory": {...}}` with
    outputs in the original input order. Raises [`WindowFailure`] when a batch
    fails, carrying the measurements of everything that ran.
    """
    unit = str(grant.get("unit") or "item")
    aggregation = str(grant.get("aggregation") or "count")
    budget = grant.get("unit_budget")
    budget = max(1, int(budget)) if isinstance(budget, int) else 1
    grant_mb = grant.get("mb")
    grant_mb = int(grant_mb) if isinstance(grant_mb, int) else None
    cap_items = grant.get("user_cap_items")
    cap_items = int(cap_items) if isinstance(cap_items, int) else None

    # Reactive shrink, before anything else in the window: the one point where
    # no batch is in flight and the grant for the work about to run is known.
    trimmed = maybe_shrink(grant_mb)

    # Pricing happens once, up front, OUTSIDE every timed section.
    units = price_inputs(inputs, unit)

    outputs: list[Any] = [None] * len(inputs)
    measurements: list[dict[str, Any]] = []
    pending = list(range(len(inputs)))

    def record(measurement: dict[str, Any]) -> dict[str, Any]:
        """Append a measurement, stamping the window's first one if the pool
        was released before it (protocol doc, `trimmed`). It rides the first
        measurement whatever that turns out to be — including a failed batch,
        which is exactly when knowing the pool had just been torn down matters.
        """
        if trimmed and not measurements:
            measurement["trimmed"] = True
        measurements.append(measurement)
        return measurement

    while pending:
        # Re-plan per batch: the defensive clamp can shrink the budget
        # mid-window, and the plan for the remaining items must honour that.
        effective = clamp_to_live_memory(budget, grant_mb)
        remaining_units = [units[index] for index in pending]
        plan = plan_batches(remaining_units, aggregation, effective, cap_items)
        batch = [pending[position] for position in plan[0]]
        priced = batch_units(batch, units, aggregation)

        state = memory.begin_batch()
        retry_before = _oom_retry_record()
        halvings_before = _oom_halvings_total()
        started = time.perf_counter()
        try:
            produced = list(instance.predict([inputs[index] for index in batch]))
        except Exception as exc:
            # A failed batch is NEVER priceable, whatever it failed of. Its
            # peaks describe however far the call got before it gave up, which
            # understates the cost of the batch we packed — and a non-OOM
            # mid-batch failure (an assertion, a processor rejecting an input)
            # would otherwise enter the fit as a clean high-water sample with an
            # under-stated peak, dragging the slope low: over-admission produced
            # by a failure. The `oom` flag still rides the measurement, and that
            # is what deflation consumes.
            _, absorbed = _batch_shape(retry_before, len(batch), halvings_before)
            oom_class = classify_oom(exc, absorbed)
            oom = oom_class is not None
            if not oom:
                logger.debug(
                    "a batch of %d inputs failed with %s, which is not an "
                    "out-of-memory condition; reporting it without the oom flag",
                    len(batch),
                    type(exc).__name__,
                )
            record(
                memory.measure_batch(
                    state,
                    items=len(batch),
                    oom=oom,
                    oom_class=oom_class,
                )
            )
            message = str(exc)
            if oom and len(batch) > 1 and OOM_WINDOW_PREFIX not in message:
                # Give the orchestrator the whole-window OOM signal; the
                # batch-1 case already carries its own prefix from
                # inferio.impl.utils.
                message = (
                    f"{OOM_WINDOW_PREFIX} out of GPU memory on a packed batch "
                    f"of {len(batch)} inputs ({priced} {unit} units): {exc}"
                )
            raise WindowFailure(message, measurements, exc) from exc
        elapsed = time.perf_counter() - started
        if len(produced) != len(batch):
            exc = RuntimeError(
                f"impl predict returned {len(produced)} outputs for a batch of "
                f"{len(batch)} inputs"
            )
            # Unpriced for the same reason as every other failure path: the
            # batch did not complete, so its peaks under-state its cost.
            record(memory.measure_batch(state, items=len(batch)))
            raise WindowFailure(str(exc), measurements, exc) from exc

        # Did the impl run the batch it was handed? Several shipped impls
        # sub-batch inside `predict`, and a `units` figure that describes more
        # work than the measured peaks biases the fitted slope low, which is
        # over-admission (protocol doc, "Memory sensing").
        executed, absorbed_ooms = _batch_shape(
            retry_before, len(batch), halvings_before
        )
        priceable = executed is None or executed >= len(batch)
        if not priceable:
            logger.debug(
                "the impl executed at most %d of the %d inputs in this GPU "
                "batch per call; reporting the batch unpriced",
                executed,
                len(batch),
            )
        measurement = memory.measure_batch(
            state,
            items=len(batch),
            units=priced if priceable else None,
            oom=absorbed_ooms > 0,
            oom_class=classify_oom(None, absorbed_ooms) if absorbed_ooms else None,
        )
        if absorbed_ooms:
            logger.warning(
                "the impl's own halving loop absorbed %d out-of-memory "
                "condition(s) inside a batch of %d inputs; reporting it as a "
                "negative sample",
                absorbed_ooms,
                len(batch),
            )
        _note_throughput(measurement, priced if priceable else None, elapsed, len(batch), unit)
        record(measurement)

        # Restore input order: bucketed packing reorders items, and the
        # dispatcher splits outputs back per request by position.
        for index, output in zip(batch, produced):
            outputs[index] = output
        remaining = set(pending) - set(batch)
        pending = [index for index in pending if index in remaining]

    payload: dict[str, Any] = {"outputs": outputs, "measurements": measurements}
    sample = memory.device_memory_sample()
    if sample is not None:
        payload["memory"] = sample
    return payload
