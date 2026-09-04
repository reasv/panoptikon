"""Worker-side packing harness (batch-calibration step 1b).

The orchestrator decides *how much* memory a window may use; this module is
the mechanism that spends it. Given a `predict` request that carries a
**grant** (docs/inferio-worker-protocol.md, "Memory grants"), it:

1. prices every input in the model's declared cost dimension — the worker is
   the only side that can, because it is the only side with the decoded
   inputs (or at least their headers);
2. packs the window into GPU batches within the grant's unit budget,
   *bucketing* `max-times-count` models so one 8000×6000 scan does not tax 63
   thumbnails — and, since run2's D1-b, breaking ties among equally-priced
   items by their **raw** size, because the R7 canvas cap deliberately prices
   every item at or above the canvas alike and that is the information the
   bucketing was sorting on;
3. applies the **defensive clamp** before every batch — shrink-only, never
   above the grant — so a window granted seconds ago cannot run past a world
   that moved, and, since run2's S1, asks the impl for its **shape ceiling**
   ([`MAX_BATCH_ATTR`]) as well, because a kernel that cannot index the
   tensor a batch builds refuses it with any amount of memory free;
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
import re
import sys
import time
from typing import Any, Iterable, NamedTuple, Sequence

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
# only ever emits it for an allocation failure — and none of them contains the
# words "out of memory", which is why each has to be spelled out. A bare
# `out of memory` is deliberately absent: run1 measured it deflating a healthy
# model 15 times on a board with 96 GB free, from an impl that worded an
# unrelated failure as "out of memory slots" (run1 report §4, Q1/B11).
OOM_MESSAGE_PATTERNS = (
    "mps backend out of memory",
    "enforce fail at alloc_cpu.cpp",
    "cublas_status_alloc_failed",
    "cudnn_status_alloc_failed",
    "cusolver_status_alloc_failed",
    "cusparse_status_alloc_failed",
    "cufft_alloc_failed",
    "cudaerrormemoryallocation",
    "hiperroroutofmemory",
    "hiperrormemoryallocation",
)

# Two-part patterns: both fragments must appear in the same message. CPU
# torch's classic allocator failure ("DefaultCPUAllocator: can't allocate
# memory: you tried to allocate …") is one string in practice, but the middle
# of it varies by torch version, and neither half alone is specific enough to
# match on.
OOM_MESSAGE_PAIRS = (("defaultcpuallocator", "allocate memory"),)

# The device-scoped form of "out of memory": the words, **and** a device-API
# token as a whole word in the same message. B11 is the reason the words alone
# are not enough; this rule is the reason they are not thrown away either.
# Every library that allocates on an accelerator words its allocation failure
# differently around those three words, and enumerating the spellings loses
# real out-of-memory conditions to a wording nobody predicted — all of these
# exist today and none matches a fixed substring list:
#
#   torch      "CUDA out of memory. Tried to allocate 2.00 GiB"
#   torch      "CUDA error: out of memory"
#   torch      "CUDA driver error: out of memory"   (the expandable-segments
#              path allocates through the driver API, not the runtime)
#   torch <2.0 "cuda runtime error (2) : out of memory"
#   CTranslate2 "CUDA failed with error out of memory"  (faster-whisper, a
#              shipped dependency whose VRAM never passes through torch)
#   ROCm       the same set with HIP in place of CUDA
#
# The token is matched on a word boundary, so "chip"/"ship"/"relationship"
# cannot stand in for "hip", and it must be a device API: a message about a
# *host* allocator saying "out of memory" is left to `MemoryError` and to the
# CPU-allocator patterns above. B11's wording ("the caption cache is out of
# memory slots") names no device and is still refused.
OOM_DEVICE_TOKENS = re.compile(r"\b(cuda|hip|rocm|nvml|xpu|sycl)\b")
OOM_DEVICE_PHRASE = "out of memory"

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

# Attribute names that hold a model's per-item pixel canvas, and the
# attributes that hold the object holding it. Read passively off an already
# constructed instance: the worker never builds anything and never imports
# anything to ask (docs/inferio-worker-protocol.md, "Memory grants").
CANVAS_ATTRS = ("canvas_pixels", "max_pixels", "image_max_pixels")
CANVAS_HOLDERS = ("processor", "image_processor", "embedder", "model")

# How deep the canvas hunt goes through [`CANVAS_HOLDERS`]. Two, because the
# shipped shapes need exactly two: `instance.embedder.max_pixels`
# (qwen3-vl-embedding) is one, `instance.model.processor.*` (nemotron,
# dots.ocr) is two. Bounded because this walks an object graph nobody here
# controls.
CANVAS_WALK_DEPTH = 2

# Smallest number a canvas reading is believed at: 512x512. Every shipped
# `pixel` model's real canvas is well above it (1.8 MP for the VLMs, 6.6 MP
# for easyOCR's detector), and a *too small* cap is the one direction that
# hurts — it under-prices an item, which over-admits — so an attribute that
# happens to be named `max_pixels` and holds something else is refused rather
# than trusted.
CANVAS_FLOOR_PIXELS = 512 * 512

# The attribute an impl sets to say it builds **one batch tensor at the
# dimensions of the batch's largest member** — `inferio.impl.eocr`'s
# `pad_images_to_same_size` is the shipped example. Such an impl's cost is a
# function of that largest member, not of the batch's mean, so a batch that
# mixes sizes costs the maximum for every item in it.
PADS_TO_COMMON_SIZE_ATTR = "pads_to_common_size"

# Raw-area ratio within one batch above which that pairing is worth a line in
# the log — but only for an impl that pads to a common size *and* states no
# canvas of its own. Run2 D1-b: under the R7 cap every item at or above the
# canvas prices identically, so the priced units carry no size information for
# the bucketing to sort on, and an impl that then pads to raw dimensions
# builds a tensor several times larger than the batch was charged for. An
# impl that exposes its own canvas has, by the protocol doc's rule, promised
# to bound each item by it before the tensor exists — so it is exempt, and
# the warning is left for the impl that makes no such promise. 2x area is the
# smallest mix worth naming: below it the padding waste is within the margin.
MIXED_SIZE_LOG_RATIO = 2.0

# Optional method an impl may expose to answer one question the grant cannot:
# *how many of these particular inputs can one call execute at all?* It is a
# **shape** ceiling, never a memory opinion — the kernel-index case run2
# measured (`inferio.impl.eocr.max_batch_for`: CRAFT's first pooling kernel
# addresses its output with a signed 32-bit int, so 2560-bounded A4 pages
# stop at 28 items whatever the board has free). Called with the window's
# `(width, height)` header readings for the planned batch, in PIL's order,
# `None` where a header could not be read; it returns a positive item count,
# or None for "no ceiling from me".
MAX_BATCH_ATTR = "max_batch_for"

# `clamped.reason` for a batch shrunk by [`MAX_BATCH_ATTR`] (or by the impl's
# own equivalent inside `predict`) rather than by the defensive memory clamp.
# The key is additive on the wire: its **absence** means the memory clamp,
# which is what every pre-run2 worker emitted and what the orchestrator
# already understands (docs/inferio-worker-protocol.md, measurement fields).
INDEX_LIMIT_REASON = "index_limit"

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


def _shape(value: Any) -> tuple[int, int] | None:
    """`(width, height)` from an image header, or None.

    `Image.open` is lazy — it parses the header and reads `size` without
    decoding pixel data, which is the whole point: pricing must not cost what
    the batch itself will cost.

    The *shape* rather than only its product, because two things want this
    one reading and they want different halves of it: pricing multiplies it
    ([`_areas`]), and an impl's batch ceiling ([`impl_max_batch`]) is a
    function of the padded height and width separately — a portrait and a
    landscape page of equal area build differently sized tensors.
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
    return (int(width), int(height))


def _text_bytes(data: Any) -> int:
    if data is None:
        return 0
    if isinstance(data, str):
        return len(data.encode("utf-8", "ignore"))
    if isinstance(data, (bytes, bytearray, memoryview)):
        return len(bytes(data))
    return len(repr(data))


def _positive_int(value: Any) -> int | None:
    """`value` as a positive int, or None. Refuses bools and anything that
    is not already a number: an attribute hunt must not coerce a string."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        number = int(value)
    except Exception:  # pragma: no cover - defensive
        return None
    return number if number > 0 else None


def _canvas_on(obj: Any) -> int | None:
    """A plausible canvas held directly on `obj`, or None."""
    for attribute in CANVAS_ATTRS:
        try:
            value = getattr(obj, attribute, None)
        except Exception:  # pragma: no cover - a property that raises
            continue
        pixels = _positive_int(value)
        if pixels is None:
            continue
        if pixels < CANVAS_FLOOR_PIXELS:
            logger.debug(
                "ignoring %s = %r as a pixel canvas: below the %d-pixel floor",
                attribute,
                value,
                CANVAS_FLOOR_PIXELS,
            )
            continue
        return pixels
    return None


def impl_canvas_pixels(instance: Any) -> int | None:
    """The loaded impl's own known input resolution, or None.

    Tier 2 of the canvas resolution order
    (docs/inferio-worker-protocol.md, "Memory grants"): what a model whose
    canvas the registry cannot state statically — because it lives in a
    processor config downloaded with the weights — still knows about itself.
    Passive: every reading is a `getattr` on an object the impl already
    built, bounded to [`CANVAS_WALK_DEPTH`] levels through
    [`CANVAS_HOLDERS`], and never trusted below [`CANVAS_FLOOR_PIXELS`].

    Never raises: a canvas that cannot be read is simply no cap, which is
    what every model did before this field existed.
    """
    try:
        seen: set[int] = set()
        level = [instance]
        for _ in range(CANVAS_WALK_DEPTH + 1):
            following = []
            for obj in level:
                if obj is None or id(obj) in seen:
                    continue
                seen.add(id(obj))
                pixels = _canvas_on(obj)
                if pixels is not None:
                    return pixels
                for holder in CANVAS_HOLDERS:
                    try:
                        following.append(getattr(obj, holder, None))
                    except Exception:  # pragma: no cover - a property that raises
                        continue
            if not following:
                return None
            level = following
        return None
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("canvas introspection failed: %s", exc)
        return None


def resolve_canvas_pixels(grant: dict[str, Any], instance: Any, unit: str) -> int | None:
    """The per-item pixel cap for this window, in the documented order.

    The grant first (`grant.canvas_pixels`, the figure both sides price
    against), the impl's own known input resolution second, uncapped third.
    Only for `pixel` inputs: the cap describes an area, and capping a token
    count or an item count by an area is meaningless — a `count`-aggregated
    model would in any case be unaffected, since `min(1, cap)` is 1.

    The grant states the canvas the *orchestrator* resolved: the registry's
    `metadata.cost.canvas_pixels` when one is declared, else the figure this
    worker itself reported for the loaded impl on its `load` response. Either
    way it is authoritative — taking it rather than re-deriving one means the
    number the host priced its window in and the number this batch is packed
    in are the same number by construction, not by two resolutions happening
    to agree. Tier 2 below is the fallback for an orchestrator that sent
    none (a pre-run2 one, or one whose load report this worker predates).

    Logged once per process, at the tier that answered, because a slope
    fitted under a cap and one fitted without it are different numbers and a
    maintainer reading a store has to be able to tell which this was.
    """
    if unit != "pixel":
        return None
    declared = _positive_int(grant.get("canvas_pixels"))
    if declared is not None:
        _log_canvas_once("the orchestrator's grant", declared)
        return declared
    measured = impl_canvas_pixels(instance)
    if measured is not None:
        _log_canvas_once("the loaded impl", measured)
        return measured
    _log_canvas_once(None, None)
    return None


_canvas_logged = False


def _log_canvas_once(source: str | None, pixels: int | None) -> None:
    global _canvas_logged
    if _canvas_logged:
        return
    _canvas_logged = True
    if source is None:
        logger.info(
            "no per-item pixel canvas declared or discoverable; pricing raw "
            "submitted pixels, as before run2"
        )
        return
    logger.info(
        "pricing each input at min(raw pixels, %d), the canvas %s states",
        pixels,
        source,
    )


def _shape_readings(inputs: Sequence[Any]) -> list[tuple[int, int] | None]:
    """Raw `(width, height)` per input, None where the header was unreadable.
    The one place an image header is opened; every priced figure and every
    batch-ceiling question is derived from this list, so a window is never
    read twice."""
    return [_shape(getattr(entry, "file", None)) for entry in inputs]


def _areas(shapes: Sequence[tuple[int, int] | None]) -> list[int | None]:
    """[`_shape_readings`] as raw pixel counts, preserving the None holes."""
    return [None if shape is None else shape[0] * shape[1] for shape in shapes]


def _pixel_units(readings: Sequence[int | None], cap: int | None) -> list[int]:
    """[`_pixel_readings`] turned into prices, capped at `cap` when given.

    The unreadable-input rule is unchanged and applied *after* the cap, on the
    capped scale: an input whose header could not be read is charged the
    largest input already priced in this window, falling back to
    [`UNREADABLE_PIXEL_UNITS`] when there is none.
    """
    priced: list[int] = []
    largest = 0
    for reading in readings:
        value = reading
        if value is not None and cap is not None:
            value = min(value, cap)
        priced.append(value if value is not None else 0)
        if value:
            largest = max(largest, value)
    fallback = largest or UNREADABLE_PIXEL_UNITS
    if cap is not None:
        fallback = min(fallback, cap)
    return [value or fallback for value in priced]


class PricedWindow(NamedTuple):
    """What one window's inputs cost, and what they cost uncapped.

    `units` is the price the grant is spent in — `min(raw, canvas)` for a
    `pixel` model with a canvas in force. `raw` is the same list without the
    cap: identical to `units` whenever no cap applies, and *strictly the
    packing's tiebreaker*, never a price. Safety reads `units` only.

    Keeping both is what [`plan_batches`] needs to stay size-homogeneous under
    the cap (run2 D1-b): the cap deliberately flattens every item at or above
    the canvas to one number, which is right for pricing and destroys exactly
    the information the bucketing sorts on.
    """

    units: list[int]
    raw: list[int]
    shapes: list[tuple[int, int] | None] | None = None


def price_window(
    inputs: Sequence[Any], unit: str, canvas_pixels: int | None = None
) -> PricedWindow:
    """[`price_inputs`], plus the same window priced without the canvas cap.

    One pass over the inputs: an image header is read once and every figure
    below is derived from that one reading. `shapes` is the reading itself,
    carried out of here so [`impl_max_batch`] can ask its question without
    opening a single header twice; it is None for a non-`pixel` window, where
    no header is read at all.
    """
    cap = canvas_pixels if canvas_pixels and canvas_pixels > 0 else None
    if unit != "pixel":
        # No image headers on this path at all — a token or item price knows
        # nothing about shapes.
        units = price_inputs(inputs, unit, canvas_pixels)
        return PricedWindow(units, units, None)
    shapes = _shape_readings(inputs)
    readings = _areas(shapes)
    if cap is None:
        # No cap applies, so the raw prices *are* the prices. Sharing the one
        # list is safe: neither is mutated anywhere.
        units = _pixel_units(readings, None)
        return PricedWindow(units, units, shapes)
    return PricedWindow(
        _pixel_units(readings, cap), _pixel_units(readings, None), shapes
    )


def _pads_without_a_canvas(instance: Any) -> bool:
    """Does this impl pad a batch to a common size *and* state no canvas?

    The pairing is the whole point. An impl that pads to a common size builds
    a tensor sized by its largest member; an impl that states a canvas of its
    own has, by the protocol doc's rule
    (docs/inferio-worker-protocol.md, "Memory grants"), promised to bound
    every item by that canvas *before* the tensor exists — which is exactly
    what `inferio.impl.eocr` was changed to do in run2's D1-b fix. So an impl
    that pads and states nothing is the one shape whose batches can cost far
    more than they were priced, and it is the only one worth a warning.

    Only a canvas held **directly on the impl** exempts it — [`_canvas_on`],
    not the full [`impl_canvas_pixels`] walk. The walk exists to *price* a
    model whose ceiling lives in a downloaded processor config, and finding a
    `max_pixels` two levels down inside somebody else's object is a fact
    about that object, not a promise by the impl that owns the padding. An
    impl that pads to a common size and happens to hold a processor with a
    canvas is precisely the case this guard must not fall silent on.

    Never raises: an attribute walk over an object nobody here controls
    decides a log line, nothing else.
    """
    try:
        if not getattr(instance, PADS_TO_COMMON_SIZE_ATTR, False):
            return False
        return _canvas_on(instance) is None
    except Exception:  # pragma: no cover - defensive
        return False


_mixed_batch_logged = False


def _warn_mixed_batch_once(batch: Sequence[int], raw: Sequence[int]) -> None:
    """One line per process when a priced-flat batch mixes raw sizes.

    Diagnostic only — nothing here changes what runs. It makes run2's D1-b
    visible in a log rather than in a throughput histogram: the batch was
    charged for the canvas and the impl will build it at raw dimensions.
    """
    global _mixed_batch_logged
    if _mixed_batch_logged or len(batch) < 2:
        return
    sizes = [raw[index] for index in batch if raw[index] > 0]
    if len(sizes) < 2:
        return
    biggest, smallest = max(sizes), min(sizes)
    if biggest < smallest * MIXED_SIZE_LOG_RATIO:
        return
    _mixed_batch_logged = True
    logger.warning(
        "this impl pads a batch to its largest member and states no canvas of "
        "its own, and this batch of %d mixes raw inputs from %d to %d pixels "
        "(%.1fx): it will build a tensor sized by the largest, while the "
        "canvas cap priced them alike. Bound each input by the canvas before "
        "padding (docs/inferio-worker-protocol.md, \"Memory grants\")",
        len(batch),
        smallest,
        biggest,
        biggest / smallest,
    )


def impl_max_batch(
    instance: Any, shapes: Sequence[tuple[int, int] | None]
) -> int | None:
    """What the impl says it can execute for a batch of these shapes, or None.

    Passive and total, like every other question this module asks a loaded
    impl: an absent method, a method that raises, and a nonsensical answer
    are all "no ceiling from me", which is what every impl said before the
    hook existed. Only a positive `int` is an answer — a `bool` is not, and
    neither is a float, because a ceiling is a count of items.
    """
    hook = getattr(instance, MAX_BATCH_ATTR, None)
    if not callable(hook):
        return None
    try:
        answer = hook(list(shapes))
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("%s raised; ignoring its ceiling: %s", MAX_BATCH_ATTR, exc)
        return None
    if isinstance(answer, bool) or not isinstance(answer, int):
        return None
    return answer if answer > 0 else None


def cap_batch_to_impl_ceiling(
    instance: Any,
    batch: Sequence[int],
    shapes: Sequence[tuple[int, int] | None] | None,
    units: Sequence[int],
    aggregation: str,
    free_mb: int | None,
) -> tuple[list[int], dict[str, Any] | None]:
    """Trim `batch` to what the impl can execute, and report the trim.

    Returns `(batch, clamped)` — `clamped` present only when something was
    actually removed, carrying the same `{from_units, to_units, free_mb}` the
    memory clamp emits plus `reason = "index_limit"`, so the orchestrator can
    tell the two apart without a new field
    (docs/inferio-worker-protocol.md, measurement fields).

    **Why one pass is enough.** `plan_batches` orders a batch by descending
    price, so the items dropped here are its smallest; the member that sets
    the padded tensor's dimensions stays, and the ceiling computed for the
    full batch is therefore still valid — and never *too small* — for the
    trimmed one. Re-asking after the trim could only ever grant more items,
    and deliberately is not done: one question per batch, no fixpoint loop
    around an impl's method.

    The dropped items are not lost. They stay in `pending` and are planned
    into the next batch of the same window.
    """
    if shapes is None or len(batch) < 2:
        return list(batch), None
    ceiling = impl_max_batch(instance, [shapes[index] for index in batch])
    if ceiling is None or ceiling >= len(batch):
        return list(batch), None
    kept = list(batch[:ceiling])
    before = batch_units(batch, units, aggregation)
    after = batch_units(kept, units, aggregation)
    logger.warning(
        "the impl can execute at most %d of the %d inputs this batch was "
        "planned with (%d units down to %d); trimming it. This is a shape "
        "ceiling, not a memory condition — the remaining inputs go to the "
        "next batch of this window",
        ceiling,
        len(batch),
        before,
        after,
    )
    clamped: dict[str, Any] = {
        "from_units": before,
        "to_units": after,
        "reason": INDEX_LIMIT_REASON,
    }
    if free_mb is not None:
        clamped["free_mb"] = free_mb
    return kept, clamped


def merge_clamps(
    memory_clamp: dict[str, Any] | None, shape_clamp: dict[str, Any] | None
) -> dict[str, Any] | None:
    """One `clamped` map for a batch both clamps touched.

    A measurement carries at most one. When the defensive memory clamp shrank
    the *budget* and the impl's ceiling then shrank the *batch*, the honest
    single statement spans both: `from_units` is what the grant started at,
    `to_units` is what actually ran, and `reason` names the constraint that
    set `to_units` — the shape ceiling, since it applied last and is the one
    that bound. Nothing is lost that the orchestrator reads: it keys on the
    map's presence, and the three figures are operator-facing provenance.
    """
    if shape_clamp is None:
        return memory_clamp
    if memory_clamp is None:
        return shape_clamp
    merged = dict(shape_clamp)
    merged["from_units"] = memory_clamp["from_units"]
    return merged


def executed_clamp(
    existing: dict[str, Any] | None,
    batch: Sequence[int],
    executed: int | None,
    units: Sequence[int],
    aggregation: str,
    priced: int,
    free_mb: int | None,
) -> dict[str, Any]:
    """`clamped` for a batch the *impl* cut short on a shape ceiling.

    The backstop to [`cap_batch_to_impl_ceiling`], for the cases the harness
    could not pre-empt: an impl that computes a ceiling the harness cannot
    (a per-request parameter it never sees), or a kernel that raised and
    `run_with_oom_retry` halved through. Either way the impl told us so
    through `inferio.impl.utils.total_index_limit_events`, and the batch's
    measurement must say the size it ran at was not the size it was granted —
    without the `oom` flag, which is the whole point (run2 probes report, S1).

    `to_units` is the largest chunk the impl reports executing, priced from
    the front of the batch: `plan_batches` ordered it by descending price, so
    that is the most expensive `executed` items in it and therefore never an
    under-statement.

    Three values of `executed` and three different honest answers. A count
    between 1 and `len(batch)` prices that many items. **Zero** is a known
    fact, not a missing one — `_executed_shape` returns it for an impl that
    consulted the retry helper, got nothing through it and did the work by
    another route (easyOCR's per-image fallback) — so it prices as zero
    rather than as the whole batch, which would produce the nonsense
    `from_units == to_units` on a batch that ran no part of its plan in one
    call. `None` is the missing one (the impl does not route through the
    helper at all), and there the whole batch is the only defensible figure.
    """
    ran = (
        list(batch[:executed])
        if isinstance(executed, int) and 0 <= executed < len(batch)
        else list(batch)
    )
    clamped = dict(existing) if existing else {}
    clamped.setdefault("from_units", priced)
    clamped["to_units"] = batch_units(ran, units, aggregation)
    clamped["reason"] = INDEX_LIMIT_REASON
    if free_mb is not None:
        clamped.setdefault("free_mb", free_mb)
    return clamped


def price_inputs(
    inputs: Sequence[Any], unit: str, canvas_pixels: int | None = None
) -> list[int]:
    """Per-input units in the model's cost dimension. Never zero, never raises.

    An unreadable `pixel` input is charged the largest input seen so far in
    this window (falling back to [`UNREADABLE_PIXEL_UNITS`]) rather than
    failing the window: one corrupt file must not cost 63 good ones their
    batch, and over-charging it only makes its batch smaller.

    `canvas_pixels` is the model's per-item ceiling (run2 R7,
    [`resolve_canvas_pixels`]): every `pixel`-class model shipped resizes or
    tiles its input onto a fixed canvas, so its real cost stops rising there
    while a raw header-derived price keeps rising with whatever the user
    submitted. Run1 measured what that costs — a fitted slope 4.33x the
    probe's on nemotron, 58 of 110 batches holding one item (report §4,
    Q3/W1, F-B). Applied to the raw reading *and* to the unreadable-input
    fallback, which is the same quantity by another route.
    """
    units: list[int] = []
    if unit == "pixel":
        cap = canvas_pixels if canvas_pixels and canvas_pixels > 0 else None
        return _pixel_units(_areas(_shape_readings(inputs)), cap)
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
    tiebreak: Sequence[int] | None = None,
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

    `tiebreak` is a **secondary descending key**, applied only among items
    whose priced units are equal and only for `max-times-count`. It exists
    because the run2 R7 canvas cap flattens every item at or above the canvas
    to one price, which is correct pricing and leaves the primary sort with
    nothing to separate a 2480x3508 scan from an 8000x6000 sheet — so they
    can land in one batch, and an impl that pads a batch to its largest
    member's raw dimensions then builds a tensor several times the area the
    batch was charged for (run2 D1-b). Padding cost is a function of the *raw*
    dimensions within the canvas, so raw pixels are what the tiebreaker
    orders by, and a bucket stays as size-homogeneous as the corpus allows.
    It never changes a batch's price, never changes safety, and cannot make a
    batch larger: it only decides *which* equally-priced item comes next.

    A batch is never smaller than one item: a single item over budget goes
    through alone and the impl's OOM backstop catches it if it truly cannot
    run (Package 1 already decided batch-1 OOM = item fails, job continues).
    `cap_items` is an additional item-count bound, never converted to units.
    """
    budget = max(1, int(unit_budget))
    cap = cap_items if cap_items and cap_items > 0 else None
    order = list(range(len(units)))
    if aggregation == "max-times-count":
        # Stable descending sort: the largest item sets each batch's price,
        # and among equals the largest raw item goes first. `reverse=True`
        # keeps input order for items equal on both keys (Python's sort is
        # stable and reverse does not re-reverse ties).
        if tiebreak is not None and len(tiebreak) == len(units):
            order.sort(
                key=lambda index: (units[index], tiebreak[index]), reverse=True
            )
        else:
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


class LiveBudget(NamedTuple):
    """What one pre-batch memory reading decided, and the reading itself.

    `units` is the budget the next batch may spend; `free_mb`/`free_source`
    are the reading it was decided from, reported on the measurement so the
    orchestrator's external-usage term refreshes at response cadence rather
    than at its own staleness timer (run2 R5; run1 report §4, T3 measured
    `external_mb` ageing to 166.9 s and a +30 GB step taking 31.5 s to reach
    `/health`). `clamped` is the clamp's own report, present only when the
    clamp actually shrank something.
    """

    units: int
    free_mb: int | None
    free_source: str | None
    clamped: dict[str, Any] | None


def clamp_to_live_memory(unit_budget: int, grant_mb: int | None) -> LiveBudget:
    """Shrink the budget if free memory has fallen below what it assumed.

    Shrink-only, and never above the grant. The rule compares live free
    memory against the grant's own MB reservation, which works pre-fit as
    well as post-fit: pre-fit the grant has no slope to convert units with,
    but its MB figure is still the contention share it was carved from, so
    the *ratio* is the honest scaling factor either way.

    Returns `unit_budget` unchanged when nothing can be read (no CUDA, no
    NVML, no torch) — the orchestrator's own staleness refresh and the impl's
    OOM backstop cover that case.

    **The reading is taken even when there is nothing to clamp against.**
    Before run2 a grant with `mb <= 0` — which is what a pre-fit grant on a
    full board carries, i.e. precisely the memory-blind case — returned
    without reading anything at all. It is still exactly **one** reading per
    batch, the same one this function always took; reporting it is free, and
    the batch that most needs the orchestrator to learn what the board looks
    like is the one whose own grant could not say.
    """
    free_mb, _, free_source = memory.free_total_mb()
    if not grant_mb or grant_mb <= 0:
        return LiveBudget(unit_budget, free_mb, free_source, None)
    if free_mb is None or free_mb >= grant_mb:
        return LiveBudget(unit_budget, free_mb, free_source, None)
    shrunk = max(1, int(unit_budget * free_mb / grant_mb))
    if shrunk >= unit_budget:
        # The ratio rounded back up to the whole budget: nothing was shrunk,
        # so nothing is reported as shrunk.
        return LiveBudget(unit_budget, free_mb, free_source, None)
    logger.info(
        "free memory fell to %d MiB against a %d MiB grant; shrinking this "
        "batch's budget from %d to %d units",
        free_mb,
        grant_mb,
        unit_budget,
        shrunk,
    )
    return LiveBudget(
        shrunk,
        free_mb,
        free_source,
        {"from_units": unit_budget, "to_units": shrunk, "free_mb": free_mb},
    )


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

    The last resort, and the only tier that reads prose. Every rule here names
    an allocator or a device API explicitly, because run1 measured what
    happens when the test is looser: a bare `out of memory` substring deflated
    a healthy model 15 times on a board with 96 GB free, purely because an
    impl worded an unrelated failure as "out of memory slots" (report §4,
    Q1/B11). That substring alone is deliberately **not** a match.

    Three rules, in the order they are tried:

    - [`OOM_MESSAGE_PATTERNS`] — allocator and driver failures that never say
      "out of memory" at all, so each spelling has to be listed. MPS is why
      the tier exists in the first place: an MPS allocation failure is a plain
      `RuntimeError("MPS backend out of memory (…)")` and there is no typed
      form of it to catch (docs/unified-memory-admission.md, "Negative
      signals"). CPU torch's `DefaultCPUAllocator` / `alloc_cpu.cpp` message
      is the same shape, and the CUBLAS/cuDNN/cuSOLVER/cuFFT/cudaError entries
      are allocation failures a driver reports in its own vocabulary;
    - [`OOM_MESSAGE_PAIRS`] — two fragments that must appear together;
    - [`OOM_DEVICE_TOKENS`] — the words "out of memory" **plus** a device-API
      token as a whole word. This is the open half, and it is what keeps the
      tier from losing a real out-of-memory condition to a wording nobody
      enumerated: torch alone emits at least four spellings and CTranslate2
      (faster-whisper) emits a fifth. It is scoped rather than bare, which is
      exactly the distinction B11 turns on.

    Adding a spelling here is how a new backend is learned; dropping the
    device-token scope is how a healthy model gets deflated.
    """
    lowered = str(error).lower()
    for pattern in OOM_MESSAGE_PATTERNS:
        if pattern in lowered:
            return _qualified_name(type(error))
    for first, second in OOM_MESSAGE_PAIRS:
        if first in lowered and second in lowered:
            return _qualified_name(type(error))
    if OOM_DEVICE_PHRASE in lowered and OOM_DEVICE_TOKENS.search(lowered):
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


def _index_limit_total() -> int:
    """`inferio.impl.utils.total_index_limit_events()`, or 0 when unavailable.

    The shape-ceiling twin of [`_oom_halvings_total`], and deliberately a
    separate counter rather than a share of that one: a kernel index ceiling
    halves a batch exactly as an out-of-memory condition does, but it is not
    one, and folding it into `oom` would deflate a model on a board with tens
    of GB free — which is the whole reason run2's S1 needed a fix rather than
    a wider OOM classifier.

    Diffed across the whole `predict` call for the same reason the OOM total
    is: an impl may consult the retry helper more than once, and may also cap
    its own batch up front without any exception being raised at all.
    """
    utils = sys.modules.get("inferio.impl.utils")
    reader = (
        getattr(utils, "total_index_limit_events", None)
        if utils is not None
        else None
    )
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
    canvas = resolve_canvas_pixels(grant, instance, unit)
    prices = price_window(inputs, unit, canvas)
    units, raw_units = prices.units, prices.raw
    # Decided once per window, not per batch: the object graph does not change
    # mid-window, and the walk is the only part of the check with a cost.
    watch_mixing = canvas is not None and _pads_without_a_canvas(instance)
    # The impl's shape ceiling needs `(width, height)`, which the pixel pricer
    # already read. A non-`pixel` window read no headers at all, so if the
    # impl exposes the hook we read them here — once, before the timed
    # sections, exactly as pricing does.
    shapes = prices.shapes
    if shapes is None and callable(getattr(instance, MAX_BATCH_ATTR, None)):
        shapes = _shape_readings(inputs)

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
        live = clamp_to_live_memory(budget, grant_mb)
        remaining_units = [units[index] for index in pending]
        remaining_raw = [raw_units[index] for index in pending]
        plan = plan_batches(
            remaining_units,
            aggregation,
            live.units,
            cap_items,
            tiebreak=remaining_raw,
        )
        batch = [pending[position] for position in plan[0]]
        # The second, non-memory bound on this batch: what the impl can
        # actually execute for these shapes. Asked after planning and before
        # anything runs, so a batch the impl would have chunked internally —
        # and reported unpriceable — instead runs whole and is a clean priced
        # sample, with `clamped.reason` saying why it was not the full budget.
        batch, shape_clamp = cap_batch_to_impl_ceiling(
            instance, batch, shapes, units, aggregation, live.free_mb
        )
        clamped = merge_clamps(live.clamped, shape_clamp)
        if watch_mixing:
            _warn_mixed_batch_once(batch, raw_units)
        priced = batch_units(batch, units, aggregation)

        state = memory.begin_batch()
        retry_before = _oom_retry_record()
        halvings_before = _oom_halvings_total()
        index_limits_before = _index_limit_total()
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
            executed, absorbed = _batch_shape(
                retry_before, len(batch), halvings_before
            )
            oom_class = classify_oom(exc, absorbed)
            oom = oom_class is not None
            if not oom:
                logger.debug(
                    "a batch of %d inputs failed with %s, which is not an "
                    "out-of-memory condition; reporting it without the oom flag",
                    len(batch),
                    type(exc).__name__,
                )
            if _index_limit_total() > index_limits_before:
                clamped = executed_clamp(
                    clamped, batch, executed, units, aggregation, priced,
                    live.free_mb,
                )
            record(
                memory.measure_batch(
                    state,
                    items=len(batch),
                    oom=oom,
                    oom_class=oom_class,
                    free_mb=live.free_mb,
                    free_source=live.free_source,
                    clamped=clamped,
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
            record(memory.measure_batch(
                    state,
                    items=len(batch),
                    free_mb=live.free_mb,
                    free_source=live.free_source,
                    clamped=clamped,
                ))
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
        if _index_limit_total() > index_limits_before:
            # The impl hit its own shape ceiling inside `predict` — the
            # harness's pre-cap did not see it (an older impl, an unreadable
            # header, a per-request parameter only the impl knows). The batch
            # is already unpriceable by `executed`; this says *why*, and says
            # it is not a memory event.
            clamped = executed_clamp(
                clamped, batch, executed, units, aggregation, priced,
                live.free_mb,
            )
        measurement = memory.measure_batch(
            state,
            items=len(batch),
            units=priced if priceable else None,
            oom=absorbed_ooms > 0,
            oom_class=classify_oom(None, absorbed_ooms) if absorbed_ooms else None,
            free_mb=live.free_mb,
            free_source=live.free_source,
            clamped=clamped,
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
