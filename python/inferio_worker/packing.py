"""Worker-side packing harness: spend a `predict` grant on GPU batches.

It prices every input in the model's cost dimension, packs the window into
batches within the grant's unit budget, clamps each batch shrink-only against
live free memory and against the impl's shape ceiling ([`MAX_BATCH_ATTR`]),
measures it, and restores the input order before replying. Safety is the
grant's business; `run_with_oom_retry` inside the impl stays the backstop, and
a request with no grant takes the grantless path.

Import rules (docs/inferio-rust-orchestrator-design.md §4): stdlib only at
module level; PIL is imported lazily inside the pixel pricer.

See docs/inferio-worker-protocol.md "Memory grants" and "Memory sensing".
"""

from __future__ import annotations

import logging
import re
import sys
import time
from typing import Any, Callable, Iterable, NamedTuple, Sequence

from inferio_worker import memory

logger = logging.getLogger("inferio_worker.packing")

# Prefix on a whole-batch OOM the impl's own halving loop did not absorb;
# `INFERENCE_OOM_BATCH_SIZE_1:` (inferio.impl.utils) covers a single item.
OOM_WINDOW_PREFIX = "INFERENCE_OOM_WINDOW:"

# The substring both of our own out-of-memory markers contain.
# Case-sensitive on purpose: it is a token we emit, not a driver's prose.
OOM_MARKER = "INFERENCE_OOM"

# `oom_class.exception` when the only witness is the impl helper's halving
# counter: a batch that absorbed an OOM internally has no exception to name.
OOM_HALVING_WITNESS = "run_with_oom_retry"

# `oom_class.source` values, strongest first. Wire vocabulary fixed by the
# protocol doc.
OOM_SOURCE_TYPED = "typed_exception"
OOM_SOURCE_MARKER = "marker"
OOM_SOURCE_PATTERN = "message_pattern"

# Driver-shaped message fragments, lower-cased, for the classifier's fallback
# tier. Each names an allocator or device API that emits it only for an
# allocation failure and never says "out of memory", which is why each is
# spelled out.
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

# Two-part patterns: both fragments must appear in one message. The middle of
# CPU torch's allocator failure varies by version and neither half alone is
# specific enough.
OOM_MESSAGE_PAIRS = (("defaultcpuallocator", "allocate memory"),)

# The device-scoped form of "out of memory": the words **and** a device-API
# token as a whole word. The open half of the pattern tier, since enumerating
# every library's spelling loses real conditions; a host allocator's bare "out
# of memory" is left to the rules above.
OOM_DEVICE_TOKENS = re.compile(r"\b(cuda|hip|rocm|nvml|xpu|sycl)\b")
OOM_DEVICE_PHRASE = "out of memory"

# Units/sec ratio below which a pool-growing batch is judged to have spilled to
# system RAM rather than run: under Windows' WDDM sysmem fallback,
# over-admission is a silent throughput collapse and never an exception.
COLLAPSE_RATIO = 0.4

# Units charged to a `pixel` input whose header cannot be read, when no input
# in the window has been priced yet. Never zero: a free item packs unbounded.
UNREADABLE_PIXEL_UNITS = 2_000_000

# Attribute names holding a model's per-item pixel canvas, and the attributes
# holding the object that holds it. Read passively off a constructed instance.
CANVAS_ATTRS = ("canvas_pixels", "max_pixels", "image_max_pixels")
CANVAS_HOLDERS = ("processor", "image_processor", "embedder", "model")

# How deep the canvas hunt goes through [`CANVAS_HOLDERS`]: the shipped shapes
# need two, and it walks an object graph nobody here controls.
CANVAS_WALK_DEPTH = 2

# Smallest number a canvas reading is believed at. Too small a cap under-prices
# an item, which over-admits, so a suspect attribute is refused, not trusted.
CANVAS_FLOOR_PIXELS = 512 * 512

# The attribute an impl sets to say it builds one batch tensor at the
# dimensions of the batch's largest member.
PADS_TO_COMMON_SIZE_ATTR = "pads_to_common_size"

# Raw-area ratio within one batch above which the pairing is worth one log
# line, for an impl that pads to a common size and states no canvas of its own
# (docs/inferio-worker-protocol.md, "Memory grants").
MIXED_SIZE_LOG_RATIO = 2.0

# Optional impl method: how many of these particular inputs can one call
# execute at all? A shape ceiling, never a memory opinion. Called with the
# planned batch's `(width, height)` readings; returns a positive item count or
# None (docs/inferio-worker-protocol.md, "Memory grants").
MAX_BATCH_ATTR = "max_batch_for"

# `clamped.reason` for a batch shrunk by a shape ceiling rather than by the
# defensive memory clamp. Additive on the wire: its absence means the memory
# clamp.
INDEX_LIMIT_REASON = "index_limit"

# Flat per-input allowance for `audio-second` pricing: a clip's real duration
# needs a decoder, and nothing shipped is priced this way yet.
AUDIO_FALLBACK_SECONDS = 30

# Bytes per token, matching the dispatcher's estimate. Tokenizing would need
# the impl's tokenizer, which the `InferenceModel` contract does not cover.
BYTES_PER_TOKEN = 4

# Consecutive non-comparable batches after which the throughput comparator is
# discarded rather than left comparing against a rate no longer the model's.
COMPARATOR_MAX_AGE = 8

# The last *comparable* pool-growing batch, as `(units, units_per_sec)`; one
# model per worker process, so module state is that model's history. The units
# ride along because a collapse claim needs an upward-or-equal step.
_last_growth: "tuple[int, float] | None" = None

# Consecutive non-comparable batches since `_last_growth` was set.
_non_comparable_streak = 0

# Reactive shrink: the ratio of releasable slack below which a grant counts as
# a squeeze, and the consecutive-window hysteresis. Both tunable. See
# docs/inferio-worker-protocol.md "Reactive shrink and trim".
SHRINK_RATIO = 0.8
SHRINK_WINDOWS = 2

# Consecutive granted windows below `SHRINK_RATIO` × the releasable slack.
_under_grant_windows = 0


class WindowFailure(Exception):
    """A packed batch failed. Carries the measurements of the batches that did
    run, the failing one included, so the orchestrator records the negative
    sample rather than inferring it. The window still fails as a whole."""

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
    """Forget the cross-window throughput comparator. Called by both
    `empty_cache()` paths: the pool regrows from nothing, so the next batch's
    units/sec is not comparable to a warm-pool rate."""
    global _last_growth, _non_comparable_streak
    _last_growth = None
    _non_comparable_streak = 0


def reset_shrink_state() -> None:
    """Forget the reactive-shrink hysteresis: a trim already released the pool
    the count was building towards releasing."""
    global _under_grant_windows
    _under_grant_windows = 0


def note_trimmed() -> None:
    """Everything a completed `empty_cache()` invalidates, in one place, so
    the `trim` arm and the reactive shrink cannot drift apart."""
    reset_comparator()
    reset_shrink_state()


def maybe_shrink(grant_mb: int | None) -> bool:
    """Release the pool when the grant is well below its **releasable slack**.

    Called once per granted window, before its first batch: the one moment when
    nothing is in flight and this window's grant is known. Slack is
    `memory_reserved() - memory_allocated()`, and the grant must sit below
    [`SHRINK_RATIO`] of it for [`SHRINK_WINDOWS`] consecutive windows. Returns
    whether `empty_cache()` ran, reported as `trimmed` (protocol doc).
    """
    global _under_grant_windows
    if not grant_mb or grant_mb <= 0:
        # No MB reservation to compare against: not evidence of a squeeze.
        _under_grant_windows = 0
        return False
    reserved_mb, allocated_mb = memory.pool_stats_mb()
    if reserved_mb is None or allocated_mb is None:
        # No live CUDA of ours: nothing to measure and nothing to release.
        _under_grant_windows = 0
        return False
    slack_mb = max(0, reserved_mb - allocated_mb)
    if slack_mb <= 0:
        # Fully occupied by live tensors: `empty_cache()` would return nothing.
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
        "(empty_cache) so the memory returns to the GPU",
        grant_mb,
        slack_mb,
        reserved_mb,
        _under_grant_windows,
    )
    # The pool regrows from here, invalidating the previous comparator.
    note_trimmed()
    return True


# --- Pricing ---


def _image_source(value: Any) -> Any | None:
    """Something PIL can open, from a `PredictionInput.file`-shaped value.
    Bytes in every shipped impl, but a path is legal and cheaper to price."""
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
    """`(width, height)` from an image header, or None. `Image.open` is lazy,
    so pricing does not cost what the batch will. The shape rather than its
    product because a batch ceiling needs height and width separately."""
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
    """The loaded impl's own known input resolution, or None. Tier 2 of the
    canvas resolution order (protocol doc, "Memory grants"): passive `getattr`s
    bounded to [`CANVAS_WALK_DEPTH`] levels, floored at
    [`CANVAS_FLOOR_PIXELS`], never raising."""
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
    """The per-item pixel cap for this window: the grant, then the impl's own
    known input resolution, then uncapped. `pixel` inputs only.

    The grant is authoritative, so the number the host priced its window in and
    the number this batch is packed in are one number by construction. Logged
    once per process at the tier that answered: a slope fitted under a cap is
    not one fitted without.
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
    The one place an image header is opened, so a window is never read twice."""
    return [_shape(getattr(entry, "file", None)) for entry in inputs]


def _areas(shapes: Sequence[tuple[int, int] | None]) -> list[int | None]:
    """[`_shape_readings`] as raw pixel counts, preserving the None holes."""
    return [None if shape is None else shape[0] * shape[1] for shape in shapes]


def _pixel_units(readings: Sequence[int | None], cap: int | None) -> list[int]:
    """[`_areas`] turned into prices, capped at `cap` when given. An unreadable
    input is charged the largest already priced in this window (else
    [`UNREADABLE_PIXEL_UNITS`]), after the cap and on the capped scale."""
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

    `units` is the price the grant is spent in, `min(raw, canvas)` under a cap.
    `raw` is the same list uncapped and is *strictly the packing's tiebreaker*,
    never a price; safety reads `units` only. Both are kept because the cap
    flattens every item at or above the canvas to one number.
    """

    units: list[int]
    raw: list[int]
    shapes: list[tuple[int, int] | None] | None = None


def price_window(
    inputs: Sequence[Any], unit: str, canvas_pixels: int | None = None
) -> PricedWindow:
    """[`price_inputs`], plus the same window priced without the canvas cap.
    One pass: a header is read once and every figure derives from it. `shapes`
    is that reading, None for a non-`pixel` window, which reads no headers."""
    cap = canvas_pixels if canvas_pixels and canvas_pixels > 0 else None
    if unit != "pixel":
        # No image headers on this path: a token or item price knows no shapes.
        units = price_inputs(inputs, unit, canvas_pixels)
        return PricedWindow(units, units, None)
    shapes = _shape_readings(inputs)
    readings = _areas(shapes)
    if cap is None:
        # No cap: the raw prices are the prices, and neither list is mutated.
        units = _pixel_units(readings, None)
        return PricedWindow(units, units, shapes)
    return PricedWindow(
        _pixel_units(readings, cap), _pixel_units(readings, None), shapes
    )


def _pads_without_a_canvas(instance: Any) -> bool:
    """Does this impl pad a batch to a common size *and* state no canvas?

    That pairing is the one shape whose batches can cost far more than they
    were priced; stating a canvas is the promise to bound every item by it
    first (protocol doc, "Memory grants"). Only a canvas held **directly on the
    impl** exempts it, not one found inside somebody else's object.
    """
    try:
        if not getattr(instance, PADS_TO_COMMON_SIZE_ATTR, False):
            return False
        return _canvas_on(instance) is None
    except Exception:  # pragma: no cover - defensive
        return False


_mixed_batch_logged = False


def _warn_mixed_batch_once(batch: Sequence[int], raw: Sequence[int]) -> None:
    """One line per process when a priced-flat batch mixes raw sizes:
    diagnostic only, the batch was charged for the canvas and the impl will
    build it at raw dimensions."""
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
    Passive and total: an absent method, one that raises and a nonsensical
    answer are all "no ceiling from me", and only a positive `int` is one."""
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

    `clamped` is present only when something was removed, carrying
    `reason = "index_limit"` so the orchestrator can tell it from the memory
    clamp. One pass is enough: `plan_batches` orders by descending price, so
    the dropped items are the smallest and the ceiling stays valid — never too
    small. They stay in `pending`.
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
    """One `clamped` map for a batch both clamps touched: `from_units` is what
    the grant started at, `to_units` what ran, and `reason` names the shape
    ceiling, which applied last and is the one that bound."""
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

    The backstop to [`cap_batch_to_impl_ceiling`], reported through
    `inferio.impl.utils.total_index_limit_events` and without the `oom` flag,
    which is the whole point. `to_units` prices the largest chunk the impl
    reports executing from the front of the batch. `executed` of zero is a
    known fact (the impl did the work by another route) and prices as zero;
    `None` is the missing one, where only the whole batch is defensible.
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

    An unreadable `pixel` input is charged the largest seen so far rather than
    failing the window: over-charging it only makes its batch smaller.
    `canvas_pixels` caps the raw reading and the fallback alike.
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
    # `item` and anything unrecognised: one unit each, so an unknown unit from
    # a newer orchestrator degrades to per-item packing and never crashes.
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


# --- Packing ---


def plan_batches(
    units: Sequence[int],
    aggregation: str,
    unit_budget: int,
    cap_items: int | None = None,
    tiebreak: Sequence[int] | None = None,
) -> list[list[int]]:
    """Split input indices into GPU batches within `unit_budget`.

    `count` spends the budget as an item count, `sum` as a greedy running total
    in FIFO order, and `max-times-count` **buckets**: indices are visited
    largest-first, so each batch's price is set by its first member and its
    count grows until `max × count` would exceed the budget.

    `tiebreak` is a secondary descending key among equally-priced items, for
    `max-times-count` only: a canvas cap prices every item at or above the
    canvas alike while padding cost still follows the raw dimensions. It never
    changes a batch's price or size (protocol doc, "Memory grants").

    A batch is never smaller than one item; `cap_items` bounds the item count.
    """
    budget = max(1, int(unit_budget))
    cap = cap_items if cap_items and cap_items > 0 else None
    order = list(range(len(units)))
    if aggregation == "max-times-count":
        # Stable descending sort; among equals the largest raw item is first.
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

    `free_mb`/`free_source` are reported on the measurement so the
    orchestrator's external-usage term refreshes at response cadence, not on
    its own staleness timer. `clamped` only when the clamp shrank something.
    """

    units: int
    free_mb: int | None
    free_source: str | None
    clamped: dict[str, Any] | None


def clamp_to_live_memory(unit_budget: int, grant_mb: int | None) -> LiveBudget:
    """Shrink the budget if free memory has fallen below what it assumed.

    Shrink-only and never above the grant: live free memory against the grant's
    own MB reservation, whose ratio is the honest scaling factor pre-fit as
    well as post-fit. The reading is taken even with nothing to clamp against —
    a grant with `mb <= 0` is the memory-blind case, which most needs the
    orchestrator to learn the GPU.
    """
    free_mb, _, free_source = memory.free_total_mb()
    if not grant_mb or grant_mb <= 0:
        return LiveBudget(unit_budget, free_mb, free_source, None)
    if free_mb is None or free_mb >= grant_mb:
        return LiveBudget(unit_budget, free_mb, free_source, None)
    shrunk = max(1, int(unit_budget * free_mb / grant_mb))
    if shrunk >= unit_budget:
        # Rounded back up to the whole budget: nothing shrunk to report.
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


# --- Running a window ---


def _qualified_name(cls: type) -> str:
    """`"torch.OutOfMemoryError"` for a library type, `"MemoryError"` for a
    builtin — the name the orchestrator sees in `oom_class.exception`.
    Qualified because `OutOfMemoryError` alone is ambiguous."""
    module = getattr(cls, "__module__", "") or ""
    # `__name__`, not `__qualname__`: every such type is module-level.
    name = getattr(cls, "__name__", None) or repr(cls)
    if not module or module in ("builtins", "__main__"):
        return name
    return f"{module}.{name}"


def _typed_oom(error: BaseException) -> str | None:
    """The exception's name when it is an allocator failure by **type**.

    Two types only: `torch.OutOfMemoryError` (the same class on CUDA and HIP,
    reached through `sys.modules` because this module must never import torch)
    and the interpreter's own `MemoryError`. Never reads the message.
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
    Both restate a classification made from a typed exception one frame lower,
    so they are structural evidence, not a message pattern. Matched by type
    name as well as by text, so a reworded marker is still recognised."""
    if type(error).__name__ == "InferenceOOMError":
        return _qualified_name(type(error))
    if OOM_MARKER in str(error):
        return _qualified_name(type(error))
    return None


def _pattern_oom(error: BaseException) -> str | None:
    """The exception's name when its text is **driver-shaped**.

    The last resort and the only tier that reads prose: every rule names an
    allocator or device API explicitly, and a bare `out of memory` substring is
    deliberately not a match. See docs/inferio-worker-protocol.md "Memory
    sensing", `oom_class.source`.
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
    """The exception and the two links Python attaches to it: an OOM re-raised
    inside an `except` block, as `run_with_oom_retry` does, would otherwise be
    invisible."""
    return tuple(
        error
        for error in (exc, getattr(exc, "__cause__", None), getattr(exc, "__context__", None))
        if error is not None
    )


def classify_oom(
    exc: BaseException | None, absorbed: int = 0
) -> dict[str, Any] | None:
    """`oom_class` for a batch, or `None` when nothing says out of memory.

    The three tiers are tried over the whole exception chain in strength order,
    not chain order, so an `InferenceOOMError` raised `from` a
    `torch.OutOfMemoryError` classifies as `typed_exception`; a batch that
    absorbed a condition without failing is classified from `absorbed`. `None`
    is a positive statement — not an OOM, do not deflate. Never raises. See
    docs/inferio-worker-protocol.md "Memory sensing".
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
            # No exception to name: the halving counter is the only witness.
            found = (OOM_SOURCE_MARKER, OOM_HALVING_WITNESS)
        if found is None:
            return None
        # The corroboration a `message_pattern` verdict needs.
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

    A falsy `enable_batching`/`enable_batch` means the impl picks its own batch
    shape inside `predict`, so reported `units` would describe a batch the
    allocator never saw; it takes the grantless path. Only an attribute
    *present and falsy* disables.
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
    """`inferio.impl.utils.last_oom_retry()`, or None. Read through
    `sys.modules`: the harness never imports `inferio`, it only observes it
    when the impl brought it. None is "no information", not "ran whole"."""
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


def _utils_total(name: str) -> int:
    """The `inferio.impl.utils` process counter `name`, or 0 when unavailable.
    Both counters read this way are diffed across the whole `predict` call: an
    impl that calls `run_with_oom_retry` twice leaves only the last call's
    halvings in the per-call record. `total_index_limit_events` stays the
    shape-ceiling twin of `total_oom_halvings`, deliberately separate: a kernel
    index ceiling halves a batch exactly as an OOM does but is not one, and
    folding it into `oom` would deflate a model on an idle GPU."""
    utils = sys.modules.get("inferio.impl.utils")
    reader = getattr(utils, name, None) if utils is not None else None
    if reader is None:
        return 0
    try:
        return int(reader())
    except Exception:  # pragma: no cover - defensive
        return 0


def _executed_shape(
    before: tuple[int, int, int] | None, planned: int
) -> tuple[int | None, int]:
    """`(largest_chunk_executed, halvings_performed)` for the batch just run,
    the chunk None when nothing is known. The generation counter separates "did
    not call the helper" from "called it and got the same numbers"."""
    after = _oom_retry_record()
    if after is None:
        return (None, 0)
    if before is not None and after[0] == before[0]:
        # The impl did not consult the retry helper for this batch.
        return (None, 0)
    _, largest, halvings = after
    if largest <= 0:
        # The record moved and still says nothing ran through the helper: the
        # impl did the work by another route. "Executed nothing here", not "ran
        # the whole batch", so 0 is reported and the batch is unpriceable.
        return (0, halvings)
    return (min(largest, planned), halvings)


def _batch_shape(
    before: tuple[int, int, int] | None, planned: int, halvings_before: int
) -> tuple[int | None, int]:
    """`(largest_chunk_executed, absorbed_ooms)` for the batch just run. The
    absorbed count is the process total diffed across the `predict` call, so a
    halving in any helper call counts; the per-call record is the fallback."""
    executed, halvings = _executed_shape(before, planned)
    across_call = max(_utils_total("total_oom_halvings") - halvings_before, 0)
    return (executed, max(across_call, halvings))


def _note_throughput(
    measurement: dict[str, Any],
    priced: int | None,
    elapsed: float,
    items: int,
    unit: str,
) -> None:
    """Apply the WDDM synthetic-negative rule to one measurement, in place.
    Sound only between two **pool-growing** batches where the second is an
    **upward-or-equal step** in units; [`COMPARATOR_MAX_AGE`] non-comparable
    ones retire the comparator (protocol doc, `throughput_collapse`)."""
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
        # A collapsed batch is NOT the new comparator: a spill must not set
        # the bar.
        return
    _last_growth = (priced, rate)


def run_window(
    instance: Any,
    inputs: Sequence[Any],
    grant: dict[str, Any],
    emit_memory: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Run one granted window and build the `predict` `ok` payload: outputs in
    the original input order, plus measurements and a memory sample. Raises
    [`WindowFailure`] when a batch fails, carrying what ran.

    `emit_memory`, when the orchestrator asked for it in the handshake
    (`batch_memory_frames`), is called with a fresh memory sample after every
    batch that is not the window's last — the per-batch memory frame of
    docs/inferio-worker-protocol.md. It exists because a window is the only
    time this process's pool grows while the orchestrator hears nothing: it
    nets a device-wide free reading (which every *other* replica's replies keep
    fresh) against our pool figure from our own last reply, and books the
    difference as another process's memory. Nothing here reads it back, so a
    window runs identically with or without it."""
    unit = str(grant.get("unit") or "item")
    aggregation = str(grant.get("aggregation") or "count")
    budget = grant.get("unit_budget")
    budget = max(1, int(budget)) if isinstance(budget, int) else 1
    grant_mb = grant.get("mb")
    grant_mb = int(grant_mb) if isinstance(grant_mb, int) else None
    cap_items = grant.get("user_cap_items")
    cap_items = int(cap_items) if isinstance(cap_items, int) else None

    # Reactive shrink: the one point where nothing is in flight.
    trimmed = maybe_shrink(grant_mb)

    # Pricing happens once, up front, OUTSIDE every timed section.
    canvas = resolve_canvas_pixels(grant, instance, unit)
    prices = price_window(inputs, unit, canvas)
    units, raw_units = prices.units, prices.raw
    # Decided once per window: the object graph does not change mid-window.
    watch_mixing = canvas is not None and _pads_without_a_canvas(instance)
    # A non-`pixel` window read none of the headers the shape ceiling needs.
    shapes = prices.shapes
    if shapes is None and callable(getattr(instance, MAX_BATCH_ATTR, None)):
        shapes = _shape_readings(inputs)

    outputs: list[Any] = [None] * len(inputs)
    measurements: list[dict[str, Any]] = []
    pending = list(range(len(inputs)))

    def record(measurement: dict[str, Any]) -> dict[str, Any]:
        """Append a measurement, stamping the window's first one if the pool
        was released before it (protocol doc, `trimmed`), a failed batch
        included."""
        if trimmed and not measurements:
            measurement["trimmed"] = True
        measurements.append(measurement)
        return measurement

    while pending:
        # Re-plan per batch: the clamp can shrink the budget mid-window.
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
        # The second, non-memory bound: what the impl can execute for these
        # shapes. Asked before anything runs, so the batch stays priceable.
        batch, shape_clamp = cap_batch_to_impl_ceiling(
            instance, batch, shapes, units, aggregation, live.free_mb
        )
        clamped = merge_clamps(live.clamped, shape_clamp)
        if watch_mixing:
            _warn_mixed_batch_once(batch, raw_units)
        priced = batch_units(batch, units, aggregation)

        state = memory.begin_batch()
        retry_before = _oom_retry_record()
        halvings_before = _utils_total("total_oom_halvings")
        index_limits_before = _utils_total("total_index_limit_events")
        started = time.perf_counter()
        try:
            produced = list(instance.predict([inputs[index] for index in batch]))
        except Exception as exc:
            # A failed batch is NEVER priceable, whatever it failed of: its
            # peaks describe how far the call got, which understates the batch
            # we packed and would drag the fitted slope low.
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
            if _utils_total("total_index_limit_events") > index_limits_before:
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
                # The whole-window OOM signal; batch-1 has its own prefix.
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
            # Unpriced like every failure path: the peaks under-state it.
            record(memory.measure_batch(
                    state,
                    items=len(batch),
                    free_mb=live.free_mb,
                    free_source=live.free_source,
                    clamped=clamped,
                ))
            raise WindowFailure(str(exc), measurements, exc) from exc

        # Did the impl run the batch it was handed? `units` describing more
        # work than the measured peaks biases the slope low: over-admission.
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
        if _utils_total("total_index_limit_events") > index_limits_before:
            # The impl's own shape ceiling, unseen by the pre-cap. Not a
            # memory event.
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

        # Restore input order: bucketed packing reordered the items.
        for index, output in zip(batch, produced):
            outputs[index] = output
        remaining = set(pending) - set(batch)
        pending = [index for index in pending if index in remaining]

        # The per-batch memory frame, and only while work remains: the reply
        # below carries this same sample, so a frame after the last batch would
        # buy the orchestrator nothing and cost one more driver query.
        # `device_memory_sample` takes the free reading **beside** the pool
        # reading rather than reusing the clamp's pre-batch `live.free_mb`:
        # pairing a pre-batch free with a post-batch pool understates external
        # usage, which is the one direction that is not safe to be wrong in.
        if emit_memory is not None and pending:
            sample = memory.device_memory_sample()
            if sample is not None:
                emit_memory(sample)

    payload: dict[str, Any] = {"outputs": outputs, "measurements": measurements}
    sample = memory.device_memory_sample()
    if sample is not None:
        payload["memory"] = sample
    return payload
