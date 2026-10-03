"""Worker-side packing harness: spend a `predict` grant on GPU batches.

Prices every input in the model's cost dimension, packs the window into
batches within the grant's unit budget, clamps each batch (shrink-only) to live
free memory and the impl's shape ceiling, measures it, and restores input order.
Stdlib only at module level; PIL is imported lazily.

See docs/inferio-worker-protocol.md "Memory grants" and "Memory sensing".
"""

from __future__ import annotations

import json
import logging
import re
import sys
import time
from typing import Any, Callable, Iterable, NamedTuple, Sequence

from inferio_worker import memory

logger = logging.getLogger("inferio_worker.packing")

# Prefix on a whole-batch OOM the impl's own halving loop did not absorb.
OOM_WINDOW_PREFIX = "INFERENCE_OOM_WINDOW:"

# The substring both of our own out-of-memory markers contain (case-sensitive).
OOM_MARKER = "INFERENCE_OOM"

# `oom_class.exception` for an OOM the impl's halving loop absorbed.
OOM_HALVING_WITNESS = "run_with_oom_retry"

# How many links of an exception's cause/context chain are read.
CHAIN_DEPTH_LIMIT = 16

# `oom_class.source` values, strongest first (wire vocabulary).
OOM_SOURCE_TYPED = "typed_exception"
OOM_SOURCE_MARKER = "marker"
OOM_SOURCE_PATTERN = "message_pattern"

# Lower-cased allocation-failure messages that do not say "out of memory".
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

# Two-part patterns: both fragments must appear in one message.
OOM_MESSAGE_PAIRS = (("defaultcpuallocator", "allocate memory"),)

# "out of memory" counts only beside a device-API token as a whole word; a
# host allocator's bare "out of memory" is not a device OOM.
OOM_DEVICE_TOKENS = re.compile(r"\b(cuda|hip|rocm|nvml|xpu|sycl)\b")
OOM_DEVICE_PHRASE = "out of memory"

# Units/sec ratio below which a pool-growing batch is judged to have spilled to
# system RAM (Windows WDDM sysmem fallback fails silently, not with an OOM).
COLLAPSE_RATIO = 0.4

# Our pool may exceed NVML's device-used memory by this much before part of it
# is judged to be in system RAM. See docs/batch-calibration-design.md,
# "Windows display driver: the pool outgrows the card".
SPILL_TOLERANCE_MB = 512

# Least fraction of the grant's unit budget a batch must carry to report
# `next_over_budget`. Uniform items that fit n to a batch fill more than
# n / (n + 1) of it, so no uniform size is left out.
NEXT_OVER_BUDGET_MIN_RATIO = 0.5

# Units for an unreadable `pixel` input when nothing else in the window priced.
# Never zero: a free item packs unbounded.
UNREADABLE_PIXEL_UNITS = 2_000_000

# Attributes holding a model's per-item pixel canvas, and objects holding them.
CANVAS_ATTRS = ("canvas_pixels", "max_pixels", "image_max_pixels")
CANVAS_HOLDERS = ("processor", "image_processor", "embedder", "model")

CANVAS_WALK_DEPTH = 2

# Smallest canvas believed; a smaller reading would under-price and over-admit.
CANVAS_FLOOR_PIXELS = 512 * 512

# Attributes holding a model's per-item token window (the most tokens of one
# input that reach the GPU), and objects holding them.
TOKEN_WINDOW_ATTRS = ("max_seq_length", "max_seq_len", "model_max_length")
TOKEN_WINDOW_HOLDERS = ("model", "embedder", "tokenizer")

# Smallest token window believed, as for `CANVAS_FLOOR_PIXELS`.
TOKEN_WINDOW_FLOOR = 16

# Largest believed: HF tokenizers spell "no limit" as `int(1e30)`.
TOKEN_WINDOW_MAX = 1_000_000

# Set by an impl that pads every batch member to the largest one's size.
PADS_TO_COMMON_SIZE_ATTR = "pads_to_common_size"

# Area ratio within one padded batch above which a log line is written.
MIXED_SIZE_LOG_RATIO = 2.0

# Optional impl method: the most of these inputs one call can execute (a shape
# ceiling, not a memory limit). Takes the batch's `(width, height)` readings;
# returns a positive item count or None.
MAX_BATCH_ATTR = "max_batch_for"

# `clamped.reason` for a shape-ceiling clamp; absent means the memory clamp.
INDEX_LIMIT_REASON = "index_limit"

# Flat per-input allowance for `audio-second` pricing (no decoder here).
AUDIO_FALLBACK_SECONDS = 30

# Bytes per token, matching the dispatcher's estimate.
BYTES_PER_TOKEN = 4

# Consecutive non-comparable batches after which the comparator is discarded.
COMPARATOR_MAX_AGE = 8

# The last comparable pool-growing batch, `(units, units_per_sec)`.
_last_growth: "tuple[int, float] | None" = None

# Consecutive non-comparable batches since `_last_growth` was set.
_non_comparable_streak = 0

# Reactive shrink: a grant below this ratio of releasable slack for this many
# consecutive windows releases the pool. See docs/inferio-worker-protocol.md
# "Reactive shrink and trim".
SHRINK_RATIO = 0.8
SHRINK_WINDOWS = 2

# Releasable slack a memory-blind window (`mb == 0`) needs before it counts as
# a squeeze. Mirrors the host's `TRIM_SLACK_MB`.
SHRINK_BLIND_SLACK_MB = 256

# Set once a spill outlived its release, or had no release: the live memory
# itself does not fit, so later spills are logged at debug.
_spill_persists = False

# Consecutive granted windows below `SHRINK_RATIO` × the releasable slack.
_under_grant_windows = 0
# Set by a release the blind rule caused; a grant with memory clears it.
_blind_released = False


class WindowFailure(Exception):
    """A packed batch failed. Carries the measurements of the batches that ran,
    the failing one included; the window still fails as a whole."""

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
    """Forget the throughput comparator; called on every pool release, since a
    regrowing pool is not comparable to a warm one."""
    global _last_growth, _non_comparable_streak
    _last_growth = None
    _non_comparable_streak = 0


def reset_shrink_state() -> None:
    """Forget the reactive-shrink hysteresis."""
    global _under_grant_windows, _blind_released
    _under_grant_windows = 0
    _blind_released = False


def note_trimmed() -> None:
    """Reset everything a completed `empty_cache()` invalidates."""
    reset_comparator()
    reset_shrink_state()


def release_pool() -> bool:
    """Release the pool for `inferio.impl.utils.clear_cache()` (the OOM-retry
    loop); returns whether it ran. Resets only the throughput comparator.
    """
    if not memory.empty_cache(memory.IMPL_RELEASE, arm=False):
        return False
    reset_comparator()
    return True


def maybe_shrink(grant_mb: int | None) -> bool:
    """Release the pool when the grant is well below its releasable slack.

    Called once per granted window, before its first batch. Slack is what
    `empty_cache()` would return (`reserved - allocated` minus unreturnable
    split blocks); the grant must stay below `SHRINK_RATIO` of it for
    `SHRINK_WINDOWS` consecutive windows. Returns whether `empty_cache()` ran.

    A memory-blind window (`mb == 0`) counts as a squeeze, so a pool that
    itself filled the device is released. It counts only above
    `SHRINK_BLIND_SLACK_MB`, and only until the first release it causes, so a
    busy shared device does not release on every other window.
    """
    global _under_grant_windows, _blind_released
    if grant_mb is None or grant_mb < 0:
        _under_grant_windows = 0
        return False
    reserved_mb, allocated_mb = memory.pool_stats_mb()
    if reserved_mb is None or allocated_mb is None:
        _under_grant_windows = 0
        return False
    split_mb = memory.unreturnable_split_mb() or 0
    slack_mb = max(0, reserved_mb - allocated_mb - split_mb)
    if slack_mb <= 0:
        _under_grant_windows = 0
        return False
    if grant_mb == 0:
        if _blind_released or slack_mb < SHRINK_BLIND_SLACK_MB:
            _under_grant_windows = 0
            return False
    elif grant_mb >= SHRINK_RATIO * slack_mb:
        _under_grant_windows = 0
        _blind_released = False
        return False
    else:
        _blind_released = False
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
    if not memory.empty_cache(memory.SHRINK_RELEASE):
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
    note_trimmed()
    _blind_released = grant_mb == 0
    return True


# --- Pricing ---


def _image_source(value: Any) -> Any | None:
    """Something PIL can open (bytes or a path), or None."""
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
    """`(width, height)` from an image header (no decode), or None."""
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
    """UTF-8 bytes of an input's text; anything else is priced as compact JSON,
    as the host does (`dispatch::text_bytes`)."""
    if data is None:
        return 0
    if isinstance(data, str):
        return len(data.encode("utf-8", "ignore"))
    if isinstance(data, (bytes, bytearray, memoryview)):
        return len(bytes(data))
    try:
        blob = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    except Exception:  # not serialisable: `repr` is the last resort
        blob = repr(data)
    return len(blob.encode("utf-8", "ignore"))


def _positive_int(value: Any) -> int | None:
    """`value` as a positive int, or None; bools and non-numbers are refused."""
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
    """The loaded impl's own input resolution in pixels, or None."""
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
    input resolution, then uncapped. `pixel` inputs only; logged once.
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
            "submitted pixels"
        )
        return
    logger.info(
        "pricing each input at min(raw pixels, %d), the canvas %s states",
        pixels,
        source,
    )


def _token_window_on(obj: Any) -> int | None:
    """A plausible token window held directly on `obj`, or None."""
    for attribute in TOKEN_WINDOW_ATTRS:
        try:
            value = getattr(obj, attribute, None)
        except Exception:  # pragma: no cover - a property that raises
            continue
        tokens = _positive_int(value)
        if tokens is None:
            continue
        if tokens < TOKEN_WINDOW_FLOOR or tokens > TOKEN_WINDOW_MAX:
            logger.debug(
                "ignoring %s = %r as a token window: outside the %d..%d band",
                attribute,
                value,
                TOKEN_WINDOW_FLOOR,
                TOKEN_WINDOW_MAX,
            )
            continue
        return tokens
    return None


def impl_max_tokens(instance: Any) -> int | None:
    """The loaded impl's own sequence window, or None. Never raises."""
    try:
        seen: set[int] = set()
        level = [instance]
        for _ in range(CANVAS_WALK_DEPTH + 1):
            following = []
            for obj in level:
                if obj is None or id(obj) in seen:
                    continue
                seen.add(id(obj))
                tokens = _token_window_on(obj)
                if tokens is not None:
                    return tokens
                for holder in TOKEN_WINDOW_HOLDERS:
                    try:
                        following.append(getattr(obj, holder, None))
                    except Exception:  # pragma: no cover - a property that raises
                        continue
            if not following:
                return None
            level = following
        return None
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("token window introspection failed: %s", exc)
        return None


def resolve_max_tokens(grant: dict[str, Any], instance: Any, unit: str) -> int | None:
    """The per-item token cap for this window: the grant, then the impl's own
    sequence window, then uncapped. `token` inputs only. A transformer's
    footprint stops rising at `max_seq_length`, so longer inputs are capped.
    """
    if unit != "token":
        return None
    declared = _positive_int(grant.get("max_tokens"))
    if declared is not None:
        _log_token_window_once("the orchestrator's grant", declared)
        return declared
    measured = impl_max_tokens(instance)
    if measured is not None:
        _log_token_window_once("the loaded impl", measured)
        return measured
    _log_token_window_once(None, None)
    return None


_token_window_logged = False


def _log_token_window_once(source: str | None, tokens: int | None) -> None:
    global _token_window_logged
    if _token_window_logged:
        return
    _token_window_logged = True
    if source is None:
        logger.info(
            "no per-item token window declared or discoverable; pricing raw "
            "submitted tokens"
        )
        return
    logger.info(
        "pricing each input at min(raw tokens, %d), the sequence window %s states",
        tokens,
        source,
    )


def _shape_readings(inputs: Sequence[Any]) -> list[tuple[int, int] | None]:
    """Raw `(width, height)` per input, None where the header was unreadable."""
    return [_shape(getattr(entry, "file", None)) for entry in inputs]


def _areas(shapes: Sequence[tuple[int, int] | None]) -> list[int | None]:
    """Shapes as raw pixel counts, keeping the Nones."""
    return [None if shape is None else shape[0] * shape[1] for shape in shapes]


def _pixel_units(readings: Sequence[int | None], cap: int | None) -> list[int]:
    """Pixel prices, capped at `cap`. An unreadable input is charged the largest
    capped price in the window (else `UNREADABLE_PIXEL_UNITS`)."""
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
    """What one window's inputs cost (`units`, capped) and uncapped (`raw`).
    `raw` is only the packing tiebreaker, never a price.
    """

    units: list[int]
    raw: list[int]
    shapes: list[tuple[int, int] | None] | None = None


def price_window(
    inputs: Sequence[Any],
    unit: str,
    canvas_pixels: int | None = None,
    max_tokens: int | None = None,
) -> PricedWindow:
    """`price_inputs` plus the uncapped prices, reading each header once.
    `shapes` is None for a non-`pixel` window."""
    cap = canvas_pixels if canvas_pixels and canvas_pixels > 0 else None
    if unit == "token":
        raw = _raw_token_units(inputs)
        return PricedWindow(_token_units(raw, max_tokens), raw, None)
    if unit != "pixel":
        units = price_inputs(inputs, unit, canvas_pixels)
        return PricedWindow(units, units, None)
    shapes = _shape_readings(inputs)
    readings = _areas(shapes)
    if cap is None:
        units = _pixel_units(readings, None)
        return PricedWindow(units, units, shapes)
    return PricedWindow(
        _pixel_units(readings, cap), _pixel_units(readings, None), shapes
    )


def _pads_without_a_canvas(instance: Any) -> bool:
    """Whether this impl pads a batch to a common size and states no canvas
    directly on itself; such batches can cost far more than priced.
    """
    try:
        if not getattr(instance, PADS_TO_COMMON_SIZE_ATTR, False):
            return False
        return _canvas_on(instance) is None
    except Exception:  # pragma: no cover - defensive
        return False


_mixed_batch_logged = False


def _warn_mixed_batch_once(batch: Sequence[int], raw: Sequence[int]) -> None:
    """Warn once per process when a padded batch mixes very different sizes."""
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
    """The impl's `max_batch_for` answer for these shapes: a positive int, or
    None for no ceiling (also when absent or raising)."""
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
    """Trim `batch` to what the impl can execute; `clamped` (reason
    `index_limit`) only when something was removed. Dropped items stay pending.
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
    """One `clamped` map for a batch both clamps touched: memory's `from_units`,
    the shape ceiling's `to_units` and `reason`."""
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
    """`clamped` for a batch the impl itself cut short on a shape ceiling (no
    `oom` flag). `to_units` prices the first `executed` items; `executed=None`
    prices the whole batch.
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


def _raw_token_units(inputs: Sequence[Any]) -> list[int]:
    """Every input's uncapped bytes-per-token price. Never zero."""
    return [
        max(
            1,
            (
                _text_bytes(getattr(entry, "file", None))
                + _text_bytes(getattr(entry, "data", None))
            )
            // BYTES_PER_TOKEN,
        )
        for entry in inputs
    ]


def _token_units(readings: Sequence[int], cap: int | None) -> list[int]:
    """Token prices capped at the model's sequence window."""
    if not cap or cap <= 0:
        return list(readings)
    return [min(reading, cap) for reading in readings]


def price_inputs(
    inputs: Sequence[Any],
    unit: str,
    canvas_pixels: int | None = None,
    max_tokens: int | None = None,
) -> list[int]:
    """Per-input units in the model's cost dimension. Never zero, never raises.
    `canvas_pixels` and `max_tokens` cap the per-item price.
    """
    units: list[int] = []
    if unit == "pixel":
        cap = canvas_pixels if canvas_pixels and canvas_pixels > 0 else None
        return _pixel_units(_areas(_shape_readings(inputs)), cap)
    if unit == "token":
        return _token_units(_raw_token_units(inputs), max_tokens)
    if unit == "audio-second":
        return [AUDIO_FALLBACK_SECONDS for _ in inputs]
    # `item` and anything unrecognised: one unit each.
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
    in FIFO order, and `max-times-count` visits indices largest-first (ties by
    descending `tiebreak`) so each batch's first member sets its price.
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
    """One pre-batch memory reading and the budget it allowed. `ram_mb` is the
    reading's RAM basis on MPS; `clamped` only when the budget shrank.
    """

    units: int
    free_mb: int | None
    free_source: str | None
    ram_mb: tuple[int | None, int | None] | None
    clamped: dict[str, Any] | None


# `clamped.reason` when host RAM, not the device, shrank a GPU worker's batch.
HOST_RAM_REASON = "host_ram"


def _scaled(
    unit_budget: int, spendable_mb: int, grant_mb: int, fixed_mb: int = 0
) -> int:
    """`unit_budget` scaled by `spendable / grant`, both taken above the
    `fixed_mb` a batch of any size costs: rounded half up, at least one, never
    more than `unit_budget`."""
    if spendable_mb >= grant_mb:
        return unit_budget
    per_units_mb = grant_mb - fixed_mb
    if per_units_mb <= 0:
        return 1
    left_mb = max(spendable_mb - fixed_mb, 0)
    return min(unit_budget, max(1, int(unit_budget * left_mb / per_units_mb + 0.5)))


def clamp_to_live_memory(
    unit_budget: int,
    grant_mb: int | None,
    ram_reserve_mb: int = 0,
    ram_grant_mb: int = 0,
    fixed_mb: int = 0,
) -> LiveBudget:
    """Shrink the budget if the memory this batch can spend has fallen below
    what the grant assumed.

    Scales the budget by `(free + releasable pool) / grant`, rounded to nearest,
    shrink-only. The pool counts because a batch reuses it without a new device
    allocation, and a grant may include it. The grant's `fixed_mb` is what a
    batch costs whatever its size: only the rest of both figures scales, and
    what the worker still holds allocated of it since load counts as
    spendable, since the batch does not allocate that again. The reading is
    taken even for a memory-blind grant (`mb <= 0`), so it is always reported.

    Free host RAM counts only above `ram_reserve_mb`, which the orchestrator
    keeps free: in a RAM-priced worker's reading, and for a GPU worker whose
    grant books `ram_grant_mb` of host RAM, which is scaled the same way
    against free RAM and runs at the smaller of the two budgets.
    """
    reading = memory.free_total_reading()
    free_mb, free_source = reading.free_mb, reading.source
    ram_mb = (
        (reading.ram_total_mb, reading.ram_available_mb)
        if reading.ram_total_mb is not None
        else None
    )
    shrunk, clamped = unit_budget, None
    if grant_mb and grant_mb > 0 and free_mb is not None:
        reserve_mb = ram_reserve_mb if free_source == "ram" else 0
        pool_mb = memory.releasable_pool_mb() or 0
        held_mb = min(fixed_mb, memory.held_since_load_mb())
        spendable_mb = max(free_mb - reserve_mb, 0) + pool_mb + held_mb
        shrunk = _scaled(unit_budget, spendable_mb, grant_mb, fixed_mb)
        if shrunk < unit_budget:
            logger.info(
                "spendable memory fell to %d MiB (%d free plus %d of releasable "
                "pool) above a %d MiB reserve against a %d MiB grant; shrinking "
                "this batch's budget from %d to %d units",
                spendable_mb,
                free_mb,
                pool_mb,
                reserve_mb,
                grant_mb,
                unit_budget,
                shrunk,
            )
            clamped = {"from_units": unit_budget, "to_units": shrunk, "free_mb": free_mb}
    if ram_grant_mb > 0:
        host_free_mb, _ = memory.ram_free_total_mb()
        if host_free_mb is not None:
            spendable_mb = max(host_free_mb - ram_reserve_mb, 0)
            host = _scaled(unit_budget, spendable_mb, ram_grant_mb)
            if host < shrunk:
                logger.info(
                    "free host RAM fell to %d MiB above its %d MiB reserve "
                    "against %d MiB booked; shrinking this batch's budget from "
                    "%d to %d units",
                    spendable_mb,
                    ram_reserve_mb,
                    ram_grant_mb,
                    unit_budget,
                    host,
                )
                shrunk = host
                clamped = {
                    "from_units": unit_budget,
                    "to_units": host,
                    "free_mb": host_free_mb,
                    "reason": HOST_RAM_REASON,
                }
    return LiveBudget(shrunk, free_mb, free_source, ram_mb, clamped)


# --- Running a window ---


def _input_size(entry: Any) -> int:
    """An input's size for comparing grantless windows: an image's pixel
    count, else the bytes of its file or text."""
    file = getattr(entry, "file", None)
    shape = _shape(file)
    if shape is not None:
        return shape[0] * shape[1]
    return _text_bytes(file if file is not None else getattr(entry, "data", None))


def run_grantless_window(instance: Any, inputs: Sequence[Any]) -> dict[str, Any]:
    """The grantless path: the whole window in one GPU batch. The `finally`
    stops the batch's peak sampler if `predict` raises.

    Where a full GPU spills to system RAM, the pool is released before a
    window whose largest input is larger than any since the last release (as
    `run_window` does before a growing batch), and a window whose pool ends
    above NVML's used memory is flagged `spilled` and the pool released.
    """
    spill_host = memory.spill_capable()
    largest = max(map(_input_size, inputs), default=0) if spill_host else 0
    if spill_host and memory.outgrows_pool(largest, "largest_input"):
        memory.empty_cache(memory.GROWTH_RELEASE)
    state = memory.begin_batch()
    try:
        outputs = list(instance.predict(inputs))
        payload = {"outputs": outputs, **memory.finish_batch(state, items=len(inputs))}
    finally:
        memory.abandon_batch(state)
    if spill_host:
        memory.note_batch_units(largest, "largest_input")
        off_device_mb = pool_off_device_mb(payload.get("memory"))
        if off_device_mb is not None and off_device_mb > SPILL_TOLERANCE_MB:
            payload["measurements"][0]["spilled"] = True
            reserved_mb = payload["memory"]["reserved_mb"]
            released = memory.empty_cache(memory.SPILL_RELEASE)
            if released:
                payload["memory"] = memory.device_memory_sample() or payload["memory"]
            after_mb = pool_off_device_mb(payload["memory"])
            _log_spill(reserved_mb, off_device_mb, released, after_mb)
    return payload




def _qualified_name(cls: type) -> str:
    """`"torch.OutOfMemoryError"` for a library type, `"MemoryError"` for a
    builtin, for `oom_class.exception`."""
    module = getattr(cls, "__module__", "") or ""
    name = getattr(cls, "__name__", None) or repr(cls)
    if not module or module in ("builtins", "__main__"):
        return name
    return f"{module}.{name}"


def _typed_oom(error: BaseException) -> str | None:
    """The exception's name when its type is `torch.OutOfMemoryError` (CUDA and
    HIP) or `MemoryError`. Never reads the message or imports torch.
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
    """The exception's name when it carries one of our own OOM markers (by type
    name or text)."""
    if type(error).__name__ == "InferenceOOMError":
        return _qualified_name(type(error))
    if OOM_MARKER in str(error):
        return _qualified_name(type(error))
    return None


def _pattern_oom(error: BaseException) -> str | None:
    """The exception's name when its text matches a device allocation failure.
    A bare `out of memory` is not a match.
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
    """The exception and its `__cause__`/`__context__` chain, nearest first,
    bounded and loop-safe. Libraries wrap the driver's exception several links
    down."""
    found: list[BaseException] = []
    seen: set[int] = set()
    pending = [exc]
    while pending and len(found) < CHAIN_DEPTH_LIMIT:
        error = pending.pop(0)
        if error is None or id(error) in seen:
            continue
        seen.add(id(error))
        found.append(error)
        pending.append(getattr(error, "__cause__", None))
        pending.append(getattr(error, "__context__", None))
    return tuple(found)


def classify_oom(
    exc: BaseException | None, absorbed: int = 0
) -> dict[str, Any] | None:
    """`oom_class` for a batch, or `None` when nothing says out of memory.

    Each tier (typed, marker, pattern) is tried over the whole chain before the
    next. `absorbed` classifies a batch whose OOMs the impl's halving loop
    absorbed. Never raises. See docs/inferio-worker-protocol.md "Memory
    sensing".
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
            found = (OOM_SOURCE_MARKER, OOM_HALVING_WITNESS)
        if found is None:
            return None
        free_mb = memory.free_at_failure_mb()
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
    """Whether the impl sets `enable_batching`/`enable_batch` falsy: it runs
    one input at a time inside `predict`, so it takes the grantless path.
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
    """`inferio.impl.utils.last_oom_retry()` via `sys.modules`, or None."""
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
    Diffed across a whole `predict` call. Index-limit events are kept separate
    from OOM halvings: they are not memory events."""
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
    """`(largest_chunk_executed, halvings_performed)` for the batch just run;
    the chunk is None when unknown."""
    after = _oom_retry_record()
    if after is None:
        return (None, 0)
    if before is not None and after[0] == before[0]:
        return (None, 0)
    _, largest, halvings = after
    if largest <= 0:
        # The impl did the work by another route: the batch is unpriceable.
        return (0, halvings)
    return (min(largest, planned), halvings)


def _batch_shape(
    before: tuple[int, int, int] | None, planned: int, halvings_before: int
) -> tuple[int | None, int]:
    """`(largest_chunk_executed, absorbed_ooms)` for the batch just run."""
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
    """Mark `throughput_collapse` in place when a pool-growing batch, no smaller
    than the previous one, runs below `COLLAPSE_RATIO` of its rate (a WDDM
    spill to system RAM)."""
    global _last_growth, _non_comparable_streak

    # A spilled batch is already a negative, and never the comparator.
    if measurement.get("spilled"):
        return
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
        # Only a spill-capable host can spill; elsewhere a first batch of a new
        # shape (per-shape kernel selection) is the usual cause, and the server
        # decides whether the flag counts.
        logger.log(
            logging.WARNING if memory.spill_capable() else logging.DEBUG,
            "batch of %d inputs (%d %s units) ran at %.0f units/sec against "
            "%.0f for the previous growing batch of %d units; flagged as a "
            "throughput collapse (possible memory spill)",
            items,
            priced,
            unit,
            rate,
            previous[1],
            previous[0],
        )
        # A collapsed batch does not become the comparator.
        return
    _last_growth = (priced, rate)


def pool_off_device_mb(sample: dict[str, Any] | None) -> int | None:
    """Our pool minus NVML's device-used memory, both from one sample; None
    without an NVML reading."""
    if sample is None or sample.get("free_source") != "nvml":
        return None
    reserved, free, total = (
        sample.get("reserved_mb"),
        sample.get("free_mb"),
        sample.get("total_mb"),
    )
    if reserved is None or free is None or total is None:
        return None
    return reserved - (total - free)


def _log_spill(
    reserved_mb: Any, off_device_mb: int, released: bool, after_mb: int | None
) -> None:
    """Warn of a spill; debug once a spill has persisted (`_spill_persists`)."""
    global _spill_persists
    persists = after_mb is not None and after_mb > SPILL_TOLERANCE_MB
    level = logging.DEBUG if persists and _spill_persists else logging.WARNING
    _spill_persists = _spill_persists or persists
    logger.log(
        level,
        "the %s MiB allocator pool is %d MiB more than NVML reports in use on "
        "the GPU, so part of it is in system memory; %s",
        reserved_mb,
        off_device_mb,
        "released the pool" if released else "left the pool as it is",
    )


def run_window(
    instance: Any,
    inputs: Sequence[Any],
    grant: dict[str, Any],
    emit_memory: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Run one granted window and build the `predict` `ok` payload: outputs in
    input order, measurements and a memory sample. Raises `WindowFailure` when
    a batch fails, carrying what ran.

    `emit_memory` (handshake `batch_memory_frames`) receives a fresh memory
    sample after every batch but the last, so the orchestrator sees the pool
    grow mid-window."""
    unit = str(grant.get("unit") or "item")
    aggregation = str(grant.get("aggregation") or "count")
    budget = grant.get("unit_budget")
    budget = max(1, int(budget)) if isinstance(budget, int) else 1
    granted = budget
    grant_mb = grant.get("mb")
    grant_mb = int(grant_mb) if isinstance(grant_mb, int) else None
    cap_items = grant.get("user_cap_items")
    cap_items = int(cap_items) if isinstance(cap_items, int) else None
    ram_reserve_mb = grant.get("ram_reserve_mb")
    ram_reserve_mb = int(ram_reserve_mb) if isinstance(ram_reserve_mb, int) else 0
    ram_grant_mb = grant.get("ram_mb")
    ram_grant_mb = int(ram_grant_mb) if isinstance(ram_grant_mb, int) else 0
    fixed_mb = grant.get("fixed_mb")
    fixed_mb = max(0, int(fixed_mb)) if isinstance(fixed_mb, int) else 0

    # Reactive shrink: the one point where nothing is in flight.
    trimmed = maybe_shrink(grant_mb)

    # Priced once, outside every timed section.
    canvas = resolve_canvas_pixels(grant, instance, unit)
    max_tokens = resolve_max_tokens(grant, instance, unit)
    prices = price_window(inputs, unit, canvas, max_tokens)
    units, raw_units = prices.units, prices.raw
    watch_mixing = canvas is not None and _pads_without_a_canvas(instance)
    # A non-`pixel` window read none of the headers the shape ceiling needs.
    shapes = prices.shapes
    if shapes is None and callable(getattr(instance, MAX_BATCH_ATTR, None)):
        shapes = _shape_readings(inputs)

    outputs: list[Any] = [None] * len(inputs)
    measurements: list[dict[str, Any]] = []
    pending = list(range(len(inputs)))

    def record(measurement: dict[str, Any]) -> dict[str, Any]:
        """Append a measurement; the first is stamped `trimmed` if the pool was
        released before it."""
        if trimmed and not measurements:
            measurement["trimmed"] = True
        measurements.append(measurement)
        return measurement

    while pending:
        # Re-plan per batch: the clamp can shrink the budget mid-window.
        live = clamp_to_live_memory(
            budget, grant_mb, ram_reserve_mb, ram_grant_mb, fixed_mb
        )
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
        # The impl's shape ceiling, asked before running.
        batch, shape_clamp = cap_batch_to_impl_ceiling(
            instance, batch, shapes, units, aggregation, live.free_mb
        )
        clamped = merge_clamps(live.clamped, shape_clamp)
        # The next item in packing order would push this batch past the grant:
        # it is as full as whole items allow. False for a window's last batch.
        next_over_budget = (
            len(plan) > 1
            and len(batch) == len(plan[0])
            and batch_units(batch, units, aggregation)
            >= NEXT_OVER_BUDGET_MIN_RATIO * granted
            and batch_units(batch + [pending[plan[1][0]]], units, aggregation)
            > granted
        )
        if watch_mixing:
            _warn_mixed_batch_once(batch, raw_units)
        priced = batch_units(batch, units, aggregation)

        # Where the driver spills instead of failing an allocation, the
        # allocator never frees its cache to retry, so a larger batch would
        # add fresh blocks beside cached ones too small to reuse.
        spill_host = memory.spill_capable()
        if (
            spill_host
            and memory.outgrows_pool(priced)
            and memory.empty_cache(memory.GROWTH_RELEASE)
        ):
            # The pre-batch free reading must include what was released. The
            # throughput comparator is kept: every growing batch here regrows
            # from a release, so they stay comparable.
            reading = memory.free_total_reading()
            live = live._replace(free_mb=reading.free_mb, free_source=reading.source)

        state = memory.begin_batch()
        # The `finally` stops this batch's sampler on any raise.
        try:
            retry_before = _oom_retry_record()
            halvings_before = _utils_total("total_oom_halvings")
            index_limits_before = _utils_total("total_index_limit_events")
            started = time.perf_counter()
            try:
                produced = list(instance.predict([inputs[index] for index in batch]))
            except Exception as exc:
                # A failed batch is never priced: its peaks under-state it.
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
                        ram_mb=live.ram_mb,
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
                record(memory.measure_batch(
                        state,
                        items=len(batch),
                        free_mb=live.free_mb,
                        free_source=live.free_source,
                        ram_mb=live.ram_mb,
                        clamped=clamped,
                    ))
                raise WindowFailure(str(exc), measurements, exc) from exc

            # Unpriced when the impl reports running it in smaller chunks.
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
                # The impl hit its own shape ceiling; not a memory event.
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
                ram_mb=live.ram_mb,
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
            # One sample after the batch, so pool and NVML are paired.
            sample = memory.device_memory_sample() if spill_host else None
            off_device_mb = pool_off_device_mb(sample)
            if off_device_mb is not None and off_device_mb > SPILL_TOLERANCE_MB:
                # A negative for this size; its outputs stand.
                measurement["spilled"] = True
            if next_over_budget:
                measurement["next_over_budget"] = True
            _note_throughput(measurement, priced if priceable else None, elapsed, len(batch), unit)
            record(measurement)
            memory.note_batch_units(priced)
        finally:
            memory.abandon_batch(state)

        # Restore input order: bucketed packing reordered the items.
        for index, output in zip(batch, produced):
            outputs[index] = output
        remaining = set(pending) - set(batch)
        pending = [index for index in pending if index in remaining]

        # Per-batch memory frame while work remains (the reply carries the
        # last). A fresh reading, so free and pool describe the same instant.
        if measurement.get("spilled"):
            reserved_mb = sample["reserved_mb"]
            released = memory.empty_cache(memory.SPILL_RELEASE)
            if released:
                if len(batch) > 1:
                    budget = max(1, min(budget, priced // 2))
                sample = memory.device_memory_sample()
            _log_spill(reserved_mb, off_device_mb, released, pool_off_device_mb(sample))
        if emit_memory is not None and pending:
            if sample is None:
                sample = memory.device_memory_sample()
            if sample is not None:
                emit_memory(sample)

    payload: dict[str, Any] = {"outputs": outputs, "measurements": measurements}
    sample = memory.device_memory_sample()
    if sample is not None:
        payload["memory"] = sample
    return payload
