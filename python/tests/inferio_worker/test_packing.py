"""Unit tests for the worker's packing harness (`inferio_worker.packing`).

These run everywhere. The harness only touches torch through
`inferio_worker.memory`, which uses torch strictly if it is *already* in
`sys.modules`, so a fake torch injected there drives the defensive clamp and
the measurement paths without any GPU. PIL is real (it is an inferio
dependency), so the pixel pricer is exercised against genuine image headers.
"""

from __future__ import annotations

import io
import logging
import sys
from types import SimpleNamespace
from unittest import mock

import pytest

from inferio_worker import memory, packing
from inferio_worker.inputs import PredictionInput

MIB = 1024 * 1024


# --- Fakes ---


class FakeCuda:
    """Just enough of `torch.cuda` for the harness's measurement + clamp."""

    def __init__(self, free_mb=8000, total_mb=8192):
        self.free = free_mb * MIB
        self.total = total_mb * MIB
        self.reserved = 0
        self.allocated = 0
        self.peak_reserved = 0
        self.peak_allocated = 0
        self.empty_cache_calls = 0

    def is_available(self):
        return True

    def is_initialized(self):
        return True

    def mem_get_info(self):
        return (self.free, self.total)

    def memory_reserved(self):
        return self.reserved

    def memory_allocated(self):
        return self.allocated

    def max_memory_reserved(self):
        return self.peak_reserved

    def max_memory_allocated(self):
        return self.peak_allocated

    def reset_peak_memory_stats(self):
        self.peak_reserved = self.reserved
        self.peak_allocated = self.allocated

    def empty_cache(self):
        """Release the pool blocks no live tensor is using: the real allocator
        returns `reserved - allocated`, so a test wanting the whole pool
        released zeroes `allocated` first."""
        self.empty_cache_calls += 1
        self.reserved = self.allocated
        self.peak_reserved = max(self.peak_reserved, self.reserved)

    def grow_pool(self, mb):
        """Pretend a batch grew the caching-allocator pool by `mb`."""
        self.reserved += mb * MIB
        self.peak_reserved = max(self.peak_reserved, self.reserved)
        self.allocated += mb * MIB
        self.peak_allocated = max(self.peak_allocated, self.allocated)


@pytest.fixture(autouse=True)
def clean_state():
    """Every test starts with no cross-window throughput comparator and no
    accumulated reactive-shrink hysteresis."""
    packing.note_trimmed()
    yield
    packing.note_trimmed()


class FakeOomRetryUtils:
    """Stand-in for `inferio.impl.utils` as the harness observes it through
    `sys.modules`. `record()` plays a `run_with_oom_retry` call completing: it
    bumps the generation, which is how the harness tells a fresh reading from
    a stale one."""

    def __init__(self):
        self.generation = 0
        self.slot = None
        self.total = 0
        self.index_limits = 0

    def record(self, largest, halvings=0):
        self.generation += 1
        self.slot = (self.generation, largest, halvings)
        self.total += halvings

    def last_oom_retry(self):
        return self.slot

    def total_oom_halvings(self):
        """The only reading that survives an impl calling the helper twice in
        one `predict`; the per-call record keeps the last call only."""
        return self.total

    def note_index_limit(self):
        """A batch the impl could not execute at the size it was formed at for
        a reason that is **not** memory. A separate total on purpose."""
        self.index_limits += 1

    def total_index_limit_events(self):
        return self.index_limits


@pytest.fixture(autouse=True)
def no_ambient_accelerator():
    """Every test in this module describes the harness, not this machine.

    `memory._free_total_mb` memoizes NVML for the life of the process and
    `_torch_cuda` answers off whatever `torch` is in `sys.modules`, so an
    earlier test module that imported torch would otherwise leave the clamp
    and `free_mb` assertions measuring the developer's real GPU — a failure
    that depends on nothing but collection order.
    """
    with (
        mock.patch.dict(sys.modules),
        mock.patch.dict(
            memory._nvml_state,
            {"module_tried": True, "module": None, "handle": None},
            clear=False,
        ),
    ):
        sys.modules.pop("torch", None)
        yield


@pytest.fixture
def fake_oom_retry():
    utils = FakeOomRetryUtils()
    with mock.patch.dict(sys.modules, {"inferio.impl.utils": utils}):
        yield utils


@pytest.fixture
def fake_torch():
    """Inject a fake torch so the memory helpers report something."""
    cuda = FakeCuda()
    torch = SimpleNamespace(cuda=cuda, __version__="9.9.9+fake", dtype=type)
    with mock.patch.dict(sys.modules, {"torch": torch}):
        yield cuda


@pytest.fixture
def fake_rocm_torch():
    """The same fake allocator behind a ROCm-shaped torch: `version.hip` set,
    which is the worker's one positive HIP signal (`memory._is_hip`)."""
    cuda = FakeCuda()
    torch = SimpleNamespace(
        cuda=cuda,
        version=SimpleNamespace(hip="7.2.0", cuda=None),
        __version__="2.11.0+rocm7.2",
        dtype=type,
    )
    with mock.patch.dict(sys.modules, {"torch": torch}):
        yield cuda


def png_bytes(width: int, height: int) -> bytes:
    from PIL import Image

    buffer = io.BytesIO()
    Image.new("RGB", (width, height)).save(buffer, format="PNG")
    return buffer.getvalue()


class Recorder:
    """Impl stand-in that records the batches it was handed."""

    def __init__(self, fail_on=None, oom=False, wrong_count=False, grow=None,
                 raises=None):
        self.batches: list[list] = []
        self.fail_on = fail_on
        self.oom = oom
        self.wrong_count = wrong_count
        self.grow = grow
        self.raises = raises

    def predict(self, inputs):
        self.batches.append(list(inputs))
        if self.grow is not None:
            self.grow(len(inputs))
        if self.raises is not None:
            raise self.raises
        if self.fail_on is not None and len(self.batches) == self.fail_on:
            if self.oom:
                raise RuntimeError("CUDA out of memory. Tried to allocate 2 GiB")
            raise ValueError("fixture failure")
        if self.wrong_count:
            return []
        return [getattr(entry, "data", None) for entry in inputs]


def grant(**overrides):
    base = {
        "unit_budget": 4,
        "mb": 1000,
        "unit": "item",
        "aggregation": "count",
        "user_cap_items": None,
    }
    base.update(overrides)
    return base


def items(count: int):
    return [PredictionInput(data=index) for index in range(count)]


# --- Pricing ---


def test_pixel_pricing_reads_headers_without_decoding(tmp_path):
    """Bytes or a path, priced from the header. One corrupt file must not fail
    the window and must not be free either — a zero-unit item would pack
    without limit — so it is charged the largest input seen so far, or the flat
    fallback when there is none."""
    inputs = [
        PredictionInput(file=png_bytes(40, 30)),
        PredictionInput(file=png_bytes(100, 100)),
    ]
    assert packing.price_inputs(inputs, "pixel") == [1200, 10_000]
    path = tmp_path / "a.png"
    path.write_bytes(png_bytes(10, 20))
    assert packing.price_inputs([PredictionInput(file=str(path))], "pixel") == [200]

    unreadable = [
        PredictionInput(file=b"not an image"),
        PredictionInput(file=png_bytes(50, 40)),
    ]
    assert packing.price_inputs(unreadable, "pixel") == [2000, 2000]
    for entry in (PredictionInput(file=b"junk"), PredictionInput()):
        assert packing.price_inputs([entry], "pixel") == [
            packing.UNREADABLE_PIXEL_UNITS
        ]


# --- Per-item pixel canvas (run2 R7) ---


def test_the_canvas_caps_the_price_and_lets_large_images_pack():
    """A model tiling at (6 + thumbnail) x 512^2 costs 1.84 MP for a 48 MP
    scan, not 26x that, and four 12 MP images then share batches instead of
    each exhausting the budget alone. The unreadable-input fallback is the same
    quantity by another route, so the same cap applies — otherwise one corrupt
    file re-creates the batch of one the cap exists to prevent. Absent or
    non-positive is uncapped, and a cap prices nothing outside `pixel`."""
    big = [PredictionInput(file=png_bytes(8000, 6000))]
    for canvas in (None, 0):
        assert packing.price_inputs(big, "pixel", canvas) == [48_000_000]
    assert packing.price_inputs(big, "pixel") == [48_000_000]
    assert packing.price_inputs(
        big + [PredictionInput(file=png_bytes(1024, 1024))], "pixel", 1_835_008
    ) == [1_835_008, 1_048_576]

    four = [PredictionInput(file=png_bytes(4000, 3000)) for _ in range(4)]
    uncapped = packing.price_inputs(four, "pixel")
    capped = packing.price_inputs(four, "pixel", 1_835_008)
    assert packing.plan_batches(uncapped, "sum", 4_000_000) == [[0], [1], [2], [3]]
    assert packing.plan_batches(capped, "sum", 4_000_000) == [[0, 1], [2, 3]]

    unreadable = big + [PredictionInput(file=b"not an image")]
    assert packing.price_inputs(unreadable, "pixel", 1_835_008) == [1_835_008] * 2
    assert packing.price_inputs([PredictionInput()], "pixel", 1_000_000) == [1_000_000]
    text = [PredictionInput(data="x" * 400)]
    assert packing.price_inputs(text, "token", 1_835_008) == [100]
    assert packing.price_inputs(items(3), "item", 1_835_008) == [1, 1, 1]


def test_the_granted_canvas_wins_over_the_impls_own():
    """The grant is authoritative — the registry's declaration, else what this
    worker reported at load — which is what makes the host's window and this
    worker's batches one denomination by construction."""
    impl = SimpleNamespace(max_pixels=999_999)
    assert (
        packing.resolve_canvas_pixels({"canvas_pixels": 1_835_008}, impl, "pixel")
        == 1_835_008
    )


def test_the_impls_own_resolution_is_the_documented_fallback():
    """Tier 2, for a model whose canvas lives in a processor downloaded with
    the weights rather than in the registry: one level reaches
    `instance.embedder.max_pixels`, two `instance.model.processor.*`. Too
    *small* a cap under-prices an item, which over-admits — the one error
    direction the ledger cannot absorb — so a suspiciously small attribute is
    treated as a misidentified one, and the walk never raises."""
    one_level = SimpleNamespace(embedder=SimpleNamespace(max_pixels=1_843_200))
    assert packing.resolve_canvas_pixels({}, one_level, "pixel") == 1_843_200
    two_levels = SimpleNamespace(
        model=SimpleNamespace(processor=SimpleNamespace(max_pixels=11_289_600))
    )
    assert packing.resolve_canvas_pixels({}, two_levels, "pixel") == 11_289_600
    assert packing.resolve_canvas_pixels({}, SimpleNamespace(), "pixel") is None
    assert packing.resolve_canvas_pixels({}, one_level, "item") is None

    floor = packing.CANVAS_FLOOR_PIXELS
    for value in (4, 1024, floor - 1, 0, -1, True, "1843200"):
        impl = SimpleNamespace(max_pixels=value)
        assert packing.resolve_canvas_pixels({}, impl, "pixel") is None, value
    at_floor = SimpleNamespace(max_pixels=floor)
    assert packing.resolve_canvas_pixels({}, at_floor, "pixel") == floor

    class Hostile:
        @property
        def max_pixels(self):
            raise RuntimeError("no")

        @property
        def processor(self):
            raise RuntimeError("no")

    assert packing.resolve_canvas_pixels({}, Hostile(), "pixel") is None


def test_a_granted_canvas_reaches_the_window(fake_torch):
    """End to end: the grant's canvas is what the batches are packed by."""
    model = Recorder()
    payload = packing.run_window(
        model,
        [PredictionInput(file=png_bytes(4000, 3000)) for _ in range(4)],
        grant(unit_budget=4_000_000, unit="pixel", aggregation="sum",
              canvas_pixels=1_835_008),
    )
    assert [len(batch) for batch in model.batches] == [2, 2]
    assert payload["measurements"][0]["units"] == 2 * 1_835_008


# --- Size homogeneity under the canvas (run2 D1-b) ---
# The cap prices every item at or above the canvas alike, removing the size
# information the `max-times-count` bucketing sorts on. Two halves: the raw
# price survives as a *tiebreaker*, and an impl that pads to its largest member
# while stating no canvas of its own is named in the log once.
#
# One canvas, one pair of sizes, both above it, raw areas 2.78x apart.
D1B_CANVAS = 1_000_000
BIG = (2000, 1500)  # 3 000 000 raw pixels
SMALL = (1200, 900)  # 1 080 000 raw pixels


def mixed_window():
    """Big/small/big/small, interleaved so input order cannot pass by luck."""
    return [
        PredictionInput(file=png_bytes(*BIG)),
        PredictionInput(file=png_bytes(*SMALL)),
        PredictionInput(file=png_bytes(*BIG)),
        PredictionInput(file=png_bytes(*SMALL)),
    ]


def test_price_window_keeps_the_uncapped_price_beside_the_capped_one():
    """And where nothing is capped there is no second reading at all: `units
    is raw`, so no caller can drift them apart."""
    raw = [3_000_000, 1_080_000, 3_000_000, 1_080_000]
    priced = packing.price_window(mixed_window(), "pixel", D1B_CANVAS)
    assert priced.units == [D1B_CANVAS] * 4, "the price is the capped one"
    assert priced.raw == raw
    for unit, canvas in (("pixel", None), ("pixel", 0), ("item", D1B_CANVAS)):
        uncapped = packing.price_window(mixed_window(), unit, canvas)
        assert uncapped.units is uncapped.raw, (unit, canvas)
    assert packing.price_window(mixed_window(), "pixel").units == raw


def test_equally_priced_items_are_ordered_by_raw_size():
    """Four items priced alike bucket by raw area, so the two 3 MP sheets share
    a batch; without it input order interleaves. Secondary means secondary,
    though: a cheaper item never overtakes a dearer one however large it is
    raw, and a mis-sized tiebreaker or an aggregation that does not sort leaves
    the primary key's plan untouched."""
    units = [D1B_CANVAS] * 4
    raw = [3_000_000, 1_080_000, 3_000_000, 1_080_000]
    budget = 2 * D1B_CANVAS
    assert packing.plan_batches(units, "max-times-count", budget) == [[0, 1], [2, 3]]
    assert packing.plan_batches(
        units, "max-times-count", budget, tiebreak=raw
    ) == [[0, 2], [1, 3]]

    plan = packing.plan_batches(
        [10, 100, 10], "max-times-count", 1000, tiebreak=[999_999, 1, 999_999]
    )
    assert plan[0][0] == 1, "the 100-unit item still leads"

    flat = [5, 5, 5, 5]
    assert packing.plan_batches(
        flat, "max-times-count", 10, tiebreak=[9, 9]
    ) == packing.plan_batches(flat, "max-times-count", 10)
    for aggregation, budget in (("sum", 10), ("count", 2)):
        assert packing.plan_batches(
            flat, aggregation, budget, tiebreak=[4, 3, 2, 1]
        ) == [[0, 1], [2, 3]], aggregation


def test_the_tiebreak_only_ever_changes_the_order():
    """The property the tiebreaker has to have, checked by exhaustion rather
    than by example: over randomised windows it never changes how many
    batches there are, what each one costs, or which prices each one holds —
    only *which* of the equally-priced items land together. And it never
    reorders across a price: the plan is still descending by units end to
    end, which is what the budget arithmetic depends on."""
    random = __import__("random").Random(7)
    for _ in range(2000):
        count = random.randint(1, 12)
        units = [random.choice([10, 10, 10, 25, 25, 40]) for _ in range(count)]
        tiebreak = [random.randint(1, 1000) for _ in range(count)]
        budget = random.randint(10, 200)
        cap = random.choice([None, 2, 3, 5])
        plain = packing.plan_batches(units, "max-times-count", budget, cap)
        broken = packing.plan_batches(
            units, "max-times-count", budget, cap, tiebreak=tiebreak
        )
        assert [len(batch) for batch in plain] == [
            len(batch) for batch in broken
        ]
        for before, after in zip(plain, broken):
            assert packing.batch_units(
                before, units, "max-times-count"
            ) == packing.batch_units(after, units, "max-times-count")
            assert sorted(units[i] for i in before) == sorted(
                units[i] for i in after
            )
        flat = [units[i] for batch in broken for i in batch]
        assert flat == sorted(flat, reverse=True)


def test_a_capped_window_buckets_size_homogeneously(fake_torch):
    """End to end through `run_window`: the batches an impl that pads to a
    common size is handed hold one raw size each, so its tensor is the size
    the batch was priced at."""
    model = Recorder()
    packing.run_window(
        model,
        mixed_window(),
        grant(
            unit_budget=2 * D1B_CANVAS,
            unit="pixel",
            aggregation="max-times-count",
            canvas_pixels=D1B_CANVAS,
        ),
    )
    assert len(model.batches) == 2
    for batch in model.batches:
        assert len({len(entry.file) for entry in batch}) == 1, "one raw size"


class Padding:
    """An impl that pads a batch to its largest member. `canvas` is what it
    tells the worker about its own ceiling — `None` is the impl that makes no
    statement, which is the one the guard is for."""

    def __init__(self, canvas=None):
        self.pads_to_common_size = True
        self.batches: list[list] = []
        if canvas is not None:
            self.canvas_pixels = canvas

    def predict(self, inputs):
        self.batches.append(list(inputs))
        return [None] * len(inputs)


@pytest.fixture
def unlogged_guard():
    """The guard logs once per process; each test needs it unfired."""
    packing._mixed_batch_logged = False
    yield
    packing._mixed_batch_logged = False


def run_padding_window(model, inputs, canvas=D1B_CANVAS):
    return packing.run_window(
        model,
        inputs,
        grant(
            unit_budget=4 * D1B_CANVAS,
            unit="pixel",
            aggregation="max-times-count",
            canvas_pixels=canvas,
        ),
    )


def padding_warnings(caplog):
    return [
        record
        for record in caplog.records
        if "pads a batch to its largest member" in record.getMessage()
    ]


def test_an_impl_that_pads_and_states_no_canvas_is_named_once(
    fake_torch, unlogged_guard, caplog
):
    """The whole batch fits one budget here, so the plan *has* to mix sizes —
    the shape run2 D1-b measured, and the shape the log line describes."""
    with caplog.at_level(logging.WARNING, logger="inferio_worker.packing"):
        run_padding_window(Padding(), mixed_window())
        run_padding_window(Padding(), mixed_window())
    warnings = padding_warnings(caplog)
    assert len(warnings) == 1, "once per process, not once per batch"
    message = warnings[0].getMessage()
    assert "1080000 to 3000000 pixels" in message
    assert "2.8x" in message


def test_a_canvas_found_inside_someone_elses_object_does_not_exempt(
    fake_torch, unlogged_guard, caplog
):
    """The exemption is the impl's own statement, not anything the pricing
    walk can reach. `impl_canvas_pixels` deliberately descends into a
    processor to *price* a model whose ceiling lives in a downloaded config —
    but that ceiling is a fact about the processor, and an impl that pads a
    batch to a common size has made no promise by holding one."""
    model = Padding()
    model.processor = SimpleNamespace(max_pixels=D1B_CANVAS)
    assert packing.impl_canvas_pixels(model) == D1B_CANVAS
    assert packing._pads_without_a_canvas(model) is True
    with caplog.at_level(logging.WARNING, logger="inferio_worker.packing"):
        run_padding_window(model, mixed_window())
    assert padding_warnings(caplog)


def test_the_guard_is_silent_where_nothing_is_under_priced(
    fake_torch, unlogged_guard, caplog
):
    """Three ways to be uninteresting: an impl that states a canvas of its own
    — which is a promise to bound every input by it before the tensor exists,
    and is what exempts `inferio.impl.eocr` after the D1-b fix — no canvas in
    force at all, and a batch whose raw sizes are within the 2x ratio."""
    with caplog.at_level(logging.WARNING, logger="inferio_worker.packing"):
        run_padding_window(Padding(canvas=D1B_CANVAS), mixed_window())
        run_padding_window(Padding(), mixed_window(), canvas=None)
        run_padding_window(
            Padding(),
            [PredictionInput(file=png_bytes(*BIG)) for _ in range(4)],
        )
    assert not padding_warnings(caplog)


def test_token_and_item_and_audio_pricing():
    """An unknown unit from a newer orchestrator degrades to per-item packing,
    and `batch_units` follows the declared aggregation."""
    assert packing.price_inputs([PredictionInput(data="x" * 400)], "token") == [100]
    assert packing.price_inputs([PredictionInput()], "token") == [1], "never zero"
    assert packing.price_inputs(items(3), "item") == [1, 1, 1]
    assert packing.price_inputs(items(2), "audio-second") == [
        packing.AUDIO_FALLBACK_SECONDS
    ] * 2
    assert packing.price_inputs(items(2), "furlong") == [1, 1]

    units = [10, 4, 6]
    for aggregation, expected in (("count", 3), ("sum", 20), ("max-times-count", 30)):
        assert packing.batch_units([0, 1, 2], units, aggregation) == expected
    assert packing.batch_units([], units, "sum") == 0


# --- Packing ---


def test_each_aggregation_packs_the_way_it_says():
    """`count` is an item count, `sum` a greedy FIFO running total, and
    `max-times-count` buckets largest-first so one big scan goes through in a
    small batch instead of taxing the thumbnails. A batch is never smaller than
    one item, whatever the budget."""
    assert packing.plan_batches([1] * 7, "count", 3) == [[0, 1, 2], [3, 4, 5], [6]]
    # 3+4 = 7 fits, +2 would be 9 -> new batch; 2+1 = 3 fits.
    assert packing.plan_batches([3, 4, 2, 1], "sum", 8) == [[0, 1], [2, 3]]

    units = [100, 10, 10, 10, 10]
    plan = packing.plan_batches(units, "max-times-count", 100)
    assert plan == [[0], [1, 2, 3, 4]], "100 alone, then the four 10s"
    for batch in plan:
        assert packing.batch_units(batch, units, "max-times-count") <= 100

    over = packing.plan_batches([500, 1, 1], "sum", 10)
    assert over[0] == [0]
    assert packing.batch_units(over[0], [500, 1, 1], "sum") > 10

    spread = [7, 3, 9, 1, 5, 5]
    for aggregation in ("count", "sum", "max-times-count"):
        flat = [
            index
            for batch in packing.plan_batches(spread, aggregation, 10)
            for index in batch
        ]
        assert sorted(flat) == list(range(6)), aggregation


def test_the_user_cap_bounds_items_on_top_of_the_unit_budget():
    """Both bounds hold at once, and the cap is applied to the *bucketed* order
    rather than the input order, so the batches stay similarly-sized
    neighbours. A non-positive cap is not an opinion."""
    assert packing.plan_batches([1] * 6, "sum", 1000, cap_items=2) == [
        [0, 1], [2, 3], [4, 5]
    ]
    assert packing.plan_batches([1] * 3, "sum", 1000, cap_items=1) == [[0], [1], [2]]
    assert packing.plan_batches([1] * 3, "count", 3, cap_items=0) == [[0, 1, 2]]

    units = [100, 100, 10, 10, 10, 10]
    plan = packing.plan_batches(units, "max-times-count", 1000, cap_items=2)
    assert plan == [[0, 1], [2, 3], [4, 5]], "the 100s pair up, then the 10s"
    for batch in plan:
        assert len(batch) <= 2
        assert packing.batch_units(batch, units, "max-times-count") <= 1000
    tight = packing.plan_batches(units, "max-times-count", 100, cap_items=4)
    assert tight[0] == [0], "100 * 2 would exceed the budget"
    assert sorted(index for batch in tight for index in batch) == list(range(6))


# --- Defensive clamp ---


def test_the_clamp_shrinks_when_free_memory_fell(fake_torch):
    """Shrink-only, never below one item, and a budget already at one is never
    a clamped batch however little memory there was."""
    fake_torch.free = 250 * MIB
    shrunk = packing.clamp_to_live_memory(64, 1000)
    assert shrunk.units == 16, "250/1000 of 64"
    assert shrunk.clamped == {"from_units": 64, "to_units": 16, "free_mb": 250}
    assert packing.clamp_to_live_memory(2, 1_000_000).units == 1

    fake_torch.free = 1 * MIB
    floored = packing.clamp_to_live_memory(1, 1000)
    assert (floored.units, floored.clamped, floored.free_mb) == (1, None, 1)

    fake_torch.free = 8000 * MIB
    for grant_mb in (1000, None, 0):
        live = packing.clamp_to_live_memory(64, grant_mb)
        assert live.units == 64, grant_mb
        assert live.clamped is None, grant_mb


def test_the_clamp_is_a_no_op_without_torch():
    """No CUDA, no NVML: nothing readable, so the budget stands and the OOM
    backstop covers the case."""
    live = packing.clamp_to_live_memory(64, 1000)
    assert live.units == 64
    assert (live.free_mb, live.free_source, live.clamped) == (None, None, None)


def test_the_clamp_reads_free_memory_even_with_nothing_to_clamp(fake_torch):
    """R5: a grant carrying `mb <= 0` is the memory-blind case — precisely the
    batch whose reading the orchestrator most needs — and before run2 it was
    the one batch that took no reading at all. Still exactly one reading."""
    fake_torch.free = 4321 * MIB
    for grant_mb in (0, None):
        live = packing.clamp_to_live_memory(64, grant_mb)
        assert live.units == 64
        assert (live.free_mb, live.free_source) == (4321, "torch")
        assert live.clamped is None


def test_every_measurement_carries_the_pre_batch_free_reading(fake_torch):
    """R5's wire half: the clamp's reading rides every measurement, so
    `external_mb` refreshes at response cadence."""
    fake_torch.free = 7000 * MIB
    payload = packing.run_window(
        Recorder(), items(6), grant(unit_budget=2, aggregation="count")
    )
    assert len(payload["measurements"]) == 3
    for measurement in payload["measurements"]:
        assert measurement["free_mb"] == 7000
        assert measurement["free_source"] == "torch"
        assert "clamped" not in measurement

    fake_torch.free = 100 * MIB
    first = packing.run_window(
        Recorder(), items(4), grant(unit_budget=8, mb=1000, aggregation="count")
    )["measurements"][0]
    assert first["clamped"] == {"from_units": 8, "to_units": 1, "free_mb": 100}
    assert first["free_mb"] == 100

    # The failure paths carry it too: a batch that died is exactly when the
    # orchestrator wants to know what the GPU looked like going in.
    fake_torch.free = 512 * MIB
    for model in (Recorder(raises=ValueError("boom")), Recorder(wrong_count=True)):
        with pytest.raises(packing.WindowFailure) as caught:
            packing.run_window(model, items(2), grant(unit_budget=2))
        assert caught.value.measurements[0]["free_mb"] == 512


def test_the_clamp_shrinks_the_batches_actually_run(fake_torch):
    """And the grantless path takes no clamp reading at all — there is no
    grant to clamp against — so it reports none rather than a post-batch
    reading under a pre-batch name."""
    fake_torch.free = 100 * MIB
    model = Recorder()
    payload = packing.run_window(
        model, items(8), grant(unit_budget=8, mb=1000, aggregation="count")
    )
    assert [len(batch) for batch in model.batches] == [1] * 8
    assert payload["outputs"] == list(range(8))

    grantless = memory.finish_batch(memory.begin_batch(), items=3)
    assert "free_mb" not in grantless["measurements"][0]
    assert grantless["memory"]["free_mb"] is not None, "the sample carries one"


# --- Running a window ---


def test_a_window_is_split_into_batches_and_order_is_restored(fake_torch):
    """`max-times-count` reorders items and the dispatcher splits outputs by
    position, so the reply is in input order regardless, and `units` are
    priced in the declared dimension."""
    model = Recorder()
    payload = packing.run_window(model, items(5), grant(unit_budget=2))
    assert [len(batch) for batch in model.batches] == [2, 2, 1]
    assert payload["outputs"] == [0, 1, 2, 3, 4]
    assert [m["items"] for m in payload["measurements"]] == [2, 2, 1]
    assert [m["units"] for m in payload["measurements"]] == [2, 2, 1]

    bucketed = Recorder()
    payload = packing.run_window(
        bucketed,
        [
            PredictionInput(data="small-a", file=png_bytes(10, 10)),
            PredictionInput(data="huge", file=png_bytes(400, 400)),
            PredictionInput(data="small-b", file=png_bytes(10, 10)),
        ],
        grant(unit_budget=400, unit="pixel", aggregation="max-times-count"),
    )
    assert payload["outputs"] == ["small-a", "huge", "small-b"]
    assert any(
        len(batch) == 1 and batch[0].data == "huge" for batch in bucketed.batches
    ), "the huge item really did travel in its own batch"

    payload = packing.run_window(
        Recorder(),
        [PredictionInput(file=png_bytes(20, 10)) for _ in range(3)],
        grant(unit_budget=400, unit="pixel", aggregation="sum"),
    )
    assert [m["units"] for m in payload["measurements"]] == [400, 200]
    assert [m["items"] for m in payload["measurements"]] == [2, 1]


def test_a_failing_batch_reports_the_oom_flag_and_the_window_prefix(fake_torch):
    """The single-item case already carries INFERENCE_OOM_BATCH_SIZE_1 from
    inferio.impl.utils and must not be double-wrapped, and a failure that is
    not an OOM gets neither the flag nor the prefix."""
    with pytest.raises(packing.WindowFailure) as caught:
        packing.run_window(Recorder(fail_on=2, oom=True), items(6), grant(unit_budget=2))
    failure = caught.value
    assert packing.OOM_WINDOW_PREFIX in str(failure)
    assert len(failure.measurements) == 2, "the batch that ran plus the one that failed"
    assert failure.measurements[0].get("oom") is None
    assert failure.measurements[1]["oom"] is True

    single = Recorder(
        raises=RuntimeError("INFERENCE_OOM_BATCH_SIZE_1: single input OOM")
    )
    with pytest.raises(packing.WindowFailure) as caught:
        packing.run_window(single, items(1), grant(unit_budget=1))
    assert str(caught.value).startswith("INFERENCE_OOM_BATCH_SIZE_1:")
    assert packing.OOM_WINDOW_PREFIX not in str(caught.value)
    assert caught.value.measurements[0]["oom"] is True

    with pytest.raises(packing.WindowFailure) as caught:
        packing.run_window(Recorder(fail_on=1), items(2), grant(unit_budget=2))
    assert "fixture failure" in str(caught.value)
    assert packing.OOM_WINDOW_PREFIX not in str(caught.value)
    assert caught.value.measurements[0].get("oom") is None


def test_the_oom_classifier_covers_the_non_cuda_backends(fake_torch):
    """The negative-signal widening (docs/unified-memory-admission.md): on MPS
    and on CPU the condition arrives untyped, and the deflation path only ever
    hears about it through this flag. Conservative all the same — a
    `RuntimeError` saying nothing about memory is not one."""
    failures = {
        "mps": RuntimeError(
            "MPS backend out of memory (MPS allocated: 18.09 GB, max allowed: "
            "18.13 GB)."
        ),
        "cpu-allocator": RuntimeError(
            "[enforce fail at alloc_cpu.cpp:117] . DefaultCPUAllocator: can't "
            "allocate memory: you tried to allocate 12884901888 bytes."
        ),
        "memory-error": MemoryError(),
    }
    for name, failure in failures.items():
        with pytest.raises(packing.WindowFailure) as caught:
            packing.run_window(Recorder(raises=failure), items(2), grant(unit_budget=2))
        assert caught.value.measurements[0]["oom"] is True, name
        assert packing.OOM_WINDOW_PREFIX in str(caught.value), name

    with pytest.raises(packing.WindowFailure) as caught:
        packing.run_window(
            Recorder(raises=RuntimeError("shape mismatch in forward()")),
            items(2),
            grant(unit_budget=2),
        )
    assert caught.value.measurements[0].get("oom") is None
    assert packing.OOM_WINDOW_PREFIX not in str(caught.value)

    # Every other form a backend with no typed exception actually emits.
    for text in (
        "CUDA out of memory. Tried to allocate 2.00 GiB",
        "CUDA error: out of memory",
        "HIP out of memory. Tried to allocate 512.00 MiB",
        "HIP error: out of memory",
        "cublas runtime error: CUBLAS_STATUS_ALLOC_FAILED",
        "cuDNN error: CUDNN_STATUS_ALLOC_FAILED",
        "cudaErrorMemoryAllocation",
    ):
        classified = packing.classify_oom(RuntimeError(text))
        assert classified is not None, text
        assert classified["source"] == packing.OOM_SOURCE_PATTERN, text


# --- Structural out-of-memory classification (run2 R3) ---


class FakeTorchOom(RuntimeError):
    """A stand-in for `torch.OutOfMemoryError`: a `RuntimeError` subclass, and
    the same class object on a CUDA build and a HIP one, which is why the
    classifier needs no ROCm entry of its own."""

    __module__ = "torch"


@pytest.fixture
def fake_torch_with_oom_type(fake_torch):
    """`fake_torch` whose module also exports the typed OOM class."""
    sys.modules["torch"].OutOfMemoryError = FakeTorchOom
    sys.modules["torch"].cuda.OutOfMemoryError = FakeTorchOom
    yield fake_torch


def test_a_typed_allocator_exception_classifies_structurally(fake_torch_with_oom_type):
    """The tier that needs no text at all: the exception *is* the answer.
    `MemoryError` is a builtin no library could hand us, so it is a type test
    too, even though the CPU allocator's other form is a message one."""
    fake_torch_with_oom_type.free = 137 * MIB
    classified = packing.classify_oom(FakeTorchOom("anything at all"))
    assert classified["source"] == packing.OOM_SOURCE_TYPED
    assert classified["exception"] == "torch.FakeTorchOom"
    assert classified["free_mb_at_failure"] == 137, "the live reading at failure"
    assert classified["device"] == "cuda"

    host = packing.classify_oom(MemoryError())
    assert host["source"] == packing.OOM_SOURCE_TYPED
    assert host["exception"] == "MemoryError"


def test_the_typed_tier_holds_on_a_hip_build(fake_rocm_torch):
    """ROCm raises the same class, so one entry covers both backends."""
    sys.modules["torch"].OutOfMemoryError = FakeTorchOom
    classified = packing.classify_oom(FakeTorchOom("HIP out of memory"))
    assert classified["source"] == packing.OOM_SOURCE_TYPED
    assert classified["device"] == "rocm"


def test_our_own_markers_classify_as_markers(fake_torch):
    """`INFERENCE_OOM_*` is our code restating a classification it made one
    frame lower, so it is structural rather than prose — and a batch that
    *succeeded* while the impl halved internally has nothing to name, so the
    witness is named instead of an exception being invented."""

    class InferenceOOMError(RuntimeError):
        __module__ = "inferio.impl.utils"

    by_type = packing.classify_oom(InferenceOOMError("reworded by an impl"))
    assert by_type["source"] == packing.OOM_SOURCE_MARKER
    assert by_type["exception"] == "inferio.impl.utils.InferenceOOMError"

    by_text = packing.classify_oom(
        RuntimeError(f"{packing.OOM_WINDOW_PREFIX} out of GPU memory on 8 inputs")
    )
    assert by_text["source"] == packing.OOM_SOURCE_MARKER

    absorbed = packing.classify_oom(None, absorbed=2)
    assert absorbed["source"] == packing.OOM_SOURCE_MARKER
    assert absorbed["exception"] == packing.OOM_HALVING_WITNESS
    assert packing.classify_oom(None, absorbed=0) is None


def test_a_marker_raised_from_a_typed_exception_reports_the_type(
    fake_torch_with_oom_type,
):
    """Strength order, not chain order: `run_with_oom_retry` raises its marker
    `from` the driver's own exception, and the driver's exception is the
    stronger statement of the two."""

    class InferenceOOMError(RuntimeError):
        pass

    try:
        try:
            raise FakeTorchOom("CUDA out of memory")
        except FakeTorchOom as driver:
            raise InferenceOOMError("INFERENCE_OOM_BATCH_SIZE_1: …") from driver
    except InferenceOOMError as marker:
        classified = packing.classify_oom(marker)
    assert classified["source"] == packing.OOM_SOURCE_TYPED


def test_every_device_wording_of_out_of_memory_is_still_an_oom(fake_torch):
    """The spellings a fixed substring list loses. Each is emitted by
    something in this project's own venv, and a missed one leaves the
    orchestrator over-admitting against a model that cannot take it."""
    wordings = (
        # torch's driver-API path (expandable_segments allocates through
        # cuMemCreate, which reports in the driver's own vocabulary)
        "CUDA driver error: out of memory",
        # torch before 2.0
        "cuda runtime error (2) : out of memory",
        # CTranslate2 (faster-whisper): "CUDA failed with error " + the
        # runtime's error string
        "CUDA failed with error out of memory",
        "HIP failed with error out of memory",
        # the HIP enum spellings, which say nothing else
        "hipErrorOutOfMemory",
        "ROCm: hipMalloc returned out of memory",
    )
    for text in wordings:
        classified = packing.classify_oom(RuntimeError(text))
        assert classified is not None, text
        assert classified["source"] == packing.OOM_SOURCE_PATTERN, text


def test_a_device_token_must_be_a_whole_word(fake_torch):
    """A bare "out of memory" naming no device is not one: run1 measured this
    exact wording deflating a healthy model 15 times on a GPU with 96 GB free.
    The scope has to be a real token, so an English word that merely *contains*
    one ("chip", "relationship") is not a device either."""
    assert packing.classify_oom(
        ValueError("refusing merged batch of 8: the caption cache is out of "
                   "memory slots")
    ) is None
    for word in ("chip", "ship", "relationship", "hipster"):
        healthy = ValueError(
            f"refusing merged batch: the {word} cache is out of memory slots"
        )
        assert packing.classify_oom(healthy) is None, word


def test_a_failed_batch_carries_its_class_on_the_measurement(
    fake_torch_with_oom_type,
):
    """And the half of R3 the orchestrator acts on: no class and no flag means
    *this was not a memory event*, so nothing may deflate on it."""
    fake_torch_with_oom_type.free = 64 * MIB
    with pytest.raises(packing.WindowFailure) as caught:
        packing.run_window(
            Recorder(raises=FakeTorchOom("CUDA out of memory")),
            items(2),
            grant(unit_budget=2),
        )
    measurement = caught.value.measurements[0]
    assert measurement["oom"] is True
    assert measurement["oom_class"]["source"] == packing.OOM_SOURCE_TYPED
    assert measurement["oom_class"]["free_mb_at_failure"] == 64

    with pytest.raises(packing.WindowFailure) as caught:
        packing.run_window(
            Recorder(raises=ValueError("the caption cache is out of memory slots")),
            items(2),
            grant(unit_budget=2),
        )
    measurement = caught.value.measurements[0]
    assert measurement.get("oom") is None
    assert "oom_class" not in measurement
    assert packing.OOM_WINDOW_PREFIX not in str(caught.value)


def test_an_internally_absorbed_oom_carries_the_marker_class(
    fake_torch, fake_oom_retry
):
    class Halving:
        def predict(self, inputs):
            fake_oom_retry.record(largest=len(inputs), halvings=1)
            return [None] * len(inputs)

    payload = packing.run_window(Halving(), items(2), grant(unit_budget=2))
    measurement = payload["measurements"][0]
    assert measurement["oom"] is True
    assert measurement["oom_class"]["source"] == packing.OOM_SOURCE_MARKER
    assert measurement["oom_class"]["exception"] == packing.OOM_HALVING_WITNESS


def test_the_classifier_never_raises(fake_torch):
    """A classifier that threw would turn a failed batch into a dead worker."""

    class Hostile(RuntimeError):
        def __str__(self):
            raise RuntimeError("no string for you")

    assert packing.classify_oom(Hostile()) is None


def test_a_failed_batch_is_never_priced(fake_torch):
    """A mid-batch failure would otherwise enter the fit as a clean high-water
    sample whose peak stops wherever the call gave up, dragging the fitted
    slope low. No failure path prices its batch — an output-count mismatch
    included — but the peaks are still reported."""
    with pytest.raises(packing.WindowFailure) as caught:
        packing.run_window(Recorder(wrong_count=True), items(2), grant(unit_budget=2))
    assert "returned 0 outputs" in str(caught.value)
    assert "units" not in caught.value.measurements[0]

    with pytest.raises(packing.WindowFailure) as caught:
        packing.run_window(Recorder(fail_on=2), items(4), grant(unit_budget=2))
    measurements = caught.value.measurements
    assert len(measurements) == 2
    assert measurements[0]["units"] == 2, "the batch that completed is priced"
    assert "units" not in measurements[1], "the batch that failed is not"
    assert measurements[1].get("oom") is None, "and it was not an OOM"
    assert measurements[1]["items"] == 2


def slowing_impl(cuda):
    """Each pool-growing batch takes 4x longer than the previous one."""
    calls = {"n": 0}

    def predict(inputs):
        import time

        calls["n"] += 1
        cuda.grow_pool(100)
        time.sleep(0.01 * (4 ** (calls["n"] - 1)))
        return [None] * len(inputs)

    return SimpleNamespace(predict=predict)


def test_throughput_collapse_flags_a_spilling_growth_batch(fake_torch):
    """The WDDM synthetic negative: an over-budget allocation silently spills
    to system RAM, so over-admission shows up as a throughput collapse instead
    of an exception. Only upward-or-equal steps compare — a window's smaller
    tail batch amortizes the fixed per-call overhead over less work and is
    legitimately slower, and flagging it would deflate a healthy worker once
    per window forever."""
    # 5 items at a budget of 2 -> batches of 2, 2, 1: two comparable steps and
    # one non-comparable tail.
    payload = packing.run_window(slowing_impl(fake_torch), items(5), grant(unit_budget=2))
    flags = [m.get("throughput_collapse") for m in payload["measurements"]]
    assert flags[0] is None, "the first growing batch has no comparator"
    assert flags[1] is True, "units/sec fell far below the previous growth batch"
    assert not flags[2], "the tail batch is a downward step, however slow"


def test_throughput_collapse_stays_active_on_a_rocm_worker(fake_rocm_torch):
    """Platform-neutral by design (docs/rocm-batch-calibration-parity.md, D8):
    on ROCm the crisp hipMalloc OOM is the primary negative signal, but the
    comparator stays live as a generic over-admission guard, and the HIP
    memory-tier differences do not starve it of the pool-growth signal."""
    payload = packing.run_window(
        slowing_impl(fake_rocm_torch), items(5), grant(unit_budget=2)
    )
    flags = [m.get("throughput_collapse") for m in payload["measurements"]]
    assert flags[0] is None
    assert flags[1] is True, "the spill is flagged on HIP exactly as on CUDA"
    assert not flags[2], "the tail batch stays non-comparable on HIP too"


def test_the_comparator_ages_out_after_a_run_of_non_comparable_batches(fake_torch):
    """A collapsed batch must not become the new comparator (that would make a
    spill the new normal), but a comparator kept forever would be measured
    against a rate the model no longer runs at — so it retires, and
    `reset_comparator` clears it outright."""

    def growing(inputs):
        fake_torch.grow_pool(1)
        return [None] * len(inputs)

    primer = SimpleNamespace(predict=growing)
    packing.run_window(primer, items(2), grant(unit_budget=2))
    assert packing._last_growth is not None
    warm = SimpleNamespace(predict=lambda inputs: [None] * len(inputs))
    for _ in range(packing.COMPARATOR_MAX_AGE):
        packing.run_window(warm, items(1), grant(unit_budget=1))
    assert packing._last_growth is None, "the stale comparator was retired"

    packing.run_window(primer, items(2), grant(unit_budget=2))
    assert packing._last_growth is not None
    packing.reset_comparator()
    assert packing._last_growth is None


# --- Impl-internal sub-batching (unpriceable batches) ---


def test_an_internally_split_batch_is_reported_unpriced(fake_torch, fake_oom_retry):
    """Several shipped impls sub-batch inside predict. The allocator peaks then
    describe a fraction of the packed units, and reporting the packed figure
    would bias the fitted slope low — which is over-admission, the failure the
    whole design exists to prevent. So `units` is omitted."""

    class Splitting:
        def predict(self, inputs):
            fake_torch.grow_pool(20)
            # The impl ran one item at a time, whatever it was handed.
            fake_oom_retry.record(largest=1)
            return [None] * len(inputs)

    payload = packing.run_window(Splitting(), items(4), grant(unit_budget=4))
    measurement = payload["measurements"][0]
    assert measurement["items"] == 4
    assert "units" not in measurement, "only partly executed: unpriceable"
    assert measurement.get("oom") is None, "no halvings, so no negative sample"

    class Whole:
        def predict(self, inputs):
            fake_torch.grow_pool(20)
            fake_oom_retry.record(largest=len(inputs))
            return [None] * len(inputs)

    whole = packing.run_window(Whole(), items(3), grant(unit_budget=3))
    assert whole["measurements"][0]["units"] == 3, "a whole chunk is priced"


def test_a_stale_retry_record_does_not_unprice_the_next_batch(
    fake_torch, fake_oom_retry
):
    """The generation counter is what makes the reading unambiguous: an impl
    that consults the retry helper on one batch and not the next must not have
    the first batch's record applied to the second."""
    calls = {"n": 0}

    class Sometimes:
        def predict(self, inputs):
            calls["n"] += 1
            fake_torch.grow_pool(5)
            if calls["n"] == 1:
                fake_oom_retry.record(largest=1)
            return [None] * len(inputs)

    payload = packing.run_window(Sometimes(), items(4), grant(unit_budget=2))
    units = [m.get("units") for m in payload["measurements"]]
    assert units == [None, 2], (
        "the first batch is unpriceable; the second consulted nothing and is "
        "priced normally"
    )


def test_absorbed_halvings_are_reported_as_a_negative_sample(
    fake_torch, fake_oom_retry
):
    """An OOM the impl's own halving loop swallowed is invisible unless the
    harness reports it, and it is exactly the signal the deflation path exists
    for. A record that moved with `largest == 0` is easyOCR's `readtext` shape
    — 'executed nothing here', not 'ran the whole batch' — so that batch is
    unpriceable too."""

    class Recording:
        def __init__(self, largest, halvings=0):
            self.largest, self.halvings = largest, halvings

        def predict(self, inputs):
            fake_torch.grow_pool(20)
            fake_oom_retry.record(largest=self.largest, halvings=self.halvings)
            return [None] * len(inputs)

    halved = packing.run_window(
        Recording(2, halvings=2), items(4), grant(unit_budget=4)
    )["measurements"][0]
    assert halved["oom"] is True
    assert "units" not in halved, "2 of 4 executed: unpriceable too"

    nothing = packing.run_window(Recording(0), items(3), grant(unit_budget=3))
    measurement = nothing["measurements"][0]
    assert measurement["items"] == 3
    assert "units" not in measurement, "largest == 0 means nothing ran there"


def test_halvings_in_an_earlier_helper_call_still_flag_the_batch(
    fake_torch, fake_oom_retry
):
    """Impls that call `run_with_oom_retry` twice per `predict` leave only the
    last call's record, so an OOM the first pass absorbed is invisible in it —
    the process-total counter, diffed across the call, catches it."""

    class TwoTowers:
        def predict(self, inputs):
            fake_torch.grow_pool(20)
            # First pass: halved twice before it fit, then ran everything.
            fake_oom_retry.record(largest=len(inputs), halvings=2)
            # Second pass: clean, and its record is the one left standing.
            fake_oom_retry.record(largest=len(inputs))
            return [None] * len(inputs)

    payload = packing.run_window(TwoTowers(), items(4), grant(unit_budget=4))
    measurement = payload["measurements"][0]
    assert measurement["oom"] is True, (
        "the first pass's absorbed OOM must not be lost with its record"
    )
    # Both passes ran the whole batch, so it stays priceable — the `oom` flag is
    # what keeps it out of the fit and deflates the ramp.
    assert measurement["units"] == 4


def test_an_impl_that_never_uses_the_retry_helper_is_priced(fake_torch):
    """No `inferio.impl.utils` in sys.modules at all: nothing is known, which
    is 'no information', not 'ran a smaller batch'."""
    payload = packing.run_window(Recorder(), items(2), grant(unit_budget=2))
    assert payload["measurements"][0]["units"] == 2


# --- The batching-disabled gate ---


def test_batching_disabled_detects_the_registry_knobs():
    assert packing.batching_disabled(SimpleNamespace(enable_batching=False))
    assert packing.batching_disabled(SimpleNamespace(enable_batch=False))
    assert packing.batching_disabled(SimpleNamespace(enable_batching=0))
    assert not packing.batching_disabled(SimpleNamespace(enable_batching=True))
    assert not packing.batching_disabled(SimpleNamespace()), (
        "an impl that never heard of the knob is batched normally"
    )


def test_a_warm_pool_batch_is_never_a_collapse(fake_torch):
    """Only pool-*growing* batches carry information about admission: a warm
    repeat that happens to be slow says nothing. The growing ones carry the
    allocator deltas the fit is built on."""

    class SlowButWarm:
        def predict(self, inputs):
            import time

            time.sleep(0.02)
            return [None] * len(inputs)

    def growing(inputs):
        fake_torch.grow_pool(64)
        return [None] * len(inputs)

    primed = packing.run_window(
        SimpleNamespace(predict=growing), items(2), grant(unit_budget=2)
    )
    measurement = primed["measurements"][0]
    assert measurement["reserved_before_mb"] == 0
    assert measurement["peak_reserved_mb"] == 64
    assert measurement["duration_ms"] is not None
    assert primed["memory"]["reserved_mb"] == 64

    payload = packing.run_window(SlowButWarm(), items(2), grant(unit_budget=1))
    assert all(
        m.get("throughput_collapse") is None for m in payload["measurements"]
    ), "no pool growth, no synthetic negative"


def test_a_window_with_no_grant_never_reaches_the_harness():
    """The compatibility path lives in `__main__`: `finish_batch` reports one
    measurement for the whole call and no `units`, there being no declared
    cost dimension to price in."""
    payload = memory.finish_batch(memory.begin_batch(), items=7)
    assert payload["measurements"][0]["items"] == 7
    assert "units" not in payload["measurements"][0]


# --- Reactive shrink (step 2) ---


def idle_impl():
    """An impl that runs a batch without growing the allocator pool."""
    return SimpleNamespace(predict=lambda inputs: [None] * len(inputs))


def test_reactive_shrink_needs_two_consecutive_under_grant_windows(fake_torch):
    """A grant well below the pool means we are holding memory the ledger has
    already taken away, and freeing tensors gives none of it back, so
    `empty_cache()` is the only lever. The two-window hysteresis keeps a
    momentary dip from costing a full pool teardown, and recovery is immediate
    — the point is reacting to a world that moved, and it can move back."""
    fake_torch.reserved = 1000 * MIB
    fake_torch.allocated = 0
    impl = idle_impl()
    squeezed = grant(unit_budget=1, mb=100)  # 100 < 0.8 * 1000

    first = packing.run_window(impl, items(1), squeezed)
    assert fake_torch.empty_cache_calls == 0, "one window is not evidence"
    assert "trimmed" not in first["measurements"][0]

    second = packing.run_window(impl, items(2), squeezed)
    assert fake_torch.empty_cache_calls == 1
    assert fake_torch.reserved == 0, "the pool went back to the driver"
    assert second["measurements"][0]["trimmed"] is True
    assert "trimmed" not in second["measurements"][1], (
        "the flag rides the window's FIRST measurement only — it describes an "
        "event that happened once, before the window's first batch"
    )
    assert packing._under_grant_windows == 0, "the count starts over after a release"

    fake_torch.reserved = 1000 * MIB
    fake_torch.allocated = 0
    packing.run_window(impl, items(1), squeezed)
    assert packing._under_grant_windows == 1
    packing.run_window(impl, items(1), grant(unit_budget=1, mb=900))
    assert packing._under_grant_windows == 0, "800 <= 900: no squeeze"
    packing.run_window(impl, items(1), squeezed)
    assert fake_torch.empty_cache_calls == 1, "the count restarted from zero"


def test_a_shrink_resets_the_throughput_comparator(fake_torch):
    """Post-`empty_cache()` batches regrow the pool from nothing and are
    legitimately slower than warm-pool ones. Comparing across the event would
    flag a healthy regrowth batch as a WDDM memory spill and deflate the
    worker for it."""

    def growing(inputs):
        fake_torch.grow_pool(500)
        return [None] * len(inputs)

    packing.run_window(SimpleNamespace(predict=growing), items(1), grant(unit_budget=1))
    assert packing._last_growth is not None, "the comparator is primed"

    fake_torch.allocated = 0
    impl = idle_impl()
    squeezed = grant(unit_budget=1, mb=100)  # 100 < 0.8 * 500
    packing.run_window(impl, items(1), squeezed)
    assert packing._last_growth is not None, "still just counting"
    packing.run_window(impl, items(1), squeezed)
    assert fake_torch.empty_cache_calls == 1
    assert packing._last_growth is None, "the released pool retired the comparator"


def test_no_grant_mb_and_no_pool_never_shrink(fake_torch):
    """Non-signals that must not accumulate towards a release: a grant with no
    MB reservation, and a worker holding no pool at all."""
    impl = idle_impl()
    for reserved, mb in ((1000 * MIB, 0), (0, 100)):
        fake_torch.reserved, fake_torch.allocated = reserved, 0
        for _ in range(4):
            packing.run_window(impl, items(1), grant(unit_budget=1, mb=mb))
        assert fake_torch.empty_cache_calls == 0, mb
        assert packing._under_grant_windows == 0, mb


def test_a_worker_without_torch_never_shrinks():
    """No live CUDA, no pool of ours, nothing to release — and crucially no
    attempt to create a context in order to find that out."""
    assert packing.maybe_shrink(1) is False
    assert packing._under_grant_windows == 0


def test_the_shrink_compares_the_grant_against_slack_not_the_whole_pool(fake_torch):
    """The grant is an *incremental* activation reservation while
    `memory_reserved()` is the whole pool, weights included, so comparing them
    would fire every other window and release a pool with nothing spare in it.
    Only `reserved - allocated` can be handed back."""
    # A loaded model: a 3000 MiB pool of which 2400 MiB is live weights.
    fake_torch.reserved = 3000 * MIB
    fake_torch.allocated = 2400 * MIB
    impl = idle_impl()
    # A window granted 600 MiB against 600 MiB of releasable slack: it wants
    # essentially everything that could be freed, so freeing it buys nobody
    # anything. Under the old rule (600 < 0.8 * 3000) this fired on window 2.
    steady = grant(unit_budget=1, mb=600)
    for _ in range(6):
        packing.run_window(impl, items(1), steady)
    assert fake_torch.empty_cache_calls == 0, (
        "a pool that is nearly all weights is not slack the worker is hoarding"
    )
    assert packing._under_grant_windows == 0
    assert fake_torch.reserved == 3000 * MIB, "and the weights were never dropped"


def test_a_grant_far_below_the_slack_still_releases_the_pool(fake_torch):
    """The other half of the same rule, on a pool that is mostly weights: two
    consecutive windows release the free blocks and keep the weights, and
    cannot immediately re-trigger because the slack is gone."""
    fake_torch.reserved = 3000 * MIB
    fake_torch.allocated = 2400 * MIB
    impl = idle_impl()
    squeezed = grant(unit_budget=1, mb=100)  # 100 < 0.8 * (3000 - 2400)

    first = packing.run_window(impl, items(1), squeezed)
    assert fake_torch.empty_cache_calls == 0, "one window is not evidence"
    assert "trimmed" not in first["measurements"][0]

    second = packing.run_window(impl, items(1), squeezed)
    assert fake_torch.empty_cache_calls == 1
    assert second["measurements"][0]["trimmed"] is True
    assert fake_torch.reserved == 2400 * MIB, "the weights stayed"

    for _ in range(2):
        packing.run_window(impl, items(1), squeezed)
    assert fake_torch.empty_cache_calls == 1, "self-limiting: no slack left"
    assert packing._under_grant_windows == 0


# --- The impl's shape ceiling (run2 S1) ---
#
# A second, non-memory bound: a kernel whose 32-bit element index cannot
# address the tensor the batch builds refuses it with the whole GPU free. Left
# unreported it is a slower success, so the ledger widens `unit_budget` past a
# batch the impl cannot execute.


class Ceiling(Recorder):
    """Impl stand-in that states a shape ceiling for any batch."""

    def __init__(self, ceiling, **kwargs):
        super().__init__(**kwargs)
        self.ceiling = ceiling
        self.asked: list[list] = []

    def max_batch_for(self, shapes):
        self.asked.append(list(shapes))
        return self.ceiling


def image_items(count: int, width: int = 40, height: int = 30):
    return [PredictionInput(file=png_bytes(width, height)) for _ in range(count)]


def test_a_shape_ceiling_trims_the_batch_and_says_why(fake_torch):
    """The designed path: asked before the batch runs, so the batch that runs
    is whole — a clean *priced* sample — and `clamped.reason` says why it was
    not the full budget."""
    model = Ceiling(2)
    payload = packing.run_window(
        model, image_items(6), grant(unit_budget=6, unit="item", aggregation="count")
    )
    assert [len(batch) for batch in model.batches] == [2, 2, 2], (
        "the trimmed items were not dropped; they went to the next batch"
    )
    assert payload["outputs"] == [None] * 6
    first, second, third = payload["measurements"]
    assert first["clamped"] == {
        "from_units": 6,
        "to_units": 2,
        "reason": "index_limit",
        "free_mb": 8000,
    }
    assert second["clamped"]["from_units"] == 4
    assert "clamped" not in third, "a batch that fit was never clamped"
    for measurement in payload["measurements"]:
        assert "oom" not in measurement and "oom_class" not in measurement
        assert measurement["units"] == 2, "a whole batch is still priceable"


def test_the_ceiling_is_asked_with_the_headers_the_pricer_already_read():
    """One header read per window, whatever wants it: the shapes handed to the
    hook are the pricer's own readings, in PIL's `(width, height)`. A
    `count`-priced model reads no headers at all, so an impl that exposes the
    hook has them read once, before the timed section."""
    model = Ceiling(1)
    inputs = [PredictionInput(file=png_bytes(40, 30)), PredictionInput(file=b"junk")]
    packing.run_window(model, inputs, grant(unit_budget=99, unit="pixel"))
    assert model.asked[0] == [(40, 30), None], "unreadable is None, not a guess"

    counted = Ceiling(2)
    packing.run_window(counted, image_items(4), grant(unit_budget=4, unit="item"))
    assert [len(batch) for batch in counted.batches] == [2, 2]
    assert counted.asked[0] == [(40, 30)] * 4

    plain = Recorder()
    packing.run_window(plain, items(4), grant(unit_budget=4, unit="item"))
    assert [len(batch) for batch in plain.batches] == [4], "no hook, no ceiling"


def test_a_memory_clamp_and_a_shape_ceiling_merge_into_one_report(fake_torch):
    """A measurement carries one `clamped`, so when both bound, the single
    statement spans them: `from_units` is what the grant started at,
    `to_units` what ran, and `reason` names the constraint that set it."""
    fake_torch.free = 500 * MIB
    model = Ceiling(2)
    payload = packing.run_window(
        model,
        image_items(4),
        grant(unit_budget=8, mb=1000, unit="item", aggregation="count"),
    )
    first = payload["measurements"][0]
    assert first["clamped"] == {
        "from_units": 8,
        "to_units": 2,
        "reason": "index_limit",
        "free_mb": 500,
    }
    assert [len(batch) for batch in model.batches] == [2, 2]

    # `reason` is additive on the wire: its absence means the memory clamp,
    # which is what every pre-run2 worker emitted.
    fake_torch.free = 100 * MIB
    alone = packing.run_window(
        Recorder(), items(4), grant(unit_budget=8, mb=1000, aggregation="count")
    )
    assert alone["measurements"][0]["clamped"] == {
        "from_units": 8,
        "to_units": 1,
        "free_mb": 100,
    }


def test_an_impl_that_caps_itself_is_reported_as_a_ceiling_not_an_oom(
    fake_torch, fake_oom_retry
):
    """The backstop, for a ceiling the harness could not pre-empt: it reaches
    the harness through `total_index_limit_events`, and the batch is
    unpriceable *and* explained without ever setting `oom`. The other half of
    the separation: the halving counter still produces the negative sample the
    deflation path exists for, and acquires no `clamped` map."""

    class SelfCapping:
        def predict(self, inputs):
            if len(inputs) > 2:
                fake_oom_retry.record(2)
                fake_oom_retry.note_index_limit()
            else:
                fake_oom_retry.record(len(inputs))
            return [None] * len(inputs)

    payload = packing.run_window(
        SelfCapping(), items(5), grant(unit_budget=5, aggregation="count")
    )
    measurement = payload["measurements"][0]
    assert measurement["clamped"] == {
        "from_units": 5,
        "to_units": 2,
        "reason": "index_limit",
        "free_mb": 8000,
    }
    assert "units" not in measurement, "it did not run the batch it was handed"
    assert "oom" not in measurement and "oom_class" not in measurement, (
        "a shape ceiling is not a negative sample"
    )

    class Halving:
        def predict(self, inputs):
            fake_oom_retry.record(2, halvings=1)
            return [None] * len(inputs)

    halved = packing.run_window(
        Halving(), items(5), grant(unit_budget=5, aggregation="count")
    )["measurements"][0]
    assert halved["oom"] is True
    assert halved["oom_class"]["exception"] == packing.OOM_HALVING_WITNESS
    assert "clamped" not in halved

    # A window that dies after the impl hit the ceiling still reports it: the
    # failure path is where the orchestrator most needs to know the size was
    # not its choice.
    class Failing:
        def predict(self, inputs):
            fake_oom_retry.record(1)
            fake_oom_retry.note_index_limit()
            raise RuntimeError("integer out of range")

    with pytest.raises(packing.WindowFailure) as caught:
        packing.run_window(Failing(), items(4), grant(unit_budget=4))
    failed = caught.value.measurements[0]
    assert failed["clamped"]["reason"] == "index_limit"
    assert failed["clamped"]["to_units"] == 1
    assert "oom" not in failed, "`classify_oom` is right to refuse it"


def test_an_impl_that_executed_nothing_in_one_call_reports_zero_not_the_batch(
    fake_torch, fake_oom_retry
):
    """Zero is a *known* fact — the impl consulted the retry helper, got
    nothing through it and did the work by another route, which is easyOCR's
    per-image fallback — so the clamp prices it as zero rather than as the
    whole batch. Against that, a record that never moved is a *missing* fact,
    and there the whole batch is the only defensible `to_units`."""

    class FallsBackPerImage:
        def __init__(self, record):
            self.record = record

        def predict(self, inputs):
            if self.record:
                fake_oom_retry.record(0)
            fake_oom_retry.note_index_limit()
            return [None] * len(inputs)

    measurement = packing.run_window(
        FallsBackPerImage(True), items(4), grant(unit_budget=4, aggregation="count")
    )["measurements"][0]
    assert measurement["clamped"]["from_units"] == 4
    assert measurement["clamped"]["to_units"] == 0
    assert measurement["clamped"]["reason"] == "index_limit"
    assert "units" not in measurement
    assert "oom" not in measurement

    clamped = packing.run_window(
        FallsBackPerImage(False), items(4), grant(unit_budget=4, aggregation="count")
    )["measurements"][0]["clamped"]
    assert (clamped["from_units"], clamped["to_units"]) == (4, 4)
    assert clamped["reason"] == "index_limit"


def test_a_ceiling_that_cannot_be_trusted_is_no_ceiling_at_all():
    """Passive and total. A ceiling is a count of items: a bool is not one, a
    float is not one, and neither is an exception. Nor is the hook asked about
    a batch of one, where there is nothing to trim."""

    class Hostile:
        def __init__(self, answer):
            self.answer = answer

        def max_batch_for(self, shapes):
            if isinstance(self.answer, Exception):
                raise self.answer
            return self.answer

    for answer in (None, True, False, 0, -3, 2.5, "4", RuntimeError("no")):
        assert packing.impl_max_batch(Hostile(answer), [(1, 1)]) is None, answer
    assert packing.impl_max_batch(Hostile(3), [(1, 1)]) == 3
    assert packing.impl_max_batch(SimpleNamespace(), [(1, 1)]) is None
    assert packing.impl_max_batch(SimpleNamespace(max_batch_for=7), [(1, 1)]) is None

    model = Ceiling(1)
    packing.run_window(model, image_items(3), grant(unit_budget=1, unit="item"))
    assert model.asked == []
    assert [len(batch) for batch in model.batches] == [1, 1, 1]


# --- Per-batch memory frames ---


def test_a_granted_window_reports_its_pool_after_every_batch_but_the_last(
    fake_torch,
):
    """The frame the ledger needs mid-window: one sample per batch boundary,
    carrying the pool *as it grew* and a free reading taken beside it.

    The last batch is deliberately silent — the `ok` reply that follows it
    microseconds later carries the same sample, so a frame there would buy
    nothing and cost one more driver query."""
    emitted: list[dict] = []
    model = Recorder(grow=lambda count: fake_torch.grow_pool(100 * count))
    payload = packing.run_window(
        model, items(6), grant(unit_budget=2), emitted.append
    )

    assert [len(batch) for batch in model.batches] == [2, 2, 2]
    assert len(emitted) == 2, "three batches, two batch boundaries"
    # The pool the orchestrator would otherwise not hear about until the reply.
    assert [sample["reserved_mb"] for sample in emitted] == [200, 400]
    assert payload["memory"]["reserved_mb"] == 600, "the reply is still last"
    # The free reading is the frame's own, taken with the pool reading and not
    # borrowed from the clamp's pre-batch one: pairing a pre-batch free with a
    # post-batch pool understates external usage.
    for sample in emitted:
        assert sample["free_source"] == "torch"
        assert sample["free_mb"] is not None
        assert sample["total_mb"] is not None


def test_a_window_runs_identically_with_and_without_the_emitter(fake_torch):
    """The old-orchestrator direction of the skew: no emitter, no frames, and
    a payload that is the same object graph either way."""
    without = packing.run_window(Recorder(), items(5), grant(unit_budget=2))
    emitted: list[dict] = []
    with_frames = packing.run_window(
        Recorder(), items(5), grant(unit_budget=2), emitted.append
    )
    assert len(emitted) == 2
    for payload in (without, with_frames):
        payload["measurements"] = [
            {key: value for key, value in measurement.items()
             if key != "duration_ms"}
            for measurement in payload["measurements"]
        ]
    assert without == with_frames


def test_a_worker_that_can_measure_nothing_emits_nothing():
    """No torch, no sample, no frame — the same silence a worker with no GPU
    answers every other memory-sensing field with."""
    emitted: list[dict] = []
    packing.run_window(Recorder(), items(6), grant(unit_budget=2), emitted.append)
    assert emitted == []


def test_the_emitter_is_bound_to_the_request_in_flight():
    """`_memory_frame_emitter` is the whole desynchronization argument: it
    writes the id it was built with, and it does not exist at all unless the
    orchestrator asked for the frames in its handshake."""
    from inferio_worker import __main__ as worker_main
    from inferio_worker import protocol

    assert worker_main._memory_frame_emitter(io.BytesIO(), 7, False) is None

    stream = io.BytesIO()
    emit = worker_main._memory_frame_emitter(stream, 7, True)
    emit({"free_mb": 10, "reserved_mb": 3})
    stream.seek(0)
    frame = protocol.read_frame(stream)
    assert frame == {
        "type": "memory",
        "id": 7,
        "memory": {"free_mb": 10, "reserved_mb": 3},
    }
    assert protocol.read_frame(stream) is None, "exactly one frame"
