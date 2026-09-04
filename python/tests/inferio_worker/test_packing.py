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


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


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
        """Release the pool blocks no live tensor is using.

        The real allocator returns `reserved - allocated`; a test that wants
        the whole pool released zeroes `allocated` first (which is what a
        window boundary looks like in reality — the impl's tensors are gone,
        the pool that held them is not).
        """
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
    """Stand-in for `inferio.impl.utils` as the harness observes it.

    The harness reads `last_oom_retry()` through `sys.modules` (it must never
    import the real `inferio` package), so a namespace with that one function is
    the whole contract. `record()` plays the role of a `run_with_oom_retry` call
    completing: it bumps the generation, which is how the harness distinguishes
    "the impl consulted the retry helper for this batch" from a stale reading.
    """

    def __init__(self):
        self.generation = 0
        self.slot = None
        self.total = 0

    def record(self, largest, halvings=0):
        self.generation += 1
        self.slot = (self.generation, largest, halvings)
        self.total += halvings

    def last_oom_retry(self):
        return self.slot

    def total_oom_halvings(self):
        """Halvings across every call, as the real helper accumulates them.

        This is the only reading that survives an impl calling the helper twice
        in one `predict` — the per-call record above keeps the last call only.
        """
        return self.total


@pytest.fixture(autouse=True)
def no_ambient_accelerator():
    """Every test in this module describes the harness, not this machine.

    `memory._free_total_mb` prefers NVML to torch and memoizes the module for
    the life of the *process*, and `_torch_cuda` answers off whatever `torch`
    happens to be in `sys.modules`. A test module that ran earlier and
    imported torch (`tests/inferio/impl`) therefore leaves this process able
    to read the developer's real board, and the clamp, `free_mb` and
    `free_mb_at_failure` assertions below start measuring that board instead
    of the fixture's — a nine-test failure that depends on nothing but
    collection order. It is the *default* order: the project's own `pytest`
    invocation collects `tests/` whole (pyproject `testpaths`).

    So the ambient driver is removed for every test here, and the fakes are
    injected on top of nothing.
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

    def __init__(self, fail_on=None, oom=False, wrong_count=False, grow=None):
        self.batches: list[list] = []
        self.fail_on = fail_on
        self.oom = oom
        self.wrong_count = wrong_count
        self.grow = grow

    def predict(self, inputs):
        self.batches.append(list(inputs))
        if self.grow is not None:
            self.grow(len(inputs))
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


# ---------------------------------------------------------------------------
# Pricing
# ---------------------------------------------------------------------------


def test_pixel_pricing_reads_headers_without_decoding():
    inputs = [
        PredictionInput(file=png_bytes(40, 30)),
        PredictionInput(file=png_bytes(100, 100)),
    ]
    assert packing.price_inputs(inputs, "pixel") == [1200, 10_000]


def test_pixel_pricing_accepts_a_path(tmp_path):
    path = tmp_path / "a.png"
    path.write_bytes(png_bytes(10, 20))
    assert packing.price_inputs([PredictionInput(file=str(path))], "pixel") == [200]


def test_an_unreadable_image_is_charged_the_largest_seen_never_zero():
    """One corrupt file must not fail the window, and must not be free
    either: a zero-unit item would pack without limit."""
    inputs = [
        PredictionInput(file=b"not an image"),
        PredictionInput(file=png_bytes(50, 40)),
    ]
    assert packing.price_inputs(inputs, "pixel") == [2000, 2000]
    # No readable input at all: the flat fallback.
    assert packing.price_inputs([PredictionInput(file=b"junk")], "pixel") == [
        packing.UNREADABLE_PIXEL_UNITS
    ]
    assert packing.price_inputs([PredictionInput()], "pixel") == [
        packing.UNREADABLE_PIXEL_UNITS
    ]


# ---------------------------------------------------------------------------
# Per-item pixel canvas (run2 R7)
# ---------------------------------------------------------------------------


def test_pricing_without_a_canvas_is_unchanged():
    """Absent = uncapped, which is what every model did before run2."""
    inputs = [PredictionInput(file=png_bytes(8000, 6000))]
    assert packing.price_inputs(inputs, "pixel") == [48_000_000]
    assert packing.price_inputs(inputs, "pixel", None) == [48_000_000]
    assert packing.price_inputs(inputs, "pixel", 0) == [48_000_000]


def test_a_48_megapixel_item_is_priced_at_the_canvas():
    """The whole point of R7: nemotron tiles at (6 + thumbnail) x 512^2, so a
    48 MP scan costs it 1.84 MP, not 26x that (run1 report §4, Q3/W1)."""
    inputs = [
        PredictionInput(file=png_bytes(8000, 6000)),
        PredictionInput(file=png_bytes(1024, 1024)),
    ]
    assert packing.price_inputs(inputs, "pixel", 1_835_008) == [1_835_008, 1_048_576]


def test_the_canvas_lets_large_images_pack():
    """The other half: 58 of 110 batches held a single item because one large
    item exhausted the whole budget on its own."""
    inputs = [PredictionInput(file=png_bytes(4000, 3000)) for _ in range(4)]
    uncapped = packing.price_inputs(inputs, "pixel")
    capped = packing.price_inputs(inputs, "pixel", 1_835_008)
    assert packing.plan_batches(uncapped, "sum", 4_000_000) == [[0], [1], [2], [3]]
    assert packing.plan_batches(capped, "sum", 4_000_000) == [[0, 1], [2, 3]]


def test_the_canvas_also_caps_the_unreadable_input_fallback():
    """The fallback is the same quantity by another route, so it is capped by
    the same ceiling — otherwise one corrupt file re-creates the batch of one
    the cap exists to prevent."""
    inputs = [
        PredictionInput(file=png_bytes(8000, 6000)),
        PredictionInput(file=b"not an image"),
    ]
    assert packing.price_inputs(inputs, "pixel", 1_835_008) == [1_835_008, 1_835_008]
    assert packing.price_inputs([PredictionInput()], "pixel", 1_000_000) == [1_000_000]


def test_a_canvas_prices_nothing_outside_pixel_units():
    text = PredictionInput(data="x" * 400)
    assert packing.price_inputs([text], "token", 1_835_008) == [100]
    assert packing.price_inputs(items(3), "item", 1_835_008) == [1, 1, 1]


def test_the_granted_canvas_wins_over_the_impls_own():
    """The grant is authoritative: it states the canvas the orchestrator
    resolved — the registry's declaration, else the figure this worker itself
    reported at load — and taking it is what makes the host's window and this
    worker's batches one denomination by construction rather than by two
    resolutions agreeing."""
    impl = SimpleNamespace(max_pixels=999_999)
    assert (
        packing.resolve_canvas_pixels({"canvas_pixels": 1_835_008}, impl, "pixel")
        == 1_835_008
    )


def test_the_impls_own_resolution_is_the_documented_fallback():
    """Tier 2, for a model whose canvas lives in a processor downloaded with
    the weights (`doctr/dots_ocr`) rather than in the registry."""
    # One level: `instance.embedder.max_pixels` (qwen3-vl-embedding).
    one_level = SimpleNamespace(embedder=SimpleNamespace(max_pixels=1_843_200))
    assert packing.resolve_canvas_pixels({}, one_level, "pixel") == 1_843_200
    # Two levels: `instance.model.processor.max_pixels` (the HF VLM shape).
    two_levels = SimpleNamespace(
        model=SimpleNamespace(processor=SimpleNamespace(max_pixels=11_289_600))
    )
    assert packing.resolve_canvas_pixels({}, two_levels, "pixel") == 11_289_600
    # Nothing to find is uncapped, exactly as before this field existed.
    assert packing.resolve_canvas_pixels({}, SimpleNamespace(), "pixel") is None
    assert packing.resolve_canvas_pixels({}, one_level, "item") is None


def test_an_implausible_canvas_reading_is_refused():
    """Too *small* a cap under-prices an item, which over-admits — the one
    error direction the ledger cannot absorb — so a suspiciously small
    attribute is treated as a misidentified one."""
    for value in (4, 1024, packing.CANVAS_FLOOR_PIXELS - 1, 0, -1, True, "1843200"):
        impl = SimpleNamespace(max_pixels=value)
        assert packing.resolve_canvas_pixels({}, impl, "pixel") is None, value
    at_the_floor = SimpleNamespace(max_pixels=packing.CANVAS_FLOOR_PIXELS)
    assert (
        packing.resolve_canvas_pixels({}, at_the_floor, "pixel")
        == packing.CANVAS_FLOOR_PIXELS
    )


def test_canvas_introspection_never_raises():
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
    inputs = [PredictionInput(file=png_bytes(4000, 3000)) for _ in range(4)]
    payload = packing.run_window(
        model,
        inputs,
        grant(unit_budget=4_000_000, unit="pixel", aggregation="sum",
              canvas_pixels=1_835_008),
    )
    assert [len(batch) for batch in model.batches] == [2, 2]
    assert payload["measurements"][0]["units"] == 2 * 1_835_008


# ---------------------------------------------------------------------------
# Size homogeneity under the canvas (run2 D1-b)
# ---------------------------------------------------------------------------
#
# The cap prices every item at or above the canvas alike, which is correct
# pricing and removes exactly the size information the `max-times-count`
# bucketing sorts on. These describe the two halves of the fix: the raw price
# survives beside the capped one as a *tiebreaker*, and an impl that pads a
# batch to its largest member while stating no canvas of its own is named in
# the log once.

# One canvas, one pair of sizes, used by every test below: both are above the
# canvas, so both price at exactly it, and their raw areas differ by 2.78x.
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
    priced = packing.price_window(mixed_window(), "pixel", D1B_CANVAS)
    assert priced.units == [D1B_CANVAS] * 4, "the price is the capped one"
    assert priced.raw == [3_000_000, 1_080_000, 3_000_000, 1_080_000]


def test_price_window_is_the_same_list_when_nothing_is_capped():
    """No cap, no second reading — and `units is raw`, so no caller can drift
    them apart."""
    for unit, canvas in (("pixel", None), ("pixel", 0), ("item", D1B_CANVAS)):
        priced = packing.price_window(mixed_window(), unit, canvas)
        assert priced.units is priced.raw
    assert packing.price_window(mixed_window(), "pixel").units == [
        3_000_000,
        1_080_000,
        3_000_000,
        1_080_000,
    ]


def test_equally_priced_items_are_ordered_by_raw_size():
    """The tiebreaker: four items priced alike bucket by raw area, so the two
    3 MP sheets share a batch and the two 1 MP scans share the other. Without
    it the sort has nothing to separate them and input order interleaves."""
    units = [D1B_CANVAS] * 4
    raw = [3_000_000, 1_080_000, 3_000_000, 1_080_000]
    budget = 2 * D1B_CANVAS
    assert packing.plan_batches(units, "max-times-count", budget) == [[0, 1], [2, 3]]
    assert packing.plan_batches(
        units, "max-times-count", budget, tiebreak=raw
    ) == [[0, 2], [1, 3]]


def test_the_tiebreaker_never_reorders_across_a_price():
    """Secondary means secondary: a cheaper item never overtakes a dearer one
    however large it is raw, so the batch a bucket is priced at is unchanged."""
    units = [10, 100, 10]
    raw = [999_999, 1, 999_999]
    plan = packing.plan_batches(units, "max-times-count", 1000, tiebreak=raw)
    assert plan[0][0] == 1, "the 100-unit item still leads"


def test_the_tiebreaker_is_ignored_where_it_cannot_apply():
    """A mis-sized tiebreaker (a caller bug) and the aggregations that do not
    sort at all are unaffected — the plan is the one the primary key gives."""
    units = [5, 5, 5, 5]
    assert packing.plan_batches(
        units, "max-times-count", 10, tiebreak=[9, 9]
    ) == packing.plan_batches(units, "max-times-count", 10)
    assert packing.plan_batches(units, "sum", 10, tiebreak=[4, 3, 2, 1]) == [
        [0, 1],
        [2, 3],
    ]
    assert packing.plan_batches(units, "count", 2, tiebreak=[4, 3, 2, 1]) == [
        [0, 1],
        [2, 3],
    ]


def test_a_capped_window_buckets_size_homogeneously(fake_torch):
    """End to end through `run_window`: the batches an impl that pads to a
    common size is handed hold one raw size each, so its tensor is the size
    the batch was priced at."""
    model = Recorder()
    inputs = mixed_window()
    packing.run_window(
        model,
        inputs,
        grant(
            unit_budget=2 * D1B_CANVAS,
            unit="pixel",
            aggregation="max-times-count",
            canvas_pixels=D1B_CANVAS,
        ),
    )
    sizes = [
        sorted(len(entry.file) for entry in batch) for batch in model.batches
    ]
    assert len(sizes) == 2
    for batch in sizes:
        assert len(set(batch)) == 1, "every batch holds one raw size"


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


def test_an_impl_that_pads_and_states_no_canvas_is_named_once(
    fake_torch, unlogged_guard, caplog
):
    """The whole batch fits one budget here, so the plan *has* to mix sizes —
    which is the shape D1-b measured, and the shape the log line describes."""
    with caplog.at_level(logging.WARNING, logger="inferio_worker.packing"):
        run_padding_window(Padding(), mixed_window())
        run_padding_window(Padding(), mixed_window())
    warnings = [
        record
        for record in caplog.records
        if "pads a batch to its largest member" in record.getMessage()
    ]
    assert len(warnings) == 1, "once per process, not once per batch"
    message = warnings[0].getMessage()
    assert "1080000 to 3000000 pixels" in message
    assert "2.8x" in message


def test_an_impl_that_states_its_canvas_is_not_named(
    fake_torch, unlogged_guard, caplog
):
    """`inferio.impl.eocr`'s shape after the D1-b fix: it pads, and it states
    a canvas — which the protocol doc makes a promise to bound every input by
    before the tensor exists. A promise is what exempts it."""
    with caplog.at_level(logging.WARNING, logger="inferio_worker.packing"):
        run_padding_window(Padding(canvas=D1B_CANVAS), mixed_window())
    assert not [
        record
        for record in caplog.records
        if "pads a batch to its largest member" in record.getMessage()
    ]


def test_the_guard_is_silent_without_a_cap_or_a_mix(
    fake_torch, unlogged_guard, caplog
):
    """Two more ways to be uninteresting: no canvas in force at all (nothing
    was priced flat, so nothing is under-priced), and a batch whose raw sizes
    are within the 2x ratio."""
    with caplog.at_level(logging.WARNING, logger="inferio_worker.packing"):
        run_padding_window(Padding(), mixed_window(), canvas=None)
        run_padding_window(
            Padding(),
            [PredictionInput(file=png_bytes(*BIG)) for _ in range(4)],
        )
    assert not [
        record
        for record in caplog.records
        if "pads a batch to its largest member" in record.getMessage()
    ]


def test_token_and_item_and_audio_pricing():
    text = PredictionInput(data="x" * 400)
    assert packing.price_inputs([text], "token") == [100]
    assert packing.price_inputs([PredictionInput()], "token") == [1], "never zero"
    assert packing.price_inputs(items(3), "item") == [1, 1, 1]
    assert packing.price_inputs(items(2), "audio-second") == [
        packing.AUDIO_FALLBACK_SECONDS
    ] * 2
    # An unknown unit from a newer orchestrator degrades to per-item packing.
    assert packing.price_inputs(items(2), "furlong") == [1, 1]


def test_batch_units_follows_the_aggregation():
    units = [10, 4, 6]
    assert packing.batch_units([0, 1, 2], units, "count") == 3
    assert packing.batch_units([0, 1, 2], units, "sum") == 20
    assert packing.batch_units([0, 1, 2], units, "max-times-count") == 30
    assert packing.batch_units([], units, "sum") == 0


# ---------------------------------------------------------------------------
# Packing
# ---------------------------------------------------------------------------


def test_count_packing_is_an_item_count():
    plan = packing.plan_batches([1] * 7, "count", 3)
    assert plan == [[0, 1, 2], [3, 4, 5], [6]]


def test_sum_packing_is_a_greedy_fifo_running_total():
    plan = packing.plan_batches([3, 4, 2, 1], "sum", 8)
    # 3+4 = 7 fits, +2 would be 9 -> new batch; 2+1 = 3 fits.
    assert plan == [[0, 1], [2, 3]]


def test_max_times_count_buckets_largest_first():
    """The bucketing that retires easyOCR's enable_batching stopgap: one big
    scan goes through in a small batch instead of taxing the thumbnails."""
    units = [100, 10, 10, 10, 10]
    plan = packing.plan_batches(units, "max-times-count", 100)
    # Largest-first: 100 alone (100*2 > 100), then the four 10s (10*4 = 40).
    assert plan == [[0], [1, 2, 3, 4]]
    # Every batch respects max x count.
    for batch in plan:
        assert packing.batch_units(batch, units, "max-times-count") <= 100


def test_a_single_over_budget_item_goes_through_alone():
    plan = packing.plan_batches([500, 1, 1], "sum", 10)
    assert plan[0] == [0], "never smaller than one item"
    assert packing.batch_units(plan[0], [500, 1, 1], "sum") > 10


def test_the_user_cap_bounds_items_on_top_of_the_unit_budget():
    plan = packing.plan_batches([1] * 6, "sum", 1000, cap_items=2)
    assert plan == [[0, 1], [2, 3], [4, 5]]
    # A cap of 1 means one item per batch whatever the budget says.
    assert packing.plan_batches([1] * 3, "sum", 1000, cap_items=1) == [[0], [1], [2]]
    # A non-positive cap is not an opinion.
    assert packing.plan_batches([1] * 3, "count", 3, cap_items=0) == [[0, 1, 2]]


def test_the_cap_and_max_times_count_bucketing_compose():
    """Both bounds hold at once, and the cap is applied to the *bucketed* order
    rather than the input order: the batches are still similarly-sized
    neighbours, just shorter."""
    units = [100, 100, 10, 10, 10, 10]
    plan = packing.plan_batches(units, "max-times-count", 1000, cap_items=2)
    # Largest-first, two per batch: the 100s pair up (100 * 2 = 200 <= 1000),
    # then the 10s in pairs.
    assert plan == [[0, 1], [2, 3], [4, 5]]
    for batch in plan:
        assert len(batch) <= 2
        assert packing.batch_units(batch, units, "max-times-count") <= 1000
    # The unit budget can still bind tighter than the cap.
    tight = packing.plan_batches(units, "max-times-count", 100, cap_items=4)
    assert tight[0] == [0], "100 * 2 would exceed the budget"
    assert sorted(index for batch in tight for index in batch) == list(range(6))


def test_every_input_is_planned_exactly_once():
    units = [7, 3, 9, 1, 5, 5]
    for aggregation in ("count", "sum", "max-times-count"):
        plan = packing.plan_batches(units, aggregation, 10)
        flat = [index for batch in plan for index in batch]
        assert sorted(flat) == list(range(len(units))), aggregation


# ---------------------------------------------------------------------------
# Defensive clamp
# ---------------------------------------------------------------------------


def test_the_clamp_shrinks_when_free_memory_fell(fake_torch):
    fake_torch.free = 250 * MIB
    shrunk = packing.clamp_to_live_memory(64, 1000)
    assert shrunk.units == 16, "250/1000 of 64"
    assert shrunk.clamped == {"from_units": 64, "to_units": 16, "free_mb": 250}
    # Never below one item.
    assert packing.clamp_to_live_memory(2, 1_000_000).units == 1


def test_the_clamp_never_grows_and_degrades_to_a_no_op(fake_torch):
    fake_torch.free = 8000 * MIB
    assert packing.clamp_to_live_memory(64, 1000).units == 64, "shrink-only"
    assert packing.clamp_to_live_memory(64, None).units == 64, "no grant MB, no rule"
    assert packing.clamp_to_live_memory(64, 0).units == 64
    for grant_mb in (1000, None, 0):
        assert packing.clamp_to_live_memory(64, grant_mb).clamped is None


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


def test_a_budget_of_one_cannot_be_clamped_and_says_so(fake_torch):
    """The floor is one item, so a budget already at one is never shrunk —
    and a batch that ran at its granted budget is not a clamped batch,
    however little memory there was."""
    fake_torch.free = 1 * MIB
    live = packing.clamp_to_live_memory(1, 1000)
    assert live.units == 1
    assert live.clamped is None
    assert live.free_mb == 1, "the reading is still reported"


def test_every_measurement_carries_the_pre_batch_free_reading(fake_torch):
    """R5's wire half: the reading the clamp took rides every measurement, so
    `external_mb` refreshes at response cadence (run1 report §4, T3)."""
    fake_torch.free = 7000 * MIB
    payload = packing.run_window(
        Recorder(), items(6), grant(unit_budget=2, aggregation="count")
    )
    assert len(payload["measurements"]) == 3
    for measurement in payload["measurements"]:
        assert measurement["free_mb"] == 7000
        assert measurement["free_source"] == "torch"
        assert "clamped" not in measurement


def test_a_clamped_batch_reports_what_the_clamp_did(fake_torch):
    fake_torch.free = 100 * MIB
    payload = packing.run_window(
        Recorder(), items(4), grant(unit_budget=8, mb=1000, aggregation="count")
    )
    first = payload["measurements"][0]
    assert first["clamped"] == {"from_units": 8, "to_units": 1, "free_mb": 100}
    assert first["free_mb"] == 100


def test_a_failed_batch_still_reports_its_pre_batch_reading(fake_torch):
    """The failure paths carry it too: a batch that died is exactly when the
    orchestrator wants to know what the board looked like going in."""
    fake_torch.free = 512 * MIB

    class Failing:
        def predict(self, inputs):
            raise ValueError("fixture failure")

    with pytest.raises(packing.WindowFailure) as caught:
        packing.run_window(Failing(), items(2), grant(unit_budget=2))
    assert caught.value.measurements[0]["free_mb"] == 512

    with pytest.raises(packing.WindowFailure) as caught:
        packing.run_window(Recorder(wrong_count=True), items(2), grant(unit_budget=2))
    assert caught.value.measurements[0]["free_mb"] == 512


def test_the_grantless_path_reports_no_pre_batch_reading(fake_torch):
    """It takes no clamp reading — there is no grant to clamp against — so it
    reports none, rather than a post-batch reading under a pre-batch name."""
    state = memory.begin_batch()
    payload = memory.finish_batch(state, items=3)
    assert "free_mb" not in payload["measurements"][0]
    assert payload["memory"]["free_mb"] is not None, "the sample still carries one"


def test_the_clamp_shrinks_the_batches_actually_run(fake_torch):
    fake_torch.free = 100 * MIB
    model = Recorder()
    payload = packing.run_window(
        model, items(8), grant(unit_budget=8, mb=1000, aggregation="count")
    )
    assert [len(batch) for batch in model.batches] == [1] * 8, (
        "the clamp cut the budget to one item per batch"
    )
    assert payload["outputs"] == list(range(8))


# ---------------------------------------------------------------------------
# Running a window
# ---------------------------------------------------------------------------


def test_a_window_is_split_into_batches_and_order_is_restored(fake_torch):
    model = Recorder()
    payload = packing.run_window(model, items(5), grant(unit_budget=2))
    assert [len(batch) for batch in model.batches] == [2, 2, 1]
    assert payload["outputs"] == [0, 1, 2, 3, 4]
    assert len(payload["measurements"]) == 3
    assert [m["items"] for m in payload["measurements"]] == [2, 2, 1]
    assert [m["units"] for m in payload["measurements"]] == [2, 2, 1]


def test_bucketed_output_order_is_restored(fake_torch):
    """max-times-count packing reorders items; the dispatcher splits outputs
    by position, so the reply must be in input order regardless."""
    inputs = [
        PredictionInput(data="small-a", file=png_bytes(10, 10)),
        PredictionInput(data="huge", file=png_bytes(400, 400)),
        PredictionInput(data="small-b", file=png_bytes(10, 10)),
    ]
    model = Recorder()
    payload = packing.run_window(
        model,
        inputs,
        grant(unit_budget=400, unit="pixel", aggregation="max-times-count"),
    )
    assert payload["outputs"] == ["small-a", "huge", "small-b"]
    # The huge item really did travel in its own batch.
    assert any(
        len(batch) == 1 and batch[0].data == "huge" for batch in model.batches
    )


def test_units_are_priced_in_the_declared_dimension(fake_torch):
    inputs = [PredictionInput(file=png_bytes(20, 10)) for _ in range(3)]
    payload = packing.run_window(
        Recorder(),
        inputs,
        grant(unit_budget=400, unit="pixel", aggregation="sum"),
    )
    assert [m["units"] for m in payload["measurements"]] == [400, 200]
    assert [m["items"] for m in payload["measurements"]] == [2, 1]


def test_a_failing_batch_reports_the_oom_flag_and_the_window_prefix(fake_torch):
    model = Recorder(fail_on=2, oom=True)
    with pytest.raises(packing.WindowFailure) as caught:
        packing.run_window(model, items(6), grant(unit_budget=2))
    failure = caught.value
    assert packing.OOM_WINDOW_PREFIX in str(failure)
    assert len(failure.measurements) == 2, "the batch that ran plus the one that failed"
    assert failure.measurements[0].get("oom") is None
    assert failure.measurements[1]["oom"] is True


def test_a_batch_one_oom_keeps_its_existing_prefix(fake_torch):
    """The single-item case already carries INFERENCE_OOM_BATCH_SIZE_1 from
    inferio.impl.utils; the harness must not double-wrap it."""

    class SingleOom:
        def predict(self, inputs):
            raise RuntimeError("INFERENCE_OOM_BATCH_SIZE_1: single input OOM")

    with pytest.raises(packing.WindowFailure) as caught:
        packing.run_window(SingleOom(), items(1), grant(unit_budget=1))
    assert str(caught.value).startswith("INFERENCE_OOM_BATCH_SIZE_1:")
    assert packing.OOM_WINDOW_PREFIX not in str(caught.value)
    assert caught.value.measurements[0]["oom"] is True


def test_a_non_oom_failure_is_not_flagged(fake_torch):
    model = Recorder(fail_on=1)
    with pytest.raises(packing.WindowFailure) as caught:
        packing.run_window(model, items(2), grant(unit_budget=2))
    assert "fixture failure" in str(caught.value)
    assert packing.OOM_WINDOW_PREFIX not in str(caught.value)
    assert caught.value.measurements[0].get("oom") is None


def test_the_oom_classifier_covers_the_non_cuda_backends(fake_torch):
    """The negative-signal widening (docs/unified-memory-admission.md).

    On MPS and on CPU the out-of-memory condition arrives as an untyped
    `RuntimeError` — or as the interpreter's own `MemoryError` — and the
    orchestrator's deflation path only ever hears about it through this
    flag, so the classifier is the whole signal. It stays conservative:
    a `RuntimeError` that says nothing about memory is not one.
    """
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

        class Failing:
            def predict(self, inputs):
                raise failure

        with pytest.raises(packing.WindowFailure) as caught:
            packing.run_window(Failing(), items(2), grant(unit_budget=2))
        assert caught.value.measurements[0]["oom"] is True, name
        assert packing.OOM_WINDOW_PREFIX in str(caught.value), name

    class Buggy:
        def predict(self, inputs):
            raise RuntimeError("shape mismatch in forward()")

    with pytest.raises(packing.WindowFailure) as caught:
        packing.run_window(Buggy(), items(2), grant(unit_budget=2))
    assert caught.value.measurements[0].get("oom") is None
    assert packing.OOM_WINDOW_PREFIX not in str(caught.value)


# ---------------------------------------------------------------------------
# Structural out-of-memory classification (run2 R3)
# ---------------------------------------------------------------------------


class FakeTorchOom(RuntimeError):
    """A stand-in for `torch.OutOfMemoryError`.

    The real one is a `RuntimeError` subclass exported as `torch.OutOfMemoryError`
    and aliased as `torch.cuda.OutOfMemoryError` — the same class object on a
    CUDA build and on a HIP one, which is why the classifier needs no ROCm
    entry of its own.
    """

    __module__ = "torch"


@pytest.fixture
def fake_torch_with_oom_type(fake_torch):
    """`fake_torch` whose module also exports the typed OOM class."""
    sys.modules["torch"].OutOfMemoryError = FakeTorchOom
    sys.modules["torch"].cuda.OutOfMemoryError = FakeTorchOom
    yield fake_torch


def test_a_typed_allocator_exception_classifies_structurally(fake_torch_with_oom_type):
    """The tier that needs no text at all: the exception *is* the answer."""
    fake_torch_with_oom_type.free = 137 * MIB
    classified = packing.classify_oom(FakeTorchOom("anything at all"))
    assert classified["source"] == packing.OOM_SOURCE_TYPED
    assert classified["exception"] == "torch.FakeTorchOom"
    assert classified["free_mb_at_failure"] == 137, "the live reading at failure"
    assert classified["device"] == "cuda"


def test_the_typed_tier_holds_on_a_hip_build(fake_rocm_torch):
    """ROCm raises the same class, so one entry covers both backends."""
    sys.modules["torch"].OutOfMemoryError = FakeTorchOom
    classified = packing.classify_oom(FakeTorchOom("HIP out of memory"))
    assert classified["source"] == packing.OOM_SOURCE_TYPED
    assert classified["device"] == "rocm"


def test_host_ram_exhaustion_is_typed(fake_torch):
    """`MemoryError` is a builtin no library could hand us, so it is a type
    test even though the CPU allocator's other form is a message one."""
    classified = packing.classify_oom(MemoryError())
    assert classified["source"] == packing.OOM_SOURCE_TYPED
    assert classified["exception"] == "MemoryError"


def test_our_own_markers_classify_as_markers(fake_torch):
    """`INFERENCE_OOM_*` is our code restating a classification it already
    made one frame lower, so it is structural rather than prose."""

    class InferenceOOMError(RuntimeError):
        __module__ = "inferio.impl.utils"

    by_type = packing.classify_oom(InferenceOOMError("reworded by an impl"))
    assert by_type["source"] == packing.OOM_SOURCE_MARKER
    assert by_type["exception"] == "inferio.impl.utils.InferenceOOMError"

    by_text = packing.classify_oom(
        RuntimeError(f"{packing.OOM_WINDOW_PREFIX} out of GPU memory on 8 inputs")
    )
    assert by_text["source"] == packing.OOM_SOURCE_MARKER


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


def test_the_message_tier_only_matches_driver_shaped_text(fake_torch):
    """Every form a backend with no typed exception actually emits."""
    driver_shaped = (
        "CUDA out of memory. Tried to allocate 2.00 GiB",
        "CUDA error: out of memory",
        "HIP out of memory. Tried to allocate 512.00 MiB",
        "HIP error: out of memory",
        "MPS backend out of memory (MPS allocated: 18.09 GB)",
        "[enforce fail at alloc_cpu.cpp:117] . DefaultCPUAllocator: can't "
        "allocate memory: you tried to allocate 12884901888 bytes.",
        "cublas runtime error: CUBLAS_STATUS_ALLOC_FAILED",
        "cuDNN error: CUDNN_STATUS_ALLOC_FAILED",
        "cudaErrorMemoryAllocation",
    )
    for text in driver_shaped:
        classified = packing.classify_oom(RuntimeError(text))
        assert classified is not None, text
        assert classified["source"] == packing.OOM_SOURCE_PATTERN, text


def test_every_device_wording_of_out_of_memory_is_still_an_oom(fake_torch):
    """The spellings a fixed substring list loses.

    Each of these is emitted by something that ships in this project's own
    venv, and each is a real out-of-memory condition: a missed one leaves the
    orchestrator over-admitting against a model that cannot take it, which is
    the failure R3 is *not* allowed to introduce while fixing B11.
    """
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


def test_a_bare_out_of_memory_substring_is_not_an_oom(fake_torch):
    """B11, verbatim: run1 measured this exact wording deflating a healthy
    model 15 times on a board with 96 GB free (run1 report §4, Q1)."""
    healthy = ValueError(
        "refusing merged batch of 8: the caption cache is out of memory slots"
    )
    assert packing.classify_oom(healthy) is None


def test_a_device_token_must_be_a_whole_word(fake_torch):
    """The device-scoped rule is what keeps "out of memory" usable at all, so
    the scope has to be a real token: an English word that merely *contains*
    one ("chip", "ship", "relationship") is not a device, and B11's message
    with any of them in it must stay a non-OOM."""
    for word in ("chip", "ship", "relationship", "hipster"):
        healthy = ValueError(
            f"refusing merged batch: the {word} cache is out of memory slots"
        )
        assert packing.classify_oom(healthy) is None, word


def test_an_absorbed_halving_is_a_marker_with_no_exception(fake_torch):
    """A batch that *succeeded* while the impl halved internally has nothing
    to name, so the witness is named instead of an exception being invented."""
    classified = packing.classify_oom(None, absorbed=2)
    assert classified["source"] == packing.OOM_SOURCE_MARKER
    assert classified["exception"] == packing.OOM_HALVING_WITNESS
    assert packing.classify_oom(None, absorbed=0) is None


def test_a_failed_batch_carries_its_class_on_the_measurement(
    fake_torch_with_oom_type,
):
    fake_torch_with_oom_type.free = 64 * MIB

    class Failing:
        def predict(self, inputs):
            raise FakeTorchOom("CUDA out of memory")

    with pytest.raises(packing.WindowFailure) as caught:
        packing.run_window(Failing(), items(2), grant(unit_budget=2))
    measurement = caught.value.measurements[0]
    assert measurement["oom"] is True
    assert measurement["oom_class"]["source"] == packing.OOM_SOURCE_TYPED
    assert measurement["oom_class"]["free_mb_at_failure"] == 64


def test_a_non_memory_failure_carries_no_class_at_all(fake_torch):
    """The half of R3 the orchestrator acts on: no class and no flag means
    *this was not a memory event*, so nothing may deflate on it."""

    class Failing:
        def predict(self, inputs):
            raise ValueError("the caption cache is out of memory slots")

    with pytest.raises(packing.WindowFailure) as caught:
        packing.run_window(Failing(), items(2), grant(unit_budget=2))
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


def test_a_wrong_output_count_fails_the_window(fake_torch):
    with pytest.raises(packing.WindowFailure) as caught:
        packing.run_window(Recorder(wrong_count=True), items(2), grant(unit_budget=2))
    assert "returned 0 outputs" in str(caught.value)
    assert "units" not in caught.value.measurements[0], (
        "a batch that did not complete is unpriceable, whatever it failed of"
    )


def test_a_failed_batch_is_never_priced(fake_torch):
    """A mid-batch failure that is *not* an OOM (an assertion, a processor
    rejecting an input) would otherwise enter the fit as a clean high-water
    sample whose peak stops wherever the call gave up — an under-stated cost for
    the units reported, which drags the fitted slope low and over-admits. So no
    failure path prices its batch; only the flags carry over."""
    model = Recorder(fail_on=2)
    with pytest.raises(packing.WindowFailure) as caught:
        packing.run_window(model, items(4), grant(unit_budget=2))
    measurements = caught.value.measurements
    assert len(measurements) == 2
    assert measurements[0]["units"] == 2, "the batch that completed is priced"
    assert "units" not in measurements[1], "the batch that failed is not"
    assert measurements[1].get("oom") is None, "and it was not an OOM"
    # Its peaks are still reported — they are the only record of how far the
    # call got, and the orchestrator uses them for diagnostics.
    assert measurements[1]["items"] == 2


def test_throughput_collapse_flags_a_spilling_growth_batch(fake_torch):
    """The WDDM synthetic negative: on Windows an over-budget allocation
    silently spills to system RAM, so over-admission shows up as a throughput
    collapse instead of an exception.

    The tail batch is the other half of the rule: a window's last, smaller
    batch pays the same fixed per-call overhead over less work, so its
    units/sec is legitimately lower. Comparing it would deflate a healthy
    worker once per window forever, so only upward-or-equal steps compare.
    """
    calls = {"n": 0}

    def grow(_size):
        fake_torch.grow_pool(100)

    class Slowing:
        """Each pool-growing batch takes 4x longer than the previous one."""

        def predict(self, inputs):
            calls["n"] += 1
            grow(len(inputs))
            import time

            time.sleep(0.01 * (4 ** (calls["n"] - 1)))
            return [None] * len(inputs)

    # 5 items at a budget of 2 -> batches of 2, 2, 1: two comparable steps and
    # one non-comparable tail.
    payload = packing.run_window(Slowing(), items(5), grant(unit_budget=2))
    flags = [m.get("throughput_collapse") for m in payload["measurements"]]
    assert flags[0] is None, "the first growing batch has no comparator"
    assert flags[1] is True, "units/sec fell far below the previous growth batch"
    assert not flags[2], (
        "the 1-unit tail batch is a downward step and is never comparable, "
        "however slow it is"
    )


def test_throughput_collapse_stays_active_on_a_rocm_worker(fake_rocm_torch):
    """The collapse detector is platform-neutral by design
    (docs/rocm-batch-calibration-parity.md, D8): on ROCm the crisp hipMalloc
    OOM is the primary negative signal, but the comparator stays live as a
    generic over-admission guard. A HIP-shaped worker must flag the same
    spilling growth batch a CUDA-shaped one does — same scenario as above,
    different torch. This also proves the HIP memory-tier differences (NVML
    refused, amdgpu sysfs absent, free/total from torch) do not starve the
    detector of the pool-growth signal it keys on."""
    calls = {"n": 0}

    class Slowing:
        def predict(self, inputs):
            calls["n"] += 1
            fake_rocm_torch.grow_pool(100)
            import time

            time.sleep(0.01 * (4 ** (calls["n"] - 1)))
            return [None] * len(inputs)

    payload = packing.run_window(Slowing(), items(5), grant(unit_budget=2))
    flags = [m.get("throughput_collapse") for m in payload["measurements"]]
    assert flags[0] is None, "the first growing batch has no comparator"
    assert flags[1] is True, "the spill is flagged on HIP exactly as on CUDA"
    assert not flags[2], "the tail batch stays non-comparable on HIP too"


def test_the_comparator_ages_out_after_a_run_of_non_comparable_batches(fake_torch):
    """A collapsed batch must not become the new comparator (that would make a
    spill the new normal), but a comparator kept forever would eventually be
    measured against a rate the model no longer runs at — so it retires."""

    def growing(inputs):
        fake_torch.grow_pool(1)
        return [None] * len(inputs)

    primer = SimpleNamespace(predict=growing)
    # One growing batch of 2 units sets the comparator.
    packing.run_window(primer, items(2), grant(unit_budget=2))
    assert packing._last_growth is not None

    # Warm (non-growing) batches are non-comparable; COMPARATOR_MAX_AGE of them
    # retire the comparator.
    warm = SimpleNamespace(predict=lambda inputs: [None] * len(inputs))
    for _ in range(packing.COMPARATOR_MAX_AGE):
        packing.run_window(warm, items(1), grant(unit_budget=1))
    assert packing._last_growth is None, "the stale comparator was retired"


def test_reset_comparator_clears_the_cross_window_state(fake_torch):
    def growing(inputs):
        fake_torch.grow_pool(10)
        return [None] * len(inputs)

    packing.run_window(SimpleNamespace(predict=growing), items(2), grant(unit_budget=2))
    assert packing._last_growth is not None
    packing.reset_comparator()
    assert packing._last_growth is None


# ---------------------------------------------------------------------------
# Impl-internal sub-batching (unpriceable batches)
# ---------------------------------------------------------------------------


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
    assert "units" not in measurement, (
        "a batch the impl only partly executed is unpriceable"
    )
    assert measurement.get("oom") is None, "no halvings, so no negative sample"


def test_a_fully_executed_batch_is_still_priced(fake_torch, fake_oom_retry):
    """The guard must not withhold `units` from impls that do use
    `run_with_oom_retry` but ran the whole batch in one chunk."""

    class Whole:
        def predict(self, inputs):
            fake_torch.grow_pool(20)
            fake_oom_retry.record(largest=len(inputs))
            return [None] * len(inputs)

    payload = packing.run_window(Whole(), items(3), grant(unit_budget=3))
    assert payload["measurements"][0]["units"] == 3


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
    """An OOM the impl's own halving loop swallowed is invisible to the
    orchestrator unless the harness reports it — and it is exactly the signal
    the deflation path exists for."""

    class Halving:
        def predict(self, inputs):
            fake_torch.grow_pool(20)
            fake_oom_retry.record(largest=2, halvings=2)
            return [None] * len(inputs)

    payload = packing.run_window(Halving(), items(4), grant(unit_budget=4))
    measurement = payload["measurements"][0]
    assert measurement["oom"] is True
    assert "units" not in measurement, "2 of 4 executed: unpriceable too"


def test_a_helper_call_that_ran_nothing_is_unpriceable(fake_torch, fake_oom_retry):
    """The record moved for this batch and still says nothing ran through the
    helper: easyOCR's `readtext` shape, where the impl consults the helper, ends
    up executing zero items there, and does the work by another route. That is
    'executed nothing here', not 'ran the whole batch' — the GPU batch the
    allocator saw is unknown, so the batch is unpriceable."""

    class ReadtextFallback:
        def predict(self, inputs):
            fake_torch.grow_pool(20)
            fake_oom_retry.record(largest=0)
            return [None] * len(inputs)

    payload = packing.run_window(ReadtextFallback(), items(3), grant(unit_budget=3))
    measurement = payload["measurements"][0]
    assert measurement["items"] == 3
    assert "units" not in measurement, (
        "a record that moved with largest == 0 means nothing ran there"
    )


def test_halvings_in_an_earlier_helper_call_still_flag_the_batch(
    fake_torch, fake_oom_retry
):
    """clip and nemotron-embed-vl call `run_with_oom_retry` twice per `predict`
    (a text pass and an image pass). Only the last call's record survives, so an
    OOM the *first* pass absorbed is invisible in it — the process-total halvings
    counter, diffed across the call, is what catches it."""

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


# ---------------------------------------------------------------------------
# The batching-disabled gate
# ---------------------------------------------------------------------------


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
    repeat that happens to be slow says nothing."""

    class SlowButWarm:
        def predict(self, inputs):
            import time

            time.sleep(0.02)
            return [None] * len(inputs)

    # Prime the comparator with a genuine growth batch.
    def growing(inputs):
        fake_torch.grow_pool(50)
        return [None] * len(inputs)

    primer = SimpleNamespace(predict=growing)
    packing.run_window(primer, items(1), grant(unit_budget=1))
    payload = packing.run_window(SlowButWarm(), items(2), grant(unit_budget=1))
    assert all(
        m.get("throughput_collapse") is None for m in payload["measurements"]
    ), "no pool growth, no synthetic negative"


def test_measurements_carry_the_allocator_deltas(fake_torch):
    def growing(inputs):
        fake_torch.grow_pool(64)
        return [None] * len(inputs)

    payload = packing.run_window(
        SimpleNamespace(predict=growing), items(2), grant(unit_budget=2)
    )
    measurement = payload["measurements"][0]
    assert measurement["reserved_before_mb"] == 0
    assert measurement["peak_reserved_mb"] == 64
    assert measurement["duration_ms"] is not None
    assert payload["memory"]["reserved_mb"] == 64


def test_a_window_with_no_grant_never_reaches_the_harness():
    """The compatibility path lives in `__main__`, not here: `finish_batch`
    reports one measurement for the whole call and no `units`, because with
    no grant there is no declared cost dimension to price in."""
    state = memory.begin_batch()
    payload = memory.finish_batch(state, items=7)
    assert payload["measurements"][0]["items"] == 7
    assert "units" not in payload["measurements"][0]


# ---------------------------------------------------------------------------
# Reactive shrink (step 2)
# ---------------------------------------------------------------------------


def idle_impl():
    """An impl that runs a batch without growing the allocator pool."""
    return SimpleNamespace(predict=lambda inputs: [None] * len(inputs))


def test_reactive_shrink_needs_two_consecutive_under_grant_windows(fake_torch):
    """A grant that has fallen well below the pool means we are holding memory
    the ledger has already taken away from us — and freeing tensors gives none
    of it back, so `empty_cache()` is the only lever. The two-window
    hysteresis is what keeps a momentary dip from costing a full pool
    teardown."""
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


def test_a_grant_back_above_the_pool_resets_the_shrink_hysteresis(fake_torch):
    """Recovery is immediate: the whole point is reacting to a world that
    moved, and it can move back."""
    fake_torch.reserved = 1000 * MIB
    fake_torch.allocated = 0
    impl = idle_impl()

    packing.run_window(impl, items(1), grant(unit_budget=1, mb=100))
    assert packing._under_grant_windows == 1
    packing.run_window(impl, items(1), grant(unit_budget=1, mb=900))
    assert packing._under_grant_windows == 0, "800 <= 900: no squeeze"
    assert fake_torch.empty_cache_calls == 0
    packing.run_window(impl, items(1), grant(unit_budget=1, mb=100))
    assert fake_torch.empty_cache_calls == 0, "the count restarted from zero"


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
    """Two non-signals that must not accumulate towards a release: a grant
    with no MB reservation (a pre-1b orchestrator, or a contention share that
    rounded to nothing) and a worker holding no pool at all."""
    impl = idle_impl()
    fake_torch.reserved = 1000 * MIB
    fake_torch.allocated = 0
    for _ in range(4):
        packing.run_window(impl, items(1), grant(unit_budget=1, mb=0))
    assert fake_torch.empty_cache_calls == 0
    assert packing._under_grant_windows == 0

    fake_torch.reserved = 0
    for _ in range(4):
        packing.run_window(impl, items(1), grant(unit_budget=1, mb=100))
    assert fake_torch.empty_cache_calls == 0
    assert packing._under_grant_windows == 0


def test_a_worker_without_torch_never_shrinks():
    """No live CUDA, no pool of ours, nothing to release — and crucially no
    attempt to create a context in order to find that out."""
    assert packing.maybe_shrink(1) is False
    assert packing._under_grant_windows == 0


def test_the_shrink_compares_the_grant_against_slack_not_the_whole_pool(fake_torch):
    """The grant is an *incremental* activation reservation; `memory_reserved()`
    is the whole pool, weights included. Comparing the two would be comparing
    different quantities — and on any calibrated model the grant is the smaller
    one essentially always, so the trigger would fire every other window,
    release a pool with nothing spare in it, and (via `note_trimmed`)
    permanently discard the WDDM throughput comparator, which needs consecutive
    comparable batches to say anything at all.

    Only `reserved - allocated` can actually be handed back, so that is what a
    window's grant is measured against.
    """
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
    """The other half of the same rule: when the pool really is holding blocks
    this window has no use for, two consecutive windows still release it — and
    then cannot immediately re-trigger, because the slack is gone."""
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
    assert fake_torch.reserved == 2400 * MIB, (
        "the free blocks went back to the driver; the weights stayed"
    )

    # Self-limiting: post-release there is no slack left, so the next window
    # cannot start counting towards another teardown.
    packing.run_window(impl, items(1), squeezed)
    packing.run_window(impl, items(1), squeezed)
    assert fake_torch.empty_cache_calls == 1
    assert packing._under_grant_windows == 0
