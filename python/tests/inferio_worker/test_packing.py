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

from inferio.impl import utils as impl_utils
from inferio_worker import memory, packing
from inferio_worker.inputs import PredictionInput
from test_memory import (
    NO_SWAPOUTS_SEEN,
    FakeMpsAllocator,
    FakeRam,
    cpu_host,
    fake_mps_torch_module,
    isolated,
    mps_host,
    pci_root,
    rocm_host,
    unified,
    write_gtt,
)

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
        self.inactive_split = 0

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

    def memory_stats(self):
        """The one key the release decision reads. The real map has hundreds;
        a fake that answers only what is asked keeps the test honest about
        which statistic the code depends on."""
        return {"inactive_split_bytes.all.current": self.inactive_split}

    def empty_cache(self):
        """Release the pool blocks no live tensor is using **and** no live
        block splits: the real allocator can only return a whole segment, so
        `inactive_split` bytes stay in the pool."""
        self.empty_cache_calls += 1
        self.reserved = self.allocated + self.inactive_split
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
    packing.reset_comparator()
    packing.reset_shrink_state()
    yield
    packing.reset_comparator()
    packing.reset_shrink_state()


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

    looks_like_index_limit = staticmethod(impl_utils.looks_like_index_limit)


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


# --- Per-item pixel canvas ---


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


def test_the_token_window_resolves_like_the_canvas():
    """The `token` twin of the two tests above. A transformer truncates or
    window-splits a long input at `max_seq_length` and only ever holds
    `count x window` tokens at once, so an uncapped bytes-per-token price
    fits a slope that is a function of the corpus, which over-admits."""
    impl = SimpleNamespace(model=SimpleNamespace(max_seq_length=256))
    assert packing.resolve_max_tokens({"max_tokens": 8192}, impl, "token") == 8192
    assert packing.resolve_max_tokens({}, impl, "token") == 256
    assert packing.resolve_max_tokens({}, SimpleNamespace(), "token") is None
    assert packing.resolve_max_tokens({}, impl, "item") is None
    assert packing.resolve_max_tokens({}, impl, "pixel") is None

    floor = packing.TOKEN_WINDOW_FLOOR
    for value in (1, floor - 1, 0, -1, True, "256"):
        small = SimpleNamespace(model=SimpleNamespace(max_seq_length=value))
        assert packing.resolve_max_tokens({}, small, "token") is None, value
    at_floor = SimpleNamespace(model=SimpleNamespace(max_seq_length=floor))
    assert packing.resolve_max_tokens({}, at_floor, "token") == floor

    # HF tokenizers spell "no limit" as int(1e30): a sentinel, not a window.
    sentinel = SimpleNamespace(model=SimpleNamespace(model_max_length=int(1e30)))
    assert packing.resolve_max_tokens({}, sentinel, "token") is None
    at_max = SimpleNamespace(model=SimpleNamespace(max_seq_length=packing.TOKEN_WINDOW_MAX))
    assert packing.resolve_max_tokens({}, at_max, "token") == packing.TOKEN_WINDOW_MAX

    class Hostile:
        @property
        def max_seq_length(self):
            raise RuntimeError("no")

        @property
        def model(self):
            raise RuntimeError("no")

    assert packing.resolve_max_tokens({}, Hostile(), "token") is None


def test_a_text_price_counts_utf8_bytes_like_the_host():
    """The host charges a non-string `data` the bytes of its compact JSON
    (`dispatch::text_bytes`), so the worker counts the same serialisation:
    counting characters priced a CJK item at 0.38x the host's figure."""
    text = "\u6f22" * 324 + "a" * 52
    assert (len(text), len(text.encode("utf-8"))) == (376, 1024)
    # `{"text":"<1024 bytes>"}`: 1035 bytes on both sides, 258 tokens.
    assert packing._text_bytes({"text": text}) == 1035
    wrapped = [PredictionInput(data={"text": text})]
    assert packing.price_inputs(wrapped, "token") == [258]
    assert packing.price_inputs(wrapped, "token", None, 256) == [256]
    assert packing.price_inputs([PredictionInput(data=text)], "token") == [256]


def test_a_token_price_is_capped_at_the_window():
    """The cap is what makes `max-times-count` describe the peak: a window of
    long texts is priced `count x window`, which is exactly what the impl puts
    on the GPU at once, instead of `count x raw length`."""
    long_text = [PredictionInput(data="x" * 8192)]
    short = [PredictionInput(data="x" * 400)]
    assert packing.price_inputs(long_text, "token") == [2048]
    assert packing.price_inputs(long_text, "token", None, 256) == [256]
    assert packing.price_inputs(short, "token", None, 256) == [100]
    assert packing.price_inputs([PredictionInput()], "token", None, 256) == [1]
    # An area caps no token, and a token count caps no area.
    assert packing.price_inputs(long_text, "token", 1_835_008) == [2048]
    assert packing.price_inputs(items(3), "item", None, 256) == [1, 1, 1]

    # The uncapped price survives as the bucketing tiebreak, so texts that
    # price alike still batch longest-first.
    mixed = [
        PredictionInput(data="x" * 8192),
        PredictionInput(data="x" * 4096),
        PredictionInput(data="x" * 40),
    ]
    priced = packing.price_window(mixed, "token", None, 256)
    assert priced.units == [256, 256, 10]
    assert priced.raw == [2048, 1024, 10]
    assert priced.shapes is None
    # 512 units buys two capped texts; uncapped it buys the longest alone.
    assert packing.plan_batches(priced.units, "max-times-count", 512) == [[0, 1], [2]]
    assert packing.plan_batches(priced.raw, "max-times-count", 512) == [[0], [1], [2]]


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


# --- Size homogeneity under the canvas ---
# The cap prices every item at or above the canvas alike, removing the size
# information the `max-times-count` bucketing sorts on. Two halves: the raw
# price survives as a *tiebreaker*, and an impl that pads to its largest member
# while stating no canvas of its own is named in the log once.
#
# One canvas, one pair of sizes, both above it, raw areas 2.78x apart.
PAD_CANVAS = 1_000_000
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
    priced = packing.price_window(mixed_window(), "pixel", PAD_CANVAS)
    assert priced.units == [PAD_CANVAS] * 4, "the price is the capped one"
    assert priced.raw == raw
    for unit, canvas in (("pixel", None), ("pixel", 0), ("item", PAD_CANVAS)):
        uncapped = packing.price_window(mixed_window(), unit, canvas)
        assert uncapped.units is uncapped.raw, (unit, canvas)
    assert packing.price_window(mixed_window(), "pixel").units == raw


def test_equally_priced_items_are_ordered_by_raw_size():
    """Four items priced alike bucket by raw area, so the two 3 MP sheets share
    a batch; without it input order interleaves. Secondary means secondary,
    though: a cheaper item never overtakes a dearer one however large it is
    raw, and a mis-sized tiebreaker or an aggregation that does not sort leaves
    the primary key's plan untouched."""
    units = [PAD_CANVAS] * 4
    raw = [3_000_000, 1_080_000, 3_000_000, 1_080_000]
    budget = 2 * PAD_CANVAS
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
            unit_budget=2 * PAD_CANVAS,
            unit="pixel",
            aggregation="max-times-count",
            canvas_pixels=PAD_CANVAS,
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


def run_padding_window(model, inputs, canvas=PAD_CANVAS):
    return packing.run_window(
        model,
        inputs,
        grant(
            unit_budget=4 * PAD_CANVAS,
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
    the shape the log line describes."""
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
    model.processor = SimpleNamespace(max_pixels=PAD_CANVAS)
    assert packing.impl_canvas_pixels(model) == PAD_CANVAS
    assert packing._pads_without_a_canvas(model) is True
    with caplog.at_level(logging.WARNING, logger="inferio_worker.packing"):
        run_padding_window(model, mixed_window())
    assert padding_warnings(caplog)


def test_the_guard_is_silent_where_nothing_is_under_priced(
    fake_torch, unlogged_guard, caplog
):
    """Three ways to be uninteresting: an impl that states a canvas of its own
    — which is a promise to bound every input by it before the tensor exists,
    and is what exempts `inferio.impl.eocr` — no canvas in
    force at all, and a batch whose raw sizes are within the 2x ratio."""
    with caplog.at_level(logging.WARNING, logger="inferio_worker.packing"):
        run_padding_window(Padding(canvas=PAD_CANVAS), mixed_window())
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


def test_the_clamp_credits_the_pool_this_batch_would_reuse(fake_torch):
    """The free reading excludes the pool this process holds,
    and a batch spends that pool without asking the device for a page. Not
    the host's own credit (`reserved_now - reserved_at_load - grants`), but the
    bytes this batch can spend in place.
    """
    fake_torch.free = 250 * MIB
    fake_torch.reserved = 800 * MIB
    fake_torch.allocated = 50 * MIB
    assert memory.releasable_pool_mb() == 750
    live = packing.clamp_to_live_memory(64, 1000)
    assert (live.units, live.clamped) == (64, None), "250 free + 750 of our own"
    assert live.free_mb == 250, "the reading reported is still the device's"

    past = packing.clamp_to_live_memory(64, 2000)
    assert past.units == 32, "1000 of 2000, not 250"
    assert past.clamped == {"from_units": 64, "to_units": 32, "free_mb": 250}

    fake_torch.allocated = fake_torch.reserved
    assert memory.releasable_pool_mb() == 0, "a fully-used pool releases nothing"
    assert packing.clamp_to_live_memory(64, 1000).units == 16


def test_the_clamp_counts_the_pool_the_grant_already_credited(fake_torch):
    """The clamp trap on a 24 GiB GPU, in its own numbers.

    A pre-fit grant is `headroom + the requester's free pool`, so it runs
    *above* the device free reading by that pool less the reserve: 23 557 MiB
    against 23 473 free, a 202 MiB pool and a 118 MiB reserve. Netted, the
    batch can spend 23 675 and nothing is short.
    """
    fake_torch.free = 23_473 * MIB
    fake_torch.reserved = 202 * MIB
    fake_torch.allocated = 0
    live = packing.clamp_to_live_memory(2, 23_557)
    assert (live.units, live.clamped) == (2, None), "the ramp keeps its 2 units"
    assert live.free_mb == 23_473, "the reported reading stays the raw one"

    # The trap, with the netting dropped: 23473/23557 of 2 floors to 1, and a
    # budget stuck at 1 never advances the ratchet anchor.
    assert int(2 * 23_473 / 23_557) == 1


def test_a_sub_unit_shortfall_never_costs_a_unit(fake_torch):
    """Rounding to nearest: a 0.1 % gap on a 2-unit budget is not a halving,
    and a gap of more than half a unit still is."""
    fake_torch.free = 999 * MIB
    assert packing.clamp_to_live_memory(2, 1000).units == 2
    fake_torch.free = 800 * MIB
    assert packing.clamp_to_live_memory(2, 1000).units == 2, "1.6 rounds to 2"
    fake_torch.free = 700 * MIB
    assert packing.clamp_to_live_memory(2, 1000).units == 1, "1.4 rounds to 1"


def test_a_real_shortfall_still_halves_the_budget(fake_torch):
    """The clamp is still a clamp: half the spendable memory, half the budget,
    pool credit included and down to the one unit a batch never goes below."""
    fake_torch.free = 400 * MIB
    fake_torch.reserved = 100 * MIB
    fake_torch.allocated = 0
    halved = packing.clamp_to_live_memory(64, 1000)
    assert halved.units == 32, "(400 + 100)/1000 of 64"
    assert halved.clamped == {"from_units": 64, "to_units": 32, "free_mb": 400}

    fake_torch.free = 4 * MIB
    fake_torch.reserved = 0
    assert packing.clamp_to_live_memory(64, 100_000).units == 1


def test_a_whole_board_pre_fit_grant_does_not_clamp_a_fresh_worker(fake_torch):
    """The other pre-fit shape: nothing in the pool yet, so the grant is
    headroom alone and sits *below* the free reading by the reserve."""
    fake_torch.free = 23_473 * MIB
    fake_torch.reserved = 0
    fake_torch.allocated = 0
    live = packing.clamp_to_live_memory(1, 23_355)
    assert (live.units, live.clamped) == (1, None)


def test_a_part_of_the_headroom_pre_fit_grant_clamps_only_below_itself(fake_torch):
    """With a neighbour on the GPU a pre-fit grant is a part of the headroom:
    7 666 of 15 333 MiB, the neighbour holding 3 833 of the rest. The
    neighbour spending its own reservation leaves 11 500 free, above this
    grant, so the batch keeps its budget; the clamp shrinks it once the device
    cannot supply the grant itself.
    """
    fake_torch.reserved = 0
    fake_torch.allocated = 0
    fake_torch.free = 15_333 * MIB
    assert packing.clamp_to_live_memory(8, 7_666).clamped is None
    fake_torch.free = (15_333 - 3_833) * MIB
    assert packing.clamp_to_live_memory(8, 7_666).clamped is None
    fake_torch.free = 3_833 * MIB
    assert packing.clamp_to_live_memory(8, 7_666).units == 4

    # An 8 GiB card, 3 817 MiB of headroom: the second model is granted the
    # 1 257 left for 3 units. The first one spending its 2 560 does not clamp
    # it. Its next grant is the same 1 257 with a 960 MiB pool inside it, so
    # neither the clamp nor the pool release fires.
    fake_torch.free = (3_817 - 2_560) * MIB
    assert packing.clamp_to_live_memory(3, 1_257).clamped is None
    fake_torch.free = (3_817 - 2_560 - 960) * MIB
    fake_torch.reserved = 960 * MIB
    assert packing.clamp_to_live_memory(3, 1_257).clamped is None
    for _ in range(packing.SHRINK_WINDOWS + 1):
        assert packing.maybe_shrink(1_257) is False
    assert fake_torch.empty_cache_calls == 0


def test_a_cpu_priced_worker_credits_nothing(monkeypatch):
    """The credit is device pool, and a RAM-priced worker has none: its "pool"
    is `(VmHWM, VmRSS)`, whose difference is memory already back in the free
    reading. Credited, 16 GiB of lifetime high-water would suppress the clamp
    entirely; uncredited, 4 000 MiB of free RAM against an 8 000 MiB grant
    halves the budget, which is the honest answer.
    """
    ram = FakeRam(total_mb=64_000, available_mb=4_000, rss_mb=20_000)
    with cpu_host(ram):
        ram.release(16_000)
        assert (ram.peak_mb, ram.rss_mb) == (20_000, 4_000)
        assert memory.free_total_mb()[0] == 20_000, "the released pages are back"
        assert memory.releasable_pool_mb() is None, "no second currency here"

        ram.available_mb = 4_000
        live = packing.clamp_to_live_memory(8, 8_000)
        assert live.units == 4, "4000/8000 of 8, with nothing to credit"
        assert live.free_source == "ram"


def test_a_cpu_priced_worker_keeps_the_ram_reserve_free(tmp_path, monkeypatch):
    """Free RAM counts only above the reserve the grant carries, so a batch
    cannot take the memory the orchestrator left for the rest of the machine.
    A 128 GiB Linux host with 30 605 MiB available, 6 000 of it reclaimable
    slab, and a 12 864 MiB reserve has 11 741 MiB to spend.
    """
    monkeypatch.setattr(sys, "platform", "linux")
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("SReclaimable:    6144000 kB\n")
    ram = FakeRam(total_mb=128_649, available_mb=30_605, rss_mb=1_200)
    with cpu_host(ram, meminfo=str(meminfo)):
        live = packing.clamp_to_live_memory(265, 12_607, ram_reserve_mb=12_864)
        assert live.units == 247, "11 741 / 12 607 of 265"
        assert live.clamped == {"from_units": 265, "to_units": 247, "free_mb": 24_605}
        assert (live.free_mb, live.free_source) == (24_605, "ram")
        # A grant sized without the reserve, against all that reads available.
        assert packing.clamp_to_live_memory(768, 36_538, 12_864).units == 247
        assert packing.clamp_to_live_memory(768, 36_538).units == 517, "no reserve"

        ram.available_mb = 6_000 + 12_864 + 12_607
        assert packing.clamp_to_live_memory(265, 12_607, 12_864).clamped is None
        ram.available_mb = 6_000 + 12_000
        floor = packing.clamp_to_live_memory(265, 12_607, 12_864)
        assert floor.units == 1, "nothing above the reserve: one unit"


def test_a_window_runs_under_the_reserve_its_grant_carries():
    """The grant's `ram_reserve_mb` reaches every batch's clamp, and the batch
    reports the resident set it left."""
    ram = FakeRam(total_mb=64_000, available_mb=6_000, rss_mb=1_000)
    with cpu_host(ram):
        model = Recorder()
        payload = packing.run_window(
            model, items(8), grant(unit_budget=8, mb=8_000, ram_reserve_mb=2_000)
        )
    assert [len(batch) for batch in model.batches] == [4, 4], "4 000 / 8 000 of 8"
    for measurement in payload["measurements"]:
        assert measurement["clamped"]["to_units"] == 4
        assert measurement["rss_after_mb"] == 1_000


def test_a_gpu_worker_keeps_the_ram_reserve_free(fake_torch):
    """A GPU worker's grant books host RAM too. Its batch is scaled by free
    RAM above the reserve against that booking, and runs at the smaller of
    this and the device's own budget. The reserve is not taken from the
    device's free reading.
    """
    fake_torch.free = 8_000 * MIB
    host = {"free_mb": 20_000}
    with mock.patch.object(
        memory, "ram_free_total_mb", side_effect=lambda: (host["free_mb"], 64_000)
    ):
        roomy = packing.clamp_to_live_memory(64, 1_000, 6_000, 14_000)
        assert (roomy.units, roomy.clamped) == (64, None), "14 000 above the reserve"

        host["free_mb"] = 13_000
        live = packing.clamp_to_live_memory(64, 1_000, 6_000, 14_000)
        assert live.units == 32, "7 000 / 14 000 of 64"
        assert live.clamped == {
            "from_units": 64,
            "to_units": 32,
            "free_mb": 13_000,
            "reason": "host_ram",
        }
        assert (live.free_mb, live.free_source) == (8_000, "torch")

        # The device is the tighter of the two: its own clamp is reported.
        fake_torch.free = 250 * MIB
        device = packing.clamp_to_live_memory(64, 1_000, 6_000, 14_000)
        assert device.clamped == {"from_units": 64, "to_units": 16, "free_mb": 250}
        # No booking, no host reading; an unreadable host changes nothing.
        fake_torch.free = 8_000 * MIB
        host["free_mb"] = 0
        assert packing.clamp_to_live_memory(64, 1_000, 6_000).units == 64
        host["free_mb"] = None
        assert packing.clamp_to_live_memory(64, 1_000, 6_000, 14_000).units == 64

        host["free_mb"] = 13_000
        model = Recorder()
        packing.run_window(
            model,
            items(8),
            grant(unit_budget=8, ram_mb=14_000, ram_reserve_mb=6_000),
        )
        assert [len(batch) for batch in model.batches] == [4, 4]


def test_an_apu_keeps_the_ram_reserve_in_its_ram_term(tmp_path, monkeypatch):
    """An APU's reading is free VRAM plus the smaller of unclaimed GTT and
    free RAM. The grant's RAM reserve comes off the RAM term only: it bites
    when RAM is short, withholds nothing when GTT is, and never takes free
    VRAM."""
    bdf = "0000:03:00.0"
    root = pci_root(tmp_path, {bdf: (512 * MIB, 256 * MIB)})
    with rocm_host(tmp_path, monkeypatch, pci=root):
        write_gtt(root, bdf, 64 * 1024 * MIB, 4 * 1024 * MIB)
        with unified(ram_available_mb=8_000):
            ram_short = packing.clamp_to_live_memory(64, 4_000, 6_000)
        with unified(ram_available_mb=4_000):
            below_reserve = packing.clamp_to_live_memory(64, 4_000, 6_000)
        write_gtt(root, bdf, 64 * 1024 * MIB, 62 * 1024 * MIB)
        with unified(ram_available_mb=100 * 1024):
            gtt_short = packing.clamp_to_live_memory(64, 4_000, 6_000)
            window = packing.run_window(Recorder(), items(1), grant(unit_budget=1))
    assert ram_short.clamped == {
        "from_units": 64,
        "to_units": 36,
        "free_mb": 256 + 8_000,
    }, "256 + 2 000 above the reserve, of 4 000"
    assert below_reserve.units == 4, "the 256 of free VRAM, of 4 000"
    assert gtt_short.clamped == {
        "from_units": 64,
        "to_units": 37,
        "free_mb": 256 + 2 * 1024,
    }, "all of it, of 4 000"
    assert gtt_short.gtt_mb == (2 * 1024, 100 * 1024)
    batch = window["measurements"][0]
    assert (batch["gtt_free_mb"], batch["ram_available_mb"]) == gtt_short.gtt_mb


def test_an_mps_worker_credits_the_metal_pool():
    """The Metal arm of the same credit: `driver_allocated -
    current_allocated`, 200 MiB of a 1 200 MiB driver pool."""
    with mps_host(available_mb=8_000) as mps:
        mps.allocate(1_000, driver_mb=1_200)
        assert memory.releasable_pool_mb() == 200
        free_mb, _, source = memory.free_total_mb()
        assert (free_mb, source) == (8_000, "mps")
        assert packing.clamp_to_live_memory(4, 8_100).units == 4, "8200 spendable"
        assert packing.clamp_to_live_memory(4, 20_000).units == 2, "8200/20000"


def test_an_mps_worker_keeps_the_ram_reserve_free():
    """On unified memory the reserve is kept in RAM: an MPS reading spends
    Metal's ceiling or the RAM above the reserve, whichever is less."""
    with mps_host(available_mb=8_000) as mps:
        mps.allocate(1_000, driver_mb=1_200)
        live = packing.clamp_to_live_memory(4, 8_100, ram_reserve_mb=2_000)
        assert live.units == 3, "6 000 above the reserve plus 200 of pool"
    with mps_host(available_mb=120 * 1024):
        live = packing.clamp_to_live_memory(8, 128 * 1024, ram_reserve_mb=13_107)
        assert live.units == 6, "the 96 GiB ceiling binds, not the reserve"
    with mps_host(available_mb=1_000) as mps:
        mps.allocate(1_000, driver_mb=5_000)
        live = packing.clamp_to_live_memory(
            8, 8_000, ram_reserve_mb=2_000, paging=True
        )
        assert live.units == 3, "4 000 of pool less the 1 000 below the reserve"
        live = packing.clamp_to_live_memory(8, 8_000, ram_reserve_mb=2_000)
        assert live.units == 4, "not paging: the 4 000 of pool is kept"


def test_at_warning_an_mps_clamp_keeps_the_pool_the_grant_kept():
    """The grant the ledger issues at warning with no RAM available and
    13 287 MiB of pool, 180 above the 13 107 MiB reserve
    (`at_warning_a_grant_below_the_reserve_keeps_the_pool_and_says_so`): the
    clamp keeps it whole. A grant that says macOS is paging takes the
    deficit off the pool, which leaves the 180 MiB, 8 units."""
    grant_figures = {"unit_budget": 64, "mb": 740, "fixed_mb": 100}
    with mps_host(available_mb=0) as mps:
        mps.allocate(0, driver_mb=13_107 + 180)
        for paging, units in [(False, 64), (True, 8)]:
            live = packing.clamp_to_live_memory(
                grant_figures["unit_budget"],
                grant_figures["mb"],
                ram_reserve_mb=13_107,
                fixed_mb=grant_figures["fixed_mb"],
                paging=paging,
            )
            assert live.units == units, paging
    # The grant's own `paging` reaches the clamp; absent reads as false. The
    # pool, far above these grants, is not released between the windows.
    for paging, first in [(True, 8), (None, 64)]:
        wire = grant(**grant_figures, ram_reserve_mb=13_107)
        if paging is not None:
            wire["paging"] = paging
        with mps_host(available_mb=0) as mps, mock.patch.object(
            packing, "maybe_shrink", return_value=False
        ):
            mps.allocate(0, driver_mb=13_107 + 180)
            model = Recorder()
            packing.run_window(model, items(64), wire)
        assert len(model.batches[0]) == first, paging


def test_paging_during_a_long_batch_cuts_the_next_one():
    """Swap-outs 5 s into a 90 s batch and none for its last 85 s: the next
    batch's reading counts them, since they came after the reading the batch
    was sized from, so that batch fits the 2000 MiB pool held. The batch
    after it saw no rise during the one before and is not cut. Once the
    worker clears the instant after the reply, the first batch of the next
    window is not cut by swap-outs in the idle time before it."""
    clock = [0.0]

    def counters():
        swapouts = 500 + 100 * (clock[0] >= 5) + 100 * (clock[0] >= 300)
        return (128 * 1024 * MIB, 0, 0, 88 * 1024 * MIB, 2, swapouts, 0)

    class Slow:
        def predict(self, inputs):
            clock[0] += 90
            return [item.data for item in inputs]

    mps = FakeMpsAllocator()
    mps.allocate(1000, driver_mb=3000)
    with (
        isolated(fake_mps_torch_module(mps)),
        mock.patch.object(memory, "_mac_memory_counters", side_effect=counters),
        mock.patch("time.monotonic", side_effect=lambda: 1000.0 + clock[0]),
        mock.patch.dict(memory._swapouts, NO_SWAPOUTS_SEEN),
    ):
        payload = packing.run_window(
            Slow(), items(16), grant(unit_budget=8, mb=4000)
        )
        memory.count_paging_from_last_reading(False)
        clock[0] = 400
        next_window = packing.run_window(
            Slow(), items(8), grant(unit_budget=8, mb=4000)
        )
    first, cut, after = payload["measurements"]
    assert (first["items"], cut["items"], after["items"]) == (8, 4, 4)
    assert cut["free_mb"] == 0
    assert cut["clamped"] == {"from_units": 8, "to_units": 4, "free_mb": 0}
    assert after["free_mb"] == 40 * 1024 and "clamped" not in after
    assert next_window["measurements"][0]["free_mb"] == 40 * 1024


def test_a_rocm_worker_uses_the_cuda_arm_of_the_credit(fake_rocm_torch):
    """HIP is `torch.cuda` under another name, so ROCm needs no arm of its own
    — the same `reserved - allocated` answers."""
    fake_rocm_torch.free = 400 * MIB
    fake_rocm_torch.reserved = 100 * MIB
    fake_rocm_torch.allocated = 0
    assert memory.releasable_pool_mb() == 100
    assert packing.clamp_to_live_memory(64, 1_000).units == 32


def test_a_worker_with_no_device_credits_nothing():
    """No torch at all: no pool to read, and the clamp treats that as 0."""
    with isolated(None):
        assert memory.releasable_pool_mb() is None


def test_the_worker_credit_is_not_the_pool_the_ledger_credited(fake_torch):
    """The two credits are different numbers and the docstring must not claim
    otherwise. `free_pool_mb` is `reserved_now - reserved_at_load - grants`;
    the worker credits `reserved_now - allocated_now`, the bytes it can spend
    in place. They agree only when `allocated_now == reserved_at_load` with no
    grant outstanding.
    """
    reserved_now, reserved_at_load, allocated_now, grants = 2_000, 1_800, 1_500, 0
    fake_torch.reserved = reserved_now * MIB
    fake_torch.allocated = allocated_now * MIB
    ledger_credit = reserved_now - reserved_at_load - grants
    assert (memory.releasable_pool_mb(), ledger_credit) == (500, 200)

    # The other direction: live allocation grew past the load frame, so the
    # worker credits *less* than the grant carries and a gap survives.
    reserved_now, reserved_at_load, allocated_now = 2_000, 500, 1_900
    fake_torch.reserved = reserved_now * MIB
    fake_torch.allocated = allocated_now * MIB
    assert memory.releasable_pool_mb() == 100
    assert reserved_now - reserved_at_load - grants == 1_500


def test_a_residual_gap_needs_a_quarter_of_the_board_to_retrap(fake_torch):
    """How big an *uncredited* gap it takes to floor a 2-unit window again:
    `int(2r + 0.5) < 2` needs `r < 0.75`, so 5 889 MiB of the 23 557 MiB
    whole-board grant on the 24 GiB card — a real shortfall, not a 0.36 % gap.
    """
    grant_mb = 23_557
    fake_torch.reserved = 0
    fake_torch.allocated = 0
    fake_torch.free = 17_668 * MIB
    assert packing.clamp_to_live_memory(2, grant_mb).units == 2
    fake_torch.free = 17_667 * MIB
    assert packing.clamp_to_live_memory(2, grant_mb).units == 1
    assert grant_mb - 17_668 == 5_889, "a quarter of the board, uncredited"


def test_rounding_to_nearest_overspends_under_half_a_unit(fake_torch):
    """The bound: the shrunk budget never exceeds the proportional share by
    half a unit, so the over-spend is under `0.5 x slope` MiB — 4.1 MiB for
    docTR's 8.155, 191 MiB for florence2's 382.5, the largest shipped profile.
    The one-unit floor already on this function over-spends by up to a whole
    unit.
    """
    fake_torch.reserved = 0
    fake_torch.allocated = 0
    grant_mb = 1_000
    worst = 0.0
    for budget in (1, 2, 3, 4, 8, 16, 64, 128):
        for free_mb in range(0, grant_mb + 1, 7):
            fake_torch.free = free_mb * MIB
            units = packing.clamp_to_live_memory(budget, grant_mb).units
            proportional = budget * free_mb / grant_mb
            assert units <= budget, "shrink-only"
            if units > 1:
                assert units - proportional <= 0.5, (budget, free_mb, units)
                worst = max(worst, units - proportional)
            else:
                assert units - proportional <= 1.0, "the floor's own over-spend"
    assert worst == 0.5, "reached exactly, at the half-unit tie"
    assert round(0.5 * 382.5) == 191, "florence2 MiB/unit, the worst class"


def test_the_half_unit_is_never_the_whole_gap_at_one_unit(fake_torch):
    """A 1-unit budget floors to 1 either way, so rounding buys nothing and
    costs nothing there."""
    fake_torch.reserved = 0
    fake_torch.allocated = 0
    for free_mb in (1, 100, 600, 999):
        fake_torch.free = free_mb * MIB
        assert packing.clamp_to_live_memory(1, 1_000).units == 1


def test_the_one_unit_floor_binds_below_half_a_unit_and_lets_go(fake_torch):
    """With nearest rounding the floor binds only under `0.5 / budget` of the
    grant — a quarter of it at 2 units, 0.8 % at 64. It latches nothing: the
    next call with the memory back is unclamped.
    """
    fake_torch.reserved = 0
    fake_torch.allocated = 0
    fake_torch.free = 240 * MIB
    floored = packing.clamp_to_live_memory(2, 1_000)
    assert (floored.units, floored.clamped["to_units"]) == (1, 1)
    fake_torch.free = 7 * MIB
    assert packing.clamp_to_live_memory(64, 1_000).units == 1, "0.7 % of the grant"
    fake_torch.free = 8 * MIB
    assert packing.clamp_to_live_memory(64, 1_000).units == 1, "0.8 %, still 1"

    fake_torch.free = 1_000 * MIB
    assert packing.clamp_to_live_memory(64, 1_000).clamped is None


def test_the_pool_is_credited_exactly_once(fake_torch, caplog):
    """One credit, not two: 400 free with a 100 MiB pool is 32 of 64 units,
    while crediting the same pool twice says 38, and the log line names 100 of
    pool rather than 200.
    """
    fake_torch.free = 400 * MIB
    fake_torch.reserved = 100 * MIB
    fake_torch.allocated = 0
    with caplog.at_level(logging.INFO):
        live = packing.clamp_to_live_memory(64, 1_000)
    assert live.units == 32, "one credit; two would be 38"
    assert "(400 free plus 100 of releasable pool)" in caplog.text
    assert live.free_mb == 400, "the reported reading stays the raw one"


def test_the_clamp_scales_only_the_part_of_the_grant_that_is_per_unit(fake_torch):
    """A 2112 MiB grant for 16 units of which 800 is fixed is 82 MiB a unit.
    With 1500 spendable, 8 units fit (800 + 8 x 82 = 1456); the plain ratio
    1500/2112 of 16 is 11, which needs 1702.
    """
    fake_torch.reserved = 0
    fake_torch.allocated = 0
    fake_torch.free = 1_500 * MIB
    assert packing.clamp_to_live_memory(16, 2_112).units == 11
    live = packing.clamp_to_live_memory(16, 2_112, fixed_mb=800)
    assert (live.units, live.clamped["to_units"]) == (9, 9), "700/1312 of 16, rounded"
    fake_torch.free = 700 * MIB
    assert packing.clamp_to_live_memory(16, 2_112, fixed_mb=800).units == 1
    assert packing.clamp_to_live_memory(16, 800, fixed_mb=800).units == 1
    fake_torch.free = 2_112 * MIB
    assert packing.clamp_to_live_memory(16, 2_112, fixed_mb=800).clamped is None

    fake_torch.free = 1_500 * MIB
    model = Recorder()
    packing.run_window(
        model, items(16), grant(unit_budget=16, mb=2_112, fixed_mb=800)
    )
    assert [len(batch) for batch in model.batches] == [9, 7]


def test_the_fixed_part_the_worker_already_holds_is_not_needed_again(
    fake_torch, monkeypatch
):
    """The first batch left 800 MiB allocated over the 300 at load, so a 2112
    MiB grant with 800 fixed needs 1312 more. The pool is 2000 with 1100
    allocated: 900 releasable, and 412 free makes the 1312.
    """
    monkeypatch.setattr(memory, "_allocated_at_load_mb", 300)
    fake_torch.reserved = 2_000 * MIB
    fake_torch.allocated = 1_100 * MIB
    fake_torch.free = 412 * MIB
    assert memory.held_since_load_mb() == 800
    assert packing.clamp_to_live_memory(16, 2_112, fixed_mb=800).clamped is None
    # No more than the fixed part counts as held: 1312 of 2900.
    assert packing.clamp_to_live_memory(16, 3_000, fixed_mb=100).units == 7
    fake_torch.free = 0
    live = packing.clamp_to_live_memory(16, 2_112, fixed_mb=800)
    assert live.units == 11, "900 of the 1312 the units need"
    monkeypatch.setattr(memory, "_allocated_at_load_mb", None)
    assert memory.held_since_load_mb() == 0

    # A RAM-priced worker's resident growth is already out of its fixed part.
    with cpu_host(FakeRam(total_mb=64_000, available_mb=6_000, rss_mb=1_100)):
        monkeypatch.setattr(memory, "_allocated_at_load_mb", 300)
        assert memory.pool_stats_mb()[1] == 1_100
        assert memory.held_since_load_mb() == 0


def test_the_netting_and_the_rounding_each_keep_two_units(fake_torch):
    """Without the credit the window is 23 473/23 557 of 2 units, which a
    plain `int()` floors to 1. Round-half-up alone already lifts it back to 2,
    so on these numbers the two levers are independent and either one is
    sufficient.
    """
    assert int(2 * 23_473 / 23_557) == 1, "the trap"
    assert int(2 * 23_473 / 23_557 + 0.5) == 2, "rounding alone escapes it"
    fake_torch.free = 23_473 * MIB
    fake_torch.reserved = 202 * MIB
    fake_torch.allocated = 0
    assert packing.clamp_to_live_memory(2, 23_557).units == 2


def test_the_clamp_is_a_no_op_without_torch():
    """No CUDA, no NVML: nothing readable, so the budget stands and the OOM
    backstop covers the case."""
    live = packing.clamp_to_live_memory(64, 1000)
    assert live.units == 64
    assert (live.free_mb, live.free_source, live.clamped) == (None, None, None)


def test_the_clamp_reads_free_memory_even_with_nothing_to_clamp(fake_torch):
    """A grant carrying `mb <= 0` is the memory-blind case — precisely the
    batch whose reading the orchestrator most needs. Still exactly one
    reading."""
    fake_torch.free = 4321 * MIB
    for grant_mb in (0, None):
        live = packing.clamp_to_live_memory(64, grant_mb)
        assert live.units == 64
        assert (live.free_mb, live.free_source) == (4321, "torch")
        assert live.clamped is None


def test_every_measurement_carries_the_pre_batch_free_reading(fake_torch):
    """The wire half: the clamp's reading rides every measurement, so
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


def next_over_budget(payload):
    return [m.get("next_over_budget", False) for m in payload["measurements"]]


def test_a_batch_with_no_room_for_the_next_item_says_so(fake_torch):
    """One 1.05 MP image is under 80 % of a 2 MP budget and two exceed it, so
    the batch is as full as whole items allow. Never on the window's last."""
    images = [PredictionInput(file=png_bytes(1024, 1025)) for _ in range(3)]
    pixels = grant(unit_budget=2_000_000, unit="pixel", aggregation="sum")
    payload = packing.run_window(Recorder(), images, pixels)
    assert [m["units"] for m in payload["measurements"]] == [1_049_600] * 3
    assert next_over_budget(payload) == [True, True, False]

    # Priced at the canvas, two fit and a third does not.
    payload = packing.run_window(
        Recorder(),
        images,
        grant(unit_budget=2_000_000, unit="pixel", aggregation="sum",
              canvas_pixels=900_000),
    )
    assert [m["units"] for m in payload["measurements"]] == [1_800_000, 900_000]
    assert next_over_budget(payload) == [True, False]

    # The next item is the one packing takes next: 200 px, not the 100 behind.
    ordered = [PredictionInput(file=png_bytes(w, 10)) for w in (25, 20, 10)]
    pixels = grant(unit_budget=400, unit="pixel", aggregation="sum")
    payload = packing.run_window(Recorder(), ordered, pixels)
    assert [m["units"] for m in payload["measurements"]] == [250, 300]
    assert next_over_budget(payload) == [True, False]

    # `max-times-count`: a short text costs as much as the batch's longest.
    long_text = PredictionInput(data="x" * 8192 * packing.BYTES_PER_TOKEN)
    short_text = PredictionInput(data="x" * 100 * packing.BYTES_PER_TOKEN)
    texts = grant(
        unit_budget=21_000, unit="token", aggregation="max-times-count"
    )
    payload = packing.run_window(
        Recorder(), [long_text, long_text, short_text], texts
    )
    assert [m["units"] for m in payload["measurements"]] == [16_384, 100]
    assert next_over_budget(payload) == [True, False]

    # The queue ran out: the same batch is not full.
    payload = packing.run_window(Recorder(), [long_text, long_text], texts)
    assert [m["units"] for m in payload["measurements"]] == [16_384]
    assert next_over_budget(payload) == [False]


def test_a_small_batch_before_a_huge_item_is_not_flagged(fake_torch):
    """Below half the budget the old rule stands, however large the next
    item: 190 px of 400 is not a full batch, 200 px is."""
    huge = png_bytes(40, 20)
    pixels = grant(unit_budget=400, unit="pixel", aggregation="sum")
    for width, flagged in ((19, False), (20, True)):
        window = [PredictionInput(file=f) for f in (png_bytes(width, 10), huge)]
        payload = packing.run_window(Recorder(), window, pixels)
        units = [m["units"] for m in payload["measurements"]]
        assert units == [width * 10, 800]
        assert next_over_budget(payload) == [flagged, False], width


def test_a_batch_cut_short_by_anything_but_the_budget_is_not_flagged(
    fake_torch,
):
    """The shape ceiling, the memory clamp and the user cap each stop a batch
    while the next item would still have fit the grant."""
    # 300 px would not fit after three 100s, but the ceiling cut at two.
    small, large = png_bytes(10, 10), png_bytes(30, 10)
    shaped = [PredictionInput(file=f) for f in (small, small, small, large)]
    model = Ceiling(2)
    payload = packing.run_window(
        model, shaped, grant(unit_budget=400, unit="pixel", aggregation="sum")
    )
    assert [len(batch) for batch in model.batches] == [2, 2]
    assert next_over_budget(payload) == [False, False]

    # Half the grant's memory is free: batches of 4 against a budget of 8.
    fake_torch.free = 500 * MIB
    payload = packing.run_window(
        Recorder(), items(8), grant(unit_budget=8, mb=1000, aggregation="count")
    )
    assert [m["units"] for m in payload["measurements"]] == [4, 4]
    assert next_over_budget(payload) == [False, False]

    # Clamped to 200 px, 150 px has no room for the 350 px item, but it is
    # under half the grant's 400, whatever the clamp left.
    clamped = [PredictionInput(file=png_bytes(w, 10)) for w in (10, 5, 35)]
    payload = packing.run_window(
        Recorder(),
        clamped,
        grant(unit_budget=400, mb=1000, unit="pixel", aggregation="sum"),
    )
    assert [m["units"] for m in payload["measurements"]] == [150, 350]
    assert next_over_budget(payload) == [False, False]

    # The cap closes at two; the next item lands exactly on the budget.
    fake_torch.free = 8000 * MIB
    capped = grant(unit_budget=3, aggregation="count", user_cap_items=2)
    payload = packing.run_window(Recorder(), items(4), capped)
    assert [m["units"] for m in payload["measurements"]] == [2, 2]
    assert next_over_budget(payload) == [False, False]


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


# --- Structural out-of-memory classification ---


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


def test_a_twice_wrapped_allocator_exception_is_still_an_oom(
    fake_torch_with_oom_type,
):
    """One re-raise is not the limit: transformers, sentence-transformers,
    easyocr and doctr all wrap what they catch, so a driver OOM can arrive two
    or more links down and an unflagged one deflates nothing and halves
    nothing. The walk is bounded and survives a chain that loops."""
    try:
        try:
            try:
                raise FakeTorchOom("CUDA out of memory")
            except FakeTorchOom as driver:
                raise ValueError("could not run the model") from driver
        except ValueError as wrapped:
            raise RuntimeError("batch failed") from wrapped
    except RuntimeError as outer:
        classified = packing.classify_oom(outer)
    assert classified is not None, "three links down, and still an allocator OOM"
    assert classified["source"] == packing.OOM_SOURCE_TYPED
    assert classified["exception"] == "torch.FakeTorchOom"

    looping = RuntimeError("batch failed")
    looping.__cause__ = FakeTorchOom("CUDA out of memory")
    looping.__cause__.__context__ = looping
    assert packing.classify_oom(looping)["source"] == packing.OOM_SOURCE_TYPED


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
    """A bare "out of memory" naming no device is not one: this exact wording
    would deflate a healthy model on a GPU with 96 GB free.
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
    """And the half the orchestrator acts on: no class and no flag means
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


def test_an_impl_clearing_the_cache_goes_through_the_accounted_path(fake_torch):
    """`inferio.impl.utils.clear_cache()` — the OOM-retry ladder's release —
    must not drop the pool behind the harness's back: the next batch would
    re-grow from cold and be scored against the previous warm-pool rate, a
    `throughput_collapse` nothing collapsed."""
    from inferio.impl.utils import clear_cache

    def growing(inputs):
        fake_torch.grow_pool(500)
        return [None] * len(inputs)

    packing.run_window(SimpleNamespace(predict=growing), items(1), grant(unit_budget=1))
    assert packing._last_growth is not None, "the comparator is primed"

    fake_torch.allocated = 0  # the batch's tensors are gone; its pool is not
    with mock.patch.dict(memory._release_state, {"released_mb": None}, clear=False):
        clear_cache()
        assert memory.last_release()[0] == 500, "the memory module sized it"
    assert packing._last_growth is None, "the cold pool retired the comparator"


def test_an_impl_release_stamps_no_regrow_on_the_next_batch(fake_torch):
    """The impls' release runs *inside* `predict`, so the batch that released
    pays the re-grow within its own wall time. Arming would stamp `regrow_mb`
    on the batch after it, which re-grew nothing."""
    from inferio.impl.utils import clear_cache

    def releasing(inputs):
        fake_torch.grow_pool(500)
        clear_cache()
        return [None] * len(inputs)

    payload = packing.run_window(
        SimpleNamespace(predict=releasing), items(2), grant(unit_budget=1)
    )
    assert fake_torch.empty_cache_calls == 2, "both batches released"
    assert all("regrow_mb" not in m for m in payload["measurements"])
    assert all("regrow_after" not in m for m in payload["measurements"])


def test_the_impls_release_still_works_without_the_harness(fake_torch):
    """`inferio` runs standalone too, and on MPS the harness may never have
    been imported: with no `inferio_worker.packing` in `sys.modules` the
    direct release must still run."""
    from inferio.impl.utils import clear_cache

    fake_torch.reserved = 500 * MIB  # a pool with nothing live in it
    with mock.patch.dict(sys.modules, {}, clear=False):
        del sys.modules["inferio_worker.packing"]
        clear_cache()
    assert fake_torch.empty_cache_calls == 1, "the torch cache was emptied"


def test_no_grant_mb_and_no_pool_never_shrink(fake_torch):
    """Non-signals that must not accumulate towards a release: a grant frame
    carrying no `mb` key at all, and a worker holding no pool."""
    impl = idle_impl()
    no_mb = grant(unit_budget=1)
    del no_mb["mb"]
    cases = ((1000 * MIB, no_mb), (0, grant(unit_budget=1, mb=100)))
    for reserved, this in cases:
        fake_torch.reserved, fake_torch.allocated = reserved, 0
        for _ in range(4):
            packing.run_window(impl, items(1), this)
        assert fake_torch.empty_cache_calls == 0, this
        assert packing._under_grant_windows == 0, this


def test_a_memory_blind_grant_releases_a_pool_that_pinned_the_gpu(fake_torch):
    """`mb = 0` is the host saying the GPU had nothing left to price this
    window against, and when what filled it is this worker's own pool, nothing
    else will ever release it — those zero-MB grants are the pool's own doing.
    So a memory-blind window counts as an under-grant window, and two of them
    hand the slack back."""
    fake_torch.reserved = 22_000 * MIB
    fake_torch.allocated = 400 * MIB
    impl = idle_impl()
    blind = grant(unit_budget=1, mb=0)

    packing.run_window(impl, items(1), blind)
    assert fake_torch.empty_cache_calls == 0, "one window is not evidence"
    assert packing._under_grant_windows == 1

    second = packing.run_window(impl, items(1), blind)
    assert fake_torch.empty_cache_calls == 1
    assert second["measurements"][0]["trimmed"] is True
    assert fake_torch.reserved == 400 * MIB, "the live tensors stayed"


def test_a_memory_blind_grant_ignores_a_pool_too_small_to_be_the_cause(
    fake_torch,
):
    """The other half: a worker squeezed to `mb = 0` by somebody else's memory
    holds nothing worth returning, and releasing every other window for the
    rest of the job would be a per-window `empty_cache()`."""
    fake_torch.reserved = packing.SHRINK_BLIND_SLACK_MB * MIB - MIB
    fake_torch.allocated = 0
    impl = idle_impl()
    for _ in range(6):
        packing.run_window(impl, items(1), grant(unit_budget=1, mb=0))
    assert fake_torch.empty_cache_calls == 0
    assert packing._under_grant_windows == 0


def test_a_blind_release_happens_once_until_a_grant_carries_memory(fake_torch):
    """A pre-fit blind window still runs a few units, so
    on a card somebody else owns the pool regrows its slack and, without a
    latch, the blind rule would release every other window for the whole job.
    After a blind release, blind windows stop counting until a grant with
    memory arrives — which is exactly what distinguishes a pool that freed
    the card (the next grant carries MiB) from one that never will."""
    impl = idle_impl()
    blind = grant(unit_budget=2, mb=0)
    fake_torch.reserved = 22_000 * MIB
    fake_torch.allocated = 400 * MIB
    packing.run_window(impl, items(1), blind)
    packing.run_window(impl, items(1), blind)
    assert fake_torch.empty_cache_calls == 1

    for _ in range(6):
        fake_torch.reserved = 22_000 * MIB  # the slack regrew under the latch
        packing.run_window(impl, items(1), blind)
    assert fake_torch.empty_cache_calls == 1, "no second blind release"

    fake_torch.reserved = 22_000 * MIB
    packing.run_window(impl, items(1), grant(unit_budget=2, mb=113))
    packing.run_window(impl, items(1), blind)
    packing.run_window(impl, items(1), blind)
    assert fake_torch.empty_cache_calls == 2, "a grant with memory re-armed it"


def test_an_impl_release_does_not_re_arm_the_blind_rule(fake_torch):
    """An impl-initiated release is not the harness releasing a pool the
    reactive rule was counting towards: it must leave the latch above alone,
    or the blind rule releases every other window again."""
    from inferio.impl.utils import clear_cache

    impl = idle_impl()
    blind = grant(unit_budget=2, mb=0)
    releases = 0
    for window in range(1, 8):
        fake_torch.reserved = 22_000 * MIB  # the slack regrows every window
        fake_torch.allocated = 400 * MIB
        payload = packing.run_window(impl, items(1), blind)
        releases += bool(payload["measurements"][0].get("trimmed"))
        if window == 3:
            clear_cache()
    assert releases == 1, "the blind rule released once and stayed latched"


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


def test_a_fragmented_pool_is_not_released_for_bytes_the_driver_keeps(fake_torch):
    """`reserved - allocated` counts the free remainder of every
    segment a live block splits, and `empty_cache()` cannot return those: an
    idle GPU measured 992 MiB claimed and **0** returned on a pool split out
    of one big allocation. Slack nets the split term, so the release the
    driver would refuse is never counted towards firing."""
    # 1 024 MiB of pool, 32 MiB live, and every free byte inside a split
    # segment: the pattern that returned nothing.
    fake_torch.reserved = 1024 * MIB
    fake_torch.allocated = 32 * MIB
    fake_torch.inactive_split = 992 * MIB
    impl = idle_impl()
    squeezed = grant(unit_budget=1, mb=1)  # far below the gross 992 MiB
    for _ in range(6):
        packing.run_window(impl, items(1), squeezed)
    assert fake_torch.empty_cache_calls == 0, (
        "a release that returns nothing is not a release worth making"
    )
    assert packing._under_grant_windows == 0, "and no hysteresis accumulates"
    assert fake_torch.reserved == 1024 * MIB


def test_a_clean_pool_of_the_same_size_is_still_released(fake_torch):
    """The control for the test above, byte for byte: the same 1 024 MiB pool
    and the same 32 MiB of live tensors, with the free blocks in whole
    segments. `empty_cache()` returns them, so the rule fires on the second
    window exactly as it did before the split term existed."""
    fake_torch.reserved = 1024 * MIB
    fake_torch.allocated = 32 * MIB
    fake_torch.inactive_split = 0
    impl = idle_impl()
    squeezed = grant(unit_budget=1, mb=1)

    first = packing.run_window(impl, items(1), squeezed)
    assert fake_torch.empty_cache_calls == 0, "one window is not evidence"
    assert "trimmed" not in first["measurements"][0]

    second = packing.run_window(impl, items(1), squeezed)
    assert fake_torch.empty_cache_calls == 1
    assert second["measurements"][0]["trimmed"] is True
    assert fake_torch.reserved == 32 * MIB, "the live tensors stayed"


def test_after_a_release_that_returned_nothing_the_slack_must_grow_first():
    """Metal keeps part of the pool through `empty_cache()` and publishes no
    counter for it, so slack can be claimed that a release does not return.
    After a release that left at least 256 MiB of its slack in the pool, or
    more than it returned, the rule waits up to 30 s for the slack to grow
    256 MiB past what it left, instead of releasing it again every other
    window. A release that returned under 256 MiB keeps the comparator."""

    class FragmentedMps(FakeMpsAllocator):
        kept = 0

        def empty_cache(self):
            self.empty_cache_calls += 1
            self.driver = min(self.driver, self.allocated + self.kept)

    mps = FragmentedMps()

    def windows(count, grant_mb=100, grow_mb=0):
        for _ in range(count):
            mps.driver += grow_mb * MIB
            packing.maybe_shrink(grant_mb)
        return mps.empty_cache_calls

    with mps_host(available_mb=40 * 1024, mps=mps):
        mps.allocate(3000, driver_mb=5000)
        mps.kept = 1000 * MIB
        packing._last_growth = (8, 100.0)
        assert windows(6) == 1, "the release left 1000 MiB of 2000"
        assert packing._last_growth is None, "it returned 1000 MiB"
        assert windows(6, grow_mb=1) == 1, "the slack grew 6 MiB"
        mps.driver += 250 * MIB
        assert windows(2) == 2, "the slack grew 256 MiB past what was left"
        assert windows(4, grant_mb=0) == 2, "memory-blind windows wait as well"
        assert windows(1, grow_mb=300) == 2
        assert windows(1, grow_mb=-100) == 3, "growing past it once is enough"
        mps.driver += 256 * MIB
        assert windows(2, grant_mb=0) == 4, "a memory-blind release"
        assert windows(1) == 4
        mps.driver += 256 * MIB
        assert windows(2, grant_mb=0) == 5, "a grant with memory re-armed it"
        packing.note_trimmed()
        mps.kept = 0
        assert windows(2) == 6, "a trim forgets what a release left"
        mps.driver += 200 * MIB
        assert windows(2) == 7, (
            "a release that returned all its slack left none"
        )
        mps.kept = 255 * MIB
        mps.driver += 1000 * MIB
        assert windows(2) == 8
        assert windows(2) == 9, "a release that left 255 MiB is not waited on"
        mps.kept = 256 * MIB
        mps.driver += 1000 * MIB
        assert windows(2) == 10
        assert windows(2) == 10, "a release that left 256 MiB is waited on"
        packing.release_pool()
        assert windows(2) == 12, "another release forgets what was left"
        packing.note_trimmed()
        mps.driver = mps.allocated + 200 * MIB
        mps.kept = 200 * MIB
        packing._last_growth = (8, 100.0)
        assert windows(2) == 13
        before = packing.time.monotonic()
        assert packing._last_growth == (8, 100.0), "it returned nothing"
        assert windows(4) == 13, "a release that returned less than it left"
        with mock.patch.object(packing.time, "monotonic") as now:
            now.return_value = before + 29
            assert windows(2) == 13
            now.return_value = before + 30
            windows(1)
            assert packing.maybe_shrink(100) is False
            assert mps.empty_cache_calls == 14, "the wait ends after 30 s"
    packing.note_trimmed()
    assert packing._last_growth == (8, 100.0), (
        "a trim that returned nothing keeps it too"
    )
    assert not memory._release_state["armed"]


def test_the_clamp_credits_a_split_pool_the_release_decision_refuses(fake_torch):
    """The two readings ask different questions and only one takes the split
    term. A batch can allocate into the hole inside a split segment, so the
    defensive clamp keeps the gross `reserved - allocated` credit — the same
    1 024/32/992 pool the release decision above prices at zero."""
    fake_torch.free = 250 * MIB
    fake_torch.reserved = 1024 * MIB
    fake_torch.allocated = 32 * MIB
    fake_torch.inactive_split = 992 * MIB
    assert memory.releasable_pool_mb() == 992, "the clamp's credit is gross"
    assert memory.unreturnable_split_mb() == 992, "and the release's is zero"
    live = packing.clamp_to_live_memory(64, 1200)
    assert (live.units, live.clamped) == (64, None), "250 free + 992 of our own"


# --- The impl's shape ceiling ---
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
    # which is what every older worker emitted.
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

    # A window that dies of another error after the impl hit the ceiling
    # still reports it: the failure path is where the orchestrator most needs
    # to know the size was not its choice.
    class Failing:
        def predict(self, inputs):
            fake_oom_retry.record(1)
            fake_oom_retry.note_index_limit()
            raise RuntimeError("unsupported input")

    with pytest.raises(packing.WindowFailure) as caught:
        packing.run_window(Failing(), items(4), grant(unit_budget=4))
    failed = caught.value.measurements[0]
    assert failed["clamped"]["reason"] == "index_limit"
    assert failed["clamped"]["to_units"] == 1
    assert "oom" not in failed, "`classify_oom` is right to refuse it"

    # A ceiling the impl raises without cutting the batch itself (an MPS array
    # over 2^32 bytes before macOS 15): the harness halves the batch and the
    # window's other items run. The clamp is carried by the first batch that
    # ran whole after a split, below the smallest batch that failed; a window
    # that fails on the error reports none.
    class Raising:
        def __init__(self, fails):
            self.fails = fails
            self.batches = []

        def predict(self, inputs):
            self.batches.append(len(inputs))
            if self.fails(inputs):
                raise RuntimeError(
                    "[MPSNDArray initWithDevice:descriptor:] Error: total "
                    "bytes of NDArray > 2**32"
                )
            return [item.data for item in inputs]

    def clamps(measurements):
        return [
            (index, (m["clamped"]["from_units"], m["clamped"]["to_units"]))
            for index, m in enumerate(measurements)
            if "clamped" in m
        ]

    impl = Raising(lambda inputs: len(inputs) > 2)
    payload = packing.run_window(impl, items(5), grant(unit_budget=5))
    assert payload["outputs"] == [0, 1, 2, 3, 4]
    assert impl.batches == [5, 2, 2, 1]
    failed, halved = payload["measurements"][:2]
    assert not {"units", "oom", "clamped"} & failed.keys()
    assert halved["clamped"] == {
        "from_units": 5,
        "to_units": 2,
        "reason": "index_limit",
        "free_mb": 8000,
    }
    assert clamps(payload["measurements"]) == [(1, (5, 2))]

    impl = Raising(lambda inputs: len(inputs) > 1)
    payload = packing.run_window(impl, items(5), grant(unit_budget=5))
    assert impl.batches == [5, 2, 1, 1, 1, 1, 1]
    assert clamps(payload["measurements"]) == [(2, (2, 1))]

    # A batch the live memory clamp cut below the halved size is not the
    # first to run at it.
    def fails_and_frees(inputs):
        fake_torch.free = (250 if len(inputs) > 4 else 8000) * MIB
        return len(inputs) > 4

    impl = Raising(fails_and_frees)
    payload = packing.run_window(impl, items(8), grant(unit_budget=8))
    assert impl.batches == [8, 2, 4, 2]
    assert clamps(payload["measurements"]) == [(1, (8, 2)), (2, (8, 4))]
    assert "reason" not in payload["measurements"][1]["clamped"]

    # Items of different sizes: the one clamp runs from the smallest batch
    # that failed to the largest that ran below it, here not the last.
    def pixel_window(widths, fails, budget):
        impl = Raising(fails)
        mixed = [
            PredictionInput(data=100 * width, file=png_bytes(width, 100))
            for width in widths
        ]
        payload = packing.run_window(
            impl,
            mixed,
            grant(unit_budget=budget, unit="pixel", aggregation="sum"),
        )
        return impl.batches, clamps(payload["measurements"])

    def over(limit):
        return lambda inputs: sum(item.data for item in inputs) > limit

    assert pixel_window([120] * 4 + [1] * 4, over(30000), 48400) == (
        [8, 4, 2, 2, 2, 2],
        [(2, (48000, 24000))],
    )
    # A batch below the halved item count counts as well.
    assert pixel_window([1] * 6 + [290], over(29000), 40000) == (
        [7, 3, 3, 1],
        [(1, (29600, 29000))],
    )
    # A padded batch fails on items times its largest item, so one that ran
    # can price above one that failed; it does not count.
    def padded(inputs):
        return len(inputs) * max(item.data for item in inputs) > 30000

    assert pixel_window([1, 1, 1, 120, 120, 120], padded, 24000) == (
        [4, 2, 2, 2],
        [(1, (12300, 12100))],
    )
    # A batch that ran whole before the first failure counts.
    widths = [150, 150, 150, 100, 100, 50, 50]
    assert pixel_window(widths, over(30000), 40000) == (
        [2, 4, 2, 2, 1],
        [(2, (40000, 30000))],
    )

    # A batch that ran whole below a later, smaller failure still counts.
    def holds_150(inputs):
        return len(inputs) > 4 or (
            len(inputs) > 1 and any(item.data == 15000 for item in inputs)
        )

    widths = [100] * 3 + [300] * 3 + [150, 100, 100] + [100] * 3
    assert pixel_window(widths, holds_150, 185000) == (
        [12, 6, 3, 3, 3] + [1] * 6,
        [(2, (35000, 30000))],
    )

    # A batch that ran at a size that later failed is not below it.
    impl = Raising(
        lambda inputs: len(inputs) > 4
        or (len(inputs) > 2 and any(item.data >= 4 for item in inputs))
    )
    payload = packing.run_window(impl, items(8), grant(unit_budget=8))
    assert impl.batches == [8, 4, 4, 2, 2]
    assert clamps(payload["measurements"]) == [(1, (4, 2))]

    # A batch the impl's own OOM halving touched did not run at the size.
    def fails_then_halves(inputs):
        if len(impl.batches) == 2:
            fake_oom_retry.record(4, halvings=1)
        return len(inputs) > 4

    impl = Raising(fails_then_halves)
    payload = packing.run_window(impl, items(8), grant(unit_budget=8))
    halved = payload["measurements"][1]
    assert halved["oom"] is True and "clamped" not in halved
    assert clamps(payload["measurements"]) == [(2, (8, 4))]

    def absorbs_200(inputs):
        if [item.data for item in inputs] == [20000]:
            fake_oom_retry.record(1, halvings=1)
        return len(inputs) > 1

    assert pixel_window([150, 200, 50], absorbs_200, 30000) == (
        [1, 2, 1, 1],
        [(3, (25000, 15000))],
    )

    # A batch the live memory clamp cut still ran whole at its size.
    def fails_then_clamps(inputs):
        low = len(inputs) < 4 and any(item.data == 20000 for item in inputs)
        fake_torch.free = (250 if low else 8000) * MIB
        return sum(item.data for item in inputs) > 20000

    batches, found = pixel_window([1, 150, 200, 200], fails_then_clamps, 55100)
    assert batches == [4, 2, 2, 1, 1]
    assert found[0] == (1, (40000, 20000))
    fake_torch.free = 8000 * MIB

    def fails_on_item_5(inputs):
        return any(item.data == 5 for item in inputs)

    def frees_then_fails_on_item_5(inputs):
        fake_torch.free = (250 if len(inputs) > 4 else 8000) * MIB
        return fails_on_item_5(inputs)

    impl = Raising(frees_then_fails_on_item_5)
    with pytest.raises(packing.WindowFailure) as caught:
        packing.run_window(impl, items(8), grant(unit_budget=8))
    assert impl.batches == [8, 2, 4, 2, 2, 1, 1]
    memory_clamp = caught.value.measurements[1]["clamped"]
    assert clamps(caught.value.measurements) == [(1, (8, 2))]
    assert "reason" not in memory_clamp

    # The same item through an impl that halves on its own: the harness does
    # not split again.
    class Retrying(Raising):
        def predict(self, inputs):
            return impl_utils.run_with_oom_retry(
                super().predict, inputs, oom_exceptions=MemoryError
            )

    impl = Retrying(fails_on_item_5)
    with mock.patch.dict(sys.modules, {"inferio.impl.utils": impl_utils}):
        with pytest.raises(packing.WindowFailure) as caught:
            packing.run_window(impl, items(8), grant(unit_budget=8))
    assert impl.batches == [8, 4, 4, 2, 1, 1]
    assert len(caught.value.measurements) == 1
    assert clamps(caught.value.measurements) == []


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


# --- Spill-capable hosts (Windows display driver) ---


def caching_impl(cuda, mb_per_item):
    """An impl whose batch needs `mb_per_item` per input: the pool grows to
    fit it and stays cached, while the batch's tensors are freed."""

    def predict(inputs):
        need = mb_per_item[0] * len(inputs) * MIB
        cuda.reserved = max(cuda.reserved, need)
        cuda.peak_reserved = max(cuda.peak_reserved, cuda.reserved)
        cuda.peak_allocated = max(cuda.peak_allocated, need)
        cuda.allocated = 0
        return [entry.data for entry in inputs]

    return SimpleNamespace(predict=predict)


def nvml_card(cuda, monkeypatch, total_mb=8192, others_mb=1000):
    """NVML for an 8 GiB card with `others_mb` used by other processes. Our
    pool is on the card only up to what they leave; the rest is in RAM."""

    def reading():
        ours = min(cuda.reserved // MIB, total_mb - others_mb)
        return (total_mb - others_mb - ours, total_mb)

    monkeypatch.setattr(memory, "_nvml_memory", reading)
    monkeypatch.setitem(memory._release_state, "armed", False)
    monkeypatch.setitem(memory._release_state, "largest_units", None)
    monkeypatch.setitem(memory._release_state, "grantless_size", None)


@pytest.fixture
def spill_host(fake_torch, monkeypatch):
    monkeypatch.setattr(memory, "spill_capable", lambda: True)
    monkeypatch.setattr(packing, "_spill_persists", False)
    nvml_card(fake_torch, monkeypatch)
    return fake_torch


def test_only_cuda_under_the_windows_display_driver_can_spill(
    fake_torch, monkeypatch, tmp_path
):
    dxg = tmp_path / "dxg"
    monkeypatch.setattr(memory, "DXG_DEVICE", str(dxg))
    monkeypatch.setattr(memory.sys, "platform", "linux")
    monkeypatch.delenv(memory.SPILL_VERDICT_ENV, raising=False)
    assert not memory.spill_capable(), "Linux"
    dxg.touch()
    assert memory.spill_capable(), "WSL2 or Docker Desktop"
    cuda_torch = SimpleNamespace(cuda=FakeCuda(), dtype=type)
    with cpu_host(torch_module=cuda_torch):
        assert not memory.spill_capable(), "the CPU device"
    with mps_host(available_mb=8_000):
        assert not memory.spill_capable(), "MPS"
    dxg.unlink()
    monkeypatch.setattr(memory.sys, "platform", "win32")
    assert memory.spill_capable(), "native Windows"
    # The orchestrator's per-GPU verdict wins over the platform.
    monkeypatch.setenv(memory.SPILL_VERDICT_ENV, "0")
    assert not memory.spill_capable(), "a TCC card on native Windows"
    monkeypatch.setattr(memory.sys, "platform", "linux")
    monkeypatch.setenv(memory.SPILL_VERDICT_ENV, "1")
    assert memory.spill_capable()


def run_growing_windows(cuda):
    """Windows of 2, 2, then 4 + 1, then 4 items at 100 MiB per item."""
    impl = caching_impl(cuda, [100])
    return [
        packing.run_window(impl, items(count), grant(unit_budget=budget))
        for count, budget in ((2, 2), (2, 2), (5, 4), (4, 4))
    ]


def test_a_spill_host_releases_the_pool_before_a_growing_batch_only(spill_host):
    """Only the batch of 4 is larger than every batch since the last release;
    the first batch has no earlier one, and the tail of 1 and the later 4 fit
    the pool already held."""
    payloads = run_growing_windows(spill_host)
    assert spill_host.empty_cache_calls == 1
    growing = payloads[2]["measurements"][0]
    assert growing["items"] == 4
    assert growing["reserved_before_mb"] == 0, "released just before it"
    assert growing["regrow_after"] == memory.GROWTH_RELEASE
    assert growing["free_mb"] == 8192 - 1000, "free is read after the release"
    others = [m for p in payloads for m in p["measurements"] if m is not growing]
    assert all("regrow_after" not in m for m in others)


def test_no_release_or_spill_flag_off_a_spill_capable_host(
    fake_torch, monkeypatch, tmp_path
):
    """Linux CUDA: the growing windows release nothing, and a pool 1000 MiB
    larger than the card is not flagged."""
    monkeypatch.setattr(memory, "DXG_DEVICE", str(tmp_path / "dxg"))
    monkeypatch.setattr(memory.sys, "platform", "linux")
    monkeypatch.setattr(memory, "_malloc_trim", lambda: None)
    nvml_card(fake_torch, monkeypatch)
    payloads = run_growing_windows(fake_torch)
    impl = caching_impl(fake_torch, [8192 + 1000])
    payloads.append(packing.run_window(impl, items(1), grant(unit_budget=1)))
    payloads.append(packing.run_grantless_window(impl, items(2)))
    assert [m["items"] for m in payloads[2]["measurements"]] == [4, 1]
    assert fake_torch.reserved == 2 * (8192 + 1000) * MIB
    assert fake_torch.empty_cache_calls == 0
    assert not any(m.get("spilled") for p in payloads for m in p["measurements"])


def test_the_backstop_needs_nvml(fake_torch, monkeypatch):
    """Without NVML the free reading is torch's own, and no spill is judged."""
    monkeypatch.setattr(memory, "spill_capable", lambda: True)
    monkeypatch.setitem(memory._release_state, "largest_units", None)
    impl = caching_impl(fake_torch, [8192 + 1000])
    payload = packing.run_window(impl, items(1), grant(unit_budget=1))
    assert payload["memory"]["free_source"] == "torch"
    assert "spilled" not in payload["measurements"][0]
    assert fake_torch.empty_cache_calls == 0


def test_a_grantless_window_releases_the_pool_before_a_larger_input_only(
    spill_host, caplog
):
    """An impl that runs one input at a time: the pool is released before a
    window whose largest input has more pixels than any since the last
    release. The third window's largest input is in the middle and has less
    width than the first, the second and fourth have more pixels only in sum,
    and a trim restarts the record; the three-item pools are 208 MiB above
    NVML's used memory, within the tolerance."""
    assert len(png_bytes(30, 45)) == len(png_bytes(40, 30)), "same PNG bytes"
    impl = caching_impl(spill_host, [2800])
    impl.enable_batching = False

    def run(window):
        inputs = [PredictionInput(data=0, file=png_bytes(w, h)) for w, h in window]
        return packing.run_grantless_window(impl, inputs)

    with caplog.at_level(logging.DEBUG, logger="inferio_worker.packing"):
        payloads = [
            run(window)
            for window in (
                [(40, 30)],
                [(30, 40), (20, 20)],
                [(40, 30), (30, 45), (20, 20)],
                [(40, 30), (40, 30), (40, 30)],
            )
        ]
        memory.empty_cache(memory.TRIM_RELEASE)
        payloads.append(run([(50, 30)]))
    assert spill_host.empty_cache_calls == 2, "the third window and the trim"
    assert [p["measurements"][0].get("regrow_after") for p in payloads] == [
        None, None, memory.GROWTH_RELEASE, None, memory.TRIM_RELEASE
    ]
    assert not any(p["measurements"][0].get("spilled") for p in payloads)
    assert not [r for r in caplog.records if r.levelno == logging.WARNING]


def test_a_batching_grantless_window_is_sized_by_its_input_count(spill_host):
    """One batch pads to its largest input, so three images outgrow one."""
    impl = caching_impl(spill_host, [100])
    image = PredictionInput(data=0, file=png_bytes(40, 30))
    packing.run_grantless_window(impl, [image])
    payload = packing.run_grantless_window(impl, [image] * 3)
    assert payload["measurements"][0]["regrow_after"] == memory.GROWTH_RELEASE


def test_a_grantless_window_that_spills_is_flagged_and_releases_the_pool(
    spill_host, caplog
):
    mb_per_item = [8192 + 1000]
    impl = caching_impl(spill_host, mb_per_item)
    with caplog.at_level(logging.DEBUG, logger="inferio_worker.packing"):
        spilled = packing.run_grantless_window(impl, items(1))
        mb_per_item[0] = 100
        after = packing.run_grantless_window(impl, items(1))
    assert spilled["measurements"][0]["spilled"] is True
    assert spilled["memory"]["reserved_mb"] == 0, "the released pool"
    assert spill_host.empty_cache_calls == 1
    assert after["measurements"][0].get("spilled") is None
    assert after["measurements"][0]["regrow_after"] == memory.SPILL_RELEASE
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1


def test_any_release_restarts_the_largest_batch_record(spill_host):
    """After a trim, the next batch regrows from the released pool, so it needs
    no release of its own however large it is."""
    impl = caching_impl(spill_host, [100])
    packing.run_window(impl, items(2), grant(unit_budget=2))
    memory.empty_cache(memory.TRIM_RELEASE)
    packing.run_window(impl, items(4), grant(unit_budget=4))
    assert spill_host.empty_cache_calls == 1, "the trim only"


@pytest.mark.parametrize("over_mb, spilled", [(512, False), (513, True)])
def test_the_spill_backstop_fires_above_the_tolerance_only(
    spill_host, over_mb, spilled
):
    """Pool minus NVML's used memory: at most 512 MiB is not a spill."""
    impl = caching_impl(spill_host, [8192 + over_mb])
    payload = packing.run_window(impl, items(1), grant(unit_budget=1))
    assert payload["measurements"][0].get("spilled", False) is spilled
    assert payload["outputs"] == [0], "the batch's outputs stand"


def test_a_spill_mid_window_releases_and_halves_the_rest_of_it(spill_host):
    """A batch of 8 at 1100 MiB each overshoots the card by 608 MiB. The pool
    is released and the other 8 items run as two batches of 4."""
    emitted: list[dict] = []
    impl = caching_impl(spill_host, [1100])
    payload = packing.run_window(
        impl, items(16), grant(unit_budget=8), emitted.append
    )
    measurements = payload["measurements"]
    assert [m["items"] for m in measurements] == [8, 4, 4]
    assert [m.get("spilled", False) for m in measurements] == [True, False, False]
    assert measurements[1]["reserved_before_mb"] == 0
    assert emitted[0]["reserved_mb"] == 0, "the frame after the release"
    assert payload["outputs"] == list(range(16))
    assert spill_host.empty_cache_calls == 1


def test_the_flag_after_a_spill_is_judged_against_the_grant(spill_host):
    """A spill halves the budget for the rest of the window; the flag still
    compares against the grant, both for the next item and for half of it."""
    impl = caching_impl(spill_host, [1100])
    payload = packing.run_window(impl, items(16), grant(unit_budget=8))
    assert [m["units"] for m in payload["measurements"]] == [8, 4, 4]
    assert next_over_budget(payload) == [True, False, False], (
        "a fifth item fits the grant of 8, whatever the halved budget says"
    )



def test_half_the_budget_after_a_spill_is_half_the_grant(spill_host):
    """100 tokens spill and halve the budget to 50; 30 tokens then have no
    room for 90 within the grant, but are under half of it."""
    impl = caching_impl(spill_host, [4500])
    texts = [PredictionInput(data="x" * 4 * n) for n in (60, 40, 30, 90)]
    payload = packing.run_window(
        impl, texts, grant(unit="token", aggregation="sum", unit_budget=100)
    )
    measurements = payload["measurements"]
    assert [m["units"] for m in measurements] == [100, 30, 90]
    assert measurements[0]["spilled"] is True
    assert next_over_budget(payload) == [True, False, False]


def test_a_spilled_batch_is_not_the_throughput_comparator(spill_host):
    impl = caching_impl(spill_host, [8192 + 1000])
    payload = packing.run_window(impl, items(1), grant(unit_budget=1))
    assert payload["measurements"][0]["spilled"] is True
    assert packing._last_growth is None


def test_halving_after_a_spill_never_exceeds_the_grant(spill_host):
    """A 400-token item alone overruns a 100-token grant; the batches after
    its spill still stay within the grant."""
    impl = caching_impl(spill_host, [9000])
    texts = [PredictionInput(data="x" * 1600)] + [
        PredictionInput(data="x" * 160) for _ in range(5)
    ]
    payload = packing.run_window(
        impl, texts, grant(unit="token", aggregation="sum", unit_budget=100)
    )
    measurements = payload["measurements"]
    assert measurements[0]["units"] == 400
    assert all(m["spilled"] for m in measurements)
    assert all(m["units"] <= 100 for m in measurements[1:])


def test_a_spill_that_outlives_its_release_releases_nothing_until_one_fits(
    spill_host, caplog
):
    """Live memory past the card: the first release gives nothing back, so
    later spills release nothing and log at debug, still halving, until a
    batch that fits re-arms the release and the warning."""
    live_mb = [8192 + 1000]

    def predict(inputs):
        spill_host.reserved = spill_host.allocated = live_mb[0] * MIB
        spill_host.peak_reserved = spill_host.reserved
        return [None] * len(inputs)

    impl = SimpleNamespace(predict=predict)
    with caplog.at_level(logging.DEBUG, logger="inferio_worker.packing"):
        first = packing.run_window(impl, items(8), grant(unit_budget=4, mb=0))
        assert memory._release_state["largest_units"] == 4, (
            "a release that returned nothing keeps the largest batch"
        )
        second = packing.run_window(impl, items(2), grant(unit_budget=1, mb=0))
        packing.run_grantless_window(impl, items(1))
        live_mb[0] = 100
        packing.run_grantless_window(impl, items(1))
        live_mb[0] = 8192 + 1000
        packing.run_window(impl, items(1), grant(unit_budget=1, mb=0))
    measurements = first["measurements"] + second["measurements"]
    assert [m["items"] for m in measurements] == [4, 2, 1, 1, 1, 1]
    assert all(m["spilled"] for m in measurements)
    assert [m.get("regrow_after") for m in measurements] == [None] * 6, (
        "a release that returned nothing arms no re-grow report"
    )
    assert spill_host.empty_cache_calls == 2, "the first spill, and the last"
    spills = [r for r in caplog.records if "system memory" in r.getMessage()]
    assert [r.levelno for r in spills] == (
        [logging.WARNING] + [logging.DEBUG] * 6 + [logging.WARNING]
    )
