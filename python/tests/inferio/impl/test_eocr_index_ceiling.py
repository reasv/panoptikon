"""easyOCR's detector batch has a ceiling that is not about memory.

Run2 surprise S1. At batch 29 of 2560-bounded A4 pages `torch.max_pool2d`
inside CRAFT's VGG backbone raises `RuntimeError: integer out of range` — a
32-bit index overflow in the pooling kernel — with 3 GiB of a 96 GiB board
still free. Batch 28 runs clean. The impl caught the failure with a bare
`except Exception`, logged one line without the traceback and reprocessed the
window one image at a time, so:

* the ledger saw no out-of-memory condition, no `clamped`, and kept widening
  `unit_budget` past a batch this impl cannot execute;
* the only symptom was throughput — 3.10 items/s at 16 against 0.91 from 29
  up, a 3.2x loss that looks like nothing but a slow window;
* `--bisect-oom` reported `largest_ok_items: 37` against a true 28.

This file pins the fix from both ends: the ceiling **formula** (derived from
the kernel's own limit, not from the measured boundary), and the behaviour
when a batch meets it anyway — halve, never claim an OOM, never fall back
silently.

The `2**31 - 1` constant is monkeypatched down in the behavioural tests so
the real arithmetic can run over images small enough to build in a unit test;
the formula tests use the real constant and the real measured shapes.
"""

from __future__ import annotations

import io
import logging

import pytest
from PIL import Image

from inferio.impl import eocr
from inferio.impl import utils as impl_utils
from inferio.impl.eocr import (
    DETECTOR_CANVAS_SIZE,
    KERNEL_INDEX_ELEMENT_LIMIT,
    EasyOCRModel,
    bounded_dims,
    detector_pool_elements,
    detector_tensor_dims,
    max_detector_batch,
)
from inferio.inferio_types import PredictionInput

# The group run2's probes measured the boundary on, and what it becomes:
# 2480x3508 fitted to the 2560 canvas is 1809x2560, padded up to the next
# multiple of 32 by `easyocr.imgproc.resize_aspect_ratio`.
SCAN = (2480, 3508)
SCAN_TENSOR = (2560, 1824)  # (height, width) of the detector's input
MEASURED_CEILING = 28  # 28 ok, 29 fails (run2-probes-report.md, S1)


def png(width: int, height: int) -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (width, height), (30, 60, 90)).save(buffer, format="PNG")
    return buffer.getvalue()


def inputs(count: int, size, **config):
    return [PredictionInput(data=dict(config), file=png(*size)) for _ in range(count)]


class IndexLimitedReader:
    """`easyocr.Reader` that overflows a 32-bit index above `ceiling` items.

    The failure is reproduced verbatim — `RuntimeError("integer out of
    range")` raised from `detect` — because the message *is* the signal: it
    is `at::native::safe_downcast`'s `TORCH_CHECK` text, and both the retry
    helper's classifier and this impl's fallback log key on it.
    """

    def __init__(self, ceiling: int = 10**9, error=None):
        self.ceiling = ceiling
        self.error = error
        self.detected: list[int] = []
        self.single: list[tuple[int, ...]] = []

    @staticmethod
    def _result():
        box = [[10.0, 20.0], [30.0, 20.0], [30.0, 40.0], [10.0, 40.0]]
        return [[box, "hello", 0.9]]

    def detect(self, images, **params):
        count = int(images.shape[0])
        self.detected.append(count)
        if self.error is not None:
            raise self.error
        if count > self.ceiling:
            raise RuntimeError("integer out of range")
        return [[[10.0, 60.0, 20.0, 70.0]] for _ in range(count)], [
            [] for _ in range(count)
        ]

    def recognize(self, image, horizontal_list=None, free_list=None, **params):
        return self._result()

    def readtext(self, image, **params):
        self.single.append(tuple(image.shape))
        return self._result()


def stubbed(reader: IndexLimitedReader, **kwargs) -> EasyOCRModel:
    model = EasyOCRModel(**kwargs)
    model.load = lambda: None  # type: ignore[method-assign]
    model.model = reader
    model._model_loaded = True
    return model


class Counters:
    """The two process totals the worker diffs across one `predict` call."""

    def __init__(self):
        self.index = impl_utils.total_index_limit_events()
        self.oom = impl_utils.total_oom_halvings()

    @property
    def index_events(self) -> int:
        return impl_utils.total_index_limit_events() - self.index

    @property
    def oom_halvings(self) -> int:
        return impl_utils.total_oom_halvings() - self.oom


# ---------------------------------------------------------------------------
# The formula
# ---------------------------------------------------------------------------


def test_the_formula_reproduces_the_measured_boundary():
    """28 ok / 29 fail, from the kernel's limit rather than from the probe.

    `max_pool2d_with_indices` launches over its **output element count**
    downcast to a signed 32-bit int (`ATen/native/Pool.h`'s `safe_downcast`,
    whose message is the literal "integer out of range"). The binding pool is
    the first one in `vgg16_bn` — `features[6]`, a `MaxPool2d(2, 2)` over the
    64-channel block — so one item costs `64 * H//2 * W//2` elements of it.
    """
    assert detector_tensor_dims([SCAN[::-1]], DETECTOR_CANVAS_SIZE) == SCAN_TENSOR
    per_item = detector_pool_elements(*SCAN_TENSOR)
    assert per_item == 64 * 1280 * 912 == 74_711_040
    assert MEASURED_CEILING * per_item <= KERNEL_INDEX_ELEMENT_LIMIT
    assert (MEASURED_CEILING + 1) * per_item > KERNEL_INDEX_ELEMENT_LIMIT
    assert (
        max_detector_batch([SCAN[::-1]], DETECTOR_CANVAS_SIZE) == MEASURED_CEILING
    )


def test_the_first_pool_is_the_one_that_binds():
    """Every other tensor CRAFT's forward builds allows a larger batch.

    Each later pool halves both spatial dimensions while only doubling the
    channels, so it is half the size of the one before; the `B x 3 x H x W`
    input is 21x smaller than the first pool's output again. If any of these
    bound instead, the measured boundary would not be 28.
    """
    height, width = SCAN_TENSOR
    first = detector_pool_elements(height, width)
    later = [
        channels * (height // divisor // 2) * (width // divisor // 2)
        for channels, divisor in ((128, 2), (256, 4), (512, 8))
    ]
    assert all(elements < first for elements in later)
    assert 3 * height * width < first
    assert all(
        KERNEL_INDEX_ELEMENT_LIMIT // elements > MEASURED_CEILING
        for elements in later + [3 * height * width]
    )


def test_smaller_pages_allow_a_larger_batch():
    """The cap is per batch, from that batch's own padded dimensions — which
    is the whole reason it is asked per batch and not fixed at a constant."""
    ceilings = {
        (2480, 3508): 28,  # A4 at 300 dpi, the measured group
        (2560, 2560): 20,  # the square canvas: this impl's worst case
        (1240, 1754): 61,  # below the canvas, so not resized at all
        (8000, 6000): 27,  # a big sheet, fitted to 2560x1920
    }
    for size, expected in ceilings.items():
        assert max_detector_batch([size], DETECTOR_CANVAS_SIZE) == expected, size


def test_a_mixed_batch_is_capped_by_the_frame_it_pads_to():
    """`pad_images_to_same_size` takes the element-wise maximum, so the frame
    can be taller than any member is tall *and* wider than any is wide."""
    portrait, landscape = (1000, 2560), (2560, 1000)
    assert bounded_dims(portrait[::-1]) == (2560, 1000)
    assert bounded_dims(landscape[::-1]) == (1000, 2560)
    assert detector_tensor_dims([portrait, landscape]) == (2560, 2560)
    assert max_detector_batch([portrait, landscape]) == 20
    assert max_detector_batch([portrait]) == 51


def test_a_magnifying_caller_is_priced_at_what_it_magnifies_to():
    """`mag_ratio` scales the detector's input up to — never past — the
    canvas (`imgproc.resize_aspect_ratio` clamps its own target), so the
    ceiling has to read it and can never fall below the square-canvas one."""
    small = [(640, 480)]
    assert max_detector_batch(small, DETECTOR_CANVAS_SIZE, 1.0) == 436
    assert max_detector_batch(small, DETECTOR_CANVAS_SIZE, 4.0) == 27
    assert max_detector_batch(small, DETECTOR_CANVAS_SIZE, 100.0) == 27
    assert detector_tensor_dims(small, DETECTOR_CANVAS_SIZE, 100.0) == (2560, 1920)


def test_the_ceiling_is_never_below_one():
    """A single item over the limit has no smaller batch to fall back to;
    saying so is the per-image fallback's job, not the formula's."""
    assert max_detector_batch([(40000, 40000)], 40000) == 1
    assert max_detector_batch([]) is None
    assert max_detector_batch([None, None]) is None


# ---------------------------------------------------------------------------
# The hook the packing harness asks
# ---------------------------------------------------------------------------


def test_max_batch_for_reads_the_harnesss_width_height_pairs():
    """The harness reads `Image.size`, so the pairs arrive PIL-ordered."""
    model = stubbed(IndexLimitedReader())
    assert model.max_batch_for([SCAN] * 4) == MEASURED_CEILING
    assert model.max_batch_for([SCAN[::-1]] * 4) == MEASURED_CEILING, (
        "a transposed page pads to a transposed frame of the same area"
    )


def test_an_unreadable_shape_is_charged_the_square_canvas():
    """A shape we cannot see must not be assumed small — the same principle
    as the harness's own unreadable-input pricing, in the same direction."""
    model = stubbed(IndexLimitedReader())
    assert model.max_batch_for([None]) == 20
    assert model.max_batch_for([SCAN, None]) == 20
    for junk in ((0, 100), (-1, -1), ("a", "b"), (5,), None):
        assert model.max_batch_for([junk]) == 20, junk


def test_an_unbatched_impl_states_no_ceiling():
    """`enable_batching = false` builds no batch tensor at all: easyOCR
    bounds each `readtext` call by itself, so there is nothing to cap and
    capping would only cost throughput."""
    model = stubbed(IndexLimitedReader(), enable_batching=False)
    assert model.max_batch_for([SCAN] * 64) is None


def test_a_per_request_canvas_moves_the_ceiling_with_it():
    """The C7nc control raises `canvas_size` to free the pricing; the ceiling
    is a fact about the tensor, so it has to follow."""
    uncapped = stubbed(IndexLimitedReader(), canvas_size=40000)
    assert uncapped.canvas_pixels == 40000 * 40000
    assert uncapped.max_batch_for([SCAN] * 4) == 15, (
        "unbounded, an A4 scan reaches the detector at 2496x3520, not 1824x2560"
    )


# ---------------------------------------------------------------------------
# What happens when a batch meets the ceiling
# ---------------------------------------------------------------------------


@pytest.fixture
def small_limit(monkeypatch):
    """Scale the kernel's element limit down so the real arithmetic can be
    exercised over images a unit test can afford to build.

    A 128x96 page costs `64 * 64 * 48 = 196 608` pooling-output elements, so
    a limit of 400 000 puts the ceiling at exactly 2 items. Nothing else is
    faked: the shapes, the padding, the ceil-to-32 and the division are the
    shipped ones.
    """
    monkeypatch.setattr(eocr, "KERNEL_INDEX_ELEMENT_LIMIT", 400_000)
    return 2


def test_the_impl_caps_its_own_batch_before_the_kernel_ever_sees_it(
    small_limit, caplog
):
    """The batch is chunked at the ceiling, and the chunking is *reported*.

    `note_index_limit_event` is what lets the worker put
    `clamped.reason = "index_limit"` on this batch's measurement, so the
    ledger learns the batch ran short of its budget for a reason that is not
    memory — instead of seeing a silently slower window (run2 S1).
    """
    caplog.set_level(logging.WARNING, logger="inferio.impl.eocr")
    reader = IndexLimitedReader()
    model = stubbed(reader)
    counters = Counters()

    outputs = model.predict(inputs(5, (128, 96)))

    assert len(outputs) == 5
    assert reader.detected == [2, 2, 1], "chunked at the ceiling, in order"
    assert reader.single == [], "and never fell back to per-image processing"
    assert counters.index_events == 1
    assert counters.oom_halvings == 0, "a shape ceiling is not an OOM"
    assert "capping easyOCR's detector batch at 2 of 5" in caplog.text
    assert "128x96" in caplog.text and "196608" in caplog.text


def test_a_batch_under_the_ceiling_is_neither_capped_nor_reported(small_limit):
    reader = IndexLimitedReader()
    model = stubbed(reader)
    counters = Counters()
    model.predict(inputs(2, (128, 96)))
    assert reader.detected == [2]
    assert (counters.index_events, counters.oom_halvings) == (0, 0)


def test_an_index_limit_that_is_raised_anyway_halves_and_is_not_an_oom(caplog):
    """The backstop, for a ceiling the impl's own formula did not predict —
    a torch version that binds on a different tensor, say. It halves exactly
    as an out-of-memory condition would, because it is size-dependent, and it
    is counted as a shape ceiling rather than as a memory event, because
    reporting it as one would deflate a model with a nearly empty board.
    """
    caplog.set_level(logging.WARNING)
    reader = IndexLimitedReader(ceiling=3)
    model = stubbed(reader)
    counters = Counters()

    outputs = model.predict(inputs(6, (128, 96)))

    assert len(outputs) == 6
    assert reader.detected == [6, 3, 3], "one halving, then it fits"
    assert reader.single == []
    assert counters.index_events == 1
    assert counters.oom_halvings == 0
    halving = [
        record
        for record in caplog.records
        if "32-bit element index overflowed" in record.getMessage()
    ]
    assert len(halving) == 1
    assert halving[0].exc_info is not None, "the traceback is kept, not dropped"


def test_an_index_limit_at_a_single_input_falls_back_with_its_traceback(caplog):
    """Nothing smaller to try, and it is not an out-of-memory condition, so
    it must not become an `InferenceOOMError`. The per-image fallback takes
    it — and says so with the classification, the batch size and the padded
    dimensions the old one-line `logger.error` never named."""
    caplog.set_level(logging.WARNING, logger="inferio.impl.eocr")
    reader = IndexLimitedReader(ceiling=0)
    model = stubbed(reader)
    counters = Counters()

    outputs = model.predict(inputs(2, (128, 96)))

    assert len(outputs) == 2
    assert len(reader.single) == 2, "the per-image fallback ran"
    assert counters.oom_halvings == 0, "and it was never called an OOM"
    fallback = [
        record
        for record in caplog.records
        if "falling back to per-image processing" in record.getMessage()
    ]
    assert len(fallback) == 1
    message = fallback[0].getMessage()
    assert "2 inputs" in message
    assert "128x96" in message, "the padded detector tensor is named"
    assert "a kernel index ceiling" in message, "and classified"
    assert fallback[0].exc_info is not None


def test_any_other_failure_falls_back_with_the_full_traceback(caplog):
    """Unchanged behaviour, no longer silent: the fallback is still per
    image, but the traceback that used to be discarded is logged, and the
    line no longer claims a classification it did not make."""
    caplog.set_level(logging.WARNING, logger="inferio.impl.eocr")
    reader = IndexLimitedReader(error=ValueError("a processor said no"))
    model = stubbed(reader)
    counters = Counters()

    outputs = model.predict(inputs(3, (128, 96)))

    assert len(outputs) == 3
    assert len(reader.single) == 3
    assert counters.index_events == 0, "not a shape ceiling, and not called one"
    assert counters.oom_halvings == 0
    fallback = [
        record
        for record in caplog.records
        if "falling back to per-image processing" in record.getMessage()
    ]
    assert len(fallback) == 1
    assert "ValueError" in fallback[0].getMessage()
    assert "a kernel index ceiling" not in fallback[0].getMessage()
    assert fallback[0].exc_info is not None


def test_the_ceiling_never_shrinks_a_batch_the_canvas_already_bounded(small_limit):
    """The two bounds compose the right way round: `fit_to_canvas` runs
    first, so the ceiling is computed on the array the detector will actually
    receive, not on the raw submission."""
    reader = IndexLimitedReader()
    model = stubbed(reader, canvas_size=128)
    # 512x384 submitted, bounded to 128x96 — the same tensor as the test
    # above, and therefore the same ceiling of 2.
    model.predict(inputs(4, (512, 384)))
    assert reader.detected == [2, 2]
