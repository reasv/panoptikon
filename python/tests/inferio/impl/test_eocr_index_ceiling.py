"""easyOCR's detector batch has a ceiling that is not about memory: a 32-bit
index overflow in CRAFT's first pooling kernel, which refuses batch 29 of
2560-bounded A4 pages with 3 GiB of a 96 GiB GPU still free. Both ends of the
fix are pinned here — the *formula*, derived from the kernel's limit rather
than the measured boundary, and the behaviour when a batch meets it anyway:
halve, never claim an OOM, never fall back silently. See
docs/inferio-worker-protocol.md, "The easyOCR ceiling in full". The `2**31 - 1`
constant is monkeypatched down below so the real arithmetic runs over images a
unit test can afford."""

from __future__ import annotations

import io
import logging
from types import SimpleNamespace

import pytest
from PIL import Image

from inferio.impl import eocr
from inferio.impl import utils as impl_utils
from inferio.impl.eocr import (
    DETECTOR_CANVAS_SIZE, KERNEL_INDEX_ELEMENT_LIMIT, EasyOCRModel,
    bounded_dims, detector_pool_elements, detector_tensor_dims,
    max_detector_batch,
)
from inferio.inferio_types import PredictionInput

# The group run2's probes measured the boundary on, and what it becomes:
# 2480x3508 fitted to the 2560 canvas is 1809x2560, padded up to the next
# multiple of 32 by `easyocr.imgproc.resize_aspect_ratio`.
SCAN = (2480, 3508)
SCAN_TENSOR = (2560, 1824)  # (height, width) of the detector's input
MEASURED_CEILING = 28  # 28 ok, 29 fails
RESULT = [[[[10.0, 20.0], [30.0, 20.0], [30.0, 40.0], [10.0, 40.0]], "hi", 0.9]]


def png(width: int, height: int) -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (width, height), (30, 60, 90)).save(buffer, format="PNG")
    return buffer.getvalue()


def inputs(count: int, size, **config):
    return [PredictionInput(data=dict(config), file=png(*size)) for _ in range(count)]


class IndexLimitedReader:
    """`easyocr.Reader` that overflows a 32-bit index above `ceiling` items.
    The message is reproduced verbatim because it *is* the signal: both the
    retry helper's classifier and this impl's fallback log key on it."""

    def __init__(self, ceiling: int = 10**9, error=None):
        self.ceiling = ceiling
        self.error = error
        self.detected: list[int] = []
        self.single: list[tuple[int, ...]] = []

    def detect(self, images, **params):
        count = int(images.shape[0])
        self.detected.append(count)
        if self.error is not None:
            raise self.error
        if count > self.ceiling:
            raise RuntimeError("integer out of range")
        return [[[10.0, 60.0, 20.0, 70.0]]] * count, [[]] * count

    def recognize(self, image, horizontal_list=None, free_list=None, **params):
        return RESULT

    def readtext(self, image, **params):
        self.single.append(tuple(image.shape))
        return RESULT


def stubbed(reader: IndexLimitedReader, **kwargs) -> EasyOCRModel:
    model = EasyOCRModel(**kwargs)
    model.load = lambda: None  # type: ignore[method-assign]
    model.model = reader
    model._model_loaded = True
    return model


def counters() -> tuple[int, int]:
    """`(index-ceiling events, OOM halvings)`: the two process totals the
    worker diffs across one `predict` call."""
    events = impl_utils.total_index_limit_events()
    return events, impl_utils.total_oom_halvings()


def device(kind: str) -> SimpleNamespace:
    """A stand-in for `torch.device`, so these tests need no torch."""
    return SimpleNamespace(type=kind)


def test_the_formula_reproduces_the_measured_boundary():
    """28 ok / 29 fail, from the kernel's limit rather than the probe — and
    the first pool binds: every later one halves both spatial dimensions while
    only doubling the channels."""
    assert detector_tensor_dims([SCAN[::-1]], DETECTOR_CANVAS_SIZE) == SCAN_TENSOR
    per_item = detector_pool_elements(*SCAN_TENSOR)
    assert per_item == 64 * 1280 * 912 == 74_711_040
    assert MEASURED_CEILING * per_item <= KERNEL_INDEX_ELEMENT_LIMIT
    assert (MEASURED_CEILING + 1) * per_item > KERNEL_INDEX_ELEMENT_LIMIT
    assert max_detector_batch([SCAN[::-1]], DETECTOR_CANVAS_SIZE) == (
        MEASURED_CEILING
    )

    height, width = SCAN_TENSOR
    later = [
        channels * (height // divisor // 2) * (width // divisor // 2)
        for channels, divisor in ((128, 2), (256, 4), (512, 8))
    ] + [3 * height * width]
    for elements in later:
        assert elements < per_item
        assert KERNEL_INDEX_ELEMENT_LIMIT // elements > MEASURED_CEILING


def test_the_ceiling_follows_the_frame_the_batch_pads_to():
    """Asked per batch, from that batch's own padded dimensions, never fixed
    at a constant. The padded frame is the element-wise maximum, so it can be
    taller than any member is tall *and* wider than any is wide; `mag_ratio`
    scales up to — never past — the canvas; and one item over the limit still
    gets 1, saying otherwise being the fallback's job."""
    for size, expected in (
        ((2480, 3508), 28),  # A4 at 300 dpi, the measured group
        ((2560, 2560), 20),  # the square canvas: this impl's worst case
        ((1240, 1754), 61),  # below the canvas, so not resized at all
        ((8000, 6000), 27),  # a big sheet, fitted to 2560x1920
    ):
        assert max_detector_batch([size], DETECTOR_CANVAS_SIZE) == expected, size

    portrait, landscape = (1000, 2560), (2560, 1000)
    assert bounded_dims(portrait[::-1]) == (2560, 1000)
    assert bounded_dims(landscape[::-1]) == (1000, 2560)
    assert detector_tensor_dims([portrait, landscape]) == (2560, 2560)
    assert max_detector_batch([portrait, landscape]) == 20
    assert max_detector_batch([portrait]) == 51

    small = [(640, 480)]
    for ratio, expected in ((1.0, 436), (4.0, 27), (100.0, 27)):
        assert max_detector_batch(small, DETECTOR_CANVAS_SIZE, ratio) == expected
    assert detector_tensor_dims(small, DETECTOR_CANVAS_SIZE, 100.0) == (2560, 1920)
    assert max_detector_batch([(40000, 40000)], 40000) == 1
    assert max_detector_batch([]) is max_detector_batch([None, None]) is None


def test_a_configured_canvas_moves_the_ceiling_with_it():
    """The ceiling is a fact about the tensor, so the model's `canvas_size`
    moves it; a per-request one arrives after the harness has asked and is
    caught by the impl's own cap instead. The hook reads `Image.size`, charges
    an unreadable shape the square canvas, and states nothing when batching is
    off."""
    uncapped = stubbed(IndexLimitedReader(), canvas_size=40000)
    assert uncapped.canvas_pixels == 40000 * 40000
    assert uncapped.max_batch_for([SCAN] * 4) == 15, "at 2496x3520, not 1824x2560"

    model = stubbed(IndexLimitedReader())
    assert model.max_batch_for([SCAN] * 4) == MEASURED_CEILING
    assert model.max_batch_for([SCAN[::-1]] * 4) == MEASURED_CEILING, "transposed"
    for junk in (None, (0, 100), (-1, -1), ("a", "b"), (5,)):
        assert model.max_batch_for([junk]) == 20, junk
    assert model.max_batch_for([SCAN, None]) == 20
    unbatched = stubbed(IndexLimitedReader(), enable_batching=False)
    assert unbatched.max_batch_for([SCAN] * 64) is None


def test_the_ceiling_belongs_to_the_cuda_kernel_only():
    """CPU torch's pooling kernel indexes in `int64_t`, so a CPU-budgeted
    worker has no such ceiling and must not be told it has one:
    `clamped.reason = "index_limit"` is what the ledger reads as permanent.
    Unknown means charged, a missing cap costing a failed batch where a
    needless one costs a smaller one."""
    unloaded = EasyOCRModel()
    assert not hasattr(unloaded, "devices"), "the harness may ask before load"
    mps, cuda = stubbed(IndexLimitedReader()), stubbed(IndexLimitedReader())
    mps.devices, cuda.devices = [device("mps")], [device("cuda")]
    for label, model, expected in (
        ("gpu=False", stubbed(IndexLimitedReader(), gpu=False), None),
        ("resolved mps", mps, None),
        ("resolved cuda", cuda, MEASURED_CEILING),
        ("unloaded gpu", unloaded, MEASURED_CEILING),
    ):
        assert model.max_batch_for([SCAN] * 64) == expected, label


@pytest.fixture
def small_limit(monkeypatch):
    """A 128x96 page costs `64 * 64 * 48 = 196 608` pooling-output elements,
    so a limit of 400 000 puts the ceiling at exactly 2 items."""
    monkeypatch.setattr(eocr, "KERNEL_INDEX_ELEMENT_LIMIT", 400_000)
    return 2


def test_the_impl_caps_its_own_batch_before_the_kernel_ever_sees_it(
    small_limit, caplog
):
    """The batch is chunked at the ceiling and *reported*, which is what puts
    `clamped.reason = "index_limit"` on the measurement. A batch under the
    ceiling is neither capped nor reported, and a CPU-budgeted model does not
    cap at all."""
    caplog.set_level(logging.WARNING, logger="inferio.impl.eocr")
    reader = IndexLimitedReader()
    events, halvings = counters()

    outputs = stubbed(reader).predict(inputs(5, (128, 96)))

    assert len(outputs) == 5
    assert reader.detected == [2, 2, 1], "chunked at the ceiling, in order"
    assert reader.single == [], "and never fell back to per-image processing"
    assert counters() == (events + 1, halvings), "a shape ceiling is not an OOM"
    assert "capping easyOCR's detector batch at 2 of 5" in caplog.text
    assert "128x96" in caplog.text and "196608" in caplog.text

    for label, kwargs, count, expected in (
        ("under the ceiling", {}, 2, [2]),
        ("cpu-budgeted", {"gpu": False}, 5, [5]),
    ):
        reader = IndexLimitedReader()
        before = counters()
        stubbed(reader, **kwargs).predict(inputs(count, (128, 96)))
        assert reader.detected == expected, label
        assert counters() == before, label


def test_an_index_limit_that_is_raised_anyway_halves_and_is_not_an_oom(caplog):
    """The backstop for a ceiling the formula did not predict: it halves,
    being size-dependent, and counts as a shape ceiling, since reporting it as
    memory would deflate a model on an idle GPU. The device gate is on the
    formula only, so a CPU model that meets one still halves."""
    caplog.set_level(logging.WARNING)
    for label, gpu in (("cuda", True), ("cpu", False)):
        caplog.clear()
        reader = IndexLimitedReader(ceiling=3)
        events, halvings = counters()

        outputs = stubbed(reader, gpu=gpu).predict(inputs(6, (128, 96)))

        assert len(outputs) == 6, label
        assert reader.detected == [6, 3, 3], f"{label}: one halving, then fits"
        assert reader.single == [], label
        assert counters() == (events + 1, halvings), label
        halving = [
            record for record in caplog.records
            if "32-bit element index overflowed" in record.getMessage()
        ]
        assert len(halving) == 1, label
        assert halving[0].exc_info is not None, f"{label}: traceback kept"


def test_a_batch_that_cannot_be_halved_falls_back_with_its_traceback(caplog):
    """The per-image fallback, no longer silent: the traceback is logged, the
    batch size and padded dimensions are named, and a classification is claimed
    only when one was made. An index limit at a single input must not become an
    `InferenceOOMError`."""
    caplog.set_level(logging.WARNING, logger="inferio.impl.eocr")
    for label, error, count, is_ceiling in (
        ("index-limit", None, 2, True),
        ("other", ValueError("a processor said no"), 3, False),
    ):
        caplog.clear()
        reader = IndexLimitedReader(ceiling=0, error=error)
        events, halvings = counters()

        outputs = stubbed(reader).predict(inputs(count, (128, 96)))

        assert len(outputs) == count, label
        assert len(reader.single) == count, f"{label}: the fallback ran"
        assert counters()[1] == halvings, f"{label}: never called an OOM"
        fallback = [
            record for record in caplog.records
            if "falling back to per-image processing" in record.getMessage()
        ]
        assert len(fallback) == 1, label
        message = fallback[0].getMessage()
        assert f"{count} inputs" in message and "128x96" in message, label
        assert ("a kernel index ceiling" in message) is is_ceiling, label
        assert fallback[0].exc_info is not None, label
        if not is_ceiling:
            assert "ValueError" in message
            assert counters()[0] == events, "not a shape ceiling"


def test_the_ceiling_never_shrinks_a_batch_the_canvas_already_bounded(small_limit):
    """The two bounds compose the right way round: `fit_to_canvas` first, so
    the ceiling is computed on the array the detector receives."""
    reader = IndexLimitedReader()  # 512x384 submitted, bounded to 128x96
    stubbed(reader, canvas_size=128).predict(inputs(4, (512, 384)))
    assert reader.detected == [2, 2]
