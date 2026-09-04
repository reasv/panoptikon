"""The easyOCR impl bounds every input by its canvas before it batches.

Run2 defect D1-b. The registry declares
`metadata.cost.canvas_pixels = 6 553 600` for the three `doctr/easyocr_*`
ids — the CRAFT detector's 2560px canvas — and both the orchestrator and the
worker price every input at `min(raw_pixels, 6 553 600)` because of it. That
price is only true if the impl never processes more than that area per item,
and before this fix it did: `pad_images_to_same_size` built the batch tensor
at the largest member's **raw** dimensions, so a 2480x3508 scan and an
8000x6000 sheet — which price identically under the cap, and can therefore
share a batch — produced a tensor 5.5x the area the batch was charged for.

Torch-free and easyOCR-free: `load` is stubbed out and the reader is a fake,
so these describe the impl's own geometry, which is the part the price
depends on. NumPy, PIL and OpenCV are all real (inferio dependencies).
"""

from __future__ import annotations

import io

import numpy as np
import pytest
from PIL import Image

from inferio.impl.eocr import (
    DETECTOR_CANVAS_SIZE,
    EasyOCRModel,
    fit_to_canvas,
    pad_images_to_same_size,
    scale_boxes_to_original,
)
from inferio.inferio_types import PredictionInput

# The two shapes run2 D1-b measured sharing a batch, and easyOCR's own canvas.
SHEET = (8000, 6000)  # 48 000 000 raw pixels
SCAN = (2480, 3508)  # 8 699 840 raw pixels
CANVAS_PIXELS = DETECTOR_CANVAS_SIZE * DETECTOR_CANVAS_SIZE  # 6 553 600


def array(width: int, height: int) -> np.ndarray:
    return np.zeros((height, width, 3), dtype=np.uint8)


def png(width: int, height: int) -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (width, height), (30, 60, 90)).save(buffer, format="PNG")
    return buffer.getvalue()


class FakeReader:
    """Stand-in for `easyocr.Reader`: records the arrays it was handed and
    returns one detection per image, in the coordinate space of the array it
    was given (which is what the real detector does)."""

    def __init__(self):
        self.batched: list[tuple[int, ...]] = []
        self.single: list[tuple[int, ...]] = []

    @staticmethod
    def _result():
        box = [[10.0, 20.0], [30.0, 20.0], [30.0, 40.0], [10.0, 40.0]]
        return [[box, "hello", 0.9]]

    def readtext_batched(self, images, **params):
        self.batched.extend(image.shape for image in images)
        return [self._result() for _ in images]

    def readtext(self, image, **params):
        self.single.append(image.shape)
        return self._result()


def stubbed(**kwargs) -> EasyOCRModel:
    """An `EasyOCRModel` whose weights are a fake: `load` is neutered so
    neither torch nor easyOCR is imported."""
    model = EasyOCRModel(**kwargs)
    model.load = lambda: None  # type: ignore[method-assign]
    model.model = FakeReader()
    model._model_loaded = True
    return model


def inputs(sizes, **config):
    return [PredictionInput(data=dict(config), file=png(*size)) for size in sizes]


# ---------------------------------------------------------------------------
# The resize itself
# ---------------------------------------------------------------------------


def test_fit_to_canvas_matches_the_detectors_own_arithmetic():
    """`imgproc.resize_aspect_ratio` at `mag_ratio = 1`: the ratio is
    `canvas / max(h, w)` and each side is that ratio times the side, truncated.
    An 8000x6000 sheet becomes 2560x1920 — 4.9 MP, inside the 6.55 MP canvas
    the registry declares and the ledger prices."""
    resized, scale = fit_to_canvas(array(*SHEET), DETECTOR_CANVAS_SIZE)
    assert resized.shape == (1920, 2560, 3)
    assert scale == pytest.approx(2560 / 8000)
    assert resized.shape[0] * resized.shape[1] <= CANVAS_PIXELS


def test_fit_to_canvas_never_upscales():
    """Only a ceiling. A small image keeps every pixel it was submitted with,
    and its scale is exactly 1.0 so nothing is mapped back afterwards."""
    for size in ((640, 480), (DETECTOR_CANVAS_SIZE, 100)):
        original = array(*size)
        resized, scale = fit_to_canvas(original, DETECTOR_CANVAS_SIZE)
        assert resized is original
        assert scale == 1.0


def test_scale_boxes_to_original_inverts_the_resize():
    """A box the detector found at canvas scale names the same region of the
    image the caller submitted."""
    _, scale = fit_to_canvas(array(*SHEET), DETECTOR_CANVAS_SIZE)
    box = [[100.0, 200.0], [300.0, 200.0], [300.0, 400.0], [100.0, 400.0]]
    (mapped,) = scale_boxes_to_original([[box, "text", 0.5]], scale)
    assert mapped[1:] == ["text", 0.5]
    assert mapped[0] == [
        [point[0] / scale, point[1] / scale] for point in box
    ]
    # Which is the original image's own coordinates: x=100 at 2560 wide is
    # x=312.5 at 8000 wide.
    assert mapped[0][0] == pytest.approx([312.5, 625.0])


def test_scale_boxes_to_original_passes_through_what_it_cannot_read():
    """`detail`, `paragraph` and `output_format` all change easyOCR's return
    shape, and a caller sets them per request, so anything that is not a
    4-point box is left exactly as it came back."""
    assert scale_boxes_to_original(["just text", "more"], 0.5) == [
        "just text",
        "more",
    ]
    assert scale_boxes_to_original([], 0.5) == []
    boxes = [[[[1.0, 2.0]], "t", 0.1]]
    assert scale_boxes_to_original(boxes, 1.0) is boxes, "a no-op at scale 1"


# ---------------------------------------------------------------------------
# The batch tensor
# ---------------------------------------------------------------------------


def test_padding_a_fitted_batch_stays_inside_the_canvas():
    """The two sizes D1-b measured, in one batch, the way `predict` builds it:
    fit first, pad second. 2560x2560 is the worst case the canvas allows, and
    it is 6 553 600 pixels — exactly what the batch was priced at per item."""
    fitted = [fit_to_canvas(array(*size), DETECTOR_CANVAS_SIZE)[0]
              for size in (SHEET, SCAN)]
    padded = pad_images_to_same_size(fitted)
    assert len({image.shape for image in padded}) == 1, "one common size"
    for image in padded:
        assert image.shape[0] * image.shape[1] <= CANVAS_PIXELS
    # What it used to be: the scan padded to the sheet's raw 8000x6000, 5.5x
    # its own area and 7.3x what the cap charged for it.
    assert padded[0].shape[0] * padded[0].shape[1] < SHEET[0] * SHEET[1] / 7


def test_predict_hands_the_reader_canvas_bounded_arrays():
    """End to end through `predict`, at a small canvas so the fixtures are
    cheap: a mixed batch reaches `readtext_batched` bounded by the canvas,
    padded to one size, and nothing is at its raw dimensions."""
    model = stubbed(canvas_size=256)
    model.predict(inputs([(800, 600), (248, 350)]))
    assert len(model.model.batched) == 2
    assert len(set(model.model.batched)) == 1, "padded to one common size"
    for shape in model.model.batched:
        assert shape[0] * shape[1] <= model.canvas_pixels
        assert max(shape[0], shape[1]) <= model.canvas_size


def test_the_unbatched_path_is_bounded_too():
    """`enable_batching = false` — the shipped registry's setting — takes the
    per-image path, where nothing is padded but the host still prices every
    input at `min(raw, canvas)`. The bound is a statement about the model, not
    about a code path."""
    model = stubbed(canvas_size=256, enable_batching=False)
    model.predict(inputs([(800, 600)]))
    assert model.model.batched == []
    assert model.model.single[0][:2] == (192, 256)


def test_a_per_request_canvas_size_is_what_bounds_the_batch():
    """`canvas_size` is one of the parameters a caller may set per request,
    and easyOCR's detector would honour it — so the bound has to be the
    effective one, not the constructor's."""
    model = stubbed(canvas_size=1024)
    model.predict(inputs([(800, 600), (248, 350)], canvas_size=128))
    for shape in model.model.batched:
        assert max(shape[0], shape[1]) <= 128
    # A nonsensical override falls back to the model's own canvas rather than
    # producing a 1-pixel batch.
    model = stubbed(canvas_size=256)
    model.predict(inputs([(800, 600), (248, 350)], canvas_size=0))
    assert max(model.model.batched[0][:2]) == 256


def test_predict_maps_every_boxs_coordinates_back(monkeypatch):
    """Each image is mapped back by *its own* scale, once."""
    import inferio.impl.eocr as eocr

    calls: list[float] = []
    original = eocr.scale_boxes_to_original

    def spy(results, scale):
        calls.append(scale)
        return original(results, scale)

    monkeypatch.setattr(eocr, "scale_boxes_to_original", spy)
    model = stubbed(canvas_size=256)
    model.predict(inputs([(800, 600), (128, 64)]))
    assert calls == [pytest.approx(256 / 800), 1.0]


# ---------------------------------------------------------------------------
# What the worker reads off the loaded impl
# ---------------------------------------------------------------------------


def test_the_impl_states_its_canvas_and_that_it_pads():
    """The two attributes the packing harness reads (protocol doc, "Memory
    grants"): tier 2 of the canvas resolution order, and the flag that pairs
    with it. Stating the canvas is the promise this module's `fit_to_canvas`
    keeps, and it is what exempts easyOCR from the harness's mixed-batch
    warning."""
    from inferio_worker import packing

    model = EasyOCRModel()
    assert model.canvas_pixels == CANVAS_PIXELS
    assert model.pads_to_common_size is True
    assert packing.impl_canvas_pixels(model) == CANVAS_PIXELS
    assert packing._pads_without_a_canvas(model) is False
    assert packing.resolve_canvas_pixels({}, model, "pixel") == CANVAS_PIXELS
