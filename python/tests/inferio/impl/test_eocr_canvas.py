"""The easyOCR impl bounds the tensor it batches, and only that.

Run2 defect D1-b. The registry declares
`metadata.cost.canvas_pixels = 6 553 600` for the three `doctr/easyocr_*`
ids — the CRAFT detector's 2560px canvas — and both the orchestrator and the
worker price every input at `min(raw_pixels, 6 553 600)` because of it. That
price is only true if the batch tensor the impl builds never exceeds that area
per item, and before the fix it did: `pad_images_to_same_size` built the batch
at the largest member's **raw** dimensions, so a 2480x3508 scan and an
8000x6000 sheet — which price identically under the cap, and can therefore
share a batch — produced a tensor 5.5x the area the batch was charged for, and
the scan reached the detector at a fifth of its own resolution.

The bound stops at the tensor. easyOCR's recogniser resizes every crop to a
fixed `imgH x imgW` before it becomes a tensor
(`easyocr/utils.py:566-577`, `easyocr/recognition.py:70-97`), so its device
memory does not depend on the page's resolution at all — which is why the
crops are taken from the **raw** image, as they were before run2, and why
`min_size` still means raw pixels.

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
    DEFAULT_MIN_SIZE,
    DETECT_PARAMS,
    DETECTOR_CANVAS_SIZE,
    RECOGNIZE_PARAMS,
    EasyOCRModel,
    filter_small_detections,
    fit_to_canvas,
    pad_images_to_same_size,
    scale_detections_to_original,
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
    """Stand-in for `easyocr.Reader`.

    `detect` records the stacked array it was handed and returns, for every
    member, the same fixed box **in that array's own coordinate space** —
    which is what the real detector returns
    (`craft_utils.adjustResultCoordinates` has already undone its internal
    ratio). So whatever reaches `recognize` is what the impl's own mapping
    did to it.

    `recognize` records the array it was given and takes the crop
    `utils.get_image_list` would take from it (`utils.py:601-605`), so a test
    can assert what resolution the recogniser actually saw.
    """

    BOX = [10.0, 60.0, 20.0, 70.0]  # x_min, x_max, y_min, y_max

    def __init__(self):
        self.batched: list[tuple[int, ...]] = []
        self.single: list[tuple[int, ...]] = []
        self.detected: list[tuple[int, ...]] = []
        self.recognized: list[tuple[int, ...]] = []
        self.crops: list[tuple[int, int]] = []
        self.detect_params: list[dict] = []
        self.recognize_params: list[dict] = []
        self.boxes: list[list] = []

    @staticmethod
    def _result():
        box = [[10.0, 20.0], [30.0, 20.0], [30.0, 40.0], [10.0, 40.0]]
        return [[box, "hello", 0.9]]

    def detect(self, images, **params):
        self.detected.append(tuple(images.shape))
        self.detect_params.append(params)
        horizontal_agg = [[list(self.BOX)] for _ in images]
        free_agg = [[] for _ in images]
        return horizontal_agg, free_agg

    def recognize(self, image, horizontal_list=None, free_list=None, **params):
        self.recognized.append(tuple(image.shape))
        self.recognize_params.append(params)
        self.boxes.append(list(horizontal_list or []))
        maximum_y, maximum_x = image.shape[0], image.shape[1]
        for box in horizontal_list or []:
            x_min = max(0, int(box[0]))
            x_max = min(int(box[1]), maximum_x)
            y_min = max(0, int(box[2]))
            y_max = min(int(box[3]), maximum_y)
            self.crops.append((x_max - x_min, y_max - y_min))
        return self._result()

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


def test_scale_detections_inverts_the_resize():
    """A box the detector found at canvas scale names the same region of the
    image the caller submitted — in both of `group_text_box`'s shapes."""
    _, scale = fit_to_canvas(array(*SHEET), DETECTOR_CANVAS_SIZE)
    horizontal = [[100.0, 300.0, 200.0, 400.0]]
    free = [[[100.0, 200.0], [300.0, 200.0], [300.0, 400.0], [100.0, 400.0]]]
    moved_h, moved_f = scale_detections_to_original(horizontal, free, scale)
    # x=100 at 2560 wide is x=312.5 at 8000 wide.
    assert moved_h[0] == pytest.approx([312.5, 937.5, 625.0, 1250.0])
    assert moved_f[0][0] == pytest.approx([312.5, 625.0])


def test_scale_detections_is_a_no_op_inside_the_canvas():
    horizontal, free = [[1.0, 2.0, 3.0, 4.0]], [[[1.0, 2.0]]]
    assert scale_detections_to_original(horizontal, free, 1.0) == (
        horizontal,
        free,
    )


def test_min_size_is_applied_in_the_submitted_images_pixels():
    """easyOCR drops a box whose longer side is not greater than `min_size`
    (`easyocr.py:343-347`). Applied after the boxes are mapped back, so 20
    still means 20 raw pixels on a page the detector saw at 0.32x."""
    small = [[10.0, 25.0, 10.0, 20.0]]  # 15 x 10
    big = [[10.0, 100.0, 10.0, 30.0]]  # 90 x 20
    kept, _ = filter_small_detections(small + big, [], DEFAULT_MIN_SIZE, (500, 500))
    assert kept == big
    # `min_size = 0` is easyOCR's "no filter", and is honoured as such.
    kept, _ = filter_small_detections(small + big, [], 0, (500, 500))
    assert kept == small + big


def test_a_box_found_in_the_padding_is_dropped():
    """Detection runs on a frame padded to the batch's largest member, so a
    box can land outside a smaller image entirely; there is nothing to crop."""
    outside = [[600.0, 700.0, 10.0, 40.0]]
    inside = [[10.0, 100.0, 10.0, 40.0]]
    kept, _ = filter_small_detections(outside + inside, [], 0, (500, 500))
    assert kept == inside


# ---------------------------------------------------------------------------
# The batch tensor
# ---------------------------------------------------------------------------


def test_padding_a_fitted_batch_stays_inside_the_canvas():
    """The two sizes D1-b measured, in one batch, the way the batched path
    builds it: fit first, pad second. 2560x2560 is the worst case the canvas
    allows, and it is 6 553 600 pixels — exactly what the batch was priced at
    per item."""
    fitted = [fit_to_canvas(array(*size), DETECTOR_CANVAS_SIZE)[0]
              for size in (SHEET, SCAN)]
    padded = pad_images_to_same_size(fitted)
    assert len({image.shape for image in padded}) == 1, "one common size"
    for image in padded:
        assert image.shape[0] * image.shape[1] <= CANVAS_PIXELS
    # What it used to be: the scan padded to the sheet's raw 8000x6000, 5.5x
    # its own area and 7.3x what the cap charged for it.
    assert padded[0].shape[0] * padded[0].shape[1] < SHEET[0] * SHEET[1] / 7


def test_the_detector_tensor_is_canvas_bounded():
    """End to end through `predict`, at a small canvas so the fixtures are
    cheap: the array `detect` is handed is one stacked batch, padded to a
    single size, and no member exceeds the canvas."""
    model = stubbed(canvas_size=256)
    model.predict(inputs([(800, 600), (248, 350)]))
    (shape,) = model.model.detected
    assert shape[0] == 2, "one stacked 4-D batch"
    assert shape[1] * shape[2] <= model.canvas_pixels
    assert max(shape[1], shape[2]) <= model.canvas_size
    assert model.model.batched == [], "readtext_batched is no longer used"


def test_recognition_crops_come_from_the_raw_image():
    """The half the canvas does not bound. The recogniser is handed the array
    the caller submitted, and takes its crop at that resolution — here 3.1x
    the width the canvas-bounded array could have given it, which is the
    transcription fidelity this split preserves."""
    model = stubbed(canvas_size=256)
    model.predict(inputs([(800, 600), (248, 350)]))
    assert model.model.recognized == [(600, 800, 3), (350, 248, 3)]
    x_min, x_max, y_min, y_max = FakeReader.BOX
    in_canvas = (int(x_max) - int(x_min), int(y_max) - int(y_min))
    for crop, scale in zip(model.model.crops, (256 / 800, 256 / 350)):
        assert crop == (
            int(x_max / scale) - int(x_min / scale),
            int(y_max / scale) - int(y_min / scale),
        )
        assert crop[0] > in_canvas[0]
    assert model.model.crops[0][0] == pytest.approx(in_canvas[0] / (256 / 800), abs=1)


def test_boxes_reach_the_recogniser_in_the_raw_images_coordinates():
    """Which is what makes the crop above the right *region* and not merely a
    big one: each image is mapped back by its own scale, exactly once."""
    model = stubbed(canvas_size=256)
    model.predict(inputs([(800, 600), (128, 64)]))
    box = FakeReader.BOX
    # 800x600 was fitted (scale 256/800) and mapped back; 128x64 never was, so
    # its box comes through untouched.
    assert model.model.boxes[0][0] == pytest.approx(
        [value / (256 / 800) for value in box]
    )
    assert model.model.boxes[1][0] == box


def test_the_unbatched_path_submits_the_raw_image():
    """`enable_batching = false` — the shipped registry's setting — takes the
    per-image path. There is no batch tensor to bound there: easyOCR's own
    `resize_aspect_ratio` bounds the detector, so resizing first would only
    cost transcription quality."""
    model = stubbed(canvas_size=256, enable_batching=False)
    model.predict(inputs([(800, 600)]))
    assert model.model.detected == []
    assert model.model.single[0][:2] == (600, 800)


def test_a_per_request_canvas_size_is_what_bounds_the_batch():
    """`canvas_size` is one of the parameters a caller may set per request,
    and easyOCR's detector would honour it — so the bound has to be the
    effective one, not the constructor's."""
    model = stubbed(canvas_size=1024)
    model.predict(inputs([(800, 600), (248, 350)], canvas_size=128))
    assert max(model.model.detected[0][1:3]) <= 128
    # A nonsensical override falls back to the model's own canvas rather than
    # producing a 1-pixel batch.
    model = stubbed(canvas_size=256)
    model.predict(inputs([(800, 600), (248, 350)], canvas_size=0))
    assert max(model.model.detected[0][1:3]) == 256


def test_each_easyocr_parameter_goes_to_exactly_one_call():
    """The batched path routes what `readtext_batched` used to route for it,
    so a parameter must belong to exactly one of the two calls — and the
    detector must not be left to filter by a raw-pixel `min_size`."""
    assert not (DETECT_PARAMS & RECOGNIZE_PARAMS)
    model = stubbed(canvas_size=256)
    model.predict(
        inputs([(800, 600), (248, 350)], text_threshold=0.5, paragraph=True)
    )
    (detect_params,) = model.model.detect_params
    assert detect_params["text_threshold"] == 0.5
    assert detect_params["min_size"] == 0, "filtered by this impl instead"
    assert "paragraph" not in detect_params
    assert model.model.recognize_params[0] == {"paragraph": True}


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


def test_the_routed_parameters_are_the_ones_easyocr_accepts():
    """The batched path routes parameters to `Reader.detect` and
    `Reader.recognize` itself, so a renamed or dropped parameter in a new
    easyOCR release becomes a `TypeError` at predict time rather than a test
    failure. Read straight out of the installed package's source — parsed,
    not imported, so this stays torch-free."""
    import ast
    import importlib.util

    spec = importlib.util.find_spec("easyocr.easyocr")
    if spec is None or not spec.origin:  # pragma: no cover - easyocr absent
        pytest.skip("easyocr is not installed")
    tree = ast.parse(open(spec.origin, encoding="utf-8").read())
    reader = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "Reader"
    )
    accepted = {}
    for node in reader.body:
        if isinstance(node, ast.FunctionDef) and node.name in {
            "detect",
            "recognize",
        }:
            args = node.args
            accepted[node.name] = {
                arg.arg for arg in args.args + args.kwonlyargs
            }
    assert DETECT_PARAMS <= accepted["detect"]
    assert RECOGNIZE_PARAMS <= accepted["recognize"]
    # The two the impl passes by hand as well.
    assert {"reformat", "min_size"} <= accepted["detect"]
    assert {"horizontal_list", "free_list"} <= accepted["recognize"]
