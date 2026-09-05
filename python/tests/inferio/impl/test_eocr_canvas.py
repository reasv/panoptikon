"""The easyOCR impl bounds the tensor it batches, and only that.

The registry prices every input at `min(raw_pixels, 6 553 600)`, which is only
true if the batch tensor never exceeds that area per item. The bound stops at
the tensor: the recogniser resizes every crop to a fixed `imgH x imgW`, so its
crops come from the raw image and `min_size` still means raw pixels
(docs/inferio-worker-protocol.md, "Memory grants"). Torch-free and
easyOCR-free: `load` is stubbed and the reader is a fake.
"""

from __future__ import annotations

import io

import numpy as np
import pytest
from PIL import Image

from inferio.impl.eocr import (
    DEFAULT_MIN_SIZE, DETECT_PARAMS, DETECTOR_CANVAS_SIZE, RECOGNIZE_PARAMS,
    EasyOCRModel, filter_small_detections, fit_to_canvas,
    pad_images_to_same_size, scale_detections_to_original,
)
from inferio.inferio_types import PredictionInput

# The two shapes run2 measured sharing a batch, and easyOCR's own canvas.
SHEET = (8000, 6000)  # 48 000 000 raw pixels
SCAN = (2480, 3508)  # 8 699 840 raw pixels
CANVAS_PIXELS = DETECTOR_CANVAS_SIZE * DETECTOR_CANVAS_SIZE  # 6 553 600
DETECT_RECOGNIZE = {"detect", "recognize"}


def array(width: int, height: int) -> np.ndarray:
    return np.zeros((height, width, 3), dtype=np.uint8)


def png(width: int, height: int) -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (width, height), (30, 60, 90)).save(buffer, format="PNG")
    return buffer.getvalue()


class FakeReader:
    """Stand-in for `easyocr.Reader`: `detect` returns one fixed box per
    member in the handed array's own coordinate space, as the real one does,
    and `recognize` takes the crop `utils.get_image_list` would take."""

    BOX = [10.0, 60.0, 20.0, 70.0]  # x_min, x_max, y_min, y_max
    RESULT = [[[[10.0, 20.0], [30.0, 20.0], [30.0, 40.0], [10.0, 40.0]],
               "hi", 0.9]]

    def __init__(self):
        self.single: list[tuple[int, ...]] = []
        self.detected: list[tuple[int, ...]] = []
        self.recognized: list[tuple[int, ...]] = []
        self.crops: list[tuple[int, int]] = []
        self.detect_params: list[dict] = []
        self.recognize_params: list[dict] = []
        self.boxes: list[list] = []

    def detect(self, images, **params):
        self.detected.append(tuple(images.shape))
        self.detect_params.append(params)
        return [[list(self.BOX)] for _ in images], [[] for _ in images]

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
        return self.RESULT

    def readtext(self, image, **params):
        self.single.append(image.shape)
        return self.RESULT


def stubbed(**kwargs) -> EasyOCRModel:
    """An `EasyOCRModel` whose `load` is neutered: no torch, no easyOCR."""
    model = EasyOCRModel(**kwargs)
    model.load = lambda: None  # type: ignore[method-assign]
    model.model = FakeReader()
    model._model_loaded = True
    return model


def inputs(sizes, **config):
    return [PredictionInput(data=dict(config), file=png(*size)) for size in sizes]


def test_fit_to_canvas_matches_the_detectors_own_arithmetic():
    """`imgproc.resize_aspect_ratio` at `mag_ratio = 1`: ratio is
    `canvas / max(h, w)`, each side truncated, and only ever a ceiling. Fitting
    first and padding second keeps the batch inside its priced area."""
    resized, scale = fit_to_canvas(array(*SHEET), DETECTOR_CANVAS_SIZE)
    assert resized.shape == (1920, 2560, 3)
    assert scale == pytest.approx(2560 / 8000)
    assert resized.shape[0] * resized.shape[1] <= CANVAS_PIXELS
    for size in ((640, 480), (DETECTOR_CANVAS_SIZE, 100)):
        original = array(*size)
        resized, unchanged = fit_to_canvas(original, DETECTOR_CANVAS_SIZE)
        assert resized is original and unchanged == 1.0, size

    fitted = [fit_to_canvas(array(*size), DETECTOR_CANVAS_SIZE)[0]
              for size in (SHEET, SCAN)]
    padded = pad_images_to_same_size(fitted)
    assert len({image.shape for image in padded}) == 1, "one common size"
    for image in padded:
        assert image.shape[0] * image.shape[1] <= CANVAS_PIXELS
    # What it used to be: the scan padded to the sheet's raw 8000x6000.
    assert padded[0].shape[0] * padded[0].shape[1] < SHEET[0] * SHEET[1] / 7


def test_detections_are_mapped_back_into_the_submitted_images_pixels():
    """A box found at canvas scale names the same region of the submitted
    image, in both of `group_text_box`'s shapes, and is a no-op at 1.0 — which
    keeps `min_size` meaning raw pixels. A box in the batch's padding is
    dropped: there is nothing to crop."""
    _, scale = fit_to_canvas(array(*SHEET), DETECTOR_CANVAS_SIZE)
    horizontal = [[100.0, 300.0, 200.0, 400.0]]
    free = [[[100.0, 200.0], [300.0, 200.0], [300.0, 400.0], [100.0, 400.0]]]
    moved_h, moved_f = scale_detections_to_original(horizontal, free, scale)
    # x=100 at 2560 wide is x=312.5 at 8000 wide.
    assert moved_h[0] == pytest.approx([312.5, 937.5, 625.0, 1250.0])
    assert moved_f[0][0] == pytest.approx([312.5, 625.0])
    assert scale_detections_to_original(horizontal, free, 1.0) == (
        horizontal,
        free,
    )

    small = [[10.0, 25.0, 10.0, 20.0]]  # 15 x 10
    big = [[10.0, 100.0, 10.0, 30.0]]  # 90 x 20
    outside = [[600.0, 700.0, 10.0, 40.0]]
    for label, boxes, min_size, expected in (
        ("filtered", small + big, DEFAULT_MIN_SIZE, big),
        ("no filter", small + big, 0, small + big),
        ("in the padding", outside + big, 0, big),
    ):
        kept, _ = filter_small_detections(boxes, [], min_size, (500, 500))
        assert kept == expected, label


def test_the_detector_tensor_is_canvas_bounded():
    """End to end through `predict`, at a small canvas so the fixtures are
    cheap: one stacked batch, padded to a single size, no member over the
    canvas. A per-request `canvas_size` is what bounds it when given, and
    `enable_batching = false` takes the per-image path, where easyOCR bounds
    the detector itself."""
    model = stubbed(canvas_size=256)
    model.predict(inputs([(800, 600), (248, 350)]))
    (shape,) = model.model.detected
    assert shape[0] == 2, "one stacked 4-D batch"
    assert shape[1] * shape[2] <= model.canvas_pixels
    assert max(shape[1], shape[2]) <= model.canvas_size

    model = stubbed(canvas_size=1024)
    model.predict(inputs([(800, 600), (248, 350)], canvas_size=128))
    assert max(model.model.detected[0][1:3]) <= 128
    # A nonsensical override falls back to the model's own canvas.
    model = stubbed(canvas_size=256)
    model.predict(inputs([(800, 600), (248, 350)], canvas_size=0))
    assert max(model.model.detected[0][1:3]) == 256

    model = stubbed(canvas_size=256, enable_batching=False)
    model.predict(inputs([(800, 600)]))
    assert model.model.detected == []
    assert model.model.single[0][:2] == (600, 800)


def test_recognition_crops_come_from_the_raw_image():
    """The half the canvas does not bound: the recogniser gets the submitted
    array and crops at that resolution — 3.1x the width a canvas-bounded array
    could have given it — and each image's boxes are mapped back by its own
    scale, exactly once, so it is the right *region* too."""
    model = stubbed(canvas_size=256)
    model.predict(inputs([(800, 600), (248, 350)]))
    assert model.model.recognized == [(600, 800, 3), (350, 248, 3)]
    x_min, x_max, y_min, y_max = FakeReader.BOX
    in_canvas = (int(x_max) - int(x_min), int(y_max) - int(y_min))
    for crop, scale in zip(model.model.crops, (256 / 800, 256 / 350)):
        assert crop == (int(x_max / scale) - int(x_min / scale),
                        int(y_max / scale) - int(y_min / scale))
        assert crop[0] > in_canvas[0]
    assert model.model.crops[0][0] == pytest.approx(in_canvas[0] / (256 / 800), abs=1)

    model = stubbed(canvas_size=256)
    model.predict(inputs([(800, 600), (128, 64)]))
    # 800x600 was fitted (scale 256/800) and mapped back; 128x64 never was.
    assert model.model.boxes[0][0] == pytest.approx(
        [value / (256 / 800) for value in FakeReader.BOX]
    )
    assert model.model.boxes[1][0] == FakeReader.BOX


def test_each_easyocr_parameter_goes_to_exactly_one_call():
    """The batched path routes what `readtext_batched` used to, so a parameter
    belongs to exactly one call — and the detector must not be left to filter
    by a raw-pixel `min_size`."""
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


def test_the_impl_states_its_canvas_and_that_it_pads():
    """The two attributes the packing harness reads: stating the canvas is
    what exempts easyOCR from its mixed-batch warning."""
    from inferio_worker import packing

    model = EasyOCRModel()
    assert model.canvas_pixels == CANVAS_PIXELS
    assert model.pads_to_common_size is True
    assert packing.impl_canvas_pixels(model) == CANVAS_PIXELS
    assert packing._pads_without_a_canvas(model) is False
    assert packing.resolve_canvas_pixels({}, model, "pixel") == CANVAS_PIXELS


def test_the_routed_parameters_are_the_ones_easyocr_accepts():
    """A renamed parameter in a new easyOCR release would become a `TypeError`
    at predict time. Parsed out of the installed package, not imported."""
    import ast
    import importlib.util

    spec = importlib.util.find_spec("easyocr.easyocr")
    if spec is None or not spec.origin:  # pragma: no cover - easyocr absent
        pytest.skip("easyocr is not installed")
    tree = ast.parse(open(spec.origin, encoding="utf-8").read())
    reader = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "Reader"
    )
    accepted = {
        node.name: {a.arg for a in node.args.args + node.args.kwonlyargs}
        for node in reader.body
        if isinstance(node, ast.FunctionDef) and node.name in DETECT_RECOGNIZE
    }
    assert DETECT_PARAMS <= accepted["detect"]
    assert RECOGNIZE_PARAMS <= accepted["recognize"]
    # The two the impl passes by hand as well.
    assert {"reformat", "min_size"} <= accepted["detect"]
    assert {"horizontal_list", "free_list"} <= accepted["recognize"]
