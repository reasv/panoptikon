"""One converted impl, end to end, with the model faked out.

`test_input_error_slots.py` covers the shared seam; this covers what an impl
does *around* it, which is where the off-by-one lives: once undecodable inputs
are excluded, the batch is shorter than `inputs`, so every per-output lookup
has to go through `kept` rather than through the loop counter. Reading
`configs[0]`, `configs[1]`, ... would silently apply a rejected input's
settings to somebody else's output.

The doctr OCR impl is the subject because it is the converted impl with a
real per-output config (`threshold`) and no heavyweight import at module
scope. Torch-free: `load` is replaced, the predictor is a fake, and the OOM
retry is handed an empty exception tuple so it never imports torch.
"""

import io

import pytest
from PIL import Image

from inferio.impl.ocr import DoctrModel
from inferio.impl.utils import (
    ERROR_CLASS_INPUT,
    ERROR_SLOT_KEY,
    run_with_oom_retry,
)


class FakeInput:
    """Duck-typed PredictionInput: impls only touch `.data` / `.file`."""

    def __init__(self, file=None, data=None):
        self.file = file
        self.data = data if data is not None else {}


class FakeWord:
    def __init__(self, value, confidence):
        self.value = value
        self.confidence = confidence


class FakeLine:
    def __init__(self, words):
        self.words = words


class FakeBlock:
    def __init__(self, lines):
        self.lines = lines


class FakePage:
    def __init__(self, words, language="en"):
        self.blocks = [FakeBlock([FakeLine(words)])]
        self.language = {"value": language, "confidence": 0.5}


class FakeResult:
    def __init__(self, pages):
        self.pages = pages


def png_bytes(size=(8, 8)) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", size, (10, 20, 30)).save(buf, format="PNG")
    return buf.getvalue()


@pytest.fixture
def doctr(monkeypatch):
    """A DoctrModel whose predictor is a fake and whose loader is a no-op."""
    model = DoctrModel(detection_model="fake", recognition_model="fake")
    monkeypatch.setattr(DoctrModel, "load", lambda self: None)

    seen_batches = []

    def predictor(images):
        seen_batches.append(len(images))
        # One page per image actually handed to the model, each with a
        # high- and a low-confidence word so `threshold` is observable.
        return FakeResult(
            [
                FakePage([FakeWord("high", 0.9), FakeWord("low", 0.3)])
                for _ in images
            ]
        )

    model.model = predictor

    def torch_free_retry(process_chunk, items, **kwargs):
        kwargs.setdefault("oom_exceptions", ())
        return run_with_oom_retry(process_chunk, items, **kwargs)

    monkeypatch.setattr("inferio.impl.ocr.run_with_oom_retry", torch_free_retry)
    return model, seen_batches


def test_a_bad_input_takes_its_own_slot_and_the_rest_run(doctr):
    model, seen_batches = doctr
    good = png_bytes()
    outputs = model.predict(
        [
            FakeInput(file=good, data={"threshold": None}),
            FakeInput(file=b"garbage", data={"threshold": None}),
            FakeInput(file=good, data={"threshold": None}),
        ]
    )

    assert seen_batches == [2], "the undecodable input never reached the model"
    assert len(outputs) == 3, "one output per input, in input order"
    assert isinstance(outputs[0], dict) and "transcription" in outputs[0]
    assert isinstance(outputs[2], dict) and "transcription" in outputs[2]
    assert set(outputs[1]) == {ERROR_SLOT_KEY}
    assert outputs[1][ERROR_SLOT_KEY]["class"] == ERROR_CLASS_INPUT


def test_each_output_uses_its_own_inputs_config(doctr):
    """The kept-index test: input 2's output must be built with `configs[2]`.

    Input 1 is the rejected one and carries a threshold that would erase both
    words, so an impl that walked configs in survivor order (`configs[0]`,
    `configs[1]`) would produce an empty transcription for input 2.
    """
    model, _ = doctr
    good = png_bytes()
    outputs = model.predict(
        [
            FakeInput(file=good, data={"threshold": 0.5}),
            FakeInput(file=b"garbage", data={"threshold": 0.95}),
            FakeInput(file=good, data={"threshold": None}),
        ]
    )

    assert outputs[0]["transcription"] == "high", "configs[0]: 0.5 drops `low`"
    assert (
        outputs[2]["transcription"] == "high low"
    ), "configs[2]: no threshold keeps both words"


def test_every_input_rejected_produces_only_slots(doctr):
    """Nothing reaches the model, and the impl must not fail on the empty
    batch — the orchestrator reads the all-slot response as a verdict."""
    model, seen_batches = doctr
    outputs = model.predict(
        [FakeInput(file=b"garbage"), FakeInput(file=b"also garbage")]
    )

    assert seen_batches == [], "an empty batch is never sent to the model"
    assert [set(output) for output in outputs] == [
        {ERROR_SLOT_KEY},
        {ERROR_SLOT_KEY},
    ]
