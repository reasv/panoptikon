"""Unit tests for the per-item error-slot seam in inferio.impl.utils.

Torch-free: only PIL and numpy, which `inferio.impl.utils` already imports.
The seam's whole job is to decide, per input, whether *this payload* is
undecodable — so the tests are about which failures become slots and which
ones still take the batch down with them
(docs/inferio-worker-protocol.md, docs/failed-media-retry-design.md).
"""

import io
from pathlib import Path

import pytest
from PIL import Image

from inferio.impl.utils import (
    ERROR_CLASS_INPUT,
    ERROR_SLOT_KEY,
    ImageDecodeError,
    assemble_slots,
    decode_image_inputs,
    input_error_slot,
    load_image_from_buffer,
    load_image_or_slot,
)


class FakeInput:
    """Duck-typed PredictionInput: impls only touch `.data` / `.file`."""

    def __init__(self, file=None, data=None):
        self.file = file
        self.data = data


def png_bytes(size=(8, 8)) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", size, (10, 20, 30)).save(buf, format="PNG")
    return buf.getvalue()


def photo_jpeg_bytes() -> bytes:
    """A JPEG with enough entropy that half of it is still decodable scan
    data (a tiny solid image is nearly all header, so truncating it destroys
    the header instead of the pixels)."""
    size = 256
    pixels = bytes(
        (x * 7 + y * 13) % 256 for y in range(size) for x in range(size * 3)
    )
    image = Image.frombytes("RGB", (size, size), pixels)
    buf = io.BytesIO()
    image.save(buf, format="JPEG", quality=95)
    return buf.getvalue()


def test_a_healthy_payload_decodes_without_a_slot():
    image, slot = load_image_or_slot(png_bytes())
    assert slot is None
    assert image is not None and image.size == (8, 8)


def test_an_undecodable_payload_becomes_an_input_slot():
    image, slot = load_image_or_slot(b"this is definitely not an image")
    assert image is None
    assert set(slot) == {ERROR_SLOT_KEY}
    body = slot[ERROR_SLOT_KEY]
    assert body["class"] == ERROR_CLASS_INPUT
    assert "Unreadable image" in body["message"]
    assert set(body) == {"class", "message"}


def test_truncation_pil_tolerates_is_not_an_error():
    """LOAD_TRUNCATED_IMAGES semantics are the parity contract: an image the
    worker can still decode must never be reported as bad input, however
    damaged the file is."""
    truncated = photo_jpeg_bytes()
    truncated = truncated[: len(truncated) // 2]
    image, slot = load_image_or_slot(truncated)
    assert slot is None, "PIL tolerates this file, so the pipeline must too"
    assert image is not None


def test_only_a_decode_failure_becomes_a_slot(monkeypatch):
    """Anything that is not a decode failure of these bytes must still fail
    the whole batch: an OOM or a broken environment is never the item's
    fault, and a slot would have it recorded as bad media forever."""
    assert issubclass(ImageDecodeError, ValueError)
    with pytest.raises(ImageDecodeError):
        load_image_from_buffer(b"not an image")

    def exploding(_buf, **_kwargs):
        raise RuntimeError("CUDA out of memory")

    monkeypatch.setattr(
        "inferio.impl.utils.load_image_from_buffer", exploding
    )
    with pytest.raises(RuntimeError, match="CUDA out of memory"):
        load_image_or_slot(b"anything")


def test_a_machine_limit_never_becomes_an_input_slot(monkeypatch):
    """The decode fallback chain must catch decode-shaped exceptions only.

    Catching bare `Exception` there turned a MemoryError, a decompression-bomb
    ceiling, a RecursionError or a broken cv2 into `ImageDecodeError` — i.e.
    into a persisted `input` verdict on a file that is very likely fine. Every
    one of these has to fail the whole predict instead, which the orchestrator
    retries.
    """
    for exception in (
        MemoryError("out of memory"),
        Image.DecompressionBombError("way too many pixels"),
        RecursionError("maximum recursion depth exceeded"),
    ):

        def exploding(*_args, _err=exception, **_kwargs):
            raise _err

        monkeypatch.setattr(Image, "open", exploding)
        with pytest.raises(type(exception)):
            load_image_or_slot(png_bytes())


def test_the_bomb_ceiling_is_a_config_limit_not_a_verdict(monkeypatch):
    """MAX_IMAGE_PIXELS is a configurable machine limit: raising it changes
    the answer, so the file must never be recorded as bad because of it."""

    def bomb(*_args, **_kwargs):
        raise Image.DecompressionBombError("exceeds limit")

    monkeypatch.setattr(Image, "open", bomb)
    with pytest.raises(Image.DecompressionBombError):
        load_image_from_buffer(png_bytes())

    # And the ceiling really is settable, which is the whole argument.
    assert isinstance(Image.MAX_IMAGE_PIXELS, int)


def test_a_broken_cv2_import_is_not_the_payloads_fault(monkeypatch):
    """A cv2 that is installed but fails to import is an environment
    problem. Folded into `last_err` it would surface as `Unreadable image`,
    blaming the item for a broken machine."""
    import builtins

    real_import = builtins.__import__

    def failing_import(name, *args, **kwargs):
        if name == "cv2":
            raise ImportError("DLL load failed while importing cv2")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", failing_import)
    with pytest.raises(ImportError, match="DLL load failed"):
        load_image_from_buffer(b"not an image")


def test_a_missing_cv2_only_means_no_fallback(monkeypatch):
    """OpenCV is optional, so "not installed" must leave the Pillow verdict
    standing rather than exploding on every undecodable file."""
    import builtins

    real_import = builtins.__import__

    def absent_cv2(name, *args, **kwargs):
        if name == "cv2":
            raise ModuleNotFoundError("No module named 'cv2'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", absent_cv2)
    with pytest.raises(ImageDecodeError, match="Unreadable image"):
        load_image_from_buffer(b"not an image")


def test_decode_image_inputs_excludes_only_the_bad_payloads():
    good = png_bytes()
    inputs = [
        FakeInput(file=good),
        FakeInput(file=b"garbage"),
        FakeInput(file=good),
    ]
    images, kept, slots = decode_image_inputs(inputs, what="Tagger")
    assert kept == [0, 2], "the batch keeps the healthy inputs, in order"
    assert len(images) == 2
    assert [index for index, _ in slots] == [1]
    assert slots[0][1][ERROR_SLOT_KEY]["class"] == ERROR_CLASS_INPUT


def test_a_fileless_input_is_still_a_caller_error():
    with pytest.raises(ValueError, match="Tagger requires image inputs."):
        decode_image_inputs([FakeInput(file=None)], what="Tagger")


def test_assemble_slots_restores_input_order():
    slots = [(1, input_error_slot("bad"))]
    assert assemble_slots(3, [0, 2], ["a", "b"], slots) == [
        "a",
        input_error_slot("bad"),
        "b",
    ]


def test_assemble_slots_rejects_a_lost_output():
    """A hole would reach the orchestrator as a count mismatch, which kills
    the worker; failing here keeps the cause visible."""
    with pytest.raises(RuntimeError, match="1 results for 2 inputs"):
        assemble_slots(2, [0, 1], ["only one"], [])
    with pytest.raises(RuntimeError, match=r"no output for inputs \[1\]"):
        assemble_slots(2, [0], ["a"], [])


def test_fixture_key_matches_the_constant():
    """The torch-free worker-protocol fixture hardcodes the reserved key;
    keep the literal in lockstep with the real constant."""
    fixture = (
        Path(__file__).resolve().parents[2]
        / "inferio_worker"
        / "fixture_impls"
        / "errorslot_impl.py"
    )
    assert f'ERROR_SLOT_KEY = "{ERROR_SLOT_KEY}"' in fixture.read_text(
        encoding="utf-8"
    )
