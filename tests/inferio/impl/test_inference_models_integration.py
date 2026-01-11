import os
from typing import Sequence

import pytest


def _env_flag(name: str) -> bool:
    value = os.environ.get(name, "")
    return value.strip().lower() not in ("", "0", "false", "no", "off")


def _requires_cuda() -> bool:
    try:
        import torch

        return bool(torch.cuda.is_available())
    except Exception:
        return False


@pytest.fixture()
def model_cache_env(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    cache_root = tmp_path / "model-cache"
    cache_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("HF_HOME", str(cache_root / "hf-home"))
    monkeypatch.setenv("HF_HUB_CACHE", str(cache_root / "hf-hub-cache"))
    monkeypatch.setenv("TRANSFORMERS_CACHE", str(cache_root / "transformers-cache"))
    monkeypatch.setenv("TORCH_HOME", str(cache_root / "torch-home"))

    monkeypatch.setenv("TOKENIZERS_PARALLELISM", "false")


def _make_test_image_bytes() -> bytes:
    from io import BytesIO

    from PIL import Image, ImageDraw

    img = Image.new("RGB", (512, 256), (255, 255, 255))
    draw = ImageDraw.Draw(img)
    draw.rectangle([10, 10, 500, 245], outline=(0, 0, 0), width=3)
    draw.text((30, 60), "HELLO WORLD", fill=(0, 0, 0))
    draw.text((30, 130), "PANOPTIKON", fill=(0, 0, 0))

    buf = BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _serialize_np_array(array) -> bytes:
    import io
    import numpy as np

    buf = io.BytesIO()
    np.save(buf, array)
    buf.seek(0)
    return buf.read()


def _make_test_audio_bytes(sample_rate: int = 16000, seconds: float = 1.0) -> bytes:
    import numpy as np

    t = np.arange(int(sample_rate * seconds), dtype=np.float32) / sample_rate
    audio = (0.1 * np.sin(2 * np.pi * 440.0 * t)).astype(np.float32)
    return _serialize_np_array(audio)


def _deserialize_np_array(blob: bytes):
    import io
    import numpy as np

    buf = io.BytesIO(blob)
    buf.seek(0)
    return np.load(buf, allow_pickle=False)


def _assert_embedding_bytes(blob: bytes, *, min_dim: int = 8) -> None:
    arr = _deserialize_np_array(blob)
    assert arr.size >= min_dim
    assert arr.ndim in (1, 2)


def _assert_ocr_dict(output: dict) -> None:
    assert isinstance(output.get("transcription"), str)
    assert isinstance(output.get("confidence"), (int, float))
    assert (
        isinstance(output.get("language"), str)
        or output.get("language") is None
    )
    assert (
        isinstance(output.get("language_confidence"), (int, float))
        or output.get("language_confidence") is None
    )


def _assert_whisper_dict(output: dict) -> None:
    assert isinstance(output.get("transcription"), str)
    assert (
        isinstance(output.get("confidence"), (int, float))
        or output.get("confidence") is None
    )
    assert isinstance(output.get("language"), str)
    assert isinstance(output.get("language_confidence"), (int, float))


def _assert_tags_dict(output: dict) -> None:
    assert isinstance(output.get("namespace"), str)
    assert isinstance(output.get("tags"), list)

def _predict_and_unload(model, inputs):
    try:
        model.load()
        return model.predict(inputs)
    finally:
        try:
            model.unload()
        except Exception:
            pass


@pytest.mark.integration
def test_clip_model_runs_text_and_image(model_cache_env):
    from inferio.impl.clip import ClipModel
    from inferio.inferio_types import PredictionInput

    model = ClipModel(model_name="ViT-B-32", pretrained="openai")
    image_bytes = _make_test_image_bytes()

    outputs = _predict_and_unload(
        model,
        [
            PredictionInput(data={"text": "a photo of a cat"}, file=None),
            PredictionInput(data={"text": "a photo of a dog"}, file=None),
            PredictionInput(data=None, file=image_bytes),
        ],
    )
    assert len(outputs) == 3
    for out in outputs:
        assert isinstance(out, (bytes, bytearray))
        _assert_embedding_bytes(bytes(out))


@pytest.mark.integration
def test_jina_clip_model_runs_if_api_key_present(model_cache_env, monkeypatch):
    if not os.environ.get("JINA_API_KEY"):
        pytest.skip("JINA_API_KEY not set")

    from inferio.impl.jina_clip import JinaClipModel
    from inferio.inferio_types import PredictionInput

    monkeypatch.setenv("JINA_MAX_RETRIES", "1")
    monkeypatch.setenv("JINA_TIMEOUT", "30")

    model = JinaClipModel(model_name="jina-clip-v2", dimensions=1024, normalized=True)
    image_bytes = _make_test_image_bytes()
    outputs = _predict_and_unload(
        model,
        [
            PredictionInput(data={"text": "a photo of a cat"}, file=None),
            PredictionInput(data=None, file=image_bytes),
        ],
    )
    assert len(outputs) == 2
    for out in outputs:
        assert isinstance(out, (bytes, bytearray))
        _assert_embedding_bytes(bytes(out))


@pytest.mark.integration
def test_sentence_transformers_model_runs(model_cache_env):
    from inferio.impl.sentence_transformers import SentenceTransformersModel
    from inferio.inferio_types import PredictionInput

    model = SentenceTransformersModel(model_name="all-MiniLM-L6-v2")
    outputs = _predict_and_unload(
        model,
        [
            PredictionInput(data={"text": "hello world", "args": {}}, file=None),
            PredictionInput(data={"text": "goodbye world", "args": {}}, file=None),
        ],
    )
    assert len(outputs) == 2
    for out in outputs:
        assert isinstance(out, (bytes, bytearray))
        arr = _deserialize_np_array(bytes(out))
        assert arr.ndim == 2
        assert arr.shape[0] >= 1
        assert arr.shape[1] >= 8


@pytest.mark.integration
def test_doctr_model_runs(model_cache_env):
    from inferio.impl.ocr import DoctrModel
    from inferio.inferio_types import PredictionInput

    model = DoctrModel(
        detection_model="db_resnet50",
        recognition_model="crnn_mobilenet_v3_small",
    )
    image_bytes = _make_test_image_bytes()
    outputs = _predict_and_unload(
        model, [PredictionInput(data={"threshold": 0.0}, file=image_bytes)]
    )
    assert len(outputs) == 1
    assert isinstance(outputs[0], dict)
    _assert_ocr_dict(outputs[0])


@pytest.mark.integration
def test_easyocr_model_runs(model_cache_env):
    from inferio.impl.eocr import EasyOCRModel
    from inferio.inferio_types import PredictionInput

    model = EasyOCRModel(languages=["en"], gpu=False, enable_batching=False, verbose=False)
    image_bytes = _make_test_image_bytes()
    outputs = _predict_and_unload(
        model, [PredictionInput(data={"threshold": 0.0}, file=image_bytes)]
    )
    assert len(outputs) == 1
    assert isinstance(outputs[0], dict)
    _assert_ocr_dict(outputs[0])


@pytest.mark.integration
def test_wd_tagger_runs(model_cache_env):
    from inferio.impl.wd_tagger import WDTagger
    from inferio.inferio_types import PredictionInput

    model = WDTagger(model_repo="SmilingWolf/wd-swinv2-tagger-v3")
    image_bytes = _make_test_image_bytes()
    outputs = _predict_and_unload(
        model,
        [
            PredictionInput(
                data={"threshold": 0.2, "character_threshold": 0.2},
                file=image_bytes,
            )
        ],
    )
    assert len(outputs) == 1
    assert isinstance(outputs[0], dict)
    _assert_tags_dict(outputs[0])


@pytest.mark.integration
def test_moondream_captioner_runs(model_cache_env):
    from inferio.impl.md_captioner import MoondreamCaptioner
    from inferio.inferio_types import PredictionInput

    model = MoondreamCaptioner(
        model_repo="vikhyatk/moondream2",
        model_revision="2025-03-27",
        task="caption",
        caption_length="short",
    )
    image_bytes = _make_test_image_bytes()
    outputs = _predict_and_unload(
        model, [PredictionInput(data={}, file=image_bytes)]
    )
    assert len(outputs) == 1
    assert isinstance(outputs[0], dict)
    _assert_ocr_dict(outputs[0])


@pytest.mark.integration
def test_moondream_tagger_runs(model_cache_env):
    from inferio.impl.md_tagger import MoondreamTagger
    from inferio.inferio_types import PredictionInput

    model = MoondreamTagger(
        model_repo="vikhyatk/moondream2",
        model_revision="2025-03-27",
        enable_rating=False,
    )
    image_bytes = _make_test_image_bytes()
    outputs = _predict_and_unload(
        model, [PredictionInput(data={}, file=image_bytes)]
    )
    assert len(outputs) == 1
    assert isinstance(outputs[0], dict)
    _assert_tags_dict(outputs[0])


@pytest.mark.integration
def test_clap_model_runs_text_and_audio(model_cache_env):
    from inferio.impl.clap import ClapModel
    from inferio.inferio_types import PredictionInput

    model = ClapModel(model_name="laion/clap-htsat-unfused")
    audio_bytes = _make_test_audio_bytes(sample_rate=48000, seconds=1.0)
    outputs = _predict_and_unload(
        model,
        [
            PredictionInput(data={"text": "a dog barking"}, file=None),
            PredictionInput(data=None, file=audio_bytes),
        ],
    )
    assert len(outputs) == 2
    for out in outputs:
        assert isinstance(out, (bytes, bytearray))
        _assert_embedding_bytes(bytes(out))


@pytest.mark.integration
def test_faster_whisper_model_runs_on_cuda(model_cache_env):
    if not _requires_cuda():
        pytest.skip("FasterWhisperModel uses float16; requires CUDA in this project configuration.")

    from inferio.impl.whisper import FasterWhisperModel
    from inferio.inferio_types import PredictionInput

    model = FasterWhisperModel(model_name="Systran/faster-distil-whisper-small.en")
    audio_bytes = _make_test_audio_bytes(sample_rate=16000, seconds=2.0)
    outputs = _predict_and_unload(
        model,
        [
            PredictionInput(
                data={"args": {"language": "en"}},
                file=audio_bytes,
            )
        ],
    )
    assert len(outputs) == 1
    assert isinstance(outputs[0], dict)
    _assert_whisper_dict(outputs[0])


@pytest.mark.integration
def test_florence2_runs_on_cuda(model_cache_env):
    if not _requires_cuda():
        pytest.skip("Florence2 uses float16 + torch.compile; requires CUDA.")
    if not _env_flag("PANOPTIKON_RUN_FLORENCE2"):
        pytest.skip("Set PANOPTIKON_RUN_FLORENCE2=1 to run Florence2 integration test.")

    from inferio.impl.florence2 import Florence2
    from inferio.inferio_types import PredictionInput

    model = Florence2(
        model_name="microsoft/Florence-2-large-ft",
        task_prompt="<CAPTION>",
        enable_batch=False,
        max_output=64,
        num_beams=1,
    )
    image_bytes = _make_test_image_bytes()
    outputs = _predict_and_unload(
        model, [PredictionInput(data={}, file=image_bytes)]
    )
    assert len(outputs) == 1
    assert isinstance(outputs[0], dict)
    _assert_ocr_dict(outputs[0])
