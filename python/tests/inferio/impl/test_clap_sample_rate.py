"""CLAP embeds audio at the rate the group declares, and says so out loud.

`ClapFeatureExtractor` does not resample: it compares the rate it is handed
against the checkpoint's own (48 kHz for every shipped CLAP) and raises on a
mismatch, warning when it is handed nothing at all. The `audio_tracks` handler
decodes at 16 kHz unless the group says otherwise, so the two ends have to be
pinned together: the registry declares one rate to the decoder and the same
rate to the impl, and the impl passes it to the processor on every batch.

Model-free: the processor and the model are fakes, `load` is stubbed.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

import numpy as np
import pytest
import torch

from inferio.impl.clap import ClapModel
from inferio.impl.utils import serialize_array
from inferio.inferio_types import PredictionInput

REGISTRY = (
    Path(__file__).resolve().parents[3]
    / "inferio"
    / "config"
    / "inference.toml"
)
# The rate every shipped CLAP checkpoint's preprocessor_config.json declares.
CHECKPOINT_RATE = 48000


class FakeFeatures(dict):
    def to(self, device):
        self.device = device
        return self


class FakeProcessor:
    """Stands in for `ClapProcessor`, keeping the extractor's one contract:
    audio at a rate other than the checkpoint's is refused, and an unstated
    rate is not silently accepted either."""

    def __init__(self, sampling_rate: int = CHECKPOINT_RATE):
        self.sampling_rate = sampling_rate
        self.calls: list[dict] = []

    def __call__(self, audios, sampling_rate=None, return_tensors=None):
        self.calls.append(
            {
                "lengths": [len(audio) for audio in audios],
                "sampling_rate": sampling_rate,
                "return_tensors": return_tensors,
            }
        )
        if sampling_rate is None:
            raise AssertionError(
                "sampling_rate was not passed to the feature extractor"
            )
        if sampling_rate != self.sampling_rate:
            raise ValueError(
                f"was trained using a sampling rate of {self.sampling_rate}. "
                f"Please make sure that the provided `raw_speech` input was "
                f"sampled with {self.sampling_rate} and not {sampling_rate}."
            )
        return FakeFeatures(input_features=torch.zeros(len(audios), 2))


class FakeClap:
    def __init__(self, dim: int = 4):
        self.dim = dim

    def get_audio_features(self, **kwargs):
        return torch.arange(
            kwargs["input_features"].shape[0] * self.dim, dtype=torch.float32
        ).reshape(-1, self.dim)


def loaded_model(sample_rate: int, processor: FakeProcessor) -> ClapModel:
    model = ClapModel("laion/clap-htsat-unfused", sample_rate=sample_rate)
    model.preprocess = processor
    model.model = FakeClap()
    model.tokenizer = None
    model.device = torch.device("cpu")
    model.devices = [model.device]
    model._model_loaded = True
    return model


def audio_inputs(count: int, seconds: float, rate: int):
    """The payload the `audio_tracks` handler sends: a mono float32 `.npy`
    buffer, no rate of its own anywhere in the bytes."""
    samples = np.zeros(int(seconds * rate), dtype=np.float32)
    return [
        PredictionInput(data=None, file=serialize_array(samples))
        for _ in range(count)
    ]


def test_processor_is_told_the_declared_rate():
    """The declared rate reaches the processor on every batch, and it is the
    rate the checkpoint wants, so the batch goes through."""
    processor = FakeProcessor()
    model = loaded_model(CHECKPOINT_RATE, processor)

    outputs = model.predict(audio_inputs(3, 30.0, CHECKPOINT_RATE))

    assert len(outputs) == 3
    assert len(processor.calls) == 1
    call = processor.calls[0]
    assert call["sampling_rate"] == CHECKPOINT_RATE
    # 30 s at 48 kHz, not the 480 000 samples a 16 kHz decode would send —
    # which the extractor would have read as exactly 10 s of 48 kHz audio.
    assert call["lengths"] == [1_440_000] * 3


def test_a_16_khz_payload_is_refused_rather_than_embedded():
    """The pre-fix defect, stated as the failure it now is: a group left at
    the handler's 16 kHz default cannot reach the model at all."""
    processor = FakeProcessor()
    model = loaded_model(16000, processor)

    with pytest.raises(ValueError, match="sampled with 48000 and not 16000"):
        model.predict(audio_inputs(1, 30.0, 16000))


def test_registry_declares_the_same_rate_to_both_ends():
    """The decoder's rate and the impl's rate are one number in two places;
    they are only correct together."""
    registry = tomllib.loads(REGISTRY.read_text(encoding="utf-8"))
    group = registry["group"]["clap"]
    declared = group["metadata"]["input_spec"]["opts"]["sample_rate"]

    assert declared == CHECKPOINT_RATE
    assert group["config"]["sample_rate"] == declared


def test_text_inputs_never_reach_the_feature_extractor():
    """Text is tokenized, not featurized, so the rate is none of its
    business — the audio branch is the only caller."""
    processor = FakeProcessor()
    model = loaded_model(CHECKPOINT_RATE, processor)

    class FakeTokens(dict):
        def to(self, device):
            return self

    model.tokenizer = lambda texts, padding, return_tensors: FakeTokens(
        input_ids=torch.zeros(len(texts), 2)
    )
    model.model.get_text_features = lambda **kwargs: torch.zeros(
        kwargs["input_ids"].shape[0], 4
    )

    outputs = model.predict([PredictionInput(data={"text": "a dog"}, file=None)])

    assert len(outputs) == 1
    assert processor.calls == []
