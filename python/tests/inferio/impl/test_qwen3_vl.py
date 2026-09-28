"""Qwen3-VL embedding: load precision, device, and bf16 output conversion.

The embedder is replaced by a stand-in, so nothing is downloaded and no GPU
is needed: what is asserted is what the impl passes to it and what it makes
of the tensor it gets back.
"""

import sys
from types import SimpleNamespace
from unittest import mock

import pytest
import torch

from inferio.impl.qwen3_vl import Qwen3VLEmbeddingModel
from inferio.impl.utils import deserialize_array
from inferio.inferio_types import PredictionInput

DEPS = "inferio.impl.deps.qwen_3_vl_embedding"


def _load(device, cap=None, torch_dtype=None, embeddings=None):
    """Load with a fake embedder on `device`; return (model, its kwargs)."""
    captured = {}

    class FakeEmbedder:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def process(self, payloads):
            return embeddings

    model = Qwen3VLEmbeddingModel("fake/qwen", torch_dtype=torch_dtype)
    with mock.patch.dict(
        sys.modules, {DEPS: SimpleNamespace(Qwen3VLEmbedder=FakeEmbedder)}
    ), mock.patch(
        "inferio.impl.qwen3_vl.get_device", return_value=[device]
    ), mock.patch.object(torch.version, "hip", None), mock.patch.object(
        torch.cuda, "get_device_capability", return_value=cap
    ):
        model.load()
    return model, captured


class TestLoad:
    def test_bf16_on_ampere_or_newer(self):
        for cap in [(8, 0), (8, 6), (12, 0)]:
            _, kwargs = _load(torch.device("cuda"), cap=cap)
            assert kwargs["torch_dtype"] is torch.bfloat16

    def test_fp32_below_ampere(self):
        _, kwargs = _load(torch.device("cuda"), cap=(7, 5))
        assert kwargs["torch_dtype"] is torch.float32

    @pytest.mark.parametrize("kind", ["cpu", "mps"])
    def test_fp32_off_cuda(self, kind):
        _, kwargs = _load(torch.device(kind))
        assert kwargs["torch_dtype"] is torch.float32

    def test_the_configured_dtype_wins(self):
        _, kwargs = _load(
            torch.device("cuda"), cap=(12, 0), torch_dtype="float32"
        )
        assert kwargs["torch_dtype"] is torch.float32

    @pytest.mark.parametrize("name", ["cpu", "mps", "cuda:1"])
    def test_the_resolved_device_is_passed(self, name):
        _, kwargs = _load(torch.device(name), cap=(8, 6))
        assert kwargs["device"] == torch.device(name)


def test_bf16_embeddings_are_returned_as_float32():
    embeddings = torch.tensor(
        [[0.6, 0.8, 0.0], [0.0, -1.0, 0.25]], dtype=torch.bfloat16
    )
    model, _ = _load(torch.device("cpu"), embeddings=embeddings)

    out = model.predict(
        [
            PredictionInput(data={"text": "a"}, file=None),
            PredictionInput(data={"text": "b"}, file=None),
        ]
    )

    assert len(out) == 2
    for row, blob in zip(embeddings, out):
        arr = deserialize_array(blob)
        assert arr.dtype == "float32"
        assert arr.tolist() == row.float().tolist()
