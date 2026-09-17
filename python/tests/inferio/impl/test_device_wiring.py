"""The impls actually *use* the device `get_device()` resolved.

Three impls used to compute `self.devices` and then let their backend probe
the hardware again — SentenceTransformer's own cuda -> mps -> cpu fallback,
EasyOCR's `torch.cuda.is_available()`, CTranslate2's `device="auto"`. On a
host the orchestrator priced against system RAM (`INFERIO_DEVICE=cpu`) that
second probe answers a different question and the model runs somewhere no
batch was budgeted against (docs/unified-memory-admission.md, backend C,
"Device coherence").

These are wiring tests: the heavyweight constructors are mocked, so what is
asserted is the argument each impl passes, on any box.
"""

import sys
from types import SimpleNamespace
from unittest import mock

import pytest
import torch


def _mocked_module(name: str, **attrs):
    """Inject a stand-in for a heavy third-party module."""
    return mock.patch.dict(sys.modules, {name: SimpleNamespace(**attrs)})


class TestSentenceTransformersDevice:
    def _load(self, devices, init_args=None):
        from inferio.impl.sentence_transformers import SentenceTransformersModel

        captured = {}

        def SentenceTransformer(**kwargs):
            captured.update(kwargs)
            return SimpleNamespace(tokenizer=None, max_seq_length=128)

        model = SentenceTransformersModel(
            model_name="fake/model", init_args=init_args or {}
        )
        with _mocked_module(
            "sentence_transformers", SentenceTransformer=SentenceTransformer
        ):
            with mock.patch(
                "inferio.impl.sentence_transformers.get_device",
                return_value=devices,
            ):
                model.load()
        return captured

    def test_our_device_is_passed_when_the_config_names_none(self):
        assert self._load([torch.device("cpu")])["device"] == "cpu"
        # A device index survives verbatim — SentenceTransformer takes the
        # same string form torch renders.
        assert self._load([torch.device("cuda:1")])["device"] == "cuda:1"

    def test_an_explicit_config_device_still_wins(self):
        # `setdefault`, not an override: this is the default, and an operator
        # who wrote a device into `init_args` meant it.
        captured = self._load(
            [torch.device("cpu")], init_args={"device": "cuda"}
        )
        assert captured["device"] == "cuda"

    def test_the_models_own_init_args_are_not_mutated(self):
        from inferio.impl.sentence_transformers import SentenceTransformersModel

        init_args: dict = {}
        model = SentenceTransformersModel(
            model_name="fake/model", init_args=init_args
        )
        with _mocked_module(
            "sentence_transformers",
            SentenceTransformer=lambda **kwargs: SimpleNamespace(),
        ):
            with mock.patch(
                "inferio.impl.sentence_transformers.get_device",
                return_value=[torch.device("cpu")],
            ):
                model.load()
        assert init_args == {}, "the config dict is the registry's, not ours"


class TestEasyOcrDevice:
    def _load(self, devices, gpu=True):
        from inferio.impl.eocr import EasyOCRModel

        captured = {}

        def Reader(**kwargs):
            captured.update(kwargs)
            return SimpleNamespace()

        model = EasyOCRModel(languages=["en"], gpu=gpu)
        with _mocked_module("easyocr", Reader=Reader):
            with mock.patch(
                "inferio.impl.eocr.get_device", return_value=devices
            ):
                model.load()
        return captured

    @pytest.mark.parametrize("kind", ["cpu", "mps"])
    def test_a_non_cuda_device_turns_the_gpu_off(self, kind):
        # EasyOCR's `gpu` argument only ever means CUDA/HIP. On a CPU-priced
        # host — including an `accelerator = "cpu"` Mac, where Metal is
        # perfectly available — it has to be False.
        assert self._load([torch.device(kind)])["gpu"] is False

    def test_a_cuda_device_turns_it_on(self):
        assert self._load([torch.device("cuda")])["gpu"] is True

    def test_the_configured_gpu_false_still_wins(self):
        assert self._load([torch.device("cuda")], gpu=False)["gpu"] is False


class TestWhisperDevice:
    def _load(self, devices):
        from inferio.impl.whisper import FasterWhisperModel

        captured = {}
        compute_type_args = {}

        def WhisperModel(**kwargs):
            captured.update(kwargs)
            return SimpleNamespace()

        def select_ct2_compute_type(preferred="float16", **kwargs):
            compute_type_args.update(kwargs)
            return "float32"

        model = FasterWhisperModel(model_name="fake/whisper")
        with _mocked_module("faster_whisper", WhisperModel=WhisperModel):
            with mock.patch(
                "inferio.impl.whisper.get_device", return_value=devices
            ):
                with mock.patch(
                    "inferio.impl.whisper.select_ct2_compute_type",
                    side_effect=select_ct2_compute_type,
                ):
                    model.load()
        return captured, compute_type_args

    def test_a_non_cuda_device_is_named_outright(self):
        # The half that has to be authoritative: a CPU-priced host must not
        # let CTranslate2's own probe find the machine's GPU.
        for kind in ("cpu", "mps"):
            captured, compute_args = self._load([torch.device(kind)])
            assert captured["device"] == "cpu"
            assert compute_args["device_kind"] == "cpu"

    def test_a_cuda_device_keeps_auto(self):
        # Deliberately `auto` rather than `"cuda"`: torch calls a ROCm board
        # `cuda` too, and a CT2 build with no GPU support raises on an
        # explicit `device="cuda"` where `auto` degrades to the CPU itself.
        captured, compute_args = self._load([torch.device("cuda")])
        assert captured["device"] == "auto"
        assert compute_args["device_kind"] == "cuda"
