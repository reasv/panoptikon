import sqlite3
from typing import Any, Dict, Tuple

from chromadb.api import ClientAPI


class ModelOpts:
    def __str__(self):
        return self.setter_id()

    def __repr__(self):
        return self.setter_id()

    def model_type(self) -> str:
        raise NotImplementedError

    def batch_size(self) -> int:
        raise NotImplementedError

    def run_extractor(self, conn: sqlite3.Connection, cdb: ClientAPI):
        raise NotImplementedError

    def setter_id(self) -> str:
        raise NotImplementedError

    @classmethod
    def available_models(cls) -> Dict[str, Any]:
        raise NotImplementedError


class TaggerModel(ModelOpts):
    _model_repo: str
    _batch_size: int

    def __init__(self, batch_size: int = 64, model_repo: str | None = None):
        if model_repo is None:
            model_repo = TaggerModel.available_models()["wd-swinv2-tagger-v3"]
        assert model_repo in [
            s for n, s in TaggerModel.available_models().items()
        ], f"Invalid model repo {model_repo}"
        self._model_repo = model_repo
        self._batch_size = batch_size

    def model_type(self) -> str:
        return "tagger"

    def setter_id(self) -> str:
        return TaggerModel.model_to_setter_id(self.model_repo())

    def batch_size(self) -> int:
        return self._batch_size

    def run_extractor(self, conn: sqlite3.Connection, cdb: ClientAPI):
        from src.data_extractors.extractor_jobs.tags import (
            run_tag_extractor_job,
        )

        return run_tag_extractor_job(conn, self)

    @classmethod
    def available_models(cls) -> Dict[str, str]:
        # Dataset v3 series of models:
        SWINV2_MODEL_DSV3_REPO = "SmilingWolf/wd-swinv2-tagger-v3"
        CONV_MODEL_DSV3_REPO = "SmilingWolf/wd-convnext-tagger-v3"
        VIT_MODEL_DSV3_REPO = "SmilingWolf/wd-vit-tagger-v3"

        V3_MODELS = [
            SWINV2_MODEL_DSV3_REPO,
            CONV_MODEL_DSV3_REPO,
            VIT_MODEL_DSV3_REPO,
        ]

        # Dataset v2 series of models:
        MOAT_MODEL_DSV2_REPO = "SmilingWolf/wd-v1-4-moat-tagger-v2"
        SWIN_MODEL_DSV2_REPO = "SmilingWolf/wd-v1-4-swinv2-tagger-v2"
        CONV_MODEL_DSV2_REPO = "SmilingWolf/wd-v1-4-convnext-tagger-v2"
        CONV2_MODEL_DSV2_REPO = "SmilingWolf/wd-v1-4-convnextv2-tagger-v2"
        VIT_MODEL_DSV2_REPO = "SmilingWolf/wd-v1-4-vit-tagger-v2"
        return {name.split("/")[-1]: name for name in V3_MODELS}

    @classmethod
    def model_to_setter_id(cls, model_repo: str) -> str:
        # Reverse the available models dict
        model_to_name = {v: k for k, v in cls.available_models().items()}
        return model_to_name[model_repo]

    # Own methods
    def model_repo(self) -> str:
        return self._model_repo


class OCRModel(ModelOpts):
    _detection_model: str
    _recognition_model: str
    _batch_size: int

    def __init__(
        self,
        batch_size: int = 64,
        detection_model="db_resnet50",
        recognition_model="crnn_mobilenet_v3_small",
    ):
        self._detection_model = detection_model
        self._recognition_model = recognition_model
        self._batch_size = batch_size

    def model_type(self) -> str:
        return "ocr"

    def setter_id(self) -> str:
        return OCRModel.model_to_setter_id(
            self.detection_model(), self.recognition_model()
        )

    def batch_size(self) -> int:
        return self._batch_size

    def run_extractor(self, conn: sqlite3.Connection, cdb: ClientAPI):
        from src.data_extractors.extractor_jobs.ocr import run_ocr_extractor_job

        return run_ocr_extractor_job(conn, cdb, self)

    @classmethod
    def available_models(cls) -> Dict[str, Tuple[str, str]]:
        return {
            "db_resnet50|crnn_vgg16_bn": ("db_resnet50", "crnn_vgg16_bn"),
            "db_resnet50|crnn_mobilenet_v3_small": (
                "db_resnet50",
                "crnn_mobilenet_v3_small",
            ),
            "db_resnet50|crnn_mobilenet_v3_large": (
                "db_resnet50",
                "crnn_mobilenet_v3_large",
            ),
            "db_resnet50|master": ("db_resnet50", "master"),
            "db_resnet50|vitstr_small": ("db_resnet50", "vitstr_small"),
            "db_resnet50|vitstr_base": ("db_resnet50", "vitstr_base"),
            "db_resnet50|parseq": ("db_resnet50", "parseq"),
        }

    @classmethod
    def model_to_setter_id(
        cls, detection_model: str, recognition_model: str
    ) -> str:
        # Reverse the available models dict
        model_to_name = {v: k for k, v in cls.available_models().items()}
        return model_to_name[(detection_model, recognition_model)]

    def recognition_model(self) -> str:
        return self._recognition_model

    def detection_model(self) -> str:
        return self._detection_model


class ImageEmbeddingModel(ModelOpts):
    _model_name: str
    _checkpoint: str
    _batch_size: int

    def __init__(
        self,
        batch_size: int = 64,
        model_name="ViT-H-14-378-quickgelu",
        pretrained="dfn5b",
    ):

        self._model_name = model_name
        self._checkpoint = pretrained
        self._batch_size = batch_size

    def model_type(self) -> str:
        return "clip"

    def setter_id(self) -> str:
        return ImageEmbeddingModel.model_to_setter_id(
            self.clip_model_name(), self.clip_model_checkpoint()
        )

    def batch_size(self) -> int:
        return self._batch_size

    def run_extractor(self, conn: sqlite3.Connection, cdb: ClientAPI):
        from src.data_extractors.extractor_jobs.clip import (
            run_image_embedding_extractor_job,
        )

        return run_image_embedding_extractor_job(conn, cdb, self)

    @classmethod
    def available_models(cls) -> Dict[str, Tuple[str, str]]:
        from src.data_extractors.ai.clip_model_list import CLIP_CHECKPOINTS

        return {
            f"{model_name}|{checkpoint}": (model_name, checkpoint)
            for model_name, checkpoint in CLIP_CHECKPOINTS
        }

    @classmethod
    def model_to_setter_id(cls, model_name: str, checkpoint: str) -> str:
        # Reverse the available models dict
        model_to_name = {v: k for k, v in cls.available_models().items()}
        return model_to_name[(model_name, checkpoint)]

    def clip_model_name(self) -> str:
        return self._model_name

    def clip_model_checkpoint(self) -> str:
        return f"{self._checkpoint}"


class WhisperSTTModel(ModelOpts):
    _model_repo: str
    _batch_size: int

    def __init__(self, batch_size: int = 8, model_repo: str | None = None):
        if model_repo is None:
            model_repo = WhisperSTTModel.available_models()["base"]

        assert (
            model_repo in WhisperSTTModel.available_models().values()
        ), f"Invalid model repo {model_repo}"

        self._model_repo = model_repo
        self._batch_size = batch_size

    def model_type(self) -> str:
        return "stt"

    def setter_id(self) -> str:
        return WhisperSTTModel.model_to_setter_id(self.model_repo())

    def batch_size(self) -> int:
        return self._batch_size

    def run_extractor(self, conn: sqlite3.Connection, cdb: ClientAPI):
        from src.data_extractors.extractor_jobs.whisper import (
            run_whisper_extractor_job,
        )

        return run_whisper_extractor_job(conn, cdb, self)

    @classmethod
    def available_models(cls) -> Dict[str, str]:
        _MODELS = {
            "tiny.en": "Systran/faster-whisper-tiny.en",
            "tiny": "Systran/faster-whisper-tiny",
            "base.en": "Systran/faster-whisper-base.en",
            "base": "Systran/faster-whisper-base",
            "small.en": "Systran/faster-whisper-small.en",
            "small": "Systran/faster-whisper-small",
            "medium.en": "Systran/faster-whisper-medium.en",
            "medium": "Systran/faster-whisper-medium",
            "large-v1": "Systran/faster-whisper-large-v1",
            "large-v2": "Systran/faster-whisper-large-v2",
            "large-v3": "Systran/faster-whisper-large-v3",
            "large": "Systran/faster-whisper-large-v3",
            "distil-large-v2": "Systran/faster-distil-whisper-large-v2",
            "distil-medium.en": "Systran/faster-distil-whisper-medium.en",
            "distil-small.en": "Systran/faster-distil-whisper-small.en",
            "distill-large-v3": "Systran/faster-distil-whisper-large-v3",
        }
        return _MODELS

    @classmethod
    def model_to_setter_id(cls, model_repo: str) -> str:
        # Reverse the available models dict
        model_to_name = {v: k for k, v in cls.available_models().items()}
        return model_to_name[model_repo]

    def model_repo(self) -> str:
        return self._model_repo
