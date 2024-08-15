import os
import sqlite3
from typing import Any, Dict, Generator, List, Sequence, Tuple, Type

import PIL
import PIL.Image

import src.data_extractors.extraction_jobs.types as job_types
from src.db.group_settings import (
    retrieve_model_group_settings,
    save_model_group_settings,
)
from src.db.rules.types import (
    MimeFilter,
    ProcessedExtractedDataFilter,
    ProcessedItemsFilter,
    RuleItemFilters,
)
from src.db.setters import delete_setter_by_name
from src.db.tags import delete_orphan_tags
from src.types import OutputDataType, TargetEntityType


class ModelOpts:

    def __init__(self, model_name: str | None = None):
        if model_name is None:
            model_name = self.default_model()
        assert self.valid_model(model_name), f"Invalid model {model_name}"

        self._init(model_name)

    def __str__(self):
        return self.setter_name()

    def __repr__(self):
        return self.setter_name()

    @classmethod
    def target_entities(cls) -> List[TargetEntityType]:
        return ["items"]

    @classmethod
    def available_models(cls) -> List[str]:
        return list(cls._available_models_mapping().keys())

    @classmethod
    def default_batch_size(cls) -> int:
        return 64

    @classmethod
    def default_threshold(cls) -> float | None:
        return None

    @classmethod
    def get_group_batch_size(cls, conn: sqlite3.Connection) -> int:
        settings = retrieve_model_group_settings(conn, cls.group_name())
        if settings:
            return settings[0]
        return cls.default_batch_size()

    @classmethod
    def get_group_threshold(cls, conn: sqlite3.Connection) -> float | None:
        settings = retrieve_model_group_settings(conn, cls.group_name())
        if settings:
            return settings[1]
        return cls.default_threshold()

    @classmethod
    def set_group_threshold(cls, conn: sqlite3.Connection, threshold: float):
        save_model_group_settings(
            conn, cls.group_name(), cls.get_group_batch_size(conn), threshold
        )

    @classmethod
    def set_group_batch_size(cls, conn: sqlite3.Connection, batch_size: int):
        save_model_group_settings(
            conn, cls.group_name(), batch_size, cls.get_group_threshold(conn)
        )

    @classmethod
    def valid_model(cls, model_name: str) -> bool:
        return model_name in cls.available_models()

    @classmethod
    def default_model(cls) -> str:
        return cls.available_models()[0]

    def delete_extracted_data(self, conn: sqlite3.Connection):
        delete_setter_by_name(conn, self.data_type(), self.setter_name())
        return f"Deleted text extracted from items by model {self.setter_name()}.\n"

    @classmethod
    def supported_mime_types(cls) -> List[str] | None:
        return None

    def item_extraction_rules(self) -> RuleItemFilters:
        rules = []
        target_entities = self.target_entities()
        if "items" in target_entities:
            rules.append(
                ProcessedItemsFilter(
                    setter_type=self.data_type(), setter_name=self.setter_name()
                )
            )
        else:
            rules.append(
                ProcessedExtractedDataFilter(
                    setter_type=self.data_type(),
                    setter_name=self.setter_name(),
                    data_types=target_entities,  # type: ignore
                )
            )

        mime_types = self.supported_mime_types()
        if mime_types:
            rules.append(
                MimeFilter(
                    mime_type_prefixes=mime_types,
                )
            )
        return RuleItemFilters(positive=rules, negative=[])

    @classmethod
    def data_type(cls) -> OutputDataType:
        raise NotImplementedError

    def run_extractor(
        self, conn: sqlite3.Connection
    ) -> Generator[
        job_types.ExtractorJobProgress | job_types.ExtractorJobReport, Any, None
    ]:
        raise NotImplementedError

    def setter_name(self) -> str:
        raise NotImplementedError

    @classmethod
    def _available_models_mapping(cls) -> Dict[str, Any]:
        raise NotImplementedError

    def _init(self, model_name: str):
        raise NotImplementedError

    @classmethod
    def name(cls) -> str:
        raise NotImplementedError

    @classmethod
    def description(cls) -> str:
        raise NotImplementedError

    @classmethod
    def group_name(cls) -> str:
        raise NotImplementedError


class TagsModel(ModelOpts):
    _model_repo: str

    def _init(self, model_name: str):
        self._model_repo = TagsModel._available_models_mapping()[model_name]

    @classmethod
    def data_type(cls) -> OutputDataType:
        return "tags"

    @classmethod
    def group_name(cls) -> str:
        return "wd-tags"

    @classmethod
    def name(cls) -> str:
        return "Tags"

    @classmethod
    def default_threshold(cls) -> float | None:
        return 0.1

    @classmethod
    def description(cls) -> str:
        return "Generate danbooru-type tags for images and videos"

    def setter_name(self) -> str:
        return TagsModel._model_to_setter_name(self.model_repo())

    @classmethod
    def default_model(cls) -> str:
        return "wd-swinv2-tagger-v3"

    def run_extractor(self, conn: sqlite3.Connection):
        from src.data_extractors.extraction_jobs.tags import (
            run_tag_extractor_job,
        )

        return run_tag_extractor_job(conn, self)

    @classmethod
    def _available_models_mapping(cls) -> Dict[str, str]:
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
    def _model_to_setter_name(cls, model_repo: str) -> str:
        # Reverse the available models dict
        model_to_name = {
            v: k for k, v in cls._available_models_mapping().items()
        }
        return model_to_name[model_repo]

    # Own methods
    def model_repo(self) -> str:
        return self._model_repo

    def delete_extracted_data(self, conn: sqlite3.Connection):
        msg = super().delete_extracted_data(conn)
        orphans_deleted = delete_orphan_tags(conn)
        msg += f"\nDeleted {orphans_deleted} orphaned tags.\n"
        return msg


class TagsModelV2(ModelOpts):
    _inference_id: str

    def _init(self, model_name: str):
        self._inference_id = model_name

    @classmethod
    def data_type(cls) -> OutputDataType:
        return get_inference_api_client().get_metadata()["tags"][
            "group_metadata"
        ]["output_type"]

    @classmethod
    def group_name(cls) -> str:
        return "tags"

    @classmethod
    def name(cls) -> str:
        return get_inference_api_client().get_metadata()["tags"][
            "group_metadata"
        ]["name"]

    @classmethod
    def default_threshold(cls) -> float | None:
        return get_inference_api_client().get_metadata()["tags"][
            "group_metadata"
        ]["default_threshold"]

    @classmethod
    def description(cls) -> str:
        return get_inference_api_client().get_metadata()["tags"][
            "group_metadata"
        ]["description"]

    def setter_name(self) -> str:
        return "tags/" + self._inference_id

    @classmethod
    def default_model(cls) -> str:
        return get_inference_api_client().get_metadata()["tags"][
            "group_metadata"
        ]["default_inference_id"]

    def run_extractor(self, conn: sqlite3.Connection):
        from src.data_extractors.extraction_jobs.tags import (
            run_tagv2_extractor_job,
        )

        return run_tagv2_extractor_job(conn, self)

    @classmethod
    def available_models(cls) -> List[str]:
        return [
            f"{option}"
            for option in list(
                get_inference_api_client()
                .get_metadata()["tags"]["inference_ids"]
                .keys()
            )
        ]

    def load_model(self, cache_key: str, lru_size: int, ttl_seconds: int):
        get_inference_api_client().load_model(
            "tags/" + self._inference_id, cache_key, lru_size, ttl_seconds
        )

    def unload_model(self, cache_key: str):
        get_inference_api_client().unload_model(
            "tags/" + self._inference_id, cache_key
        )

    def run_batch_inference(
        self,
        cache_key: str,
        lru_size: int,
        ttl_seconds: int,
        threshold: float,
        images: Sequence[str | PIL.Image.Image],
    ) -> List[dict]:
        imgs = [
            (
                ({"threshold": threshold}, img)
                if isinstance(img, str)
                else ({"threshold": threshold}, img.tobytes())
            )
            for img in images
        ]
        result = get_inference_api_client().predict(
            "tags/" + self._inference_id, cache_key, lru_size, ttl_seconds, imgs
        )
        return result["outputs"]  # type: ignore

    def delete_extracted_data(self, conn: sqlite3.Connection):
        msg = super().delete_extracted_data(conn)
        orphans_deleted = delete_orphan_tags(conn)
        msg += f"\nDeleted {orphans_deleted} orphaned tags.\n"
        return msg


class OCRModel(ModelOpts):
    _detection_model: str
    _recognition_model: str

    def _init(self, model_name: str):
        self._detection_model, self._recognition_model = (
            OCRModel._available_models_mapping()[model_name]
        )

    @classmethod
    def default_threshold(cls) -> float | None:
        return 0.41

    @classmethod
    def data_type(cls) -> OutputDataType:
        return "text"

    @classmethod
    def group_name(cls) -> str:
        return "doctr"

    @classmethod
    def name(cls) -> str:
        return "DocTR"

    @classmethod
    def description(cls) -> str:
        return "Extract text from images, videos, and documents through OCR"

    def setter_name(self) -> str:
        return OCRModel._model_to_setter_name(
            self.detection_model(), self.recognition_model()
        )

    @classmethod
    def default_model(cls) -> str:
        return "doctr|db_resnet50|crnn_mobilenet_v3_small"

    def run_extractor(self, conn: sqlite3.Connection):
        from src.data_extractors.extraction_jobs.ocr import (
            run_ocr_extractor_job,
        )

        return run_ocr_extractor_job(conn, self)

    @classmethod
    def _available_models_mapping(cls) -> Dict[str, Tuple[str, str]]:
        options = [
            ("db_resnet50", "crnn_vgg16_bn"),
            ("db_resnet50", "crnn_mobilenet_v3_small"),
            ("db_resnet50", "crnn_mobilenet_v3_large"),
            ("db_resnet50", "master"),
            ("db_resnet50", "vitstr_small"),
            ("db_resnet50", "vitstr_base"),
            ("db_resnet50", "parseq"),
        ]
        return {
            f"doctr|{detection}|{recognition}": (detection, recognition)
            for detection, recognition in options
        }

    @classmethod
    def _model_to_setter_name(
        cls, detection_model: str, recognition_model: str
    ) -> str:
        # Reverse the available models dict
        model_to_name = {
            v: k for k, v in cls._available_models_mapping().items()
        }
        return model_to_name[(detection_model, recognition_model)]

    def recognition_model(self) -> str:
        return self._recognition_model

    def detection_model(self) -> str:
        return self._detection_model


class ImageEmbeddingModel(ModelOpts):
    _model_name: str
    _checkpoint: str

    def _init(self, model_name: str):

        self._model_name, self._checkpoint = (
            ImageEmbeddingModel._available_models_mapping()[model_name]
        )

    @classmethod
    def data_type(cls) -> OutputDataType:
        return "clip"

    @classmethod
    def group_name(cls) -> str:
        return "clip"

    @classmethod
    def name(cls) -> str:
        return "CLIP Image Embeddings"

    @classmethod
    def description(cls) -> str:
        return "Generate Image Embeddings using OpenAI's CLIP model for semantic image search"

    def setter_name(self) -> str:
        return ImageEmbeddingModel._model_to_setter_name(
            self.clip_model_name(), self.clip_model_checkpoint()
        )

    @classmethod
    def default_model(cls) -> str:
        return "ViT-H-14-378-quickgelu|dfn5b"

    def run_extractor(self, conn: sqlite3.Connection):
        from src.data_extractors.extraction_jobs.clip import (
            run_image_embedding_extractor_job,
        )

        return run_image_embedding_extractor_job(conn, self)

    @classmethod
    def _available_models_mapping(cls) -> Dict[str, Tuple[str, str]]:
        from src.data_extractors.ai.clip_model_list import CLIP_CHECKPOINTS

        return {
            f"{model_name}|{checkpoint}": (model_name, checkpoint)
            for model_name, checkpoint in CLIP_CHECKPOINTS
        }

    @classmethod
    def _model_to_setter_name(cls, model_name: str, checkpoint: str) -> str:
        # Reverse the available models dict
        model_to_name = {
            v: k for k, v in cls._available_models_mapping().items()
        }
        return model_to_name[(model_name, checkpoint)]

    def clip_model_name(self) -> str:
        return self._model_name

    def clip_model_checkpoint(self) -> str:
        return self._checkpoint


class TextEmbeddingModel(ModelOpts):
    _model_name: str

    def _init(self, model_name: str):

        self._model_name = TextEmbeddingModel._available_models_mapping()[
            model_name
        ]

    @classmethod
    def target_entities(cls) -> List[TargetEntityType]:
        return ["text", "tags"]  # Tags are also stored as text

    @classmethod
    def data_type(cls) -> OutputDataType:
        return "text-embedding"

    @classmethod
    def group_name(cls) -> str:
        return "sentence-transformers"

    @classmethod
    def name(cls) -> str:
        return "Text Embeddings"

    @classmethod
    def description(cls) -> str:
        return (
            "Generate Text Embeddings from extracted text "
            + "using Sentence Transformers. "
            + "Enables semantic text search. "
            + "This will generate embeddings for text already extracted "
            + "by other models such as Whisper Speech-to-Text, or OCR. "
            + "If you haven't run those models yet, you should do so first."
        )

    def setter_name(self) -> str:
        return TextEmbeddingModel._model_to_setter_name(self._model_name)

    @classmethod
    def default_model(cls) -> str:
        return "all-mpnet-base-v2"

    def run_extractor(self, conn: sqlite3.Connection):
        from src.data_extractors.extraction_jobs.text_embeddings import (
            run_text_embedding_extractor_job,
        )

        return run_text_embedding_extractor_job(conn, self)

    @classmethod
    def _available_models_mapping(cls) -> Dict[str, str]:

        SENTENCE_TRANSFORMERS = [
            "all-mpnet-base-v2",
            "all-MiniLM-L6-v2",
            "dunzhang/stella_en_400M_v5",
        ]
        return {model_name: model_name for model_name in SENTENCE_TRANSFORMERS}

    @classmethod
    def _model_to_setter_name(cls, model_name: str) -> str:
        # Reverse the available models dict
        model_to_name = {
            v: k for k, v in cls._available_models_mapping().items()
        }
        return model_to_name[model_name]

    def load_model(self):
        from src.data_extractors.ai.text_embed import TextEmbedder

        TextEmbedder(self._model_name, persistent=True)

    def unload_model(self):
        from src.data_extractors.ai.text_embed import TextEmbedder

        embedder_model = TextEmbedder(self._model_name, load_model=False)
        embedder_model.unload_model()

    def run_batch_inference(self, texts: List[str]) -> List[List[float]]:
        from src.data_extractors.ai.text_embed import TextEmbedder

        embedder = TextEmbedder(self._model_name)
        embeddings = embedder.get_text_embeddings(texts)
        return embeddings


class WhisperSTTModel(ModelOpts):
    _model_repo: str

    def _init(self, model_name: str):
        self._model_repo = WhisperSTTModel._available_models_mapping()[
            model_name
        ]

    @classmethod
    def default_batch_size(cls) -> int:
        return 1

    @classmethod
    def default_threshold(cls) -> float | None:
        return 0

    @classmethod
    def group_name(cls) -> str:
        return "whisper"

    @classmethod
    def default_model(cls) -> str:
        return "whisper|distill-large-v3"

    @classmethod
    def data_type(cls) -> OutputDataType:
        return "text"

    @classmethod
    def name(cls) -> str:
        return "Whisper Speech-to-Text"

    @classmethod
    def description(cls) -> str:
        return "Extract text from audio in audio and video files using OpenAI's Whisper model"

    def setter_name(self) -> str:
        return WhisperSTTModel._model_to_setter_name(self.model_repo())

    def run_extractor(self, conn: sqlite3.Connection):
        from src.data_extractors.extraction_jobs.whisper import (
            run_whisper_extractor_job,
        )

        return run_whisper_extractor_job(conn, self)

    @classmethod
    def _available_models_mapping(cls) -> Dict[str, str]:
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
        _MODELS = {f"whisper|{k}": v for k, v in _MODELS.items()}
        return _MODELS

    @classmethod
    def _model_to_setter_name(cls, model_repo: str) -> str:
        # Reverse the available models dict
        model_to_name = {
            v: k for k, v in cls._available_models_mapping().items()
        }
        return model_to_name[model_repo]

    def model_repo(self) -> str:
        return self._model_repo

    @classmethod
    def supported_mime_types(cls) -> List[str] | None:
        return ["audio/", "video/"]


class ModelOptsFactory:
    @classmethod
    def get_all_model_opts(cls) -> List[Type[ModelOpts]]:
        return [
            TagsModel,
            # TagsModelV2,
            OCRModel,
            WhisperSTTModel,
            ImageEmbeddingModel,
            TextEmbeddingModel,
        ]

    @classmethod
    def get_model_opts(cls, setter_name: str) -> Type[ModelOpts]:
        for model_opts in cls.get_all_model_opts():
            if model_opts.valid_model(setter_name):
                return model_opts
        raise ValueError(f"Invalid model name {setter_name}")

    @classmethod
    def get_model(cls, model_name: str) -> ModelOpts:
        model_opts = cls.get_model_opts(model_name)
        return model_opts(model_name)


def get_inference_api_client():
    from src.inference.client import InferenceAPIClient

    if url := os.getenv("INFERENCE_API_URL"):
        return InferenceAPIClient(url)
    else:
        hostname = os.getenv("HOST", "127.0.0.1")
        port = int(os.getenv("PORT", 6342))
        os.environ["INFERENCE_API_URL"] = (
            f"http://{hostname}:{port}/api/inference"
        )
        return InferenceAPIClient(os.environ["INFERENCE_API_URL"])
