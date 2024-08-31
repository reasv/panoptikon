import logging
import os
import sqlite3
from abc import ABC, abstractmethod
from typing import Any, Dict, Generator, List, Sequence, Tuple, Type

import panoptikon.data_extractors.extraction_jobs.types as job_types
from panoptikon.db.group_settings import (
    retrieve_model_group_settings,
    save_model_group_settings,
)
from panoptikon.db.rules.types import (
    MimeFilter,
    ProcessedExtractedDataFilter,
    ProcessedItemsFilter,
    RuleItemFilters,
)
from panoptikon.db.setters import delete_setter_by_name
from panoptikon.db.tags import delete_orphan_tags
from panoptikon.types import OutputDataType, TargetEntityType

logger = logging.getLogger(__name__)


class ModelOpts(ABC):

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
    @abstractmethod
    def available_models(cls) -> List[str]:
        raise NotImplementedError

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
        delete_setter_by_name(conn, self.setter_name())
        return f"Deleted data extracted from items by model {self.setter_name()}.\n"

    @classmethod
    def supported_mime_types(cls) -> List[str] | None:
        return None

    def item_extraction_rules(self) -> RuleItemFilters:
        rules = []
        target_entities = self.target_entities()
        if "items" in target_entities:
            rules.append(ProcessedItemsFilter(setter_name=self.setter_name()))
        else:
            rules.append(
                ProcessedExtractedDataFilter(
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
    @abstractmethod
    def data_type(cls) -> OutputDataType:
        raise NotImplementedError

    @abstractmethod
    def run_extractor(self, conn: sqlite3.Connection) -> Generator[
        job_types.ExtractorJobProgress
        | job_types.ExtractorJobReport
        | job_types.ExtractorJobStart,
        Any,
        None,
    ]:
        raise NotImplementedError

    @abstractmethod
    def setter_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def _init(self, model_name: str):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def name(cls) -> str:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def description(cls) -> str:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def group_name(cls) -> str:
        raise NotImplementedError

    @abstractmethod
    def run_batch_inference(
        self,
        cache_key: str,
        lru_size: int,
        ttl_seconds: int,
        inputs: Sequence[Tuple[str | dict | None, bytes | None]],
    ):
        raise NotImplementedError

    @abstractmethod
    def load_model(self, cache_key: str, lru_size: int, ttl_seconds: int):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def model_metadata(cls, model_name) -> Sequence[str]:
        raise NotImplementedError


class ModelGroup(ModelOpts):
    _group: str

    def _init(self, model_name: str):
        self._inference_id = model_name

    @classmethod
    def _meta(cls):
        return ModelOptsFactory.get_group_metadata(cls._group)

    def _id_meta(self):
        return ModelOptsFactory.get_inference_id_metadata(
            self._group, self._inference_id
        )

    @classmethod
    def _models(cls):
        return ModelOptsFactory.get_group_models(cls._group)

    @classmethod
    def model_metadata(cls, model_name) -> Sequence[str]:
        meta = ModelOptsFactory.get_inference_id_metadata(
            cls._group, model_name
        )
        return meta.get("description", ""), meta.get("link", {})

    @classmethod
    def target_entities(cls) -> List[TargetEntityType]:
        return cls._meta().get("target_entities", ["items"])

    @classmethod
    def available_models(cls) -> List[str]:
        return list(cls._models().keys())

    @classmethod
    def default_batch_size(cls) -> int:
        return cls._meta().get("default_batch_size", 64)

    @classmethod
    def default_threshold(cls) -> float | None:
        return cls._meta().get("default_threshold")

    def input_spec(self) -> Tuple[str, dict]:
        spec = self._id_meta().get("input_spec", None)
        assert (
            spec is not None
        ), f"Input spec not found for {self.setter_name()}"
        handler_name = spec.get("handler", None)
        assert (
            handler_name is not None
        ), f"Input handler not found for {self.setter_name()}"
        opts = spec.get("opts", {})
        return handler_name, opts

    @classmethod
    def default_model(cls) -> str:
        return cls._meta().get(
            "default_inference_id", cls.available_models()[0]
        )

    @classmethod
    def supported_mime_types(cls) -> List[str] | None:
        return cls._meta().get("input_mime_types")

    @classmethod
    def data_type(cls) -> OutputDataType:
        return cls._meta().get("output_type", "text")

    def setter_name(self) -> str:
        return self._group + "/" + self._inference_id

    @classmethod
    def name(cls) -> str:
        return cls._meta().get("name", cls._group)

    @classmethod
    def description(cls) -> str:
        return cls._meta().get("description", f"Run {cls._group} extractor")

    @classmethod
    def group_name(cls) -> str:
        return cls._group

    def load_model(self, cache_key: str, lru_size: int, ttl_seconds: int):
        get_inference_api_client().load_model(
            self.setter_name(), cache_key, lru_size, ttl_seconds
        )

    def unload_model(self, cache_key: str):
        get_inference_api_client().unload_model(self.setter_name(), cache_key)

    def delete_extracted_data(self, conn: sqlite3.Connection):
        msg = super().delete_extracted_data(conn)
        if self.data_type() == "tags":
            orphans_deleted = delete_orphan_tags(conn)
            msg += f"\nDeleted {orphans_deleted} orphaned tags.\n"
        return msg

    def run_extractor(self, conn: sqlite3.Connection):
        from panoptikon.data_extractors.extraction_jobs.dynamic_job import (
            run_dynamic_extraction_job,
        )

        return run_dynamic_extraction_job(conn, self)

    def run_batch_inference(
        self,
        cache_key: str,
        lru_size: int,
        ttl_seconds: int,
        inputs: Sequence[Tuple[str | dict | None, bytes | None]],
    ):
        result = get_inference_api_client().predict(
            self.setter_name(), cache_key, lru_size, ttl_seconds, inputs
        )
        return result


class ModelOptsFactory:
    _group_metadata = {}
    _api_models: Dict[str, Type["ModelGroup"]] = {}

    @classmethod
    def get_all_model_opts(cls) -> List[Type[ModelOpts]]:
        api_modelopts = []
        try:
            cls.refetch_metadata()
            api_modelopts = cls.get_api_model_opts()
        except Exception as e:
            logger.error(f"Failed to load API model opts: {e}", exc_info=True)
        return [
            # TagsModel,
            # OCRModel,
            # WhisperSTTModel,
            # ImageEmbeddingModel,
            # TextEmbeddingModel,
        ] + api_modelopts

    @classmethod
    def get_api_model_opts(cls) -> List[Type[ModelOpts]]:
        for group_name, _ in cls.get_metadata().items():
            if group_name in cls._api_models:
                continue
            cls._api_models[group_name] = type(
                f"Group_{group_name}",
                (ModelGroup,),
                {"_group": group_name},
            )
        return list(cls._api_models.values())

    @classmethod
    def get_model_opts(cls, setter_name: str) -> Type[ModelOpts]:
        for model_opts in cls.get_all_model_opts():
            if model_opts.valid_model(setter_name):
                return model_opts
        raise ValueError(f"Invalid model name {setter_name}")

    @classmethod
    def get_model(cls, setter_name: str) -> ModelOpts:
        s = setter_name.split("/", 1)
        if len(s) == 2:
            group_name, inference_id = s
        else:
            group_name, inference_id = None, None
        if group_name in cls._api_models:
            return cls._api_models[group_name](model_name=inference_id)
        model_opts = cls.get_model_opts(setter_name)
        return model_opts(setter_name)

    @classmethod
    def get_metadata(cls) -> Dict[str, Any]:
        if not cls._group_metadata:
            cls._group_metadata = get_inference_api_client().get_metadata()
        return cls._group_metadata

    @classmethod
    def get_group_metadata(cls, group_name) -> Dict[str, Any]:
        return cls.get_metadata()[group_name]["group_metadata"]

    @classmethod
    def get_inference_id_metadata(
        cls, group_name, inference_id
    ) -> Dict[str, Any]:
        group_metadata = cls.get_group_metadata(group_name)
        item_meta: Dict[str, Any] = cls.get_metadata()[group_name][
            "inference_ids"
        ][inference_id]
        return {
            **group_metadata,
            **item_meta,
        }

    @classmethod
    def get_group_models(cls, group_name) -> Dict[str, Any]:
        return cls.get_metadata()[group_name]["inference_ids"]

    @classmethod
    def refetch_metadata(cls):
        cls._group_metadata = get_inference_api_client().get_metadata()


def get_inference_api_client():
    from inferio.client import InferenceAPIClient

    if not os.getenv("INFERENCE_API_URL"):
        hostname = os.getenv("HOST", "127.0.0.1")
        port = int(os.getenv("PORT", 6342))
        os.environ["INFERENCE_API_URL"] = f"http://{hostname}:{port}"
    return InferenceAPIClient(
        f"{os.environ['INFERENCE_API_URL']}/api/inference"
    )
