from __future__ import annotations

import logging
from typing import List, Sequence, Union

from inferio.impl.utils import (
    ERROR_SLOT_KEY,
    assemble_slots,
    clear_cache,
    load_image_or_slot,
    serialize_array,
)
from inferio.inferio_types import PredictionInput
from inferio.model import InferenceModel

logger = logging.getLogger(__name__)


class Qwen3VLEmbeddingModel(InferenceModel):
    """
    Qwen3-VL embedding model wrapper.

    Supports per-input embeddings for:
    - text only: {"text": "..."}
    - image only: uploaded file bytes OR {"image": "..."} / {"image_url": "..."} (paths/URLs/base64 supported by qwen-vl-utils)
    - text + image: combine both in the same input
    """

    def __init__(
        self,
        model_name_or_path: str,
        *,
        torch_dtype: str | None = None,
        attn_implementation: str | None = None,
        init_args: dict | None = None,
    ) -> None:
        self.model_name_or_path = model_name_or_path
        self.torch_dtype = torch_dtype
        self.attn_implementation = attn_implementation
        self.init_args = init_args or {}

        self._model_loaded: bool = False
        self.embedder = None

    @classmethod
    def name(cls) -> str:
        return "qwen3-vl-embedding"

    def load(self) -> None:
        if self._model_loaded:
            return
        import torch

        from inferio.impl.deps.qwen_3_vl_embedding import Qwen3VLEmbedder

        dtype = getattr(torch, self.torch_dtype) if self.torch_dtype else None
        self.embedder = Qwen3VLEmbedder(
            model_name_or_path=self.model_name_or_path,
            torch_dtype=dtype,
            attn_implementation=self.attn_implementation,
            **(self.init_args or {}),
        )
        self._model_loaded = True

    def predict(self, inputs: Sequence[PredictionInput]) -> Sequence[Union[bytes, dict, list, str]]:
        self.load()
        assert self.embedder is not None

        payloads: List[dict] = []
        kept: List[int] = []
        slots: List[tuple] = []
        for idx, input_item in enumerate(inputs):
            payload: dict = {}
            if isinstance(input_item.data, dict):
                if "text" in input_item.data:
                    payload["text"] = input_item.data["text"]
                if "image" in input_item.data:
                    payload["image"] = input_item.data["image"]
                if "image_url" in input_item.data:
                    payload["image_url"] = input_item.data["image_url"]

            if input_item.file:
                # An undecodable payload takes its own slot instead of the
                # whole batch (docs/inferio-worker-protocol.md) — but only if
                # it leaves nothing to embed. This model is multimodal: an
                # input carrying text as well as a file still has a complete,
                # embeddable payload without the image, and slotting it would
                # throw away work the model can do (and record a verdict on
                # an input the model did not actually fail).
                image, slot = load_image_or_slot(input_item.file)
                if slot is not None:
                    if not payload:
                        slots.append((idx, slot))
                        continue
                    logger.warning(
                        "Dropping an undecodable image from input %d; "
                        "embedding its text payload alone: %s",
                        idx,
                        slot[ERROR_SLOT_KEY]["message"],
                    )
                else:
                    payload["image"] = image

            if not payload:
                raise ValueError("Each input must provide at least 'text' and/or an image.")

            payloads.append(payload)
            kept.append(idx)

        results: List[bytes] = []
        if payloads:
            embeddings = self.embedder.process(payloads)
            embeddings_np = embeddings.detach().cpu().numpy()
            results = [
                serialize_array(embeddings_np[i]) for i in range(len(payloads))
            ]
        return assemble_slots(len(inputs), kept, results, slots)

    def unload(self) -> None:
        if self._model_loaded:
            try:
                del self.embedder
            finally:
                self.embedder = None
                clear_cache()
                self._model_loaded = False


IMPL_CLASS = Qwen3VLEmbeddingModel
