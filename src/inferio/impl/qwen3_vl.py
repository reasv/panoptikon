from __future__ import annotations

from typing import List, Sequence, Union

from inferio.impl.utils import clear_cache, load_image_from_buffer, serialize_array
from inferio.inferio_types import PredictionInput
from inferio.model import InferenceModel


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
        for input_item in inputs:
            payload: dict = {}
            if isinstance(input_item.data, dict):
                if "text" in input_item.data:
                    payload["text"] = input_item.data["text"]
                if "image" in input_item.data:
                    payload["image"] = input_item.data["image"]
                if "image_url" in input_item.data:
                    payload["image_url"] = input_item.data["image_url"]

            if input_item.file:
                payload["image"] = load_image_from_buffer(input_item.file)

            if not payload:
                raise ValueError("Each input must provide at least 'text' and/or an image.")

            payloads.append(payload)

        embeddings = self.embedder.process(payloads)
        embeddings_np = embeddings.detach().cpu().numpy()
        return [serialize_array(embeddings_np[i]) for i in range(len(payloads))]

    def unload(self) -> None:
        if self._model_loaded:
            try:
                del self.embedder
            finally:
                self.embedder = None
                clear_cache()
                self._model_loaded = False


IMPL_CLASS = Qwen3VLEmbeddingModel
