from __future__ import annotations

import logging
from typing import List, Sequence, Union

from inferio.impl.utils import (
    clear_cache,
    get_device,
    load_image_from_buffer,
    run_with_oom_retry,
    select_dtype,
    serialize_array,
)
from inferio.inferio_types import PredictionInput
from inferio.model import InferenceModel

logger = logging.getLogger(__name__)


class NemotronEmbedVLModel(InferenceModel):
    """NVIDIA llama-nemotron-embed-vl unified multimodal embedder.

    Registered as an image-embedding (clip-group) model only: the model is
    an asymmetric retriever, so every bare text input is embedded as a
    retrieval *query* and every image as a *passage*. The `query:` /
    `passage:` prefixes it was trained with are applied by the model's own
    processor inside encode_queries/encode_documents. Do not reuse this id
    in a text-indexing group: indexed text would be embedded query-side.
    """

    def __init__(
        self,
        model_name_or_path: str,
        *,
        precision: str | None = None,
        attn_implementation: str | None = None,
        max_input_tiles: int = 6,
        use_thumbnails: bool = True,
        passage_max_length: int = 2048,
        init_args: dict | None = None,
    ):
        self.model_name_or_path = model_name_or_path
        self.precision = precision
        self.attn_implementation = attn_implementation
        self.max_input_tiles = max_input_tiles
        self.use_thumbnails = use_thumbnails
        # Documents are always images here; the model card recommends 2048
        # for image-only inputs (10240 is the interleaved image+text bound).
        self.passage_max_length = passage_max_length
        self.init_args = init_args or {}
        self._model_loaded: bool = False

    @classmethod
    def name(cls) -> str:
        return "nemotron-embed-vl"

    def load(self) -> None:
        if self._model_loaded:
            return
        from transformers import AutoModel

        self.device = get_device()[0]
        self.dtype = select_dtype(
            self.device, "bf16", explicit=self.precision, logger=logger
        )

        kwargs: dict = dict(self.init_args)
        if self.attn_implementation:
            kwargs["attn_implementation"] = self.attn_implementation
        self.model = (
            AutoModel.from_pretrained(
                self.model_name_or_path,
                torch_dtype=self.dtype,
                trust_remote_code=True,
                **kwargs,
            )
            .eval()
            .to(self.device)
        )
        self.model.processor.p_max_length = self.passage_max_length
        self.model.processor.max_input_tiles = self.max_input_tiles
        self.model.processor.use_thumbnail = self.use_thumbnails
        self._model_loaded = True

    def predict(
        self, inputs: Sequence[PredictionInput]
    ) -> Sequence[Union[bytes, dict, list, str]]:
        import torch

        self.load()

        text_inputs = []
        image_inputs = []
        results: List[None | bytes] = [None] * len(inputs)

        for idx, input_item in enumerate(inputs):
            if input_item.file:
                image_inputs.append(
                    (idx, load_image_from_buffer(input_item.file))
                )
            else:
                assert isinstance(
                    input_item.data, dict
                ), "Input must be a dictionary"
                assert "text" in input_item.data, "Input must have 'text' key"
                text_inputs.append((idx, input_item.data["text"]))

        def serialize_batch(features: "torch.Tensor") -> list[bytes]:
            # encode_queries/encode_documents return unnormalized vectors;
            # normalize so the stored space matches cosine/L2 search.
            features = torch.nn.functional.normalize(features, dim=-1)
            arr = features.float().cpu().numpy()
            return [serialize_array(arr[i]) for i in range(arr.shape[0])]

        def encode_query_chunk(chunk):
            return serialize_batch(self.model.encode_queries(list(chunk)))

        def encode_image_chunk(chunk):
            return serialize_batch(
                self.model.encode_documents(images=list(chunk))
            )

        with torch.inference_mode():
            if text_inputs:
                indices, texts = zip(*text_inputs)
                for idx, res in zip(
                    indices,
                    run_with_oom_retry(
                        encode_query_chunk, list(texts), logger=logger
                    ),
                ):
                    results[idx] = res

            if image_inputs:
                indices, images = zip(*image_inputs)
                for idx, res in zip(
                    indices,
                    run_with_oom_retry(
                        encode_image_chunk, list(images), logger=logger
                    ),
                ):
                    results[idx] = res

        output = [res for res in results if res is not None]
        assert len(output) == len(
            inputs
        ), "Mismatched output length and input length"
        return output

    def unload(self) -> None:
        if self._model_loaded:
            del self.model
            clear_cache()
            self._model_loaded = False


IMPL_CLASS = NemotronEmbedVLModel
