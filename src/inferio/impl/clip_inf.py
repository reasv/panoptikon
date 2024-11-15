import logging
import os
from calendar import c
from typing import List, Sequence, Tuple, Union

from PIL import ImageFile

from inferio.impl.utils import clear_cache, serialize_array
from inferio.model import InferenceModel
from inferio.types import PredictionInput

logger = logging.getLogger(__name__)

import asyncio

ImageFile.LOAD_TRUNCATED_IMAGES = True


class InfinityCLIP(InferenceModel):
    def __init__(
        self,
        model_name: str,
        init_args: dict = {},
    ):
        self.model_name: str = model_name
        self.init_args = init_args
        self._model_loaded: bool = False

    @classmethod
    def name(cls) -> str:
        return "clip_infinity"

    def load(self) -> None:
        if self._model_loaded:
            return
        from infinity_emb import (
            AsyncEmbeddingEngine,
            AsyncEngineArray,
            EngineArgs,
        )
        from infinity_emb.log_handler import logger
        from infinity_emb.primitives import Dtype, InferenceEngine

        logger.setLevel(5)  # Debug
        from panoptikon.log import setup_logging

        setup_logging()
        data_dir = os.getenv("DATA_FOLDER", "data")
        cache_dir = os.path.join(data_dir, "cache")
        infinity_cache_dir = os.path.join(cache_dir, "emb")
        os.makedirs(infinity_cache_dir, exist_ok=True)
        array = AsyncEngineArray.from_args(
            [
                EngineArgs(
                    model_name_or_path=self.model_name,
                    dtype=Dtype.float32,  # type: ignore
                    engine=InferenceEngine.torch,
                    lengths_via_tokenize=True,
                    vector_disk_cache_path=infinity_cache_dir,
                    **self.init_args,
                )
            ]
        )
        self.engine: AsyncEmbeddingEngine = array[self.model_name]
        self._model_loaded = True

    def predict(
        self, inputs: Sequence[PredictionInput]
    ) -> Sequence[Union[bytes, dict, list, str]]:
        # Ensure the model is loaded
        self.load()
        text_inputs: List[Tuple[int, str]] = []
        image_inputs: List[Tuple[int, bytes]] = []
        audio_inputs: List[Tuple[int, bytes]] = []

        # Separate text and image inputs, storing their original indices
        for idx, input_item in enumerate(inputs):
            if input_item.file:
                if isinstance(input_item.data, dict):
                    if "type" in input_item.data:
                        if input_item.data["type"] == "audio":
                            audio_inputs.append((idx, input_item.file))
                            continue
                image_inputs.append((idx, input_item.file))
            else:
                assert isinstance(
                    input_item.data, dict
                ), "Input must be a dictionary"
                assert "text" in input_item.data, "Input must have 'text' key"
                text_inputs.append((idx, input_item.data["text"]))

        results = asyncio.run(
            self.run_inference(
                text_inputs,
                image_inputs,
                audio_inputs,
                len(inputs),
            )
        )

        output = [res for res in results if res is not None]
        assert len(output) == len(
            inputs
        ), "Mismatched output length and input length"
        return output

    async def run_inference(
        self,
        text_inputs: List[Tuple[int, str]],
        image_inputs: List[Tuple[int, bytes]],
        audio_inputs: List[Tuple[int, bytes]],
        total_length: int,
    ) -> List[bytes | None]:
        await self.engine.astart()
        results: List[None | bytes] = [None] * total_length
        # Process text inputs if any
        if text_inputs:
            indices, texts = zip(*text_inputs)

            text_features, n = await self.engine.embed(sentences=texts)
            # Convert text features to list and store them in the results list
            for i, idx in enumerate(indices):
                results[idx] = serialize_array(text_features[i])

        # Process image inputs if any
        if image_inputs:
            indices, images = zip(*image_inputs)

            image_features, n = await self.engine.image_embed(images=images)

            # Convert image features to list and store them in the results list
            for i, idx in enumerate(indices):
                results[idx] = serialize_array(image_features[i])

        # Process audio inputs if any
        if audio_inputs:
            indices, audios = zip(*audio_inputs)

            audio_features, n = await self.engine.audio_embed(audios=audios)

            # Convert audio features to list and store them in the results list
            for i, idx in enumerate(indices):
                results[idx] = serialize_array(audio_features[i])
        await self.engine.astop()
        return results

    def unload(self) -> None:
        if self._model_loaded:
            del self.engine
            clear_cache()
            self._model_loaded = False
