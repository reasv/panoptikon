from typing import List, Sequence, Tuple, Type, TypeVar, Union

import numpy as np

from inferio.impl.utils import (
    clear_cache,
    deserialize_array,
    get_device,
    serialize_array,
)
from inferio.model import InferenceModel
from inferio.process_model import ProcessIsolatedInferenceModel
from inferio.types import PredictionInput


class ClapModel(InferenceModel):
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
        return "clap"

    def load(self) -> None:
        if self._model_loaded:
            return
        from transformers import AutoTokenizer, ClapModel, ClapProcessor

        self.model: ClapModel = ClapModel.from_pretrained(self.model_name)  # type: ignore
        processor: ClapProcessor = ClapProcessor.from_pretrained(
            self.model_name
        )  # type: ignore
        assert not isinstance(
            processor, tuple
        ), "Expected single preprocess function"
        self.preprocess = processor
        self.devices = get_device()
        self.device = (
            self.devices[0] if isinstance(self.devices, list) else self.devices
        )
        self.model.eval().to(self.device)  # type: ignore
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self._model_loaded = True

    def predict(
        self, inputs: Sequence[PredictionInput]
    ) -> Sequence[Union[bytes, dict, list, str]]:
        import torch

        # Ensure the model is loaded
        self.load()

        text_inputs: List[Tuple[int, str]] = []
        audio_inputs: List[Tuple[int, np.ndarray]] = []
        results: List[None | bytes] = [None] * len(inputs)

        # Separate text and audio inputs, storing their original indices
        for idx, input_item in enumerate(inputs):
            if input_item.file:
                audio: np.ndarray = deserialize_array(input_item.file)
                audio_inputs.append((idx, audio))
            else:
                assert isinstance(
                    input_item.data, dict
                ), "Input must be a dictionary"
                assert "text" in input_item.data, "Input must have 'text' key"
                text_inputs.append((idx, input_item.data["text"]))

        # Use inference_mode for optimized inference
        with torch.inference_mode():
            # Process text inputs if any
            if text_inputs:
                indices, texts = zip(*text_inputs)
                tokens = self.tokenizer(
                    list(texts), padding=True, return_tensors="pt"
                ).to(self.device)

                text_features = self.model.get_text_features(
                    **tokens  # type: ignore
                )

                # Convert text features to list and store them in the results list
                for i, idx in enumerate(indices):
                    results[idx] = serialize_array(
                        text_features[i].cpu().numpy()
                    )

            # Process audio inputs if any
            if audio_inputs:
                indices, audios = zip(*audio_inputs)
                processed_audios = self.preprocess(
                    audios=audios, return_tensors="pt"
                ).to(self.device)

                audio_features = self.model.get_audio_features(
                    **processed_audios  # type: ignore
                )

                # Convert audio features to list and store them in the results list
                for i, idx in enumerate(indices):
                    results[idx] = serialize_array(
                        audio_features[i].cpu().numpy()
                    )

        output = [res for res in results if res is not None]
        assert len(output) == len(
            inputs
        ), "Mismatched output length and input length"
        return output

    def unload(self) -> None:
        if self._model_loaded:
            del self.model
            del self.tokenizer
            del self.preprocess
            clear_cache()
            self._model_loaded = False


class ClapModelIsolated(ProcessIsolatedInferenceModel):
    @classmethod
    def concrete_class(cls) -> Type[ClapModel]:  # type: ignore
        return ClapModel
