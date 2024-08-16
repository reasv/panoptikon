from typing import List, Sequence

from src.inference.impl.utils import clear_cache, get_device, serialize_array
from src.inference.model import InferenceModel
from src.inference.types import PredictionInput


class SentenceTransformersModel(InferenceModel):
    def __init__(
        self,
        model_name: str,
        init_args: dict = {},
        encode_args: dict = {},
    ):
        self.model_name: str = model_name
        self.init_args = init_args
        self.encode_args = encode_args
        self._model_loaded: bool = False

    @classmethod
    def name(cls) -> str:
        return "sentence_transformers"

    def load(self) -> None:
        from sentence_transformers import SentenceTransformer

        if self._model_loaded:
            return

        self.devices = get_device()
        self.model = SentenceTransformer(
            model_name_or_path=self.model_name,
            **self.init_args,
        )
        if len(self.devices) > 1:
            self.pool = self.model.start_multi_process_pool()
        else:
            self.pool = None
        self._model_loaded = True

    def predict(self, inputs: Sequence[PredictionInput]) -> List[bytes]:
        import numpy as np

        # Ensure the model is loaded
        self.load()
        input_strings: List[str] = []
        for inp in inputs:
            assert isinstance(
                inp.data, dict
            ), f"Input must be dict, got {inp.data}"
            assert (
                "text" in inp.data
            ), f"Input dict must have 'text' key, got {inp.data}"
            assert isinstance(
                inp.data["text"], str
            ), f"Input 'text' must be string, got {inp.data['text']}"
            input_strings.append(inp.data["text"])

        if self.pool:
            # Use multi-process pool for parallel inference
            embeddings = self.model.encode_multi_process(
                input_strings, pool=self.pool, **self.encode_args
            )
        else:
            embeddings = self.model.encode(input_strings, **self.encode_args)

        assert isinstance(embeddings, np.ndarray), "Embeddings not numpy array"
        # Convert embeddings to bytes
        return [serialize_array(emb) for emb in embeddings]

    def unload(self) -> None:
        if self._model_loaded:
            if self.pool:
                self.model.stop_multi_process_pool(self.pool)
            del self.model
            del self.pool
            clear_cache()
            self._model_loaded = False

    def __del__(self):
        self.unload()
