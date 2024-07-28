from typing import List

from sentence_transformers import SentenceTransformer


class TextEmbedderSingleton:
    _instances = {}
    _reference_counts = {}

    @classmethod
    def get_instance(cls, model_name: str):
        if model_name not in cls._instances:
            model = SentenceTransformer(model_name)
            cls._instances[model_name] = model
            cls._reference_counts[model_name] = 0
        cls._reference_counts[model_name] += 1
        return cls._instances[model_name]

    @classmethod
    def release_instance(cls, model_name: str):
        if model_name in cls._reference_counts:
            cls._reference_counts[model_name] -= 1
            if cls._reference_counts[model_name] == 0:
                del cls._instances[model_name]
                del cls._reference_counts[model_name]
                return True
        return False


class TextEmbedder:
    _model: SentenceTransformer | None

    def __init__(
        self,
        model_name: str = "all-mpnet-base-v2",
        persistent: bool = False,
        load_model: bool = True,
    ):
        self._model_name = model_name
        self._model = None
        self._model_loaded = False
        self._persistent = persistent
        if load_model:
            self._load_model()

    def _load_model(self):
        if not self._model_loaded:
            self._model = TextEmbedderSingleton.get_instance(self._model_name)
            self._model_loaded = True

    def get_text_embeddings(self, texts: List[str]) -> List[List[float]]:
        self._load_model()
        embeddings = self._model.encode(texts)
        return embeddings.tolist()  # Convert numpy array to list of lists

    def model_name(self) -> str:
        return self._model_name

    def model_type(self) -> str:
        return "text-embedding"

    def unload_model(self):
        if self._model_loaded:
            TextEmbedderSingleton.release_instance(self._model_name)
            self._model = None
            self._model_loaded = False

    def __del__(self):
        if not self._persistent:
            self.unload_model()
