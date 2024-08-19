from abc import ABC, abstractmethod
from typing import List, Sequence

from panoptikon.inferio.types import PredictionInput


class InferenceModel(ABC):
    @classmethod
    @abstractmethod
    def name(cls) -> str:
        pass

    @abstractmethod
    def load(self) -> None:
        pass

    @abstractmethod
    def predict(
        self, inputs: Sequence[PredictionInput]
    ) -> Sequence[bytes | dict | list | str]:
        pass

    @abstractmethod
    def unload(self) -> None:
        pass
