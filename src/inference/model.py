from abc import ABC, abstractmethod
from typing import List, Sequence

from src.inference.types import PredictionInput


class BaseModel(ABC):
    @abstractmethod
    def load(self) -> None:
        pass

    @abstractmethod
    def predict(
        self, inputs: Sequence[PredictionInput]
    ) -> List[bytes | dict | list | str]:
        pass

    @abstractmethod
    def unload(self) -> None:
        pass
