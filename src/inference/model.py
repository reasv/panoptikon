from abc import ABC, abstractmethod
from collections import OrderedDict, defaultdict
from datetime import datetime, timedelta
from threading import Lock
from typing import Any, Dict, List, Optional, Set

from src.inference.types import PredictionInput


class BaseModel(ABC):
    @abstractmethod
    def load(self) -> None:
        pass

    @abstractmethod
    def predict(self, inputs: List[PredictionInput]) -> List[Any]:
        pass

    @abstractmethod
    def unload(self) -> None:
        pass
