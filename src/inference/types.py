from dataclasses import dataclass


@dataclass
class PredictionInput:
    data: dict | str | None
    file: bytes | None
