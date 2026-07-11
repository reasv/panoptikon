"""PredictionInput-compatible input type for the worker harness.

Mirrors `inferio.inferio_types.PredictionInput` field-for-field so impl
classes (which only ever duck-type `.data` / `.file`) work unchanged,
without the harness importing the `inferio` package.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class PredictionInput:
    data: Any = None
    file: bytes | None = None


def prediction_input_from_frame(entry: Any) -> PredictionInput:
    """Build a PredictionInput from one `inputs` array entry of a predict frame.

    Unknown map keys are ignored per protocol forward-compatibility rules.
    """
    if not isinstance(entry, dict):
        raise ValueError(
            f"predict input entry must be a map, got {type(entry).__name__}"
        )
    file = entry.get("file")
    if file is not None and not isinstance(file, (bytes, bytearray)):
        raise ValueError(
            f"predict input 'file' must be bin or nil, got {type(file).__name__}"
        )
    if isinstance(file, bytearray):
        file = bytes(file)
    return PredictionInput(data=entry.get("data"), file=file)
