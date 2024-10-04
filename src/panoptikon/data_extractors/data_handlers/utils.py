import io

import numpy as np


def deserialize_array(buffer: bytes) -> np.ndarray:
    bio = io.BytesIO(buffer)
    bio.seek(0)
    return np.load(bio, allow_pickle=False)


def serialize_array(array: np.ndarray) -> bytes:
    buffer = io.BytesIO()
    np.save(buffer, array)
    buffer.seek(0)
    return buffer.read()


from dataclasses import fields, is_dataclass
from typing import (
    Any,
    Dict,
    Optional,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

T = TypeVar("T")


def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
    # if not is_dataclass(cls):
    #     raise TypeError(f"{cls} is not a dataclass.")

    field_types = {f.name: f.type for f in fields(cls)}  # type: ignore
    init_kwargs = {}
    for field_name, field_type in field_types.items():

        field_value = data.get(field_name, None)

        # Check if the field type is Optional
        origin_type = get_origin(field_type)
        if origin_type is Union:
            # Extract types in the Union
            union_args = get_args(field_type)
            # Handle the case where the Union includes None
            non_none_types = [
                arg for arg in union_args if arg is not type(None)
            ]
            # If there's only one non-None type, use it
            if len(non_none_types) == 1:
                field_type = non_none_types[0]
            else:
                # If multiple non-None types exist, decide how to handle
                # For simplicity, we use the first one here, but more complex logic may be required
                field_type = non_none_types[0]

        if field_value is not None:
            if is_dataclass(field_type):
                # Recursively convert for nested dataclasses
                init_kwargs[field_name] = from_dict(field_type, field_value)
            else:
                # Directly assign the value
                init_kwargs[field_name] = field_value
        # Else: field_value is None, leave out if not needed or keep as None

    return cls(**init_kwargs)
