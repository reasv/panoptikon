import base64
import io
from typing import Literal, Optional

import numpy as np

from panoptikon.db.utils import serialize_f32


def deserialize_array(buffer: bytes) -> np.ndarray:
    bio = io.BytesIO(buffer)
    bio.seek(0)
    return np.load(bio, allow_pickle=False)


def extract_embeddings(buffer: str) -> bytes:
    numpy_array = deserialize_array(base64.b64decode(buffer))
    assert isinstance(
        numpy_array, np.ndarray
    ), "Expected a numpy array for embeddings"
    # Check the number of dimensions
    if len(numpy_array.shape) == 1:
        # If it is a 1D array, it is a single embedding
        array_list = numpy_array.tolist()
        assert isinstance(array_list, list), "Expected a list"
        return serialize_f32(array_list)
    # If it is a 2D array, it is a list of embeddings, get the first one
    return serialize_f32(numpy_array[0].tolist())

def get_distance_func_override(
        model_name: str,
) -> Optional[Literal["L2", "COSINE"]]:
    from panoptikon.data_extractors.models import ModelOptsFactory

    model = ModelOptsFactory.get_model(model_name)
    distance_func_override = model.metadata().get("distance_func", None)
    assert distance_func_override in [None, "L2", "cosine"], f"""
    Invalid `distance_func` value for {model_name}: {distance_func_override}.
    Must be one of: null, 'L2', 'cosine'
    """  
    return distance_func_override # type: ignore