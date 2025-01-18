import base64
import io

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
