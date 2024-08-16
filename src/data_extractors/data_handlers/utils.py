import io

import numpy as np


def deserialize_array(buffer: bytes) -> np.ndarray:
    bio = io.BytesIO(buffer)
    bio.seek(0)
    return np.load(bio, allow_pickle=False)
