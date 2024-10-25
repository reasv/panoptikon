import sqlite3
from typing import Sequence

import numpy as np

from panoptikon.data_extractors.data_handlers.utils import deserialize_array
from panoptikon.data_extractors.types import JobInputData
from panoptikon.db.embeddings import add_embedding
from panoptikon.db.extraction_log import add_item_data


def handle_text_embeddings(
    conn: sqlite3.Connection,
    job_id: int,
    setter_name: str,
    item: JobInputData,
    embeddings: Sequence[bytes],
):
    data_ids = []
    assert len(embeddings) == 1, "Mismatch in data ids"
    assert isinstance(embeddings[0], bytes), "Embedding is not a byte string"
    assert item.data_id is not None, "Item data id is not set"
    assert item.text is not None, "Item text is not set"
    text_embeddings = deserialize_array(embeddings[0])

    # Check if the embedding is a single-dimensional array and reshape if necessary
    if text_embeddings.ndim == 1:
        text_embeddings = text_embeddings.reshape(1, -1)

    # Ensure that the array is now two-dimensional (i.e., a list of embeddings)
    assert text_embeddings.ndim == 2, "Embeddings are not a list of embeddings"

    # Iterate over each row of the numpy array (each row is an embedding)
    for idx, embedding in enumerate(text_embeddings):
        assert isinstance(
            embedding, np.ndarray
        ), "Embedding is not a numpy array"

        data_id = add_item_data(
            conn,
            item=item.sha256,
            setter_name=setter_name,
            job_id=job_id,
            data_type="text-embedding",
            src_data_id=item.data_id,
            index=idx,
        )
        add_embedding(conn, data_id, "text-embedding", embedding.tolist())
        data_ids.append(data_id)

    if not data_ids:
        add_item_data(
            conn,
            item=item.sha256,
            setter_name=setter_name,
            job_id=job_id,
            data_type="text-embedding",
            index=0,
            is_placeholder=True,
        )

    return data_ids
