import sqlite3
from typing import Sequence

import numpy as np

from panoptikon.data_extractors.data_handlers.utils import deserialize_array
from panoptikon.db.embeddings import add_embedding
from panoptikon.db.extraction_log import add_item_data
from panoptikon.types import ItemData


def handle_text_embeddings(
    conn: sqlite3.Connection,
    job_id: int,
    setter_name: str,
    item: ItemData,
    embeddings: Sequence[bytes],
):
    data_ids = []
    assert len(item.item_data_ids) == len(embeddings), "Mismatch in data ids"
    for text_id, embedding_set in zip(item.item_data_ids, embeddings):
        text_embeddings = deserialize_array(embedding_set)

        # Check if the embedding is a single-dimensional array and reshape if necessary
        if text_embeddings.ndim == 1:
            text_embeddings = text_embeddings.reshape(1, -1)

        # Ensure that the array is now two-dimensional (i.e., a list of embeddings)
        assert (
            text_embeddings.ndim == 2
        ), "Embeddings are not a list of embeddings"

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
                src_data_id=text_id,
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
