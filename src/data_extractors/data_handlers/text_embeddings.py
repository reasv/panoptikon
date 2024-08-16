import sqlite3
from typing import List, Sequence, Tuple

import numpy as np

from src.data_extractors.data_handlers.utils import deserialize_array
from src.db.text_embeddings import add_text_embedding


def handle_text_embeddings(
    conn: sqlite3.Connection,
    log_id: int,
    input_ids: Sequence[int],
    embeddings: Sequence[bytes],
):
    embeddings_list: List[List[float]] = [
        deserialize_array(embedding).tolist() for embedding in embeddings
    ]
    for text_id, embedding in zip(input_ids, embeddings_list):
        add_text_embedding(conn, text_id, log_id, embedding)
