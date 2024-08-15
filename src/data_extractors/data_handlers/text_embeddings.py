import sqlite3
from typing import List, Sequence, Tuple

import numpy as np

from src.db.text_embeddings import add_text_embedding


def handle_text_embeddings(
    conn: sqlite3.Connection,
    log_id: int,
    inputs: Sequence[Tuple[int, str]],
    embeddings: Sequence[bytes],
):
    embeddings_list: List[List[float]] = [
        np.frombuffer(embedding, dtype=np.float32).tolist()
        for embedding in embeddings
    ]
    for (text_id, _), embedding in zip(inputs, embeddings_list):
        add_text_embedding(conn, text_id, log_id, embedding)
