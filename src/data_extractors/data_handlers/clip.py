import sqlite3
from typing import Sequence

import numpy as np

from src.db.image_embeddings import insert_image_embedding
from src.types import ItemWithPath


def handle_clip(
    conn: sqlite3.Connection,
    log_id: int,
    item: ItemWithPath,
    embeddings: Sequence[bytes],
):
    embeddings_list = [
        np.frombuffer(embedding, dtype=np.float32).tolist()
        for embedding in embeddings
    ]
    for embedding in embeddings_list:
        insert_image_embedding(conn, item.sha256, log_id, embedding)
