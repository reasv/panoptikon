import sqlite3
from typing import List

from src.db.utils import serialize_f32


def add_text_embedding(
    conn: sqlite3.Connection,
    text_id: int,
    setter_id: int,
    embedding: List[float],
):
    cursor = conn.cursor()
    embedding_bytes = serialize_f32(embedding)
    cursor.execute(
        """
        INSERT INTO text_embeddings 
        (item_id, log_id, text_setter_id, text_id, setter_id, embedding)
        SELECT text.item_id, text.log_id, text.setter_id, text.id, ?, ?
        FROM extracted_text AS text
        WHERE text.id = ?
        """,
        (setter_id, embedding_bytes, text_id),
    )
    assert cursor.lastrowid is not None, "Last row ID is None"
    return cursor.lastrowid
