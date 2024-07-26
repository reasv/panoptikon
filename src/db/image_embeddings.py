import sqlite3
from typing import List

from src.db import get_item_id
from src.db.utils import serialize_f32


def insert_image_embedding(
    conn: sqlite3.Connection,
    item_sha256: str,
    log_id: int,
    embedding: List[float],
) -> int:
    """
    Insert image embedding into the database
    """
    item_id = get_item_id(conn, item_sha256)
    assert item_id is not None, f"Item with SHA256 {item_sha256} not found"

    sql = """
        INSERT INTO image_embeddings (item_id, log_id, embedding)
        VALUES (?, ?, ?)
    """
    embedding_bytes = serialize_f32(embedding)
    cursor = conn.cursor()
    cursor.execute(sql, (item_id, log_id, embedding_bytes))
    assert cursor.lastrowid is not None, "Last row ID is None"
    return cursor.lastrowid


def delete_embeddings_generated_by_setter(
    conn: sqlite3.Connection, model_type: str, setter: str
):
    cursor = conn.cursor()
    cursor.execute(
        """
        DELETE FROM image_embeddings
        WHERE log_id IN (
            SELECT data_extraction_log.id
            FROM data_extraction_log
            WHERE setter = ?
            AND type = ?
        )
    """,
        (model_type, setter),
    )
