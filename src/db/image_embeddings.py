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
        INSERT INTO image_embeddings (item_id, log_id, setter_id, embedding)
        SELECT ?, ?, logs.setter_id, ?
        FROM data_extraction_log AS logs
        WHERE logs.id = ?
    """
    embedding_bytes = serialize_f32(embedding)
    cursor = conn.cursor()
    cursor.execute(sql, (item_id, log_id, embedding_bytes, log_id))
    assert cursor.lastrowid is not None, "Last row ID is None"
    return cursor.lastrowid
