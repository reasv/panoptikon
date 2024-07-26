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

    cursor = conn.cursor()

    sql = """
    INSERT INTO image_embeddings_meta (item_id, log_id)
    VALUES (?, ?)
    """
    cursor.execute(sql, (item_id, log_id))
    assert cursor.lastrowid is not None, "Last row ID is None"
    metadata_id = cursor.lastrowid

    embedding_bytes = serialize_f32(embedding)
    cursor.execute(
        "INSERT INTO image_embeddings (id, embedding) VALUES (?, ?)",
        (metadata_id, embedding_bytes),
    )
    return metadata_id


def delete_embedding_generated_by_setter(
    conn: sqlite3.Connection, model_type: str, setter: str
):
    cursor = conn.cursor()
    cursor.execute(
        """
        DELETE FROM image_embeddings_meta
        WHERE log_id IN (
            SELECT data_extraction_log.id
            FROM data_extraction_log
            WHERE setter = ?
            AND type = ?
        )
    """,
        (model_type, setter),
    )
