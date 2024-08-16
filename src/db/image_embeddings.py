import sqlite3
from typing import List

from src.db import get_item_id
from src.db.utils import serialize_f32


def insert_image_embedding(
    conn: sqlite3.Connection,
    item_sha256: str,
    log_id: int,
    embedding: List[float],
    source_extraction_id: int | None = None,
) -> int:
    """
    Insert image embedding into the database
    """
    item_id = get_item_id(conn, item_sha256)
    assert item_id is not None, f"Item with SHA256 {item_sha256} not found"
    src_cond = (
        "AND extractions.source_extraction_id = ?"
        if source_extraction_id is not None
        else "AND extractions.is_origin = 1"
    )
    src_params = (
        (source_extraction_id,) if source_extraction_id is not None else ()
    )
    sql = f"""
        INSERT INTO image_embeddings
        (item_id, log_id, setter_id, extraction_id, embedding)
        SELECT ?, ?, logs.setter_id, extractions.id, ?
        FROM data_extraction_log AS logs
        WHERE logs.id = ?
        JOIN items_extractions AS extractions
        ON extractions.log_id = logs.id
        AND extractions.item_id = ?
        {src_cond}
    """
    embedding_bytes = serialize_f32(embedding)
    cursor = conn.cursor()
    cursor.execute(
        sql, (item_id, log_id, embedding_bytes, log_id, item_id, *src_params)
    )
    assert cursor.lastrowid is not None, "Last row ID is None"
    return cursor.lastrowid
