import sqlite3
from typing import List, Tuple

from src.db import get_item_id
from src.db.setters import get_setter_id
from src.db.utils import serialize_f32


def add_text_embedding(
    conn: sqlite3.Connection,
    text_id: int,
    log_id: int,
    embedding: List[float],
):
    cursor = conn.cursor()
    embedding_bytes = serialize_f32(embedding)
    cursor.execute(
        """
        INSERT INTO text_embeddings 
        (item_id, log_id, text_setter_id, text_id, setter_id, item_data_id, embedding)
        SELECT 
        text.item_id, log.id, text.setter_id, text.id, item_data.setter_id, item_data.id, ?
        FROM extracted_text AS text
        JOIN
        item_data
        ON item_data.source_id = text.item_data_id
        AND item_data.item_id = item_id
        WHERE text.id = ?
        """,
        (embedding_bytes, log_id, text_id),
    )
    assert cursor.lastrowid is not None, "Last row ID is None"
    return cursor.lastrowid


def get_text_missing_embeddings(
    conn: sqlite3.Connection,
    item: str,
    setter_name: str,
) -> List[Tuple[int, str]]:
    item_id = get_item_id(conn, item)
    setter_id = get_setter_id(conn, setter_name)
    assert item_id is not None, "Item ID is None"
    assert setter_id is not None, "Setter ID is None"
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT 
            et.id AS text_id,
            et.text
        FROM 
            extracted_text et
        LEFT JOIN 
            text_embeddings te ON et.id = te.text_id AND te.setter_id = ?
        WHERE 
            et.item_id = ?
            AND te.text_id IS NULL
        """,
        (setter_id, item_id),
    )

    results = cursor.fetchall()
    cursor.close()

    return [(row[0], row[1]) for row in results]
