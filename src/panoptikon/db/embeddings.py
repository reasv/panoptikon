import logging
import sqlite3
from typing import List

from panoptikon.db.utils import serialize_f32
from panoptikon.types import OutputDataType

logger = logging.getLogger(__name__)


def add_embedding(
    conn: sqlite3.Connection,
    data_id: int,
    data_type: OutputDataType,
    embedding: List[float],
) -> int:
    """
    Insert image embedding into the database
    """
    embedding_bytes = serialize_f32(embedding)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO embeddings
            (id, embedding)
        SELECT item_data.id, ?
        FROM item_data
        WHERE item_data.id = ?
        AND item_data.data_type = ?
    """,
        (embedding_bytes, data_id, data_type),
    )

    assert cursor.lastrowid is not None, "Last row ID is None"
    return cursor.lastrowid
