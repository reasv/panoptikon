import sqlite3
from typing import List, Optional

from panoptikon.db.utils import serialize_f32
from panoptikon.types import OutputDataType


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


def find_similar_items(
    conn: sqlite3.Connection,
    sha256: str,
    setter_name: str,
    src_setter_names: Optional[List[str]] = None,
    limit: int = 10,
) -> List[int]:
    # Step 1: Retrieve item_id, setter_id, and data_type from the provided sha256 and setter_name
    query = """
    SELECT 
        items.id AS item_id,
        setters.id AS setter_id,
        item_data.data_type AS data_type
    FROM items
    JOIN item_data ON items.id = item_data.item_id
    JOIN setters ON item_data.setter_id = setters.id
    WHERE items.sha256 = ? AND setters.name = ?
    LIMIT 1;
    """
    cursor = conn.execute(query, (sha256, setter_name))
    result = cursor.fetchone()

    if not result:
        return []  # No item or setter found, return empty list

    item_id, setter_id, data_type = result

    # Step 2: If setter_names is provided, retrieve the corresponding setter_ids
    setter_ids = None
    if src_setter_names:
        placeholder = ",".join(
            "?" for _ in src_setter_names
        )  # Create placeholders for IN clause
        query = f"SELECT id FROM setters WHERE name IN ({placeholder})"
        cursor = conn.execute(query, tuple(src_setter_names))
        setter_ids = [row[0] for row in cursor.fetchall()]

    # Step 2: Choose the appropriate distance function based on the data_type
    if data_type == "clip":
        distance_function = "vec_distance_cosine(vec_normalize(other_embeddings.embedding), vec_normalize(main_embeddings.embedding))"
    else:
        distance_function = "vec_distance_L2(other_embeddings.embedding, main_embeddings.embedding)"

    # Step 2: Find the top N most similar items by comparing all embeddings in one query
    query = f"""
    SELECT 
        other_item_data.item_id AS similar_item_id,
        MIN({distance_function}) AS min_distance
    FROM embeddings AS main_embeddings
    JOIN item_data AS main_item_data
        ON main_embeddings.id = main_item_data.id
        AND main_item_data.item_id = ?
        AND main_item_data.setter_id = ?
    JOIN item_data AS other_item_data
        ON other_item_data.item_id != ?
        AND other_item_data.setter_id = ?
    JOIN embeddings AS other_embeddings
        ON other_item_data.id = other_embeddings.id
        AND other_embeddings.id != main_embeddings.id
    GROUP BY other_item_data.item_id
    ORDER BY min_distance ASC
    LIMIT ?;
    """

    cursor = conn.execute(
        query, (item_id, setter_id, item_id, setter_id, limit)
    )

    similar_items = cursor.fetchall()

    # Extract and return the item_ids
    return [item_id for item_id, _ in similar_items]
