import os
import sqlite3
from typing import List, Optional

from panoptikon.db.files import get_existing_file_for_sha256
from panoptikon.db.utils import serialize_f32
from panoptikon.types import FileSearchResult, OutputDataType


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
) -> List[FileSearchResult]:
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
    main_setter_ids_clause = ""
    other_setter_ids_clause = ""
    parameters = [item_id, setter_id]  # Base parameters for the query

    if src_setter_names:
        # Retrieve setter_ids from setter_names
        placeholder = ",".join(
            "?" for _ in src_setter_names
        )  # Create placeholders for IN clause
        query = f"SELECT id FROM setters WHERE name IN ({placeholder})"
        cursor = conn.execute(query, tuple(src_setter_names))
        setter_ids = [row[0] for row in cursor.fetchall()]

        if setter_ids:
            # Build the filtering clause for derived data based on setter_ids
            setter_ids_placeholder = ",".join("?" for _ in setter_ids)
            main_setter_ids_clause = f"""
            JOIN item_data AS derived_main_item_data
                ON main_item_data.source_id = derived_main_item_data.id
                AND derived_main_item_data.setter_id IN ({setter_ids_placeholder})
            """
            other_setter_ids_clause = f"""
            JOIN item_data AS derived_other_item_data
                ON other_item_data.source_id = derived_other_item_data.id
                AND derived_other_item_data.setter_id IN ({setter_ids_placeholder})
            """
            # Add setter_ids to the query parameters
            parameters.extend(setter_ids)  # For filtering `main_item_data`
            parameters.extend(setter_ids)  # For filtering `other_item_data`

    # Step 3: Choose the appropriate distance function based on the data_type
    if data_type == "clip":
        distance_function = "vec_distance_cosine(vec_normalize(other_embeddings.embedding), vec_normalize(main_embeddings.embedding))"
    else:
        distance_function = "vec_distance_L2(other_embeddings.embedding, main_embeddings.embedding)"

    query = f"""
    SELECT 
        files.path AS path,
        items.sha256 AS sha256,
        files.last_modified,
        items.type AS type
    FROM embeddings AS main_embeddings
    JOIN item_data AS main_item_data
        ON main_embeddings.id = main_item_data.id
        AND main_item_data.item_id = ?
        AND main_item_data.setter_id = ?
    {main_setter_ids_clause}
    JOIN item_data AS other_item_data
        ON other_item_data.item_id != main_item_data.item_id
        AND other_item_data.setter_id = main_item_data.setter_id
    {other_setter_ids_clause}
    JOIN embeddings AS other_embeddings
        ON other_item_data.id = other_embeddings.id
        AND other_embeddings.id != main_embeddings.id
    JOIN items ON other_item_data.item_id = items.id
    JOIN files ON files.item_id = items.id
    GROUP BY other_item_data.item_id
    ORDER BY MIN({distance_function}) ASC
    LIMIT ?;
    """

    parameters.append(limit)

    # Step 5: Execute the query
    cursor = conn.execute(query, tuple(parameters))

    # Step 6: Fetch results and create a list of FileSearchResult objects
    results = cursor.fetchall()
    file_search_results = [
        FileSearchResult(
            path=row[0], sha256=row[1], last_modified=row[2], type=row[3]
        )
        for row in results
    ]
    existing_results = []
    for result in file_search_results:
        if not os.path.exists(result.path):
            file_record = get_existing_file_for_sha256(conn, result.sha256)
            if file_record:
                result.path = file_record.path
                result.last_modified = file_record.last_modified
                existing_results.append(result)
        else:
            existing_results.append(result)

    return file_search_results
