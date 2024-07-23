import sqlite3
from typing import List

from src.db import get_item_id


def create_tag_setter(conn: sqlite3.Connection, namespace, name, setter):
    cursor = conn.cursor()
    result = cursor.execute(
        """
    INSERT INTO tags_setters (namespace, name, setter)
    VALUES (?, ?, ?)
    ON CONFLICT(namespace, name, setter) DO NOTHING
    """,
        (namespace, name, setter),
    )

    tag_setter_inserted = result.rowcount > 0
    if tag_setter_inserted and cursor.lastrowid is not None:
        rowid: int = cursor.lastrowid
    else:
        rowid: int = cursor.execute(
            "SELECT id FROM tags_setters WHERE namespace = ? AND name = ? AND setter = ?",
            (namespace, name, setter),
        ).fetchone()[0]
    return rowid


def insert_tag_item(
    conn: sqlite3.Connection, item_id: int, tag_id: int, confidence=1.0
):
    # Round confidence to 3 decimal places
    confidence_float = round(float(confidence), 4)
    cursor = conn.cursor()
    cursor.execute(
        """
    INSERT INTO tags_items (item_id, tag_id, confidence)
    VALUES (?, ?, ? )
    ON CONFLICT(item_id, tag_id) DO UPDATE SET confidence=excluded.confidence
    """,
        (item_id, tag_id, confidence_float),
    )


def add_tag_to_item(
    conn: sqlite3.Connection,
    namespace: str,
    name: str,
    setter: str,
    sha256: str,
    confidence: float = 1.0,
):
    item_id = get_item_id(conn, sha256)
    assert item_id is not None, f"Item with sha256 {sha256} not found"
    tag_rowid = create_tag_setter(conn, namespace, name, setter)
    insert_tag_item(conn, item_id, tag_rowid, confidence)


def delete_tags_from_setter(conn: sqlite3.Connection, setter: str):
    cursor = conn.cursor()
    cursor.execute(
        """
    DELETE FROM tags_items
    WHERE rowid IN (
        SELECT tags_items.rowid
        FROM tags_items
        JOIN tags_setters as tags
        ON tags_items.tag_id = tags.id
        AND tags.setter = ?
    )
    """,
        (setter,),
    )

    result = cursor.execute(
        """
    DELETE FROM tags_setters
    WHERE setter = ?
    """,
        (setter,),
    )

    tags_removed = result.rowcount

    result_items = cursor.execute(
        """
    DELETE FROM extraction_log_items
    WHERE log_id IN (
        SELECT data_extraction_log.id
        FROM data_extraction_log
        WHERE setter = ?
        AND type = 'tags'
    )
    """,
        (setter,),
    )

    items_tags_removed = result_items.rowcount
    return tags_removed, items_tags_removed


def get_all_tags_for_item_name_confidence(conn: sqlite3.Connection, sha256):
    tags = get_all_tags_for_item(conn, sha256)
    return [(row[1], row[2]) for row in tags]


def get_tag_names_list(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT name FROM tags_setters")
    tag_names = cursor.fetchall()
    return [tag[0] for tag in tag_names]


def get_all_tags_for_item(conn: sqlite3.Connection, sha256):
    cursor = conn.cursor()
    cursor.execute(
        """
    SELECT tags.namespace, tags.name, tags_items.confidence, tags.setter
    FROM items
    JOIN tags_items ON items.id = tags_items.item_id
    AND items.sha256 = ?
    JOIN tags_setters as tags ON tags_items.tag_id = tags.id
    """,
        (sha256,),
    )
    tags = cursor.fetchall()
    return tags


def delete_tags_without_items(
    conn: sqlite3.Connection, batch_size: int = 10000
):
    cursor = conn.cursor()
    total_deleted = 0
    while True:
        # Perform the deletion in batches
        cursor.execute(
            """
        DELETE FROM tags_items
        WHERE rowid IN (
            SELECT tags_items.rowid
            FROM tags_items
            LEFT JOIN items ON items.id = tags_items.item_id
            WHERE items.id IS NULL
            LIMIT ?
        )
        """,
            (batch_size,),
        )

        # Check the number of rows affected in this batch
        deleted_rows = cursor.rowcount
        total_deleted += deleted_rows

        # If no rows were deleted, we are done
        if deleted_rows == 0:
            break

    return total_deleted


def get_most_common_tags(
    conn: sqlite3.Connection,
    namespace: str | None = None,
    setters: List[str] | None = [],
    confidence_threshold: float | None = None,
    limit=10,
):
    cursor = conn.cursor()
    namespace_clause = "AND tags.namespace LIKE ? || '%'" if namespace else ""
    setters_clause = (
        f"AND tags.setter IN ({','.join(['?']*len(setters))})"
        if setters
        else ""
    )
    confidence_clause = (
        f"AND tags_items.confidence >= ?" if confidence_threshold else ""
    )
    setters = setters or []
    query_args = [
        arg
        for arg in [namespace, *setters, confidence_threshold, limit]
        if arg is not None
    ]

    query = f"""
    SELECT namespace, name, COUNT(*) as count
    FROM tags_setters as tags
    JOIN tags_items ON tags.id = tags_items.tag_id
    {namespace_clause}
    {setters_clause}
    {confidence_clause}
    GROUP BY namespace, name
    ORDER BY count DESC
    LIMIT ?
    """
    cursor.execute(query, query_args)

    tags = cursor.fetchall()
    return tags


def get_most_common_tags_frequency(
    conn: sqlite3.Connection,
    namespace=None,
    setters: List[str] | None = [],
    confidence_threshold=None,
    limit=10,
):
    tags = get_most_common_tags(
        conn,
        namespace=namespace,
        setters=setters,
        confidence_threshold=confidence_threshold,
        limit=limit,
    )
    # Get the total number of item_setter pairs
    cursor = conn.cursor()
    setters_clause = (
        f"WHERE data_extraction_log.setter IN ({','.join(['?']*len(setters))})"
        if setters
        else ""
    )
    cursor.execute(
        f"""
        SELECT COUNT(
            DISTINCT extraction_log_items.item_id || '-' || data_extraction_log.setter
        ) AS distinct_count
        FROM extraction_log_items
        JOIN data_extraction_log
        ON extraction_log_items.log_id = data_extraction_log.id
        AND data_extraction_log.type = 'tags'
        {setters_clause}""",
        setters if setters else (),
    )
    total_items_setters = cursor.fetchone()[0]
    # Calculate the frequency
    tags = [
        (tag[0], tag[1], tag[2], tag[2] / (total_items_setters)) for tag in tags
    ]
    return tags
