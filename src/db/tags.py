import sqlite3

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


def get_all_tag_namespaces(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute(
        """
    SELECT DISTINCT namespace
    FROM tags_setters
    """
    )
    namespaces = [namespace[0] for namespace in cursor.fetchall()]
    namespace_prefixes = set(
        [namespace.split(":")[0] for namespace in namespaces]
    )
    namespaces += list(namespace_prefixes)
    namespaces.sort()
    return namespaces
