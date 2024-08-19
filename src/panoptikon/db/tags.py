import sqlite3

from panoptikon.db import get_item_id
from panoptikon.db.setters import upsert_setter


def upsert_tag(
    conn: sqlite3.Connection,
    namespace: str,
    name: str,
):
    cursor = conn.cursor()
    result = cursor.execute(
        """
    INSERT INTO tags (namespace, name)
    VALUES (?, ?)
    ON CONFLICT(namespace, name) DO NOTHING
    """,
        (namespace, name),
    )

    tag_setter_inserted = result.rowcount > 0
    if tag_setter_inserted and cursor.lastrowid is not None:
        rowid: int = cursor.lastrowid
    else:
        rowid: int = cursor.execute(
            "SELECT id FROM tags WHERE namespace = ? AND name = ?",
            (namespace, name),
        ).fetchone()[0]
    return rowid


def insert_tag_item(
    conn: sqlite3.Connection,
    data_id: int,
    tag_id: int,
    confidence=1.0,
):
    # Round confidence to 4 decimal places
    confidence_float = round(float(confidence), 4)
    cursor = conn.cursor()
    cursor.execute(
        f"""
        INSERT INTO tags_items
        (item_data_id, tag_id, confidence)
        SELECT item_data.id, ?, ?
        FROM item_data
        WHERE item_data.id = ?
        AND item_data.data_type = 'tags'
        """,
        (
            tag_id,
            confidence_float,
            data_id,
        ),
    )
    assert cursor.lastrowid is not None, "No tag item was inserted"
    return cursor.lastrowid


def add_tag_to_item(
    conn: sqlite3.Connection,
    data_id: int,
    namespace: str,
    name: str,
    confidence: float = 1.0,
):
    tag_id = upsert_tag(conn, namespace, name)
    insert_tag_item(conn, data_id, tag_id, confidence)


def delete_orphan_tags(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute(
        """
        DELETE FROM tags
        WHERE rowid IN (
            SELECT tags.rowid
            FROM tags
            LEFT JOIN tags_items ON tags_items.tag_id = tags.id
            WHERE tags_items.rowid IS NULL
        )
    """
    )
    return cursor.rowcount


def get_all_tags_for_item_name_confidence(conn: sqlite3.Connection, sha256):
    tags = get_all_tags_for_item(conn, sha256)
    return [(row[1], row[2]) for row in tags]


def get_tag_names_list(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT name FROM tags")
    tag_names = cursor.fetchall()
    return [tag[0] for tag in tag_names]


def get_all_tags_for_item(conn: sqlite3.Connection, sha256):
    cursor = conn.cursor()
    cursor.execute(
        """
    SELECT tags.namespace, tags.name, tags_items.confidence, setters.name
    FROM items
    JOIN item_data
        ON items.id = item_data.item_id
    JOIN tags_items
        ON tags_items.item_data_id = item_data.id
    JOIN tags
        ON tags_items.tag_id = tags.id        
    JOIN setters 
        ON item_data.setter_id = setters.id
    WHERE items.sha256 = ?
    """,
        (sha256,),
    )
    tags = cursor.fetchall()
    return tags


def get_all_tag_namespaces(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute(
        """
    SELECT DISTINCT namespace
    FROM tags
    """
    )
    namespaces = [namespace[0] for namespace in cursor.fetchall()]
    namespace_prefixes = set(
        [namespace.split(":")[0] for namespace in namespaces]
    )
    namespaces += list(namespace_prefixes)
    namespaces.sort()
    return namespaces
