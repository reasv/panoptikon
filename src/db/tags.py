import sqlite3

from src.db import get_item_id
from src.db.setters import upsert_setter


def upsert_tag(
    conn: sqlite3.Connection,
    namespace: str,
    name: str,
):
    cursor = conn.cursor()
    result = cursor.execute(
        """
    INSERT INTO tags (namespace, name)
    VALUES (?, ?, ?, ?)
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
    item_id: int,
    tag_id: int,
    setter_id: int,
    confidence=1.0,
    log_id: int | None = None,
):
    # Round confidence to 3 decimal places
    confidence_float = round(float(confidence), 4)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO tags_items (item_id, tag_id, setter_id, log_id, confidence)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(item_id, tag_id, setter_id)
        DO UPDATE SET confidence=excluded.confidence
    """,
        (item_id, tag_id, setter_id, log_id, confidence_float),
    )


def add_tag_to_item(
    conn: sqlite3.Connection,
    namespace: str,
    name: str,
    setter: str,
    sha256: str,
    confidence: float = 1.0,
    log_id: int | None = None,
):
    item_id = get_item_id(conn, sha256)
    assert item_id is not None, f"Item with sha256 {sha256} not found"
    setter_id = upsert_setter(conn, setter_type="tags", setter_name=setter)
    tag_id = upsert_tag(conn, namespace, name)
    insert_tag_item(conn, item_id, tag_id, setter_id, confidence, log_id)


def delete_orphan_tags(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute(
        """
        DELETE FROM tags
        WHERE rowid IN (
            SELECT tags.rowid
            FROM tags
            LEFT JOIN tags_items ON tags_items.tag_id = tags.id
            WHERE tags_items.id IS NULL
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
    JOIN tags_items ON items.id = tags_items.item_id
    AND items.sha256 = ?
    JOIN tags ON tags_items.tag_id = tags.id
    JOIN setters ON tags_items.setter_id = setters.id
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
