import sqlite3
from typing import Dict, List, Optional, Tuple

from panoptikon.db.tagstats import get_tag_frequency_by_ids


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


def get_all_tags_for_item(
    conn: sqlite3.Connection,
    sha256: str,
    setters: List[str] = [],
    confidence_threshold: float = 0.0,
    limit_per_namespace: Optional[int] = None,
) -> List[Tuple[str, str, float, str]]:
    cursor = conn.cursor()
    setters_clause = (
        f"AND setters.name IN ({','.join(['?']*len(setters))})"
        if setters
        else ""
    )
    confidence_clause = (
        f"AND tags_items.confidence >= {confidence_threshold}"
        if confidence_threshold
        else ""
    )
    cursor.execute(
        f"""
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
    {setters_clause}
    {confidence_clause}
    ORDER BY tags_items.rowid
    """,
        (
            sha256,
            *(setters if setters else ()),
            *((confidence_threshold,) if confidence_threshold else ()),
        ),
    )
    tags = cursor.fetchall()
    if limit_per_namespace:
        tags = limit_tags_by_namespace(tags, limit_per_namespace)
    return tags


def limit_tags_by_namespace(
    tags: List[Tuple[str, str, float, str]], limit: int
) -> List[Tuple[str, str, float, str]]:
    # Save the original index of each tag
    tags_with_index = [(rowid, tag) for rowid, tag in enumerate(tags)]
    # Sort tags by confidence, descending
    ordered_tags = sorted(tags_with_index, key=lambda x: x[1][2], reverse=True)

    ns_setter_counts = {}
    limited_tags: List[Tuple[int, Tuple[str, str, float, str]]] = []
    for index, tag in ordered_tags:
        ns_setter = f"{tag[3]}:{tag[0]}"
        if ns_setter in ns_setter_counts:
            ns_setter_counts[ns_setter] += 1
        else:
            ns_setter_counts[ns_setter] = 1
        if ns_setter_counts[ns_setter] <= limit:
            limited_tags.append((index, tag))

    # Sort the limited tags by their original index
    limited_tags.sort(key=lambda x: x[0])
    return [tag for _, tag in limited_tags]


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


def find_tags(
    conn: sqlite3.Connection,
    name: str,
    limit: int = 10,
) -> List[Tuple[str, str, int]]:
    cursor = conn.cursor()
    cursor.execute(
        """
    SELECT id, namespace, name
    FROM tags
    WHERE name LIKE ?
    LIMIT ?
    """,
        (f"%{name}%", limit),
    )
    # Generate a mapping from tag ID to namespace, name
    tags = cursor.fetchall()
    id_to_tag: Dict[int, Tuple[str, str]] = {
        tag[0]: (tag[1], tag[2]) for tag in tags
    }
    tags_with_frequency = get_tag_frequency_by_ids(conn, list(id_to_tag.keys()))
    return [
        (
            id_to_tag[tag_id][0],
            id_to_tag[tag_id][1],
            tags_with_frequency[tag_id],
        )
        for tag_id in tags_with_frequency
    ]
