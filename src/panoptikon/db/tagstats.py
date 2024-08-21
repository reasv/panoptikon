import sqlite3
from typing import Dict, List

from regex import D


def get_most_common_tags(
    conn: sqlite3.Connection,
    namespace: str | None = None,
    setters: List[str] | None = [],
    confidence_threshold: float | None = None,
    limit=10,
):
    cursor = conn.cursor()
    namespace_clause = "AND tags.namespace LIKE ? || '%'" if namespace else ""

    confidence_clause = (
        f"AND tags_items.confidence >= ?" if confidence_threshold else ""
    )
    setters_clause = (
        f"AND setters.name IN ({','.join(['?']*len(setters))})"
        if setters
        else ""
    )
    setters = setters or []
    query_args = [
        arg
        for arg in [namespace, confidence_threshold, *setters, limit]
        if arg is not None
    ]

    query = f"""
    SELECT tags.namespace, tags.name, COUNT(*) as count
    FROM tags
    JOIN tags_items
        ON tags.id = tags_items.tag_id
    {namespace_clause}
    {confidence_clause}
    JOIN item_data
        ON tags_items.item_data_id = item_data.id
    JOIN setters
        ON item_data.setter_id = setters.id
    {setters_clause}
    GROUP BY tags.namespace, tags.name
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
        f"WHERE setters.name IN ({','.join(['?']*len(setters))})"
        if setters
        else ""
    )
    cursor.execute(
        f"""
        SELECT COUNT(
            DISTINCT item_data.item_id || '-' || item_data.setter_id
        ) AS distinct_count
        FROM tags_items
        JOIN item_data
            ON tags_items.item_data_id = item_data.id
        JOIN setters
            ON item_data.setter_id = setters.id
        {setters_clause}""",
        setters if setters else (),
    )
    total_items_setters = cursor.fetchone()[0]
    # Calculate the frequency
    tags = [
        (tag[0], tag[1], tag[2], tag[2] / (total_items_setters)) for tag in tags
    ]
    return tags


def get_min_tag_confidence(conn: sqlite3.Connection) -> float:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT MIN(confidence) FROM tags_items
        """
    )
    # If there are no tags, return 0
    if res := cursor.fetchone():
        return res[0]
    else:
        return 0.0


def get_tag_frequency_by_ids(
    conn: sqlite3.Connection, ids: List[int]
) -> Dict[int, int]:
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT tag_id, COUNT(DISTINCT item_id)
        FROM tags_items
        JOIN item_data
            ON tags_items.item_data_id = item_data.id
        WHERE tag_id IN ({', '.join(['?'] * len(ids))})
        GROUP BY tag_id
        """,
        (ids),
    )
    # Return a mapping from tag_id to frequency
    return dict(cursor.fetchall())
