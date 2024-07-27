import sqlite3
from typing import List


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
            DISTINCT items_extractions.item_id || '-' || data_extraction_log.setter
        ) AS distinct_count
        FROM items_extractions
        JOIN data_extraction_log
        ON items_extractions.log_id = data_extraction_log.id
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
