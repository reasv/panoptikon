import os
import sqlite3
from datetime import datetime
from typing import List, Tuple

from src.db.files import get_existing_file_for_sha256
from src.db.search import FileSearchResult


def update_bookmarks(
    conn: sqlite3.Connection,
    items_sha256: List[str],
    namespace: str = "default",
):
    cursor = conn.cursor()
    # Add all items as bookmarks, if they don't already exist, in a single query
    cursor.executemany(
        """
    INSERT INTO bookmarks (namespace, sha256, time_added)
    VALUES (?, ?, ?)
    ON CONFLICT(namespace, sha256) DO NOTHING
    """,
        [
            (namespace, sha256, datetime.now().isoformat())
            for sha256 in items_sha256
        ],
    )

    # Remove all items that are not in the list
    cursor.execute(
        """
    DELETE FROM bookmarks
    WHERE sha256 NOT IN ({}) AND namespace = ?
    """.format(
            ",".join(["?"] * len(items_sha256)), items_sha256, namespace
        )
    )


def add_bookmark(
    conn: sqlite3.Connection,
    sha256: str,
    namespace: str = "default",
    metadata: str | None = None,
):
    cursor = conn.cursor()
    cursor.execute(
        """
    INSERT INTO bookmarks (namespace, sha256, time_added, metadata)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(namespace, sha256) DO NOTHING
    """,
        (namespace, sha256, datetime.now().isoformat(), metadata),
    )


def remove_bookmark(
    conn: sqlite3.Connection, sha256: str, namespace: str = "default"
):
    cursor = conn.cursor()
    cursor.execute(
        """
    DELETE FROM bookmarks
    WHERE sha256 = ? AND namespace = ?
    """,
        (sha256, namespace),
    )


def get_bookmark_metadata(
    conn: sqlite3.Connection, sha256: str, namespace: str = "default"
):
    cursor = conn.cursor()
    cursor.execute(
        """
    SELECT metadata
    FROM bookmarks
    WHERE sha256 = ? AND namespace = ?
    """,
        (sha256, namespace),
    )
    metadata = cursor.fetchone()

    return (True, metadata[0]) if metadata else (False, None)


def delete_bookmarks_exclude_last_n(
    conn: sqlite3.Connection, n: int, namespace: str = "default"
):
    cursor = conn.cursor()
    # Delete all bookmarks except the last n based on time_added
    cursor.execute(
        """
        DELETE FROM bookmarks
        WHERE namespace = ?
        AND sha256 NOT IN (
            SELECT sha256
            FROM bookmarks
            WHERE namespace = ?
            ORDER BY time_added DESC
            LIMIT ?
        )
    """,
        (namespace, namespace, n),
    )

    conn.commit()


def get_all_bookmark_namespaces(conn: sqlite3.Connection) -> List[str]:
    cursor = conn.cursor()
    # Get all bookmark namespaces, order by namespace name
    cursor.execute(
        """
        SELECT DISTINCT namespace
        FROM bookmarks
        ORDER BY namespace
    """
    )
    namespaces = cursor.fetchall()
    return [namespace[0] for namespace in namespaces]


def get_bookmarks(
    conn: sqlite3.Connection,
    namespace: str = "default",
    page_size=1000,
    page=1,
    order_by="time_added",
    order=None,
) -> Tuple[List[FileSearchResult], int]:

    if page_size < 1:
        page_size = 1000000
    offset = (page - 1) * page_size

    # Fetch bookmarks with their paths, prioritizing available files
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COUNT(DISTINCT bookmarks.sha256)
        FROM bookmarks
        JOIN files
        ON bookmarks.sha256 = files.sha256
        WHERE bookmarks.namespace = ?
    """,
        (namespace,),
    )
    total_results = cursor.fetchone()[0]
    # Can order by time_added, path, or last_modified

    if order_by == "path":
        order_by_clause = "path"
        if order == None:
            order = "asc"
    elif order_by == "last_modified":
        order_by_clause = "MAX(any_files.last_modified)"
        if order == None:
            order = "desc"
    else:
        order_by_clause = "bookmarks.time_added"
        if order == None:
            order = "desc"

    order_clause = "DESC" if order == "desc" else "ASC"
    cursor.execute(
        f"""
        SELECT 
        COALESCE(available_files.path, any_files.path) as path,
        bookmarks.sha256,
        COALESCE(MAX(available_files.last_modified), MAX(any_files.last_modified)) as last_modified,
        items.type
        FROM bookmarks
        LEFT JOIN files AS available_files 
               ON bookmarks.sha256 = available_files.sha256 
               AND available_files.available = 1
        JOIN files AS any_files 
               ON bookmarks.sha256 = any_files.sha256
        JOIN items ON any_files.item_id = items.id
        WHERE bookmarks.namespace = ?
        GROUP BY bookmarks.sha256
        ORDER BY {order_by_clause}
        {order_clause}
        LIMIT ? OFFSET ?
    """,
        (namespace, page_size, offset),
    )

    bookmarks: List[FileSearchResult] = []
    for row in cursor.fetchall():
        item = FileSearchResult(*row)
        if not os.path.exists(item.path):
            if file := get_existing_file_for_sha256(conn, item.sha256):
                item.path = file.path
                bookmarks.append(item)
            # If the path does not exist and no working path is found, skip this item
            continue
        bookmarks.append(item)

    return bookmarks, total_results
