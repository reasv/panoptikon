import json
import os
import sqlite3
from datetime import datetime
from typing import List, Tuple

from src.db.files import get_existing_file_for_sha256
from src.types import FileSearchResult


def update_bookmarks(
    conn: sqlite3.Connection,
    items: List[Tuple[str, dict | None]],  # List of (sha256, metadata)
    namespace: str = "default",
    user: str = "user",
):
    cursor = conn.cursor()
    # Add all items as bookmarks, if they don't already exist, in a single query
    cursor.executemany(
        """
    INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(user, namespace, sha256)
    DO UPDATE SET metadata = excluded.metadata
    """,
        [
            (
                user,
                namespace,
                sha256,
                datetime.now().isoformat(),
                json.dumps(metadata) if metadata else None,
            )
            for sha256, metadata in items
        ],
    )


def add_bookmark(
    conn: sqlite3.Connection,
    sha256: str,
    namespace: str = "default",
    user: str = "user",
    metadata: dict | None = None,
):
    metadata_str = json.dumps(metadata) if metadata else None
    cursor = conn.cursor()
    cursor.execute(
        """
    INSERT INTO user_data.bookmarks
        (user, namespace, sha256, time_added, metadata)
        VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(user, namespace, sha256) DO NOTHING
    """,
        (user, namespace, sha256, datetime.now().isoformat(), metadata_str),
    )


def remove_bookmark(
    conn: sqlite3.Connection,
    sha256: str,
    namespace: str = "default",
    user: str = "user",
):
    cursor = conn.cursor()
    cursor.execute(
        """
    DELETE FROM user_data.bookmarks
    WHERE sha256 = ? AND namespace = ? AND user = ?
    """,
        (sha256, namespace, user),
    )


def get_bookmark_metadata(
    conn: sqlite3.Connection,
    sha256: str,
    namespace: str = "default",
    user: str = "user",
) -> Tuple[bool, dict | None]:
    cursor = conn.cursor()
    cursor.execute(
        """
    SELECT metadata
    FROM user_data.bookmarks
    WHERE sha256 = ?
    AND namespace = ?
    AND user = ?
    """,
        (sha256, namespace, user),
    )
    result = cursor.fetchone()
    if not result:
        return False, None

    metadata: dict | None = json.loads(result[0]) if result[0] else None

    return (True, metadata)


def delete_bookmarks_exclude_last_n(
    conn: sqlite3.Connection,
    n: int,
    namespace: str = "default",
    user: str = "user",
):
    cursor = conn.cursor()
    # Delete all bookmarks except the last n based on time_added
    cursor.execute(
        """
        DELETE FROM user_data.bookmarks
        WHERE namespace = ?
        AND user = ?
        AND sha256 NOT IN (
            SELECT sha256
            FROM user_data.bookmarks
            WHERE namespace = ?
            AND user = ?
            ORDER BY time_added DESC
            LIMIT ?
        )
    """,
        (namespace, user, namespace, user, n),
    )


def get_all_bookmark_namespaces(
    conn: sqlite3.Connection, user: str = "user", include_wildcard: bool = False
) -> List[str]:
    cursor = conn.cursor()
    wildcard_user = "OR user = '*'" if include_wildcard else ""
    # Get all bookmark namespaces, order by namespace name
    cursor.execute(
        f"""
        SELECT DISTINCT namespace
        FROM user_data.bookmarks
        WHERE user = ?
        {wildcard_user}
        ORDER BY namespace
        """,
        (user,),
    )
    namespaces = cursor.fetchall()
    return [namespace[0] for namespace in namespaces]


def get_bookmarks(
    conn: sqlite3.Connection,
    namespace: str = "default",
    user: str = "user",
    page_size: int = 1000,
    page: int = 1,
    order_by: str = "time_added",
    order: str | None = None,
    include_wildcard: bool = False,
) -> Tuple[List[FileSearchResult], int]:

    if page_size < 1:
        page_size = 1000000
    offset = (page - 1) * page_size
    wildcard_user = "OR user = '*'" if include_wildcard else ""
    # Fetch bookmarks with their paths, prioritizing available files
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT
            COUNT(DISTINCT user_data.bookmarks.sha256)
        FROM user_data.bookmarks
        JOIN files
        ON user_data.bookmarks.sha256 = files.sha256
        WHERE user_data.bookmarks.namespace = ?
        AND (user_data.bookmarks.user = ? {wildcard_user})
    """,
        (namespace, user),
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
        order_by_clause = "user_data.bookmarks.time_added"
        if order == None:
            order = "desc"

    order_clause = "DESC" if order == "desc" else "ASC"
    cursor.execute(
        f"""
        SELECT 
            COALESCE(
                available_files.path,
                any_files.path
            ) as path,
            user_data.bookmarks.sha256,
            COALESCE(
                MAX(available_files.last_modified),
                MAX(any_files.last_modified)
            ) as last_modified,
            items.type
        FROM user_data.bookmarks
        LEFT JOIN files AS available_files 
               ON user_data.bookmarks.sha256 = available_files.sha256 
               AND available_files.available = 1
        JOIN files AS any_files 
               ON user_data.bookmarks.sha256 = any_files.sha256
        JOIN items ON any_files.item_id = items.id
        WHERE user_data.bookmarks.namespace = ?
        AND (user_data.bookmarks.user = ? {wildcard_user})
        GROUP BY user_data.bookmarks.sha256
        ORDER BY {order_by_clause}
        {order_clause}
        LIMIT ? OFFSET ?
    """,
        (namespace, user, page_size, offset),
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
