import json
import os
import sqlite3
from datetime import datetime
from typing import List, Tuple

from panoptikon.db.files import get_existing_file_for_sha256
from panoptikon.types import FileSearchResult


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
    if namespace == "*":
        raise ValueError("Cannot add bookmark with wildcard namespace")
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
    if namespace != "*":
        ns_condition = "AND namespace = ?"
        params = [sha256, user, namespace]
    else:
        ns_condition = ""
        params = [sha256, user]
    cursor.execute(
        f"""
    DELETE FROM user_data.bookmarks
    WHERE sha256 = ? AND user = ? {ns_condition}
    """,
        params,
    )
    return cursor.rowcount


def get_bookmark_metadata(
    conn: sqlite3.Connection,
    sha256: str,
    namespace: str = "default",
    user: str = "user",
) -> Tuple[bool, str | None, dict | None]:
    cursor = conn.cursor()
    params = [sha256, user]
    if namespace != "*":
        namespace_condition = "AND namespace = ?"
        params.append(namespace)
    else:
        namespace_condition = ""
    cursor.execute(
        f"""
    SELECT namespace, metadata
    FROM user_data.bookmarks
    WHERE sha256 = ?
    AND user = ?
    {namespace_condition}
    """,
        params,
    )
    result = cursor.fetchone()
    if not result:
        return False, None, None

    metadata: dict | None = json.loads(result[1]) if result[1] else None
    namespace = result[0]
    return (True, namespace, metadata)


def get_bookmarks_item(
    conn: sqlite3.Connection,
    sha256: str,
    user: str = "user",
) -> List[Tuple[str | None, dict | None]]:
    cursor = conn.cursor()
    params = [sha256, user]

    cursor.execute(
        f"""
    SELECT namespace, metadata
    FROM user_data.bookmarks
    WHERE sha256 = ?
    AND user = ?
    """,
        params,
    )
    results = []
    for row in cursor.fetchall():
        metadata: dict | None = json.loads(row[1]) if row[1] else None
        namespace = row[0]
        results.append((namespace, metadata))
    return results


def delete_bookmarks_exclude_last_n(
    conn: sqlite3.Connection,
    n: int,
    namespace: str = "default",
    user: str = "user",
):
    if namespace != "*":
        namespace_condition = "AND namespace = ?"
        params = [user, namespace, user, namespace, n]
    else:
        namespace_condition = ""
        params = [user, user, n]
    cursor = conn.cursor()
    # Delete all bookmarks except the last n based on time_added
    cursor.execute(
        f"""
        DELETE FROM user_data.bookmarks
        WHERE user = ?
        {namespace_condition}
        AND sha256 NOT IN (
            SELECT sha256
            FROM user_data.bookmarks
            WHERE user = ?
            {namespace_condition}
            ORDER BY time_added DESC
            LIMIT ?
        )
    """,
        params,
    )
    return cursor.rowcount


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


def get_all_bookmark_users(
    conn: sqlite3.Connection,
) -> List[str]:
    cursor = conn.cursor()
    # Get all bookmark users, order by user name
    cursor.execute(
        f"""
        SELECT DISTINCT user
        FROM user_data.bookmarks
        ORDER BY user
        """,
    )
    users = cursor.fetchall()
    return [user[0] for user in users]


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
    if namespace != "*":
        ns_condition = "AND user_data.bookmarks.namespace = ?"
        count_params = [user, namespace]
    else:
        ns_condition = ""
        count_params = [user]

    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT
            COUNT(DISTINCT user_data.bookmarks.sha256)
        FROM user_data.bookmarks
        JOIN files
        ON user_data.bookmarks.sha256 = files.sha256
        WHERE (user_data.bookmarks.user = ? {wildcard_user}) 
        {ns_condition}
    """,
        count_params,
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
        WHERE (user_data.bookmarks.user = ? {wildcard_user})
        {ns_condition}
        GROUP BY user_data.bookmarks.sha256
        ORDER BY {order_by_clause}
        {order_clause}
        LIMIT ? OFFSET ?
    """,
        (
            (user, namespace, page_size, offset)
            if namespace != "*"
            else (user, page_size, offset)
        ),
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
