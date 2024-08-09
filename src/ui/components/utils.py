from __future__ import annotations

import logging
import os
from typing import List

import gradio as gr

from src.db import get_database_connection
from src.db.bookmarks import (
    add_bookmark,
    delete_bookmarks_exclude_last_n,
    get_all_bookmark_namespaces,
    get_bookmark_metadata,
    get_bookmarks,
    remove_bookmark,
)
from src.types import FileSearchResult

logger = logging.getLogger(__name__)


def toggle_bookmark(
    bookmarks_namespace: str,
    selected_files: List[FileSearchResult],
    button_name: str,
):
    if len(selected_files) == 0:
        return gr.update(value="Bookmark")
    selected_image_sha256 = selected_files[0].sha256
    conn = get_database_connection(write_lock=True)
    if button_name == "Bookmark":
        add_bookmark(
            conn, namespace=bookmarks_namespace, sha256=selected_image_sha256
        )
        logger.debug(f"Added bookmark")
    else:
        remove_bookmark(
            conn, namespace=bookmarks_namespace, sha256=selected_image_sha256
        )
        logger.debug(f"Removed bookmark")
    conn.commit()
    conn.close()
    return on_selected_image_get_bookmark_state(
        bookmarks_namespace=bookmarks_namespace, selected_files=selected_files
    )


def on_selected_image_get_bookmark_state(
    bookmarks_namespace: str, selected_files: List[FileSearchResult]
):
    if len(selected_files) == 0:
        return gr.update(value="Bookmark")
    sha256 = selected_files[0].sha256
    conn = get_database_connection(write_lock=False)
    is_bookmarked, _ = get_bookmark_metadata(
        conn, namespace=bookmarks_namespace, sha256=sha256
    )
    conn.commit()
    conn.close()
    # If the image is bookmarked, we want to show the "Remove Bookmark" button
    return gr.update(value="Remove Bookmark" if is_bookmarked else "Bookmark")


def get_all_bookmark_folders():
    conn = get_database_connection(write_lock=False)
    bookmark_folders = get_all_bookmark_namespaces(conn)
    conn.close()
    return bookmark_folders


def get_all_bookmarks_in_folder(
    bookmarks_namespace: str,
    page_size: int = 1000,
    page: int = 1,
    order_by: str = "time_added",
    order=None,
):
    conn = get_database_connection(write_lock=False)
    bookmarks, total_bookmarks = get_bookmarks(
        conn,
        namespace=bookmarks_namespace,
        page_size=page_size,
        page=page,
        order_by=order_by,
        order=order,
    )
    conn.close()
    return bookmarks, total_bookmarks


def delete_bookmarks_except_last_n(bookmarks_namespace: str, keep_last_n: int):
    conn = get_database_connection(write_lock=True)
    delete_bookmarks_exclude_last_n(
        conn, namespace=bookmarks_namespace, n=keep_last_n
    )
    conn.commit()
    conn.close()


def delete_bookmark(bookmarks_namespace: str, sha256: str):
    conn = get_database_connection(write_lock=True)
    remove_bookmark(conn, namespace=bookmarks_namespace, sha256=sha256)
    conn.commit()
    conn.close()


def get_thumbnail(file: FileSearchResult | None, big: bool = True):
    if file is None:
        return None
    thumbnail_dir = os.getenv("THUMBNAIL_DIR", "./data/thumbs")
    if file.type and file.type.startswith("video"):
        # Get thumbnails directory from environment variable
        return (
            f"{thumbnail_dir}/{file.sha256}-grid.jpg"
            if big
            else f"{thumbnail_dir}/{file.sha256}-0.jpg"
        )
    else:
        if os.path.exists(f"{thumbnail_dir}/{file.sha256}.jpg"):
            return f"{thumbnail_dir}/{file.sha256}.jpg"
        return file.path
