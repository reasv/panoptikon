from __future__ import annotations

import logging
import random
import sqlite3
from typing import List

import gradio as gr
from PIL import Image, ImageDraw, ImageFont

from src.db import get_database_connection
from src.db.bookmarks import (
    add_bookmark,
    delete_bookmarks_exclude_last_n,
    get_all_bookmark_namespaces,
    get_bookmark_metadata,
    get_bookmarks,
    remove_bookmark,
)
from src.db.storage import get_thumbnail
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
    conn = get_database_connection(write_lock=False, user_data_wl=True)
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
    conn = get_database_connection(write_lock=False, user_data_wl=True)
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


def create_placeholder_image_with_gradient(size=(512, 512), text="No Preview"):
    # Create a gradient background
    gradient = Image.new("RGB", size)
    draw = ImageDraw.Draw(gradient)

    for y in range(size[1]):
        r = int(255 * (y / size[1]))
        g = int(255 * (random.random()))
        b = int(255 * ((size[1] - y) / size[1]))
        for x in range(size[0]):
            draw.point((x, y), fill=(r, g, b))

    # Draw the blocked symbol (a circle with a diagonal line through it)
    symbol_radius = min(size) // 6
    symbol_center = (size[0] // 2, size[1] // 3)
    draw.ellipse(
        [
            (
                symbol_center[0] - symbol_radius,
                symbol_center[1] - symbol_radius,
            ),
            (
                symbol_center[0] + symbol_radius,
                symbol_center[1] + symbol_radius,
            ),
        ],
        outline="black",
        width=5,
    )
    draw.line(
        [
            (
                symbol_center[0] - symbol_radius,
                symbol_center[1] + symbol_radius,
            ),
            (
                symbol_center[0] + symbol_radius,
                symbol_center[1] - symbol_radius,
            ),
        ],
        fill="black",
        width=5,
    )

    # Load a default font
    try:
        font = ImageFont.load_default()
    except IOError:
        font = ImageFont.load_default()

    # Calculate text size using textbbox
    text_bbox = draw.textbbox((0, 0), text, font=font)
    text_width = text_bbox[2] - text_bbox[0]
    text_height = text_bbox[3] - text_bbox[1]

    # Draw the "No Preview" text below the symbol
    text_position = (
        size[0] // 2 - text_width // 2,
        size[1] // 2 + symbol_radius // 2,
    )
    draw.text(text_position, text, fill="black", font=font)

    return gradient


def get_item_thumbnail(
    conn: sqlite3.Connection, file: FileSearchResult | None, big: bool = True
):
    if file is None:
        return None
    if file.type is None:
        return file.path
    if file.type.startswith("video"):
        index = 0 if big else 1
        return (
            get_thumbnail(conn, file.sha256, index)
            or create_placeholder_image_with_gradient()
        )
    elif file.type.startswith("image/gif"):
        return file.path
    elif file.type.startswith("image"):
        return get_thumbnail(conn, file.sha256, 0) or file.path
    else:
        return (
            get_thumbnail(conn, file.sha256, 0)
            or create_placeholder_image_with_gradient()
        )
