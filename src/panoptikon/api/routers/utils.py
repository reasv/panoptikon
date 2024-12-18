import random
from typing import Any, Dict, Optional

from fastapi import HTTPException, Query
from PIL import Image, ImageDraw, ImageFont

from panoptikon.db import get_db_default_names, get_db_lists


def check_dbs(index_db: Optional[str], user_data_db: Optional[str]):
    if not index_db and not user_data_db:
        return
    index_dbs, user_data_dbs = get_db_lists()
    if index_db and index_db not in index_dbs:
        raise HTTPException(
            status_code=404, detail=f"Index database {index_db} not found"
        )
    if user_data_db and user_data_db not in user_data_dbs:
        raise HTTPException(
            status_code=404, detail=f"Index database {user_data_db} not found"
        )


def get_db_readonly(
    index_db: Optional[str] = Query(
        None,
        description="The name of the `index` database to open and use for this API call. Find available databases with `/api/db`",
    ),
    user_data_db: Optional[str] = Query(
        None,
        description="The name of the `user_data` database to open and use for this API call. Find available databases with `/api/db`",
    ),
) -> Dict[str, Any]:
    check_dbs(index_db, user_data_db)
    index, user_data = get_db_default_names()
    if not index_db:
        index_db = index
    if not user_data_db:
        user_data_db = user_data
    return {
        "write_lock": False,
        "index_db": index_db,
        "user_data_db": user_data_db,
    }


def get_db_user_data_wl(
    index_db: Optional[str] = Query(
        None,
        description="The name of the `index` database to open and use for this API call. Find available databases with `/api/db`",
    ),
    user_data_db: Optional[str] = Query(
        None,
        description="The name of the `user_data` database to open and use for this API call. Find available databases with `/api/db`",
    ),
) -> Dict[str, str | bool | None]:
    check_dbs(index_db, user_data_db)
    index, user_data = get_db_default_names()
    if not index_db:
        index_db = index
    if not user_data_db:
        user_data_db = user_data
    return {
        "write_lock": False,
        "user_data_wl": True,
        "index_db": index_db,
        "user_data_db": user_data_db,
    }


def strip_non_latin1_chars(input_string):
    return "".join(
        char for char in input_string if char.encode("latin-1", errors="ignore")
    )


def get_db_system_wl(
    index_db: Optional[str] = Query(
        None,
        description="The name of the `index` database to open and use for this API call. Find available databases with `/api/db`",
    ),
    user_data_db: Optional[str] = Query(
        None,
        description="The name of the `user_data` database to open and use for this API call. Find available databases with `/api/db`",
    ),
) -> Dict[str, str | bool | None]:
    check_dbs(index_db, user_data_db)
    index, user_data = get_db_default_names()
    if not index_db:
        index_db = index
    if not user_data_db:
        user_data_db = user_data
    return {
        "write_lock": True,
        "user_data_wl": False,
        "index_db": index_db,
        "user_data_db": user_data_db,
    }


def create_placeholder_image_with_gradient(size=(512, 512), text="No Preview"):
    """
    Creates a placeholder image with a smooth gradient background, a blocked symbol, and custom text.

    Args:
        size (tuple): Size of the image in pixels (width, height).
        text (str): Text to display below the symbol.

    Returns:
        PIL.Image: The generated placeholder image.
    """
    # Create a gradient background
    gradient = Image.new("RGB", size)
    draw = ImageDraw.Draw(gradient)

    for y in range(size[1]):
        # Smoothly vary all color channels based on y
        r = int(255 * (y / size[1]))  # Red increases from top to bottom
        g = int(255 * (y / size[1]))  # Green increases from top to bottom
        b = int(
            255 * ((size[1] - y) / size[1])
        )  # Blue decreases from top to bottom
        for x in range(size[0]):
            draw.point((x, y), fill=(r, g, b))

    # Draw the blocked symbol (a circle with a diagonal line through it)
    symbol_radius = min(size) // 6
    symbol_center = (size[0] // 2, size[1] // 3)
    ellipse_bbox = [
        (
            symbol_center[0] - symbol_radius,
            symbol_center[1] - symbol_radius,
        ),
        (
            symbol_center[0] + symbol_radius,
            symbol_center[1] + symbol_radius,
        ),
    ]
    draw.ellipse(ellipse_bbox, outline="black", width=5)
    line_start = (
        symbol_center[0] - symbol_radius,
        symbol_center[1] + symbol_radius,
    )
    line_end = (
        symbol_center[0] + symbol_radius,
        symbol_center[1] - symbol_radius,
    )
    draw.line([line_start, line_end], fill="black", width=5)

    # Load a default font
    try:
        font = ImageFont.load_default()
    except IOError:
        font = ImageFont.load_default()

    # Calculate text size using textbbox for better positioning
    text_bbox = draw.textbbox((0, 0), text, font=font)
    text_width = text_bbox[2] - text_bbox[0]
    text_height = text_bbox[3] - text_bbox[1]

    # Draw the "No Preview" text below the symbol
    text_position = (
        (size[0] - text_width) // 2,
        symbol_center[1] + symbol_radius + 20,  # 20 pixels below the symbol
    )
    draw.text(text_position, text, fill="black", font=font)

    return gradient
