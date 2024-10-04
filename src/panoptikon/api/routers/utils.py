import random
from typing import Dict, Optional

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
) -> Dict[str, str | bool | None]:
    check_dbs(index_db, user_data_db)
    index, user_data, _ = get_db_default_names()
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
    index, user_data, _ = get_db_default_names()
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
    index, user_data, _ = get_db_default_names()
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
