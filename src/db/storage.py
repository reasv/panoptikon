import io
import logging
import sqlite3
import time
from typing import Sequence

import PIL.Image as PILImage

logger = logging.getLogger(__name__)


def store_thumbnails(
    conn: sqlite3.Connection,
    sha256: str,
    file_mime_type: str,
    process_version: int,
    thumbnails: Sequence[PILImage.Image],
):
    cursor = conn.cursor()
    # Delete existing thumbnails for the item if they have a lower version
    cursor.execute(
        """
        DELETE FROM thumbnails
        WHERE item_sha256 = ? AND version < ?
    """,
        (sha256, process_version),
    )
    cursor.executemany(
        """
    INSERT INTO thumbnails (item_sha256, idx, item_mime_type, width, height, version, thumbnail)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """,
        [
            (
                sha256,
                idx,
                file_mime_type,
                thumbnail.width,
                thumbnail.height,
                process_version,
                thumbnail_to_bytes(thumbnail, get_thumb_format(file_mime_type)),
            )
            for idx, thumbnail in enumerate(thumbnails)
        ],
    )


def get_thumb_format(item_mime_type: str) -> str:
    # Check if the source file format is lossless
    # if item_mime_type in ["image/png", "image/tiff", "image/bmp"]:
    #     return "PNG"
    # Default to JPEG for lossy formats
    return "JPEG"


def convert_image_mode(image: PILImage.Image, format: str) -> PILImage.Image:
    if format == "JPEG":
        # Allowed mode for JPEG is RGB
        if image.mode != "RGB":
            image = image.convert("RGB")
    elif format == "PNG":
        # Allowed modes for PNG are RGBA, RGB, L (grayscale), LA (grayscale with alpha)
        if image.mode not in ["RGBA", "RGB", "L", "LA"]:
            # Convert paletted images to RGBA or grayscale images to LA
            image = image.convert("RGBA")
    return image


def thumbnail_to_bytes(thumbnail: PILImage.Image, format: str = "PNG") -> bytes:
    thumbnail = convert_image_mode(thumbnail, format)
    start_time = time.time()
    with io.BytesIO() as output:
        thumbnail.save(output, format)  # Save as PNG or any other format
        size = round(output.tell() / (1024 * 1024), 4)  # Size in MB
        logger.debug(
            f"Thumbnail converted to {format} (size {size}) in {time.time() - start_time:.2f} seconds"
        )
        return output.getvalue()


def has_thumbnail(
    conn: sqlite3.Connection, sha256: str, process_version: int
) -> bool:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT EXISTS(
            SELECT 1 
            FROM thumbnails 
            WHERE item_sha256 = ? AND idx = 0 AND version >= ?
            LIMIT 1
        )
        """,
        (sha256, process_version),
    )
    result = cursor.fetchone()
    return result[0] == 1 if result else False


def get_thumbnail(
    conn: sqlite3.Connection, sha256: str, idx: int
) -> PILImage.Image | None:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT thumbnail 
        FROM thumbnails 
        WHERE item_sha256 = ? AND idx = ?
        """,
        (sha256, idx),
    )
    result = cursor.fetchone()
    if result:
        thumbnail_data = result[0]
        thumbnail = PILImage.open(io.BytesIO(thumbnail_data))
        return thumbnail
    else:
        return None


def delete_orphaned_thumbnails(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute(
        """
        DELETE FROM thumbnails
        WHERE item_sha256 IN (
            SELECT thumbnails.item_sha256
            FROM thumbnails
            LEFT JOIN items ON thumbnails.item_sha256 = items.sha256
            WHERE items.sha256 IS NULL
        )
        """
    )
    if cursor.rowcount > 0:
        logger.info(f"Deleted {cursor.rowcount} orphaned thumbnails")
    return cursor.rowcount  # Return the number of rows deleted


def store_frames(
    conn: sqlite3.Connection,
    sha256: str,
    file_mime_type: str,
    process_version: int,
    frames: list[PILImage.Image],
):
    cursor = conn.cursor()
    # Delete existing frames for the item if they have a lower version
    cursor.execute(
        """
        DELETE FROM frames
        WHERE item_sha256 = ? AND version < ?
    """,
        (sha256, process_version),
    )
    cursor.executemany(
        """
    INSERT INTO frames (item_sha256, idx, item_mime_type, width, height, version, frame)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """,
        [
            (
                sha256,
                idx,
                file_mime_type,
                frame.width,
                frame.height,
                process_version,
                thumbnail_to_bytes(frame, get_thumb_format(file_mime_type)),
            )
            for idx, frame in enumerate(frames)
        ],
    )


def has_frame(
    conn: sqlite3.Connection, sha256: str, process_version: int = 0
) -> bool:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT EXISTS(
            SELECT 1 
            FROM frames 
            WHERE item_sha256 = ? AND idx = 0 AND version >= ?
            LIMIT 1
        )
        """,
        (sha256, process_version),
    )
    result = cursor.fetchone()
    return result[0] == 1 if result else False


def get_frames(conn: sqlite3.Connection, sha256: str) -> list[PILImage.Image]:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT frame 
        FROM frames 
        WHERE item_sha256 = ?
        ORDER BY idx
        """,
        (sha256,),
    )
    results = cursor.fetchall()
    frames = []
    for result in results:
        frame_data = result[0]
        frame = PILImage.open(io.BytesIO(frame_data))
        frames.append(frame)
    return frames


def delete_orphaned_frames(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute(
        """
        DELETE FROM frames
        WHERE item_sha256 IN (
            SELECT frames.item_sha256
            FROM frames
            LEFT JOIN items ON frames.item_sha256 = items.sha256
            WHERE items.sha256 IS NULL
        )
        """
    )
    if cursor.rowcount > 0:
        logger.info(f"Deleted {cursor.rowcount} orphaned frames")
    return cursor.rowcount  # Return the number of rows deleted
