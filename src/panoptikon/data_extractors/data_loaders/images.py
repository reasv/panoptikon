import logging
import os
import sqlite3
from typing import Any, List, Sequence

import numpy as np
from PIL import Image as PILImage
from PIL import ImageSequence

from panoptikon.data_extractors.data_loaders.pdf import read_pdf
from panoptikon.data_extractors.data_loaders.video import video_to_frames
from panoptikon.data_extractors.extraction_jobs.types import JobInputData
from panoptikon.db.storage import (
    get_frames_bytes,
    store_frames,
    thumbnail_to_bytes,
)

logger = logging.getLogger(__name__)


def gif_to_frames(path: str) -> List[PILImage.Image]:
    gif = PILImage.open(path)
    frames = []

    # Count the total number of frames
    total_frames = 0
    for _ in ImageSequence.Iterator(gif):
        total_frames += 1

    # Calculate the step to get 4 evenly spaced frames
    step = max(total_frames // 4, 1)

    # Extract 4 evenly spaced frames
    for i, frame in enumerate(ImageSequence.Iterator(gif)):
        if i % step == 0:
            frames.append(frame.copy())
        if len(frames) == 4:  # Stop after extracting 4 frames
            break

    return frames


def image_loader(
    conn: sqlite3.Connection, item: JobInputData
) -> Sequence[bytes]:
    if item.type.startswith("image/gif"):
        return [
            thumbnail_to_bytes(frame, "JPEG")
            for frame in gif_to_frames(item.path)
        ]
    if item.type.startswith("image"):
        # Load image as bytes
        with open(item.path, "rb") as f:
            return [f.read()]
    if item.type.startswith("video"):
        if frames := get_frames_bytes(conn, item.sha256):
            logger.debug(f"Loaded {len(frames)} frames from database")
        else:
            pil_frames = video_to_frames(item.path, num_frames=4)
            frames = store_frames(
                conn,
                item.sha256,
                file_mime_type=item.type,
                process_version=1,
                frames=pil_frames,
            )
        return frames

    if item.type.startswith("application/pdf"):
        return [
            thumbnail_to_bytes(PILImage.fromarray(page), "JPEG")
            for page in read_pdf(item.path)
        ]
    if item.type.startswith("text/html"):
        res = read_html(item.path)
        assert res is not None, "Failed to read HTML file"
        return [
            thumbnail_to_bytes(PILImage.fromarray(page), "JPEG")
            for page in read_pdf(res)
        ]
    return []


def get_pdf_image(file_path: str) -> PILImage.Image:

    return PILImage.fromarray(read_pdf(file_path)[0])


def read_html(url: str, **kwargs: Any) -> bytes | None:
    from weasyprint import HTML

    """Read a PDF file and convert it into an image in numpy format
    
    Args:
    ----
        url: URL of the target web page
        **kwargs: keyword arguments from `weasyprint.HTML`

    Returns:
    -------
        decoded PDF file as a bytes stream
    """
    return HTML(url, **kwargs).write_pdf()


def get_html_image(file_path: str) -> PILImage.Image:
    res = read_html(file_path)
    assert res is not None, "Failed to read HTML file"
    return PILImage.fromarray(read_pdf(res)[0])


def generate_thumbnail(
    image_path,
    max_dimensions=(4096, 4096),
    max_file_size=24 * 1024 * 1024,
):
    """
    Generates a thumbnail for an overly large image.

    Parameters:
    - image_path (str): Path to the original image.
    - thumbnail_path (str): Path where the thumbnail will be saved.
    - max_dimensions (tuple): Maximum width and height for an image to be considered overly large.
    - max_file_size (int): Maximum file size (in bytes) for an image to be considered overly large.

    Returns:
    - bool: True if a thumbnail was created, False if the image was not overly large.
    """
    really_small_file_size = 5 * 1024 * 1024  # 5 MB
    # Check if the image is overly large based on file size
    file_size = os.path.getsize(image_path)
    if file_size <= really_small_file_size:
        return None

    img = PILImage.open(image_path)
    # Check if the image is overly large based on dimensions
    if (
        img.size[0] <= max_dimensions[0]
        and img.size[1] <= max_dimensions[1]
        and file_size <= max_file_size
    ):
        return None

    # Generate thumbnail
    img.thumbnail(max_dimensions, PILImage.Resampling.LANCZOS)
    return img
