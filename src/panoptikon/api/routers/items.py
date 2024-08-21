import io
import logging
import sqlite3
from dataclasses import dataclass
from typing import List

import PIL
import PIL.Image
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import FileResponse

from panoptikon.api.routers.utils import get_db_readonly
from panoptikon.db.files import (
    get_existing_file_for_sha256,
    get_item_metadata_by_sha256,
)
from panoptikon.db.storage import get_thumbnail_bytes
from panoptikon.types import FileRecord, ItemRecord
from panoptikon.ui.components.utils import (
    create_placeholder_image_with_gradient,
)
from panoptikon.utils import get_mime_type

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/api/items",
    tags=["items"],
    responses={404: {"description": "Not found"}},
)


@dataclass
class ItemMetadata:
    item: ItemRecord
    files: List[FileRecord]


@router.get(
    "/item/{sha256}",
    summary="Get item metadata from its sha256 hash",
    description="""
Returns metadata for a given item by its sha256 hash.
This includes the item metadata and a list of all files associated with the item.
Files that do not exist on disk will not be included in the response.
This means the file list may be empty.
    """,
    response_model=ItemMetadata,
)
def get_item_by_sha256(
    sha256: str,
    conn: sqlite3.Connection = Depends(get_db_readonly),
):
    item, files = get_item_metadata_by_sha256(conn, sha256)
    if item is None or files is None:
        raise HTTPException(status_code=404, detail="Item not found")

    return ItemMetadata(item=item, files=files)


@router.get(
    "/file/{sha256}",
    summary="Get file by sha256",
    description="""
Returns the actual file contents for a given sha256 hash.
Content type is determined by the file extension.
    """,
    response_class=FileResponse,
)
def get_file_by_sha256(
    sha256: str,
    conn: sqlite3.Connection = Depends(get_db_readonly),
):
    # Get the file path from the database
    file_record = get_existing_file_for_sha256(conn, sha256)

    if file_record is None:
        raise HTTPException(status_code=404, detail="File not found")
    path = file_record.path
    mime = get_mime_type(path)
    # Use FileResponse to serve the file, FastAPI will handle the correct content type
    return FileResponse(path, media_type=mime)


@router.get(
    "/thumbnail/{sha256}",
    summary="Get thumbnail for an item by its sha256",
    description="""
Returns a thumbnail for a given item by its sha256 hash.
The thumbnail may be a thumbnail,
the unmodified original image (only for images),
or a placeholder image generated on the fly.
GIFs are always returned as the original file.
For video thumbnails, the `big` parameter can be used to
select between the 2x2 frame grid (big=True) or the first frame from the grid (big=False).
    """,
)
def get_thumbnail_by_sha256(
    sha256: str,
    big: bool = Query(True),
    conn: sqlite3.Connection = Depends(get_db_readonly),
):
    file = get_existing_file_for_sha256(conn, sha256)
    if not file:
        raise HTTPException(status_code=404, detail="Item not found")
    mime = get_mime_type(file.path)

    if mime is None or mime.startswith("image/gif"):
        return FileResponse(file.path, media_type=mime)

    index = 0
    if mime.startswith("video"):
        index = 0 if big else 1

    buffer = get_thumbnail_bytes(conn, file.sha256, index)
    if buffer:
        return Response(content=buffer, media_type="image/jpeg")

    if mime.startswith("image"):
        return FileResponse(file.path, media_type=mime)
    gradient: PIL.Image.Image = create_placeholder_image_with_gradient()
    # Convert the PIL image to bytes
    img_byte_array = io.BytesIO()
    gradient.save(img_byte_array, format="PNG")
    img_byte_array = img_byte_array.getvalue()
    return Response(content=img_byte_array, media_type="image/png")
