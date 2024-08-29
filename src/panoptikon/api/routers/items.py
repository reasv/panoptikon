import io
import logging
import os
from typing import Any, Dict, List, Tuple

import PIL
import PIL.Image
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import FileResponse
from pydantic.dataclasses import dataclass

from panoptikon.api.routers.utils import get_db_readonly, strip_non_latin1_chars
from panoptikon.db import get_database_connection
from panoptikon.db.extracted_text import get_extracted_text_for_item
from panoptikon.db.files import (
    get_existing_file_for_sha256,
    get_file_by_path,
    get_item_metadata_by_sha256,
)
from panoptikon.db.storage import get_thumbnail_bytes
from panoptikon.db.tags import get_all_tags_for_item
from panoptikon.types import ExtractedText, FileRecord, ItemRecord
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

An `item` is a unique file. `item`s can have multiple `file`s associated with them, but unlike `file`s, `item`s have a unique sha256 hash.
Files are unique by `path`. If all files associated with an `item` are deleted, the item is deleted.
    """,
    response_model=ItemMetadata,
)
def get_item_by_sha256(
    sha256: str,
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
):
    conn = get_database_connection(**conn_args)
    try:
        item, files = get_item_metadata_by_sha256(conn, sha256)
        if item is None or files is None:
            raise HTTPException(status_code=404, detail="Item not found")
        return ItemMetadata(item=item, files=files)
    finally:
        conn.close()


@router.get(
    "/from-path/{path}",
    summary="Get item metadata from a path",
    description="""
Returns metadata for a given item from its original file path.
This includes the item metadata and a list of all files associated with the item.
Files that do not exist on disk will not be included in the response.
This means the file list may be empty.
    """,
    response_model=ItemMetadata,
)
def get_item_by_path(
    path: str,
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
):
    conn = get_database_connection(**conn_args)
    try:
        file_record = get_file_by_path(conn, path)
        if file_record is None:
            raise HTTPException(status_code=404, detail="File not found")
        sha256 = file_record.sha256
        item, files = get_item_metadata_by_sha256(conn, sha256)
        if item is None or files is None:
            raise HTTPException(status_code=404, detail="Item not found")
        return ItemMetadata(item=item, files=files)
    finally:
        conn.close()


@router.get(
    "/file/{sha256}",
    summary="Get file by sha256",
    description="""
Returns the actual file contents for a given sha256 hash.
Content type is determined by the file extension.
    """,
    responses={
        200: {
            "description": "Arbitrary binary data",
            "content": {"*/*": {}},  # Accepts any MIME type
        },
        404: {"description": "Item not found"},
    },
)
def get_file_by_sha256(
    sha256: str,
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
) -> FileResponse:
    conn = get_database_connection(**conn_args)
    try:
        file_record = get_existing_file_for_sha256(conn, sha256)

        if file_record is None:
            raise HTTPException(status_code=404, detail="File not found")
        path = file_record.path
        mime = get_mime_type(path)
        return FileResponse(
            path,
            media_type=mime,
            filename=os.path.basename(path),
            content_disposition_type="inline",
        )
    finally:
        conn.close()


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
    responses={
        200: {
            "description": "Image file binary",
            "content": {"*/*": {}},  # Accepts any MIME type
        },
        404: {"description": "Item not found"},
    },
)
def get_thumbnail_by_sha256(
    sha256: str,
    big: bool = Query(True),
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
) -> Response:
    conn = get_database_connection(**conn_args)
    try:
        file = get_existing_file_for_sha256(conn, sha256)
        if not file:
            raise HTTPException(status_code=404, detail="Item not found")
        mime = get_mime_type(file.path)
        original_filename = strip_non_latin1_chars(os.path.basename(file.path))
        original_filename_no_ext, _ = os.path.splitext(original_filename)

        if mime is None or mime.startswith("image/gif"):
            return FileResponse(
                file.path,
                media_type=mime,
                filename=original_filename,
                content_disposition_type="inline",
            )

        index = 0
        if mime.startswith("video"):
            index = 0 if big else 1

        buffer = get_thumbnail_bytes(conn, file.sha256, index)
        if buffer:
            return Response(
                content=buffer,
                media_type="image/jpeg",
                headers={
                    "Content-Disposition": f'inline; filename="{original_filename_no_ext}.jpg"'
                },
            )

        if mime.startswith("image"):
            return FileResponse(
                file.path,
                media_type=mime,
                filename=original_filename,
                content_disposition_type="inline",
            )
        gradient: PIL.Image.Image = create_placeholder_image_with_gradient()
        # Convert the PIL image to bytes
        img_byte_array = io.BytesIO()
        gradient.save(img_byte_array, format="PNG")
        img_byte_array = img_byte_array.getvalue()
        return Response(
            content=img_byte_array,
            media_type="image/png",
            headers={
                "Content-Disposition": f'inline; filename="{original_filename_no_ext}.png"'
            },
        )
    finally:
        conn.close()


@dataclass
class TextResponse:
    text: List[ExtractedText]


@router.get(
    "/text/{sha256}",
    summary="Get text extracted from an item by its sha256",
    description="""
Returns the text extracted from a given item by its sha256 hash.
""",
    response_model=TextResponse,
)
def get_text_by_sha256(
    sha256: str,
    setters: List[str] = Query([]),
    max_length: int | None = Query(
        None,
        description="Text will be truncated to this length, if set. The `length` field will contain the original length.",
    ),
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
):
    conn = get_database_connection(**conn_args)
    try:
        text = get_extracted_text_for_item(conn, sha256, max_length)
        if setters:
            text = [t for t in text if t.setter_name in setters]
        return TextResponse(text=text)
    finally:
        conn.close()


@dataclass
class TagResponse:
    tags: List[Tuple[str, str, float, str]]


@router.get(
    "/tags/{sha256}",
    summary="Get tags for an item by its sha256",
    description="""
Returns the tags associated with a given item by its sha256 hash.
The response contains a list of tuples, where each tuple contains
the tag namespace, tag name, confidence, and setter name.
The `setters` parameter can be used to filter tags by the setter name.
The `confidence_threshold` parameter can be used to filter tags based on
the minimum confidence threshold
""",
    response_model=TagResponse,
)
def get_tags_by_sha256(
    sha256: str,
    setters: List[str] = Query([]),
    confidence_threshold: float = Query(0.0),
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
):
    conn = get_database_connection(**conn_args)
    try:
        tags = get_all_tags_for_item(
            conn, sha256, setters, confidence_threshold
        )
        return TagResponse(tags=tags)
    finally:
        conn.close()
