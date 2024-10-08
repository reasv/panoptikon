import io
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import PIL
import PIL.Image
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import FileResponse, StreamingResponse
from pydantic.dataclasses import dataclass

from panoptikon.api.routers.utils import (
    create_placeholder_image_with_gradient,
    get_db_readonly,
    strip_non_latin1_chars,
)
from panoptikon.db import get_database_connection
from panoptikon.db.extracted_text import (
    get_extracted_text_for_item,
    get_text_by_ids,
)
from panoptikon.db.files import (
    get_existing_file_for_sha256,
    get_file_by_path,
    get_item_metadata_by_sha256,
    get_sha256_for_file_id,
    get_sha256_for_item_id,
)
from panoptikon.db.storage import get_thumbnail_bytes
from panoptikon.db.tags import get_all_tags_for_item
from panoptikon.types import ExtractedText, FileRecord, ItemRecord
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
    "/from-id/{item_id}",
    summary="Get item metadata from its item_id",
    description="""
Returns metadata for a given item by its item_id.
This includes the item metadata and a list of all files associated with the item.
Files that do not exist on disk will not be included in the response.
This means the file list may be empty.

An `item` is a unique file. `item`s can have multiple `file`s associated with them, but unlike `file`s, `item`s have a unique sha256 hash.
Files are unique by `path`. If all files associated with an `item` are deleted, the item is deleted.
    """,
    response_model=ItemMetadata,
)
def get_item_by_id(
    item_id: int,
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
):
    conn = get_database_connection(**conn_args)
    try:
        sha256 = get_sha256_for_item_id(conn, item_id)
        if sha256 is None:
            raise HTTPException(status_code=404, detail="Item not found")
        item, files = get_item_metadata_by_sha256(conn, sha256)
        if item is None or files is None:
            raise HTTPException(status_code=404, detail="Item not found")
        return ItemMetadata(item=item, files=files)
    finally:
        conn.close()


@router.get(
    "/from-file-id/{file_id}",
    summary="Get item metadata from a file_id",
    description="""
Returns metadata for a given item by the file_id of one of its files.
This includes the item metadata and a list of all files associated with the item.
Files that do not exist on disk will not be included in the response.
This means the file list may be empty.

An `item` is a unique file. `item`s can have multiple `file`s associated with them, but unlike `file`s, `item`s have a unique sha256 hash.
Files are unique by `path`. If all files associated with an `item` are deleted, the item is deleted.
    """,
    response_model=ItemMetadata,
)
def get_item_by_file_id(
    file_id: int,
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
):
    conn = get_database_connection(**conn_args)
    try:
        sha256 = get_sha256_for_file_id(conn, file_id)
        if sha256 is None:
            raise HTTPException(status_code=404, detail="Item not found")
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
) -> StreamingResponse:
    conn = get_database_connection(**conn_args)
    try:
        file_record = get_existing_file_for_sha256(conn, sha256)

        if file_record is None:
            raise HTTPException(status_code=404, detail="File not found")

        path = file_record.path
        mime = get_mime_type(path)

        # Open the file in binary mode for streaming
        file_handle = open(path, "rb")

        # Return a StreamingResponse to avoid relying on filesystem-reported size
        return StreamingResponse(
            file_handle,
            media_type=mime,
            headers={
                "Content-Disposition": f'inline; filename="{os.path.basename(path)}"'
            },
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
) -> StreamingResponse:
    conn = get_database_connection(**conn_args)
    resp_type = "default"
    try:
        file = get_existing_file_for_sha256(conn, sha256)
        if not file:
            raise HTTPException(status_code=404, detail="Item not found")
        mime = get_mime_type(file.path)
        original_filename = strip_non_latin1_chars(os.path.basename(file.path))
        original_filename_no_ext, _ = os.path.splitext(original_filename)

        if mime is None or mime.startswith("image/gif"):
            resp_type = "file/gif"
            logger.debug(f"{resp_type} for {sha256}: ")
            file_handle = open(file.path, "rb")
            return StreamingResponse(
                file_handle,
                media_type=mime,
                headers={
                    "Content-Disposition": f'inline; filename="{original_filename}"',
                },
            )

        index = 0
        if mime.startswith("video"):
            index = 0 if big else 1

        buffer = get_thumbnail_bytes(conn, file.sha256, index)
        if buffer:
            resp_type = "thumbnail/buffer"
            logger.debug(f"{resp_type} for {sha256}: ")
            return StreamingResponse(
                io.BytesIO(buffer),
                media_type="image/jpeg",
                headers={
                    "Content-Disposition": f'inline; filename="{original_filename_no_ext}.jpg"',
                },
            )

        if mime.startswith("image"):
            resp_type = "file/image"
            logger.debug(f"{resp_type} for {sha256}: ")
            file_handle = open(file.path, "rb")
            return StreamingResponse(
                file_handle,
                media_type=mime,
                headers={
                    "Content-Disposition": f'inline; filename="{original_filename}"',
                },
            )

        # Handle placeholder image
        gradient = create_placeholder_image_with_gradient()
        img_byte_array = io.BytesIO()
        gradient.save(img_byte_array, format="PNG")
        img_byte_array.seek(0)
        resp_type = "file/generated"
        logger.debug(f"{resp_type} for {sha256}: ")
        return StreamingResponse(
            img_byte_array,
            media_type="image/png",
            headers={
                "Content-Disposition": f'inline; filename="{original_filename_no_ext}.png"',
            },
        )
    except Exception as e:
        logger.error(f"Error generating thumbnail ({resp_type}): {e}")
        raise HTTPException(status_code=404, detail="Thumbnail not found")
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
    truncate_length: int | None = Query(
        None,
        description="Text will be truncated to this length, if set. The `length` field will contain the original length.",
    ),
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
):
    conn = get_database_connection(**conn_args)
    try:
        text = get_extracted_text_for_item(conn, sha256, truncate_length)
        if setters:
            text = [t for t in text if t.setter_name in setters]
        return TextResponse(text=text)
    finally:
        conn.close()


@router.get(
    "/text",
    summary="Get text from text_ids",
    description="""
Returns texts given a list of text IDs.
""",
    response_model=TextResponse,
)
def get_texts_by_text_ids(
    text_ids: List[int] = Query(..., description="List of extracted text IDs"),
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
):
    conn = get_database_connection(**conn_args)
    try:
        result = get_text_by_ids(conn, text_ids)
        texts = [t[1] for t in result]
        return TextResponse(text=texts)
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
    setters: List[str] = Query(
        [],
        description="List of models that set the tags to filter by (default: all)",
    ),
    namespaces: List[str] = Query(
        [],
        description="List of namespaces to filter by (default: all). A namespace includes all namespaces that start with the namespace string.",
    ),
    confidence_threshold: float = Query(
        0.0,
        ge=0.0,
        le=1.0,
        description="Minimum confidence threshold, between 0 and 1 (default: 0.0)",
    ),
    limit_per_namespace: Optional[int] = Query(
        None,
        description="Maximum number of tags to return for each *setter, namespace pair* (default: all). Higher confidence tags are given priority.",
    ),
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
):
    conn = get_database_connection(**conn_args)
    try:
        tags = get_all_tags_for_item(
            conn,
            sha256,
            setters,
            confidence_threshold,
            namespaces,
            limit_per_namespace,
        )
        return TagResponse(tags=tags)
    finally:
        conn.close()
