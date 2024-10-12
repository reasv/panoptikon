import io
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import PIL.Image
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import FileResponse, StreamingResponse
from pydantic.dataclasses import dataclass

from panoptikon.api.routers.utils import (
    create_placeholder_image_with_gradient,
    get_db_readonly,
    strip_non_latin1_chars,
)
from panoptikon.db import (
    ItemIdentifierType,
    get_database_connection,
    get_item_metadata,
)
from panoptikon.db.extracted_text import (
    get_extracted_text_for_item,
    get_text_by_ids,
)
from panoptikon.db.storage import get_thumbnail_bytes
from panoptikon.db.tags import get_all_tags_for_item
from panoptikon.types import ExtractedText, FileRecord, ItemRecord

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/api/items",
    tags=["items"],
    responses={404: {"description": "Not found"}},
)


def get_item(
    id: str | int = Query(
        ...,
        description="An item identifier (sha256 hash, file ID, path, item ID, or data ID for associated data)",
    ),
    id_type: ItemIdentifierType = Query(
        ...,
        description="The type of the item identifier",
    ),
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
) -> Tuple[ItemRecord, List[FileRecord], Dict[str, Any]]:
    conn = get_database_connection(**conn_args)
    try:
        item, files = get_item_metadata(conn, id, id_type)
        if item is None:
            raise HTTPException(status_code=404, detail="Item not found")
        return item, files, conn_args
    finally:
        conn.close()


@dataclass
class ItemMetadata:
    item: ItemRecord
    files: List[FileRecord]


@router.get(
    "/item",
    summary="Get item metadata and associated file metadata",
    description="""
Returns metadata for a given item.
This includes the item metadata and a list of all files associated with the item.
Files that do not exist on disk will not be included in the response.
This means the file list may be empty.

An `item` is a unique file. `item`s can have multiple `file`s associated with them, but unlike `file`s, `item`s have a unique sha256 hash.
Files are unique by `path`. If all files associated with an `item` are deleted, the item is deleted.
    """,
)
def get_item_meta(
    item_data: Tuple[ItemRecord, List[FileRecord], Dict[str, Any]] = Depends(
        get_item
    ),
):
    item, files, conn_args = item_data
    return ItemMetadata(item=item, files=files)


@router.get(
    "/item/file",
    summary="Get actual file contents for an item",
    description="""
Returns the actual file contents for a given item.
Content type is determined by the file extension.
""",
)
def get_item_file(
    item_data: Tuple[ItemRecord, List[FileRecord], Dict[str, Any]] = Depends(
        get_item
    ),
):
    item, files, conn_args = item_data
    if len(files) == 0:
        raise HTTPException(status_code=404, detail="No file found for item")
    file = files[0]
    return FileResponse(
        path=file.path,
        media_type=item.type,
        filename=file.filename,
        content_disposition_type="inline",
    )


placeholder_byte_array = io.BytesIO()
create_placeholder_image_with_gradient().save(
    placeholder_byte_array, format="PNG"
)
placeholder_byte_array = placeholder_byte_array.getvalue()


@router.get(
    "/item/thumbnail",
    summary="Get thumbnail for an item",
    description="""
Returns a thumbnail for a given item.
The thumbnail may be a thumbnail,
the unmodified original image (only for images),
or a placeholder image generated on the fly.
GIFs are always returned as the original file.
For video thumbnails, the `big` parameter can be used to
select between the 2x2 frame grid (big=True) or the first frame from the grid (big=False).
    """,
)
def get_item_thumbnail(
    item_data: Tuple[ItemRecord, List[FileRecord], Dict[str, Any]] = Depends(
        get_item
    ),
    big: bool = Query(True),
):
    item, files, conn_args = item_data
    if len(files) == 0:
        raise HTTPException(status_code=404, detail="No file found for item")
    file = files[0]
    resp_type = "unknown"
    file_str = f"{file.sha256} ({file.filename})"
    conn = get_database_connection(**conn_args)
    try:
        mime = item.type
        original_filename = strip_non_latin1_chars(file.filename)
        original_filename_no_ext, _ = os.path.splitext(original_filename)

        if mime is None or mime.startswith("image/gif"):
            resp_type = "file/gif"
            logger.debug(f"Returning {resp_type} for {file_str}")
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
            resp_type = "thumbnail/buffer"
            logger.debug(f"Returning {resp_type} for {file_str}")
            return Response(
                content=buffer,
                media_type="image/jpeg",
                headers={
                    "Content-Disposition": f'inline; filename="{original_filename_no_ext}.jpg"'
                },
            )

        if mime.startswith("image"):
            resp_type = "file/image"
            logger.debug(f"Returning {resp_type} for {file_str}")
            return FileResponse(
                file.path,
                media_type=mime,
                filename=original_filename,
                content_disposition_type="inline",
            )

        resp_type = "file/placeholder"
        logger.debug(f"Returning {resp_type} for {file_str}")
        return Response(
            content=placeholder_byte_array,
            media_type="image/png",
            headers={
                "Content-Disposition": f'inline; filename="{original_filename_no_ext}.png"'
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
    "/item/text",
    summary="Get all text extracted from an item",
    description="""
Returns the text extracted from a given item
""",
)
def get_text_by_sha256(
    item_data: Tuple[ItemRecord, List[FileRecord], Dict[str, Any]] = Depends(
        get_item
    ),
    setters: List[str] = Query([]),
    truncate_length: int | None = Query(
        None,
        description="Text will be truncated to this length, if set. The `length` field will contain the original length.",
    ),
) -> TextResponse:
    item, files, conn_args = item_data
    conn = get_database_connection(**conn_args)
    try:
        text = get_extracted_text_for_item(conn, item.id, truncate_length)
        if setters:
            text = [t for t in text if t.setter_name in setters]
        return TextResponse(text=text)
    finally:
        conn.close()


@dataclass
class TagResponse:
    tags: List[Tuple[str, str, float, str]]


@router.get(
    "/item/tags",
    summary="Get tags for an item",
    description="""
Returns the tags associated with a given item.
The response contains a list of tuples, where each tuple contains
the tag namespace, tag name, confidence, and setter name.
The `setters` parameter can be used to filter tags by the setter name.
The `confidence_threshold` parameter can be used to filter tags based on
the minimum confidence threshold
""",
)
def get_tags(
    item_data: Tuple[ItemRecord, List[FileRecord], Dict[str, Any]] = Depends(
        get_item
    ),
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
) -> TagResponse:
    item, files, conn_args = item_data
    conn = get_database_connection(**conn_args)
    try:
        tags = get_all_tags_for_item(
            conn,
            item.id,
            setters,
            confidence_threshold,
            namespaces,
            limit_per_namespace,
        )
        return TagResponse(tags=tags)
    finally:
        conn.close()


@router.get(
    "/text/any",
    summary="Get text from text_ids",
    description="""
Returns texts given a list of text IDs
""",
)
def get_texts_by_text_ids(
    text_ids: List[int] = Query(..., description="List of extracted text IDs"),
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
) -> TextResponse:
    conn = get_database_connection(**conn_args)
    try:
        result = get_text_by_ids(conn, text_ids)
        texts = [t[1] for t in result]
        return TextResponse(text=texts)
    finally:
        conn.close()
