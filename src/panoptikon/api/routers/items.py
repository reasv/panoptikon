import logging
import os
import sqlite3
from dataclasses import dataclass
from typing import Dict, List, Literal, Optional, Tuple, Union

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from regex import B

from panoptikon.api.routers.utils import get_db_readonly
from panoptikon.db import get_database_connection
from panoptikon.db.bookmarks import get_all_bookmark_namespaces
from panoptikon.db.extracted_text import get_text_stats
from panoptikon.db.extraction_log import get_existing_setters
from panoptikon.db.files import (
    get_all_mime_types,
    get_existing_file_for_sha256,
    get_file_stats,
    get_item_metadata_by_sha256,
)
from panoptikon.db.folders import get_folders_from_database
from panoptikon.db.search import search_files
from panoptikon.db.search.types import OrderByType, OrderType, SearchQuery
from panoptikon.db.search.utils import from_dict
from panoptikon.db.tags import find_tags, get_all_tag_namespaces
from panoptikon.db.tagstats import (
    get_min_tag_confidence,
    get_most_common_tags_frequency,
)
from panoptikon.types import FileRecord, FileSearchResult, ItemRecord
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
