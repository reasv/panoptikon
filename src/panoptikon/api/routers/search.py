import base64
import io
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from fastapi import APIRouter, Body, Depends, Query
from pydantic import BaseModel
from pydantic.dataclasses import dataclass

from inferio.impl.utils import deserialize_array
from panoptikon.api.routers.utils import get_db_readonly
from panoptikon.db import get_database_connection
from panoptikon.db.bookmarks import get_all_bookmark_namespaces
from panoptikon.db.extracted_text import get_text_stats
from panoptikon.db.extraction_log import get_existing_setters
from panoptikon.db.files import get_all_mime_types, get_file_stats
from panoptikon.db.folders import get_folders_from_database
from panoptikon.db.pql.pql_model import PQLQuery
from panoptikon.db.pql.search import search_pql
from panoptikon.db.pql.types import SearchResult
from panoptikon.db.tags import find_tags, get_all_tag_namespaces
from panoptikon.db.tagstats import (
    get_min_tag_confidence,
    get_most_common_tags_frequency,
)
from panoptikon.db.utils import serialize_f32
from panoptikon.types import (
    ExtractedTextStats,
    FileSearchResult,
    OutputDataType,
)

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/api/search",
    tags=["search"],
    responses={404: {"description": "Not found"}},
)


@dataclass
class FileSearchResultModel:
    count: int
    results: List[FileSearchResult]


def process_results(
    results: List[Tuple[FileSearchResult, int]]
) -> FileSearchResultModel:
    if len(results) == 0:
        return FileSearchResultModel(count=0, results=[])
    return FileSearchResultModel(
        count=results[0][1], results=[r[0] for r in results]
    )


def deserialize_array(buffer: bytes) -> np.ndarray:
    bio = io.BytesIO(buffer)
    bio.seek(0)
    return np.load(bio, allow_pickle=False)


def extract_embeddings(buffer: bytes) -> bytes:
    numpy_array = deserialize_array(base64.b64decode(buffer))
    assert isinstance(
        numpy_array, np.ndarray
    ), "Expected a numpy array for embeddings"
    # Check the number of dimensions
    if len(numpy_array.shape) == 1:
        # If it is a 1D array, it is a single embedding
        return serialize_f32(numpy_array.tolist())
    # If it is a 2D array, it is a list of embeddings, get the first one
    return serialize_f32(numpy_array[0].tolist())


class FileSearchResponse(BaseModel):
    count: int
    results: List[SearchResult]


@router.post(
    "/pql",
    summary="Search for files and items in the database",
    description="""
Search for files in the database based on the provided query parameters.
This endpoint is meant to be used with the Panoptikon Query Language.
    """,
    response_model=FileSearchResponse,
    response_model_exclude_none=True,
)
def pql(
    search_query: PQLQuery = Body(
        default_factory=lambda: PQLQuery(),
        description="The PQL Search query to execute",
    ),
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
) -> FileSearchResponse:
    conn = get_database_connection(**conn_args)
    try:
        logger.debug(f"Searching for files with PQL: {search_query}")
        results, count = search_pql(conn, search_query)
        return FileSearchResponse(count=count, results=list(results))
    finally:
        conn.close()


@dataclass
class TagStats:
    namespaces: List[str]
    min_confidence: float


@dataclass
class FileStats:
    total: int
    unique: int
    mime_types: List[str]


@dataclass
class APISearchStats:
    setters: List[Tuple[OutputDataType, str]]
    bookmarks: List[str]
    files: FileStats
    tags: TagStats
    folders: List[str]
    text_stats: ExtractedTextStats


@router.get(
    "/stats",
    summary="Get statistics on the searchable data",
    description="""
Get statistics on the data indexed in the database.
This includes information about the tag namespaces, bookmark namespaces, file types, and folders present.
Most importantly, it includes the list of currently existing setters for each data type.
This information is relevant for building search queries.
    """,
    response_model=APISearchStats,
)
def get_stats(
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
    user: str = Query(
        "user",
        description="The bookmarks user to get the bookmark namespaces for",
    ),
    include_wildcard: bool = Query(
        True,
        description="Include namespaces from bookmarks with the * user value",
    ),
):
    conn = get_database_connection(**conn_args)
    try:
        setters = get_existing_setters(conn)
        bookmark_namespaces = get_all_bookmark_namespaces(
            conn, include_wildcard=include_wildcard, user=user
        )
        file_types = get_all_mime_types(conn)
        tag_namespaces = get_all_tag_namespaces(conn)
        folders = get_folders_from_database(conn)
        min_tags_threshold = get_min_tag_confidence(conn)
        text_stats = get_text_stats(conn)
        files, items = get_file_stats(conn)
        return APISearchStats(
            setters=setters,
            bookmarks=bookmark_namespaces,
            files=FileStats(total=files, unique=items, mime_types=file_types),
            tags=TagStats(
                namespaces=tag_namespaces,
                min_confidence=min_tags_threshold or 0.0,
            ),
            folders=folders,
            text_stats=text_stats,
        )
    finally:
        conn.close()


@dataclass
class TagFrequency:
    tags: List[Tuple[str, str, int, float]]


@router.get(
    "/tags/top",
    summary="Get the most common tags in the database",
    description="""
Get the most common tags in the database, based on the provided query parameters.
The result is a list of tuples, where each tuple contains the namespace, tag name, 
occurrences count, and relative frequency % (occurrences / total item_setter pairs).
The latter value is expressed as a float between 0 and 1.
The tags are returned in descending order of frequency.
The `limit` parameter can be used to control the number of tags to return.
The `namespace` parameter can be used to restrict the search to a specific tag namespace.
The `setters` parameter can be used to restrict the search to specific setters.
The `confidence_threshold` parameter can be used to filter tags based on the minimum confidence threshold.
    """,
    response_model=TagFrequency,
)
def get_top_tags(
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
    namespace: Optional[str] = Query(
        None, description="The tag namespace to search in"
    ),
    setters: List[str] = Query(
        [],
        description="The tag setter names to restrict the search to. Default is all",
    ),
    confidence_threshold: Optional[float] = Query(
        None,
        ge=0.0,
        le=1.0,
        description="The minimum confidence threshold for tags",
    ),
    limit: int = Query(10),
):
    conn = get_database_connection(**conn_args)
    try:
        return TagFrequency(
            tags=get_most_common_tags_frequency(
                conn,
                namespace=namespace,
                setters=setters,
                confidence_threshold=confidence_threshold,
                limit=limit,
            )
        )
    finally:
        conn.close()


@dataclass
class TagSearchResults:
    tags: List[Tuple[str, str, int]]


@router.get(
    "/tags",
    summary="Search tag names for autocompletion",
    description="""
Given a string, finds tags whose names contain the string.
Meant to be used for autocompletion in the search bar.
The `limit` parameter can be used to control the number of tags to return.
Returns a list of tuples, where each tuple contains the namespace, name, 
and the number of unique items tagged with the tag.
The tags are returned in descending order of the number of items tagged.
    """,
    response_model=TagSearchResults,
)
def get_tags(
    name: str = Query(..., description="The (partial) tag name to search for"),
    limit: int = Query(10),
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
):
    conn = get_database_connection(**conn_args)
    try:
        tags = find_tags(conn, name, limit)
        tags.sort(key=lambda x: x[2], reverse=True)
        return TagSearchResults(tags)
    finally:
        conn.close()
