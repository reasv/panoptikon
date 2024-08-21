import base64
import io
import logging
from typing import List, Literal, Optional, Tuple, Union

import numpy as np
from fastapi import APIRouter, Body, Depends, Query
from librosa import ex
from pydantic import BaseModel, Field
from pydantic.dataclasses import dataclass

from inferio.impl.utils import deserialize_array
from panoptikon.api.routers.search_types import SearchQueryModel
from panoptikon.api.routers.utils import get_db_readonly
from panoptikon.db.bookmarks import get_all_bookmark_namespaces
from panoptikon.db.extracted_text import get_text_stats
from panoptikon.db.extraction_log import get_existing_setters
from panoptikon.db.files import get_all_mime_types, get_file_stats
from panoptikon.db.folders import get_folders_from_database
from panoptikon.db.search import search_files
from panoptikon.db.search.types import SearchQuery
from panoptikon.db.tags import find_tags, get_all_tag_namespaces
from panoptikon.db.tagstats import (
    get_min_tag_confidence,
    get_most_common_tags_frequency,
)
from panoptikon.db.utils import serialize_f32
from panoptikon.types import ExtractedTextStats, FileSearchResult

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


@router.post(
    "/",
    summary="Search for files in the database",
    description="""
Search for files in the database based on the provided query parameters.

The search query takes a `SearchQuery` object as input, which contains all the parameters supported by search.
Search operates on `files`, which means that results are not unique by `sha256` value.
The `count` returned in the response is the total number of unique files that match the query.
There could be zero results even if the `count` is higher than zero,
if the `page` parameter is set beyond the number of pages available.

For semantic search, embeddings should be provided as base64-encoded byte strings in npy format.
To get the correct embeddings, use the /api/inference/predict endpoint with the correct inference_id.

It will return an application/octet-stream response with the embeddings in the correct format, which can be base64-encoded and used in the search query.

To get the list of embedding models the data is indexed with, use /api/search/stats and look for setters with "text-embedding" or "clip" type.
    """,
    response_model=FileSearchResultModel,
)
def search(
    data: SearchQuery = Body(default_factory=lambda: SearchQuery()),
    conn=Depends(get_db_readonly),
):
    logger.debug(f"Searching for files with query: {data}")
    if data.query.filters.image_embeddings:
        query = data.query.filters.image_embeddings.query
        data.query.filters.image_embeddings.query = extract_embeddings(query)
    if data.query.filters.extracted_text_embeddings:
        query = data.query.filters.extracted_text_embeddings.query
        data.query.filters.extracted_text_embeddings.query = extract_embeddings(
            query
        )
    results = list(search_files(conn, data))
    return process_results(results)


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
    setters: List[Tuple[str, str]]
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
def get_stats(conn=Depends(get_db_readonly), user: Optional[str] = Query(None)):
    setters = get_existing_setters(conn)
    if not user:
        bookmark_namespaces = get_all_bookmark_namespaces(
            conn, include_wildcard=True
        )
    else:
        bookmark_namespaces = get_all_bookmark_namespaces(
            conn, include_wildcard=False, user=user
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
        files=FileStats(total=items, unique=files, mime_types=file_types),
        tags=TagStats(
            namespaces=tag_namespaces, min_confidence=min_tags_threshold
        ),
        folders=folders,
        text_stats=text_stats,
    )


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
    conn=Depends(get_db_readonly),
    namespace: Optional[str] = Query(None),
    setters: List[str] = Query([]),
    confidence_threshold: Optional[float] = Query(None),
    limit: int = Query(10),
):
    return TagFrequency(
        tags=get_most_common_tags_frequency(
            conn,
            namespace=namespace,
            setters=setters,
            confidence_threshold=confidence_threshold,
            limit=limit,
        )
    )


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
    name: str = Query(...),
    limit: int = Query(10),
    conn=Depends(get_db_readonly),
):
    tags = find_tags(conn, name, limit)
    tags.sort(key=lambda x: x[2], reverse=True)
    return TagSearchResults(tags)
