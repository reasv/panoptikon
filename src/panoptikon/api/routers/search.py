import logging
from typing import Dict, List, Literal, Optional, Tuple, Union

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from regex import B

from panoptikon.api.routers.utils import get_db_readonly
from panoptikon.db import get_database_connection
from panoptikon.db.bookmarks import get_all_bookmark_namespaces
from panoptikon.db.extracted_text import get_text_stats
from panoptikon.db.extraction_log import get_existing_setters
from panoptikon.db.files import get_all_mime_types, get_file_stats
from panoptikon.db.folders import get_folders_from_database
from panoptikon.db.search import search_files
from panoptikon.db.search.types import OrderByType, OrderType, SearchQuery
from panoptikon.db.search.utils import from_dict
from panoptikon.db.tags import find_tags, get_all_tag_namespaces
from panoptikon.db.tagstats import (
    get_min_tag_confidence,
    get_most_common_tags_frequency,
)
from panoptikon.types import FileSearchResult

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/api/search",
    tags=["search"],
    responses={404: {"description": "Not found"}},
)


class QueryTagFiltersModel(BaseModel):
    pos_match_all: List[str] = Field(default_factory=list)
    pos_match_any: List[str] = Field(default_factory=list)
    neg_match_any: List[str] = Field(default_factory=list)
    neg_match_all: List[str] = Field(default_factory=list)
    all_setters_required: bool = False
    setters: List[str] = Field(default_factory=list)
    namespaces: List[str] = Field(default_factory=list)
    min_confidence: Union[float, None] = None


class OrderParamsModel(BaseModel):
    order_by: OrderByType = "last_modified"
    order: OrderType = None
    page: int = 1
    page_size: int = 10


class ExtractedTextFilterModel(BaseModel):
    query: str
    targets: List[str] = Field(default_factory=list)
    languages: List[str] = Field(default_factory=list)
    language_min_confidence: Union[float, None] = None
    min_confidence: Union[float, None] = None


class ExtractedTextEmbeddingsFilterModel(BaseModel):
    query: bytes
    model: str
    targets: List[str] = Field(default_factory=list)
    languages: List[str] = Field(default_factory=list)
    language_min_confidence: Union[float, None] = None
    min_confidence: Union[float, None] = None


class BookmarksFilterModel(BaseModel):
    restrict_to_bookmarks: Literal[True] = True
    namespaces: List[str] = Field(default_factory=list)


class PathTextFilterModel(BaseModel):
    query: str
    only_match_filename: bool = False


class AnyTextFilterModel(BaseModel):
    path: Union[PathTextFilterModel, None] = None
    extracted_text: Union[ExtractedTextFilterModel, None] = None


class ImageEmbeddingFilterModel(BaseModel):
    query: bytes
    model: str


class FileFiltersModel(BaseModel):
    item_types: List[str] = Field(default_factory=list)
    include_path_prefixes: List[str] = Field(default_factory=list)


class QueryFiltersModel(BaseModel):
    files: Union[FileFiltersModel, None] = None
    path: Union[PathTextFilterModel, None] = None
    extracted_text: Union[ExtractedTextFilterModel, None] = None
    extracted_text_embeddings: Union[
        ExtractedTextEmbeddingsFilterModel, None
    ] = None
    image_embeddings: Union[ImageEmbeddingFilterModel, None] = None
    any_text: Union[AnyTextFilterModel, None] = None
    bookmarks: Union[BookmarksFilterModel, None] = None


class QueryParamsModel(BaseModel):
    tags: QueryTagFiltersModel = Field(default_factory=QueryTagFiltersModel)
    filters: QueryFiltersModel = Field(default_factory=QueryFiltersModel)


class SearchQueryModel(BaseModel):
    query: QueryParamsModel = Field(default_factory=QueryParamsModel)
    order_args: OrderParamsModel = Field(default_factory=OrderParamsModel)
    count: bool = True
    check_path: bool = False


def process_results(results: List[Tuple[FileSearchResult, int]]):
    if len(results) == 0:
        return {"count": 0, "results": []}
    return {
        "count": results[0][1],
        "results": [res for res, _ in results if res],
    }


@router.get(
    "/",
    summary="Search for files in the database",
    description="""
    Search for files in the database based on the provided query parameters.
    The search query takes a `SearchQuery` object as input, which contains all the parameters supported by search.
    Search operates on `files`, which means that results are not unique by `sha256` value.
    """,
    response_model=Dict[str, Union[int, List[FileSearchResult]]],
)
def search(data: SearchQueryModel = Depends()):
    conn = get_database_connection(write_lock=False)
    results = list(search_files(conn, from_dict(SearchQuery, data.__dict__)))
    return process_results(results)


@router.get(
    "/stats",
    summary="Get statistics on the searchable data",
    description="""
    Get statistics on the data indexed in the database.
    This includes information about the tag namespaces, bookmark namespaces, file types, and folders present.
    Most importantly, it includes the list of currently existing setters for each data type.
    This information is relevant for building search queries.
    """,
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
    return {
        "setters": setters,
        "bookmarks": bookmark_namespaces,
        "files": {
            "total": files,
            "unique": items,
            "mime_types": file_types,
        },
        "tags": {
            "namespaces": tag_namespaces,
            "min_confidence": min_tags_threshold,
        },
        "folders": folders,
        "text_stats": text_stats,
    }


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
    response_model=Dict[str, List[Tuple[str, str, int, float]]],
)
def get_top_tags(
    conn=Depends(get_db_readonly),
    namespace: Optional[str] = Query(None),
    setters: List[str] = Query([]),
    confidence_threshold: Optional[float] = Query(None),
    limit: int = Query(10),
):
    return {
        "tags": get_most_common_tags_frequency(
            conn,
            namespace=namespace,
            setters=setters,
            confidence_threshold=confidence_threshold,
            limit=limit,
        )
    }


@router.get(
    "/tags",
    summary="Find a tag from a substring for autocompletion",
    description="""
    Given a string, finds tags whose names contain the string.
    Meant to be used for autocompletion in the search bar.
    The `limit` parameter can be used to control the number of tags to return.
    Returns a list of tuples, where each tuple contains the namespace, name, 
    and the number of unique items tagged with the tag.
    The tags are returned in descending order of the number of items tagged.
    """,
)
def get_tags(
    name: str = Query(...),
    limit: int = Query(10),
    conn=Depends(get_db_readonly),
):
    tags = find_tags(conn, name, limit)
    tags.sort(key=lambda x: x[2], reverse=True)
    return {"tags": tags}
