from dataclasses import dataclass
from itertools import count
from os import path
from tabnanny import check
from typing import List, Literal, Tuple, TypeVar
from warnings import filters

from sqlalchemy import any_
from sympy import Q
from typeguard import typechecked

from src.db import tags


@dataclass
class FileScanData:
    sha256: str
    md5: str
    mime_type: str
    last_modified: str
    size: int
    path: str
    path_in_db: bool
    modified: bool


@dataclass
class ItemWithPath:
    sha256: str
    md5: str
    type: str
    size: int
    time_added: str
    path: str


@dataclass
class ExtractedText:
    item_sha256: str
    model_type: str
    setter: str
    language: str
    text: str
    confidence: float | None
    score: float


OrderByType = (
    Literal[
        "last_modified",
        "path",
        "rank_fts",
        "rank_path_fts",
        "time_added",
        "rank_any_text",
        "text_vec_distance",
        "image_vec_distance",
    ]
    | None
)

OrderType = Literal["asc", "desc"] | None


@dataclass
class FileSearchResult:
    path: str
    sha256: str
    last_modified: str
    type: str


@dataclass
class FileScanRecord:
    id: int
    start_time: str
    end_time: str
    path: str
    total_available: int
    new_items: int
    unchanged_files: int
    new_files: int
    modified_files: int
    marked_unavailable: int
    errors: int


@dataclass
class FileRecord:
    sha256: str
    path: str
    last_modified: str


@dataclass
class LogRecord:
    id: int
    start_time: str
    end_time: str
    type: str
    setter: str
    threshold: float | None
    batch_size: int
    image_files: int
    video_files: int
    other_files: int
    total_segments: int
    errors: int
    total_remaining: int


# Search Query Types
@dataclass
class FileParams:
    item_types: List[str] = []
    include_path_prefixes: List[str] = []


# str or bytes
Q = TypeVar("Q", str, bytes)


@dataclass
class ExtractedTextParams[Q]:
    query: Q
    targets: List[Tuple[str, str]] = []
    languages: List[str] = []
    language_min_confidence: float | None = None
    min_confidence: float | None = None


@dataclass
class BookmarkParams:
    restrict_to_bookmarks: Literal[True] = True
    namespaces: List[str] = []


@dataclass
class PathQueryParams:
    query: str
    only_match_filename: bool = False


@dataclass
class AnyTextParams:
    query: str
    targets: List[Tuple[str, str]] = []


@dataclass
class InnerQueryTagParams:
    positive: List[str]
    negative: List[str] = []
    all_setters_required: bool = False
    any_positive_tags_match: bool = False
    setters: List[str] = []
    namespaces: List[str] = []
    min_confidence: float | None = 0.5


@dataclass
class QueryTagParams:
    pos_match_all: List[str] = []
    pos_match_any: List[str] = []
    neg_match_any: List[str] = []
    neg_match_all: List[str] = []
    all_setters_required: bool = False
    setters: List[str] = []
    namespaces: List[str] = []
    min_confidence: float | None = None


@dataclass
class QueryFilters:
    files: FileParams | None = None
    path: PathQueryParams | None = None
    extracted_text: ExtractedTextParams[str] | None = None
    extracted_text_embeddings: ExtractedTextParams[bytes] | None = None
    any_text: AnyTextParams | None = None
    bookmarks: BookmarkParams | None = None


@dataclass
class QueryParams:
    tags: QueryTagParams
    filters: QueryFilters


@dataclass
class InnerQueryParams:
    tags: InnerQueryTagParams
    filters: QueryFilters


@dataclass
class OrderParams:
    order_by: OrderByType = "last_modified"
    order: OrderType = None
    page: int = 1
    page_size: int = 10


@typechecked
@dataclass
class SearchQuery:
    query: QueryParams
    order_args: OrderParams = OrderParams()
    count: bool = True
    check_path: bool = False
