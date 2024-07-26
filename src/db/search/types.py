from dataclasses import dataclass
from typing import List, Literal, Tuple, TypeVar

from typeguard import typechecked

# Search Query Types
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


@typechecked
@dataclass
class FileParams:
    item_types: List[str] = []
    include_path_prefixes: List[str] = []


# str or bytes
Q = TypeVar("Q", str, bytes)


@typechecked
@dataclass
class ExtractedTextParams[Q]:
    query: Q
    targets: List[Tuple[str, str]] = []
    languages: List[str] = []
    language_min_confidence: float | None = None
    min_confidence: float | None = None


@typechecked
@dataclass
class BookmarkParams:
    restrict_to_bookmarks: Literal[True] = True
    namespaces: List[str] = []


@typechecked
@dataclass
class PathQueryParams:
    query: str
    only_match_filename: bool = False


@typechecked
@dataclass
class AnyTextParams:
    query: str
    targets: List[Tuple[str, str]] = []


@typechecked
@dataclass
class InnerQueryTagParams:
    positive: List[str]
    negative: List[str] = []
    all_setters_required: bool = False
    any_positive_tags_match: bool = False
    setters: List[str] = []
    namespaces: List[str] = []
    min_confidence: float | None = 0.5


@typechecked
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


@typechecked
@dataclass
class QueryFilters:
    files: FileParams | None = None
    path: PathQueryParams | None = None
    extracted_text: ExtractedTextParams[str] | None = None
    extracted_text_embeddings: ExtractedTextParams[bytes] | None = None
    any_text: AnyTextParams | None = None
    bookmarks: BookmarkParams | None = None


@typechecked
@dataclass
class QueryParams:
    tags: QueryTagParams
    filters: QueryFilters


@typechecked
@dataclass
class InnerQueryParams:
    tags: InnerQueryTagParams
    filters: QueryFilters


@typechecked
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
