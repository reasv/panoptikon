from dataclasses import dataclass, field
from typing import List, Literal, Tuple, TypeVar, Union

from typeguard import typechecked

# Search Query Types
OrderByType = Union[
    Literal[
        "last_modified",
        "path",
        "rank_fts",
        "rank_path_fts",
        "time_added",
        "rank_any_text",
        "text_vec_distance",
        "image_vec_distance",
    ],
    None,
]

OrderType = Union[Literal["asc", "desc"], None]


@typechecked
@dataclass
class FileFilters:
    item_types: List[str] = field(default_factory=list)
    include_path_prefixes: List[str] = field(default_factory=list)


# str or bytes
Q = TypeVar("Q", str, bytes)


@typechecked
@dataclass
class ExtractedTextFilter[Q]:
    query: Q
    targets: List[Tuple[str, str]] = field(default_factory=list)
    languages: List[str] = field(default_factory=list)
    language_min_confidence: Union[float, None] = None
    min_confidence: Union[float, None] = None


@typechecked
@dataclass
class BookmarksFilter:
    restrict_to_bookmarks: Literal[True] = True
    namespaces: List[str] = field(default_factory=list)


@typechecked
@dataclass
class PathTextFilter:
    query: str
    only_match_filename: bool = False


@typechecked
@dataclass
class AnyTextFilter:
    query: str
    targets: List[Tuple[str, str]] = field(default_factory=list)


@typechecked
@dataclass
class InnerQueryTagFilters:
    positive: List[str] = field(default_factory=list)
    negative: List[str] = field(default_factory=list)
    all_setters_required: bool = False
    any_positive_tags_match: bool = False
    setters: List[str] = field(default_factory=list)
    namespaces: List[str] = field(default_factory=list)
    min_confidence: Union[float, None] = 0.5


@typechecked
@dataclass
class QueryTagFilters:
    pos_match_all: List[str] = field(default_factory=list)
    pos_match_any: List[str] = field(default_factory=list)
    neg_match_any: List[str] = field(default_factory=list)
    neg_match_all: List[str] = field(default_factory=list)
    all_setters_required: bool = False
    setters: List[str] = field(default_factory=list)
    namespaces: List[str] = field(default_factory=list)
    min_confidence: Union[float, None] = None


@typechecked
@dataclass
class QueryFilters:
    files: Union[FileFilters, None] = None
    path: Union[PathTextFilter, None] = None
    extracted_text: Union[ExtractedTextFilter[str], None] = None
    extracted_text_embeddings: Union[ExtractedTextFilter[bytes], None] = None
    any_text: Union[AnyTextFilter, None] = None
    bookmarks: Union[BookmarksFilter, None] = None


@typechecked
@dataclass
class InnerQueryParams:
    tags: InnerQueryTagFilters
    filters: QueryFilters


@typechecked
@dataclass
class QueryParams:
    tags: QueryTagFilters
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
    order_args: OrderParams = field(default_factory=OrderParams)
    count: bool = True
    check_path: bool = False
