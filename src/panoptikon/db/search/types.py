from dataclasses import dataclass, field
from typing import List, Literal, Union

from pydantic import Field
from pydantic.dataclasses import dataclass as pydantic_dataclass

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


@dataclass
class FileFilters:
    item_types: List[str] = field(default_factory=list)
    include_path_prefixes: List[str] = field(default_factory=list)


@dataclass
class ExtractedTextFilter:
    query: str
    targets: List[str] = field(default_factory=list)
    languages: List[str] = field(default_factory=list)
    language_min_confidence: Union[float, None] = None
    min_confidence: Union[float, None] = None


@dataclass
class ExtractedTextEmbeddingsFilter:
    query: bytes
    model: str
    targets: List[str] = field(default_factory=list)
    languages: List[str] = field(default_factory=list)
    language_min_confidence: Union[float, None] = None
    min_confidence: Union[float, None] = None


@dataclass
class BookmarksFilter:
    restrict_to_bookmarks: Literal[True] = True
    namespaces: List[str] = field(default_factory=list)


@dataclass
class PathTextFilter:
    query: str
    only_match_filename: bool = False


@dataclass
class AnyTextFilter:
    path: Union[PathTextFilter, None] = None
    extracted_text: Union[ExtractedTextFilter, None] = None


@dataclass
class ImageEmbeddingFilter:
    query: bytes
    model: str


@dataclass
class InnerQueryTagFilters:
    positive: List[str] = field(default_factory=list)
    negative: List[str] = field(default_factory=list)
    all_setters_required: bool = False
    any_positive_tags_match: bool = False
    setters: List[str] = field(default_factory=list)
    namespaces: List[str] = field(default_factory=list)
    min_confidence: Union[float, None] = 0.5


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


@dataclass
class QueryFilters:
    files: Union[FileFilters, None] = None
    path: Union[PathTextFilter, None] = None
    extracted_text: Union[ExtractedTextFilter, None] = None
    extracted_text_embeddings: Union[ExtractedTextEmbeddingsFilter, None] = None
    image_embeddings: Union[ImageEmbeddingFilter, None] = None
    any_text: Union[AnyTextFilter, None] = None
    bookmarks: Union[BookmarksFilter, None] = None


@dataclass
class InnerQueryParams:
    tags: InnerQueryTagFilters
    filters: QueryFilters


@dataclass
class QueryParams:
    tags: QueryTagFilters = field(default_factory=QueryTagFilters)
    filters: QueryFilters = field(default_factory=QueryFilters)


@dataclass
class OrderParams:
    order_by: OrderByType = "last_modified"
    order: OrderType = None
    page: int = 1
    page_size: int = 10


@dataclass
class SearchQuery:
    query: QueryParams = field(default_factory=QueryParams)
    order_args: OrderParams = field(default_factory=OrderParams)
    count: bool = True
    check_path: bool = False


@pydantic_dataclass
class SearchQueryModel:
    query: QueryParams = Field(default_factory=QueryParams)
    order_args: OrderParams = Field(default_factory=OrderParams)
    count: bool = True
    check_path: bool = False
