from typing import List, Literal, Optional, Union

from pydantic import BaseModel, Field

OrderByType = Union[
    Literal["last_modified", "path"],
    None,
]

OrderType = Union[Literal["asc", "desc"], None]


# Filter arguments
class BookmarksFilter(BaseModel):
    enable: bool = True
    namespaces: List[str] = Field(default_factory=list)
    user: str = "user"
    include_wildcard: bool = True


class PathTextFilter(BaseModel):
    query: str
    only_match_filename: bool = False
    raw_fts5_match: bool = True


class ExtractedTextFilter(BaseModel):
    query: str
    targets: List[str] = Field(default_factory=list)
    languages: List[str] = Field(default_factory=list)
    language_min_confidence: Optional[float] = None
    min_confidence: Optional[float] = None
    raw_fts5_match: bool = True


class ExtractedTextEmbeddingsFilter(BaseModel):
    query: bytes
    model: str
    targets: List[str] = Field(default_factory=list)
    languages: List[str] = Field(default_factory=list)
    language_min_confidence: Optional[float] = None
    min_confidence: Optional[float] = None


class TagFilter(BaseModel):
    pos_match_all: List[str] = Field(default_factory=list)
    pos_match_any: List[str] = Field(default_factory=list)
    neg_match_any: List[str] = Field(default_factory=list)
    neg_match_all: List[str] = Field(default_factory=list)
    all_setters_required: bool = False
    setters: List[str] = Field(default_factory=list)
    namespaces: List[str] = Field(default_factory=list)
    min_confidence: Optional[float] = None


class ImageEmbeddingFilter(BaseModel):
    query: bytes
    model: str


class AnyTextFilter(BaseModel):
    path_text: Union[PathTextFilter, None] = None
    extracted_text: Union[ExtractedTextFilter, None] = None


FieldValueType = Union[str, int, float, bool]
FieldName = Literal["last_modified", "path"]


class KVFilter(BaseModel):
    k: FieldName
    v: FieldValueType


class KVInFilter(BaseModel):
    k: FieldName
    v: List[FieldValueType]


# Define filters
class Filter(BaseModel):
    pass


class EqualsFilterModel(Filter):
    eq: KVFilter


class NotEqualsFilterModel(Filter):
    neq: KVFilter


class InFilterModel(Filter):
    in_: KVInFilter


class NotInFilterModel(Filter):
    nin: KVInFilter


class GreaterThanFilterModel(Filter):
    gt: KVFilter


class GreaterThanOrEqualFilterModel(Filter):
    gte: KVFilter


class LessThanFilterModel(Filter):
    lt: KVFilter


class LessThanOrEqualFilterModel(Filter):
    lte: KVFilter


class BookmarksFilterModel(Filter):
    bookmarks: BookmarksFilter


class PathTextFilterModel(Filter):
    path_text: PathTextFilter


class ExtractedTextFilterModel(Filter):
    extracted_text: ExtractedTextFilter


class ExtractedTextEmbeddingsFilterModel(Filter):
    extracted_text_embeddings: ExtractedTextEmbeddingsFilter


class ImageEmbeddingFilterModel(Filter):
    image_embeddings: ImageEmbeddingFilter


class TagFilterModel(Filter):
    tags: TagFilter


class AnyTextFilterModel(Filter):
    any_text: AnyTextFilter


class PathFilterModel(Filter):
    in_paths: List[str] = Field(default_factory=list)


class TypeFilterModel(Filter):
    mime_types: List[str] = Field(default_factory=list)


class Operator(BaseModel):
    pass


# Define operators
class AndOperator(Operator):
    and_: List["QueryElement"]


class OrOperator(Operator):
    or_: List["QueryElement"]


class NotOperator(Operator):
    not_: "QueryElement"


Filters = Union[
    BookmarksFilterModel,
    PathTextFilterModel,
    ExtractedTextFilterModel,
    ExtractedTextEmbeddingsFilterModel,
    ImageEmbeddingFilterModel,
    TagFilterModel,
    AnyTextFilterModel,
    PathFilterModel,
    TypeFilterModel,
    EqualsFilterModel,
    NotEqualsFilterModel,
    InFilterModel,
    NotInFilterModel,
    GreaterThanFilterModel,
    GreaterThanOrEqualFilterModel,
    LessThanFilterModel,
    LessThanOrEqualFilterModel,
]

QueryElement = Union[
    Filters,
    AndOperator,
    OrOperator,
    NotOperator,
]


class OrderParams(BaseModel):
    order_by: OrderByType = "last_modified"
    order: OrderType = None
    page: int = 1
    page_size: int = 10


# Use model_rebuild for Pydantic v2
AndOperator.model_rebuild()
OrOperator.model_rebuild()
NotOperator.model_rebuild()


class SearchQuery(BaseModel):
    query: Optional[QueryElement] = None
    order_args: OrderParams = Field(default_factory=OrderParams)
    count: bool = True
    check_path: bool = False


# # Example usage
# example_query = AndOperator(
#     and_=[
#         BookmarksFilterModel(
#             bookmarks=BookmarksFilter(namespaces=["namespace1"])
#         ),
#         NotOperator(
#             not_=PathTextFilterModel(path_text=PathTextFilter(query="example"))
#         ),
#     ]
# )

# model_dict = example_query.model_dump_json(indent=2)
# print(model_dict)
