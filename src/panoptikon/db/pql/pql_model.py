from typing import List, Literal, Optional, Union

from pydantic import BaseModel, Field

OrderByType = Literal[
    "last_modified",
    "path",
    "type",
    "size",
    "filename",
    "width",
    "height",
    "duration",
    "time_added",
]


OrderType = Union[Literal["asc", "desc"], None]
OrderTypeNN = Literal["asc", "desc"]


# Filter arguments
class BookmarksFilter(BaseModel):
    require: bool = True
    namespaces: List[str] = Field(default_factory=list)
    user: str = "user"
    include_wildcard: bool = True


class PathTextFilter(BaseModel):
    query: str
    filename_only: bool = False
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


def get_order_by_field(default: bool):
    return Field(
        default=default,
        title="Order by this filter's rank output",
        description="This filter generates a value that can be used for ordering.",
    )


def get_order_direction_field(default: OrderTypeNN):
    return Field(
        default=default,
        title="Order Direction",
        description="""
The order direction for this filter.
If not set, the default order direction for this field is used.
""",
    )


def get_order_priority_field(default: int):
    return Field(
        default=default,
        title="Order By Priority",
        description="""
The priority of this filter in the order by clause.
If there are multiple filters with order_by set to True,
the priority is used to determine the order.
If two filter order bys have the same priority,
their values are coalesced into a single column to order by
""",
    )


class SortableFilter(Filter):
    order_by: bool = get_order_by_field(False)
    order_direction: OrderTypeNN = get_order_direction_field("desc")
    order_priority: int = get_order_priority_field(0)


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


class TagFilterModel(Filter):
    tags: TagFilter


class BookmarksFilterModel(SortableFilter):
    order_by: bool = get_order_by_field(False)
    order_direction: OrderTypeNN = get_order_direction_field("desc")
    bookmarks: BookmarksFilter


class PathTextFilterModel(SortableFilter):
    order_by: bool = get_order_by_field(False)
    order_direction: OrderTypeNN = get_order_direction_field("desc")
    path_text: PathTextFilter


class ExtractedTextFilterModel(SortableFilter):
    order_by: bool = get_order_by_field(False)
    order_direction: OrderTypeNN = get_order_direction_field("asc")
    extracted_text: ExtractedTextFilter


class ExtractedTextEmbeddingsFilterModel(SortableFilter):
    order_by: bool = get_order_by_field(True)
    order_direction: OrderTypeNN = get_order_direction_field("asc")
    order_priority: int = get_order_priority_field(100)
    extracted_text_embeddings: ExtractedTextEmbeddingsFilter


class ImageEmbeddingFilterModel(SortableFilter):
    order_by: bool = get_order_by_field(True)
    order_direction: OrderTypeNN = get_order_direction_field("asc")
    order_priority: int = get_order_priority_field(100)
    image_embeddings: ImageEmbeddingFilter


class AnyTextFilterModel(SortableFilter):
    order_by: bool = get_order_by_field(False)
    order_direction: OrderTypeNN = get_order_direction_field("desc")
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


class OrderArgs(BaseModel):
    order_by: OrderByType = "last_modified"
    order: OrderType = None
    priority: int = Field(
        default=0,
        title="Order Priority",
        description="""
The priority of this order by field. If multiple fields are ordered by,
the priority is used to determine the order they are applied in.
The order in the list is used if the priority is the same.
""",
    )


AndOperator.model_rebuild()
OrOperator.model_rebuild()
NotOperator.model_rebuild()


class SearchQuery(BaseModel):
    query: Optional[QueryElement] = None
    order_args: List[OrderArgs] = Field(
        default_factory=lambda: [
            OrderArgs(order_by="last_modified", order="desc")
        ],
        title="Values to order results by",
        description="""
The order_args field is a list of { order_by: [field name], order: ["asc" or "desc"] }
objects that define how the results should be ordered.
Results can be ordered by multiple fields by adding multiple objects.
        """,
    )
    page: int = 1
    page_size: int = 10
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
