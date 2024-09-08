from typing import List, Literal, Optional, Union

from pydantic import BaseModel, Field
from pypika.queries import Selectable

from panoptikon.db.pql.filters.bookmarks import InBookmarks
from panoptikon.db.pql.filters.path_in import InPaths
from panoptikon.db.pql.filters.path_text import MatchPath, MatchPathArgs
from panoptikon.db.pql.filters.type_in import TypeIn
from panoptikon.db.pql.utils import (
    get_order_by_field,
    get_order_direction_field,
    get_order_priority_field,
)

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
    path_text: Union[MatchPathArgs, None] = None
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
    def build_query(self, context: Selectable) -> Selectable:
        raise NotImplementedError("build_query not implemented")


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
    InPaths,
    InBookmarks,
    TypeIn,
    MatchPath,
]

Operators = Union[AndOperator, OrOperator, NotOperator]
QueryElement = Union[Filters, Operators]


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
