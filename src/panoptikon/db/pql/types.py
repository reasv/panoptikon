from typing import List, Literal, Optional, Union

from pydantic import BaseModel, Field
from pypika.queries import Selectable
from sqlalchemy import CTE, Select

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


class Filter(BaseModel):
    def build_query(self, context: CTE) -> Select:
        raise NotImplementedError("build_query not implemented")


class SortableFilter(Filter):
    order_by: bool = get_order_by_field(False)
    order_direction: OrderTypeNN = get_order_direction_field("desc")
    order_priority: int = get_order_priority_field(0)


# class ExtractedTextEmbeddingsFilter(BaseModel):
#     query: bytes
#     model: str
#     targets: List[str] = Field(default_factory=list)
#     languages: List[str] = Field(default_factory=list)
#     language_min_confidence: Optional[float] = None
#     min_confidence: Optional[float] = None


# class TagFilter(BaseModel):
#     pos_match_all: List[str] = Field(default_factory=list)
#     pos_match_any: List[str] = Field(default_factory=list)
#     neg_match_any: List[str] = Field(default_factory=list)
#     neg_match_all: List[str] = Field(default_factory=list)
#     all_setters_required: bool = False
#     setters: List[str] = Field(default_factory=list)
#     namespaces: List[str] = Field(default_factory=list)
#     min_confidence: Optional[float] = None


# class ImageEmbeddingFilter(BaseModel):
#     query: bytes
#     model: str


# class AnyTextFilter(BaseModel):
#     path_text: Union[MatchPathArgs, None] = None
#     extracted_text: Union[ExtractedTextFilter, None] = None


# FieldValueType = Union[str, int, float, bool]
# FieldName = Literal["last_modified", "path"]


# class KVFilter(BaseModel):
#     k: FieldName
#     v: FieldValueType


# class KVInFilter(BaseModel):
#     k: FieldName
#     v: List[FieldValueType]


# Define filters

# class EqualsFilterModel(Filter):
#     eq: KVFilter


# class NotEqualsFilterModel(Filter):
#     neq: KVFilter


# class InFilterModel(Filter):
#     in_: KVInFilter


# class NotInFilterModel(Filter):
#     nin: KVInFilter


# class GreaterThanFilterModel(Filter):
#     gt: KVFilter


# class GreaterThanOrEqualFilterModel(Filter):
#     gte: KVFilter


# class LessThanFilterModel(Filter):
#     lt: KVFilter


# class LessThanOrEqualFilterModel(Filter):
#     lte: KVFilter


# class TagFilterModel(Filter):
#     tags: TagFilter


# class ExtractedTextFilterModel(SortableFilter):
#     order_by: bool = get_order_by_field(False)
#     order_direction: OrderTypeNN = get_order_direction_field("asc")
#     extracted_text: ExtractedTextFilter


# class ExtractedTextEmbeddingsFilterModel(SortableFilter):
#     order_by: bool = get_order_by_field(True)
#     order_direction: OrderTypeNN = get_order_direction_field("asc")
#     order_priority: int = get_order_priority_field(100)
#     extracted_text_embeddings: ExtractedTextEmbeddingsFilter


# class ImageEmbeddingFilterModel(SortableFilter):
#     order_by: bool = get_order_by_field(True)
#     order_direction: OrderTypeNN = get_order_direction_field("asc")
#     order_priority: int = get_order_priority_field(100)
#     image_embeddings: ImageEmbeddingFilter


# class AnyTextFilterModel(SortableFilter):
#     order_by: bool = get_order_by_field(False)
#     order_direction: OrderTypeNN = get_order_direction_field("desc")
#     any_text: AnyTextFilter
