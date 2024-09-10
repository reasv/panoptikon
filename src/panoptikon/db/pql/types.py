from typing import List, Literal, Optional, Union

from pydantic import BaseModel, Field
from pypika.queries import Selectable
from sqlalchemy import (
    CTE,
    Column,
    ColumnClause,
    Select,
    asc,
    desc,
    func,
    over,
    select,
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
their values are coalesced into a single column to order by,
and the order direction is determined by the first filter that we find from this set.

It's assumed that if the filters have the same priority, and should be coalesced,
they will have the same order direction.
""",
    )


def get_order_direction_field_rownum(default: OrderTypeNN):
    return Field(
        default=default,
        title="Order Direction For Row Number",
        description="""
The order direction (asc or desc) for the internal row number calculation.
Only used if `order_by_row_n` is True.
When `order_by_row_n` is True, the filter's output is sorted by its rank_order column
following this direction, and a row number is assigned to each row.
This row number is used to order the final query.
You should generally leave this as the default value.
""",
    )


class Filter(BaseModel):
    _validated: bool = False

    def build_query(self, context: CTE) -> Select:
        raise NotImplementedError("build_query not implemented")

    def is_validated(self) -> bool:
        return self._validated

    def set_validated(self, value: bool):
        self._validated = value
        return self._validated

    def raise_if_not_validated(self):
        """Raise a ValueError if validate() has not been called.
        Raises:
            ValueError: If the filter has not been validated.
        """
        if not self.is_validated():
            raise ValueError("Filter was not validated")

    def validate(self) -> bool:
        """Pre-process filter args and validate them.
        Must return True if the filter should be included, False otherwise.
        Must be called before build_query.
        Can raise a ValueError if the filter args are invalid.
        """
        raise NotImplementedError("validate not implemented")


class SortableFilter(Filter):
    order_by: bool = get_order_by_field(False)
    order_direction: OrderTypeNN = get_order_direction_field("asc")
    order_priority: int = get_order_priority_field(0)
    order_by_row_n: bool = Field(
        default=False,
        title="Use Row Number for rank column",
        description="""
Has no effect if order_by is False.

If True, internally sorts the filter's output by its rank_order
column and assigns a row number to each row.

The row number is used to order the final query.

This is useful for combining multiple filters with different 
rank_order types that may not be directly comparable,
such as text search and embeddings search.
        """,
    )
    order_by_row_n_direction: OrderTypeNN = get_order_direction_field_rownum(
        "asc"
    )

    def get_rank_column(self, column: ColumnClause) -> ColumnClause:
        """Applies the row number function to the column if `order_by_row_n` is set.

        Args:
            column (ColumnClause): The column that this filter exposes for ordering.

        Returns:
            ColumnClause: The new sorting column that will be exposed by this filter.
            Always aliased to "order_rank".
        """
        if self.order_by and self.order_by_row_n:
            dir_str = self.order_by_row_n_direction
            direction = asc if dir_str == "asc" else desc
            column = func.row_number().over(order_by=direction(column))

        return column.label("order_rank")


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