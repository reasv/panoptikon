import sqlite3
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Sequence, Union, get_args

from pydantic import BaseModel, Field
from sqlalchemy import (
    CTE,
    Column,
    ColumnClause,
    Label,
    Select,
    asc,
    desc,
    func,
    literal_column,
    over,
    select,
)
from sqlalchemy.sql.elements import KeyedColumnElement

VERY_LARGE_NUMBER = 9223372036854775805
VERY_SMALL_NUMBER = -9223372036854775805


FileColumns = Literal["file_id", "sha256", "path", "filename", "last_modified"]
ItemColumns = Literal[
    "item_id",
    "sha256",
    "md5",
    "type",
    "size",
    "width",
    "height",
    "duration",
    "time_added",
    "audio_tracks",
    "video_tracks",
    "subtitle_tracks",
]

TextColumns = Literal[
    "text_id",
    "language",
    "language_confidence",
    "text",
    "confidence",
    "text_length",
    "job_id",
    "setter_id",
    "setter_name",
    "text_index",
    "source_id",
]


OrderByType = Union[FileColumns, ItemColumns, TextColumns]

OrderType = Union[Literal["asc", "desc"], None]
OrderTypeNN = Literal["asc", "desc"]


def contains_text_columns(lst: Sequence[str]) -> bool:
    # Get the allowed values from the Literal
    literal_values = set(get_args(TextColumns))

    # Check if there's any intersection between the list and the literal values
    return bool(set(lst) & literal_values)


@dataclass
class OrderByFilter:
    cte: CTE
    direction: OrderTypeNN
    priority: int = 0


@dataclass
class ExtraColumn:
    column: ColumnClause | Column | KeyedColumnElement
    cte: CTE
    alias: str
    need_join: bool = False


@dataclass
class QueryState:
    order_list: List[OrderByFilter] = field(default_factory=list)
    extra_columns: List[ExtraColumn] = field(default_factory=list)
    cte_counter: int = 0
    is_count_query: bool = False
    is_text_query: bool = False


def get_std_cols(cte: CTE, state: QueryState) -> List[KeyedColumnElement]:
    if state.is_text_query:
        return [cte.c.item_id, cte.c.file_id, cte.c.text_id]
    return [cte.c.item_id, cte.c.file_id]


def get_std_group_by(cte: CTE, state: QueryState) -> List[KeyedColumnElement]:
    if state.is_text_query:
        return [cte.c.text_id, cte.c.file_id]
    return [cte.c.file_id]


# Define Search results
class SearchResult(BaseModel):
    file_id: int
    item_id: int
    path: Optional[str] = ""
    filename: Optional[str] = ""
    sha256: Optional[str] = ""
    last_modified: Optional[str] = ""
    type: Optional[str] = ""
    size: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None
    duration: Optional[float] = None
    time_added: Optional[str] = None
    md5: Optional[str] = None
    audio_tracks: Optional[int] = None
    video_tracks: Optional[int] = None
    subtitle_tracks: Optional[int] = None
    # Text columns (only present for text-* queries)
    text_id: Optional[int] = None  # Always present for text-* queries
    language: Optional[str] = None
    language_confidence: Optional[float] = None
    text: Optional[str] = None
    confidence: Optional[float] = None
    text_length: Optional[int] = None
    job_id: Optional[int] = None
    setter_id: Optional[int] = None
    setter_name: Optional[str] = None
    text_index: Optional[int] = None
    source_id: Optional[int] = None

    extra: Optional[Dict[str, float | int | str | None]] = Field(
        default=None,
        title="Extra Fields",
        description="Extra fields retrieved from filters that are not part of the main result object.",
    )


def map_row_to_class(row: sqlite3.Row, class_instance):
    for key, value in dict(row).items():
        if hasattr(class_instance, key):
            setattr(class_instance, key, value)


def get_extra_columns(row: sqlite3.Row, column_aliases: List[str] | None):
    if not column_aliases:
        return None
    extras: Dict[str, float | int | str | None] = {}
    for i, alias in enumerate(column_aliases):
        value = dict(row).get(f"extra_{i}")
        extras[alias] = value

    return extras if extras else None


def get_column(column: Union[FileColumns, ItemColumns, TextColumns]) -> Column:
    from panoptikon.db.pql.tables import (
        extracted_text,
        files,
        item_data,
        items,
        setters,
    )

    return {
        "file_id": files.c.id,
        "item_id": files.c.item_id,
        "text_id": extracted_text.c.id,
        "sha256": files.c.sha256,
        "path": files.c.path,
        "filename": files.c.filename,
        "last_modified": files.c.last_modified,
        "type": items.c.type,
        "size": items.c.size,
        "width": items.c.width,
        "height": items.c.height,
        "duration": items.c.duration,
        "time_added": items.c.time_added,
        "md5": items.c.md5,
        "audio_tracks": items.c.audio_tracks,
        "video_tracks": items.c.video_tracks,
        "subtitle_tracks": items.c.subtitle_tracks,
        # Text columns
        "language": extracted_text.c.language,
        "language_confidence": extracted_text.c.language_confidence,
        "text": extracted_text.c.text,
        "confidence": extracted_text.c.confidence,
        "text_length": extracted_text.c.text_length,
        "job_id": item_data.c.job_id,
        "setter_id": item_data.c.setter_id,
        "setter_name": setters.c.name,
        "text_index": item_data.c.idx,
        "source_id": item_data.c.source_id,
    }[column]


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


class SelectFields(BaseModel):
    fields: List[str] = Field(
        default_factory=list,
        title="Fields to select",
        description="The fields to select from the database.",
    )


class Operator(BaseModel):
    pass


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
