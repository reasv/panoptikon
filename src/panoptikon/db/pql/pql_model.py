from typing import List, Literal, Optional, Union

from pydantic import BaseModel, Field

from panoptikon.db.pql.filters import Filters
from panoptikon.db.pql.types import (
    FileColumns,
    ItemColumns,
    Operator,
    OrderByType,
    OrderType,
    TextColumns,
)


class AndOperator(Operator):
    and_: List["QueryElement"]


class OrOperator(Operator):
    or_: List["QueryElement"]


class NotOperator(Operator):
    not_: "QueryElement"


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


class PQLQuery(BaseModel):
    query: Optional[QueryElement] = None
    order_by: List[OrderArgs] = Field(
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
    select: List[Union[FileColumns, ItemColumns, TextColumns]] = Field(
        default_factory=lambda: ["sha256", "path", "last_modified", "type"],
        title="Data to return",
        description="""
The columns to return in the query.
The default columns are sha256, path, last_modified, and type.
Columns belonging to text can only be selected if the entity is "text".
""",
    )
    entity: Literal["file", "text"] = Field(
        default="file",
        title="Target Entity",
        description="""
The entity to query on.
You can perform the search on either files or text.
This means that intermediate results will be one per file, or one per text-file pair.
There are generally more text-file pairs than files, so this incurs overhead.

However, "text" queries allow you to include text-specific columns in the select list.
The final results will also be one for each text-file pair.

Most of the same filters can be used on both.
"text" queries will include "data_id" in each result. "file_id" and "item_id" are always included.
""",
    )
    partition_by: Optional[
        List[Union[FileColumns, ItemColumns, TextColumns]]
    ] = Field(
        default=None,
        title="Partition results By",
        description="""
Group results by the values of the specified column(s) and return the first result
for each group according to all of the order settings of the query.

For example, if you partition by "item_id", you'll get one result per unique item.
If you partition by "file_id", you'll get one result per unique file.
Multiple columns yield one result for each unique combination of values for those columns.

You cannot partition by text columns if the entity is "file".
""",
    )
    page: int = Field(default=1)
    page_size: int = Field(default=10)
    count: bool = Field(
        default=True,
        title="Count Results",
        description="""
If true, the query will return the total number of results that match the query.
This is useful for pagination, but it requires an additional query to be executed.
""",
    )
    results: bool = Field(
        default=True,
        title="Return Results",
        description="""
If true, the query will return the results that match the query.
If false, only the total count will be returned, if requested.
""",
    )
    check_path: bool = Field(
        default=False,
        title="Check Paths Exist",
        description="""
If true, the query will check if the path exists on disk before returning it.

For `file` queries with no partition by,
the result will be omitted if the path does not exist.
This is because if another file exists, it will be included later in the results.

In other cases, the system will try to find another file for the item and substitute it.
If no other working path is found, the result will be omitted.

This is not reflected in the total count of results.
""",
    )
