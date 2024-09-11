from typing import List, Literal, Optional, Union

from pydantic import BaseModel, Field

from panoptikon.db.pql.filters import Filters
from panoptikon.db.pql.types import (
    FileColumns,
    ItemColumns,
    OrderByType,
    OrderType,
    TextColumns,
)


class Operator(BaseModel):
    pass


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
These columns are always returned, even if they are not in the select list.
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
"text" queries will include "text_id" in each result. "file_id" and "item_id" are always included.
""",
    )
    partition_by: Optional[List[Literal["text_id", "file_id", "item_id"]]] = (
        Field(
            default=None,
            title="Partition results By",
            description="""
Group results by the specified column(s) and return the first result for each group according to all of the order settings of the query.
""",
        )
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
    check_path: bool = Field(
        default=False,
        title="Check Paths Exist",
        description="""
If true, the query will check if the path exists on disk before returning it.

For `file` queries, the file will be omitted if the path does not exist.

For `item` queries, the system will try to find another path for the item and substitute it.
This will substitute last_modified to the new path's last_modified, but not any other fields such as filename.

If no other working path is found, the item will be omitted from the results.
""",
    )
