from typing import List, Literal, Optional, Union

from pydantic import BaseModel, Field

from panoptikon.db.pql.filters import Filters
from panoptikon.db.pql.types import (
    FileColumns,
    ItemColumns,
    OrderByType,
    OrderType,
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
    select: List[Union[FileColumns, ItemColumns]] = Field(
        default_factory=lambda: ["sha256", "path", "last_modified", "type"],
        title="Data to return",
        description="""
The columns to return in the query.
""",
    )
    entity: Literal["file", "item"] = Field(
        default="file",
        title="Target Entity",
        description="""
The entity to query for.
`Items` are unique files.
`Files` represent actual files on disk. They are unique by path.
If you search for files, you will get duplicates for items that have multiple identical files.
If you search for items, you will get one result per item, even if multiple identical files exist with different paths.

When searching for items, sorting by path or last_modified may not work as expected.
The file with the highest internal ID will be returned with the item.

Searching files is generally faster than searching items.
""",
    )
    page: int = Field(default=1)
    page_size: int = Field(default=10)
    count: bool = Field(default=True)
    check_path: bool = Field(default=False)
