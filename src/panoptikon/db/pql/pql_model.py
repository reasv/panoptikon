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
    select: List[Union[FileColumns, ItemColumns, TextColumns]] = Field(
        default_factory=lambda: ["sha256", "path", "last_modified", "type"],
        title="Data to return",
        description="""
The columns to return in the query.
The default columns are sha256, path, last_modified, and type.
These columns are always returned, even if they are not in the select list.
""",
    )
    entity: Literal["file", "item", "text-item", "text-file"] = Field(
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

`Text-item` queries are used to search extracted text for items.
You'll get one result for each text, containing the text_id, 
text itself, as well as all the normal item columns.
Since each text is associated to exactly one item, 
you can use all the normal filters, orders, 
and selects as well as the text-specific ones.

`text-file` queries are used to search extracted text for files.
The result is exactly the same as a text-item query, but you'll get one result for each text-file pair.
There is exactly one item for each text, so text-item returns one result per text,
but there can be multiple files for the same unique item, and consequently multiple files for the same text,
meaning multiple results with the same text.

Using text-item involves the same tradeoffs as item vs file queries, so file-text is recommended for the most consistent results.
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
