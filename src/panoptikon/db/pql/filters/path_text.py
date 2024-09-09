from pydantic import BaseModel, Field
from sqlalchemy import Select, literal_column, or_, text
from sqlalchemy.sql.expression import CTE, select

from panoptikon.db.pql.tables import files_path_fts
from panoptikon.db.pql.types import (
    OrderTypeNN,
    SortableFilter,
    get_order_by_field,
    get_order_direction_field,
)
from panoptikon.db.pql.utils import Match, wrap_select


class MatchPathArgs(BaseModel):
    match: str = Field(
        ...,
        title="Match",
        description="The query to match against file paths",
    )
    filename_only: bool = Field(default=False, title="Match on filenames Only")
    raw_fts5_match: bool = Field(
        default=True,
        title="Allow raw FTS5 MATCH Syntax",
        description="If set to False, the query will be escaped before being passed to the FTS5 MATCH function",
    )


class MatchPath(SortableFilter):
    order_by: bool = get_order_by_field(False)
    order_direction: OrderTypeNN = get_order_direction_field("desc")
    match_path: MatchPathArgs = Field(
        ...,
        title="Match Path",
        description="Match a query against file paths",
    )

    def build_query(self, context: CTE) -> Select:
        args = self.match_path
        column = (
            files_path_fts.c.filename
            if args.filename_only
            else files_path_fts.c.path
        )
        return (
            select(
                context.c.file_id,
                context.c.item_id,
                literal_column("rank").label("order_rank"),
            )
            .join(
                files_path_fts,
                literal_column("files_path_fts.rowid") == context.c.file_id,
            )
            .where(column.match(args.match))
        )
