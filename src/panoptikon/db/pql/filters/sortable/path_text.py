from pydantic import BaseModel, Field
from sqlalchemy import Select, asc, desc, func, literal_column, or_, text
from sqlalchemy.sql.expression import CTE, select

from panoptikon.db.pql.filters.sortable.sortable_filter import SortableFilter
from panoptikon.db.pql.types import (
    OrderTypeNN,
    QueryState,
    get_order_by_field,
    get_order_direction_field,
    get_std_cols,
)
from panoptikon.db.search.utils import parse_and_escape_query


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
    direction: OrderTypeNN = get_order_direction_field("asc")
    match_path: MatchPathArgs = Field(
        ...,
        title="Match Path",
        description="Match a query against file paths",
    )

    def validate(self):
        if len(self.match_path.match.strip()) == 0:
            return self.set_validated(False)
        if not self.match_path.raw_fts5_match:
            self.match_path.match = parse_and_escape_query(
                self.match_path.match
            )
        return self.set_validated(True)

    def build_query(self, context: CTE, state: QueryState) -> CTE:
        self.raise_if_not_validated()
        from panoptikon.db.pql.tables import files_path_fts

        args = self.match_path
        column = (
            files_path_fts.c.filename
            if args.filename_only
            else files_path_fts.c.path
        )
        rank_column = self.derive_rank_column(literal_column("rank"))
        return self.wrap_query(
            (
                select(*get_std_cols(context, state), rank_column)
                .select_from(context)
                .join(
                    files_path_fts,
                    literal_column("files_path_fts.rowid") == context.c.file_id,
                )
                .where(column.match(args.match))
            ),
            context,
            state,
        )
