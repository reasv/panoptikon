from pydantic import BaseModel, Field
from pypika import Field as SQLField
from pypika.queries import Selectable
from pypika.terms import BasicCriterion, Term

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

    def build_query(self, context: Selectable) -> Selectable:
        args = self.match_path
        query = (
            wrap_select(context)
            .join(files_path_fts)
            .on(context.file_id == files_path_fts.rowid)
            .select(SQLField("rank").as_("order_rank"))
        )
        column = (
            files_path_fts.filename
            if args.filename_only
            else files_path_fts.path
        )
        query = query.where(
            BasicCriterion(
                Match.match_,
                column,
                Term.wrap_constant(args.match),  # type: ignore
            )
        )
        return query
