from pydantic import BaseModel
from pypika import Field
from pypika.queries import Selectable
from pypika.terms import BasicCriterion, Term

from panoptikon.db.pql.pql_model import SortableFilter
from panoptikon.db.pql.tables import files_path_fts
from panoptikon.db.pql.types import OrderTypeNN
from panoptikon.db.pql.utils import (
    Match,
    get_order_by_field,
    get_order_direction_field,
    wrap_select,
)


class MatchPathArgs(BaseModel):
    match: str
    filename_only: bool = False
    raw_fts5_match: bool = True


class MatchPath(SortableFilter):
    order_by: bool = get_order_by_field(False)
    order_direction: OrderTypeNN = get_order_direction_field("desc")
    match_path: MatchPathArgs

    def build_query(self, context: Selectable) -> Selectable:
        args = self.match_path
        query = (
            wrap_select(context)
            .join(files_path_fts)
            .on(context.file_id == files_path_fts.rowid)
            .select(Field("rank").as_("order_rank"))
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
