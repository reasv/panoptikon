from pypika import Field
from pypika.queries import Selectable
from pypika.terms import BasicCriterion, Term

from panoptikon.db.pql.pql_model import PathTextFilterModel
from panoptikon.db.pql.utils import Match, files_path_fts_table, wrap_select


def path_text_filter(
    filter: PathTextFilterModel, context: Selectable
) -> Selectable:
    query = (
        wrap_select(context)
        .join(files_path_fts_table)
        .on(context.file_id == files_path_fts_table.rowid)
        .select(Field("rank").as_("order_rank"))
    )
    column = (
        files_path_fts_table.filename
        if filter.path_text.filename_only
        else files_path_fts_table.path
    )
    query = query.where(
        BasicCriterion(
            Match.match_,
            column,
            Term.wrap_constant(filter.path_text.query),  # type: ignore
        )
    )
    return query
