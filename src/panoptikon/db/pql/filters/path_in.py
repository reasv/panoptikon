from pypika import Criterion
from pypika.queries import Selectable

from panoptikon.db.pql.pql_model import PathFilterModel
from panoptikon.db.pql.utils import files_table, wrap_select


def path_in(filter: PathFilterModel, context: Selectable) -> Selectable:
    query = (
        wrap_select(context)
        .join(files_table)
        .on(files_table.id == context.file_id)
        .where(
            Criterion.any(
                [files_table.path.like(f"{path}%") for path in filter.in_paths]
            )
        )
    )
    return query
