from typing import List

from pydantic import Field
from pypika import Criterion
from pypika.queries import Selectable

from panoptikon.db.pql.pql_model import Filter
from panoptikon.db.pql.tables import files
from panoptikon.db.pql.utils import wrap_select


class InPaths(Filter):
    in_paths: List[str] = Field(default_factory=list)

    def build_query(self, context: Selectable) -> Selectable:
        paths = self.in_paths
        query = (
            wrap_select(context)
            .join(files)
            .on(files.id == context.file_id)
            .where(
                Criterion.any([files.path.like(f"{path}%") for path in paths])
            )
        )
        return query
