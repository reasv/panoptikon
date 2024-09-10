from typing import List

from pydantic import Field
from sqlalchemy import Select, or_
from sqlalchemy.sql.expression import CTE, select

from panoptikon.db.pql.types import Filter, QueryState


class InPaths(Filter):
    in_paths: List[str] = Field(
        default_factory=list,
        title="Path must begin with one of the given strings",
    )

    def validate(self) -> bool:
        return self.set_validated(bool(self.in_paths))

    def build_query(self, context: CTE, state: QueryState) -> CTE:
        self.raise_if_not_validated()
        from panoptikon.db.pql.tables import files

        paths = self.in_paths
        return self.wrap_query(
            (
                select(context.c.file_id, context.c.item_id)
                .join(files, files.c.id == context.c.file_id)
                .where(or_(*[files.c.path.like(f"{path}%") for path in paths]))
            ),
            context,
            state,
        )
