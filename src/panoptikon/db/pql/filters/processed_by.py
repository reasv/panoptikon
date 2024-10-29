from typing import List

from pydantic import Field
from sqlalchemy.sql.expression import CTE, select

from panoptikon.db.pql.filters.filter import Filter
from panoptikon.db.pql.types import QueryState, get_std_cols, get_std_group_by


class ProcessedBy(Filter):
    processed_by: str = Field(
        ...,
        title="This Item Data must have been processed by this setter name and have derived data from it",
    )

    def _validate(self):
        if not self.processed_by:
            return self.set_validated(False)
        return self.set_validated(True)

    def build_query(self, context: CTE, state: QueryState) -> CTE:
        self.raise_if_not_validated()
        from panoptikon.db.pql.tables import item_data, setters

        if not state.item_data_query:
            raise ValueError(
                "ProcessedBy filter only works with Item Data queries such as 'text' entity queries"
            )
        setter = self.processed_by
        return self.wrap_query(
            select(*get_std_cols(context, state))
            .join(
                item_data,
                item_data.c.source_id == context.c.data_id,
            )
            .join(
                setters,
                setters.c.id == item_data.c.setter_id,
            )
            .where(setters.c.name == setter)
            .group_by(
                *get_std_group_by(context, state),
            ),
            context,
            state,
        )
