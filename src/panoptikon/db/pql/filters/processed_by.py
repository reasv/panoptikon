from pydantic import Field
from sqlalchemy.sql.expression import CTE, select

from panoptikon.db.pql.filters.filter import Filter
from panoptikon.db.pql.types import QueryState, get_std_cols, get_std_group_by


class ProcessedBy(Filter):
    processed_by: str = Field(
        ...,
        title="This Item or Item Data must have been processed by this setter name and have data derived from it",
    )

    def _validate(self):
        return self.set_validated(bool(self.processed_by))

    def build_query(self, context: CTE, state: QueryState) -> CTE:
        self.raise_if_not_validated()
        from panoptikon.db.pql.tables import item_data, setters

        setter = self.processed_by
        join_cond = (
            item_data.c.source_id == context.c.data_id
            if state.item_data_query
            else item_data.c.item_id == context.c.item_id
        )
        return self.wrap_query(
            select(*get_std_cols(context, state))
            .join(
                item_data,
                join_cond,
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
