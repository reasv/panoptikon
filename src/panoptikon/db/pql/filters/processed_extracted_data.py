from typing import List

from pydantic import BaseModel, Field
from sqlalchemy import Select, and_, exists, not_, or_
from sqlalchemy.sql.expression import CTE, select

from panoptikon.db.pql.filters.filter import Filter
from panoptikon.db.pql.types import QueryState, get_std_cols, get_std_group_by


class DerivedDataArgs(BaseModel):
    setter_name: str = Field(
        ...,
        title="Name of the setter that would produce the derived data",
    )
    data_types: List[str] = Field(
        ...,
        title="Data types that the associated data must have",
    )


class HasUnprocessedData(Filter):
    has_data_unprocessed: DerivedDataArgs = Field(
        ...,
        title="Item must have item_data of given types that has not been processed by the given setter name",
    )

    def _validate(self):
        if (
            not self.has_data_unprocessed.data_types
            or not self.has_data_unprocessed.setter_name
        ):
            return self.set_validated(False)
        return self.set_validated(True)

    def build_query(self, context: CTE, state: QueryState) -> CTE:
        self.raise_if_not_validated()
        from panoptikon.db.pql.tables import files, item_data, items, setters

        src_data = item_data.alias("src_item_data")
        derived = item_data.alias("derived_data")
        setter = self.has_data_unprocessed.setter_name
        data_types = self.has_data_unprocessed.data_types
        not_exists_subquery = (
            select(1)
            .select_from(derived)
            .join(setters, derived.c.setter_id == setters.c.id)
            .where(
                and_(
                    derived.c.source_id == src_data.c.id,
                    setters.c.name == setter,
                )
            )
        )
        return self.wrap_query(
            select(*get_std_cols(context, state))
            .join(
                src_data,
                src_data.c.item_id == context.c.item_id,
            )
            .where(
                and_(
                    src_data.c.data_type.in_(data_types),
                    src_data.c.is_placeholder == 0,
                    not_(exists(not_exists_subquery)),
                )
            )
            .group_by(
                *get_std_group_by(context, state),
            ),
            context,
            state,
        )
