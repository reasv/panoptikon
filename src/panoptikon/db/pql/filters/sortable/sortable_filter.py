from typing import Any, Optional

from pydantic import Field
from sqlalchemy import (
    CTE,
    Column,
    ColumnClause,
    Label,
    Select,
    asc,
    desc,
    func,
    literal_column,
    over,
    select,
)

from panoptikon.db.pql.filters.filter import Filter
from panoptikon.db.pql.types import (
    ExtraColumn,
    OrderTypeNN,
    QueryState,
    get_order_by_field,
    get_order_direction_field,
    get_order_direction_field_rownum,
    get_order_priority_field,
)


class SortableFilter(Filter):
    order_by: bool = get_order_by_field(False)
    direction: OrderTypeNN = get_order_direction_field("asc")
    priority: int = get_order_priority_field(0)
    row_n: bool = Field(
        default=False,
        title="Use Row Number for rank column",
        description="""
Only applied if either order_by is True, or select_as is set.

If True, internally sorts the filter's output by its rank_order
column and assigns a row number to each row.

The row number is used to order the final query.

This is useful for combining multiple filters with different 
rank_order types that may not be directly comparable,
such as text search and embeddings search.
        """,
    )
    row_n_direction: OrderTypeNN = get_order_direction_field_rownum("asc")
    gt: Optional[int | str | float] = Field(
        default=None,
        title="Order By Greater Than",
        description="""
If set, only include items with an order_rank greater than this value.
Can be used for cursor-based pagination.
The type depends on the filter.
Will be ignored in the count query, which is 
used to determine the total number of results when count = True.
With cursor-based pagination, you should probably not rely on count = True anyhow.
        """,
    )
    lt: Optional[int | str | float] = Field(
        default=None,
        title="Order By Less Than",
        description="""
If set, only include items with an order_rank less than this value.
Can be used for cursor-based pagination.
The type depends on the filter.
Will be ignored in the count query, which is 
used to determine the total number of results when count = True.
        """,
    )
    select_as: Optional[str] = Field(
        default=None,
        title="Order By Select As",
        description="""
If set, the order_rank column will be returned with the results as this alias under the "extra" object.
""",
    )

    def derive_rank_column(self, column: Any) -> ColumnClause | Label:
        """Applies the row number function to the column if `order_by_row_n` is set.

        Args:
            column (ColumnClause): The column that this filter exposes for ordering.

        Returns:
            ColumnClause: The new sorting column that will be exposed by this filter.
            Always aliased to "order_rank".
        """
        if self.row_n and (self.order_by or self.select_as):
            dir_str = self.row_n_direction
            direction = asc if dir_str == "asc" else desc
            column = func.row_number().over(order_by=direction(column))

        return column.label("order_rank")

    def wrap_query(self, query: Select, context: CTE, state: QueryState) -> CTE:
        if state.is_count_query:
            return super().wrap_query(query, context, state)

        order_rank = literal_column("order_rank")
        if self.gt or self.lt:
            query = select(
                query.alias(f"wrapped_{self.get_cte_name(state.cte_counter)}")
            )
            if self.gt:
                query = query.where(order_rank > self.gt)
            if self.lt:
                query = query.where(order_rank < self.lt)

        cte = super().wrap_query(query, context, state)
        if self.select_as:
            state.extra_columns.append(
                ExtraColumn(
                    column=cte.c.order_rank,
                    cte=cte,
                    alias=self.select_as,
                    need_join=not self.order_by,
                )
            )
        return cte