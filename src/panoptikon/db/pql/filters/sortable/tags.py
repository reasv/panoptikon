from typing import List

from pydantic import BaseModel, Field
from sqlalchemy import String, and_, distinct, func, or_
from sqlalchemy.sql.expression import CTE, select

from panoptikon.db.pql.filters.sortable.sortable_filter import SortableFilter
from panoptikon.db.pql.types import (
    OrderTypeNN,
    QueryState,
    get_order_by_field,
    get_order_direction_field,
    get_order_direction_field_rownum,
    get_std_cols,
    get_std_group_by,
)


class TagsArgs(BaseModel):
    tags: List[str] = Field(
        default_factory=list,
        title="List of tags to match",
    )
    match_any: bool = Field(
        default=False,
        title="Match any tag",
        description="""
If true, match items with at least one of the given tags.
If false (default), only match items with all of the given tags.
""",
    )
    min_confidence: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        title="Minimum confidence",
        description="""
Only consider tags with a confidence greater than or equal to this value
""",
    )
    setters: List[str] = Field(
        default_factory=list,
        title="Only consider tags set by these setters",
    )
    namespaces: List[str] = Field(
        default_factory=list,
        title="Only consider tags in these namespaces (includes sub-namespaces)",
    )
    all_setters_required: bool = Field(
        default=False,
        title="Require all setters to match",
        description="""
Only consider tags that have been set by all of the given setters.
If match_any is true, and there is more than one tag, this will be ignored.

If you really want to match any tag set by all of the given setters,
you can combine this with a separate filter for each tag in an OrOperator.
""",
    )


class MatchTags(SortableFilter):
    order_by: bool = get_order_by_field(False)
    direction: OrderTypeNN = get_order_direction_field("desc")
    row_n_direction: OrderTypeNN = get_order_direction_field_rownum("desc")
    match_tags: TagsArgs

    def _validate(self):
        if not self.match_tags.tags:
            return self.set_validated(False)
        if self.match_tags.all_setters_required and not self.match_tags.setters:
            # If we require all setters to match, we need to know what setters to match
            self.match_tags.all_setters_required = False

        return self.set_validated(True)

    def build_query(self, context: CTE, state: QueryState) -> CTE:
        self.raise_if_not_validated()
        from panoptikon.db.pql.tables import (
            item_data,
            setters,
            tags,
            tags_items,
        )

        args = self.match_tags
        conditions = [tags.c.name.in_(args.tags)]
        if args.min_confidence:
            conditions.append(tags_items.c.confidence >= args.min_confidence)
        if args.setters:
            conditions.append(setters.c.name.in_(args.setters))
        if args.namespaces:
            conditions.append(
                or_(
                    *[tags.c.namespace.like(f"{ns}%") for ns in args.namespaces]
                )
            )
        # Number of tags to match
        having_clause = [func.count(tags.c.name.distinct()) == len(args.tags)]
        if args.all_setters_required:
            # Tag-setter pairs to match
            # if we require all setters to be present for all tags
            having_clause = [
                func.count(
                    distinct(
                        item_data.c.setter_id.op("||")("-").op("||")(
                            tags.c.name
                        )
                    )
                )
                == len(args.tags) * len(args.setters)
            ]
        if args.match_any and len(args.tags) > 1:
            having_clause = []

        avg_confidence = func.avg(tags_items.c.confidence)
        matching_items_select = (
            select(
                *get_std_cols(context, state),
                self.derive_rank_column(avg_confidence),
            )
            .join(
                item_data,
                (item_data.c.item_id == context.c.item_id)
                & (item_data.c.data_type == "tags"),
            )
            .join(setters, item_data.c.setter_id == setters.c.id)
            .join(tags_items, tags_items.c.item_data_id == item_data.c.id)
            .join(tags, tags.c.id == tags_items.c.tag_id)
            .where(and_(*conditions))
            .group_by(*get_std_group_by(context, state))
            .having(*having_clause)
        )
        matching_items = matching_items_select.cte(
            f"match_{self.get_cte_name(state.cte_counter)}"
        )
        join_condition = (
            context.c.data_id == matching_items.c.data_id
            if state.item_data_query
            else context.c.file_id == matching_items.c.file_id
        )
        return self.wrap_query(
            select(
                *get_std_cols(context, state),
                matching_items.c.order_rank,
            ).join(
                matching_items,
                join_condition,
            ),
            context,
            state,
        )
