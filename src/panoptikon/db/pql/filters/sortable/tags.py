from typing import List

from pydantic import BaseModel, Field
from sqlalchemy import Select, all_, and_, distinct, func, or_
from sqlalchemy.sql.expression import CTE, select
from torch import cond

from panoptikon.db import get_database_connection
from panoptikon.db.extraction_log import get_existing_setters
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
            conn = get_database_connection(write_lock=False)
            setters = get_existing_setters(conn)
            tag_setters = [name for type, name in setters if type == "tags"]
            self.match_tags.setters = tag_setters

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
                        func.concat(item_data.c.setter_id, "-", tags.c.name)
                    )
                )
                == len(args.tags) * len(args.setters)
            ]
        if args.match_any and len(args.tags) > 1:
            having_clause = []

        unique_items = select(context.c.item_id.distinct()).cte(
            f"unqitems_{self.get_cte_name(state.cte_counter)}"
        )
        avg_confidence = func.avg(tags_items.c.confidence).label(
            "avg_confidence"
        )
        matching_items = (
            select(
                unique_items.c.item_id,
                avg_confidence,
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
            .group_by(unique_items.c.item_id)
            .having(*having_clause)
        ).cte(f"match_{self.get_cte_name(state.cte_counter)}")

        return self.wrap_query(
            (
                select(
                    *get_std_cols(context, state),
                    self.derive_rank_column(matching_items.c.avg_confidence),
                ).join(
                    matching_items,
                    matching_items.c.item_id == context.c.item_id,
                )
            ),
            context,
            state,
        )
