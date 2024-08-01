from src.db.rules.types import ProcessedItemsFilter


def build_processed_items_filter_cte(
    filter: ProcessedItemsFilter, filter_on: str | None, name: str
):
    if filter_on:
        prev_cte_join_clause = f"""
        JOIN
            {filter_on} ON items.id = {filter_on}.id
        """
    else:
        prev_cte_join_clause = ""
    cte = f"""
    WITH {name} AS (
        SELECT items.id
        FROM items
        {prev_cte_join_clause}
        LEFT JOIN items_extractions ON items.id = items_extractions.item_id 
            AND items_extractions.setter_id = ?
        GROUP BY items.id
        HAVING COUNT(items_extractions.item_id) = 0
    )
    """
    return cte, [filter.setter_id]
