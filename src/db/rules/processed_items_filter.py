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
    {name} AS (
        SELECT items.id
        FROM items
        {prev_cte_join_clause}
        EXCEPT
        SELECT items.id
        FROM items
        JOIN items_extractions 
        ON items.id = items_extractions.item_id
        JOIN setters 
        ON items_extractions.setter_id = setters.id
        WHERE setters.type = ? AND setters.name = ?
        GROUP BY items.id
    )
    """
    return cte, [filter.setter_type, filter.setter_name]
