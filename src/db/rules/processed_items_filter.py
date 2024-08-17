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
        JOIN item_data 
        ON items.id = item_data.item_id
        JOIN setters 
        ON item_data.setter_id = setters.id
        WHERE setters.name = ?
        GROUP BY items.id
    )
    """
    return cte, [filter.setter_name]
