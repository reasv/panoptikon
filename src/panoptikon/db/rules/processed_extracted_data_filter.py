from panoptikon.db.rules.types import ProcessedExtractedDataFilter


def build_processed_item_data_filter_cte(
    filter: ProcessedExtractedDataFilter, filter_on: str | None, name: str
):
    if filter_on:
        prev_cte_join_clause = f"""
        JOIN {filter_on} ON src.item_id = {filter_on}.id
        """
    else:
        prev_cte_join_clause = ""

    target_type_conditions = ", ".join(["?" for _ in filter.data_types])
    cte = f"""
    {name} AS (
        SELECT DISTINCT src.item_id AS id
        FROM item_data AS src
        {prev_cte_join_clause}
        WHERE src.data_type IN ({target_type_conditions})
        AND src.is_placeholder = 0
        AND NOT EXISTS (
            SELECT 1
            FROM item_data AS derived
            JOIN setters
            ON derived.setter_id = setters.id
            WHERE derived.source_id = src.id
            AND setters.name = ?
        )
    )
    """
    return cte, [*filter.data_types, filter.setter_name]
