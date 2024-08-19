from typing import List, Tuple

from panoptikon.db.search.types import OrderParams, QueryFilters


def build_order_by_clause(
    filters: QueryFilters,
    oargs: OrderParams,
) -> Tuple[str, List[str | int | float]]:
    # Determine order_by_clause and default order setting based on order_by value

    order = oargs.order
    default_order_by_clause = "last_modified"
    match oargs.order_by:
        case "rank_fts":
            if filters.extracted_text:
                order_by_clause = "rank_fts"
            else:
                order_by_clause = default_order_by_clause
        case "rank_path_fts":
            if filters.path:
                order_by_clause = "rank_path_fts"
            else:
                order_by_clause = default_order_by_clause
        case "time_added":
            if filters.bookmarks:
                order_by_clause = "time_added"
            else:
                order_by_clause = default_order_by_clause
        case "rank_any_text":
            if filters.any_text:
                order_by_clause = "rank_any_text"
            else:
                order_by_clause = default_order_by_clause
        case "path":
            order_by_clause = "path"
            # Default order for path is ascending
            if order is None:
                order = "asc"
        case "text_vec_distance":
            if filters.extracted_text_embeddings:
                order_by_clause = "text_vec_distance"
                # Default order for text_vec_distance is ascending
                if order is None:
                    order = "asc"
            else:
                order_by_clause = default_order_by_clause
        case "image_vec_distance":
            if filters.image_embeddings:
                order_by_clause = "image_vec_distance"
                # Default order for image_vec_distance is ascending
                if order is None:
                    order = "asc"
            else:
                order_by_clause = default_order_by_clause
        case _:
            order_by_clause = default_order_by_clause

    # Default order for all other order_by values is descending
    if order is None:
        order = "desc"
    # Determine the order clause
    order_clause = "DESC" if order == "desc" else "ASC"

    # Second query to get the items with pagination
    clause = f"""
    ORDER BY {order_by_clause} {order_clause}
    LIMIT ? OFFSET ?
    """
    page = max(oargs.page, 1)
    page_size = oargs.page_size or 1000000  # Mostly for debugging purposes
    offset = (page - 1) * page_size

    query_params: List[str | int | float] = [page_size, offset]

    return clause, query_params
