import os
import sqlite3

from typeguard import typechecked

from src.db.search.clauses.order import build_order_by_clause
from src.db.search.search_query import build_search_query
from src.db.search.types import SearchQuery
from src.db.search.utils import clean_input
from src.db.utils import pretty_print_SQL
from src.types import FileSearchResult


@typechecked
def search_files(
    conn: sqlite3.Connection,
    args: SearchQuery,
):
    args = clean_input(args)

    # Build the main query
    search_query, search_query_params = build_search_query(args=args.query)
    # Debugging
    # print_search_query(count_query, params)
    cursor = conn.cursor()
    if args.count:
        # First query to get the total count of items matching the criteria
        count_query = f"""
        SELECT COUNT(*)
        FROM (
            {search_query}
        )
        """
        try:
            cursor.execute(count_query, search_query_params)
        except Exception as e:
            # Debugging
            pretty_print_SQL(count_query, search_query_params)
            raise e
        total_count: int = cursor.fetchone()[0]
    else:
        total_count = 0

    # Build the ORDER BY clause
    order_by_clause, order_by_params = build_order_by_clause(
        filters=args.query.filters, oargs=args.order_args
    )

    try:
        cursor.execute(
            (search_query + order_by_clause),
            [*search_query_params, *order_by_params],
        )
    except Exception as e:
        # Debugging
        pretty_print_SQL(
            (search_query + order_by_clause),
            [*search_query_params, *order_by_params],
        )
        raise e
    results_count = cursor.rowcount
    while row := cursor.fetchone():
        file = FileSearchResult(*row[0:4])
        if args.check_path and not os.path.exists(file.path):
            continue
        yield file, total_count
    if results_count == 0:
        return []
