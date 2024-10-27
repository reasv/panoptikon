import logging
import re
import shlex
from typing import List, TypeVar

from sqlalchemy import FromClause, Join, Select, Table
from sqlalchemy.sql.expression import Alias as SQLAlchemyAlias

logger = logging.getLogger(__name__)


def relabel_column(query: Select, old_label: str, new_label: str) -> Select:
    updated_columns = []
    for col in query.selected_columns:
        if col._label == old_label:  # Check for the specific label
            updated_columns.append(col.label(new_label))  # Relabel it
        else:
            updated_columns.append(col)  # Keep the other columns unchanged

    # Create a new query with the updated column labels
    return query.with_only_columns(*updated_columns)


def has_joined(query: Select, table: Table) -> bool:
    def table_in_from(from_clause: FromClause) -> bool:
        # Base case: the from_clause is exactly the same table (no alias)
        if from_clause is table:
            return True

        # If from_clause is an Alias of the table, do not consider it as the table itself
        if isinstance(from_clause, (Join, SQLAlchemyAlias)):
            # If it's an Alias, check if the original element is the table
            if isinstance(from_clause, SQLAlchemyAlias):
                if from_clause.element is table:
                    # It's an alias of the table, so don't consider it as the table itself
                    return False

            # If from_clause is a Join, recursively check both sides
            if isinstance(from_clause, Join):
                return table_in_from(from_clause.left) or table_in_from(
                    from_clause.right
                )

        # Handle other cases, like when from_clause has an underlying element (e.g., subqueries)
        if hasattr(from_clause, "element"):
            return table_in_from(from_clause.element)  # type: ignore

        return False

    # Check all from clauses in the query
    return any(
        table_in_from(from_clause) for from_clause in query.get_final_froms()
    )


def parse_and_escape_query(user_input: str) -> str:
    """
    Parse and escape a query string for use in FTS5 MATCH statements.
    """
    original_str = user_input
    # Replace escaped double quotes with a double double quote
    user_input = re.sub(r'\\"', r'""', user_input)
    # Step 1: Count the number of double quotes
    double_quote_count = user_input.count('"')

    # Step 2: If the number of double quotes is odd, add a closing quote
    if double_quote_count % 2 != 0:
        user_input += '"'
    # Escape for shlex
    user_input = re.sub(r"'", r"\'", user_input)
    # Convert SQL escape sequence to shlex accepted escape sequence
    user_input = re.sub(r'""', r"\"", user_input)
    # Step 3: Split the string into tokens
    try:
        tokens = shlex.split(user_input)
    except ValueError:
        logger.error(
            f"Shlex failed to parse query: '{user_input}' (from '{original_str}')"
        )
        tokens = user_input.split()
    # # Step 4: Escape double quotes in each token using the SQL escape sequence
    escaped_tokens = [re.sub(r'"', r'""', token) for token in tokens]

    # Step 5: Join the escaped tokens back into a single string
    return " ".join([f'"{token}"' for token in escaped_tokens])


T = TypeVar("T")


def clean_params(params: List[T]) -> List[T]:
    cleaned = []
    for param in params:
        if isinstance(param, bytes):
            cleaned.append(f"[{len(param)} Bytes]")
        else:
            cleaned.append(param)
    return cleaned
