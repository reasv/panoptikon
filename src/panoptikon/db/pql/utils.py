from sqlalchemy import FromClause, Join, Select, Table
from sqlalchemy.sql.expression import Alias as SQLAlchemyAlias


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
            print("from", from_clause, "table", table)
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
