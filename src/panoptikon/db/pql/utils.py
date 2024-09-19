from sqlalchemy import FromClause, Join, Select, Table


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
        # Base case: the from_clause is the same as the table
        if from_clause == table:
            return True
        # If from_clause is a Join, check both left and right sides
        if isinstance(from_clause, Join):
            return table_in_from(from_clause.left) or table_in_from(
                from_clause.right
            )
        # Handle other cases, like Alias, if needed
        if hasattr(from_clause, "element"):
            return table_in_from(from_clause.element)  # type: ignore
        return False

    # Check all from clauses
    return any(
        table_in_from(from_clause) for from_clause in query.get_final_froms()
    )
