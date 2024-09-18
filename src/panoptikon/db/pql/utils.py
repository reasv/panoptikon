from sqlalchemy import Select, Table


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
    for from_clause in query.froms:
        # Ensure it's the exact table, not an alias
        if isinstance(from_clause, table.__class__) and from_clause == table:
            return True
    return False
