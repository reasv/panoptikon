import sqlite3
from typing import List

# Assuming we have the serialization functions from before
from src.db.rules.serializer import (
    deserialize_rule_item_filters,
    serialize_rule_item_filters,
)
from src.db.rules.types import RuleItemFilters, StoredRule


def add_rule(
    conn: sqlite3.Connection,
    setters: List[str],
    filters: RuleItemFilters,
):
    cursor = conn.cursor()

    # Serialize the filters
    serialized_filters = serialize_rule_item_filters(filters)

    # Insert the rule
    cursor.execute(
        "INSERT INTO extraction_rules (rule) VALUES (?)", (serialized_filters,)
    )
    rule_id = cursor.lastrowid

    # Insert the setters
    cursor.executemany(
        "INSERT INTO extraction_rules_setters (rule_id, setter_name) VALUES (?, ?)",
        [(rule_id, setter_name) for setter_name in setters],
    )

    return rule_id


def get_rule(
    conn: sqlite3.Connection,
    rule_id: int,
    include_disabled: bool = False,
) -> StoredRule | None:
    rules_list = get_rules_by_ids(
        conn, [rule_id], include_disabled=include_disabled
    )

    if not rules_list:
        return None
    else:
        return rules_list[0]


def get_rules(
    conn: sqlite3.Connection,
) -> List[StoredRule]:
    cursor = conn.cursor()

    # Get all rules
    cursor.execute("SELECT id, rule, enabled FROM extraction_rules")
    rules_data = cursor.fetchall()

    stored_rules = []
    for rule_id, rule_data, enabled in rules_data:
        # Get the setters for each rule
        cursor.execute(
            "SELECT setter_name FROM extraction_rules_setters WHERE rule_id = ?",
            (rule_id,),
        )
        setters = [row[0] for row in cursor.fetchall()]

        # Deserialize the filters
        filters = deserialize_rule_item_filters(rule_data)

        stored_rules.append(
            StoredRule(
                id=rule_id,
                enabled=bool(enabled),
                setters=setters,
                filters=filters,
            )
        )

    return stored_rules


def delete_rule(
    conn: sqlite3.Connection,
    rule_id: int,
):
    cursor = conn.cursor()

    # Delete the rule (this will also delete associated setters due to ON DELETE CASCADE)
    cursor.execute("DELETE FROM extraction_rules WHERE id = ?", (rule_id,))


def update_rule(
    conn: sqlite3.Connection,
    rule_id: int,
    setters: List[str],
    filters: RuleItemFilters,
):
    cursor = conn.cursor()

    # Serialize the filters
    serialized_filters = serialize_rule_item_filters(filters)

    # Update the rule
    cursor.execute(
        "UPDATE extraction_rules SET rule = ? WHERE id = ?",
        (serialized_filters, rule_id),
    )

    # Delete existing setters
    cursor.execute(
        "DELETE FROM extraction_rules_setters WHERE rule_id = ?", (rule_id,)
    )

    # Insert new setters
    cursor.executemany(
        "INSERT INTO extraction_rules_setters (rule_id, setter_name) VALUES (?, ?)",
        [(rule_id, setter_name) for setter_name in setters],
    )


def get_rules_for_setter(
    conn: sqlite3.Connection, setter_name: str
) -> List[StoredRule]:
    cursor = conn.cursor()

    # First, get all rule IDs that have this setter
    cursor.execute(
        """
        SELECT DISTINCT rule_id 
        FROM extraction_rules_setters 
        WHERE setter_name = ?
    """,
        (setter_name,),
    )

    rule_ids = [row[0] for row in cursor.fetchall()]

    if not rule_ids:
        return []  # No rules found for this setter

    return get_rules_by_ids(conn, rule_ids)


def get_rules_by_ids(
    conn: sqlite3.Connection, rule_ids: List[int], include_disabled: bool = True
) -> List[StoredRule]:
    cursor = conn.cursor()
    # Now, get the details for these rules
    stored_rules = []
    for rule_id in rule_ids:
        # Get the rule data
        cursor.execute(
            f"""
            SELECT rule, enabled
            FROM extraction_rules
            WHERE id = ?
            {'AND enabled = 1' if not include_disabled else ''}
            """,
            (rule_id,),
        )
        rule_data = cursor.fetchone()

        if rule_data is None:
            continue  # This shouldn't happen, but just in case

        # Get all setters for this rule
        cursor.execute(
            """
            SELECT setter_name 
            FROM extraction_rules_setters 
            WHERE rule_id = ?
        """,
            (rule_id,),
        )
        setters = [row[0] for row in cursor.fetchall()]

        # Deserialize the filters
        filters = deserialize_rule_item_filters(rule_data[0])

        stored_rules.append(
            StoredRule(
                id=rule_id,
                enabled=bool(rule_data[1]),
                setters=setters,
                filters=filters,
            )
        )

    return stored_rules


def get_rules_for_setter_id(
    conn: sqlite3.Connection, setter_id: int
) -> List[StoredRule]:
    cursor = conn.cursor()

    # Join on name to go from setter_id to rule_ids
    cursor.execute(
        """
        SELECT DISTINCT rule_id 
        FROM extraction_rules_setters 
        JOIN setters
        ON extraction_rules_setters.setter_name = setters.name
        WHERE setters.id = ?
        """,
        (setter_id,),
    )
    rule_ids = [row[0] for row in cursor.fetchall()]

    if not rule_ids:
        return []

    # Use the existing function to get the rules
    return get_rules_by_ids(conn, rule_ids)


def disable_rule(
    conn: sqlite3.Connection,
    rule_id: int,
):
    cursor = conn.cursor()

    cursor.execute(
        "UPDATE extraction_rules SET enabled = 0 WHERE id = ?", (rule_id,)
    )


def enable_rule(
    conn: sqlite3.Connection,
    rule_id: int,
):
    cursor = conn.cursor()

    cursor.execute(
        "UPDATE extraction_rules SET enabled = 1 WHERE id = ?", (rule_id,)
    )
