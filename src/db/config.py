import sqlite3
from dataclasses import asdict, dataclass, fields
from typing import Any

from src.types import SystemConfig


def persist_system_config(conn: sqlite3.Connection, config: SystemConfig):
    cursor = conn.cursor()

    for field in fields(config):
        value = getattr(config, field.name)
        cursor.execute(
            """
            INSERT OR REPLACE INTO system_config (k, v)
            VALUES (?, ?)
        """,
            (field.name, str(value)),
        )


def retrieve_system_config(conn: sqlite3.Connection) -> SystemConfig:
    cursor = conn.cursor()

    cursor.execute("SELECT k, v FROM system_config")
    db_values = dict(cursor.fetchall())

    config = SystemConfig()

    for field in fields(config):
        if field.name in db_values:
            value = db_values[field.name]
            if isinstance(getattr(config, field.name), bool):
                value = value.lower() == "true"
            setattr(config, field.name, value)

    return config
