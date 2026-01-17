#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path


def dump_schema(cur, schema: str, out_path: Path) -> None:
    rows = cur.execute(f"""
      SELECT type, name, tbl_name, sql
      FROM {schema}.sqlite_master
      WHERE sql IS NOT NULL
        AND name NOT LIKE 'sqlite_%'
      ORDER BY
        CASE type
          WHEN 'table' THEN 1
          WHEN 'view' THEN 2
          WHEN 'trigger' THEN 3
          WHEN 'index' THEN 4
          ELSE 5
        END,
        name;
    """).fetchall()

    with out_path.open("w", encoding="utf-8") as f:
        f.write(f"-- Generated from {schema}.sqlite_master; do not edit by hand.\n")
        f.write("PRAGMA foreign_keys=OFF;\n")
        f.write("BEGIN;\n")
        for _type, _name, _tbl, sql in rows:
            sql = (sql or "").strip()
            if not sql:
                continue
            f.write(sql)
            if not sql.endswith(";"):
                f.write(";")
            f.write("\n")
        f.write("COMMIT;\n")
        f.write("PRAGMA foreign_keys=ON;\n")


def main() -> None:
    from panoptikon.db import get_database_connection

    conn = get_database_connection(
        write_lock=False,
        user_data_wl=False,
        index_db="default",
        user_data_db="default",
    )

    cur = conn.cursor()

    dump_schema(cur, "main", Path("index_schema_dump.sql"))
    dump_schema(cur, "storage", Path("storage_schema_dump.sql"))
    dump_schema(cur, "user_data", Path("user_data_schema_dump.sql"))


if __name__ == "__main__":
    main()
