"""Initial creation

Revision ID: 31adcda83d69
Revises: 
Create Date: 2024-08-19 23:03:26.259523

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "31adcda83d69"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:

    op.execute(
        """
    CREATE TABLE IF NOT EXISTS bookmarks (
        user TEXT NOT NULL, -- User who created the bookmark
        namespace TEXT NOT NULL, -- Namespace for the bookmark
        sha256 TEXT NOT NULL, -- SHA256 of the item
        time_added TEXT NOT NULL, -- Using TEXT to store ISO-8601 formatted datetime
        metadata TEXT, -- JSON string to store additional metadata
        PRIMARY KEY(user, namespace, sha256)
    )
    """
    )

    # Create indexes
    # Tuples are table name, followed by a list of columns
    indices = [
        ("bookmarks", ["time_added"]),
        ("bookmarks", ["sha256"]),
        ("bookmarks", ["metadata"]),
        ("bookmarks", ["namespace"]),
        ("bookmarks", ["user"]),
    ]

    for table, columns in indices:
        if "." in table:
            database, table = table.split(".")
            index_name = f"{database}.idx_{table}_{'_'.join(columns)}"
        else:
            index_name = f"idx_{table}_{'_'.join(columns)}"

        sql = f"""
            CREATE INDEX IF NOT EXISTS
            {index_name} ON {table}({', '.join(columns)})
            """
        op.execute(sql)
