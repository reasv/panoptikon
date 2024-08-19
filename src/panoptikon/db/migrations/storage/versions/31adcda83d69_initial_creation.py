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
        f"""
            CREATE TABLE IF NOT EXISTS thumbnails (
                id INTEGER PRIMARY KEY,
                item_sha256 TEXT NOT NULL,
                idx INTEGER NOT NULL,
                item_mime_type TEXT NOT NULL,        -- MIME type of the source file
                width INTEGER NOT NULL,              -- Width of the thumbnail in pixels
                height INTEGER NOT NULL,             -- Height of the thumbnail in pixels
                version INTEGER NOT NULL,            -- Version of the thumbnail creation process
                thumbnail BLOB NOT NULL,             -- The thumbnail image data (stored as a BLOB)
                UNIQUE(item_sha256, idx)
            );
        """
    )

    op.execute(
        f"""
            CREATE TABLE IF NOT EXISTS frames (
                id INTEGER PRIMARY KEY,
                item_sha256 TEXT NOT NULL,
                idx INTEGER NOT NULL,
                item_mime_type TEXT NOT NULL,        -- MIME type of the source file
                width INTEGER NOT NULL,              -- Width of the frame in pixels
                height INTEGER NOT NULL,             -- Height of the frame in pixels
                version INTEGER NOT NULL,            -- Version of the frame extraction process
                frame BLOB NOT NULL,                 -- The extracted frame image data (stored as a BLOB)
                UNIQUE(item_sha256, idx)
            );
        """
    )

    # Create indexes
    # Tuples are table name, followed by a list of columns
    indices = [
        ("thumbnails", ["item_sha256"]),
        ("thumbnails", ["idx"]),
        ("thumbnails", ["item_mime_type"]),
        ("thumbnails", ["width"]),
        ("thumbnails", ["height"]),
        ("thumbnails", ["version"]),
        ("frames", ["item_sha256"]),
        ("frames", ["idx"]),
        ("frames", ["item_mime_type"]),
        ("frames", ["width"]),
        ("frames", ["height"]),
        ("frames", ["version"]),
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
