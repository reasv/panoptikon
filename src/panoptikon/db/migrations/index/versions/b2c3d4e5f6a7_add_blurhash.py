"""Add blurhash to items and blurhash_time to file_scans

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2024-10-30 12:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b2c3d4e5f6a7"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add blurhash column to items table
    op.add_column("items", sa.Column("blurhash", sa.String, nullable=True))

    # Create an index on the blurhash column
    op.create_index("ix_items_blurhash", "items", ["blurhash"])

    # Add blurhash_time column to file_scans table
    op.add_column(
        "file_scans",
        sa.Column("blurhash_time", sa.REAL, nullable=False, server_default="0"),
    )

    # Create an index on the blurhash_time column
    op.create_index(
        "ix_file_scans_blurhash_time", "file_scans", ["blurhash_time"]
    )


def downgrade() -> None:
    # Remove index and blurhash column from items table
    op.drop_index("ix_items_blurhash", table_name="items")
    op.drop_column("items", "blurhash")

    # Remove index and blurhash_time column from file_scans table
    op.drop_index("ix_file_scans_blurhash_time", table_name="file_scans")
    op.drop_column("file_scans", "blurhash_time")
