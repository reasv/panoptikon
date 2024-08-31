"""Add length column to extracted_text

Revision ID: 4b5f60e1b8d7
Revises: 31adcda83d69
Create Date: 2024-08-31 12:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "4b5f60e1b8d7"
down_revision: Union[str, None] = "31adcda83d69"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Step 1: Add the new column
    op.add_column("extracted_text", sa.Column("text_length", sa.Integer))

    # Step 2: Update the new column with the length of existing text
    op.execute(
        """
        UPDATE extracted_text
        SET text_length = LENGTH(text)
        """
    )

    # Step 3: Create an index on the new length column
    op.create_index(
        "idx_extracted_text_text_length", "extracted_text", ["text_length"]
    )


def downgrade() -> None:
    # Reverse the migration: Drop the index and column
    op.drop_index("idx_extracted_text_text_length", table_name="extracted_text")
    op.drop_column("extracted_text", "text_length")
