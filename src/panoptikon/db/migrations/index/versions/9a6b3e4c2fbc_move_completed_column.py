"""Add completed column to data_log and copy values from data_jobs

Revision ID: 9a6b3e4c2fbc
Revises: 4b5f60e1b8d7
Create Date: 2024-10-06 12:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9a6b3e4c2fbc"
down_revision: Union[str, None] = "4b5f60e1b8d7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Step 1: Add the new completed column to the data_log table
    op.add_column(
        "data_log",
        sa.Column("completed", sa.Boolean, nullable=False, server_default="0"),
    )

    # Step 2: Copy the completed value from data_jobs to data_log where it exists, otherwise set it to 0
    op.execute(
        """
        UPDATE data_log
        SET completed = COALESCE(
            (
                SELECT completed
                FROM data_jobs
                WHERE data_jobs.id = data_log.job_id
            ), 0
        )
        """
    )


def downgrade() -> None:
    # Reverse the migration: Drop the completed column
    op.drop_column("data_log", "completed")
