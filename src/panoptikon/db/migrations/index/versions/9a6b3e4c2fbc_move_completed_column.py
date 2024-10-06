"""Add completed column to data_log, copy values from data_jobs, and drop completed column from data_jobs

Revision ID: 9a6b3e4c2fbc
Revises: 4b5f60e1b8d7
Create Date: 2024-10-06 12:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

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

    # Step 2: Copy the completed value from data_jobs to data_log
    op.execute(
        """
        UPDATE data_log
        SET completed = (
            SELECT completed
            FROM data_jobs
            WHERE data_jobs.id = data_log.job_id
        )
        """
    )
    # Step 3: Drop the completed column from data_jobs
    op.drop_column("data_jobs", "completed")


def downgrade() -> None:
    # Step 1: Re-add the completed column to data_jobs
    op.add_column(
        "data_jobs",
        sa.Column("completed", sa.Boolean, nullable=False, server_default="0"),
    )

    # Step 2: Copy the completed value back from data_log to data_jobs
    op.execute(
        """
        UPDATE data_jobs
        SET completed = (
            SELECT completed
            FROM data_log
            WHERE data_log.job_id = data_jobs.id
        )
        """
    )

    # Step 3: Remove the default value for future inserts in data_jobs
    op.alter_column("data_jobs", "completed", server_default=None)

    # Step 4: Drop the completed column from data_log
    op.drop_column("data_log", "completed")
