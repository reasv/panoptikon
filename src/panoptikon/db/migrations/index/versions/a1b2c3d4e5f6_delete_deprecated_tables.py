"""Delete tables: extraction_rules, extraction_rules_setters, model_group_settings, system_config

Revision ID: a1b2c3d4e5f6
Revises: 9a6b3e4c2fbc
Create Date: 2024-10-25 10:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "9a6b3e4c2fbc"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop the tables
    op.drop_table("extraction_rules")
    op.drop_table("extraction_rules_setters")
    op.drop_table("model_group_settings")
    op.drop_table("system_config")


def downgrade() -> None:
    # Recreate the extraction_rules table
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS extraction_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            enabled BOOLEAN NOT NULL DEFAULT 1,
            rule TEXT NOT NULL
        );
        """
    )

    # Recreate the extraction_rules_setters table with a foreign key constraint
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS extraction_rules_setters (
            rule_id INTEGER NOT NULL,
            setter_name TEXT NOT NULL,
            FOREIGN KEY(rule_id) REFERENCES extraction_rules(id) ON DELETE CASCADE,
            UNIQUE(rule_id, setter_name)
        );
        """
    )

    # Recreate the system_config table
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS system_config (
            k STRING NOT NULL UNIQUE,
            v
        );
        """
    )

    # Recreate the model_group_settings table
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS model_group_settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            batch_size INTEGER NOT NULL,
            threshold REAL
        );
        """
    )
