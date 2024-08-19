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
    CREATE TABLE IF NOT EXISTS items (
        id INTEGER PRIMARY KEY,
        sha256 TEXT UNIQUE NOT NULL,
        md5 TEXT NOT NULL,
        type TEXT NOT NULL,              -- Mime type of the file (e.g. image/jpeg)
        size INTEGER,                    -- Size of the file in bytes
        width INTEGER,                   -- Width of the frame in pixels
        height INTEGER,                  -- Height of the frame in pixels
        duration REAL,                   -- Duration of the video/audio in seconds
        audio_tracks INTEGER,            -- Number of audio tracks
        video_tracks INTEGER,            -- Number of video tracks
        subtitle_tracks INTEGER,         -- Number of subtitle tracks
        time_added TEXT NOT NULL         -- Using TEXT to store ISO-8601 formatted datetime
    )
    """
    )


def downgrade() -> None:
    pass
