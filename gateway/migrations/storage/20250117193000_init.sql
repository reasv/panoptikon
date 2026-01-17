-- Generated from storage.sqlite_master; do not edit by hand.
PRAGMA foreign_keys=OFF;
CREATE TABLE alembic_version (
	version_num VARCHAR(32) NOT NULL, 
	CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);
CREATE TABLE frames (
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
CREATE TABLE thumbnails (
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
CREATE INDEX idx_frames_height ON frames(height);
CREATE INDEX idx_frames_idx ON frames(idx);
CREATE INDEX idx_frames_item_mime_type ON frames(item_mime_type);
CREATE INDEX idx_frames_item_sha256 ON frames(item_sha256);
CREATE INDEX idx_frames_version ON frames(version);
CREATE INDEX idx_frames_width ON frames(width);
CREATE INDEX idx_thumbnails_height ON thumbnails(height);
CREATE INDEX idx_thumbnails_idx ON thumbnails(idx);
CREATE INDEX idx_thumbnails_item_mime_type ON thumbnails(item_mime_type);
CREATE INDEX idx_thumbnails_item_sha256 ON thumbnails(item_sha256);
CREATE INDEX idx_thumbnails_version ON thumbnails(version);
CREATE INDEX idx_thumbnails_width ON thumbnails(width);
PRAGMA foreign_keys=ON;
