-- Generated from user_data.sqlite_master; do not edit by hand.
CREATE TABLE alembic_version (
	version_num VARCHAR(32) NOT NULL, 
	CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);
CREATE TABLE bookmarks (
        user TEXT NOT NULL, -- User who created the bookmark
        namespace TEXT NOT NULL, -- Namespace for the bookmark
        sha256 TEXT NOT NULL, -- SHA256 of the item
        time_added TEXT NOT NULL, -- Using TEXT to store ISO-8601 formatted datetime
        metadata TEXT, -- JSON string to store additional metadata
        PRIMARY KEY(user, namespace, sha256)
    );
CREATE INDEX idx_bookmarks_metadata ON bookmarks(metadata);
CREATE INDEX idx_bookmarks_namespace ON bookmarks(namespace);
CREATE INDEX idx_bookmarks_sha256 ON bookmarks(sha256);
CREATE INDEX idx_bookmarks_time_added ON bookmarks(time_added);
CREATE INDEX idx_bookmarks_user ON bookmarks(user);
