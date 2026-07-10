-- Pinboards: saved pinboard arrangements with append-only version history.
--
-- `pinboards` is the identity (what the library lists); `pinboard_versions`
-- holds immutable content snapshots. `layout` stores the UI's pinboard URL
-- param verbatim (a JSON array of strings whose first element is the in-band
-- grid version token, e.g. "v2") — the gateway never parses it, the UI owns
-- the format. `pinboard_version_items` is a search index over each version's
-- distinct member items (full sha256 hashes, supplied by the client at save
-- time); "only the head version is searchable" is enforced by joining
-- through pinboards.head_version_id, so version deletion self-heals.
CREATE TABLE pinboards (
    id INTEGER PRIMARY KEY,
    user TEXT NOT NULL,
    name TEXT,
    head_version_id INTEGER,
    time_added TEXT NOT NULL,
    time_updated TEXT NOT NULL
);
CREATE INDEX idx_pinboards_user ON pinboards(user);
CREATE INDEX idx_pinboards_time_updated ON pinboards(time_updated);

CREATE TABLE pinboard_versions (
    id INTEGER PRIMARY KEY,
    pinboard_id INTEGER NOT NULL REFERENCES pinboards(id),
    layout JSON NOT NULL CHECK (json_valid(layout)),
    name_at_save TEXT,
    preview BLOB,
    preview_w INTEGER,
    preview_h INTEGER,
    -- Height in preview-image pixels of one save-time viewport screenful;
    -- the library crops cards to this line and fades below it.
    screenful_h INTEGER,
    time_added TEXT NOT NULL
);
CREATE INDEX idx_pinboard_versions_board ON pinboard_versions(pinboard_id, id);

CREATE TABLE pinboard_version_items (
    version_id INTEGER NOT NULL REFERENCES pinboard_versions(id),
    sha256 TEXT NOT NULL,
    PRIMARY KEY (version_id, sha256)
) WITHOUT ROWID;
CREATE INDEX idx_pinboard_version_items_sha256
    ON pinboard_version_items(sha256, version_id);

-- Optional-name search. External-content FTS5 over pinboards.name; the
-- triggers keep it in sync (NULL names index as empty strings).
CREATE VIRTUAL TABLE pinboards_fts USING fts5(
    name,
    content='pinboards',
    content_rowid='id'
);
CREATE TRIGGER pinboards_fts_insert AFTER INSERT ON pinboards BEGIN
    INSERT INTO pinboards_fts(rowid, name) VALUES (new.id, new.name);
END;
CREATE TRIGGER pinboards_fts_delete AFTER DELETE ON pinboards BEGIN
    INSERT INTO pinboards_fts(pinboards_fts, rowid, name)
        VALUES ('delete', old.id, old.name);
END;
CREATE TRIGGER pinboards_fts_update AFTER UPDATE OF name ON pinboards BEGIN
    INSERT INTO pinboards_fts(pinboards_fts, rowid, name)
        VALUES ('delete', old.id, old.name);
    INSERT INTO pinboards_fts(rowid, name) VALUES (new.id, new.name);
END;
