-- Generated from main.sqlite_master; do not edit by hand.
CREATE TABLE alembic_version (
	version_num VARCHAR(32) NOT NULL, 
	CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);
CREATE TABLE data_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        completed BOOLEAN NOT NULL DEFAULT 0
    );
CREATE TABLE data_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,                 -- Foreign key to data_jobs table
        start_time TEXT NOT NULL,               -- Using TEXT to store ISO-8601 formatted datetime
        end_time TEXT NOT NULL,                 -- Using TEXT to store ISO-8601 formatted datetime
        type TEXT NOT NULL,
        setter TEXT NOT NULL,
        threshold REAL DEFAULT NULL,
        batch_size INTEGER NOT NULL,
        image_files INTEGER NOT NULL DEFAULT 0,
        video_files INTEGER NOT NULL DEFAULT 0,
        other_files INTEGER NOT NULL DEFAULT 0,
        total_segments INTEGER NOT NULL DEFAULT 0,
        errors INTEGER NOT NULL DEFAULT 0,
        total_remaining INTEGER NOT NULL DEFAULT 0,
        data_load_time REAL DEFAULT 0,
        inference_time REAL DEFAULT 0, completed BOOLEAN DEFAULT '0' NOT NULL,
        FOREIGN KEY(job_id) REFERENCES data_jobs(id) ON DELETE SET NULL
    );
CREATE TABLE embeddings (
            id INTEGER PRIMARY KEY,
            embedding float[],
            FOREIGN KEY(id) REFERENCES item_data(id) ON DELETE CASCADE
        );
CREATE TABLE extracted_text (
        id INTEGER PRIMARY KEY,
        language TEXT,
        language_confidence REAL,
        confidence REAL,
        text TEXT NOT NULL, text_length INTEGER,
        FOREIGN KEY(id) REFERENCES item_data(id) ON DELETE CASCADE
    );
CREATE VIRTUAL TABLE extracted_text_fts
        USING fts5(
            text,
            content="extracted_text",
            content_rowid="id",
            tokenize="trigram case_sensitive 0"
        );
CREATE TABLE file_scans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        start_time TEXT NOT NULL,         -- Using TEXT to store ISO-8601 formatted datetime
        end_time TEXT,           -- Using TEXT to store ISO-8601 formatted datetime
        path TEXT NOT NULL,
        total_available INTEGER NOT NULL DEFAULT 0,
        new_items INTEGER NOT NULL DEFAULT 0,
        unchanged_files INTEGER NOT NULL DEFAULT 0,
        new_files INTEGER NOT NULL DEFAULT 0,
        modified_files INTEGER NOT NULL DEFAULT 0,
        marked_unavailable INTEGER NOT NULL DEFAULT 0,
        errors INTEGER NOT NULL DEFAULT 0,
        false_changes INTEGER NOT NULL DEFAULT 0,
        metadata_time REAL DEFAULT 0,
        hashing_time REAL DEFAULT 0,
        thumbgen_time REAL DEFAULT 0
    , blurhash_time REAL DEFAULT '0' NOT NULL);
CREATE TABLE files (
        id INTEGER PRIMARY KEY,
        sha256 TEXT NOT NULL,
        item_id INTEGER NOT NULL,         -- Foreign key to items table
        path TEXT UNIQUE NOT NULL,        -- Ensuring path is unique
        filename TEXT NOT NULL,           -- Filename extracted from path
        last_modified TEXT NOT NULL,      -- Using TEXT to store ISO-8601 formatted datetime
        scan_id INTEGER NOT NULL,
        available BOOLEAN NOT NULL,       -- BOOLEAN to indicate if the path is available
        FOREIGN KEY(item_id) REFERENCES items(id),
        FOREIGN KEY(scan_id) REFERENCES file_scans(id) ON DELETE CASCADE
    );
CREATE VIRTUAL TABLE files_path_fts
        USING fts5(
            path,
            filename,
            content='files',
            content_rowid='id',
            tokenize='trigram case_sensitive 0'
        );
CREATE TABLE folders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        time_added TEXT NOT NULL,               -- Using TEXT to store ISO-8601 formatted datetime
        path TEXT NOT NULL,
        included BOOLEAN NOT NULL,              -- BOOLEAN to indicate if folder is included or specifically excluded
        UNIQUE(path)  -- Unique constraint on path
    );
CREATE TABLE item_data (
        id INTEGER PRIMARY KEY,
        item_id INTEGER NOT NULL,         -- Reference to the item that the data is extracted from
        job_id INTEGER,                   -- Reference to the job that extracted the data
        setter_id INTEGER NOT NULL,       -- Reference to the setter that extracted the data from the item
        data_type TEXT NOT NULL,          -- Type of data extracted (e.g. text, image, etc.)
        idx INTEGER NOT NULL,             -- Index of the data in the item (page number, frame number, etc.)
        source_id INTEGER,                -- Reference to a previous item_data from which data was further processed
        is_origin BOOLEAN,                -- Whether the data is from the item directly or derived from other data. True if it is, NULL if not
        is_placeholder BOOLEAN,           -- Whether the data is a placeholder (e.g. no data extracted) Needed to mark an item as processed
        UNIQUE(item_id, setter_id, data_type, idx, is_origin),            -- Origin data should be unique per item, setter, datatype and index
        UNIQUE(item_id, setter_id, data_type, idx, source_id),            -- Derived extractions should be unique per data they are derived from (and setter, datatype, index)
        FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE,
        FOREIGN KEY(job_id) REFERENCES data_jobs(id) ON DELETE CASCADE,
        FOREIGN KEY(setter_id) REFERENCES setters(id) ON DELETE CASCADE,
        FOREIGN KEY(source_id) REFERENCES item_data(id) ON DELETE CASCADE,
        CHECK ((is_origin = TRUE AND source_id IS NULL) OR (is_origin IS NULL AND source_id IS NOT NULL))
    );
CREATE TABLE items (
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
    , blurhash VARCHAR);
CREATE TABLE setters (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        );
CREATE TABLE tags (
        id INTEGER PRIMARY KEY,
        namespace TEXT NOT NULL,
        name TEXT NOT NULL,
        UNIQUE(namespace, name)
    );
CREATE TABLE tags_items (
        item_data_id INTEGER NOT NULL,
        tag_id INTEGER NOT NULL,
        confidence REAL DEFAULT 1.0,
        UNIQUE(item_data_id, tag_id),
        FOREIGN KEY(item_data_id) REFERENCES item_data(id) ON DELETE CASCADE,
        FOREIGN KEY(tag_id) REFERENCES tags(id) ON DELETE CASCADE
    );
CREATE TRIGGER extracted_text_ad AFTER DELETE ON extracted_text BEGIN
            INSERT INTO extracted_text_fts(extracted_text_fts, rowid, text)
            VALUES('delete', old.id, old.text);
        END;
CREATE TRIGGER extracted_text_ai AFTER INSERT ON extracted_text BEGIN
            INSERT INTO extracted_text_fts(rowid, text)
            VALUES (new.id, new.text);
        END;
CREATE TRIGGER extracted_text_au AFTER UPDATE ON extracted_text BEGIN
            INSERT INTO extracted_text_fts(extracted_text_fts, rowid, text)
            VALUES('delete', old.id, old.text);
            INSERT INTO extracted_text_fts(rowid, text)
            VALUES (new.id, new.text);
        END;
CREATE TRIGGER files_path_ad AFTER DELETE ON files BEGIN
            INSERT INTO files_path_fts(files_path_fts, rowid, path, filename)
            VALUES('delete', old.id, old.path, old.filename);
        END;
CREATE TRIGGER files_path_ai AFTER INSERT ON files BEGIN
            INSERT INTO files_path_fts(rowid, path, filename)
            VALUES (new.id, new.path, new.filename);
        END;
CREATE TRIGGER files_path_au AFTER UPDATE ON files BEGIN
            INSERT INTO files_path_fts(files_path_fts, rowid, path, filename)
            VALUES('delete', old.id, old.path, old.filename);
            INSERT INTO files_path_fts(rowid, path, filename)
            VALUES (new.id, new.path, new.filename);
        END;
CREATE INDEX idx_data_log_batch_size ON data_log(batch_size);
CREATE INDEX idx_data_log_data_load_time ON data_log(data_load_time);
CREATE INDEX idx_data_log_end_time ON data_log(end_time);
CREATE INDEX idx_data_log_errors ON data_log(errors);
CREATE INDEX idx_data_log_image_files ON data_log(image_files);
CREATE INDEX idx_data_log_inference_time ON data_log(inference_time);
CREATE INDEX idx_data_log_job_id ON data_log(job_id);
CREATE INDEX idx_data_log_other_files ON data_log(other_files);
CREATE INDEX idx_data_log_setter ON data_log(setter);
CREATE INDEX idx_data_log_start_time ON data_log(start_time);
CREATE INDEX idx_data_log_threshold ON data_log(threshold);
CREATE INDEX idx_data_log_total_remaining ON data_log(total_remaining);
CREATE INDEX idx_data_log_total_segments ON data_log(total_segments);
CREATE INDEX idx_data_log_type ON data_log(type);
CREATE INDEX idx_data_log_video_files ON data_log(video_files);
CREATE INDEX idx_extracted_text_confidence ON extracted_text(confidence);
CREATE INDEX idx_extracted_text_language ON extracted_text(language);
CREATE INDEX idx_extracted_text_language_confidence ON extracted_text(language_confidence);
CREATE INDEX idx_extracted_text_text_length ON extracted_text (text_length);
CREATE INDEX idx_file_scans_end_time ON file_scans(end_time);
CREATE INDEX idx_file_scans_path ON file_scans(path);
CREATE INDEX idx_file_scans_start_time ON file_scans(start_time);
CREATE INDEX idx_files_available ON files(available);
CREATE INDEX idx_files_filename ON files(filename);
CREATE INDEX idx_files_item_id ON files(item_id);
CREATE INDEX idx_files_last_modified ON files(last_modified);
CREATE INDEX idx_files_path ON files(path);
CREATE INDEX idx_files_scan_id ON files(scan_id);
CREATE INDEX idx_files_sha256 ON files(sha256);
CREATE INDEX idx_folders_included ON folders(included);
CREATE INDEX idx_folders_path ON folders(path);
CREATE INDEX idx_folders_time_added ON folders(time_added);
CREATE INDEX idx_item_data_data_type ON item_data(data_type);
CREATE INDEX idx_item_data_idx ON item_data(idx);
CREATE INDEX idx_item_data_is_origin ON item_data(is_origin);
CREATE INDEX idx_item_data_is_placeholder ON item_data(is_placeholder);
CREATE INDEX idx_item_data_item_id ON item_data(item_id);
CREATE INDEX idx_item_data_job_id ON item_data(job_id);
CREATE INDEX idx_item_data_setter_id ON item_data(setter_id);
CREATE INDEX idx_item_data_source_id ON item_data(source_id);
CREATE INDEX idx_items_audio_tracks ON items(audio_tracks);
CREATE INDEX idx_items_duration ON items(duration);
CREATE INDEX idx_items_height ON items(height);
CREATE INDEX idx_items_md5 ON items(md5);
CREATE INDEX idx_items_size ON items(size);
CREATE INDEX idx_items_subtitle_tracks ON items(subtitle_tracks);
CREATE INDEX idx_items_time_added ON items(time_added);
CREATE INDEX idx_items_type ON items(type);
CREATE INDEX idx_items_video_tracks ON items(video_tracks);
CREATE INDEX idx_items_width ON items(width);
CREATE INDEX idx_setters_name ON setters(name);
CREATE INDEX idx_tags_items_confidence ON tags_items(confidence);
CREATE INDEX idx_tags_items_item_data_id ON tags_items(item_data_id);
CREATE INDEX idx_tags_items_tag_id ON tags_items(tag_id);
CREATE INDEX idx_tags_name ON tags(name);
CREATE INDEX idx_tags_namespace ON tags(namespace);
CREATE INDEX idx_tags_namespace_name ON tags(namespace, name);
CREATE INDEX ix_file_scans_blurhash_time ON file_scans (blurhash_time);
CREATE INDEX ix_items_blurhash ON items (blurhash);
