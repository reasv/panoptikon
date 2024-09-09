import pickle

from panoptikon.db import get_db_paths
from panoptikon.db.pql.build_table_meta import build_metadata

try:
    db_file, user_db_file, storage_db_file = get_db_paths()
    with open(db_file + ".pkl", "rb") as f:
        metadata = pickle.load(f)
except FileNotFoundError:
    metadata = build_metadata()

bookmarks = metadata.tables["bookmarks"]
files = metadata.tables["files"]
items = metadata.tables["items"]
files_path_fts = metadata.tables["files_path_fts"]
extracted_text = metadata.tables["extracted_text"]
extracted_text_fts = metadata.tables["extracted_text_fts"]
item_data = metadata.tables["item_data"]
setters = metadata.tables["setters"]
embeddings = metadata.tables["embeddings"]
tags = metadata.tables["tags"]
