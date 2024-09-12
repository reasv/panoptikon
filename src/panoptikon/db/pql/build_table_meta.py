import pickle

from sqlalchemy import MetaData, Table, create_engine

from panoptikon.db import get_db_paths


def build_metadata():
    db_file, user_db_file, storage_db_file = get_db_paths()
    engine = create_engine(f"sqlite:///{db_file}")
    engine_user_data = create_engine(f"sqlite:///{user_db_file}")
    metadata = MetaData()
    bookmarks = Table("bookmarks", metadata, autoload_with=engine_user_data)
    files = Table("files", metadata, autoload_with=engine)
    items = Table("items", metadata, autoload_with=engine)
    files_path_fts = Table("files_path_fts", metadata, autoload_with=engine)
    extracted_text = Table("extracted_text", metadata, autoload_with=engine)
    extracted_text_fts = Table(
        "extracted_text_fts", metadata, autoload_with=engine
    )
    item_data = Table("item_data", metadata, autoload_with=engine)
    setters = Table("setters", metadata, autoload_with=engine)
    embeddings = Table("embeddings", metadata, autoload_with=engine)
    tags = Table("tags", metadata, autoload_with=engine)
    tags_items = Table("tags_items", metadata, autoload_with=engine)
    with open(db_file + ".pkl", "wb") as f:
        pickle.dump(metadata, f)

    return metadata
