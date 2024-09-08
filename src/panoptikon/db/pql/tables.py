from pypika import Table

bookmarks = Table("bookmarks")
files = Table("files")
items = Table("items")
files_path_fts = Table("files_path_fts")
extracted_text = Table("extracted_text")
extracted_text_fts = Table("extracted_text_fts")
item_data = Table("item_data")
setters = Table("setters")
embeddings = Table("embeddings")
tags = Table("tags")
