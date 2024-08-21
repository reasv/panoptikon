from typing import List, Literal, Union

from pydantic import BaseModel, Field

from panoptikon.db.search.types import OrderByType, OrderType


class QueryTagFiltersModel(BaseModel):
    pos_match_all: List[str] = Field(default_factory=list)
    pos_match_any: List[str] = Field(default_factory=list)
    neg_match_any: List[str] = Field(default_factory=list)
    neg_match_all: List[str] = Field(default_factory=list)
    all_setters_required: bool = False
    setters: List[str] = Field(default_factory=list)
    namespaces: List[str] = Field(default_factory=list)
    min_confidence: Union[float, None] = None


class OrderParamsModel(BaseModel):
    order_by: OrderByType = "last_modified"
    order: OrderType = None
    page: int = 1
    page_size: int = 10


class ExtractedTextFilterModel(BaseModel):
    query: str
    targets: List[str] = Field(default_factory=list)
    languages: List[str] = Field(default_factory=list)
    language_min_confidence: Union[float, None] = None
    min_confidence: Union[float, None] = None


class ExtractedTextEmbeddingsFilterModel(BaseModel):
    query: bytes
    model: str
    targets: List[str] = Field(default_factory=list)
    languages: List[str] = Field(default_factory=list)
    language_min_confidence: Union[float, None] = None
    min_confidence: Union[float, None] = None


class BookmarksFilterModel(BaseModel):
    restrict_to_bookmarks: Literal[True] = True
    namespaces: List[str] = Field(default_factory=list)


class PathTextFilterModel(BaseModel):
    query: str
    only_match_filename: bool = False


class AnyTextFilterModel(BaseModel):
    path: Union[PathTextFilterModel, None] = None
    extracted_text: Union[ExtractedTextFilterModel, None] = None


class ImageEmbeddingFilterModel(BaseModel):
    query: bytes
    model: str


class FileFiltersModel(BaseModel):
    item_types: List[str] = Field(default_factory=list)
    include_path_prefixes: List[str] = Field(default_factory=list)


class QueryFiltersModel(BaseModel):
    files: Union[FileFiltersModel, None] = None
    path: Union[PathTextFilterModel, None] = None
    extracted_text: Union[ExtractedTextFilterModel, None] = None
    extracted_text_embeddings: Union[
        ExtractedTextEmbeddingsFilterModel, None
    ] = None
    image_embeddings: Union[ImageEmbeddingFilterModel, None] = None
    any_text: Union[AnyTextFilterModel, None] = None
    bookmarks: Union[BookmarksFilterModel, None] = None


class QueryParamsModel(BaseModel):
    tags: QueryTagFiltersModel = Field(default_factory=QueryTagFiltersModel)
    filters: QueryFiltersModel = Field(default_factory=QueryFiltersModel)


class SearchQueryModel(BaseModel):
    query: QueryParamsModel = Field(default_factory=QueryParamsModel)
    order_args: OrderParamsModel = Field(default_factory=OrderParamsModel)
    count: bool = True
    check_path: bool = False
