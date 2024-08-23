import logging
import sqlite3
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Body, Depends, Query
from pydantic import BaseModel
from pydantic.dataclasses import dataclass

from panoptikon.api.routers.utils import get_db_readonly, get_db_user_data_wl
from panoptikon.db import get_database_connection
from panoptikon.db.bookmarks import (
    add_bookmark,
    delete_bookmarks_exclude_last_n,
    get_all_bookmark_namespaces,
    get_bookmark_metadata,
    get_bookmarks,
    remove_bookmark,
)
from panoptikon.db.search.types import OrderType
from panoptikon.types import FileSearchResult

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/bookmarks",
    tags=["bookmarks"],
    responses={404: {"description": "Not found"}},
)


@dataclass
class BookmarkNamespaces:
    namespaces: List[str]


@router.get(
    "/",
    summary="Get all bookmark namespaces",
    response_model=BookmarkNamespaces,
)
def get_ns_list(
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
):
    conn = get_database_connection(**conn_args)
    return BookmarkNamespaces(namespaces=get_all_bookmark_namespaces(conn))


class Items(BaseModel):
    sha256: List[str]


@dataclass
class Results:
    count: int
    results: List[FileSearchResult]


@router.get(
    "/{namespace}",
    summary="Get all bookmarks in a namespace",
    description="""
Get all items bookmarked in namespace.
Note that unlike the search API, this returns unique items, not files.
This has two implications:
1. Results are unique by `sha256` value.
2. Even if multiple files have the same `sha256` value, they will only appear once in the results, with the path of the first reachable file found.

The `order_by` parameter can be used to sort the results by `last_modified`, `path`, or `time_added`.
The `order` parameter can be used to sort the results in ascending or descending order.
The `include_wildcard` parameter can be used to include bookmarks with the `*` user value.
    """,
    response_model=Results,
)
def get_bookmarks_by_namespace(
    namespace: str,
    user: str = Query("user"),
    page_size: int = Query(1000),
    page: int = Query(1),
    order_by: Literal["last_modified", "path", "time_added"] = Query(
        "time_added"
    ),
    order: OrderType = Query(None),
    include_wildcard: bool = Query(True),
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
):
    conn = get_database_connection(**conn_args)
    files, count = get_bookmarks(
        conn,
        namespace=namespace,
        user=user,
        page_size=page_size,
        page=page,
        order_by=order_by,
        order=order,
        include_wildcard=include_wildcard,
    )
    return Results(count=count, results=files)


@dataclass
class MessageResult:
    message: str


@router.delete(
    "/{namespace}",
    summary="Delete multiple bookmarks in a namespace",
    description="""
Delete all bookmarks in a namespace. If `exclude_last_n` is provided, the last `n` added bookmarks will be kept.
Alternatively, a list of `sha256` values can be provided in the request body to only delete specific bookmarks.
""",
    response_model=MessageResult,
)
def delete_bookmarks_by_namespace(
    namespace: str,
    user: str = Query("user"),
    exclude_last_n: int = Query(0),
    items: Optional[Items] = Body(None),
    conn_args: Dict[str, Any] = Depends(get_db_user_data_wl),
):
    conn = get_database_connection(**conn_args)
    if items:
        c = 0
        for s in items.sha256:
            remove_bookmark(conn, s, namespace=namespace, user=user)
            c += 1
        return MessageResult(message=f"Deleted {c} bookmarks")
    elif items is None:
        delete_bookmarks_exclude_last_n(
            conn,
            exclude_last_n,
            namespace=namespace,
            user=user,
        )
        return MessageResult(message="Deleted bookmarks")
    else:
        return MessageResult(message="No items provided")


class ItemsMeta(BaseModel):
    sha256: List[str]
    metadata: Optional[Dict] = None


@router.post(
    "/{namespace}",
    summary="Add multiple bookmarks to a namespace",
    description="""
Add multiple bookmarks to a namespace.
The `sha256` values of the items to be bookmarked should be provided in the request body.
Optionally, metadata can be provided.
If metadata is provided, it should be a dictionary where the keys are the `sha256`
values and the values are dictionaries of metadata.
If the sha256 value is not in the metadata dictionary keys, the entire metadata dictionary
will be used as metadata for the the sha256 item.
You can use this to set the same metadata for all items.

Example request body:
```
{
    "sha256": ["<sha256_1>", "<sha256_2>", ...],
    "metadata": {
        "<sha256_1>: {
            "key1": "value1",
            "key2": "value2",
            ...
        },
        "key1": "value1",
        "key2": "value2",
        ...
    }
}
```
    """,
    response_model=MessageResult,
)
def add_bookmarks_by_sha256(
    namespace: str,
    items: ItemsMeta = Body(...),
    user: str = Query("user"),
    conn_args: Dict[str, Any] = Depends(get_db_user_data_wl),
):
    conn = get_database_connection(**conn_args)
    c = 0
    for s in items.sha256:
        if items.metadata:
            metadata = items.metadata
            if item_meta := metadata.get(s):
                metadata = item_meta
        else:
            metadata = None
        add_bookmark(conn, s, namespace=namespace, user=user, metadata=metadata)
        c += 1
    return MessageResult(message=f"Added {c} bookmarks")


@router.delete(
    "/{namespace}/{sha256}",
    summary="Delete a bookmark by namespace and sha256",
    response_model=MessageResult,
)
def delete_bookmark_by_sha256(
    namespace: str,
    sha256: str,
    user: str = Query("user"),
    conn_args: Dict[str, Any] = Depends(get_db_user_data_wl),
):
    conn = get_database_connection(**conn_args)
    remove_bookmark(conn, sha256, namespace=namespace, user=user)
    return MessageResult(message="Deleted bookmark")


@router.put(
    "/{namespace}/{sha256}",
    summary="Add a bookmark by namespace and sha256",
    description="""
Add a bookmark by namespace and sha256.
Optionally, metadata can be provided as the request body.
Metadata should be a dictionary of key-value pairs.
""",
    response_model=MessageResult,
)
def add_bookmark_by_sha256(
    namespace: str,
    sha256: str,
    user: str = Query("user"),
    metadata: Optional[Dict] = Body(None),
    conn_args: Dict[str, Any] = Depends(get_db_user_data_wl),
):
    conn = get_database_connection(**conn_args)
    add_bookmark(
        conn, sha256, namespace=namespace, user=user, metadata=metadata
    )
    return MessageResult(message="Added bookmark")


@dataclass
class BookmarkMetadata:
    exists: bool
    metadata: Optional[Dict] = None


@router.get(
    "/{namespace}/{sha256}",
    summary="Get a bookmark by namespace and sha256",
    description="""
Get a bookmark by namespace and sha256.
Returns whether the bookmark exists and the metadata.
    """,
    response_model=BookmarkMetadata,
)
def get_bookmark(
    namespace: str,
    sha256: str,
    user: str = Query("user"),
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
):
    conn = get_database_connection(**conn_args)
    exists, metadata = get_bookmark_metadata(
        conn, sha256, namespace=namespace, user=user
    )
    return BookmarkMetadata(exists=exists, metadata=metadata)
