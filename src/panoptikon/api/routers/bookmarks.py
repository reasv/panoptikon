import logging
from os import name
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query
from pydantic import BaseModel
from pydantic.dataclasses import dataclass

from panoptikon.api.routers.utils import get_db_readonly, get_db_user_data_wl
from panoptikon.db import get_database_connection
from panoptikon.db.bookmarks import (
    add_bookmark,
    delete_bookmarks_exclude_last_n,
    get_all_bookmark_namespaces,
    get_all_bookmark_users,
    get_bookmark_metadata,
    get_bookmarks,
    get_bookmarks_item,
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
    "/ns",
    summary="Get all bookmark namespaces",
    response_model=BookmarkNamespaces,
)
def get_ns_list(
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
):
    conn = get_database_connection(**conn_args)
    try:
        return BookmarkNamespaces(namespaces=get_all_bookmark_namespaces(conn))
    finally:
        conn.close()


class Items(BaseModel):
    sha256: List[str]


@dataclass
class BookmarkUsers:
    users: List[str]


@router.get(
    "/users",
    summary="Get all users with bookmarks",
    response_model=BookmarkUsers,
)
def get_user_list(
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
):
    conn = get_database_connection(**conn_args)
    try:
        return BookmarkUsers(users=get_all_bookmark_users(conn))
    finally:
        conn.close()


@dataclass
class Results:
    count: int
    results: List[FileSearchResult]


@router.get(
    "/ns/{namespace}",
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
    namespace: str = Path(
        ...,
        description="The namespace to get the bookmarks from. Wildcard ('*') results in getting bookmarks from all namespaces.",
    ),
    user: str = Query("user"),
    page_size: int = Query(1000),
    page: int = Query(1),
    order_by: Literal["last_modified", "path", "time_added"] = Query(
        "time_added"
    ),
    order: OrderType = Query(None),
    include_wildcard: bool = Query(
        True,
        description="Whether or not to include bookmarks set under the wildcard user.",
    ),
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
):
    conn = get_database_connection(**conn_args)
    try:
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
    finally:
        conn.close()


@dataclass
class MessageResult:
    message: str


@router.delete(
    "/ns/{namespace}",
    summary="Delete all/many bookmarks in a namespace",
    description="""
Delete all bookmarks in a namespace. If `exclude_last_n` is provided, the last `n` added bookmarks will be kept.
Alternatively, a list of `sha256` values can be provided in the request body to only delete specific bookmarks.
""",
    response_model=MessageResult,
)
def delete_bookmarks_by_namespace(
    namespace: str = Path(
        ...,
        description="The namespace to delete the bookmarks from. Wildcard ('*') results in deleting bookmarks from all namespaces.",
    ),
    user: str = Query(
        "user", description="The user to delete the bookmarks from."
    ),
    exclude_last_n: int = Query(0),
    items: Optional[Items] = Body(None),
    conn_args: Dict[str, Any] = Depends(get_db_user_data_wl),
):
    conn = get_database_connection(**conn_args)
    try:
        conn.execute("BEGIN TRANSACTION")
        if items and items.sha256:
            c = 0
            for s in items.sha256:
                remove_bookmark(conn, s, namespace=namespace, user=user)
                c += 1
            conn.commit()
            return MessageResult(message=f"Deleted {c} bookmarks")
        elif items is None:
            delete_bookmarks_exclude_last_n(
                conn,
                exclude_last_n,
                namespace=namespace,
                user=user,
            )
            conn.commit()
            return MessageResult(message="Deleted bookmarks")
        else:
            conn.rollback()
            return MessageResult(message="No items provided")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error deleting bookmarks: {e}")
        raise HTTPException(status_code=500, detail="Error deleting bookmarks")
    finally:
        conn.close()


class ItemsMeta(BaseModel):
    sha256: List[str]
    metadata: Optional[Dict] = None


@router.post(
    "/ns/{namespace}",
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
    namespace: str = Path(
        ...,
        description="The namespace to save the bookmarks under. Wildcard is not allowed here.",
    ),
    items: ItemsMeta = Body(...),
    user: str = Query(
        "user",
        description="The user to save the bookmark under. The wildcard '*' can be used to set `wildcard user` bookmarks that apply to all users.",
    ),
    conn_args: Dict[str, Any] = Depends(get_db_user_data_wl),
):
    if namespace == "*":
        raise HTTPException(
            status_code=400,
            detail="Cannot add bookmarks to wildcard namespace",
        )
    conn = get_database_connection(**conn_args)
    try:
        conn.execute("BEGIN TRANSACTION")
        c = 0
        for s in items.sha256:
            if items.metadata:
                metadata = items.metadata
                if item_meta := metadata.get(s):
                    metadata = item_meta
            else:
                metadata = None
            add_bookmark(
                conn, s, namespace=namespace, user=user, metadata=metadata
            )
            c += 1
        conn.commit()
        return MessageResult(message=f"Added {c} bookmarks")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error adding bookmarks: {e}")
        raise HTTPException(status_code=500, detail="Error adding bookmarks")
    finally:
        conn.close()


@router.delete(
    "/ns/{namespace}/{sha256}",
    summary="Delete a specific bookmark by namespace and sha256",
    response_model=MessageResult,
)
def delete_bookmark_by_sha256(
    namespace: str = Path(
        ...,
        description="The namespace to delete the bookmark from. Wildcard ('*') results in deleting bookmarks for an item from all namespaces.",
    ),
    sha256: str = Path(..., description="The sha256 of the item"),
    user: str = Query(
        "user", description="The user to delete the bookmark from."
    ),
    conn_args: Dict[str, Any] = Depends(get_db_user_data_wl),
):
    conn = get_database_connection(**conn_args)
    try:
        conn.execute("BEGIN TRANSACTION")
        remove_bookmark(conn, sha256, namespace=namespace, user=user)
        conn.commit()
        return MessageResult(message="Deleted bookmark")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error deleting bookmark: {e}")
        raise HTTPException(status_code=500, detail="Error deleting bookmark")
    finally:
        conn.close()


@router.put(
    "/ns/{namespace}/{sha256}",
    summary="Add a bookmark by namespace and sha256",
    description="""
Add a bookmark by namespace and sha256.
Optionally, metadata can be provided as the request body.
Metadata should be a dictionary of key-value pairs.
""",
    response_model=MessageResult,
)
def add_bookmark_by_sha256(
    namespace: str = Path(
        ...,
        description="The namespace to save the bookmark under. Wildcard is not allowed here.",
    ),
    sha256: str = Path(..., description="The sha256 of the item"),
    user: str = Query(
        "user",
        description="The user to save the bookmark under. The wildcard '*' can be used to set `wildcard user` bookmarks that apply to all users.",
    ),
    metadata: Optional[Dict] = Body(None),
    conn_args: Dict[str, Any] = Depends(get_db_user_data_wl),
):
    if namespace == "*":
        raise HTTPException(
            status_code=400,
            detail="Cannot add bookmarks to wildcard namespace",
        )
    conn = get_database_connection(**conn_args)
    try:
        conn.execute("BEGIN TRANSACTION")
        add_bookmark(
            conn, sha256, namespace=namespace, user=user, metadata=metadata
        )
        conn.commit()
        return MessageResult(message="Added bookmark")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error adding bookmark: {e}")
        raise HTTPException(status_code=500, detail="Error adding bookmark")
    finally:
        conn.close()


@dataclass
class BookmarkMetadata:
    exists: bool
    namespace: Optional[str] = None
    metadata: Optional[Dict] = None


@router.get(
    "/ns/{namespace}/{sha256}",
    summary="Get a bookmark by namespace and sha256",
    description="""
Get a bookmark by namespace and sha256.
Returns whether the bookmark exists and the metadata.
    """,
    response_model=BookmarkMetadata,
)
def get_bookmark(
    namespace: str = Path(
        ...,
        description="The namespace to get the bookmark from. Use '*' wildcard to mean 'any namespace', in which case it will return the first result found.",
    ),
    sha256: str = Path(..., description="The sha256 of the item"),
    user: str = Query(
        "user",
        description="The user to get the bookmark from. The wildcard '*' can be used to get `wildcard user` bookmarks that apply to all users.",
    ),
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
):
    conn = get_database_connection(**conn_args)
    try:
        exists, ns, metadata = get_bookmark_metadata(
            conn, sha256, namespace=namespace, user=user
        )
        return BookmarkMetadata(exists=exists, namespace=ns, metadata=metadata)
    finally:
        conn.close()


@dataclass
class ExistingBookmarkMetadata:
    namespace: Optional[str] = None
    metadata: Optional[Dict] = None


@dataclass
class ItemBookmarks:
    bookmarks: List[ExistingBookmarkMetadata]


@router.get(
    "/item/{sha256}",
    summary="Get all bookmarks for an item",
    description="""
Get all bookmarks for an item.
Returns a list of namespaces and metadata for each bookmark.
    """,
    response_model=ItemBookmarks,
)
def get_bookmarks_for_item(
    sha256: str,
    user: str = Query(
        "user",
        description="The user to get the bookmark from. The wildcard '*' can be used to get `wildcard user` bookmarks that apply to all users.",
    ),
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
) -> ItemBookmarks:
    conn = get_database_connection(**conn_args)
    try:
        return ItemBookmarks(
            bookmarks=[
                ExistingBookmarkMetadata(namespace=ns, metadata=metadata)
                for ns, metadata in get_bookmarks_item(conn, sha256, user=user)
            ]
        )
    finally:
        conn.close()
