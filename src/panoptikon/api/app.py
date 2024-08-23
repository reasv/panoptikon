import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from typing import Any, Callable, Dict, List

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi_utilities.repeat.repeat_at import repeat_at
from pydantic.dataclasses import dataclass

import inferio
import panoptikon.api.routers.bookmarks as bookmarks
import panoptikon.api.routers.items as items
import panoptikon.api.routers.legacy as legacy
import panoptikon.api.routers.search as search
from panoptikon.api.job import try_cronjob
from panoptikon.api.routers.utils import get_db_readonly
from panoptikon.db import get_database_connection, get_db_lists, get_db_names
from panoptikon.db.files import (
    get_existing_file_for_sha256,
    get_item_metadata_by_sha256,
)
from panoptikon.utils import open_file, open_in_explorer
from searchui.router import get_routers

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    cronjob()
    await inferio.check_ttl()
    yield


@repeat_at(cron="* * * * *", logger=logger)
def cronjob():
    try_cronjob()


app = FastAPI(lifespan=lifespan)

if os.getenv("LEGACY_GALLERY", "false").lower() == "true":
    app.include_router(legacy.router)


@dataclass
class SingleDBInfo:
    current: str
    all: List[str]


@dataclass
class DBInfo:
    index: SingleDBInfo
    user_data: SingleDBInfo


@app.get(
    "/api/db",
    summary="Get information about all available databases",
    description="""
Get information about the database, including the names of all other available databases.
Most API endpoints support specifying the databases to use for index and user data
through the `index_db` and `user_data_db` query parameters.
Regardless of which database is currently being used by panoptikon,
the API allows you to perform actions and query data from any of the available databases.
The current databases are simply the ones that are used by default.
    """,
    response_model=DBInfo,
    tags=["database"],
)
def get_db_info():
    index_db, user_data_db, _ = get_db_names()
    index_dbs, user_data_dbs = get_db_lists()
    return DBInfo(
        index=SingleDBInfo(current=index_db, all=index_dbs),
        user_data=SingleDBInfo(current=user_data_db, all=user_data_dbs),
    )


@dataclass
class OpenResponse:
    path: str
    message: str


def get_correct_path(conn: sqlite3.Connection, sha256: str, path: str):
    if not path:
        file = get_existing_file_for_sha256(conn, sha256)
        if not file:
            raise HTTPException(status_code=404, detail="File not found")
        path = file.path
    else:
        path = path.strip()
        _, files = get_item_metadata_by_sha256(conn, sha256)
        if not files or not any(f.path == path for f in files):
            logger.debug(
                f"File {path} not found in {', '.join([f.path for f in (files or [])])}"
            )
            raise HTTPException(
                status_code=404,
                detail=f"File {path} not found in {', '.join([f.path for f in (files or [])])}",
            )
    return path


@app.post(
    "/api/open/file/{sha256}",
    summary="Open a file in the default application",
    description="""
Open a file in the default application on the host system.
This is done using os.startfile on Windows and xdg-open on Linux.
This is a potentially dangerous operation, as it can execute arbitrary code.
""",
    tags=["open"],
    response_model=OpenResponse,
)
def open_file_on_host(
    sha256: str,
    path: str = Query(None),
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
):
    conn = get_database_connection(**conn_args)
    path = get_correct_path(conn, sha256, path)
    msg = open_file(path)

    return OpenResponse(path=path, message=msg)


@app.post(
    "/api/open/folder/{sha256}",
    summary="Show a file in the host system's file manager",
    description="""
Show a file in the host system's file manager.
This is done using the appropriate command for the host system.
On Windows, the file is highlighted in the Windows Explorer.
On macOS, the file is revealed in the Finder.
This is a potentially dangerous operation.
""",
    tags=["open"],
    response_model=OpenResponse,
)
def show_in_file_manager(
    sha256: str,
    path: str = Query(None),
    conn_args: Dict[str, Any] = Depends(get_db_readonly),
):
    conn = get_database_connection(**conn_args)
    path = get_correct_path(conn, sha256, path)
    msg = open_in_explorer(path)

    return OpenResponse(path=path, message=msg)


app.include_router(search.router)
app.include_router(items.router)
app.include_router(bookmarks.router)
app.include_router(inferio.router)


def get_app(
    hostname: str, port: int, callback: Callable[[FastAPI], None]
) -> FastAPI:
    # Add the reverse HTTP and WebSocket routers
    callback(app)
    if os.getenv("ENABLE_CLIENT", "false").lower() == "true":
        client_redirect_router, client_url = get_routers(hostname, port)
        app.include_router(client_redirect_router)
        # allowed_origins = [
        #     client_url[:-1],
        # ]
        # logger.debug(f"Allowed origins: {allowed_origins}")
        # # Add CORS middleware to allow requests from the specific origin
        # app.add_middleware(
        #     CORSMiddleware,
        #     allow_origins=allowed_origins,  # Allow requests from this list of origins
        #     allow_credentials=True,
        #     allow_methods=["*"],  # Allow all methods (GET, POST, etc.)
        #     allow_headers=["*"],  # Allow all headers
        # )
    else:

        @app.get("/")
        async def redirect_to_gradio():
            return RedirectResponse(url="/gradio/")

    return app
