import asyncio
import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from typing import Any, Dict, List

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi_utilities.repeat.repeat_at import repeat_at
from pydantic import BaseModel
from pydantic.dataclasses import dataclass

import inferio
import panoptikon.api.routers.bookmarks as bookmarks
import panoptikon.api.routers.items as items
import panoptikon.api.routers.search as search
from panoptikon.api.cronjob.schedule import try_cronjob
from panoptikon.api.preload import preload_embedding_models
from panoptikon.api.routers import jobs
from panoptikon.api.routers.utils import get_db_readonly
from panoptikon.db import (
    get_database_connection,
    get_db_default_names,
    get_db_lists,
    run_migrations,
    set_db_names,
)
from panoptikon.db.files import (
    get_existing_file_for_sha256,
    get_item_metadata_by_sha256,
)
from panoptikon.utils import get_inference_api_url, is_external_inference_api, open_file, show_in_fm
from searchui.router import get_routers

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    cronjob()
    await inferio.check_ttl()
    yield


@repeat_at(cron="* * * * *", logger=logger)
def cronjob():
    # Loop through all the index dbs
    for index_db in get_db_lists()[0]:
        try_cronjob(index_db=index_db)
        preload_embedding_models(index_db=index_db)


app = FastAPI(
    lifespan=lifespan,
    separate_input_output_schemas=False,
)


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
Get the name of the current default databases and a list of all available databases.
Most API endpoints support specifying the databases to use for index and user data
through the `index_db` and `user_data_db` query parameters.
Regardless of which database is currently being defaulted to by panoptikon,
the API allows you to perform actions and query data from any of the available databases.
The current databases are simply the ones that are used by default.
    """,
    response_model=DBInfo,
    tags=["database"],
)
def get_db_info():
    index_db, user_data_db = get_db_default_names()
    index_dbs, user_data_dbs = get_db_lists()
    return DBInfo(
        index=SingleDBInfo(current=index_db, all=index_dbs),
        user_data=SingleDBInfo(current=user_data_db, all=user_data_dbs),
    )


class DBCreateResponse(BaseModel):
    index_db: str
    user_data_db: str


@app.post(
    "/api/db/create",
    summary="Create new databases",
    description="""
Create new databases with the specified names.
It runs the migration scripts on the provided database names.
If the databases already exist, the effect is the same as running the migrations.
    """,
    tags=["database"],
)
def create_db(
    new_index_db: str = Query(None),
    new_user_data_db: str = Query(None),
) -> DBCreateResponse:
    default_index_db, default_user_data_db = get_db_default_names()
    if new_index_db:
        index_db = new_index_db
    else:  # Use the default index database
        index_db = default_index_db
    if new_user_data_db:
        user_data_db = new_user_data_db
    else:  # Use the default user data database
        user_data_db = default_user_data_db
    # Set the new database names as current databases
    set_db_names(index_db, user_data_db)
    # Run migrations to create the new databases
    run_migrations()
    # Set the default databases back to the original values
    set_db_names(default_index_db, default_user_data_db)

    return DBCreateResponse(
        index_db=index_db,
        user_data_db=user_data_db,
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
    try:
        path = get_correct_path(conn, sha256, path)
        open_file(path)

        return OpenResponse(path=path, message=f"Attempting to open: {path}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


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
    try:
        path = get_correct_path(conn, sha256, path)
        show_in_fm(path)

        return OpenResponse(path=path, message=f"Attempting to open: {path}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


app.include_router(search.router)
app.include_router(items.router)
app.include_router(bookmarks.router)
app.include_router(inferio.router)
app.include_router(jobs.router)


def get_app(hostname: str, port: int) -> FastAPI:
    if os.getenv("ENABLE_CLIENT", "true").lower() == "true":
        client_redirect_router, client_url = get_routers(hostname, port)

        @app.middleware("http")
        async def proxy_middleware(request: Request, call_next):
            logger.debug(
                f"Received request: {request.method} {request.url.path}"
            )

            # If the request is for the API, let FastAPI handle it
            if (
                request.url.path.startswith("/api")
                or request.url.path.startswith("/docs")
                or request.url.path.startswith("/redoc")
                or request.url.path.startswith("/openapi.json")
            ):
                if not request.url.path.startswith(
                    "/api/inference"
                ) or not os.getenv(
                    "INFERENCE_API_URL"
                ) or not is_external_inference_api():
                    return await call_next(request)

            timeout = 60
            proxy_url = client_url
            # If the request is for the inference API, and we are using a custom URL for it, proxy it to the inference API
            if request.url.path.startswith("/api/inference"):
                proxy_url = get_inference_api_url()
                timeout = None
            # Otherwise, proxy the request to the Next.js frontend
            async with httpx.AsyncClient() as client:
                try:
                    # Construct the target URL
                    target_url = f"{proxy_url}{request.url.path}"
                    if request.url.query:
                        target_url += f"?{request.url.query}"

                    logger.debug(f"Proxying request to: {target_url}")

                    # Forward the request method, headers, and body
                    proxy_response = await client.request(
                        method=request.method,
                        url=target_url,
                        headers=request.headers.raw,
                        content=await request.body(),
                        timeout=timeout,
                    )

                    logger.debug(
                        f"Received response from frontend: {proxy_response.status_code}"
                    )
                except httpx.RequestError as exc:
                    logger.error(
                        f"Error proxying request to {proxy_url}: {exc}"
                    )
                    raise HTTPException(
                        status_code=502, detail=f"Error proxying request: {exc}"
                    )

            # Prepare headers by excluding certain problematic headers
            excluded_headers = {
                "content-encoding",
                "transfer-encoding",
                "connection",
                "keep-alive",
                "content-length",
                "keep-alive",
                "proxy-authenticate",
                "proxy-authorization",
                "te",
                "trailers",
                "upgrade",
            }

            headers = {
                k: v
                for k, v in proxy_response.headers.items()
                if k.lower() not in excluded_headers
            }

            logger.debug("Building response to client.")

            # Create the FastAPI response
            response = Response(
                content=proxy_response.content,
                status_code=proxy_response.status_code,
                headers=headers,
            )

            return response

    return app
