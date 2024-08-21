import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi_utilities.repeat.repeat_at import repeat_at

import inferio
import panoptikon.api.routers.legacy as legacy
import panoptikon.api.routers.search as search
from panoptikon.api.job import try_cronjob
from panoptikon.db import get_db_lists, get_db_names

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


app.include_router(inferio.router)
app.include_router(search.router)

if os.getenv("LEGACY_GALLERY", "false").lower() == "true":
    app.include_router(legacy.router)


# Redirect / to /gradio
@app.get("/")
async def redirect_to_gradio():
    return RedirectResponse(url="/gradio/")


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
    tags=["database"],
)
def get_db_info():
    index_db, user_data_db, _ = get_db_names()
    index_dbs, user_data_dbs = get_db_lists()
    return {
        "index": {
            "current": index_db,
            "all": index_dbs,
        },
        "user_data": {
            "current": user_data_db,
            "all": user_data_dbs,
        },
    }
