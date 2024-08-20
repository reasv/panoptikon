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
