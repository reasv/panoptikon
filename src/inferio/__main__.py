from dotenv import load_dotenv

load_dotenv()
from panoptikon.log import setup_logging

setup_logging()
import os

from fastapi import FastAPI
from fastapi.concurrency import asynccontextmanager

from inferio.router import check_ttl, router


@asynccontextmanager
async def lifespan(app: FastAPI):
    await check_ttl()
    yield


def launch_app():
    app = FastAPI(
        lifespan=lifespan,
        separate_input_output_schemas=False,
    )
    app.include_router(router)
    import uvicorn

    host = os.getenv("INFERIO_HOST", "127.0.0.1")
    port = int(os.getenv("INFERIO_PORT", "7777"))
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    launch_app()
