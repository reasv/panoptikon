import logging
import os

import uvicorn

from panoptikon.api.app import get_app
from panoptikon.db import run_migrations
from panoptikon.db.pql.build_table_meta import build_metadata
from panoptikon.log import setup_logging

setup_logging()

logger = logging.getLogger(__name__)


def launch_app():
    readonly_mode = os.environ.get("READONLY", "false").lower() in ["true", "1"]
    if not readonly_mode:
        logger.info("Running in read-write mode")
        run_migrations()
        build_metadata()
    else:
        logger.info("Running in read-only mode")
    hostname = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", 6342))
    app = get_app(hostname, port)
    logger.info(f"Starting API server at http://{hostname}:{port}/")
    uvicorn.run(
        app,
        host=hostname,
        port=port,
        log_level=os.getenv("GRADIO_LOGLEVEL", "error"),
    )
