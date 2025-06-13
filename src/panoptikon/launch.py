import logging
import os
import sqlite3

import uvicorn
import static_ffmpeg
from dotenv import load_dotenv
load_dotenv()
from panoptikon.log import setup_logging
setup_logging()
from panoptikon.signal_handler import setup_signal_handlers
from panoptikon.api.app import get_app
from panoptikon.db import run_migrations
from panoptikon.db.pql.build_table_meta import build_metadata

logger = logging.getLogger(__name__)

def launch_app():
    static_ffmpeg.add_paths()  # blocks until files are downloaded
    setup_signal_handlers()
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
    # Retrieve and print the SQLite version
    logger.info(f"SQLite version: {sqlite3.sqlite_version}")
    logger.info(f"Starting API server at http://{hostname}:{port}/")
    uvicorn.run(
        app,
        host=hostname,
        port=port,
        log_level="error",
        # workers=int(os.getenv("UVICORN_WORKERS", "8")),
    )
