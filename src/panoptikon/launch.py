import logging
import os
import sqlite3

import uvicorn
import static_ffmpeg
from dotenv import load_dotenv

from searchui.router import run_node_client
load_dotenv()
from panoptikon.log import setup_logging
setup_logging()
from panoptikon.signal_handler import setup_signal_handlers
from panoptikon.db import run_migrations
from panoptikon.db.pql.build_table_meta import build_metadata

logger = logging.getLogger(__name__)

def launch_app():
    static_ffmpeg.add_paths()  # blocks until files are downloaded
    from inferio.cudnnsetup import cudnn_setup
    cudnn_setup()
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
    if os.getenv("ENABLE_CLIENT", "true").lower() in ["true", "1"] and os.getenv("CLIENT_URL") is None:
        client_host = os.getenv("CLIENT_HOST", hostname)
        client_port = int(os.getenv("CLIENT_PORT", 6339))
        run_node_client(client_host, client_port)

    # Retrieve and print the SQLite version
    logger.info(f"SQLite version: {sqlite3.sqlite_version}")
    logger.info(f"Starting API server at http://{hostname}:{port}/")
    workers  = os.getenv("UVICORN_WORKERS", str(1))
    if workers == "1":
        from panoptikon.api.app import get_app
        app = get_app()
        return uvicorn.run(
            app,
            host=hostname,
            port=port,
            log_level="error",
        )
    
    logger.info(f"Using {workers} workers for Uvicorn")
    return uvicorn.run(
        "panoptikon.api.app:get_app",
        host=hostname,
        port=port,
        log_level="error",
        factory=True,
        workers=int(os.getenv("UVICORN_WORKERS", "8")),
    )
