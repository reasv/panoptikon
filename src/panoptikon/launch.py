import logging
import os

import gradio as gr
import uvicorn
from fastapi import FastAPI
from gradio.routes import mount_gradio_app

from panoptikon.api.app import get_app
from panoptikon.db import get_database_connection, run_migrations
from panoptikon.db.folders import get_folders_from_database
from panoptikon.log import setup_logging

setup_logging()
readonly_mode = os.environ.get("READONLY", "false").lower() == "true"

logger = logging.getLogger(__name__)

if readonly_mode:
    import panoptikon.ui.root_readonly as root

    logger.info("Running in read-only mode")
else:
    import panoptikon.ui.root as root

    logger.info("Running in read-write mode")


def launch_app():
    index = os.getenv("INDEX_DB", "default")
    user_data = os.getenv("USER_DATA_DB", "default")

    logger.info(f"Index DB: {index}")
    logger.info(f"User Data DB: {user_data}")
    if not readonly_mode:
        run_migrations()
    conn = get_database_connection(write_lock=False)
    folders = get_folders_from_database(conn, included=True)
    conn.close()
    # gr.set_static_paths(folders)
    ui = root.create_root_UI()
    hostname = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", 6342))

    def gradio_mount(app: FastAPI):
        mount_gradio_app(app, ui, path="/gradio")

    app = get_app(hostname, port, gradio_mount)
    logger.info(f"Starting server at http://{hostname}:{port}/gradio/")
    uvicorn.run(
        app,
        host=hostname,
        port=port,
        log_level=os.getenv("GRADIO_LOGLEVEL", "error"),
    )
