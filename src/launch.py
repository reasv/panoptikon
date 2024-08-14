import logging
import os

import gradio as gr
import uvicorn
from gradio.routes import mount_gradio_app

from src.api.app import app
from src.db import get_database_connection, initialize_database
from src.db.folders import get_folders_from_database
from src.log import setup_logging
from src.utils import add_cudnn_to_path

add_cudnn_to_path()
setup_logging()
readonly_mode = os.environ.get("READONLY", "false").lower() == "true"

logger = logging.getLogger(__name__)

if readonly_mode:
    import src.ui.root_readonly as root

    logger.info("Running in read-only mode")
else:
    import src.ui.root as root

    logger.info("Running in read-write mode")


def run_database_migrations():
    conn = get_database_connection(write_lock=True, user_data_wl=True)
    cursor = conn.cursor()
    cursor.execute("BEGIN")
    initialize_database(conn)
    conn.commit()
    conn.close()


def launch_app():
    index = os.getenv("INDEX_DB", "default")
    user_data = os.getenv("USER_DATA_DB", "default")

    logger.info(f"Index DB: {index}")
    logger.info(f"User Data DB: {user_data}")
    if not readonly_mode:
        run_database_migrations()
    conn = get_database_connection(write_lock=False)
    folders = get_folders_from_database(conn, included=True)
    conn.close()
    # gr.set_static_paths(folders)
    ui = root.create_root_UI()
    mount_gradio_app(app, ui, path="/gradio")
    uvicorn.run(
        app,
        host=os.getenv("HOSTNAME", "127.0.0.1"),
        port=int(os.getenv("PORT", 7860)),
        log_level=os.getenv("LOG_LEVEL", "error"),
    )
