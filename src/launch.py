import os

import gradio as gr
import uvicorn
from gradio.routes import mount_gradio_app

from src.db import get_database_connection, initialize_database
from src.db.folders import get_folders_from_database
from src.fapi.app import app

readonly_mode = os.environ.get("READONLY", "false").lower() == "true"

if readonly_mode:
    import src.ui.root_readonly as root

    print("Running in read-only mode")
else:
    import src.ui.root as root

    print("Running in read-write mode")


def run_database_migrations():
    conn = get_database_connection(write_lock=True)
    cursor = conn.cursor()
    cursor.execute("BEGIN")
    initialize_database(conn)
    conn.commit()
    conn.close()


def launch_app():
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
