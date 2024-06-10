import os

from gradio.routes import mount_gradio_app
import uvicorn

from src.db import initialize_database, get_database_connection
from src.fapi.app import app

readonly_mode = os.environ.get('READONLY', 'false').lower() == 'true'

if readonly_mode:
    import src.ui.root_readonly as root
    print("Running in read-only mode")
else:
    import src.ui.root as root
    print("Running in read-write mode")

def launch_app():
    conn = get_database_connection()
    initialize_database(conn)
    conn.commit()
    conn.close()
    ui = root.create_root_UI()
    mount_gradio_app(app, ui, path="/")
    uvicorn.run(app, host=os.getenv("HOSTNAME", "127.0.0.1"), port=os.getenv("PORT", 7860), log_level=os.getenv("LOG_LEVEL", "debug"))