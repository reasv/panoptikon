from src.ui import create_UI
from src.db import initialize_database
def launch_app():
    initialize_database()
    create_UI()