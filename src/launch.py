from src.ui import create_UI
from src.db import initialize_database, get_database_connection
def launch_app():
    conn = get_database_connection()
    initialize_database()
    conn.commit()
    conn.close()
    create_UI()