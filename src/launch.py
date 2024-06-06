import os

readonly_mode = os.environ.get('READONLY', 'false').lower() == 'true'

if readonly_mode:
    import src.ui.root_readonly as root
    print("Running in read-only mode")
else:
    import src.ui.root as root
    print("Running in read-write mode")

from src.db import initialize_database, get_database_connection

def launch_app():
    conn = get_database_connection()
    initialize_database(conn)
    conn.commit()
    conn.close()
    root.create_root_UI()