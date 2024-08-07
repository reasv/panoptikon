from src.db import get_database_connection
from src.folders import rescan_all_folders


def run_cronjob():
    conn = get_database_connection(write_lock=True)
    rescan_all_folders(conn)
    conn.close()
