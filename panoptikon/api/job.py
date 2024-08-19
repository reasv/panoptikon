import logging
from datetime import datetime

from croniter import croniter

from panoptikon.cronjob import run_cronjob
from panoptikon.db import get_database_connection
from panoptikon.db.config import retrieve_system_config

logger = logging.getLogger(__name__)


# Function to get the cron string
def get_cron_string():
    conn = get_database_connection(write_lock=False)
    system_config = retrieve_system_config(conn)
    conn.close()
    if system_config.enable_cron_job:
        return system_config.cron_schedule
    return None


# Variable to store the next scheduled time
next_scheduled_time = None
current_cron_string = None


# Function to initialize or update the next scheduled time
def update_schedule():
    global next_scheduled_time, current_cron_string
    cron_string = get_cron_string()

    # Check if the cron string is None or has changed
    if cron_string is None:
        # If cron string is None, disable scheduling
        next_scheduled_time = None
        current_cron_string = None
    elif cron_string != current_cron_string or next_scheduled_time is None:
        # If the cron string has changed or first initialization
        now = datetime.now()
        iter = croniter(cron_string, now)
        next_scheduled_time = iter.get_next(datetime)
        current_cron_string = cron_string


# Function to check and run the task if scheduled
def try_cronjob():
    global next_scheduled_time
    # Always update the schedule with the latest cron string
    update_schedule()
    if next_scheduled_time and datetime.now() >= next_scheduled_time:
        # Call the task function
        run_cronjob()
        next_scheduled_time = None
        # Update the schedule after running the task
        update_schedule()
        logger.info(f"Next scheduled time: {next_scheduled_time}")
