import logging
from datetime import datetime
from typing import Dict

from croniter import croniter

from panoptikon.api.cronjob.job import run_cronjob
from panoptikon.config import retrieve_system_config
from panoptikon.db import get_db_lists

logger = logging.getLogger(__name__)


# Function to get the cron string
def get_cron_string(index_db: str) -> str | None:
    system_config = retrieve_system_config(index_db)
    if system_config.enable_cron_job:
        return system_config.cron_schedule
    return None


# Variable to store the next scheduled time
next_scheduled_time: Dict[str, datetime] = {}
current_cron_string: Dict[str, str] = {}


# Function to initialize or update the next scheduled time
def update_schedule(index_db: str):
    global next_scheduled_time, current_cron_string
    cron_string = get_cron_string(index_db=index_db)

    # Check if the cron string is None or has changed
    if cron_string is None:
        # If cron string is None, disable scheduling
        del next_scheduled_time[index_db]
        del current_cron_string[index_db]
    elif (
        cron_string != current_cron_string.get(index_db)
        or next_scheduled_time.get(index_db) is None
    ):
        # If the cron string has changed or first initialization
        now = datetime.now()
        iter = croniter(cron_string, now)
        next_scheduled_time[index_db] = iter.get_next(datetime)
        current_cron_string[index_db] = cron_string


# Function to check and run the task if scheduled
def try_cronjob(index_db: str):
    global next_scheduled_time
    # Always update the schedule with the latest cron string
    update_schedule(index_db=index_db)
    if (next_scheduled_time.get(index_db) is not None) and (
        datetime.now() >= next_scheduled_time.get(index_db, datetime.now())
    ):
        # Call the task function
        run_cronjob(index_db=index_db)
        del next_scheduled_time[index_db]
        # Update the schedule after running the task
        update_schedule(index_db=index_db)
        logger.info(f"Next scheduled time: {next_scheduled_time.get(index_db)}")


def try_cronjobs():
    # Loop through all the index dbs
    for index_db in get_db_lists()[0]:
        try_cronjob(index_db=index_db)
