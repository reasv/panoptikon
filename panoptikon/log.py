import io
import logging
import logging.config
import os
import sys

from panoptikon.db import get_db_names


def setup_logging():
    # Wrap sys.stdout to use UTF-8 encoding
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

    # Read log level from environment variable
    loglevel = os.getenv("LOGLEVEL", "INFO").upper()
    # Get logs folder from environment variable
    index, _, _ = get_db_names()
    data_dir = os.getenv("DATA_FOLDER", "data")
    default_logs_file = os.path.join(data_dir, "logs", f"{index}.log")
    logs_file = os.getenv("LOGS_FILE", default_logs_file)
    # Ensure the directory for the log file exists
    logs_folder = os.path.dirname(logs_file)
    os.makedirs(logs_folder, exist_ok=True)
    # Set up basic configuration for logging
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "loggers": {
                "PIL": {
                    "level": "WARNING",
                    "propagate": False,
                },
                "matplotlib": {
                    "level": "WARNING",
                    "propagate": False,
                },
                "weasyprint": {
                    "level": "ERROR",
                    "propagate": False,
                },
                "multipart.multipart": {
                    "level": "WARNING",
                    "propagate": False,
                },
                "speechbrain": {
                    "level": "WARNING",
                    "propagate": False,
                },
                "torio._extension.utils": {
                    "level": "WARNING",
                    "propagate": False,
                },
                "faster_whisper": {
                    "level": "DEBUG",
                    "propagate": False,
                },
            },
        }
    )
    logging.basicConfig(
        level=loglevel,
        format="%(asctime)s [%(levelname)s][%(name)s] - %(message)s",
        handlers=[
            logging.FileHandler(logs_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
