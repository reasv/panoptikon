import logging
import os


def setup_logging():
    # Read log level from environment variable
    loglevel = os.getenv("LOGLEVEL", "INFO").upper()
    # Get logs folder from environment variable
    logs_file = os.getenv("LOGS_FILE", "./data/panoptikon.log")
    # Ensure the directory for the log file exists
    logs_folder = os.path.dirname(logs_file)
    os.makedirs(logs_folder, exist_ok=True)
    # Set up basic configuration for logging
    logging.basicConfig(
        level=loglevel,
        format="%(asctime)s [%(levelname)s][%(name)s] - %(message)s",
        handlers=[
            logging.FileHandler(logs_file),
            logging.StreamHandler(),
        ],
    )
