import logging
from typing import List

from panoptikon.data_extractors.models import ModelOpts, ModelOptsFactory
from panoptikon.db import get_database_connection
from panoptikon.db.rules.rules import get_rules
from panoptikon.db.utils import vacuum_database
from panoptikon.folders import rescan_all_folders

logger = logging.getLogger(__name__)


def run_cronjob():
    conn = get_database_connection(write_lock=True)
    logger.info("Running cronjob")
    conn.execute("BEGIN TRANSACTION")
    rescan_all_folders(conn)
    conn.commit()
    logger.info("Rescanned all folders")
    rules = get_rules(conn)
    setters = []
    for rule in rules:
        setters.extend(
            [setter for setter in rule.setters if setter != "file_scan"]
        )
    setters = list(set(setters))
    logger.info(f"Found {len(setters)} models to run ({','.join(setters)})")
    model_opts: List[ModelOpts] = []
    for setter in setters:
        model_opts.append(ModelOptsFactory.get_model(setter))

    for model_opt in model_opts:
        logger.info(f"Running model {model_opt}")
        conn.execute("BEGIN TRANSACTION")
        model_opt.run_extractor(conn)
        conn.commit()

    vacuum_database(conn)
    conn.close()
    logger.info("Cronjob finished")
