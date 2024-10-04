import logging
from typing import List

from panoptikon.config import retrieve_system_config
from panoptikon.data_extractors.models import ModelOpts, ModelOptsFactory
from panoptikon.db import get_database_connection
from panoptikon.db.rules.rules import get_rules
from panoptikon.db.utils import vacuum_database
from panoptikon.folders import rescan_all_folders

logger = logging.getLogger(__name__)


def run_cronjob(index_db: str):
    conn = get_database_connection(write_lock=True, index_db=index_db)
    try:
        system_config = retrieve_system_config(index_db)
        logger.info("Running cronjob")
        conn.execute("BEGIN TRANSACTION")
        rescan_all_folders(conn, system_config)
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

        src_model_opts = [
            model
            for model in model_opts
            if model.target_entities() == ["items"]
        ]
        derived_model_opts = [
            model
            for model in model_opts
            if model.target_entities() != ["items"]
        ]
        # Run source models first
        model_opts = src_model_opts + derived_model_opts
        for model_opt in model_opts:
            logger.info(f"Running model {model_opt}")
            conn.execute("BEGIN TRANSACTION")
            model_opt.run_extractor(conn)
            conn.commit()

        vacuum_database(conn)
        logger.info("Cronjob finished")
    finally:
        conn.close()
