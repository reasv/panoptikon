from typing import List

from src.data_extractors.models import ModelOpts, ModelOptsFactory
from src.db import get_database_connection
from src.db.rules.rules import get_rules
from src.folders import rescan_all_folders


def run_cronjob():
    conn = get_database_connection(write_lock=True)
    print("Running cronjob")
    conn.execute("BEGIN TRANSACTION")
    rescan_all_folders(conn)
    conn.commit()
    print("Rescanned all folders")
    rules = get_rules(conn)
    setters = []
    for rule in rules:
        setters.extend([setter for _, setter in rule.setters])
    setters = list(set(setters))
    print(f"Found {len(setters)} models to run ({','.join(setters)})")
    model_opts: List[ModelOpts] = []
    for setter in setters:
        model_opts.append(ModelOptsFactory.get_model(setter))

    for model_opt in model_opts:
        print(f"Running model {model_opt}")
        conn.execute("BEGIN TRANSACTION")
        model_opt.run_extractor(conn)
        conn.commit()
    conn.close()
    print("Cronjob finished")
