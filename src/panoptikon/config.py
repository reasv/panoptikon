import logging
import os

import tomli
import tomli_w

from panoptikon.folders import clean_folder_list
from panoptikon.types import SystemConfig

logger = logging.getLogger(__name__)


def get_config_path(index: str) -> str:
    data_dir = os.getenv("DATA_FOLDER", "data")
    index_db_dir = os.path.join(data_dir, "index")
    index_dir = os.path.join(index_db_dir, index)
    os.makedirs(index_dir, exist_ok=True)
    return os.path.join(index_dir, "config.toml")


def persist_system_config(name: str, config: SystemConfig):
    if len(config.included_folders):
        config.included_folders = clean_folder_list(config.included_folders)
    if len(config.excluded_folders):
        config.excluded_folders = clean_folder_list(config.excluded_folders)
    config_file = get_config_path(name)
    config_dict = config.model_dump(exclude_none=True)
    serialized = tomli_w.dumps(config_dict)
    with open(config_file, "w", encoding="utf-8") as f:
        f.write(serialized)


def retrieve_system_config(name: str) -> SystemConfig:
    config_file = get_config_path(name)
    if not os.path.exists(config_file):
        config = SystemConfig()
        persist_system_config(name, config)
        return config
    with open(config_file, "rb") as f:
        config_dict = tomli.load(f)
    return SystemConfig(**config_dict)
