import os

import tomli
import tomli_w

from panoptikon.types import SystemConfig


def get_config_path(name: str) -> str:
    data_dir = os.getenv("DATA_FOLDER", "data")
    config_dir = os.path.join(data_dir, "configs")
    os.makedirs(config_dir, exist_ok=True)
    return os.path.join(config_dir, f"{name}.toml")


def persist_system_config(name: str, config: SystemConfig):
    config_file = get_config_path(name)
    config_dict = config.model_dump()
    with open(config_file, "wb") as f:
        tomli_w.dump(config_dict, f)


def retrieve_system_config(name: str) -> SystemConfig:
    config_file = get_config_path(name)
    if not os.path.exists(config_file):
        config = SystemConfig()
        persist_system_config(name, config)
        return config
    with open(config_file, "rb") as f:
        config_dict = tomli.load(f)
    return SystemConfig(**config_dict)
