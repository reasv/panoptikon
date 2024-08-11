import logging
import os
from collections import defaultdict
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional, Type

import tomlkit

from src.inference.model import BaseModel

logger = logging.getLogger(__name__)


class ModelRegistry:
    _registry: Dict[str, Type["BaseModel"]] = (
        {}
    )  # Class property shared across all instances

    def __init__(
        self,
        base_folder: str,
        user_folder: str,
        allow_inference_id_overrides: bool = False,
    ) -> None:
        self._config: Dict[str, Dict[str, Any]] = {}  # Instance property
        self._last_modified_time: float = 0.0  # Instance property
        self._lock: Lock = Lock()  # Instance property, unique to each instance
        self.base_folder = Path(base_folder)  # Instance property
        self.user_folder = Path(user_folder)  # Instance property
        self.allow_inference_id_overrides = allow_inference_id_overrides
        self.reload_registry()

    @classmethod
    def register_model(
        cls, model_class: Type["BaseModel"], model_name: str
    ) -> None:
        """Register a BaseModel subclass with a given name."""
        cls._registry[model_name] = model_class

    def _get_latest_modified_time(self) -> float:
        """Get the latest modified time of all TOML files in the config folders."""
        latest_time = 0.0
        for folder in [self.base_folder, self.user_folder]:
            for file in sorted(folder.glob("*.toml")):
                file_time = file.stat().st_mtime
                if file_time > latest_time:
                    latest_time = file_time
        return latest_time

    def reload_registry(self) -> None:
        """Reload the registry if TOML files have been modified."""
        with self._lock:
            latest_time = self._get_latest_modified_time()
            if latest_time > self._last_modified_time:
                config_data = defaultdict(lambda: {"inference_ids": {}})

                # Load and merge configurations from both folders
                self._load_folder(self.base_folder, config_data)
                self._load_folder(self.user_folder, config_data)

                # Safely update the config
                self._config = dict(config_data)
                self._last_modified_time = latest_time
                logger.info("Model registry reloaded successfully")

    def _load_folder(
        self, folder: Path, config_data: Dict[str, Dict[str, Any]]
    ) -> None:
        """Load all TOML files from a folder in alphabetical order and merge them into config_data."""
        if folder.is_dir():
            for file in sorted(
                folder.glob("*.toml")
            ):  # Sort files for predictable loading
                try:
                    with open(file, "r") as f:
                        data = tomlkit.load(f)
                    for group_name, group_data in data.get("group", {}).items():
                        if "model_class" not in group_data:
                            raise ValueError(
                                f"Group '{group_name}' in {file} must define a 'model_class'"
                            )

                        # Store or merge group-level config and metadata separately
                        if "model_class" in config_data[group_name]:
                            if (
                                config_data[group_name]["model_class"]
                                != group_data["model_class"]
                            ):
                                raise ValueError(
                                    f"Conflicting model classes for group '{group_name}' in {file}"
                                )
                        else:
                            config_data[group_name]["model_class"] = group_data[
                                "model_class"
                            ]

                        # Merge group config, giving precedence to the latest loaded
                        config_data[group_name]["group_config"].update(
                            group_data.get("config", {})
                        )

                        # Merge group metadata, giving precedence to the latest loaded
                        config_data[group_name]["group_metadata"].update(
                            group_data.get("metadata", {})
                        )

                        # Process and merge inference IDs within the group
                        for inference_id, inf_data in group_data.get(
                            "inference_ids", {}
                        ).items():
                            if (
                                inference_id
                                in config_data[group_name]["inference_ids"]
                                and not self.allow_inference_id_overrides
                            ):
                                raise ValueError(
                                    f"Duplicate inference_id '{group_name}/{inference_id}' found in {file}"
                                )

                            # Merge group-level and inference_id-level config
                            inf_config = {
                                **config_data[group_name]["group_config"],
                                **inf_data.get("config", {}),
                            }

                            config_data[group_name]["inference_ids"][
                                inference_id
                            ] = {
                                "config": inf_config,
                                "metadata": inf_data.get("metadata", {}),
                            }
                except Exception as e:
                    logger.error(f"Error loading TOML file {file}: {e}")

    def get_model_instance(
        self, group_name: str, inference_id: str
    ) -> "BaseModel":
        """Retrieve and instantiate a BaseModel subclass based on the inference ID and group name."""
        self.reload_registry()  # Ensure the registry is up to date before retrieving a model
        with self._lock:
            if group_name not in self._config:
                raise ValueError(f"Group '{group_name}' not found in registry")
            group_data = self._config[group_name]
            if inference_id not in group_data["inference_ids"]:
                raise ValueError(
                    f"Inference ID '{inference_id}' not found in group '{group_name}'"
                )
            config = group_data["inference_ids"][inference_id]
            model_class_name = group_data["model_class"]
            model_class = self._registry.get(model_class_name)
            if not model_class:
                raise ValueError(
                    f"Model class '{model_class_name}' not found in registry for inference_id '{inference_id}'"
                )

            model_instance = model_class(**config["config"])
            return model_instance

    def get_metadata(
        self, group_name: str, inference_id: str
    ) -> Optional[Dict[str, Any]]:
        """Retrieve the metadata associated with an inference ID."""
        self.reload_registry()  # Ensure the registry is up to date before retrieving metadata
        with self._lock:
            if group_name not in self._config:
                return None
            group_data = self._config[group_name]
            if inference_id not in group_data["inference_ids"]:
                return None
            return {
                "group_metadata": group_data.get("group_metadata", {}),
                "inference_id_metadata": group_data["inference_ids"][
                    inference_id
                ].get("metadata", {}),
            }

    def list_inference_ids(self) -> Dict[str, Dict[str, Any]]:
        """List all inference IDs divided by group, including group and individual metadata."""
        self.reload_registry()  # Ensure the registry is up to date before listing inference IDs
        with self._lock:
            result = {}
            for group_name, group_data in self._config.items():
                result[group_name] = {
                    "group_metadata": group_data.get("group_metadata", {}),
                    "inference_ids": {
                        inf_id: inf_data.get("metadata", {})
                        for inf_id, inf_data in group_data[
                            "inference_ids"
                        ].items()
                    },
                }
            return result


import os
from pathlib import Path


def get_base_config_folder() -> Path:
    """Return the path to the base configuration folder inside the source directory."""
    # Get the absolute path of the current script's directory
    current_script_path = Path(__file__).resolve()

    # Assuming the base config folder is located in the 'config/base' directory relative to the source directory
    base_config_folder = current_script_path.parent / "config"

    # Verify that the directory exists
    if not base_config_folder.is_dir():
        raise FileNotFoundError(
            f"Base configuration folder not found at: {base_config_folder}"
        )

    return base_config_folder
