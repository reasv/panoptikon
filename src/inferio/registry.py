import logging
import os
from collections import defaultdict
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional, Type

import tomlkit

from inferio.model import InferenceModel

logger = logging.getLogger(__name__)


class ModelRegistry:
    _registry: Dict[str, Type["InferenceModel"]] = {}
    _instance: Optional["ModelRegistry"] = (
        None  # Class-level variable to hold the singleton instance
    )
    _user_folder: Optional[Path] = None

    def __new__(cls, *args, **kwargs) -> "ModelRegistry":
        """Override the __new__ method to ensure a single instance."""
        if cls._instance is None:
            cls._instance = super(ModelRegistry, cls).__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if not hasattr(
            self, "_initialized"
        ):  # Ensure __init__ is only run once
            self._config: Dict[str, Dict[str, Any]] = {}
            self._last_modified_time: float = 0.0
            self._lock: Lock = Lock()
            self.base_folder = get_base_config_folder()
            self.allow_inference_id_overrides = False
            self.reload_registry()
            self._initialized = True  # Mark the instance as initialized

    @classmethod
    def set_user_folder(cls, folder: str) -> None:
        """Set the path to the user configuration folder."""
        cls._user_folder = Path(folder)

    @classmethod
    def register_model(cls, model_class: Type["InferenceModel"]) -> None:
        """Register a BaseModel subclass"""
        cls._registry[model_class.name()] = model_class

    def _get_latest_modified_time(self) -> float:
        """Get the latest modified time of all TOML files in the config folders."""
        latest_time = 0.0
        for folder in [
            f
            for f in [self.base_folder, self._user_folder]
            if isinstance(f, Path)
        ]:
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
                config_data = defaultdict(
                    lambda: {
                        "inference_ids": {},
                        "group_config": {},
                        "group_metadata": {},
                    }
                )

                # Load and merge configurations from both folders
                self._load_folder(self.base_folder, config_data)
                if self._user_folder:
                    self._load_folder(self._user_folder, config_data)

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
                        logger.debug(f"Loading TOML file: {file}")

                    self.allow_inference_id_overrides = data.get(
                        "allow_override", False
                    )
                    for group_name, group_data in data.get("group", {}).items():
                        # Store or merge group-level config and metadata separately
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
                    raise e

    def get_model_instance(self, full_inference_id: str) -> "InferenceModel":
        """Retrieve and instantiate a BaseModel subclass based on the inference ID and group name."""

        group_name, inference_id = full_inference_id.split("/", 1)
        self.reload_registry()  # Ensure the registry is up to date before retrieving a model
        with self._lock:
            if group_name not in self._config:
                raise ValueError(f"Group '{group_name}' not found in registry")
            group_data = self._config[group_name]
            if inference_id not in group_data["inference_ids"]:
                raise ValueError(
                    f"Inference ID '{inference_id}' not found in group '{group_name}'"
                )
            inference_id_config = group_data["inference_ids"][inference_id]
            model_config = inference_id_config["config"]
            model_class_name = model_config["impl_class"]
            model_class = self._registry.get(model_class_name)
            if not model_class:
                raise ValueError(
                    f"Inference Implementation class '{model_class_name}' not found in registry for inference_id '{group_name}/{inference_id}'"
                )
            # Instantiate the model with the merged config but without the impl_class
            # Copy the config to avoid modifying the original
            model_config = dict(model_config)
            model_config.pop("impl_class", None)
            model_instance = model_class(**model_config)
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


def get_base_config_folder() -> Path:
    """Return the path to the base configuration folder inside the source directory."""
    if folder := os.getenv("BASE_INFERENCE_CONFIG_FOLDER"):
        return Path(folder)
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
