import os
import logging
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import tomli

logger = logging.getLogger(__name__)

# --- Environment-variable templating for TOML config values -----------------
#
# Mirrors the Rust gateway's gateway/src/env_template.rs exactly, so the same
# registry TOML behaves identically under both implementations. Syntax, inside
# string values only (shell / docker-compose conventions):
#
# - `${NAME}`         -> env value; hard error when unset (set-but-empty -> "")
# - `${NAME:-default}` -> default when unset OR set-but-empty
# - `${NAME-default}`  -> default only when unset
# - `$${`             -> literal `${` (never re-scanned)
# - NAME matches [A-Za-z_][A-Za-z0-9_]*; anything else after `${` is an error.
#   A `$` not followed by `{` is literal.
#
# Substitution is a single pass over parsed values: placeholder-looking text
# inside a substituted env value is NOT re-expanded (no recursion).

_ENV_VAR_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _template_snippet(text: str) -> str:
    """A short prefix of the offending text for error messages."""
    return text[:24]


def _substitute_env_str(text: str) -> str:
    """Substitute env placeholders in one string (single pass, no recursion)."""
    out: list[str] = []
    rest = text
    while (pos := rest.find("$")) != -1:
        out.append(rest[:pos])
        tail = rest[pos:]
        if tail.startswith("$${"):
            # `$${` -> literal `${`, never re-scanned (single pass).
            out.append("${")
            rest = tail[3:]
        elif tail.startswith("${"):
            inner = tail[2:]
            name_match = _ENV_VAR_NAME_RE.match(inner)
            if not name_match:
                raise ValueError(
                    f"malformed substitution '{_template_snippet(tail)}': "
                    "expected a variable name ([A-Za-z_][A-Za-z0-9_]*) after "
                    "'${' (write '$${' for a literal '${')"
                )
            name = name_match.group(0)
            after_name = inner[len(name):]
            if after_name.startswith("}"):
                # ${NAME}: unset is a hard error -- this form is for secrets
                # and required values, which must fail loudly. Set-but-empty
                # yields "" (shell semantics).
                value = os.environ.get(name)
                if value is None:
                    raise ValueError(
                        f"environment variable '{name}' is not set "
                        f"(referenced as '${{{name}}}'; use "
                        f"'${{{name}:-default}}' to provide a fallback)"
                    )
                out.append(value)
                rest = after_name[1:]
            elif after_name.startswith(":-") or after_name.startswith("-"):
                # Shell conventions: `:-` substitutes the default when the
                # variable is unset OR set-but-empty; `-` only when unset.
                empty_uses_default = after_name.startswith(":-")
                after_op = after_name[2 if empty_uses_default else 1:]
                end = after_op.find("}")
                if end == -1:
                    raise ValueError(
                        f"malformed substitution '{_template_snippet(tail)}': "
                        "missing closing '}'"
                    )
                default = after_op[:end]
                value = os.environ.get(name)
                if value is not None and not (
                    empty_uses_default and value == ""
                ):
                    out.append(value)
                else:
                    out.append(default)
                rest = after_op[end + 1:]
            else:
                raise ValueError(
                    f"malformed substitution '{_template_snippet(tail)}': "
                    "expected '}', ':-default}' or '-default}' after the "
                    "variable name"
                )
        else:
            # A lone `$` (or `$$` not followed by `{`) is literal.
            out.append("$")
            rest = tail[1:]
    out.append(rest)
    return "".join(out)


def _substitute_env_walk(value: Any) -> Any:
    """Recursively substitute env templates in every string value."""
    if isinstance(value, str):
        return _substitute_env_str(value) if "$" in value else value
    if isinstance(value, list):
        return [_substitute_env_walk(item) for item in value]
    if isinstance(value, dict):
        return {key: _substitute_env_walk(item) for key, item in value.items()}
    return value


def substitute_env_templates(data: Any, source: Path | str) -> Any:
    """Substitute `${NAME}` / `${NAME:-default}` env placeholders in every
    string value of a parsed TOML tree (dicts and lists included). Errors name
    `source` (the file the value came from) and the offending variable."""
    try:
        return _substitute_env_walk(data)
    except ValueError as e:
        raise ValueError(f"in config file {source}: {e}") from e

def get_config_mtime(base_folder: Path, user_folder: Path) -> float:
    latest_time = 0.0
    for folder in [
        f
        for f in [base_folder, user_folder]
        if isinstance(f, Path)
    ]:
        for file in sorted(folder.glob("*.toml")):
            file_time = file.stat().st_mtime
            if file_time > latest_time:
                latest_time = file_time
    return latest_time

def load_config_folder(
        folder: Path, parent_config: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
    """Load all TOML files from a folder in alphabetical order and merge them into config_data."""
    config_data = parent_config.copy()
    if not folder.is_dir():
        logger.warning(f"Folder {folder} does not exist or is not a directory.")
        return config_data
    for file in sorted(
        folder.glob("*.toml")
    ):  # Sort files for predictable loading
        try:
            with open(file, "rb") as f:
                data = tomli.load(f)
                logger.debug(f"Loading TOML file: {file}")
            data = substitute_env_templates(data, file)
            allow_inference_id_overrides = data.get("allow_override", False)
            groups: Dict[str, Dict[str, Any]] = data.get("group", {})
            for group_name, group_data in groups.items():
                # Store or merge group-level config and metadata separately
                # Merge group config, giving precedence to the latest loaded
                config_data[group_name]["group_config"].update(
                    group_data.get("config", {})
                )

                # Merge group metadata, giving precedence to the latest loaded
                config_data[group_name]["group_metadata"].update(
                    group_data.get("metadata", {})
                )

                group_inference_id_data: Dict[str, Dict[str, Any]] = group_data.get("inference_ids", {})
                # Process and merge inference IDs within the group
                for inference_id, inf_data in group_inference_id_data.items():
                    if (
                        inference_id
                        in config_data[group_name]["inference_ids"]
                        and not allow_inference_id_overrides
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
    return config_data

def load_config(
        config: Dict[str, Dict[str, Any]] | None = None,
        mtime: float | None = None,
    ) -> Tuple[Dict[str, Dict[str, Any]], float]:
    """Reload the registry if TOML files have been modified."""
    base_folder = get_base_config_folder()
    user_folder = Path(os.getenv("INFERIO_CONFIG_DIR", "config/inference"))
    latest_time = get_config_mtime(base_folder, user_folder)
    if config and mtime and latest_time <= mtime:
        logger.debug("No changes detected in configuration files.")
        return config, mtime
    config_data = defaultdict(
        lambda: {
            "inference_ids": {},
            "group_config": {},
            "group_metadata": {},
        }
    )
    config_data = load_config_folder(base_folder, config_data)
    config_data = load_config_folder(user_folder, config_data)
    logger.info(f"Configuration reloaded from {base_folder} and {user_folder}")
    return config_data, latest_time

def list_inference_ids(config: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """List all inference IDs divided by group, including group and individual metadata."""
    result = {}
    for group_name, group_data in config.items():
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

def get_metadata(
    group_name: str,
    inference_id: str,
    config: Dict[str, Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Retrieve the metadata associated with an inference ID."""
    if group_name not in config:
        return None
    group_data = config[group_name]
    if inference_id not in group_data["inference_ids"]:
        return None
    return {
        "group_metadata": group_data.get("group_metadata", {}),
        "inference_id_metadata": group_data["inference_ids"][
            inference_id
        ].get("metadata", {}),
    }

def get_model_config(
    full_inference_id: str,
    config: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """Retrieve the model class name and configuration for a given inference ID."""
    group_name, inference_id = full_inference_id.split("/", 1)
    if group_name not in config:
        raise ValueError(f"Group '{group_name}' not found in registry")
    group_data = config[group_name]
    if inference_id not in group_data["inference_ids"]:
        raise ValueError(
            f"Inference ID '{inference_id}' not found in group '{group_name}'"
        )
    inference_id_config = group_data["inference_ids"][inference_id]
    model_config = inference_id_config["config"]
    # Copy the config to avoid modifying the original
    model_config = dict(model_config)
    return model_config

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
