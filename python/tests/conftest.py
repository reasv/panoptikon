import os
import sys
from pathlib import Path

import pytest


def _env_flag(name: str) -> bool:
    value = os.environ.get(name, "")
    return value.strip().lower() not in ("", "0", "false", "no", "off")


@pytest.fixture(scope="session", autouse=True)
def _load_dotenv() -> None:
    """Load the repo-root .env (integration tests read API keys etc. from it).

    python-dotenv comes from the `test` dependency group.
    """
    try:
        from dotenv import load_dotenv
    except Exception:
        return

    repo_root = Path(__file__).resolve().parents[2]
    dotenv_path = os.environ.get("PANOPTIKON_DOTENV_PATH")
    if dotenv_path:
        load_dotenv(dotenv_path, override=False)
        return

    load_dotenv(repo_root / ".env", override=False)


@pytest.fixture(scope="session", autouse=True)
def _add_python_to_path() -> None:
    """Make the inferio/inferio_worker packages importable in-process."""
    python_dir = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(python_dir))


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if _env_flag("PANOPTIKON_RUN_INTEGRATION"):
        return
    skip = pytest.mark.skip(
        reason="Set PANOPTIKON_RUN_INTEGRATION=1 to run integration tests."
    )
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)
