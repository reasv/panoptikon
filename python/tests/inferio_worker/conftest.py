import threading

import pytest


def _live_samplers() -> list[str]:
    """Every peak-sampler thread alive right now, by name. They are daemons on
    a 900 s deadline, so a leaked one polls for the rest of the session."""
    return [
        t.name
        for t in threading.enumerate()
        if t.name.startswith("inferio-") and t.name.endswith("-peak") and t.is_alive()
    ]


@pytest.fixture(autouse=True)
def _no_sampler_survives_the_test():
    """Suite-level bracket check: `begin_batch` starts a polling thread and
    only `measure_batch`/`finish_batch`/`abandon_batch` stop it, so a test that
    opens a bracket and never closes it is testing the leak it should catch.
    """
    assert not _live_samplers(), "a sampler leaked in before this test"
    yield
    leaked = _live_samplers()
    assert not leaked, f"this test left peak samplers running: {leaked}"
