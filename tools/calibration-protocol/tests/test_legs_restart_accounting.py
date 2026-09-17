"""`legs.py`'s teardown must account for both of S3's gateways.

`results/windows/S3-final` recorded `processes_stopped` as
`gateway: "already exited rc=0"` after a leg whose second gateway shut down
gracefully at 03:38:03 ("shutdown signal received; stopping gracefully" ->
"gateway stopped"). `Supervisor.stop_all` keys its result by `child.name` and
S3 registers two children called `gateway`, so the first process's row -- dead
since the restart at 03:36:16 -- overwrote the live one's, and
`plan["processes"]`, keyed the same way, kept the opposite one (D2).

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

LEGS = Path(__file__).resolve().parents[1] / "legs.py"


def _load():
    spec = importlib.util.spec_from_file_location("_calib_legs_restart", LEGS)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


legs = _load()


class _Popen:
    def __init__(self, pid, returncode):
        self.pid = pid
        self.returncode = returncode

    def poll(self):
        return self.returncode


def _child(name, pid, returncode):
    return legs.Child(name, _Popen(pid, returncode), None)


# --- D2: two gateways, two rows --------------------------------------------


def test_a_restarted_gateway_gets_its_own_child_name():
    supervisor = legs.Supervisor(grace=1.0)
    supervisor.children.append(_child("vramrec", 1, 0))
    supervisor.children.append(_child("gateway", 2, 0))
    started = sum(1 for child in supervisor.children
                  if child.name.startswith("gateway"))
    assert started == 1
    supervisor.children.append(_child(f"gateway-{started + 1}", 3, 0))
    stopped = supervisor.stop_all()
    # Both gateways are accounted for, and neither row overwrote the other.
    assert sorted(stopped) == ["gateway", "gateway-2", "vramrec"]
    assert stopped["gateway"] == "already exited rc=0"
    assert stopped["gateway-2"] == "already exited rc=0"


def test_the_gateway_started_event_carries_the_child_name():
    """`Leg.mark`'s own first parameter is `name`, so the event cannot use it."""
    leg = legs.Leg.__new__(legs.Leg)
    leg.events = []
    leg.mark("gateway_started", child="gateway-2", pid=7, argv=["panoptikon"])
    assert leg.events[-1]["child"] == "gateway-2"


def test_one_gateway_keeps_the_plain_name():
    """The single-process legs' key must not change."""
    supervisor = legs.Supervisor(grace=1.0)
    supervisor.children.append(_child("gateway", 2, 0))
    assert sorted(supervisor.stop_all()) == ["gateway"]
