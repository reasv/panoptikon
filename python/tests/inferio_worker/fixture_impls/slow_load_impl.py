"""Test fixture impl with a slow `load()`.

The counterpart of `slow_test`, which is slow in `predict()`: this one is
slow in `load()`, which is the phase the manager used to hold a host-wide
lock across (run1 finding P5-3/B18 — an 11.9 s load stalled every in-flight
predict on the host for 11.9 s). The manager's R6 tests use it to prove that
a load of one model does not delay predicts to another resident one.

`config.load_seconds` (default 3.0) is how long `load()` sleeps; predict is
immediate and echoes the delay it paid, so a test can tell this fixture's
outputs from another's.
"""

import time


class SlowLoadModel:
    def __init__(self, **config):
        self.config = config
        self.load_seconds = float(config.get("load_seconds", 3.0))

    @classmethod
    def name(cls) -> str:
        return "slow_load_test"

    def load(self) -> None:
        time.sleep(self.load_seconds)

    def predict(self, inputs):
        return [{"loaded_after": self.load_seconds} for _ in inputs]

    def unload(self) -> None:
        pass


IMPL_CLASS = SlowLoadModel
