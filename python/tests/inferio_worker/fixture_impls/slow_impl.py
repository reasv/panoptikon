"""Test fixture impl with a slow predict.

Used by the manager tests to hold a worker busy: the 1.5s sleep is long
enough for a short-TTL sweeper to tick several times mid-predict (the pin
test) and for follow-up requests to queue behind the first batch. An input
`{"wait_for": path}` instead holds the reply until that file exists, for
tests that must order their own steps before the reply.
"""

import os
import time


class SlowModel:
    def __init__(self, **config):
        self.config = config

    @classmethod
    def name(cls) -> str:
        return "slow_test"

    def load(self) -> None:
        pass

    def predict(self, inputs):
        gates = [
            inp.data["wait_for"]
            for inp in inputs
            if isinstance(inp.data, dict) and "wait_for" in inp.data
        ]
        if not gates:
            time.sleep(1.5)
        # Bounded so a worker whose test process was killed cannot poll forever.
        deadline = time.monotonic() + 600
        while not all(map(os.path.exists, gates)) and time.monotonic() < deadline:
            time.sleep(0.005)
        return [{"slow": True} for _ in inputs]

    def unload(self) -> None:
        pass


IMPL_CLASS = SlowModel
