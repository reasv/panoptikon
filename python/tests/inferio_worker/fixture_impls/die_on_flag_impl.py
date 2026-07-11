"""Test fixture impl that kills its own process when told to.

An input with data `{"die": true}` makes the worker sleep briefly and then
`os._exit(7)` — a hard death bypassing all cleanup, so the parent sees the
process vanish with a request pending. Any other input sleeps ~1s and
echoes. The timings make the multi-replica death test deterministic: the
poison request holds one replica for 200ms (long enough for the normal
requests to be queued/dispatched on the other replica) and the normal
predict is slow enough (1s) to still be in flight when the death is
detected — so every outstanding request must error under the
whole-set-death policy.
"""

import os
import time


class DieOnFlagModel:
    def __init__(self, **config):
        self.config = config

    @classmethod
    def name(cls) -> str:
        return "dieflag_test"

    def load(self) -> None:
        pass

    def predict(self, inputs):
        for inp in inputs:
            if isinstance(inp.data, dict) and inp.data.get("die"):
                time.sleep(0.2)
                os._exit(7)
        time.sleep(1.0)
        return [{"echo": inp.data} for inp in inputs]

    def unload(self) -> None:
        pass


IMPL_CLASS = DieOnFlagModel
