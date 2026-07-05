"""Test fixture impl with a slow predict.

Used by the manager tests to hold a worker busy: the 1.5s sleep is long
enough for a short-TTL sweeper to tick several times mid-predict (the pin
test) and for follow-up requests to queue behind the first batch.
"""

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
        time.sleep(1.5)
        return [{"slow": True} for _ in inputs]

    def unload(self) -> None:
        pass


IMPL_CLASS = SlowModel
