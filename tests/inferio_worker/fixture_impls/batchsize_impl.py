"""Test fixture impl that reports the batch size it received.

Every output is `{"batch": len(inputs)}`, so a client can observe exactly
how the orchestrator merged its request with others. The short sleep keeps
the worker busy long enough for concurrent requests to queue and merge into
the next dispatch window, making batching observable deterministically.
"""

import time


class BatchSizeModel:
    def __init__(self, **config):
        self.config = config

    @classmethod
    def name(cls) -> str:
        return "batchsize_test"

    def load(self) -> None:
        pass

    def predict(self, inputs):
        time.sleep(0.3)
        n = len(inputs)
        return [{"batch": n} for _ in inputs]

    def unload(self) -> None:
        pass


IMPL_CLASS = BatchSizeModel
