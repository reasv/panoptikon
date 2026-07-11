"""Test fixture impl whose predict kills the worker process.

os._exit bypasses all Python cleanup, so the parent sees the process die
with a pending request — the manager must treat it as a fatal worker death:
fail the request, drop the model from every LRU/cache-key, and let the next
predict auto-load a fresh worker.
"""

import os


class DyingModel:
    def __init__(self, **config):
        self.config = config

    @classmethod
    def name(cls) -> str:
        return "dying_test"

    def load(self) -> None:
        pass

    def predict(self, inputs):
        os._exit(3)

    def unload(self) -> None:
        pass


IMPL_CLASS = DyingModel
