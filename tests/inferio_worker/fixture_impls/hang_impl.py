"""Test fixture impl whose predict never returns.

Simulates a worker wedged in a GPU kernel: the manager tests use it to
prove that unload/shutdown converge by killing the stuck worker after the
unload grace instead of waiting for the predict. The sleep is finite only
so a failed kill cannot leak a process past the test run.
"""

import time


class HangModel:
    def __init__(self, **config):
        self.config = config

    @classmethod
    def name(cls) -> str:
        return "hang_test"

    def load(self) -> None:
        pass

    def predict(self, inputs):
        time.sleep(600)
        return [{"hang": True} for _ in inputs]

    def unload(self) -> None:
        pass


IMPL_CLASS = HangModel
