"""Test fixture impl whose predict kills the worker process.

os._exit bypasses all Python cleanup, so the parent sees the process die
with a pending request — the manager must treat it as a fatal worker death:
fail the request, drop the model from every LRU/cache-key, and let the next
predict auto-load a fresh worker. With a `signal` config the process sends
itself that signal instead, and with an `exit_code` config it raises
SystemExit, the exit a Python worker makes by itself. A `load_report`
config is merged into the load response, standing in for the device a real
load reports.
"""

import os


class DyingModel:
    def __init__(self, **config):
        self.config = config

    @classmethod
    def name(cls) -> str:
        return "dying_test"

    def load(self) -> None:
        report = self.config.get("load_report")
        if report:
            from inferio_worker import memory

            finish_load = memory.finish_load
            memory.finish_load = lambda before, instance: {
                **finish_load(before, instance),
                **report,
            }

    def predict(self, inputs):
        if "signal" in self.config:
            os.kill(os.getpid(), self.config["signal"])
        if "exit_code" in self.config:
            raise SystemExit(self.config["exit_code"])
        os._exit(3)

    def unload(self) -> None:
        pass


IMPL_CLASS = DyingModel
