"""Test fixture impl whose prepare() classmethod raises.

Used to verify that prewarm errors are per-request and NON-fatal: the
worker stays alive and a later configure/load/predict succeeds — a failed
prepare just means load pays the imports.
"""


class PrepareFailModel:
    def __init__(self, **config):
        self.config = config
        self.load_called = False

    @classmethod
    def name(cls) -> str:
        return "prepare_fail_test"

    @classmethod
    def prepare(cls) -> None:
        raise RuntimeError("prepare exploded (simulated missing heavy dep)")

    def load(self) -> None:
        if self.load_called:
            return
        self.load_called = True

    def predict(self, inputs):
        return [{"ok": True} for _ in inputs]

    def unload(self) -> None:
        pass


IMPL_CLASS = PrepareFailModel
