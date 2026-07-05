"""Test fixture impl with an optional prepare() classmethod (v2 prewarm).

prepare() sets a module-level flag and writes a marker line to stderr, so
tests (Python and Rust alike) can prove it ran exactly when prewarm was
requested. load() asserts nothing about prepare having run — the protocol
guarantees load works with or without a prior prewarm — and predict()
reports the flag so the pooled flow (prewarm -> park -> configure -> load
-> predict) is observable end to end.
"""

import sys

PREPARED = False


class PrepareModel:
    def __init__(self, **config):
        self.config = config
        self.load_called = False

    @classmethod
    def name(cls) -> str:
        return "prepare_test"

    @classmethod
    def prepare(cls) -> None:
        global PREPARED
        PREPARED = True
        print("prepare_test-prepare-marker", file=sys.stderr, flush=True)

    def load(self) -> None:
        if self.load_called:
            return
        # load must work whether or not prepare() ran; the flag is a plain
        # bool in both cases (module state intact).
        assert isinstance(PREPARED, bool)
        self.load_called = True

    def predict(self, inputs):
        return [{"prepared": PREPARED} for _ in inputs]

    def unload(self) -> None:
        pass


IMPL_CLASS = PrepareModel
