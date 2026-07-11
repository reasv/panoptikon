"""Test fixture impl whose predict fails for merged batches only.

predict raises for any batch with more than one input but succeeds for
singles — the exact shape of "one poisoned input / batch-size-sensitive
failure" that the orchestrator's per-request fallback exists for (ported
from process_model.py `_batch_predict`). The sleep lets concurrent requests
queue so the dispatcher actually forms a merged batch to fail.
"""

import time


class FailBatchModel:
    def __init__(self, **config):
        self.config = config

    @classmethod
    def name(cls) -> str:
        return "failbatch_test"

    def load(self) -> None:
        pass

    def predict(self, inputs):
        time.sleep(0.3)
        if len(inputs) > 1:
            raise ValueError(f"refusing merged batch of {len(inputs)}")
        return [{"ok": True} for _ in inputs]

    def unload(self) -> None:
        pass


IMPL_CLASS = FailBatchModel
