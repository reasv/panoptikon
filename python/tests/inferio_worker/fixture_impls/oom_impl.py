"""Test fixture impl that raises the classified batch-1 OOM error.

Mirrors what inferio.impl.utils.run_with_oom_retry raises when a single
input still OOMs after cache clearing: an error whose str() starts with
OOM_BATCH1_PREFIX. Torch-free — the literal is pinned to the real
constant by tests/inferio/impl/test_oom_retry.py.
"""


class OomModel:
    def __init__(self, **config):
        self.config = config

    @classmethod
    def name(cls) -> str:
        return "oom_test"

    def load(self) -> None:
        pass

    def predict(self, inputs):
        raise RuntimeError(
            "INFERENCE_OOM_BATCH_SIZE_1: fixture single-item OOM"
        )

    def unload(self) -> None:
        pass


IMPL_CLASS = OomModel
