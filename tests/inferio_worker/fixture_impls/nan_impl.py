"""Test fixture impl returning a non-JSON-encodable float on demand.

msgpack happily carries NaN over the wire, but the orchestrator cannot
represent it as JSON. That conversion failure must be a per-request error
that leaves the worker alive — inputs with data == "nan" yield float NaN,
anything else yields a normal JSON-like output so follow-up predicts on
the same worker can prove it survived.
"""


class NanModel:
    def __init__(self, **config):
        self.config = config

    @classmethod
    def name(cls) -> str:
        return "nan_test"

    def load(self) -> None:
        pass

    def predict(self, inputs):
        return [
            float("nan") if inp.data == "nan" else {"ok": True}
            for inp in inputs
        ]

    def unload(self) -> None:
        pass


IMPL_CLASS = NanModel
