"""Test fixture impl whose own GPU batching is switched off.

`enable_batching = False` is the shipped registry's knob for "process one item
at a time inside predict" (easyOCR's OOM stopgap). Such an impl decides its own
batch shape regardless of what it is handed, so the worker must ignore any
memory grant and take the grantless compatibility path — reporting one
measurement for the whole call and no cost-dimension `units`
(docs/inferio-worker-protocol.md, "Memory grants").

Every output reports the batch size the impl received, so a test can see
whether a grant's unit budget reached it.
"""


class NoBatchingModel:
    enable_batching = False

    def __init__(self, **config):
        self.config = config

    @classmethod
    def name(cls) -> str:
        return "nobatching_test"

    def load(self) -> None:
        pass

    def predict(self, inputs):
        return [{"batch": len(inputs)} for _ in inputs]

    def unload(self) -> None:
        pass


IMPL_CLASS = NoBatchingModel
