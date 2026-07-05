"""Test fixture impl: echoes its inputs back.

Stdlib-only stand-in for a real InferenceModel implementation. It relies on
duck typing exactly like real impls do: inputs are only accessed via
`.data` / `.file`.
"""


class EchoModel:
    def __init__(self, **config):
        self.config = config
        self.load_called = False
        self.unload_called = False

    @classmethod
    def name(cls) -> str:
        return "echo_test"

    def load(self) -> None:
        # Idempotency guard, mirroring real impls' _model_loaded pattern.
        if self.load_called:
            return
        self.load_called = True

    def predict(self, inputs):
        outputs = []
        for inp in inputs:
            if inp.file is not None:
                outputs.append(b"echo:" + inp.file)
            else:
                outputs.append({"echo": inp.data})
        return outputs

    def unload(self) -> None:
        self.unload_called = True


IMPL_CLASS = EchoModel
