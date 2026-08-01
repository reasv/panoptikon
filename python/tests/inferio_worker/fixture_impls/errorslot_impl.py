"""Test fixture impl emitting typed per-item error slots.

Stdlib-only (the real seam lives in `inferio.impl.utils`, which needs PIL):
an input whose `data` is "bad" comes back as an `input` error slot, "flaky"
as a `transient` one, "malformed" as a slot carrying the reserved key with a
body the protocol does not define (a fatal violation on the orchestrator
side), and anything else as an ordinary payload — so one predict can prove
that mixed batches stay aligned.
"""

ERROR_SLOT_KEY = "__error__"


class ErrorSlotModel:
    def __init__(self, **config):
        self.config = config

    @classmethod
    def name(cls) -> str:
        return "errorslot_test"

    def load(self) -> None:
        pass

    def predict(self, inputs):
        outputs = []
        for inp in inputs:
            if inp.data == "bad":
                outputs.append(
                    {
                        ERROR_SLOT_KEY: {
                            "class": "input",
                            "message": "Unreadable image: truncated",
                        }
                    }
                )
            elif inp.data == "flaky":
                outputs.append(
                    {
                        ERROR_SLOT_KEY: {
                            "class": "transient",
                            "message": "try again",
                        }
                    }
                )
            elif inp.data == "malformed":
                outputs.append({ERROR_SLOT_KEY: {"class": "nonsense"}})
            elif inp.file is not None:
                outputs.append(b"bytes:" + inp.file)
            else:
                outputs.append({"ok": inp.data})
        return outputs

    def unload(self) -> None:
        pass


IMPL_CLASS = ErrorSlotModel
