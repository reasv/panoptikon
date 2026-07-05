"""Test fixture impl whose predict() prints to stdout.

Used to verify the worker's stdout hygiene: print() output must land on
stderr (fd 1 is dup2'd to stderr and sys.stdout is rebound) and must never
corrupt the protocol frames on the real stdout channel.
"""


class PrintingModel:
    def __init__(self, **config):
        self.config = config

    @classmethod
    def name(cls) -> str:
        return "printing_test"

    def load(self) -> None:
        print("garbage on load stdout")

    def predict(self, inputs):
        print("garbage on predict stdout")
        return [{"printed": True} for _ in inputs]

    def unload(self) -> None:
        print("garbage on unload stdout")


IMPL_CLASS = PrintingModel
