"""Test fixture reporting a declared environment-backed external input."""

import os


class ExternalEnvModel:
    def __init__(self, **config):
        self.config = config

    @classmethod
    def name(cls) -> str:
        return "external_env_test"

    def load(self) -> None:
        pass

    def predict(self, inputs):
        value = os.environ.get("INFERIO_MANAGER_EXTERNAL_INPUT_XYZ")
        return [{"external_input": value} for _ in inputs]

    def unload(self) -> None:
        pass


IMPL_CLASS = ExternalEnvModel
