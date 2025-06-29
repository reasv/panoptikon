# Example custom model implementation for Inferio

from inferio.model import InferenceModel
from inferio.inferio_types import PredictionInput
from typing import Sequence

class ExampleModel(InferenceModel):
    """
    Example implementation of a custom InferenceModel.
    You must implement the abstract methods: name, load, predict, unload.
    """
    @classmethod
    def name(cls) -> str:
        # Return a unique name for this model implementation
        # This is used to reference this implementation with `impl_class` in the config.
        return "example_model"

    def load(self) -> None:
        # Load model weights/resources here
        pass

    def predict(self, inputs: Sequence[PredictionInput]) -> Sequence[str]:
        # Implement your prediction logic here
        # 'inputs' will be a sequence of PredictionInput objects
        # MUST return the same number of items as inputs,
        # in the same order as the corresponding inputs.
        return [f"Echo: {inp.data}" for inp in inputs]

    def unload(self) -> None:
        # Cleanup resources if needed
        pass

# This variable must be present for your custom model to be correctly registered
IMPL_CLASS = ExampleModel
