# Example custom model implementation for Inferio

from inferio.model import InferenceModel
from inferio.inferio_types import PredictionInput
from typing import Sequence

class ExampleModel(InferenceModel):
    """
    Example implementation of a custom InferenceModel.
    You must implement the abstract methods: name, load, predict, unload.
    """
    def __init__(
        self,
        **kwargs  # Use kwargs to accept any additional parameters
    ):
        # __init__ receives the model's configuration object from the toml config through kwargs.
        # You can initialize any resources or configurations here
        # But do not load model weights into memory or perform other heavy tasks here.
        pass

    @classmethod
    def name(cls) -> str:
        # Return a unique name for this model implementation
        # This is used to reference this implementation with `impl_class` in the config.
        return "example_model"

    def load(self) -> None:
        # Load model weights/resources here
        # Downloading of missing weights or resources should be done here.
        # This method is called once when the model is loaded into memory.
        pass

    def predict(self, inputs: Sequence[PredictionInput]) -> Sequence[str]:
        # Implement your prediction logic here
        # 'inputs' will be a sequence of PredictionInput objects
        # MUST return the same number of items as inputs,
        # in the same order as the corresponding inputs.
        return [f"Echo: {inp.data}" for inp in inputs]

    def unload(self) -> None:
        # Cleanup resources if needed
        # Generally, this method can be left empty because each model
        # is loaded into its own process and the process will be terminated on unload.
        pass

# This variable must be present for your custom model to be correctly registered
IMPL_CLASS = ExampleModel
