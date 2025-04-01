import logging
import os
import re
from io import BytesIO
from typing import Dict, List, Sequence, Tuple, Type
from unittest.mock import patch

from PIL import Image as PILImage

from inferio.impl.utils import clean_whitespace, clear_cache, get_device
from inferio.model import InferenceModel
from inferio.process_model import ProcessIsolatedInferenceModel
from inferio.types import PredictionInput

logger = logging.getLogger(__name__)

class Florence2(InferenceModel):
    def __init__(
        self,
        model_name: str,
        task_prompt: str,
        text_input: str | None = None,
        flash_attention: bool = False,
        max_output: int = 1024,
        num_beams: int = 3,
        init_args: dict = {},
    ):
        self.model_name: str = model_name
        self.task_prompt: str = task_prompt
        self.text_input: str | None = text_input
        self.flash_attention: bool = flash_attention
        self.max_output: int = max_output
        self.num_beams: int = num_beams
        self.init_args = init_args
        self._model_loaded: bool = False

    @classmethod
    def name(cls) -> str:
        return "florence2"

    def load(self) -> None:
        if self._model_loaded:
            return

        self.devices = get_device()
        import torch
        from transformers import AutoModelForCausalLM, AutoProcessor

        device = self.devices[0]
        # Set to True if you want to use Flash Attention instead of SDPA
        if not self.flash_attention:
            from transformers.dynamic_module_utils import get_imports
            def fixed_get_imports(filename: str | os.PathLike) -> list[str]:
                # workaround for unnecessary flash_attn requirement

                if not str(filename).endswith("modeling_florence2.py"):
                    return get_imports(filename)
                imports = get_imports(filename)
                if "flash_attn" in imports:
                    imports.remove("flash_attn")
                return imports

            # Patch the get_imports function to remove flash_attn from imports
            with patch(
                "transformers.dynamic_module_utils.get_imports",
                fixed_get_imports,
            ):  # workaround for unnecessary flash_attn requirement

                self.model = (
                    AutoModelForCausalLM.from_pretrained(
                        self.model_name,
                        attn_implementation="sdpa",
                        torch_dtype=torch.float16,
                        trust_remote_code=True,
                    )
                    .to(device)
                    .eval()
                )
        else:
            self.model = (
                AutoModelForCausalLM.from_pretrained(
                    self.model_name,
                    attn_implementation="flash_attention_2",
                    torch_dtype=torch.float16,
                    trust_remote_code=True,
                )
                .to(device)
                .eval()
            )
        self.processor = AutoProcessor.from_pretrained(
            self.model_name, trust_remote_code=True
        )
        self.model = torch.compile(self.model, mode="reduce-overhead")
        logger.debug(f"Model {self.model_name} loaded.")
        self._model_loaded = True

    def predict(self, inputs: Sequence[PredictionInput]) -> List[dict]:
        import torch

        self.load()
        image_inputs: List[PILImage.Image] = []
        configs: List[dict] = [inp.data for inp in inputs]  # type: ignore
        for input_item in inputs:
            if input_item.file:
                image: PILImage.Image = PILImage.open(
                    BytesIO(input_item.file)
                ).convert("RGB")
                image_inputs.append(image)
            else:
                raise ValueError("Florence2 requires image inputs.")

        if self.text_input is None:
            prompt = self.task_prompt
        else:
            prompt = self.task_prompt + self.text_input

        results: List[str] = []
        for image in image_inputs:
            # Ensure the image is in RGB mode
            if image.mode != "RGB":
                image = image.convert("RGB")

            # Process the inputs and ensure they are in the correct dtype and device
            device = self.devices[0]
            processed_inputs = self.processor(
                text=prompt, images=image, return_tensors="pt"
            ).to(device)
            processed_inputs = {
                k: v.half() if v.dtype == torch.float else v
                for k, v in processed_inputs.items()
            }

            generated_ids = self.model.generate(
                input_ids=processed_inputs["input_ids"],
                pixel_values=processed_inputs["pixel_values"],
                max_new_tokens=self.max_output,
                num_beams=self.num_beams,
            )

            generated_text = self.processor.batch_decode(
                generated_ids, skip_special_tokens=False
            )[0]
            parsed_answer: Dict[str, str] = (
                self.processor.post_process_generation(
                    generated_text,
                    task=self.task_prompt,
                    image_size=(image.width, image.height),
                )
            )
            assert (
                parsed_answer.get(self.task_prompt) is not None
            ), f"No output found. (Result: {parsed_answer})"
            logger.debug(f"Output: {parsed_answer}")
            results.append(parsed_answer[self.task_prompt])

        assert len(results) == len(
            image_inputs
        ), "Mismatch in input and output."

        outputs: List[dict] = []
        for file_text, config in zip(results, configs):
            file_text = file_text.strip()
            file_text = clean_whitespace(file_text)
            outputs.append(
                {
                    "transcription": file_text,
                    "confidence": 1,
                    "language": self.task_prompt,
                    "language_confidence": 1,
                }
            )

        assert len(outputs) == len(
            inputs
        ), f"Expected {len(inputs)} outputs but got {len(outputs)}"

        return outputs

    def unload(self) -> None:
        if self._model_loaded:
            del self.model
            del self.processor
            clear_cache()
            self._model_loaded = False

class Florence2Isolated(ProcessIsolatedInferenceModel):
    @classmethod
    def concrete_class(cls) -> Type[Florence2]:  # type: ignore
        return Florence2
