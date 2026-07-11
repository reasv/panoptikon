import logging
import platform
from typing import Dict, List, Sequence

from PIL import Image as PILImage

from inferio.impl.utils import (
    clean_whitespace,
    clear_cache,
    get_device,
    load_image_from_buffer,
    print_resource_usage,
)
from inferio.model import InferenceModel
from inferio.inferio_types import PredictionInput

logger = logging.getLogger(__name__)


class Florence2(InferenceModel):
    """
    Florence 2 (community models) implementation.
    """

    def __init__(
        self,
        model_name: str,
        task_prompt: str,
        text_input: str | None = None,
        flash_attention: bool = False,
        max_output: int = 1024,
        num_beams: int = 3,
        do_sample: bool = False,
        enable_batch: bool = True,
        init_args: dict = {},
    ):
        self.model_name: str = model_name
        self.task_prompt: str = task_prompt
        self.text_input: str | None = text_input
        self.flash_attention: bool = flash_attention
        self.max_output: int = max_output
        self.num_beams: int = num_beams
        self.init_args = init_args
        self.do_sample: bool = do_sample
        self.enable_batch: bool = enable_batch
        self._model_loaded: bool = False

        self.devices = []
        self.model = None
        self.processor = None
        self._dtype = None
        self._device = None

    @classmethod
    def name(cls) -> str:
        return "florence2"

    def load(self) -> None:
        if self._model_loaded:
            return

        import torch
        from transformers import AutoProcessor, Florence2ForConditionalGeneration

        self.devices = get_device()
        self._device = self.devices[0]

        # Prefer fp16 on CUDA, otherwise use fp32 for CPU (and avoid half on CPU)
        if str(self._device).startswith("cuda"):
            self._dtype = torch.float16
        else:
            self._dtype = torch.float32

        attn_impl = "flash_attention_2" if self.flash_attention else "sdpa"

        # Community models are native in transformers; no trust_remote_code required.
        self.model = (
            Florence2ForConditionalGeneration.from_pretrained(
                self.model_name,
                attn_implementation=attn_impl,
                dtype=self._dtype,
                **(self.init_args or {}),
            )
            .to(self._device) # type: ignore
            .eval()
        )

        self.processor = AutoProcessor.from_pretrained(
            self.model_name,
            **(self.init_args or {}),
        )

        if platform.system() != "Darwin" and str(self._device).startswith("cuda"):
            # Reduce overhead is generally best for inference, but only compile on CUDA
            self.model = torch.compile(self.model, mode="reduce-overhead")

        logger.debug(f"Model {self.model_name} loaded.")
        print_resource_usage(logger=logger)
        self._model_loaded = True

    def predict(self, inputs: Sequence[PredictionInput]) -> List[dict]:
        self.load()
        assert self.processor is not None
        assert self.model is not None

        image_inputs: List[PILImage.Image] = []
        configs: List[dict] = [inp.data for inp in inputs]  # type: ignore

        for input_item in inputs:
            if input_item.file:
                image_inputs.append(load_image_from_buffer(input_item.file))
            else:
                raise ValueError("Florence2 requires image inputs.")

        prompt = self.task_prompt if self.text_input is None else (self.task_prompt + self.text_input)

        if self.enable_batch:
            results = self.batch_predict(prompt, image_inputs)
        else:
            results = [self.single_predict(prompt, img) for img in image_inputs]

        assert len(results) == len(image_inputs), "Mismatch in input and output."

        outputs: List[dict] = []
        for file_text, _config in zip(results, configs):
            file_text = clean_whitespace(file_text.strip())
            outputs.append(
                {
                    "transcription": file_text,
                    "confidence": 1,
                    "language": self.task_prompt,
                    "language_confidence": 1,
                }
            )

        assert len(outputs) == len(inputs), f"Expected {len(inputs)} outputs but got {len(outputs)}"
        return outputs

    def _to_device_dtype(self, batch: Dict[str, "torch.Tensor"]) -> Dict[str, "torch.Tensor"]: # type: ignore
        """
        Moves processor outputs to correct device and dtype.
        Florence2 uses pixel_values + input_ids; pixel_values should match model dtype.
        """
        import torch

        assert self._device is not None
        assert self._dtype is not None

        out: Dict[str, torch.Tensor] = {}
        for k, v in batch.items():
            if not isinstance(v, torch.Tensor):
                continue
            v = v.to(self._device)
            # Only cast floating tensors; keep integer ids as-is
            if v.is_floating_point():
                v = v.to(self._dtype)
            out[k] = v
        return out

    def batch_predict(self, prompt: str, image_inputs: List[PILImage.Image]) -> List[str]:
        import torch

        assert self.processor is not None
        assert self.model is not None

        processed_inputs = self.processor(
            text=[prompt] * len(image_inputs),
            images=image_inputs,
            return_tensors="pt",
        )
        processed_inputs = self._to_device_dtype(processed_inputs)

        with torch.no_grad():
            generated_ids = self.model.generate(  # type: ignore[union-attr]
                input_ids=processed_inputs["input_ids"],
                pixel_values=processed_inputs["pixel_values"],
                max_new_tokens=self.max_output,
                num_beams=self.num_beams,
                do_sample=self.do_sample,
            )

        generated_texts = self.processor.batch_decode(generated_ids, skip_special_tokens=False)

        parsed_answers: List[Dict[str, str]] = [
            self.processor.post_process_generation(
                text,
                task=self.task_prompt,
                image_size=(img.width, img.height),
            )
            for text, img in zip(generated_texts, image_inputs)
        ]

        results: List[str] = []
        for answer in parsed_answers:
            task_answer = answer.get(self.task_prompt)
            if task_answer is None:
                raise RuntimeError(f"No output found for task '{self.task_prompt}'. (Result: {answer})")
            # Clean the output text
            task_answer = (
                task_answer.replace("</s>", "")
                .replace("<s>", "")
                .replace("<pad>", "")
            )
            results.append(task_answer)

        return results

    def single_predict(self, prompt: str, image: PILImage.Image) -> str:
        import torch

        assert self.processor is not None
        assert self.model is not None

        processed_inputs = self.processor(text=prompt, images=image, return_tensors="pt")
        processed_inputs = self._to_device_dtype(processed_inputs)

        with torch.no_grad():
            generated_ids = self.model.generate(  # type: ignore[union-attr]
                input_ids=processed_inputs["input_ids"],
                pixel_values=processed_inputs["pixel_values"],
                max_new_tokens=self.max_output,
                num_beams=self.num_beams,
                do_sample=self.do_sample,
            )

        generated_text = self.processor.batch_decode(generated_ids, skip_special_tokens=False)[0]
        parsed_answer: Dict[str, str] = self.processor.post_process_generation(
            generated_text,
            task=self.task_prompt,
            image_size=(image.width, image.height),
        )

        task_answer = parsed_answer.get(self.task_prompt)
        if task_answer is None:
            raise RuntimeError(f"No output found for task '{self.task_prompt}'. (Result: {parsed_answer})")

        return (
            task_answer.replace("</s>", "")
            .replace("<s>", "")
            .replace("<pad>", "")
        )

    def unload(self) -> None:
        if self._model_loaded:
            try:
                del self.model
                del self.processor
            finally:
                self.model = None
                self.processor = None
                clear_cache()
                self._model_loaded = False


IMPL_CLASS = Florence2
