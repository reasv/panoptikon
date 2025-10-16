from typing import List, Sequence
import torch
from PIL import Image as PILImage
from transformers import AutoTokenizer, AutoProcessor, AutoModelForCausalLM
from inferio.model import InferenceModel
from inferio.inferio_types import PredictionInput
from inferio.impl.utils import get_device, load_image_from_buffer
import logging

logger = logging.getLogger(__name__)

DEFAULT_OCR_PROMPT = "Extract all text from the image."

class DotsOCRModel(InferenceModel):
    def __init__(
        self,
        model_name: str = "rednote-hilab/dots.ocr",
        prompt: str = DEFAULT_OCR_PROMPT,
        gpu: bool = True,
        enable_batching: bool = True,
        max_new_tokens: int = 128,
        do_sample: bool = True,
        temperature: float = 0.7,
        top_p: float = 0.9,
    ):
        self.model_name = model_name
        self.prompt = prompt
        self.gpu = gpu
        self.enable_batching = enable_batching
        self._model_loaded = False
        self.max_new_tokens = max_new_tokens
        self.batch_size = batch_size
        self.do_sample = do_sample
        self.temperature = temperature
        self.top_p = top_p

    @classmethod
    def name(cls) -> str:
        return "dotsocr"

    def load(self) -> None:
        if self._model_loaded:
            return

        use_gpu = self.gpu and torch.cuda.is_available()

        self.model = AutoModelForCausalLM.from_pretrained(
            self.model_name,
            trust_remote_code=True,
            torch_dtype=torch.bfloat16,
            attn_implementation="flash_attention_2",
            device_map="auto" if use_gpu else None,
        )

        if not use_gpu:
            self.model = self.model.to("cpu")

        self.processor = AutoProcessor.from_pretrained(
            self.model_name, trust_remote_code=True
        )
        self.processor.tokenizer.padding_side = "left"
        self.device = self.model.device

        self._model_loaded = True

    def predict(self, inputs: Sequence[PredictionInput]) -> List[dict]:
        self.load()
        
        all_outputs = []
        images = [load_image_from_buffer(inp.file) for inp in inputs]

        if self.enable_batching and len(images) > 1:
            for i in range(0, len(images), self.batch_size):
                batch = images[i:i + self.batch_size]
                try:
                    all_outputs.extend(self._predict_batch(batch))
                except Exception as e:
                    logger.error(f"Batch processing failed for a chunk: {e}. Falling back to individual processing for this chunk.")
                    for image in batch:
                        all_outputs.append(self._predict_single(image))
        else:
            for image in images:
                all_outputs.append(self._predict_single(image))

        return all_outputs

    def _create_messages(self, image: PILImage.Image):
        return [
            {
                "role": "user",
                "content": [
                    {"type": "image", "image": image},
                    {"type": "text", "text": self.prompt},
                ],
            }
        ]

    def _predict_batch(self, images: List[PILImage.Image]) -> List[dict]:
        all_messages = [self._create_messages(img) for img in images]
        
        texts = [self.processor.apply_chat_template(
            messages, tokenize=False, add_generation_prompt=True
        ) for messages in all_messages]

        inputs_processed = self.processor(
            text=texts,
            images=images,
            return_tensors="pt",
            padding=True,
        ).to(self.device)
        
        with torch.no_grad():
            generated_ids = self.model.generate(
                **inputs_processed,
                max_new_tokens=self.max_new_tokens,
                use_cache=True,
                do_sample=self.do_sample,
                temperature=self.temperature,
                top_p=self.top_p,
            )
        
        generated_ids_trimmed = [
            out_ids[len(in_ids):] 
            for in_ids, out_ids in zip(inputs_processed.input_ids, generated_ids)
        ]

        generated_texts = self.processor.batch_decode(
            generated_ids_trimmed, skip_special_tokens=True, clean_up_tokenization_spaces=False
        )

        return [{ "transcription": text.strip(), "confidence": 1.0 } for text in generated_texts]


    def _predict_single(self, image: PILImage.Image) -> dict:
        messages = self._create_messages(image)
        text = self.processor.apply_chat_template(
            messages, tokenize=False, add_generation_prompt=True
        )

        inputs_processed = self.processor(
            text=[text],
            images=[image],
            return_tensors="pt",
        ).to(self.device)

        with torch.no_grad():
            generated_ids = self.model.generate(
                **inputs_processed,
                max_new_tokens=self.max_new_tokens,
                use_cache=True,
                do_sample=self.do_sample,
                temperature=self.temperature,
                top_p=self.top_p,
            )
        
        generated_ids_trimmed = generated_ids[:, inputs_processed.input_ids.shape[1]:]
        generated_text = self.processor.batch_decode(generated_ids_trimmed, skip_special_tokens=True)[0]

        return {
            "transcription": generated_text.strip(),
            "confidence": 1.0,
        }

    def unload(self) -> None:
        if self._model_loaded:
            del self.model
            del self.processor
            torch.cuda.empty_cache()
            self._model_loaded = False

IMPL_CLASS = DotsOCRModel
