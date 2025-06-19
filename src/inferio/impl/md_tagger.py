import logging
import re
import json
from io import BytesIO
from typing import List, Sequence, Tuple, Type

from PIL import Image as PILImage

from inferio.impl.utils import clear_cache, extract_partial_json_array, get_device
from inferio.model import InferenceModel
from inferio.process_model import ProcessIsolatedInferenceModel
from inferio.types import PredictionInput

logger = logging.getLogger(__name__)

class MoondreamTagger(InferenceModel):
    def __init__(
        self,
        model_repo: str = "vikhyatk/moondream2",
        model_revision: str = "2025-03-27",
        task: str = "query",
        namespace: str = "moondream",
        sub_namespace: str = "general",
        prompt: str = "List all visible objects, features, and characteristics of this image. Return the result as a JSON array.",
        enable_rating: bool = False,
        rating_prompt: str = "Give this image a safety rating. Return the result as one of these strings, in order of severity (from most safe to least safe): 'general', 'safe', 'sensitive', 'questionable', 'explicit'.",
        rating_list: list = [
            "general",
            "safe",
            "sensitive",
            "questionable",
            "explicit",
        ],
        rating_name: str = "rating",
        confidence: float = 1.0,
        max_output: int = 1024,
        init_args: dict = {},
    ):
        self.model_repo: str = model_repo
        self.namespace: str = namespace
        self.sub_namespace: str = sub_namespace
        self.model_revision: str = model_revision
        self.task: str = task
        self.prompt: str = prompt
        self.confidence: float = confidence
        if self.confidence < 0.0 or self.confidence > 1.0:
            logger.error(
                f"Confidence value {self.confidence} is out of range. Setting to 1.0.")
            self.confidence = 1.0
        
        if self.task not in ["query"]:
            logger.error(
                f"Task {self.task} is not supported. Defaulting to query.")
            self.task = "query"
        self.enable_rating: bool = enable_rating
        self.rating_prompt: str = rating_prompt
        if self.enable_rating and self.rating_prompt == "":
            logger.error(
                "Rating is enabled but no rating prompt provided. Disabling rating.")
            self.enable_rating = False
        self.rating_list: list = rating_list
        if self.enable_rating and not self.rating_list:
            logger.error(
                "Rating is enabled but no rating list provided. Disabling rating.")
            self.enable_rating = False
        self.rating_name: str = rating_name
        if self.rating_name == "":
            self.rating_name = "rating"
        self.max_output: int = max_output
        self.init_args = init_args
        self._model_loaded: bool = False

    @classmethod
    def name(cls) -> str:
        return "moondream_tagger"

    def load(self) -> None:
        if self._model_loaded:
            return
        
        from transformers import AutoModelForCausalLM
        
        # Check if accelerate is installed
        try:
            import accelerate
            ACCELERATE_AVAILABLE = True
        except ImportError:
            ACCELERATE_AVAILABLE = False

        self.devices = get_device()
        device = self.devices[0]

        if ACCELERATE_AVAILABLE and device == "cuda":
            # Optimized loading for GPU using accelerate
            self.model = AutoModelForCausalLM.from_pretrained(
                self.model_repo,
                revision=self.model_revision,
                trust_remote_code=True,
                device_map={"": "cuda"},
                **self.init_args,
            ).eval()
        else:
            # Fallback loading (standard PyTorch)
            self.model = AutoModelForCausalLM.from_pretrained(
                self.model_repo,
                revision=self.model_revision,
                trust_remote_code=True,
                **self.init_args,
            ).to(device).eval()
        logger.debug(f"Model {self.model_repo} loaded.")
        logger.debug(f"Compiling model...")
        self.model.compile()
        logger.debug(f"Model compiled.")
        self._model_loaded = True

    def predict(self, inputs: Sequence[PredictionInput]) -> List[dict]:
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
                raise ValueError("Moondream requires image inputs.")

        results: List[Tuple[str, str | None]] = []
        for image in image_inputs:
            # Process the inputs and ensure they are in the correct dtype and device
            encoded_image = self.model.encode_image(image)
            if self.task == "query":
                answer: str = self.model.query(encoded_image, self.prompt)["answer"]
            else:
                raise ValueError(f"Unsupported task: {self.task}")
            assert (
                answer is not None and answer != ""
            ), f"No output found. (Result: {answer})"
            logger.debug(f"Output: {answer}")

            rating_answer: str |None = None
            if self.enable_rating:
                rating_answer = self.model.query(encoded_image, self.rating_prompt)["answer"]
                assert (
                    rating_answer is not None and rating_answer != ""
                ), f"No output found. (Result: {rating_answer})"
                logger.debug(f"Rating Output: {rating_answer}")

            results.append((answer, rating_answer))

        assert len(results) == len(
            image_inputs
        ), "Mismatch in input and output."

        outputs: List[dict] = []
        for (file_text, rating_text), config in zip(results, configs):
            # Parse the JSON output
            try:
                tag_list = json.loads(file_text)
            except json.JSONDecodeError:
                logger.error(
                    f"Failed to parse JSON output: {file_text}. Attempting to extract JSON."
                )
                # Attempt to extract JSON from the string
                match = re.search(r"\[(.*?)\]", file_text)
                if match:
                    tag_list = json.loads(match.group(0))
                else:
                    logger.error(
                        f"Failed to extract JSON from output: {file_text}."
                    )
                    final_match = extract_partial_json_array(file_text)
                    if final_match is None:
                        raise ValueError("Invalid JSON format.")
                    else:
                        tag_list = final_match
            
            tag_list = [format_tag(tag) for tag in tag_list if isinstance(tag, str)]
            # Deduplicate tags
            tag_list = list(set(tag_list))
            
            tags: list[tuple[str, dict[str, float]]] = [(self.sub_namespace, dict((tag, self.confidence) for tag in tag_list)),]
            if self.enable_rating:
                found_rating: str | None = None
                # Reverse the list, because we match the *most severe* rating first
                reversed_rating_list: list[str] = self.rating_list.copy()
                reversed_rating_list.reverse()
                if rating_text is not None:
                    for rating_name in reversed_rating_list:
                        if rating_name in rating_text:
                            found_rating = rating_name
                            break # Only the most severe rating is used
                if not found_rating:
                    logger.error(
                        f"Rating not found in output: {rating_text}. Defaulting to 'general'."
                    )
                    found_rating = "unknown"
                else:
                    logger.debug(f"Rating found: {found_rating}")

                assert found_rating is not None, "Rating not found."
                # Insert as first element
                tags.insert(0, (self.rating_name, {found_rating: self.confidence}))
            outputs.append(
                {
                    "namespace": self.namespace,
                    "tags": tags,
                    "mcut": self.confidence,
                    "rating_severity": self.rating_list,
                    "metadata": {},
                    "metadata_score": 0.0,
                }
            )

        assert len(outputs) == len(
            inputs
        ), f"Expected {len(inputs)} outputs but got {len(outputs)}"
        return outputs

    def unload(self) -> None:
        if self._model_loaded:
            del self.model
            clear_cache()
            self._model_loaded = False

IMPL_CLASS = MoondreamTagger

class MoondreamTaggerIsolated(ProcessIsolatedInferenceModel):
    @classmethod
    def concrete_class(cls) -> Type[MoondreamTagger]:  # type: ignore
        return MoondreamTagger

def format_tag(tag: str) -> str:
    """
    Format the tag to be used in the output.
    Replace spaces with underscores and convert to lowercase.
    """
    tag = tag.strip()
    tag = tag.lower()
    tag = tag.replace(",", "_")
    tag = tag.replace(" ", "_")
    tag = tag.replace("-", "_")
    tag = tag.replace(".", "_")
    return tag