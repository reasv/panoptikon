import re
import logging
from io import BytesIO
from typing import List, Sequence, Type
import numpy as np
from PIL import Image as PILImage
from inferio.impl.utils import clean_whitespace, clear_cache, get_device
from inferio.model import InferenceModel
from inferio.process_model import ProcessIsolatedInferenceModel
from inferio.types import PredictionInput

logger = logging.getLogger(__name__)

class EasyOCRModel(InferenceModel):
    def __init__(
        self,
        languages: List[str] = ["en"],
        gpu: bool = True,
        enable_batching: bool = True,
        model_storage_directory: str | None = None,
        download_enabled: bool = True,
        recog_network: str = 'standard',
        detector: bool = True,
        recognizer: bool = True,
        verbose: bool = True,
        quantize: bool = True,
        cudnn_benchmark: bool = False,
    ):
        self.languages = languages
        self.gpu = gpu
        self.model_storage_directory = model_storage_directory
        self.download_enabled = download_enabled
        self.recog_network = recog_network
        self.detector = detector
        self.recognizer = recognizer
        self.verbose = verbose
        self.quantize = quantize
        self.enable_batching = enable_batching
        self.cudnn_benchmark = cudnn_benchmark
        self._model_loaded: bool = False

    @classmethod
    def name(cls) -> str:
        return "easyocr"

    def load(self) -> None:
        import torch
        import easyocr
        
        if self._model_loaded:
            return

        self.devices = get_device()
        use_gpu = self.gpu and torch.cuda.is_available()
        
        self.model = easyocr.Reader(
            lang_list=self.languages,
            gpu=use_gpu,
            model_storage_directory=self.model_storage_directory,
            download_enabled=self.download_enabled,
            recog_network=self.recog_network,
            detector=self.detector,
            recognizer=self.recognizer,
            verbose=self.verbose,
            quantize=self.quantize,
            cudnn_benchmark=self.cudnn_benchmark
        )
        
        self._model_loaded = True

    def predict(self, inputs: Sequence[PredictionInput]) -> List[dict]:
        self.load()
        
        outputs: List[dict] = []
        configs: List[dict] = [inp.data for inp in inputs]  # type: ignore
        
        # Collect all images
        image_inputs: List[np.ndarray] = []
        for input_item in inputs:
            if input_item.file:
                image = PILImage.open(BytesIO(input_item.file)).convert("RGB")
                image_inputs.append(np.array(image))
            else:
                raise ValueError("OCR requires image inputs.")
        
        # Check if we need to pad images
        heights = [img.shape[0] for img in image_inputs]
        widths = [img.shape[1] for img in image_inputs]
        
        use_batched = self.enable_batching and len(image_inputs) > 1
        
        # If images have different sizes, pad them
        if (len(set(heights)) > 1 or len(set(widths)) > 1) and use_batched:
            image_inputs = pad_images_to_same_size(image_inputs)
        
        # Extract batch parameters from configs
        batch_params = {}
        if configs and len(configs) > 0:
            # Use parameters from the first config if available
            first_config = configs[0]
            for param in ['decoder', 'beamWidth', 'batch_size', 'workers', 'allowlist', 
                          'blocklist', 'detail', 'rotation_info', 'paragraph', 'min_size',
                          'contrast_ths', 'adjust_contrast', 'filter_ths', 'text_threshold',
                          'low_text', 'link_threshold', 'canvas_size', 'mag_ratio', 
                          'slope_ths', 'ycenter_ths', 'height_ths', 'width_ths', 'y_ths',
                          'x_ths', 'add_margin', 'threshold', 'bbox_min_score', 
                          'bbox_min_size', 'max_candidates', 'output_format']:
                if param in first_config:
                    batch_params[param] = first_config[param]
        
        batch_results = []
        # Process with batched method
        if use_batched:
            try:
                batch_results = self.model.readtext_batched(
                    image_inputs,
                    **batch_params
                )
            except Exception as e:
                # Fall back to individual processing if batched processing fails
                logger.error(f"Batch processing failed with error: {e}. Falling back to individual processing.")
                use_batched = False
        
        if not use_batched:
            # Process images individually
            batch_results = []
            for img in image_inputs:
                result = self.model.readtext(img, **batch_params)
                batch_results.append(result)
        
        # Process results for each image
        for result, config in zip(batch_results, configs):
            threshold = config.get("threshold", None)
            assert (
                isinstance(threshold, float) or threshold is None
            ), "Threshold must be a float."
            
            if not result:
                outputs.append({
                    "transcription": "",
                    "confidence": 0.0,
                    "language": self.languages[0] if self.languages else None,
                    "language_confidence": None,
                })
                continue
            
            # Group text into lines based on vertical position
            line_height_median = np.median([bbox[2][1] - bbox[0][1] for bbox, _, _ in result])
            line_gap = line_height_median * 0.5  # Use half the median line height as line gap threshold
            
            # Sort by top coordinate
            result.sort(key=lambda x: x[0][0][1])
            
            lines = []
            current_line = []
            last_bottom = None
            
            for detection in result:
                bbox, text, confidence = detection
                
                if threshold and confidence < threshold:
                    continue
                
                top = bbox[0][1]
                bottom = bbox[2][1]
                
                if last_bottom is not None and top > last_bottom + line_gap:
                    # This text is significantly below the previous line
                    if current_line:
                        lines.append(current_line)
                        current_line = []
                
                current_line.append((bbox, text, confidence))
                last_bottom = max(bottom, last_bottom) if last_bottom is not None else bottom
            
            if current_line:
                lines.append(current_line)
            
            # Sort each line by x-coordinate
            for i in range(len(lines)):
                lines[i].sort(key=lambda x: x[0][0][0])  # Sort by left x-coordinate
            
            # Construct the text
            file_text = ""
            confidences = []
            
            for line in lines:
                line_text = ""
                for _, text, confidence in line:
                    line_text += text + " "
                    confidences.append(confidence)
                file_text += line_text.strip() + "\n"
            
            file_text = file_text.strip()
            file_text = clean_whitespace(file_text)
            
            avg_confidence = sum(confidences) / max(len(confidences), 1)
            
            outputs.append({
                "transcription": file_text,
                "confidence": avg_confidence,
                "language": self.languages[0] if self.languages else None,
                "language_confidence": 1,  # EasyOCR doesn't provide language confidence
            })
        
        assert len(outputs) == len(
            inputs
        ), f"Expected {len(inputs)} outputs but got {len(outputs)}"
        
        return outputs

    def unload(self) -> None:
        if self._model_loaded:
            del self.model
            clear_cache()
            self._model_loaded = False

def pad_images_to_same_size(images: List[np.ndarray]) -> List[np.ndarray]:
        """
        Pad all images to the size of the largest image in the batch.
        
        Args:
            images: List of numpy arrays representing images
            
        Returns:
            List of padded images all with the same dimensions
        """
        if not images:
            return []
            
        # Find max height and width
        max_height = max(img.shape[0] for img in images)
        max_width = max(img.shape[1] for img in images)
        
        # Pad images to max dimensions
        padded_images = []
        for img in images:
            h, w = img.shape[:2]
            # Create a black canvas of the max size
            padded_img = np.zeros((max_height, max_width, 3), dtype=np.uint8)
            # Place the original image in the top-left corner
            padded_img[:h, :w] = img
            padded_images.append(padded_img)
            
        return padded_images

IMPL_CLASS = EasyOCRModel
class EasyOCRModelIsolated(ProcessIsolatedInferenceModel):
    @classmethod
    def concrete_class(cls) -> Type[EasyOCRModel]:  # type: ignore
        return EasyOCRModel