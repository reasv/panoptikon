import re
from io import BytesIO
from typing import List, Sequence, Type
import numpy as np
from PIL import Image as PILImage
from inferio.impl.utils import clear_cache, get_device
from inferio.model import InferenceModel
from inferio.process_model import ProcessIsolatedInferenceModel
from inferio.types import PredictionInput

class EasyOCRModel(InferenceModel):
    def __init__(
        self,
        languages: List[str] = ["en"],
        gpu: bool = True,
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
        
        for input_item, config in zip(inputs, configs):
            if input_item.file:
                image = PILImage.open(BytesIO(input_item.file)).convert("RGB")
                image_np = np.array(image)
                
                threshold = config.get("threshold", None)
                assert (
                    isinstance(threshold, float) or threshold is None
                ), "Threshold must be a float."
                
                # EasyOCR readtext returns a list of [bbox, text, confidence]
                result = self.model.readtext(image_np)
                
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
            else:
                raise ValueError("OCR requires image inputs.")
        
        assert len(outputs) == len(
            inputs
        ), f"Expected {len(inputs)} outputs but got {len(outputs)}"
        
        return outputs

    def unload(self) -> None:
        if self._model_loaded:
            del self.model
            clear_cache()
            self._model_loaded = False

def clean_whitespace(input_string: str) -> str:
    # Replace three or more consecutive whitespaces with just two
    cleaned_string = re.sub(r"(\s)\1{2,}", r"\1\1", input_string)
    return cleaned_string

class EasyOCRModelIsolated(ProcessIsolatedInferenceModel):
    @classmethod
    def concrete_class(cls) -> Type[EasyOCRModel]:  # type: ignore
        return EasyOCRModel