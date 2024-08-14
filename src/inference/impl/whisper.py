from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, List, Sequence, Tuple

import numpy as np

from src.inference.impl.utils import clear_cache, get_device
from src.inference.model import InferenceModel
from src.inference.types import PredictionInput


class FasterWhisperModel(InferenceModel):
    def __init__(
        self,
        model_name: str,
        init_args: dict = {},
        transcribe_args: dict = {},
    ):
        self.model_name: str = model_name
        self.init_args = init_args
        self.transcribe_args = transcribe_args
        self._model_loaded: bool = False

    @classmethod
    def name(cls) -> str:
        return "faster_whisper"

    def load(self) -> None:
        from faster_whisper import WhisperModel

        if self._model_loaded:
            return

        self.devices = get_device()
        self.model = WhisperModel(
            model_size_or_path=self.model_name,
            device="auto",
            device_index=[i for i in range(len(self.devices))],
            compute_type="float16",
            num_workers=len(self.devices),
            **self.init_args,
        )
        self._model_loaded = True

    def __del__(self):
        self.unload()

    def unload(self) -> None:
        if self._model_loaded:
            del self.model
            clear_cache()
            self._model_loaded = False

    def predict(self, inputs: Sequence[PredictionInput]) -> List[dict]:
        from faster_whisper.transcribe import Segment, TranscriptionInfo

        self.load()
        configs = [inp.data for inp in inputs]
        assert all(
            isinstance(inp, dict) or inp is None for inp in configs
        ), "Input data must be dicts or None"
        input_files: List[bytes] = [inp.file for inp in inputs]  # type: ignore
        assert all(
            isinstance(inp, bytes) for inp in input_files
        ), "Inputs must be files"

        audio_inputs: List[np.ndarray] = []
        for file in input_files:
            audio: np.ndarray = np.frombuffer(file, dtype=np.float32)
            audio_inputs.append(audio)

        num_devices = len(self.devices)

        def process_audio(audio):
            return self.model.transcribe(audio=audio, **self.transcribe_args)

        if num_devices > 1:
            with ThreadPoolExecutor(max_workers=num_devices) as executor:
                future_to_audio = {
                    executor.submit(process_audio, audio): i
                    for i, audio in enumerate(audio_inputs)
                }
                transcriptions: List[
                    Tuple[Iterable[Segment], TranscriptionInfo]
                ] = [None] * len(
                    audio_inputs
                )  # type: ignore
                for future in as_completed(future_to_audio):
                    index = future_to_audio[future]
                    transcriptions[index] = future.result()
        else:
            transcriptions = [process_audio(audio) for audio in audio_inputs]
        # Remove all None values
        initial_length = len(transcriptions)
        transcriptions = [
            transcription for transcription in transcriptions if transcription
        ]
        assert (
            len(transcriptions) == initial_length
        ), "None values found in transcriptions"

        outputs: List[dict] = []
        for (segments, info), config in zip(transcriptions, configs):
            if isinstance(config, dict):
                threshold = config.get("threshold")
                assert isinstance(threshold, float), "Threshold must be a float"
            else:
                threshold = None

            segment_list = [
                (segment.text, segment.avg_logprob)
                for segment in segments
                if not threshold or segment.avg_logprob >= threshold
            ]
            text_segments = [segment[0] for segment in segment_list]
            merged_text = "\n".join(text_segments)

            merged_text = merged_text.strip()
            average_log_prob = (
                sum(segment[1] for segment in segment_list) / len(segment_list)
                if len(segment_list) > 0
                else None
            )
            outputs.append(
                {
                    "transcription": merged_text,
                    "confidence": average_log_prob,
                    "language": info.language,
                    "language_confidence": info.language_probability,
                }
            )
        return outputs