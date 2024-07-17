import json
import sqlite3
import subprocess
from typing import List, Sequence

import numpy as np
import torch
import whisperx
from chromadb.api import ClientAPI
from whisperx.audio import SAMPLE_RATE
from whisperx.types import TranscriptionResult

from src.data_extractors.extractor_job import run_extractor_job
from src.types import ItemWithPath


def format_ffmpeg_error(error: str) -> str:
    lines = error.splitlines()
    error_message = lines[-2:]
    return " ".join(error_message)


def check_audio_stream(file: str) -> bool:
    """
    Check if a file has any audio streams

    Parameters
    ----------
    file: str
        The file to check for audio streams

    Returns
    -------
    bool
        True if the file has audio streams, False otherwise.
    """
    try:
        # Run ffprobe to get stream information
        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "stream=codec_type",
            "-of",
            "json",
            file,
        ]
        result = subprocess.run(cmd, capture_output=True, check=True)
        streams = json.loads(result.stdout)

        # Check if any of the streams are of type "audio"
        has_audio = any(
            stream["codec_type"] == "audio" for stream in streams["streams"]
        )
        return has_audio
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Failed to check audio streams: {e.stderr.decode()}"
        ) from e


def load_audio(file: str, sr: int = SAMPLE_RATE):
    """
    Open an audio file and read as mono waveform, resampling as necessary

    Parameters
    ----------
    file: str
        The audio file to open

    sr: int
        The sample rate to resample the audio if necessary

    Returns
    -------
    A NumPy array containing the audio waveform, in float32 dtype.
    """
    try:
        # Launches a subprocess to decode audio while down-mixing and resampling as necessary.
        # Requires the ffmpeg CLI to be installed.
        cmd = [
            "ffmpeg",
            "-nostdin",
            "-threads",
            "0",
            "-i",
            file,
            "-f",
            "s16le",
            "-ac",
            "1",
            "-acodec",
            "pcm_s16le",
            "-ar",
            str(sr),
            "-",
        ]
        out = subprocess.run(cmd, capture_output=True, check=True).stdout
    except subprocess.CalledProcessError as e:
        if not check_audio_stream(file):
            return None
        raise RuntimeError(
            f"Failed to load audio: {format_ffmpeg_error(e.stderr.decode())}"
        ) from e

    return np.frombuffer(out, np.int16).flatten().astype(np.float32) / 32768.0


def run_whisper_extractor_job(
    conn: sqlite3.Connection,
    cdb: ClientAPI,
    batch_size=8,
    whisper_model="base",
):
    """
    Run a job that processes items in the database using the given batch inference function and item extractor.
    """
    setter_name = f"{whisper_model}"
    collection_name = f"text_embeddings"
    try:
        collection = cdb.get_collection(name=collection_name)
    except ValueError:
        collection = cdb.create_collection(name=collection_name)

    device = "cpu"
    if torch.cuda.is_available():
        device = "cuda"

    whisper_model = whisperx.load_model(whisper_model, device=device)

    def get_media_paths(item: ItemWithPath) -> Sequence[np.ndarray]:
        if item.type.startswith("video"):
            audio = load_audio(item.path)
            return [audio] if audio is not None else []
        elif item.type.startswith("audio"):
            audio = load_audio(item.path)
            return [audio] if audio is not None else []
        return []

    def process_batch(batch: Sequence[np.ndarray]) -> List[TranscriptionResult]:
        outputs: List[TranscriptionResult] = []
        for audio in batch:
            outputs.append(
                whisper_model.transcribe(audio=audio, batch_size=batch_size)
            )
        return outputs

    def handle_item_result(
        item: ItemWithPath,
        _: Sequence[np.ndarray],
        outputs: Sequence[TranscriptionResult],
    ):
        if len(outputs) == 0:
            return
        transcriptionResult = outputs[0]  # Only one output per item
        merged_text = "\n".join(
            [segment["text"] for segment in transcriptionResult["segments"]]
        )
        collection.add(
            ids=[f"{item.sha256}-{setter_name}"],
            documents=[merged_text],
            metadatas=[
                {
                    "item": item.sha256,
                    "source": "stt",
                    "model": setter_name,
                    "setter": setter_name,
                    "type": item.type,
                    "language": transcriptionResult["language"],
                    "general_type": item.type.split("/")[0],
                }
            ],
        )

    return run_extractor_job(
        conn, setter_name, 1, get_media_paths, process_batch, handle_item_result
    )
