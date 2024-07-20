import json
import subprocess

import numpy as np

SAMPLE_RATE = 16000


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
