import json
import subprocess
from dataclasses import dataclass
from typing import List, Optional

import numpy as np


@dataclass
class AudioTrack:
    index: int
    duration: float
    codec: str
    language: Optional[str]


@dataclass
class VideoTrack:
    duration: float
    width: int
    height: int
    codec: str


@dataclass
class SubtitleTrack:
    index: int
    codec: str
    language: Optional[str]


@dataclass
class MediaInfo:
    audio_tracks: List[AudioTrack]
    video_track: Optional[VideoTrack]
    subtitle_tracks: List[SubtitleTrack]


def extract_media_info(file: str) -> MediaInfo:
    """
    Extract detailed information from an audio or video file, including subtitles.

    Parameters
    ----------
    file: str
        The path to the audio or video file to analyze.

    Returns
    -------
    MediaInfo
        A dataclass containing information about audio tracks, video track (if present), and subtitle tracks.
    """
    try:
        # Run ffprobe to get detailed stream and format information
        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "stream=index,codec_type,codec_name,duration,width,height,tags:format=duration",
            "-of",
            "json",
            file,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)

        audio_tracks = []
        video_track = None
        subtitle_tracks = []

        # Extract information for each stream
        for stream in data.get("streams", []):
            if stream["codec_type"] == "audio":
                audio_tracks.append(
                    AudioTrack(
                        index=stream["index"],
                        duration=float(stream.get("duration", 0)),
                        codec=stream["codec_name"],
                        language=stream.get("tags", {}).get("language"),
                    )
                )
            elif stream["codec_type"] == "video":
                video_track = VideoTrack(
                    duration=float(stream.get("duration", 0)),
                    width=stream["width"],
                    height=stream["height"],
                    codec=stream["codec_name"],
                )
            elif stream["codec_type"] == "subtitle":
                subtitle_tracks.append(
                    SubtitleTrack(
                        index=stream["index"],
                        codec=stream["codec_name"],
                        language=stream.get("tags", {}).get("language"),
                    )
                )

        # If stream duration is not available, use format duration
        format_duration = float(data["format"].get("duration", 0))
        if video_track and video_track.duration == 0:
            video_track.duration = format_duration
        for track in audio_tracks:
            if track.duration == 0:
                track.duration = format_duration

        return MediaInfo(
            audio_tracks=audio_tracks,
            video_track=video_track,
            subtitle_tracks=subtitle_tracks,
        )

    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to analyze file: {e.stderr}") from e
    except (json.JSONDecodeError, KeyError) as e:
        raise RuntimeError(f"Failed to parse ffprobe output: {str(e)}") from e


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


def load_audio(file: str, sr: int = SAMPLE_RATE) -> List[np.ndarray]:
    """
    Open an audio file and read all audio tracks as mono waveforms, resampling as necessary.

    Parameters
    ----------
    file: str
        The audio file to open

    sr: int
        The sample rate to resample the audio if necessary

    Returns
    -------
    List[np.ndarray]
        A list of NumPy arrays, each containing the audio waveform of a track, in float32 dtype.
        Returns an empty list if no audio tracks are found.
    """
    try:
        # Get the number of audio streams in the file
        probe_cmd = [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "a",
            "-show_entries",
            "stream=index",
            "-of",
            "csv=p=0",
            file,
        ]
        result = subprocess.run(
            probe_cmd, capture_output=True, text=True, check=True
        )
        stream_indices = [
            int(x) for x in result.stdout.strip().split("\n") if x
        ]

        if not stream_indices:
            return []

        audio_tracks = []
        for index in stream_indices:
            cmd = [
                "ffmpeg",
                "-nostdin",
                "-threads",
                "0",
                "-i",
                file,
                "-map",
                f"0:a:{index}",
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
            audio_data = (
                np.frombuffer(out, np.int16).flatten().astype(np.float32)
                / 32768.0
            )
            audio_tracks.append(audio_data)

        return audio_tracks

    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"FFmpeg failed: {e.stderr.decode()}") from e
    except Exception as e:
        raise RuntimeError(f"An unexpected error occurred: {str(e)}") from e
