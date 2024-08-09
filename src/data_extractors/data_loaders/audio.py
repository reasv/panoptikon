import json
import random
import subprocess
from dataclasses import dataclass
from typing import List, Optional

import mutagen
import numpy as np
from mutagen.flac import FLAC
from mutagen.mp3 import MP3
from mutagen.mp4 import MP4
from PIL import Image, ImageDraw, ImageFont


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


def load_audio_single(file: str, sr: int = SAMPLE_RATE) -> List[np.ndarray]:
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
            return []
        raise RuntimeError(
            f"Failed to load audio: {format_ffmpeg_error(e.stderr.decode())}"
        ) from e

    return [np.frombuffer(out, np.int16).flatten().astype(np.float32) / 32768.0]


def create_audio_placeholder(
    mime_type: str, file_name: str, artist: str, album: str, title: str
) -> None:
    """idk what this does, chatgpt wrote it"""
    width, height = 1024, 1024
    image = Image.new("RGB", (width, height), "#1a1a1a")

    # Randomized gradient start and end colors
    start_color = (
        random.randint(20, 50),
        random.randint(20, 50),
        random.randint(50, 100),
    )
    end_color = (
        random.randint(150, 200),
        random.randint(200, 255),
        random.randint(200, 255),
    )

    # Creating gradient background with randomness
    for i in range(height):
        r = int(start_color[0] + (end_color[0] - start_color[0]) * (i / height))
        g = int(start_color[1] + (end_color[1] - start_color[1]) * (i / height))
        b = int(start_color[2] + (end_color[2] - start_color[2]) * (i / height))
        ImageDraw.Draw(image).line([(0, i), (width, i)], fill=(r, g, b))

    # Load fonts for the musical note, MIME type, and other text
    try:
        font_large = ImageFont.truetype("arial.ttf", 400)
        font_small = ImageFont.truetype("arial.ttf", 50)
        font_medium = ImageFont.truetype("arial.ttf", 60)
    except IOError:
        font_large = ImageFont.load_default()
        font_small = ImageFont.load_default()
        font_medium = ImageFont.load_default()

    # Draw the musical note (text representation)
    draw = ImageDraw.Draw(image)
    note_text = "♪"
    text_bbox = draw.textbbox((0, 0), note_text, font=font_large)
    textwidth, textheight = (
        text_bbox[2] - text_bbox[0],
        text_bbox[3] - text_bbox[1],
    )

    # Randomized position with a small offset for the musical note
    offset_x = random.randint(-50, 50)
    offset_y = random.randint(-50, 50)
    position = (
        (width - textwidth) // 2 + offset_x,
        (height - textheight) // 2 + offset_y,
    )

    draw.text(position, note_text, font=font_large, fill=(255, 255, 255))

    # Draw the MIME type in the lower left corner
    mime_position = (10, height - 60)
    draw.text(mime_position, mime_type, font=font_small, fill=(255, 255, 255))

    # Draw the artist, album, and title at the top of the image
    draw.text((10, 10), f"{artist}", font=font_medium, fill=(255, 255, 255))
    draw.text((10, 80), f"{album}", font=font_medium, fill=(255, 255, 255))
    draw.text((10, 150), f"{title}", font=font_medium, fill=(255, 255, 255))

    image.save(file_name)


def get_audio_thumbnail(mime_type: str, file_path: str, save_path: str):
    artist, album, title = None, None, None
    try:
        audio = mutagen.File(file_path)  # type: ignore

        if audio is None:
            raise ValueError("Unsupported audio format")

        artwork = None

        if isinstance(audio, MP3):
            tags = audio.tags
            if tags:
                artist = tags.get("TPE1", [None])[0]
                album = tags.get("TALB", [None])[0]
                title = tags.get("TIT2", [None])[0]
                if "APIC:" in tags:
                    artwork = tags["APIC:"].data

        elif isinstance(audio, MP4):
            artist = audio.get("\xa9ART", [None])[0]  # type: ignore
            album = audio.get("\xa9alb", [None])[0]  # type: ignore
            title = audio.get("\xa9nam", [None])[0]  # type: ignore
            if "covr" in audio:
                artwork = audio["covr"][0]

        elif isinstance(audio, FLAC):
            artist = audio.get("artist", [None])[0]  # type: ignore
            album = audio.get("album", [None])[0]  # type: ignore
            title = audio.get("title", [None])[0]  # type: ignore
            if audio.pictures:
                artwork = audio.pictures[0].data

        else:
            raise ValueError("Unsupported audio format")

        if artwork:
            # Save the extracted artwork
            with open(save_path, "wb") as img_file:
                img_file.write(artwork)
        else:
            raise ValueError("No cover art found")

    except Exception as e:
        create_audio_placeholder(
            mime_type, save_path, artist or "", album or "", title or ""
        )
