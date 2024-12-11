import logging
import os
import subprocess
from io import BytesIO
from typing import List

import PIL.Image
from PIL.Image import Image

from panoptikon.data_extractors.data_loaders.audio import extract_media_info

logger = logging.getLogger(__name__)


def extract_frames_ffmpeg(path: str, num_frames: int):
    # Create a temporary directory to store extracted frames
    temp_dir = os.getenv("TEMP_DIR", "./data/tmp")
    os.makedirs(temp_dir, exist_ok=True)

    # Get the duration of the video in seconds
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        duration = float(result.stdout)
    except Exception as e:
        info = extract_media_info(path)
        if info.video_track and info.video_track.duration:
            duration = info.video_track.duration
        else:
            raise ValueError(
                f"Could not extract duration of video at {path}"
            ) from e

    # Calculate interval between frames
    interval = duration / num_frames

    # Build ffmpeg command to extract frames at regular intervals
    command = [
        "ffmpeg",
        "-hwaccel", "auto",
        "-i",
        path,
        "-vf",
        f"fps=1/{interval}",
        "-vsync",
        "vfr",
        f"{temp_dir}/frame_%04d.png",
    ]

    # Execute ffmpeg command and suppress output
    subprocess.run(
        command,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Get list of extracted frames
    extracted_frames = sorted(os.listdir(temp_dir))

    # Load frames into PIL images and clean up immediately
    frames = []
    for frame_file in extracted_frames:
        frame_path = os.path.join(temp_dir, frame_file)
        with open(frame_path, "rb") as f:
            if len(frames) < num_frames:
                frame_image = PIL.Image.open(BytesIO(f.read()))
                frames.append(frame_image)
        os.remove(frame_path)  # Remove the frame after reading

    return frames


def video_to_frames(
    video_path: str, num_frames: int | None = None
) -> List[Image]:
    """
    Extract keyframes from a video and save them as images.
    :param video_path: Path to the video file
    :param num_frames: Number of frames to extract (default: None, extract all keyframes)
    """
    if num_frames is None:
        num_frames = 4
    logger.debug(f"Extracting {num_frames} frames from video at {video_path}")
    keyframes = extract_frames_ffmpeg(video_path, num_frames=num_frames)
    return keyframes
