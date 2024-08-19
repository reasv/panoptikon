import logging
import os
import subprocess
from io import BytesIO
from typing import List

import PIL.Image
from PIL.Image import Image

logger = logging.getLogger(__name__)


def select_representative_frames(frames, max_frames):
    """
    Select a specified number of representative frames from a list.
    :param frames: List of frames to select from
    :param max_frames: Maximum number of frames to return
    :return: List of selected representative frames
    """
    if max_frames is None or len(frames) <= max_frames:
        return frames

    indices = [0]  # Always include the first frame
    if max_frames > 1:
        indices.append(len(frames) - 1)  # Always include the last frame
    if max_frames > 2 and len(frames) > 2:
        indices.append(len(frames) // 2)  # Include the middle frame

    if max_frames > 3 and len(frames) > 3:
        additional_frames_needed = max_frames - len(indices)
        step = len(frames) // (additional_frames_needed + 1)
        additional_indices = [
            i * step for i in range(1, additional_frames_needed + 1)
        ]
        indices.extend(additional_indices)

    # Remove duplicates and sort indices
    indices = sorted(set(indices))

    selected_frames = [frames[i] for i in indices]

    return selected_frames


# def saveImages(basePath, images: List[Image]):
#     # Normalize the path
#     basePath = os.path.normpath(basePath)
#     # Create the directory if it doesn't exist
#     os.makedirs(basePath, exist_ok=True)
#     for i in range(len(images)):
#         imagePath = os.path.join(basePath, f"{i}" + ".jpg")
#         images[i].save(imagePath)

#     create_image_grid(images).save(os.path.join(basePath, "grid.jpg"))


def extract_keyframes_ffmpeg(path: str, num_frames: int):
    # Create a temporary directory to store extracted frames
    # Get directory name from environment variable

    temp_dir = os.getenv("TEMP_DIR", "./data/tmp")
    os.makedirs(temp_dir, exist_ok=True)

    # Build ffmpeg command to extract keyframes
    command = [
        "ffmpeg",
        "-i",
        path,
        "-vf",
        "select=eq(pict_type\\,I)",
        "-vsync",
        "vfr",
        "-frame_pts",
        "true",
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


def extract_frames_ffmpeg(path: str, num_frames: int):
    # Create a temporary directory to store extracted frames
    temp_dir = os.getenv("TEMP_DIR", "./data/tmp")
    os.makedirs(temp_dir, exist_ok=True)

    # Get the duration of the video in seconds
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

    # Calculate interval between frames
    interval = duration / num_frames

    # Build ffmpeg command to extract frames at regular intervals
    command = [
        "ffmpeg",
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
