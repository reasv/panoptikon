from typing import List
import os

import cv2
import numpy as np
from PIL.Image import Image
import PIL.Image

from src.utils import create_image_grid

def extract_frames(video_path, num_frames=10):
    """
    Extract a specified number of evenly spaced frames from a video.
    :param video_path: Path to the video file
    :param num_frames: Number of frames to extract
    :return: List of extracted frames as PIL Images
    """
    frames = []
    cap = cv2.VideoCapture(video_path)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    frame_indices = np.linspace(0, total_frames - 1, num=num_frames, dtype=int)
    
    for idx in frame_indices:
        cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
        success, image = cap.read()
        if success:
            # Convert the frame to RGB and then to a PIL Image
            image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
            pil_image = PIL.Image.fromarray(image)
            frames.append(pil_image)
    
    cap.release()
    return frames

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
        additional_indices = [i * step for i in range(1, additional_frames_needed + 1)]
        indices.extend(additional_indices)
    
    # Remove duplicates and sort indices
    indices = sorted(set(indices))
    
    selected_frames = [frames[i] for i in indices]
    
    return selected_frames

def extract_keyframes(video_path, threshold=0.8) -> List[Image]:
    """
    Extract keyframes from a video based on color histogram differences.
    :param video_path: Path to the video file
    :param threshold: Threshold for histogram difference to consider a frame as keyframe
    :return: List of extracted keyframes as PIL Images
    """
    keyframes: List[Image] = []
    cap = cv2.VideoCapture(video_path)
    ret, prev_frame = cap.read()
    if not ret:
        return keyframes

    prev_hist = cv2.calcHist([prev_frame], [0, 1, 2], None, [8, 8, 8], [0, 256, 0, 256, 0, 256])
    prev_hist = cv2.normalize(prev_hist, prev_hist).flatten()
    keyframes.append(PIL.Image.fromarray(cv2.cvtColor(prev_frame, cv2.COLOR_BGR2RGB)))

    while True:
        ret, frame = cap.read()
        if not ret:
            break
        
        hist = cv2.calcHist([frame], [0, 1, 2], None, [8, 8, 8], [0, 256, 0, 256, 0, 256])
        hist = cv2.normalize(hist, hist).flatten()
        
        hist_diff = cv2.compareHist(prev_hist, hist, cv2.HISTCMP_CORREL)
        
        if hist_diff < threshold:
            keyframes.append(PIL.Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)))
            prev_hist = hist
    
    cap.release()
    return keyframes

def combine_results(results: List[dict[str, float]]) -> dict[str, float]:
    """
    Combine multiple results into a single result by picking the highest confidence score for each tag.
    :param results: List of results to combine
    :return: Combined result as a dictionary of tags and scores
    """
    combined_result = dict()
    for result in results:
        for tag, score in result.items():
            if tag not in combined_result or score > combined_result[tag]:
                combined_result[tag] = score
    return combined_result

def saveImages(basePath, images: List[Image]):
    # Normalize the path
    basePath = os.path.normpath(basePath)
    # Create the directory if it doesn't exist
    os.makedirs(basePath, exist_ok=True)
    for i in range(len(images)):
        imagePath = os.path.join(basePath, f"{i}" + ".jpg")
        images[i].save(imagePath)

    create_image_grid(images).save(os.path.join(basePath, "grid.jpg"))

def video_to_frames(video_path: str, keyframe_threshold=0.8, num_frames: int = None, thumbnail_save_path=None):
    """
    Extract keyframes from a video and save them as images.
    :param video_path: Path to the video file
    :param keyframe_threshold: Threshold for keyframe extraction
    :param num_frames: Number of frames to extract (default: None, extract all keyframes)
    """
    if keyframe_threshold:
        # Extract keyframes from the video
        keyframes = extract_keyframes(video_path, threshold=keyframe_threshold)
    else:
        # Extract frames from the video
        if not num_frames:
            num_frames = 1
        keyframes = extract_frames(video_path, num_frames=num_frames)

    if num_frames and len(keyframes) > num_frames:
        # Select representative frames
        keyframes: List[Image] = select_representative_frames(keyframes, max_frames=num_frames)

    if num_frames and len(keyframes) < num_frames:
        additional_samples = extract_frames(video_path, num_frames=num_frames-len(keyframes))
        keyframes.extend(additional_samples)
    # Save the keyframes as images
    if thumbnail_save_path:
        saveImages(thumbnail_save_path, images=keyframes)
    return keyframes