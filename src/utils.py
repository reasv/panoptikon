import os
import subprocess
import platform
from typing import Any, Callable, Generator, List, Sequence, Tuple
import math
from datetime import datetime
import mimetypes

from src.video import video_to_frames
mimetypes.add_type('image/webp', '.webp')
from PIL import Image, ImageDraw, ImageFont
from PIL import Image as PILImage
import numpy as np
from doctr.io.html import read_html
from doctr.io.pdf import read_pdf

from src.types import ItemWithPath

def show_in_fm(path):
    """
    Open the given path in the file explorer and select the file, works on Windows, macOS, and Linux.

    :param path: The path to the file to be shown in the file explorer.
    """

    system_name = platform.system()

    try:
        if system_name == 'Windows':
            # subprocess.run(['explorer', '/select,', os.path.normpath(image_path)])
            # Using 'explorer' with '/select,' to highlight the file
            subprocess.run(['explorer', '/select,', os.path.normpath(path)])
        elif system_name == 'Darwin':  # macOS
            # Using 'open' with '-R' to reveal the file in Finder
            subprocess.run(['open', '-R', path])
        elif system_name == 'Linux':
            # This is trickier on Linux, as it depends on the file manager.
            # Here's a generic approach using 'xdg-open' to open the directory,
            # followed by attempts to focus the file.
            directory, file_name = os.path.split(path)
            subprocess.run(['xdg-open', directory])
            # Additional steps might be required depending on the desktop environment and file manager.
        else:
            raise OSError(f"Unsupported operating system: {system_name}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to open path '{path}' in file explorer: {e}")

def open_file(image_path):
    if os.path.exists(image_path):
        os.startfile(image_path, cwd=os.path.dirname(image_path))
        return f"Attempting to open: {image_path}"
    else:
        return "File does not exist"
    
def open_in_explorer(image_path):
    if os.path.exists(image_path):
        show_in_fm(image_path)
        return f"Attempting to open: {image_path}"
    else:
        return "File does not exist"

def ensure_trailing_slash(path: str) -> str:
    """
    Ensure the path ends with a trailing slash appropriate for the OS.
    """
    return os.path.join(path, '')

def normalize_path(path: str) -> str:
    """
    Normalize the path to be in our preferred format.
    """
    return ensure_trailing_slash(os.path.abspath(path.strip()))

def get_mime_type(file_path: str):
    """
    Get the MIME type of the file at the given path.
    """
    mime_type, _ = mimetypes.guess_type(file_path, strict=False)
    return mime_type

def write_text_on_image(image: Image.Image, text: str):
    draw = ImageDraw.Draw(image)
    font_size = 20  # Adjust as needed
    font = ImageFont.load_default(size=font_size)

    # Text position
    x = 10
    y = image.height - font_size - 10
    
    # Draw outline
    outline_range = 1
    for dx in range(-outline_range, outline_range + 1):
        for dy in range(-outline_range, outline_range + 1):
            if dx != 0 or dy != 0:
                draw.text((x + dx, y + dy), text, font=font, fill="black")
    
    # Draw text
    draw.text((x, y), text, font=font, fill="white")

def create_image_grid(image_list) -> Image.Image:
    """
    Create a grid of images from a list of PIL.Image.Image objects, automatically
    determining the grid size to form a square or slightly rectangular shape if needed.
    
    Args:
    - image_list (list of PIL.Image.Image): List of images to include in the grid.
    
    Returns:
    - PIL.Image.Image: The resulting grid image.
    """
    if not image_list:
        raise ValueError("The image_list is empty.")
    
    # Number of images
    num_images = len(image_list)
    
    # Determine the grid size
    grid_size = math.ceil(math.sqrt(num_images))
    
    # Get the size of each image (assuming all images are the same size)
    img_width, img_height = image_list[0].size
    
    # Calculate the size of the output image
    grid_width = grid_size * img_width
    grid_height = grid_size * img_height
    
    # Create a new blank image with the calculated size
    grid_image = Image.new('RGB', (grid_width, grid_height))
    
    # Paste each image into the grid
    for index, img in enumerate(image_list):
        row = index // grid_size
        col = index % grid_size
        grid_image.paste(img, (col * img_width, row * img_height))
    
    return grid_image

def seconds_to_hms(seconds):
    # Format the time as a string in the format HHhMMmSSs eg 1h23m45s
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    remaining_seconds = int(round(seconds % 60, 0))
    if hours == 0 and minutes == 0:
        return f"{remaining_seconds}s"
    if hours == 0:
        return f"{minutes}m{remaining_seconds}s"
    return f"{hours}h{minutes}m{remaining_seconds}s"

def estimate_eta(scan_start_time: str, items_processed: int, remaining_items: int):
    """
    Estimate the time remaining for the scan to complete based on the number of items processed and the total number of items.
    """
    time_elapsed = datetime.now() - datetime.fromisoformat(scan_start_time)
    items_per_second = items_processed / time_elapsed.total_seconds()
    remaining_time = remaining_items / (items_per_second or 1)
    return seconds_to_hms(remaining_time)

def make_video_thumbnails(frames: list[Image.Image], sha256: str, mime_type: str):
    """
    Create thumbnails for a video file.
    :param frames: List of frames to create thumbnails from.
    :param sha256: SHA256 hash of the video file.
    :param mime_type: MIME type of the video file.
    """
    os.makedirs("./thumbs", exist_ok=True)
    grid = create_image_grid(frames)
    write_text_on_image(grid, mime_type)
    grid.save(f"./thumbs/{sha256}-grid.jpg")
    write_text_on_image(frames[0], mime_type)
    frames[0].save(f"./thumbs/{sha256}-0.jpg")

def pil_ensure_rgb(image: Image.Image) -> Image.Image:
    # convert to RGB/RGBA if not already (deals with palette images etc.)
    if image.mode not in ["RGB", "RGBA"]:
        image = image.convert("RGBA") if "transparency" in image.info else image.convert("RGB")
    # convert RGBA to RGB with white background
    if image.mode == "RGBA":
        canvas = Image.new("RGBA", image.size, (255, 255, 255))
        canvas.alpha_composite(image)
        image = canvas.convert("RGB")
    return image

def pil_pad_square(image: Image.Image) -> Image.Image:
    w, h = image.size
    # get the largest dimension so we can pad to a square
    px = max(image.size)
    # pad to square with white background
    canvas = Image.new("RGB", (px, px), (255, 255, 255))
    canvas.paste(image, ((px - w) // 2, (px - h) // 2))
    return canvas

def batch_items(
        items_generator: Generator[Tuple[ItemWithPath, int, int], Any, None],
        batch_size: int,
        process_batch_func,
        item_extractor_func
    ):
    while True:
        batch: List[Tuple[ItemWithPath, int, int]] = []
        work_units = []
        batch_index_to_work_units: dict[int, List[int]] = {}
        for item, remaining, total_items in items_generator:
            batch_index = len(batch)
            batch.append((item, remaining, total_items))
            batch_index_to_work_units[batch_index] = []
            item_wus = item_extractor_func(item)
            for wu in item_wus:
                # The index of the work unit we are adding
                wu_index = len(work_units)
                work_units.append(wu)
                batch_index_to_work_units[batch_index].append(wu_index)
            if len(work_units) >= batch_size:
                # Stop adding items to the batch, and process
                break
        if len(work_units) == 0:
            # No more work to do
            break
        processed_batch_items = process_batch_func(work_units)
        # Yield the batch and the processed items matching the work units to the batch item
        for batch_index, wu_indices in batch_index_to_work_units.items():
            item, remaining, total_items = batch[batch_index]
            yield item, remaining, total_items, [processed_batch_items[i] for i in wu_indices]

def minibatcher(input_list: Sequence, run_minibatch, batch_size: int):
    result = [None] * len(input_list)  # Initialize a result list with None values
    start = 0  # Starting index for each batch

    while start < len(input_list):
        end = min(start + batch_size, len(input_list))  # Calculate end index for the current batch
        batch = input_list[start:end]  # Extract the current batch
        batch_result = run_minibatch(batch)  # Process the batch
        result[start:end] = batch_result  # Insert the batch result into the result list
        start = end  # Move to the next batch

    return result

def create_item_image_extractor(error_callback: Callable[[ItemWithPath], None]) -> Callable[[ItemWithPath], List[np.ndarray]]:
    def item_image_extractor(item: ItemWithPath) -> List[np.ndarray]:
        try:
            if item.type.startswith("image"):
                return [np.array(pil_ensure_rgb(PILImage.open(item.path)))]
            if item.type.startswith("video"):
                frames = video_to_frames(item.path, num_frames=4)
                make_video_thumbnails(frames, item.sha256, item.type)
                return [np.array(pil_ensure_rgb(frame)) for frame in frames]
            if item.type.startswith("application/pdf"):
                return read_pdf(item.path)
            if item.type.startswith("text/html"):
                return read_pdf(read_html(item.path))
        except Exception as e:
            print(f"Failed to read {item.path}: {e}")
            error_callback(item)
        return []
    return item_image_extractor

def create_item_image_extractor_pil(error_callback: Callable[[ItemWithPath], None]) -> Callable[[ItemWithPath], List[PILImage.Image]]:
    def item_image_extractor(item: ItemWithPath) -> List[PILImage.Image]:
        try:
            if item.type.startswith("image"):
                return [PILImage.open(item.path)]
            if item.type.startswith("video"):
                frames = video_to_frames(item.path, num_frames=4)
                make_video_thumbnails(frames, item.sha256, item.type)
                return frames
            if item.type.startswith("application/pdf"):
                return [PILImage.fromarray(page) for page in read_pdf(item.path)]
            if item.type.startswith("text/html"):
                return [PILImage.fromarray(page) for page in read_pdf(read_html(item.path))]
        except Exception as e:
            print(f"Failed to read {item.path}: {e}")
            error_callback(item)
        return []
    return item_image_extractor