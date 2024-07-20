import math
import mimetypes
import os
import platform
import subprocess
from datetime import datetime

from src.data_extractors.data_loaders.video import video_to_frames

mimetypes.add_type("image/webp", ".webp")
from PIL import Image, ImageDraw, ImageFont


def show_in_fm(path):
    """
    Open the given path in the file explorer and select the file, works on Windows, macOS, and Linux.

    :param path: The path to the file to be shown in the file explorer.
    """

    system_name = platform.system()

    try:
        if system_name == "Windows":
            # subprocess.run(['explorer', '/select,', os.path.normpath(image_path)])
            # Using 'explorer' with '/select,' to highlight the file
            subprocess.run(["explorer", "/select,", os.path.normpath(path)])
        elif system_name == "Darwin":  # macOS
            # Using 'open' with '-R' to reveal the file in Finder
            subprocess.run(["open", "-R", path])
        elif system_name == "Linux":
            # This is trickier on Linux, as it depends on the file manager.
            # Here's a generic approach using 'xdg-open' to open the directory,
            # followed by attempts to focus the file.
            directory, file_name = os.path.split(path)
            subprocess.run(["xdg-open", directory])
            # Additional steps might be required depending on the desktop environment and file manager.
        else:
            raise OSError(f"Unsupported operating system: {system_name}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Failed to open path '{path}' in file explorer: {e}"
        )


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
    return os.path.join(path, "")


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
    grid_image = Image.new("RGB", (grid_width, grid_height))

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


def estimate_eta(
    scan_start_time: str, items_processed: int, remaining_items: int
):
    """
    Estimate the time remaining for the scan to complete based on the number of items processed and the total number of items.
    """
    time_elapsed = datetime.now() - datetime.fromisoformat(scan_start_time)
    items_per_second = items_processed / time_elapsed.total_seconds()
    remaining_time = remaining_items / (items_per_second or 1)
    return seconds_to_hms(remaining_time)


def make_video_thumbnails(
    frames: list[Image.Image], sha256: str, mime_type: str
):
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
        image = (
            image.convert("RGBA")
            if "transparency" in image.info
            else image.convert("RGB")
        )
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


def pretty_print_isodate(date: str):
    return datetime.fromisoformat(date).strftime("%Y-%m-%d %H:%M:%S")


def isodate_to_epoch(date: str):
    return int(datetime.fromisoformat(date).timestamp())


def isodate_minutes_diff(date1: str, date2: str) -> str:
    a, b = datetime.fromisoformat(date1), datetime.fromisoformat(date2)
    total_seconds = abs((a - b).total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if hours > 0:
        return f"{int(hours)}h{int(minutes)}m"
    elif minutes > 0:
        return f"{int(minutes)}m{int(seconds)}s"
    else:
        return f"{int(seconds)}s"
