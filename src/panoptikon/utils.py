import math
import mimetypes
import os
import platform
import shlex
import subprocess
from datetime import datetime
from typing import List, Union

mimetypes.add_type("image/webp", ".webp")
from PIL import Image, ImageDraw, ImageFont


def execute_custom_command(command_template: str, path: str):
    """
    Executes the custom command by replacing placeholders with actual values.

    :param command_template: The command template with placeholders.
    :param path: The full path to the file.
    """
    directory, file_name = os.path.split(path)
    # Define placeholders
    replacements = {
        "{path}": f'"{path}"',
        "{folder}": f'"{directory}"',
        "{filename}": f'"{file_name}"',
    }

    # Replace placeholders
    for placeholder, actual in replacements.items():
        command_template = command_template.replace(placeholder, actual)

    if not command_template.strip():
        # Empty command; do nothing
        return

    # Split the command into arguments
    if platform.system() == "Windows":
        # On Windows, use shell=True to handle built-in commands
        subprocess.run(command_template, shell=True)
    else:
        # On Unix-like systems, use shlex to split the command
        args = shlex.split(command_template)
        subprocess.run(args)


def show_in_fm(path):
    """
    Open the given path in the file explorer and select the file, works on Windows, macOS, and Linux.

    :param path: The path to the file to be shown in the file explorer.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Path '{path}' does not exist")
    custom_cmd = os.getenv("SHOW_IN_FM_COMMAND")
    if custom_cmd is not None:
        try:
            execute_custom_command(custom_cmd, path)
            return
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Failed to execute custom SHOW_IN_FM_COMMAND for path '{path}': {e}"
            )

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
            # Check for specific file managers and use their select commands
                try:
                    # KDE - Dolphin
                    if subprocess.run(["which", "dolphin"], capture_output=True).returncode == 0:
                        subprocess.run(["dolphin", "--select", path])
                        return

                    # GNOME - Nautilus
                    if subprocess.run(["which", "nautilus"], capture_output=True).returncode == 0:
                        subprocess.run(["nautilus", "--select", path])
                        return

                    # XFCE - Thunar
                    if subprocess.run(["which", "thunar"], capture_output=True).returncode == 0:
                        subprocess.run(["thunar", "--select", path])
                        return

                    # Cinnamon/MATE - Nemo
                    if subprocess.run(["which", "nemo"], capture_output=True).returncode == 0:
                        subprocess.run(["nemo", path])  # Note: Nemo lacks a direct select flag
                        return
                except subprocess.CalledProcessError as e:
                    raise RuntimeError(
                        f"Failed to open path '{path}' in file explorer: {e}"
                    )
                # Fallback to xdg-open for directory
                directory = os.path.dirname(path)
                subprocess.run(["xdg-open", directory])
        else:
            raise OSError(f"Unsupported operating system: {system_name}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Failed to open path '{path}' in file explorer: {e}"
        )


def open_file(file_path):
    """
    Open the specified file using the default application.

    :param image_path: The path to the file to be opened.
    """
    # Check for custom command
    custom_cmd = os.getenv("OPEN_FILE_COMMAND")
    if custom_cmd is not None:
        try:
            execute_custom_command(custom_cmd, file_path)
            return
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Failed to execute custom OPEN_FILE_COMMAND for path '{file_path}': {e}"
            )

    # Default behavior
    if os.path.exists(file_path):
        try:
            if platform.system() == "Windows":
                os.startfile(file_path)
            elif platform.system() == "Darwin":  # macOS
                subprocess.run(["open", file_path])
            elif platform.system() == "Linux":
                subprocess.run(["xdg-open", file_path])
            else:
                raise OSError(
                    f"Unsupported operating system: {platform.system()}"
                )
            return
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to open file '{file_path}': {e}")
    else:
        raise FileNotFoundError(f"File '{file_path}' not found")


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
    assert (
        mime_type is not None
    ), f"Could not determine MIME type for {file_path}"
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
    # Get thumbnail directory from environment variable

    grid = create_image_grid(frames)
    write_text_on_image(grid, mime_type)
    write_text_on_image(frames[0], mime_type)
    return [grid, frames[0]]


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


def parse_tags(tags_str: str):
    tags = [tag.strip() for tag in tags_str.split(",") if tag.strip() != ""]

    def extract_tags_subtype(tag_list: list[str], prefix: str = "-"):
        remaining = []
        subtype = []
        for tag in tag_list:
            if tag.startswith(prefix):
                subtype.append(tag[1:])
            else:
                remaining.append(tag)
        return remaining, subtype

    tags, negative_tags = extract_tags_subtype(tags, "-")
    tags, negative_tags_match_all = extract_tags_subtype(tags, "~")
    tags, tags_match_any = extract_tags_subtype(tags, "*")
    return tags, tags_match_any, negative_tags, negative_tags_match_all

def get_inference_api_url(all: bool = False) -> Union[str, List[str]]:
    """
    Return the inference‑server URL(s).

    Environment variable precedence
    -------------------------------
    1. INFERENCE_API_URL          – may contain one URL or a comma‑separated list.
    2. HOST / PORT fallback       – if INFERENCE_API_URL is unset.

    Parameters
    ----------
    all : bool, default False
        • False → keep the historical behaviour (return first URL as str)  
        • True  → return *all* URLs as a list[str]

    Examples
    --------
    >>> os.environ["INFERENCE_API_URL"] = "http://gpu0:6342,http://gpu1:6342"
    >>> get_inference_api_url()
    'http://gpu0:6342'
    >>> get_inference_api_url(all=True)
    ['http://gpu0:6342', 'http://gpu1:6342']
    """
    raw = os.getenv("INFERENCE_API_URL")
    if raw:
        urls = [u.strip() for u in raw.split(",") if u.strip()]
    else:
        hostname = os.getenv("HOST", "127.0.0.1")
        if hostname == "0.0.0.0":
            hostname = "127.0.0.1"
        port = int(os.getenv("PORT", 6342))
        urls = [f"http://{hostname}:{port}"]

    return urls if all else urls[0]

def get_inference_api_urls() -> List[str]:
    """Convenience wrapper that **always** returns a list."""

    urls = get_inference_api_url(all=True)
    if isinstance(urls, str):
        return [urls]
    return urls

def is_external_inference_api() -> bool:
    """
    Check if the inference API is external (not local).

    Returns
    -------
    bool
        True if the inference API is external, False otherwise.
    """
    urls = get_inference_api_urls()
    # Filter out local URLs (having the same host and port as the current node)
    hostname = os.getenv("HOST", "127.0.0.1")
    if hostname == "0.0.0.0":
        hostname = "127.0.0.1"
    port = int(os.getenv("PORT", 6342))
    # Remove trailing slashes from URLs
    urls = [url.rstrip("/") for url in urls]
    # Check if there is a single URL with the current node's host and port
    for url in urls:
        if f"{hostname}:{port}" in url:
            # If the URL contains the current node's host and port, it's not external
            return False
    return True
    
def get_inference_api_url_weights() -> List[float] | None:
    """
    Get the weights for the URLs of the inference API.
    """
    weight_string_list = os.getenv("INFERENCE_API_URL_WEIGHTS")
    if weight_string_list is None:
        return None
    weight_list = [float(w) for w in weight_string_list.split(",")]
    if len(weight_list) != len(get_inference_api_urls()):
        raise ValueError(
            "The number of weights must match the number of URLs."
        )
    return weight_list