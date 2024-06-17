import os
import subprocess
import platform

from PIL import Image
import math

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