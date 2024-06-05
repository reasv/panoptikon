import os
import subprocess
import platform

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

# Example usage:
# show_in_file_explorer('/path/to/your/file.txt')