
from src import load_paths_from_file, save_items_to_database, scan_images

if __name__ == '__main__':
    file_path = 'paths.txt'
    starting_points = load_paths_from_file(file_path)
    hashes_info = scan_images(starting_points)
    save_items_to_database(hashes_info, starting_points)