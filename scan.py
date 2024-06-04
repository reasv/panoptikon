
from src import load_paths_from_file, find_images_and_hashes, save_items_to_database

if __name__ == '__main__':
    file_path = 'paths.txt'
    starting_points = load_paths_from_file(file_path)
    hashes_info = find_images_and_hashes(starting_points)
    save_items_to_database(hashes_info, starting_points)