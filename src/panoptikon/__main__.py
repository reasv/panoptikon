from dotenv import load_dotenv
import static_ffmpeg
static_ffmpeg.add_paths()  # blocks until files are downloaded

load_dotenv()
from panoptikon.launch import launch_app

if __name__ == "__main__":
    launch_app()
