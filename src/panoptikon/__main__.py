from dotenv import load_dotenv
import static_ffmpeg
static_ffmpeg.add_paths()  # blocks until files are downloaded

load_dotenv()
from panoptikon.signal_handler import setup_signal_handlers
from panoptikon.launch import launch_app

if __name__ == "__main__":
    setup_signal_handlers()
    launch_app()
