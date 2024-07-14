from dotenv import load_dotenv
load_dotenv()
import os
from src.launch import launch_app

if __name__ == '__main__':
    print(os.getenv("DB_FILE"))
    launch_app()