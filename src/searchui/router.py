import logging
import os
import shutil
import subprocess
import threading
from typing import Tuple
from venv import logger

from fastapi import APIRouter
from fastapi.responses import RedirectResponse
from fastapi_proxy_lib.core.http import ReverseHttpProxy
from fastapi_proxy_lib.core.websocket import ReverseWebSocketProxy
from fastapi_proxy_lib.fastapi.router import RouterHelper
from nodejs_wheel import npm, npx

logger = logging.getLogger(__name__)


def get_client_url(parent_hostname: str, parent_port: int) -> str:
    if url := os.getenv("CLIENT_URL"):
        return url
    else:
        client_hostname = os.getenv("CLIENT_HOST", parent_hostname)
        client_port = int(os.getenv("CLIENT_PORT", 6339))
        run_node_client(client_hostname, client_port)
        if client_hostname == "0.0.0.0":
            client_hostname = "127.0.0.1"
        return f"http://{client_hostname}:{client_port}/"


def get_routers(
    parent_hostname: str, parent_port: int
) -> Tuple[APIRouter, str]:
    client_url = get_client_url(parent_hostname, parent_port)

    logger.info(f"Client URL: {client_url}")
    router = APIRouter(
        tags=["client"],
    )

    # Redirect to client
    @router.get("/")
    async def root():
        return RedirectResponse(url=client_url)

    @router.get("/search")
    async def search():
        return RedirectResponse(url=f"{client_url}/search")

    @router.get("/scan")
    async def scan():
        return RedirectResponse(url=f"{client_url}/scan")

    return router, client_url


REPO_URL = "https://github.com/reasv/panoptikon-ui.git"


def run_node_client(hostname: str, port: int):
    logger.info("Running Node.js client")

    client_dir = os.path.join(os.path.dirname(__file__), "panoptikon-ui")
    build_dir = os.path.join(client_dir, ".next")

    logger.debug(f"Client directory: {client_dir}")
    if os.getenv("DISABLE_CLIENT_UPDATE", "false").lower() in ["1", "true"]:
        logger.info("Client build is disabled. Skipping build step.")
    else:
        # Fetch the repository or pull the latest changes
        fetch_or_pull_repo(REPO_URL, client_dir)

        # Check if build is needed based on the latest commit timestamp
        if is_build_needed(build_dir, client_dir):
            logger.info("Building the Next.js application...")
            # Install dependencies
            delete_build_directory(build_dir)
            npm(
                ["install", "--include=dev"],
                cwd=client_dir,
                stdout=subprocess.DEVNULL,
            )
            npx(
                ["--yes", "next@rc", "build"],
                cwd=client_dir,
                stdout=subprocess.DEVNULL,
            )
        else:
            logger.info("Build is up to date. Skipping build step.")

    # Function to start the server in a separate thread
    def start_server():
        logger.info("Starting the Node.js client server...")
        if public_api := os.getenv("PANOPTIKON_API_URL"):
            logger.info(f"API URL for client: {public_api}")
        npx(
            ["--yes", "next@rc", "start", "-p", str(port), "-H", hostname],
            cwd=client_dir,
        )

    # Start the server in a new thread
    server_thread = threading.Thread(target=start_server)
    server_thread.start()

    logger.info(f"Node.js client started on {hostname}:{port}")


def delete_build_directory(build_dir):
    """
    Delete the .next build directory to ensure a fresh build.
    """
    if os.path.exists(build_dir):
        logger.info(f"Deleting existing build directory: {build_dir}")
        shutil.rmtree(build_dir)


def get_latest_commit_timestamp(repo_dir):
    """
    Get the timestamp of the latest commit in the Git repository.
    """
    result = subprocess.run(
        ["git", "-C", repo_dir, "log", "-1", "--format=%ct"],
        capture_output=True,
        text=True,
        check=True,
    )
    return int(result.stdout.strip())


def get_build_timestamp(build_dir):
    """
    Get the modification time of the build directory.
    Returns None if the directory does not exist.
    """
    if not os.path.exists(build_dir):
        return None
    # Get the latest modification time of the .next directory
    return int(os.path.getmtime(build_dir))


def is_build_needed(build_dir, repo_dir):
    """
    Determine if a build is needed by comparing the latest commit timestamp with the build directory timestamp.
    """
    latest_commit_timestamp = get_latest_commit_timestamp(repo_dir)
    build_timestamp = get_build_timestamp(build_dir)

    # If build directory doesn't exist or if the latest commit is newer than the build, we need to rebuild
    if build_timestamp is None or latest_commit_timestamp > build_timestamp:
        return True
    return False


def fetch_or_pull_repo(repo_url, repo_dir):
    """
    Fetch the Git repository. If it doesn't exist, clone it. Otherwise, pull the latest changes.
    """
    if not os.path.exists(os.path.join(repo_dir, ".git")):
        logger.info(f"Cloning repository from {repo_url}...")
        subprocess.run(
            ["git", "clone", repo_url, repo_dir],
            check=True,
            stdout=subprocess.DEVNULL,
        )
    else:
        logger.info("Repository already exists. Pulling the latest changes...")
        subprocess.run(
            ["git", "-C", repo_dir, "pull"],
            check=True,
            stdout=subprocess.DEVNULL,
        )
