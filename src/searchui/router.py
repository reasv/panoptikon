import logging
import os
import shutil
import subprocess
import threading
from typing import Tuple
from venv import logger

from fastapi import APIRouter
from fastapi.responses import RedirectResponse
from nodejs_wheel import npm, npx
import subprocess
import os
from panoptikon.signal_handler import register_child

logger = logging.getLogger(__name__)


def get_client_url(parent_hostname: str) -> str:
    if url := os.getenv("CLIENT_URL"):
        return url
    else:
        client_hostname = os.getenv("CLIENT_HOST", parent_hostname)
        client_port = int(os.getenv("CLIENT_PORT", 6339))
        if client_hostname == "0.0.0.0":
            client_hostname = "127.0.0.1"
        return f"http://{client_hostname}:{client_port}"


def get_routers(
    parent_hostname: str, parent_port: int
) -> Tuple[APIRouter, str]:
    client_url = get_client_url(parent_hostname)

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
        try:
            # Fetch the repository or pull the latest changes
            fetch_or_pull_repo(REPO_URL, client_dir)
        except Exception as e:
            logger.error(f"Failed to fetch or pull the UI repository: {e}")
            logger.error(f"Do you have an internet connection?")
            # Check if the directory exists and is not empty
            if not os.path.exists(client_dir) or not os.listdir(client_dir):
                logger.error("The client UI directory is empty. Exiting...")
                logger.error(
                    "If you want to disable the UI client, set ENABLE_CLIENT=false in the environment."
                )
                raise e
            return
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
                ["--yes", "next", "build"],
                cwd=client_dir,
                # stdout=subprocess.DEVNULL,
            )
        else:
            logger.info("Build is up to date. Skipping build step.")

    logger.info(f"Launching the webui on http://{hostname}:{port} ...")
    if public_api := os.getenv("PANOPTIKON_API_URL"):
        logger.info(f"API URL for client: {public_api}")
    
    run_node_client_server(client_dir, port, hostname)

def get_next_bin(client_dir):
    bin_dir = os.path.join(client_dir, "node_modules", ".bin")
    # On Windows: .cmd or .ps1, on UNIX: symlink/executable script
    for suffix in (".cmd", ".ps1", "") if os.name == "nt" else ("",):
        path = os.path.join(bin_dir, "next" + suffix)
        if os.path.exists(path):
            return path
    raise RuntimeError("Could not find node_modules/.bin/next script")

def run_node_client_server(client_dir, port, hostname):
    next_bin = get_next_bin(client_dir)

    cmd = [next_bin, "start", "-p", str(port), "-H", hostname]
    kwargs = dict(cwd=client_dir)
    # Pass through parent's env
    kwargs["env"] = os.environ.copy()

    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
        # Need to specify shell=True so .cmd/.ps1 resolution works
        kwargs["shell"] = True
    else:
        kwargs["preexec_fn"] = os.setsid

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        **kwargs
    )
    register_child(proc)

    # Print lines as they appear
    def log_output(stream, logger_method):
        for line in iter(stream.readline, ''):
            logger_method(f'{line.strip()}')

    logger = logging.getLogger("panoptikon.webui")
    t1 = threading.Thread(target=log_output, args=(proc.stdout, logger.info))
    t1.daemon = True
    t1.start()
    t2 = threading.Thread(target=log_output, args=(proc.stderr, logger.error))
    t2.daemon = True
    t2.start()

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
    ensure_git_available()
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

def ensure_git_available():
    if shutil.which("git") is None:
        logger.error(
            "'git' is required to download and update the frontend UI.\n"
            "Please install git and try to start Panoptikon again.\n"
            "https://git-scm.com/downloads"
            "Alternatively, you can set the environment variable ENABLE_CLIENT=false to disable the UI client entirely."
        )
        raise RuntimeError("git is not installed")
