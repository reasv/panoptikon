import logging
import os
import subprocess
import threading
from typing import Tuple
from venv import logger

from fastapi import APIRouter
from fastapi_proxy_lib.core.http import ReverseHttpProxy
from fastapi_proxy_lib.core.websocket import ReverseWebSocketProxy
from fastapi_proxy_lib.fastapi.router import RouterHelper
from nodejs_wheel import npm, npx

logger = logging.getLogger(__name__)


def get_routers(
    parent_hostname: str, parent_port: int
) -> Tuple[APIRouter, APIRouter]:
    if url := os.getenv("CLIENT_URL"):
        client_url = url
    else:
        client_hostname = os.getenv("CLIENT_HOST", parent_hostname)
        client_port = int(os.getenv("CLIENT_PORT", 6339))
        client_url = f"http://{client_hostname}:{client_port}/"
        run_node_client(client_hostname, client_port)

    logger.info(f"Client URL: {client_url}")
    reverse_http_proxy = ReverseHttpProxy(base_url=client_url)
    reverse_ws_proxy = ReverseWebSocketProxy(base_url=client_url)

    helper = RouterHelper()

    reverse_http_router = helper.register_router(
        reverse_http_proxy,
        APIRouter(tags=["client"]),
    )

    reverse_ws_router = helper.register_router(
        reverse_ws_proxy, APIRouter(tags=["client"])
    )

    return reverse_http_router, reverse_ws_router


def run_node_client(hostname: str, port: int):
    logger.info("Running Node.js client")
    # The client is in the folder this file is in, under "panoptikon-ui"
    client_dir = os.path.join(os.path.dirname(__file__), "panoptikon-ui")
    logger.debug(f"Client directory: {client_dir}")
    # Save current working directory
    cwd = os.getcwd()
    # Change cwd to the client directory
    os.chdir(client_dir)
    # Install dependencies
    npm("install", stdout=subprocess.DEVNULL)
    # Start the client
    npx(["--yes", "next", "build"], stdout=subprocess.DEVNULL)

    # Function to start the server in a separate thread
    def start_server():
        npx(
            ["--yes", "next", "start", "-p", str(port), "-H", hostname],
            stdout=subprocess.DEVNULL,
        )

    # Start the server in a new thread
    server_thread = threading.Thread(target=start_server)
    server_thread.start()

    os.chdir(cwd)
