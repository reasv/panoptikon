import logging
import os
from typing import Tuple
from venv import logger

from fastapi import APIRouter
from fastapi_proxy_lib.core.http import ReverseHttpProxy
from fastapi_proxy_lib.core.websocket import ReverseWebSocketProxy
from fastapi_proxy_lib.fastapi.router import RouterHelper

logger = logging.getLogger(__name__)


def get_routers(
    parent_hostname: str, parent_port: int
) -> Tuple[APIRouter, APIRouter]:
    # Node.js client host and port (with fallbacks)
    client_hostname = os.getenv("CLIENT_HOST", parent_hostname)
    client_port = int(os.getenv("CLIENT_PORT", 6339))

    client_url = f"http://{client_hostname}:{client_port}/"
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
