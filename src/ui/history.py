from __future__ import annotations

import logging
from typing import List

import gradio as gr

from src.ui.components.multi_view import create_multiview

logger = logging.getLogger(__name__)


def get_history_paths(select_history: List[str]):
    logger.debug(f"History length is {len(select_history)}")
    # Should be in reverse order
    return select_history[::-1]


def erase_history_fn(select_history: List[str], keep_last_n: int):
    if keep_last_n > 0:
        select_history = select_history[-keep_last_n:]
    else:
        select_history = []
    logger.debug("History erased")
    history = get_history_paths(select_history)
    return select_history, history


def create_history_UI(select_history: gr.State, bookmarks_namespace: gr.State):
    with gr.TabItem(label="History") as history_tab:
        with gr.Column(elem_classes="centered-content", scale=0):
            with gr.Row():
                erase_history = gr.Button("Erase History")
                keep_last_n = gr.Slider(
                    minimum=0,
                    maximum=100,
                    value=0,
                    step=1,
                    label="Keep last N items on erase",
                )

        multi_view = create_multiview(bookmarks_namespace=bookmarks_namespace)

    history_tab.select(
        fn=get_history_paths,
        inputs=[select_history],
        outputs=[multi_view.files],
    )

    erase_history.click(
        fn=erase_history_fn,
        inputs=[select_history, keep_last_n],
        outputs=[select_history, multi_view.files],
    )
