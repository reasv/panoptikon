from __future__ import annotations
from typing import List

import gradio as gr


def get_history_paths(select_history: gr.State):
    print(f"History length is {len(select_history)}")
    # Should be in reverse order
    reverse = [item['path'] for item in select_history[::-1]]
    return reverse

def set_columns(columns: int):
    return gr.update(columns=columns)

def erase_history_fn(select_history: List[str], keep_last_n: int):
    if keep_last_n > 0:
        select_history = select_history[-keep_last_n:]
    else:
        select_history = []
    print("History erased")
    return select_history, gr.update(value=select_history)

def create_history_UI(select_history: gr.State):
    with gr.TabItem(label="History") as history_tab:
        with gr.Column(elem_classes="centered-content", scale=0):
            with gr.Row():
                erase_history = gr.Button("Erase History")
                keep_last_n = gr.Slider(minimum=0, maximum=100, value=0, step=1, label="Keep last N items on erase")
        with gr.Tabs():
            with gr.TabItem(label="Gallery"):
                columns_slider = gr.Slider(minimum=1, maximum=10, value=5, step=1, label="Number of columns")
                image_output = gr.Gallery(label="Results", scale=2, columns=5)

    history_tab.select(fn=get_history_paths, inputs=[select_history], outputs=[image_output])

    columns_slider.release(
        fn=set_columns,
        inputs=[columns_slider],
        outputs=[image_output]
    )

    erase_history.click(
        fn=erase_history_fn,
        inputs=[select_history, keep_last_n],
        outputs=[select_history, image_output]
    )