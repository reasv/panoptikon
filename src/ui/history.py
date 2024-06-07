from __future__ import annotations
from typing import List

import gradio as gr
from src.db import get_all_tags_for_item_name_confidence, get_database_connection

def get_history_paths(select_history: gr.State):
    print(f"History length is {len(select_history)}")
    # Should be in reverse order
    reverse = [item['path'] for item in select_history[::-1]]
    return reverse

def get_history_list(select_history: List[dict]):
    reverse = [[item['path'], item['path'], item["sha256"]] for item in select_history[::-1]]
    return gr.update(samples=reverse)

def set_columns(columns: int):
    return gr.update(columns=columns)

def erase_history_fn(select_history: List[str], keep_last_n: int):
    if keep_last_n > 0:
        select_history = select_history[-keep_last_n:]
    else:
        select_history = []
    print("History erased")
    return select_history, gr.update(value=select_history), gr.update(samples=[])

def update_selected(dataset_data):
    sha256 = dataset_data[2]
    conn = get_database_connection()
    tags = { t[0]: t[1] for t in get_all_tags_for_item_name_confidence(conn, sha256)}
    conn.close()
    # Tags in the format "tag1, tag2, tag3"
    text = ", ".join(tags.keys())
    return dataset_data[1], tags, text

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

            with gr.TabItem(label="List") as list_tab:
                with gr.Row():
                    history_list = gr.Dataset(label="History", type="values", samples_per_page=25, samples=[], components=["image", "textbox"], scale=1)

                    selected_image = gr.Image(label="Selected Image", elem_id="bighistoryPreview", interactive=False, scale=2)
                    with gr.Tabs():
                        with gr.TabItem(label="Tags"):
                            tag_labels = gr.Label(label="Image Tags", scale=2)
                        with gr.TabItem(label="Tags List"):
                            tag_text = gr.Textbox(label="Tags", interactive=False, lines=5, scale=2)

    history_list.click(
        fn=update_selected,
        inputs=history_list,
        outputs=[selected_image, tag_labels, tag_text]
    )

    history_tab.select(
        fn=get_history_paths,
        inputs=[select_history],
        outputs=[image_output]
    )

    list_tab.select(
        fn=get_history_list,
        inputs=[select_history],
        outputs=[history_list]
    )

    columns_slider.release(
        fn=set_columns,
        inputs=[columns_slider],
        outputs=[image_output]
    )

    erase_history.click(
        fn=erase_history_fn,
        inputs=[select_history, keep_last_n],
        outputs=[select_history, image_output, history_list]
    )