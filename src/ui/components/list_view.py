from __future__ import annotations
from typing import List

import gradio as gr
from dataclasses import dataclass

from src.db import get_all_tags_for_item_name_confidence, get_database_connection
from src.utils import open_file, open_in_explorer
from src.ui.components.utils import toggle_bookmark, on_selected_image_get_bookmark_state

def on_select_image(dataset_data):
    sha256 = dataset_data[2]
    pathstr = dataset_data[1]
    return pathstr, sha256, pathstr

def on_tag_click(evt: gr.SelectData):
    return evt.value

def on_select_image_sha256_change(sha256: str):
    if sha256.strip() == "":
        return {}, ""
    conn = get_database_connection()
    tags = { t[0]: t[1] for t in get_all_tags_for_item_name_confidence(conn, sha256)}
    conn.close()
    # Tags in the format "tag1, tag2, tag3"
    text = ", ".join(tags.keys())
    return tags, text

# We define a dataclass to use as return value for create_image_list which contains all the components we want to expose
@dataclass
class ImageList:
    file_list: gr.Dataset
    image_preview: gr.Image
    tag_text: gr.Textbox
    tag_list: gr.Label
    selected_image_path: gr.Textbox
    selected_image_sha256: gr.Textbox
    btn_open_file: gr.Button
    btn_open_file_explorer: gr.Button
    bookmark: gr.Button
    extra: List[gr.Button]

def create_image_list(bookmarks_state: gr.State = None, extra_actions: List[str] = [], tag_input: gr.Textbox = None):
    with gr.Row():
        with gr.Column(scale=1):
            file_list = gr.Dataset(label="Results", type="values", samples_per_page=10, samples=[], components=["image", "textbox"], scale=1)
        with gr.Column(scale=2):
            image_preview = gr.Image(elem_classes=["listViewImagePreview"], value=None, label="Selected Image")
        with gr.Column(scale=1):
            with gr.Tabs():
                with gr.Tab(label="Tags"):
                    tag_text = gr.Textbox(label="Tags", show_copy_button=True, interactive=False, lines=5)
                with gr.Tab(label="Tags Confidence"):
                    tag_list = gr.Label(label="Tags", show_label=False)
            selected_image_path = gr.Textbox(value="", label="Last Selected Image", show_copy_button=True, interactive=False)
            selected_image_sha256 = gr.Textbox(value="", label="Last Selected Image SHA256", show_copy_button=True, interactive=False, visible=False) # Hidden
            with gr.Row():
                btn_open_file = gr.Button("Open File", interactive=False, scale=3)
                btn_open_file_explorer = gr.Button("Show in Explorer", interactive=False, scale=3)
                bookmark = gr.Button("Bookmark", interactive=False, scale=3, visible=bookmarks_state != None)
                extra: List[gr.Button] = []
                for action in extra_actions:
                    extra.append(gr.Button(action, interactive=False, scale=3))

    def on_selected_image_path_change(path: str):
        nonlocal extra_actions
        interactive = True
        if path.strip() == "":
            interactive = False
            path = None
        updates = path, gr.update(interactive=interactive), gr.update(interactive=interactive), gr.update(interactive=interactive)
        # Add updates to the tuple for extra actions
        for _ in extra_actions:
            updates += (gr.update(interactive=interactive),)
        return updates

    file_list.click(
        fn=on_select_image,
        inputs=[file_list],
        outputs=[selected_image_path, selected_image_sha256]
    )

    selected_image_path.change(
        fn=on_selected_image_path_change,
        inputs=[selected_image_path],
        outputs=[image_preview, btn_open_file, btn_open_file_explorer, bookmark, *extra]
    )

    selected_image_sha256.change(
        fn=on_select_image_sha256_change,
        inputs=[selected_image_sha256],
        outputs=[tag_list, tag_text]
    )

    if tag_input:
        tag_list.select(on_tag_click, None, [tag_input])

    btn_open_file.click(
        fn=open_file,
        inputs=selected_image_path,
    )

    btn_open_file_explorer.click(
        fn=open_in_explorer,
        inputs=selected_image_path,
    )

    if bookmarks_state != None:
        bookmark.click(
            fn=toggle_bookmark,
            inputs=[bookmarks_state, selected_image_sha256, bookmark],
            outputs=[bookmark]
        )
        selected_image_sha256.change(
            fn=on_selected_image_get_bookmark_state,
            inputs=[bookmarks_state, selected_image_sha256],
            outputs=[bookmark]
        )

    return ImageList(
        file_list=file_list,
        image_preview=image_preview,
        tag_text=tag_text,
        tag_list=tag_list,
        selected_image_path=selected_image_path,
        selected_image_sha256=selected_image_sha256,
        btn_open_file=btn_open_file,
        btn_open_file_explorer=btn_open_file_explorer,
        bookmark=bookmark,
        extra=extra
    )