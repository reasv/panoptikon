from __future__ import annotations

from dataclasses import dataclass
from typing import List

import gradio as gr

from src.db import get_database_connection
from src.db.tags import get_all_tags_for_item_name_confidence
from src.types import FileSearchResult
from src.ui.components.bookmark_folder_selector import (
    create_bookmark_folder_chooser,
)
from src.ui.components.text_viewer import create_text_viewer
from src.ui.components.utils import (
    get_thumbnail,
    on_selected_image_get_bookmark_state,
    toggle_bookmark,
)
from src.utils import open_file, open_in_explorer


def on_files_change(files: List[FileSearchResult]):
    image_list = [[get_thumbnail(file, False), file.path] for file in files]
    print(f"Received {len(image_list)} images")
    return gr.update(samples=image_list), (
        [] if len(image_list) == 0 else [files[0]]
    )


def on_selected_files_change_extra_actions(extra_actions: List[str]):
    def on_selected_files_change(
        selected_files: List[FileSearchResult], selected_image_path: str
    ):
        nonlocal extra_actions
        if len(selected_files) == 0:
            interactive = False
            path = None
            tags = None
            text = None
            updates = (
                None,
                None,
                None,
                None,
                gr.update(interactive=interactive),
                gr.update(interactive=interactive),
                gr.update(interactive=interactive),
            )
        else:
            selected_file = selected_files[0]
            interactive = True
            sha256 = selected_file.sha256
            path = selected_file.path
            thumbnail = get_thumbnail(selected_file, True)
            if path != selected_image_path:
                conn = get_database_connection(write_lock=False)
                tags = {
                    t[0]: t[1]
                    for t in get_all_tags_for_item_name_confidence(conn, sha256)
                }
                conn.close()
                # Tags in the format "tag1 tag2 tag3"
                text = ", ".join(tags.keys())

                if path.strip() == "":
                    interactive = False
                    path = None

                updates = (
                    tags,
                    text,
                    path,
                    thumbnail,
                    gr.update(interactive=interactive),
                    gr.update(interactive=interactive),
                    gr.update(interactive=interactive),
                )
            else:
                updates = (
                    gr.update(),
                    gr.update(),
                    gr.update(),
                    gr.update(),
                    gr.update(),
                    gr.update(),
                    gr.update(),
                )
        # Add updates to the tuple for extra actions
        for _ in extra_actions:
            updates += (gr.update(interactive=interactive),)

        return updates

    return on_selected_files_change


def on_select_image(
    evt: int,
    files: List[FileSearchResult],
    selected_files: List[FileSearchResult],
):
    print(f"Selected image index: {evt} in file list")
    image_index: int = evt
    image = files[image_index]
    if len(selected_files) > 0:
        selected_files[0] = image
    else:
        selected_files.append(image)
    return selected_files


# We define a dataclass to use as return value for create_image_list
# which contains all the components we want to expose
@dataclass
class ImageList:
    file_list: gr.Dataset
    image_preview: gr.Image
    tag_text: gr.Textbox
    tag_list: gr.Label
    selected_image_path: gr.Textbox
    btn_open_file: gr.Button
    btn_open_file_explorer: gr.Button
    bookmark: gr.Button
    extra: List[gr.Button]


def create_image_list(
    selected_files: gr.State,
    files: gr.State,
    parent_tab: gr.TabItem | None = None,
    bookmarks_namespace: gr.State | None = None,
    extra_actions: List[str] = [],
):
    with gr.Row():
        with gr.Column(scale=1):
            file_list = gr.Dataset(
                label="Results",
                type="index",
                samples_per_page=10,
                samples=[],
                components=["image", "textbox"],
                scale=1,
            )
        with gr.Column(scale=2):
            image_preview = gr.Image(
                elem_classes=["listViewImagePreview"],
                value=None,
                label="Selected Image",
            )
        with gr.Column(scale=1):
            with gr.Tabs():
                with gr.Tab(label="Tags"):
                    tag_text = gr.Textbox(
                        label="Tags",
                        show_copy_button=True,
                        interactive=False,
                        lines=5,
                    )
                with gr.Tab(label="Tags Confidence"):
                    tag_list = gr.Label(label="Tags", show_label=False)
            selected_image_path = gr.Textbox(
                value="",
                label="Last Selected Image",
                show_copy_button=True,
                interactive=False,
            )

            with gr.Row():
                btn_open_file = gr.Button(
                    "Open File", interactive=False, scale=3
                )
                btn_open_file_explorer = gr.Button(
                    "Show in Explorer", interactive=False, scale=3
                )
            with gr.Row():
                if bookmarks_namespace != None:
                    create_bookmark_folder_chooser(
                        parent_tab=parent_tab,
                        bookmarks_namespace=bookmarks_namespace,
                    )
                bookmark = gr.Button(
                    "Bookmark",
                    interactive=False,
                    scale=1,
                    visible=bookmarks_namespace != None,
                )
            with gr.Row():
                extra: List[gr.Button] = []
                for action in extra_actions:
                    extra.append(gr.Button(action, interactive=False, scale=3))
            with gr.Row():
                create_text_viewer(selected_files)

    files.change(
        fn=on_files_change, inputs=[files], outputs=[file_list, selected_files]
    )

    file_list.click(
        fn=on_select_image,
        inputs=[file_list, files, selected_files],
        outputs=[selected_files],
    )

    selected_files.change(
        fn=on_selected_files_change_extra_actions(extra_actions),
        inputs=[selected_files, selected_image_path],
        outputs=[
            tag_list,
            tag_text,
            selected_image_path,
            image_preview,
            btn_open_file,
            btn_open_file_explorer,
            bookmark,
            *extra,
        ],
    )

    btn_open_file.click(
        fn=open_file,
        inputs=selected_image_path,
    )

    btn_open_file_explorer.click(
        fn=open_in_explorer,
        inputs=selected_image_path,
    )

    if bookmarks_namespace != None:
        bookmark.click(
            fn=toggle_bookmark,
            inputs=[bookmarks_namespace, selected_files, bookmark],
            outputs=[bookmark],
        )
        selected_files.change(
            fn=on_selected_image_get_bookmark_state,
            inputs=[bookmarks_namespace, selected_files],
            outputs=[bookmark],
        )
        bookmarks_namespace.change(
            fn=on_selected_image_get_bookmark_state,
            inputs=[bookmarks_namespace, selected_files],
            outputs=[bookmark],
        )
        if parent_tab is not None:
            parent_tab.select(
                fn=on_selected_image_get_bookmark_state,
                inputs=[bookmarks_namespace, selected_files],
                outputs=[bookmark],
            )

    return ImageList(
        file_list=file_list,
        image_preview=image_preview,
        tag_text=tag_text,
        tag_list=tag_list,
        selected_image_path=selected_image_path,
        btn_open_file=btn_open_file,
        btn_open_file_explorer=btn_open_file_explorer,
        bookmark=bookmark,
        extra=extra,
    )
