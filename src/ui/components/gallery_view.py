from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List

import gradio as gr

from src.db import get_database_connection
from src.types import FileSearchResult
from src.ui.components.bookmark_folder_selector import (
    create_bookmark_folder_chooser,
)
from src.ui.components.utils import (
    get_item_thumbnail,
    on_selected_image_get_bookmark_state,
    toggle_bookmark,
)
from src.utils import open_file, open_in_explorer

logger = logging.getLogger(__name__)


def on_change_columns_slider(columns_slider: int):
    return gr.update(columns=columns_slider)


def on_files_change(files: List[FileSearchResult]):
    conn = get_database_connection(write_lock=False)
    image_list = [
        (get_item_thumbnail(conn, file, True), file.path) for file in files
    ]
    conn.close()
    logger.debug(f"Received {len(image_list)} images")
    return (
        (gr.update(value=image_list), [files[0]])
        if len(image_list) > 0
        else ([], [])
    )


def on_select_image(
    evt: gr.SelectData,
    files: List[FileSearchResult],
    selected_files: List[FileSearchResult],
):
    logger.debug(f"Selected image index: {evt.index} in gallery")
    if isinstance(evt.index, int):
        image_index = evt.index
    else:
        image_index = int(evt.index[0])

    # Check if index is valid
    if image_index < 0 or image_index >= len(files):
        # Don't update selected_files if index is invalid
        return selected_files

    image = files[image_index]
    if len(selected_files) > 0:
        selected_files[0] = image
    else:
        selected_files.append(image)
    return selected_files


def on_selected_image_change_extra_actions(extra_actions: List[str]):
    def on_selected_image_path_change(
        selected_files: List[FileSearchResult],
        files: List[FileSearchResult],
        selected_image_path: str,
    ):
        nonlocal extra_actions

        if len(selected_files) == 0:
            interactive = False
            selected_file_index = 0
            path = ""
        else:
            interactive = True
            selected_file = selected_files[0]
            path = selected_file.path
            selected_file_index = files.index(selected_file)
            if path.strip() == "":
                interactive = False
        # gallery_update = (
        #     gr.update(selected_index=selected_file_index)
        #     if len(files) > 0
        #     else gr.update(value=[])
        # )

        gallery_update = gr.update() if len(files) > 0 else gr.update(value=[])

        # Do not update if the path is the same
        if path == selected_image_path:
            gallery_update = gr.update()
            path = gr.update()

        # Do not update gallery if no files are selected
        if len(selected_files) == 0:
            gallery_update = gr.update()

        updates = (
            path,
            gallery_update,
            gr.update(interactive=interactive),
            gr.update(interactive=interactive),
            gr.update(interactive=interactive),
        )
        # Add updates to the tuple for extra actions
        for _ in extra_actions:
            updates += (gr.update(interactive=interactive),)
        return updates

    return on_selected_image_path_change


# We define a dataclass to use as return value for create_gallery_view which contains all the components we want to expose
@dataclass
class GalleryView:
    columns_slider: gr.Slider
    selected_image_path: gr.Textbox
    open_file_button: gr.Button
    open_file_explorer: gr.Button
    bookmark: gr.Button
    extra: List[gr.Button]
    image_output: gr.Gallery


def create_gallery_view(
    selected_files: gr.State,
    files: gr.State,
    parent_tab: gr.TabItem | None = None,
    bookmarks_namespace: gr.State | None = None,
    extra_actions: List[str] = [],
):
    with gr.Row():
        columns_slider = gr.Slider(
            minimum=1, maximum=15, value=5, step=1, label="Number of columns"
        )
        selected_image_path = gr.Textbox(
            value="",
            label="Last Selected Image",
            show_copy_button=True,
            interactive=False,
        )
        open_file_button = gr.Button("Open File", interactive=False)
        open_file_explorer = gr.Button("Show in Explorer", interactive=False)
        if bookmarks_namespace != None:
            create_bookmark_folder_chooser(
                parent_tab=parent_tab, bookmarks_namespace=bookmarks_namespace
            )
        bookmark = gr.Button(
            "Bookmark", interactive=False, visible=bookmarks_namespace != None
        )
        extra: List[gr.Button] = []
        for action in extra_actions:
            extra.append(gr.Button(action, interactive=False))
    image_output = gr.Gallery(
        label="Results", elem_classes=["gallery-view"], columns=5, scale=2
    )

    files.change(
        fn=on_files_change,
        inputs=[files],
        outputs=[image_output, selected_files],
    )

    selected_files.change(
        fn=on_selected_image_change_extra_actions(extra_actions),
        inputs=[selected_files, files, selected_image_path],
        outputs=[
            selected_image_path,
            image_output,
            open_file_button,
            open_file_explorer,
            bookmark,
            *extra,
        ],
    )

    image_output.select(
        fn=on_select_image,
        inputs=[files, selected_files],
        outputs=[selected_files],
    )

    columns_slider.release(
        fn=on_change_columns_slider,
        inputs=[columns_slider],
        outputs=[image_output],
    )

    open_file_button.click(
        fn=open_file,
        inputs=selected_image_path,
    )

    open_file_explorer.click(
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
        if parent_tab != None:
            parent_tab.select(
                fn=on_selected_image_get_bookmark_state,
                inputs=[bookmarks_namespace, selected_files],
                outputs=[bookmark],
            )

    return GalleryView(
        columns_slider=columns_slider,
        selected_image_path=selected_image_path,
        open_file_button=open_file_button,
        open_file_explorer=open_file_explorer,
        bookmark=bookmark,
        extra=extra,
        image_output=image_output,
    )
