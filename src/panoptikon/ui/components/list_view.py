from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List

import gradio as gr

from panoptikon.db import get_database_connection
from panoptikon.db.embeddings import find_similar_items
from panoptikon.db.extraction_log import get_existing_setters
from panoptikon.db.tags import get_all_tags_for_item
from panoptikon.types import FileSearchResult
from panoptikon.ui.components.bookmark_folder_selector import (
    create_bookmark_folder_chooser,
)
from panoptikon.ui.components.text_viewer import create_text_viewer
from panoptikon.ui.components.utils import (
    get_item_thumbnail,
    on_selected_image_get_bookmark_state,
    toggle_bookmark,
)
from panoptikon.utils import open_file, open_in_explorer

logger = logging.getLogger(__name__)


def on_files_change(files: List[FileSearchResult]):
    conn = get_database_connection(write_lock=False)
    image_list = [
        [get_item_thumbnail(conn, file, False), file.path] for file in files
    ]
    conn.close()
    logger.debug(f"Received {len(image_list)} images")
    return gr.update(samples=image_list), (
        [] if len(image_list) == 0 else [files[0]]
    )


def get_tag_data(conn, sha256):
    rating_tags = {}
    character_tags = {}
    general_tags = {}
    tags_tuples = get_all_tags_for_item(conn, sha256)
    for namespace, name, confidence, setter_name in tags_tuples:
        if namespace.endswith("rating"):
            if rating_tags.get(name, 0) < confidence:
                rating_tags[name] = confidence
        elif namespace.endswith("character"):
            if character_tags.get(name, 0) < confidence:
                character_tags[name] = confidence
        else:
            if general_tags.get(name, 0) < confidence:
                general_tags[name] = confidence
    text = ", ".join(
        list(rating_tags.keys())
        + list(character_tags.keys())
        + list(general_tags.keys())
    )
    return rating_tags, character_tags, general_tags, text


def on_selected_files_change_extra_actions(extra_actions: List[str]):
    def on_selected_files_change(
        selected_files: List[FileSearchResult], selected_image_path: str
    ):
        nonlocal extra_actions
        if len(selected_files) == 0:
            interactive = False
            path = None
            text = None
            updates = (
                gr.update(value=None, visible=False),
                gr.update(value=None, visible=False),
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
            conn = get_database_connection(write_lock=False)
            thumbnail = get_item_thumbnail(conn, selected_file, True)
            if path != selected_image_path:

                rating_tags, character_tags, general_tags, text = get_tag_data(
                    conn, sha256
                )

                if path.strip() == "":
                    interactive = False
                    path = None

                updates = (
                    gr.update(value=rating_tags, visible=rating_tags != {}),
                    gr.update(
                        value=character_tags, visible=character_tags != {}
                    ),
                    general_tags,
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
                    gr.update(),
                    gr.update(),
                )
            conn.close()
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
    logger.debug(f"Selected image index: {evt} in file list")
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
                    max_labels = gr.Slider(
                        label="Display Top N (0 for max)",
                        minimum=0,
                        maximum=100,
                        step=1,
                        value=5,
                        interactive=True,
                    )
                    tag_rating = gr.Label(
                        label="Rating", show_label=True, visible=False
                    )
                    tag_characters = gr.Label(
                        label="Characters", show_label=True, visible=False
                    )
                    tag_list = gr.Label(
                        label="Tags", show_label=False, num_top_classes=5
                    )

                    def on_max_labels_change(
                        value: int,
                        selected: List[FileSearchResult],
                    ):
                        n_top = value if value > 0 else 999
                        conn = get_database_connection(write_lock=False)
                        if len(selected) > 0:
                            rating_tags, character_tags, general_tags, text = (
                                get_tag_data(conn, selected[0].sha256)
                            )
                            return {
                                tag_rating: gr.update(
                                    value=rating_tags,
                                    num_top_classes=n_top,
                                    visible=rating_tags != {},
                                ),
                                tag_characters: gr.update(
                                    value=character_tags,
                                    num_top_classes=n_top,
                                    visible=character_tags != {},
                                ),
                                tag_list: gr.update(
                                    value=general_tags, num_top_classes=n_top
                                ),
                            }
                        else:
                            return {
                                tag_rating: gr.update(
                                    value=None, num_top_classes=n_top
                                ),
                                tag_characters: gr.update(
                                    value=None, num_top_classes=n_top
                                ),
                                tag_list: gr.update(
                                    value=None, num_top_classes=n_top
                                ),
                            }

                    max_labels.release(
                        fn=on_max_labels_change,
                        inputs=[
                            max_labels,
                            selected_files,
                        ],
                        outputs=[tag_list, tag_rating, tag_characters],
                    )
                with gr.Tab(label="Similar Items") as tab_similar_images:
                    embedding_model = gr.Dropdown(
                        label="Embedding Model (Similarity)",
                        choices=[],
                        multiselect=False,
                        interactive=True,
                    )

                    def get_embedding_models():
                        conn = get_database_connection(write_lock=False)
                        setters = get_existing_setters(conn)
                        conn.close()
                        return gr.Dropdown(
                            choices=[
                                setter
                                for type, setter in setters
                                if type in ["clip", "text-embedding"]
                            ]
                        )

                    tab_similar_images.select(
                        fn=get_embedding_models,
                        outputs=[embedding_model],
                    )

                    max_results = gr.Slider(
                        label="Max Results",
                        minimum=1,
                        maximum=100,
                        step=1,
                        value=5,
                        interactive=True,
                    )

                    @gr.render(
                        triggers=[
                            selected_files.change,
                            embedding_model.select,
                            max_results.release,
                        ],
                        inputs=[selected_files, embedding_model, max_results],
                    )
                    def render_similar_items(
                        selected_files: List[FileSearchResult],
                        embedding_model: str,
                        max_results: int,
                    ):
                        if len(selected_files) == 0 or not embedding_model:
                            return
                        selected_file = selected_files[0]
                        conn = get_database_connection(write_lock=False)
                        similar_items = find_similar_items(
                            conn,
                            selected_file.sha256,
                            setter_name=embedding_model,
                            limit=max_results,
                        )

                        for result in similar_items:
                            res_img = gr.Image(
                                key=result.sha256,
                                value=result.path,
                                label=result.path,
                            )
                            open_in_fm = gr.Button(
                                "Open in File Manager",
                                key=f"open_in_fm_{result.sha256}",
                                interactive=True,
                                scale=3,
                            )

                            @open_in_fm.click
                            def open_in_file_manager():
                                open_in_explorer(result.path)

                        conn.close()

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
            tag_rating,
            tag_characters,
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
