from __future__ import annotations

import gradio as gr
import numpy as np

from src.data_extractors.image_embeddings import (
    CLIPEmbedder,
    search_item_image_embeddings,
)
from src.data_extractors.utils import get_chromadb_client
from src.db import get_database_connection
from src.ui.components.multi_view import create_multiview


def create_semantic_search_UI(
    select_history: gr.State | None = None,
    bookmarks_namespace: gr.State | None = None,
):
    with gr.TabItem(label="Semantic Search") as search_tab:
        with gr.Column(elem_classes="centered-content", scale=0):
            with gr.Row():
                with gr.Column():
                    with gr.Tabs():
                        with gr.Tab(label="Search by Text"):
                            with gr.Row():
                                search_text = gr.Textbox(
                                    label="Search for images",
                                    placeholder="Search for images",
                                    lines=1,
                                    scale=3,
                                )
                                submit_button = gr.Button("Search", scale=0)
                        with gr.Tab(label="Search by Image"):
                            search_image = gr.Image(
                                label="Search for similar images",
                                scale=1,
                                type="numpy",
                            )
                            submit_button_image = gr.Button("Search", scale=0)
                with gr.Column():
                    n_results = gr.Slider(
                        label="Number of results",
                        value=10,
                        minimum=1,
                        maximum=500,
                        step=1,
                        scale=1,
                    )
                    unload_model = gr.Button(
                        "Unload Model", scale=0, interactive=False
                    )

        multiview = create_multiview(
            select_history=select_history,
            bookmarks_namespace=bookmarks_namespace,
        )

    embedder = CLIPEmbedder(
        model_name="ViT-H-14-378-quickgelu", pretrained="dfn5b"
    )

    def items_semantic_search(
        search_text: str | None, search_image: np.ndarray | None, n_results: int
    ):
        conn = get_database_connection()
        cdb = get_chromadb_client()
        files, scores = search_item_image_embeddings(
            conn,
            cdb,
            embedder,
            image_query=search_image,
            text_query=search_text,
            limit=n_results,
        )
        return files, gr.update(interactive=True)

    def search_by_image(search_image: np.ndarray, n_results: int):
        return items_semantic_search(None, search_image, n_results)

    def search_by_text(search_text: str, n_results: int):
        return items_semantic_search(search_text, None, n_results)

    def on_unload_model():
        embedder.unload_model()
        return gr.update(interactive=False)

    submit_button.click(
        fn=search_by_text,
        inputs=[search_text, n_results],
        outputs=[multiview.files, unload_model],
    )

    submit_button_image.click(
        fn=search_by_image,
        inputs=[search_image, n_results],
        outputs=[multiview.files, unload_model],
    )

    unload_model.click(fn=on_unload_model, outputs=[unload_model])
