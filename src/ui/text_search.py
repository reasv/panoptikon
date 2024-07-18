from __future__ import annotations

import gradio as gr

from src.data_extractors.text_embeddings import search_item_text
from src.data_extractors.utils import get_chromadb_client
from src.db import get_database_connection
from src.ui.components.multi_view import create_multiview


def create_semantic_text_search_UI(
    select_history: gr.State | None = None,
    bookmarks_namespace: gr.State | None = None,
):
    with gr.TabItem(label="Semantic Text Search") as search_tab:
        with gr.Column(elem_classes="centered-content", scale=0):
            with gr.Row():
                with gr.Column():
                    with gr.Row():
                        search_text = gr.Textbox(
                            label="Search for images",
                            placeholder="Search for images",
                            lines=1,
                            scale=3,
                        )
                        submit_button = gr.Button("Search", scale=0)
                with gr.Column():
                    n_results = gr.Slider(
                        label="Number of results",
                        value=10,
                        minimum=1,
                        maximum=500,
                        step=1,
                        scale=1,
                    )
                    enable_semantic_search = gr.Checkbox(
                        label="Semantic Search",
                        value=True,
                        scale=1,
                        interactive=False,
                    )
                    enable_full_text_search = gr.Checkbox(
                        label="Require text to include query",
                        value=False,
                        scale=1,
                    )

        multiview = create_multiview(
            select_history=select_history,
            bookmarks_namespace=bookmarks_namespace,
        )

    def search_by_text(
        search_text: str,
        n_results: int,
        semantic_search: bool = False,
        full_text_search: bool = False,
    ):
        conn = get_database_connection()
        cdb = get_chromadb_client()
        files, scores = search_item_text(
            conn,
            cdb,
            text_query=search_text,
            limit=n_results,
            semantic_search=semantic_search,
            full_text_search=full_text_search,
        )
        return files

    submit_button.click(
        fn=search_by_text,
        inputs=[
            search_text,
            n_results,
            enable_semantic_search,
            enable_full_text_search,
        ],
        outputs=[multiview.files],
    )
