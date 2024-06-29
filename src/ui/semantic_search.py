from __future__ import annotations

import gradio as gr

from src.db import get_database_connection
from src.ui.components.multi_view import create_multiview
from src.image_embeddings import CLIPEmbedder, search_item_image_embeddings, get_chromadb_client

def create_semantic_search_UI(
        select_history: gr.State | None = None,
        bookmarks_namespace: gr.State | None = None
    ):
    with gr.TabItem(label="Semantic Search") as search_tab:
        with gr.Column(elem_classes="centered-content", scale=0):
            with gr.Row():
                search_text = gr.Textbox(
                    label="Search for images",
                    placeholder="Search for images",
                    lines=1,
                    scale=3
                )
                n_results = gr.Slider(
                    label="Number of results",
                    value=10,
                    minimum=1,
                    maximum=500,
                    step=1,
                    scale=1
                )
                submit_button = gr.Button("Search", scale=0)
    
        multiview = create_multiview(
            select_history=select_history,
            bookmarks_namespace=bookmarks_namespace
        )
    embedder = CLIPEmbedder(model_name="ViT-H-14-378-quickgelu", pretrained="dfn5b")

    def items_semantic_search(search_text: str, n_results: int):
        conn = get_database_connection()
        cdb = get_chromadb_client()
        files, scores = search_item_image_embeddings(
            conn,
            cdb,
            embedder,
            image_query=None,
            text_query=search_text,
            limit=n_results,
        )
        return files

    submit_button.click(
        fn=items_semantic_search,
        inputs=[search_text, n_results],
        outputs=[multiview.files]
    )

