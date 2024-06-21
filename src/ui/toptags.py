from __future__ import annotations

import gradio as gr
from src.db import get_most_common_tags_frequency, get_database_connection

def get_labels():
    conn = get_database_connection()
    tags_character = get_most_common_tags_frequency(conn, namespace="danbooru:character", limit=25)
    tags_general = get_most_common_tags_frequency(conn, namespace="danbooru:general", limit=100)
    conn.close()
    if len(tags_general) == 0 or len(tags_character) == 0:
        return {"None": 1}

    labels_character = {tag[1]: tag[3] for tag in tags_character}
    labels_general = {tag[1]: tag[3] for tag in tags_general if not tag[1].startswith("rating:")}
    labels_rating = {tag[1]: tag[3] for tag in tags_general if tag[1].startswith("rating:")}
    return labels_rating, labels_character, labels_general

def create_toptags_UI():
    with gr.TabItem(label="Tag Frequency") as tab:
        with gr.Column(elem_classes="centered-content", scale=0):
            with gr.Row():
                with gr.Column():
                    top_tags_rating = gr.Label(label="Rating tags")
                    top_tags_characters = gr.Label(label="Character tags")
                    refresh_button = gr.Button("Refresh")
                top_tags_general = gr.Label(label="General tags")

    refresh_button.click(fn=get_labels, outputs=[top_tags_rating, top_tags_characters, top_tags_general])
    tab.select(
        fn=get_labels,
        outputs=[top_tags_rating, top_tags_characters, top_tags_general]
    )