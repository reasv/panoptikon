from __future__ import annotations

import gradio as gr
from src.db import get_most_common_tags_frequency, get_database_connection

def get_labels():
    conn = get_database_connection()
    tags = get_most_common_tags_frequency(conn, limit=100)
    conn.close()
    if len(tags) == 0:
        return {"None": 1}

    labels_character = {tag[1]: tag[3] for tag in tags if tag[0] == "danbooru:character"}
    labels_general = {tag[1]: tag[3] for tag in tags if tag[0] != "danbooru:character"}
    labels_rating = {tag[1]: tag[3] for tag in tags if tag[1].startswith("rating:")}
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