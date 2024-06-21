from __future__ import annotations

import gradio as gr
from src.db import get_most_common_tags_frequency, get_database_connection
from src.wd_tagger import V3_MODELS

def get_labels(setters = None):
    conn = get_database_connection()
    tags_character = get_most_common_tags_frequency(conn, namespace="danbooru:character", setters=setters, limit=25)
    tags_general = get_most_common_tags_frequency(conn, namespace="danbooru:general", setters=setters, limit=100)
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
                    include_from_models = gr.Dropdown(label="Only include tags from model(s)", value=[], multiselect=True, choices=V3_MODELS)
                    refresh_button = gr.Button("Update")
                top_tags_general = gr.Label(label="General tags")

    refresh_button.click(
        inputs=[include_from_models],
        fn=get_labels,
        outputs=[top_tags_rating, top_tags_characters, top_tags_general]
    )

    tab.select(
        inputs=[include_from_models],
        fn=get_labels,
        outputs=[top_tags_rating, top_tags_characters, top_tags_general]
    )