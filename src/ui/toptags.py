from __future__ import annotations

import gradio as gr

import src.data_extractors.models as models
from src.db import get_database_connection
from src.db.tagstats import get_most_common_tags_frequency


def get_labels(setters=None, confidence_threshold=None):
    if confidence_threshold == 0.1:
        confidence_threshold = None
    conn = get_database_connection(write_lock=False)
    tags_character = get_most_common_tags_frequency(
        conn,
        namespace="danbooru:character",
        setters=setters,
        confidence_threshold=confidence_threshold,
        limit=25,
    )
    tags_general = get_most_common_tags_frequency(
        conn,
        namespace="danbooru:general",
        setters=setters,
        confidence_threshold=confidence_threshold,
        limit=100,
    )
    tags_rating = get_most_common_tags_frequency(
        conn,
        namespace="danbooru:rating",
        setters=setters,
        confidence_threshold=confidence_threshold,
        limit=5,
    )
    conn.close()
    if len(tags_general) == 0 or len(tags_character) == 0:
        return {"None": 1}, {"None": 1}, {"None": 1}

    labels_character = {tag[1]: tag[3] for tag in tags_character}
    labels_general = {tag[1]: tag[3] for tag in tags_general}
    labels_rating = {tag[1]: tag[3] for tag in tags_rating}
    return labels_rating, labels_character, labels_general


def create_toptags_UI():
    with gr.TabItem(label="Tag Frequency") as tab:
        with gr.Column(elem_classes="centered-content", scale=0):
            with gr.Row():
                with gr.Column():
                    top_tags_rating = gr.Label(label="Rating tags")
                    top_tags_characters = gr.Label(label="Character tags")
                    include_from_models = gr.Dropdown(
                        label="Only include tags from model(s)",
                        value=[],
                        multiselect=True,
                        choices=[
                            (name, name)
                            for name in models.TagsModel.available_models()
                        ],
                    )
                    confidence_threshold = gr.Slider(
                        label="Confidence threshold",
                        minimum=0.05,
                        maximum=1,
                        step=0.01,
                        value=0.1,
                    )
                    refresh_button = gr.Button("Update")
                top_tags_general = gr.Label(label="General tags")

    refresh_button.click(
        inputs=[include_from_models, confidence_threshold],
        fn=get_labels,
        outputs=[top_tags_rating, top_tags_characters, top_tags_general],
    )

    tab.select(
        inputs=[include_from_models, confidence_threshold],
        fn=get_labels,
        outputs=[top_tags_rating, top_tags_characters, top_tags_general],
    )
