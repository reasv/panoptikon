from __future__ import annotations

import gradio as gr
from src.db import get_most_common_tags_frequency, get_database_connection

def get_labels():
    conn = get_database_connection()
    tags = get_most_common_tags_frequency(conn, limit=100)
    conn.close()
    if len(tags) == 0:
        return {"None": 1}

    labels = {tag[1]: tag[3] for tag in tags}
    return labels

def create_toptags_UI():
    with gr.TabItem(label="Tag Frequency"):
        with gr.Column(elem_classes="centered-content", scale=0):
            top_tags = gr.Label(value=get_labels, label="Percentages are calculated on items that have tags")
            refresh_button = gr.Button("Refresh")
            refresh_button.click(fn=get_labels, outputs=top_tags)