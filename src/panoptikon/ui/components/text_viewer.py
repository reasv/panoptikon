from dataclasses import dataclass
from typing import List

import gradio as gr

from panoptikon.db import get_database_connection
from panoptikon.db.extracted_text import get_extracted_text_for_item
from panoptikon.db.extraction_log import get_existing_setters
from panoptikon.types import ExtractedText, FileSearchResult


def on_item_change(selected_files: List[FileSearchResult]):
    if len(selected_files) == 0:
        return [], gr.update(choices=[]), gr.update(value="", visible=False)

    selected_file = selected_files[0]
    conn = get_database_connection(write_lock=False)
    extracted_texts = get_extracted_text_for_item(
        conn, item_sha256=selected_file.sha256
    )
    setters_pairs = get_existing_setters(conn)
    choices = [
        setter for data_type, setter in setters_pairs if data_type == "text"
    ]
    conn.close()
    return extracted_texts, gr.update(choices=choices)


@dataclass
class TextViewer:
    texts: gr.State
    text_picker: gr.Dropdown


def create_text_viewer(selected_items: gr.State):
    with gr.Column():
        with gr.Row():
            texts_state = gr.State([])
            text_picker = gr.Dropdown(
                type="value",
                choices=[],
                interactive=True,
                multiselect=True,
                label="View Text Extracted by Model(s)",
            )

        @gr.render(inputs=[text_picker, texts_state])
        def show_text(
            text_picker: List[str] | None,
            texts_state: List[ExtractedText],
        ):
            if text_picker is None:
                return
            for text in texts_state:
                if text.setter_name in text_picker:
                    with gr.Row():
                        gr.Textbox(
                            label=f"Source: {text.setter_name}, Language: {text.language}, Confidence: {text.confidence}",
                            interactive=False,
                            lines=4,
                            value=text.text,
                        )

    selected_items.change(
        fn=on_item_change,
        inputs=[selected_items],
        outputs=[texts_state, text_picker],
    )

    return TextViewer(
        texts=texts_state,
        text_picker=text_picker,
    )
