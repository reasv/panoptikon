from dataclasses import dataclass
from typing import List, Tuple

import gradio as gr

from src.db import get_database_connection
from src.db.extracted_text import get_extracted_text_for_item
from src.db.extraction_log import get_existing_setters
from src.types import ExtractedText, FileSearchResult


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
        (f"{model_type}|{setter}", (model_type, setter))
        for model_type, setter in setters_pairs
        if model_type not in ["tags", "clip"]
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
            text_picker: List[Tuple[str, str]] | None,
            texts_state: List[ExtractedText],
        ):
            if text_picker is None:
                return
            for model_type, setter in text_picker:
                text = next(
                    (
                        t
                        for t in texts_state
                        if t.setter_name == setter
                        and t.model_type == model_type
                    ),
                    None,
                )
                if text is not None:
                    with gr.Row():
                        gr.Textbox(
                            label=f"Source: ({setter})",
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
