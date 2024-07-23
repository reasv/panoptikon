from dataclasses import dataclass
from typing import List, Tuple

import gradio as gr

from src.data_extractors.utils import ExtractedText
from src.db import (
    FileSearchResult,
    get_database_connection,
    get_existing_type_setter_pairs,
    get_extracted_text_for_item,
)


def on_text_picker_change(choice: Tuple[str, str], texts: List[ExtractedText]):
    model_type, setter = choice
    text = next(
        (t for t in texts if t.setter == setter and t.model_type == model_type),
        None,
    )
    if text is not None:
        return gr.update(value=text.text, visible=True)
    return gr.update(value="", visible=False)


def on_item_change(
    selected_files: List[FileSearchResult], chosen_text_setter: Tuple[str, str]
):
    if len(selected_files) == 0:
        return gr.update(choices=[]), gr.update(value="", visible=False)

    selected_file = selected_files[0]
    conn = get_database_connection(force_readonly=True)
    extracted_texts = get_extracted_text_for_item(
        conn, item_sha256=selected_file.sha256
    )
    setters_pairs = get_existing_type_setter_pairs(conn)
    choices = [
        (f"{model_type}|{setter}", (model_type, setter))
        for model_type, setter in setters_pairs
        if model_type not in ["tags", "clip"]
    ]
    conn.close()
    return (
        extracted_texts,
        gr.update(choices=choices),
        on_text_picker_change(chosen_text_setter, extracted_texts),
    )


@dataclass
class TextViewer:
    texts: gr.State
    text_picker: gr.Dropdown
    extracted_text: gr.Textbox


def create_text_viewer(selected_items: gr.State):
    texts_state = gr.State([])
    text_picker = gr.Dropdown(
        choices=[], label="View Text Extracted by Model", value=None
    )
    extracted_text = gr.Textbox(
        label="Extracted Text",
        interactive=False,
        lines=5,
        visible=False,
    )

    text_picker.select(
        fn=on_text_picker_change,
        inputs=[text_picker, texts_state],
        outputs=[extracted_text],
    )

    selected_items.change(
        fn=on_item_change,
        inputs=[selected_items, text_picker],
        outputs=[texts_state, text_picker, extracted_text],
    )

    return TextViewer(
        texts=texts_state,
        text_picker=text_picker,
        extracted_text=extracted_text,
    )
