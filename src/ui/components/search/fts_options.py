from typing import List, Tuple

import gradio as gr


def create_fts_options(text_setters: List[Tuple[str, Tuple[str, str]]]):
    if text_setters:
        text_setters.extend(
            [
                ("Full Path", ("path", "path")),
                ("Filename", ("path", "filename")),
            ]
        )
        with gr.Tab(label="Full Text Search"):
            with gr.Row():
                any_text_search = gr.Textbox(
                    label="Match text in any field (supports SQLite MATCH grammar)",
                    value="",
                    show_copy_button=True,
                    scale=2,
                )
                restrict_to_query_types = gr.Dropdown(
                    choices=text_setters,  # type: ignore
                    interactive=True,
                    label="(Optional) Restrict query to these targets",
                    multiselect=True,
                    scale=1,
                )
