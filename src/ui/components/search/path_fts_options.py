import gradio as gr


def create_path_fts_opts():
    with gr.Tab(label="MATCH Filename/Path"):
        with gr.Row():
            path_search = gr.Textbox(
                label="MATCH query on filename or path",
                value="",
                show_copy_button=True,
                scale=2,
            )
            search_path_in = gr.Radio(
                choices=[
                    ("Full Path", "full_path"),
                    ("Filename", "filename"),
                ],
                interactive=True,
                label="Match",
                value="full_path",
                scale=1,
            )
            path_order_by_rank = gr.Checkbox(
                label="Order results by relevance if this query is present",
                interactive=True,
                value=True,
                scale=1,
            )
