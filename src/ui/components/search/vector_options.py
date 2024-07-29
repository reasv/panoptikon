from typing import List, Tuple

import gradio as gr


def create_vector_search_opts(setters: List[Tuple[str, str]]):
    setter_types = [setter[0] for setter in setters]
    extracted_text_setters = [
        (f"{model_type}|{setter_id}", (model_type, setter_id))
        for model_type, setter_id in setters
        if model_type == "text"
    ]
    clip_setters = [
        setter_id for model_type, setter_id in setters if model_type == "clip"
    ]
    if "clip" in setter_types or "text-embedding" in setter_types:
        with gr.Tab(label="Semantic Search"):
            with gr.Row():
                vec_query_type = gr.Dropdown(
                    choices=[
                        *(
                            [
                                "CLIP Text Query",
                                "CLIP Reverse Image Search",
                            ]
                            if clip_setters
                            else []
                        ),
                        *(
                            ["Text Vector Search"]
                            if "text-embedding" in setter_types
                            else []
                        ),
                    ],
                    label="Search Type",
                    value=(
                        "CLIP Text Query"
                        if clip_setters
                        else "Text Vector Search"
                    ),
                    scale=1,
                )

                @gr.render(inputs=[vec_query_type])
                def render_vec_query_inputs(vec_query_type: str):
                    if vec_query_type == "Text Vector Search":
                        vec_text_search = gr.Textbox(
                            label="Search for similar text extracted from images",
                            value="",
                            show_copy_button=True,
                            scale=2,
                        )
                        vec_targets = gr.Dropdown(
                            choices=extracted_text_setters,
                            interactive=True,
                            label="Restrict query to text from these sources",
                            multiselect=True,
                            scale=2,
                        )
                    elif vec_query_type == "CLIP Text Query":
                        clip_text_search = gr.Textbox(
                            label="Describe the image you are looking for",
                            value="",
                            show_copy_button=True,
                            scale=2,
                        )
                    elif vec_query_type == "CLIP Reverse Image Search":
                        with gr.Accordion(label="Image Upload"):
                            clip_image_search = gr.Image(
                                label="Search for similar images",
                                scale=2,
                                type="numpy",
                            )
                    if vec_query_type in [
                        "CLIP Reverse Image Search",
                        "CLIP Text Query",
                    ]:
                        clip_model = gr.Dropdown(
                            choices=extracted_text_setters,
                            interactive=True,
                            label="Select CLIP model",
                            multiselect=False,
                            scale=1,
                        )