import gradio as gr

from src.data_extractors.utils import get_threshold_from_env


def create_tags_opts(namespaces: list[str], tag_setters: list[str]):
    if tag_setters:
        with gr.Tab(label="Tag Filters"):
            with gr.Group():
                with gr.Row():
                    tag_input = gr.Textbox(
                        label="Enter tags separated by commas",
                        value="",
                        show_copy_button=True,
                        scale=3,
                    )
                    min_confidence = gr.Slider(
                        minimum=0.05,
                        maximum=1,
                        value=get_threshold_from_env(),
                        step=0.05,
                        label="Min. Confidence Level for Tags",
                        scale=2,
                    )
                    chosen_tag_setters = gr.Dropdown(
                        label="Only search tags set by model(s)",
                        multiselect=True,
                        choices=tag_setters,
                        value=[],
                        scale=2,
                    )
                    all_setters_required = gr.Checkbox(
                        label="Require each tag to have been set by ALL selected models",
                        scale=1,
                    )
                    tag_namespace_prefixes = gr.Dropdown(
                        label="Tag Namespace Prefixes",
                        choices=namespaces,
                        allow_custom_value=True,
                        multiselect=True,
                        value=None,
                        scale=2,
                    )
