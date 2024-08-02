import gradio as gr

from src.ui.components.rule_builder import create_rule_builder_UI


def create_rule_config_UI(app: gr.Blocks):
    with gr.Tab(label="Rule Configuration") as tab:
        create_rule_builder_UI(app, tab)
