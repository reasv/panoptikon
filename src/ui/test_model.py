from __future__ import annotations

import gradio as gr
import PIL.Image

from src.tags import predict, load_labels, load_model

DESCRIPTION = "# [KichangKim/DeepDanbooru](https://github.com/KichangKim/DeepDanbooru)"

def create_dd_UI():
    model = None
    labels = None
    def predict_load(image: PIL.Image.Image, score_threshold: float) -> tuple[dict[str, float], dict[str, float], str]:
        nonlocal model, labels
        if model is None:
            model = load_model()
        if labels is None:
            labels = load_labels()
        return predict(image, score_threshold, model, labels)

    with gr.Column():
        gr.Markdown(DESCRIPTION)
        gr.Markdown("## Test Tagging Model")
        with gr.Row():
            with gr.Column():
                image = gr.Image(label="Input", type="pil")
                score_threshold = gr.Slider(label="Score threshold", minimum=0, maximum=1, step=0.05, value=0.3)
                run_button = gr.Button("Run")
            with gr.Column():
                with gr.Tabs():
                    with gr.Tab(label="Output"):
                        result = gr.Label(label="Output", show_label=False)
                    with gr.Tab(label="JSON"):
                        result_json = gr.JSON(label="JSON output", show_label=False)
                    with gr.Tab(label="Text"):
                        result_text = gr.Text(label="Text output", show_label=False, lines=5)

        run_button.click(
            fn=predict_load,
            inputs=[image, score_threshold],
            outputs=[result, result_json, result_text],
            api_name="predict",
        )