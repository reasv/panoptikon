from __future__ import annotations

import gradio as gr
import PIL.Image

from src.deepdanbooru import predict, load_labels, load_model
from src.wd_tagger import Predictor, V3_MODELS

def create_model_demo():
    with gr.TabItem(label="Tagging Models"):
        with gr.Tabs():
            with gr.Tab(label="DeepDanbooru"):
                create_dd_UI()
            with gr.Tab(label="WD Taggers"):
                create_wd_tagger_UI()

def create_dd_UI():
    DESCRIPTION = "# [KichangKim/DeepDanbooru](https://github.com/KichangKim/DeepDanbooru)"
    model = None
    labels = None
    def predict_load(image: PIL.Image.Image, score_threshold: float) -> tuple[dict[str, float], dict[str, float], str]:
        nonlocal model, labels
        if model is None:
            model = load_model()
        if labels is None:
            labels = load_labels()
        return predict(image, model, labels, score_threshold)

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

def create_wd_tagger_UI():
    TITLE = "WaifuDiffusion Tagger"
    DESCRIPTION = """
    Demo for the WaifuDiffusion tagger models
    """
    score_slider_step = 0.05
    score_character_threshold = 0.25
    score_general_threshold = 0.25

    predictor = Predictor()

    dropdown_list = V3_MODELS

    with gr.Column():
        gr.Markdown(
            value=f"<h1 style='text-align: center; margin-bottom: 1rem'>{TITLE}</h1>"
        )
        gr.Markdown(value=DESCRIPTION)
        with gr.Row():
            with gr.Column(variant="panel"):
                image = gr.Image(type="pil", image_mode="RGBA", label="Input")
                model_repo = gr.Dropdown(
                    dropdown_list,
                    value=V3_MODELS[0],
                    label="Model",
                )
                with gr.Row():
                    general_thresh = gr.Slider(
                        0,
                        1,
                        step=score_slider_step,
                        value=score_general_threshold,
                        label="General Tags Threshold",
                        scale=3,
                    )
                    general_mcut_enabled = gr.Checkbox(
                        value=False,
                        label="Use MCut threshold",
                        scale=1,
                    )
                with gr.Row():
                    character_thresh = gr.Slider(
                        0,
                        1,
                        step=score_slider_step,
                        value=score_character_threshold,
                        label="Character Tags Threshold",
                        scale=3,
                    )
                    character_mcut_enabled = gr.Checkbox(
                        value=False,
                        label="Use MCut threshold",
                        scale=1,
                    )
                with gr.Row():
                    clear = gr.ClearButton(
                        components=[
                            image,
                            model_repo,
                            general_thresh,
                            general_mcut_enabled,
                            character_thresh,
                            character_mcut_enabled,
                        ],
                        variant="secondary",
                        size="lg",
                    )
                    submit = gr.Button(value="Submit", variant="primary", size="lg")
            with gr.Column(variant="panel"):
                sorted_general_strings = gr.Textbox(label="Output (string)")
                rating = gr.Label(label="Rating")
                character_res = gr.Label(label="Output (characters)")
                general_res = gr.Label(label="Output (tags)")
                clear.add(
                    [
                        sorted_general_strings,
                        rating,
                        character_res,
                        general_res,
                    ]
                )
    def run_predict(image, model_repo, general_thresh, general_mcut_enabled, character_thresh, character_mcut_enabled):
        return predictor.predict(
            image,
            model_repo,
            general_thresh if not general_mcut_enabled else None,
            character_thresh if not character_mcut_enabled else None,
        )
    submit.click(
        fn=run_predict,
        inputs=[
            image,
            model_repo,
            general_thresh,
            general_mcut_enabled,
            character_thresh,
            character_mcut_enabled,
        ],
        outputs=[rating, character_res, general_res, sorted_general_strings],
    )