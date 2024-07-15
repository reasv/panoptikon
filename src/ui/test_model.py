from __future__ import annotations

import gradio as gr
import PIL.Image

from src.data_extractors.wd_tagger import Predictor, V3_MODELS
from src.ui.paddleocr import create_paddleocr_UI
from src.ui.clip import create_CLIP_ui
# from src.ui.whisper_jax import create_whisper_ui

def create_model_demo():
    with gr.TabItem(label="Models"):
        with gr.Tabs():
            with gr.Tab(label="WD Taggers"):
                create_wd_tagger_UI()
            with gr.Tab(label="PaddleOCR"):
                create_paddleocr_UI()
            with gr.Tab(label="CLIP Semantic Search"):
                create_CLIP_ui()
            # with gr.Tab(label="WhisperJAX") as tab:
            #     create_whisper_ui(tab)

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
                    choices=[(c,c) for c in dropdown_list],
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
        rating, character_res, general_res = predictor.predict(
            [image],
            model_repo,
            general_thresh if not general_mcut_enabled else None,
            character_thresh if not character_mcut_enabled else None,
        )[0]
        sorted_general_strings = sorted(
            general_res.items(),
            key=lambda x: x[1],
            reverse=True,
        )

        sorted_general_strings = [x[0] for x in sorted_general_strings]
        sorted_general_strings = (
            ", ".join(sorted_general_strings).replace("(", r"\(").replace(")", r"\)")
        )
        return rating, character_res, general_res, sorted_general_strings

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