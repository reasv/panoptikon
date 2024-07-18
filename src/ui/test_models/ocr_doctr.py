from typing import List, Sequence

import gradio as gr
import numpy as np
import torch
from PIL import Image

from src.utils import pil_ensure_rgb


def create_doctr_UI():
    DESCRIPTION = """
    Run OCR on an image using the Doctr library.
    """
    doctr_model = None
    with gr.Column():
        gr.Markdown(
            value=f"<h1 style='text-align: center; margin-bottom: 1rem'>PaddleOCR</h1>"
        )
        gr.Markdown(value=DESCRIPTION)
        with gr.Row():
            with gr.Column(variant="panel"):
                image = gr.Image(type="pil", image_mode="RGBA", label="Input")
                language = gr.Dropdown(
                    ["en", "ch", "fr", "german", "korean", "japan"],
                    value="en",
                    label="Language",
                )
                with gr.Row():
                    clear = gr.ClearButton(
                        components=[image],
                        variant="secondary",
                        size="lg",
                    )
                    submit = gr.Button(
                        value="Submit", variant="primary", size="lg"
                    )
            with gr.Column(variant="panel"):
                text = gr.Textbox(label="Output")
                clear.add([text])

    def process_batch(model, batch: Sequence[np.ndarray]) -> List[str]:
        result = model(batch)
        files_texts: List[str] = []
        for page in result.pages:
            file_text = ""
            for block in page.blocks:
                for line in block.lines:
                    for word in line.words:
                        file_text += word.value + " "
                    file_text += "\n"
                file_text += "\n"
            files_texts.append(file_text)
        return files_texts

    def run_doctr(image: Image.Image, language):
        from doctr.models import ocr_predictor

        image_ndarray = np.array(pil_ensure_rgb(image))
        nonlocal doctr_model
        if doctr_model is None:
            doctr_model = ocr_predictor(
                det_arch="db_resnet50",
                reco_arch="crnn_mobilenet_v3_small",
                pretrained=True,
            )
            if torch.cuda.is_available():
                doctr_model = doctr_model.cuda().half()
        text = process_batch(doctr_model, [image_ndarray])[0]
        return text

    submit.click(
        fn=run_doctr,
        inputs=[image, language],
        outputs=[text],
    )
