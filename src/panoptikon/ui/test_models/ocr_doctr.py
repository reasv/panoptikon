from typing import List, Sequence, Tuple

import gradio as gr
import numpy as np
import torch
from PIL import Image
from sympy import li

from panoptikon.utils import pil_ensure_rgb


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
                threshold = gr.Slider(
                    value=0,
                    label="Confidence Threshold for Words",
                    minimum=0,
                    maximum=1,
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
                with gr.Tabs():
                    with gr.Tab(label="Output"):
                        text = gr.Textbox(label="Output", lines=20)
                        clear.add([text])
                    with gr.Tab(label="JSON Data"):
                        json_data = gr.JSON(label="JSON Data")
                        clear.add([json_data])
                    with gr.Tab(label="Word Confidences"):
                        wc_text = gr.Textbox(label="Word Confidences", lines=20)
                        clear.add([wc_text])
                    with gr.Tab(label="Confidence Labels"):
                        confidence_labels = gr.Label(
                            label="Confidence For Each Word"
                        )
                        clear.add([confidence_labels])

    def process_batch(
        model, batch: Sequence[np.ndarray], threshold: float
    ) -> Tuple[List[str], dict, List[Tuple[str, float]]]:
        result = model(batch)
        files_texts: List[str] = []
        words_confidences: List[Tuple[str, float]] = []
        for page in result.pages:
            file_text = ""
            for block in page.blocks:
                for line in block.lines:
                    for word in line.words:
                        if word.confidence < threshold:
                            continue
                        file_text += word.value + " "
                        words_confidences.append((word.value, word.confidence))
                    file_text += "\n"
                file_text += "\n"
            files_texts.append(file_text)
        j = result.export()
        return files_texts, j, words_confidences

    def run_doctr(image: Image.Image, threshold: float):
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
        text, j, wc = process_batch(doctr_model, [image_ndarray], threshold)
        text = text[0]
        wc_string = "\n".join(
            [f"{word}: {confidence}" for word, confidence in wc]
        )
        wc_dict = {word: confidence for word, confidence in wc}
        return (
            text,
            j,
            wc_string,
            wc_dict,
        )

    submit.click(
        fn=run_doctr,
        inputs=[image, threshold],
        outputs=[text, json_data, wc_text, confidence_labels],
    )
