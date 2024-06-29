import gradio as gr
from paddleocr import PaddleOCR, draw_ocr # type: ignore
from PIL import Image
import numpy as np

def create_paddleocr_UI():
    DESCRIPTION = """
    PaddleOCR is an OCR tool based on PaddlePaddle.
    It is easy to use and provides a variety of text detection and recognition models.
    """
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
                    submit = gr.Button(value="Submit", variant="primary", size="lg")
            with gr.Column(variant="panel"):
                text = gr.Textbox(label="Output")
                clear.add([text])
                output_image = gr.Image(type="pil", image_mode="RGBA", label="Output")

    def draw_ocr_result(image: Image.Image, result: list):
        image = image.convert("RGB")
        boxes = [line[0] for line in result]
        txts = [line[1][0] for line in result]
        scores = [line[1][1] for line in result]
        im_show = draw_ocr(image, boxes, txts, scores, font_path='./fonts/simfang.ttf')
        im_show = Image.fromarray(im_show)
        return im_show

    def run_paddleocr(image: Image.Image, language):
        print(image)
        image_ndarray = np.array(image)
        ocr = PaddleOCR(use_angle_cls=True, lang=language, use_gpu=False)
        result = ocr.ocr(image_ndarray)[0]
        text = "\n".join([line[1][0] for line in result])
        return text, draw_ocr_result(image, result)

    submit.click(
        fn=run_paddleocr,
        inputs=[image, language],
        outputs=[text, output_image],
    )