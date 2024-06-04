#!/usr/bin/env python

from __future__ import annotations

import os
import pathlib
import tarfile

import deepdanbooru as dd
import gradio as gr
import huggingface_hub
import numpy as np
import PIL.Image
import tensorflow as tf

DESCRIPTION = "# [KichangKim/DeepDanbooru](https://github.com/KichangKim/DeepDanbooru)"


def load_sample_image_paths() -> list[pathlib.Path]:
    image_dir = pathlib.Path("images")
    if not image_dir.exists():
        path = huggingface_hub.hf_hub_download("public-data/sample-images-TADNE", "images.tar.gz", repo_type="dataset")
        with tarfile.open(path) as f:
            f.extractall()
    return sorted(image_dir.glob("*"))


def load_model() -> tf.keras.Model:
    path = huggingface_hub.hf_hub_download("public-data/DeepDanbooru", "model-resnet_custom_v3.h5")
    model = tf.keras.models.load_model(path)
    return model


def load_labels() -> list[str]:
    path = huggingface_hub.hf_hub_download("public-data/DeepDanbooru", "tags.txt")
    with open(path) as f:
        labels = [line.strip() for line in f.readlines()]
    return labels


model = load_model()
labels = load_labels()


def predict(image: PIL.Image.Image, score_threshold: float) -> tuple[dict[str, float], dict[str, float], str]:
    _, height, width, _ = model.input_shape
    image = np.asarray(image)
    image = tf.image.resize(image, size=(height, width), method=tf.image.ResizeMethod.AREA, preserve_aspect_ratio=True)
    image = image.numpy()
    image = dd.image.transform_and_pad_image(image, width, height)
    image = image / 255.0
    probs = model.predict(image[None, ...])[0]
    probs = probs.astype(float)

    indices = np.argsort(probs)[::-1]
    result_all = dict()
    result_threshold = dict()
    for index in indices:
        label = labels[index]
        prob = probs[index]
        result_all[label] = prob
        if prob < score_threshold:
            break
        result_threshold[label] = prob
    result_text = ", ".join(result_all.keys())
    return result_threshold, result_all, result_text


image_paths = load_sample_image_paths()
examples = [[path.as_posix(), 0.5] for path in image_paths]

with gr.Blocks(css="style.css") as demo:
    gr.Markdown(DESCRIPTION)
    with gr.Row():
        with gr.Column():
            image = gr.Image(label="Input", type="pil")
            score_threshold = gr.Slider(label="Score threshold", minimum=0, maximum=1, step=0.05, value=0.5)
            run_button = gr.Button("Run")
        with gr.Column():
            with gr.Tabs():
                with gr.Tab(label="Output"):
                    result = gr.Label(label="Output", show_label=False)
                with gr.Tab(label="JSON"):
                    result_json = gr.JSON(label="JSON output", show_label=False)
                with gr.Tab(label="Text"):
                    result_text = gr.Text(label="Text output", show_label=False, lines=5)
    gr.Examples(
        examples=examples,
        inputs=[image, score_threshold],
        outputs=[result, result_json, result_text],
        fn=predict,
        cache_examples=os.getenv("CACHE_EXAMPLES") == "1",
    )

    run_button.click(
        fn=predict,
        inputs=[image, score_threshold],
        outputs=[result, result_json, result_text],
        api_name="predict",
    )

if __name__ == "__main__":
    demo.queue(max_size=20).launch()