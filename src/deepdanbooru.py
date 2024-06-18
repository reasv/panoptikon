from __future__ import annotations

import PIL.IcnsImagePlugin
import deepdanbooru as dd
import huggingface_hub
import numpy as np
import PIL.Image
import tensorflow as tf

def load_model() -> tf.keras.Model:
    print("Loading model...")
    path = huggingface_hub.hf_hub_download("public-data/DeepDanbooru", "model-resnet_custom_v3.h5")
    model = tf.keras.models.load_model(path)
    return model

def load_labels() -> list[str]:
    path = huggingface_hub.hf_hub_download("public-data/DeepDanbooru", "tags.txt")
    with open(path) as f:
        labels = [line.strip() for line in f.readlines()]
    return labels

def predict(image: PIL.Image.Image, model: tf.keras.Model, labels: list[str], score_threshold: float=0.05) -> tuple[dict[str, float], dict[str, float], str]:
    if image.mode != 'RGB':
        image = image.convert('RGB')
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


def predict_batch(images: list[PIL.Image.Image], model: tf.keras.Model, labels: list[str], score_threshold: float=0.05) -> list[tuple[dict[str, float], dict[str, float], str]]:
    processed_images = []
    for image in images:
        if image.mode != 'RGB':
            image = image.convert('RGB')
        _, height, width, _ = model.input_shape
        image = np.asarray(image)
        image = tf.image.resize(image, size=(height, width), method=tf.image.ResizeMethod.AREA, preserve_aspect_ratio=True)
        image = image.numpy()
        # Assuming dd.image.transform_and_pad_image is defined elsewhere
        image = dd.image.transform_and_pad_image(image, width, height)  
        image = image / 255.0
        processed_images.append(image)

    # Convert list of processed images to a numpy array
    processed_images_array = np.stack(processed_images, axis=0)

    # Run inference
    probs_list = model.predict(processed_images_array)

    # Process results
    results = []
    for probs in probs_list:
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
        results.append((result_threshold, result_all, result_text))

    return results