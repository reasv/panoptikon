from typing import List
import hashlib

import gradio as gr

from src.ui.components.multi_view import create_multiview
from src.db import FileSearchResult
from src.files import get_files_by_extension, get_last_modified_time_and_size, get_mime_type
from src.clip import CLIPEmbedder

def get_images(folder_path: str):
    image_paths = list(get_files_by_extension([folder_path], [], ['.jpg', '.jpeg', '.png']))
    files = [
        FileSearchResult(
            image_path,
            hashlib.sha256(image_path.encode()).hexdigest(),
            get_last_modified_time_and_size(image_path)[0],
            get_mime_type(image_path) or ""
        ) for image_path in image_paths]
    return files

def create_CLIP_ui():
    DESCRIPTION = """
    User can input the path to a folder of images, and a text query. The model will return the probability of the text query being in the image.
    We will first read each image, generate embeddings for each image and text query, and then calculate the cosine similarity between the image and text embeddings.
    Images will be returned in the order of their similarity to the text query inside a multiview.
    """

    clip = CLIPEmbedder()
    embeddings = {}

    with gr.Column():
        gr.Markdown(DESCRIPTION)
        with gr.Row():
            folder_path = gr.Textbox(label="Folder Path")
            generate_embeddings = gr.Button("Generate Embeddings")
            delete_embeddings_button = gr.Button("Unload Model/Embeddings")
            text_query = gr.Textbox(label="Text Query", value="a cat", interactive=True)
            search = gr.Button("Search", interactive=False)
        with gr.Row():
            multiview = create_multiview()

    def embed_images(folder_path: str):
        nonlocal clip, embeddings
        files = get_images(folder_path)
        image_paths = [file.path for file in files]
        embeddings = clip.get_image_embeddings(image_paths)
        # Create a dictionary of image sha256 to embeddings
        embeddings = {file.sha256: embedding for file, embedding in zip(files, embeddings)}
        return files, gr.update(interactive=True)

    def search_images(folder_path: str, text_query: str, current_files: List[FileSearchResult]):
        nonlocal clip, embeddings
        if text_query == "":
            return get_images(folder_path)
        text_embeddings = clip.get_text_embeddings([text_query])
        text_embedding = text_embeddings[0]
        images_dict = {file.sha256: file for file in current_files}
        ranked_images = [images_dict[hash] for hash in clip.rank_images_by_similarity(embeddings, text_embedding)]
        return ranked_images
    
    def delete_embeddings():
        nonlocal clip, embeddings
        clip.unload_model()
        embeddings = {}
        return [], gr.update(interactive=False),

    generate_embeddings.click(
        fn=embed_images,
        inputs=[folder_path],
        outputs=[multiview.files, search]
    )

    search.click(
        fn=search_images,
        inputs=[folder_path, text_query, multiview.files],
        outputs=[multiview.files]
    )

    delete_embeddings_button.click(
        fn=delete_embeddings,
        inputs=[],
        outputs=[multiview.files, search]
    )
