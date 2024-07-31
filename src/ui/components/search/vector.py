from typing import Any, Dict, List, Tuple

import gradio as gr
import numpy as np

from src.data_extractors.ai.clip import CLIPEmbedder
from src.data_extractors.ai.text_embed import TextEmbedder
from src.db.search.types import (
    ExtractedTextFilter,
    ImageEmbeddingFilter,
    SearchQuery,
)
from src.db.utils import serialize_f32
from src.types import SearchStats
from src.ui.components.search.utils import AnyComponent


def create_vector_search_opts(query_state: gr.State):
    elements: List[Any] = []
    with gr.Tab(label="Semantic Search") as tab:
        elements.append(tab)
        with gr.Row():
            vec_query_type = gr.Dropdown(
                key="vec_query_type",
                choices=[],
                label="Search Type",
                scale=1,
            )
            elements.append(vec_query_type)
            te_embedding_model = gr.Dropdown(
                key="te_embedding_model",
                choices=[],
                interactive=True,
                visible=False,
                label="Select text embedding model",
                multiselect=False,
                scale=1,
            )
            elements.append(te_embedding_model)
            te_text_query = gr.Textbox(
                key="vec_text_search",
                label="Search for similar text extracted from images",
                show_copy_button=True,
                visible=False,
                scale=2,
            )
            elements.append(te_text_query)
            te_text_targets = gr.Dropdown(
                key="vec_targets",
                choices=[],
                interactive=True,
                label="Restrict query to text from these sources",
                multiselect=True,
                visible=False,
                scale=2,
            )
            elements.append(te_text_targets)
            clip_model = gr.Dropdown(
                key="clip_model",
                choices=[],
                interactive=True,
                label="Select CLIP model",
                multiselect=False,
                visible=False,
                scale=1,
            )
            elements.append(clip_model)
            clip_text_query = gr.Textbox(
                key="clip_text_search",
                label="Describe the image you are looking for",
                show_copy_button=True,
                visible=False,
                scale=2,
            )
            elements.append(clip_text_query)
            with gr.Accordion(
                label="Image Upload", visible=False
            ) as clip_image_accordion:
                elements.append(clip_image_accordion)
                clip_image_search = gr.Image(
                    key="clip_image_search",
                    label="Search for similar images",
                    scale=2,
                    type="numpy",
                )
                elements.append(clip_image_search)

        def on_vec_query_type_change(query_type: str):
            is_clip = query_type.startswith("CLIP")
            is_clip_image = query_type == "CLIP Reverse Image Search"
            return {
                clip_model: gr.Tab(visible=is_clip),
                clip_text_query: gr.Tab(
                    visible=(is_clip and not is_clip_image)
                ),
                clip_image_accordion: gr.Tab(visible=is_clip_image),
                te_embedding_model: gr.Tab(visible=not is_clip),
                te_text_query: gr.Tab(visible=not is_clip),
                te_text_targets: gr.Tab(visible=not is_clip),
            }

        vec_query_type.change(
            inputs=[vec_query_type],
            outputs=[query_state, *elements],
            fn=on_vec_query_type_change,
        )

    def on_data_change(
        query: SearchQuery,
        args: dict[AnyComponent, Any],
        final_query_build: bool = False,
    ) -> SearchQuery:
        vec_query_type_val: str | None = args[vec_query_type]
        te_embedding_model_val: str | None = args[te_embedding_model]
        te_text_query_val: str | None = args[te_text_query]
        te_text_targets_val: List[Tuple[str, str]] | None = args[
            te_text_targets
        ]
        clip_model_val: str | None = args[clip_model]
        clip_text_query_val: str | None = args[clip_text_query]
        clip_image_search_val: np.ndarray | None = args[clip_image_search]

        query.query.filters.extracted_text_embeddings = None
        query.query.filters.image_embeddings = None
        if vec_query_type_val == "Text Vector Search":
            if te_text_query_val:
                if not final_query_build:
                    embedded_query = get_embed(te_text_query_val)
                else:
                    embedded_query = te_text_query_val.encode("utf-8")
                query.query.filters.extracted_text_embeddings = (
                    ExtractedTextFilter[bytes](
                        query=embedded_query,
                        targets=te_text_targets_val or [],
                    )
                )
        elif vec_query_type_val == "CLIP Text Query":
            if clip_text_query_val and clip_model_val:
                if not final_query_build:
                    embedded_query = clip_text_query_val.encode("utf-8")
                else:
                    embedded_query = get_clip_embed(
                        clip_text_query_val, clip_model_val
                    )
                query.query.filters.image_embeddings = ImageEmbeddingFilter(
                    query=embedded_query,
                    target=("clip", clip_model_val),
                )
        elif vec_query_type_val == "CLIP Reverse Image Search":
            if clip_image_search_val is not None and clip_model_val:
                assert isinstance(
                    clip_image_search_val, np.ndarray
                ), "Expected numpy array for image search"
                if not final_query_build:
                    embedded_query = "placeholder".encode("utf-8")
                else:
                    embedded_query = get_clip_embed(
                        clip_image_search_val, clip_model_val
                    )
                query.query.filters.image_embeddings = ImageEmbeddingFilter(
                    query=embedded_query,
                    target=("clip", clip_model_val),
                )

        return query

    def on_stats_change(
        query: SearchQuery,
        search_stats: SearchStats,
    ) -> Dict[AnyComponent, Any]:
        query_types = []

        if search_stats.clip_setters:
            query_types += ["CLIP Text Query", "CLIP Reverse Image Search"]
        if search_stats.te_setters:
            query_types += ["Text Vector Search"]

        return {
            tab: gr.Tab(visible=bool(query_types)),
            vec_query_type: gr.update(choices=query_types),
            te_embedding_model: gr.update(choices=search_stats.te_setters),
            clip_model: gr.update(choices=search_stats.clip_setters),
            te_text_targets: gr.update(choices=search_stats.et_setters),
        }

    return elements, on_data_change, on_stats_change


last_embedded_text: str | None = None
last_embedded_text_embed: bytes | None = None


def get_embed(text: str) -> bytes:
    global last_embedded_text, last_embedded_text_embed
    if text == last_embedded_text and last_embedded_text_embed is not None:
        return last_embedded_text_embed
    # Set as persistent so that the model is not reloaded every time the function is called
    embedder = TextEmbedder(persistent=True)
    text_embed = embedder.get_text_embeddings([text])[0]
    last_embedded_text = text
    last_embedded_text_embed = serialize_f32(text_embed)
    return last_embedded_text_embed


def get_clip_embed(input: str | np.ndarray, model_name: str):

    from src.data_extractors.models import ImageEmbeddingModel

    model_opt = ImageEmbeddingModel(1, model_name)
    name = model_opt.clip_model_name()
    pretrained = model_opt.clip_model_checkpoint()
    # Set as persistent so that the model is not reloaded every time the function is called
    clip_model = CLIPEmbedder(name, pretrained, persistent=True)
    clip_model.load_model()
    if isinstance(input, str):
        embed = clip_model.get_text_embeddings([input])[0]
        assert isinstance(embed, np.ndarray)
        return serialize_f32(embed.tolist())
    else:  # input is an image
        embed = clip_model.get_image_embeddings([input])[0]
        assert isinstance(embed, np.ndarray)
        return serialize_f32(embed.tolist())