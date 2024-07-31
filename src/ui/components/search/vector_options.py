from dataclasses import asdict
from typing import Any, List, Tuple

import gradio as gr
import numpy as np
from torch import Tag

from src.db.search.types import (
    ExtractedTextFilter,
    ImageEmbeddingFilter,
    SearchQuery,
)
from src.db.search.utils import from_dict
from src.types import SearchStats


def create_vector_search_opts(
    query_state: gr.State, search_stats_state: gr.State
):
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

        def on_stats_change(
            query_state_dict: dict,
            search_stats_dict: dict,
        ):
            query = from_dict(SearchQuery, query_state_dict)
            search_stats = from_dict(SearchStats, search_stats_dict)

            query_types = []

            if search_stats.clip_setters:
                query_types += ["CLIP Text Query", "CLIP Reverse Image Search"]
            if search_stats.te_setters:
                query_types += ["Text Vector Search"]

            return {
                query_state: asdict(query),
                tab: gr.Tab(visible=bool(query_types)),
                vec_query_type: gr.update(choices=query_types),
                te_embedding_model: gr.update(choices=search_stats.te_setters),
                clip_model: gr.update(choices=search_stats.clip_setters),
                te_text_targets: gr.update(choices=search_stats.et_setters),
            }

        gr.on(
            triggers=[search_stats_state.change],
            fn=on_stats_change,
            inputs=[query_state, search_stats_state],
            outputs=[query_state, *elements],
        )
        gr.on(
            triggers=[
                vec_query_type.select,
                te_embedding_model.select,
                te_text_query.input,
                te_text_targets.select,
                clip_model.select,
                clip_text_query.input,
                clip_image_search.input,
            ],
            fn=on_change_data,
            inputs=[
                query_state,
                vec_query_type,
                te_embedding_model,
                te_text_query,
                te_text_targets,
                clip_model,
                clip_text_query,
                clip_image_search,
            ],
            outputs=[query_state],
        )


def on_change_data(
    query_state_dict: dict,
    vec_query_type: str | None,
    te_embedding_model: str | None,
    te_text_query: str | None,
    te_text_targets: List[Tuple[str, str]] | None,
    clip_model: str | None,
    clip_text_query: str | None,
    clip_image_search: np.ndarray | None,
):
    query_state = from_dict(SearchQuery, query_state_dict)
    query_state.query.filters.extracted_text_embeddings = None
    query_state.query.filters.image_embeddings = None
    if vec_query_type == "Text Vector Search":
        if te_text_query:
            query_state.query.filters.extracted_text_embeddings = (
                ExtractedTextFilter[bytes](
                    query=te_text_query.encode("utf-8"),
                    targets=te_text_targets or [],
                )
            )
    elif vec_query_type == "CLIP Text Query":
        if clip_text_query and clip_model:
            query_state.query.filters.image_embeddings = ImageEmbeddingFilter(
                query=clip_text_query.encode("utf-8"),
                target=("clip", clip_model),
            )
    elif vec_query_type == "CLIP Reverse Image Search":
        if clip_image_search is not None and clip_model:
            query_state.query.filters.image_embeddings = ImageEmbeddingFilter(
                query=clip_image_search,  # type: ignore
                target=("clip", clip_model),
            )

    return asdict(query_state)
