import io
from typing import Any, Dict, List

import gradio as gr
import numpy as np
import PIL.Image

from src.db.search.types import (
    ExtractedTextEmbeddingsFilter,
    ImageEmbeddingFilter,
    SearchQuery,
)
from src.db.utils import serialize_f32
from src.inference.impl.utils import deserialize_array
from src.types import SearchStats
from src.ui.components.search.utils import AnyComponent


def create_vector_search_opts(query_state: gr.State):
    elements: List[Any] = []
    with gr.Tab(label="Semantic Search") as tab:
        elements.append(tab)
        with gr.Row():
            with gr.Column(scale=2):
                vec_query_type = gr.Dropdown(
                    key="vec_query_type",
                    choices=[],
                    label="Search Type",
                    scale=1,
                )
                elements.append(vec_query_type)
            with gr.Column(scale=10):
                with gr.Row():
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
                    confidence = gr.Slider(
                        key="vec_extracted_text_confidence",
                        minimum=0,
                        maximum=1,
                        value=0,
                        step=0.05,
                        label="Min. Confidence Level from Text Extraction",
                        visible=False,
                        scale=1,
                    )
                    elements.append(confidence)
                    languages = gr.Dropdown(
                        key="vec_extracted_text_languages",
                        label="Languages",
                        choices=[
                            "en",
                        ],
                        multiselect=True,
                        value=[],
                        visible=False,
                        scale=1,
                    )
                    elements.append(languages)
                    language_confidence = gr.Slider(
                        key="vec_extracted_text_language_confidence",
                        minimum=0,
                        maximum=1,
                        value=0,
                        step=0.05,
                        label="Min. Confidence Level for Language Detection",
                        visible=False,
                        scale=1,
                    )
                    elements.append(language_confidence)
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
                            type="pil",
                        )
                        elements.append(clip_image_search)

        def on_vec_query_type_change(query_type: str):

            is_clip = query_type.startswith("CLIP")
            is_clip_image = query_type == "CLIP Reverse Image Search"
            return {
                clip_model: gr.update(visible=is_clip),
                clip_text_query: gr.update(
                    visible=(is_clip and not is_clip_image)
                ),
                clip_image_accordion: gr.update(visible=is_clip_image),
                te_embedding_model: gr.update(visible=not is_clip),
                te_text_query: gr.update(visible=not is_clip),
                te_text_targets: gr.update(visible=not is_clip),
                confidence: gr.update(visible=not is_clip),
                languages: gr.update(visible=not is_clip),
                language_confidence: gr.update(visible=not is_clip),
            }

        vec_query_type.change(
            inputs=[vec_query_type],
            outputs=[*elements],
            fn=on_vec_query_type_change,
        )

    def on_data_change(
        query: SearchQuery,
        args: dict[AnyComponent, Any],
        final_query_build: bool = False,
    ) -> SearchQuery:
        from src.data_extractors.models import ModelOptsFactory

        vec_query_type_val: str | None = args[vec_query_type]
        te_embedding_model_val: str | None = args[te_embedding_model]
        te_text_query_val: str | None = args[te_text_query]
        te_text_targets_val: List[str] | None = args[te_text_targets]
        confidence_val: float = args[confidence]
        languages_val: List[str] | None = args[languages]
        language_confidence_val: float = args[language_confidence]
        clip_model_val: str | None = args[clip_model]
        clip_text_query_val: str | None = args[clip_text_query]
        clip_image_search_val: PIL.Image.Image | None = args[clip_image_search]

        query.query.filters.extracted_text_embeddings = None
        query.query.filters.image_embeddings = None
        if vec_query_type_val == "Text Vector Search":
            if te_text_query_val and te_embedding_model_val:
                if not final_query_build:
                    embedded_query = te_text_query_val.encode("utf-8")
                    model = ModelOptsFactory.get_model(te_embedding_model_val)
                    model.load_model(
                        "search",
                        1,
                        60,
                    )
                else:
                    embedded_query = get_embed(
                        te_text_query_val, te_embedding_model_val
                    )
                query.query.filters.extracted_text_embeddings = (
                    ExtractedTextEmbeddingsFilter(
                        query=embedded_query,
                        model=te_embedding_model_val,
                        targets=te_text_targets_val or [],
                        min_confidence=confidence_val or None,
                        languages=languages_val or [],
                        language_min_confidence=language_confidence_val or None,
                    )
                )
                query.order_args.order_by = "text_vec_distance"
        elif vec_query_type_val == "CLIP Text Query":
            if clip_text_query_val and clip_model_val:
                if not final_query_build:
                    embedded_query = clip_text_query_val.encode("utf-8")
                    model = ModelOptsFactory.get_model(clip_model_val)
                    model.load_model(
                        "search",
                        1,
                        60,
                    )
                else:
                    embedded_query = get_clip_embed(
                        clip_text_query_val, clip_model_val
                    )
                query.query.filters.image_embeddings = ImageEmbeddingFilter(
                    query=embedded_query,
                    model=clip_model_val,
                )
                query.order_args.order_by = "image_vec_distance"
        elif vec_query_type_val == "CLIP Reverse Image Search":
            if clip_image_search_val is not None and clip_model_val:
                assert isinstance(
                    clip_image_search_val, PIL.Image.Image
                ), "Expected numpy array for image search"
                if not final_query_build:
                    embedded_query = "placeholder".encode("utf-8")
                    model = ModelOptsFactory.get_model(clip_model_val)
                    model.load_model(
                        "search",
                        1,
                        60,
                    )
                else:
                    embedded_query = get_clip_embed(
                        clip_image_search_val, clip_model_val
                    )
                query.query.filters.image_embeddings = ImageEmbeddingFilter(
                    query=embedded_query,
                    model=clip_model_val,
                )
                query.order_args.order_by = "image_vec_distance"

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

        updates: Dict[AnyComponent, Any] = {
            tab: gr.Tab(visible=bool(query_types)),
            vec_query_type: gr.update(choices=query_types),
            te_embedding_model: gr.update(choices=search_stats.te_setters),
            clip_model: gr.update(choices=search_stats.clip_setters),
            te_text_targets: gr.update(choices=search_stats.et_setters),
            languages: gr.update(choices=search_stats.et_stats.languages),
        }
        if not query.query.filters.extracted_text_embeddings:
            updates[te_embedding_model] = gr.Dropdown(
                choices=search_stats.te_setters,
                value=(
                    search_stats.te_setters[0]
                    if search_stats.te_setters
                    else None
                ),
            )

        if not query.query.filters.image_embeddings:
            updates[clip_model] = gr.Dropdown(
                choices=search_stats.clip_setters,
                value=(
                    search_stats.clip_setters[0]
                    if search_stats.clip_setters
                    else None
                ),
            )
        return updates

    return elements, on_data_change, on_stats_change


last_embedded_text: str | None = None
last_embedded_text_embed: bytes | None = None
last_used_model: str | None = None


def get_embed(text: str, model_name: str) -> bytes:

    global last_embedded_text, last_embedded_text_embed, last_used_model
    if (
        text == last_embedded_text
        and model_name == last_used_model
        and last_embedded_text_embed is not None
    ):
        return last_embedded_text_embed

    from src.data_extractors.models import ModelOptsFactory

    model = ModelOptsFactory.get_model(model_name)
    embed_bytes: bytes = model.run_batch_inference(
        "search", 1, 60, [({"text": text, "task": "s2s"}, None)]
    )[0]
    text_embed = deserialize_array(embed_bytes)[0]
    assert isinstance(text_embed, np.ndarray)
    # Set as persistent so that the model is not reloaded every time the function is called
    last_embedded_text = text
    last_used_model = model_name
    last_embedded_text_embed = serialize_f32(text_embed.tolist())
    return last_embedded_text_embed


def get_clip_embed(input: str | PIL.Image.Image, model_name: str):

    from src.data_extractors.models import ModelOptsFactory

    model = ModelOptsFactory.get_model(model_name)

    if isinstance(input, str):
        embed_bytes: bytes = model.run_batch_inference(
            "search", 1, 60, [({"text": input}, None)]
        )[0]
        embed = deserialize_array(embed_bytes)
        assert isinstance(embed, np.ndarray)
        return serialize_f32(embed.tolist())
    else:  # input is an image
        # Save image into a buffer
        image_buffer = io.BytesIO()
        input.save(image_buffer, format="PNG")
        input_bytes = image_buffer.getvalue()

        embed_bytes: bytes = model.run_batch_inference(
            "search", 1, 60, [({}, input_bytes)]
        )[0]
        embed = deserialize_array(embed_bytes)
        assert isinstance(embed, np.ndarray)
        return serialize_f32(embed.tolist())
