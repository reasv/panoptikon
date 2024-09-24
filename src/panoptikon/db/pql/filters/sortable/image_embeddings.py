import io
import logging
from typing import List, Literal, Optional

import numpy as np
import PIL
import PIL.Image
from pydantic import BaseModel, Field, PrivateAttr
from sqlalchemy import and_, func, literal, not_, or_
from sqlalchemy.sql.expression import CTE, select

from inferio.impl.utils import deserialize_array
from panoptikon.db.pql.filters.sortable.item_similarity import SourceArgs
from panoptikon.db.pql.filters.sortable.sortable_filter import SortableFilter
from panoptikon.db.pql.filters.sortable.text_embeddings import (
    EmbedArgs,
    extract_embeddings,
)
from panoptikon.db.pql.types import (
    OrderTypeNN,
    QueryState,
    get_order_by_field,
    get_order_direction_field,
    get_std_cols,
    get_std_group_by,
)
from panoptikon.db.utils import serialize_f32

logger = logging.getLogger(__name__)


class SemanticImageArgs(BaseModel):
    query: str = Field(
        ...,
        title="Query",
        description="""
Semantic query to match against the image.
Can be a string or a base64 encoded numpy array
to supply an embedding directly.
""",
    )

    _embedding: Optional[bytes] = PrivateAttr(None)

    model: str = Field(
        title="The image embedding model to use",
        description="""
The image embedding model to use for the semantic search.
Will search embeddings produced by this model.
""",
    )
    distance_aggregation: Literal["MIN", "MAX", "AVG"] = Field(
        default="MIN",
        description="The method to aggregate distances when an item has multiple embeddings. Default is MIN.",
    )
    embed: EmbedArgs = Field(
        default_factory=EmbedArgs,
        title="Embed The Query",
        description="""
Embed the query using the model already specified in `model`.
This is useful when the query is a string and needs to be converted to an embedding.

If this is not present, the query is assumed to be an embedding already.
In that case, it must be a base64 encoded string of a numpy array.
        """,
    )
    clip_xmodal: bool = Field(
        default=False,
        description="""
If true, will search among text embeddings as well as image embeddings created by the same CLIP model.

Note that you must have both image and text embeddings with the same CLIP model for this setting to work.
Text embeddings are derived from text which must have been already previously produced by another model, such as an OCR model or a tagger.
They are generated *separately* from the image embeddings, using a different job (Under 'CLIP Text Embeddings').
Run a batch job with the same clip model for both image and text embeddings to use this setting.
        """,
    )
    src_text: Optional[SourceArgs] = Field(
        default=None,
        description="""
Filters and options to apply on source text.
Can exclusively be used with `clip_xmodal` set to True.
Otherwise, it will be ignored, as it only applies to text embeddings.
""",
    )


class SemanticImageSearch(SortableFilter):
    order_by: bool = get_order_by_field(True)
    direction: OrderTypeNN = get_order_direction_field("asc")
    image_embeddings: SemanticImageArgs = Field(
        ...,
        title="Search Image Embeddings",
        description="""
Search for image using semantic search on image embeddings.
""",
    )

    def _validate(self):
        if len(self.image_embeddings.query.strip()) == 0:
            return self.set_validated(False)

        if self.image_embeddings.embed:
            self.image_embeddings._embedding = get_clip_embed(
                self.image_embeddings.query,
                self.image_embeddings.model,
                self.image_embeddings.embed,
            )
        else:
            self.image_embeddings._embedding = extract_embeddings(
                self.image_embeddings.query
            )
        if (
            not self.image_embeddings.clip_xmodal
            and self.image_embeddings.src_text is not None
        ):
            self.image_embeddings.src_text = None

        return self.set_validated(True)

    def build_query(self, context: CTE, state: QueryState) -> CTE:
        self.raise_if_not_validated()
        from panoptikon.db.pql.tables import (
            embeddings,
            extracted_text,
            item_data,
            items,
            setters,
        )

        args = self.image_embeddings
        model_cond = setters.c.name == args.model

        if args.clip_xmodal:
            # If using cross-modal similarity, use the
            # corresponding text embedding setter in the main embeddings query
            model_cond = model_cond | (setters.c.name == f"t{args.model}")
        # Gets all results with the requested embeddings
        embeddings_query = (
            select(
                items.c.id.label("item_id"),
            )
            .select_from(items)
            .join(
                item_data,
                item_data.c.item_id == context.c.item_id,
            )
            .join(
                setters,
                (setters.c.id == item_data.c.setter_id) & model_cond,
            )
            .join(
                embeddings,
                item_data.c.id == embeddings.c.id,
            )
        )

        src_setters = setters.alias("src_setters")
        src_item_data = item_data.alias("src_item_data")

        if args.src_text:
            # Filter text embeddings based on source text
            src_args = args.src_text
            # Join with extracted_text and apply filters
            # If the query is cross-modal, we only apply the source text filters to the text embeddings
            embeddings_query = embeddings_query.join(
                src_item_data,
                src_item_data.c.id == item_data.c.source_id,
                isouter=True,
            )
            if src_args.setter_names:
                embeddings_query = embeddings_query.join(
                    src_setters,
                    src_setters.c.id == src_item_data.c.setter_id,
                    isouter=True,
                )
            join_text = False
            conditions = []
            if src_args.setter_names:
                conditions.append(src_setters.c.name.in_(src_args.setter_names))

            if src_args.languages:
                join_text = True
                conditions.append(
                    extracted_text.c.language.in_(src_args.languages)
                )

            if src_args.min_confidence > 0:
                join_text = True
                conditions.append(
                    extracted_text.c.confidence >= src_args.min_confidence
                )

            if src_args.min_language_confidence > 0:
                join_text = True
                conditions.append(
                    extracted_text.c.language_confidence
                    >= src_args.min_language_confidence
                )

            if src_args.min_length > 0:
                join_text = True
                conditions.append(
                    extracted_text.c.text_length >= src_args.min_length
                )
            if (
                args.src_text.confidence_weight != 0
                or args.src_text.language_confidence_weight != 0
            ):
                join_text = True

            if join_text:
                embeddings_query = embeddings_query.join(
                    extracted_text,
                    extracted_text.c.id == item_data.c.source_id,
                    isouter=True,
                )
            # Only apply the source text filters to the text embeddings
            embeddings_query = embeddings_query.where(
                or_(
                    src_item_data.c.id.is_(None),
                    and_(*conditions),
                )
            )

        embeddings_query = embeddings_query.join(
            context,
            context.c.item_id == items.c.id,
            isouter=True,
        ).where(not_(context.c.item_id.is_(None)))

        if state.is_count_query:
            # No need to order by distance if we are just counting
            return self.wrap_query(
                embeddings_query.group_by(*get_std_group_by(context, state)),
                context,
                state,
            )

        # Image embeddings are connected to items via item_data
        # We want to do distance calculation on all unique item_id, embedding pairs
        # and then order by the distance

        vec_distance = func.vec_distance_cosine(
            embeddings.c.embedding,
            literal(args._embedding),
        )
        if args.distance_aggregation == "MAX":
            rank_column = func.max(vec_distance)
        elif args.distance_aggregation == "AVG":
            rank_column = func.avg(vec_distance)
        elif args.distance_aggregation == "MIN":
            rank_column = func.min(vec_distance)
        else:
            raise ValueError(
                f"Invalid distance aggregation method: {args.distance_aggregation}"
            )

        if args.src_text:
            conf_weight_clause = func.pow(
                func.coalesce(extracted_text.c.confidence, 1),
                args.src_text.confidence_weight,
            )
            lang_conf_weight_clause = func.pow(
                func.coalesce(extracted_text.c.language_confidence, 1),
                args.src_text.language_confidence_weight,
            )
            if (
                args.src_text.confidence_weight != 0
                and args.src_text.language_confidence_weight != 0
            ):
                weights = conf_weight_clause * lang_conf_weight_clause
                rank_column = func.sum(vec_distance * weights) / func.sum(
                    weights
                )
            elif args.src_text.confidence_weight != 0:
                rank_column = func.sum(
                    vec_distance * conf_weight_clause
                ) / func.sum(conf_weight_clause)
            elif args.src_text.language_confidence_weight != 0:
                rank_column = func.sum(
                    vec_distance * lang_conf_weight_clause
                ) / func.sum(lang_conf_weight_clause)

        # Now we join back with the embeddings table and get the distance
        # between the query embedding and the embeddings in the database
        embeddings_query = embeddings_query.with_only_columns(
            *get_std_cols(context, state),
            self.derive_rank_column(rank_column),
        ).group_by(*get_std_group_by(context, state))

        # Now we join with the original query to give the min distance for each item

        return self.wrap_query(embeddings_query, context, state)


def get_clip_embed(
    input: str | PIL.Image.Image,
    model_name: str,
    embed_args: EmbedArgs,
):

    from panoptikon.data_extractors.models import ModelOptsFactory

    model = ModelOptsFactory.get_model(model_name)

    if isinstance(input, str):
        embed_bytes: bytes = model.run_batch_inference(
            embed_args.cache_key,
            embed_args.lru_size,
            embed_args.ttl_seconds,
            [({"text": input}, None)],
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
            embed_args.cache_key,
            embed_args.lru_size,
            embed_args.ttl_seconds,
            [({}, input_bytes)],
        )[0]
        embed = deserialize_array(embed_bytes)
        assert isinstance(embed, np.ndarray)
        return serialize_f32(embed.tolist())
