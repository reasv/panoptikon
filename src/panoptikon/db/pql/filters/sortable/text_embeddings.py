import base64
import io
import logging
import time
from typing import List, Literal, Optional

import numpy as np
from pydantic import BaseModel, Field, PrivateAttr
from sqlalchemy import and_, func, literal
from sqlalchemy.sql.expression import CTE, select

from inferio.impl.utils import deserialize_array
from panoptikon.db.pql.filters.sortable.item_similarity import SourceArgs
from panoptikon.db.pql.filters.sortable.sortable_filter import SortableFilter
from panoptikon.db.pql.filters.sortable.utils import extract_embeddings
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


class EmbedArgs(BaseModel):
    cache_key: str = Field(
        default="search",
        title="Cache Key",
        description="The cache key to use for the inference *model*",
    )
    lru_size: int = Field(
        default=1,
        ge=1,
        title="LRU Cache Size",
        description="The size of the LRU cache to use for the inference *model*",
    )
    ttl_seconds: int = Field(
        default=60,
        title="TTL Seconds",
        description="The time-to-live in seconds for the inference *model* to be kept in memory",
    )


class SemanticTextArgs(BaseModel):
    query: str = Field(
        ...,
        title="Query",
        description="Semantic query to match against the text",
    )

    _embedding: Optional[bytes] = PrivateAttr(None)

    model: str = Field(
        title="The text embedding model to use",
        description="""
The text embedding model to use for the semantic search.
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
    src_text: Optional[SourceArgs] = Field(
        default=None,
        description="""
Filters and options to apply on source text that the embeddings are derived from.
""",
    )


class SemanticTextSearch(SortableFilter):
    order_by: bool = get_order_by_field(True)
    direction: OrderTypeNN = get_order_direction_field("asc")
    text_embeddings: SemanticTextArgs = Field(
        ...,
        title="Search Text Embeddings",
        description="""
Search for text using semantic search on text embeddings.
""",
    )

    def _validate(self):
        if len(self.text_embeddings.query.strip()) == 0:
            return self.set_validated(False)

        if self.text_embeddings.embed:
            self.text_embeddings._embedding = get_embed(
                self.text_embeddings.query,
                self.text_embeddings.model,
                self.text_embeddings.embed,
            )

        else:
            self.text_embeddings._embedding = extract_embeddings(
                self.text_embeddings.query
            )

        return self.set_validated(True)

    def build_query(self, context: CTE, state: QueryState) -> CTE:
        self.raise_if_not_validated()
        from panoptikon.db.pql.tables import (
            embeddings,
            extracted_text,
            item_data,
            setters,
        )

        args = self.text_embeddings
        criteria = []
        text_data = item_data.alias("text_data")
        text_setters = setters.alias("text_setters")
        vec_data = item_data.alias("vec_data")
        vec_setters = setters.alias("vec_setters")
        if args.src_text:
            if args.src_text.min_length:
                criteria.append(
                    extracted_text.c.text_length >= args.src_text.min_length
                )
            if args.src_text.max_length:
                criteria.append(
                    extracted_text.c.text_length <= args.src_text.max_length
                )
            if args.src_text.setters:
                criteria.append(text_setters.c.name.in_(args.src_text.setters))
            if args.src_text.languages:
                criteria.append(
                    extracted_text.c.language.in_(args.src_text.languages)
                )
            if args.src_text.min_language_confidence:
                criteria.append(
                    extracted_text.c.language_confidence
                    >= args.src_text.min_language_confidence
                )
            if args.src_text.min_confidence:
                criteria.append(
                    extracted_text.c.confidence >= args.src_text.min_confidence
                )

        vec_distance = func.vec_distance_L2(
            embeddings.c.embedding, literal(args._embedding)
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

        if state.item_data_query and state.entity == "text":
            return self.wrap_query(
                select(
                    *get_std_cols(context, state),
                    self.derive_rank_column(rank_column),
                )
                .join(
                    text_data,
                    (text_data.c.id == context.c.data_id),
                )
                .join(
                    text_setters,
                    text_setters.c.id == text_data.c.setter_id,
                )
                .join(
                    extracted_text,
                    context.c.data_id == extracted_text.c.id,
                )
                .join(
                    vec_data,
                    vec_data.c.source_id == extracted_text.c.id,
                )
                .join(
                    vec_setters,
                    (vec_setters.c.id == vec_data.c.setter_id)
                    & (vec_setters.c.name == args.model),
                )
                .join(
                    embeddings,
                    embeddings.c.id == vec_data.c.id,
                )
                .where(and_(*criteria))
                .group_by(*get_std_group_by(context, state)),
                context,
                state,
            )
        embeddings_query = (
            select(
                *get_std_cols(context, state),
                self.derive_rank_column(rank_column),
            )
            .join(
                vec_data,
                vec_data.c.item_id == context.c.item_id,
            )
            .join(
                vec_setters,
                (vec_setters.c.id == vec_data.c.setter_id)
                & (vec_setters.c.name == args.model),
            )
            .join(
                embeddings,
                embeddings.c.id == vec_data.c.id,
            )
            .where(and_(*criteria))
            .group_by(*get_std_group_by(context, state))
        )
        if len(criteria) > 0:
            embeddings_query = (
                embeddings_query.join(
                    text_data,
                    text_data.c.id == vec_data.c.source_id,
                )
                .join(
                    text_setters,
                    text_setters.c.id == text_data.c.setter_id,
                )
                .join(
                    extracted_text,
                    text_data.c.id == extracted_text.c.id,
                )
            )

        return self.wrap_query(
            embeddings_query,
            context,
            state,
        )


last_embedded_text: str | None = None
last_embedded_text_embed: bytes | None = None
last_used_model: str | None = None


def get_embed(
    text: str,
    model_name: str,
    cache_args: EmbedArgs,
) -> bytes:

    global last_embedded_text, last_embedded_text_embed, last_used_model
    if (
        text == last_embedded_text
        and model_name == last_used_model
        and last_embedded_text_embed is not None
    ):
        return last_embedded_text_embed

    from panoptikon.data_extractors.models import run_batch_inference

    logger.debug(f"Getting embedding for text: {text}")
    start_time = time.time()
    embed_bytes: bytes = run_batch_inference(
        model_name,
        cache_args.cache_key,
        cache_args.lru_size,
        cache_args.ttl_seconds,
        [({"text": text, "task": "s2s"}, None)],
    )[0]
    deserialized_embedding = deserialize_array(embed_bytes)
    if isinstance(deserialized_embedding[0], np.ndarray):
        text_embed = deserialized_embedding[0]
    else:
        text_embed = deserialized_embedding
    # Set as persistent so that the model is not reloaded every time the function is called
    last_embedded_text = text
    last_used_model = model_name
    embed_list = text_embed.tolist()
    assert isinstance(embed_list, list), "Expected a list"
    last_embedded_text_embed = serialize_f32(embed_list)
    logger.debug(
        f"Embedding generation took {time.time() - start_time} seconds"
    )
    return last_embedded_text_embed
