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
    setters: List[str] = Field(
        default_factory=list,
        title="Include text from these setters",
        description="""
Filter out text that is was not set by these setters.
The setters are usually the names of the models that extracted or generated the text.
For example, the OCR model, the Whisper STT model, the captioning model or the tagger model.
""",
    )
    languages: List[str] = Field(
        default_factory=list,
        title="Included languages",
        description="Filter out text that is not in these languages",
    )
    language_min_confidence: Optional[float] = Field(
        default=None,
        ge=0,
        le=1,
        title="Minimum Confidence for Language Detection",
        description="""
Filter out text that has a language confidence score below this threshold.
Must be a value between 0 and 1.
Language confidence scores are usually set by the model that extracted the text.
For tagging models, it's always 1.
""",
    )
    min_confidence: Optional[float] = Field(
        default=None,
        ge=0,
        le=1,
        title="Minimum Confidence for the text",
        description="""
Filter out text that has a confidence score below this threshold.
Must be a value between 0 and 1.
Confidence scores are usually set by the model that extracted the text.
""",
    )
    min_length: Optional[int] = Field(
        default=None,
        ge=0,
        title="Minimum Length",
        description="Filter out text that is shorter than this. Inclusive.",
    )
    max_length: Optional[int] = Field(
        default=None,
        ge=0,
        title="Maximum Length",
        description="Filter out text that is longer than this. Inclusive.",
    )
    distance_aggregation: Literal["MIN", "MAX", "AVG"] = Field(
        default="MIN",
        description="The method to aggregate distances when an item has multiple embeddings. Default is MIN.",
    )
    confidence_weight: float = Field(
        default=0.0,
        description="""
The weight to apply to the confidence of the source text
on the embedding distance aggregation for individual items with multiple embeddings.
Default is 0.0, which means that the confidence of the source text
does not affect the distance aggregation.
This parameter is only relevant when the source text has a confidence value.
The confidence of the source text is multiplied by the confidence of the other
source text when calculating the distance between two items.
The formula for the distance calculation is as follows:
```
weights = POW(COALESCE(text.confidence, 1)), src_confidence_weight)
distance = SUM(distance * weights) / SUM(weights)
```
So this weight is the exponent to which the confidence is raised, which means that it can be greater than 1.
When confidence weights are set, the distance_aggregation setting is ignored.
""",
    )
    language_confidence_weight: float = Field(
        default=0.0,
        description="""
The weight to apply to the confidence of the source text language
on the embedding distance aggregation.
Default is 0.0, which means that the confidence of the source text language detection
does not affect the distance calculation.
Totally analogous to `src_confidence_weight`, but for the language confidence.
When both are present, the results of the POW() functions for both are multiplied together before being applied to the distance.
```
weights = POW(..., src_confidence_weight) * POW(..., src_language_confidence_weight)
```
""",
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
        if args.min_length:
            criteria.append(extracted_text.c.text_length >= args.min_length)
        if args.max_length:
            criteria.append(extracted_text.c.text_length <= args.max_length)
        if args.setters:
            criteria.append(text_setters.c.name.in_(args.setters))
        if args.languages:
            criteria.append(extracted_text.c.language.in_(args.languages))
        if args.language_min_confidence:
            criteria.append(
                extracted_text.c.language_confidence
                >= args.language_min_confidence
            )
        if args.min_confidence:
            criteria.append(extracted_text.c.confidence >= args.min_confidence)

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

        conf_weight_clause = func.pow(
            func.coalesce(extracted_text.c.confidence, 1),
            args.confidence_weight,
        )
        lang_conf_weight_clause = func.pow(
            func.coalesce(extracted_text.c.language_confidence, 1),
            args.language_confidence_weight,
        )
        if args.confidence_weight != 0 and args.language_confidence_weight != 0:
            weights = conf_weight_clause * lang_conf_weight_clause
            rank_column = func.sum(vec_distance * weights) / func.sum(weights)
        elif args.confidence_weight != 0:
            rank_column = func.sum(
                vec_distance * conf_weight_clause
            ) / func.sum(conf_weight_clause)
        elif args.language_confidence_weight != 0:
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

    from panoptikon.data_extractors.models import ModelOptsFactory

    logger.debug(f"Getting embedding for text: {text}")
    start_time = time.time()
    model = ModelOptsFactory.get_model(model_name)
    embed_bytes: bytes = model.run_batch_inference(
        cache_args.cache_key,
        cache_args.lru_size,
        cache_args.ttl_seconds,
        [({"text": text, "task": "s2s"}, None)],
    )[0]
    text_embed = deserialize_array(embed_bytes)[0]
    assert isinstance(text_embed, np.ndarray)
    # Set as persistent so that the model is not reloaded every time the function is called
    last_embedded_text = text
    last_used_model = model_name
    last_embedded_text_embed = serialize_f32(text_embed.tolist())
    logger.debug(
        f"Embedding generation took {time.time() - start_time} seconds"
    )
    return last_embedded_text_embed
