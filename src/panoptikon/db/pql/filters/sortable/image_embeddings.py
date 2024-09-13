import io
import logging
from typing import List, Literal, Optional

import numpy as np
import PIL
import PIL.Image
from pydantic import BaseModel, Field, PrivateAttr
from sqlalchemy import and_, func, literal
from sqlalchemy.sql.expression import CTE, select

from inferio.impl.utils import deserialize_array
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
        description="Semantic query to match against the image",
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

    def validate(self):
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

        return self.set_validated(True)

    def build_query(self, context: CTE, state: QueryState) -> CTE:
        self.raise_if_not_validated()
        from panoptikon.db.pql.tables import embeddings, item_data, setters

        args = self.image_embeddings

        # Gets all results with the requested embeddings
        embeddings_query = (
            select(
                *get_std_cols(context, state),
            )
            .join(
                item_data,
                (item_data.c.item_id == context.c.item_id)
                & (item_data.c.data_type == "clip"),
            )
            .join(
                setters,
                (setters.c.id == item_data.c.setter_id)
                & (setters.c.name == args.model),
            )
            .join(embeddings, item_data.c.id == embeddings.c.id)
        )

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
        emb_cte = (
            embeddings_query.with_only_columns(
                context.c.item_id.label("item_id"),
                embeddings.c.id.label("emb_id"),
            )
            .group_by(
                context.c.item_id,
                embeddings.c.id,
            )
            .cte(f"unqemb_{self.get_cte_name(state.cte_counter)}")
        )

        vec_distance = func.vec_distance_cosine(
            func.vec_normalize(embeddings.c.embedding),
            func.vec_normalize(literal(args._embedding)),
        )
        if args.distance_aggregation == "MAX":
            rank_column = func.max(vec_distance)
        elif args.distance_aggregation == "AVG":
            rank_column = func.avg(vec_distance)
        elif args.distance_aggregation == "MIN":
            rank_column = func.min(vec_distance)

        # Now we join back with the embeddings table and get the distance
        # between the query embedding and the embeddings in the database

        dist_select = (
            select(
                emb_cte.c.item_id,
                emb_cte.c.emb_id,
                rank_column.label("min_distance"),
            )
            .join(
                embeddings,
                embeddings.c.id == emb_cte.c.emb_id,
            )
            .group_by(emb_cte.c.item_id)
        ).cte(f"dist_{self.get_cte_name(state.cte_counter)}")

        # Now we join with the original query to give the min distance for each item
        res = select(
            *get_std_cols(context, state),
            self.derive_rank_column(dist_select.c.min_distance),
        ).join(
            dist_select,
            context.c.item_id == dist_select.c.item_id,
        )
        return self.wrap_query(res, context, state)


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
