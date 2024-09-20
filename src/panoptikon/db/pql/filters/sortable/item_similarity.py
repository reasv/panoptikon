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


class SourceArgs(BaseModel):
    setter_names: Optional[List[str]] = Field(
        default=None,
        description="The source model names to restrict the search to. These are the models that produced the text.",
    )
    languages: Optional[List[str]] = Field(
        default=None,
        description="The source languages to restrict the search to. These are the languages of the text produced by the source models.",
    )
    min_confidence: float = Field(
        default=0.0,
        description="The minimum confidence of the text as given by its source model",
    )
    min_language_confidence: float = Field(
        default=0.0,
        description="The minimum confidence for language detection in the text",
    )
    min_length: int = Field(
        default=0,
        description="The minimum length of the text in characters",
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
weights = POW((COALESCE(main_source_text.confidence, 1) * COALESCE(other_source_text.confidence, 1)), src_confidence_weight)
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


class SimilarityArgs(BaseModel):
    target: str = Field(
        ...,
        description="Sha256 hash of the target item to find similar items for",
    )
    setter_name: str = Field(
        ...,
        description="The name of the embedding model used for similarity search",
    )
    distance_aggregation: Literal["MIN", "MAX", "AVG"] = Field(
        default="AVG",
        description="The method to aggregate distances when an item has multiple embeddings. Default is AVG.",
    )
    src_text: Optional[SourceArgs] = Field(
        default=None,
        description="""
Filters and options to apply on source text.
If not provided, all text embeddings are considered.
The source text is the text which was used to produce the text embeddings.
""",
    )

    clip_xmodal: bool = Field(
        default=False,
        description="""
Whether to use cross-modal similarity for CLIP models.
Default is False. What this means is that the similarity is calculated between image and text embeddings,
rather than just between image embeddings. By default will also use text-to-text similarity.

Note that you must have both image and text embeddings with the same CLIP model for this setting to work.
Text embeddings are derived from text which must have been already previously produced by another model, such as an OCR model or a tagger.
They are generated *separately* from the image embeddings, using a different job (Under 'CLIP Text Embeddings').
Run a batch job with the same clip model for both image and text embeddings to use this setting.
        """,
    )
    xmodal_t2t: bool = Field(
        default=True,
        description="""
When using CLIP cross-modal similarity, whether to use text-to-text similarity as well or just image-to-text and image-to-image.
        """,
    )
    xmodal_i2i: bool = Field(
        default=False,
        description="""
When using CLIP cross-modal similarity, whether to use image-to-image similarity as well or just image-to-text and text-to-text.
        """,
    )


class SimilarTo(SortableFilter):
    order_by: bool = get_order_by_field(True)
    direction: OrderTypeNN = get_order_direction_field("asc")
    similar_to: SimilarityArgs = Field(
        ...,
        title="Item Similarity Search",
        description="""
Search for items similar to a target item using similarity search on embeddings.
The search is based on the image or text embeddings of the provided item.

The count value in the response is equal to the number 
of items returned (+ (page_size - 1) * page for page > 1),
rather than the total number of similar items in the database.
This is because there is no way to define what 
constitutes a "similar" item in a general sense.
We just return the top N items that are most similar to the provided item.
If you still need the total number of "similar" items in the database,
set the `full_count` parameter to true.

The setter name refers to the model that produced the embeddings.
You can find a list of available values for this parameter using the /api/search/stats endpoint.
Any setters of type "text-embedding" or "clip" can be used for this search.

The `limit` parameter can be used to control the number of similar items to return.

"text" embeddings are derived from text produced by another model, such as an OCR model or a tagger.
You can restrict the search to embeddings derived from text that was 
produced by one of a list of specific models by providing the appropriate filter.
You can find a list of available values for text sources using the 
/api/search/stats endpoint, specifically any setter of type "text" will apply.
Remember that tagging models also produce text by concatenating the tags,
 and are therefore also returned as "text" models by the stats endpoint.
Restricting similarity to a tagger model or a set of tagger models
 is recommended for item similarity search based on text embeddings.

""",
    )

    def _validate(self):
        if len(self.similar_to.target.strip()) == 0:
            return self.set_validated(False)

        if len(self.similar_to.setter_name.strip()) == 0:
            return self.set_validated(False)

        return self.set_validated(True)

    def build_query(self, context: CTE, state: QueryState) -> CTE:
        self.raise_if_not_validated()
        from panoptikon.db.pql.tables import (
            embeddings,
            extracted_text,
            item_data,
            setters,
        )

        args = self.similar_to

        return self.wrap_query(
            select(*get_std_cols(context, state)), context, state
        )
