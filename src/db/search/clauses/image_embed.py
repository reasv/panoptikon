from typing import List

from src.db.search.types import ImageEmbeddingFilter


def build_image_embedding_clause(args: ImageEmbeddingFilter | None):
    """
    Build a subquery to match image embeddings.
    """

    if not args or not args.query:
        return "", [], ""
    params: List[str] = [args.model]

    subclause = f"""
        JOIN image_embeddings
        ON image_embeddings.item_id = files.item_id
        JOIN setters AS image_setters
        ON image_embeddings.setter_id = image_setters.id
        AND image_setters.name = ?
    """
    add_column = ",\n MIN(vec_distance_L2(image_embeddings.embedding, ?)) AS image_vec_distance"
    return subclause, params, add_column
