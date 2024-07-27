from typing import List

from src.db.search.types import ImageEmbeddingFilter


def build_image_embedding_clause(args: ImageEmbeddingFilter | None):
    """
    Build a subquery to match image embeddings.
    """

    params: List[str] = []

    if not args or not args.query:
        return "", params, ""

    params.extend(list(args.target))

    subclause = f"""
        JOIN image_embeddings
        ON image_embeddings.item_id = files.item_id
        JOIN setters as image_setters
        ON image_embeddings.setter_id = image_setters.id
        AND image_setters.type = ? AND image_setters.name = ?
    """
    add_column = ",\n MIN(vec_distance_L2(image_embeddings.embedding, ?)) AS image_vec_distance"
    return subclause, params, add_column
