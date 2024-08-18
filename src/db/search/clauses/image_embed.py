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
        JOIN item_data AS clip_data
            ON clip_data.data_type = 'clip'
            AND clip_data.item_id = files.item_id 
        JOIN setters AS clip_setters
            ON clip_data.setter_id = clip_setters.id
            AND clip_setters.name = ?
        JOIN embeddings AS image_embeddings
            ON image_embeddings.id = clip_data.id
    """
    add_column = ",\n MIN(vec_distance_cosine(vec_normalize(image_embeddings.embedding), vec_normalize(?))) AS image_vec_distance"
    return subclause, params, add_column
