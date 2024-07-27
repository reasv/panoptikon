def get_text_embedding_model():
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer("all-mpnet-base-v2")
    return model, "text-embedding", "all-mpnet-base-v2"
