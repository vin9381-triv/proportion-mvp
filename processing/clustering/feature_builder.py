import numpy as np

def build_feature_matrix(articles):
    """
    Returns:
    - X: np.ndarray of shape (n_articles, embedding_dim)
    - article_ids: aligned list of article IDs
    """
    vectors = []
    article_ids = []

    for article in articles:
        vectors.append(article["embeddings"]["body"])
        article_ids.append(article["_id"])

    return np.array(vectors), article_ids
