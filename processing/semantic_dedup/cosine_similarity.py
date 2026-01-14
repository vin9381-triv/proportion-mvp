# processing/semantic_dedup/cosine_similarity.py

import numpy as np


def cosine_similarity(vec_a, vec_b) -> float:
    a = np.array(vec_a)
    b = np.array(vec_b)

    if a.shape != b.shape:
        raise ValueError("Embedding vectors must have same shape")

    denom = (np.linalg.norm(a) * np.linalg.norm(b))
    if denom == 0:
        return 0.0

    return float(np.dot(a, b) / denom)
