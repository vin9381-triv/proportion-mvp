from sklearn.cluster import KMeans

def run_kmeans(X, k):
    """
    Coarse clustering step.
    Returns cluster labels.
    """
    model = KMeans(
        n_clusters=k,
        random_state=42,
        n_init="auto"
    )
    return model.fit_predict(X)
