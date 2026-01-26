from sklearn.cluster import DBSCAN

def run_dbscan(X, eps=0.3, min_samples=3):
    return DBSCAN(
        eps=eps,
        min_samples=min_samples,
        metric="cosine"
    ).fit_predict(X)
