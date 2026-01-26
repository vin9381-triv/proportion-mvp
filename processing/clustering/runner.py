from processing.common.mongo_client import get_collection
from .queries import get_clustering_candidates_query
from .feature_builder import build_feature_matrix
from .kmeans_stage import run_kmeans
from .density_stage import run_dbscan
from .summarizer import summarize_clusters_to_file

from datetime import datetime
import os

def run():
    articles_col = get_collection("articles_embedded")

    articles = list(
        articles_col.find(get_clustering_candidates_query())
    )

    print(f"Clustering candidates: {len(articles)}")

    X, _ = build_feature_matrix(articles)

    k = int(len(articles) ** 0.5)
    print(f"KMeans k={k}")

    run_kmeans(X, k=k)

    final_labels = run_dbscan(X)

    output_dir = "processing/clustering/outputs"
    os.makedirs(output_dir, exist_ok=True)

    run_date = datetime.utcnow().strftime("%Y-%m-%d")
    output_filename = f"clustering_run_{run_date}.txt"

    output_path = os.path.join(output_dir, output_filename)

    summarize_clusters_to_file(
        final_labels,
        articles,
        output_path
    )

    print(f"Cluster summary written to: {output_path}")

if __name__ == "__main__":
    run()
