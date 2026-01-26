from datetime import datetime, timedelta
import os
import yaml

from processing.common.mongo_client import get_collection
from processing.clustering.input_resolver import get_raw_article_ids_for_entity
from processing.clustering.queries import get_clustering_candidates_query
from processing.clustering.feature_builder import build_feature_matrix
from processing.clustering.density_stage import run_dbscan
from processing.clustering.summarizer import summarize_clusters_to_file


# ---------- CONFIG ----------
WINDOW_DAYS = 3
TICKER_CONFIG_PATH = "config/ticker.yaml"
OUTPUT_DIR = "processing/clustering/outputs_clusters"
MIN_ARTICLES_PER_TICKER = 5


def load_companies(ticker_config_path):
    with open(ticker_config_path, "r") as f:
        config = yaml.safe_load(f)
    return config.get("companies", [])


def run():
    # ---- time window ----
    end_utc = datetime.utcnow()
    start_utc = end_utc - timedelta(days=WINDOW_DAYS)

    print(f"Clustering window: {start_utc.date()} → {end_utc.date()}")

    companies = load_companies(TICKER_CONFIG_PATH)
    embedded_col = get_collection("articles_embedded")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for company in companies:
        ticker = company["ticker"]
        name = company["name"]

        print(f"\n--- Processing ticker: {ticker} ({name}) ---")

        # ---- resolve raw article IDs for THIS ticker ----
        raw_article_ids = get_raw_article_ids_for_entity(
            start_utc=start_utc,
            end_utc=end_utc,
            entity_type="company",
            tickers=[ticker],
            entity_id=None
        )

        print(f"Raw articles found: {len(raw_article_ids)}")

        if len(raw_article_ids) < MIN_ARTICLES_PER_TICKER:
            print("Not enough raw articles, skipping.")
            continue

        # ---- fetch embedded articles (dedup-safe) ----
        articles = list(
            embedded_col.find(
                get_clustering_candidates_query(raw_article_ids)
            )
        )

        print(f"Embedded articles after dedup: {len(articles)}")

        if len(articles) < MIN_ARTICLES_PER_TICKER:
            print("Not enough embedded articles, skipping.")
            continue

        # ---- build features (FIXED) ----
        X, valid_articles = build_feature_matrix(articles)

        if len(X) < MIN_ARTICLES_PER_TICKER:
            print("Not enough valid embeddings, skipping.")
            continue

        # ---- clustering ----
        labels = run_dbscan(X)

        # ---- output ----
        filename = (
            f"clustering_run_{ticker}_"
            f"{start_utc.date()}_to_{end_utc.date()}.txt"
        )

        output_path = os.path.join(OUTPUT_DIR, filename)

        summarize_clusters_to_file(
            labels=labels,
            articles=valid_articles,
            output_path=output_path
        )

        print(f"Written: {output_path}")


if __name__ == "__main__":
    run()
