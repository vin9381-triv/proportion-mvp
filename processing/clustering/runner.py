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
TICKER_CONFIG_PATH = "config/ticker.yaml"
OUTPUT_DIR = "outputs"

MIN_ARTICLES = 5
COMPANY_WINDOW_DAYS = 3
INDUSTRY_WINDOW_DAYS = 7


def load_entities(path):
    with open(path, "r") as f:
        config = yaml.safe_load(f)

    entities = []

    for c in config.get("companies", []):
        entities.append(c)

    for i in config.get("industries", []):
        entities.append(i)

    return entities


def run():
    entities = load_entities(TICKER_CONFIG_PATH)
    embedded_col = get_collection("articles_embedded")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for entity in entities:
        entity_type = entity["entity_type"]
        name = entity["name"]

        if entity_type == "company":
            window_days = COMPANY_WINDOW_DAYS
            identifier = entity["ticker"]
            tickers = [entity["ticker"]]
            entity_id = None

        elif entity_type == "industry":
            window_days = INDUSTRY_WINDOW_DAYS
            identifier = entity["entity_id"]
            tickers = None
            entity_id = entity["entity_id"]

        else:
            continue

        end_utc = datetime.utcnow()
        start_utc = end_utc - timedelta(days=window_days)

        print(f"\n--- Clustering {name} ({entity_type}) ---")
        print(f"Window: {start_utc.date()} → {end_utc.date()}")

        raw_article_ids = get_raw_article_ids_for_entity(
            start_utc=start_utc,
            end_utc=end_utc,
            entity_type=entity_type,
            tickers=tickers,
            entity_id=entity_id
        )

        print(f"Raw articles: {len(raw_article_ids)}")

        if len(raw_article_ids) < MIN_ARTICLES:
            print("Skipping: not enough data")
            continue

        articles = list(
            embedded_col.find(
                get_clustering_candidates_query(raw_article_ids)
            )
        )

        print(f"Embedded after dedup: {len(articles)}")

        if len(articles) < MIN_ARTICLES:
            print("Skipping after dedup")
            continue

        X, _ = build_feature_matrix(articles)
        labels = run_dbscan(X)

        filename = (
            f"clustering_run_{identifier}_"
            f"{start_utc.date()}_to_{end_utc.date()}.txt"
        )

        output_path = os.path.join(OUTPUT_DIR, filename)

        summarize_clusters_to_file(
            labels=labels,
            articles=articles,
            output_path=output_path
        )

        print(f"Written: {output_path}")


if __name__ == "__main__":
    run()
