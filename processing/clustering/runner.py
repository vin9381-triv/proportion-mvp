from datetime import datetime, timedelta
import os
import yaml
from collections import defaultdict

from processing.common.mongo_client import get_collection
from processing.clustering.input_resolver import get_raw_article_ids_for_entity
from processing.clustering.queries import get_clustering_candidates_query
from processing.clustering.tagger import tag_article


# ---------- CONFIG ----------
TICKER_CONFIG_PATH = "config/ticker.yaml"
OUTPUT_DIR = "/home/vineet-trivedi/Proportion/processing/clustering/outputs"

MIN_ARTICLES = 5
COMPANY_WINDOW_DAYS = 3
INDUSTRY_WINDOW_DAYS = 7


def load_entities(path):
    with open(path, "r") as f:
        config = yaml.safe_load(f)

    entities = []
    entities.extend(config.get("companies", []))
    entities.extend(config.get("industries", []))
    return entities


def write_tagging_preview(
    *,
    entity_name: str,
    identifier: str,
    entity_type: str,
    start_utc,
    end_utc,
    tag_buckets: dict
):
    filename = (
        f"tagging_preview_{identifier}_"
        f"{start_utc.date()}_to_{end_utc.date()}.txt"
    )

    path = os.path.join(OUTPUT_DIR, filename)

    with open(path, "w", encoding="utf-8") as f:
        f.write("Proportion — Tagging Preview (Phase 1)\n")
        f.write("=" * 80 + "\n")
        f.write(f"Entity      : {entity_name}\n")
        f.write(f"Type        : {entity_type}\n")
        f.write(f"Window      : {start_utc.date()} → {end_utc.date()}\n")
        f.write("=" * 80 + "\n\n")

        for tag, items in sorted(
            tag_buckets.items(),
            key=lambda x: len(x[1]),
            reverse=True
        ):
            f.write(f"[{tag}] — {len(items)} articles\n")
            f.write("-" * 60 + "\n")

            for art in items:
                title = art.get("title", "").strip()
                published = art.get("published_at_utc", "N/A")
                f.write(f"- {title}\n")
                f.write(f"  Published: {published}\n")

            f.write("\n")

    print(f"Tagging preview written: {path}")


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

        print(f"\n==============================")
        print(f"Entity: {name}")
        print(f"Type  : {entity_type}")
        print(f"Window: {start_utc.date()} → {end_utc.date()}")

        raw_article_ids = get_raw_article_ids_for_entity(
            start_utc=start_utc,
            end_utc=end_utc,
            entity_type=entity_type,
            tickers=tickers,
            entity_id=entity_id
        )

        if len(raw_article_ids) < MIN_ARTICLES:
            print("Skipping: not enough raw articles")
            continue

        articles = list(
            embedded_col.find(
                get_clustering_candidates_query(raw_article_ids)
            )
        )

        if len(articles) < MIN_ARTICLES:
            print("Skipping: not enough embedded articles")
            continue

        # -------- TAGGING --------
        tag_buckets = defaultdict(list)

        for article in articles:
            tag = tag_article(article)
            tag_buckets[tag].append(article)

        print("Tag distribution:")
        for tag, items in tag_buckets.items():
            print(f"  {tag:<25} {len(items)}")

        # -------- WRITE PREVIEW FILE --------
        write_tagging_preview(
            entity_name=name,
            identifier=identifier,
            entity_type=entity_type,
            start_utc=start_utc,
            end_utc=end_utc,
            tag_buckets=tag_buckets
        )

        print("Phase 1 tagging complete for this entity.")


if __name__ == "__main__":
    run()
