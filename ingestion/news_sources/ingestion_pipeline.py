import time
from datetime import datetime, timezone

from ingestion.news_sources.ticker_loader import load_entities
from ingestion.news_sources.gnews_fetcher import fetch_google_news_articles
from ingestion.news_sources.article_processor import process_article
from ingestion.news_sources.mongo_client import get_articles_collection
from ingestion.news_sources.content_hash import compute_content_hash
from ingestion.utils.time_normalizer import normalize_published_at


# ---------------- CONFIG ----------------
MAX_ARTICLES_PER_ENTITY = 25
MIN_TEXT_LENGTH = 500
SLEEP_BETWEEN_ENTITIES = 3
# --------------------------------------


def ingest() -> None:
    """
    Run the news ingestion pipeline.

    Guarantees:
    - Exactly ONE raw document per real-world article
    - Idempotent across reruns
    - Safe across overlapping queries and entities
    """

    entities = load_entities()
    if not entities:
        print("⚠️ No entities loaded. Exiting ingestion.")
        return

    collection = get_articles_collection()

    total_requests = 0
    total_fetched = 0
    total_upserts = 0
    total_skipped = 0

    run_start = datetime.now(timezone.utc)
    print(f"\n🚀 Ingestion started at {run_start.isoformat()}")

    for entity in entities:
        print(f"\n🔹 Entity: {entity['entity_name']} ({entity['entity_type']})")

        for query in entity["query_terms"]:
            try:
                articles, request_count = fetch_google_news_articles(
                    query=query,
                    entity_id=entity["entity_id"],
                    entity_name=entity["entity_name"],
                    ticker=entity.get("ticker"),
                    entity_type=entity["entity_type"],
                    max_articles=MAX_ARTICLES_PER_ENTITY,
                )
            except Exception as e:
                print(f"❌ Fetch failed for '{query}': {e}")
                continue

            total_requests += request_count
            total_fetched += len(articles)

            print(f"📥 Fetched {len(articles)} articles for query: '{query}'")

            for article in articles:
                processed = process_article(article["url"])
                if not processed:
                    total_skipped += 1
                    continue

                if len(processed["raw_text"]) < MIN_TEXT_LENGTH:
                    total_skipped += 1
                    continue

                content_hash = compute_content_hash(processed["raw_text"])
                ingested_at = datetime.now(timezone.utc)

                published_at_utc = normalize_published_at(
                    article.get("published_at"),
                    ingested_at,
                )

                doc = {
                    # ---- Entity ----
                    "entity_id": entity["entity_id"],
                    "entity_name": entity["entity_name"],
                    "entity_type": entity["entity_type"],
                    "ticker": entity.get("ticker"),
                    "sector": entity.get("sector"),

                    # ---- Source ----
                    "source": article["source"],
                    "source_type": article["source_type"],
                    "publisher": article["publisher"],

                    # ---- Article ----
                    "title": article["title"],
                    "url": article["url"],

                    # ---- Time ----
                    "published_at_raw": article.get("published_at"),
                    "published_at_utc": published_at_utc,
                    "ingested_at": ingested_at,

                    # ---- Content ----
                    "raw_text": processed["raw_text"],
                    "summary": processed["summary"],
                    "language": processed["language"],
                    "text_length": len(processed["raw_text"]),
                    "content_hash": content_hash,

                    # ---- Processing Flags ----
                    "processing": {
                        "embedded": False,
                        "semantically_deduped": False,
                        "clustered": False,
                    },
                }

                # 🔐 CRITICAL FIX: UPSERT BY content_hash
                result = collection.update_one(
                    {"content_hash": content_hash},
                    {"$setOnInsert": doc},
                    upsert=True,
                )

                if result.upserted_id:
                    total_upserts += 1
                else:
                    total_skipped += 1

            time.sleep(SLEEP_BETWEEN_ENTITIES)

    run_end = datetime.now(timezone.utc)

    print("\n✅ Ingestion finished")
    print("──────── SUMMARY ────────")
    print(f"Run time        : {run_start.isoformat()} → {run_end.isoformat()}")
    print(f"API requests    : {total_requests}")
    print(f"Articles fetched: {total_fetched}")
    print(f"Articles upserted: {total_upserts}")
    print(f"Articles skipped: {total_skipped}")
    print("─────────────────────────")


if __name__ == "__main__":
    ingest()
