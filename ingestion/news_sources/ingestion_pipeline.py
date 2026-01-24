import time
from datetime import datetime, timezone
from pymongo.errors import DuplicateKeyError

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

    Responsibilities:
    - Load configured entities (companies, industries, etc.)
    - Fetch recent news articles per entity/query
    - Normalize and enrich article metadata
    - Persist clean, deduplicable documents to MongoDB

    Design principles:
    - Entity-agnostic (no company-specific logic)
    - No interpretation or tagging at ingestion time
    - Safe retries and non-fatal error handling
    - Deterministic, restart-safe behavior

    This function is intended to be:
    - Scheduled (cron / job runner)
    - Idempotent at the article level (via downstream deduplication)
    """

    # ---- Load entities from config ----
    entities = load_entities()
    if not entities:
        print("⚠️ No entities loaded. Exiting ingestion.")
        return

    collection = get_articles_collection()

    total_requests = 0
    total_fetched = 0
    total_inserted = 0
    total_skipped = 0

    run_start = datetime.now(timezone.utc)
    print(f"\n🚀 Ingestion started at {run_start.isoformat()}")

    # ---- Main ingestion loop ----
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
                # Fetch-level failures are non-fatal
                print(f"❌ Fetch failed for '{query}': {e}")
                continue

            total_requests += request_count
            total_fetched += len(articles)

            print(f"📥 Fetched {len(articles)} articles for query: '{query}'")

            # ---- Article processing loop ----
            for article in articles:
                try:
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
                            "clustered": False,
                        },
                    }

                    collection.insert_one(doc)
                    total_inserted += 1

                except DuplicateKeyError:
                    # Expected during reruns / overlap
                    total_skipped += 1

                except Exception as e:
                    total_skipped += 1
                    print(f"⚠️ Error processing article: {e}")

            # Gentle throttling between queries
            time.sleep(SLEEP_BETWEEN_ENTITIES)

    run_end = datetime.now(timezone.utc)

    # ---- Run summary ----
    print("\n✅ Ingestion finished")
    print("──────── SUMMARY ────────")
    print(f"Run time        : {run_start.isoformat()} → {run_end.isoformat()}")
    print(f"API requests    : {total_requests}")
    print(f"Articles fetched: {total_fetched}")
    print(f"Articles stored : {total_inserted}")
    print(f"Articles skipped: {total_skipped}")
    print("─────────────────────────")


if __name__ == "__main__":
    ingest()
