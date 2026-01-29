"""
Multi-source news ingestion pipeline - TEST VERSION

CHANGES FROM PRODUCTION:
- Uses test database: proportion_db_test
- Uses test collection: articles_raw_test
- No impact on production data or downstream processes

This allows safe testing of NewsData.io integration without
affecting existing clustering, processing, or analytics workflows.
"""

import time
import yaml
from datetime import datetime, timezone
from pathlib import Path

from ingestion.news_sources.ticker_loader import load_entities
from ingestion.news_sources.gnews_fetcher import fetch_google_news_articles
from ingestion.news_sources.newsdata_fetcher import fetch_newsdata_articles
from ingestion.news_sources.article_processor import process_article
from ingestion.news_sources.content_hash import compute_content_hash
from ingestion.utils.time_normalizer import normalize_published_at

# ====== TEST DATABASE CONFIG ======
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "proportion_db_test"  # ← SEPARATE TEST DATABASE
COLLECTION_NAME = "articles_raw_test"  # ← TEST COLLECTION

def get_test_collection():
    """Get test collection (separate from production)"""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    return collection
# ==================================


# ---------------- CONFIG ----------------
MAX_ARTICLES_PER_ENTITY = 25
MIN_TEXT_LENGTH = 500
SLEEP_BETWEEN_ENTITIES = 3
DATA_SOURCES_CONFIG = Path("config/data_sources.yaml")
# --------------------------------------


def load_data_sources_config():
    """Load data sources configuration from YAML file."""
    try:
        with open(DATA_SOURCES_CONFIG, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        print("⚠️  data_sources.yaml not found, using GNews only")
        return {
            "data_sources": [
                {"name": "gnews", "enabled": True, "priority": 1}
            ],
            "request_strategy": {"mode": "priority"}
        }


def get_enabled_sources(config):
    """Get list of enabled data sources sorted by priority."""
    sources = [s for s in config.get("data_sources", []) if s.get("enabled")]
    sources.sort(key=lambda x: x.get("priority", 999))
    return sources


def fetch_from_source(source_name, query, entity):
    """Fetch articles from specified news source."""
    if source_name == "gnews":
        return fetch_google_news_articles(
            query=query,
            entity_id=entity["entity_id"],
            entity_name=entity["entity_name"],
            ticker=entity.get("ticker"),
            entity_type=entity["entity_type"],
            max_articles=MAX_ARTICLES_PER_ENTITY,
        )
    elif source_name == "newsdata":
        return fetch_newsdata_articles(
            query=query,
            entity_id=entity["entity_id"],
            entity_name=entity["entity_name"],
            ticker=entity.get("ticker"),
            entity_type=entity["entity_type"],
            max_articles=MAX_ARTICLES_PER_ENTITY,
        )
    else:
        print(f"⚠️  Unknown source: {source_name}")
        return [], 0


def ingest_test() -> None:
    """
    Run multi-source ingestion in TEST database.
    
    SAFE TO RUN:
    - Uses proportion_db_test database
    - Uses articles_raw_test collection
    - No impact on production data
    - Existing clustering/processing scripts unaffected
    """

    print("=" * 80)
    print("🧪 MULTI-SOURCE INGESTION - TEST MODE")
    print("=" * 80)
    print(f"⚠️  Database: {DB_NAME}")
    print(f"⚠️  Collection: {COLLECTION_NAME}")
    print(f"⚠️  This will NOT affect your production database!")
    print("=" * 80)

    # Load configuration
    entities = load_entities()
    if not entities:
        print("⚠️ No entities loaded. Exiting ingestion.")
        return

    config = load_data_sources_config()
    enabled_sources = get_enabled_sources(config)
    
    if not enabled_sources:
        print("⚠️ No data sources enabled. Check data_sources.yaml")
        return

    print(f"\n📡 Enabled data sources: {[s['name'] for s in enabled_sources]}")
    
    strategy = config.get("request_strategy", {}).get("mode", "round_robin")
    print(f"🔄 Request strategy: {strategy}")

    collection = get_test_collection()  # ← TEST COLLECTION

    # Metrics tracking
    total_requests = 0
    total_fetched = 0
    total_upserts = 0
    total_skipped = 0
    source_usage = {s["name"]: 0 for s in enabled_sources}
    
    # Round-robin state
    current_source_idx = 0

    run_start = datetime.now(timezone.utc)
    print(f"\n🚀 Test ingestion started at {run_start.isoformat()}")

    for entity in entities:
        print(f"\n📹 Entity: {entity['entity_name']} ({entity['entity_type']})")

        for query in entity["query_terms"]:
            
            # ---------- SOURCE SELECTION ----------
            if strategy == "round_robin":
                source = enabled_sources[current_source_idx]
                current_source_idx = (current_source_idx + 1) % len(enabled_sources)
                sources_to_query = [source]
                
            elif strategy == "priority":
                sources_to_query = [enabled_sources[0]]
                
            elif strategy == "all":
                sources_to_query = enabled_sources
            else:
                sources_to_query = [enabled_sources[0]]

            # ---------- FETCH FROM SELECTED SOURCE(S) ----------
            all_articles = []
            
            for source in sources_to_query:
                source_name = source["name"]
                
                try:
                    print(f"  📡 Querying {source_name} for: '{query}'")
                    articles, request_count = fetch_from_source(
                        source_name, query, entity
                    )
                    
                    total_requests += request_count
                    source_usage[source_name] += request_count
                    total_fetched += len(articles)
                    
                    print(f"    ✅ {len(articles)} articles from {source_name}")
                    all_articles.extend(articles)
                    
                except Exception as e:
                    print(f"    ❌ Fetch failed for '{query}' from {source_name}: {e}")
                    continue

            # ---------- PROCESS ARTICLES ----------
            for article in all_articles:
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

                # 🔑 UPSERT BY content_hash
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

    print("\n" + "=" * 80)
    print("🧪 TEST INGESTION FINISHED")
    print("=" * 80)
    print(f"Database        : {DB_NAME}")
    print(f"Collection      : {COLLECTION_NAME}")
    print(f"Run time        : {run_start.isoformat()} → {run_end.isoformat()}")
    print(f"API requests    : {total_requests}")
    print(f"Articles fetched: {total_fetched}")
    print(f"Articles upserted: {total_upserts}")
    print(f"Articles skipped: {total_skipped}")
    print("\n📊 SOURCE USAGE:")
    for source_name, count in source_usage.items():
        print(f"  {source_name}: {count} requests")
    print("=" * 80)
    print("\n✅ Test complete! Check MongoDB:")
    print(f"   db.getSiblingDB('{DB_NAME}').{COLLECTION_NAME}.count()")
    print("=" * 80)


if __name__ == "__main__":
    ingest_test()