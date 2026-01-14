import time
from datetime import datetime, timezone
from pymongo.errors import DuplicateKeyError

from ingestion.news_sources.ticker_loader import load_companies
from ingestion.news_sources.gnews_fetcher import fetch_google_news_articles
from ingestion.news_sources.article_processor import process_article
from ingestion.news_sources.mongo_client import get_articles_collection
from ingestion.news_sources.content_hash import compute_content_hash

from ingestion.utils.time_normalizer import normalize_published_at


# ---------------- CONFIG ----------------
MAX_ARTICLES_PER_COMPANY = 25
MIN_TEXT_LENGTH = 500
SLEEP_BETWEEN_COMPANIES = 3

# 🔬 TEST MODE
TEST_MODE = False          # ⛔ set False after validation
TEST_MAX_COMPANIES = 1
TEST_MAX_ARTICLES = 1
# --------------------------------------


def ingest():
    companies = load_companies()

    if TEST_MODE:
        companies = companies[:TEST_MAX_COMPANIES]
        print("🧪 TEST MODE ENABLED")
        print(f"   Companies limited to {TEST_MAX_COMPANIES}")
        print(f"   Articles per company limited to {TEST_MAX_ARTICLES}")

    collection = get_articles_collection()

    total_requests = 0
    total_fetched = 0
    total_inserted = 0
    total_skipped = 0

    run_start = datetime.now(timezone.utc)
    print(f"\n🚀 Ingestion started at {run_start.isoformat()}")

    for company in companies:
        print(f"\n🔹 Company: {company['company_name']} ({company['ticker']})")

        try:
            articles, req_count = fetch_google_news_articles(
                query=company["company_name"],
                entity_id=company["entity_id"],
                company_name=company["company_name"],
                ticker=company["ticker"],
                max_articles=TEST_MAX_ARTICLES if TEST_MODE else MAX_ARTICLES_PER_COMPANY,
            )
        except Exception as e:
            print(f"❌ Skipping company {company['company_name']} due to error: {e}")
            continue

        total_requests += req_count
        total_fetched += len(articles)

        print(f"📥 Fetched {len(articles)} articles")

        for article in articles:
            try:
                processed = process_article(article["url"])
                if not processed or len(processed["raw_text"]) < MIN_TEXT_LENGTH:
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
                    "entity_id": article["entity_id"],
                    "company_name": article["company_name"],
                    "ticker": article["ticker"],

                    # ---- Source ----
                    "source": article["source"],
                    "source_type": article["source_type"],
                    "publisher": article["publisher"],

                    # ---- Article ----
                    "title": article["title"],
                    "url": article["url"],

                    # ---- Time (IMPORTANT) ----
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

                if TEST_MODE:
                    print("\n🧪 TEST INSERT")
                    print(f"   Title              : {doc['title'][:100]}")
                    print(f"   published_at_raw   : {doc['published_at_raw']}")
                    print(f"   published_at_utc   : {doc['published_at_utc']}")
                    print(f"   ingested_at        : {doc['ingested_at']}")

            except DuplicateKeyError:
                total_skipped += 1

            except Exception as e:
                total_skipped += 1
                print(f"⚠️ Error processing article: {e}")

        time.sleep(SLEEP_BETWEEN_COMPANIES)

    run_end = datetime.now(timezone.utc)

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
