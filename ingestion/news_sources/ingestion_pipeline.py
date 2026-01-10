import time
from datetime import datetime, timezone
from pymongo.errors import DuplicateKeyError

from ticker_loader import load_companies
from gnews_fetcher import fetch_google_news_articles
from article_processor import process_article
from mongo_client import get_articles_collection
from content_hash import compute_content_hash


# ---------------- CONFIG ----------------
MAX_ARTICLES_PER_COMPANY = 25
MIN_TEXT_LENGTH = 500
SLEEP_BETWEEN_COMPANIES = 3
# ----------------------------------------


def ingest():
    companies = load_companies()
    collection = get_articles_collection()

    total_requests = 0
    total_fetched = 0
    total_inserted = 0
    total_skipped = 0

    run_start = datetime.now(timezone.utc)
    print(f"\n🚀 Ingestion started at {run_start.isoformat()}")

    for company in companies:
        print(f"\n🔹 Company: {company['company_name']} ({company['ticker']})")

        query = f"{company['company_name']} {company['ticker']}"

        try:
            articles, req_count = fetch_google_news_articles(
                query=company["company_name"],
                entity_id=company["entity_id"],
                company_name=company["company_name"],
                ticker=company["ticker"],
                max_articles=MAX_ARTICLES_PER_COMPANY,
    )
        except Exception as e:
            print(f"❌ Skipping company {company['company_name']} due to error: {e}")
            continue

        total_requests += req_count
        total_fetched += len(articles)

        print(f"📥 Fetched {len(articles)} articles (requests so far: {total_requests})")

        for article in articles:
            try:
                processed = process_article(article["url"])
                if not processed or len(processed["raw_text"]) < MIN_TEXT_LENGTH:
                    total_skipped += 1
                    continue

                content_hash = compute_content_hash(processed["raw_text"])

                doc = {
                    "entity_id": article["entity_id"],
                    "company_name": article["company_name"],
                    "ticker": article["ticker"],

                    "source": article["source"],
                    "source_type": article["source_type"],
                    "publisher": article["publisher"],

                    "title": article["title"],
                    "url": article["url"],

                    "published_at": article["published_at"],
                    "ingested_at": datetime.now(timezone.utc),

                    "raw_text": processed["raw_text"],
                    "summary": processed["summary"],
                    "language": processed["language"],
                    "text_length": len(processed["raw_text"]),
                    "content_hash": content_hash,

                    "processing": {
                        "clustered": False,
                        "embedded": False
                    }
                }

                collection.insert_one(doc)
                total_inserted += 1

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
