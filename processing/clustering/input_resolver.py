from datetime import datetime
from processing.common.mongo_client import get_collection

def get_raw_article_ids_for_window_and_tickers(
    start_utc: datetime,
    end_utc: datetime,
    tickers: list[str]
):
    raw_col = get_collection("articles_raw")

    cursor = raw_col.find(
        {
            "ticker": {"$in": tickers},
            "published_at_utc": {
                "$gte": start_utc,
                "$lt": end_utc
            }
        },
        {"_id": 1}
    )

    return [doc["_id"] for doc in cursor]
