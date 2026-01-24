from datetime import datetime
from processing.common.mongo_client import get_collection


def get_raw_article_ids_for_entity(
    *,
    start_utc: datetime,
    end_utc: datetime,
    entity_type: str,
    tickers: list[str] | None = None,
    entity_id: str | None = None
):
    raw_col = get_collection("articles_raw")

    base_query = {
        "published_at_utc": {
            "$gte": start_utc,
            "$lt": end_utc
        }
    }

    if entity_type == "company":
        base_query["ticker"] = {"$in": tickers}

    elif entity_type == "industry":
        # IMPORTANT:
        # articles_raw must already be tagged with entity_id during ingestion
        base_query["entity_id"] = entity_id

    else:
        raise ValueError(f"Unknown entity_type: {entity_type}")

    cursor = raw_col.find(base_query, {"_id": 1})

    return [doc["_id"] for doc in cursor]
