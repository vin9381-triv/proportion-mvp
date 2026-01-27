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
    """
    Fetch raw article IDs for an entity within a time window.
    
    Args:
        start_utc: Start of time window
        end_utc: End of time window
        entity_type: Type of entity ("company" or any other type)
        tickers: List of tickers (for companies only)
        entity_id: Entity ID (for all non-company entities)
    
    Returns:
        List of article IDs (ObjectIds)
    """
    raw_col = get_collection("articles_raw")

    base_query = {
        "published_at_utc": {
            "$gte": start_utc,
            "$lt": end_utc
        }
    }

    if entity_type == "company":
        # Companies: query by ticker
        if not tickers:
            raise ValueError("tickers must be provided for entity_type='company'")
        base_query["ticker"] = {"$in": tickers}

    else:
        # All other entities (industry, monetary_policy, currency, etc.):
        # Query by entity_id
        # IMPORTANT: articles_raw must be tagged with entity_id during ingestion
        if not entity_id:
            raise ValueError(f"entity_id must be provided for entity_type='{entity_type}'")
        base_query["entity_id"] = entity_id

    cursor = raw_col.find(base_query, {"_id": 1})

    return [doc["_id"] for doc in cursor]