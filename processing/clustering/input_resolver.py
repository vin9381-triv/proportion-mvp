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

    TEST VERSION
    Unified resolver:
    - Prefer entity_id for ALL entity types (including companies)
    - Fall back to ticker only if entity_id is not provided
    """

    raw_col = get_collection("articles_raw")

    base_query = {
        "published_at_utc": {
            "$gte": start_utc,
            "$lt": end_utc
        }
    }

    # ✅ PRIMARY: entity_id (preferred for ALL entities)
    if entity_id:
        base_query["entity_id"] = entity_id

    # 🔁 FALLBACK: ticker (legacy / safety net)
    elif tickers:
        base_query["ticker"] = {"$in": tickers}

    else:
        raise ValueError(
            f"No valid identifier provided for entity_type='{entity_type}'"
        )

    cursor = raw_col.find(base_query, {"_id": 1})
    result = [doc["_id"] for doc in cursor]

    # Debug (keep this for now)
    print(
        f"  DEBUG: Found {len(result)} raw articles | "
        f"entity_type={entity_type} | "
        f"entity_id={entity_id} | "
        f"tickers={tickers}"
    )

    return result
