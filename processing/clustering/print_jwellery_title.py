from processing.common.mongo_client import get_collection


def print_jewellery_titles():
    raw_col = get_collection("articles_raw")

    query = {
        "entity_type": "industry",
        "entity_id": {
            "$in": [
                "industry_jewellery_gold_001",
                "industry_jewellery_silver_002"
            ]
        }
    }

    cursor = raw_col.find(
        query,
        {
            "title": 1,
            "entity_id": 1,
            "entity_type": 1,
            "published_at_utc": 1
        }
    ).sort("published_at_utc", -1)

    print("\n=== Jewellery Articles (Gold + Silver) ===\n")

    count = 0
    for doc in cursor:
        count += 1
        title = doc.get("title", "").strip()
        entity_id = doc.get("entity_id")
        published = doc.get("published_at_utc")

        print(f"{count:03d}. [{entity_id}] {title}")
        print(f"     Published: {published}\n")

    print(f"Total jewellery articles found: {count}")


if __name__ == "__main__":
    print_jewellery_titles()
