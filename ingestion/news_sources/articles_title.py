from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client["proportion_db_test"]

cursor = (
    db.articles_raw_test
    .find(
        {"source": "newsdata_io"},
        {
            "_id": 0,
            "title": 1,
            "entity_name": 1,
            "entity_id": 1,
            "entity_type": 1,
            "ingested_at": 1
        }
    )
    .sort("ingested_at", -1)
    .limit(50)
)

for doc in cursor:
    print(
        f"[{doc.get('entity_type')}] "
        f"{doc.get('entity_name')} → {doc.get('title')}"
    )
