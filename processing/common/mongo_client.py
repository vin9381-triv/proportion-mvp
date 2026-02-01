from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "proportion_db_v1"

_client = None

def _get_client():
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI)
    return _client

def get_collection(collection_name):
    return _get_client()[DB_NAME][collection_name]

# ---- existing functions stay as-is ----

def get_raw_articles_collection():
    return get_collection("articles_raw")

def get_embedded_articles_collection():
    return get_collection("articles_embedded")

def get_semantic_dedup_groups_collection():
    return get_collection("semantic_dedup_groups")
