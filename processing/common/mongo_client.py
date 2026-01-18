from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "proportion_db_v1"

RAW_COLLECTION = "articles_raw"
EMBEDDED_COLLECTION = "articles_embedded"
DEDUP_GROUPS_COLLECTION = "semantic_dedup_groups"


def get_raw_articles_collection():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME][RAW_COLLECTION]


def get_embedded_articles_collection():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME][EMBEDDED_COLLECTION]


def get_semantic_dedup_groups_collection():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME][DEDUP_GROUPS_COLLECTION]