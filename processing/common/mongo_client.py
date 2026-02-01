#!/usr/bin/env python3
"""
MongoDB Client Utilities
========================

Centralized MongoDB connection and collection helpers
for the Proportion pipeline.

Design:
- Single MongoClient instance (recommended by pymongo)
- Explicit collection helpers for core datasets
- Generic get_collection() for flexibility
"""

from pymongo import MongoClient

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "proportion_db_v1"

# Core collections
RAW_COLLECTION = "articles_raw"
EMBEDDED_COLLECTION = "articles_embedded"
DEDUP_GROUPS_COLLECTION = "semantic_dedup_groups"
CLUSTERS_COLLECTION = "story_clusters"


# ---------------------------------------------------------------------
# SINGLE CLIENT (IMPORTANT)
# ---------------------------------------------------------------------

_client = MongoClient(MONGO_URI)
_db = _client[DB_NAME]


# ---------------------------------------------------------------------
# GENERIC ACCESS
# ---------------------------------------------------------------------

def get_collection(name: str):
    """
    Generic collection accessor.

    Usage:
        col = get_collection("story_clusters")
    """
    return _db[name]


# ---------------------------------------------------------------------
# EXPLICIT HELPERS (READABILITY + SAFETY)
# ---------------------------------------------------------------------

def get_raw_articles_collection():
    return _db[RAW_COLLECTION]


def get_embedded_articles_collection():
    return _db[EMBEDDED_COLLECTION]


def get_semantic_dedup_groups_collection():
    return _db[DEDUP_GROUPS_COLLECTION]


def get_story_clusters_collection():
    return _db[CLUSTERS_COLLECTION]


# ---------------------------------------------------------------------
# SANITY CHECK
# ---------------------------------------------------------------------

def _test():
    print("MongoDB connection test")
    print(f"DB name: {_db.name}")
    print("Collections:")
    for name in _db.list_collection_names():
        print(f"  • {name}")


if __name__ == "__main__":
    _test()
