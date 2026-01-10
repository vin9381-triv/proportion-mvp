from pymongo import MongoClient


# MongoDB connection configuration
# NOTE:
# - Local-first setup for MVP
# - No credentials required for local development
# - Connection string can be externalized later if needed
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "proportion_db"
COLLECTION_NAME = "articles_raw"


def get_articles_collection():
    """
    Initialize and return a MongoDB collection handle.

    Purpose:
    - Centralizes MongoDB connection logic
    - Ensures all ingestion components use the same database/collection
    - Keeps database access logic out of ingestion pipeline

    Design notes:
    - A new MongoClient is instantiated per call
    - PyMongo manages connection pooling internally
    - This is sufficient and safe for MVP-scale workloads

    Returns:
        pymongo.collection.Collection: MongoDB collection handle
    """

    # Create MongoDB client
    client = MongoClient(MONGO_URI)

    # Access database
    db = client[DB_NAME]

    # Access collection
    collection = db[COLLECTION_NAME]

    return collection


# Simple sanity check for local development
if __name__ == "__main__":
    collection = get_articles_collection()
    print(f"Connected to collection: {collection.name} in database: {DB_NAME}")
