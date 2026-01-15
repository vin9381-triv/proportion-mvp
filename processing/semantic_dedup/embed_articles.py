from datetime import datetime, timezone
from sentence_transformers import SentenceTransformer
from pymongo import InsertOne

from processing.common.mongo_client import (
    get_raw_articles_collection,
    get_embedded_articles_collection,
)

# ---------------- CONFIG ----------------
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
BODY_CHAR_LIMIT = 4000
BULK_SIZE = 16
# --------------------------------------


def embed_articles():
    """
    IDENTITY-SAFE PIPELINE (v1):

    - Reads from articles_raw
    - Embeds ONLY raw articles that are not yet embedded
    - Writes exactly one embedded doc per raw article
    - Safe to run daily / repeatedly
    """

    raw_col = get_raw_articles_collection()
    emb_col = get_embedded_articles_collection()

    model = SentenceTransformer(MODEL_NAME)

    # 🔑 Find already embedded raw_article_ids
    embedded_ids = set(
        doc["raw_article_id"]
        for doc in emb_col.find({}, {"raw_article_id": 1})
    )

    print(f"🔎 Found {len(embedded_ids)} already embedded articles")

    # Only embed NEW raw articles
    cursor = raw_col.find(
        {"_id": {"$nin": list(embedded_ids)}}
    )

    bulk_ops = []
    processed = 0
    skipped = 0

    print("🚀 Starting embedding pipeline (idempotent)")

    for doc in cursor:
        try:
            title = doc.get("title", "").strip()
            raw_text = doc.get("raw_text", "")

            if not title or not raw_text:
                skipped += 1
                continue

            body_text = raw_text[:BODY_CHAR_LIMIT]

            title_emb = model.encode(title).tolist()
            body_emb = model.encode(body_text).tolist()

            embedded_doc = {
                # ---- Lineage ----
                "raw_article_id": doc["_id"],

                # ---- Entity ----
                "entity_id": doc.get("entity_id"),
                "company_name": doc.get("company_name"),
                "ticker": doc.get("ticker"),

                # ---- Article ----
                "title": title,
                "url": doc.get("url"),

                # ---- Time (v1) ----
                "published_at_raw": doc.get("published_at_raw"),
                "published_at_utc": doc.get("published_at_utc"),
                "ingested_at": doc.get("ingested_at"),

                # ---- Content ----
                "raw_text": raw_text,
                "text_length": len(raw_text),

                # ---- Embeddings ----
                "embeddings": {
                    "title": title_emb,
                    "body": body_emb,
                    "model": MODEL_NAME,
                    "embedded_at": datetime.now(timezone.utc),
                },

                # ---- Processing flags ----
                "processing": {
                    "embedded": True,
                    "semantically_deduped": False,
                    "clustered": False,
                },
            }

            bulk_ops.append(InsertOne(embedded_doc))
            processed += 1

            if len(bulk_ops) >= BULK_SIZE:
                emb_col.bulk_write(bulk_ops, ordered=False)
                bulk_ops = []

        except Exception as e:
            skipped += 1
            print(f"⚠️ Failed embedding raw_article_id={doc.get('_id')}: {e}")

    if bulk_ops:
        emb_col.bulk_write(bulk_ops, ordered=False)

    print("✅ Embedding complete")
    print(f"   New articles embedded : {processed}")
    print(f"   Articles skipped      : {skipped}")


if __name__ == "__main__":
    embed_articles()
