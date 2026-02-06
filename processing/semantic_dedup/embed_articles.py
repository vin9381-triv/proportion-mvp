"""
Article Embedding Pipeline with Eligibility Gate Support

CHANGES FROM ORIGINAL:
- Only embeds articles where ingestion_gate.allowed == True
- Skips articles rejected by eligibility gate
- Reports gate filtering metrics

This is the ONLY change needed in downstream processing.
All other logic (dedup, clustering) remains unchanged.
"""

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
    v2 EMBEDDING PIPELINE (WITH ELIGIBILITY GATE)

    NEW BEHAVIOR:
    - Only embeds articles where ingestion_gate.allowed == True
    - Respects upstream eligibility decisions
    - Reports gate filtering metrics

    Idempotent guarantees:
    - Reads from articles_raw
    - Embeds ONLY new raw articles
    - Writes exactly one embedded doc per raw article
    """

    raw_col = get_raw_articles_collection()
    emb_col = get_embedded_articles_collection()

    model = SentenceTransformer(MODEL_NAME)

    # ðŸ"' Already embedded raw IDs
    embedded_ids = {
        doc["raw_article_id"]
        for doc in emb_col.find({}, {"raw_article_id": 1})
    }

    # âœ¨ NEW: Only select articles allowed by ingestion gate
    query = {
        "_id": {"$nin": list(embedded_ids)},
        "ingestion_gate.allowed": True  # âœ¨ CRITICAL FILTER
    }

    cursor = raw_col.find(query)

    bulk_ops = []
    processed = 0
    skipped = 0

    # âœ¨ NEW: Count gate-filtered articles
    total_raw = raw_col.count_documents({"_id": {"$nin": list(embedded_ids)}})
    gate_allowed = raw_col.count_documents(query)
    gate_filtered = total_raw - gate_allowed

    print(f"Found {len(embedded_ids)} already embedded articles")
    print(f"Found {total_raw} new raw articles")
    print(f"Eligibility gate filtered: {gate_filtered} articles")
    print(f"✅ Ready to embed: {gate_allowed} articles")
    print("Starting embedding pipeline (idempotent)")

    for doc in cursor:
        try:
            title = doc.get("title", "").strip()
            raw_text = doc.get("raw_text", "")

            if not title or not raw_text:
                skipped += 1
                continue

            body_text = raw_text[:BODY_CHAR_LIMIT]

            embedded_doc = {
                # ---- Lineage ----
                "raw_article_id": doc["_id"],

                # ---- Entity ----
                "entity_id": doc.get("entity_id"),
                "entity_name": doc.get("entity_name"),  # âœ¨ FIXED: was company_name
                "ticker": doc.get("ticker"),
                "entity_type": doc.get("entity_type"),  # âœ¨ NEW

                # ---- Article ----
                "title": title,
                "url": doc.get("url"),

                # ---- Time ----
                "published_at_raw": doc.get("published_at_raw"),
                "published_at_utc": doc.get("published_at_utc"),
                "ingested_at": doc.get("ingested_at"),

                # ---- Content ----
                "raw_text": raw_text,
                "text_length": len(raw_text),

                # ---- Embeddings ----
                "embeddings": {
                    "title": model.encode(title).tolist(),
                    "body": model.encode(body_text).tolist(),
                    "model": MODEL_NAME,
                    "embedded_at": datetime.now(timezone.utc),
                },

                # âœ¨ NEW: Copy gate metadata for traceability
                "ingestion_gate": doc.get("ingestion_gate"),

                # ---- Processing ----
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
            print(f"âš ï¸ Failed embedding raw_article_id={doc.get('_id')}: {e}")

    if bulk_ops:
        emb_col.bulk_write(bulk_ops, ordered=False)

    print("✅ Embedding complete")
    print(f"   New embedded : {processed}")
    print(f"   Skipped      : {skipped}")
    print(f"   Gate filtered: {gate_filtered}")
    print(f"\nŠ EFFECTIVE FILTERING:")
    print(f"   Total raw articles    : {total_raw}")
    print(f"   Passed eligibility    : {gate_allowed} ({gate_allowed/max(total_raw,1)*100:.1f}%)")
    print(f"   Rejected by gate      : {gate_filtered} ({gate_filtered/max(total_raw,1)*100:.1f}%)")


def analyze_gate_rejections():
    """
    Analyze why articles were rejected by eligibility gate.
    
    Useful for:
    - Understanding rule effectiveness
    - Tuning eligibility rules
    - Debugging gate decisions
    """
    raw_col = get_raw_articles_collection()
    
    print("\n" + "=" * 80)
    print("GATE REJECTION ANALYSIS")
    print("=" * 80)
    
    # Count total rejected
    total_rejected = raw_col.count_documents({"ingestion_gate.allowed": False})
    total_allowed = raw_col.count_documents({"ingestion_gate.allowed": True})
    
    print(f"\nTotal articles:")
    print(f"  Allowed  : {total_allowed}")
    print(f"  Rejected : {total_rejected}")
    print(f"  Rate     : {total_rejected/(total_allowed+total_rejected)*100:.1f}%")
    
    # Breakdown by reason
    pipeline = [
        {"$match": {"ingestion_gate.allowed": False}},
        {"$group": {
            "_id": "$ingestion_gate.reason",
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": -1}}
    ]
    
    reasons = list(raw_col.aggregate(pipeline))
    
    print("\n Rejection reasons:")
    for reason_doc in reasons:
        reason = reason_doc['_id']
        count = reason_doc['count']
        pct = count / total_rejected * 100
        print(f"  ❌ {reason:40s} : {count:6d} ({pct:5.1f}%)")
    
    # Breakdown by entity type
    pipeline = [
        {"$match": {"ingestion_gate.allowed": False}},
        {"$group": {
            "_id": "$entity_type",
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": -1}}
    ]
    
    entity_types = list(raw_col.aggregate(pipeline))
    
    print(f" Rejections by entity type:")
    for et_doc in entity_types:
        entity_type = et_doc['_id']
        count = et_doc['count']
        pct = count / total_rejected * 100
        print(f"  📂 {entity_type:20s} : {count:6d} ({pct:5.1f}%)")
    
    print("=" * 80)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--analyze":
        analyze_gate_rejections()
    else:
        embed_articles()