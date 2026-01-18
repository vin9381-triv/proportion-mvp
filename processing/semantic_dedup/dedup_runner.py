from datetime import datetime, timedelta, timezone
from uuid import uuid4

from pymongo import InsertOne, UpdateOne

from processing.common.mongo_client import (
    get_embedded_articles_collection,
    get_semantic_dedup_groups_collection,
)
from processing.semantic_dedup.cosine_similarity import cosine_similarity
from processing.semantic_dedup.constants import (
    TITLE_COSINE_THRESHOLD,
    BODY_COSINE_THRESHOLD,
    TIME_WINDOW_HOURS,
)

# ---------------- CONFIG ----------------
DRY_RUN = False          # 🔴 SET TRUE TO TEST
DEDUP_VERSION = "v1"

HARD_DUP_TITLE_THRESHOLD = 0.99
HARD_DUP_BODY_THRESHOLD = 0.99
# --------------------------------------


def run_semantic_dedup():
    emb_col = get_embedded_articles_collection()
    group_col = get_semantic_dedup_groups_collection()

    query = {
        "processing.semantically_deduped": {"$ne": True}
    }

    articles = list(emb_col.find(query))

    print(f"🔹 Loaded {len(articles)} candidate articles")

    articles.sort(
        key=lambda x: x.get("published_at_utc")
        or datetime.min.replace(tzinfo=timezone.utc)
    )

    processed_ids = set()
    group_inserts = []
    article_updates = []

    for i, base in enumerate(articles):
        if base["_id"] in processed_ids:
            continue

        base_time = base.get("published_at_utc")
        if not base_time:
            continue

        members = [base]
        hard_dups = []
        semantic_dups = []

        for candidate in articles[i + 1:]:
            if candidate["_id"] in processed_ids:
                continue

            if candidate.get("entity_id") != base.get("entity_id"):
                continue

            cand_time = candidate.get("published_at_utc")
            if not cand_time:
                continue

            if abs(cand_time - base_time) > timedelta(hours=TIME_WINDOW_HOURS):
                continue

            title_sim = cosine_similarity(
                base["embeddings"]["title"],
                candidate["embeddings"]["title"],
            )
            body_sim = cosine_similarity(
                base["embeddings"]["body"],
                candidate["embeddings"]["body"],
            )

            if title_sim >= HARD_DUP_TITLE_THRESHOLD and body_sim >= HARD_DUP_BODY_THRESHOLD:
                members.append(candidate)
                hard_dups.append(candidate["_id"])
                processed_ids.add(candidate["_id"])
                continue

            if title_sim >= TITLE_COSINE_THRESHOLD and body_sim >= BODY_COSINE_THRESHOLD:
                members.append(candidate)
                semantic_dups.append(candidate["_id"])
                processed_ids.add(candidate["_id"])

        if len(members) <= 1:
            continue

        group_id = str(uuid4())
        canonical = max(members, key=lambda x: x.get("text_length", 0))

        group_doc = {
            "group_id": group_id,
            "dedup_version": DEDUP_VERSION,

            "entity_id": base.get("entity_id"),
            "company_name": base.get("company_name"),
            "ticker": base.get("ticker"),

            "canonical_article_id": canonical["_id"],
            "member_article_ids": [m["_id"] for m in members],

            "hard_duplicate_ids": hard_dups,
            "semantic_duplicate_ids": semantic_dups,

            "group_size": len(members),
            "created_at": datetime.now(timezone.utc),

            "dedup_params": {
                "title_threshold": TITLE_COSINE_THRESHOLD,
                "body_threshold": BODY_COSINE_THRESHOLD,
                "hard_dup_title": HARD_DUP_TITLE_THRESHOLD,
                "hard_dup_body": HARD_DUP_BODY_THRESHOLD,
                "time_window_hours": TIME_WINDOW_HOURS,
            },
        }

        group_inserts.append(InsertOne(group_doc))

        for m in members:
            article_updates.append(
                UpdateOne(
                    {"_id": m["_id"]},
                    {
                        "$set": {
                            "processing.semantically_deduped": True,
                            "processing.dedup_group_id": group_id,
                            "processing.is_canonical": m["_id"] == canonical["_id"],
                        }
                    },
                )
            )

        processed_ids.update(m["_id"] for m in members)

    print(f"🧠 Dedup groups to write : {len(group_inserts)}")

    if DRY_RUN:
        print("⛔ DRY RUN — no Mongo writes performed")
        return

    if group_inserts:
        group_col.bulk_write(group_inserts, ordered=False)

    if article_updates:
        emb_col.bulk_write(article_updates, ordered=False)

    print("✅ Semantic dedup persisted to MongoDB")


if __name__ == "__main__":
    run_semantic_dedup()
