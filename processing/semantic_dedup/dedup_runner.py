from datetime import datetime, timedelta, timezone
from uuid import uuid4
from pathlib import Path

from processing.common.mongo_client import get_embedded_articles_collection
from processing.semantic_dedup.cosine_similarity import cosine_similarity
from processing.semantic_dedup.constants import (
    TITLE_COSINE_THRESHOLD,
    BODY_COSINE_THRESHOLD,
    TIME_WINDOW_HOURS,
)

# ---------------- CONFIG ----------------
DRY_RUN = True                  # ⛔ KEEP TRUE
ENABLE_TIME_WINDOW = True

HARD_DUP_TITLE_THRESHOLD = 0.99
HARD_DUP_BODY_THRESHOLD = 0.99

LOG_SIMILARITY_THRESHOLD = 0.70

LOG_DIR = Path("processing/semantic_dedup/logs")
# --------------------------------------


def run_semantic_dedup():
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now(timezone.utc)
    log_path = LOG_DIR / f"dedup_run_{run_ts.date().isoformat()}.txt"

    def log(msg: str = ""):
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(msg + "\n")

    col = get_embedded_articles_collection()

    query = {
        "$or": [
            {"processing.semantically_deduped": False},
            {"processing.semantically_deduped": {"$exists": False}},
        ]
    }

    articles = list(col.find(query))

    stats = {
        "articles_scanned": len(articles),
        "hard_duplicates": 0,
        "semantic_duplicates": 0,
        "groups": 0,
    }

    log("DEDUP RUN")
    log("════════════════════════════════════════")
    log(f"Run timestamp (UTC) : {run_ts.isoformat()}")
    log(f"Articles scanned   : {stats['articles_scanned']}")
    log("")

    if not articles:
        log("No articles found. Exiting.")
        return

    # Safe sort by publish time
    articles.sort(
        key=lambda x: x.get("published_at_utc")
        or datetime.min.replace(tzinfo=timezone.utc)
    )

    processed_ids = set()

    for i, base in enumerate(articles):
        if base["_id"] in processed_ids:
            continue

        base_time = base.get("published_at_utc")
        if base_time is None:
            continue

        group_id = str(uuid4())
        group_members = [base]
        processed_ids.add(base["_id"])

        hard_dups = []
        semantic_dups = []

        for candidate in articles[i + 1:]:
            if candidate["_id"] in processed_ids:
                continue

            if candidate.get("entity_id") != base.get("entity_id"):
                continue

            candidate_time = candidate.get("published_at_utc")
            if candidate_time is None:
                continue

            if ENABLE_TIME_WINDOW:
                if abs(candidate_time - base_time) > timedelta(hours=TIME_WINDOW_HOURS):
                    continue

            title_sim = cosine_similarity(
                base["embeddings"]["title"],
                candidate["embeddings"]["title"],
            )
            body_sim = cosine_similarity(
                base["embeddings"]["body"],
                candidate["embeddings"]["body"],
            )

            # ---- HARD DUPLICATE ----
            if (
                title_sim >= HARD_DUP_TITLE_THRESHOLD
                and body_sim >= HARD_DUP_BODY_THRESHOLD
            ):
                hard_dups.append((candidate, title_sim, body_sim))
                group_members.append(candidate)
                processed_ids.add(candidate["_id"])
                stats["hard_duplicates"] += 1
                continue

            # ---- SEMANTIC DUPLICATE ----
            if (
                title_sim >= TITLE_COSINE_THRESHOLD
                and body_sim >= BODY_COSINE_THRESHOLD
            ):
                semantic_dups.append((candidate, title_sim, body_sim))
                group_members.append(candidate)
                processed_ids.add(candidate["_id"])
                stats["semantic_duplicates"] += 1

        if len(group_members) > 1:
            canonical = max(group_members, key=lambda x: x.get("text_length", 0))

            stats["groups"] += 1

            log("────────────────────────────────────────")
            log(f"🧠 SEMANTIC GROUP #{stats['groups']}")
            log(f"Entity     : {base.get('company_name')} ({base.get('ticker')})")
            log(f"Group ID   : {group_id}")
            log(f"Group size : {len(group_members)}")
            log(f"Canonical  : {canonical.get('title', '')}")
            log(f"Published  : {base_time.isoformat()}")
            log("")

            for doc, t_sim, b_sim in hard_dups:
                log("  🧱 HARD DUPLICATE")
                log(f"    Title     : {doc.get('title', '')}")
                log(f"    Title cos : {t_sim:.3f}")
                log(f"    Body cos  : {b_sim:.3f}")
                log("")

            for doc, t_sim, b_sim in semantic_dups:
                log("  🔗 SEMANTIC DUPLICATE")
                log(f"    Title     : {doc.get('title', '')}")
                log(f"    Title cos : {t_sim:.3f}")
                log(f"    Body cos  : {b_sim:.3f}")
                log("")

    unique_articles = stats["articles_scanned"] - stats["hard_duplicates"]

    log("")
    log("DEDUP RUN SUMMARY")
    log("────────────────────────────────────────")
    log(f"Articles scanned        : {stats['articles_scanned']}")
    log(f"Unique articles         : {unique_articles}")
    log(f"Hard duplicates merged  : {stats['hard_duplicates']}")
    log(f"Semantic groups formed  : {stats['groups']}")
    log(f"Singleton groups        : {unique_articles - stats['groups']}")
    log(f"Mongo writes            : 0 (dry run)")
    log("")

    print(f"✅ Dedup run complete. Log written to:\n{log_path}")


if __name__ == "__main__":
    run_semantic_dedup()
