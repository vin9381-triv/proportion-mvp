"""
Multi-Timeframe Clustering - SNAPSHOT MODE (FIXED)

Guarantees:
- Exactly ONE cluster per (entity, window_days, tag)
- Old clusters are deleted before insert
- DBSCAN labels are internal only
- Safe to re-run without duplicates
"""

import yaml
from datetime import datetime, timedelta
from collections import defaultdict
import numpy as np

from processing.common.mongo_client import get_collection
from processing.clustering.input_resolver import get_raw_article_ids_for_entity
from processing.clustering.queries import get_clustering_candidates_query
from processing.clustering.density_stage import run_dbscan


# ============================================================
# CONFIG
# ============================================================

TICKER_CONFIG_PATH = "config/ticker.yaml"
TAG_CONFIG_PATH = "config/clustering_tags.yaml"

TIME_WINDOWS = [3, 7, 14, 30]

MIN_ARTICLES = {
    3: 3,
    7: 5,
    14: 8,
    30: 10,
}

EXCLUDED_TAGS = {"crime_noise", "spam_clickbait", "other"}

DBSCAN_EPS = 0.5
DBSCAN_MIN_SAMPLES = 2


# ============================================================
# LOADERS
# ============================================================

def load_entities():
    with open(TICKER_CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)

    entities = []

    for company in config.get("companies", []):
        entities.append({
            "entity_id": company["entity_id"],
            "entity_name": company["name"],
            "entity_type": "company",
            "ticker": company.get("ticker"),
        })

    for key in [
        "monetary_policy",
        "macroeconomic_dollar",
        "macroeconomic_inflation",
        "industries",
        "physical_demand",
    ]:
        for entity in config.get(key, []):
            entities.append({
                "entity_id": entity["entity_id"],
                "entity_name": entity["name"],
                "entity_type": entity.get("entity_type", key),
                "ticker": None,
            })

    return entities


def load_tag_config():
    with open(TAG_CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)
    return config.get("tags", {})


# ============================================================
# TAGGER
# ============================================================

class ArticleTagger:
    def __init__(self, tag_config):
        self.tag_config = tag_config

    def tag_article(self, article):
        text = (
            (article.get("title") or "") + " " +
            (article.get("raw_text") or "")
        ).lower()

        # Noise first
        for tag in ["crime_noise", "spam_clickbait"]:
            for kw in self.tag_config.get(tag, {}).get("keywords", []):
                if kw.lower() in text:
                    return tag

        # Normal tags
        for tag, cfg in self.tag_config.items():
            if tag in {"crime_noise", "spam_clickbait"}:
                continue
            for kw in cfg.get("keywords", []):
                if kw.lower() in text:
                    return tag

        return "other"


# ============================================================
# CLUSTERING CORE
# ============================================================

def cluster_entity_for_window(entity, window_days, tagger, embedded_col, clusters_col):
    print(f"\n  [{window_days}d window]")

    # 🔥 SNAPSHOT DELETE (CORRECT)
    clusters_col.delete_many({
        "entity_id": entity["entity_id"],
        "window_days": window_days,
    })

    end_utc = datetime.utcnow()
    start_utc = end_utc - timedelta(days=window_days)

    raw_ids = get_raw_article_ids_for_entity(
        start_utc=start_utc,
        end_utc=end_utc,
        entity_type=entity["entity_type"],
        tickers=[entity["ticker"]] if entity["ticker"] else None,
        entity_id=entity["entity_id"],
    )

    if len(raw_ids) < MIN_ARTICLES[window_days]:
        print("    ⚠️ Not enough raw articles")
        return 0

    articles = list(
        embedded_col.find(get_clustering_candidates_query(raw_ids))
    )

    if len(articles) < MIN_ARTICLES[window_days]:
        print("    ⚠️ Not enough embedded articles")
        return 0

    # Tag buckets
    tag_buckets = defaultdict(list)
    for article in articles:
        tag = tagger.tag_article(article)
        if tag not in EXCLUDED_TAGS:
            tag_buckets[tag].append(article)

    clusters_created = 0

    # 🚨 ONE CLUSTER PER TAG — ENFORCED
    for tag, tag_articles in tag_buckets.items():
        if len(tag_articles) < 2:
            continue

        vectors = []
        valid_articles = []

        for a in tag_articles:
            emb = a.get("embeddings", {}).get("body")
            if emb:
                vectors.append(emb)
                valid_articles.append(a)

        if len(vectors) < 2:
            continue

        X = np.array(vectors)
        labels = run_dbscan(X, eps=DBSCAN_EPS, min_samples=DBSCAN_MIN_SAMPLES)

        # 🔒 COLLAPSE ALL LABELS → ONE SNAPSHOT CLUSTER
        members = []
        for label, article in zip(labels, valid_articles):
            if label == -1:
                continue
            members.append(article)

        if not members:
            continue

        write_cluster(
            entity=entity,
            tag=tag,
            window_days=window_days,
            members=members,
            clusters_col=clusters_col,
        )

        clusters_created += 1

    print(f"    ✅ Created {clusters_created} clusters")
    return clusters_created


def write_cluster(entity, tag, window_days, members, clusters_col):
    now = datetime.utcnow()
    date_str = now.strftime("%Y%m%d")

    # ✅ FIXED ID (NO LABEL)
    cluster_id = f"{entity['entity_id']}|{tag}|{window_days}d|{date_str}"

    article_refs = []
    article_ids = []
    published_times = []
    embeddings = []

    for a in members:
        article_refs.append({
            "article_id": a["_id"],
            "title": a.get("title"),
            "published_at_utc": a.get("published_at_utc"),
            "raw_article_id": a.get("raw_article_id"),
        })

        article_ids.append(a["_id"])

        if a.get("published_at_utc"):
            published_times.append(a["published_at_utc"])

        emb = a.get("embeddings", {}).get("body")
        if emb:
            embeddings.append(emb)

    centroid = np.mean(np.array(embeddings), axis=0).tolist() if embeddings else None

    doc = {
        "cluster_id": cluster_id,

        "entity_id": entity["entity_id"],
        "entity_name": entity["entity_name"],
        "entity_type": entity["entity_type"],
        "ticker": entity.get("ticker"),

        "tag": tag,
        "window_days": window_days,
        "timeframe": f"{window_days}d",

        "articles": article_refs,
        "article_ids": article_ids,

        "centroid": centroid,

        "cluster_metadata": {
            "size": len(article_ids),
            "first_published": min(published_times) if published_times else None,
            "last_published": max(published_times) if published_times else None,
        },

        "created_at": now,
        "last_updated": now,
    }

    clusters_col.insert_one(doc)


# ============================================================
# MAIN
# ============================================================

def run_multi_timeframe_clustering():
    print("=" * 80)
    print("MULTI-TIMEFRAME CLUSTERING — SNAPSHOT MODE (FIXED)")
    print("=" * 80)

    entities = load_entities()
    tag_config = load_tag_config()
    tagger = ArticleTagger(tag_config)

    embedded_col = get_collection("articles_embedded")
    clusters_col = get_collection("story_clusters")

    for idx, entity in enumerate(entities, 1):
        print(f"\n[{idx}/{len(entities)}] {entity['entity_name']}")

        for window_days in TIME_WINDOWS:
            cluster_entity_for_window(
                entity,
                window_days,
                tagger,
                embedded_col,
                clusters_col,
            )

    print("\n✅ CLUSTERING COMPLETE")


if __name__ == "__main__":
    run_multi_timeframe_clustering()