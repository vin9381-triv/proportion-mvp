"""
Story-First Multi-Timeframe Clustering (Snapshot Mode)

- DBSCAN clusters = stories
- Tags are metadata, not identity
- Snapshot overwrite per (entity, window)
- Strong noise suppression
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

MIN_ARTICLES_WINDOW = {
    3: 3,
    7: 4,
    14: 5,
    30: 6,
}

MIN_STORY_SIZE = 3
MIN_COHESION = 0.55

DBSCAN_EPS = 0.5
DBSCAN_MIN_SAMPLES = 2

EXCLUDED_TAGS = {"crime_noise", "spam_clickbait", "other"}


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
        return yaml.safe_load(f).get("tags", {})


# ============================================================
# TAGGER (METADATA ONLY)
# ============================================================

class ArticleTagger:
    def __init__(self, tag_config):
        self.tag_config = tag_config

    def tag_article(self, article):
        text = (
            (article.get("title") or "") + " " +
            (article.get("raw_text") or "")
        ).lower()

        for tag, cfg in self.tag_config.items():
            for kw in cfg.get("keywords", []):
                if kw.lower() in text:
                    return tag

        return "other"


# ============================================================
# UTILS
# ============================================================

def cosine_cohesion(vectors):
    if len(vectors) < 2:
        return 0.0

    centroid = np.mean(vectors, axis=0)
    sims = [
        np.dot(v, centroid) / (np.linalg.norm(v) * np.linalg.norm(centroid))
        for v in vectors
    ]
    return float(np.mean(sims))


# ============================================================
# CLUSTERING
# ============================================================

def cluster_entity_window(entity, window_days, tagger, embedded_col, clusters_col):
    print(f"  [{window_days}d window]")

    # SNAPSHOT DELETE
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

    if len(raw_ids) < MIN_ARTICLES_WINDOW[window_days]:
        return 0

    articles = list(
        embedded_col.find(get_clustering_candidates_query(raw_ids))
    )

    vectors = []
    valid_articles = []

    for a in articles:
        emb = a.get("embeddings", {}).get("body")
        if emb:
            vectors.append(emb)
            valid_articles.append(a)

    if len(vectors) < MIN_ARTICLES_WINDOW[window_days]:
        return 0

    X = np.array(vectors)
    labels = run_dbscan(X, eps=DBSCAN_EPS, min_samples=DBSCAN_MIN_SAMPLES)

    clusters = defaultdict(list)
    for label, article in zip(labels, valid_articles):
        if label == -1:
            continue
        clusters[label].append(article)

    stories_written = 0

    for label, members in clusters.items():
        if len(members) < MIN_STORY_SIZE:
            continue

        story_vectors = [
            a["embeddings"]["body"] for a in members
        ]

        cohesion = cosine_cohesion(story_vectors)
        if cohesion < MIN_COHESION:
            continue

        # Assign tags AFTER story formation
        tag_counts = defaultdict(int)
        for a in members:
            tag = tagger.tag_article(a)
            if tag not in EXCLUDED_TAGS:
                tag_counts[tag] += 1

        tags = sorted(tag_counts, key=tag_counts.get, reverse=True)

        write_story_cluster(
            entity=entity,
            window_days=window_days,
            members=members,
            cohesion=cohesion,
            tags=tags,
            clusters_col=clusters_col,
        )

        stories_written += 1

    return stories_written


def write_story_cluster(entity, window_days, members, cohesion, tags, clusters_col):
    now = datetime.utcnow()

    article_ids = []
    article_refs = []
    published_times = []
    embeddings = []

    for a in members:
        article_ids.append(a["_id"])
        article_refs.append({
            "article_id": a["_id"],
            "title": a.get("title"),
            "published_at_utc": a.get("published_at_utc"),
        })

        if a.get("published_at_utc"):
            published_times.append(a["published_at_utc"])

        embeddings.append(a["embeddings"]["body"])

    centroid = np.mean(np.array(embeddings), axis=0).tolist()

    story_id = f"{entity['entity_id']}|{window_days}d|{hash(tuple(article_ids))}"

    clusters_col.insert_one({
        "story_id": story_id,

        "entity_id": entity["entity_id"],
        "entity_name": entity["entity_name"],
        "entity_type": entity["entity_type"],
        "ticker": entity.get("ticker"),

        "window_days": window_days,
        "timeframe": f"{window_days}d",

        "tags": tags[:3],
        "cohesion": cohesion,

        "articles": article_refs,
        "article_ids": article_ids,
        "centroid": centroid,

        "story_metadata": {
            "size": len(article_ids),
            "first_published": min(published_times),
            "last_published": max(published_times),
        },

        "created_at": now,
        "last_updated": now,
    })


# ============================================================
# MAIN
# ============================================================

def run_story_clustering():
    print("STORY-FIRST MULTI-TIMEFRAME CLUSTERING")

    entities = load_entities()
    tagger = ArticleTagger(load_tag_config())

    embedded_col = get_collection("articles_embedded")
    clusters_col = get_collection("story_clusters")

    for entity in entities:
        print(f"\n{entity['entity_name']}")
        for window in TIME_WINDOWS:
            count = cluster_entity_window(
                entity,
                window,
                tagger,
                embedded_col,
                clusters_col,
            )
            print(f"    {window}d → {count} stories")

    print("\nDONE")


if __name__ == "__main__":
    run_story_clustering()