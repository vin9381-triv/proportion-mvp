"""
MongoDB Writer for Story Clustering Results
============================================

Saves clustering outputs to MongoDB for downstream processing:
- Stance detection
- Impact analysis
- Insight generation
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
from bson import ObjectId
import numpy as np

from processing.common.mongo_client import get_collection


# ============================================================
# SCHEMA DESIGN
# ============================================================

"""
Collection: story_clusters

Document Structure:
{
    _id: ObjectId,
    cluster_id: str,
    entity_id: str,
    entity_name: str,
    entity_type: str,
    ticker: str | null,

    tag: str,
    cluster_label: int,

    time_window: {
        start_utc: datetime,
        end_utc: datetime
    },

    articles: [
        {
            article_id: ObjectId,
            title: str,
            published_at_utc: datetime,
            raw_article_id: ObjectId
        }
    ],

    cluster_metadata: {
        size: int,
        first_published: datetime,
        last_published: datetime,
        duration_hours: float,
        velocity: float,
        is_noise: bool
    },

    created_at: datetime,
    clustering_run_id: ObjectId
}
"""


# ============================================================
# CLUSTERING RUN METADATA
# ============================================================

def create_clustering_run(config: dict, stats: dict) -> ObjectId:
    """
    Create a new clustering run record.
    """
    runs_col = get_collection("clustering_runs")

    run_doc = {
        "run_timestamp": datetime.utcnow(),
        "config": config,
        "stats": stats,
        "status": "completed"
    }

    result = runs_col.insert_one(run_doc)
    return result.inserted_id


# ============================================================
# CLUSTER WRITER
# ============================================================

def write_cluster_to_mongodb(
    *,
    entity_info: dict,
    tag: str,
    cluster_label: int,
    articles: List[dict],
    time_window: dict,
    clustering_run_id: ObjectId
) -> ObjectId:
    """
    Write a single story cluster to MongoDB.
    """
    clusters_col = get_collection("story_clusters")

    # ✅ CRITICAL FIX: force native Python int
    cluster_label = int(cluster_label)

    identifier = entity_info.get("ticker") or entity_info.get("entity_id")
    date_str = time_window["start_utc"].strftime("%Y%m%d")
    cluster_id = f"{identifier}_{tag}_{cluster_label}_{date_str}"

    article_refs = []
    published_times = []

    for article in articles:
        article_refs.append({
            "article_id": article["_id"],
            "title": article.get("title", ""),
            "published_at_utc": article.get("published_at_utc"),
            "raw_article_id": article.get("raw_article_id")
        })

        if article.get("published_at_utc"):
            published_times.append(article["published_at_utc"])

    first_published = min(published_times) if published_times else None
    last_published = max(published_times) if published_times else None

    duration_hours = None
    velocity = None
    if first_published and last_published:
        duration = (last_published - first_published).total_seconds() / 3600
        duration_hours = float(duration)
        velocity = float(len(articles) / duration if duration > 0 else len(articles))

    cluster_doc = {
        "cluster_id": cluster_id,
        "entity_id": entity_info.get("entity_id"),
        "entity_name": entity_info["name"],
        "entity_type": entity_info["entity_type"],
        "ticker": entity_info.get("ticker"),

        "tag": tag,
        "cluster_label": cluster_label,

        "time_window": time_window,

        "articles": article_refs,

        "cluster_metadata": {
            "size": int(len(articles)),
            "first_published": first_published,
            "last_published": last_published,
            "duration_hours": duration_hours,
            "velocity": velocity,
            "is_noise": bool(cluster_label == -1)
        },

        "created_at": datetime.utcnow(),
        "clustering_run_id": clustering_run_id
    }

    clusters_col.update_one(
        {"cluster_id": cluster_id},
        {"$set": cluster_doc},
        upsert=True
    )

    return cluster_id


# ============================================================
# BATCH CLUSTER WRITER
# ============================================================

def write_entity_clusters_to_mongodb(
    *,
    entity_info: dict,
    tag_results: List[dict],
    time_window: dict,
    clustering_run_id: ObjectId
) -> Dict[str, Any]:
    """
    Write all clusters for a single entity.
    """
    stats = {
        "entity_name": entity_info["name"],
        "clusters_written": 0,
        "articles_written": 0,
        "tags_processed": 0,
        "cluster_ids": []
    }

    for result in tag_results:
        if not result:
            continue

        tag = result["tag"]
        clusters = result["clusters"]

        for cluster_label, articles in clusters.items():
            cluster_id = write_cluster_to_mongodb(
                entity_info=entity_info,
                tag=tag,
                cluster_label=cluster_label,
                articles=articles,
                time_window=time_window,
                clustering_run_id=clustering_run_id
            )

            stats["clusters_written"] += 1
            stats["articles_written"] += len(articles)
            stats["cluster_ids"].append(cluster_id)

        stats["tags_processed"] += 1

    return stats


# ============================================================
# ARTICLE CLUSTER ASSIGNMENT (REVERSE INDEX)
# ============================================================

def update_article_cluster_assignments(
    *,
    entity_info: dict,
    tag_results: List[dict],
    clustering_run_id: ObjectId
):
    """
    Update articles_embedded with cluster assignments.
    """
    embedded_col = get_collection("articles_embedded")

    for result in tag_results:
        if not result:
            continue

        tag = result["tag"]
        clusters = result["clusters"]

        for cluster_label, articles in clusters.items():
            # ✅ CRITICAL FIX: force native Python int
            cluster_label = int(cluster_label)

            identifier = entity_info.get("ticker") or entity_info.get("entity_id")
            date_str = datetime.utcnow().strftime("%Y%m%d")
            cluster_id = f"{identifier}_{tag}_{cluster_label}_{date_str}"

            article_ids = [article["_id"] for article in articles]

            embedded_col.update_many(
                {"_id": {"$in": article_ids}},
                {
                    "$set": {
                        "clustering.cluster_id": cluster_id,
                        "clustering.tag": tag,
                        "clustering.cluster_label": cluster_label,
                        "clustering.entity_id": entity_info.get("entity_id"),
                        "clustering.entity_name": entity_info["name"],
                        "clustering.clustering_run_id": clustering_run_id,
                        "clustering.clustered_at": datetime.utcnow()
                    }
                }
            )


# ============================================================
# QUERY HELPERS
# ============================================================

def get_clusters_for_entity(entity_id: str, min_size: int = 2) -> List[dict]:
    clusters_col = get_collection("story_clusters")

    query = {
        "entity_id": entity_id,
        "cluster_metadata.size": {"$gte": min_size},
        "cluster_metadata.is_noise": False
    }

    return list(clusters_col.find(query).sort("cluster_metadata.size", -1))


def get_clusters_by_tag(tag: str, min_size: int = 2) -> List[dict]:
    clusters_col = get_collection("story_clusters")

    query = {
        "tag": tag,
        "cluster_metadata.size": {"$gte": min_size},
        "cluster_metadata.is_noise": False
    }

    return list(clusters_col.find(query).sort("cluster_metadata.size", -1))


def get_articles_for_cluster(cluster_id: str) -> List[dict]:
    clusters_col = get_collection("story_clusters")
    embedded_col = get_collection("articles_embedded")

    cluster = clusters_col.find_one({"cluster_id": cluster_id})
    if not cluster:
        return []

    article_ids = [ref["article_id"] for ref in cluster["articles"]]
    return list(embedded_col.find({"_id": {"$in": article_ids}}))


def get_clusters_for_stance_detection(
    min_size: int = 3,
    max_age_days: int = 7
) -> List[dict]:
    clusters_col = get_collection("story_clusters")
    cutoff_date = datetime.utcnow() - timedelta(days=max_age_days)

    query = {
        "cluster_metadata.size": {"$gte": min_size},
        "cluster_metadata.is_noise": False,
        "time_window.end_utc": {"$gte": cutoff_date}
    }

    return list(
        clusters_col.find(query).sort([
            ("cluster_metadata.size", -1),
            ("time_window.end_utc", -1)
        ])
    )


# ============================================================
# INDEX CREATION
# ============================================================

def create_indexes():
    clusters_col = get_collection("story_clusters")

    clusters_col.create_index([("entity_id", 1), ("tag", 1)])
    clusters_col.create_index([("tag", 1), ("cluster_metadata.size", -1)])
    clusters_col.create_index([("cluster_id", 1)], unique=True)
    clusters_col.create_index([("time_window.end_utc", -1)])
    clusters_col.create_index([("clustering_run_id", 1)])
    clusters_col.create_index([
        ("cluster_metadata.is_noise", 1),
        ("cluster_metadata.size", -1)
    ])

    print("✓ Indexes created successfully")


# ============================================================
# STATS
# ============================================================

def get_clustering_stats() -> dict:
    clusters_col = get_collection("story_clusters")

    total_clusters = clusters_col.count_documents({})
    non_noise_clusters = clusters_col.count_documents({"cluster_metadata.is_noise": False})
    noise_clusters = clusters_col.count_documents({"cluster_metadata.is_noise": True})

    tag_pipeline = [
        {"$match": {"cluster_metadata.is_noise": False}},
        {"$group": {
            "_id": "$tag",
            "count": {"$sum": 1},
            "total_articles": {"$sum": "$cluster_metadata.size"}
        }},
        {"$sort": {"count": -1}}
    ]

    entity_pipeline = [
        {"$match": {"cluster_metadata.is_noise": False}},
        {"$group": {
            "_id": "$entity_name",
            "count": {"$sum": 1},
            "total_articles": {"$sum": "$cluster_metadata.size"}
        }},
        {"$sort": {"count": -1}}
    ]

    return {
        "total_clusters": total_clusters,
        "non_noise_clusters": non_noise_clusters,
        "noise_clusters": noise_clusters,
        "by_tag": list(clusters_col.aggregate(tag_pipeline)),
        "by_entity": list(clusters_col.aggregate(entity_pipeline))
    }


if __name__ == "__main__":
    create_indexes()
    stats = get_clustering_stats()
    print(stats)
