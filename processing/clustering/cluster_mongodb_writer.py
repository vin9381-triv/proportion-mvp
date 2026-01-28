"""
MongoDB Writer for Story Clustering Results
============================================

Saves clustering outputs to MongoDB for downstream processing:
- Stance detection
- Impact analysis
- Insight generation
"""

from datetime import datetime
from typing import List, Dict, Any
from bson import ObjectId

from processing.common.mongo_client import get_collection


# ============================================================
# SCHEMA DESIGN
# ============================================================

"""
Collection: story_clusters

Document Structure:
{
    _id: ObjectId,
    cluster_id: str,              # e.g., "MSFT_product_launch_0_20260124"
    entity_id: str,               # e.g., "company_us_tech_002"
    entity_name: str,             # e.g., "Microsoft"
    entity_type: str,             # e.g., "company"
    ticker: str | null,           # e.g., "MSFT" (companies only)
    
    tag: str,                     # e.g., "product_launch"
    cluster_label: int,           # DBSCAN label (0, 1, 2, ... or -1 for noise)
    
    time_window: {
        start_utc: datetime,
        end_utc: datetime
    },
    
    articles: [
        {
            article_id: ObjectId,    # Reference to articles_embedded
            title: str,
            published_at_utc: datetime,
            raw_article_id: ObjectId # Reference to articles_raw
        },
        ...
    ],
    
    cluster_metadata: {
        size: int,                   # Number of articles
        first_published: datetime,   # Earliest article in cluster
        last_published: datetime,    # Latest article in cluster
        duration_hours: float,       # Time span of cluster
        velocity: float,             # articles per hour
        is_noise: bool              # True if cluster_label == -1
    },
    
    created_at: datetime,
    clustering_run_id: ObjectId      # Reference to clustering_runs collection
}

Collection: clustering_runs

Document Structure:
{
    _id: ObjectId,
    run_timestamp: datetime,
    config: {
        window_days: int,
        dbscan_eps: float,
        dbscan_min_samples: int,
        min_articles_per_tag: int
    },
    stats: {
        total_entities: int,
        total_clusters: int,
        total_articles: int
    }
}
"""


# ============================================================
# CLUSTERING RUN METADATA
# ============================================================

def create_clustering_run(config: dict, stats: dict) -> ObjectId:
    """
    Create a new clustering run record.
    
    Args:
        config: Clustering parameters used
        stats: Summary statistics
        
    Returns:
        ObjectId of created run
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
    
    Args:
        entity_info: {name, entity_type, ticker, entity_id, ...}
        tag: Tag name (e.g., "product_launch")
        cluster_label: DBSCAN label
        articles: List of article dicts with full data
        time_window: {start_utc, end_utc}
        clustering_run_id: Reference to clustering run
        
    Returns:
        ObjectId of created cluster
    """
    clusters_col = get_collection("story_clusters")
    
    # Generate cluster ID
    identifier = entity_info.get('ticker') or entity_info.get('entity_id')
    date_str = time_window['start_utc'].strftime("%Y%m%d")
    cluster_id = f"{identifier}_{tag}_{cluster_label}_{date_str}"
    
    # Extract article metadata
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
    
    # Calculate cluster metadata
    first_published = min(published_times) if published_times else None
    last_published = max(published_times) if published_times else None
    
    duration_hours = None
    velocity = None
    if first_published and last_published:
        duration = (last_published - first_published).total_seconds() / 3600
        duration_hours = duration
        velocity = len(articles) / duration if duration > 0 else len(articles)
    
    # Build document
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
            "size": len(articles),
            "first_published": first_published,
            "last_published": last_published,
            "duration_hours": duration_hours,
            "velocity": velocity,
            "is_noise": cluster_label == -1
        },
        
        "created_at": datetime.utcnow(),
        "clustering_run_id": clustering_run_id
    }
    
    # Upsert (in case re-running)
    result = clusters_col.update_one(
        {"cluster_id": cluster_id},
        {"$set": cluster_doc},
        upsert=True
    )
    
    return result.upserted_id if result.upserted_id else cluster_doc["cluster_id"]


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
    Write all clusters for a single entity to MongoDB.
    
    Args:
        entity_info: Entity metadata
        tag_results: List of clustering results per tag
        time_window: {start_utc, end_utc}
        clustering_run_id: Reference to clustering run
        
    Returns:
        Stats dict with counts
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
    
    Adds cluster_id to each article for easy lookups.
    
    Args:
        entity_info: Entity metadata
        tag_results: List of clustering results
        clustering_run_id: Reference to clustering run
    """
    embedded_col = get_collection("articles_embedded")
    
    for result in tag_results:
        if not result:
            continue
        
        tag = result["tag"]
        clusters = result["clusters"]
        
        for cluster_label, articles in clusters.items():
            identifier = entity_info.get('ticker') or entity_info.get('entity_id')
            date_str = datetime.utcnow().strftime("%Y%m%d")
            cluster_id = f"{identifier}_{tag}_{cluster_label}_{date_str}"
            
            article_ids = [article["_id"] for article in articles]
            
            # Update all articles in this cluster
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
# QUERY HELPERS (for downstream processing)
# ============================================================

def get_clusters_for_entity(entity_id: str, min_size: int = 2) -> List[dict]:
    """
    Retrieve all clusters for an entity.
    
    Args:
        entity_id: Entity identifier
        min_size: Minimum cluster size (default: exclude noise and singletons)
        
    Returns:
        List of cluster documents
    """
    clusters_col = get_collection("story_clusters")
    
    query = {
        "entity_id": entity_id,
        "cluster_metadata.size": {"$gte": min_size},
        "cluster_metadata.is_noise": False
    }
    
    return list(clusters_col.find(query).sort("cluster_metadata.size", -1))


def get_clusters_by_tag(tag: str, min_size: int = 2) -> List[dict]:
    """
    Retrieve all clusters with a specific tag.
    
    Args:
        tag: Tag name
        min_size: Minimum cluster size
        
    Returns:
        List of cluster documents
    """
    clusters_col = get_collection("story_clusters")
    
    query = {
        "tag": tag,
        "cluster_metadata.size": {"$gte": min_size},
        "cluster_metadata.is_noise": False
    }
    
    return list(clusters_col.find(query).sort("cluster_metadata.size", -1))


def get_articles_for_cluster(cluster_id: str) -> List[dict]:
    """
    Retrieve full article documents for a cluster.
    
    Args:
        cluster_id: Cluster identifier
        
    Returns:
        List of article documents from articles_embedded
    """
    clusters_col = get_collection("story_clusters")
    embedded_col = get_collection("articles_embedded")
    
    # Get cluster
    cluster = clusters_col.find_one({"cluster_id": cluster_id})
    
    if not cluster:
        return []
    
    # Get article IDs
    article_ids = [ref["article_id"] for ref in cluster["articles"]]
    
    # Fetch articles
    return list(embedded_col.find({"_id": {"$in": article_ids}}))


def get_clusters_for_stance_detection(
    min_size: int = 3,
    max_age_days: int = 7
) -> List[dict]:
    """
    Get clusters ready for stance detection.
    
    Args:
        min_size: Minimum articles per cluster
        max_age_days: Maximum age in days
        
    Returns:
        List of cluster documents
    """
    clusters_col = get_collection("story_clusters")
    
    cutoff_date = datetime.utcnow() - timedelta(days=max_age_days)
    
    query = {
        "cluster_metadata.size": {"$gte": min_size},
        "cluster_metadata.is_noise": False,
        "time_window.end_utc": {"$gte": cutoff_date}
    }
    
    return list(
        clusters_col.find(query)
        .sort([
            ("cluster_metadata.size", -1),
            ("time_window.end_utc", -1)
        ])
    )


# ============================================================
# INDEX CREATION (run once)
# ============================================================

def create_indexes():
    """
    Create indexes for efficient querying.
    Run this once after setting up the collections.
    """
    clusters_col = get_collection("story_clusters")
    
    # Compound indexes for common queries
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
# STATS AND MONITORING
# ============================================================

def get_clustering_stats() -> dict:
    """
    Get summary statistics about clustering results.
    
    Returns:
        Stats dictionary
    """
    clusters_col = get_collection("story_clusters")
    
    total_clusters = clusters_col.count_documents({})
    non_noise_clusters = clusters_col.count_documents({"cluster_metadata.is_noise": False})
    noise_clusters = clusters_col.count_documents({"cluster_metadata.is_noise": True})
    
    # Count by tag
    tag_pipeline = [
        {"$match": {"cluster_metadata.is_noise": False}},
        {"$group": {
            "_id": "$tag",
            "count": {"$sum": 1},
            "total_articles": {"$sum": "$cluster_metadata.size"}
        }},
        {"$sort": {"count": -1}}
    ]
    
    tag_stats = list(clusters_col.aggregate(tag_pipeline))
    
    # Count by entity
    entity_pipeline = [
        {"$match": {"cluster_metadata.is_noise": False}},
        {"$group": {
            "_id": "$entity_name",
            "count": {"$sum": 1},
            "total_articles": {"$sum": "$cluster_metadata.size"}
        }},
        {"$sort": {"count": -1}}
    ]
    
    entity_stats = list(clusters_col.aggregate(entity_pipeline))
    
    return {
        "total_clusters": total_clusters,
        "non_noise_clusters": non_noise_clusters,
        "noise_clusters": noise_clusters,
        "by_tag": tag_stats,
        "by_entity": entity_stats
    }


if __name__ == "__main__":
    # Create indexes on first run
    create_indexes()
    
    # Print stats
    stats = get_clustering_stats()
    print("\n=== Clustering Statistics ===")
    print(f"Total clusters: {stats['total_clusters']}")
    print(f"Non-noise: {stats['non_noise_clusters']}")
    print(f"Noise: {stats['noise_clusters']}")
    print("\nTop tags:")
    for tag_stat in stats['by_tag'][:5]:
        print(f"  {tag_stat['_id']}: {tag_stat['count']} clusters, {tag_stat['total_articles']} articles")