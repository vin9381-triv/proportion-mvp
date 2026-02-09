"""
Multi-Timeframe Clustering - Snapshot Mode

Simplest approach:
- For each (entity, window_days) pair, keep only latest clustering result
- Delete old clusters before creating new ones
- No deduplication logic needed
- Re-running is safe (overwrites previous)

Time windows:
- 3d: Breaking stories
- 7d: Developing narratives
- 14d: Ongoing stories
- 30d: Long-term trends
"""

import os
import yaml
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path
import numpy as np

from processing.common.mongo_client import get_collection
from processing.clustering.input_resolver import get_raw_article_ids_for_entity
from processing.clustering.queries import get_clustering_candidates_query
from processing.clustering.density_stage import run_dbscan


# ============================================================
# CONFIGURATION
# ============================================================

TICKER_CONFIG_PATH = "config/ticker.yaml"
TAG_CONFIG_PATH = "config/clustering_tags.yaml"
OUTPUT_DIR = "processing/clustering/outputs"

# Time windows (days)
TIME_WINDOWS = [3, 7, 14, 30]

# Minimum articles required per window
MIN_ARTICLES = {
    3: 3,
    7: 5,
    14: 8,
    30: 10,
}

# Tags to exclude
EXCLUDED_TAGS = {"crime_noise", "spam_clickbait", "other"}

# DBSCAN parameters
DBSCAN_EPS = 0.5
DBSCAN_MIN_SAMPLES = 2


# ============================================================
# ENTITY LOADER
# ============================================================

def load_entities():
    """Load entities from ticker.yaml."""
    with open(TICKER_CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f)
    
    entities = []
    
    # Companies
    for company in config.get('companies', []):
        entities.append({
            'entity_id': company['entity_id'],
            'entity_name': company['name'],
            'entity_type': 'company',
            'ticker': company.get('ticker'),
        })
    
    # Other entities
    for key in ['monetary_policy', 'macroeconomic_dollar', 'macroeconomic_inflation', 'industries', 'physical_demand']:
        for entity in config.get(key, []):
            entities.append({
                'entity_id': entity['entity_id'],
                'entity_name': entity['name'],
                'entity_type': entity.get('entity_type', key),
                'ticker': None,
            })
    
    return entities


# ============================================================
# TAG LOADER
# ============================================================

def load_tag_config():
    """Load tag configuration."""
    with open(TAG_CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f)
    return config.get('tags', {})


# ============================================================
# ARTICLE TAGGER
# ============================================================

class ArticleTagger:
    """Tag articles based on keyword matching."""
    
    def __init__(self, tag_config):
        self.tag_config = tag_config
    
    def tag_article(self, article):
        """Assign single tag to article."""
        text = (
            (article.get("title") or "") + " " +
            (article.get("raw_text") or "")
        ).lower()
        
        # Check noise tags FIRST
        for tag in ["crime_noise", "spam_clickbait"]:
            if tag in self.tag_config:
                keywords = self.tag_config[tag].get("keywords", [])
                for kw in keywords:
                    if kw.lower() in text:
                        return tag
        
        # Then check other tags
        for tag, cfg in self.tag_config.items():
            if tag in ["crime_noise", "spam_clickbait"]:
                continue
            keywords = cfg.get("keywords", [])
            for kw in keywords:
                if kw.lower() in text:
                    return tag
        
        return "other"


# ============================================================
# CLUSTERING LOGIC
# ============================================================

def cluster_articles_for_window(
    entity,
    window_days,
    tagger,
    embedded_col,
    clusters_col
):
    """
    Cluster articles for specific entity + window combination.
    
    Steps:
    1. Delete old clusters for this (entity, window_days)
    2. Fetch articles
    3. Tag and filter
    4. Run DBSCAN
    5. Write new clusters
    """
    
    print(f"\n  [{window_days}d window]")
    
    # STEP 1: Delete old clusters for this (entity, window_days)
    delete_result = clusters_col.delete_many({
        'entity_id': entity['entity_id'],
        'window_days': window_days
    })
    
    if delete_result.deleted_count > 0:
        print(f"    🗑️  Deleted {delete_result.deleted_count} old clusters")
    
    # STEP 2: Fetch articles
    end_utc = datetime.utcnow()
    start_utc = end_utc - timedelta(days=window_days)
    
    raw_article_ids = get_raw_article_ids_for_entity(
        start_utc=start_utc,
        end_utc=end_utc,
        entity_type=entity['entity_type'],
        tickers=[entity['ticker']] if entity['ticker'] else None,
        entity_id=entity['entity_id']
    )
    
    print(f"    Raw articles: {len(raw_article_ids)}")
    
    if len(raw_article_ids) < MIN_ARTICLES[window_days]:
        print(f"    ⚠️  Not enough articles (need {MIN_ARTICLES[window_days]})")
        return 0
    
    articles = list(
        embedded_col.find(
            get_clustering_candidates_query(raw_article_ids)
        )
    )
    
    print(f"    Embedded: {len(articles)}")
    
    if len(articles) < MIN_ARTICLES[window_days]:
        print(f"    ⚠️  Not enough embedded")
        return 0
    
    # STEP 3: Tag and filter
    tag_buckets = defaultdict(list)
    for article in articles:
        tag = tagger.tag_article(article)
        if tag not in EXCLUDED_TAGS:
            tag_buckets[tag].append(article)
    
    print(f"    Tag buckets: {len(tag_buckets)}")
    
    # STEP 4 & 5: Cluster each tag bucket and write
    total_clusters = 0
    
    for tag, tag_articles in tag_buckets.items():
        if len(tag_articles) < 2:
            continue
        
        # Extract embeddings
        vectors = []
        valid_articles = []
        
        for article in tag_articles:
            if 'embeddings' in article and 'body' in article['embeddings']:
                vectors.append(article['embeddings']['body'])
                valid_articles.append(article)
        
        if len(vectors) < 2:
            continue
        
        # Run DBSCAN
        X = np.array(vectors)
        labels = run_dbscan(X, eps=DBSCAN_EPS, min_samples=DBSCAN_MIN_SAMPLES)
        
        # Group by cluster label
        clusters_dict = defaultdict(list)
        for label, article in zip(labels, valid_articles):
            clusters_dict[int(label)].append(article)
        
        # Write clusters
        for label, members in clusters_dict.items():
            # Skip tiny noise clusters
            if label == -1 and len(members) < 2:
                continue
            
            write_cluster(
                entity=entity,
                tag=tag,
                label=label,
                window_days=window_days,
                members=members,
                clusters_col=clusters_col
            )
            
            total_clusters += 1
    
    print(f"    ✅ Created {total_clusters} clusters")
    return total_clusters


def write_cluster(entity, tag, label, window_days, members, clusters_col):
    """Write cluster to MongoDB."""
    
    # Build cluster ID
    date_str = datetime.utcnow().strftime("%Y%m%d")
    cluster_id = f"{entity['entity_id']}|{tag}|{label}|{window_days}d|{date_str}"
    
    # Build article references
    article_refs = []
    article_ids = []
    published_times = []
    embeddings_list = []
    
    for article in members:
        article_refs.append({
            'article_id': article['_id'],
            'title': article.get('title'),
            'published_at_utc': article.get('published_at_utc'),
            'raw_article_id': article.get('raw_article_id')
        })
        
        article_ids.append(article['_id'])
        
        if article.get('published_at_utc'):
            published_times.append(article['published_at_utc'])
        
        emb = article.get('embeddings', {}).get('body')
        if emb:
            embeddings_list.append(emb)
    
    # Calculate centroid
    centroid = None
    if embeddings_list:
        centroid = np.mean(np.array(embeddings_list), axis=0).tolist()
    
    # Temporal metadata
    first_published = min(published_times) if published_times else None
    last_published = max(published_times) if published_times else None
    
    # Build document
    doc = {
        'cluster_id': cluster_id,
        
        # Entity
        'entity_id': entity['entity_id'],
        'entity_name': entity['entity_name'],
        'entity_type': entity['entity_type'],
        'ticker': entity.get('ticker'),
        
        # Tag
        'tag': tag,
        
        # Timeframe (NEW)
        'window_days': window_days,
        'timeframe': f"{window_days}d",
        
        # Cluster metadata
        'cluster_label': int(label),
        'is_closed': False,
        
        # Articles
        'articles': article_refs,
        'article_ids': article_ids,
        
        # Geometry
        'centroid': centroid,
        
        # Metrics
        'cluster_metadata': {
            'size': len(article_ids),
            'first_published': first_published,
            'last_published': last_published,
            'is_noise': bool(label == -1)
        },
        
        # Audit
        'created_at': datetime.utcnow(),
        'last_updated': datetime.utcnow(),
    }
    
    # Write
    clusters_col.insert_one(doc)


# ============================================================
# MAIN PIPELINE
# ============================================================

def run_multi_timeframe_clustering():
    """
    Run clustering with multiple time windows.
    
    For each (entity, window_days):
    - Delete old clusters
    - Create new clusters
    - Result: Latest snapshot only
    """
    
    print("=" * 80)
    print("MULTI-TIMEFRAME CLUSTERING (SNAPSHOT MODE)")
    print("=" * 80)
    print(f"\nTime windows: {TIME_WINDOWS} days")
    print()
    
    # Load config
    entities = load_entities()
    tag_config = load_tag_config()
    tagger = ArticleTagger(tag_config)
    
    embedded_col = get_collection('articles_embedded')
    clusters_col = get_collection('story_clusters')
    
    print(f"Loaded {len(entities)} entities")
    print(f"Loaded {len(tag_config)} tags")
    print(f"Excluded tags: {EXCLUDED_TAGS}")
    print()
    
    # Stats
    total_clusters = 0
    stats_by_window = {w: 0 for w in TIME_WINDOWS}
    
    # Process each entity
    for idx, entity in enumerate(entities, 1):
        print(f"\n{'=' * 80}")
        print(f"[{idx}/{len(entities)}] {entity['entity_name']} ({entity['entity_type']})")
        print(f"{'=' * 80}")
        
        # Process each time window
        for window_days in TIME_WINDOWS:
            clusters_created = cluster_articles_for_window(
                entity=entity,
                window_days=window_days,
                tagger=tagger,
                embedded_col=embedded_col,
                clusters_col=clusters_col
            )
            
            total_clusters += clusters_created
            stats_by_window[window_days] += clusters_created
    
    # Final stats
    print("\n" + "=" * 80)
    print("CLUSTERING COMPLETE")
    print("=" * 80)
    print(f"\nTotal clusters created: {total_clusters}")
    print("\nBy timeframe:")
    for window_days in TIME_WINDOWS:
        print(f"  {window_days}d: {stats_by_window[window_days]} clusters")
    print("=" * 80)


if __name__ == "__main__":
    run_multi_timeframe_clustering()