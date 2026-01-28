"""
Unified Story Clustering Pipeline
==================================

Two-stage clustering approach:
1. Tag-based pre-filtering (removes noise, groups by story type)
2. DBSCAN clustering within each tag bucket (finds actual story clusters)

Works for both:
- Tech companies (ticker-based)
- Macro/Industry entities (entity_id-based)
"""

from datetime import datetime, timedelta
from collections import defaultdict
import os
import yaml
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
COMPANY_WINDOW_DAYS = 3
INDUSTRY_WINDOW_DAYS = 7

# Minimum thresholds
MIN_ARTICLES_PER_ENTITY = 5
MIN_ARTICLES_PER_TAG_BUCKET = 2  # Need at least 3 for DBSCAN

# Tags to exclude from clustering (noise)
EXCLUDED_TAGS = {"crime_noise", "spam_clickbait", "other"}

# DBSCAN parameters
DBSCAN_EPS = 0.5
DBSCAN_MIN_SAMPLES = 2


# ============================================================
# TAG CONFIGURATION LOADER
# ============================================================

def load_tag_config(config_path):
    """Load tag configuration from YAML."""
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    if "tags" not in config:
        raise ValueError("Invalid tag config: missing 'tags' key")
    
    return config["tags"]


# ============================================================
# ENTITY LOADER
# ============================================================

def load_entities(ticker_config_path):
    """
    Load all entities (companies + industries/macro entities).
    
    Returns list of dicts with keys:
    - entity_type: "company" | "industry" | "monetary_policy" | etc.
    - name
    - ticker (for companies) or entity_id (for industries)
    - window_days (computed based on entity_type)
    """
    with open(ticker_config_path, "r") as f:
        config = yaml.safe_load(f)
    
    entities = []
    
    # Add companies
    for company in config.get("companies", []):
        entities.append({
            "entity_type": "company",
            "name": company["name"],
            "ticker": company["ticker"],
            "entity_id": None,
            "window_days": COMPANY_WINDOW_DAYS
        })
    
    # Add all industry/macro entities
    # These can be under various keys: monetary_policy, industries, etc.
    for key in config.keys():
        if key == "companies":
            continue
        
        for entity in config.get(key, []):
            if "entity_id" in entity and "name" in entity:
                entities.append({
                    "entity_type": entity.get("entity_type", key),
                    "name": entity["name"],
                    "ticker": None,
                    "entity_id": entity["entity_id"],
                    "window_days": INDUSTRY_WINDOW_DAYS
                })
    
    return entities


# ============================================================
# TAGGING LOGIC
# ============================================================

class ArticleTagger:
    """
    Tags articles based on keyword matching.
    First match wins, falls back to 'other'.
    """
    
    def __init__(self, tag_config):
        self.tag_config = tag_config
    
    def tag_article(self, article):
        """
        Assign single primary tag to article.
        
        Args:
            article: dict with 'title' and 'body' fields
            
        Returns:
            str: tag name
        """
        text = (
            (article.get("title") or "") + " " +
            (article.get("body") or "")
        ).lower()
        
        for tag, cfg in self.tag_config.items():
            keywords = cfg.get("keywords", [])
            for kw in keywords:
                if kw.lower() in text:
                    return tag
        
        return "other"
    
    def tag_articles(self, articles):
        """
        Tag multiple articles and organize into buckets.
        
        Args:
            articles: list of article dicts
            
        Returns:
            dict: {tag_name: [article, article, ...]}
        """
        tag_buckets = defaultdict(list)
        
        for article in articles:
            tag = self.tag_article(article)
            tag_buckets[tag].append(article)
        
        return dict(tag_buckets)


# ============================================================
# CLUSTERING LOGIC
# ============================================================

def cluster_tag_bucket(articles, tag_name):
    """
    Cluster articles within a single tag bucket.
    
    Args:
        articles: list of article dicts (all same tag)
        tag_name: str, name of the tag
        
    Returns:
        dict: {
            "tag": tag_name,
            "total_articles": int,
            "clusters": {
                cluster_id: [article, article, ...],
                ...
            }
        }
    """
    if len(articles) < MIN_ARTICLES_PER_TAG_BUCKET:
        return None
    
    # Build feature matrix - NOTE: we need both embeddings AND original articles
    vectors = []
    valid_articles = []
    
    for article in articles:
        # Skip articles without embeddings
        if "embeddings" not in article or "body" not in article["embeddings"]:
            continue
        
        vectors.append(article["embeddings"]["body"])
        valid_articles.append(article)  # Keep the FULL article object
    
    if len(vectors) < MIN_ARTICLES_PER_TAG_BUCKET:
        return None
    
    X = np.array(vectors)
    
    # Run DBSCAN
    labels = run_dbscan(X, eps=DBSCAN_EPS, min_samples=DBSCAN_MIN_SAMPLES)
    
    # Organize into clusters
    clusters = defaultdict(list)
    for label, article in zip(labels, valid_articles):
        clusters[label].append(article)
    
    return {
        "tag": tag_name,
        "total_articles": len(articles),
        "clusters": dict(clusters)
    }


# ============================================================
# OUTPUT WRITER
# ============================================================

def write_clustering_results(entity_info, tag_results, output_path):
    """
    Write human-readable clustering results to file.
    
    Args:
        entity_info: dict with entity metadata
        tag_results: list of clustering results per tag
        output_path: str, path to output file
    """
    with open(output_path, "w", encoding="utf-8") as f:
        # Header
        f.write("=" * 80 + "\n")
        f.write("PROPORTION — Story Clustering Results\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated at: {datetime.utcnow().isoformat()} UTC\n\n")
        
        f.write(f"Entity      : {entity_info['name']}\n")
        f.write(f"Type        : {entity_info['entity_type']}\n")
        f.write(f"Identifier  : {entity_info.get('ticker') or entity_info.get('entity_id')}\n")
        f.write(f"Window      : {entity_info['start_utc'].date()} → {entity_info['end_utc'].date()}\n")
        f.write("=" * 80 + "\n\n")
        
        # Summary stats
        f.write("SUMMARY\n")
        f.write("-" * 80 + "\n")
        total_articles = sum(r["total_articles"] for r in tag_results if r)
        total_tags = len([r for r in tag_results if r])
        total_clusters = sum(len(r["clusters"]) for r in tag_results if r)
        
        f.write(f"Total Articles Processed: {total_articles}\n")
        f.write(f"Active Tag Buckets      : {total_tags}\n")
        f.write(f"Total Clusters Found    : {total_clusters}\n\n")
        
        # Tag distribution
        f.write("TAG DISTRIBUTION\n")
        f.write("-" * 80 + "\n")
        for result in tag_results:
            if result:
                tag = result["tag"]
                count = result["total_articles"]
                num_clusters = len(result["clusters"])
                f.write(f"  {tag:<30} {count:>4} articles → {num_clusters} clusters\n")
        f.write("\n\n")
        
        # Detailed cluster results
        f.write("=" * 80 + "\n")
        f.write("DETAILED CLUSTER RESULTS\n")
        f.write("=" * 80 + "\n\n")
        
        for result in tag_results:
            if not result:
                continue
            
            tag = result["tag"]
            clusters = result["clusters"]
            
            f.write("=" * 80 + "\n")
            f.write(f"TAG: {tag.upper()}\n")
            f.write("=" * 80 + "\n\n")
            
            # Sort clusters by size (largest first)
            sorted_clusters = sorted(
                clusters.items(),
                key=lambda x: len(x[1]),
                reverse=True
            )
            
            for cluster_id, members in sorted_clusters:
                f.write("-" * 80 + "\n")
                
                if cluster_id == -1:
                    f.write(f"NOISE (Cluster ID: {cluster_id})\n")
                else:
                    f.write(f"Cluster ID: {cluster_id}\n")
                
                f.write(f"Size: {len(members)} articles\n")
                f.write("-" * 80 + "\n\n")
                
                for article in members:
                    title = article.get("title", "").strip()
                    published_utc = article.get("published_at_utc", "N/A")
                    
                    f.write(f"• {title}\n")
                    f.write(f"  Published: {published_utc}\n\n")
                
                f.write("\n")
            
            f.write("\n\n")


# ============================================================
# MAIN PIPELINE
# ============================================================

def run_clustering_pipeline():
    """
    Main clustering pipeline entry point.
    
    Process:
    1. Load entities and tags
    2. For each entity:
       a. Fetch articles in time window
       b. Tag all articles
       c. For each tag bucket (excluding noise):
          - Run DBSCAN clustering
       d. Write results to file
    """
    print("=" * 80)
    print("Starting Unified Story Clustering Pipeline")
    print("=" * 80)
    print()
    
    # Load configurations
    entities = load_entities(TICKER_CONFIG_PATH)
    tag_config = load_tag_config(TAG_CONFIG_PATH)
    tagger = ArticleTagger(tag_config)
    
    embedded_col = get_collection("articles_embedded")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print(f"Loaded {len(entities)} entities")
    print(f"Loaded {len(tag_config)} tags")
    print(f"Excluding tags: {', '.join(EXCLUDED_TAGS)}")
    print()
    
    # Process each entity
    for idx, entity in enumerate(entities, 1):
        print(f"\n{'=' * 80}")
        print(f"[{idx}/{len(entities)}] Processing: {entity['name']}")
        print(f"{'=' * 80}")
        
        # Time window
        end_utc = datetime.utcnow()
        start_utc = end_utc - timedelta(days=entity['window_days'])
        
        print(f"Type       : {entity['entity_type']}")
        print(f"Identifier : {entity.get('ticker') or entity.get('entity_id')}")
        print(f"Window     : {start_utc.date()} → {end_utc.date()}")
        
        # Fetch raw article IDs
        raw_article_ids = get_raw_article_ids_for_entity(
            start_utc=start_utc,
            end_utc=end_utc,
            entity_type=entity['entity_type'],
            tickers=[entity['ticker']] if entity['ticker'] else None,
            entity_id=entity['entity_id']
        )
        
        print(f"Raw articles: {len(raw_article_ids)}")
        
        if len(raw_article_ids) < MIN_ARTICLES_PER_ENTITY:
            print("❌ Not enough raw articles, skipping\n")
            continue
        
        # Fetch embedded articles (dedup-safe)
        articles = list(
            embedded_col.find(
                get_clustering_candidates_query(raw_article_ids)
            )
        )
        
        print(f"Embedded articles (post-dedup): {len(articles)}")
        
        if len(articles) < MIN_ARTICLES_PER_ENTITY:
            print("❌ Not enough embedded articles, skipping\n")
            continue
        
        # STAGE 1: Tag all articles
        print("\n--- STAGE 1: Tagging ---")
        tag_buckets = tagger.tag_articles(articles)
        
        print("Tag distribution:")
        for tag, items in sorted(tag_buckets.items(), key=lambda x: len(x[1]), reverse=True):
            status = "❌ EXCLUDED" if tag in EXCLUDED_TAGS else "✓"
            print(f"  {status} {tag:<30} {len(items):>4} articles")
        
        # STAGE 2: Cluster each tag bucket
        print("\n--- STAGE 2: Clustering ---")
        tag_results = []
        
        for tag, tag_articles in tag_buckets.items():
            # Skip excluded tags
            if tag in EXCLUDED_TAGS:
                continue
            
            # Skip small buckets
            if len(tag_articles) < MIN_ARTICLES_PER_TAG_BUCKET:
                print(f"  ⚠️  {tag}: {len(tag_articles)} articles (too few, skipping)")
                continue
            
            print(f"  Clustering {tag}: {len(tag_articles)} articles...", end=" ")
            
            result = cluster_tag_bucket(tag_articles, tag)
            
            if result:
                num_clusters = len(result["clusters"])
                print(f"✓ {num_clusters} clusters found")
                tag_results.append(result)
            else:
                print("❌ Failed (insufficient embeddings)")
        
        # Write results
        if tag_results:
            identifier = entity.get('ticker') or entity.get('entity_id')
            filename = f"clustering_{identifier}_{start_utc.date()}_to_{end_utc.date()}.txt"
            output_path = os.path.join(OUTPUT_DIR, filename)
            
            entity_info = {
                **entity,
                "start_utc": start_utc,
                "end_utc": end_utc
            }
            
            write_clustering_results(entity_info, tag_results, output_path)
            
            print(f"\n✓ Results written to: {output_path}")
        else:
            print("\n❌ No clusters generated for this entity")
    
    print("\n" + "=" * 80)
    print("Pipeline Complete")
    print("=" * 80)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    run_clustering_pipeline()