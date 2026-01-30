#!/usr/bin/env python3
"""
Cluster Viewer - View Story Clusters from MongoDB
==================================================

Usage:
  python view_clusters.py                    # View all clusters
  python view_clusters.py --entity MSFT      # View MSFT clusters
  python view_clusters.py --tag product_launch  # View product launch clusters
  python view_clusters.py --min-size 5       # Only clusters with 5+ articles
  python view_clusters.py --latest           # Only today's clusters
"""

import argparse
from datetime import datetime, timedelta
from processing.clustering.cluster_mongodb_writer import (
    get_clusters_for_entity,
    get_clusters_by_tag,
    get_articles_for_cluster,
    get_clustering_stats
)
from processing.common.mongo_client import get_collection


def format_cluster_summary(cluster):
    """Format a cluster for display."""
    lines = []
    lines.append("=" * 80)
    lines.append(f"Cluster ID: {cluster['cluster_id']}")
    lines.append(f"Entity: {cluster['entity_name']} ({cluster['entity_type']})")
    lines.append(f"Tag: {cluster['tag']}")
    lines.append(f"Size: {cluster['cluster_metadata']['size']} articles")
    
    if cluster['cluster_metadata'].get('first_published'):
        lines.append(f"First Published: {cluster['cluster_metadata']['first_published']}")
    if cluster['cluster_metadata'].get('last_published'):
        lines.append(f"Last Published: {cluster['cluster_metadata']['last_published']}")
    
    if cluster['cluster_metadata'].get('velocity'):
        lines.append(f"Velocity: {cluster['cluster_metadata']['velocity']:.2f} articles/hour")
    
    lines.append("-" * 80)
    lines.append("Articles:")
    
    for article_ref in cluster['articles']:
        lines.append(f"  • {article_ref['title']}")
        lines.append(f"    Published: {article_ref['published_at_utc']}")
    
    lines.append("")
    return "\n".join(lines)


def view_all_clusters(min_size=2, max_results=50):
    """View all clusters."""
    clusters_col = get_collection("story_clusters")
    
    query = {
        "cluster_metadata.size": {"$gte": min_size},
        "cluster_metadata.is_noise": False
    }
    
    clusters = list(
        clusters_col.find(query)
        .sort("cluster_metadata.size", -1)
        .limit(max_results)
    )
    
    print(f"\n{'=' * 80}")
    print(f"ALL CLUSTERS (min_size={min_size}, showing top {max_results})")
    print(f"{'=' * 80}\n")
    
    for cluster in clusters:
        print(format_cluster_summary(cluster))
    
    print(f"\nTotal clusters shown: {len(clusters)}")


def view_clusters_by_entity(entity_identifier, min_size=2):
    """View clusters for a specific entity."""
    # Try to find entity_id from ticker
    # This is a simple lookup - you may need to enhance this
    clusters_col = get_collection("story_clusters")
    
    # Search by ticker or entity_id
    query = {
        "$or": [
            {"ticker": entity_identifier},
            {"entity_id": entity_identifier},
            {"entity_name": {"$regex": entity_identifier, "$options": "i"}}
        ],
        "cluster_metadata.size": {"$gte": min_size},
        "cluster_metadata.is_noise": False
    }
    
    clusters = list(
        clusters_col.find(query)
        .sort("cluster_metadata.size", -1)
    )
    
    if not clusters:
        print(f"\n❌ No clusters found for entity: {entity_identifier}")
        return
    
    print(f"\n{'=' * 80}")
    print(f"CLUSTERS FOR: {entity_identifier}")
    print(f"{'=' * 80}\n")
    
    for cluster in clusters:
        print(format_cluster_summary(cluster))
    
    print(f"\nTotal clusters: {len(clusters)}")


def view_clusters_by_tag(tag, min_size=2):
    """View clusters with a specific tag."""
    clusters = get_clusters_by_tag(tag, min_size)
    
    if not clusters:
        print(f"\n❌ No clusters found for tag: {tag}")
        return
    
    print(f"\n{'=' * 80}")
    print(f"CLUSTERS WITH TAG: {tag}")
    print(f"{'=' * 80}\n")
    
    for cluster in clusters:
        print(format_cluster_summary(cluster))
    
    print(f"\nTotal clusters: {len(clusters)}")


def view_latest_clusters(hours=24, min_size=2):
    """View clusters created in the last N hours."""
    clusters_col = get_collection("story_clusters")
    
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    
    query = {
        "created_at": {"$gte": cutoff},
        "cluster_metadata.size": {"$gte": min_size},
        "cluster_metadata.is_noise": False
    }
    
    clusters = list(
        clusters_col.find(query)
        .sort("created_at", -1)
    )
    
    if not clusters:
        print(f"\n❌ No clusters found in the last {hours} hours")
        return
    
    print(f"\n{'=' * 80}")
    print(f"LATEST CLUSTERS (last {hours} hours)")
    print(f"{'=' * 80}\n")
    
    for cluster in clusters:
        print(format_cluster_summary(cluster))
    
    print(f"\nTotal clusters: {len(clusters)}")


def view_cluster_details(cluster_id):
    """View full details of a specific cluster."""
    clusters_col = get_collection("story_clusters")
    
    cluster = clusters_col.find_one({"cluster_id": cluster_id})
    
    if not cluster:
        print(f"\n❌ Cluster not found: {cluster_id}")
        return
    
    print(f"\n{'=' * 80}")
    print(f"CLUSTER DETAILS: {cluster_id}")
    print(f"{'=' * 80}\n")
    
    print(format_cluster_summary(cluster))
    
    # Get full articles
    articles = get_articles_for_cluster(cluster_id)
    
    if articles:
        print("\n" + "=" * 80)
        print("FULL ARTICLE CONTENT")
        print("=" * 80 + "\n")
        
        for article in articles:
            print("-" * 80)
            print(f"Title: {article.get('title', 'N/A')}")
            print(f"Published: {article.get('published_at_utc', 'N/A')}")
            print(f"Source: {article.get('source_name', 'N/A')}")
            
            body = article.get('body', '')
            if body:
                preview = body[:500] + "..." if len(body) > 500 else body
                print(f"\nBody Preview:\n{preview}\n")
            
            print()


def view_statistics():
    """View clustering statistics."""
    stats = get_clustering_stats()
    
    print(f"\n{'=' * 80}")
    print("CLUSTERING STATISTICS")
    print(f"{'=' * 80}\n")
    
    print(f"Total Clusters: {stats['total_clusters']}")
    print(f"  Non-noise: {stats['non_noise_clusters']}")
    print(f"  Noise: {stats['noise_clusters']}")
    
    print(f"\n{'=' * 80}")
    print("TOP TAGS")
    print(f"{'=' * 80}")
    
    for tag_stat in stats['by_tag'][:10]:
        print(f"  {tag_stat['_id']:<30} {tag_stat['count']:>3} clusters, {tag_stat['total_articles']:>4} articles")
    
    print(f"\n{'=' * 80}")
    print("TOP ENTITIES")
    print(f"{'=' * 80}")
    
    for entity_stat in stats['by_entity'][:10]:
        print(f"  {entity_stat['_id']:<30} {entity_stat['count']:>3} clusters, {entity_stat['total_articles']:>4} articles")
    
    print()


def view_ready_for_stance():
    """View clusters ready for stance detection."""
    from processing.clustering.cluster_mongodb_writer import get_clusters_for_stance_detection
    
    clusters = get_clusters_for_stance_detection(min_size=3, max_age_days=7)
    
    if not clusters:
        print("\n❌ No clusters ready for stance detection")
        return
    
    print(f"\n{'=' * 80}")
    print("CLUSTERS READY FOR STANCE DETECTION")
    print(f"{'=' * 80}\n")
    
    for cluster in clusters:
        print(format_cluster_summary(cluster))
    
    print(f"\nTotal clusters ready: {len(clusters)}")


def main():
    parser = argparse.ArgumentParser(description="View story clusters from MongoDB")
    
    parser.add_argument("--entity", help="Filter by entity (ticker or entity_id)")
    parser.add_argument("--tag", help="Filter by tag")
    parser.add_argument("--cluster-id", help="View specific cluster details")
    parser.add_argument("--min-size", type=int, default=2, help="Minimum cluster size (default: 2)")
    parser.add_argument("--latest", action="store_true", help="Show only latest clusters (24h)")
    parser.add_argument("--hours", type=int, default=24, help="Hours for --latest (default: 24)")
    parser.add_argument("--stats", action="store_true", help="Show statistics")
    parser.add_argument("--stance-ready", action="store_true", help="Show clusters ready for stance detection")
    parser.add_argument("--max-results", type=int, default=50, help="Maximum results to show (default: 50)")
    
    args = parser.parse_args()
    
    try:
        if args.stats:
            view_statistics()
        elif args.stance_ready:
            view_ready_for_stance()
        elif args.cluster_id:
            view_cluster_details(args.cluster_id)
        elif args.entity:
            view_clusters_by_entity(args.entity, args.min_size)
        elif args.tag:
            view_clusters_by_tag(args.tag, args.min_size)
        elif args.latest:
            view_latest_clusters(args.hours, args.min_size)
        else:
            view_all_clusters(args.min_size, args.max_results)
    
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()