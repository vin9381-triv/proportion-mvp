"""
Duplicate Cluster Checker

Check if there are duplicate clusters for same (entity, window_days) combination.
This should NOT happen with snapshot mode, but let's verify.
"""

from collections import defaultdict
from processing.common.mongo_client import get_collection


def check_for_duplicates():
    """Check for duplicate clusters."""
    
    print("=" * 80)
    print("DUPLICATE CLUSTER CHECKER")
    print("=" * 80)
    
    clusters_col = get_collection('story_clusters')
    
    # Fetch all clusters
    clusters = list(clusters_col.find(
        {},
        {'cluster_id': 1, 'entity_id': 1, 'entity_name': 1, 'window_days': 1, 'tag': 1, 'created_at': 1}
    ))
    
    print(f"\nTotal clusters: {len(clusters)}")
    
    # Group by (entity_id, window_days, tag)
    grouped = defaultdict(list)
    
    for cluster in clusters:
        key = (
            cluster.get('entity_id'),
            cluster.get('window_days'),
            cluster.get('tag')
        )
        grouped[key].append(cluster)
    
    # Find duplicates
    duplicates = {}
    for key, cluster_list in grouped.items():
        if len(cluster_list) > 1:
            duplicates[key] = cluster_list
    
    if not duplicates:
        print("\n✅ NO DUPLICATES FOUND")
        print("Each (entity, window, tag) combination has exactly 1 cluster")
        return
    
    # Report duplicates
    print(f"\n❌ FOUND {len(duplicates)} DUPLICATE GROUPS")
    print("=" * 80)
    
    for key, cluster_list in duplicates.items():
        entity_id, window_days, tag = key
        entity_name = cluster_list[0].get('entity_name', 'Unknown')
        
        print(f"\n🔴 DUPLICATE GROUP:")
        print(f"   Entity: {entity_name} ({entity_id})")
        print(f"   Window: {window_days}d")
        print(f"   Tag: {tag}")
        print(f"   Count: {len(cluster_list)} clusters (should be 1!)")
        print(f"\n   Clusters:")
        
        for idx, cluster in enumerate(cluster_list, 1):
            cluster_id = cluster.get('cluster_id')
            created_at = cluster.get('created_at')
            created_str = created_at.strftime('%Y-%m-%d %H:%M:%S') if created_at else 'Unknown'
            
            print(f"     {idx}. {cluster_id}")
            print(f"        Created: {created_str}")
    
    print("\n" + "=" * 80)
    print("RECOMMENDATION:")
    print("=" * 80)
    print("If duplicates exist, they're from multiple clustering runs.")
    print("Solution: Re-run clustering to replace old clusters with fresh snapshots.")
    print("\nOr manually delete duplicates:")
    print("  python fix_duplicates.py --delete-oldest")


def check_by_timeframe():
    """Show cluster distribution by timeframe."""
    
    print("\n" + "=" * 80)
    print("CLUSTER DISTRIBUTION BY TIMEFRAME")
    print("=" * 80)
    
    clusters_col = get_collection('story_clusters')
    
    pipeline = [
        {
            '$group': {
                '_id': {
                    'window_days': '$window_days',
                    'entity_id': '$entity_id'
                },
                'count': {'$sum': 1}
            }
        },
        {
            '$group': {
                '_id': '$_id.window_days',
                'total_clusters': {'$sum': 1},
                'avg_clusters_per_entity': {'$avg': '$count'},
                'max_clusters_per_entity': {'$max': '$count'}
            }
        },
        {'$sort': {'_id': 1}}
    ]
    
    results = list(clusters_col.aggregate(pipeline))
    
    print("\nBy Window:")
    for result in results:
        window = result['_id']
        total = result['total_clusters']
        avg = result['avg_clusters_per_entity']
        max_val = result['max_clusters_per_entity']
        
        print(f"\n  {window}d window:")
        print(f"    Total entity-window combinations: {total}")
        print(f"    Avg clusters per entity: {avg:.1f}")
        print(f"    Max clusters per entity: {max_val}")


def check_overlapping_articles():
    """Check if articles appear in multiple clusters."""
    
    print("\n" + "=" * 80)
    print("ARTICLE OVERLAP ANALYSIS")
    print("=" * 80)
    
    clusters_col = get_collection('story_clusters')
    
    # Get all article IDs from all clusters
    article_to_clusters = defaultdict(list)
    
    for cluster in clusters_col.find({}, {'cluster_id': 1, 'article_ids': 1, 'window_days': 1}):
        cluster_id = cluster.get('cluster_id')
        window_days = cluster.get('window_days')
        article_ids = cluster.get('article_ids', [])
        
        for article_id in article_ids:
            article_to_clusters[article_id].append((cluster_id, window_days))
    
    # Find articles in multiple clusters
    multi_cluster_articles = {
        aid: clusters 
        for aid, clusters in article_to_clusters.items() 
        if len(clusters) > 1
    }
    
    print(f"\nTotal unique articles: {len(article_to_clusters)}")
    print(f"Articles in multiple clusters: {len(multi_cluster_articles)}")
    
    if multi_cluster_articles:
        print(f"\nThis is EXPECTED for multi-timeframe clustering:")
        print(f"  - Article from 2 days ago appears in: 3d, 7d, 14d, 30d windows")
        print(f"  - This is correct behavior!")
        
        # Show example
        example_article_id = list(multi_cluster_articles.keys())[0]
        example_clusters = multi_cluster_articles[example_article_id]
        
        print(f"\nExample article: {example_article_id}")
        print(f"  Appears in {len(example_clusters)} clusters:")
        for cluster_id, window_days in example_clusters:
            print(f"    - {cluster_id} ({window_days}d)")


if __name__ == "__main__":
    # Check for duplicates
    check_for_duplicates()
    
    # Show distribution
    check_by_timeframe()
    
    # Check article overlap
    check_overlapping_articles()