"""
Multi-Timeframe Cluster Viewer

View clusters organized by timeframe with filtering and stats.
"""

import argparse
from processing.common.mongo_client import get_collection


def view_clusters(
    entity_id=None,
    window_days=None,
    tag=None,
    show_articles=False
):
    """View clusters with filters."""
    
    print("=" * 80)
    print("MULTI-TIMEFRAME CLUSTER VIEWER")
    print("=" * 80)
    
    clusters_col = get_collection('story_clusters')
    
    # Build query
    query = {}
    
    if entity_id:
        query['entity_id'] = entity_id
    
    if window_days:
        query['window_days'] = window_days
    
    if tag:
        query['tag'] = tag
    
    # Fetch clusters
    clusters = list(clusters_col.find(query).sort([
        ('entity_id', 1),
        ('window_days', 1),
        ('tag', 1)
    ]))
    
    if not clusters:
        print("\n❌ No clusters found")
        return
    
    print(f"\nFound {len(clusters)} clusters")
    
    # Group by entity and window
    current_entity = None
    current_window = None
    
    for cluster in clusters:
        entity_name = cluster.get('entity_name')
        entity_id_val = cluster.get('entity_id')
        window = cluster.get('window_days')
        tag_val = cluster.get('tag')
        cluster_id = cluster.get('cluster_id')
        size = cluster.get('cluster_metadata', {}).get('size', 0)
        
        # New entity header
        if entity_id_val != current_entity:
            current_entity = entity_id_val
            current_window = None
            print(f"\n{'=' * 80}")
            print(f"📊 {entity_name}")
            print(f"{'=' * 80}")
        
        # New window header
        if window != current_window:
            current_window = window
            
            timeframe_labels = {
                3: "🔥 BREAKING",
                7: "📰 DEVELOPING", 
                14: "📊 ONGOING",
                30: "📈 LONG-TERM"
            }
            
            label = timeframe_labels.get(window, f"{window}d")
            print(f"\n  [{label} ({window}d window)]")
            print(f"  {'-' * 76}")
        
        # Cluster info
        print(f"    • {tag_val} | {size} articles")
        print(f"      ID: {cluster_id}")
        
        # Show summary if available
        summary = cluster.get('summary', {})
        if summary and summary.get('text'):
            summary_text = summary['text'][:100]
            print(f"      Summary: {summary_text}...")
        
        # Show stance if available
        stance = cluster.get('stance', {})
        if stance:
            label = stance.get('label', 'unknown')
            conf = stance.get('confidence', 0)
            print(f"      Stance: {label.upper()} ({conf:.0%})")
        
        # Show articles if requested
        if show_articles:
            article_refs = cluster.get('articles', [])
            if article_refs:
                print(f"      Articles:")
                for idx, article in enumerate(article_refs[:3], 1):
                    title = article.get('title', 'No title')
                    print(f"        {idx}. {title}")
                if len(article_refs) > 3:
                    print(f"        ... and {len(article_refs) - 3} more")
        
        print()


def show_stats():
    """Show clustering statistics."""
    
    print("=" * 80)
    print("CLUSTERING STATISTICS")
    print("=" * 80)
    
    clusters_col = get_collection('story_clusters')
    
    # Total clusters
    total = clusters_col.count_documents({})
    print(f"\nTotal clusters: {total}")
    
    # By timeframe
    print("\nBy Timeframe:")
    for window in [3, 7, 14, 30]:
        count = clusters_col.count_documents({'window_days': window})
        print(f"  {window}d: {count} clusters")
    
    # By entity
    print("\nTop 10 Entities:")
    pipeline = [
        {'$group': {
            '_id': '$entity_name',
            'count': {'$sum': 1}
        }},
        {'$sort': {'count': -1}},
        {'$limit': 10}
    ]
    
    for result in clusters_col.aggregate(pipeline):
        entity = result['_id']
        count = result['count']
        print(f"  {entity}: {count} clusters")
    
    # By tag
    print("\nTop 10 Tags:")
    pipeline = [
        {'$group': {
            '_id': '$tag',
            'count': {'$sum': 1}
        }},
        {'$sort': {'count': -1}},
        {'$limit': 10}
    ]
    
    for result in clusters_col.aggregate(pipeline):
        tag = result['_id']
        count = result['count']
        print(f"  {tag}: {count} clusters")
    
    # Clusters with stance
    with_stance = clusters_col.count_documents({'stance': {'$exists': True}})
    print(f"\nClusters with stance: {with_stance}/{total} ({with_stance/max(total,1)*100:.1f}%)")
    
    # Clusters with summary
    with_summary = clusters_col.count_documents({'summary': {'$exists': True}})
    print(f"Clusters with summary: {with_summary}/{total} ({with_summary/max(total,1)*100:.1f}%)")


def compare_timeframes(entity_id):
    """Compare entity across timeframes."""
    
    print("=" * 80)
    print(f"TIMEFRAME COMPARISON")
    print("=" * 80)
    
    clusters_col = get_collection('story_clusters')
    
    # Get clusters for this entity
    clusters = list(clusters_col.find({'entity_id': entity_id}).sort('window_days', 1))
    
    if not clusters:
        print(f"\n❌ No clusters for entity: {entity_id}")
        return
    
    entity_name = clusters[0].get('entity_name', 'Unknown')
    print(f"\nEntity: {entity_name}")
    
    # Group by window
    by_window = {}
    for cluster in clusters:
        window = cluster.get('window_days')
        if window not in by_window:
            by_window[window] = []
        by_window[window].append(cluster)
    
    # Compare windows
    for window in sorted(by_window.keys()):
        window_clusters = by_window[window]
        
        print(f"\n{window}d window: {len(window_clusters)} clusters")
        print("-" * 40)
        
        # Group by tag
        by_tag = {}
        for cluster in window_clusters:
            tag = cluster.get('tag')
            if tag not in by_tag:
                by_tag[tag] = []
            by_tag[tag].append(cluster)
        
        for tag, tag_clusters in sorted(by_tag.items()):
            total_articles = sum(c.get('cluster_metadata', {}).get('size', 0) for c in tag_clusters)
            print(f"  {tag}: {len(tag_clusters)} cluster(s), {total_articles} articles")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--entity', help='Filter by entity ID')
    parser.add_argument('--window', type=int, choices=[3, 7, 14, 30], help='Filter by window')
    parser.add_argument('--tag', help='Filter by tag')
    parser.add_argument('--articles', action='store_true', help='Show articles')
    parser.add_argument('--stats', action='store_true', help='Show statistics')
    parser.add_argument('--compare', help='Compare entity across timeframes')
    
    args = parser.parse_args()
    
    if args.stats:
        show_stats()
    elif args.compare:
        compare_timeframes(args.compare)
    else:
        view_clusters(
            entity_id=args.entity,
            window_days=args.window,
            tag=args.tag,
            show_articles=args.articles
        )