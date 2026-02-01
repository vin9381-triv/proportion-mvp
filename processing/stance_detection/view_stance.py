#!/usr/bin/env python3
"""
View Cluster Stance Results
==========================
"""

import argparse
from typing import List, Dict
from processing.common.mongo_client import get_collection


def get_recent_clusters_with_stance(limit: int = 10) -> List[Dict]:
    """Fetch recent clusters with stance."""
    clusters_col = get_collection("story_clusters")
    return list(
        clusters_col.find({"stance": {"$exists": True}})
        .sort("stance.generated_at", -1)
        .limit(limit)
    )


def get_cluster_by_id(cluster_id: str) -> Dict:
    """Fetch cluster by ID."""
    clusters_col = get_collection("story_clusters")
    return clusters_col.find_one({"cluster_id": cluster_id})


def print_cluster(cluster: Dict):
    """Pretty-print cluster details."""
    print("\n" + "=" * 90)
    print(f"CLUSTER ID : {cluster.get('cluster_id')}")
    print("=" * 90)
    print(f"Entity     : {cluster.get('entity_name')}")
    print(f"Tag        : {cluster.get('tag')}")
    print(f"Size       : {cluster.get('cluster_metadata', {}).get('size', 'N/A')}")

    summary = cluster.get("summary")
    stance = cluster.get("stance")

    if summary:
        print("\nSUMMARY:")
        print(f"  {summary.get('text', 'N/A')}")

        if summary.get("main_points"):
            print("\nKEY POINTS:")
            for point in summary["main_points"]:
                print(f"  • {point}")

        print(f"\nTimeframe  : {summary.get('timeframe', 'N/A')}")
        print(f"Generated  : {summary.get('generated_at', 'N/A')}")
    else:
        print("\nSUMMARY: ❌ Not available")

    if stance:
        print("\nSTANCE:")
        print(
            f"  {stance.get('label', '').upper()} "
            f"(confidence: {stance.get('confidence', 0):.2f}, "
            f"method: {stance.get('method')})"
        )
        print(f"  → {stance.get('text', '')}")
        print(f"Generated  : {stance.get('generated_at', 'N/A')}")
    else:
        print("\nSTANCE: ❌ Not available")

    print("=" * 90)


def view_recent(limit: int):
    """View recent cluster stances."""
    clusters = get_recent_clusters_with_stance(limit)

    if not clusters:
        print("❌ No clusters with stance found.")
        return

    print(f"\nShowing {len(clusters)} most recent clusters:\n")
    for cluster in clusters:
        print_cluster(cluster)


def view_cluster(cluster_id: str):
    """View specific cluster."""
    cluster = get_cluster_by_id(cluster_id)

    if not cluster:
        print(f"❌ Cluster not found: {cluster_id}")
        return

    print_cluster(cluster)


def view_statistics():
    """View stance statistics."""
    clusters_col = get_collection("story_clusters")

    print("\n" + "=" * 90)
    print("CLUSTER STANCE STATISTICS")
    print("=" * 90)

    pipeline = [
        {"$match": {"stance": {"$exists": True}}},
        {"$group": {
            "_id": "$stance.label",
            "count": {"$sum": 1},
            "avg_confidence": {"$avg": "$stance.confidence"}
        }},
        {"$sort": {"count": -1}}
    ]

    results = list(clusters_col.aggregate(pipeline))

    if not results:
        print("❌ No clusters with stance found.")
        return

    total = sum(r["count"] for r in results)

    print(f"\nTotal clusters with stance: {total}\n")
    print("Stance Distribution:")
    print("-" * 90)

    for result in results:
        label = result["_id"]
        count = result["count"]
        avg_conf = result["avg_confidence"]
        pct = (count / total) * 100

        print(f"  {label:<12} {count:>4} clusters ({pct:>5.1f}%)  Avg confidence: {avg_conf:.2f}")

    print("=" * 90)


def main():
    parser = argparse.ArgumentParser(
        description="View cluster-level stance results",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--recent", action="store_true", help="Show recent clusters")
    parser.add_argument("--limit", type=int, default=10, help="Limit for --recent")
    parser.add_argument("--cluster-id", type=str, help="View specific cluster")
    parser.add_argument("--stats", action="store_true", help="Show statistics")

    args = parser.parse_args()

    if args.stats:
        view_statistics()
    elif args.cluster_id:
        view_cluster(args.cluster_id)
    else:
        view_recent(args.limit)


if __name__ == "__main__":
    main()