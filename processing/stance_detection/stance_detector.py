#!/usr/bin/env python3
"""
Cluster Stance Detection Pipeline - Production
=============================================
Local models only, no API calls
"""

import argparse
from datetime import datetime

from processing.clustering.cluster_mongodb_writer import (
    get_clusters_for_stance_detection,
    get_articles_for_cluster,
)

from processing.stance_detection.cluster_summarizer import ClusterSummarizer
from processing.stance_detection.cluster_stance_resolver import ClusterStanceResolver
from processing.stance_detection.stance_mongo_writer import StanceMongoDBWriter


def run_stance_detection(
    min_cluster_size: int = 3,
    max_age_days: int = 7,
    max_clusters: int = None,
    test_mode: bool = False,
):
    """Run cluster-level stance detection."""

    print("=" * 80)
    print("CLUSTER-LEVEL STANCE DETECTION (LOCAL)")
    print("=" * 80)
    print(f"Started: {datetime.utcnow().isoformat()} UTC\n")

    # Initialize components
    summarizer = ClusterSummarizer(test_mode=test_mode)
    resolver = ClusterStanceResolver()
    writer = StanceMongoDBWriter(test_mode=test_mode)

    # Fetch clusters
    print("Fetching eligible clusters...")
    clusters = get_clusters_for_stance_detection(
        min_size=min_cluster_size,
        max_age_days=max_age_days,
    )

    if max_clusters:
        clusters = clusters[:max_clusters]

    if not clusters:
        print("❌ No clusters found.")
        return

    print(f"Found {len(clusters)} clusters\n")

    # Process clusters
    processed = 0

    for idx, cluster in enumerate(clusters, 1):
        cluster_id = cluster.get("cluster_id")
        entity_name = cluster.get("entity_name")
        tag = cluster.get("tag")

        print("-" * 80)
        print(f"[{idx}/{len(clusters)}] Cluster: {cluster_id}")
        print(f"Entity: {entity_name} | Tag: {tag}")

        # Fetch articles
        articles = get_articles_for_cluster(cluster_id)

        if not articles:
            print("⚠️  No articles found, skipping")
            continue

        print(f"Articles: {len(articles)}")

        try:
            # Step 1: Generate summary
            print("→ Generating cluster summary...")
            summary = summarizer.summarize_cluster(cluster, articles)

            # Step 2: Resolve stance
            print("→ Resolving cluster stance...")
            titles = [a.get("title", "") for a in articles]
            stance = resolver.resolve(summary=summary, titles=titles)

            # Step 3: Write to MongoDB
            print("→ Writing to MongoDB...")
            writer.write_summary_to_cluster(cluster_id, summary)
            writer.write_cluster_stance(cluster_id, stance)

            print(
                f"✅ STANCE: {stance['label'].upper()} "
                f"(conf={stance['confidence']}, method={stance['method']})"
            )
            print(f"→ {stance['text']}")

            processed += 1

        except Exception as e:
            print(f"❌ Error processing cluster: {e}")
            continue

    # Final report
    print("\n" + "=" * 80)
    print("PIPELINE COMPLETE")
    print("=" * 80)
    print(f"Clusters processed: {processed}")
    print(f"Finished: {datetime.utcnow().isoformat()} UTC")


def main():
    parser = argparse.ArgumentParser(
        description="Run cluster-level stance detection (local models)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--min-size", type=int, default=3, help="Min cluster size")
    parser.add_argument("--max-age", type=int, default=7, help="Max age (days)")
    parser.add_argument("--max-clusters", type=int, default=None, help="Limit clusters")
    parser.add_argument("--test-mode", action="store_true", help="Enable test mode")

    args = parser.parse_args()

    run_stance_detection(
        min_cluster_size=args.min_size,
        max_age_days=args.max_age,
        max_clusters=args.max_clusters,
        test_mode=args.test_mode,
    )


if __name__ == "__main__":
    main()