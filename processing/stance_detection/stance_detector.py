#!/usr/bin/env python3
"""
Story Stance Detection Pipeline
==============================

Runs stance detection at STORY level.

Flow:
1. Fetch eligible stories
2. Fetch articles per story
3. Generate story summary
4. Resolve story stance
5. Persist to MongoDB
"""

import argparse
from datetime import datetime, timedelta
from typing import List, Dict

from processing.common.mongo_client import get_collection
from processing.stance_detection.cluster_summarizer import ClusterSummarizer
from processing.stance_detection.cluster_stance_resolver import ClusterStanceResolver
from processing.stance_detection.stance_mongo_writer import StanceMongoDBWriter


# ============================================================
# DB HELPERS
# ============================================================

def get_stories_for_stance_detection(
    min_size: int,
    max_age_days: int,
) -> List[Dict]:
    """
    Fetch eligible story documents.
    """
    clusters_col = get_collection("story_clusters")

    cutoff = datetime.utcnow() - timedelta(days=max_age_days)

    query = {
        "story_metadata.size": {"$gte": min_size},
        "created_at": {"$gte": cutoff},
    }

    return list(clusters_col.find(query))


def get_articles_for_story(story: Dict) -> List[Dict]:
    """
    Fetch article documents already embedded in story.
    """
    return story.get("articles", [])


# ============================================================
# PIPELINE
# ============================================================

def run_stance_detection(
    min_story_size: int = 3,
    max_age_days: int = 7,
    max_stories: int | None = None,
    test_mode: bool = False,
):
    print("=" * 80)
    print("STORY-LEVEL STANCE DETECTION")
    print("=" * 80)
    print(f"Started: {datetime.utcnow().isoformat()} UTC\n")

    summarizer = ClusterSummarizer(test_mode=test_mode)
    resolver = ClusterStanceResolver()
    writer = StanceMongoDBWriter(test_mode=test_mode)

    print("Fetching eligible stories...")
    stories = get_stories_for_stance_detection(
        min_size=min_story_size,
        max_age_days=max_age_days,
    )

    if max_stories:
        stories = stories[:max_stories]

    if not stories:
        print("❌ No eligible stories found.")
        return

    print(f"Found {len(stories)} stories\n")

    processed = 0

    for idx, story in enumerate(stories, 1):
        story_id = story.get("story_id")
        entity = story.get("entity_name")
        window = story.get("window_days")

        print("-" * 80)
        print(f"[{idx}/{len(stories)}] {entity} | {window}d")
        print(f"Story ID: {story_id}")

        articles = get_articles_for_story(story)

        if not articles:
            print("⚠️ No articles attached, skipping")
            continue

        print(f"Articles: {len(articles)}")

        try:
            # --------------------------------------------------
            # 1. Summarize story
            # --------------------------------------------------
            print("→ Generating story summary...")
            summary = summarizer.summarize_cluster(story, articles)

            # --------------------------------------------------
            # 2. Resolve stance
            # --------------------------------------------------
            print("→ Resolving story stance...")
            titles = [a.get("title", "") for a in articles]
            stance = resolver.resolve(summary=summary, titles=titles)

            # --------------------------------------------------
            # 3. Persist results
            # --------------------------------------------------
            print("→ Writing results to MongoDB...")
            writer.write_summary_to_story(story_id, summary)
            writer.write_story_stance(story_id, stance)

            print(
                f"✅ STANCE: {stance['label'].upper()} "
                f"(conf={stance['confidence']}, method={stance['method']})"
            )
            print(f"→ {stance['text']}")

            processed += 1

        except Exception as e:
            print(f"❌ Error processing story {story_id}: {e}")
            continue

    print("\n" + "=" * 80)
    print("PIPELINE COMPLETE")
    print("=" * 80)
    print(f"Stories processed: {processed}")
    print(f"Finished: {datetime.utcnow().isoformat()} UTC")


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Run story-level stance detection",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--min-size", type=int, default=3, help="Min story size")
    parser.add_argument("--max-age", type=int, default=7, help="Max age (days)")
    parser.add_argument("--max-stories", type=int, default=None, help="Limit stories")
    parser.add_argument("--test-mode", action="store_true", help="Enable test mode")

    args = parser.parse_args()

    run_stance_detection(
        min_story_size=args.min_size,
        max_age_days=args.max_age,
        max_stories=args.max_stories,
        test_mode=args.test_mode,
    )


if __name__ == "__main__":
    main()