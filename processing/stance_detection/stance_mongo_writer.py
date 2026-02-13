#!/usr/bin/env python3
"""
Story Stance MongoDB Writer
===========================

Persists story summaries and story-level stance
into the story_clusters collection.

Assumes:
- story documents already exist
- identified by story_id
"""

from datetime import datetime
from typing import Dict
from processing.common.mongo_client import get_collection


class StanceMongoDBWriter:
    """Write story summaries and stances to MongoDB."""

    def __init__(self, test_mode: bool = False):
        self.test_mode = test_mode
        self.col = get_collection("story_clusters")

    # ========================================================
    # SUMMARY
    # ========================================================

    def write_summary_to_story(self, story_id: str, summary: Dict):
        """
        Attach generated summary to a story document.
        """
        summary_doc = {
            "text": summary.get("summary", ""),
            "main_points": summary.get("main_points", []),
            "timeframe": summary.get("timeframe", ""),
            "article_count": summary.get("article_count", 0),
            "generated_at": summary.get("generated_at", datetime.utcnow()),
        }

        result = self.col.update_one(
            {"story_id": story_id},
            {"$set": {"summary": summary_doc}},
            upsert=False,
        )

        if self.test_mode:
            print(
                f"[Mongo] Summary written | story_id={story_id} "
                f"| matched={result.matched_count} "
                f"| modified={result.modified_count}"
            )

    # ========================================================
    # STANCE
    # ========================================================

    def write_story_stance(self, story_id: str, stance: Dict):
        """
        Attach resolved stance to a story document.
        """
        stance_doc = {
            "label": stance.get("label", "neutral"),
            "confidence": float(stance.get("confidence", 0.5)),
            "method": stance.get("method", "rules"),
            "text": stance.get("text", ""),
            "generated_at": datetime.utcnow(),
        }

        result = self.col.update_one(
            {"story_id": story_id},
            {"$set": {"stance": stance_doc}},
            upsert=False,
        )

        if self.test_mode:
            print(
                f"[Mongo] Stance written | story_id={story_id} "
                f"| label={stance_doc['label']} "
                f"| conf={stance_doc['confidence']}"
            )

    # ========================================================
    # READ HELPERS (OPTIONAL)
    # ========================================================

    def get_recent_story_stances(self, limit: int = 10):
        """
        Fetch recently updated story stances.
        Useful for dashboards / debugging.
        """
        return list(
            self.col.find(
                {"stance": {"$exists": True}}
            )
            .sort("stance.generated_at", -1)
            .limit(limit)
        )


# ============================================================
# TEST
# ============================================================

def test_writer():
    print("=" * 80)
    print("TESTING STORY STANCE MONGO WRITER")
    print("=" * 80)

    writer = StanceMongoDBWriter(test_mode=True)

    test_story_id = "TEST_story_001"

    test_summary = {
        "summary": "Test story summary text.",
        "main_points": ["Point A", "Point B"],
        "timeframe": "Feb 2026",
        "article_count": 4,
        "generated_at": datetime.utcnow(),
    }

    test_stance = {
        "label": "critical",
        "confidence": 0.72,
        "method": "rules",
        "text": "Coverage highlights risks and regulatory concerns.",
    }

    print("\nWriting test summary...")
    writer.write_summary_to_story(test_story_id, test_summary)

    print("\nWriting test stance...")
    writer.write_story_stance(test_story_id, test_stance)

    print("\n✅ Test complete")


if __name__ == "__main__":
    test_writer()