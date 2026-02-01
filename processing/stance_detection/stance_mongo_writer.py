#!/usr/bin/env python3
"""
Stance MongoDB Writer - Production Ready
=======================================
"""

from datetime import datetime
from typing import Dict
from processing.common.mongo_client import get_collection


class StanceMongoDBWriter:
    """Persist cluster summaries and stances to MongoDB."""

    def __init__(self, test_mode: bool = False):
        self.test_mode = test_mode
        self.clusters_col = get_collection("story_clusters")

    def write_summary_to_cluster(self, cluster_id: str, summary: Dict):
        """Write cluster summary to story_clusters."""
        summary_doc = {
            "text": summary.get("summary", ""),
            "main_points": summary.get("main_points", []),
            "key_entities": summary.get("key_entities", []),
            "timeframe": summary.get("timeframe", ""),
            "article_count": summary.get("article_count", 0),
            "generated_at": summary.get("generated_at", datetime.utcnow()),
        }

        result = self.clusters_col.update_one(
            {"cluster_id": cluster_id},
            {"$set": {"summary": summary_doc}},
            upsert=False,
        )

        if self.test_mode:
            print(
                f"[Mongo] Summary written | cluster={cluster_id} "
                f"| matched={result.matched_count} modified={result.modified_count}"
            )

    def write_cluster_stance(self, cluster_id: str, stance: Dict):
        """Write cluster stance to story_clusters."""
        stance_doc = {
            "label": stance.get("label", "neutral"),
            "text": stance.get("text", ""),
            "confidence": float(stance.get("confidence", 0.5)),
            "method": stance.get("method", "rules"),
            "generated_at": datetime.utcnow(),
        }

        result = self.clusters_col.update_one(
            {"cluster_id": cluster_id},
            {"$set": {"stance": stance_doc}},
            upsert=False,
        )

        if self.test_mode:
            print(
                f"[Mongo] Stance written | cluster={cluster_id} "
                f"| label={stance_doc['label']} "
                f"| conf={stance_doc['confidence']}"
            )

    def get_recent_cluster_stances(self, limit: int = 10):
        """Fetch recently updated cluster stances."""
        return list(
            self.clusters_col.find(
                {"stance": {"$exists": True}}
            )
            .sort("stance.generated_at", -1)
            .limit(limit)
        )


def test_writer():
    """Test function - will be removed after validation."""
    print("="*80)
    print("TESTING MONGO WRITER")
    print("="*80)

    writer = StanceMongoDBWriter(test_mode=True)

    test_cluster_id = "TEST_cluster_001"

    test_summary = {
        "summary": "Test summary",
        "main_points": ["Point 1", "Point 2"],
        "key_entities": ["Entity1"],
        "timeframe": "Jan 2026",
        "article_count": 3,
        "generated_at": datetime.utcnow(),
    }

    test_stance = {
        "label": "neutral",
        "text": "Test stance",
        "confidence": 0.70,
        "method": "rules",
    }

    print("\nWriting test summary...")
    writer.write_summary_to_cluster(test_cluster_id, test_summary)

    print("\nWriting test stance...")
    writer.write_cluster_stance(test_cluster_id, test_stance)

    print("\n✅ Test complete")


if __name__ == "__main__":
    test_writer()