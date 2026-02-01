#!/usr/bin/env python3
"""
Cluster Summarizer - BART Model (Better for Summarization)
=========================================================
Uses facebook/bart-large-cnn - specifically designed for summarization
"""

import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict
from transformers import pipeline


class ClusterSummarizer:
    """Generate cluster summaries using BART summarization model."""

    def __init__(self, test_mode: bool = False):
        self.test_mode = test_mode

        # BART is specifically designed for summarization
        self.model_id = "facebook/bart-large-cnn"

        print(f"Loading model: {self.model_id}...")
        self.summarizer = pipeline(
            "summarization",
            model=self.model_id,
            device=-1,  # CPU
        )
        print("Model loaded!")

        if self.test_mode:
            self.test_dir = Path("test_outputs/summaries")
            self.test_dir.mkdir(parents=True, exist_ok=True)
            print(f"📁 Test mode: {self.test_dir}")

    def summarize_cluster(self, cluster: Dict, articles: List[Dict]) -> Dict:
        """Generate summary for cluster."""
        if not articles:
            return self._empty_summary()

        cluster_id = cluster.get("cluster_id", "unknown")

        if self.test_mode:
            print(f"\n{'='*80}\nSUMMARIZING: {cluster_id}\n{'='*80}")

        # Combine article content
        combined_text = self._combine_articles(cluster, articles[:10])
        
        # Generate summary using BART
        summary_text = self._generate_summary(combined_text)
        
        # Extract structured information
        summary = self._structure_summary(cluster, articles, summary_text)

        summary["article_count"] = len(articles)
        summary["generated_at"] = datetime.utcnow()

        if self.test_mode:
            self._save_test_output(cluster_id, combined_text, summary_text, summary)
            print(f"✅ Summary: {summary['summary'][:90]}...")

        return summary

    def _combine_articles(self, cluster: Dict, articles: List[Dict]) -> str:
        """Combine articles into single text for summarization."""
        texts = []
        for article in articles:
            title = article.get('title', '')
            body = article.get('body', '')[:300]  # Limit body length
            if title:
                texts.append(f"{title}. {body}")
        
        combined = " ".join(texts)
        return combined[:1024]  # BART max input length

    def _generate_summary(self, text: str) -> str:
        """Generate summary using BART."""
        if self.test_mode:
            print("🤖 Running BART summarization...")

        try:
            result = self.summarizer(
                text,
                max_length=130,
                min_length=30,
                do_sample=False,
                truncation=True
            )
            return result[0]['summary_text']
        except Exception as e:
            if self.test_mode:
                print(f"⚠️  Model error: {e}")
            return text[:200]

    def _structure_summary(self, cluster: Dict, articles: List[Dict], summary_text: str) -> Dict:
        """Structure summary into required format."""
        # Extract main points from titles
        main_points = []
        for article in articles[:5]:
            title = article.get('title', '')
            if title and title not in main_points:
                main_points.append(title)

        # Extract entities (simple approach: capitalized words)
        entity_name = cluster.get('entity_name', '')
        key_entities = [entity_name] if entity_name else []
        
        # Extract timeframe from dates
        timeframe = self._extract_timeframe(articles)

        return {
            "summary": summary_text,
            "main_points": main_points[:5],
            "key_entities": key_entities,
            "timeframe": timeframe,
        }

    def _extract_timeframe(self, articles: List[Dict]) -> str:
        """Extract timeframe from article dates."""
        dates = []
        for article in articles:
            pub_date = article.get('published_at_utc')
            if pub_date and hasattr(pub_date, 'strftime'):
                dates.append(pub_date)
        
        if dates:
            min_date = min(dates)
            max_date = max(dates)
            if min_date == max_date:
                return min_date.strftime("%b %d, %Y")
            else:
                return f"{min_date.strftime('%b %d')} - {max_date.strftime('%b %d, %Y')}"
        
        return "Unknown"

    def _empty_summary(self) -> Dict:
        """Return empty summary."""
        return {
            "summary": "Empty cluster",
            "main_points": [],
            "key_entities": [],
            "timeframe": "",
            "article_count": 0,
            "generated_at": datetime.utcnow(),
        }

    def _save_test_output(self, cluster_id: str, input_text: str, summary_text: str, parsed: Dict):
        """Save test outputs."""
        if not self.test_mode:
            return

        cluster_dir = self.test_dir / cluster_id
        cluster_dir.mkdir(parents=True, exist_ok=True)

        (cluster_dir / "input_text.txt").write_text(input_text, encoding="utf-8")
        (cluster_dir / "summary_text.txt").write_text(summary_text, encoding="utf-8")
        (cluster_dir / "parsed_summary.json").write_text(
            json.dumps(parsed, indent=2, default=str),
            encoding="utf-8"
        )


def test_summarizer():
    """Test function."""
    print("="*80)
    print("TESTING BART SUMMARIZER")
    print("="*80)

    test_cluster = {
        "cluster_id": "TEST_ai_chip_20260128",
        "entity_name": "Microsoft",
        "tag": "ai_innovation",
    }

    test_articles = [
        {
            "title": "Microsoft unveils Maia 200 AI chip",
            "body": "Microsoft announced the Maia 200, a custom AI chip designed to accelerate AI workloads in its Azure cloud infrastructure.",
        },
        {
            "title": "New chip promises 3x performance boost",
            "body": "The Maia 200 is designed to deliver three times the performance of previous generation chips for AI inference tasks.",
        },
    ]

    summarizer = ClusterSummarizer(test_mode=True)
    summary = summarizer.summarize_cluster(test_cluster, test_articles)

    print("\nFINAL RESULT:")
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    test_summarizer()