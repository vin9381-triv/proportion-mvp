#!/usr/bin/env python3
"""
Story Summarizer (Local, BART)
=============================

Generates a concise, factual summary for a STORY cluster.
Optimized for downstream stance detection.
"""

from datetime import datetime
from typing import List, Dict
from transformers import pipeline


# ============================================================
# CONFIG
# ============================================================

MAX_ARTICLES = 6          # limit context
MAX_BODY_CHARS = 400      # per article
MAX_INPUT_CHARS = 1200    # BART-safe
SUMMARY_MAX_LEN = 120
SUMMARY_MIN_LEN = 40


# ============================================================
# SUMMARIZER
# ============================================================

class ClusterSummarizer:
    """Story-level summarizer using BART."""

    def __init__(self, test_mode: bool = False):
        self.test_mode = test_mode
        self.model_id = "facebook/bart-large-cnn"

        print(f"🧠 Loading summarization model: {self.model_id}")
        self.summarizer = pipeline(
            "summarization",
            model=self.model_id,
            device=-1,  # CPU
        )
        print("✅ Summarizer ready")

    # --------------------------------------------------------

    def summarize_cluster(self, cluster: Dict, articles: List[Dict]) -> Dict:
        """
        Generate a structured summary for a STORY.

        Returns:
        {
          summary: str
          main_points: list[str]
          timeframe: str
          article_count: int
          generated_at: datetime
        }
        """
        if not articles:
            return self._empty_summary()

        # Sort articles chronologically (story progression)
        articles = sorted(
            articles,
            key=lambda a: a.get("published_at_utc") or datetime.utcnow()
        )

        # Build input text
        combined_text = self._build_input_text(articles)

        # Run model
        summary_text = self._generate_summary(combined_text)

        # Structure output
        return {
            "summary": summary_text,
            "main_points": self._extract_main_points(articles),
            "timeframe": self._extract_timeframe(articles),
            "article_count": len(articles),
            "generated_at": datetime.utcnow(),
        }

    # ========================================================
    # INTERNALS
    # ========================================================

    def _build_input_text(self, articles: List[Dict]) -> str:
        """
        Build clean, factual input for BART.
        Focus: what happened, not opinion.
        """
        chunks = []

        for article in articles[:MAX_ARTICLES]:
            title = article.get("title", "").strip()
            body = article.get("body", "").strip()

            if body:
                body = body[:MAX_BODY_CHARS]

            if title:
                chunks.append(f"{title}. {body}")
            elif body:
                chunks.append(body)

        text = " ".join(chunks)
        return text[:MAX_INPUT_CHARS]

    # --------------------------------------------------------

    def _generate_summary(self, text: str) -> str:
        """Run BART summarization."""
        try:
            result = self.summarizer(
                text,
                max_length=SUMMARY_MAX_LEN,
                min_length=SUMMARY_MIN_LEN,
                do_sample=False,
                truncation=True,
            )
            return result[0]["summary_text"]
        except Exception as e:
            # Fail gracefully — never break stance pipeline
            if self.test_mode:
                print(f"⚠️ Summarizer error: {e}")
            return text[:200]

    # --------------------------------------------------------

    def _extract_main_points(self, articles: List[Dict]) -> List[str]:
        """
        Use article titles as grounded main points.
        Keeps stance resolver anchored to facts.
        """
        points = []
        for article in articles[:5]:
            title = article.get("title")
            if title and title not in points:
                points.append(title)
        return points

    # --------------------------------------------------------

    def _extract_timeframe(self, articles: List[Dict]) -> str:
        dates = [
            a["published_at_utc"]
            for a in articles
            if a.get("published_at_utc")
        ]

        if not dates:
            return "Unknown"

        start = min(dates)
        end = max(dates)

        if start.date() == end.date():
            return start.strftime("%b %d, %Y")

        return f"{start.strftime('%b %d')} – {end.strftime('%b %d, %Y')}"

    # --------------------------------------------------------

    def _empty_summary(self) -> Dict:
        return {
            "summary": "No content available.",
            "main_points": [],
            "timeframe": "",
            "article_count": 0,
            "generated_at": datetime.utcnow(),
        }