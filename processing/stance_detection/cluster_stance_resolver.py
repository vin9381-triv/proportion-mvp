#!/usr/bin/env python3
"""
Cluster Stance Resolver - Local Only
===================================
Rules + BERT MNLI fallback (no API calls)
"""

import re
import json
from typing import List, Dict
from transformers import pipeline


# Rule-based lexicons
CRITICAL_PATTERNS = [
    r"\bwarn(s|ed|ing)?\b", r"\bconcern(s|ed|ing)?\b", r"\brisk(s)?\b",
    r"\bregulatory\b", r"\bscrutiny\b", r"\bheadwind(s)?\b",
    r"\buncertain(ty)?\b", r"\bthreat(s)?\b", r"\bslowdown\b",
    r"\bimpact margins?\b",
]

SUPPORTIVE_PATTERNS = [
    r"\bgrowth\b", r"\bbenefit(s|ed)?\b", r"\btailwind(s)?\b",
    r"\badoption\b", r"\bstrong demand\b", r"\bresilient\b",
    r"\bboost(s|ed)?\b", r"\bexpected to drive\b",
]


class ClusterStanceResolver:
    """Resolve cluster stance using rules + local MNLI model."""

    def __init__(self, use_mnli: bool = True):
        self.use_mnli = use_mnli

        if self.use_mnli:
            print("Loading MNLI model: M-FAC/bert-mini-finetuned-mnli...")
            self.classifier = pipeline(
                "zero-shot-classification",
                model="M-FAC/bert-mini-finetuned-mnli",
                device=-1  # CPU
            )
            print("MNLI model loaded!")

    def resolve(self, summary: Dict, titles: List[str]) -> Dict:
        """Resolve stance for cluster."""
        rule_result = self._rule_based_resolution(titles)

        # If rules confident, use them
        if rule_result["confidence"] >= 0.65 or not self.use_mnli:
            return rule_result

        # Otherwise use MNLI fallback
        return self._mnli_resolution(summary, titles)

    def _rule_based_resolution(self, titles: List[str]) -> Dict:
        """Rule-based stance using keyword patterns."""
        text = " ".join(titles).lower()

        critical_hits = sum(len(re.findall(p, text)) for p in CRITICAL_PATTERNS)
        supportive_hits = sum(len(re.findall(p, text)) for p in SUPPORTIVE_PATTERNS)
        total_hits = critical_hits + supportive_hits

        if total_hits == 0:
            return self._format_output("neutral", 0.60, "rules")

        critical_ratio = critical_hits / total_hits
        supportive_ratio = supportive_hits / total_hits

        if critical_ratio >= 0.60:
            return self._format_output("critical", round(critical_ratio, 2), "rules")

        if supportive_ratio >= 0.60:
            return self._format_output("supportive", round(supportive_ratio, 2), "rules")

        return self._format_output("mixed", 0.55, "rules")

    def _mnli_resolution(self, summary: Dict, titles: List[str]) -> Dict:
        """Local MNLI zero-shot classification."""
        text = summary.get("summary", "") + " " + " ".join(titles[:5])

        labels = [
            "supportive toward the subject",
            "critical toward the subject",
            "neutral reporting",
        ]

        result = self.classifier(
            text,
            candidate_labels=labels,
            hypothesis_template="This text is {}.",
            truncation=True,
            max_length=256,
        )

        top_label = result["labels"][0]
        score = result["scores"][0]

        if "supportive" in top_label:
            return self._format_output("supportive", score, "mnli")
        if "critical" in top_label:
            return self._format_output("critical", score, "mnli")

        return self._format_output("neutral", score, "mnli")

    def _format_output(self, label: str, confidence: float, method: str) -> Dict:
        """Format stance output."""
        return {
            "label": label,
            "confidence": round(min(confidence, 0.95), 2),
            "method": method,
            "text": self._stance_text(label),
        }

    def _stance_text(self, label: str) -> str:
        """Human-readable stance text."""
        templates = {
            "supportive": "Coverage is broadly supportive, highlighting potential benefits.",
            "critical": "Coverage is largely critical, emphasizing risks and concerns.",
            "neutral": "Coverage is primarily informational without strong framing.",
            "mixed": "Narrative is mixed, with both risks and benefits highlighted.",
        }
        return templates[label]


def test_resolver():
    """Test function - will be removed after validation."""
    print("="*80)
    print("TESTING LOCAL STANCE RESOLVER")
    print("="*80)

    resolver = ClusterStanceResolver(use_mnli=True)

    summary = {"summary": "Regulators reviewing new AI compliance rules."}
    titles = [
        "Regulators warn new AI rules could hurt margins",
        "Tech firms raise concerns over compliance costs",
    ]

    stance = resolver.resolve(summary, titles)

    print("\nRESULT:")
    print(json.dumps(stance, indent=2))


if __name__ == "__main__":
    test_resolver()