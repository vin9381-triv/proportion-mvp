#!/usr/bin/env python3
"""
Story Stance Resolver (Local)
============================

Resolves stance for a STORY (not a tag cluster).

Inputs:
- story summary (text)
- article titles

Approach:
1. Rule-based signals (high precision)
2. MNLI fallback (local, conservative)

Output:
- stance label
- confidence
- explanation text
"""

import re
from typing import List, Dict, Optional
from transformers import pipeline


# ============================================================
# LEXICONS (TUNED FOR STORY-LEVEL SIGNAL)
# ============================================================

CRITICAL_PATTERNS = [
    r"\bwarn(s|ed|ing)?\b",
    r"\bconcern(s|ed|ing)?\b",
    r"\brisk(s)?\b",
    r"\bregulator(y|ies)?\b",
    r"\bscrutiny\b",
    r"\bheadwind(s)?\b",
    r"\buncertain(ty)?\b",
    r"\bthreat(s)?\b",
    r"\bslowdown\b",
    r"\bprobe\b",
    r"\binvestigation\b",
    r"\bantitrust\b",
    r"\bfine(s|d)?\b",
    r"\bimpact margins?\b",
]

SUPPORTIVE_PATTERNS = [
    r"\bgrowth\b",
    r"\bstrong demand\b",
    r"\bresilient\b",
    r"\bboost(s|ed)?\b",
    r"\bbenefit(s|ed)?\b",
    r"\btailwind(s)?\b",
    r"\badoption\b",
    r"\bexpansion\b",
    r"\bprofit(s|able)?\b",
    r"\brecord\b",
    r"\bbeat expectations\b",
]


# ============================================================
# RESOLVER
# ============================================================

class ClusterStanceResolver:
    """
    Resolve stance for a STORY using:
    - rules (high precision)
    - MNLI fallback (conservative)
    """

    def __init__(self, use_mnli: bool = True):
        self.use_mnli = use_mnli
        self.classifier: Optional[object] = None

        if self.use_mnli:
            print("🔎 Loading local MNLI model (bert-mini-mnli)...")
            self.classifier = pipeline(
                "zero-shot-classification",
                model="M-FAC/bert-mini-finetuned-mnli",
                device=-1,  # CPU
            )
            print("✅ MNLI model loaded")

    # --------------------------------------------------------

    def resolve(self, summary: Dict, titles: List[str]) -> Dict:
        """
        Resolve story stance.

        Returns:
        {
          label: supportive | critical | neutral | mixed
          confidence: float
          method: rules | mnli
          text: human explanation
        }
        """
        rule_result = self._rule_based_resolution(summary, titles)

        # High-confidence rule → accept
        if rule_result["confidence"] >= 0.70 or not self.use_mnli:
            return rule_result

        # Otherwise fallback to MNLI
        return self._mnli_resolution(summary, titles)

    # ========================================================
    # RULE-BASED
    # ========================================================

    def _rule_based_resolution(self, summary: Dict, titles: List[str]) -> Dict:
        text = (
            (summary.get("summary", "") + " " + " ".join(titles))
            .lower()
        )

        critical_hits = sum(len(re.findall(p, text)) for p in CRITICAL_PATTERNS)
        supportive_hits = sum(len(re.findall(p, text)) for p in SUPPORTIVE_PATTERNS)
        total_hits = critical_hits + supportive_hits

        # No signal → neutral but low confidence
        if total_hits == 0:
            return self._format_output(
                label="neutral",
                confidence=0.55,
                method="rules",
            )

        critical_ratio = critical_hits / total_hits
        supportive_ratio = supportive_hits / total_hits

        # Strong directional signal
        if critical_ratio >= 0.65:
            return self._format_output(
                label="critical",
                confidence=round(critical_ratio, 2),
                method="rules",
            )

        if supportive_ratio >= 0.65:
            return self._format_output(
                label="supportive",
                confidence=round(supportive_ratio, 2),
                method="rules",
            )

        # Mixed narrative
        return self._format_output(
            label="mixed",
            confidence=0.60,
            method="rules",
        )

    # ========================================================
    # MNLI FALLBACK
    # ========================================================

    def _mnli_resolution(self, summary: Dict, titles: List[str]) -> Dict:
        text = (
            summary.get("summary", "") + " " + " ".join(titles[:5])
        )

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
        score = float(result["scores"][0])

        if "supportive" in top_label:
            return self._format_output("supportive", score, "mnli")

        if "critical" in top_label:
            return self._format_output("critical", score, "mnli")

        return self._format_output("neutral", score, "mnli")

    # ========================================================
    # OUTPUT FORMAT
    # ========================================================

    def _format_output(self, label: str, confidence: float, method: str) -> Dict:
        return {
            "label": label,
            "confidence": round(min(max(confidence, 0.0), 0.95), 2),
            "method": method,
            "text": self._stance_text(label),
        }

    def _stance_text(self, label: str) -> str:
        return {
            "supportive": "Coverage is broadly supportive, emphasizing positive outcomes or benefits.",
            "critical": "Coverage is largely critical, focusing on risks, scrutiny, or negative implications.",
            "neutral": "Coverage is primarily informational without strong evaluative framing.",
            "mixed": "Narrative is mixed, highlighting both opportunities and concerns.",
        }.get(label, "No stance interpretation available.")