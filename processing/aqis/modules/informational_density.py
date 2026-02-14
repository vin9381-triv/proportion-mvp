"""
Informational Density Module (v2)
Measures how much factual, event-based content the article contains
Supports strong + weak verbs with proper weighting.
"""

import logging
from typing import Dict, List, Optional

from ..config import INFORMATIONAL_DENSITY_CONFIG

logger = logging.getLogger(__name__)


class InformationalDensityModule:
    """
    Computes informational density based on:
    - Strong event verbs (structural change)
    - Weak event verbs (minor signal)
    - Sentence count
    - Opinion marker penalty
    """

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or INFORMATIONAL_DENSITY_CONFIG
        logger.info("Informational Density Module initialized (v2)")

    def score(self, article: Dict) -> float:
        try:
            clean_text = article.get("clean_text", "").lower()
            sentence_list = article.get("sentence_list", [])

            if not clean_text or not sentence_list:
                logger.warning(f"Missing text data for article {article.get('_id')}")
                return 0.0

            score = 0.0

            # -------------------------------
            # 1️⃣ Event Verb Signal
            # -------------------------------
            verb_score = self._calculate_event_verb_score(clean_text)
            score += verb_score

            # -------------------------------
            # 2️⃣ Sentence Count Signal
            # -------------------------------
            sentence_score = self._calculate_sentence_count_score(sentence_list)
            weighted_sentence_score = (
                sentence_score * self.config["sentence_count_weight"]
            )
            score += weighted_sentence_score

            # -------------------------------
            # 3️⃣ Opinion Penalty Signal
            # -------------------------------
            opinion_penalty = self._calculate_opinion_penalty(clean_text)
            weighted_opinion_score = (
                (1.0 - opinion_penalty) * self.config["opinion_penalty_weight"]
            )
            score += weighted_opinion_score

            final_score = min(score, 1.0)

            logger.debug(
                f"Density breakdown | verbs: {verb_score:.3f}, "
                f"sentence: {weighted_sentence_score:.3f}, "
                f"opinion: {weighted_opinion_score:.3f}, "
                f"final: {final_score:.3f}"
            )

            return round(final_score, 4)

        except Exception as e:
            logger.error(
                f"Error calculating informational density: {e}",
                exc_info=True,
            )
            return 0.0

    # =====================================================
    # Event Verb Logic (Strong + Weak)
    # =====================================================

    def _calculate_event_verb_score(self, text: str) -> float:
        """
        Strong verbs contribute more.
        Weak verbs contribute less.
        """

        strong_verbs = self.config.get("strong_event_verbs", [])
        weak_verbs = self.config.get("weak_event_verbs", [])

        strong_weight = self.config.get("strong_verb_weight", 0.5)
        weak_weight = self.config.get("weak_verb_weight", 0.15)

        strong_count = sum(1 for verb in strong_verbs if verb in text)
        weak_count = sum(1 for verb in weak_verbs if verb in text)

        # Normalize counts (avoid runaway inflation)
        strong_score = min(strong_count / 4.0, 1.0)
        weak_score = min(weak_count / 6.0, 1.0)

        weighted_score = (
            strong_score * strong_weight +
            weak_score * weak_weight
        )

        return min(weighted_score, 1.0)

    # =====================================================
    # Sentence Count Logic
    # =====================================================

    def _calculate_sentence_count_score(self, sentence_list: List[str]) -> float:
        min_count = self.config["min_sentence_count"]
        sentence_count = len(sentence_list)

        if sentence_count < min_count:
            return 0.0

        # Cap normalization at 20 sentences
        normalized = min((sentence_count - min_count) / 15.0, 1.0)
        return normalized

    # =====================================================
    # Opinion Penalty Logic
    # =====================================================

    def _calculate_opinion_penalty(self, text: str) -> float:
        opinion_markers = self.config["opinion_markers"]

        found_markers = sum(1 for marker in opinion_markers if marker in text)

        # Cap at 3 markers
        penalty = min(found_markers / 3.0, 1.0)

        return penalty