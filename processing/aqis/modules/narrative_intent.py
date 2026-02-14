"""
Narrative Intent Module
Measures whether the article reports factual events vs. opinion/commentary
Uses a pluggable classifier for AI-based scoring
"""

import logging
from typing import Dict, Optional, Protocol

logger = logging.getLogger(__name__)


class ClassifierProtocol(Protocol):
    """Protocol for zero-shot classifier interface"""
    
    def predict(self, text: str, hypothesis: str) -> float:
        """
        Predict probability that hypothesis is true for given text
        
        Args:
            text: Input text to classify
            hypothesis: Hypothesis to test
            
        Returns:
            float: Probability between 0.0 and 1.0
        """
        ...


class NarrativeIntentModule:
    """
    Computes narrative intent score using AI classifier
    Distinguishes factual reporting from opinion/commentary
    """

    def __init__(self, classifier: Optional[ClassifierProtocol] = None):
        """
        Initialize Narrative Intent Module
        
        Args:
            classifier: Zero-shot classifier that implements predict(text, hypothesis)
        """
        self.classifier = classifier
        logger.info(f"Narrative Intent Module initialized with classifier: {type(classifier).__name__ if classifier else 'None'}")

    def score(self, article: Dict) -> float:
        """
        Calculate narrative intent score
        
        Args:
            article: Article dict with clean_text
            
        Returns:
            float: Score between 0.0 and 1.0
        """
        try:
            clean_text = article.get("clean_text", "")
            if not clean_text:
                logger.warning(f"Missing clean_text for article {article.get('_id')}")
                return 0.5  # Neutral default
            
            # If no classifier provided, use heuristic fallback
            if self.classifier is None:
                logger.warning("No classifier provided, using heuristic fallback")
                return self._heuristic_fallback(clean_text)
            
            # Use classifier to get probabilities
            factual_prob = self.classifier.predict(
                clean_text,
                hypothesis="This article reports a factual event."
            )
            
            opinion_prob = self.classifier.predict(
                clean_text,
                hypothesis="This article expresses opinion or commentary."
            )
            
            # Score is factual probability minus opinion probability
            # Higher score = more factual reporting
            score = max(0.0, factual_prob - opinion_prob)
            final_score = min(score, 1.0)
            
            logger.info(f"Narrative intent score: {final_score:.4f} (factual: {factual_prob:.3f}, opinion: {opinion_prob:.3f})")
            return round(final_score, 4)
            
        except Exception as e:
            logger.error(f"Error calculating narrative intent: {e}", exc_info=True)
            return 0.5  # Neutral default on error

    def _heuristic_fallback(self, text: str) -> float:
        """
        Fallback heuristic scoring when no classifier is available
        
        Args:
            text: Article text (lowercase)
            
        Returns:
            float: Score between 0.0 and 1.0
        """
        text_lower = text.lower()
        
        # Factual indicators
        factual_indicators = [
            "announced", "reported", "stated", "according to",
            "data shows", "statistics", "results indicate",
            "confirmed", "revealed", "disclosed"
        ]
        
        # Opinion indicators
        opinion_indicators = [
            "i think", "i believe", "in my opinion", "should",
            "must", "arguably", "seems", "appears to",
            "might", "could be", "suggests that"
        ]
        
        factual_count = sum(1 for indicator in factual_indicators if indicator in text_lower)
        opinion_count = sum(1 for indicator in opinion_indicators if indicator in text_lower)
        
        # Normalize counts
        factual_score = min(factual_count / 5.0, 1.0)
        opinion_score = min(opinion_count / 5.0, 1.0)
        
        # Calculate final score
        score = max(0.0, factual_score - opinion_score)
        return min(score + 0.5, 1.0)  # Add baseline to avoid too-low scores


# Example stub classifier for testing
class StubClassifier:
    """Stub classifier for testing/development"""
    
    def predict(self, text: str, hypothesis: str) -> float:
        """
        Simple keyword-based prediction for testing
        
        Args:
            text: Input text
            hypothesis: Hypothesis string
            
        Returns:
            float: Mock probability
        """
        text_lower = text.lower()
        
        if "factual event" in hypothesis.lower():
            # Check for factual indicators
            factual_keywords = ["announced", "reported", "data", "confirmed"]
            count = sum(1 for kw in factual_keywords if kw in text_lower)
            return min(0.5 + (count * 0.1), 1.0)
        
        elif "opinion" in hypothesis.lower():
            # Check for opinion indicators
            opinion_keywords = ["think", "believe", "should", "opinion"]
            count = sum(1 for kw in opinion_keywords if kw in text_lower)
            return min(0.3 + (count * 0.1), 1.0)
        
        return 0.5
