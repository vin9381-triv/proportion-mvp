"""
Temporal Grounding Module
Measures how well the article is grounded in specific time references
"""

import logging
import re
from typing import Dict, Optional
from ..config import TEMPORAL_GROUNDING_CONFIG

logger = logging.getLogger(__name__)


class TemporalGroundingModule:
    """
    Computes temporal grounding score based on:
    - Specific dates
    - Relative time expressions
    - Verb tense patterns
    """

    def __init__(self, config: Optional[Dict] = None, nlp=None):
        """
        Initialize Temporal Grounding Module
        
        Args:
            config: Optional configuration override
            nlp: Optional SpaCy NLP model for verb tense detection
        """
        self.config = config or TEMPORAL_GROUNDING_CONFIG
        self.nlp = nlp
        logger.info("Temporal Grounding Module initialized")

    def score(self, article: Dict) -> float:
        """
        Calculate temporal grounding score
        
        Args:
            article: Article dict with clean_text
            
        Returns:
            float: Score between 0.0 and 1.0
        """
        try:
            score = 0.0
            
            clean_text = article.get("clean_text", "")
            if not clean_text:
                logger.warning(f"Missing clean_text for article {article.get('_id')}")
                return 0.0
            
            # Signal 1: Date references
            date_score = self._calculate_date_score(clean_text)
            weighted_date_score = date_score * self.config["date_weight"]
            score += weighted_date_score
            logger.debug(f"Date score: {date_score:.3f}, weighted: +{weighted_date_score:.3f}")
            
            # Signal 2: Relative time expressions
            relative_time_score = self._calculate_relative_time_score(clean_text)
            weighted_relative_score = relative_time_score * self.config["relative_time_weight"]
            score += weighted_relative_score
            logger.debug(f"Relative time score: {relative_time_score:.3f}, weighted: +{weighted_relative_score:.3f}")
            
            # Signal 3: Verb tense (past tense indicates completed events)
            verb_tense_score = self._calculate_verb_tense_score(clean_text)
            weighted_tense_score = verb_tense_score * self.config["verb_tense_weight"]
            score += weighted_tense_score
            logger.debug(f"Verb tense score: {verb_tense_score:.3f}, weighted: +{weighted_tense_score:.3f}")
            
            # Normalize to 0-1
            final_score = min(score, 1.0)
            
            logger.info(f"Temporal grounding score: {final_score:.4f}")
            return round(final_score, 4)
            
        except Exception as e:
            logger.error(f"Error calculating temporal grounding: {e}", exc_info=True)
            return 0.0

    def _calculate_date_score(self, text: str) -> float:
        """
        Calculate score based on specific date references
        
        Args:
            text: Article text
            
        Returns:
            float: Score between 0.0 and 1.0
        """
        # Date patterns (similar to specificity module but focused on temporal context)
        date_patterns = [
            r'\d{1,2}/\d{1,2}/\d{2,4}',  # MM/DD/YYYY
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}',
            r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+\d{1,2},?\s+\d{4}',
            r'\d{1,2}\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}',
            r'Q[1-4]\s+\d{4}',  # Q1 2024
            r'(first|second|third|fourth)\s+quarter\s+\d{4}',
        ]
        
        total_matches = 0
        for pattern in date_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            total_matches += len(matches)
        
        # Finding 3+ specific dates gives score of 1.0
        score = min(total_matches / 3.0, 1.0)
        return score

    def _calculate_relative_time_score(self, text: str) -> float:
        """
        Calculate score based on relative time expressions
        
        Args:
            text: Article text
            
        Returns:
            float: Score between 0.0 and 1.0
        """
        text_lower = text.lower()
        relative_expressions = self.config["relative_time_expressions"]
        
        found_expressions = sum(1 for expr in relative_expressions if expr in text_lower)
        
        # Finding 5+ relative time expressions gives score of 1.0
        score = min(found_expressions / 5.0, 1.0)
        return score

    def _calculate_verb_tense_score(self, text: str) -> float:
        """
        Calculate score based on verb tense patterns
        Uses SpaCy if available, otherwise uses regex heuristics
        
        Args:
            text: Article text
            
        Returns:
            float: Score between 0.0 and 1.0
        """
        if self.nlp:
            try:
                doc = self.nlp(text)
                verbs = [token for token in doc if token.pos_ == "VERB"]
                
                if len(verbs) > 0:
                    # Count past tense verbs (VBD, VBN tags)
                    past_tense_verbs = [v for v in verbs if v.tag_ in ["VBD", "VBN"]]
                    past_tense_ratio = len(past_tense_verbs) / len(verbs)
                    
                    # Higher ratio of past tense = better temporal grounding
                    # 60% past tense gives score of 1.0
                    score = min(past_tense_ratio / 0.6, 1.0)
                    return score
            except Exception as e:
                logger.warning(f"Error using SpaCy for verb tense detection: {e}")
        
        # Fallback: regex-based past tense detection
        past_tense_patterns = [
            r'\b\w+ed\b',  # Regular past tense: reported, announced
            r'\bwas\b', r'\bwere\b',  # Past to be
            r'\bhad\b',  # Past perfect
            r'\bdid\b',  # Past auxiliary
        ]
        
        past_tense_count = 0
        for pattern in past_tense_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            past_tense_count += len(matches)
        
        # Normalize by word count
        words = text.split()
        if len(words) > 0:
            past_tense_density = past_tense_count / len(words)
            # 10% past tense density gives score of 1.0
            score = min(past_tense_density / 0.1, 1.0)
            return score
        
        return 0.0
