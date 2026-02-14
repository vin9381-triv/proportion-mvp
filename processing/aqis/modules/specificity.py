"""
Specificity Module
Measures how specific and concrete the article content is
"""

import logging
import re
from typing import Dict, Optional
from ..config import SPECIFICITY_CONFIG

logger = logging.getLogger(__name__)


class SpecificityModule:
    """
    Computes specificity score based on:
    - Numbers and quantitative data
    - Dates and temporal markers
    - Proper noun density
    - Named roles and titles
    """

    def __init__(self, config: Optional[Dict] = None, nlp=None):
        """
        Initialize Specificity Module
        
        Args:
            config: Optional configuration override
            nlp: Optional SpaCy NLP model for proper noun detection
        """
        self.config = config or SPECIFICITY_CONFIG
        self.nlp = nlp
        logger.info("Specificity Module initialized")

    def score(self, article: Dict) -> float:
        """
        Calculate specificity score
        
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
            
            # Signal 1: Numbers detection
            number_score = self._calculate_number_score(clean_text)
            weighted_number_score = number_score * self.config["number_weight"]
            score += weighted_number_score
            logger.debug(f"Number score: {number_score:.3f}, weighted: +{weighted_number_score:.3f}")
            
            # Signal 2: Date mentions
            date_score = self._calculate_date_score(clean_text)
            weighted_date_score = date_score * self.config["date_weight"]
            score += weighted_date_score
            logger.debug(f"Date score: {date_score:.3f}, weighted: +{weighted_date_score:.3f}")
            
            # Signal 3: Proper noun density
            proper_noun_score = self._calculate_proper_noun_score(clean_text)
            weighted_proper_noun_score = proper_noun_score * self.config["proper_noun_weight"]
            score += weighted_proper_noun_score
            logger.debug(f"Proper noun score: {proper_noun_score:.3f}, weighted: +{weighted_proper_noun_score:.3f}")
            
            # Signal 4: Named roles
            role_score = self._calculate_role_score(clean_text)
            weighted_role_score = role_score * self.config["role_weight"]
            score += weighted_role_score
            logger.debug(f"Role score: {role_score:.3f}, weighted: +{weighted_role_score:.3f}")
            
            # Clamp to 1.0
            final_score = min(score, 1.0)
            
            logger.info(f"Specificity score: {final_score:.4f}")
            return round(final_score, 4)
            
        except Exception as e:
            logger.error(f"Error calculating specificity: {e}", exc_info=True)
            return 0.0

    def _calculate_number_score(self, text: str) -> float:
        """
        Calculate score based on presence of numbers, percentages, currencies
        
        Args:
            text: Article text
            
        Returns:
            float: Score between 0.0 and 1.0
        """
        # Patterns for numbers, percentages, currencies
        patterns = [
            r'\d+\.?\d*%',  # Percentages: 25%, 3.5%
            r'\$\d+\.?\d*[KMB]?',  # Currency: $100, $1.5M, $2B
            r'€\d+\.?\d*[KMB]?',  # Euro
            r'£\d+\.?\d*[KMB]?',  # Pound
            r'\d+\.?\d*\s*(million|billion|trillion)',  # Written numbers
            r'\d{4}',  # Years: 2024
            r'\d+\.\d+',  # Decimals: 3.14
            r'\d+,\d+',  # Comma-separated: 1,000
        ]
        
        total_matches = 0
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            total_matches += len(matches)
        
        # Finding 10+ numerical references gives score of 1.0
        score = min(total_matches / 10.0, 1.0)
        return score

    def _calculate_date_score(self, text: str) -> float:
        """
        Calculate score based on date and quarter mentions
        
        Args:
            text: Article text
            
        Returns:
            float: Score between 0.0 and 1.0
        """
        # Date patterns
        date_patterns = [
            r'\d{1,2}/\d{1,2}/\d{2,4}',  # MM/DD/YYYY
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}',
            r'Q[1-4]\s+\d{4}',  # Q1 2024
            r'(first|second|third|fourth)\s+quarter',
        ]
        
        total_matches = 0
        for pattern in date_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            total_matches += len(matches)
        
        # Finding 5+ date references gives score of 1.0
        score = min(total_matches / 5.0, 1.0)
        return score

    def _calculate_proper_noun_score(self, text: str) -> float:
        """
        Calculate score based on proper noun density
        Uses SpaCy if available, otherwise uses capitalization heuristic
        
        Args:
            text: Article text
            
        Returns:
            float: Score between 0.0 and 1.0
        """
        if self.nlp:
            try:
                doc = self.nlp(text)
                proper_nouns = [token for token in doc if token.pos_ == "PROPN"]
                total_words = len([token for token in doc if not token.is_punct and not token.is_space])
                
                if total_words > 0:
                    density = len(proper_nouns) / total_words
                    # 15% proper noun density gives score of 1.0
                    score = min(density / 0.15, 1.0)
                    return score
            except Exception as e:
                logger.warning(f"Error using SpaCy for proper noun detection: {e}")
        
        # Fallback: capitalized word heuristic
        words = text.split()
        capitalized_words = [w for w in words if w and w[0].isupper() and len(w) > 1]
        
        if len(words) > 0:
            density = len(capitalized_words) / len(words)
            score = min(density / 0.15, 1.0)
            return score
        
        return 0.0

    def _calculate_role_score(self, text: str) -> float:
        """
        Calculate score based on named roles and titles
        
        Args:
            text: Article text
            
        Returns:
            float: Score between 0.0 and 1.0
        """
        roles = self.config["roles"]
        found_roles = sum(1 for role in roles if role in text)
        
        # Finding 5+ roles gives score of 1.0
        score = min(found_roles / 5.0, 1.0)
        return score
