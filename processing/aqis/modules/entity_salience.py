"""
Entity Salience Module
Measures how prominently the primary entity appears in the article
"""

import logging
from typing import Dict, List, Optional
from ..config import ENTITY_SALIENCE_CONFIG

logger = logging.getLogger(__name__)


class EntitySalienceModule:
    """
    Computes entity salience score based on:
    - Primary entity presence in title
    - Primary entity presence in first paragraph
    - Mention ratio compared to other entities
    """

    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize Entity Salience Module
        
        Args:
            config: Optional configuration override
        """
        self.config = config or ENTITY_SALIENCE_CONFIG
        logger.info("Entity Salience Module initialized")

    def score(self, article: Dict) -> float:
        """
        Calculate entity salience score
        
        Args:
            article: Article dict with clean_text, title, entities, primary_entity_id
            
        Returns:
            float: Score between 0.0 and 1.0
        """
        try:
            score = 0.0
            
            # Get primary entity
            primary_entity_id = article.get("primary_entity_id")
            if not primary_entity_id:
                logger.warning(f"No primary_entity_id for article {article.get('_id')}")
                return 0.0
            
            # Find primary entity details
            primary_entity = None
            for entity in article.get("entities", []):
                if entity.get("entity_id") == primary_entity_id:
                    primary_entity = entity
                    break
            
            if not primary_entity:
                logger.warning(f"Primary entity {primary_entity_id} not found in entities list")
                return 0.0
            
            entity_name = primary_entity.get("entity_name", "").lower()
            
            # Signal 1: Primary entity in title
            title = article.get("title", "").lower()
            if entity_name in title:
                score += self.config["title_weight"]
                logger.debug(f"Entity '{entity_name}' found in title: +{self.config['title_weight']}")
            
            # Signal 2: Primary entity in first paragraph
            sentence_list = article.get("sentence_list", [])
            first_para_count = self.config["first_para_sentence_count"]
            first_paragraph = " ".join(sentence_list[:first_para_count]).lower()
            
            if entity_name in first_paragraph:
                score += self.config["first_para_weight"]
                logger.debug(f"Entity '{entity_name}' found in first paragraph: +{self.config['first_para_weight']}")
            
            # Signal 3: Mention ratio
            mention_ratio = self._calculate_mention_ratio(article, entity_name)
            mention_score = mention_ratio * self.config["mention_ratio_weight"]
            score += mention_score
            logger.debug(f"Mention ratio: {mention_ratio:.3f}, score: +{mention_score:.3f}")
            
            # Clamp to 1.0
            final_score = min(score, 1.0)
            
            logger.info(f"Entity salience score: {final_score:.4f}")
            return round(final_score, 4)
            
        except Exception as e:
            logger.error(f"Error calculating entity salience: {e}", exc_info=True)
            return 0.0

    def _calculate_mention_ratio(self, article: Dict, entity_name: str) -> float:
        """
        Calculate ratio of primary entity mentions to total entity mentions
        
        Args:
            article: Article dictionary
            entity_name: Primary entity name
            
        Returns:
            float: Ratio between 0.0 and 1.0
        """
        clean_text = article.get("clean_text", "").lower()
        
        # Count primary entity mentions
        primary_mentions = clean_text.count(entity_name)
        
        # Count total entity mentions
        total_mentions = 0
        for entity in article.get("entities", []):
            ent_name = entity.get("entity_name", "").lower()
            if ent_name:
                total_mentions += clean_text.count(ent_name)
        
        if total_mentions == 0:
            return 0.0
        
        ratio = primary_mentions / total_mentions
        return min(ratio, 1.0)
