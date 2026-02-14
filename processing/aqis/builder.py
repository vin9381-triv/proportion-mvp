"""
AQIS Builder
Main class for computing Article Quality Intelligence Score
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional
from .config import (
    AQIS_WEIGHTS,
    AQIS_VERSION,
    NARRATIVE_ELIGIBILITY_THRESHOLD,
    LOGGING_CONFIG
)
from .modules import (
    EntitySalienceModule,
    InformationalDensityModule,
    SpecificityModule,
    NarrativeIntentModule,
    TemporalGroundingModule
)

# Configure logging
logging.basicConfig(
    level=LOGGING_CONFIG["log_level"],
    format=LOGGING_CONFIG["log_format"]
)
logger = logging.getLogger(__name__)


class AQISBuilder:
    """
    Article Quality Intelligence Score Builder
    
    Computes multi-dimensional quality scores for articles and aggregates them
    into an overall score. Each dimension is computed by a pluggable module.
    
    Dimensions:
    - Entity Salience: How prominently the primary entity appears
    - Informational Density: Amount of factual, event-based content
    - Specificity: Concrete details like numbers, dates, proper nouns
    - Narrative Intent: Factual reporting vs opinion/commentary
    - Temporal Grounding: Time-specific references and context
    """

    def __init__(
        self,
        entity_salience_module: EntitySalienceModule,
        informational_density_module: InformationalDensityModule,
        specificity_module: SpecificityModule,
        narrative_intent_module: NarrativeIntentModule,
        temporal_grounding_module: TemporalGroundingModule,
        weights: Optional[Dict[str, float]] = None
    ):
        """
        Initialize AQIS Builder with dimension modules
        
        Args:
            entity_salience_module: Module for entity salience scoring
            informational_density_module: Module for informational density scoring
            specificity_module: Module for specificity scoring
            narrative_intent_module: Module for narrative intent scoring
            temporal_grounding_module: Module for temporal grounding scoring
            weights: Optional custom weights (must sum to 1.0)
        """
        self.entity_salience = entity_salience_module
        self.informational_density = informational_density_module
        self.specificity = specificity_module
        self.narrative_intent = narrative_intent_module
        self.temporal_grounding = temporal_grounding_module
        
        # Use custom weights or defaults
        self.weights = weights or AQIS_WEIGHTS
        
        # Validate weights
        self._validate_weights()
        
        logger.info(f"AQIS Builder initialized (version: {AQIS_VERSION})")
        logger.info(f"Weights: {self.weights}")

    def _validate_weights(self) -> None:
        """Validate that weights sum to 1.0"""
        weight_sum = sum(self.weights.values())
        if abs(weight_sum - 1.0) > 0.001:
            logger.warning(f"Weights sum to {weight_sum:.4f}, expected 1.0. Normalizing...")
            # Normalize weights
            for key in self.weights:
                self.weights[key] /= weight_sum

    def score(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compute AQIS score for an article
        
        Args:
            article: Article dictionary with required fields:
                - _id: ObjectId
                - title: str
                - clean_text: str
                - sentence_list: List[str]
                - entities: List[Dict]
                - primary_entity_id: str
                - published_at: datetime
        
        Returns:
            Dict containing:
                - overall: float (0.0-1.0)
                - dimensions: Dict[str, float]
                - eligible_for_narrative: bool
                - version: str
                - scored_at: datetime
        """
        article_id = article.get("_id", "unknown")
        logger.info(f"Scoring article: {article_id}")
        
        try:
            # Validate input
            self._validate_article(article)
            
            # Compute dimension scores
            dim_scores = {}
            
            logger.info("Computing entity salience...")
            dim_scores["entity_salience"] = self.entity_salience.score(article)
            
            logger.info("Computing informational density...")
            dim_scores["informational_density"] = self.informational_density.score(article)
            
            logger.info("Computing specificity...")
            dim_scores["specificity"] = self.specificity.score(article)
            
            logger.info("Computing narrative intent...")
            dim_scores["narrative_intent"] = self.narrative_intent.score(article)
            
            logger.info("Computing temporal grounding...")
            dim_scores["temporal_grounding"] = self.temporal_grounding.score(article)
            
            # Compute weighted overall score
            overall = 0.0
            for dimension, weight in self.weights.items():
                score = dim_scores.get(dimension, 0.0)
                overall += weight * score
                logger.debug(f"{dimension}: {score:.4f} × {weight:.2f} = {weight * score:.4f}")
            
            # Round scores for consistency
            overall = round(overall, 4)
            for key in dim_scores:
                dim_scores[key] = round(dim_scores[key], 4)
            
            # Determine narrative eligibility
            eligible = overall >= NARRATIVE_ELIGIBILITY_THRESHOLD
            
            result = {
                "overall": overall,
                "dimensions": dim_scores,
                "eligible_for_narrative": eligible,
                "version": AQIS_VERSION,
                "scored_at": datetime.utcnow()
            }
            
            logger.info(f"Article {article_id} scored: {overall:.4f} (eligible: {eligible})")
            logger.info(f"Dimension breakdown: {dim_scores}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error scoring article {article_id}: {e}", exc_info=True)
            # Return minimal valid result on error
            return {
                "overall": 0.0,
                "dimensions": {
                    "entity_salience": 0.0,
                    "informational_density": 0.0,
                    "specificity": 0.0,
                    "narrative_intent": 0.0,
                    "temporal_grounding": 0.0
                },
                "eligible_for_narrative": False,
                "version": AQIS_VERSION,
                "scored_at": datetime.utcnow()
            }

    def _validate_article(self, article: Dict[str, Any]) -> None:
        """
        Validate that article has required fields
        
        Args:
            article: Article dictionary
            
        Raises:
            ValueError: If required fields are missing
        """
        required_fields = [
            "_id", "title", "clean_text", "sentence_list",
            "entities", "primary_entity_id"
        ]
        
        missing_fields = [field for field in required_fields if field not in article]
        
        if missing_fields:
            raise ValueError(f"Article missing required fields: {missing_fields}")
        
        # Validate types
        if not isinstance(article.get("sentence_list"), list):
            raise ValueError("sentence_list must be a list")
        
        if not isinstance(article.get("entities"), list):
            raise ValueError("entities must be a list")

    def get_distribution_stats(self, scores: list) -> Dict[str, float]:
        """
        Calculate distribution statistics for a batch of AQIS scores
        
        Args:
            scores: List of AQIS overall scores
            
        Returns:
            Dict with mean, median, std, min, max
        """
        if not scores:
            return {}
        
        import statistics
        
        return {
            "mean": round(statistics.mean(scores), 4),
            "median": round(statistics.median(scores), 4),
            "std": round(statistics.stdev(scores), 4) if len(scores) > 1 else 0.0,
            "min": round(min(scores), 4),
            "max": round(max(scores), 4),
            "count": len(scores)
        }
