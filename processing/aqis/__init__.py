"""
AQIS (Article Quality Intelligence Score) Package

A modular system for scoring article quality across multiple dimensions:
- Entity Salience
- Informational Density
- Specificity
- Narrative Intent
- Temporal Grounding

Usage:
    from processing.aqis import AQISBuilder
    from processing.aqis.modules import *
    
    # Initialize modules
    entity_salience = EntitySalienceModule()
    informational_density = InformationalDensityModule()
    specificity = SpecificityModule()
    narrative_intent = NarrativeIntentModule(classifier=your_classifier)
    temporal_grounding = TemporalGroundingModule()
    
    # Create builder
    builder = AQISBuilder(
        entity_salience_module=entity_salience,
        informational_density_module=informational_density,
        specificity_module=specificity,
        narrative_intent_module=narrative_intent,
        temporal_grounding_module=temporal_grounding
    )
    
    # Score an article
    aqis_result = builder.score(article)
"""

from .builder import AQISBuilder
from .config import (
    AQIS_VERSION,
    AQIS_WEIGHTS,
    NARRATIVE_ELIGIBILITY_THRESHOLD
)

__version__ = AQIS_VERSION
__all__ = [
    "AQISBuilder",
    "AQIS_VERSION",
    "AQIS_WEIGHTS",
    "NARRATIVE_ELIGIBILITY_THRESHOLD"
]
