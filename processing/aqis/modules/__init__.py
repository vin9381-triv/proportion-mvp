"""
AQIS Modules Package
Contains all dimension scoring modules
"""

from .entity_salience import EntitySalienceModule
from .informational_density import InformationalDensityModule
from .specificity import SpecificityModule
from .narrative_intent import NarrativeIntentModule, StubClassifier
from .temporal_grounding import TemporalGroundingModule

__all__ = [
    "EntitySalienceModule",
    "InformationalDensityModule",
    "SpecificityModule",
    "NarrativeIntentModule",
    "TemporalGroundingModule",
    "StubClassifier",
]
