"""
AQIS Configuration - Precision First (v2)
==========================================

Design Goals:
- Create strong score separation
- Prevent density inflation
- Protect entity centrality
- Avoid threshold gaming
- Target 20–35% eligibility naturally

Philosophy:
AQIS is a gate, not a volume booster.
Low recall, high precision.
"""

# ============================================================================
# VERSION
# ============================================================================

AQIS_VERSION = "aqis_v2_precision"

# ============================================================================
# THRESHOLD (Conservative)
# ============================================================================

# Do NOT optimize for % eligible.
# Tune based on score distribution after weights stabilize.

NARRATIVE_ELIGIBILITY_THRESHOLD = 0.55

# ============================================================================
# WEIGHTS (Balanced & Stable)
# ============================================================================

AQIS_WEIGHTS = {
    "entity_salience": 0.20,            # Critical for story integrity
    "informational_density": 0.30,      # Important but not dominant
    "specificity": 0.20,                # Structural grounding
    "narrative_intent": 0.20,           # Reduces opinion leakage
    "temporal_grounding": 0.10          # Contextual support
}

# ============================================================================
# ENTITY SALIENCE (Stricter)
# ============================================================================

ENTITY_SALIENCE_CONFIG = {
    "title_weight": 0.45,               # Strong signal
    "first_para_weight": 0.30,
    "mention_ratio_weight": 0.25,
    "first_para_sentence_count": 3,
    
    # NEW: Penalize multi-entity dilution
    "multi_entity_penalty_threshold": 5,
    "multi_entity_penalty_factor": 0.85
}

# ============================================================================
# INFORMATIONAL DENSITY (Strong Verbs Only)
# ============================================================================

INFORMATIONAL_DENSITY_CONFIG = {

    # Only STRUCTURAL change verbs
    "strong_event_verbs": [
        "announced", "launched", "reported", "released",
        "approved", "filed", "acquired", "merged",
        "invested", "expanded", "opened", "closed",
        "appointed", "resigned", "signed", "terminated",
        "cut", "reduced", "increased", "raised",
        "delayed", "suspended", "halted", "restructured",
        "won", "lost"
    ],

    # Weak verbs contribute minimal weight
    "weak_event_verbs": [
        "said", "added", "noted", "stated",
        "expects", "plans", "believes"
    ],

    "strong_verb_weight": 0.5,
    "weak_verb_weight": 0.15,

    "min_sentence_count": 3,
    "sentence_count_weight": 0.25,

    "opinion_penalty_weight": 0.25,

    "opinion_markers": [
        "opinion", "believe", "think", "feel",
        "editorial", "commentary", "analysis",
        "perspective", "slams", "blasts"
    ]
}

# ============================================================================
# SPECIFICITY (Structural Anchoring)
# ============================================================================

SPECIFICITY_CONFIG = {
    "number_weight": 0.30,
    "date_weight": 0.25,
    "proper_noun_weight": 0.25,
    "role_weight": 0.20,

    "roles": [
        "CEO", "CFO", "CTO", "COO", "President",
        "Chairman", "Director", "Manager",
        "Minister", "Secretary", "Governor",
        "Commissioner", "Regulator"
    ]
}

# ============================================================================
# TEMPORAL GROUNDING
# ============================================================================

TEMPORAL_GROUNDING_CONFIG = {
    "date_weight": 0.5,
    "relative_time_weight": 0.3,
    "verb_tense_weight": 0.2,

    "relative_time_expressions": [
        "today", "yesterday", "this week",
        "last week", "this month", "last month",
        "this year", "recently", "earlier"
    ]
}

# ============================================================================
# NARRATIVE INTENT (AI Module Weighting)
# ============================================================================

NARRATIVE_INTENT_CONFIG = {
    "factual_hypothesis": "This article reports a concrete real-world event.",
    "opinion_hypothesis": "This article expresses opinion or commentary.",
    "min_confidence_threshold": 0.15
}

# ============================================================================
# LOGGING
# ============================================================================

LOGGING_CONFIG = {
    "log_level": "INFO",
    "log_format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
}

# ============================================================================
# EXPECTED BEHAVIOR
# ============================================================================

"""
Expected Score Behavior:

Top 15%:
- Clear structural events
- Entity central
- Strong verbs
- Concrete numbers

Middle 40%:
- Mixed quality
- Some weak verbs
- Partial structure

Bottom 30%:
- Opinion, fluff, multi-entity noise

Target Eligibility:
20–35% (naturally from score separation)
NOT achieved by threshold gaming.

If >50% eligible:
Your density scoring is too permissive.

If <15% eligible:
Entity salience extraction is failing.
"""