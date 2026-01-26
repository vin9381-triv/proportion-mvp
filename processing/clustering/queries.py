def get_clustering_candidates_query():
    """
    Include:
    - Singletons (not semantically deduped)
    - Canonical articles from dedup groups

    Exclude:
    - Non-canonical duplicates
    """
    return {
        "$or": [
            {"processing.semantically_deduped": {"$ne": True}},
            {"processing.is_canonical": True}
        ]
    }
