def get_clustering_candidates_query(raw_article_ids):
    return {
        "$and": [
            {
                "$or": [
                    # Singleton articles (never deduped)
                    {"processing.semantically_deduped": {"$ne": True}},
                    # Canonical representatives
                    {"processing.is_canonical": True}
                ]
            },
            {
                # Explicitly exclude suppressed duplicates
                "processing.is_canonical": {"$ne": False}
            },
            {
                "raw_article_id": {"$in": raw_article_ids}
            }
        ]
    }
