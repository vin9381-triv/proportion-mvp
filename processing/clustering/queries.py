def get_clustering_candidates_query(raw_article_ids):
    return {
        "$and": [
            {
                "$or": [
                    {"processing.semantically_deduped": {"$ne": True}},
                    {"processing.is_canonical": True}
                ]
            },
            {
                "raw_article_id": {"$in": raw_article_ids}
            }
        ]
    }
