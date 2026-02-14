"""
MongoDB Integration for AQIS
Utilities for updating articles with AQIS scores
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from pymongo.database import Database
from pymongo.collection import Collection
from bson import ObjectId

logger = logging.getLogger(__name__)


def update_aqis(
    db: Database,
    article_id: ObjectId,
    aqis_result: Dict[str, Any]
) -> bool:
    """
    Update an article with AQIS score
    
    Args:
        db: MongoDB database instance
        article_id: Article ObjectId
        aqis_result: AQIS scoring result dictionary
        
    Returns:
        bool: True if update successful, False otherwise
    """
    try:
        result = db.articles.update_one(
            {"_id": article_id},
            {"$set": {"aqis": aqis_result}}
        )
        
        if result.modified_count > 0:
            logger.info(f"Updated AQIS for article {article_id}")
            return True
        else:
            logger.warning(f"No article found with _id: {article_id}")
            return False
            
    except Exception as e:
        logger.error(f"Error updating AQIS for article {article_id}: {e}", exc_info=True)
        return False


def batch_update_aqis(
    db: Database,
    aqis_results: List[Dict[str, Any]]
) -> Dict[str, int]:
    """
    Batch update multiple articles with AQIS scores
    
    Args:
        db: MongoDB database instance
        aqis_results: List of dicts with 'article_id' and 'aqis' keys
        
    Returns:
        Dict with 'success' and 'failed' counts
    """
    success_count = 0
    failed_count = 0
    
    for result in aqis_results:
        article_id = result.get("article_id")
        aqis_data = result.get("aqis")
        
        if not article_id or not aqis_data:
            logger.warning(f"Invalid result format: {result}")
            failed_count += 1
            continue
        
        if update_aqis(db, article_id, aqis_data):
            success_count += 1
        else:
            failed_count += 1
    
    logger.info(f"Batch update complete: {success_count} success, {failed_count} failed")
    return {"success": success_count, "failed": failed_count}


def get_aqis_distribution(
    db: Database,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Get AQIS score distribution statistics
    
    Args:
        db: MongoDB database instance
        start_date: Optional start date filter
        end_date: Optional end date filter
        
    Returns:
        Dict with distribution statistics
    """
    try:
        match_filter = {"aqis": {"$exists": True}}
        
        if start_date or end_date:
            date_filter = {}
            if start_date:
                date_filter["$gte"] = start_date
            if end_date:
                date_filter["$lte"] = end_date
            match_filter["published_at"] = date_filter
        
        pipeline = [
            {"$match": match_filter},
            {
                "$group": {
                    "_id": None,
                    "avg_overall": {"$avg": "$aqis.overall"},
                    "min_overall": {"$min": "$aqis.overall"},
                    "max_overall": {"$max": "$aqis.overall"},
                    "count": {"$sum": 1},
                    "eligible_count": {
                        "$sum": {"$cond": ["$aqis.eligible_for_narrative", 1, 0]}
                    },
                    # Dimension averages
                    "avg_entity_salience": {"$avg": "$aqis.dimensions.entity_salience"},
                    "avg_informational_density": {"$avg": "$aqis.dimensions.informational_density"},
                    "avg_specificity": {"$avg": "$aqis.dimensions.specificity"},
                    "avg_narrative_intent": {"$avg": "$aqis.dimensions.narrative_intent"},
                    "avg_temporal_grounding": {"$avg": "$aqis.dimensions.temporal_grounding"}
                }
            }
        ]
        
        result = list(db.articles.aggregate(pipeline))
        
        if result:
            stats = result[0]
            stats.pop("_id", None)
            return stats
        else:
            return {"count": 0}
            
    except Exception as e:
        logger.error(f"Error getting AQIS distribution: {e}", exc_info=True)
        return {}


def get_low_quality_articles(
    db: Database,
    threshold: float = 0.3,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Get articles with low AQIS scores
    
    Args:
        db: MongoDB database instance
        threshold: AQIS score threshold (articles below this)
        limit: Maximum number of articles to return
        
    Returns:
        List of article documents
    """
    try:
        articles = db.articles.find(
            {
                "aqis.overall": {"$lt": threshold},
                "aqis": {"$exists": True}
            },
            {
                "_id": 1,
                "title": 1,
                "aqis": 1,
                "published_at": 1
            }
        ).sort("aqis.overall", 1).limit(limit)
        
        return list(articles)
        
    except Exception as e:
        logger.error(f"Error getting low quality articles: {e}", exc_info=True)
        return []


def get_narrative_eligible_articles(
    db: Database,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    limit: Optional[int] = None
) -> List[ObjectId]:
    """
    Get article IDs that are eligible for narrative extraction
    
    Args:
        db: MongoDB database instance
        start_date: Optional start date filter
        end_date: Optional end date filter
        limit: Optional limit on number of results
        
    Returns:
        List of article ObjectIds
    """
    try:
        match_filter = {
            "aqis.eligible_for_narrative": True
        }
        
        if start_date or end_date:
            date_filter = {}
            if start_date:
                date_filter["$gte"] = start_date
            if end_date:
                date_filter["$lte"] = end_date
            match_filter["published_at"] = date_filter
        
        cursor = db.articles.find(match_filter, {"_id": 1})
        
        if limit:
            cursor = cursor.limit(limit)
        
        return [doc["_id"] for doc in cursor]
        
    except Exception as e:
        logger.error(f"Error getting narrative eligible articles: {e}", exc_info=True)
        return []


def create_aqis_indexes(db: Database) -> None:
    """
    Create indexes for efficient AQIS queries
    
    Args:
        db: MongoDB database instance
    """
    try:
        # Index on overall score
        db.articles.create_index("aqis.overall")
        logger.info("Created index on aqis.overall")
        
        # Index on eligibility
        db.articles.create_index("aqis.eligible_for_narrative")
        logger.info("Created index on aqis.eligible_for_narrative")
        
        # Compound index for time-based queries
        db.articles.create_index([
            ("published_at", -1),
            ("aqis.overall", -1)
        ])
        logger.info("Created compound index on published_at and aqis.overall")
        
    except Exception as e:
        logger.error(f"Error creating AQIS indexes: {e}", exc_info=True)
