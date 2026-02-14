"""
AQIS MongoDB Integration
========================
Extends the existing MongoDB setup with AQIS-specific functions.
Uses the existing mongo client from common/mongo_client.py

Location: processing/aqis/aqis_mongo_integration.py
"""

from datetime import datetime
from typing import Dict, Any, List, Optional
from bson import ObjectId
import logging

# Import your existing mongo setup
from processing.common.mongo_client import get_collection

logger = logging.getLogger(__name__)


# ============================================================================
# AQIS-Specific Collections
# ============================================================================

def get_aqis_scored_articles_collection():
    """
    Get collection for articles with AQIS scores.
    
    Uses the same 'articles_raw' collection.
    """
    return get_collection("articles_raw")


# ============================================================================
# AQIS Update Functions
# ============================================================================

def update_article_aqis(article_id: ObjectId, aqis_result: Dict[str, Any]) -> bool:
    """
    Update a single article with AQIS score
    
    Args:
        article_id: Article ObjectId
        aqis_result: AQIS scoring result from builder.score()
        
    Returns:
        bool: True if update successful, False otherwise
    """
    try:
        collection = get_aqis_scored_articles_collection()
        
        result = collection.update_one(
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


def batch_update_article_aqis(aqis_results: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Batch update multiple articles with AQIS scores
    
    Args:
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
        
        if update_article_aqis(article_id, aqis_data):
            success_count += 1
        else:
            failed_count += 1
    
    logger.info(f"Batch update complete: {success_count} success, {failed_count} failed")
    return {"success": success_count, "failed": failed_count}


# ============================================================================
# AQIS Query Functions
# ============================================================================

def get_unscored_articles(limit: Optional[int] = None) -> List[Dict]:
    """
    Get articles that haven't been scored with AQIS yet
    
    Args:
        limit: Maximum number of articles to return
        
    Returns:
        List of article documents
    """
    try:
        collection = get_aqis_scored_articles_collection()
        
        query = {"aqis": {"$exists": False}}
        
        cursor = collection.find(query)
        if limit:
            cursor = cursor.limit(limit)
        
        return list(cursor)
        
    except Exception as e:
        logger.error(f"Error getting unscored articles: {e}", exc_info=True)
        return []


def get_narrative_eligible_articles(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    limit: Optional[int] = None
) -> List[ObjectId]:
    """
    Get article IDs that are eligible for narrative extraction
    
    Args:
        start_date: Optional start date filter
        end_date: Optional end date filter
        limit: Optional limit on number of results
        
    Returns:
        List of article ObjectIds
    """
    try:
        collection = get_aqis_scored_articles_collection()
        
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
        
        cursor = collection.find(match_filter, {"_id": 1})
        
        if limit:
            cursor = cursor.limit(limit)
        
        return [doc["_id"] for doc in cursor]
        
    except Exception as e:
        logger.error(f"Error getting narrative eligible articles: {e}", exc_info=True)
        return []


def get_low_quality_articles(threshold: float = 0.3, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Get articles with low AQIS scores
    
    Args:
        threshold: AQIS score threshold (articles below this)
        limit: Maximum number of articles to return
        
    Returns:
        List of article documents
    """
    try:
        collection = get_aqis_scored_articles_collection()
        
        articles = collection.find(
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


def get_aqis_distribution(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Get AQIS score distribution statistics
    
    Args:
        start_date: Optional start date filter
        end_date: Optional end date filter
        
    Returns:
        Dict with distribution statistics
    """
    try:
        collection = get_aqis_scored_articles_collection()
        
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
        
        result = list(collection.aggregate(pipeline))
        
        if result:
            stats = result[0]
            stats.pop("_id", None)
            return stats
        else:
            return {"count": 0}
            
    except Exception as e:
        logger.error(f"Error getting AQIS distribution: {e}", exc_info=True)
        return {}


# ============================================================================
# Index Management
# ============================================================================

def create_aqis_indexes():
    """
    Create indexes for efficient AQIS queries
    
    Call this once during setup
    """
    try:
        collection = get_aqis_scored_articles_collection()
        
        # Index on overall score
        collection.create_index("aqis.overall")
        logger.info("Created index on aqis.overall")
        
        # Index on eligibility
        collection.create_index("aqis.eligible_for_narrative")
        logger.info("Created index on aqis.eligible_for_narrative")
        
        # Compound index for time-based queries
        collection.create_index([
            ("published_at", -1),
            ("aqis.overall", -1)
        ])
        logger.info("Created compound index on published_at and aqis.overall")
        
    except Exception as e:
        logger.error(f"Error creating AQIS indexes: {e}", exc_info=True)


# ============================================================================
# Statistics Helpers
# ============================================================================

def get_aqis_summary() -> Dict[str, Any]:
    """
    Get quick summary of AQIS scoring status
    
    Returns:
        Dict with counts and basic stats
    """
    try:
        collection = get_aqis_scored_articles_collection()
        
        total = collection.count_documents({})
        scored = collection.count_documents({"aqis": {"$exists": True}})
        unscored = total - scored
        eligible = collection.count_documents({"aqis.eligible_for_narrative": True})
        
        return {
            "total_articles": total,
            "scored_articles": scored,
            "unscored_articles": unscored,
            "eligible_articles": eligible,
            "scored_percentage": (scored / total * 100) if total > 0 else 0,
            "eligible_percentage": (eligible / scored * 100) if scored > 0 else 0
        }
        
    except Exception as e:
        logger.error(f"Error getting AQIS summary: {e}", exc_info=True)
        return {}
