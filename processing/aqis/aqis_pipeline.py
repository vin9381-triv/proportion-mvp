"""
AQIS Pipeline Integration
==========================
Integrates AQIS scoring into the Proportion processing pipeline.

Location: processing/aqis/aqis_pipeline.py

Pipeline Flow:
    Raw Article Ingestion
        ↓
    Article Cleaning
        ↓
    Entity Extraction
        ↓
    🎯 AQIS Scoring ← THIS SCRIPT
        ↓
    Narrative Primitive Extraction (if eligible)
        ↓
    Embedding Generation
        ↓
    Semantic Deduplication
        ↓
    Hypothesis Builder

Usage:
    # From Proportion root directory
    
    # Score new unscored articles
    python -m processing.aqis.aqis_pipeline --mode incremental --limit 100
    
    # Score specific article
    python -m processing.aqis.aqis_pipeline --mode single --article-id 507f1f77bcf86cd799439011
    
    # Show statistics
    python -m processing.aqis.aqis_pipeline --mode stats
    
    # Re-score all articles (after config changes)
    python -m processing.aqis.aqis_pipeline --mode rescore-all
"""

import argparse
import logging
from datetime import datetime
from typing import List, Dict, Any
from bson import ObjectId
import sys

# Your existing mongo client
from processing.common.mongo_client import get_collection

# AQIS imports (same folder)
from .builder import AQISBuilder
from .modules import (
    EntitySalienceModule,
    InformationalDensityModule,
    SpecificityModule,
    NarrativeIntentModule,
    TemporalGroundingModule,
    StubClassifier
)

# AQIS MongoDB integration (same folder)
from .aqis_mongo_integration_corrected import (
    update_article_aqis,
    get_unscored_articles,
    get_narrative_eligible_articles,
    get_aqis_distribution,
    get_aqis_summary,
    create_aqis_indexes
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# AQIS Pipeline Class
# ============================================================================

class AQISPipeline:
    """AQIS Pipeline integrated with Proportion system"""
    
    def __init__(self, custom_classifier=None):
        """
        Initialize AQIS Pipeline
        
        Args:
            custom_classifier: Optional custom classifier for narrative intent
                              If None, uses StubClassifier
        """
        logger.info("Initializing AQIS Pipeline...")
        
        # Try to load SpaCy if available
        self.nlp = self._load_spacy()
        
        # Use custom classifier or default stub
        classifier = custom_classifier or StubClassifier()
        
        # Create AQIS builder
        self.builder = AQISBuilder(
            entity_salience_module=EntitySalienceModule(),
            informational_density_module=InformationalDensityModule(),
            specificity_module=SpecificityModule(nlp=self.nlp),
            narrative_intent_module=NarrativeIntentModule(classifier=classifier),
            temporal_grounding_module=TemporalGroundingModule(nlp=self.nlp)
        )
        
        logger.info("✅ AQIS Pipeline initialized")
    
    def _load_spacy(self):
        """Load SpaCy model if available"""
        try:
            import spacy
            nlp = spacy.load("en_core_web_sm")
            logger.info("✅ SpaCy model loaded")
            return nlp
        except:
            logger.warning("⚠️  SpaCy not available, using fallback")
            return None
    
    def score_article(self, article: Dict) -> Dict[str, Any]:
        """
        Score a single article with AQIS
        
        Args:
            article: Article document from MongoDB
            
        Returns:
            AQIS result dict
        """
        return self.builder.score(article)
    
    def process_single_article(self, article_id: str) -> bool:
        """
        Process and score a single article by ID
        
        Args:
            article_id: Article ObjectId as string
            
        Returns:
            bool: Success status
        """
        try:
            # Convert to ObjectId
            oid = ObjectId(article_id)
            
            # Fetch article
            collection = get_collection("articles_raw")
            article = collection.find_one({"_id": oid})
            
            if not article:
                logger.error(f"Article not found: {article_id}")
                return False
            
            # Validate required fields
            if not self._validate_article(article):
                return False
            
            # Score article
            logger.info(f"Scoring article: {article.get('title', 'Untitled')[:50]}...")
            aqis_result = self.score_article(article)
            
            # Display results
            self._display_result(article_id, article, aqis_result)
            
            # Update in database
            success = update_article_aqis(oid, aqis_result)
            
            if success:
                logger.info(f"✅ Article updated in database")
            
            return success
            
        except Exception as e:
            logger.error(f"Error processing article {article_id}: {e}", exc_info=True)
            return False
    
    def process_batch(self, limit: int = 100, skip_existing: bool = True) -> Dict[str, Any]:
        """
        Process a batch of articles
        
        Args:
            limit: Maximum number of articles to process
            skip_existing: If True, only process articles without AQIS scores
            
        Returns:
            Dict with processing statistics
        """
        try:
            # Get unscored articles
            if skip_existing:
                articles = get_unscored_articles(limit=limit)
                logger.info(f"Found {len(articles)} unscored articles")
            else:
                collection = get_collection("articles_raw")
                articles = list(collection.find().limit(limit))
                logger.info(f"Processing {len(articles)} articles (including already scored)")
            
            if not articles:
                logger.info("No articles to process")
                return {"total": 0, "success": 0, "failed": 0, "eligible": 0}
            
            # Statistics
            stats = {
                "total": 0,
                "success": 0,
                "failed": 0,
                "eligible": 0,
                "skipped": 0,
                "scores": []
            }
            
            # Process each article
            for i, article in enumerate(articles, 1):
                try:
                    article_id = article["_id"]
                    title = article.get("title", "Untitled")[:50]
                    
                    # Validate article
                    if not self._validate_article(article):
                        logger.warning(f"[{i}/{len(articles)}] Skipping {title} - invalid format")
                        stats["skipped"] += 1
                        continue
                    
                    # Score article
                    logger.info(f"[{i}/{len(articles)}] Scoring: {title}...")
                    aqis_result = self.score_article(article)
                    
                    # Update in database
                    success = update_article_aqis(article_id, aqis_result)
                    
                    if success:
                        stats["success"] += 1
                        stats["scores"].append(aqis_result["overall"])
                        
                        if aqis_result["eligible_for_narrative"]:
                            stats["eligible"] += 1
                        
                        # Log progress every 10 articles
                        if i % 10 == 0:
                            avg = sum(stats["scores"]) / len(stats["scores"])
                            logger.info(f"Progress: {i}/{len(articles)} | Avg: {avg:.4f} | Eligible: {stats['eligible']}")
                    else:
                        stats["failed"] += 1
                    
                    stats["total"] += 1
                    
                except Exception as e:
                    logger.error(f"Error processing article: {e}")
                    stats["failed"] += 1
                    continue
            
            # Log summary
            self._log_batch_summary(stats)
            
            return stats
            
        except Exception as e:
            logger.error(f"Error in batch processing: {e}", exc_info=True)
            return {"error": str(e)}
    
    def _validate_article(self, article: Dict) -> bool:
        """
        Validate article has required fields for AQIS
        
        Args:
            article: Article document
            
        Returns:
            bool: True if valid
        """
        required = ["title", "clean_text", "sentence_list", "entities", "primary_entity_id"]
        missing = [f for f in required if f not in article]
        
        if missing:
            logger.warning(f"Article {article.get('_id')} missing fields: {missing}")
            logger.warning("Make sure article has been through cleaning and entity extraction")
            return False
        
        return True
    
    def _display_result(self, article_id: str, article: Dict, aqis_result: Dict):
        """Display scoring result"""
        print("\n" + "="*70)
        print("AQIS SCORING RESULT")
        print("="*70)
        print(f"Article ID: {article_id}")
        print(f"Title: {article.get('title', 'Untitled')[:60]}...")
        print(f"\nOverall Score: {aqis_result['overall']:.4f}")
        print(f"Eligible for Narrative: {'✅ YES' if aqis_result['eligible_for_narrative'] else '❌ NO'}")
        print(f"\nDimension Scores:")
        for dim, score in aqis_result['dimensions'].items():
            bar = "█" * int(score * 20)
            print(f"  {dim:25s}: {score:.4f} {bar}")
        print("="*70 + "\n")
    
    def _log_batch_summary(self, stats: Dict):
        """Log batch processing summary"""
        print("\n" + "="*70)
        print("AQIS BATCH PROCESSING SUMMARY")
        print("="*70)
        print(f"Total Processed: {stats['total']}")
        print(f"Successful: {stats['success']}")
        print(f"Failed: {stats['failed']}")
        print(f"Skipped (invalid): {stats['skipped']}")
        
        if stats['success'] > 0:
            print(f"Eligible for Narrative: {stats['eligible']} ({stats['eligible']/stats['success']*100:.1f}%)")
        
        if stats['scores']:
            avg = sum(stats['scores']) / len(stats['scores'])
            min_score = min(stats['scores'])
            max_score = max(stats['scores'])
            print(f"\nScore Distribution:")
            print(f"  Average: {avg:.4f}")
            print(f"  Range: [{min_score:.4f}, {max_score:.4f}]")
        
        print("="*70 + "\n")
    
    def show_statistics(self):
        """Show AQIS statistics"""
        # Get summary
        summary = get_aqis_summary()
        
        # Get distribution
        distribution = get_aqis_distribution()
        
        print("\n" + "="*70)
        print("AQIS STATISTICS")
        print("="*70)
        print(f"Total Articles: {summary.get('total_articles', 0):,}")
        print(f"Scored Articles: {summary.get('scored_articles', 0):,} ({summary.get('scored_percentage', 0):.1f}%)")
        print(f"Unscored Articles: {summary.get('unscored_articles', 0):,}")
        print(f"Eligible for Narrative: {summary.get('eligible_articles', 0):,} ({summary.get('eligible_percentage', 0):.1f}%)")
        
        if distribution.get('count', 0) > 0:
            print(f"\nScore Distribution:")
            print(f"  Average: {distribution.get('avg_overall', 0):.4f}")
            print(f"  Range: [{distribution.get('min_overall', 0):.4f}, {distribution.get('max_overall', 0):.4f}]")
            
            print(f"\nAverage Dimension Scores:")
            print(f"  Entity Salience: {distribution.get('avg_entity_salience', 0):.4f}")
            print(f"  Informational Density: {distribution.get('avg_informational_density', 0):.4f}")
            print(f"  Specificity: {distribution.get('avg_specificity', 0):.4f}")
            print(f"  Narrative Intent: {distribution.get('avg_narrative_intent', 0):.4f}")
            print(f"  Temporal Grounding: {distribution.get('avg_temporal_grounding', 0):.4f}")
        
        print("="*70 + "\n")


# ============================================================================
# CLI Interface
# ============================================================================

def main():
    """Main CLI entry point"""
    
    parser = argparse.ArgumentParser(description='AQIS Pipeline for Proportion')
    parser.add_argument('--mode', required=True,
                       choices=['single', 'incremental', 'rescore-all', 'stats', 'setup'],
                       help='Processing mode')
    parser.add_argument('--article-id', help='Article ID for single mode')
    parser.add_argument('--limit', type=int, default=100,
                       help='Limit for batch modes (default: 100)')
    
    args = parser.parse_args()
    
    # Execute based on mode
    if args.mode == 'setup':
        logger.info("Creating AQIS indexes...")
        create_aqis_indexes()
        logger.info("✅ Setup complete")
        return
        
    elif args.mode == 'stats':
        pipeline = AQISPipeline()
        pipeline.show_statistics()
        return
    
    # Initialize pipeline for other modes
    pipeline = AQISPipeline()
    
    if args.mode == 'single':
        if not args.article_id:
            logger.error("--article-id required for single mode")
            parser.print_help()
            sys.exit(1)
        pipeline.process_single_article(args.article_id)
        
    elif args.mode == 'incremental':
        logger.info(f"Processing up to {args.limit} unscored articles...")
        pipeline.process_batch(limit=args.limit, skip_existing=True)
        
    elif args.mode == 'rescore-all':
        logger.warning("⚠️  Re-scoring ALL articles!")
        confirm = input("Continue? (yes/no): ")
        if confirm.lower() == 'yes':
            pipeline.process_batch(limit=999999, skip_existing=False)
        else:
            logger.info("Cancelled")


if __name__ == "__main__":
    main()
