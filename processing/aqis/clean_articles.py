"""
IMPROVED Article Cleaning Script
=================================
Fixes the low Entity Salience issue by:
1. Better entity extraction from titles
2. Proper primary entity detection
3. Ensuring entity appears in clean_text

Based on analysis showing Entity Salience averaging only 0.12
(should be 0.50+)

Location: processing/aqis/clean_articles_v2.py

Usage:
    python -m processing.aqis.clean_articles_v2 --mode incremental --limit 100
"""

import re
import argparse
import logging
from typing import Dict, List, Any, Optional
from bson import ObjectId
from datetime import datetime

from processing.common.mongo_client import get_collection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Improved Entity Extractor
# ============================================================================

class ImprovedEntityExtractor:
    """Better entity extraction from titles and text"""
    
    # Common tech companies and products
    KNOWN_ENTITIES = {
        'apple', 'google', 'microsoft', 'amazon', 'meta', 'facebook',
        'tesla', 'nvidia', 'alphabet', 'openai', 'anthropic',
        'spacex', 'twitter', 'x', 'instagram', 'youtube',
        'android', 'ios', 'windows', 'chrome', 'safari',
        'chatgpt', 'gemini', 'claude', 'grok'
    }
    
    def extract_from_title(self, title: str) -> List[Dict]:
        """
        Extract entities from title with high confidence
        Title entities are most likely to be primary
        """
        entities = []
        title_lower = title.lower()
        
        # Check for known entities in title
        for known_entity in self.KNOWN_ENTITIES:
            if known_entity in title_lower:
                # Find actual case in title
                pattern = re.compile(re.escape(known_entity), re.IGNORECASE)
                match = pattern.search(title)
                if match:
                    actual_name = match.group(0)
                    entities.append({
                        "entity_id": f"company_{known_entity}",
                        "entity_name": actual_name.capitalize(),
                        "entity_type": "company",
                        "confidence": "high",
                        "source": "title"
                    })
        
        # Extract capitalized words (likely companies/products)
        # Match sequences like "Apple Inc", "Tesla Motors", "Meta Platforms"
        cap_patterns = [
            r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})\b',  # Apple, Tesla Motors, Meta Platforms
            r'\b([A-Z]{2,})\b'  # NASA, IBM, AI
        ]
        
        for pattern in cap_patterns:
            for match in re.finditer(pattern, title):
                name = match.group(1)
                if len(name) > 2 and name.lower() not in ['the', 'and', 'for', 'with']:
                    entity_id = f"extracted_{name.lower().replace(' ', '_')}"
                    
                    # Don't duplicate if already found
                    if not any(e['entity_id'] == entity_id for e in entities):
                        entities.append({
                            "entity_id": entity_id,
                            "entity_name": name,
                            "entity_type": "company",
                            "confidence": "medium",
                            "source": "title"
                        })
        
        return entities
    
    def extract_from_text(self, text: str, max_entities: int = 5) -> List[Dict]:
        """Extract entities from text body"""
        entities = []
        text_lower = text.lower()
        
        # Check for known entities in text
        for known_entity in self.KNOWN_ENTITIES:
            if known_entity in text_lower:
                entities.append({
                    "entity_id": f"company_{known_entity}",
                    "entity_name": known_entity.capitalize(),
                    "entity_type": "company",
                    "confidence": "medium",
                    "source": "text"
                })
        
        # Limit to avoid too many entities
        return entities[:max_entities]
    
    def get_primary_entity(self, title_entities: List[Dict], text_entities: List[Dict]) -> str:
        """
        Determine primary entity
        Priority: Title entities > Known entities > First entity
        """
        # Prefer title entities (highest confidence)
        if title_entities:
            # Prefer known entities from title
            for entity in title_entities:
                if entity['confidence'] == 'high':
                    return entity['entity_id']
            # Otherwise first title entity
            return title_entities[0]['entity_id']
        
        # Fallback to text entities
        if text_entities:
            return text_entities[0]['entity_id']
        
        # Last resort
        return "unknown_entity"


# ============================================================================
# Improved Article Cleaner
# ============================================================================

class ImprovedArticleCleaner:
    """Enhanced article cleaner with better entity extraction"""
    
    def __init__(self):
        self.entity_extractor = ImprovedEntityExtractor()
        self.stats = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "too_short": 0,
            "no_entities": 0
        }
    
    def clean_article(self, article: Dict) -> Optional[Dict]:
        """Clean article with improved entity extraction"""
        try:
            raw_text = article.get("raw_text", "")
            title = article.get("title", "")
            
            if not raw_text or not title:
                logger.warning(f"Missing raw_text or title")
                return None
            
            # Clean text
            clean_text = self._clean_text(raw_text)
            
            if len(clean_text) < 50:
                logger.warning(f"Text too short after cleaning")
                self.stats["too_short"] += 1
                return None
            
            # Split sentences
            sentences = self._split_sentences(clean_text)
            
            if len(sentences) < 2:
                logger.warning(f"Too few sentences")
                self.stats["too_short"] += 1
                return None
            
            # IMPROVED: Extract entities from title first (most reliable)
            title_entities = self.entity_extractor.extract_from_title(title)
            text_entities = self.entity_extractor.extract_from_text(clean_text)
            
            # Merge entities (remove duplicates)
            all_entities = title_entities.copy()
            for entity in text_entities:
                if not any(e['entity_id'] == entity['entity_id'] for e in all_entities):
                    all_entities.append(entity)
            
            if not all_entities:
                logger.warning(f"No entities extracted from: {title[:50]}")
                self.stats["no_entities"] += 1
                # Create a fallback entity from first word of title
                first_word = title.split()[0] if title.split() else "Unknown"
                all_entities = [{
                    "entity_id": f"fallback_{first_word.lower()}",
                    "entity_name": first_word,
                    "entity_type": "unknown",
                    "confidence": "low",
                    "source": "fallback"
                }]
            
            # Get primary entity
            primary_entity_id = self.entity_extractor.get_primary_entity(
                title_entities, text_entities
            )
            
            # Clean up entity list for storage (remove internal fields)
            clean_entities = [
                {
                    "entity_id": e["entity_id"],
                    "entity_name": e["entity_name"],
                    "entity_type": e["entity_type"]
                }
                for e in all_entities
            ]
            
            cleaned_data = {
                "clean_text": clean_text,
                "sentence_list": sentences,
                "entities": clean_entities,
                "primary_entity_id": primary_entity_id,
                "cleaned_at": datetime.utcnow(),
                "cleaner_version": "v2_improved"
            }
            
            return cleaned_data
            
        except Exception as e:
            logger.error(f"Error cleaning article: {e}", exc_info=True)
            return None
    
    def _clean_text(self, raw_text: str) -> str:
        """Clean raw text"""
        text = raw_text
        
        # Remove prices
        text = re.sub(r'\$\d+\.?\d*\+?', '', text)
        text = re.sub(r'Reg\.\s*\$\d+', '', text)
        text = re.sub(r'\(\s*Reg\.[^)]*\)', '', text)
        
        # Remove promotional phrases
        promo_phrases = [
            r'Today\'s lineup.*?below',
            r'More.*?deals.*?:',
            r'Save up to.*?%',
            r'from \$\d+ to \$\d+',
        ]
        for phrase in promo_phrases:
            text = re.sub(phrase, '', text, flags=re.IGNORECASE | re.DOTALL)
        
        # Clean URLs
        text = re.sub(r'https?://\S+', '', text)
        
        # Remove excessive punctuation
        text = re.sub(r'[!]{2,}', '!', text)
        text = re.sub(r'[?]{2,}', '?', text)
        
        # Clean whitespace
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        
        return text
    
    def _split_sentences(self, text: str) -> List[str]:
        """Split into sentences"""
        # Split on sentence boundaries
        sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', text)
        
        # Clean and filter
        sentences = [s.strip() for s in sentences if s.strip()]
        sentences = [s for s in sentences if len(s) > 10]
        
        return sentences


# ============================================================================
# Processing Pipeline
# ============================================================================

class ImprovedCleaningPipeline:
    """Improved cleaning pipeline"""
    
    def __init__(self):
        self.cleaner = ImprovedArticleCleaner()
        self.collection = get_collection("articles_raw")
    
    def process_single(self, article_id: str) -> bool:
        """Clean single article"""
        try:
            oid = ObjectId(article_id)
            article = self.collection.find_one({"_id": oid})
            
            if not article:
                logger.error(f"Article not found: {article_id}")
                return False
            
            logger.info(f"Cleaning: {article.get('title', 'Untitled')[:50]}")
            
            cleaned_data = self.cleaner.clean_article(article)
            
            if not cleaned_data:
                logger.error("Cleaning failed")
                return False
            
            # Show what was extracted
            logger.info(f"Extracted {len(cleaned_data['entities'])} entities")
            logger.info(f"Primary entity: {cleaned_data['primary_entity_id']}")
            
            # Update database
            result = self.collection.update_one(
                {"_id": oid},
                {"$set": cleaned_data}
            )
            
            if result.modified_count > 0:
                logger.info("✅ Article cleaned and updated")
                return True
            else:
                logger.error("Failed to update")
                return False
            
        except Exception as e:
            logger.error(f"Error: {e}", exc_info=True)
            return False
    
    def process_batch(self, limit: int = 100, skip_existing: bool = True) -> Dict:
        """Clean batch of articles"""
        try:
            # Find articles to clean
            query = {}
            if skip_existing:
                # Re-clean articles with old cleaner or no cleaning
                query = {
                    "$or": [
                        {"clean_text": {"$exists": False}},
                        {"cleaner_version": {"$exists": False}},
                        {"cleaner_version": {"$ne": "v2_improved"}}
                    ]
                }
            
            articles = list(self.collection.find(query).limit(limit))
            
            logger.info(f"Found {len(articles)} articles to clean")
            
            if not articles:
                return {"total": 0, "success": 0, "failed": 0}
            
            stats = {"total": 0, "success": 0, "failed": 0}
            
            for i, article in enumerate(articles, 1):
                try:
                    article_id = article["_id"]
                    title = article.get("title", "Untitled")[:50]
                    
                    logger.info(f"[{i}/{len(articles)}] Cleaning: {title}")
                    
                    cleaned_data = self.cleaner.clean_article(article)
                    
                    if not cleaned_data:
                        stats["failed"] += 1
                        continue
                    
                    # Update database
                    result = self.collection.update_one(
                        {"_id": article_id},
                        {"$set": cleaned_data}
                    )
                    
                    if result.modified_count > 0:
                        stats["success"] += 1
                    else:
                        stats["failed"] += 1
                    
                    stats["total"] += 1
                    
                    if i % 10 == 0:
                        logger.info(f"Progress: {i}/{len(articles)} | Success: {stats['success']}")
                    
                except Exception as e:
                    logger.error(f"Error: {e}")
                    stats["failed"] += 1
            
            self._log_summary(stats)
            return stats
            
        except Exception as e:
            logger.error(f"Error: {e}", exc_info=True)
            return {"error": str(e)}
    
    def show_stats(self):
        """Show statistics"""
        total = self.collection.count_documents({})
        v2_cleaned = self.collection.count_documents({"cleaner_version": "v2_improved"})
        old_cleaned = self.collection.count_documents({
            "clean_text": {"$exists": True},
            "cleaner_version": {"$ne": "v2_improved"}
        })
        uncleaned = total - v2_cleaned - old_cleaned
        
        print("\n" + "="*70)
        print("IMPROVED CLEANING STATISTICS")
        print("="*70)
        print(f"Total Articles: {total:,}")
        print(f"V2 Cleaned (improved): {v2_cleaned:,}")
        print(f"Old Cleaned: {old_cleaned:,}")
        print(f"Uncleaned: {uncleaned:,}")
        print(f"\nNeed re-cleaning: {old_cleaned + uncleaned:,}")
        print("="*70 + "\n")
    
    def _log_summary(self, stats: Dict):
        """Log summary"""
        print("\n" + "="*70)
        print("CLEANING SUMMARY")
        print("="*70)
        print(f"Total: {stats['total']}")
        print(f"Success: {stats['success']}")
        print(f"Failed: {stats['failed']}")
        
        if self.cleaner.stats['no_entities'] > 0:
            print(f"\nWarnings:")
            print(f"  No entities extracted: {self.cleaner.stats['no_entities']}")
            print(f"  Too short: {self.cleaner.stats['too_short']}")
        
        print("="*70 + "\n")


def main():
    parser = argparse.ArgumentParser(description='Improved Article Cleaning')
    parser.add_argument('--mode', required=True,
                       choices=['single', 'incremental', 'reclean-all', 'stats'],
                       help='Processing mode')
    parser.add_argument('--article-id', help='Article ID for single mode')
    parser.add_argument('--limit', type=int, default=100,
                       help='Limit for batch modes')
    
    args = parser.parse_args()
    
    pipeline = ImprovedCleaningPipeline()
    
    if args.mode == 'stats':
        pipeline.show_stats()
    
    elif args.mode == 'single':
        if not args.article_id:
            logger.error("--article-id required for single mode")
            return
        pipeline.process_single(args.article_id)
    
    elif args.mode == 'incremental':
        logger.info(f"Cleaning up to {args.limit} articles...")
        pipeline.process_batch(limit=args.limit, skip_existing=True)
    
    elif args.mode == 'reclean-all':
        logger.warning("⚠️  Re-cleaning ALL articles with improved extractor!")
        confirm = input("Continue? (yes/no): ")
        if confirm.lower() == 'yes':
            pipeline.process_batch(limit=999999, skip_existing=False)
        else:
            logger.info("Cancelled")


if __name__ == "__main__":
    main()