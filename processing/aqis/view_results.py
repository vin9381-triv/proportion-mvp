"""
AQIS Results Viewer
===================
View which articles passed/failed and why.

Usage:
    # View top scoring articles
    python -m processing.aqis.view_results --mode top --limit 20
    
    # View failing articles
    python -m processing.aqis.view_results --mode failed --limit 20
    
    # View borderline articles (around threshold)
    python -m processing.aqis.view_results --mode borderline --limit 20
    
    # Analyze specific article
    python -m processing.aqis.view_results --mode single --article-id ARTICLE_ID
    
    # Export to CSV
    python -m processing.aqis.view_results --mode export --output results.csv
"""

import argparse
import logging
import csv
from typing import Dict, List
from bson import ObjectId

from processing.common.mongo_client import get_collection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AQISViewer:
    """View and analyze AQIS results"""
    
    def __init__(self):
        self.collection = get_collection("articles_raw")
    
    def view_top_scoring(self, limit: int = 20):
        """Show highest scoring articles"""
        articles = self.collection.find(
            {"aqis": {"$exists": True}},
            {"title": 1, "aqis": 1, "_id": 1}
        ).sort("aqis.overall", -1).limit(limit)
        
        print("\n" + "="*100)
        print(f"TOP {limit} SCORING ARTICLES")
        print("="*100)
        
        for i, article in enumerate(articles, 1):
            self._display_article(i, article)
        
        print("="*100 + "\n")
    
    def view_failed(self, limit: int = 20):
        """Show lowest scoring articles"""
        articles = self.collection.find(
            {"aqis": {"$exists": True}},
            {"title": 1, "aqis": 1, "_id": 1}
        ).sort("aqis.overall", 1).limit(limit)
        
        print("\n" + "="*100)
        print(f"LOWEST {limit} SCORING ARTICLES (FAILED)")
        print("="*100)
        
        for i, article in enumerate(articles, 1):
            self._display_article(i, article)
        
        print("="*100 + "\n")
    
    def view_borderline(self, limit: int = 20, threshold: float = 0.50):
        """Show articles near the threshold"""
        # Get articles within 0.1 of threshold
        articles = self.collection.find(
            {
                "aqis.overall": {
                    "$gte": threshold - 0.1,
                    "$lte": threshold + 0.1
                }
            },
            {"title": 1, "aqis": 1, "_id": 1}
        ).sort("aqis.overall", -1).limit(limit)
        
        print("\n" + "="*100)
        print(f"BORDERLINE ARTICLES (around threshold {threshold})")
        print("="*100)
        
        for i, article in enumerate(articles, 1):
            self._display_article(i, article)
        
        print("="*100 + "\n")
    
    def view_single(self, article_id: str):
        """Detailed view of single article"""
        article = self.collection.find_one({"_id": ObjectId(article_id)})
        
        if not article:
            logger.error(f"Article not found: {article_id}")
            return
        
        if "aqis" not in article:
            logger.error(f"Article not scored yet: {article_id}")
            return
        
        print("\n" + "="*100)
        print("DETAILED AQIS ANALYSIS")
        print("="*100)
        
        print(f"\nArticle ID: {article_id}")
        print(f"Title: {article.get('title', 'Untitled')}")
        
        aqis = article["aqis"]
        
        print(f"\n{'='*50}")
        print(f"Overall Score: {aqis['overall']:.4f}")
        print(f"Eligible: {'✅ YES' if aqis['eligible_for_narrative'] else '❌ NO'}")
        print(f"Threshold: 0.50 (need >= to pass)")
        print(f"{'='*50}")
        
        print(f"\nDimension Breakdown:")
        dims = aqis['dimensions']
        
        # Sort by score to see weakest
        sorted_dims = sorted(dims.items(), key=lambda x: x[1])
        
        for dim, score in sorted_dims:
            bar = "█" * int(score * 30)
            status = "✅" if score >= 0.5 else "❌"
            print(f"  {status} {dim:25s}: {score:.4f} {bar}")
        
        print(f"\n💡 Analysis:")
        
        # Find weakest dimension
        weakest_dim, weakest_score = sorted_dims[0]
        print(f"  Weakest: {weakest_dim} ({weakest_score:.4f})")
        
        if weakest_score < 0.3:
            print(f"  ⚠️  CRITICAL: {weakest_dim} is very low - this is dragging down the score")
        
        # Suggest improvements
        if weakest_dim == "entity_salience":
            print(f"  💡 Suggestion: Entity not prominent in title/first paragraph")
        elif weakest_dim == "informational_density":
            print(f"  💡 Suggestion: Needs more factual content, event verbs")
        elif weakest_dim == "specificity":
            print(f"  💡 Suggestion: Needs numbers, dates, concrete details")
        elif weakest_dim == "narrative_intent":
            print(f"  💡 Suggestion: Too promotional or opinion-based")
        elif weakest_dim == "temporal_grounding":
            print(f"  💡 Suggestion: Needs time references (dates, 'today', etc.)")
        
        print("="*100 + "\n")
    
    def export_to_csv(self, output_file: str):
        """Export all results to CSV"""
        articles = self.collection.find(
            {"aqis": {"$exists": True}},
            {"title": 1, "aqis": 1, "_id": 1}
        )
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Header
            writer.writerow([
                "ID", "Title", "Overall", "Eligible",
                "Entity Salience", "Info Density", "Specificity",
                "Narrative Intent", "Temporal Grounding"
            ])
            
            count = 0
            for article in articles:
                aqis = article["aqis"]
                dims = aqis["dimensions"]
                
                writer.writerow([
                    str(article["_id"]),
                    article.get("title", "")[:100],
                    aqis["overall"],
                    "Yes" if aqis["eligible_for_narrative"] else "No",
                    dims["entity_salience"],
                    dims["informational_density"],
                    dims["specificity"],
                    dims["narrative_intent"],
                    dims["temporal_grounding"]
                ])
                count += 1
        
        print(f"\n✅ Exported {count} articles to {output_file}\n")
    
    def show_statistics_breakdown(self):
        """Show detailed statistics"""
        # Get all scored articles
        all_articles = list(self.collection.find(
            {"aqis": {"$exists": True}},
            {"aqis": 1}
        ))
        
        if not all_articles:
            print("No scored articles found")
            return
        
        total = len(all_articles)
        eligible = sum(1 for a in all_articles if a["aqis"]["eligible_for_narrative"])
        
        print("\n" + "="*100)
        print("DETAILED STATISTICS BREAKDOWN")
        print("="*100)
        
        print(f"\nTotal Scored: {total:,}")
        print(f"Eligible: {eligible:,} ({eligible/total*100:.1f}%)")
        print(f"Not Eligible: {total-eligible:,} ({(total-eligible)/total*100:.1f}%)")
        
        # Score distribution
        print(f"\nScore Distribution:")
        ranges = [
            ("0.00-0.20", 0.0, 0.20),
            ("0.20-0.40", 0.20, 0.40),
            ("0.40-0.50", 0.40, 0.50),  # Just below threshold
            ("0.50-0.60", 0.50, 0.60),  # Just above threshold
            ("0.60-0.80", 0.60, 0.80),
            ("0.80-1.00", 0.80, 1.00)
        ]
        
        for label, min_score, max_score in ranges:
            count = sum(1 for a in all_articles 
                       if min_score <= a["aqis"]["overall"] < max_score)
            pct = count/total*100
            bar = "█" * int(pct/2)
            print(f"  {label}: {count:5d} ({pct:5.1f}%) {bar}")
        
        # Dimension averages
        print(f"\nAverage Dimension Scores:")
        dim_names = ["entity_salience", "informational_density", "specificity", 
                     "narrative_intent", "temporal_grounding"]
        
        for dim in dim_names:
            avg = sum(a["aqis"]["dimensions"][dim] for a in all_articles) / total
            bar = "█" * int(avg * 30)
            print(f"  {dim:25s}: {avg:.4f} {bar}")
        
        print("="*100 + "\n")
    
    def _display_article(self, index: int, article: Dict):
        """Display article in list view"""
        aqis = article["aqis"]
        title = article.get("title", "Untitled")[:70]
        
        score = aqis["overall"]
        eligible = "✅" if aqis["eligible_for_narrative"] else "❌"
        
        # Find weakest dimension
        dims = aqis["dimensions"]
        weakest = min(dims.items(), key=lambda x: x[1])
        
        print(f"\n{index}. {eligible} Score: {score:.4f} | Weakest: {weakest[0]} ({weakest[1]:.2f})")
        print(f"   Title: {title}")
        print(f"   ID: {article['_id']}")


def main():
    parser = argparse.ArgumentParser(description='View AQIS Results')
    parser.add_argument('--mode', required=True,
                       choices=['top', 'failed', 'borderline', 'single', 'export', 'stats'],
                       help='Viewing mode')
    parser.add_argument('--limit', type=int, default=20,
                       help='Number of articles to show')
    parser.add_argument('--article-id', help='Article ID for single mode')
    parser.add_argument('--output', default='aqis_results.csv',
                       help='Output file for export mode')
    parser.add_argument('--threshold', type=float, default=0.50,
                       help='Threshold for borderline mode')
    
    args = parser.parse_args()
    
    viewer = AQISViewer()
    
    if args.mode == 'top':
        viewer.view_top_scoring(args.limit)
    elif args.mode == 'failed':
        viewer.view_failed(args.limit)
    elif args.mode == 'borderline':
        viewer.view_borderline(args.limit, args.threshold)
    elif args.mode == 'single':
        if not args.article_id:
            logger.error("--article-id required for single mode")
            return
        viewer.view_single(args.article_id)
    elif args.mode == 'export':
        viewer.export_to_csv(args.output)
    elif args.mode == 'stats':
        viewer.show_statistics_breakdown()


if __name__ == "__main__":
    main()