"""
Stance Detection Module - Local Models Only
==========================================

Components:
- ClusterSummarizer: Generate summaries (microsoft/phi-2)
- ClusterStanceResolver: Determine stance (rules + BERT MNLI)
- StanceMongoDBWriter: Save to MongoDB
"""

from processing.stance_detection.cluster_summarizer import ClusterSummarizer
from processing.stance_detection.cluster_stance_resolver import ClusterStanceResolver
from processing.stance_detection.stance_mongo_writer import StanceMongoDBWriter

__all__ = [
    "ClusterSummarizer",
    "ClusterStanceResolver",
    "StanceMongoDBWriter",
]