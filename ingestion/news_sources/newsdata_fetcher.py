"""
NewsData.io API fetcher for news ingestion pipeline.

This module provides an alternative/complementary news source to GNews API.
NewsData.io offers 200 requests/day on free tier (vs GNews 100/day).

API Documentation: https://newsdata.io/documentation
Free Tier Limits: 200 requests/day, up to 10 results per request
"""

import os
import time
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# NewsData.io API credentials and endpoint
NEWSDATA_API_KEY = os.getenv("NEWSDATA_API_KEY")
BASE_URL = "https://newsdata.io/api/1/news"


def fetch_newsdata_articles(
    query: str,
    entity_id: str,
    entity_name: str,
    ticker: str | None,
    entity_type: str,
    max_articles: int = 10,
    language: str = "en",
    sleep_after: int = 1,
):
    """
    Fetch recent news articles for a given entity from NewsData.io API.

    Design principles:
    - Entity-agnostic (company, industry, commodity, etc.)
    - Uses licensed API (no scraping)
    - Safe by default: API errors never crash the pipeline
    - Idempotent at caller level (duplicates handled downstream)
    - Request count explicitly tracked for quota awareness

    API Differences from GNews:
    - NewsData.io returns 'results' array instead of 'articles'
    - Field names slightly different (title, link, description, pubDate)
    - Supports more filtering options (category, country, domain)

    Args:
        query (str): Search query string (keyword search)
        entity_id (str): Internal entity identifier
        entity_name (str): Human-readable entity name (metadata)
        ticker (str | None): Stock ticker (companies only)
        entity_type (str): company | industry | monetary_policy | currency | inflation
        max_articles (int): Upper bound on articles requested (API caps at 10 on free tier)
        language (str): Language filter for articles (default: en)
        sleep_after (int): Seconds to sleep after API call

    Returns:
        tuple:
            - articles (list[dict]): Normalized article metadata
            - request_count (int): Number of API requests used (always 1 here)
    """

    # Validate API key
    if not NEWSDATA_API_KEY:
        print("⚠️ NEWSDATA_API_KEY not found in environment variables")
        return [], 1

    # ---- NewsData.io API query parameters ----
    params = {
        "apikey": NEWSDATA_API_KEY,
        "q": query,                        # Query/keyword search
        "language": language,
        "size": min(max_articles, 10),     # Free tier max: 10 results per request
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Check for API errors
        if data.get("status") == "error":
            error_msg = data.get("results", {}).get("message", "Unknown error")
            print(f"⚠️ NewsData.io API error for '{query}': {error_msg}")
            return [], 1

    except requests.exceptions.RequestException as e:
        print(f"⚠️ NewsData.io API failed for '{query}' ({entity_name}): {e}")
        return [], 1

    articles = []

    # ---- Normalize NewsData.io response ----
    # NewsData.io uses 'results' instead of 'articles'
    for a in data.get("results", []):
        # Skip articles without required fields
        if not a.get("title") or not a.get("link"):
            continue

        articles.append({
            # ---- Entity metadata ----
            "entity_id": entity_id,
            "entity_name": entity_name,
            "entity_type": entity_type,
            "ticker": ticker,

            # ---- Source metadata ----
            "source": "newsdata_io",
            "source_type": "api",
            "publisher": a.get("source_id"),  # NewsData.io uses 'source_id'

            # ---- Article metadata ----
            "title": a.get("title"),
            "url": a.get("link"),              # NewsData.io uses 'link' not 'url'
            "published_at": a.get("pubDate"),  # NewsData.io uses 'pubDate'
            "description": a.get("description"),
            "content": a.get("content"),       # May be None for some articles

            # ---- Additional NewsData.io fields (optional) ----
            "category": a.get("category", []),      # List of categories
            "country": a.get("country", []),        # List of countries
            "language": a.get("language"),
            "image_url": a.get("image_url"),        # Featured image
        })

    # Light throttling to avoid burst API usage
    time.sleep(sleep_after)

    # Always return request_count = 1 (one API call per query)
    return articles, 1


def fetch_newsdata_articles_by_category(
    entity_id: str,
    entity_name: str,
    ticker: str | None,
    entity_type: str,
    category: str = "business",
    max_articles: int = 10,
    language: str = "en",
    sleep_after: int = 1,
):
    """
    Fetch articles by category instead of keyword query.
    
    Useful for broad market coverage when you want all business/finance news
    rather than specific keyword matching.
    
    Available categories:
    - business
    - top (general news)
    - technology
    - sports
    - science
    - health
    - entertainment
    
    Args:
        entity_id (str): Internal entity identifier
        entity_name (str): Human-readable entity name
        ticker (str | None): Stock ticker (companies only)
        entity_type (str): Entity type
        category (str): NewsData.io category
        max_articles (int): Max articles to fetch
        language (str): Language code
        sleep_after (int): Sleep duration after request
        
    Returns:
        tuple: (articles, request_count)
    """
    
    if not NEWSDATA_API_KEY:
        print("⚠️ NEWSDATA_API_KEY not found in environment variables")
        return [], 1
    
    params = {
        "apikey": NEWSDATA_API_KEY,
        "category": category,
        "language": language,
        "size": min(max_articles, 10),
    }
    
    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data.get("status") == "error":
            error_msg = data.get("results", {}).get("message", "Unknown error")
            print(f"⚠️ NewsData.io API error for category '{category}': {error_msg}")
            return [], 1
            
    except requests.exceptions.RequestException as e:
        print(f"⚠️ NewsData.io API failed for category '{category}': {e}")
        return [], 1
    
    articles = []
    
    for a in data.get("results", []):
        if not a.get("title") or not a.get("link"):
            continue
            
        articles.append({
            "entity_id": entity_id,
            "entity_name": entity_name,
            "entity_type": entity_type,
            "ticker": ticker,
            "source": "newsdata_io",
            "source_type": "api",
            "publisher": a.get("source_id"),
            "title": a.get("title"),
            "url": a.get("link"),
            "published_at": a.get("pubDate"),
            "description": a.get("description"),
            "content": a.get("content"),
            "category": a.get("category", []),
            "country": a.get("country", []),
            "language": a.get("language"),
            "image_url": a.get("image_url"),
        })
    
    time.sleep(sleep_after)
    return articles, 1


# Simple test function
if __name__ == "__main__":
    print("Testing NewsData.io API...")
    
    # Test with a gold-related query
    articles, count = fetch_newsdata_articles(
        query="Federal Reserve gold",
        entity_id="test_001",
        entity_name="Test Entity",
        ticker=None,
        entity_type="monetary_policy",
        max_articles=5,
    )
    
    print(f"\n✅ Fetched {len(articles)} articles")
    print(f"📊 API requests: {count}")
    
    if articles:
        print("\nSample article:")
        print(f"Title: {articles[0]['title']}")
        print(f"URL: {articles[0]['url']}")
        print(f"Published: {articles[0]['published_at']}")
        print(f"Publisher: {articles[0]['publisher']}")