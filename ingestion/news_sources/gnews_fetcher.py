import os
import time
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# GNews API credentials and endpoint
GNEWS_API_KEY = os.getenv("GNEWS_API_KEY")
BASE_URL = "https://gnews.io/api/v4/search"


def fetch_google_news_articles(
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
    Fetch recent news articles for a given entity from the GNews API.

    Design principles:
    - Entity-agnostic (company, industry, commodity, etc.)
    - Uses a licensed, stable news API (no scraping)
    - Safe by default: API errors never crash the pipeline
    - Idempotent at caller level (duplicates handled downstream)
    - Request count explicitly tracked for quota awareness

    Args:
        query (str): Search query string (used directly for GNews search)
        entity_id (str): Internal entity identifier
        entity_name (str): Human-readable entity name (metadata)
        ticker (str | None): Stock ticker (companies only)
        entity_type (str): company | industry | commodity
        max_articles (int): Upper bound on articles requested (API caps at 10)
        language (str): Language filter for articles
        sleep_after (int): Seconds to sleep after API call

    Returns:
        tuple:
            - articles (list[dict]): Normalized article metadata
            - request_count (int): Number of API requests used (always 1 here)
    """

    # ---- GNews API query parameters ----
    params = {
        "q": query,                       # 🔑 USE QUERY TERMS (critical for jewellery)
        "token": GNEWS_API_KEY,
        "lang": language,
        "max": min(max_articles, 10),     # Free tier hard limit
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

    except requests.exceptions.RequestException as e:
        print(f"⚠️ GNews API failed for '{query}' ({entity_name}): {e}")
        return [], 1

    articles = []

    # ---- Normalize GNews response ----
    for a in data.get("articles", []):
        articles.append({
            # ---- Entity metadata ----
            "entity_id": entity_id,
            "entity_name": entity_name,
            "entity_type": entity_type,
            "ticker": ticker,

            # ---- Source metadata ----
            "source": "gnews_api",
            "source_type": "api",
            "publisher": a.get("source", {}).get("name"),

            # ---- Article metadata ----
            "title": a.get("title"),
            "url": a.get("url"),
            "published_at": a.get("publishedAt"),
            "description": a.get("description"),
            "content": a.get("content"),
        })

    # Light throttling to avoid burst API usage
    time.sleep(sleep_after)

    # Always return request_count = 1 (one API call per query)
    return articles, 1
