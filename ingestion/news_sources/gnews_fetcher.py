import os
import time
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
# This is where the GNEWS_API_KEY is stored (not hardcoded)
load_dotenv()

# GNews API credentials and endpoint
GNEWS_API_KEY = os.getenv("GNEWS_API_KEY")
BASE_URL = "https://gnews.io/api/v4/search"


def fetch_google_news_articles(
    query: str,
    entity_id: str,
    company_name: str,
    ticker: str,
    max_articles: int = 10,
    language: str = "en",
    sleep_after: int = 1
):
    """
    Fetch recent news articles for a given company from the GNews API.

    Design principles:
    - Uses a licensed, stable news API (no scraping)
    - Safe by default: API errors are caught and never crash the pipeline
    - Idempotent at caller level (duplicates handled downstream)
    - Request count is explicitly tracked for quota awareness

    Args:
        query (str): Search query (currently unused in favor of company_name)
        entity_id (str): Internal entity identifier
        company_name (str): Company name used as GNews query
        ticker (str): Stock ticker (metadata only)
        max_articles (int): Upper bound on articles requested (API caps at 10)
        language (str): Language filter for articles
        sleep_after (int): Seconds to sleep after API call

    Returns:
        tuple:
            - articles (list[dict]): Normalized article metadata
            - request_count (int): Number of API requests used (always 1 here)
    """

    # GNews API query parameters
    # NOTE:
    # We intentionally query using company_name only.
    # Exchange-specific tickers (e.g. ".NS") can cause API errors.
    params = {
        "q": company_name,          # 🔒 SAFE QUERY STRING
        "token": GNEWS_API_KEY,     # API authentication
        "lang": language,
        "max": min(max_articles, 10)  # Free tier hard limit
    }

    try:
        # Perform API request
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

    except requests.exceptions.RequestException as e:
        # API-level failure is non-fatal:
        # - Log the error
        # - Return empty result
        # - Still count the request for quota tracking
        print(f"⚠️ GNews API failed for {company_name}: {e}")
        return [], 1

    articles = []

    # Normalize GNews response into internal article schema
    for a in data.get("articles", []):
        articles.append({
            # Entity metadata
            "entity_id": entity_id,
            "company_name": company_name,
            "ticker": ticker,

            # Source metadata
            "source": "gnews_api",
            "source_type": "api",

            # Article metadata
            "title": a.get("title"),
            "url": a.get("url"),
            "publisher": a.get("source", {}).get("name"),
            "published_at": a.get("publishedAt"),
            "description": a.get("description"),
            "content": a.get("content"),
        })

    # Light throttling to avoid burst API usage
    time.sleep(sleep_after)

    # Always return request_count = 1 (one API call per company)
    return articles, 1
