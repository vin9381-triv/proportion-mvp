from newspaper import Article
from readability import Document
import requests


# Static headers used for direct publisher requests.
# These help reduce basic bot blocking and improve extraction success
# without resorting to headless browsers.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}


def _extract_with_readability(url: str):
    """
    Fallback article extraction using readability-lxml.

    Purpose:
    - Used when newspaper3k fails or returns insufficient text
    - Focuses on extracting the main readable content block
    - Layout-based, not semantic

    Design notes:
    - This is a best-effort extractor
    - Text cleanup is intentionally minimal (MVP scope)
    - HTML stripping here is naive but sufficient for clustering inputs

    Args:
        url (str): Publisher article URL

    Returns:
        str: Extracted article text

    Raises:
        requests.exceptions.RequestException if HTTP request fails
    """

    # Fetch raw HTML from publisher
    response = requests.get(url, headers=HEADERS, timeout=15)
    response.raise_for_status()

    # Use Readability to isolate main content block
    doc = Document(response.text)
    html = doc.summary(html_partial=True)

    # Very lightweight HTML-to-text cleanup
    # (kept intentionally simple for MVP)
    text = (
        html.replace("<p>", "\n")
            .replace("</p>", "")
            .replace("<br>", "\n")
    )

    return text.strip()


def process_article(url: str):
    """
    Best-effort article content extraction with fallback strategy.

    Extraction strategy (ordered):
    1. Try newspaper3k (semantic-aware, clean when successful)
    2. Fallback to readability-lxml (layout-based extraction)

    Design principles:
    - Never crash ingestion due to extraction failures
    - Return None if usable text cannot be obtained
    - Favor content quality over completeness

    This function is intentionally conservative:
    - Short or empty texts are discarded
    - Downstream pipelines rely on clean, sufficiently long text

    Args:
        url (str): Publisher article URL

    Returns:
        dict | None:
            If successful:
                {
                    "raw_text": str,
                    "summary": str,
                    "language": str
                }
            If extraction fails:
                None
    """

    # ---------- Primary extractor: newspaper3k ----------
    try:
        article = Article(url)
        article.download()
        article.parse()

        # Only accept sufficiently long, non-empty text
        if article.text and len(article.text) >= 500:
            article.nlp()
            return {
                "raw_text": article.text.strip(),
                "summary": article.summary.strip(),
                "language": article.meta_lang or "en",
            }
    except Exception:
        # Fail silently and allow fallback extractor to run
        pass

    # ---------- Fallback extractor: readability-lxml ----------
    try:
        text = _extract_with_readability(url)
        if text and len(text) >= 500:
            return {
                "raw_text": text,
                "summary": "",
                "language": "en",
            }
    except Exception:
        # Final failure: return None to caller
        pass

    # Explicit signal that extraction failed
    return None
