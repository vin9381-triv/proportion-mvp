import yaml
from pathlib import Path


# Path to tagging config
CONFIG_PATH = Path("config/clustering_tags.yaml")


def load_tag_config():
    """
    Load domain / intent tag configuration from YAML.
    """
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Tag config not found at {CONFIG_PATH.resolve()}"
        )

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if "tags" not in config:
        raise ValueError("Invalid tag config: missing 'tags' key")

    return config["tags"]


# Load once at import time (intentional)
TAG_CONFIG = load_tag_config()


def tag_article(article: dict) -> str:
    """
    Assign a single primary intent/domain tag to an article.

    Tagging rules:
    - Deterministic
    - First match wins
    - Title + body are used
    - Falls back to 'other'
    """

    text = (
        (article.get("title") or "") + " " +
        (article.get("body") or "")
    ).lower()

    for tag, cfg in TAG_CONFIG.items():
        keywords = cfg.get("keywords", [])
        for kw in keywords:
            if kw.lower() in text:
                return tag

    return "other"
