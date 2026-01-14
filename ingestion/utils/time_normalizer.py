import re
from datetime import datetime, timedelta, timezone


def normalize_published_at(published_at_raw: str, ingested_at: datetime):
    """
    Convert relative published_at strings into absolute UTC timestamps.
    Returns None if unparseable.
    """
    if not published_at_raw:
        return None

    value = published_at_raw.strip().lower()

    patterns = [
        (r"(\d+)\s+minutes?\s+ago", lambda n: timedelta(minutes=n)),
        (r"(\d+)\s+hours?\s+ago", lambda n: timedelta(hours=n)),
        (r"(\d+)\s+days?\s+ago", lambda n: timedelta(days=n)),
        (r"(\d+)\s+months?\s+ago", lambda n: timedelta(days=30 * n)),
    ]

    for pattern, delta_fn in patterns:
        match = re.match(pattern, value)
        if match:
            return ingested_at - delta_fn(int(match.group(1)))

    if value == "yesterday":
        return ingested_at - timedelta(days=1)

    try:
        return datetime.fromisoformat(value.replace("z", "+00:00"))
    except Exception:
        return None
