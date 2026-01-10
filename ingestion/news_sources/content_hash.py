import xxhash


def compute_content_hash(text: str) -> str:
    """
    Compute a fast, deterministic 64-bit hash for article content.

    Purpose:
    - Used for near-exact deduplication at ingestion time
    - Identifies syndicated or trivially modified copies of the same article
    - Complements URL-based deduplication

    Why xxHash:
    - Extremely fast (designed for high-throughput systems)
    - Stable and deterministic
    - Lower collision risk than built-in hashes
    - Widely used in production data pipelines

    Notes:
    - This is NOT semantic deduplication
    - Semantic similarity (cosine, clustering) is handled downstream
    - Hash changes if article text meaningfully changes

    Args:
        text (str): Clean article text

    Returns:
        str: Hexadecimal representation of the 64-bit content hash
    """

    # Initialize 64-bit xxHash object
    h = xxhash.xxh64()

    # Update hash state with article text
    h.update(text)

    # Return hex digest for storage and indexing
    return h.hexdigest()
