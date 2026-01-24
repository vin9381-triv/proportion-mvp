import yaml
from pathlib import Path


# Path to master entity configuration file
# This file acts as the single source of truth for:
# - Which companies / industries are ingested
# - Entity identifiers
# - Tickers and metadata
CONFIG_PATH = Path("config/ticker.yaml")


def load_entities():
    """
    Load entity definitions from YAML configuration
    and normalize field names for internal consistency.

    Purpose:
    - Decouple ingestion logic from raw configuration format
    - Provide a stable internal schema to the pipeline
    - Allow YAML structure to evolve without breaking code
    - Support multiple entity types (companies, industries, etc.)

    Normalization performed:
    - 'name' (YAML) → 'entity_name' (internal)
    - Company entities expose 'ticker'
    - Industry entities expose 'query_terms'
    - All entities expose 'entity_type'

    Returns:
        list[dict]: List of normalized entity definitions with keys:
            - entity_id
            - entity_type
            - entity_name
            - ticker (optional)
            - query_terms
            - sector (optional)
    """

    # Load YAML configuration safely
    with open(CONFIG_PATH, "r") as file:
        data = yaml.safe_load(file)

    entities = []

    # ---- Companies ----
    for company in data.get("companies", []):
        entities.append({
            "entity_id": company["entity_id"],
            "entity_type": "company",
            "entity_name": company["name"],
            "ticker": company["ticker"],
            "query_terms": [company["name"]],  # single canonical query
            "sector": company.get("sector"),
        })

    # ---- Industries ----
    for industry in data.get("industries", []):
        entities.append({
            "entity_id": industry["entity_id"],
            "entity_type": "industry",
            "entity_name": industry["name"],
            "ticker": None,
            "query_terms": industry["query_terms"],  # multiple semantic queries
            "sector": industry.get("sector"),
        })

    return entities


def load_companies():
    """
    Backward-compatible loader for company-only ingestion.

    NOTE:
    This is retained temporarily to avoid breaking existing
    ingestion logic. Internally, this now derives companies
    from the unified entity loader.

    Returns:
        list[dict]: List of normalized company definitions with keys:
            - entity_id
            - ticker
            - company_name
    """

    entities = load_entities()

    companies = []
    for entity in entities:
        if entity["entity_type"] == "company":
            companies.append({
                "entity_id": entity["entity_id"],
                "ticker": entity["ticker"],
                "company_name": entity["entity_name"],
            })

    return companies


# Simple sanity check for local development
# Allows quick verification of config loading and normalization
if __name__ == "__main__":
    entities = load_entities()
    for e in entities:
        print(e["entity_type"], "-", e["entity_name"])
