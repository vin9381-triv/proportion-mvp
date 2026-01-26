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
    - Support multiple entity types (companies, industries, monetary_policy, etc.)

    Normalization performed:
    - 'name' (YAML) → 'entity_name' (internal)
    - Company entities expose 'ticker'
    - All other entities expose 'query_terms'
    - All entities expose 'entity_type'

    Returns:
        list[dict]: List of normalized entity definitions with keys:
            - entity_id
            - entity_type
            - entity_name
            - ticker (optional)
            - query_terms
            - sector (optional)
            - priority (optional)
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
            "priority": company.get("priority", "medium"),
        })

    # ---- Industries (Physical Demand) ----
    for industry in data.get("industries", []):
        entities.append({
            "entity_id": industry["entity_id"],
            "entity_type": industry.get("entity_type", "industry"),  # Allow override
            "entity_name": industry["name"],
            "ticker": None,
            "query_terms": industry["query_terms"],  # multiple semantic queries
            "sector": industry.get("sector"),
            "priority": industry.get("priority", "medium"),
        })

    # ---- Monetary Policy (Phase 1: CRITICAL) ----
    for mp in data.get("monetary_policy", []):
        entities.append({
            "entity_id": mp["entity_id"],
            "entity_type": mp.get("entity_type", "monetary_policy"),
            "entity_name": mp["name"],
            "ticker": None,
            "query_terms": mp["query_terms"],
            "sector": mp.get("sector"),
            "priority": mp.get("priority", "critical"),
        })

    # ---- Macroeconomic - Dollar (Phase 1: CRITICAL) ----
    for macro in data.get("macroeconomic_dollar", []):
        entities.append({
            "entity_id": macro["entity_id"],
            "entity_type": macro.get("entity_type", "currency"),
            "entity_name": macro["name"],
            "ticker": None,
            "query_terms": macro["query_terms"],
            "sector": macro.get("sector"),
            "priority": macro.get("priority", "critical"),
        })

    # ---- Macroeconomic - Inflation (Phase 1: CRITICAL) ----
    for macro in data.get("macroeconomic_inflation", []):
        entities.append({
            "entity_id": macro["entity_id"],
            "entity_type": macro.get("entity_type", "inflation"),
            "entity_name": macro["name"],
            "ticker": None,
            "query_terms": macro["query_terms"],
            "sector": macro.get("sector"),
            "priority": macro.get("priority", "critical"),
        })

    # ---- Future Phase 2+ Categories (Commented for reference) ----
    # When you add Phase 2, uncomment and add similar blocks for:
    # - geopolitical_risk
    # - silver_industrial
    # - mining_supply
    # - market_sentiment

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
    
    print("=" * 60)
    print("LOADED ENTITIES")
    print("=" * 60)
    
    # Group by entity_type for cleaner output
    from collections import defaultdict
    by_type = defaultdict(list)
    
    for e in entities:
        by_type[e["entity_type"]].append(e)
    
    total_queries = 0
    for entity_type in sorted(by_type.keys()):
        entities_of_type = by_type[entity_type]
        type_queries = sum(len(e["query_terms"]) for e in entities_of_type)
        total_queries += type_queries
        
        print(f"\n{entity_type.upper()}: {len(entities_of_type)} entities, {type_queries} queries")
        for e in entities_of_type:
            query_count = len(e["query_terms"])
            priority = e.get("priority", "N/A")
            print(f"  - {e['entity_name']} ({query_count} queries, priority: {priority})")
    
    print("\n" + "=" * 60)
    print(f"TOTAL: {len(entities)} entities, {total_queries} queries")
    print("=" * 60)