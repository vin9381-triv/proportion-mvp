import yaml
from pathlib import Path


# Path to master entity configuration file
# This file acts as the single source of truth for:
# - Which companies are ingested
# - Entity identifiers
# - Tickers and metadata
CONFIG_PATH = Path("config/ticker.yaml")


def load_companies():
    """
    Load company/entity definitions from YAML configuration
    and normalize field names for internal consistency.

    Purpose:
    - Decouple ingestion logic from raw configuration format
    - Provide a stable internal schema to the pipeline
    - Allow YAML structure to evolve without breaking code

    Normalization performed:
    - 'name' (YAML) → 'company_name' (internal)
      This ensures consistent naming across ingestion, storage,
      clustering, and downstream analytics.

    Returns:
        list[dict]: List of normalized company definitions with keys:
            - entity_id
            - ticker
            - company_name
    """

    # Load YAML configuration safely
    with open(CONFIG_PATH, "r") as file:
        data = yaml.safe_load(file)

    # Normalize company entries to internal schema
    companies = []
    for company in data["companies"]:
        companies.append({
            "entity_id": company["entity_id"],
            "ticker": company["ticker"],
            "company_name": company["name"]  # ← Map 'name' to 'company_name'
        })

    return companies


# Simple sanity check for local development
# Allows quick verification of config loading and normalization
if __name__ == "__main__":
    companies = load_companies()
    for c in companies:
        print(c["ticker"], "-", c["company_name"])  # ← Use 'company_name' consistently
