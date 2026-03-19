""" @bruin
name: ingestion.raw_ember_generation
type: python
connection: duckdb-default
materialization:
  type: table
description: >
  Fetches Ember Climate yearly electricity data (generation, capacity,
  emissions, demand) for Kenya from the public CSV release.
  Source: https://ember-energy.org/data/yearly-electricity-data/
  License: CC BY 4.0
columns:
  - name: country_code
    checks:
      - not_null
  - name: year
    checks:
      - not_null
  - name: variable
    checks:
      - not_null
  - name: value
    checks:
      - not_null
@bruin """

import pandas as pd
import duckdb
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

EMBER_CSV_URL = (
    "https://ember-energy.org/app/uploads/2022/07/"
    "yearly_full_release_long_format.csv"
)

COUNTRY_CODE = "KEN"

logger.info(f"Fetching Ember yearly data from: {EMBER_CSV_URL}")

df = pd.read_csv(EMBER_CSV_URL, low_memory=False)
logger.info(f"Raw shape: {df.shape}")

# Filter to Kenya only
df = df[df["Country code"] == COUNTRY_CODE].copy()
logger.info(f"Kenya rows: {len(df)}")

# Normalise column names to snake_case
df.columns = (
    df.columns
    .str.lower()
    .str.replace(" ", "_")
    .str.replace(r"[^a-z0-9_]", "", regex=True)
)

# Keep relevant columns (schema varies across releases; select what exists)
keep_cols = [
    c for c in [
        "country_code", "country", "year", "category", "subcategory",
        "variable", "unit", "value", "source", "source_name", "ember_region"
    ]
    if c in df.columns
]
df = df[keep_cols]

# Add ingestion metadata
df["_ingested_at"] = pd.Timestamp.utcnow()
df["_source"] = "ember_yearly_csv"

# Write to DuckDB
conn = duckdb.connect("kenya_energy.db")
conn.execute("CREATE SCHEMA IF NOT EXISTS ingestion")
conn.execute(
    "CREATE OR REPLACE TABLE ingestion.raw_ember_generation AS SELECT * FROM df"
)
row_count = conn.execute(
    "SELECT COUNT(*) FROM ingestion.raw_ember_generation"
).fetchone()[0]
conn.close()

logger.info(
    f"Wrote {row_count} rows to ingestion.raw_ember_generation"
)
