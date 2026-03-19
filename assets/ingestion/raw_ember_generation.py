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
      - name: not_null
  - name: year
    checks:
      - name: not_null
  - name: variable
    checks:
      - name: not_null
  - name: value
    checks:
      - name: not_null
@bruin """

import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

EMBER_CSV_URL = (
    "https://files.ember-energy.org/public-downloads/"
    "yearly_full_release_long_format.csv"
)

COUNTRY_CODE = "KEN"


def materialize() -> pd.DataFrame:
    logger.info(f"Fetching Ember yearly data from: {EMBER_CSV_URL}")

    df = pd.read_csv(EMBER_CSV_URL, low_memory=False)
    logger.info(f"Raw shape: {df.shape}")

    # Normalise column names to snake_case before filtering
    df.columns = (
        df.columns
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )

    # Determine which column holds the ISO-3 country code (varies by release)
    iso_col = next(
        (c for c in df.columns if c in ("iso_3_code", "country_code")),
        None
    )
    if iso_col is None:
        raise KeyError(f"Cannot find ISO-3 code column. Available: {list(df.columns)}")

    # Filter to Kenya only
    df = df[df[iso_col] == COUNTRY_CODE].copy()
    logger.info(f"Kenya rows: {len(df)}")

    # Normalise the ISO-3 column to a consistent name
    if iso_col != "country_code":
        df = df.rename(columns={iso_col: "country_code"})

    # Keep relevant columns (schema varies across releases; select what exists)
    keep_cols = [
        c for c in [
            "country_code", "area", "country", "year", "category", "subcategory",
            "variable", "unit", "value", "source", "source_name", "ember_region"
        ]
        if c in df.columns
    ]
    df = df[keep_cols]

    # Drop rows with no value (unreported entries in source)
    df = df.dropna(subset=["value"])

    # Add ingestion metadata
    df["_ingested_at"] = pd.Timestamp.now("UTC")
    df["_source"] = "ember_yearly_csv"

    logger.info(f"Returning {len(df)} rows")
    return df
