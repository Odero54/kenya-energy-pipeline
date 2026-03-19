""" @bruin
name: ingestion.raw_owid_energy
type: python
connection: duckdb-default
materialization:
  type: table
description: >
  Fetches Our World in Data (OWID) energy dataset for Kenya from the
  public GitHub CSV. Contains 130+ indicators: per-capita electricity,
  carbon intensity, primary energy by source, renewable shares, etc.
  Source: https://github.com/owid/energy-data
  License: CC BY 4.0
depends:
  - ingestion.raw_ember_generation
columns:
  - name: iso_code
    checks:
      - name: not_null
  - name: year
    checks:
      - name: not_null
@bruin """

import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

OWID_URL = (
    "https://raw.githubusercontent.com/owid/energy-data/master/"
    "owid-energy-data.csv"
)

COUNTRY_ISO = "KEN"


def materialize() -> pd.DataFrame:
    logger.info(f"Fetching OWID energy data from: {OWID_URL}")

    df = pd.read_csv(OWID_URL, low_memory=False)
    logger.info(f"Raw shape: {df.shape}")

    # Filter to Kenya
    df = df[df["iso_code"] == COUNTRY_ISO].copy()
    logger.info(f"Kenya rows: {len(df)}")

    # Drop columns that are entirely null for Kenya
    df = df.dropna(axis=1, how="all")

    # Ensure year is integer
    df["year"] = df["year"].astype(int)

    # Add ingestion metadata
    df["_ingested_at"] = pd.Timestamp.now("UTC")
    df["_source"] = "owid_energy_github"

    logger.info(f"Returning {len(df)} rows")
    return df
