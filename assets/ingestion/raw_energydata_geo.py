""" @bruin
name: ingestion.raw_energydata_geo
type: python
connection: duckdb-default
materialization:
  type: table
description: >
  Downloads Kenya energy geospatial datasets from EnergyData.info (World Bank).
  Fetches:
    1. Kenya Electricity Transmission Network (KPLC lines)
    2. Kenya Transmission Stations (substations)
    3. Solar Radiation Measurement Data (ground stations: Laisamis, Narok, Homa Bay)
  All datasets are stored as tabular records in DuckDB for joining with
  the statistical pipeline. Spatial geometries stored as WKT strings.
  Source: https://energydata.info/dataset?vocab_country_names=KEN
  License: CC BY 4.0
depends:
  - ingestion.raw_ember_generation
@bruin """

import pandas as pd
import duckdb
import requests
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# EnergyData.info CKAN API endpoints
# ---------------------------------------------------------------------------
ENERGYDATA_API = "https://energydata.info/api/3/action"

DATASETS = {
    "transmission_network": "kenya-kenya-electricity-network",
    "transmission_stations": "kenya-transmission-stations",
    "solar_radiation":       "kenya-solar-radiation-measurement-data",
}


def fetch_ckan_resources(dataset_id: str) -> list[dict]:
    """Return list of resource metadata for a CKAN dataset."""
    url = f"{ENERGYDATA_API}/package_show"
    resp = requests.get(url, params={"id": dataset_id}, timeout=30)
    resp.raise_for_status()
    result = resp.json()
    if not result.get("success"):
        raise ValueError(f"CKAN API returned success=false for {dataset_id}")
    return result["result"]["resources"]


def geojson_to_df(geojson: dict, dataset_name: str) -> pd.DataFrame:
    """Flatten a GeoJSON FeatureCollection to a DataFrame."""
    features = geojson.get("features", [])
    rows = []
    for feat in features:
        props = feat.get("properties", {}) or {}
        geom  = feat.get("geometry", {}) or {}
        row = {k: v for k, v in props.items()}
        row["geometry_type"] = geom.get("type")
        # Store geometry as JSON string for DuckDB compatibility
        row["geometry_wkt"] = json.dumps(geom.get("coordinates"))
        row["dataset"] = dataset_name
        rows.append(row)
    return pd.DataFrame(rows)


def fetch_geojson_resource(url: str, dataset_name: str) -> pd.DataFrame:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    geojson = resp.json()
    return geojson_to_df(geojson, dataset_name)


# ---------------------------------------------------------------------------
# Main ingestion
# ---------------------------------------------------------------------------
all_frames = []

for dataset_key, dataset_id in DATASETS.items():
    logger.info(f"Fetching resources for: {dataset_id}")
    try:
        resources = fetch_ckan_resources(dataset_id)
        for res in resources:
            fmt = (res.get("format") or "").upper()
            name = res.get("name", "")
            url = res.get("url", "")

            if fmt in ("GEOJSON", "JSON") and url:
                logger.info(f"  Downloading GeoJSON: {name} ({url[:60]}...)")
                try:
                    df = fetch_geojson_resource(url, dataset_key)
                    df["resource_name"] = name
                    df["resource_id"]   = res.get("id")
                    all_frames.append(df)
                    logger.info(f"    → {len(df)} features")
                except Exception as e:
                    logger.warning(f"    Skipped (error: {e})")

            elif fmt == "CSV" and url and "solar_radiation" in dataset_key:
                # For solar radiation, ingest the station summary CSV
                logger.info(f"  Downloading CSV: {name}")
                try:
                    df_csv = pd.read_csv(url)
                    df_csv["dataset"]       = dataset_key
                    df_csv["resource_name"] = name
                    df_csv["geometry_type"] = None
                    df_csv["geometry_wkt"]  = None
                    all_frames.append(df_csv)
                    logger.info(f"    → {len(df_csv)} rows")
                except Exception as e:
                    logger.warning(f"    Skipped CSV (error: {e})")

    except Exception as e:
        logger.warning(f"Failed to fetch {dataset_id}: {e}")

if all_frames:
    result_df = pd.concat(all_frames, ignore_index=True)
else:
    logger.warning("No EnergyData.info resources fetched — creating empty placeholder")
    result_df = pd.DataFrame({
        "dataset": pd.Series(dtype="str"),
        "resource_name": pd.Series(dtype="str"),
        "geometry_type": pd.Series(dtype="str"),
        "geometry_wkt": pd.Series(dtype="str"),
    })

result_df["_ingested_at"] = pd.Timestamp.utcnow()
result_df["_source"] = "energydata_info_ckan"

# Write to DuckDB
conn = duckdb.connect("kenya_energy.db")
conn.execute("CREATE SCHEMA IF NOT EXISTS ingestion")
conn.execute(
    "CREATE OR REPLACE TABLE ingestion.raw_energydata_geo AS SELECT * FROM result_df"
)
row_count = conn.execute(
    "SELECT COUNT(*) FROM ingestion.raw_energydata_geo"
).fetchone()[0]
conn.close()

logger.info(f"Wrote {row_count} rows to ingestion.raw_energydata_geo")
