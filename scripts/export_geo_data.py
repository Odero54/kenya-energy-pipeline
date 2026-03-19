"""
Create geo spatial mart tables and export the GeoJSON asset.

Run after `bruin run pipeline.yml` so that ingestion.raw_energydata_geo exists.
Used by both the CI/CD workflow and local `make build-dashboard`.

Usage:
    uv run python scripts/export_geo_data.py
"""

import json
import logging
import os
import sys

import duckdb

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

DB_PATH = os.getenv("KENYA_DB", "kenya_energy.db")
GEOJSON_OUT = os.path.join("dashboard", "static", "kenya_infrastructure.geojson")


def build_spatial_marts(con: duckdb.DuckDBPyConnection) -> None:
    log.info("Installing / loading DuckDB spatial extension...")
    con.execute("INSTALL spatial; LOAD spatial;")

    log.info("Building mart.geo_map_substations …")
    con.execute("""
        CREATE OR REPLACE TABLE mart.geo_map_substations AS
        SELECT
            name_ims11   AS station_name,
            county2      AS county,
            ownershi23   AS owner,
            source,
            status,
            manned50     AS manned,
            'Substation' AS feature_type,
            -- always_xy=true → (X=lng, Y=lat) standard geographic order
            ST_Y(ST_Transform(ST_Point(
                TRY_CAST(json_extract(geometry_wkt, '$[0]') AS DOUBLE),
                TRY_CAST(json_extract(geometry_wkt, '$[1]') AS DOUBLE)
            ), 'EPSG:32737', 'EPSG:4326', true)) AS lat,
            ST_X(ST_Transform(ST_Point(
                TRY_CAST(json_extract(geometry_wkt, '$[0]') AS DOUBLE),
                TRY_CAST(json_extract(geometry_wkt, '$[1]') AS DOUBLE)
            ), 'EPSG:32737', 'EPSG:4326', true)) AS lng
        FROM ingestion.raw_energydata_geo
        WHERE geometry_type = 'Point'
          AND geometry_wkt IS NOT NULL
          AND geometry_wkt NOT IN ('null', '[]')
    """)

    log.info("Building mart.geo_map_lines …")
    con.execute("""
        CREATE OR REPLACE TABLE mart.geo_map_lines AS
        SELECT
            resource_name,
            voltage47 AS voltage_kv,
            CASE resource_name
                WHEN '220kV Network' THEN '#c0392b'
                WHEN '132kV Network' THEN '#e74c3c'
                WHEN '66kV Network'  THEN '#e67e22'
                ELSE '#95a5a6'
            END AS line_color,
            ST_AsGeoJSON(ST_Transform(
                ST_GeomFromGeoJSON(
                    '{"type":"LineString","coordinates":' || geometry_wkt || '}'
                ),
                'EPSG:32737',
                'EPSG:4326',
                true
            )) AS geojson_wgs84
        FROM ingestion.raw_energydata_geo
        WHERE geometry_type = 'LineString'
          AND resource_name IN ('220kV Network', '132kV Network', '66kV Network')
          AND geometry_wkt IS NOT NULL
          AND TRY_CAST(json_array_length(geometry_wkt) AS INT) >= 2
    """)


def export_geojson(con: duckdb.DuckDBPyConnection) -> None:
    log.info("Exporting GeoJSON FeatureCollection …")

    lines = con.execute(
        "SELECT resource_name, voltage_kv, line_color, geojson_wgs84 "
        "FROM mart.geo_map_lines"
    ).fetchall()

    points = con.execute(
        "SELECT station_name, county, owner, manned, lat, lng "
        "FROM mart.geo_map_substations "
        "WHERE lat IS NOT NULL AND lng IS NOT NULL"
    ).fetchall()

    features = []

    for resource_name, voltage_kv, line_color, geojson_wgs84 in lines:
        features.append({
            "type": "Feature",
            "geometry": json.loads(geojson_wgs84),
            "properties": {
                "layer": "transmission_line",
                "resource_name": resource_name,
                "voltage_kv": voltage_kv,
                "line_color": line_color,
            },
        })

    for station_name, county, owner, manned, lat, lng in points:
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lng, lat]},
            "properties": {
                "layer": "substation",
                "station_name": station_name or "",
                "county": county or "",
                "owner": owner or "",
                "manned": manned or "",
            },
        })

    os.makedirs(os.path.dirname(GEOJSON_OUT), exist_ok=True)
    with open(GEOJSON_OUT, "w") as fh:
        json.dump({"type": "FeatureCollection", "features": features}, fh,
                  separators=(",", ":"))

    size_kb = os.path.getsize(GEOJSON_OUT) // 1024
    log.info(
        "Written %s — %d features (%d lines, %d substations), %d KB",
        GEOJSON_OUT, len(features), len(lines), len(points), size_kb,
    )


def main() -> None:
    if not os.path.exists(DB_PATH):
        log.error("Database not found: %s (run `bruin run pipeline.yml` first)", DB_PATH)
        sys.exit(1)

    con = duckdb.connect(DB_PATH)
    try:
        build_spatial_marts(con)
        export_geojson(con)
    finally:
        con.close()

    log.info("Done ✓")


if __name__ == "__main__":
    main()
