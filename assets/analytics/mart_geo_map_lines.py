""" @bruin
name: mart.geo_map_lines
type: python
connection: duckdb-default
materialization:
  type: table
description: >
  Converts high-voltage Kenya transmission lines (66 kV, 132 kV, 220 kV) from
  UTM EPSG:32737 to WGS84 GeoJSON strings for interactive map display.
  Only the major HV corridors are included — distribution lines (11/33 kV) are
  excluded to keep the map readable and the dataset small.
  Source: ingestion.raw_energydata_geo (LineString geometries).
depends:
  - ingestion.raw_energydata_geo
@bruin """

import duckdb
import pandas as pd


def materialize() -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("ATTACH 'kenya_energy.db' AS main_db (READ_ONLY)")

    df = con.execute("""
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
                ST_GeomFromGeoJSON('{"type":"LineString","coordinates":' || geometry_wkt || '}'),
                'EPSG:32737',
                'EPSG:4326',
                true
            )) AS geojson_wgs84
        FROM main_db.ingestion.raw_energydata_geo
        WHERE geometry_type = 'LineString'
          AND resource_name IN ('220kV Network', '132kV Network', '66kV Network')
          AND geometry_wkt IS NOT NULL
          AND TRY_CAST(json_array_length(geometry_wkt) AS INT) >= 2
    """).df()

    return df
