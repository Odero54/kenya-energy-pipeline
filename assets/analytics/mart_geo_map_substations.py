""" @bruin
name: mart.geo_map_substations
type: python
connection: duckdb-default
materialization:
  type: table
description: >
  Converts Kenya transmission substation locations from UTM EPSG:32737
  to WGS84 lat/lng for interactive map display.
  Produced from ingestion.raw_energydata_geo (Point geometries).
  83 substations sourced from KPLC via EnergyData.info.
depends:
  - ingestion.raw_energydata_geo
@bruin """

import duckdb
import pandas as pd


def materialize() -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    # Attach the main database to read raw geo data
    con.execute("ATTACH 'kenya_energy.db' AS main_db (READ_ONLY)")

    df = con.execute("""
        SELECT
            name_ims11   AS station_name,
            county2      AS county,
            ownershi23   AS owner,
            source,
            status,
            manned50     AS manned,
            'Substation' AS feature_type,
            -- always_xy=true forces (X=lng, Y=lat) — standard geographic order
            ST_Y(ST_Transform(
                ST_Point(
                    TRY_CAST(json_extract(geometry_wkt, '$[0]') AS DOUBLE),
                    TRY_CAST(json_extract(geometry_wkt, '$[1]') AS DOUBLE)
                ), 'EPSG:32737', 'EPSG:4326', true
            )) AS lat,
            ST_X(ST_Transform(
                ST_Point(
                    TRY_CAST(json_extract(geometry_wkt, '$[0]') AS DOUBLE),
                    TRY_CAST(json_extract(geometry_wkt, '$[1]') AS DOUBLE)
                ), 'EPSG:32737', 'EPSG:4326', true
            )) AS lng
        FROM main_db.ingestion.raw_energydata_geo
        WHERE geometry_type = 'Point'
          AND geometry_wkt IS NOT NULL
          AND geometry_wkt NOT IN ('null', '[]')
    """).df()

    # Filter to Kenya bounding box as a sanity check
    df = df[df["lat"].between(-5, 5) & df["lng"].between(33, 42)]
    return df
