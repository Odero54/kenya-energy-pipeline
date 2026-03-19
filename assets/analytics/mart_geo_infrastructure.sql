/* @bruin
name: mart.geo_infrastructure
type: duckdb.sql
materialization:
  type: table
description: >
  Summarises Kenya energy infrastructure from EnergyData.info.
  Counts transmission lines by voltage class, substations,
  and solar measurement stations. Used in the geospatial dashboard panel.
depends:
  - ingestion.raw_energydata_geo
@bruin */

SELECT
    dataset,
    resource_name,
    geometry_type,
    COUNT(*)                        AS feature_count,
    MIN(_ingested_at)               AS first_ingested_at

FROM ingestion.raw_energydata_geo

WHERE dataset IS NOT NULL

GROUP BY
    dataset,
    resource_name,
    geometry_type

ORDER BY dataset, feature_count DESC
