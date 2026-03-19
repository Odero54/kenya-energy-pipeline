/* @bruin
name: staging.stg_irena_capacity
type: duckdb.sql
materialization:
  type: table
description: >
  Cleans and normalises IRENA renewable capacity data for Kenya.
  Maps IRENA technology labels to a standard set used across marts.
  Adds renewable flag and tech_group for aggregation.
depends:
  - ingestion.raw_irena_capacity
columns:
  - name: year
    checks:
      - not_null
  - name: technology
    checks:
      - not_null
  - name: capacity_mw
    checks:
      - not_null
@bruin */

SELECT
    CAST(year AS INTEGER)       AS year,
    TRIM(country)               AS country,
    TRIM(technology)            AS technology_raw,

    -- Map IRENA technology labels → standard names
    CASE
        WHEN LOWER(technology) LIKE '%geothermal%'        THEN 'Geothermal'
        WHEN LOWER(technology) LIKE '%hydropower%'
          OR LOWER(technology) LIKE '%hydro%'             THEN 'Hydro'
        WHEN LOWER(technology) LIKE '%onshore wind%'
          OR LOWER(technology) LIKE '%wind%'              THEN 'Wind'
        WHEN LOWER(technology) LIKE '%solar photovoltaic%'
          OR LOWER(technology) LIKE '%solar pv%'          THEN 'Solar PV'
        WHEN LOWER(technology) LIKE '%solar thermal%'     THEN 'Solar Thermal'
        WHEN LOWER(technology) LIKE '%bioenergy%'
          OR LOWER(technology) LIKE '%biomass%'           THEN 'Bioenergy'
        WHEN LOWER(technology) LIKE '%marine%'
          OR LOWER(technology) LIKE '%ocean%'             THEN 'Marine'
        ELSE TRIM(technology)
    END                         AS technology,

    -- Tech group for aggregation
    CASE
        WHEN LOWER(technology) LIKE '%geothermal%'        THEN 'Geothermal'
        WHEN LOWER(technology) LIKE '%hydro%'             THEN 'Hydro'
        WHEN LOWER(technology) LIKE '%wind%'              THEN 'Wind'
        WHEN LOWER(technology) LIKE '%solar%'             THEN 'Solar'
        WHEN LOWER(technology) LIKE '%bio%'               THEN 'Bioenergy'
        ELSE 'Other Renewables'
    END                         AS tech_group,

    TRUE                        AS is_renewable,

    ROUND(CAST(capacity_mw AS DOUBLE), 2)   AS capacity_mw,
    _source,
    _ingested_at

FROM ingestion.raw_irena_capacity

WHERE
    year IS NOT NULL
    AND capacity_mw IS NOT NULL
    AND CAST(capacity_mw AS DOUBLE) >= 0
    AND CAST(year AS INTEGER) BETWEEN 2000 AND 2030
