/* @bruin
name: mart.renewable_mix
type: duckdb.sql
materialization:
  type: table
description: >
  Yearly renewable generation mix for Kenya (2000–present).
  One row per year. Columns: total generation, breakdown by major
  renewable source (TWh), renewable share (%), fossil share (%).
  Primary mart for the "generation mix" dashboard panel.
depends:
  - staging.stg_energy_unified
columns:
  - name: year
    checks:
      - not_null
      - unique
      - accepted_range:
          min: 2000
          max: 2030
  - name: total_generation_twh
    checks:
      - not_null
  - name: renewable_share_pct
    checks:
      - accepted_range:
          min: 0
          max: 100
@bruin */

WITH gen AS (
    SELECT
        year,
        source_type,
        category,
        COALESCE(generation_twh, 0)     AS generation_twh,
        COALESCE(share_pct, 0)          AS share_pct
    FROM staging.stg_energy_unified
    WHERE category IS NOT NULL
)

SELECT
    year,

    -- Total (excl. imports / demand)
    ROUND(SUM(CASE WHEN category IN ('Renewables','Fossil Fuels','Nuclear')
                   THEN generation_twh ELSE 0 END), 4)
        AS total_generation_twh,

    -- Renewables aggregate
    ROUND(SUM(CASE WHEN category = 'Renewables'
                   THEN generation_twh ELSE 0 END), 4)
        AS renewable_twh,

    -- By source
    ROUND(SUM(CASE WHEN source_type = 'Geothermal'
                   THEN generation_twh ELSE 0 END), 4)  AS geothermal_twh,
    ROUND(SUM(CASE WHEN source_type IN ('Hydro','Hydropower')
                   THEN generation_twh ELSE 0 END), 4)  AS hydro_twh,
    ROUND(SUM(CASE WHEN source_type = 'Wind'
                   THEN generation_twh ELSE 0 END), 4)  AS wind_twh,
    ROUND(SUM(CASE WHEN source_type = 'Solar'
                   THEN generation_twh ELSE 0 END), 4)  AS solar_twh,
    ROUND(SUM(CASE WHEN source_type = 'Bioenergy'
                   THEN generation_twh ELSE 0 END), 4)  AS bioenergy_twh,
    ROUND(SUM(CASE WHEN source_type = 'Other Renewables'
                   THEN generation_twh ELSE 0 END), 4)  AS other_renewables_twh,

    -- Fossil fuels
    ROUND(SUM(CASE WHEN category = 'Fossil Fuels'
                   THEN generation_twh ELSE 0 END), 4)  AS fossil_twh,
    ROUND(SUM(CASE WHEN source_type = 'Gas'
                   THEN generation_twh ELSE 0 END), 4)  AS gas_twh,
    ROUND(SUM(CASE WHEN source_type = 'Oil'
                   THEN generation_twh ELSE 0 END), 4)  AS oil_twh,
    ROUND(SUM(CASE WHEN source_type = 'Coal'
                   THEN generation_twh ELSE 0 END), 4)  AS coal_twh,

    -- Shares
    ROUND(
        SUM(CASE WHEN category = 'Renewables' THEN generation_twh ELSE 0 END)
        / NULLIF(SUM(CASE WHEN category IN ('Renewables','Fossil Fuels','Nuclear')
                          THEN generation_twh ELSE 0 END), 0) * 100
    , 1)                                                AS renewable_share_pct,

    ROUND(
        SUM(CASE WHEN category = 'Fossil Fuels' THEN generation_twh ELSE 0 END)
        / NULLIF(SUM(CASE WHEN category IN ('Renewables','Fossil Fuels','Nuclear')
                          THEN generation_twh ELSE 0 END), 0) * 100
    , 1)                                                AS fossil_share_pct,

    -- YoY change in renewable share
    ROUND(
        ROUND(SUM(CASE WHEN category = 'Renewables' THEN generation_twh ELSE 0 END)
              / NULLIF(SUM(CASE WHEN category IN ('Renewables','Fossil Fuels','Nuclear')
                                THEN generation_twh ELSE 0 END), 0) * 100, 1)
        - LAG(ROUND(SUM(CASE WHEN category = 'Renewables' THEN generation_twh ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN category IN ('Renewables','Fossil Fuels','Nuclear')
                                      THEN generation_twh ELSE 0 END), 0) * 100, 1))
          OVER (ORDER BY year)
    , 2)                                                AS renewable_share_yoy_pp

FROM gen
GROUP BY year
ORDER BY year
