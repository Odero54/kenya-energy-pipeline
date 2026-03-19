/* @bruin
name: mart.capacity_trend
type: duckdb.sql
materialization:
  type: table
description: >
  Yearly installed renewable capacity (MW) for Kenya from IRENA,
  broken down by technology. Includes total renewable capacity,
  YoY MW additions, and capacity utilisation factor (CUF) per technology.
  Primary mart for the "capacity trend" dashboard panel.
depends:
  - staging.stg_irena_capacity
  - staging.stg_energy_unified
columns:
  - name: year
    checks:
      - not_null
      - unique
  - name: total_renewable_mw
    checks:
      - not_null
@bruin */

WITH capacity_by_tech AS (
    SELECT
        year,
        tech_group,
        SUM(capacity_mw)    AS capacity_mw
    FROM staging.stg_irena_capacity
    GROUP BY year, tech_group
),

capacity_wide AS (
    SELECT
        year,
        ROUND(SUM(capacity_mw), 1)                                          AS total_renewable_mw,
        ROUND(SUM(CASE WHEN tech_group='Geothermal' THEN capacity_mw END),1) AS geothermal_mw,
        ROUND(SUM(CASE WHEN tech_group='Hydro'      THEN capacity_mw END),1) AS hydro_mw,
        ROUND(SUM(CASE WHEN tech_group='Wind'       THEN capacity_mw END),1) AS wind_mw,
        ROUND(SUM(CASE WHEN tech_group='Solar'      THEN capacity_mw END),1) AS solar_mw,
        ROUND(SUM(CASE WHEN tech_group='Bioenergy'  THEN capacity_mw END),1) AS bioenergy_mw
    FROM capacity_by_tech
    GROUP BY year
),

with_additions AS (
    SELECT
        *,
        total_renewable_mw
            - LAG(total_renewable_mw) OVER (ORDER BY year)  AS mw_added_yoy,
        ROUND(
            (total_renewable_mw
             - LAG(total_renewable_mw) OVER (ORDER BY year))
            / NULLIF(LAG(total_renewable_mw) OVER (ORDER BY year), 0) * 100
        , 1)                                                  AS capacity_growth_pct
    FROM capacity_wide
),

-- Join CUF from unified staging
cuf_by_year AS (
    SELECT
        year,
        ROUND(AVG(CASE WHEN source_type='Geothermal' THEN cuf END),3) AS geothermal_cuf,
        ROUND(AVG(CASE WHEN source_type IN ('Hydro','Hydropower') THEN cuf END),3) AS hydro_cuf,
        ROUND(AVG(CASE WHEN source_type='Wind' THEN cuf END),3)       AS wind_cuf,
        ROUND(AVG(CASE WHEN source_type='Solar' THEN cuf END),3)      AS solar_cuf
    FROM staging.stg_energy_unified
    WHERE cuf IS NOT NULL
    GROUP BY year
)

SELECT
    a.*,
    c.geothermal_cuf,
    c.hydro_cuf,
    c.wind_cuf,
    c.solar_cuf
FROM with_additions a
LEFT JOIN cuf_by_year c USING (year)
ORDER BY year
