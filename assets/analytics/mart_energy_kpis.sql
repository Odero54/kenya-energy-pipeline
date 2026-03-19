/* @bruin
name: mart.energy_kpis
type: duckdb.sql
materialization:
  type: table
description: >
  Key energy performance indicators for Kenya by year.
  Combines OWID context metrics with Ember generation data.
  Tracks progress toward Kenya's 2030 target of 100% renewable electricity.
  Primary mart for the KPI cards and trend lines dashboard panel.
depends:
  - staging.stg_owid_energy
  - mart.renewable_mix
columns:
  - name: year
    checks:
      - name: not_null
      - name: unique
@bruin */

WITH owid AS (
    SELECT *
    FROM staging.stg_owid_energy
    WHERE year >= 2000
),

mix AS (
    SELECT *
    FROM mart.renewable_mix
),

joined AS (
    SELECT
        o.year,

        -- Access & consumption
        o.access_to_electricity_pct,
        o.per_capita_elec_kwh,

        -- Generation totals (from Ember via mix mart)
        m.total_generation_twh,
        m.renewable_twh,
        m.fossil_twh,

        -- Shares
        m.renewable_share_pct,
        m.fossil_share_pct,
        m.renewable_share_yoy_pp,

        -- Carbon
        o.carbon_intensity_gco2_kwh,
        o.co2_from_elec_mtco2,

        -- Generation breakdown for dashboard sparklines
        m.geothermal_twh,
        m.hydro_twh,
        m.wind_twh,
        m.solar_twh,
        m.bioenergy_twh,

        -- Progress to 2030 target (100% renewable)
        ROUND(m.renewable_share_pct / 100.0, 4)           AS target_progress_ratio,
        ROUND(100.0 - m.renewable_share_pct, 1)           AS gap_to_100pct_target,

        -- Decade classification for dashboard filtering
        CASE
            WHEN o.year BETWEEN 2000 AND 2009 THEN '2000s'
            WHEN o.year BETWEEN 2010 AND 2019 THEN '2010s'
            WHEN o.year >= 2020              THEN '2020s'
        END                                                AS decade

    FROM owid o
    LEFT JOIN mix m ON o.year = m.year
)

SELECT *
FROM joined
ORDER BY year DESC
