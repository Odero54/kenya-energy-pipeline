/* @bruin
name: staging.stg_owid_energy
type: duckdb.sql
materialization:
  type: table
description: >
  Selects and cleans key OWID energy indicators for Kenya.
  Focuses on per-capita metrics, carbon intensity, primary energy,
  renewable share, and access rate — all contextual KPIs for the dashboard.
depends:
  - ingestion.raw_owid_energy
columns:
  - name: year
    checks:
      - not_null
      - accepted_range:
          min: 1990
          max: 2030
  - name: country
    checks:
      - not_null
@bruin */

SELECT
    CAST(year AS INTEGER)                                       AS year,
    country,
    iso_code,

    -- Electricity access
    ROUND(CAST(access_to_electricity                AS DOUBLE), 2)  AS access_to_electricity_pct,

    -- Per-capita consumption (kWh)
    ROUND(CAST(per_capita_electricity               AS DOUBLE), 2)  AS per_capita_elec_kwh,
    ROUND(CAST(energy_per_capita                    AS DOUBLE), 2)  AS energy_per_capita_kwh,

    -- Carbon intensity
    ROUND(CAST(carbon_intensity_elec                AS DOUBLE), 4)  AS carbon_intensity_gco2_kwh,

    -- Electricity generation by source (TWh)
    ROUND(CAST(electricity_generation               AS DOUBLE), 4)  AS total_elec_twh,
    ROUND(CAST(renewables_electricity               AS DOUBLE), 4)  AS renewables_elec_twh,
    ROUND(CAST(hydro_electricity                    AS DOUBLE), 4)  AS hydro_elec_twh,
    ROUND(CAST(wind_electricity                     AS DOUBLE), 4)  AS wind_elec_twh,
    ROUND(CAST(solar_electricity                    AS DOUBLE), 4)  AS solar_elec_twh,
    ROUND(CAST(other_renewables_electricity         AS DOUBLE), 4)  AS other_renewables_elec_twh,
    ROUND(CAST(fossil_electricity                   AS DOUBLE), 4)  AS fossil_elec_twh,
    ROUND(CAST(nuclear_electricity                  AS DOUBLE), 4)  AS nuclear_elec_twh,

    -- Renewable share of electricity (%)
    ROUND(CAST(renewables_share_elec                AS DOUBLE), 2)  AS renewables_share_elec_pct,
    ROUND(CAST(low_carbon_share_elec                AS DOUBLE), 2)  AS low_carbon_share_elec_pct,
    ROUND(CAST(fossil_share_elec                    AS DOUBLE), 2)  AS fossil_share_elec_pct,

    -- Primary energy
    ROUND(CAST(primary_energy_consumption           AS DOUBLE), 4)  AS primary_energy_twh,
    ROUND(CAST(renewables_share_energy              AS DOUBLE), 2)  AS renewables_share_energy_pct,

    -- CO2
    ROUND(CAST(co2_emissions_from_electricity       AS DOUBLE), 4)  AS co2_from_elec_mtco2,
    ROUND(CAST(greenhouse_gas_emissions             AS DOUBLE), 4)  AS ghg_emissions_mtco2eq,

    _ingested_at

FROM ingestion.raw_owid_energy

WHERE
    year IS NOT NULL
    AND CAST(year AS INTEGER) BETWEEN 1990 AND 2030
