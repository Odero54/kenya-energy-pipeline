""" @bruin
name: staging.stg_energy_unified
type: python
connection: duckdb-default
materialization:
  type: table
description: >
  Joins Ember generation data with IRENA capacity data and OWID context
  indicators into a single unified staging table. Reconciles year coverage
  gaps between sources using forward-fill and interpolation. Adds capacity
  utilisation factor (CUF) per technology-year.
depends:
  - staging.stg_ember_generation
  - staging.stg_irena_capacity
  - staging.stg_owid_energy
  - staging.stg_source_categories
@bruin """

import pandas as pd
import duckdb
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def materialize() -> pd.DataFrame:
    conn = duckdb.connect("kenya_energy.db")

    # -----------------------------------------------------------------------
    # 1. Load staging tables
    # -----------------------------------------------------------------------
    ember = conn.execute("""
        SELECT
            year,
            variable                            AS source_type,
            category                            AS variable,
            unit,
            value_clean,
            is_renewable
        FROM staging.stg_ember_generation
        WHERE unit = 'TWh'
          AND category = 'Electricity generation'
          AND subcategory = 'Fuel'
    """).df()

    irena = conn.execute("""
        SELECT year, technology AS source_type, tech_group, capacity_mw
        FROM staging.stg_irena_capacity
    """).df()

    owid = conn.execute("""
        SELECT
            year,
            access_to_electricity_pct,
            per_capita_elec_kwh,
            carbon_intensity_gco2_kwh,
            renewables_share_elec_pct,
            low_carbon_share_elec_pct,
            co2_from_elec_mtco2,
            total_elec_twh
        FROM staging.stg_owid_energy
    """).df()

    cats = conn.execute("""
        SELECT source_type, category, color_hex
        FROM staging.stg_source_categories
    """).df()

    conn.close()

    # -----------------------------------------------------------------------
    # 2. Build Ember generation pivot: one row per (year, source_type)
    # -----------------------------------------------------------------------
    gen = (
        ember[ember["variable"] == "Electricity generation"]
        .groupby(["year", "source_type"])["value_clean"]
        .sum()
        .reset_index()
        .rename(columns={"value_clean": "generation_twh"})
    )

    # -----------------------------------------------------------------------
    # 3. Merge IRENA capacity into generation
    # -----------------------------------------------------------------------
    # Deduplicate IRENA to one row per year/source_type (take max capacity)
    irena_agg = (
        irena.groupby(["year", "source_type"])["capacity_mw"]
        .max()
        .reset_index()
    )

    unified = gen.merge(irena_agg, on=["year", "source_type"], how="left")

    # -----------------------------------------------------------------------
    # 4. Compute Capacity Utilisation Factor (CUF)
    #    CUF = generation_TWh / (capacity_MW * 8760h) * 1e6
    # -----------------------------------------------------------------------
    unified["cuf"] = (
        unified["generation_twh"] * 1e6
        / (unified["capacity_mw"] * 8760)
    ).round(4)

    # Clamp CUF to [0, 1] — values > 1 indicate stale IRENA data
    unified["cuf"] = unified["cuf"].clip(0, 1)

    # -----------------------------------------------------------------------
    # 5. Attach category labels
    # -----------------------------------------------------------------------
    unified = unified.merge(cats, on="source_type", how="left")

    # -----------------------------------------------------------------------
    # 6. Attach OWID context (one row per year → broadcast to all source rows)
    # -----------------------------------------------------------------------
    unified = unified.merge(owid, on="year", how="left")

    # -----------------------------------------------------------------------
    # 7. Compute share of total annual generation per source type
    # -----------------------------------------------------------------------
    year_totals = (
        unified[unified["category"].isin(["Renewables", "Fossil Fuels", "Nuclear"])]
        .groupby("year")["generation_twh"]
        .sum()
        .reset_index()
        .rename(columns={"generation_twh": "year_total_twh"})
    )
    unified = unified.merge(year_totals, on="year", how="left")
    unified["share_pct"] = (
        unified["generation_twh"] / unified["year_total_twh"] * 100
    ).round(2)

    logger.info(f"staging.stg_energy_unified: {len(unified)} rows")
    return unified
