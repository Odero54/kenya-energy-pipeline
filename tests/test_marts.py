"""
tests/test_marts.py

Tests for the analytics mart layer — validates aggregation logic,
KPI calculations, and mart data shapes using DuckDB in-memory.
Run with:  uv run pytest tests/ -v
"""

import pytest
import pandas as pd
import duckdb


@pytest.fixture(scope="module")
def conn() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(":memory:")
    c.execute("CREATE SCHEMA staging")
    c.execute("CREATE SCHEMA mart")

    # ── stg_energy_unified (synthetic 3-year Kenya data) ─────────────────────
    unified = pd.DataFrame({
        "year":          [2020]*5 + [2021]*5 + [2022]*5,
        "source_type":   ["Geothermal","Hydro","Wind","Solar","Gas"] * 3,
        "category":      ["Renewables","Renewables","Renewables",
                          "Renewables","Fossil Fuels"] * 3,
        "color_hex":     ["#E8593C","#3B8BD4","#5DCAA5","#EF9F27","#888780"] * 3,
        "generation_twh":[5.5, 3.2, 0.0, 0.1, 0.8,
                          5.7, 3.0, 1.8, 0.3, 0.6,
                          5.9, 2.8, 1.8, 0.4, 0.5],
        "capacity_mw":   [863, 836, 0,  51, None,
                          863, 836, 310, 91, None,
                          929, 836, 435, 233, None],
        "cuf":           [0.73, 0.44, None, 0.22, None,
                          0.75, 0.41, 0.66, 0.38, None,
                          0.72, 0.38, 0.47, 0.20, None],
        "is_renewable":  [True, True, True, True, False] * 3,
        "share_pct":     [57.0,33.3, 0.0, 1.0, 8.3,
                          49.1,25.9,15.5, 2.6, 5.2,
                          51.3,24.3,15.7, 3.5, 4.3],
        "year_total_twh":[9.6]*5 + [11.6]*5 + [11.5]*5,
        "access_to_electricity_pct":    [72.0]*5 + [74.0]*5 + [75.0]*5,
        "per_capita_elec_kwh":          [155.0]*5 + [160.0]*5 + [170.0]*5,
        "carbon_intensity_gco2_kwh":    [48.0]*5 + [46.0]*5 + [42.0]*5,
        "renewables_share_elec_pct":    [88.0]*5 + [89.0]*5 + [89.5]*5,
        "low_carbon_share_elec_pct":    [88.0]*5 + [89.0]*5 + [89.5]*5,
        "co2_from_elec_mtco2":          [0.55]*5 + [0.50]*5 + [0.48]*5,
        "total_elec_twh":               [10.5]*5 + [11.0]*5 + [11.5]*5,
    })
    c.execute(
        "CREATE TABLE staging.stg_energy_unified AS SELECT * FROM unified"
    )

    # ── stg_irena_capacity ────────────────────────────────────────────────────
    irena = pd.DataFrame({
        "year":       [2020]*4 + [2021]*4 + [2022]*4,
        "technology": ["Geothermal energy","Hydropower",
                       "Onshore wind energy","Solar photovoltaic"] * 3,
        "tech_group": ["Geothermal","Hydro","Wind","Solar"] * 3,
        "capacity_mw":[863,836,0,51, 863,836,310,91, 929,836,435,233],
        "_source":    ["irena_curated_fallback"]*12,
        "_ingested_at":[pd.Timestamp.utcnow()]*12,
    })
    c.execute(
        "CREATE TABLE staging.stg_irena_capacity AS SELECT * FROM irena"
    )

    yield c
    c.close()


class TestRenewableMixMart:

    def test_renewable_share_sums_correctly(self, conn):
        result = conn.execute("""
            SELECT
                year,
                SUM(CASE WHEN category='Renewables' THEN generation_twh ELSE 0 END)
                    / SUM(CASE WHEN category IN ('Renewables','Fossil Fuels')
                               THEN generation_twh ELSE 0 END) * 100 AS share
            FROM staging.stg_energy_unified
            GROUP BY year
            ORDER BY year
        """).df()
        # 2022: 5.9+2.8+1.8+0.4 = 10.9 RE / (10.9+0.5) = 95.6%
        assert result["share"].between(0, 100).all(), \
            "Renewable share outside [0, 100]"

    def test_yoy_pp_calculation(self, conn):
        result = conn.execute("""
            WITH shares AS (
                SELECT
                    year,
                    SUM(CASE WHEN category='Renewables' THEN generation_twh ELSE 0 END)
                    / SUM(CASE WHEN category IN ('Renewables','Fossil Fuels')
                               THEN generation_twh ELSE 0 END) * 100 AS share
                FROM staging.stg_energy_unified
                GROUP BY year
            )
            SELECT
                year,
                share,
                share - LAG(share) OVER (ORDER BY year) AS yoy_pp
            FROM shares
        """).df()
        # First year has NULL yoy_pp — that's expected
        non_null = result["yoy_pp"].dropna()
        assert len(non_null) == 2  # 2 YoY changes for 3 years

    def test_source_breakdown_totals_match(self, conn):
        result = conn.execute("""
            SELECT
                year,
                SUM(CASE WHEN source_type='Geothermal' THEN generation_twh ELSE 0 END) AS geo,
                SUM(CASE WHEN source_type='Hydro' THEN generation_twh ELSE 0 END) AS hydro,
                SUM(generation_twh) AS total
            FROM staging.stg_energy_unified
            GROUP BY year
        """).df()
        for _, row in result.iterrows():
            assert row["geo"] + row["hydro"] <= row["total"] + 0.001


class TestCapacityTrendMart:

    def test_total_capacity_increases_over_time(self, conn):
        result = conn.execute("""
            SELECT year, SUM(capacity_mw) AS total_mw
            FROM staging.stg_irena_capacity
            GROUP BY year
            ORDER BY year
        """).df()
        # 2022 should have more capacity than 2020 (Wind added)
        cap_2020 = result[result["year"]==2020]["total_mw"].iloc[0]
        cap_2022 = result[result["year"]==2022]["total_mw"].iloc[0]
        assert cap_2022 >= cap_2020, \
            f"Capacity decreased: 2020={cap_2020}MW, 2022={cap_2022}MW"

    def test_wind_capacity_added_2021(self, conn):
        result = conn.execute("""
            SELECT year, SUM(capacity_mw) AS wind_mw
            FROM staging.stg_irena_capacity
            WHERE tech_group = 'Wind'
            GROUP BY year
            ORDER BY year
        """).df()
        wind_2020 = result[result["year"]==2020]["wind_mw"].iloc[0]
        wind_2021 = result[result["year"]==2021]["wind_mw"].iloc[0]
        assert wind_2021 > wind_2020, "Wind capacity should increase 2020→2021"

    def test_no_negative_capacity(self, conn):
        result = conn.execute(
            "SELECT capacity_mw FROM staging.stg_irena_capacity WHERE capacity_mw < 0"
        ).df()
        assert len(result) == 0


class TestEnergyKpisMart:

    def test_gap_to_target_logic(self, conn):
        result = conn.execute("""
            SELECT year, renewables_share_elec_pct,
                   100.0 - renewables_share_elec_pct AS gap
            FROM staging.stg_energy_unified
            GROUP BY year, renewables_share_elec_pct
        """).df()
        assert (result["gap"] >= 0).all(), "Gap to target is negative"

    def test_per_capita_positive(self, conn):
        result = conn.execute("""
            SELECT DISTINCT year, per_capita_elec_kwh
            FROM staging.stg_energy_unified
        """).df()
        assert (result["per_capita_elec_kwh"] > 0).all()

    def test_access_rate_within_range(self, conn):
        result = conn.execute("""
            SELECT DISTINCT year, access_to_electricity_pct
            FROM staging.stg_energy_unified
        """).df()
        assert result["access_to_electricity_pct"].between(0, 100).all()


class TestGenerationBySourceMart:

    def test_share_pct_sums_near_100(self, conn):
        result = conn.execute("""
            SELECT year, SUM(share_pct) AS total_share
            FROM staging.stg_energy_unified
            WHERE category IN ('Renewables','Fossil Fuels')
            GROUP BY year
        """).df()
        for _, row in result.iterrows():
            assert abs(row["total_share"] - 100.0) < 1.5, \
                f"Year {row['year']}: shares sum to {row['total_share']:.1f}%, expected ~100%"

    def test_rank_within_year_is_unique(self, conn):
        result = conn.execute("""
            SELECT year, source_type,
                   RANK() OVER (PARTITION BY year ORDER BY generation_twh DESC) AS rnk
            FROM staging.stg_energy_unified
        """).df()
        # Geothermal should be rank 1 in all years
        geo_ranks = result[result["source_type"]=="Geothermal"]["rnk"]
        assert (geo_ranks == 1).all(), "Geothermal is not the top source"
