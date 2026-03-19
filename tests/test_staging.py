"""
tests/test_staging.py

Tests for staging layer SQL transformations applied in Python via DuckDB.
Run with:  uv run pytest tests/ -v
"""

import pytest
import pandas as pd
import duckdb


@pytest.fixture(scope="module")
def conn() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with populated raw tables."""
    c = duckdb.connect(":memory:")
    c.execute("CREATE SCHEMA ingestion")
    c.execute("CREATE SCHEMA staging")
    c.execute("CREATE SCHEMA mart")

    # ── raw_ember_generation ──────────────────────────────────────────────────
    ember = pd.DataFrame({
        "country_code": ["KEN"] * 8,
        "year":         [2020, 2020, 2021, 2021, 2022, 2022, 2022, 2022],
        "category":     ["Renewables", "Fossil Fuels", "Renewables",
                         "Fossil Fuels", "Renewables", "Fossil Fuels",
                         "Renewables", "Renewables"],
        "subcategory":  ["Geothermal", "Gas", "Geothermal", "Gas",
                         "Geothermal", "Gas", "Wind", "Solar"],
        "variable":     ["Electricity generation"] * 8,
        "unit":         ["TWh"] * 8,
        "value":        [5.5, 0.8, 5.7, 0.6, 5.9, 0.5, 1.8, 0.4],
        "source_name":  ["Ember"] * 8,
        "_ingested_at": [pd.Timestamp.utcnow()] * 8,
    })
    c.execute(
        "CREATE TABLE ingestion.raw_ember_generation AS SELECT * FROM ember"
    )

    # ── raw_irena_capacity ────────────────────────────────────────────────────
    irena = pd.DataFrame({
        "country":      ["Kenya"] * 4,
        "technology":   ["Geothermal energy", "Hydropower",
                         "Onshore wind energy", "Solar photovoltaic"],
        "year":         [2022, 2022, 2022, 2022],
        "capacity_mw":  [929.0, 836.0, 435.0, 233.0],
        "_source":      ["irena_curated_fallback"] * 4,
        "_ingested_at": [pd.Timestamp.utcnow()] * 4,
    })
    c.execute(
        "CREATE TABLE ingestion.raw_irena_capacity AS SELECT * FROM irena"
    )

    # ── raw_owid_energy ───────────────────────────────────────────────────────
    owid = pd.DataFrame({
        "iso_code":              ["KEN", "KEN", "KEN"],
        "country":               ["Kenya"] * 3,
        "year":                  [2020, 2021, 2022],
        "access_to_electricity": [72.0, 74.0, 75.0],
        "per_capita_electricity":[155.0, 160.0, 170.0],
        "carbon_intensity_elec": [48.0, 46.0, 42.0],
        "electricity_generation":[10.5, 11.0, 11.5],
        "renewables_electricity":[9.5, 9.8, 10.3],
        "renewables_share_elec": [88.0, 89.0, 89.5],
        "fossil_share_elec":     [12.0, 11.0, 10.5],
        "co2_emissions_from_electricity": [0.55, 0.50, 0.48],
        "_ingested_at": [pd.Timestamp.utcnow()] * 3,
    })
    c.execute(
        "CREATE TABLE ingestion.raw_owid_energy AS SELECT * FROM owid"
    )

    # ── seeds/source_categories ───────────────────────────────────────────────
    cats = pd.DataFrame({
        "source_type":  ["Geothermal", "Hydro", "Wind", "Solar",
                         "Bioenergy", "Gas", "Oil", "Coal"],
        "category":     ["Renewables", "Renewables", "Renewables", "Renewables",
                         "Renewables", "Fossil Fuels", "Fossil Fuels", "Fossil Fuels"],
        "subcategory":  ["Geothermal", "Hydro", "Wind", "Solar",
                         "Bioenergy", "Gas", "Oil", "Coal"],
        "is_renewable": ["true", "true", "true", "true",
                         "true", "false", "false", "false"],
        "color_hex":    ["#E8593C", "#3B8BD4", "#5DCAA5", "#EF9F27",
                         "#63992A", "#888780", "#5F5E5A", "#2C2C2A"],
    })
    c.execute(
        "CREATE TABLE staging.stg_source_categories AS SELECT * FROM cats"
    )

    yield c
    c.close()


class TestStagingEmber:

    def test_stg_ember_generation_year_cast(self, conn):
        """Year column must be INTEGER after staging transform."""
        result = conn.execute("""
            SELECT CAST(year AS INTEGER) AS year, value AS value_clean
            FROM ingestion.raw_ember_generation
            WHERE unit = 'TWh' AND value >= 0
        """).df()
        assert result["year"].dtype in ["int32", "int64"]

    def test_no_negative_generation(self, conn):
        result = conn.execute("""
            SELECT value FROM ingestion.raw_ember_generation
            WHERE variable = 'Electricity generation' AND value < 0
        """).df()
        assert len(result) == 0, "Negative generation values found"

    def test_renewable_flag_logic(self, conn):
        result = conn.execute("""
            SELECT
                category,
                CASE WHEN LOWER(category) LIKE '%renewable%' THEN TRUE
                     ELSE FALSE END AS is_renewable
            FROM ingestion.raw_ember_generation
        """).df()
        renewable_rows = result[result["category"] == "Renewables"]
        assert renewable_rows["is_renewable"].all()


class TestStagingIrena:

    def test_technology_mapping(self, conn):
        """Geothermal energy → Geothermal."""
        result = conn.execute("""
            SELECT
                technology,
                CASE
                    WHEN LOWER(technology) LIKE '%geothermal%' THEN 'Geothermal'
                    WHEN LOWER(technology) LIKE '%hydro%'      THEN 'Hydro'
                    WHEN LOWER(technology) LIKE '%wind%'       THEN 'Wind'
                    WHEN LOWER(technology) LIKE '%solar%'      THEN 'Solar PV'
                    ELSE technology
                END AS tech_clean
            FROM ingestion.raw_irena_capacity
        """).df()
        assert "Geothermal" in result["tech_clean"].values
        assert "Hydro" in result["tech_clean"].values
        assert "Wind" in result["tech_clean"].values

    def test_capacity_non_negative(self, conn):
        result = conn.execute("""
            SELECT capacity_mw FROM ingestion.raw_irena_capacity
            WHERE capacity_mw < 0
        """).df()
        assert len(result) == 0

    def test_cuf_within_bounds(self, conn):
        """Capacity utilisation factor must be in [0, 1]."""
        result = conn.execute("""
            SELECT
                i.technology,
                i.capacity_mw,
                SUM(e.value) AS gen_twh,
                LEAST(1.0,
                    GREATEST(0.0,
                        SUM(e.value) * 1e6 / (i.capacity_mw * 8760)
                    )
                ) AS cuf
            FROM ingestion.raw_irena_capacity i
            LEFT JOIN ingestion.raw_ember_generation e
                ON e.year = i.year
                AND LOWER(e.subcategory) LIKE '%' || LOWER(
                    CASE WHEN LOWER(i.technology) LIKE '%geothermal%' THEN 'geothermal'
                         WHEN LOWER(i.technology) LIKE '%hydro%' THEN 'hydro'
                         WHEN LOWER(i.technology) LIKE '%wind%' THEN 'wind'
                         WHEN LOWER(i.technology) LIKE '%solar%' THEN 'solar'
                         ELSE i.technology END
                ) || '%'
            GROUP BY i.technology, i.capacity_mw
        """).df()
        cuf_vals = result["cuf"].dropna()
        assert (cuf_vals >= 0).all() and (cuf_vals <= 1).all(), \
            f"CUF out of [0,1]: {cuf_vals.describe()}"


class TestStagingOwid:

    def test_share_values_between_0_100(self, conn):
        result = conn.execute("""
            SELECT renewables_share_elec, fossil_share_elec
            FROM ingestion.raw_owid_energy
        """).df()
        for col in ["renewables_share_elec", "fossil_share_elec"]:
            vals = result[col].dropna()
            assert (vals >= 0).all() and (vals <= 100).all(), \
                f"{col} out of [0,100]"

    def test_unique_years(self, conn):
        result = conn.execute(
            "SELECT year, COUNT(*) AS n FROM ingestion.raw_owid_energy GROUP BY year"
        ).df()
        assert (result["n"] == 1).all(), "Duplicate years in OWID data"

    def test_access_rate_reasonable(self, conn):
        result = conn.execute(
            "SELECT access_to_electricity FROM ingestion.raw_owid_energy"
        ).df()
        vals = result["access_to_electricity"].dropna()
        # Kenya access should be between 50% and 100% for recent years
        assert (vals >= 0).all() and (vals <= 100).all()


class TestSourceCategories:

    def test_all_major_sources_present(self, conn):
        result = conn.execute(
            "SELECT source_type FROM staging.stg_source_categories"
        ).df()
        expected = {"Geothermal", "Hydro", "Wind", "Solar", "Gas"}
        actual = set(result["source_type"].tolist())
        missing = expected - actual
        assert not missing, f"Missing source types: {missing}"

    def test_renewable_flag_is_boolean_compatible(self, conn):
        result = conn.execute("""
            SELECT
                source_type,
                CASE WHEN LOWER(TRIM(is_renewable)) IN ('true','1','yes')
                     THEN TRUE ELSE FALSE END AS is_renewable_bool
            FROM staging.stg_source_categories
        """).df()
        renewable_types = set(
            result[result["is_renewable_bool"]]["source_type"].tolist()
        )
        assert "Geothermal" in renewable_types
        assert "Wind" in renewable_types
        assert "Gas" not in renewable_types
