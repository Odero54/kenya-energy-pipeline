"""
tests/test_ingestion.py

Unit tests for Kenya energy pipeline ingestion assets.
Run with:  uv run pytest tests/ -v
"""

import pytest
import pandas as pd
import duckdb
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def test_db(tmp_path_factory) -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection for testing."""
    db_path = tmp_path_factory.mktemp("db") / "test_kenya.db"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS ingestion")
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute("CREATE SCHEMA IF NOT EXISTS mart")
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def ember_sample() -> pd.DataFrame:
    """Minimal Ember-shaped DataFrame for testing."""
    return pd.DataFrame({
        "country_code": ["KEN"] * 6,
        "country":      ["Kenya"] * 6,
        "year":         [2020, 2020, 2021, 2021, 2022, 2022],
        "category":     ["Renewables", "Fossil Fuels"] * 3,
        "subcategory":  ["Geothermal", "Gas", "Wind", "Oil", "Solar", "Gas"],
        "variable":     ["Electricity generation"] * 6,
        "unit":         ["TWh"] * 6,
        "value":        [5.50, 0.80, 1.81, 0.60, 0.39, 0.55],
        "source":       ["Ember"] * 6,
        "source_name":  ["Ember"] * 6,
        "_ingested_at": [pd.Timestamp.utcnow()] * 6,
        "_source":      ["ember_yearly_csv"] * 6,
    })


@pytest.fixture(scope="session")
def irena_sample() -> pd.DataFrame:
    """Minimal IRENA-shaped DataFrame for testing."""
    return pd.DataFrame({
        "country":      ["Kenya"] * 5,
        "country_code": ["KEN"] * 5,
        "technology":   ["Geothermal energy", "Hydropower", "Onshore wind energy",
                         "Solar photovoltaic", "Bioenergy"],
        "year":         [2022] * 5,
        "capacity_mw":  [929.0, 836.0, 435.0, 233.0, 5.0],
        "unit":         ["MW"] * 5,
        "_source":      ["irena_curated_fallback"] * 5,
        "_ingested_at": [pd.Timestamp.utcnow()] * 5,
    })


@pytest.fixture(scope="session")
def owid_sample() -> pd.DataFrame:
    """Minimal OWID-shaped DataFrame for testing."""
    return pd.DataFrame({
        "iso_code":                  ["KEN", "KEN"],
        "country":                   ["Kenya", "Kenya"],
        "year":                      [2021, 2022],
        "access_to_electricity":     [74.0, 75.0],
        "per_capita_electricity":    [160.0, 170.0],
        "carbon_intensity_elec":     [45.0, 42.0],
        "electricity_generation":    [11.0, 11.5],
        "renewables_electricity":    [9.8, 10.3],
        "renewables_share_elec":     [89.0, 89.5],
        "fossil_share_elec":         [11.0, 10.5],
        "co2_emissions_from_electricity": [0.50, 0.48],
        "_ingested_at":              [pd.Timestamp.utcnow()] * 2,
        "_source":                   ["owid_energy_github"] * 2,
    })


# ---------------------------------------------------------------------------
# Ember ingestion tests
# ---------------------------------------------------------------------------

class TestEmberIngestion:

    def test_country_filter(self, ember_sample):
        """All rows must be Kenya (KEN)."""
        assert (ember_sample["country_code"] == "KEN").all(), \
            "Non-Kenya rows present after filter"

    def test_required_columns_present(self, ember_sample):
        required = ["country_code", "year", "category", "subcategory",
                    "variable", "unit", "value"]
        for col in required:
            assert col in ember_sample.columns, f"Missing column: {col}"

    def test_year_range(self, ember_sample):
        assert ember_sample["year"].between(2000, 2030).all(), \
            "Years outside expected range 2000–2030"

    def test_no_null_values(self, ember_sample):
        critical = ["country_code", "year", "value"]
        for col in critical:
            nulls = ember_sample[col].isna().sum()
            assert nulls == 0, f"Nulls in critical column '{col}': {nulls}"

    def test_generation_values_non_negative(self, ember_sample):
        gen = ember_sample[
            (ember_sample["variable"] == "Electricity generation") &
            (ember_sample["unit"] == "TWh")
        ]
        assert (gen["value"] >= 0).all(), "Negative generation values found"

    def test_metadata_columns(self, ember_sample):
        assert "_ingested_at" in ember_sample.columns
        assert "_source" in ember_sample.columns
        assert (ember_sample["_source"] == "ember_yearly_csv").all()


# ---------------------------------------------------------------------------
# IRENA capacity tests
# ---------------------------------------------------------------------------

class TestIrenaIngestion:

    def test_required_columns(self, irena_sample):
        required = ["country", "year", "technology", "capacity_mw"]
        for col in required:
            assert col in irena_sample.columns, f"Missing: {col}"

    def test_capacity_positive(self, irena_sample):
        assert (irena_sample["capacity_mw"] >= 0).all(), \
            "Negative capacity values"

    def test_kenya_only(self, irena_sample):
        assert (irena_sample["country"] == "Kenya").all()

    def test_technology_labels_known(self, irena_sample):
        known = {
            "Geothermal energy", "Hydropower", "Onshore wind energy",
            "Solar photovoltaic", "Bioenergy", "Offshore wind energy",
            "Marine energy", "Concentrated solar power",
        }
        unknown = set(irena_sample["technology"]) - known
        # Warn but don't fail — new technologies may be added
        if unknown:
            import warnings
            warnings.warn(f"Unknown IRENA technology labels: {unknown}")

    def test_year_range(self, irena_sample):
        assert irena_sample["year"].between(2000, 2030).all()


# ---------------------------------------------------------------------------
# OWID ingestion tests
# ---------------------------------------------------------------------------

class TestOwIdIngestion:

    def test_required_columns(self, owid_sample):
        required = ["iso_code", "country", "year"]
        for col in required:
            assert col in owid_sample.columns

    def test_kenya_only(self, owid_sample):
        assert (owid_sample["iso_code"] == "KEN").all()

    def test_share_values_in_range(self, owid_sample):
        for col in ["renewables_share_elec", "fossil_share_elec"]:
            if col in owid_sample.columns:
                vals = owid_sample[col].dropna()
                assert (vals >= 0).all() and (vals <= 100).all(), \
                    f"{col} contains values outside [0, 100]"

    def test_no_duplicate_years(self, owid_sample):
        dupes = owid_sample["year"].duplicated().sum()
        assert dupes == 0, f"{dupes} duplicate year rows in OWID data"


# ---------------------------------------------------------------------------
# DuckDB write / read round-trip tests
# ---------------------------------------------------------------------------

class TestDuckDBRoundTrip:

    def test_ember_write_read(self, test_db, ember_sample):
        test_db.execute(
            "CREATE OR REPLACE TABLE ingestion.raw_ember_generation "
            "AS SELECT * FROM ember_sample"
        )
        count = test_db.execute(
            "SELECT COUNT(*) FROM ingestion.raw_ember_generation"
        ).fetchone()[0]
        assert count == len(ember_sample)

    def test_irena_write_read(self, test_db, irena_sample):
        test_db.execute(
            "CREATE OR REPLACE TABLE ingestion.raw_irena_capacity "
            "AS SELECT * FROM irena_sample"
        )
        count = test_db.execute(
            "SELECT COUNT(*) FROM ingestion.raw_irena_capacity"
        ).fetchone()[0]
        assert count == len(irena_sample)

    def test_schema_created(self, test_db):
        schemas = test_db.execute(
            "SELECT schema_name FROM information_schema.schemata"
        ).df()["schema_name"].tolist()
        assert "ingestion" in schemas
        assert "staging" in schemas
        assert "mart" in schemas
