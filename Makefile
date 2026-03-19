.PHONY: help setup install validate run run-ingestion run-staging \
        run-analytics geo-export sources dashboard build-dashboard lint test clean

# ─── Help ─────────────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "  Kenya Energy Pipeline — available targets"
	@echo "  ─────────────────────────────────────────"
	@echo "  setup           Install uv, Bruin CLI, Node deps"
	@echo "  install         uv sync (Python deps only)"
	@echo "  validate        Bruin validate all assets"
	@echo "  run             Run full pipeline end-to-end"
	@echo "  run-ingestion   Run only ingestion layer"
	@echo "  run-staging     Run only staging layer"
	@echo "  run-analytics   Run only mart layer"
	@echo "  geo-export      Build geo spatial marts + export GeoJSON"
	@echo "  sources         Refresh Evidence source queries"
	@echo "  dashboard       Start Evidence.dev dev server"
	@echo "  build-dashboard Build Evidence static site"
	@echo "  lint            Ruff linter"
	@echo "  test            pytest"
	@echo "  clean           Remove DB and caches"
	@echo ""

# ─── Setup ────────────────────────────────────────────────────────────────────

setup:
	@echo "→ Checking uv..."
	@which uv || curl -LsSf https://astral.sh/uv/install.sh | sh
	@echo "→ Installing Bruin CLI..."
	@which bruin || curl -fsSL https://raw.githubusercontent.com/bruin-data/bruin/main/install.sh | sh
	@echo "→ Syncing Python deps with uv..."
	uv sync
	@echo "→ Installing Evidence.dev dashboard deps..."
	cd dashboard && npm install
	@echo "✓ Setup complete"

install:
	uv sync

# ─── Pipeline operations ──────────────────────────────────────────────────────

validate:
	bruin validate .

run:
	@echo "→ Running full Kenya energy pipeline..."
	bruin run pipeline.yml
	@echo "✓ Pipeline complete"

run-ingestion:
	@echo "→ Ingestion layer..."
	bruin run assets/ingestion/raw_ember_generation.py
	bruin run assets/ingestion/raw_owid_energy.py
	bruin run assets/ingestion/raw_irena_capacity.py
	bruin run assets/ingestion/raw_energydata_geo.py

run-staging:
	@echo "→ Staging layer..."
	bruin run assets/staging/stg_source_categories.sql
	bruin run assets/staging/stg_ember_generation.sql
	bruin run assets/staging/stg_owid_energy.sql
	bruin run assets/staging/stg_irena_capacity.sql
	bruin run assets/staging/stg_energy_unified.py

run-analytics:
	@echo "→ Analytics / mart layer..."
	bruin run assets/analytics/mart_renewable_mix.sql
	bruin run assets/analytics/mart_capacity_trend.sql
	bruin run assets/analytics/mart_generation_by_source.sql
	bruin run assets/analytics/mart_energy_kpis.sql
	bruin run assets/analytics/mart_geo_infrastructure.sql

geo-export:
	@echo "→ Building geo spatial marts and exporting GeoJSON..."
	uv run python scripts/export_geo_data.py
	@echo "✓ GeoJSON written to dashboard/static/kenya_infrastructure.geojson"

# ─── Evidence.dev dashboard ───────────────────────────────────────────────────

sources:
	@echo "→ Syncing DuckDB file to dashboard sources..."
	cp kenya_energy.db dashboard/sources/kenya_energy/kenya_energy.db
	@echo "→ Refreshing Evidence source queries..."
	cd dashboard && npm run sources

dashboard:
	@echo "→ Starting Evidence.dev dev server at http://localhost:3000"
	cp kenya_energy.db dashboard/sources/kenya_energy/kenya_energy.db
	cd dashboard && npm run dev

build-dashboard:
	uv run python scripts/export_geo_data.py
	cp kenya_energy.db dashboard/sources/kenya_energy/kenya_energy.db
	cd dashboard && npm run sources && npm run build

# ─── Full refresh ─────────────────────────────────────────────────────────────

refresh: run sources
	@echo "✓ Pipeline + dashboard sources refreshed"

# ─── Dev utilities ────────────────────────────────────────────────────────────

lint:
	uv run ruff check assets/

test:
	uv run pytest tests/ -v --cov=assets

shell:
	@echo "→ Opening DuckDB shell..."
	uv run python -c "import duckdb; duckdb.connect('kenya_energy.db').execute('.open').fetchall()" \
	  || duckdb kenya_energy.db

clean:
	rm -f kenya_energy.db
	rm -rf dashboard/.evidence/ dashboard/node_modules/.cache/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	@echo "✓ Cleaned"
