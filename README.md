# Kenya Renewable Energy Data Pipeline

A full end-to-end data pipeline built with **Bruin** that ingests Kenya energy data
from four open-source datasets, transforms it through a medallion architecture
(Bronze → Silver → Gold), and serves it via an **Evidence.dev** dashboard.

---

## Architecture

```
Data Sources              Ingestion (Bronze)            Staging (Silver)
─────────────────         ──────────────────────        ──────────────────────────────
Ember Climate       ───►  raw_ember_generation.py  ───► stg_ember_generation.sql
Our World in Data   ───►  raw_owid_energy.py        ───► stg_owid_energy.sql
IRENA               ───►  raw_irena_capacity.py     ───► stg_irena_capacity.sql
EnergyData.info     ───►  raw_energydata_geo.py     ───► (flat geo features)
seeds/              ───►  (CSV seed)                ───► stg_source_categories.sql
                                                         stg_energy_unified.py  ──┐
                                                                                   │
Analytics (Gold)          Dashboard                                                │
──────────────────         ──────────────────────────────────────────────────────◄─┘
mart_renewable_mix   ───► pages/index.md      (Generation mix, 2030 progress)
mart_capacity_trend  ───► pages/capacity.md   (Installed MW, CUF)
mart_energy_kpis     ───► pages/kpis.md       (Access, per-capita, CO₂)
mart_generation_by_source ► pages/sources.md  (Source explorer, filters)
mart_geo_infrastructure ─► (Infrastructure summary)
```

**Storage:** Single DuckDB file (`kenya_energy.db`) with schemas:
`ingestion` → `staging` → `mart`

---

## Data sources

| Source | What it provides | License | Update cadence |
|---|---|---|---|
| [Ember Climate](https://ember-energy.org/data/yearly-electricity-data/) | Generation (TWh), capacity (MW), emissions, demand — 215 countries | CC BY 4.0 | Annual + monthly API |
| [Our World in Data](https://github.com/owid/energy-data) | 130+ indicators: per-capita kWh, carbon intensity, renewable share, access % | CC BY 4.0 | ~Monthly |
| [IRENA](https://www.irena.org/Data) | Installed renewable capacity (MW) by technology, 2000–present | Free, attribution | Annual (March) |
| [EnergyData.info](https://energydata.info/dataset?vocab_country_names=KEN) | Transmission network, substations, solar radiation stations (GeoJSON/CSV) | CC BY 4.0 | Ad hoc |

---

## Project structure

```
kenya-energy-pipeline/
│
├── .bruin.yml                          # DuckDB connection config
├── pipeline.yml                        # Pipeline name + schedule
├── pyproject.toml                      # uv / Python deps (uv sync)
├── requirements.txt                    # Legacy stub — see pyproject.toml
├── Makefile                            # Common operations
│
├── seeds/
│   └── source_categories.csv           # Energy source → category/color lookup
│
├── assets/
│   ├── ingestion/                      # Bronze layer — raw data fetch
│   │   ├── raw_ember_generation.py     # Ember yearly CSV (Kenya filtered)
│   │   ├── raw_owid_energy.py          # OWID GitHub CSV (Kenya filtered)
│   │   ├── raw_irena_capacity.py       # IRENA capacity (API + curated fallback)
│   │   └── raw_energydata_geo.py       # EnergyData.info CKAN API (GeoJSON/CSV)
│   │
│   ├── staging/                        # Silver layer — clean + normalise
│   │   ├── stg_source_categories.sql   # Seed loader
│   │   ├── stg_ember_generation.sql    # Ember: clean, flag renewable/fossil
│   │   ├── stg_owid_energy.sql         # OWID: select & round KPI columns
│   │   ├── stg_irena_capacity.sql      # IRENA: map tech labels, validate MW
│   │   └── stg_energy_unified.py       # JOIN Ember + IRENA + OWID, add CUF
│   │
│   └── analytics/                      # Gold layer — business marts
│       ├── mart_renewable_mix.sql       # Yearly gen by source + share %
│       ├── mart_capacity_trend.sql      # MW trend + YoY additions + CUF
│       ├── mart_energy_kpis.sql         # KPIs: access, per-capita, CO₂, 2030 gap
│       ├── mart_generation_by_source.sql # Long-format for charting
│       └── mart_geo_infrastructure.sql  # Infrastructure feature counts
│
└── dashboard/                          # Evidence.dev app
    ├── sources/kenya_energy/           # DuckDB connection + source queries
    │   ├── connection.yaml
    │   ├── renewable_mix.sql
    │   ├── capacity_trend.sql
    │   ├── energy_kpis.sql
    │   └── generation_by_source.sql
    └── pages/
        ├── index.md                    # Main dashboard: mix + 2030 progress
        ├── capacity.md                 # Installed capacity + CUF
        ├── kpis.md                     # Access, per-capita, carbon intensity
        └── sources.md                  # Source-level explorer with filters
```

---

## Quick start

### 1. Install dependencies

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install Bruin CLI
curl -fsSL https://raw.githubusercontent.com/bruin-data/bruin/main/install.sh | sh

# Sync all Python deps from pyproject.toml
uv sync

# Node (for Evidence.dev dashboard) — requires Node.js >= 18
# https://nodejs.org
cd dashboard && npm install
```

Or use the Makefile one-liner:
```bash
make setup
```

### 2. Validate the pipeline

```bash
bruin validate kenya-energy-pipeline/
# or
make validate
```

### 3. Run the full pipeline

```bash
bruin run kenya-energy-pipeline/pipeline.yml
# or
make run
```

Bruin resolves the dependency graph automatically and executes:
`ingestion → staging → analytics`

### 4. Run a single asset (and all its dependents)

```bash
bruin run assets/staging/stg_energy_unified.py --downstream
```

### 5. Run layer by layer

```bash
make run-ingestion   # Bronze: fetch raw data
make run-staging     # Silver: clean + join
make run-analytics   # Gold: business marts
```

---

## Dashboard

### Setup (first time only)

```bash
make dashboard-setup
```

This scaffolds a blank Evidence.dev project and symlinks `kenya_energy.db`
into the sources folder.

### Refresh data

```bash
# After running the Bruin pipeline:
make dashboard-sources
```

### Development server

```bash
make dashboard-dev
# Opens http://localhost:3000
```

### Dashboard pages

| Page | URL | What it shows |
|---|---|---|
| Overview | `/` | Generation mix, 2030 target progress, YoY change |
| Installed Capacity | `/capacity` | MW by technology, additions, CUF |
| Energy KPIs | `/kpis` | Access rate, per-capita, carbon intensity |
| Source Explorer | `/sources` | Filterable by decade, renewable vs fossil |

---

## Full refresh workflow

```bash
# After new data is published by Ember/OWID/IRENA:
bruin run kenya-energy-pipeline/pipeline.yml && \
cd kenya-energy-pipeline/dashboard && npm run sources
```

---

## Key metrics produced

| Metric | Mart | Description |
|---|---|---|
| `renewable_share_pct` | `mart.renewable_mix` | Renewable % of total generation |
| `renewable_share_yoy_pp` | `mart.renewable_mix` | YoY change in renewable share (pp) |
| `total_renewable_mw` | `mart.capacity_trend` | Total installed renewable capacity |
| `mw_added_yoy` | `mart.capacity_trend` | New MW installed per year |
| `capacity_utilisation_factor` | `mart.capacity_trend` | CUF per technology |
| `access_to_electricity_pct` | `mart.energy_kpis` | % population with electricity |
| `per_capita_elec_kwh` | `mart.energy_kpis` | kWh per person per year |
| `carbon_intensity_gco2_kwh` | `mart.energy_kpis` | Grid carbon intensity |
| `gap_to_100pct_target` | `mart.energy_kpis` | Percentage points to 2030 goal |

---

## Data quality checks

Every asset includes Bruin quality checks. Examples:

- `not_null` on `year`, `source_type`, `generation_twh`
- `accepted_range` on year (2000–2030), `renewable_share_pct` (0–100)
- `unique` on `year` in mart tables
- Negative generation values are nullified for non-import rows
- CUF is clamped to [0, 1]

Run checks only (without re-executing assets):

```bash
bruin run --only-checks kenya-energy-pipeline/pipeline.yml
```

---

## uv workflows

```bash
# Install / sync all deps
uv sync

# Add a runtime dependency
uv add requests

# Add a dev-only dependency
uv add --dev pytest-cov

# Run any command in the managed venv
uv run python assets/ingestion/raw_ember_generation.py
uv run pytest tests/ -v
uv run ruff check assets/

# Show dependency tree
uv tree

# Upgrade all deps to latest compatible versions
uv lock --upgrade
```

All Python assets in Bruin are invoked through `uv run` automatically
when you set the `BRUIN_PYTHON` environment variable:

```bash
export BRUIN_PYTHON="uv run python"
bruin run pipeline.yml
```

Or, set it once in your shell profile (`~/.bashrc` / `~/.zshrc`):

```bash
echo 'export BRUIN_PYTHON="uv run python"' >> ~/.zshrc
```

---

## Testing

```bash
# Run all tests
uv run pytest tests/ -v

# Run a single test file
uv run pytest tests/test_ingestion.py -v

# With coverage report
uv run pytest tests/ --cov=assets --cov-report=term-missing

# Lint with ruff
uv run ruff check assets/

# Type-check with mypy
uv run mypy assets/
```

Tests cover:
- **`test_ingestion.py`** — Kenya-only filter, required columns, value ranges, DuckDB round-trip
- **`test_staging.py`** — Normalisation logic, CUF bounds, renewable flag, year deduplication
- **`test_marts.py`** — Aggregation totals, YoY calculations, share % sums, capacity growth

---



All source data is open access:
- Ember Climate — CC BY 4.0
- Our World in Data — CC BY 4.0
- IRENA — Free for non-commercial use with attribution
- EnergyData.info (World Bank) — CC BY 4.0

Please credit sources when publishing dashboards or reports derived from this pipeline.
