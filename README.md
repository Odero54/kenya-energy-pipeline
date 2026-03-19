# Kenya Renewable Energy Data Pipeline

A full end-to-end data pipeline built with **Bruin** that ingests Kenya energy data
from four open-source datasets, transforms it through a medallion architecture
(Bronze → Silver → Gold), and serves it via an **Evidence.dev** dashboard.

**Live dashboard:** https://odero54.github.io/kenya-energy-pipeline/  
**Repository:** https://github.com/Odero54/kenya-energy-pipeline/

---

## Architecture

![Kenya renewable energy — bruin data pipeline](docs/architecture.png)

The pipeline follows a **medallion architecture** across four layers, all stored in a single DuckDB file:

| Layer | Schema | What happens |
|---|---|---|
| **Bronze** | `ingestion` | Raw data fetched from Ember, OWID, IRENA, EnergyData.info |
| **Silver** | `staging` | Cleaned, normalised, joined — quality checks enforced |
| **Gold** | `mart` | Business-ready aggregations: mix, capacity, KPIs, geo |
| **Serving** | — | Evidence.dev static dashboard, deployed via GitHub Pages |

**Storage:** Single DuckDB file (`kenya_energy.db`) — schemas: `ingestion` → `staging` → `mart`

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
├── Makefile                            # Common operations
├── docs/
│   └── architecture.png               # Pipeline architecture diagram
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
├── tests/
│   ├── test_ingestion.py               # Kenya-only filter, columns, value ranges
│   ├── test_staging.py                 # Normalisation, CUF bounds, deduplication
│   └── test_marts.py                   # Aggregations, YoY, share % sums
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
        ├── sources.md                  # Source-level explorer with filters
        └── geo-infrastructure.md       # Infrastructure map: substations, lines
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
bruin validate .
# or
make validate
```

### 3. Run the full pipeline

```bash
export BRUIN_PYTHON="uv run python"
bruin run pipeline.yml
# or
make run
```

Bruin resolves the dependency graph automatically and executes:
`ingestion → staging → analytics`

### 4. Run a single asset and all its dependents

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

### Development server

```bash
make dashboard      # copies DB, starts server at http://localhost:3000
```

### Refresh data after pipeline run

```bash
make sources        # copies DB → dashboard/sources/ and runs npm run sources
```

### Build static site

```bash
make build-dashboard
```

### Dashboard pages

| Page | URL | What it shows |
|---|---|---|
| Overview | `/` | Generation mix, 2030 target progress, YoY change |
| Installed Capacity | `/capacity` | MW by technology, additions, CUF |
| Energy KPIs | `/kpis` | Access rate, per-capita, carbon intensity |
| Source Explorer | `/sources` | Filterable by decade, renewable vs fossil |
| Geo Infrastructure | `/geo-infrastructure` | Transmission network, substations map |

---

## CI/CD — GitHub Actions

The pipeline runs automatically via GitHub Actions on every push to `main` and
rebuilds daily at 06:00 UTC. Each run:

1. Installs Python 3.11, uv, Bruin CLI, and Node.js 20
2. Validates all pipeline assets (`bruin validate .`)
3. Runs the full Bruin pipeline (`bruin run pipeline.yml`)
4. Builds the Evidence.dev static dashboard (`npm run build`)
5. Deploys to GitHub Pages

Trigger a manual run from the [Actions tab](https://github.com/Odero54/kenya-energy-pipeline/actions),
or push locally with:

```bash
make deploy
```

---

## Full refresh workflow

```bash
# After new data is published by Ember/OWID/IRENA:
make refresh        # = make run && make sources
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

Every asset includes Bruin quality checks:

- `not_null` on `year`, `source_type`, `generation_twh`
- `accepted_range` on year (2000–2030), `renewable_share_pct` (0–100)
- `unique` on `year` in mart tables
- Negative generation values are nullified for non-import rows
- CUF is clamped to [0, 1]

Run checks only (without re-executing assets):

```bash
bruin run --only-checks pipeline.yml
```

---

## uv workflows

```bash
uv sync                                          # install / sync all deps
uv add requests                                  # add a runtime dependency
uv add --dev pytest-cov                          # add a dev-only dependency
uv run python assets/ingestion/raw_ember_generation.py
uv run pytest tests/ -v
uv run ruff check assets/
uv tree                                          # dependency tree
uv lock --upgrade                                # upgrade all to latest
```

Set Bruin to use uv's Python (add to `~/.zshrc` to make permanent):

```bash
export BRUIN_PYTHON="uv run python"
```

---

## Testing

```bash
uv run pytest tests/ -v                          # all tests
uv run pytest tests/test_ingestion.py -v         # single file
uv run pytest tests/ --cov=assets --cov-report=term-missing
uv run ruff check assets/                        # lint
uv run mypy assets/                              # type-check
```

Tests cover:

- `test_ingestion.py` — Kenya-only filter, required columns, value ranges, DuckDB round-trip
- `test_staging.py` — Normalisation logic, CUF bounds, renewable flag, year deduplication
- `test_marts.py` — Aggregation totals, YoY calculations, share % sums, capacity growth

---

## Licenses

All source data is open access:

- Ember Climate — CC BY 4.0
- Our World in Data — CC BY 4.0
- IRENA — Free for non-commercial use with attribution
- EnergyData.info (World Bank) — CC BY 4.0

Please credit sources when publishing dashboards or reports derived from this pipeline.