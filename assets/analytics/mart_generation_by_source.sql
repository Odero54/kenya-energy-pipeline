/* @bruin
name: mart.generation_by_source
type: duckdb.sql
materialization:
  type: table
description: >
  Long-format generation table with one row per (year, source_type).
  Includes generation TWh, share %, capacity MW, CUF, and category labels.
  Ideal for Evidence.dev AreaChart/BarChart with series = source_type,
  and for filtering by renewable vs fossil.
depends:
  - staging.stg_energy_unified
columns:
  - name: year
    checks:
      - not_null
  - name: source_type
    checks:
      - not_null
  - name: generation_twh
    checks:
      - not_null
@bruin */

SELECT
    year,
    source_type,
    category,
    COALESCE(color_hex, '#888780')      AS color_hex,
    ROUND(generation_twh, 4)            AS generation_twh,
    ROUND(share_pct, 2)                 AS share_pct,
    ROUND(capacity_mw, 1)               AS capacity_mw,
    ROUND(cuf, 3)                       AS capacity_utilisation_factor,
    is_renewable,

    -- Rank within year by generation (for sorting in charts)
    RANK() OVER (
        PARTITION BY year
        ORDER BY generation_twh DESC
    )                                   AS rank_within_year

FROM staging.stg_energy_unified
WHERE
    generation_twh IS NOT NULL
    AND category IS NOT NULL
    AND category != 'Imports'

ORDER BY year, generation_twh DESC
