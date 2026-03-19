/* @bruin
name: staging.stg_ember_generation
type: duckdb.sql
materialization:
  type: table
description: >
  Cleans and normalises Ember yearly electricity data for Kenya.
  Pivots long-format rows into typed records per year/source_type/category.
  Applies quality checks on generation values and year range.
depends:
  - ingestion.raw_ember_generation
columns:
  - name: year
    checks:
      - name: not_null
      - accepted_range:
          min: 2000
          max: 2030
  - name: variable
    checks:
      - name: not_null
  - name: unit
    checks:
      - name: not_null
  - name: value_clean
    checks:
      - name: not_null
@bruin */

SELECT
    CAST(year AS INTEGER)                               AS year,

    -- Normalise category / subcategory labels
    TRIM(category)                                      AS category,
    TRIM(subcategory)                                   AS subcategory,
    TRIM(variable)                                      AS variable,
    TRIM(unit)                                          AS unit,

    -- Clean numeric value — nullify obvious outliers (negative generation)
    CASE
        WHEN CAST(value AS DOUBLE) < 0
         AND unit = 'TWh'
         AND TRIM(variable) NOT IN ('Net Imports', 'Demand')
        THEN NULL
        ELSE ROUND(CAST(value AS DOUBLE), 6)
    END                                                 AS value_clean,

    -- Convenience flags
    CASE
        WHEN LOWER(category) LIKE '%renewable%'        THEN TRUE
        ELSE FALSE
    END                                                 AS is_renewable,

    CASE
        WHEN LOWER(category) LIKE '%fossil%'
          OR LOWER(subcategory) IN ('gas', 'oil', 'coal', 'other fossil')
        THEN TRUE
        ELSE FALSE
    END                                                 AS is_fossil,

    'Ember'                                             AS data_source,
    _ingested_at

FROM ingestion.raw_ember_generation

WHERE
    year IS NOT NULL
    AND CAST(year AS INTEGER) BETWEEN 2000 AND 2030
    AND value IS NOT NULL
