/* @bruin
name: staging.stg_source_categories
type: duckdb.sql
materialization:
  type: table
description: >
  Loads the source_categories seed CSV into the staging schema.
  Provides a lookup table mapping energy source types to categories,
  renewable flags, and display colors for the dashboard.
@bruin */

SELECT
    TRIM(source_type)    AS source_type,
    TRIM(category)       AS category,
    TRIM(subcategory)    AS subcategory,
    CAST(
        CASE
            WHEN LOWER(TRIM(CAST(is_renewable AS VARCHAR))) IN ('true','1','yes') THEN TRUE
            ELSE FALSE
        END
    AS BOOLEAN)          AS is_renewable,
    TRIM(color_hex)      AS color_hex
FROM read_csv_auto('seeds/source_categories.csv', header=true)
