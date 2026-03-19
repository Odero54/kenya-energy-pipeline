SELECT
    station_name,
    county,
    owner,
    manned,
    feature_type,
    ROUND(lat, 5) AS lat,
    ROUND(lng, 5) AS lng
FROM mart.geo_map_substations
WHERE lat IS NOT NULL
  AND lng IS NOT NULL
ORDER BY station_name
