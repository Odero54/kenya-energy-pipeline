---
title: Geo Infrastructure
---

# Kenya Energy Geo Infrastructure

Physical energy infrastructure sourced from [EnergyData.info](https://energydata.info):
transmission corridors (66 kV–220 kV), substations, and solar measurement stations across Kenya.

---

```sql geo
SELECT * FROM kenya_energy.geo_infrastructure ORDER BY dataset, feature_count DESC
```

## Infrastructure summary

```sql totals
SELECT
  COUNT(DISTINCT dataset)       AS total_datasets,
  COUNT(DISTINCT resource_name) AS total_layers,
  SUM(feature_count)            AS total_features
FROM kenya_energy.geo_infrastructure
```

<BigValue data={totals} value="total_datasets"  title="Datasets"       fmt="num0" />
<BigValue data={totals} value="total_layers"    title="Layers"         fmt="num0" />
<BigValue data={totals} value="total_features"  title="Total features" fmt="num0" />

---

## Infrastructure Map

High-voltage transmission corridors (66 kV–220 kV) and substations.
Toggle layers with the control panel. Click any feature for details.

<iframe
  src="/map.html"
  style="width:100%; height:580px; border:0; border-radius:8px; box-shadow:0 2px 8px rgba(0,0,0,0.12);"
  title="Kenya Energy Infrastructure Map"
  loading="lazy"
></iframe>

---

## Substation locations

83 KPLC substations with county and ownership details.

```sql substation_table
SELECT
  station_name,
  county,
  owner,
  manned,
  ROUND(lat,4) AS latitude,
  ROUND(lng,4) AS longitude
FROM kenya_energy.geo_map_substations
ORDER BY county, station_name
```

<DataTable data={substation_table} search=true downloadable=true rows=10 />

---

## Feature count by dataset

```sql by_dataset
SELECT
  dataset,
  SUM(feature_count) AS feature_count
FROM kenya_energy.geo_infrastructure
GROUP BY dataset
ORDER BY feature_count DESC
```

<BarChart
  data={by_dataset}
  x="dataset"
  y="feature_count"
  title="Total features per dataset"
  yAxisTitle="Feature count"
  swapXY=true
/>

---

## Feature count by geometry type

```sql by_geom
SELECT
  geometry_type,
  SUM(feature_count) AS feature_count
FROM kenya_energy.geo_infrastructure
GROUP BY geometry_type
ORDER BY feature_count DESC
```

<BarChart
  data={by_geom}
  x="geometry_type"
  y="feature_count"
  title="Features by geometry type"
  yAxisTitle="Feature count"
/>

---

## Breakdown by resource layer

Each row is one named layer (resource) within a dataset.
Geometry type indicates whether the layer contains lines (e.g. transmission corridors),
points (e.g. substations, solar stations), or polygons.

```sql by_resource
SELECT
  dataset,
  resource_name,
  geometry_type,
  feature_count
FROM kenya_energy.geo_infrastructure
ORDER BY dataset, feature_count DESC
```

<DataTable data={by_resource} search=true downloadable=true />
