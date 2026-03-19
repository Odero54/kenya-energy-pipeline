---
title: Generation by Source
---

# Generation by Source — Explorer

Drill into Kenya's electricity generation at the source-type level.
Filter by decade, renewable vs fossil, and explore capacity utilisation
for each technology.

---

```sql by_source
SELECT * FROM kenya_energy.generation_by_source ORDER BY year, generation_twh DESC
```

```sql decades
SELECT DISTINCT decade FROM kenya_energy.energy_kpis ORDER BY decade
```

## Filter by decade

<Dropdown
  data={decades}
  name="decade_filter"
  value="decade"
  title="Decade"
  defaultValue="2020s"
/>

```sql filtered_source
SELECT *
FROM kenya_energy.generation_by_source gs
JOIN kenya_energy.energy_kpis k ON gs.year = k.year
WHERE k.decade = '${inputs.decade_filter.value}'
ORDER BY gs.year, gs.generation_twh DESC
```

<BarChart
  data={filtered_source}
  x="year"
  y="generation_twh"
  series="source_type"
  title="Generation by source type — {inputs.decade_filter.value}"
  yAxisTitle="TWh"
  type="stacked"
/>

---

## Renewable vs fossil split — all years

```sql renew_vs_fossil
SELECT
  year,
  SUM(CASE WHEN is_renewable THEN generation_twh ELSE 0 END) AS renewable_twh,
  SUM(CASE WHEN NOT is_renewable THEN generation_twh ELSE 0 END) AS fossil_twh
FROM kenya_energy.generation_by_source
GROUP BY year
ORDER BY year
```

<AreaChart
  data={renew_vs_fossil}
  x="year"
  y={["renewable_twh","fossil_twh"]}
  labels={["Renewables","Fossil fuels"]}
  title="Renewable vs fossil generation (TWh)"
  type="stacked"
/>

---

## Capacity utilisation by source type

```sql cuf_by_source
SELECT
  year,
  source_type,
  capacity_utilisation_factor
FROM kenya_energy.generation_by_source
WHERE capacity_utilisation_factor IS NOT NULL
  AND is_renewable = true
ORDER BY year, source_type
```

<LineChart
  data={cuf_by_source}
  x="year"
  y="capacity_utilisation_factor"
  series="source_type"
  title="Capacity utilisation factor by renewable source"
  yAxisTitle="CUF (0–1)"
  yMin=0
  yMax=1
/>

---

## Full source detail table

<DataTable data={by_source} search=true downloadable=true />
