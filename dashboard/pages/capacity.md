---
title: Installed Capacity
---

# Installed Capacity

Tracking Kenya's renewable power capacity growth from IRENA statistics,
broken down by technology: Geothermal, Hydro, Wind, Solar, and Bioenergy.

---

## Total renewable capacity (MW)

```sql capacity
SELECT * FROM kenya_energy.capacity_trend ORDER BY year
```

<AreaChart
  data={capacity}
  x="year"
  y={["geothermal_mw", "hydro_mw", "wind_mw", "solar_mw", "bioenergy_mw"]}
  labels={["Geothermal", "Hydro", "Wind", "Solar", "Bioenergy"]}
  title="Installed renewable capacity by technology (MW)"
  yAxisTitle="MW"
  type="stacked"
/>

---

## MW added per year

```sql additions
SELECT
  year,
  mw_added_yoy,
  capacity_growth_pct
FROM kenya_energy.capacity_trend
WHERE mw_added_yoy IS NOT NULL
ORDER BY year
```

<BarChart
  data={additions}
  x="year"
  y="mw_added_yoy"
  title="New renewable capacity added year-on-year (MW)"
  yAxisTitle="MW added"
/>

---

## Capacity by technology — latest year

```sql latest_cap
SELECT * FROM kenya_energy.capacity_trend ORDER BY year DESC LIMIT 1
```

<BigValue data={latest_cap} value="total_renewable_mw"  title="Total renewable (MW)" fmt="num0" />
<BigValue data={latest_cap} value="geothermal_mw"       title="Geothermal (MW)"      fmt="num0" />
<BigValue data={latest_cap} value="hydro_mw"            title="Hydro (MW)"           fmt="num0" />
<BigValue data={latest_cap} value="wind_mw"             title="Wind (MW)"            fmt="num0" />
<BigValue data={latest_cap} value="solar_mw"            title="Solar (MW)"           fmt="num0" />

---

## Capacity utilisation factors (CUF)

CUF = actual generation ÷ (installed capacity × 8,760 h). Values near 1.0
indicate a technology is running near its nameplate rating.

```sql cuf
SELECT
  year,
  geothermal_cuf,
  hydro_cuf,
  wind_cuf,
  solar_cuf
FROM kenya_energy.capacity_trend
WHERE geothermal_cuf IS NOT NULL
ORDER BY year
```

<LineChart
  data={cuf}
  x="year"
  y={["geothermal_cuf", "hydro_cuf", "wind_cuf", "solar_cuf"]}
  labels={["Geothermal", "Hydro", "Wind", "Solar"]}
  title="Capacity utilisation factor by technology"
  yAxisTitle="CUF (0–1)"
  yMin=0
  yMax=1
/>

---

## Full capacity table

<DataTable data={capacity} search=true downloadable=true />
