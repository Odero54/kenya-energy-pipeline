---
title: Energy KPIs
---

# Energy KPIs & Access

Contextual indicators for Kenya's energy system: electricity access rate,
per-capita consumption, carbon intensity, and primary energy mix.
Data sourced from Our World in Data (OWID) combining IEA, Ember, and World Bank.

---

## Electricity access over time

```sql kpis
SELECT * FROM kenya_energy.energy_kpis ORDER BY year
```

<LineChart
  data={kpis}
  x="year"
  y="access_to_electricity_pct"
  title="Population with access to electricity (%)"
  yAxisTitle="%"
  yMin=0
  yMax=100
/>

---

## Per-capita electricity consumption

```sql per_capita
SELECT year, per_capita_elec_kwh FROM kenya_energy.energy_kpis ORDER BY year
```

<LineChart
  data={per_capita}
  x="year"
  y="per_capita_elec_kwh"
  title="Per-capita electricity consumption (kWh/person)"
  yAxisTitle="kWh"
/>

---

## Carbon intensity of electricity

Lower values indicate a cleaner grid. Kenya's geothermal-heavy mix keeps
carbon intensity well below the global average.

```sql carbon
SELECT year, carbon_intensity_gco2_kwh, co2_from_elec_mtco2
FROM kenya_energy.energy_kpis
WHERE carbon_intensity_gco2_kwh IS NOT NULL
ORDER BY year
```

<LineChart
  data={carbon}
  x="year"
  y="carbon_intensity_gco2_kwh"
  title="Carbon intensity of electricity generation (gCO₂/kWh)"
  yAxisTitle="gCO₂/kWh"
/>

<BarChart
  data={carbon}
  x="year"
  y="co2_from_elec_mtco2"
  title="CO₂ emissions from electricity generation (MtCO₂)"
  yAxisTitle="MtCO₂"
/>

---

## Renewable vs fossil share trends

```sql shares
SELECT year, renewable_share_pct, fossil_share_pct
FROM kenya_energy.energy_kpis
ORDER BY year
```

<LineChart
  data={shares}
  x="year"
  y={["renewable_share_pct", "fossil_share_pct"]}
  labels={["Renewable share (%)", "Fossil share (%)"]}
  title="Renewable vs fossil share of electricity generation"
  yAxisTitle="%"
/>

---

## Full KPI table

```sql all_kpis
SELECT
  year,
  renewable_share_pct,
  access_to_electricity_pct,
  per_capita_elec_kwh,
  carbon_intensity_gco2_kwh,
  co2_from_elec_mtco2,
  total_generation_twh,
  decade
FROM kenya_energy.energy_kpis
ORDER BY year DESC
```

<DataTable data={all_kpis} search=true downloadable=true />
