---
title: Kenya Renewable Energy Dashboard
---

# Kenya Renewable Energy Dashboard

Kenya targets **100% renewable electricity by 2030**. This dashboard tracks
progress across generation mix, installed capacity, and key energy KPIs using
data from Ember Climate, IRENA, Our World in Data, and EnergyData.info.

Pages: [Generation mix](/) · [Installed capacity](/capacity) · [Sources explorer](/sources) · [KPIs & access](/kpis) · [Geo infrastructure](/geo-infrastructure)

---

## At a glance

```sql latest
SELECT
  *,
  LAG(renewable_share_pct) OVER (ORDER BY year) AS prev_renewable_share_pct
FROM kenya_energy.energy_kpis
ORDER BY year DESC
LIMIT 1
```

<BigValue
  data={latest}
  value="renewable_share_pct"
  title="Renewable share (%)"
  fmt="0.0"
  comparison="prev_renewable_share_pct"
  comparisonTitle="vs prior year"
/>

<BigValue
  data={latest}
  value="total_generation_twh"
  title="Total generation (TWh)"
  fmt="0.00"
/>

<BigValue
  data={latest}
  value="per_capita_elec_kwh"
  title="Per capita (kWh)"
  fmt="num0"
/>

<BigValue
  data={latest}
  value="access_to_electricity_pct"
  title="Electricity access (%)"
  fmt="0.0"
/>

<BigValue
  data={latest}
  value="carbon_intensity_gco2_kwh"
  title="Carbon intensity (gCO₂/kWh)"
  fmt="0.0"
/>

<BigValue
  data={latest}
  value="gap_to_100pct_target"
  title="Gap to 2030 target (pp)"
  fmt="num1"
/>

---

## Generation mix over time

```sql renewable_mix
SELECT * FROM kenya_energy.renewable_mix ORDER BY year
```

<AreaChart
  data={renewable_mix}
  x="year"
  y={["geothermal_twh", "hydro_twh", "wind_twh", "solar_twh", "bioenergy_twh", "fossil_twh"]}
  title="Electricity generation by source (TWh)"
  yAxisTitle="TWh"
  type="stacked"
/>

<LineChart
  data={renewable_mix}
  x="year"
  y="renewable_share_pct"
  title="Renewable share of total generation (%)"
  yAxisTitle="% of total"
  yMin=0
  yMax=100
>
  <ReferenceLine y=100 label="2030 target: 100%" color="#E8593C" />
</LineChart>

---

## Progress to 2030 target

```sql target_progress
SELECT
  year,
  renewable_share_pct,
  100 - renewable_share_pct AS gap_pp,
  target_progress_ratio
FROM kenya_energy.energy_kpis
ORDER BY year
```

<BarChart
  data={target_progress}
  x="year"
  y={["renewable_share_pct", "gap_pp"]}
  title="Progress toward 100% renewable electricity by 2030"
  type="stacked"
  yAxisTitle="%"
  yMax=100
/>

---

## Year-on-year renewable share change

```sql yoy
SELECT year, renewable_share_yoy_pp
FROM kenya_energy.renewable_mix
WHERE renewable_share_yoy_pp IS NOT NULL
ORDER BY year
```

<BarChart
  data={yoy}
  x="year"
  y="renewable_share_yoy_pp"
  title="Year-on-year change in renewable share (percentage points)"
  yAxisTitle="pp change"
/>

---

## Geothermal dominance

Kenya is one of the world's top geothermal producers. The chart below shows
geothermal's share of total generation versus all other renewables.

```sql geo_vs_others
SELECT
  year,
  geothermal_twh,
  hydro_twh + wind_twh + solar_twh + bioenergy_twh AS other_renewables_twh,
  fossil_twh
FROM kenya_energy.renewable_mix
ORDER BY year
```

<AreaChart
  data={geo_vs_others}
  x="year"
  y={["geothermal_twh", "other_renewables_twh", "fossil_twh"]}
  title="Geothermal vs other renewables vs fossil (TWh)"
  type="stacked"
/>
