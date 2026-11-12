# Data dictionary - BI semantic layer

Views of the schema `psi_bi` in PostgreSQL, registered as Superset datasets by the Superset stack
([`superset/`](../../../superset/README.md)). Superset reads only these views, with a read-only role.
Generated from `superset/assets/datasets.yml` and the real view columns (`superset/assets/bi_dictionary.py`);
do not edit by hand. The Gold tables behind the views are in the [Gold data dictionary](../README.md).

Download: [Excel](bi_dictionary.xlsx) - [CSV columns](bi_columns.csv) - [CSV metrics](bi_metrics.csv)

| Dataset | Columns | Metrics | Time column | Description |
|---|---|---|---|---|
| [municipality_profile](#municipality_profile) | 32 | 8 | - | One row per municipality - indicators, CVLI rate, hotspots, IVSC and LISA cluster in the analysis window. |
| [municipality_profile_items](#municipality_profile_items) | 7 | 2 | - | Profile of each municipality as label and value rows (population, IVSC, Z-score, hotspot, LISA cluster, IDHM), for the profile table of the dashboard. |
| [state_profile](#state_profile) | 14 | 5 | - | One row per state - lethal violence, vehicle crimes and drug seizures in the analysis window. |
| [cvli_monthly_state](#cvli_monthly_state) | 9 | 2 | month | One row per state and month - CVLI victims, including victims without informed municipality. |
| [cvli_monthly_municipality](#cvli_monthly_municipality) | 15 | 6 | month | Main dataset of the dashboard - one row per area and month on a complete grid. Areas are the municipalities with SINESP records plus one Not assigned row per state, so sums by state equal the state facts. |
| [cvli_kpis](#cvli_kpis) | 11 | 5 | - | One row - national KPIs of the analysis window and the change against the previous window of the same length. |

## municipality_profile

One row per municipality - indicators, CVLI rate, hotspots, IVSC and LISA cluster in the analysis window.

| Column | Label | Type | Description |
|---|---|---|---|
| `municipality_code` |  | text | IBGE 7-digit municipality code (primary key). |
| `municipality_name` |  | text | Official municipality name (IBGE). Names repeat across states: always filter or show state_code too. |
| `state_code` |  | text | Two-letter state code (UF), e.g. SP, PE. |
| `state_name` |  | text | State name. |
| `region_name` |  | text | Brazilian region: Norte, Nordeste, Sudeste, Sul, Centro-Oeste. |
| `municipality_label` | Municipality | text | Municipality name and state code, e.g. Bom Jesus (PI). Names repeat across states. |
| `population` |  | bigint | Estimated resident population (IBGE), year in population_year. |
| `population_year_label` |  | text | Reference year of the population, as text. |
| `gdp_per_capita_brl` |  | double precision | GDP per capita in BRL. |
| `economic_tier` |  | text | National tertile of GDP per capita: Low, Medium, High. |
| `idhm` |  | double precision | Municipal Human Development Index (0 to 1), year in idhm_year (2010, latest municipal edition). |
| `fleet_total` |  | bigint | Registered vehicle fleet (SENATRAN), month in fleet_reference. |
| `cvli_victims` |  | bigint | CVLI victims in the analysis window: intentional homicide, feminicide, robbery followed by death and bodily injury followed by death. Null means no SINESP record. |
| `has_sinesp_records` |  | boolean | True when the municipality has SINESP records in the window. |
| `cvli_rate_100k` |  | double precision | CVLI victims per 100,000 inhabitants, annualized. Main lethal violence indicator. |
| `cvli_z_score_state` |  | double precision | Z-Score of cvli_rate_100k against municipalities of the same state with population >= 20,000. Null for smaller municipalities. |
| `cvli_z_score_national` |  | double precision | Z-Score of cvli_rate_100k against all municipalities in Brazil with population >= 20,000. Null for smaller municipalities. |
| `is_state_hotspot` |  | boolean | True when cvli_z_score_state >= 2 (stands out within its state). |
| `is_national_hotspot` |  | boolean | True when cvli_z_score_national >= 2 (among the highest rates in Brazil). |
| `ivsc_score` |  | double precision | Social & Criminal Vulnerability Index, 0 to 1: 0.4 x crime_component + 0.6 x social_component. |
| `months_covered` |  | integer | Number of months in the analysis window. |
| `window_start` |  | date | First month of the analysis window. |
| `window_end` |  | date | Last month of the analysis window. |
| `lisa_cluster` |  | text | LISA cluster at 5% (High-High, Low-Low, High-Low, Low-High, Not significant, No neighbors), or No data when the municipality has no spatial metrics. |
| `lisa_p_value` | LISA p-value | double precision | Pseudo p-value of the local Moran's I (999 permutations). |
| `community_id` |  | bigint | Louvain community id; see graph_communities. |
| `centroid_lat` |  | double precision | Latitude of a point inside the municipality (SIRGAS 2000). |
| `centroid_lon` |  | double precision | Longitude of a point inside the municipality (SIRGAS 2000). |
| `area_km2` |  | double precision | Municipality area in km2. |
| `neighbor_count` |  | bigint | Number of municipalities that share a border with it. 0 for ocean islands. |
| `neighbor_avg_cvli_rate_100k` |  | double precision | Average CVLI rate of the neighbors (spatial lag). |
| `neighbor_national_hotspots` |  | bigint | Number of neighbors that are national hotspots. |

| Metric | Label | Format | Expression | Description |
|---|---|---|---|---|
| `municipalities` | Municipalities | `,d` | `COUNT(*)` |  |
| `population` | Population | `,d` | `SUM(population)` |  |
| `cvli_victims` | CVLI victims | `,d` | `SUM(cvli_victims)` |  |
| `cvli_rate_100k` | CVLI rate per 100k | `,.1f` | `SUM(cvli_victims) * 12.0 / MAX(months_covered) * 100000 / NULLIF(SUM(CASE WHEN has_sinesp_records THEN population END), 0)` | Annualized CVLI victims per 100,000 inhabitants of the selected municipalities with SINESP records. Victims without informed municipality are not included (see state_profile). |
| `national_hotspots` | National hotspots | `,d` | `SUM(CASE WHEN is_national_hotspot THEN 1 ELSE 0 END)` |  |
| `state_hotspots` | State hotspots | `,d` | `SUM(CASE WHEN is_state_hotspot THEN 1 ELSE 0 END)` |  |
| `high_high_municipalities` | High-High municipalities | `,d` | `SUM(CASE WHEN lisa_cluster = 'High-High' THEN 1 ELSE 0 END)` |  |
| `avg_ivsc_score` | Average IVSC | `.3f` | `AVG(ivsc_score)` | Simple average of the Social & Criminal Vulnerability Index (0 to 1) of the selected municipalities. |

## municipality_profile_items

Profile of each municipality as label and value rows (population, IVSC, Z-score, hotspot, LISA cluster, IDHM), for the profile table of the dashboard.

| Column | Label | Type | Description |
|---|---|---|---|
| `municipality_code` |  | text | IBGE 7-digit municipality code (primary key). |
| `municipality_label` | Municipality | text | Municipality name and state code, e.g. Porto Velho (RO). |
| `state_code` |  | text | Two-letter state code (UF), e.g. SP, PE. |
| `state_name` |  | text | State name. |
| `item_order` |  | integer | Display order of the item. |
| `item` | Indicator | text | Name of the indicator, with its reference period. |
| `value` | Value | text | Value of the indicator, formatted as text. |

| Metric | Label | Format | Expression | Description |
|---|---|---|---|---|
| `value` | Value | `` | `MAX(value)` |  |
| `municipalities` | Municipalities | `` | `COUNT(DISTINCT municipality_code)` |  |

## state_profile

One row per state - lethal violence, vehicle crimes and drug seizures in the analysis window.

| Column | Label | Type | Description |
|---|---|---|---|
| `state_code` |  | text | Two-letter state code (UF). |
| `state_name` |  | text | State name. |
| `region_code` |  | text | Region code: N, NE, SE, S, CO. |
| `region_name` |  | text | Region name. |
| `population` |  | bigint | State population (sum of municipalities). |
| `fleet_total` |  | bigint | State vehicle fleet (sum of municipalities). |
| `cvli_victims` |  | double precision | State CVLI victims in the window, including victims without informed municipality. |
| `cvli_rate_100k` |  | double precision | State CVLI victims per 100,000 inhabitants, annualized. |
| `vehicle_crimes` |  | double precision | Vehicle robberies and thefts (occurrences) in the window, all reporting agencies. |
| `vehicle_crime_rate_1k_fleet` |  | double precision | Vehicle robberies and thefts per 1,000 vehicles, annualized. |
| `drug_seizure_kg` |  | double precision | Seized drugs (cocaine and marijuana) in kg in the window. |
| `months_covered` |  | integer | Number of months in the analysis window. |
| `window_start` |  | date | First month of the analysis window. |
| `window_end` |  | date | Last month of the analysis window. |

| Metric | Label | Format | Expression | Description |
|---|---|---|---|---|
| `population` | Population | `,d` | `SUM(population)` |  |
| `cvli_victims` | CVLI victims | `,d` | `SUM(cvli_victims)` |  |
| `cvli_rate_100k` | CVLI rate per 100k | `,.1f` | `SUM(cvli_victims) * 12.0 / MAX(months_covered) * 100000 / NULLIF(SUM(population), 0)` | Annualized CVLI victims per 100,000 inhabitants of the selected states, including victims without informed municipality. |
| `vehicle_crime_rate_1k_fleet` | Vehicle crimes per 1k vehicles | `,.2f` | `SUM(vehicle_crimes) * 12.0 / MAX(months_covered) * 1000 / NULLIF(SUM(fleet_total), 0)` |  |
| `drug_seizure_kg` | Drug seizures (kg) | `,.0f` | `SUM(drug_seizure_kg)` |  |

## cvli_monthly_state

One row per state and month - CVLI victims, including victims without informed municipality.

| Column | Label | Type | Description |
|---|---|---|---|
| `state_code` |  | text | Two-letter state code (UF). |
| `state_name` |  | text | State name. |
| `region_name` |  | text | Region name. |
| `month` | Month | date | First day of the reference month. |
| `year` |  | integer | Year. |
| `year_month` |  | text | Month as yyyy-MM. |
| `cvli_victims` |  | double precision | CVLI victims in the month (intentional homicide, feminicide, robbery followed by death and bodily injury followed by death), all reporting agencies. |
| `population` |  | bigint | Latest population estimate of the state (IBGE), the same in every month. |
| `cvli_rate_100k_annualized` | CVLI rate per 100k (annualized, month) | numeric | CVLI victims of the month x 12 per 100,000 inhabitants. |

| Metric | Label | Format | Expression | Description |
|---|---|---|---|---|
| `cvli_victims` | CVLI victims | `,d` | `SUM(cvli_victims)` |  |
| `cvli_rate_100k_annualized` | CVLI rate per 100k (annualized) | `,.1f` | `SUM(cvli_victims) * 12 * 100000.0 / NULLIF(SUM(population), 0)` | Annualized CVLI rate of the selected states. Valid per month and over several months, because every row carries its state population. |

## cvli_monthly_municipality

Main dataset of the dashboard - one row per area and month on a complete grid. Areas are the municipalities with SINESP records plus one Not assigned row per state, so sums by state equal the state facts.

| Column | Label | Type | Description |
|---|---|---|---|
| `municipality_code` |  | text | IBGE 7-digit municipality code. Null on the Not assigned rows. |
| `municipality_name` |  | text | Official municipality name (IBGE), or Not assigned for the victims of the state without informed municipality. |
| `municipality_label` | Municipality | text | Municipality name and state code, e.g. Porto Velho (RO), or Not assigned (RO). Names repeat across states. |
| `is_municipality` | Is municipality | boolean | False on the Not assigned rows. Filter on it for rankings and tables of municipalities. |
| `state_code` | State | text | Two-letter state code (UF), e.g. SP, PE. |
| `state_name` | State name | text | State name. |
| `region_name` |  | text | Brazilian region: Norte, Nordeste, Sudeste, Sul, Centro-Oeste. |
| `month` | Month | date | First day of the reference month. |
| `year` |  | integer | Year. |
| `month_of_year` | Month of year | integer | Month number (1-12), to compare the same months of different years. |
| `year_month` |  | text | Month as yyyy-MM. |
| `cvli_victims` |  | double precision | CVLI victims in the month (intentional homicide, feminicide, robbery followed by death and bodily injury followed by death), all reporting agencies. Zero when the municipality has no record in the month. |
| `cvli_victims_previous_year` | CVLI victims (same month, previous year) | double precision | CVLI victims of the same area in the same month of the previous year. Null when that month is not in the data. |
| `population` |  | double precision | Latest population estimate (IBGE), repeated every month. On the Not assigned row, the population of the state's municipalities without records. |
| `lisa_cluster` | LISA cluster | text | LISA cluster of the municipality (fixed 12-month window of the spatial analysis, not the selected period). Null on the Not assigned rows. |

| Metric | Label | Format | Expression | Description |
|---|---|---|---|---|
| `cvli_victims` | CVLI victims | `,d` | `SUM(cvli_victims)` |  |
| `cvli_rate_100k_annualized` | CVLI rate per 100k (annualized) | `,.2f` | `SUM(cvli_victims) * 12 * 100000.0 / NULLIF(SUM(population), 0)` | Annualized CVLI victims per 100,000 inhabitants of the selected areas and months. Valid for any filter, because every row carries the population of its area. |
| `cvli_yoy_change` | Year-over-year change in CVLI | `+.2%` | `CASE WHEN COUNT(cvli_victims_previous_year) = COUNT(*) THEN SUM(cvli_victims) / NULLIF(SUM(cvli_victims_previous_year), 0) - 1 END` | CVLI victims of the selected months against the same months of the previous year. Null when any of those previous months is missing from the data. |
| `cvli_victims_previous_year` | CVLI victims (previous year) | `,d` | `SUM(cvli_victims_previous_year)` |  |
| `period_rate_reference` | Period rate | `,.2f` | `SUM(SUM(cvli_victims)) OVER () * 12 * 100000.0 / NULLIF(SUM(SUM(population)) OVER (), 0)` | Annualized CVLI rate of the whole selection, the same value on every row of the chart. Reference line of the rate trend. |
| `municipalities` | Municipalities | `,d` | `COUNT(DISTINCT municipality_code)` |  |

## cvli_kpis

One row - national KPIs of the analysis window and the change against the previous window of the same length.

| Column | Label | Type | Description |
|---|---|---|---|
| `window_start` |  | date | First month of the analysis window. |
| `window_end` |  | date | Last month of the analysis window. |
| `months_covered` |  | integer | Number of months in the analysis window. |
| `cvli_victims` |  | double precision | CVLI victims in Brazil in the analysis window, including victims without informed municipality. |
| `population` |  | numeric | Population of Brazil (sum of the states). |
| `cvli_rate_100k` |  | numeric | Annualized CVLI victims per 100,000 inhabitants in Brazil. |
| `previous_cvli_victims` | CVLI victims (previous window) | double precision | CVLI victims in the window of the same length right before the analysis window. |
| `previous_months_covered` |  | bigint | Months with data in the previous window. |
| `cvli_change_pct` | CVLI change (%) | numeric | Change of CVLI victims against the previous window, in percent. Null when the previous window is not complete. |
| `national_hotspots` |  | bigint | Number of municipalities flagged as national hotspots (Z-Score >= 2). |
| `high_high_municipalities` |  | bigint | Number of municipalities in a High-High LISA cluster (regional violence clusters). |

| Metric | Label | Format | Expression | Description |
|---|---|---|---|---|
| `cvli_rate_100k` | CVLI rate per 100k | `,.1f` | `MAX(cvli_rate_100k)` |  |
| `cvli_victims` | CVLI victims | `,d` | `MAX(cvli_victims)` |  |
| `cvli_change_pct` | CVLI change (%) | `+.1%` | `MAX(cvli_change_pct) / 100.0` |  |
| `national_hotspots` | National hotspots | `,d` | `MAX(national_hotspots)` |  |
| `high_high_municipalities` | High-High municipalities | `,d` | `MAX(high_high_municipalities)` |  |
