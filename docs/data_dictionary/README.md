# Data dictionary - Gold layer

Tables and columns of the Gold layer (`workspace.psi_gold`) that the analytics and the AI agents use.
Generated from `psi_gold.data_dictionary` (notebook 16); do not edit by hand.

Download: [Excel](data_dictionary.xlsx) - [CSV](data_dictionary.csv)

| Table | Columns | Description |
|---|---|---|
| [municipality_analytics](#municipality_analytics) | 31 | One row per municipality: population, GDP, IDHM, fleet and lethal violence (CVLI) indicators for the analysis window. Main table for municipal questions. |
| [state_analytics](#state_analytics) | 14 | One row per state: lethal violence, vehicle crimes and drug seizures in the analysis window. Use for state-level questions and for vehicle crimes and drug seizures, which SINESP publishes by state only. |
| [municipality_spatial_metrics](#municipality_spatial_metrics) | 17 | One row per municipality: spatial statistics and graph metrics on the neighborhood graph (municipalities that share a border). Join to municipality_analytics by municipality_code. |
| [graph_communities](#graph_communities) | 6 | One row per Louvain community: contiguous groups of municipalities with similar lethal violence levels. |
| [dim_event](#dim_event) | 6 | Public safety event types published by SINESP, with groups and level of detail. |
| [dim_date](#dim_date) | 6 | Months available in the public safety facts. |
| [fact_occurrences](#fact_occurrences) | 7 | Monthly public safety counts per municipality, for events published at municipal level. Join dim_event (event_id), dim_date (date_id) and municipality_analytics (municipality_code). |
| [fact_state_occurrences](#fact_state_occurrences) | 7 | Monthly public safety values per state, for all events (including those published by state only). Join dim_event and dim_date. |

## municipality_analytics

One row per municipality: population, GDP, IDHM, fleet and lethal violence (CVLI) indicators for the analysis window. Main table for municipal questions.

| Column | Type | Description |
|---|---|---|
| `municipality_code` | string | IBGE 7-digit municipality code (primary key). |
| `municipality_name` | string | Official municipality name (IBGE). Names repeat across states: always filter or show state_code too. |
| `state_code` | string | Two-letter state code (UF), e.g. SP, PE. |
| `region_name` | string | Brazilian region: Norte, Nordeste, Sudeste, Sul, Centro-Oeste. |
| `population` | bigint | Estimated resident population (IBGE), year in population_year. |
| `population_year` | int | Reference year of population. |
| `gdp_year` | int | Reference year of the GDP figures. |
| `gdp_thousand_brl` | double | GDP at current prices, in thousand BRL (IBGE). |
| `gdp_population_year` | int | Population year used to compute GDP per capita (closest to gdp_year). |
| `gdp_per_capita_brl` | double | GDP per capita in BRL. |
| `idhm` | double | Municipal Human Development Index (0 to 1), year in idhm_year (2010, latest municipal edition). |
| `idhm_education` | double | IDHM education dimension (0 to 1). |
| `idhm_longevity` | double | IDHM longevity dimension (0 to 1). |
| `idhm_income` | double | IDHM income dimension (0 to 1). |
| `idhm_year` | int | Reference year of IDHM. |
| `fleet_total` | bigint | Registered vehicle fleet (SENATRAN), month in fleet_reference. |
| `fleet_reference` | string | Reference month of the fleet (yyyy-MM). |
| `cvli_victims` | bigint | CVLI victims in the analysis window: intentional homicide, feminicide, robbery followed by death and bodily injury followed by death. Null means no SINESP record. |
| `has_sinesp_records` | boolean | True when the municipality has SINESP records in the window. |
| `months_covered` | int | Number of months in the analysis window. |
| `window_start` | date | First month of the analysis window. |
| `window_end` | date | Last month of the analysis window. |
| `cvli_rate_100k` | double | CVLI victims per 100,000 inhabitants, annualized. Main lethal violence indicator. |
| `cvli_z_score_state` | double | Z-Score of cvli_rate_100k against municipalities of the same state with population >= 20,000. Null for smaller municipalities. |
| `cvli_z_score_national` | double | Z-Score of cvli_rate_100k against all municipalities in Brazil with population >= 20,000. Null for smaller municipalities. |
| `is_state_hotspot` | boolean | True when cvli_z_score_state >= 2 (stands out within its state). |
| `is_national_hotspot` | boolean | True when cvli_z_score_national >= 2 (among the highest rates in Brazil). |
| `crime_component` | double | CVLI rate rescaled to 0-1 (capped at national p1/p99), used in ivsc_score. |
| `social_component` | double | 1 - IDHM rescaled to 0-1, used in ivsc_score. |
| `ivsc_score` | double | Social & Criminal Vulnerability Index, 0 to 1: 0.4 x crime_component + 0.6 x social_component. |
| `economic_tier` | string | National tertile of GDP per capita: Low, Medium, High. |

## state_analytics

One row per state: lethal violence, vehicle crimes and drug seizures in the analysis window. Use for state-level questions and for vehicle crimes and drug seizures, which SINESP publishes by state only.

| Column | Type | Description |
|---|---|---|
| `state_code` | string | Two-letter state code (UF). |
| `state_name` | string | State name. |
| `region_code` | string | Region code: N, NE, SE, S, CO. |
| `region_name` | string | Region name. |
| `population` | bigint | State population (sum of municipalities). |
| `fleet_total` | bigint | State vehicle fleet (sum of municipalities). |
| `cvli_victims` | double | State CVLI victims in the window, including victims without informed municipality. |
| `vehicle_crimes` | double | Vehicle robberies and thefts (occurrences) in the window, all reporting agencies. |
| `drug_seizure_kg` | double | Seized drugs (cocaine and marijuana) in kg in the window. |
| `months_covered` | int | Number of months in the analysis window. |
| `window_start` | date | First month of the analysis window. |
| `window_end` | date | Last month of the analysis window. |
| `cvli_rate_100k` | double | State CVLI victims per 100,000 inhabitants, annualized. |
| `vehicle_crime_rate_1k_fleet` | double | Vehicle robberies and thefts per 1,000 vehicles, annualized. |

## municipality_spatial_metrics

One row per municipality: spatial statistics and graph metrics on the neighborhood graph (municipalities that share a border). Join to municipality_analytics by municipality_code.

| Column | Type | Description |
|---|---|---|
| `municipality_code` | string | IBGE 7-digit municipality code (primary key). |
| `centroid_lat` | double | Latitude of a point inside the municipality (SIRGAS 2000). |
| `centroid_lon` | double | Longitude of a point inside the municipality (SIRGAS 2000). |
| `area_km2` | double | Municipality area in km2. |
| `neighbor_count` | bigint | Number of municipalities that share a border with it. 0 for ocean islands. |
| `neighbor_avg_cvli_rate_100k` | double | Average CVLI rate of the neighbors (spatial lag). |
| `neighbor_national_hotspots` | bigint | Number of neighbors that are national hotspots. |
| `lisa_z_sim` | double | Standardized local Moran statistic from permutations. |
| `lisa_i` | double | Local Moran's I of the Empirical Bayes CVLI rate. |
| `lisa_p_value` | double | Pseudo p-value of the local Moran's I (999 permutations). |
| `lisa_cluster` | string | LISA cluster at 5%: High-High (regional violence cluster), Low-Low, High-Low (isolated outlier), Low-High (low rate surrounded by high rates, spillover target), Not significant, No neighbors, No data. |
| `eb_rate_std` | double | Empirical Bayes standardized CVLI rate (Assuncao & Reis). |
| `degree` | bigint | Number of neighbors in the graph (same as neighbor_count). |
| `betweenness` | double | Betweenness centrality in the neighborhood graph (sampled): bridges between regions. |
| `community_id` | bigint | Louvain community id; see graph_communities. |
| `global_moran_i` | double | Global Moran's I of the CVLI rate for Brazil (same value on every row). |
| `global_moran_p_value` | double | Pseudo p-value of the global Moran's I (same value on every row). |

## graph_communities

One row per Louvain community: contiguous groups of municipalities with similar lethal violence levels.

| Column | Type | Description |
|---|---|---|
| `community_id` | bigint | Community id (primary key). |
| `municipalities` | bigint | Number of municipalities in the community. |
| `states` | string | Comma-separated state codes covered by the community. |
| `population` | bigint | Total population of the community. |
| `cvli_victims` | bigint | Total CVLI victims in the window. |
| `cvli_rate_100k` | double | CVLI victims per 100,000 inhabitants of the community (not annualized). |

## dim_event

Public safety event types published by SINESP, with groups and level of detail.

| Column | Type | Description |
|---|---|---|
| `event_id` | bigint | Event key (stable hash of event_name). |
| `event_name` | string | Normalized event name (uppercase, no accents), e.g. HOMICIDIO DOLOSO, ROUBO DE VEICULO. |
| `event_level` | string | municipal (published per municipality) or state (published per state only). |
| `is_cvli` | boolean | True for the events that compose CVLI. |
| `is_vehicle_crime` | boolean | True for vehicle robbery and theft. |
| `is_drug_seizure` | boolean | True for drug seizures (measured in kg). |

## dim_date

Months available in the public safety facts.

| Column | Type | Description |
|---|---|---|
| `date_id` | int | Month key in yyyyMMdd (first day of month). |
| `date` | date | First day of the month. |
| `year` | int | Year. |
| `quarter` | int | Quarter (1-4). |
| `month` | int | Month (1-12). |
| `year_month` | string | Month as yyyy-MM. |

## fact_occurrences

Monthly public safety counts per municipality, for events published at municipal level. Join dim_event (event_id), dim_date (date_id) and municipality_analytics (municipality_code).

| Column | Type | Description |
|---|---|---|
| `municipality_code` | string | IBGE 7-digit municipality code. |
| `event_id` | bigint | Event key (dim_event). |
| `agency` | string | Reporting agency: Estadual (state police), Polícia Federal or Polícia Rodoviária Federal. |
| `date_id` | int | Month key (dim_date). |
| `measure_type` | string | occurrences or victims. Never sum different measure types. |
| `count` | bigint | Number of occurrences or victims, according to measure_type. |
| `year` | int | Year of the reference month (partition column). Filter on it to read fewer files. |

## fact_state_occurrences

Monthly public safety values per state, for all events (including those published by state only). Join dim_event and dim_date.

| Column | Type | Description |
|---|---|---|
| `state_code` | string | Two-letter state code (UF). |
| `event_id` | bigint | Event key (dim_event). |
| `agency` | string | Reporting agency. |
| `date_id` | int | Month key (dim_date). |
| `measure_type` | string | occurrences, victims or weight_kg. Never sum different measure types. |
| `value` | double | Value according to measure_type. |
| `year` | int | Year of the reference month (partition column). Filter on it to read fewer files. |
