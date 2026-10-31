# Databricks notebook source
# MAGIC %md
# MAGIC # 16 - Gold - Data dictionary for analytics and AI agents
# MAGIC
# MAGIC Documents every table and column that the AI agents (notebooks 17-19) are allowed to query, in two places:
# MAGIC
# MAGIC 1. **Unity Catalog comments** (`COMMENT ON TABLE` / `ALTER COLUMN ... COMMENT`), visible in Catalog Explorer and to any tool that reads the catalog;
# MAGIC 2. the table **`psi_gold.data_dictionary`**, which the agents read to understand the schema instead of guessing it.
# MAGIC
# MAGIC The list of documented tables is also the **allowlist** of the SQL agent: a table that is not described here cannot be queried.
# MAGIC
# MAGIC A quality gate compares the dictionary with the real columns: an undocumented column, or a documented column that no longer exists, stops the notebook.

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

DICTIONARY = {
    "municipality_analytics": {
        "_table": "One row per municipality: population, GDP, IDHM, fleet and lethal violence (CVLI) indicators for the analysis window. Main table for municipal questions.",
        "municipality_code": "IBGE 7-digit municipality code (primary key).",
        "municipality_name": "Official municipality name (IBGE). Names repeat across states: always filter or show state_code too.",
        "state_code": "Two-letter state code (UF), e.g. SP, PE.",
        "region_name": "Brazilian region: Norte, Nordeste, Sudeste, Sul, Centro-Oeste.",
        "population": "Estimated resident population (IBGE), year in population_year.",
        "population_year": "Reference year of population.",
        "gdp_year": "Reference year of the GDP figures.",
        "gdp_thousand_brl": "GDP at current prices, in thousand BRL (IBGE).",
        "gdp_population_year": "Population year used to compute GDP per capita (closest to gdp_year).",
        "gdp_per_capita_brl": "GDP per capita in BRL.",
        "idhm": "Municipal Human Development Index (0 to 1), year in idhm_year (2010, latest municipal edition).",
        "idhm_education": "IDHM education dimension (0 to 1).",
        "idhm_longevity": "IDHM longevity dimension (0 to 1).",
        "idhm_income": "IDHM income dimension (0 to 1).",
        "idhm_year": "Reference year of IDHM.",
        "fleet_total": "Registered vehicle fleet (SENATRAN), month in fleet_reference.",
        "fleet_reference": "Reference month of the fleet (yyyy-MM).",
        "cvli_victims": "CVLI victims in the analysis window: intentional homicide, feminicide, robbery followed by death and bodily injury followed by death. Null means no SINESP record.",
        "has_sinesp_records": "True when the municipality has SINESP records in the window.",
        "months_covered": "Number of months in the analysis window.",
        "window_start": "First month of the analysis window.",
        "window_end": "Last month of the analysis window.",
        "cvli_rate_100k": "CVLI victims per 100,000 inhabitants, annualized. Main lethal violence indicator.",
        "cvli_z_score_state": "Z-Score of cvli_rate_100k against municipalities of the same state with population >= 20,000. Null for smaller municipalities.",
        "cvli_z_score_national": "Z-Score of cvli_rate_100k against all municipalities in Brazil with population >= 20,000. Null for smaller municipalities.",
        "is_state_hotspot": "True when cvli_z_score_state >= 2 (stands out within its state).",
        "is_national_hotspot": "True when cvli_z_score_national >= 2 (among the highest rates in Brazil).",
        "crime_component": "CVLI rate rescaled to 0-1 (capped at national p1/p99), used in ivsc_score.",
        "social_component": "1 - IDHM rescaled to 0-1, used in ivsc_score.",
        "ivsc_score": "Social & Criminal Vulnerability Index, 0 to 1: 0.4 x crime_component + 0.6 x social_component.",
        "economic_tier": "National tertile of GDP per capita: Low, Medium, High.",
    },
    "state_analytics": {
        "_table": "One row per state: lethal violence, vehicle crimes and drug seizures in the analysis window. Use for state-level questions and for vehicle crimes and drug seizures, which SINESP publishes by state only.",
        "state_code": "Two-letter state code (UF).",
        "state_name": "State name.",
        "region_code": "Region code: N, NE, SE, S, CO.",
        "region_name": "Region name.",
        "population": "State population (sum of municipalities).",
        "fleet_total": "State vehicle fleet (sum of municipalities).",
        "cvli_victims": "State CVLI victims in the window, including victims without informed municipality.",
        "vehicle_crimes": "Vehicle robberies and thefts (occurrences) in the window, all reporting agencies.",
        "drug_seizure_kg": "Seized drugs (cocaine and marijuana) in kg in the window.",
        "months_covered": "Number of months in the analysis window.",
        "window_start": "First month of the analysis window.",
        "window_end": "Last month of the analysis window.",
        "cvli_rate_100k": "State CVLI victims per 100,000 inhabitants, annualized.",
        "vehicle_crime_rate_1k_fleet": "Vehicle robberies and thefts per 1,000 vehicles, annualized.",
    },
    "municipality_spatial_metrics": {
        "_table": "One row per municipality: spatial statistics and graph metrics on the neighborhood graph (municipalities that share a border). Join to municipality_analytics by municipality_code.",
        "municipality_code": "IBGE 7-digit municipality code (primary key).",
        "centroid_lat": "Latitude of a point inside the municipality (SIRGAS 2000).",
        "centroid_lon": "Longitude of a point inside the municipality (SIRGAS 2000).",
        "area_km2": "Municipality area in km2.",
        "neighbor_count": "Number of municipalities that share a border with it. 0 for ocean islands.",
        "neighbor_avg_cvli_rate_100k": "Average CVLI rate of the neighbors (spatial lag).",
        "neighbor_national_hotspots": "Number of neighbors that are national hotspots.",
        "lisa_z_sim": "Standardized local Moran statistic from permutations.",
        "lisa_i": "Local Moran's I of the Empirical Bayes CVLI rate.",
        "lisa_p_value": "Pseudo p-value of the local Moran's I (999 permutations).",
        "lisa_cluster": "LISA cluster at 5%: High-High (regional violence cluster), Low-Low, High-Low (isolated outlier), Low-High (low rate surrounded by high rates, spillover target), Not significant, No neighbors, No data.",
        "eb_rate_std": "Empirical Bayes standardized CVLI rate (Assuncao & Reis).",
        "degree": "Number of neighbors in the graph (same as neighbor_count).",
        "betweenness": "Betweenness centrality in the neighborhood graph (sampled): bridges between regions.",
        "community_id": "Louvain community id; see graph_communities.",
        "global_moran_i": "Global Moran's I of the CVLI rate for Brazil (same value on every row).",
        "global_moran_p_value": "Pseudo p-value of the global Moran's I (same value on every row).",
    },
    "graph_communities": {
        "_table": "One row per Louvain community: contiguous groups of municipalities with similar lethal violence levels.",
        "community_id": "Community id (primary key).",
        "municipalities": "Number of municipalities in the community.",
        "states": "Comma-separated state codes covered by the community.",
        "population": "Total population of the community.",
        "cvli_victims": "Total CVLI victims in the window.",
        "cvli_rate_100k": "CVLI victims per 100,000 inhabitants of the community (not annualized).",
    },
    "dim_event": {
        "_table": "Public safety event types published by SINESP, with groups and level of detail.",
        "event_id": "Event key (stable hash of event_name).",
        "event_name": "Normalized event name (uppercase, no accents), e.g. HOMICIDIO DOLOSO, ROUBO DE VEICULO.",
        "event_level": "municipal (published per municipality) or state (published per state only).",
        "is_cvli": "True for the events that compose CVLI.",
        "is_vehicle_crime": "True for vehicle robbery and theft.",
        "is_drug_seizure": "True for drug seizures (measured in kg).",
    },
    "dim_date": {
        "_table": "Months available in the public safety facts.",
        "date_id": "Month key in yyyyMMdd (first day of month).",
        "date": "First day of the month.",
        "year": "Year.",
        "quarter": "Quarter (1-4).",
        "month": "Month (1-12).",
        "year_month": "Month as yyyy-MM.",
    },
    "fact_occurrences": {
        "_table": "Monthly public safety counts per municipality, for events published at municipal level. Join dim_event (event_id), dim_date (date_id) and municipality_analytics (municipality_code).",
        "municipality_code": "IBGE 7-digit municipality code.",
        "event_id": "Event key (dim_event).",
        "agency": "Reporting agency: Estadual (state police), Polícia Federal or Polícia Rodoviária Federal.",
        "date_id": "Month key (dim_date).",
        "measure_type": "occurrences or victims. Never sum different measure types.",
        "count": "Number of occurrences or victims, according to measure_type.",
        "year": "Year of the reference month (partition column). Filter on it to read fewer files.",
    },
    "fact_state_occurrences": {
        "_table": "Monthly public safety values per state, for all events (including those published by state only). Join dim_event and dim_date.",
        "state_code": "Two-letter state code (UF).",
        "event_id": "Event key (dim_event).",
        "agency": "Reporting agency.",
        "date_id": "Month key (dim_date).",
        "measure_type": "occurrences, victims or weight_kg. Never sum different measure types.",
        "value": "Value according to measure_type.",
        "year": "Year of the reference month (partition column). Filter on it to read fewer files.",
    },
}

# COMMAND ----------

# Quality gate: the dictionary must match the real columns exactly
problems = []
for table, columns in DICTIONARY.items():
    real = set(spark.table(tbl("gold", table)).columns)
    documented = {c for c in columns if c != "_table"}
    if real - documented:
        problems.append(f"{table}: undocumented columns {sorted(real - documented)}")
    if documented - real:
        problems.append(f"{table}: documented columns that do not exist {sorted(documented - real)}")
if problems:
    raise AssertionError("Data dictionary out of date:\n" + "\n".join(problems))
print(f"OK  dictionary matches {len(DICTIONARY)} tables")


def sql_string(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


# 1. Unity Catalog comments
for table, columns in DICTIONARY.items():
    target = tbl("gold", table)
    spark.sql(f"COMMENT ON TABLE {target} IS {sql_string(columns['_table'])}")
    for column, description in columns.items():
        if column != "_table":
            try:
                spark.sql(f"ALTER TABLE {target} ALTER COLUMN `{column}` COMMENT {sql_string(description)}")
            except Exception as error:
                # Some engines do not allow altering a partition column (e.g. `year` of the facts in
                # Parquet); the description stays in data_dictionary, which is what the agents read.
                if "partition" not in str(error).lower():
                    raise
                print(f"Comment not applied to partition column {target}.{column} (kept in data_dictionary)")
print("Unity Catalog comments applied.")

# 2. Dictionary table, with the real data types
rows = []
types_by_table = {t: dict(spark.table(tbl("gold", t)).dtypes) for t in DICTIONARY}
for table, columns in DICTIONARY.items():
    rows.append((table, None, None, columns["_table"]))
    for column, description in columns.items():
        if column != "_table":
            rows.append((table, column, types_by_table[table][column], description))

dictionary = spark.createDataFrame(rows, "table_name string, column_name string, data_type string, description string")
write_table(dictionary, "gold", "data_dictionary")
display(spark.table(tbl("gold", "data_dictionary")).filter("column_name IS NULL"))
