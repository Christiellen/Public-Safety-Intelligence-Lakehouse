# Databricks notebook source
# MAGIC %md
# MAGIC # 08 - Gold - Star schema
# MAGIC
# MAGIC Dimensional model for BI tools and for the next stages of the project (graph and agents):
# MAGIC
# MAGIC | Table | Type | Grain / key |
# MAGIC |---|---|---|
# MAGIC | `dim_municipality` | Dimension | `municipality_code` (IBGE) |
# MAGIC | `dim_state` | Dimension | `state_code` |
# MAGIC | `dim_event` | Dimension | `event_id` (stable hash of the normalized event name), with event groups (CVLI, vehicle crime, drug seizure) and level of detail |
# MAGIC | `dim_date` | Dimension | `date_id` (yyyyMMdd, first day of month) |
# MAGIC | `fact_occurrences` | Fact (monthly) | municipality × event × agency × month × measure type - municipal-level events only |
# MAGIC | `fact_state_occurrences` | Fact (monthly) | state × event × agency × month × measure type - all events |
# MAGIC | `fact_municipality_indicators` | Fact (snapshot) | municipality |
# MAGIC
# MAGIC `event_id` is a hash, not a sequence, so it stays the same across reloads.
# MAGIC
# MAGIC **Incremental.** The two SINESP facts are partitioned by `year` and only the years reprocessed by `07_silver_sinesp_occurrences` are replaced. The dimensions and the indicator snapshot are small and are rebuilt whenever this notebook runs. Downstream notebooks are notified through the change log (`gold_facts` by year, `gold_dimensions`).

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

CONSUMER = "08_gold_star_schema"
FACTS = ["fact_occurrences", "fact_state_occurrences"]
pending = run_or_exit(CONSUMER, ["silver_sinesp", "silver_dim_municipality", "silver_municipality_indicators"],
                      outputs=[("gold", name) for name in ["dim_municipality", "dim_state", "dim_event", "dim_date",
                                                          "fact_municipality_indicators", *FACTS]])
if FULL_REFRESH or not all(spark.catalog.tableExists(tbl("gold", name)) for name in FACTS):
    fact_years = sorted(r.year for r in spark.table(tbl("silver", "sinesp_state_occurrences")).select("year")
                        .distinct().collect())
else:
    fact_years = sorted(int(year) for year in pending["silver_sinesp"])
print(f"Fact years to replace: {fact_years or 'none (only dimensions and indicators are rebuilt)'}")

# COMMAND ----------

ensure_schemas()

silver_dim = spark.table(tbl("silver", "dim_municipality"))
occurrences = spark.table(tbl("silver", "sinesp_occurrences"))
state_occurrences = spark.table(tbl("silver", "sinesp_state_occurrences"))
indicators = spark.table(tbl("silver", "municipality_indicators"))

# dim_municipality and dim_state
dim_municipality = silver_dim.drop("name_key")
assert_unique(dim_municipality, ["municipality_code"], "gold.dim_municipality")
write_table(dim_municipality, "gold", "dim_municipality")

dim_state = spark.createDataFrame(
    [(values[0], values[1], values[2], values[3]) for values in UF_REFERENCE.values()],
    "state_code string, state_name string, region_code string, region_name string")
write_table(dim_state, "gold", "dim_state")

# dim_event: every event of the state facts (a superset of the municipal ones)
dim_event = (state_occurrences.groupBy("event_name")
             .agg(F.max(F.when(F.col("event_level") == "municipal", 1).otherwise(0)).alias("_municipal"))
             .withColumn("event_id", F.xxhash64("event_name"))
             .withColumn("event_level", F.when(F.col("_municipal") == 1, "municipal").otherwise("state"))
             .withColumn("is_cvli", F.col("event_name").isin(CVLI_EVENTS))
             .withColumn("is_vehicle_crime", F.col("event_name").rlike(VEHICLE_EVENT_PATTERN))
             .withColumn("is_drug_seizure", F.col("event_name").rlike(DRUG_SEIZURE_PATTERN))
             .select("event_id", "event_name", "event_level", "is_cvli", "is_vehicle_crime", "is_drug_seizure"))
assert_unique(dim_event, ["event_id"], "gold.dim_event")
for flag in ["is_cvli", "is_vehicle_crime", "is_drug_seizure"]:
    if dim_event.filter(flag).count() == 0:
        print(f"WARNING: no event flagged as {flag}; check the patterns in 00_config_utils.")
write_table(dim_event, "gold", "dim_event")
display(spark.table(tbl("gold", "dim_event")).orderBy("event_level", "event_name"))

# COMMAND ----------

# dim_date (months present in the facts)
dim_date = (state_occurrences.select(F.col("reference_date").alias("date")).distinct()
            .select(F.date_format("date", "yyyyMMdd").cast("int").alias("date_id"),
                    "date",
                    F.year("date").alias("year"),
                    F.quarter("date").alias("quarter"),
                    F.month("date").alias("month"),
                    F.date_format("date", "yyyy-MM").alias("year_month")))
write_table(dim_date, "gold", "dim_date")

# fact_occurrences (municipal level): only the years that changed
fact_occurrences = (occurrences.filter(F.col("year").isin(fact_years))
                    .withColumn("event_id", F.xxhash64("event_name"))
                    .withColumn("date_id", F.date_format("reference_date", "yyyyMMdd").cast("int"))
                    .select("municipality_code", "event_id", "agency", "date_id", "measure_type", "count", "year"))
keys = ["municipality_code", "event_id", "agency", "date_id", "measure_type"]
assert_unique(fact_occurrences, keys, "gold.fact_occurrences")
orphans = fact_occurrences.join(dim_municipality, "municipality_code", "left_anti").count()
if orphans:
    raise AssertionError(f"{orphans} fact rows without municipality in the dimension")
if fact_years:
    replace_partitions(fact_occurrences, "gold", "fact_occurrences", "year", fact_years)

# fact_state_occurrences (all events)
fact_state = (state_occurrences.filter(F.col("year").isin(fact_years))
              .withColumn("event_id", F.xxhash64("event_name"))
              .withColumn("date_id", F.date_format("reference_date", "yyyyMMdd").cast("int"))
              .select("state_code", "event_id", "agency", "date_id", "measure_type", "value", "year"))
assert_unique(fact_state, ["state_code", "event_id", "agency", "date_id", "measure_type"], "gold.fact_state_occurrences")
if fact_years:
    replace_partitions(fact_state, "gold", "fact_state_occurrences", "year", fact_years)

# fact_municipality_indicators (snapshot)
fact_indicators = indicators.drop("state_code")
assert_unique(fact_indicators, ["municipality_code"], "gold.fact_municipality_indicators")
write_table(fact_indicators, "gold", "fact_municipality_indicators")

if fact_years:
    log_change("gold_facts", fact_years)
log_change("gold_dimensions", ["all"])
finish_step(CONSUMER)
