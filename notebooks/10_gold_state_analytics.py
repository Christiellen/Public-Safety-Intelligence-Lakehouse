# Databricks notebook source
# MAGIC %md
# MAGIC # 10 - Gold - State analytics
# MAGIC
# MAGIC One row per state, for the indicators SINESP publishes at state level only, plus state-level CVLI for comparison.
# MAGIC
# MAGIC | Metric | Definition |
# MAGIC |---|---|
# MAGIC | `cvli_rate_100k` | CVLI victims (all municipalities and *not informed* rows of the state), annualized, per 100,000 inhabitants |
# MAGIC | `vehicle_crimes` | Vehicle robberies and thefts (occurrences), all reporting agencies, in the window |
# MAGIC | `vehicle_crime_rate_1k_fleet` | Vehicle crimes, annualized, per 1,000 vehicles in the state fleet (SENATRAN) |
# MAGIC | `drug_seizure_kg` | Seized drugs in kg, all reporting agencies, in the window |
# MAGIC
# MAGIC Window and annualization follow `09_gold_municipality_analytics`. Population and fleet are the sums of the municipalities of each state.
# MAGIC
# MAGIC **Incremental:** Recomputed only when the Gold facts or dimensions changed.

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

CONSUMER = "10_gold_state_analytics"
pending = run_or_exit(CONSUMER, ["gold_facts", "gold_dimensions"],
                      outputs=[("gold", "state_analytics")])

# COMMAND ----------

WINDOW_MONTHS = 12

fact_state = spark.table(tbl("gold", "fact_state_occurrences"))
dim_event = spark.table(tbl("gold", "dim_event"))
dim_date = spark.table(tbl("gold", "dim_date"))
dim_state = spark.table(tbl("gold", "dim_state"))
dim_municipality = spark.table(tbl("gold", "dim_municipality"))
indicators = spark.table(tbl("gold", "fact_municipality_indicators"))

last_date = dim_date.agg(F.max("date")).first()[0]
window_dates = dim_date.filter(F.col("date") > F.add_months(F.lit(last_date), -WINDOW_MONTHS))
months_covered = window_dates.count()
first_date = window_dates.agg(F.min("date")).first()[0]
annualize = F.lit(12.0 / months_covered)
print(f"Window: {first_date} to {last_date} ({months_covered} months)")

window_facts = (fact_state.join(window_dates.select("date_id"), "date_id")
                .join(dim_event.select("event_id", "is_cvli", "is_vehicle_crime", "is_drug_seizure"), "event_id"))

measures = window_facts.groupBy("state_code").agg(
    F.sum(F.when(F.col("is_cvli") & (F.col("measure_type") == "victims"), F.col("value"))).alias("cvli_victims"),
    F.sum(F.when(F.col("is_vehicle_crime") & (F.col("measure_type") == "occurrences"), F.col("value"))).alias("vehicle_crimes"),
    F.sum(F.when(F.col("is_drug_seizure") & (F.col("measure_type") == "weight_kg"), F.col("value"))).alias("drug_seizure_kg"))

state_totals = (dim_municipality.select("municipality_code", "state_code")
                .join(indicators.select("municipality_code", "population", "fleet_total"), "municipality_code", "left")
                .groupBy("state_code")
                .agg(F.sum("population").alias("population"), F.sum("fleet_total").alias("fleet_total")))

state_analytics = (dim_state
                   .join(state_totals, "state_code", "left")
                   .join(measures, "state_code", "left")
                   .withColumn("months_covered", F.lit(months_covered))
                   .withColumn("window_start", F.lit(first_date))
                   .withColumn("window_end", F.lit(last_date))
                   .withColumn("cvli_rate_100k", F.round(F.col("cvli_victims") * annualize / F.col("population") * 100000, 2))
                   .withColumn("vehicle_crime_rate_1k_fleet",
                               F.round(F.col("vehicle_crimes") * annualize / F.col("fleet_total") * 1000, 3))
                   .withColumn("drug_seizure_kg", F.round("drug_seizure_kg", 1)))

assert_unique(state_analytics, ["state_code"], "gold.state_analytics")
write_table(state_analytics, "gold", "state_analytics")
log_change("gold_state_analytics", ["all"])
finish_step(CONSUMER)
display(spark.table(tbl("gold", "state_analytics")).orderBy(F.desc("cvli_rate_100k")))
