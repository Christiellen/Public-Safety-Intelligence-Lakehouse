# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Silver - Public safety occurrences (SINESP)
# MAGIC
# MAGIC The SINESP file mixes two levels of detail and three kinds of measure, so this notebook separates them before anything is summed.
# MAGIC
# MAGIC **Level of detail.** Some events are published per municipality (e.g. intentional homicide, published for every municipality and month, including zeros). Others are published **per state only**, one row per state and month (e.g. vehicle robbery and theft, drug seizures). The level is detected from the data: an event × agency is *state level* when it never has more than one row per state, month and detail combination.
# MAGIC
# MAGIC **Measures.** `total` (occurrences), `total_vitima` (victims) and `total_peso` (weight in kg, drug seizures) are kept apart in `measure_type`, never mixed.
# MAGIC
# MAGIC **Agency.** `abrangencia` tells which body reported the data (state police, Federal Police, Federal Highway Police). It is part of the key, as `agency`.
# MAGIC
# MAGIC **Detail columns** (`agente`, `arma`, `faixa_etaria`, sex): events that use them are published only as detailed rows, with no total row alongside, so summing them does not double count.
# MAGIC
# MAGIC **Outputs**
# MAGIC - `sinesp_occurrences`: municipal-level events matched to the IBGE code. Rows with municipality not informed, or with a name that does not match, are reported (the latter in `qa_unmatched_municipalities`).
# MAGIC - `sinesp_state_occurrences`: every valid row aggregated by state, which gives complete state totals for all events.
# MAGIC
# MAGIC Both are partitioned by `year` (year of the reference month).
# MAGIC
# MAGIC **Incremental.** Only the reference years present in the SINESP files that changed in Bronze are reprocessed, and only their partitions are replaced (`replace_partitions`); the other years stay as they are. Everything is reprocessed when the municipality dimension changed (the name matching depends on it), with `FULL_REFRESH`, or on the first run. The level of detail is detected on the years being processed. Name-matching QA is kept per source and year (`SINESP 2026`), so reprocessing one year does not erase the QA of the others.

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

CONSUMER = "07_silver_sinesp_occurrences"
OUTPUTS = ["sinesp_occurrences", "sinesp_state_occurrences"]
pending = run_or_exit(CONSUMER, ["bronze_sinesp_occurrences", "silver_dim_municipality"],
                      outputs=[("silver", name) for name in OUTPUTS])
reprocess_all = (FULL_REFRESH or bool(pending["silver_dim_municipality"])
                 or not all(spark.catalog.tableExists(tbl("silver", name)) for name in OUTPUTS))

# COMMAND ----------

dim = spark.table(tbl("silver", "dim_municipality"))
raw = spark.table(tbl("bronze", "bronze_sinesp_occurrences"))

required = {"uf", "municipio", "evento", "data_referencia", "total", "total_vitima", "total_peso", "abrangencia"}
missing = required - set(raw.columns)
if missing:
    raise ValueError(f"SINESP layout changed; missing columns {missing}. Found: {raw.columns}")

date_text = F.substring(F.trim("data_referencia"), 1, 10)
rows = (raw
        .withColumn("_total", parse_count("total"))
        .withColumn("_victims", parse_count("total_vitima"))
        .withColumn("_weight", try_cast("total_peso", "DOUBLE"))
        .withColumn("measure_type", F.when(F.col("_total").isNotNull(), "occurrences")
                                     .when(F.col("_victims").isNotNull(), "victims")
                                     .when(F.col("_weight").isNotNull(), "weight_kg"))
        .withColumn("value", F.coalesce(F.col("_total").cast("double"), F.col("_victims").cast("double"), F.col("_weight")))
        .withColumn("reference_date", F.trunc(F.coalesce(
            F.to_date(F.try_to_timestamp(date_text, F.lit("yyyy-MM-dd"))),
            F.to_date(F.try_to_timestamp(date_text, F.lit("dd/MM/yyyy")))), "month"))
        .withColumn("event_name", normalize_text(F.col("evento")))
        .withColumn("agency", F.coalesce(F.trim("abrangencia"), F.lit("Não informado")))
        .withColumn("state_code", F.upper(F.trim("uf")))
        .withColumn("year", F.year("reference_date")))

# Reference years to process: all, or those present in the SINESP files that changed in Bronze
if reprocess_all:
    years = sorted(r.year for r in rows.select("year").distinct().collect() if r.year is not None)
    print(f"Reprocessing every year: {years}")
else:
    changed_files = pending["bronze_sinesp_occurrences"]
    years = sorted(r.year for r in rows.filter(F.col("_source_year").isin(changed_files))
                   .select("year").distinct().collect() if r.year is not None)
    rows = rows.filter(F.col("year").isin(years))
    print(f"Changed SINESP files: {changed_files} -> reference years to reprocess: {years}")
if not years:
    finish_step(CONSUMER)
    dbutils.notebook.exit("The changed SINESP files have no valid reference month; nothing to do.")

invalid = rows.filter(F.col("value").isNull() | F.col("reference_date").isNull())
print(f"Rows without value or date (excluded): {invalid.count():,}")
valid = rows.filter(F.col("value").isNotNull() & F.col("reference_date").isNotNull())

valid_states = [values[0] for values in UF_REFERENCE.values()]
unknown_states = valid.filter(~F.col("state_code").isin(valid_states))
if unknown_states.count():
    unknown_states.groupBy("state_code").count().show()
    raise AssertionError("SINESP rows with an unknown state code")

# COMMAND ----------

# Level of detail per event and agency: state level when there is at most one row per state, month
# and detail combination (agent, weapon, age group). Detailed state-level events, such as seized
# weapons by type, have several rows per state and month but still no municipality.
detail_columns = [c for c in ["agente", "arma", "faixa_etaria"] if c in valid.columns]
level = (valid.groupBy("event_name", "agency", "state_code", "reference_date", *detail_columns).count()
         .groupBy("event_name", "agency").agg(F.max("count").alias("_max_rows_per_state_month"))
         .withColumn("event_level", F.when(F.col("_max_rows_per_state_month") <= 1, "state").otherwise("municipal"))
         .drop("_max_rows_per_state_month"))
valid = valid.join(F.broadcast(level), ["event_name", "agency"])
level.orderBy("event_level", "event_name", "agency").show(100, truncate=False)

# 1. State totals: every valid row, all events
state_occurrences = (valid.groupBy("state_code", "event_name", "agency", "event_level", "measure_type",
                                   "reference_date", "year")
                     .agg(F.sum("value").alias("value")))
assert_unique(state_occurrences, ["state_code", "event_name", "agency", "measure_type", "reference_date"],
              "sinesp_state_occurrences")
replace_partitions(state_occurrences, "silver", "sinesp_state_occurrences", "year", years)

# COMMAND ----------

# 2. Municipal events matched to the IBGE code
municipal = valid.filter(F.col("event_level") == "municipal")
is_not_informed = F.col("municipio").isNull() | name_key(F.col("municipio")).isin(NOT_INFORMED_KEYS)
not_informed = municipal.filter(is_not_informed)
print(f"Municipal-event rows with municipality not informed (kept in state totals only): {not_informed.count():,}")

# Matched per year, so the QA of each year (source "SINESP <year>") is replaced only with that year
if reprocess_all:  # remove QA rows of the former layout (one "SINESP" source for every year)
    for qa_table in ["qa_unmatched_municipalities", "qa_fuzzy_matches"]:
        if spark.catalog.tableExists(tbl("silver", qa_table)):
            write_source_qa(spark.table(tbl("silver", qa_table)).limit(0), qa_table, "SINESP")
to_match = municipal.filter(~is_not_informed).drop("state_code")
matched = None
for year in years:
    matched_year, unmatched_year = match_municipalities(to_match.filter(F.col("year") == year),
                                                        "uf", "municipio", dim, f"SINESP {year}")
    write_unmatched(unmatched_year, f"SINESP {year}")
    matched = matched_year if matched is None else matched.unionByName(matched_year)

occurrences = (matched
               .groupBy("municipality_code", "state_code", "event_name", "agency", "measure_type", "reference_date",
                        "year")
               .agg(F.sum("value").cast("long").alias("count")))
assert_unique(occurrences, ["municipality_code", "event_name", "agency", "measure_type", "reference_date"],
              "sinesp_occurrences")
replace_partitions(occurrences, "silver", "sinesp_occurrences", "year", years)
log_change("silver_sinesp", years)
finish_step(CONSUMER)

missing_cvli = set(CVLI_EVENTS) - {r.event_name for r in occurrences.select("event_name").distinct().collect()}
if missing_cvli:
    print(f"WARNING: CVLI events not found at municipal level: {missing_cvli}")

display(spark.table(tbl("silver", "sinesp_occurrences"))
        .groupBy("event_name", "agency", "measure_type").agg(F.sum("count").alias("count"))
        .orderBy(F.desc("count")))
