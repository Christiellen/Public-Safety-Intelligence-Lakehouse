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

# COMMAND ----------

# MAGIC %run ./00_config_utils

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
        .withColumn("state_code", F.upper(F.trim("uf"))))

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
state_occurrences = (valid.groupBy("state_code", "event_name", "agency", "event_level", "measure_type", "reference_date")
                     .agg(F.sum("value").alias("value")))
assert_unique(state_occurrences, ["state_code", "event_name", "agency", "measure_type", "reference_date"],
              "sinesp_state_occurrences")
write_table(state_occurrences, "silver", "sinesp_state_occurrences")

# COMMAND ----------

# 2. Municipal events matched to the IBGE code
municipal = valid.filter(F.col("event_level") == "municipal")
is_not_informed = F.col("municipio").isNull() | name_key(F.col("municipio")).isin(NOT_INFORMED_KEYS)
not_informed = municipal.filter(is_not_informed)
print(f"Municipal-event rows with municipality not informed (kept in state totals only): {not_informed.count():,}")

matched, unmatched = match_municipalities(municipal.filter(~is_not_informed).drop("state_code"),
                                          "uf", "municipio", dim, "SINESP")
write_unmatched(unmatched, "SINESP")

occurrences = (matched
               .groupBy("municipality_code", "state_code", "event_name", "agency", "measure_type", "reference_date")
               .agg(F.sum("value").cast("long").alias("count")))
assert_unique(occurrences, ["municipality_code", "event_name", "agency", "measure_type", "reference_date"],
              "sinesp_occurrences")
write_table(occurrences, "silver", "sinesp_occurrences")

missing_cvli = set(CVLI_EVENTS) - {r.event_name for r in occurrences.select("event_name").distinct().collect()}
if missing_cvli:
    print(f"WARNING: CVLI events not found at municipal level: {missing_cvli}")

display(spark.table(tbl("silver", "sinesp_occurrences"))
        .groupBy("event_name", "agency", "measure_type").agg(F.sum("count").alias("count"))
        .orderBy(F.desc("count")))