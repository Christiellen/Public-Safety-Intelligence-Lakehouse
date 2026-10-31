# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Silver - Municipality indicators (population, GDP, IDHM, fleet)
# MAGIC
# MAGIC One row per municipality (`municipality_code`), with each indicator and **the year it refers to**, because the sources are published on different calendars:
# MAGIC
# MAGIC | Indicator | Source | Reference |
# MAGIC |---|---|---|
# MAGIC | Population | IBGE SIDRA 6579 | Latest estimate (`population_year`) |
# MAGIC | GDP and GDP per capita | IBGE SIDRA 5938 | Latest GDP year (`gdp_year`); per capita uses the population of the same year, or the nearest available (`gdp_population_year`) |
# MAGIC | IDHM (overall, education, longevity, income) | Ipeadata / Atlas Brasil | 2010 (`idhm_year`) |
# MAGIC | Vehicle fleet | SENATRAN | Month in `fleet_reference` |
# MAGIC
# MAGIC SENATRAN has no IBGE code, so its rows are matched by state + name: exact, then alias, then a conservative fuzzy match (audited in `qa_fuzzy_matches`). Names that still do not match go to `qa_unmatched_municipalities` instead of disappearing.
# MAGIC
# MAGIC **Incremental:** Rebuilt only when one of its sources changed. The fleet comes from the latest `_reference_month` loaded in Bronze, which keeps older months as history.

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

CONSUMER = "06_silver_municipality_indicators"
pending = run_or_exit(CONSUMER, ["bronze_ibge_population", "bronze_ibge_gdp", "bronze_ipea_idhm", "bronze_senatran_fleet",
                                 "silver_dim_municipality"],
                      outputs=[("silver", "municipality_indicators")])

# COMMAND ----------

dim = spark.table(tbl("silver", "dim_municipality"))


# SIDRA uses '-', '...', 'X' etc. for missing or confidential values: try_cast turns them into null.


# 1. Population: last published years -> latest per municipality
population = (spark.table(tbl("bronze", "bronze_ibge_population"))
              .select(F.lpad("municipio_codigo", 7, "0").alias("municipality_code"),
                      try_cast("ano", "INT").alias("year"),
                      try_cast("valor", "DOUBLE").alias("population"))
              .filter(F.col("population").isNotNull()))

latest_population = (population
                     .withColumn("_rn", F.row_number().over(Window.partitionBy("municipality_code").orderBy(F.desc("year"))))
                     .filter("_rn = 1")
                     .select("municipality_code", F.col("population").cast("long"), F.col("year").alias("population_year")))

# 2. GDP (R$ thousand) and GDP per capita with the population closest to the GDP year
gdp = (spark.table(tbl("bronze", "bronze_ibge_gdp"))
       .select(F.lpad("municipio_codigo", 7, "0").alias("municipality_code"),
               try_cast("ano", "INT").alias("gdp_year"),
               try_cast("valor", "DOUBLE").alias("gdp_thousand_brl")))

gdp_population = (gdp.join(population, "municipality_code")
                  .withColumn("_distance", F.abs(F.col("year") - F.col("gdp_year")))
                  .withColumn("_rn", F.row_number().over(
                      Window.partitionBy("municipality_code").orderBy("_distance", F.desc("year"))))
                  .filter("_rn = 1")
                  .select("municipality_code", F.col("year").alias("gdp_population_year"),
                          F.col("population").alias("_gdp_population")))

gdp = (gdp.join(gdp_population, "municipality_code", "left")
          .withColumn("gdp_per_capita_brl",
                      F.round(F.col("gdp_thousand_brl") * 1000 / F.col("_gdp_population"), 2))
          .drop("_gdp_population"))

# COMMAND ----------

# 3. IDHM: municipal level, latest year per series, one column per series
idhm_raw = spark.table(tbl("bronze", "bronze_ipea_idhm"))
series_to_column = {series: label for label, series in IDHM_SERIES.items()}
series_map = F.create_map(*[F.lit(x) for kv in series_to_column.items() for x in kv])

idhm_long = (idhm_raw
             .filter(F.lower(F.col("nivnome")).startswith("munic"))
             .select(F.col("tercodigo").alias("_territory"),
                     series_map[F.col("sercodigo")].alias("indicator"),
                     F.expr("try_cast(substring(valdata, 1, 4) AS INT)").alias("idhm_year"),
                     try_cast("valvalor", "DOUBLE").alias("value"))
             .filter(F.col("value").isNotNull()))

# Ipeadata may use the 7-digit IBGE code or the 6-digit one (without check digit): accept both
codes = dim.select("municipality_code", F.substring("municipality_code", 1, 6).alias("_code6"))
idhm_long = (idhm_long
             .join(F.broadcast(codes), (F.col("_territory") == F.col("municipality_code")) |
                   (F.col("_territory") == F.col("_code6")), "inner")
             .drop("_territory", "_code6"))

idhm_latest = (idhm_long
               .withColumn("_rn", F.row_number().over(
                   Window.partitionBy("municipality_code", "indicator").orderBy(F.desc("idhm_year"))))
               .filter("_rn = 1"))
idhm = (idhm_latest.groupBy("municipality_code")
        .pivot("indicator", list(IDHM_SERIES.keys()))
        .agg(F.round(F.first("value"), 3))
        .join(idhm_latest.groupBy("municipality_code").agg(F.max("idhm_year").alias("idhm_year")), "municipality_code"))

# COMMAND ----------

# 4. Vehicle fleet (SENATRAN): match state + name to the IBGE code
fleet_history = spark.table(tbl("bronze", "bronze_senatran_fleet"))
# Bronze keeps every month loaded; the indicators use the latest one
latest_month = fleet_history.agg(F.max("_reference_month")).first()[0]
fleet_raw = fleet_history.filter(F.col("_reference_month") == latest_month)
print(f"Fleet reference month: {latest_month}")
required = {"uf", "municipio", "total"}
missing = required - set(fleet_raw.columns)
if missing:
    raise ValueError(f"SENATRAN layout changed; missing columns {missing}. Found: {fleet_raw.columns}")

fleet_rows = (fleet_raw
              .filter(F.col("municipio").isNotNull() & ~name_key(F.col("municipio")).isin(NOT_INFORMED_KEYS))
              .withColumn("fleet_total", parse_count("total")))

fleet_matched, fleet_unmatched = match_municipalities(fleet_rows, "uf", "municipio", dim, "SENATRAN")
fleet = (fleet_matched.groupBy("municipality_code")
         .agg(F.sum("fleet_total").alias("fleet_total"),
              F.max("_reference_month").alias("fleet_reference")))
write_unmatched(fleet_unmatched, "SENATRAN")

# COMMAND ----------

# 5. Assemble: one row per municipality, keeping every municipality of the dimension
indicators = (dim.select("municipality_code", "state_code")
              .join(latest_population, "municipality_code", "left")
              .join(gdp, "municipality_code", "left")
              .join(idhm, "municipality_code", "left")
              .join(fleet, "municipality_code", "left"))

assert_unique(indicators, ["municipality_code"], "municipality_indicators")

total = indicators.count()
coverage = indicators.agg(*[
    F.round(F.count(c) / F.lit(total) * 100, 1).alias(f"{c}_pct")
    for c in ["population", "gdp_thousand_brl", "idhm", "fleet_total"]
])
print("Coverage (% of IBGE municipalities with a value):")
coverage.show(truncate=False)

write_table(indicators, "silver", "municipality_indicators")
log_change("silver_municipality_indicators", ["all"])
finish_step(CONSUMER)
