# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Silver - Municipality dimension (conformed)
# MAGIC
# MAGIC Builds the conformed municipality dimension. Every other table in the lakehouse joins to it through **`municipality_code`**, the 7-digit IBGE code.
# MAGIC
# MAGIC **Where the list comes from.** The SIDRA population response (table 6579) lists every municipality with its code and the label `Name - UF`. State and region come from the code itself: its first two digits are the official IBGE state code (`UF_REFERENCE`). The IBGE Localidades API, when available, only adds the regional hierarchy (meso, micro, immediate and intermediate regions).
# MAGIC
# MAGIC **Why this matters.** Brazil has many municipalities with the same name in different states (e.g. *Bom Jesus* exists in five states). Joining by name alone silently attaches one city's population to another. Here the name is used only once, to match sources that do not carry the IBGE code (SINESP, SENATRAN), and always together with the state.
# MAGIC
# MAGIC `name_key` (letters and digits only) is the matching key, and it must be unique within each state.
# MAGIC
# MAGIC **Incremental:** Rebuilt (one row per municipality) only when the IBGE population or Localidades load changed.

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

CONSUMER = "05_silver_dim_municipality"
run_or_exit(CONSUMER, ["bronze_ibge_population", "bronze_ibge_municipalities"],
            outputs=[("silver", "dim_municipality")])

# COMMAND ----------

# 1. Municipality list from SIDRA: latest label per code ("Name - UF")
population = spark.table(tbl("bronze", "bronze_ibge_population"))
latest_label = (population
                .select(F.lpad("municipio_codigo", 7, "0").alias("municipality_code"),
                        F.col("municipio").alias("_label"),
                        try_cast("ano", "INT").alias("_year"))
                .filter(F.col("municipality_code").rlike(r"^\d{7}$"))
                .withColumn("_rn", F.row_number().over(Window.partitionBy("municipality_code").orderBy(F.desc("_year"))))
                .filter("_rn = 1"))

uf_reference = spark.createDataFrame(
    [(code, *values) for code, values in UF_REFERENCE.items()],
    "_uf_code string, state_code string, state_name string, region_code string, region_name string")

dim_municipality = (latest_label
                    .withColumn("municipality_name", F.trim(F.regexp_extract("_label", r"^(.*) - [A-Z]{2}$", 1)))
                    .withColumn("_label_uf", F.regexp_extract("_label", r" - ([A-Z]{2})$", 1))
                    .withColumn("_uf_code", F.substring("municipality_code", 1, 2))
                    .join(F.broadcast(uf_reference), "_uf_code", "left"))

# The state in the label must agree with the state derived from the code
inconsistent = dim_municipality.filter((F.col("_label_uf") != "") & (F.col("_label_uf") != F.col("state_code")))
if inconsistent.count():
    inconsistent.select("municipality_code", "_label", "state_code").show(truncate=False)
    raise AssertionError("State in the SIDRA label differs from the state derived from the IBGE code")

# COMMAND ----------

# 2. Optional enrichment with the regional hierarchy (IBGE Localidades)
hierarchy_columns = {
    "mesoregion_name": "microrregiao_mesorregiao_nome",
    "microregion_name": "microrregiao_nome",
    "intermediate_region_name": "regiao_imediata_regiao_intermediaria_nome",
    "immediate_region_name": "regiao_imediata_nome",
}
localidades = tbl("bronze", "bronze_ibge_municipalities")
if spark.catalog.tableExists(localidades):
    src = spark.table(localidades)
    hierarchy = src.select(F.lpad("id", 7, "0").alias("municipality_code"),
                           *[(F.col(c) if c in src.columns else F.lit(None).cast("string")).alias(alias)
                             for alias, c in hierarchy_columns.items()])
    dim_municipality = dim_municipality.join(hierarchy, "municipality_code", "left")
    print("Regional hierarchy added from IBGE Localidades.")
else:
    for alias in hierarchy_columns:
        dim_municipality = dim_municipality.withColumn(alias, F.lit(None).cast("string"))
    print("IBGE Localidades not available: regional hierarchy columns left empty.")

dim_municipality = (dim_municipality
                    .withColumn("name_key", name_key(F.col("municipality_name")))
                    .select("municipality_code", "municipality_name", "state_code", "state_name",
                            "region_code", "region_name", *hierarchy_columns.keys(), "name_key"))

# COMMAND ----------

# 3. Quality gates
assert_not_empty(dim_municipality, "dim_municipality")
assert_unique(dim_municipality, ["municipality_code"], "dim_municipality")
assert_unique(dim_municipality, ["state_code", "name_key"], "dim_municipality name within state")
for column in ["municipality_name", "state_code"]:
    missing = dim_municipality.filter(F.col(column).isNull() | (F.col(column) == "")).count()
    if missing:
        raise AssertionError(f"{missing} municipalities without {column}")

# Every alias must point to an existing IBGE name, otherwise it would hide a mismatch
alias_targets = spark.createDataFrame(
    [(uf, dst) for (uf, _), dst in MUNICIPALITY_ALIASES.items()], "state_code string, name_key string")
broken_aliases = alias_targets.join(dim_municipality, ["state_code", "name_key"], "left_anti")
if broken_aliases.count():
    broken_aliases.show(truncate=False)
    print("WARNING: aliases above point to names that do not exist in IBGE; review MUNICIPALITY_ALIASES.")

write_table(dim_municipality, "silver", "dim_municipality")
log_change("silver_dim_municipality", ["all"])
finish_step(CONSUMER)
display(spark.table(tbl("silver", "dim_municipality")).groupBy("region_name").count().orderBy("region_name"))
