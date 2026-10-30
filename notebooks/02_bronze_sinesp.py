# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze - SINESP (public safety occurrences)
# MAGIC
# MAGIC Downloads the national public safety statistics file (SINESP / VDE) for each year in `SINESP_YEARS` and stores every row as published, as text.
# MAGIC
# MAGIC - A missing year (file not yet published, or URL changed) is reported and skipped; the notebook fails only if **no** year could be loaded.
# MAGIC - `_source_year` records which yearly file each row came from.

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

import io

ensure_schemas()

frames, loaded_years, failed_years = [], [], []
for year in SINESP_YEARS:
    url = SINESP_URL_TEMPLATE.format(year=year)
    try:
        content = http_get(url).content
        pdf = pd.read_excel(io.BytesIO(content), engine="openpyxl", dtype=str)
        df_year = add_audit(pandas_to_spark_str(pdf), "SINESP_VDE_XLSX", url).withColumn("_source_year", F.lit(year))
        frames.append(df_year)
        loaded_years.append(year)
        print(f"Year {year}: {len(pdf):,} rows")
    except Exception as error:  # keep going with the other years
        failed_years.append(year)
        print(f"WARNING: year {year} not loaded: {error}")

if not frames:
    raise RuntimeError("No SINESP file could be loaded. Check SINESP_URL_TEMPLATE and SINESP_YEARS.")

df_bronze_sinesp = frames[0]
for frame in frames[1:]:
    df_bronze_sinesp = df_bronze_sinesp.unionByName(frame, allowMissingColumns=True)

write_table(df_bronze_sinesp, "bronze", "bronze_sinesp_occurrences")
print(f"Loaded years: {loaded_years} | Failed years: {failed_years or 'none'}")
display(spark.table(tbl("bronze", "bronze_sinesp_occurrences")).limit(5))