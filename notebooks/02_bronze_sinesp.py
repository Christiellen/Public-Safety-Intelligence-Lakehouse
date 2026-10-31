# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze - SINESP (public safety occurrences)
# MAGIC
# MAGIC Downloads the national public safety statistics file (SINESP / VDE) for each year in `SINESP_YEARS` and stores every row as published, as text.
# MAGIC
# MAGIC - A missing year (file not yet published, or URL changed) is reported and skipped; the notebook fails only if **no** year could be loaded.
# MAGIC - `_source_year` records which yearly file each row came from, and is the partition of the table.
# MAGIC
# MAGIC **Incremental:** the current year's file is republished every month, while closed years rarely change. Each file is compared with the last load by its SHA-256 hash: an unchanged year is skipped, and a changed year **replaces only its own partition**. The years written are recorded in `psi_ops.change_log`, so `07_silver_sinesp_occurrences` reprocesses only them.

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

import io

ensure_schemas()
DATASET = "bronze_sinesp_occurrences"

frames, changed_years, unchanged_years, failed_years, digests = [], [], [], [], {}
for year in SINESP_YEARS:
    url = SINESP_URL_TEMPLATE.format(year=year)
    try:
        content = http_get(url).content
    except Exception as error:  # keep going with the other years
        failed_years.append(year)
        print(f"WARNING: year {year} not loaded: {error}")
        continue
    digests[year] = sha256(content)
    if is_unchanged(DATASET, year, digests[year]):
        unchanged_years.append(year)
        print(f"Year {year}: file unchanged since the last load, skipped")
        continue
    pdf = pd.read_excel(io.BytesIO(content), engine="openpyxl", dtype=str)
    frames.append(add_audit(pandas_to_spark_str(pdf), "SINESP_VDE_XLSX", url).withColumn("_source_year", F.lit(year)))
    changed_years.append((year, len(pdf)))
    print(f"Year {year}: {len(pdf):,} rows (new or changed file)")

if not frames and not unchanged_years:
    raise RuntimeError("No SINESP file could be loaded. Check SINESP_URL_TEMPLATE and SINESP_YEARS.")

if frames:
    df_bronze_sinesp = frames[0]
    for frame in frames[1:]:
        df_bronze_sinesp = df_bronze_sinesp.unionByName(frame, allowMissingColumns=True)
    replace_partitions(df_bronze_sinesp, "bronze", DATASET, "_source_year", [year for year, _ in changed_years])
    for year, rows in changed_years:
        log_change(DATASET, [year], digests[year], rows)

print(f"Changed years: {[y for y, _ in changed_years] or 'none'} | Unchanged: {unchanged_years or 'none'} | "
      f"Failed: {failed_years or 'none'}")
display(spark.table(tbl("bronze", DATASET)).groupBy("_source_year").count().orderBy("_source_year"))
