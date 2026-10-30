# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Bronze - Municipal Human Development Index (IDHM)
# MAGIC
# MAGIC Loads the IDHM series (overall, education, longevity and income) from the Ipeadata OData API, which republishes the Atlas of Human Development in Brazil (PNUD, Ipea and FJP).
# MAGIC
# MAGIC **Known limitation:** the latest municipal IDHM is from the **2010** Census. Newer editions of the Atlas cover states and metropolitan regions, not municipalities. The year travels with the data (`idhm_year` in Silver) so consumers can see it.

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

ensure_schemas()

frames = []
for label, series in IDHM_SERIES.items():
    url = IPEADATA_URL_TEMPLATE.format(series=series)
    values = http_get(url).json().get("value", [])
    if not values:
        raise ValueError(f"Ipeadata returned no values for {series}")
    frames.append(add_audit(pandas_to_spark_str(pd.DataFrame(values)), "IPEADATA_ODATA4", url))
    print(f"{series}: {len(values):,} values")

df_bronze_idhm = frames[0]
for frame in frames[1:]:
    df_bronze_idhm = df_bronze_idhm.unionByName(frame, allowMissingColumns=True)

write_table(df_bronze_idhm, "bronze", "bronze_ipea_idhm")
display(spark.table(tbl("bronze", "bronze_ipea_idhm")).groupBy("sercodigo", "nivnome").count())