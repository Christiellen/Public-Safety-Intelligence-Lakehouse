# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Bronze - Municipal Human Development Index (IDHM)
# MAGIC
# MAGIC Loads the IDHM series (overall, education, longevity and income) from the Ipeadata OData API, which republishes the Atlas of Human Development in Brazil (PNUD, Ipea and FJP).
# MAGIC
# MAGIC **Known limitation:** the latest municipal IDHM is from the **2010** Census. Newer editions of the Atlas cover states and metropolitan regions, not municipalities. The year travels with the data (`idhm_year` in Silver) so consumers can see it.
# MAGIC
# MAGIC **Incremental:** the responses of the four series are hashed together; the table is written again only when any of them changed.

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

import json

ensure_schemas()

responses = {}
for series in IDHM_SERIES.values():
    url = IPEADATA_URL_TEMPLATE.format(series=series)
    responses[series] = (url, http_get(url).content)


def build_idhm():
    frames = []
    for series, (url, content) in responses.items():
        values = json.loads(content).get("value", [])
        if not values:
            raise ValueError(f"Ipeadata returned no values for {series}")
        frames.append(add_audit(pandas_to_spark_str(pd.DataFrame(values)), "IPEADATA_ODATA4", url))
        print(f"{series}: {len(values):,} values")
    df = frames[0]
    for frame in frames[1:]:
        df = df.unionByName(frame, allowMissingColumns=True)
    return df


write_if_changed("bronze_ipea_idhm", b"".join(content for _, content in responses.values()), build_idhm)
display(spark.table(tbl("bronze", "bronze_ipea_idhm")).groupBy("sercodigo", "nivnome").count())
