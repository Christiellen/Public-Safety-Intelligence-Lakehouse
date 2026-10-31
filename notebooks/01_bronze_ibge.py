# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze - IBGE (municipalities, population, GDP)
# MAGIC
# MAGIC Raw ingestion from two official IBGE APIs, with no business transformation:
# MAGIC
# MAGIC | Table | Source | Content |
# MAGIC |---|---|---|
# MAGIC | `bronze_ibge_population` | SIDRA table 6579, variable 9324 | Estimated resident population, last 5 published years. Also the **source of the municipality list** (code, name and state) |
# MAGIC | `bronze_ibge_gdp` | SIDRA table 5938, variable 37 | GDP at current prices (R$ thousand), latest year |
# MAGIC | `bronze_ibge_municipalities` *(optional)* | IBGE Localidades API | Regional hierarchy (meso, micro, immediate and intermediate regions), used only as enrichment |
# MAGIC
# MAGIC SIDRA column codes (`D1C`, `D2N`, `V`...) are renamed with the labels the API itself returns in its header row, converted to snake_case so Delta accepts them.
# MAGIC
# MAGIC The Localidades API is optional because some compute environments cannot reach `servicodados.ibge.gov.br`. Without it the pipeline still has every municipality, with code, name, state and region.
# MAGIC
# MAGIC **Incremental:** each response is compared with the last load by its SHA-256 hash; an unchanged response is not written again, so the notebooks downstream have nothing to reprocess.

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

import json

ensure_schemas()

# COMMAND ----------

def sidra_get(table: str, variable: str, period: str) -> tuple:
    """Calls the SIDRA API at municipality level (n6). Returns the raw response (for the change
    check) and a function that builds the Spark DataFrame with columns named after the header row."""
    url = f"{SIDRA_BASE}/t/{table}/n6/all/v/{variable}/p/{period}"
    content = http_get(url).content

    def build():
        payload = json.loads(content)
        if len(payload) < 2:
            raise ValueError(f"SIDRA returned no data rows for {url}")
        header, rows = payload[0], payload[1:]
        return add_audit(pandas_to_spark_str(pd.DataFrame(rows).rename(columns=header)), f"IBGE_SIDRA_{table}", url)

    return content, build


# 2. Estimated population: last 5 years, so Silver can pick the latest one and the year closest to the GDP year
write_if_changed("bronze_ibge_population",
                 *sidra_get(SIDRA_POPULATION["table"], SIDRA_POPULATION["variable"], SIDRA_POPULATION["period"]))

# 3. Municipal GDP: latest published year
write_if_changed("bronze_ibge_gdp", *sidra_get(SIDRA_GDP["table"], SIDRA_GDP["variable"], "last%201"))

# 4. Optional enrichment: regional hierarchy (IBGE Localidades). Skipped if the host is unreachable.
try:
    content = http_get(IBGE_MUNICIPALITIES_URL, retries=1).content
    write_if_changed("bronze_ibge_municipalities", content, lambda: add_audit(
        pandas_to_spark_str(pd.json_normalize(json.loads(content), sep=".")),
        "IBGE_LOCALIDADES_API", IBGE_MUNICIPALITIES_URL))
except Exception as error:
    print(f"WARNING: IBGE Localidades not loaded ({error}). Regional hierarchy columns will be empty; "
          "code, name, state and region still come from SIDRA.")

display(spark.table(tbl("bronze", "bronze_ibge_gdp")).limit(5))
