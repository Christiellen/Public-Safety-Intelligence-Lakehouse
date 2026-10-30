# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Bronze - SENATRAN (vehicle fleet by municipality)
# MAGIC
# MAGIC Downloads the monthly *Frota por Município e Tipo* spreadsheet published by SENATRAN (Ministry of Transport) and stores it as text.
# MAGIC
# MAGIC The spreadsheet has title rows above the table, so the header row is located automatically: the first row containing a `MUNICIPIO` cell. Copies of the header row inside the table are removed. To refresh, update `SENATRAN_FLEET_URL` and `SENATRAN_REFERENCE` in `00_config_utils`.

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

import io

ensure_schemas()

content = http_get(SENATRAN_FLEET_URL).content
raw = pd.read_excel(io.BytesIO(content), engine="openpyxl", header=None, dtype=str)


def find_header_row(frame: pd.DataFrame, marker: str = "MUNICIPIO", max_rows: int = 40) -> int:
    for idx in range(min(max_rows, len(frame))):
        cells = [to_snake(v).upper() for v in frame.iloc[idx].tolist() if isinstance(v, str)]
        if marker in cells:
            return idx
    raise ValueError(f"Header row with '{marker}' not found in the first {max_rows} rows")


header_idx = find_header_row(raw)
pdf_fleet = raw.iloc[header_idx + 1:].copy()
pdf_fleet.columns = raw.iloc[header_idx].tolist()
pdf_fleet = pdf_fleet.dropna(how="all")

# The spreadsheet repeats the header row below the first one: drop rows that are a copy of the header
header_values = [str(v).strip().upper() for v in pdf_fleet.columns]
is_header_copy = pdf_fleet.apply(lambda r: [str(v).strip().upper() for v in r.tolist()] == header_values, axis=1)
pdf_fleet = pdf_fleet[~is_header_copy]
print(f"Header found at row {header_idx}; repeated header rows removed: {int(is_header_copy.sum())}")
print(f"Columns: {list(pdf_fleet.columns)}")

df_bronze_fleet = (add_audit(pandas_to_spark_str(pdf_fleet), "SENATRAN_FROTA_MUNICIPIO_TIPO", SENATRAN_FLEET_URL)
                   .withColumn("_reference_month", F.lit(SENATRAN_REFERENCE)))

assert_not_empty(df_bronze_fleet, "SENATRAN fleet")
write_table(df_bronze_fleet, "bronze", "bronze_senatran_fleet")
display(spark.table(tbl("bronze", "bronze_senatran_fleet")).limit(5))