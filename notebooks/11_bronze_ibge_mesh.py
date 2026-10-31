# Databricks notebook source
# MAGIC %md
# MAGIC # 11 - Bronze - IBGE municipal mesh (raw file)
# MAGIC
# MAGIC Downloads the official municipal boundaries (*Malha Municipal*) published by IBGE and stores the original zip file in a Unity Catalog **volume**, the Bronze layer for raw files.
# MAGIC
# MAGIC - The latest edition is **discovered** from the GeoFTP directory listing (`municipio_YYYY/Brasil/BR_Municipios_YYYY.zip`), so the notebook keeps working when a new year is published.
# MAGIC - `bronze_ibge_mesh_files` records which file was downloaded, from where, its size and when, so Silver always reads a known file.
# MAGIC
# MAGIC **Incremental:** an edition already in the volume is not downloaded again (the mesh is about 150 MB and changes once a year). A new edition is added to the manifest, which keeps the history of editions; Silver reads the latest one.

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

import os
import re
import shutil

ensure_schemas()
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMAS['bronze']}.{RAW_VOLUME}")

# 1. Discover the latest edition with a national file
listing = http_get(IBGE_MESH_BASE).text
years = sorted({int(y) for y in re.findall(r"municipio_(\d{4})/", listing)}, reverse=True)
if not years:
    raise ValueError(f"No municipio_YYYY folder found in {IBGE_MESH_BASE}")

mesh_url, mesh_year = None, None
for year in years:
    folder = f"{IBGE_MESH_BASE}municipio_{year}/Brasil/"
    try:
        files = re.findall(r'href="(BR_Municipios_\d{4}\.zip)"', http_get(folder, retries=1).text)
    except Exception as error:
        print(f"{year}: no national folder ({error})")
        continue
    if files:
        mesh_url, mesh_year = folder + files[0], year
        break

if mesh_url is None:
    raise ValueError(f"No BR_Municipios_YYYY.zip found for years {years}")
print(f"Latest edition: {mesh_year} -> {mesh_url}")

manifest_table = tbl("bronze", "bronze_ibge_mesh_files")
if not FULL_REFRESH and spark.catalog.tableExists(manifest_table):
    loaded = spark.table(manifest_table).filter(F.col("mesh_year") == mesh_year).orderBy(F.desc("_ingested_at")).first()
    if loaded is not None and os.path.exists(loaded.volume_path):
        dbutils.notebook.exit(f"Mesh {mesh_year} already loaded ({loaded.volume_path}); nothing to do.")

# COMMAND ----------

# 2. Download to local disk (streamed), then copy to the volume
file_name = mesh_url.rsplit("/", 1)[-1]
local_path = f"/tmp/{file_name}"
digest = hashlib.sha256()
with requests.get(mesh_url, stream=True, timeout=HTTP_TIMEOUT) as response:
    response.raise_for_status()
    with open(local_path, "wb") as handle:
        for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
            handle.write(chunk)
            digest.update(chunk)

target_dir = volume_path("ibge_mesh", str(mesh_year))
os.makedirs(target_dir, exist_ok=True)
target_path = f"{target_dir}/{file_name}"
shutil.copy(local_path, target_path)
size_mb = os.path.getsize(target_path) / 1024 / 1024
print(f"Saved {target_path} ({size_mb:.1f} MB)")

# 3. Manifest
manifest = spark.createDataFrame(
    [(mesh_year, file_name, target_path, mesh_url, round(size_mb, 1))],
    "mesh_year int, file_name string, volume_path string, source_url string, size_mb double",
).withColumn("_ingested_at", F.current_timestamp())
# Appended, not overwritten: the manifest keeps every edition loaded (Silver reads the latest)
manifest.write.format(TABLE_FORMAT).mode("append").saveAsTable(manifest_table)
log_change("bronze_ibge_mesh", [mesh_year], digest.hexdigest())
display(spark.table(manifest_table).orderBy(F.desc("_ingested_at")))
