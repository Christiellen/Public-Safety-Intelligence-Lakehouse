# Databricks notebook source
# MAGIC %md
# MAGIC # 21 - Gold - Export for BI (Superset)
# MAGIC
# MAGIC Exports the Gold tables used by the Superset dashboard ([`superset/`](../superset/README.md)) as compressed CSV files, one per table, plus a `manifest.json` with the row count, the columns and their types, and the SHA-256 of each file.
# MAGIC
# MAGIC The files are saved to the volume `exports` of the Gold schema, under `superset/`. Copy them to `superset/data/` in the repository: the loader of the Superset stack creates the tables in PostgreSQL from the manifest, loads the files and refuses any file whose hash or row count differs from the manifest.
# MAGIC
# MAGIC | Table | Grain |
# MAGIC |---|---|
# MAGIC | `municipality_analytics` | One row per municipality |
# MAGIC | `municipality_spatial_metrics` | One row per municipality |
# MAGIC | `state_analytics` | One row per state |
# MAGIC | `graph_communities` | One row per Louvain community |
# MAGIC | `dim_state`, `dim_event`, `dim_date` | Dimensions |
# MAGIC | `fact_occurrences` | Municipality x event x agency x month x measure type |
# MAGIC | `fact_state_occurrences` | State x event x agency x month x measure type |
# MAGIC
# MAGIC Rows are sorted by the primary key, so a new export of unchanged data produces the same files (and a clean Git diff).
# MAGIC
# MAGIC **On demand:** not part of the scheduled job. **Incremental:** the files are exported again only when one of the Gold inputs changed, or when the manifest is missing.

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

import os

CONSUMER = "21_gold_export_bi"
EXPORT_DIR = f"/Volumes/{CATALOG}/{SCHEMAS['gold']}/exports/superset"
MANIFEST_FILE = "manifest.json"
pending = run_or_exit(CONSUMER, ["gold_facts", "gold_dimensions", "gold_municipality_analytics",
                                 "gold_state_analytics", "gold_spatial"],
                      output_missing=not os.path.exists(os.path.join(EXPORT_DIR, MANIFEST_FILE)))

# COMMAND ----------

import glob
import json
import shutil
from datetime import datetime, timezone

# Table -> primary key (sort order of the file, and the key checked for uniqueness)
EXPORT_TABLES = {
    "municipality_analytics": ["municipality_code"],
    "municipality_spatial_metrics": ["municipality_code"],
    "state_analytics": ["state_code"],
    "graph_communities": ["community_id"],
    "dim_state": ["state_code"],
    "dim_event": ["event_id"],
    "dim_date": ["date_id"],
    "fact_occurrences": ["municipality_code", "event_id", "agency", "date_id", "measure_type"],
    "fact_state_occurrences": ["state_code", "event_id", "agency", "date_id", "measure_type"],
}
# GitHub rejects files above 100 MB; warn well before that
SIZE_WARNING_MB = 50


def export_csv(df, target_file: str) -> None:
    """Writes `df` as a single gzip CSV file. Spark writes a folder with one part file; the part is
    moved to `target_file` and the folder removed. Nulls are written as an empty field and empty
    strings as "", which is how PostgreSQL COPY (CSV) tells them apart."""
    staging = f"{target_file}.parts"
    shutil.rmtree(staging, ignore_errors=True)
    (df.coalesce(1).write.mode("overwrite")
     .option("header", True).option("compression", "gzip")
     .option("dateFormat", "yyyy-MM-dd").option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
     .csv(staging))
    parts = glob.glob(os.path.join(staging, "part-*.csv.gz"))
    if len(parts) != 1:
        raise AssertionError(f"Expected one part file in {staging}, found {len(parts)}")
    shutil.move(parts[0], target_file)
    shutil.rmtree(staging, ignore_errors=True)


def file_sha256(path: str) -> str:
    with open(path, "rb") as handle:
        return sha256(handle.read())

# COMMAND ----------

if CATALOG:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMAS['gold']}.exports")
os.makedirs(EXPORT_DIR, exist_ok=True)

entries = []
for name, keys in EXPORT_TABLES.items():
    df = spark.table(tbl("gold", name))
    missing_keys = set(keys) - set(df.columns)
    if missing_keys:
        raise AssertionError(f"gold.{name}: key columns {sorted(missing_keys)} not found")
    assert_unique(df, keys, f"gold.{name}")
    rows = assert_not_empty(df, f"gold.{name}")
    file_name = f"{name}.csv.gz"
    path = os.path.join(EXPORT_DIR, file_name)
    export_csv(df.orderBy(*keys), path)
    size_mb = os.path.getsize(path) / 1024 / 1024
    if size_mb > SIZE_WARNING_MB:
        print(f"WARNING {file_name}: {size_mb:.1f} MB; GitHub rejects files above 100 MB")
    entries.append({"table": name, "file": file_name, "rows": rows, "primary_key": keys,
                    "columns": [{"name": c, "type": t} for c, t in df.dtypes],
                    "sha256": file_sha256(path), "size_bytes": os.path.getsize(path)})
    print(f"{file_name}: {rows:,} rows, {size_mb:.2f} MB")

window = spark.table(tbl("gold", "municipality_analytics")).select("window_start", "window_end").first()
manifest = {
    "exported_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "window_start": str(window.window_start),
    "window_end": str(window.window_end),
    "tables": entries,
}
with open(os.path.join(EXPORT_DIR, MANIFEST_FILE), "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=2, ensure_ascii=False)
    handle.write("\n")

finish_step(CONSUMER)
print(f"Exported {len(entries)} tables to {EXPORT_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy the files to the repository
# MAGIC
# MAGIC With the Databricks CLI, from the root of the repository:
# MAGIC
# MAGIC ```bash
# MAGIC databricks fs cp -r --overwrite dbfs:/Volumes/workspace/psi_gold/exports/superset superset/data
# MAGIC ```
# MAGIC
# MAGIC Or download them from Catalog Explorer (*workspace > psi_gold > exports > superset*). Then start the stack (`docker compose up` in `superset/`): the loader checks every file against the manifest.
