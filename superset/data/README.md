# Gold export

Files produced by `notebooks/21_gold_export_bi`: one `<table>.csv.gz` per Gold table and `manifest.json` (rows, columns and types, primary key and SHA-256 of each file). The loader of the Superset stack refuses any file that does not match the manifest, so copy the whole folder from the same export:

```bash
databricks fs cp -r --overwrite dbfs:/Volumes/workspace/psi_gold/exports/superset superset/data
```

Do not edit the files by hand: change the pipeline and export again.
