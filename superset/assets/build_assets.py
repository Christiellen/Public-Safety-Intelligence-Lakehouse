"""Builds the Superset import bundle (database + datasets) from datasets.yml and the real views.

    python build_assets.py --out /tmp/psi_datasets.zip

The bundle is imported with `superset import-datasources -p <zip> -u <admin>`. For each dataset of
datasets.yml it reads the columns of the view in PostgreSQL (information_schema, as the read-only
role), takes each column description from datasets.yml or from the Gold data dictionary
(docs/data_dictionary/data_dictionary.csv, tables listed in `sources`) and fails when a column is
left without description or a configured column does not exist.

UUIDs are derived from the names (uuid5), so every environment gets the same UUIDs and a dashboard
exported from one environment (superset/dashboards) imports into any other.

Settings (environment variables):
    PSI_READER_DSN        connection Superset uses (read-only role), e.g.
                          postgresql://psi_reader:<password>@db:5432/psi (required)
    PSI_DICTIONARY_CSV    data dictionary (default: ../../docs/data_dictionary/data_dictionary.csv)
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import sys
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import yaml

HERE = Path(__file__).resolve().parent
SPEC_FILE = HERE / "datasets.yml"
DEFAULT_DICTIONARY = HERE.parents[1] / "docs" / "data_dictionary" / "data_dictionary.csv"
BI_SCHEMA = "psi_bi"
BUNDLE = "psi_datasets"
UUID_NAMESPACE = uuid.UUID("5b0a3f3e-4f1c-4a55-9c5e-7e1d2c0a9b61")  # fixed: changing it breaks dashboards
DATETIME_TYPES = {"date", "timestamp without time zone", "timestamp with time zone"}


class AssetError(Exception):
    """datasets.yml, the dictionary and the real views disagree."""


def stable_uuid(*parts: str) -> str:
    return str(uuid.uuid5(UUID_NAMESPACE, "/".join(parts)))


def database_uuid(spec: dict) -> str:
    return stable_uuid("database", spec["database"]["name"])


def dataset_uuid(name: str) -> str:
    return stable_uuid("dataset", BI_SCHEMA, name)


def load_spec(path: Path = SPEC_FILE) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def load_dictionary(path: Path) -> dict:
    """{table: {column: description}} from the published data dictionary."""
    result: dict = {}
    with open(path, encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["column_name"]:
                result.setdefault(row["table_name"], {})[row["column_name"]] = row["description"]
    return result


def superset_uri(dsn: str) -> str:
    """postgresql://... -> postgresql+psycopg2://... (the driver Superset uses)."""
    scheme, rest = dsn.split("://", 1)
    return f"postgresql+psycopg2://{rest}" if scheme in ("postgres", "postgresql") else dsn


def read_view_columns(dsn: str, views: list[str]) -> dict:
    """{view: [(column, data_type)]} in the order of the view."""
    import psycopg2

    connection = psycopg2.connect(dsn)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT table_name, column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = ANY(%s) ORDER BY table_name, ordinal_position",
                (BI_SCHEMA, views))
            result: dict = {}
            for table, column, data_type in cursor.fetchall():
                result.setdefault(table, []).append((column, data_type))
    finally:
        connection.close()
    missing = sorted(set(views) - set(result))
    if missing:
        raise AssetError(f"Views not found in {BI_SCHEMA} (or not readable by the role): {missing}")
    return result


def describe_columns(name: str, config: dict, columns: list, dictionary: dict) -> list[dict]:
    overrides = config.get("columns") or {}
    real = [c for c, _ in columns]
    unknown = sorted(set(overrides) - set(real))
    if unknown:
        raise AssetError(f"{name}: columns in datasets.yml that the view does not have: {unknown}")
    undocumented, result = [], []
    for column, data_type in columns:
        override = overrides.get(column) or {}
        description = override.get("description")
        if not description:
            description = next((dictionary.get(t, {}).get(column) for t in config.get("sources", [])
                                if dictionary.get(t, {}).get(column)), None)
        if not description:
            undocumented.append(column)
        result.append({
            "column_name": column,
            "verbose_name": override.get("verbose_name"),
            "is_dttm": data_type in DATETIME_TYPES,
            "is_active": True,
            "type": data_type.upper(),
            "advanced_data_type": None,
            "groupby": True,
            "filterable": True,
            "expression": None,
            "description": description,
            "python_date_format": None,
            "datetime_format": None,
            "extra": None,
        })
    if undocumented:
        raise AssetError(f"{name}: columns without description (add them to datasets.yml or to the "
                         f"data dictionary): {undocumented}")
    return result


def dataset_config(name: str, config: dict, columns: list, dictionary: dict, spec: dict, catalog: str) -> dict:
    real = {c for c, _ in columns}
    dttm = config.get("main_dttm_col")
    if dttm and dttm not in real:
        raise AssetError(f"{name}: main_dttm_col {dttm} is not a column of the view")
    metrics = [{
        "metric_name": metric,
        "verbose_name": m.get("verbose_name"),
        "metric_type": None,
        "expression": m["expression"],
        "description": m.get("description"),
        "d3format": m.get("d3format"),
        "currency": None,
        "extra": None,
        "warning_text": None,
    } for metric, m in (config.get("metrics") or {}).items()]
    return {
        "table_name": name,
        "main_dttm_col": dttm,
        "currency_code_column": None,
        "description": config.get("description"),
        "default_endpoint": None,
        "offset": 0,
        "cache_timeout": None,
        "catalog": catalog,
        "schema": BI_SCHEMA,
        "sql": None,
        "params": None,
        "template_params": None,
        "filter_select_enabled": True,
        "fetch_values_predicate": None,
        "extra": None,
        "normalize_columns": False,
        "always_filter_main_dttm": False,
        "folders": None,
        "uuid": dataset_uuid(name),
        "metrics": metrics,
        "columns": describe_columns(name, config, columns, dictionary),
        "version": "1.0.0",
        "database_uuid": database_uuid(spec),
    }


def database_config(spec: dict, reader_dsn: str) -> dict:
    return {
        "database_name": spec["database"]["name"],
        "sqlalchemy_uri": superset_uri(reader_dsn),
        "cache_timeout": None,
        "expose_in_sqllab": True,
        "allow_run_async": False,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
        "allow_file_upload": False,
        "extra": {"metadata_params": {}, "engine_params": {}, "metadata_cache_timeout": {},
                  "schemas_allowed_for_file_upload": []},
        "impersonate_user": False,
        "configuration_method": "sqlalchemy_form",
        "uuid": database_uuid(spec),
        "version": "1.0.0",
    }


def build_bundle(spec: dict, view_columns: dict, dictionary: dict, reader_dsn: str) -> dict:
    """{path inside the zip: YAML content}."""
    catalog = urlsplit(reader_dsn).path.lstrip("/") or None
    files = {
        f"{BUNDLE}/metadata.yaml": {"version": "1.0.0", "type": "SqlaTable",
                                    "timestamp": datetime.now(timezone.utc).isoformat()},
        f"{BUNDLE}/databases/PSI_Lakehouse.yaml": database_config(spec, reader_dsn),
    }
    for name, config in spec["datasets"].items():
        files[f"{BUNDLE}/datasets/PSI_Lakehouse/{name}.yaml"] = dataset_config(
            name, config, view_columns[name], dictionary, spec, catalog)
    return files


def write_zip(files: dict, out: Path) -> None:
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        for path, content in files.items():
            text = io.StringIO()
            yaml.safe_dump(content, text, sort_keys=False, allow_unicode=True)
            bundle.writestr(path, text.getvalue())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", required=True, type=Path, help="zip file to write")
    args = parser.parse_args()
    dsn = os.environ.get("PSI_READER_DSN")
    if not dsn:
        print("PSI_READER_DSN is not set", file=sys.stderr)
        return 2
    spec = load_spec()
    dictionary = load_dictionary(Path(os.environ.get("PSI_DICTIONARY_CSV", DEFAULT_DICTIONARY)))
    try:
        files = build_bundle(spec, read_view_columns(dsn, list(spec["datasets"])), dictionary, dsn)
    except AssetError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    write_zip(files, args.out)
    print(f"Wrote {args.out}: database {spec['database']['name']} and {len(spec['datasets'])} datasets")
    return 0


if __name__ == "__main__":
    sys.exit(main())
