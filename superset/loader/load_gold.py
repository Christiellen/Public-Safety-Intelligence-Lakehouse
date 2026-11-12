"""Loads the Gold export (notebook 21) into PostgreSQL and rebuilds the BI views for Superset.

Reads superset/data/manifest.json and, in a single transaction:

1. checks every file against the manifest (SHA-256 and CSV header) before touching the database;
2. recreates the schema psi_gold with the tables and column types of the manifest and loads the
   files with COPY;
3. checks the row count of every table and the uniqueness of its primary key;
4. recreates the views of the schema psi_bi (superset/sql/views.sql) and checks them (VIEW_CHECKS);
5. grants the read-only role used by Superset access to psi_bi only (never to psi_gold).

Superset keeps reading the previous version until the transaction commits; a failed load leaves the
database as it was. A load with the same manifest and the same views is skipped (psi_meta.load_log).

Settings (environment variables):
    PSI_DB_DSN           connection of the owner of the psi database (required)
    PSI_READER_USER      read-only role for Superset (default psi_reader)
    PSI_READER_PASSWORD  password of that role; when empty the role is not created or changed
    PSI_DATA_DIR         folder with manifest.json and the .csv.gz files (default: ../data)
    PSI_VIEWS_SQL        views file (default: ../sql/views.sql)
    PSI_FORCE_RELOAD     1 to load even when nothing changed
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import os
import sys
from pathlib import Path

import psycopg2
from psycopg2 import sql

HERE = Path(__file__).resolve().parent
DEFAULT_DATA_DIR = HERE.parent / "data"
DEFAULT_VIEWS_SQL = HERE.parent / "sql" / "views.sql"
MANIFEST_FILE = "manifest.json"
GOLD_SCHEMA = "psi_gold"
BI_SCHEMA = "psi_bi"
META_SCHEMA = "psi_meta"

# Spark SQL type (as in DataFrame.dtypes) -> PostgreSQL type
SPARK_TO_POSTGRES = {
    "string": "text",
    "boolean": "boolean",
    "tinyint": "smallint",
    "smallint": "smallint",
    "int": "integer",
    "bigint": "bigint",
    "float": "real",
    "double": "double precision",
    "date": "date",
    "timestamp": "timestamp",
    "timestamp_ntz": "timestamp",
}


# Checks on the BI views, run in the load transaction: a non-zero count stops the load
VIEW_CHECKS = [
    ("cvli_monthly_municipality has negative victims: a state publishes fewer CVLI victims than its "
     "municipalities add up to, so the 'Not assigned' rows cannot be derived",
     "SELECT COUNT(*) FROM psi_bi.cvli_monthly_municipality WHERE cvli_victims < 0"),
    ("cvli_monthly_municipality repeats an area and month",
     "SELECT COUNT(*) - COUNT(DISTINCT (municipality_label, month)) FROM psi_bi.cvli_monthly_municipality"),
]


class LoadError(Exception):
    """The export is not consistent with its manifest, or the loaded data fails a check."""


def postgres_type(spark_type: str) -> str:
    spark_type = spark_type.strip().lower()
    if spark_type in SPARK_TO_POSTGRES:
        return SPARK_TO_POSTGRES[spark_type]
    if spark_type.startswith("decimal(") and spark_type.endswith(")"):
        return "numeric" + spark_type[len("decimal"):]
    raise LoadError(f"Unsupported column type: {spark_type}")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def read_manifest(data_dir: Path) -> tuple[dict, str]:
    path = data_dir / MANIFEST_FILE
    if not path.exists():
        raise LoadError(f"{path} not found. Run notebook 21_gold_export_bi and copy its files to {data_dir}.")
    content = path.read_bytes()
    return json.loads(content), hashlib.sha256(content).hexdigest()


def csv_header(path: Path) -> list[str]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        return next(csv.reader(handle))


def verify_files(manifest: dict, data_dir: Path) -> None:
    """Every file exists, has the hash of the manifest and the expected columns, in order."""
    problems = []
    for entry in manifest["tables"]:
        path = data_dir / entry["file"]
        if not path.exists():
            problems.append(f"{entry['file']}: file not found")
            continue
        if file_sha256(path) != entry["sha256"]:
            problems.append(f"{entry['file']}: SHA-256 differs from the manifest (file changed or truncated)")
            continue
        expected = [c["name"] for c in entry["columns"]]
        if csv_header(path) != expected:
            problems.append(f"{entry['file']}: header differs from the manifest columns")
        missing_keys = set(entry["primary_key"]) - set(expected)
        if missing_keys:
            problems.append(f"{entry['file']}: primary key columns {sorted(missing_keys)} not in the file")
    if problems:
        raise LoadError("Export does not match its manifest:\n  " + "\n  ".join(problems))


def create_table(cursor, entry: dict) -> None:
    columns = sql.SQL(", ").join(
        sql.SQL("{} {}").format(sql.Identifier(c["name"]), sql.SQL(postgres_type(c["type"])))
        for c in entry["columns"])
    cursor.execute(sql.SQL("CREATE TABLE {}.{} ({})").format(
        sql.Identifier(GOLD_SCHEMA), sql.Identifier(entry["table"]), columns))


def copy_table(cursor, entry: dict, data_dir: Path) -> None:
    columns = sql.SQL(", ").join(sql.Identifier(c["name"]) for c in entry["columns"])
    statement = sql.SQL("COPY {}.{} ({}) FROM STDIN WITH (FORMAT csv, HEADER true)").format(
        sql.Identifier(GOLD_SCHEMA), sql.Identifier(entry["table"]), columns)
    with gzip.open(data_dir / entry["file"], "rt", encoding="utf-8", newline="") as handle:
        cursor.copy_expert(statement.as_string(cursor.connection), handle)


def check_table(cursor, entry: dict) -> None:
    """Row count as in the manifest, and the primary key unique (nulls compared as equal, as Spark
    does in the Gold quality gates)."""
    table = sql.SQL("{}.{}").format(sql.Identifier(GOLD_SCHEMA), sql.Identifier(entry["table"]))
    cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(table))
    rows = cursor.fetchone()[0]
    if rows != entry["rows"]:
        raise LoadError(f"{entry['table']}: {rows:,} rows loaded, {entry['rows']:,} in the manifest")
    keys = sql.SQL(", ").join(sql.Identifier(k) for k in entry["primary_key"])
    cursor.execute(sql.SQL("CREATE UNIQUE INDEX {} ON {} ({}) NULLS NOT DISTINCT").format(
        sql.Identifier(f"{entry['table']}_pk"), table, keys))
    cursor.execute(sql.SQL("ANALYZE {}").format(table))


def ensure_meta(cursor) -> None:
    cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(META_SCHEMA)))
    cursor.execute(sql.SQL(
        "CREATE TABLE IF NOT EXISTS {}.load_log (manifest_sha256 text NOT NULL, views_sha256 text NOT NULL, "
        "exported_at text, window_start date, window_end date, tables integer, total_rows bigint, "
        "loaded_at timestamptz NOT NULL DEFAULT now())").format(sql.Identifier(META_SCHEMA)))


def already_loaded(cursor, manifest_sha: str, views_sha: str) -> bool:
    cursor.execute(sql.SQL("SELECT manifest_sha256, views_sha256 FROM {}.load_log ORDER BY loaded_at DESC LIMIT 1")
                   .format(sql.Identifier(META_SCHEMA)))
    last = cursor.fetchone()
    cursor.execute("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name IN (%s, %s)",
                   (GOLD_SCHEMA, BI_SCHEMA))
    return last == (manifest_sha, views_sha) and cursor.fetchone()[0] == 2


def ensure_reader(cursor, reader: str, password: str | None) -> None:
    """Creates or updates the read-only role (when a password is given) and grants it psi_bi only."""
    if password:
        cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (reader,))
        verb = "ALTER" if cursor.fetchone() else "CREATE"
        cursor.execute(sql.SQL(verb + " ROLE {} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD {}")
                       .format(sql.Identifier(reader), sql.Literal(password)))
    cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (reader,))
    if not cursor.fetchone():
        print(f"Role {reader} does not exist and PSI_READER_PASSWORD is empty: grants skipped")
        return
    role = sql.Identifier(reader)
    cursor.execute("SELECT current_database()")
    database = sql.Identifier(cursor.fetchone()[0])
    cursor.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(database, role))
    cursor.execute(sql.SQL("REVOKE CREATE ON SCHEMA public FROM {}").format(role))
    cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(sql.Identifier(BI_SCHEMA), role))
    cursor.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(sql.Identifier(BI_SCHEMA), role))


def load(dsn: str, data_dir: Path = DEFAULT_DATA_DIR, views_sql: Path = DEFAULT_VIEWS_SQL,
         reader: str = "psi_reader", reader_password: str | None = None, force: bool = False) -> dict:
    """Runs the whole load. Returns a summary: {"status": "loaded" | "skipped", "rows": {table: rows}}."""
    manifest, manifest_sha = read_manifest(data_dir)
    views = views_sql.read_text(encoding="utf-8")
    views_sha = hashlib.sha256(views.encode("utf-8")).hexdigest()
    verify_files(manifest, data_dir)
    rows = {e["table"]: e["rows"] for e in manifest["tables"]}

    connection = psycopg2.connect(dsn)
    try:
        with connection, connection.cursor() as cursor:   # one transaction: commit on success
            ensure_meta(cursor)
            if not force and already_loaded(cursor, manifest_sha, views_sha):
                ensure_reader(cursor, reader, reader_password)
                print(f"Export of {manifest.get('exported_at')} already loaded; nothing to do.")
                return {"status": "skipped", "rows": rows}
            cursor.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(BI_SCHEMA)))
            cursor.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(GOLD_SCHEMA)))
            cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(GOLD_SCHEMA)))
            for entry in manifest["tables"]:
                create_table(cursor, entry)
                copy_table(cursor, entry, data_dir)
                check_table(cursor, entry)
                print(f"{GOLD_SCHEMA}.{entry['table']}: {entry['rows']:,} rows")
            cursor.execute(views)
            for problem, query in VIEW_CHECKS:
                cursor.execute(query)
                if cursor.fetchone()[0]:
                    raise LoadError(problem)
            ensure_reader(cursor, reader, reader_password)
            cursor.execute(sql.SQL(
                "INSERT INTO {}.load_log (manifest_sha256, views_sha256, exported_at, window_start, window_end, "
                "tables, total_rows) VALUES (%s, %s, %s, %s, %s, %s, %s)").format(sql.Identifier(META_SCHEMA)),
                (manifest_sha, views_sha, manifest.get("exported_at"), manifest.get("window_start"),
                 manifest.get("window_end"), len(rows), sum(rows.values())))
    finally:
        connection.close()
    print(f"Loaded {len(rows)} tables ({sum(rows.values()):,} rows); views rebuilt in {BI_SCHEMA}.")
    return {"status": "loaded", "rows": rows}


def main() -> int:
    dsn = os.environ.get("PSI_DB_DSN")
    if not dsn:
        print("PSI_DB_DSN is not set", file=sys.stderr)
        return 2
    try:
        load(dsn,
             data_dir=Path(os.environ.get("PSI_DATA_DIR", DEFAULT_DATA_DIR)),
             views_sql=Path(os.environ.get("PSI_VIEWS_SQL", DEFAULT_VIEWS_SQL)),
             reader=os.environ.get("PSI_READER_USER", "psi_reader"),
             reader_password=os.environ.get("PSI_READER_PASSWORD") or None,
             force=os.environ.get("PSI_FORCE_RELOAD") == "1")
    except LoadError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
