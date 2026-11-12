"""Superset stack: export for BI (notebook 21), load into PostgreSQL (superset/loader) and the
Superset datasets built from the views (superset/assets).

The export runs for real on the local Gold tables. The load tests need a PostgreSQL server: they
run when PSI_TEST_POSTGRES_DSN points to an empty database the tests may write to (in CI, a
postgres service container) and are skipped otherwise.
"""

import csv
import gzip
import hashlib
import json
import os
import shutil
import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pytest
from notebook_loader import run_notebook

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "superset" / "loader"))
sys.path.insert(0, str(ROOT / "superset" / "assets"))

EXPORT_DIR_SOURCE = "f\"/Volumes/{CATALOG}/{SCHEMAS['gold']}/exports/superset\""
POSTGRES_DSN = os.environ.get("PSI_TEST_POSTGRES_DSN")
READER_USER, READER_PASSWORD = "psi_test_reader", "test-only-password"


def _reader_dsn(dsn):
    """The test DSN with the read-only role in place of the owner."""
    if not dsn:
        return None
    parts = urlsplit(dsn)
    host = parts.hostname + (f":{parts.port}" if parts.port else "")
    return urlunsplit((parts.scheme, f"{READER_USER}:{READER_PASSWORD}@{host}", parts.path, parts.query, ""))


READER_DSN = _reader_dsn(POSTGRES_DSN)
needs_postgres = pytest.mark.skipif(not POSTGRES_DSN, reason="PSI_TEST_POSTGRES_DSN not set")


def run_export(namespace, target: Path):
    return run_notebook("21_gold_export_bi.py", namespace, replace={EXPORT_DIR_SOURCE: repr(str(target))})


def read_csv(path: Path) -> list[dict]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


@pytest.fixture(scope="module")
def export_dir(spatial, tmp_path_factory):
    target = tmp_path_factory.mktemp("bi_export")
    assert run_export(spatial, target) is None, "21_gold_export_bi stopped early on the first run"
    return target


@pytest.fixture(scope="module")
def manifest(export_dir):
    return json.loads((export_dir / "manifest.json").read_text(encoding="utf-8"))


def test_manifest_covers_every_table(spatial, export_dir, manifest):
    spark, tbl = spatial["spark"], spatial["tbl"]
    assert [e["table"] for e in manifest["tables"]] == list(spatial["EXPORT_TABLES"])
    for entry in manifest["tables"]:
        df = spark.table(tbl("gold", entry["table"]))
        assert entry["rows"] == df.count() > 0
        assert [(c["name"], c["type"]) for c in entry["columns"]] == df.dtypes
        path = export_dir / entry["file"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == entry["sha256"]
        assert entry["size_bytes"] == path.stat().st_size
    assert manifest["window_start"] < manifest["window_end"]
    # Only the files of the manifest are left: no staging folders
    assert sorted(p.name for p in export_dir.iterdir()) == sorted(
        ["manifest.json", *(e["file"] for e in manifest["tables"])])


def test_files_are_sorted_by_primary_key(export_dir, manifest):
    for entry in manifest["tables"]:
        rows = read_csv(export_dir / entry["file"])
        assert len(rows) == entry["rows"]
        types = {c["name"]: c["type"] for c in entry["columns"]}

        def key(row, types=types, entry=entry):
            return tuple(int(row[k]) if types[k] in ("int", "bigint") else row[k] for k in entry["primary_key"])

        assert [key(r) for r in rows] == sorted(key(r) for r in rows), entry["table"]


def test_nulls_are_empty_fields(spatial, export_dir, manifest):
    """Nulls are written as empty, unquoted fields, which PostgreSQL COPY (CSV) loads as NULL; an
    empty string would be written as "" and loaded as text. Every null of the Gold tables must come
    out as an empty field, and nothing else may."""
    from pyspark.sql import functions as F

    spark, tbl = spatial["spark"], spatial["tbl"]
    checked = 0
    for entry in manifest["tables"]:
        df = spark.table(tbl("gold", entry["table"]))
        nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).first().asDict()
        rows = read_csv(export_dir / entry["file"])
        empty = {c: sum(1 for r in rows if r[c] == "") for c in df.columns}
        assert empty == nulls, entry["table"]
        checked += sum(nulls.values())
        with gzip.open(export_dir / entry["file"], "rt", encoding="utf-8", newline="") as handle:
            assert '""' not in handle.read(), f"{entry['file']}: quoted empty string"
    assert checked > 0, "the fake sources should produce some nulls"


def test_rerun_without_changes_does_nothing(spatial, export_dir):
    assert "nothing to do" in run_export(spatial, export_dir)


# -- Loader (PostgreSQL) -------------------------------------------------------------------------

def test_loader_rejects_a_changed_file(export_dir, tmp_path):
    import load_gold

    copy = tmp_path / "data"
    shutil.copytree(export_dir, copy)
    with gzip.open(copy / "dim_state.csv.gz", "at", encoding="utf-8") as handle:
        handle.write("XX,Extra,N,Norte\n")
    manifest, _ = load_gold.read_manifest(copy)
    with pytest.raises(load_gold.LoadError, match="dim_state.csv.gz: SHA-256 differs"):
        load_gold.verify_files(manifest, copy)


def test_loader_maps_every_exported_type(manifest):
    import load_gold

    for entry in manifest["tables"]:
        for column in entry["columns"]:
            assert load_gold.postgres_type(column["type"])
    with pytest.raises(load_gold.LoadError):
        load_gold.postgres_type("map<string,int>")


@pytest.fixture(scope="module")
def loaded(export_dir):
    import load_gold
    import psycopg2

    connection = psycopg2.connect(POSTGRES_DSN)
    connection.autocommit = True
    with connection.cursor() as cursor:
        for schema in ("psi_bi", "psi_gold", "psi_meta"):
            cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    result = load_gold.load(POSTGRES_DSN, data_dir=export_dir, reader=READER_USER,
                            reader_password=READER_PASSWORD)
    yield connection, result
    connection.close()


@needs_postgres
def test_load_creates_tables_and_views(loaded, manifest, spatial):
    connection, result = loaded
    assert result["status"] == "loaded"
    with connection.cursor() as cursor:
        for entry in manifest["tables"]:
            cursor.execute(f"SELECT COUNT(*) FROM psi_gold.{entry['table']}")
            assert cursor.fetchone()[0] == entry["rows"]
        cursor.execute("SELECT table_name FROM information_schema.views WHERE table_schema = 'psi_bi'")
        views = {r[0] for r in cursor.fetchall()}
        assert views == {"municipality_profile", "municipality_profile_items", "state_profile", "cvli_monthly_state",
                         "cvli_monthly_municipality", "cvli_kpis"}
        # Types survive the CSV: booleans, dates and nulls
        cursor.execute("SELECT COUNT(*) FROM psi_gold.municipality_analytics WHERE cvli_rate_100k IS NULL")
        spark_nulls = spatial["spark"].table(spatial["tbl"]("gold", "municipality_analytics")) \
            .filter("cvli_rate_100k IS NULL").count()
        assert cursor.fetchone()[0] == spark_nulls
        cursor.execute("SELECT pg_typeof(window_start)::text, pg_typeof(is_national_hotspot)::text "
                       "FROM psi_gold.municipality_analytics LIMIT 1")
        assert cursor.fetchone() == ("date", "boolean")


@needs_postgres
def test_views_match_the_gold_tables(loaded, spatial):
    """The views recompute nothing the Gold layer already defines: monthly CVLI summed over the
    window equals state_analytics, and the KPI rate equals the national rate."""
    connection, _ = loaded
    spark, tbl = spatial["spark"], spatial["tbl"]
    state = {r.state_code: r for r in spark.table(tbl("gold", "state_analytics")).collect()}
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT m.state_code, SUM(m.cvli_victims)
            FROM psi_bi.cvli_monthly_state m
            JOIN psi_bi.state_profile s USING (state_code)
            WHERE m.month BETWEEN s.window_start AND s.window_end
            GROUP BY m.state_code""")
        monthly = dict(cursor.fetchall())
        for code, victims in monthly.items():
            assert victims == pytest.approx(state[code].cvli_victims)
        cursor.execute("SELECT cvli_victims, population, cvli_rate_100k, months_covered FROM psi_bi.cvli_kpis")
        victims, population, rate, months = cursor.fetchone()
        total_victims = sum(r.cvli_victims or 0 for r in state.values())
        total_population = sum(r.population or 0 for r in state.values())
        assert victims == pytest.approx(total_victims)
        assert float(rate) == pytest.approx(total_victims * 12 / months * 100000 / total_population, abs=0.01)
        cursor.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE lisa_cluster IS NULL) FROM psi_bi.municipality_profile")
        profiles, without_cluster = cursor.fetchone()
        assert profiles == spark.table(tbl("gold", "municipality_analytics")).count()
        assert without_cluster == 0


@needs_postgres
def test_monthly_grid_adds_up_to_the_gold_facts(loaded, spatial):
    """cvli_monthly_municipality, the main dataset: summed by state it equals the state facts (victims
    without municipality included), summed by municipality it equals the municipal facts, every area
    has every month, and the state population is the population of all its municipalities."""
    connection, _ = loaded
    spark, tbl = spatial["spark"], spatial["tbl"]
    cvli = spark.table(tbl("gold", "dim_event")).filter("is_cvli").select("event_id")
    state_facts = (spark.table(tbl("gold", "fact_state_occurrences")).filter("measure_type = 'victims'")
                   .join(cvli, "event_id").groupBy("state_code").sum("value").collect())
    municipal_facts = (spark.table(tbl("gold", "fact_occurrences")).filter("measure_type = 'victims'")
                       .join(cvli, "event_id").groupBy("municipality_code").sum("count").collect())
    months = spark.table(tbl("gold", "dim_date")).count()
    with connection.cursor() as cursor:
        cursor.execute("SELECT state_code, SUM(cvli_victims) FROM psi_bi.cvli_monthly_municipality GROUP BY state_code")
        by_state = dict(cursor.fetchall())
        assert {r[0]: pytest.approx(r[1]) for r in state_facts if r[1]} == \
            {k: v for k, v in by_state.items() if v}
        cursor.execute("SELECT municipality_code, SUM(cvli_victims) FROM psi_bi.cvli_monthly_municipality "
                       "WHERE is_municipality GROUP BY municipality_code")
        assert dict(cursor.fetchall()) == {r[0]: pytest.approx(r[1]) for r in municipal_facts}
        cursor.execute("SELECT COUNT(*) FROM psi_bi.cvli_monthly_municipality GROUP BY municipality_label "
                       "HAVING COUNT(*) <> %s", (months,))
        assert cursor.fetchall() == []
        cursor.execute("SELECT state_code, SUM(population) / COUNT(DISTINCT month) FROM psi_bi.cvli_monthly_municipality "
                       "GROUP BY state_code")
        populations = dict(cursor.fetchall())
    state_population = {r.state_code: r.population for r in spark.table(tbl("gold", "state_analytics")).collect()}
    for code, population in populations.items():
        assert population == pytest.approx(state_population[code] or 0)


@needs_postgres
def test_year_over_year_metric(loaded):
    """The KPI of the prototype: Jan-Sep of the last year against Jan-Sep of the year before, through
    the metric of datasets.yml, equals the sums computed by hand; a year without the previous one
    gives null instead of a misleading change."""
    import build_assets

    metric = build_assets.load_spec()["datasets"]["cvli_monthly_municipality"]["metrics"]["cvli_yoy_change"]
    connection, _ = loaded
    with connection.cursor() as cursor:
        cursor.execute("SELECT MIN(year), MAX(year) FROM psi_bi.cvli_monthly_municipality")
        first, last = cursor.fetchone()
        cursor.execute(f"SELECT {metric['expression']} FROM psi_bi.cvli_monthly_municipality "
                       "WHERE year = %s AND month_of_year <= 9", (last,))
        change = cursor.fetchone()[0]
        cursor.execute("SELECT SUM(cvli_victims) FROM psi_bi.cvli_monthly_municipality "
                       "WHERE year = %s AND month_of_year <= 9 GROUP BY year ORDER BY year", (last,))
        current = cursor.fetchone()[0]
        cursor.execute("SELECT SUM(cvli_victims) FROM psi_bi.cvli_monthly_municipality "
                       "WHERE year = %s AND month_of_year <= 9", (last - 1,))
        previous = cursor.fetchone()[0]
        assert change == pytest.approx(current / previous - 1)
        cursor.execute(f"SELECT {metric['expression']} FROM psi_bi.cvli_monthly_municipality WHERE year = %s",
                       (first,))
        assert cursor.fetchone()[0] is None


@needs_postgres
def test_profile_shows_only_one_municipality(loaded):
    """The profile table groups the items with HAVING COUNT(DISTINCT municipality_code) = 1: six rows
    when one municipality is selected, none when the selection has several (a state, Brazil)."""
    query = ("SELECT item_order, item, MAX(value) FROM psi_bi.municipality_profile_items {where} "
             "GROUP BY item_order, item HAVING COUNT(DISTINCT municipality_code) = 1 ORDER BY item_order")
    connection, _ = loaded
    with connection.cursor() as cursor:
        cursor.execute("SELECT municipality_label FROM psi_bi.municipality_profile LIMIT 1")
        label = cursor.fetchone()[0]
        cursor.execute(query.format(where="WHERE municipality_label = %s"), (label,))
        rows = cursor.fetchall()
        assert [r[0] for r in rows] == [1, 2, 3, 4, 5, 6] and all(r[2] for r in rows)
        cursor.execute(query.format(where=""))
        assert cursor.fetchall() == []


@needs_postgres
def test_reader_sees_only_the_bi_views(loaded):
    import psycopg2

    connection, _ = loaded
    params = connection.get_dsn_parameters()
    reader = psycopg2.connect(host=params.get("host"), port=params.get("port"), dbname=params["dbname"],
                              user=READER_USER, password=READER_PASSWORD)
    try:
        with reader.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM psi_bi.municipality_profile")
            assert cursor.fetchone()[0] > 0
        with reader.cursor() as cursor, pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cursor.execute("SELECT COUNT(*) FROM psi_gold.municipality_analytics")
        reader.rollback()
        with reader.cursor() as cursor, pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cursor.execute("CREATE TABLE psi_bi.not_allowed (id int)")
    finally:
        reader.close()


@needs_postgres
def test_second_load_is_skipped(loaded, export_dir):
    import load_gold

    assert load_gold.load(POSTGRES_DSN, data_dir=export_dir, reader=READER_USER)["status"] == "skipped"


# -- Superset assets (superset/assets/build_assets.py) ---------------------------------------------

def test_every_view_is_a_dataset():
    import re

    import build_assets

    views = re.findall(r"CREATE VIEW psi_bi\.(\w+)", (ROOT / "superset" / "sql" / "views.sql").read_text())
    assert sorted(build_assets.load_spec()["datasets"]) == sorted(views)


def test_uuids_are_stable():
    """Dashboards exported to superset/dashboards refer to the database and datasets by UUID: a new
    UUID would leave every chart without its dataset."""
    import build_assets

    spec = build_assets.load_spec()
    assert build_assets.database_uuid(spec) == "bf1307a0-48be-56be-878d-345a950db592"
    assert build_assets.dataset_uuid("municipality_profile") == "4459254e-b8ec-572c-b8ae-18445a512226"


@pytest.fixture(scope="module")
def bundle(loaded):
    import build_assets

    spec = build_assets.load_spec()
    dictionary = build_assets.load_dictionary(build_assets.DEFAULT_DICTIONARY)
    columns = build_assets.read_view_columns(READER_DSN, list(spec["datasets"]))
    return spec, dictionary, columns, build_assets.build_bundle(spec, columns, dictionary, READER_DSN)


@needs_postgres
def test_bundle_documents_every_column(bundle):
    spec, _, columns, files = bundle
    datasets = {path.rsplit("/", 1)[1][:-5]: config for path, config in files.items() if "/datasets/" in path}
    assert sorted(datasets) == sorted(spec["datasets"])
    for name, config in datasets.items():
        assert [c["column_name"] for c in config["columns"]] == [c for c, _ in columns[name]]
        assert all(c["description"] for c in config["columns"]), name
        assert config["database_uuid"] == files["psi_datasets/databases/PSI_Lakehouse.yaml"]["uuid"]
    database = files["psi_datasets/databases/PSI_Lakehouse.yaml"]
    assert database["sqlalchemy_uri"].startswith("postgresql+psycopg2://psi_test_reader:")
    assert database["allow_dml"] is False


@needs_postgres
def test_bundle_fails_on_undocumented_column(bundle):
    import build_assets

    spec, dictionary, columns, _ = bundle
    view_columns = {**columns, "cvli_kpis": [*columns["cvli_kpis"], ("new_column", "integer")]}
    with pytest.raises(build_assets.AssetError, match="without description.*new_column"):
        build_assets.build_bundle(spec, view_columns, dictionary, READER_DSN)


@needs_postgres
def test_metrics_run_as_the_reader(bundle):
    """Every metric expression of datasets.yml is valid SQL on its view, run with the read-only role
    (Superset wraps the same expression in its own SELECT)."""
    import psycopg2

    spec, *_ = bundle
    reader = psycopg2.connect(READER_DSN)
    try:
        with reader.cursor() as cursor:
            for name, config in spec["datasets"].items():
                expressions = ", ".join(f"{m['expression']} AS {metric}"
                                        for metric, m in (config.get("metrics") or {}).items())
                cursor.execute(f"SELECT {expressions} FROM psi_bi.{name}")
                assert len(cursor.fetchone()) == len(config["metrics"])
                if config.get("main_dttm_col"):   # time series: metrics grouped by month, as in a line chart
                    cursor.execute(f"SELECT {config['main_dttm_col']}, {expressions} FROM psi_bi.{name} GROUP BY 1")
                    assert cursor.fetchall()
    finally:
        reader.close()


def test_dashboard_exports_match_the_datasets():
    """Every dashboard in superset/dashboards refers to the database and datasets by the stable UUIDs
    of build_assets.py, points to the Docker host and carries no password."""
    import zipfile

    import build_assets
    import yaml

    spec = build_assets.load_spec()
    exports = sorted((ROOT / "superset" / "dashboards").glob("*.zip"))
    assert exports, "no dashboard export in superset/dashboards"
    for export in exports:
        with zipfile.ZipFile(export) as bundle:
            files = {n: yaml.safe_load(bundle.read(n)) for n in bundle.namelist() if n.endswith(".yaml")}
        datasets = [c for n, c in files.items() if "/datasets/" in n]
        databases = [c for n, c in files.items() if "/databases/" in n]
        assert datasets and len(databases) == 1, export.name
        assert databases[0]["uuid"] == build_assets.database_uuid(spec)
        assert databases[0]["sqlalchemy_uri"] == "postgresql+psycopg2://psi_reader:XXXXXXXXXX@db:5432/psi"
        for dataset in datasets:
            assert dataset["table_name"] in spec["datasets"], dataset["table_name"]
            assert dataset["uuid"] == build_assets.dataset_uuid(dataset["table_name"]), dataset["table_name"]
        charts = [c for n, c in files.items() if "/charts/" in n]
        assert {c["dataset_uuid"] for c in charts} <= {d["uuid"] for d in datasets}


def test_dashboard_import_replaces_existing_charts():
    """superset/docker/import_dashboards.py builds a chart bundle from each dashboard export, so
    charts that already exist are replaced on every superset-init run."""
    import importlib.util
    import zipfile

    import yaml

    path = ROOT / "superset" / "docker" / "import_dashboards.py"
    spec = importlib.util.spec_from_file_location("import_dashboards", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    for export in sorted((ROOT / "superset" / "dashboards").glob("*.zip")):
        with zipfile.ZipFile(export) as bundle:
            contents = {n.split("/", 1)[1]: bundle.read(n).decode() for n in bundle.namelist()}
        charts = module.chart_bundle(contents)
        assert yaml.safe_load(charts["metadata.yaml"])["type"] == "Slice"
        assert yaml.safe_load(contents["metadata.yaml"])["type"] == "Dashboard"   # input untouched
        assert not any(p.startswith("dashboards/") for p in charts)
        assert {p for p in contents if p.startswith("charts/")} == {p for p in charts if p.startswith("charts/")}
    assert "import_dashboards.py" in (ROOT / "superset" / "docker" / "init.sh").read_text()


@needs_postgres
def test_published_bi_dictionary_is_up_to_date(loaded):
    """docs/data_dictionary/bi (Markdown, Excel, CSV) must match the datasets Superset receives.
    Regenerate with: UPDATE_DOCS=1 pytest tests/test_superset_stack.py -k bi_dictionary"""
    import bi_dictionary as docs

    doc = docs.build(READER_DSN)
    if os.environ.get("UPDATE_DOCS") == "1":
        docs.write_all(doc)
    hint = "run UPDATE_DOCS=1 pytest tests/test_superset_stack.py -k bi_dictionary"
    assert docs.COLUMNS_CSV.read_text(encoding="utf-8") == docs.render_columns_csv(doc), f"columns CSV out of date; {hint}"
    assert docs.METRICS_CSV.read_text(encoding="utf-8") == docs.render_metrics_csv(doc), f"metrics CSV out of date; {hint}"
    assert docs.MD_FILE.read_text(encoding="utf-8") == docs.render_markdown(doc), f"Markdown out of date; {hint}"
    assert docs.read_xlsx(docs.XLSX_FILE) == doc, f"Excel out of date; {hint}"
