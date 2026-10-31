"""Shared fixtures: one local Spark session and one notebook namespace for the whole test run.

The pipeline is built once, in layers, and each test module asks for the layer it needs:
`config` (00) -> `lakehouse` (Bronze 02-03 from fake files + 05-10) -> `spatial` (12-13)
-> `agents` (16-17). The Bronze notebooks 02 and 03 run for real against `FakeWeb`.
"""

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from fake_sources import FakeWeb, write_mesh, write_reference_bronze  # noqa: E402
from notebook_loader import fake_dbutils, run_config, run_notebook  # noqa: E402

PIPELINE = [
    "05_silver_dim_municipality.py",
    "06_silver_municipality_indicators.py",
    "07_silver_sinesp_occurrences.py",
    "08_gold_star_schema.py",
    "09_gold_municipality_analytics.py",
    "10_gold_state_analytics.py",
]
SPATIAL = ["12_silver_municipality_adjacency.py", "13_gold_spatial_analytics.py"]


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    warehouse = tmp_path_factory.mktemp("warehouse")
    session = (SparkSession.builder.master("local[2]").appName("psi-tests")
               .config("spark.sql.warehouse.dir", str(warehouse))
               .config("spark.sql.ansi.enabled", "true")  # same as Databricks serverless
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.session.timeZone", "UTC")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def config(spark):
    namespace = {"spark": spark, "display": lambda df: None, "dbutils": fake_dbutils}
    run_config(namespace)
    namespace["ensure_schemas"]()
    return namespace


@pytest.fixture(scope="session")
def web(config):
    fake = FakeWeb(config)
    config["__overrides__"] = {"http_get": fake.get}
    return fake


@pytest.fixture(scope="session")
def lakehouse(config, web):
    write_reference_bronze(config)
    for name in ["02_bronze_sinesp.py", "03_bronze_senatran.py", *PIPELINE]:
        assert run_notebook(name, config) is None, f"{name} stopped early on the first run"
    return config


@pytest.fixture(scope="session")
def spatial(lakehouse, tmp_path_factory):
    write_mesh(lakehouse, tmp_path_factory.mktemp("mesh"))
    for name in SPATIAL:
        assert run_notebook(name, lakehouse) is None, f"{name} stopped early on the first run"
    return lakehouse


@pytest.fixture(scope="session")
def agents(spatial):
    run_notebook("16_gold_data_dictionary.py", spatial)
    run_notebook("17_agents_multiagent.py", spatial)
    return spatial


@pytest.fixture
def table(config):
    """Reads a table as pandas: table('gold', 'state_analytics')."""
    return lambda layer, name: config["spark"].table(config["tbl"](layer, name)).toPandas()
