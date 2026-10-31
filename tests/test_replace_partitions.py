"""replace_partitions (00_config_utils): the write primitive of the incremental notebooks."""

import pytest


@pytest.fixture
def write(config):
    spark = config["spark"]

    def _write(name, rows, schema, values, column="year"):
        df = spark.createDataFrame(rows, schema)
        config["replace_partitions"](df, "silver", name, column, values)
        return sorted(tuple(r) for r in spark.table(config["tbl"]("silver", name)).collect())

    return _write


def test_creates_then_replaces_only_the_given_partitions(write):
    schema = "id string, value int, year int"
    assert write("zz_parts", [("a", 1, 2025), ("b", 2, 2026)], schema, [2025, 2026]) == [("a", 1, 2025), ("b", 2, 2026)]
    # 2026 republished with different rows; 2025 must stay as it is
    result = write("zz_parts", [("b", 20, 2026), ("c", 3, 2026)], schema, [2026])
    assert result == [("a", 1, 2025), ("b", 20, 2026), ("c", 3, 2026)]


def test_schema_change_keeps_the_other_partitions(write):
    write("zz_schema", [("a", 1, 2025), ("b", 2, 2026)], "id string, value int, year int", [2025, 2026])
    result = write("zz_schema", [("b", 2, "new", 2026)], "id string, value int, note string, year int", [2026])
    assert result == [("a", 1, None, 2025), ("b", 2, "new", 2026)] or result == [("a", 1, 2025, None), ("b", 2, 2026, "new")]


def test_legacy_table_without_partition_column_is_rewritten(config, write):
    spark = config["spark"]
    config["write_table"](spark.createDataFrame([("old", 9)], "id string, value int"), "silver", "zz_legacy")
    result = write("zz_legacy", [("a", 1, 2025)], "id string, value int, year int", [2025])
    assert result == [("a", 1, 2025)]


def test_full_refresh_rewrites_everything(config, write):
    schema = "id string, value int, year int"
    write("zz_full", [("a", 1, 2025), ("b", 2, 2026)], schema, [2025, 2026])
    config["FULL_REFRESH"] = True
    try:
        assert write("zz_full", [("c", 3, 2026)], schema, [2026]) == [("c", 3, 2026)]
    finally:
        config["FULL_REFRESH"] = False
