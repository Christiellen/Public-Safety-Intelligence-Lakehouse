"""Data dictionary (notebook 16): documentation in sync with the real Gold schema."""

import pytest
from notebook_loader import run_cell


def test_dictionary_table(agents):
    spark, tbl = agents["spark"], agents["tbl"]
    dictionary = spark.table(tbl("gold", "data_dictionary")).toPandas()
    documented = agents["DICTIONARY"]
    columns = sum(len(c) - 1 for c in documented.values())  # minus the "_table" entry
    assert len(dictionary) == len(documented) + columns
    assert dictionary.description.str.strip().str.len().gt(0).all()
    assert dictionary[dictionary.column_name.notna()].data_type.notna().all()


def test_column_comments_are_applied(agents):
    spark, tbl = agents["spark"], agents["tbl"]
    described = spark.sql(f"DESCRIBE TABLE {tbl('gold', 'state_analytics')}").toPandas().set_index("col_name")
    assert "per 1,000 vehicles" in described.loc["vehicle_crime_rate_1k_fleet", "comment"]


def test_quality_gate_fails_on_undocumented_column(agents):
    original = agents["DICTIONARY"]
    agents["DICTIONARY"] = {t: dict(c) for t, c in original.items()}
    del agents["DICTIONARY"]["state_analytics"]["fleet_total"]
    try:
        with pytest.raises(AssertionError, match="undocumented columns"):
            run_cell("16_gold_data_dictionary.py", agents, "Quality gate")
    finally:
        agents["DICTIONARY"] = original


def test_quality_gate_fails_on_removed_column(agents):
    original = agents["DICTIONARY"]
    agents["DICTIONARY"] = {t: dict(c) for t, c in original.items()}
    agents["DICTIONARY"]["state_analytics"]["column_that_does_not_exist"] = "x"
    try:
        with pytest.raises(AssertionError, match="do not exist"):
            run_cell("16_gold_data_dictionary.py", agents, "Quality gate")
    finally:
        agents["DICTIONARY"] = original
