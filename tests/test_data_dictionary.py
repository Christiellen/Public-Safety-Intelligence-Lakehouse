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


def test_published_dictionary_is_up_to_date(agents):
    """docs/data_dictionary (CSV, Excel, Markdown) must match psi_gold.data_dictionary.
    Regenerate with: UPDATE_DOCS=1 pytest tests/test_data_dictionary.py -k published"""
    import os

    import dictionary_docs as docs

    table = agents["spark"].table(agents["tbl"]("gold", "data_dictionary")).toPandas()
    rows = docs.rows_of(table, agents["DICTIONARY"])
    if os.environ.get("UPDATE_DOCS") == "1":
        docs.write_all(rows)
    hint = "run UPDATE_DOCS=1 pytest tests/test_data_dictionary.py -k published"
    assert docs.CSV_FILE.read_text(encoding="utf-8") == docs.render_csv(rows), f"CSV out of date; {hint}"
    assert docs.MD_FILE.read_text(encoding="utf-8") == docs.render_markdown(rows), f"Markdown out of date; {hint}"
    assert docs.read_xlsx_columns(docs.XLSX_FILE) == [r for r in rows if r[1]], f"Excel out of date; {hint}"
