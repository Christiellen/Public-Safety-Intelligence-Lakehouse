"""Incremental processing: change log, skipped runs, replaced partitions and history.

The tests run the real notebooks again on top of the pipeline built by the fixtures, publish revised
"files" through FakeWeb, and check that only what changed is reprocessed. At the end, the original
files are served again and everything is rebuilt with FULL_REFRESH, so the other test modules see
the original data.
"""

import pytest
from conftest import PIPELINE, SPATIAL
from fake_sources import CVLI_PER_MONTH, FLEET
from notebook_loader import run_notebook

EXTRA_VICTIMS = 100
NEW_MONTH, NEW_MONTH_URL = "2026-10", "https://example.test/senatran/frota-2026-10.xlsx"


@pytest.fixture(scope="module")
def ns(spatial, web):
    yield spatial
    web.reset()
    overrides = spatial["__overrides__"]
    for key in ["SENATRAN_REFERENCE", "SENATRAN_FLEET_URL"]:
        overrides.pop(key, None)
    overrides["FULL_REFRESH"] = True
    try:
        for name in ["02_bronze_sinesp.py", "03_bronze_senatran.py", *PIPELINE, *SPATIAL]:
            run_notebook(name, spatial)
    finally:
        overrides.pop("FULL_REFRESH")
        spatial["FULL_REFRESH"] = False


def read(ns, layer, name):
    return ns["spark"].table(ns["tbl"](layer, name)).toPandas()


def changes(ns):
    return read(ns, "ops", "change_log")


def test_every_dataset_is_logged(ns):
    log = changes(ns)
    keys = log.groupby("dataset")["key"].apply(set).to_dict()
    assert keys["bronze_sinesp_occurrences"] == {"2025", "2026"}
    assert keys["bronze_senatran_fleet"] == {"2026-09"}
    assert keys["silver_sinesp"] == {"2025", "2026"}
    assert keys["gold_facts"] == {"2025", "2026"}
    assert {"silver_dim_municipality", "gold_dimensions", "gold_municipality_analytics", "gold_spatial"} <= set(keys)
    bronze = log[log.dataset == "bronze_sinesp_occurrences"]
    assert bronze.content_sha256.str.len().eq(64).all()  # SHA-256 of each yearly file


def test_rerun_without_new_data_does_nothing(ns, web):
    before = len(changes(ns))
    assert run_notebook("02_bronze_sinesp.py", ns) is None  # downloads, compares hashes, writes nothing
    assert "unchanged" in run_notebook("03_bronze_senatran.py", ns)
    for name in [*PIPELINE, *SPATIAL]:
        message = run_notebook(name, ns)
        assert message and "nothing to do" in message, f"{name} should have been skipped"
    assert len(changes(ns)) == before


def test_revised_year_reprocesses_only_that_year(ns, web):
    keys = ["municipality_code", "event_id", "agency", "date_id", "measure_type"]
    gold_2025 = read(ns, "gold", "fact_occurrences").query("year == 2025").sort_values(keys).reset_index(drop=True)
    bronze = read(ns, "bronze", "bronze_sinesp_occurrences")
    loaded_2025 = bronze.loc[bronze._source_year == "2025", "_ingested_at"].max()
    victims_before = read(ns, "gold", "municipality_analytics").set_index("municipality_code").loc["3550308", "cvli_victims"]
    state_before = read(ns, "gold", "state_analytics").set_index("state_code").loc["SP", "cvli_victims"]
    last_change = changes(ns).changed_at.max()

    web.publish_sinesp("2026", extra_victims=EXTRA_VICTIMS)  # the 2026 file is republished with a revision
    assert run_notebook("02_bronze_sinesp.py", ns) is None
    log = changes(ns)
    new = log[log.changed_at > last_change]
    assert list(zip(new.dataset, new.key, strict=True)) == [("bronze_sinesp_occurrences", "2026")]
    bronze = read(ns, "bronze", "bronze_sinesp_occurrences")
    assert bronze.loc[bronze._source_year == "2025", "_ingested_at"].max() == loaded_2025  # 2025 not rewritten

    for name in ["05_silver_dim_municipality.py", "06_silver_municipality_indicators.py"]:
        assert "nothing to do" in run_notebook(name, ns)
    assert run_notebook("07_silver_sinesp_occurrences.py", ns) is None
    assert ns["years"] == [2026]
    assert run_notebook("08_gold_star_schema.py", ns) is None
    assert ns["fact_years"] == [2026]

    after_2025 = read(ns, "gold", "fact_occurrences").query("year == 2025").sort_values(keys).reset_index(drop=True)
    assert after_2025.equals(gold_2025)  # the 2025 partition is untouched

    assert run_notebook("09_gold_municipality_analytics.py", ns) is None
    assert run_notebook("10_gold_state_analytics.py", ns) is None
    victims = read(ns, "gold", "municipality_analytics").set_index("municipality_code").loc["3550308", "cvli_victims"]
    assert victims == victims_before + EXTRA_VICTIMS
    assert read(ns, "gold", "state_analytics").set_index("state_code").loc["SP", "cvli_victims"] == state_before + EXTRA_VICTIMS

    assert "nothing to do" in run_notebook("12_silver_municipality_adjacency.py", ns)  # borders did not change
    assert run_notebook("13_gold_spatial_analytics.py", ns) is None  # rates changed


def test_name_matching_qa_is_kept_per_year(ns):
    sources = set(read(ns, "silver", "qa_unmatched_municipalities").source)
    assert {"SINESP 2025", "SINESP 2026", "SENATRAN"} <= sources
    assert "SINESP" not in sources


def test_new_fleet_month_is_added_and_used(ns, web):
    fleet = [(uf, name, "9100000" if name == "SAO PAULO" else total) for uf, name, total in FLEET]
    web.publish_senatran(NEW_MONTH_URL, fleet)
    ns["__overrides__"].update({"SENATRAN_REFERENCE": NEW_MONTH, "SENATRAN_FLEET_URL": NEW_MONTH_URL})
    assert run_notebook("03_bronze_senatran.py", ns) is None
    months = set(read(ns, "bronze", "bronze_senatran_fleet")._reference_month)
    assert months == {"2026-09", NEW_MONTH}  # history kept

    assert run_notebook("06_silver_municipality_indicators.py", ns) is None
    indicators = read(ns, "silver", "municipality_indicators").set_index("municipality_code")
    assert indicators.loc["3550308", "fleet_total"] == 9_100_000
    assert indicators.loc["3550308", "fleet_reference"] == NEW_MONTH


def test_full_refresh_reprocesses_every_year(ns):
    ns["__overrides__"]["FULL_REFRESH"] = True
    try:
        assert run_notebook("07_silver_sinesp_occurrences.py", ns) is None
        assert ns["years"] == [2025, 2026]
    finally:
        ns["__overrides__"].pop("FULL_REFRESH")
        ns["FULL_REFRESH"] = False
    rows = read(ns, "silver", "sinesp_occurrences")
    sao_paulo = rows[(rows.municipality_code == "3550308") & (rows.event_name == "HOMICIDIO DOLOSO")]
    assert sao_paulo["count"].sum() == CVLI_PER_MONTH["São Paulo"] * 21 + EXTRA_VICTIMS  # 21 months published
