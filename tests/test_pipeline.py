"""Bronze -> Silver -> Gold (notebooks 05-10) on the simulated sources of fake_sources.py."""

import pytest
from fake_sources import (
    COCAINE_KG_PER_MONTH,
    CVLI_PER_MONTH,
    MUNICIPALITIES,
    NOT_INFORMED_CVLI_PER_MONTH,
    VEHICLE_CRIMES_PER_MONTH,
)

WINDOW_MONTHS = 12  # analysis window: the last 12 months available (2025-10 to 2026-09)


@pytest.fixture(scope="module")
def gold(lakehouse):
    spark, tbl = lakehouse["spark"], lakehouse["tbl"]
    return lambda name, layer="gold": spark.table(tbl(layer, name)).toPandas()


def by_code(frame):
    return frame.set_index("municipality_code")


# ----------------------------------------------------------------------------- conformed dimension
def test_dimension_has_one_row_per_ibge_code(gold):
    dim = gold("dim_municipality", "silver")
    assert sorted(dim.municipality_code) == sorted(code for code, *_ in MUNICIPALITIES)
    assert dim.municipality_code.is_unique


def test_homonyms_get_their_own_state_data(gold):
    """'Bom Jesus' exists in PI and RS: matching by name alone would mix their data."""
    analytics = by_code(gold("municipality_analytics"))
    assert analytics.loc["2201903", "population"] == 25_000
    assert analytics.loc["4302303", "population"] == 11_800
    assert analytics.loc["2201903", "fleet_total"] == 9_000
    assert analytics.loc["4302303", "fleet_total"] == 8_000


# ----------------------------------------------------------------------------- name matching
def test_alias_fuzzy_and_number_formats(gold):
    indicators = by_code(gold("municipality_indicators", "silver"))
    assert indicators.loc["3530805", "fleet_total"] == 60_000  # MOJI MIRIM -> alias, "60000.0"
    assert indicators.loc["3545803", "fleet_total"] == 1_234  # "1.234" thousands separator
    assert indicators.loc["3552205", "fleet_total"] == 500_000  # SOROCAVA -> fuzzy match


def test_fuzzy_matches_are_audited(gold):
    fuzzy = gold("qa_fuzzy_matches", "silver")
    row = fuzzy[fuzzy.source_name == "SOROCAVA"].iloc[0]
    assert row.municipality_code == "3552205" and row.distance == 1
    # MOJI MIRIM is resolved by MUNICIPALITY_ALIASES before fuzzy matching is tried
    assert "MOJI MIRIM" not in set(fuzzy.source_name)


def test_unmatched_names_are_reported_not_dropped(gold):
    unmatched = gold("qa_unmatched_municipalities", "silver")
    names = set(unmatched.source_name)
    assert {"SAO PAULO DO NORTE DISTANTE", "CIDADE INEXISTENTE", "Cidade Errada"} <= names
    # "not informed" rows are expected in the sources and are not data quality problems
    assert not names & {"MUNICIPIO NAO INFORMADO", "NÃO INFORMADO"}


def test_idhm_six_digit_code_and_missing_municipality(gold):
    indicators = by_code(gold("municipality_indicators", "silver"))
    assert indicators.loc["3509502", "idhm"] == pytest.approx(0.75)  # published with a 6-digit code
    assert indicators.loc["2201903", "idhm"] == pytest.approx(0.60)
    assert indicators["idhm"].isna().loc["5101837"]  # created after the 2010 census


def test_population_ignores_placeholders_and_uses_latest_year(gold):
    indicators = by_code(gold("municipality_indicators", "silver"))
    assert (indicators.population_year == 2025).all()
    assert indicators.loc["5101837", "population"] == 7_000


# ----------------------------------------------------------------------------- events and measures
def test_event_levels_and_groups(gold):
    events = gold("dim_event").set_index("event_name")
    assert events.loc["HOMICIDIO DOLOSO", "event_level"] == "municipal"
    assert events.loc["ROUBO DE VEICULO", "event_level"] == "state"
    assert events.loc["FEMINICIDIO", "is_cvli"] and events.loc["HOMICIDIO DOLOSO", "is_cvli"]
    assert events.loc["FURTO DE VEICULO", "is_vehicle_crime"]
    assert events.loc["APREENSAO DE COCAINA", "is_drug_seizure"]
    assert not events.loc["MANDADO DE PRISAO CUMPRIDO", "is_cvli"]


def test_measures_are_never_mixed(gold):
    fact = gold("fact_state_occurrences")
    assert set(fact.measure_type) <= {"occurrences", "victims", "weight_kg"}
    keys = ["state_code", "event_id", "agency", "date_id", "measure_type"]
    assert not fact.duplicated(keys).any()


# ----------------------------------------------------------------------------- analytics
def test_municipal_cvli_victims_and_rate(gold):
    analytics = by_code(gold("municipality_analytics"))
    expected_victims = CVLI_PER_MONTH["São Paulo"] * WINDOW_MONTHS
    assert analytics.loc["3550308", "cvli_victims"] == expected_victims
    assert analytics.loc["3550308", "months_covered"] == WINDOW_MONTHS
    rate = expected_victims / 11_900_000 * 100_000
    assert analytics.loc["3550308", "cvli_rate_100k"] == pytest.approx(rate, abs=0.01)


def test_state_totals_include_victims_without_municipality(gold):
    state = gold("state_analytics").set_index("state_code")
    sp_municipal = sum(v for name, v in CVLI_PER_MONTH.items() if name != "Bom Jesus")
    unmatched_city = 1  # "Cidade Errada", SP
    expected = (sp_municipal + NOT_INFORMED_CVLI_PER_MONTH + unmatched_city) * WINDOW_MONTHS
    assert state.loc["SP", "cvli_victims"] == expected


def test_state_vehicle_crimes_and_seizures(gold):
    state = gold("state_analytics").set_index("state_code")
    assert state.loc["SP", "vehicle_crimes"] == VEHICLE_CRIMES_PER_MONTH * WINDOW_MONTHS  # null PF row ignored
    assert state.loc["SP", "drug_seizure_kg"] == pytest.approx(COCAINE_KG_PER_MONTH * WINDOW_MONTHS)
    assert (
        state.loc["MT", "vehicle_crime_rate_1k_fleet"] != state.loc["MT", "vehicle_crime_rate_1k_fleet"]
    )  # NaN: no fleet


def test_scores_are_bounded(gold):
    analytics = gold("municipality_analytics")
    for column in ["ivsc_score", "crime_component", "social_component"]:
        values = analytics[column].dropna()
        assert ((values >= 0) & (values <= 1)).all(), column
    assert set(analytics.economic_tier.dropna()) <= {"Low", "Medium", "High"}


def test_z_scores_only_for_municipalities_above_threshold(gold):
    analytics = gold("municipality_analytics")
    small = analytics[analytics.population < 20_000]
    assert small.cvli_z_score_state.isna().all() and small.cvli_z_score_national.isna().all()
