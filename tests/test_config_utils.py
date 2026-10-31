"""Shared helpers of 00_config_utils: text normalization, matching keys and safe parsing."""

import pytest


def evaluate(config, expression, value):
    spark, F = config["spark"], config["F"]
    df = spark.createDataFrame([(value,)], "v string")
    return df.select(expression(F.col("v")).alias("r") if callable(expression) else expression.alias("r")).first().r


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Santa Bárbara d'Oeste", "SANTABARBARADOESTE"),
        ("SANTA BARBARA D OESTE", "SANTABARBARADOESTE"),
        ("  São João del-Rei ", "SAOJOAODELREI"),
        ("Assú", "ASSU"),
    ],
)
def test_name_key_collides_spelling_variants(config, raw, expected):
    assert evaluate(config, config["name_key"], raw) == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Homicídio  doloso", "HOMICIDIO DOLOSO"),
        ("Roubo seguido de morte (latrocínio)", "ROUBO SEGUIDO DE MORTE LATROCINIO"),
        ("Lesão corporal seguida de morte", "LESAO CORPORAL SEGUIDA DE MORTE"),
    ],
)
def test_normalize_text_matches_event_constants(config, raw, expected):
    assert evaluate(config, config["normalize_text"], raw) == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("1234", 1234),
        ("1234.0", 1234),
        ("1.234", 1234),  # thousands separator, as published by SENATRAN
        ("1.234.567", 1234567),
        (" 60000.0 ", 60000),
        ("abc", None),  # ANSI mode: invalid text becomes null instead of failing
        ("...", None),  # IBGE placeholder for a missing value
    ],
)
def test_parse_count(config, raw, expected):
    assert evaluate(config, config["parse_count"]("v"), raw) == expected


def test_try_cast_returns_null_instead_of_failing(config):
    assert evaluate(config, config["try_cast"]("v", "INT"), "not a number") is None


def test_cvli_events_are_normalized_names(config):
    normalize = config["normalize_text"]
    for event in config["CVLI_EVENTS"]:
        assert evaluate(config, normalize, event) == event


def test_tbl_uses_project_schemas(config):
    assert config["tbl"]("gold", "x").endswith("psi_gold.x")
    assert config["tbl"]("bronze", "x").endswith("psi_bronze.x")
