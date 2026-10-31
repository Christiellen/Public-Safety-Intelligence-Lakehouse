"""Answer checks used by the evaluation (notebook 19): numbers in Brazilian or English format and
names with or without accents."""

import pytest
from notebook_loader import run_cell


@pytest.fixture(scope="module")
def helpers(config):
    namespace = dict(config)
    run_cell("19_agents_evaluation.py", namespace, "def numbers_in")
    return namespace


@pytest.mark.parametrize(
    "text, expected",
    [
        ("1.234,5 victims", 1234.5),  # Brazilian
        ("1,234.5 victims", 1234.5),  # English
        ("rate of 52,2 per 100k", 52.2),
        ("rate of 52.2 per 100k", 52.2),
        ("496 municipalities", 496),
    ],
)
def test_has_number(helpers, text, expected):
    assert helpers["has_number"](text, expected)


def test_has_number_tolerance_and_missing(helpers):
    assert helpers["has_number"]("about 138 victims", 138.4)  # within 1% / rounding
    assert not helpers["has_number"]("about 150 victims", 138)
    assert not helpers["has_number"]("anything", None)


def test_has_names_ignores_accents_and_case(helpers):
    answer = "Neighbors: Sao Lourenco da Mata and JABOATÃO DOS GUARARAPES"
    assert helpers["has_names"](answer, ["São Lourenço da Mata", "Jaboatão dos Guararapes"])
    assert helpers["has_names"](answer, ["São Lourenço da Mata", "Olinda"], min_share=0.5)
    assert not helpers["has_names"](answer, ["São Lourenço da Mata", "Olinda"])
