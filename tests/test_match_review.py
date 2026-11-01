"""GenAI review of fuzzy municipality matches (notebook 20) with a fake LLM: answers checked in code,
actions, incremental runs, the labeled evaluation and malformed answers. No network access."""

import json

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from notebook_loader import run_notebook

USAGE = {"input_tokens": 50, "output_tokens": 10, "total_tokens": 60}
FAKE = {"llms, chain = build_llms()": "llms, chain = [FakeReviewer()], ['fake-reviewer']"}


class FakeReviewer:
    """Item 0: same (high). Item 1: another candidate (high). Item 2: an invented code. Others: uncertain."""

    calls = 0

    def invoke(self, messages):
        FakeReviewer.calls += 1
        items = json.loads(next(m.content for m in messages if isinstance(m, HumanMessage)))
        reviews = []
        for item in items:
            others = [c["code"] for c in item["candidates"] if c["code"] != item["matched"]["code"]]
            if item["id"] == 0:
                review = {"decision": "same", "confidence": "high"}
            elif item["id"] == 1 and others:
                review = {"decision": "other", "code": others[0], "confidence": "high"}
            elif item["id"] == 2:
                review = {"decision": "other", "code": "9999999", "confidence": "high"}
            else:
                review = {"decision": "uncertain", "confidence": "low"}
            reviews.append({"id": item["id"], "reason": "test", **review})
        return AIMessage(content="```json\n" + json.dumps({"reviews": reviews}) + "\n```", usage_metadata=USAGE)


@pytest.fixture(scope="module")
def reviewed(agents):
    agents["FakeReviewer"] = FakeReviewer
    agents["__overrides__"]["FULL_REFRESH"] = True  # start from an empty review table
    try:
        run_notebook("20_genai_match_review.py", agents, replace=FAKE)
    finally:
        agents["__overrides__"].pop("FULL_REFRESH")
        agents["FULL_REFRESH"] = False
    return agents


def reviews(ns):
    return ns["spark"].table(ns["tbl"]("silver", ns["REVIEW_TABLE"])).toPandas()


def test_every_fuzzy_match_is_reviewed_once(reviewed):
    fuzzy = reviewed["fuzzy"].count()
    table = reviews(reviewed)
    assert fuzzy > 0 and len(table) == fuzzy
    assert table.review_key.is_unique
    assert set(table.prompt_version) == {reviewed["PROMPT_VERSION"]}
    assert set(table.decision) <= reviewed["DECISIONS"]


def test_rerun_reviews_nothing_new(reviewed):
    before = len(reviews(reviewed))
    calls = FakeReviewer.calls
    run_notebook("20_genai_match_review.py", reviewed, replace=FAKE)
    assert reviewed["items"] == []
    assert len(reviews(reviewed)) == before
    assert FakeReviewer.calls == calls + 1  # only the golden-set evaluation called the model


def test_actions_follow_the_checked_decision(reviewed):
    table = reviews(reviewed).set_index("source_name")
    for _, row in table.iterrows():
        if row.decision == "same" and row.confidence == "high":
            assert row.action == "confirm" and row.suggested_code == row.matched_code
        elif row.decision == "other" and row.confidence == "high":
            assert row.action == "fix" and row.suggested_code != row.matched_code
        else:
            assert row.action == "human review"


def test_invented_code_is_rejected(reviewed):
    item = {"state_code": "SP", "matched_code": "3550308", "candidates": [{"code": "3550308", "name": "São Paulo"}]}
    review = reviewed["validate"](item, {"decision": "other", "code": "9999999", "confidence": "high"}, {})
    assert review["decision"] == "uncertain" and review["suggested_code"] is None
    assert "not another candidate" in review["validation"]
    assert reviewed["action_of"](review, item) == "human review"


def test_official_name_must_exist_in_ibge(reviewed):
    item = {"state_code": "SP", "matched_code": "1", "candidates": [{"code": "1", "name": "X"}]}
    names = {("SP", "MOGIMIRIM"): "3530706"}
    found = reviewed["validate"](item, {"decision": "none", "official_name": "Mogi Mirim", "confidence": "high"}, names)
    assert found["suggested_code"] == "3530706" and reviewed["action_of"](found, item) == "fix"
    invented = reviewed["validate"](item, {"decision": "none", "official_name": "Cidade Nova", "confidence": "high"}, names)
    assert invented["suggested_code"] is None and "not found in IBGE" in invented["validation"]


def test_malformed_answer_becomes_uncertain(reviewed):
    class Broken:
        def invoke(self, messages):
            return AIMessage(content="I think they are the same.", usage_metadata=USAGE)

    item = {"review_key": "k", "source": "TEST", "state_code": "SP", "source_name": "A", "matched_code": "1",
            "matched_name": "B", "rows": 1, "candidates": [{"code": "1", "name": "B"}]}
    result = reviewed["review_all"](Broken(), [item], "broken")
    assert result[0]["decision"] == "uncertain" and result[0]["action"] == "human review"
    assert result[0]["validation"] == "missing answer"


def test_golden_set_evaluation(reviewed):
    """evaluate() maps 'same' and anything else to the expected labels."""
    dim = reviewed["dim"].limit(2).collect()
    original = reviewed["GOLDEN_SET"]
    reviewed["GOLDEN_SET"] = [(dim[0].state_code, dim[0].municipality_name.upper(), dim[0].municipality_name, "same"),
                              (dim[1].state_code, "SOMEWHERE ELSE", dim[1].municipality_name, "different")]
    try:
        result = reviewed["evaluate"](FakeReviewer())  # answers "same" for item 0 and "other"/"uncertain" after
    finally:
        reviewed["GOLDEN_SET"] = original
    assert list(result.expected) == ["same", "different"]
    assert result.correct.all()
