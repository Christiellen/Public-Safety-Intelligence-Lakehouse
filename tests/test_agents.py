"""Multi-agent flow (notebook 17) with a rule-based fake LLM and a fake Neo4j driver: routing, parallel
specialists, self-correction after guardrail rejections and query errors, answer language, model
fallback and the audit log. No network access and no API key are needed."""

import json
import re

import pandas as pd
import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

USAGE = {"input_tokens": 100, "output_tokens": 20, "total_tokens": 120}


class FakeLLM:
    """Same interface the graph uses (invoke, bind_tools). Decides by the system prompt of each agent."""

    def bind_tools(self, tools):
        return self

    def invoke(self, messages):
        system = messages[0].content
        question = next(m.content for m in messages if isinstance(m, HumanMessage))
        if system.startswith("You route"):
            agents = []
            if "border" in question or "neighbor" in question:
                agents.append("graph")
            if "rate" in question or "highest" in question:
                agents.append("sql")
            if question.startswith("Delete"):
                agents = []
            return AIMessage(
                content=json.dumps({"agents": agents, "reason": "test", "language": "English"}), usage_metadata=USAGE
            )
        if system.startswith("You write the final"):
            language = re.search(r"in (\w+)\. Be concise", system).group(1)
            return AIMessage(
                content=f"[{language}] " + messages[-1].content.split("Findings:\n", 1)[1], usage_metadata=USAGE
            )
        tool_messages = [m for m in messages if isinstance(m, ToolMessage)]
        if "SQL analyst" in system:
            attempts = [
                "SELECT * FROM psi_silver.dim_municipality",  # blocked
                "SELECT municipality_name FROM psi_gold.municipality_analytics WHERE nope > 0",  # error
                "SELECT municipality_name, state_code, round(cvli_rate_100k, 1) AS rate "
                "FROM psi_gold.municipality_analytics WHERE cvli_rate_100k IS NOT NULL ORDER BY rate DESC LIMIT 1",
            ]
            if len(tool_messages) < len(attempts):
                return self._call("run_sql", attempts[len(tool_messages)], len(tool_messages))
            return AIMessage(content="SQL: " + tool_messages[-1].content, usage_metadata=USAGE)
        if "graph analyst" in system:
            if not tool_messages:
                return self._call(
                    "run_cypher", "MATCH (m:PSI:Municipality)-[:BORDERS]-(n:PSI:Municipality) RETURN n.name AS name", 0
                )
            return AIMessage(content="Graph: " + tool_messages[-1].content, usage_metadata=USAGE)
        raise AssertionError(system[:80])

    @staticmethod
    def _call(tool, query, index):
        return AIMessage(
            content="",
            tool_calls=[{"name": tool, "args": {"query": query}, "id": f"{tool}{index}"}],
            usage_metadata=USAGE,
        )


@pytest.fixture(scope="module")
def assistant(agents):
    agents["execute_cypher"] = lambda query: pd.DataFrame([{"name": "Neighbor A"}, {"name": "Neighbor B"}])
    agents["_assistant"] = agents["build_assistant"]([FakeLLM()])
    agents["_model_name"] = "fake"
    return agents


def runs(ns):
    return ns["spark"].table(ns["tbl"]("gold", ns["RUNS_TABLE"])).toPandas()


def test_sql_question_self_corrects(assistant):
    result = assistant["ask"]("Which municipality has the highest rate?")
    statuses = [s["status"] for s in json.loads(result["trace"]) if s["type"] == "tool"]
    assert result["route"] == "sql"
    assert statuses == ["blocked", "error", "ok"]
    assert result["blocked_queries"] == 1 and result["failed_queries"] == 1
    assert "Bom Jesus" in result["answer"]  # highest rate in the fake data (RS)


def test_hybrid_question_runs_both_specialists(assistant):
    result = assistant["ask"]("Which neighbor of a border town has the highest rate?")
    assert set(result["route"].split(",")) == {"graph", "sql"}
    assert result["cypher_queries"] and result["sql_queries"]
    assert "Neighbor A" in result["answer"] and "SQL:" in result["answer"]


def test_write_request_queries_nothing(assistant):
    result = assistant["ask"]("Delete the municipality_analytics table.")
    assert result["route"] == "" and not result["sql_queries"] and not result["cypher_queries"]


def test_answer_language_comes_from_the_supervisor(assistant):
    result = assistant["ask"]("Which border municipalities?")
    assert result["answer"].startswith("[English]")


def test_every_run_is_audited(assistant):
    logged = runs(assistant)
    assert len(logged) >= 4
    assert {"run_id", "route", "sql_queries", "cypher_queries", "input_tokens", "latency_seconds"} <= set(
        logged.columns
    )
    assert logged.run_id.is_unique


# ----------------------------------------------------------------------------- model chain
class Failing:
    def __init__(self, message):
        self.message, self.calls = message, 0

    def invoke(self, messages):
        self.calls += 1
        raise RuntimeError(self.message)


class Ok:
    def invoke(self, messages):
        return AIMessage(content="ok")


def test_model_chain_falls_back_and_retires_missing_models(agents):
    retired = set()
    missing, busy = Failing("404 NOT_FOUND no longer available"), Failing("503 UNAVAILABLE")
    chain = agents["ModelChain"](["a", "b", "c"], [missing, busy, Ok()], retired)
    assert chain.invoke([]).content == "ok"
    assert chain.invoke([]).content == "ok"
    assert retired == {"a"} and missing.calls == 1 and busy.calls == 2


def test_model_chain_reports_every_failure(agents):
    chain = agents["ModelChain"](["a", "b"], [Failing("503 UNAVAILABLE"), Failing("429 RESOURCE_EXHAUSTED")], set())
    with pytest.raises(RuntimeError, match="(?s)a: .*503.*b: .*429"):
        chain.invoke([])


def test_model_selection(agents, monkeypatch):
    available = [
        "gemini-2.5-flash-lite",
        "gemini-3.5-flash",
        "gemini-3.6-flash",
        "gemini-3.8-flash",
        "gemini-3.8-flash-tts",
        "gemini-flash-latest",
        "gemini-omni-1.1-flash",
        "gemini-3-flash-preview",
    ]
    monkeypatch.setitem(agents, "GEMINI_MODEL_PREFERENCE", [])
    monkeypatch.setitem(agents, "GEMINI_FALLBACK_MODELS", 2)
    assert agents["model_chain"](available) == ["gemini-3.8-flash", "gemini-3.6-flash", "gemini-3.5-flash"]
    monkeypatch.setitem(agents, "GEMINI_MODEL_PREFERENCE", ["gemini-3.6-flash", "gemini-9-flash"])
    assert agents["model_chain"](available)[0] == "gemini-3.6-flash"


# ----------------------------------------------------------------------------- Neo4j execution path
def test_cypher_runs_in_a_read_transaction_with_timeout(agents, monkeypatch):
    import neo4j

    calls = {}

    class Result:
        def fetch(self, n):
            calls["fetch"] = n
            return [type("Record", (), {"data": lambda self: {"name": "Olinda"}})()]

    class Tx:
        def run(self, query, parameters=None, **kwargs):
            if isinstance(query, neo4j.Query):  # same rule as the real driver
                raise TypeError("Query object is only supported for session.run")
            return Result()

    class Session:
        def __init__(self, **kwargs):
            calls["mode"] = kwargs.get("default_access_mode")

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute_read(self, work):
            calls["timeout"] = getattr(work, "timeout", None)
            return work(Tx())

    class Driver:
        def session(self, **kwargs):
            return Session(**kwargs)

    monkeypatch.setattr(neo4j.GraphDatabase, "driver", staticmethod(lambda *a, **k: Driver()))
    monkeypatch.setitem(agents, "_neo4j_driver", None)
    from notebook_loader import run_cell

    namespace = dict(agents)
    run_cell("17_agents_multiagent.py", namespace, "def execute_cypher")  # the real function, not the fake
    frame = namespace["execute_cypher"]("MATCH (m:PSI:Municipality) RETURN m.name AS name")
    assert frame.name.tolist() == ["Olinda"]
    assert calls == {"mode": "READ", "timeout": agents["NEO4J_TIMEOUT_SECONDS"], "fetch": agents["MAX_ROWS"]}


# ----------------------------------------------------------------------------- evaluation notebook
def test_evaluation_notebook_runs_end_to_end(assistant):
    """Runs notebook 19 with the fake assistant. The fake data has no Recife or Salvador, so the
    ground-truth queries use municipalities of the fake mesh. The scores themselves are not checked
    (the fake LLM is not meant to answer well); the notebook must run and log one row per case."""
    from notebook_loader import code_cells

    namespace = assistant
    replacements = {'"Recife", "PE"': '"Sorocaba", "SP"', '"Salvador", "BA"': '"São Paulo", "SP"'}
    for cell in code_cells("19_agents_evaluation.py"):
        if cell.lstrip().startswith("# MAGIC"):
            continue  # %pip and %run 17 (already loaded with the fake LLM)
        for old, new in replacements.items():
            cell = cell.replace(old, new)
        exec(compile(cell, "19_agents_evaluation", "exec"), namespace)  # raises if a safety check fails
    results = namespace["spark"].table(namespace["tbl"]("gold", namespace["EVAL_TABLE"])).toPandas()
    assert len(results) == len(namespace["CASES"])
    assert results.safety_ok.all()
