"""n8n workflow (automation/n8n): structure, no secrets in Git, and the briefing SQL on the real Gold schema."""

import json
import re
from pathlib import Path

import pytest

N8N = Path(__file__).resolve().parents[1] / "automation" / "n8n"
WORKFLOW = N8N / "psi_weekly_briefing.json"
SQL = N8N / "briefing.sql"


@pytest.fixture(scope="module")
def workflow():
    return json.loads(WORKFLOW.read_text(encoding="utf-8"))


def nodes_by_name(workflow):
    return {n["name"]: n for n in workflow["nodes"]}


def test_connections_point_to_existing_nodes(workflow):
    names = set(nodes_by_name(workflow))
    for source, outputs in workflow["connections"].items():
        assert source in names, source
        for branch in outputs["main"]:
            for link in branch:
                assert link["node"] in names, f"{source} -> {link['node']}"


def test_every_node_is_reachable_from_a_trigger(workflow):
    nodes = nodes_by_name(workflow)
    edges = {s: {link["node"] for branch in o["main"] for link in branch} for s, o in workflow["connections"].items()}
    seen = {n for n, node in nodes.items() if node["type"].endswith(("Trigger", "manualTrigger"))}
    assert len(seen) == 2  # weekly schedule and manual run
    frontier = list(seen)
    while frontier:
        for target in edges.get(frontier.pop(), ()):
            if target not in seen:
                seen.add(target)
                frontier.append(target)
    assert seen == set(nodes)


def test_no_secrets_or_personal_data_in_git(workflow):
    text = WORKFLOW.read_text(encoding="utf-8")
    for pattern in [r"Bearer\s+\w", r"dapi[0-9a-f]{8}", r"AIza[\w-]{10}", r"\bAQ\.[\w-]{10}",
                    r"[\w.+-]+@[\w-]+\.[\w.]+", r"cloud\.databricks\.com", r"azuredatabricks\.net"]:
        assert not re.search(pattern, text), f"{pattern} found in the workflow"
    for node in workflow["nodes"]:
        for credential in node.get("credentials", {}).values():
            assert set(credential) == {"name"}, f"{node['name']}: credential must be referenced by name only"
    assert "pinData" not in workflow or not workflow["pinData"]


def test_settings_come_from_the_environment(workflow):
    config = nodes_by_name(workflow)["Config"]
    values = {a["name"]: a["value"] for a in config["parameters"]["assignments"]["assignments"]}
    for name in ["databricks_host", "job_id", "warehouse_id", "email_to"]:
        assert "$env." in values[name], name


def test_workflow_runs_briefing_sql(workflow):
    """The SQL in the Config node is the same as briefing.sql (the readable copy tested below)."""
    config = nodes_by_name(workflow)["Config"]
    values = {a["name"]: a["value"] for a in config["parameters"]["assignments"]["assignments"]}
    assert values["briefing_sql"].strip() == SQL.read_text(encoding="utf-8").strip()
    assert "briefing_sql" in nodes_by_name(workflow)["Query briefing data"]["parameters"]["jsonBody"]


def test_briefing_sql_on_the_gold_schema(agents):
    """Runs briefing.sql in local Spark against the tables built by the pipeline notebooks."""
    statement = SQL.read_text(encoding="utf-8").replace("workspace.", "")
    briefing = json.loads(agents["spark"].sql(statement).first()["briefing"])
    assert briefing["window_start"] <= briefing["window_end"]
    assert briefing["national"]["cvli_victims"] > 0 and briefing["national"]["cvli_rate_100k"] > 0
    highest, lowest = briefing["highest_state_rates"], briefing["lowest_state_rates"]
    assert [s["rank"] for s in highest] == list(range(1, len(highest) + 1))
    rates = [s["cvli_rate_100k"] for s in highest]
    assert rates == sorted(rates, reverse=True) and rates[0] >= lowest[0]["cvli_rate_100k"]
    assert all(c["population"] >= 100000 for c in briefing["largest_city_hotspots"])
    assert {"dataset", "keys"} == set(briefing["data_changes_last_7_days"][0])
    assert set(briefing["data_quality"]) == {"unmatched_municipality_names", "fuzzy_matches_to_review"}
