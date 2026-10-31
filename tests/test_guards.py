"""Guardrails of the AI agents (notebook 17): what the SQL and Cypher tools accept and reject."""

import pytest

ALLOWED_SQL = [
    "SELECT municipality_name, cvli_rate_100k FROM psi_gold.municipality_analytics ORDER BY 2 DESC",
    "SELECT * FROM municipality_analytics LIMIT 5",
    "WITH t AS (SELECT state_code, avg(cvli_rate_100k) r FROM psi_gold.municipality_analytics GROUP BY 1) "
    "SELECT * FROM t",
    "SELECT a.municipality_name, s.lisa_cluster FROM psi_gold.municipality_analytics a "
    "JOIN psi_gold.municipality_spatial_metrics s USING (municipality_code)",
    "SELECT state_code FROM psi_gold.state_analytics UNION SELECT state_code FROM psi_gold.municipality_analytics",
]

BLOCKED_SQL = {
    "DROP TABLE psi_gold.municipality_analytics": "Only SELECT",
    "DELETE FROM psi_gold.municipality_analytics": "Only SELECT",
    "UPDATE psi_gold.state_analytics SET population = 0": "Only SELECT",
    "INSERT INTO psi_gold.state_analytics SELECT * FROM psi_gold.state_analytics": "Only SELECT",
    "CREATE TABLE psi_gold.x AS SELECT 1": "Only SELECT",
    "SELECT 1; DROP TABLE psi_gold.municipality_analytics": "exactly one",
    "SHOW TABLES IN psi_gold": "Only SELECT",
    "SELECT * FROM psi_silver.dim_municipality": "not allowed",
    "SELECT * FROM psi_gold.agent_runs": "not allowed",  # audit log is not documented
    "SELECT table_name FROM information_schema.tables": "not allowed",
    "SELECT * FROM other_catalog.psi_gold.municipality_analytics": "not allowed",
    "SELECT * FROM (SELECT * FROM psi_bronze.bronze_sinesp_occurrences)": "not allowed",
    "SELECT * FROM read_files('/Volumes/workspace/psi_bronze/raw_files')": "Table functions",
}

ALLOWED_CYPHER = [
    "MATCH (m:PSI:Municipality {is_national_hotspot: true})-[:BORDERS]-(n:PSI:Municipality) "
    "RETURN n.name, count(DISTINCT m) AS k ORDER BY k DESC LIMIT 5",
    "MATCH (m:PSI:Municipality)-[:IN_STATE]->(s:PSI:State {code: 'PE'}) WHERE m.name = 'Recife' RETURN m.name",
    "MATCH p = shortestPath((a:PSI:Municipality {name: 'Recife'})-[:BORDERS*..10]-(b:PSI:Municipality)) "
    "RETURN [n IN nodes(p) | n.name] AS path",
    "MATCH (c:PSI:Community) WHERE c.states CONTAINS 'PE' RETURN c.id // comment with CREATE",
    "MATCH (m:PSI:Municipality) WHERE m.name = 'SET DELETE' RETURN m.name",  # keywords inside strings
    "MATCH (m:PSI:Municipality {lisa_cluster: 'Low-High'}) "
    "OPTIONAL MATCH (m)-[:BORDERS]-(h:PSI:Municipality {is_national_hotspot: true}) RETURN m.name, count(h)",
]

BLOCKED_CYPHER = {
    "MATCH (m:PSI:Municipality) SET m.name = 'x'": "SET",
    "MATCH (m:PSI) DETACH DELETE m": "DETACH",
    "CREATE (:PSI:Municipality {name: 'x'})": "CREATE",
    "MATCH (m:PSI:Municipality) MERGE (m)-[:X]->(m)": "MERGE",
    "CALL db.labels()": "CALL",
    "LOAD CSV FROM 'http://x' AS row RETURN row": "LOAD",
    "MATCH (m:PSI:Municipality) CALL apoc.export.csv.all('x', {}) YIELD file RETURN file": "CALL",
    "MATCH (m:PSI) RETURN m; MATCH (n) DELETE n": "exactly one",
    "MATCH (n) RETURN n LIMIT 10": "without the :PSI label",  # other projects' nodes
    "MATCH (n:Customer) RETURN n": "must include :PSI",
    "MATCH (m:PSI:Municipality)-[:BORDERS]-(x:Other) RETURN x": "must include :PSI",
    "MATCH (a:PSI:Municipality)-[:BORDERS]-(b) RETURN b": "without the :PSI label",
    "RETURN 1": "at least one",
}


@pytest.mark.parametrize("query", ALLOWED_SQL)
def test_sql_allowed_and_qualified(agents, query):
    safe = agents["guard_sql"](query)
    assert "LIMIT" in safe.upper()
    assert agents["execute_sql"](safe) is not None  # runs on the local Gold tables


@pytest.mark.parametrize("query, reason", BLOCKED_SQL.items())
def test_sql_blocked(agents, query, reason):
    with pytest.raises(agents["GuardError"], match=reason):
        agents["guard_sql"](query)


def test_sql_limit_is_capped(agents):
    safe = agents["guard_sql"]("SELECT * FROM psi_gold.municipality_analytics LIMIT 100000")
    assert safe.endswith(f"LIMIT {agents['MAX_ROWS']}")


def test_sql_allowlist_comes_from_the_dictionary(agents):
    assert set(agents["ALLOWED_TABLES"]) == set(agents["DICTIONARY"])


@pytest.mark.parametrize("query", ALLOWED_CYPHER)
def test_cypher_allowed(agents, query):
    agents["guard_cypher"](query)


@pytest.mark.parametrize("query, reason", BLOCKED_CYPHER.items())
def test_cypher_blocked(agents, query, reason):
    with pytest.raises(agents["GuardError"], match=reason):
        agents["guard_cypher"](query)
